"""
leaf_ratelimit.py — per-token / per-IP stream rate limiting for the Leaf MASTER.

Only the master node serves end users, so this is wired into leaf_server.py's
aiohttp middleware and is a no-op unless LEAF_MODE == "master".

Two independent limiters, both using a "concurrent active streams" model
(sliding TTL — NOT a fixed calendar window):

  • Premium (request carries a valid ?token=): keyed by token. A token may hold
    at most PREMIUM_MAX_STREAMS distinct slugs "active" at once. A slug counts as
    active while it has been requested within ACTIVE_TTL seconds. Re-polling a
    slug you already hold ALWAYS passes (never breaks a stream you're watching).
    Stopping a stream frees its slot after ACTIVE_TTL. This caps simultaneous
    streams / account-sharing without ever locking a live viewer out.

  • Free (token-less, or token that failed validation): keyed by client IP. Same
    concurrent-slug cap (FREE_MAX_STREAMS) PLUS a flood cap (FREE_RATE_MAX
    requests per FREE_RATE_WINDOW s) to stop scraping/hammering.

Infrastructure (workers, backends, recorders, poster, localhost) is exempted by
IP so its HEAD availability checks and internal traffic are never counted. HEAD
requests are exempted by the caller regardless of IP.

Single event loop, no threads touch this state, and there is no await between the
read and the write inside each check — so plain dict ops are atomic here.
"""

from __future__ import annotations

import logging
import os
import time
from collections import deque
from typing import NamedTuple

logger = logging.getLogger("leaf.ratelimit")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


# ── Tunables (env-overridable) ──────────────────────────────────────────────
ENABLED           = os.getenv("LEAF_RL_ENABLED", "true").lower() not in ("0", "false", "no")
# Per-token DEVICE cap: max distinct client IPs allowed to use one token within
# ACTIVE_TTL. This is the ONLY premium control — a real subscriber streams from
# a handful of devices (a few IPs); a reseller (e.g. scorezonetv.live) fans one
# token out to hundreds of IPs. The (limit+1)-th distinct IP on a token is 429'd.
# There is intentionally NO per-match cap — it counted matches not viewers and
# false-429'd real requests.
PREMIUM_MAX_IPS     = _env_int("LEAF_RL_PREMIUM_MAX_IPS", 4)
FREE_MAX_STREAMS    = _env_int("LEAF_RL_FREE_MAX_STREAMS", 3)
ACTIVE_TTL          = _env_int("LEAF_RL_ACTIVE_TTL", 120)          # seconds a slug/IP stays "active"
FREE_RATE_MAX       = _env_int("LEAF_RL_FREE_RATE_MAX", 240)       # max requests ...
FREE_RATE_WINDOW    = _env_int("LEAF_RL_FREE_RATE_WINDOW", 60)     # ... per this window (s)

# Infra IPs that must never be rate-limited (workers, backends, recorders, poster).
# Merged with anything in LEAF_RL_EXEMPT_IPS (comma-separated) for easy additions
# without a code change.
_DEFAULT_EXEMPT_IPS = {
    # localhost / the master itself
    "127.0.0.1", "::1", "152.53.187.232",
    # leaf workers (LEAF_WORKERS)
    "206.189.199.203", "159.223.98.4", "147.182.209.110", "142.93.118.241",
    # sportsaddon backends (HEAD-check playlists + poll catalog)
    "135.235.218.139",   # Free VPS 1
    "192.241.132.251",   # Free VPS 2
    "24.144.97.121",     # Free VPS 3
    "52.237.80.112",     # Premium Main
    "172.183.129.215",   # Premium US-1
    "52.159.88.252",     # Premium US-2
    "152.53.17.30",      # Premium EU-1 (new)
    "68.210.201.192",    # Premium EU-1 (old/cold backup)
    # poster / recorders / bolt
    "20.40.56.148",      # Poster VPS
    "178.62.57.45",      # Recorder (old) + Bolt VPS
    "159.223.139.163",   # Recorder (new)
}


def _load_exempt_ips() -> set[str]:
    ips = set(_DEFAULT_EXEMPT_IPS)
    extra = os.getenv("LEAF_RL_EXEMPT_IPS", "")
    for part in extra.replace(";", ",").split(","):
        part = part.strip()
        if part:
            ips.add(part)
    return ips


EXEMPT_IPS = _load_exempt_ips()

# ── State ───────────────────────────────────────────────────────────────────
# token → {client_ip: last_seen_monotonic}  (per-token device cap)
_premium_ips: dict[str, dict[str, float]] = {}
# ip → {slug: last_seen_monotonic}  (free per-IP concurrent-stream cap)
_free_slugs: dict[str, dict[str, float]] = {}
# ip → deque[monotonic] of recent request times (for the free flood cap)
_free_hits: dict[str, "deque[float]"] = {}

_last_sweep = 0.0
_SWEEP_INTERVAL = 300.0   # drop fully-idle keys every 5 min


class Decision(NamedTuple):
    allowed: bool
    reason: str          # "" when allowed; else short machine tag
    retry_after: int     # seconds hint for 429 Retry-After (0 if n/a)


_ALLOW = Decision(True, "", 0)


def is_exempt_ip(ip: str) -> bool:
    return ip in EXEMPT_IPS


def _prune_slugs(slots: dict[str, float], now: float) -> None:
    """Drop slugs not seen within ACTIVE_TTL. Mutates in place."""
    dead = [s for s, ts in slots.items() if now - ts > ACTIVE_TTL]
    for s in dead:
        slots.pop(s, None)


def _check_concurrent(store: dict[str, dict[str, float]], key: str,
                      slug: str, limit: int, now: float) -> bool:
    """Concurrent-active-streams gate. Returns True if allowed.

    Atomic under asyncio: no await between the read and the write below."""
    slots = store.get(key)
    if slots is None:
        slots = {}
        store[key] = slots
    _prune_slugs(slots, now)
    if slug in slots:
        slots[slug] = now            # refresh — an active stream never blocks
        return True
    if len(slots) >= limit:
        return False                 # would be the (limit+1)-th distinct stream
    slots[slug] = now
    return True


def _check_free_rate(ip: str, now: float) -> bool:
    """Sliding-window flood cap for free IPs. Returns True if allowed."""
    dq = _free_hits.get(ip)
    if dq is None:
        dq = deque()
        _free_hits[ip] = dq
    cutoff = now - FREE_RATE_WINDOW
    while dq and dq[0] < cutoff:
        dq.popleft()
    if len(dq) >= FREE_RATE_MAX:
        return False
    dq.append(now)
    return True


def check_request(*, ip: str, token_key: str | None, slug: str) -> Decision:
    """Resolve limiter + key and return a Decision.

    token_key is a validated premium token (already confirmed active) or None.
    ip is the real client IP (already confirmed non-exempt)."""
    if not ENABLED or not slug:
        return _ALLOW

    now = time.monotonic()
    _maybe_sweep(now)

    if token_key:
        # Premium: ONLY the device cap — a token may be used from at most
        # PREMIUM_MAX_IPS distinct client IPs within ACTIVE_TTL. An already-seen
        # IP always refreshes and passes (real viewer keeps streaming); a NEW IP
        # beyond the cap is rejected. There is deliberately NO per-match cap —
        # that counted matches not viewers and false-429'd legit requests when a
        # token legitimately watched more than N matches. Device cap alone kills
        # reselling (hundreds of IPs) without ever blocking a real subscriber's
        # matches. _check_concurrent treats each IP like a member of the token's
        # active-IP set.
        if _check_concurrent(_premium_ips, token_key, ip, PREMIUM_MAX_IPS, now):
            return _ALLOW
        return Decision(False, "premium_device_cap", ACTIVE_TTL)

    # Free / token-less → per-IP concurrent cap + flood cap.
    if not _check_free_rate(ip, now):
        return Decision(False, "free_rate", FREE_RATE_WINDOW)
    if _check_concurrent(_free_slugs, ip, slug, FREE_MAX_STREAMS, now):
        return _ALLOW
    return Decision(False, "free_stream_cap", ACTIVE_TTL)


def _maybe_sweep(now: float) -> None:
    """Periodically drop fully-idle keys so memory stays bounded even as tokens
    and IPs churn. Cheap: runs at most once per _SWEEP_INTERVAL."""
    global _last_sweep
    if now - _last_sweep < _SWEEP_INTERVAL:
        return
    _last_sweep = now
    for store in (_premium_ips, _free_slugs):
        for key in list(store.keys()):
            slots = store[key]
            _prune_slugs(slots, now)
            if not slots:
                store.pop(key, None)
    cutoff = now - FREE_RATE_WINDOW
    for ip in list(_free_hits.keys()):
        dq = _free_hits[ip]
        while dq and dq[0] < cutoff:
            dq.popleft()
        if not dq:
            _free_hits.pop(ip, None)


def stats() -> dict:
    """Snapshot for the admin panel / debugging."""
    now = time.monotonic()
    return {
        "enabled": ENABLED,
        "premium_max_ips": PREMIUM_MAX_IPS,
        "free_max_streams": FREE_MAX_STREAMS,
        "active_ttl": ACTIVE_TTL,
        "free_rate": f"{FREE_RATE_MAX}/{FREE_RATE_WINDOW}s",
        "tracked_tokens": len(_premium_ips),
        "tracked_free_ips": len(_free_slugs),
        "exempt_ip_count": len(EXEMPT_IPS),
        # tokens using more than PREMIUM_MAX_IPS distinct IPs = likely resellers
        "premium_device_counts": {
            k: sum(1 for ts in v.values() if now - ts <= ACTIVE_TTL)
            for k, v in sorted(_premium_ips.items(),
                               key=lambda kv: -len(kv[1]))[:20]
        },
    }

