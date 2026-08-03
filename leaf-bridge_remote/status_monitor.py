"""
status_monitor.py — isolated stream-stability monitor for the leaf master.

Design goals (in priority order):
  1. NEVER affect the streaming path. Runs as ONE asyncio task. Every poll and
     every Mongo write is individually exception-guarded; the whole loop is
     wrapped so a crash logs and dies ALONE — streaming keeps running untouched.
  2. Poll like a real player: hits each stream's PUBLIC playlist URL
     ({LEAF_SERVER_PUBLIC_URL}/m3u/{slug}/live.m3u8), never worker internals.
     4K streams need a token → we reuse SCHEDULER_TOKEN (the token the master
     already owns, which passes _validate_token).
  3. Bounded cost: a semaphore caps concurrency, per-request timeouts are short,
     and results are held in memory and flushed to Mongo every STATUS_FLUSH_SECS
     (not per-poll) so Atlas write volume stays tiny.
  4. Atlas 500MB safe: ONE rolling document per stream-session, updated in place
     (never per-poll rows). A TTL index on `ttl_at` auto-purges ended sessions
     STATUS_TTL_DAYS after their last update. Live docs refresh ttl_at on every
     flush so they never expire while running.

Metrics recorded per session:
  - up/down polls, uptime %, accumulated downtime, longest single outage
  - irregular segment durations (any #EXTINF that deviates from 6.0s by more
    than the tolerance — e.g. a 5.6s segment), with capped samples
  - playlist stalls (media-sequence not advancing across polls = frozen feed)
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger("leaf.status_monitor")

# ── Tunables (env-overridable) ────────────────────────────────────────────────
STATUS_POLL_SECS   = int(os.getenv("STATUS_POLL_SECS", "6"))     # player-like cadence
STATUS_CONCURRENCY = int(os.getenv("STATUS_CONCURRENCY", "10"))  # max simultaneous polls
STATUS_TTL_DAYS    = int(os.getenv("STATUS_TTL_DAYS", "2"))      # past-data retention
STATUS_FLUSH_SECS  = int(os.getenv("STATUS_FLUSH_SECS", "30"))   # Mongo write cadence
STATUS_REQ_TIMEOUT = float(os.getenv("STATUS_REQ_TIMEOUT", "5")) # per-request timeout
_SEG_TARGET        = float(os.getenv("STATUS_SEG_TARGET", "6.0"))
_SEG_TOLERANCE     = float(os.getenv("STATUS_SEG_TOLERANCE", "0.35"))  # ±0.35s = OK
_MAX_IRREGULAR_SAMPLES = 30
# A real freeze = the playlist media-sequence hasn't advanced for this many
# WALL-CLOCK seconds. Poll-count comparison aliased with the 6s segment cadence
# and produced false "stalls"; wall-clock is immune to that jitter.
_STALL_SECS        = float(os.getenv("STATUS_STALL_FREEZE_SECS", "20"))
# Debounce: a single transient 404/timeout (normal buffering during segment
# rollover) must NOT flip a stream to "down" — a real player just retries. Only
# count as down after this many CONSECUTIVE bad polls, like a player giving up.
_DOWN_STREAK_POLLS = int(os.getenv("STATUS_DOWN_AFTER_BAD", "2"))
_COLLECTION        = "leaf_stream_status"

# ── In-memory session state (source of truth for LIVE cards) ──────────────────
_sessions: dict[str, dict] = {}          # slug → session dict
_client:   Optional[httpx.AsyncClient] = None
_started   = False

_EXTINF_RE = re.compile(r"#EXTINF:([\d.]+)")
_SEQ_RE    = re.compile(r"#EXT-X-MEDIA-SEQUENCE:(\d+)")

# Fields persisted to Mongo (everything except the in-memory-only bookkeeping)
_PERSIST_FIELDS = (
    "_id", "slug", "display_name", "source", "session_start", "last_seen",
    "event_end", "ended_at", "status", "polls_total", "polls_up", "polls_down",
    "downtime_secs", "longest_outage_secs", "irregular_count",
    "irregular_samples", "stall_total",
)


def _public_base() -> str:
    try:
        from config import LEAF_SERVER_PUBLIC_URL
        return (LEAF_SERVER_PUBLIC_URL or "").rstrip("/")
    except Exception:
        return ""


def _token() -> str:
    # Reuse the token the master already owns for the backend — it passes
    # _validate_token, so 4K streams (which require a premium token) return 200.
    return os.getenv("SCHEDULER_TOKEN", "") or os.getenv("ADDON_ADMIN_TOKEN", "")


async def _get_col():
    """Shared motor client from iptv_accounts — no new connection pool."""
    try:
        import iptv_accounts as _ia
        _ia._get_motor_col()
        if _ia._motor_client is None:
            return None
        return _ia._motor_client["stremio"][_COLLECTION]
    except Exception:
        return None


async def _ensure_ttl() -> None:
    """TTL index on ttl_at: purge ended sessions STATUS_TTL_DAYS after last update.
    Live sessions refresh ttl_at on every flush so they never expire mid-run."""
    col = await _get_col()
    if col is None:
        return
    want = STATUS_TTL_DAYS * 86400
    try:
        await col.create_index("ttl_at", expireAfterSeconds=want, name="ttl_at_idx")
    except Exception:
        # Index likely exists with a different TTL — drop and recreate.
        try:
            await col.drop_index("ttl_at_idx")
            await col.create_index("ttl_at", expireAfterSeconds=want, name="ttl_at_idx")
        except Exception as e:
            logger.warning("status: TTL index setup failed: %s", e)


def _enumerate_live() -> dict[str, dict]:
    """slug → meta for streams actually routed to a worker and not pending.
    Reads master in-memory maps; imported lazily to avoid circular imports."""
    out: dict[str, dict] = {}
    try:
        from leaf_server import _slug_to_worker
        from leaf_admin import _recording_meta
    except Exception:
        return out
    for slug in list(_slug_to_worker.keys()):
        meta = _recording_meta.get(slug, {}) or {}
        if meta.get("pending_start"):
            continue  # scheduled-but-not-started — nothing to poll yet
        out[slug] = meta
    return out


def _parse_playlist(text: str):
    """Return (media_sequence:int|None, segments:list[(seg_key, duration|None)])."""
    seq = None
    segs = []
    pending_dur = None
    for raw in text.splitlines():
        ln = raw.strip()
        if not ln:
            continue
        if ln.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            m = _SEQ_RE.match(ln)
            if m:
                seq = int(m.group(1))
        elif ln.startswith("#EXTINF:"):
            m = _EXTINF_RE.match(ln)
            pending_dur = float(m.group(1)) if m else None
        elif not ln.startswith("#"):
            key = ln.rsplit("/", 1)[-1] or ln   # seg_241.ts
            segs.append((key, pending_dur))
            pending_dur = None
    return seq, segs


def _start_session(slug: str, meta: dict) -> None:
    now = time.time()
    event_end = meta.get("auto_end_at") or meta.get("ends_at")
    _sessions[slug] = {
        "_id":                 f"{slug}:{int(now)}",
        "slug":                slug,
        "display_name":        meta.get("display_name", slug),
        "source":              meta.get("source") or "iptv",
        "session_start":       now,
        "last_seen":           0.0,
        "event_end":           event_end,
        "ended_at":            None,
        "status":              "live",
        "polls_total":         0,
        "polls_up":            0,
        "polls_down":          0,
        "downtime_secs":       0.0,
        "longest_outage_secs": 0.0,
        "cur_outage_start":    None,       # in-memory only
        "irregular_count":     0,
        "irregular_samples":   [],
        "stall_total":         0,
        "last_seq":            None,       # in-memory only
        "last_seq_change_at":  now,        # in-memory only — wall clock of last seq move
        "stall_counted":       False,      # in-memory only — count each freeze once
        "bad_streak":          0,          # in-memory only — debounce false-downs
        "seen_segs":           set(),      # in-memory only, bounded
        "dirty":               True,
    }
    logger.info("status: monitoring '%s' (event_end=%s)", slug, event_end)


async def _poll_one(slug: str, meta: dict) -> None:
    st = _sessions.get(slug)
    if st is None:
        return
    now = time.time()
    base = _public_base()
    if not base:
        return
    tok = _token()
    url = f"{base}/m3u/{slug}/live.m3u8" + (f"?token={tok}" if tok else "")

    # `raw_ok` = this single fetch succeeded (200 + real playlist + advancing feed).
    # `ok` (the value that drives up/down) is DEBOUNCED below: a real player rides
    # out a transient 404/blip by retrying, so one bad fetch must NOT mark the
    # stream down. Only _DOWN_STREAK_POLLS consecutive bad fetches = truly down.
    raw_ok = False
    try:
        r = await _client.get(url)
        if r.status_code == 200 and "#EXTINF" in r.text:
            seq, segs = _parse_playlist(r.text)

            # Wall-clock stall detection. A healthy live playlist advances its
            # media-sequence as new segments arrive. Comparing seq between polls
            # naively is wrong: at a 6s poll cadence on 6s segments the two alias,
            # so a poll landing just before the playlist updates sees the SAME seq
            # and looks "stalled" when nothing is wrong. Instead we track WHEN the
            # seq last actually changed; only a seq stuck for >_STALL_SECS (real
            # freeze) counts, and each such freeze is counted ONCE (not per poll).
            if seq is not None:
                if st["last_seq"] is None or seq != st["last_seq"]:
                    st["last_seq"] = seq
                    st["last_seq_change_at"] = now
                    st["stall_counted"] = False
                else:
                    stalled_for = now - (st.get("last_seq_change_at") or now)
                    if stalled_for >= _STALL_SECS and not st["stall_counted"]:
                        st["stall_total"] += 1
                        st["stall_counted"] = True

            # Frozen feed (seq stuck past the stall threshold) reads as NOT ok.
            frozen = (st.get("last_seq_change_at") is not None
                      and (now - st["last_seq_change_at"]) >= _STALL_SECS)
            raw_ok = not frozen

            # Irregular-duration detection on NEW segments only (sliding window →
            # dedup by filename so one bad segment isn't counted every poll).
            for seg_key, dur in segs:
                if seg_key in st["seen_segs"]:
                    continue
                st["seen_segs"].add(seg_key)
                if dur is not None and abs(dur - _SEG_TARGET) > _SEG_TOLERANCE:
                    st["irregular_count"] += 1
                    if len(st["irregular_samples"]) < _MAX_IRREGULAR_SAMPLES:
                        st["irregular_samples"].append(
                            {"seg": seg_key, "dur": round(dur, 3), "at": int(now)}
                        )
            if len(st["seen_segs"]) > 600:   # keep memory bounded
                st["seen_segs"] = set(list(st["seen_segs"])[-300:])
    except Exception:
        raw_ok = False

    # ── Debounce: convert a run of raw fetches into an up/down verdict ──────────
    # Track consecutive bad fetches. The stream is only counted DOWN once we've
    # seen _DOWN_STREAK_POLLS bad fetches in a row — mirroring how a real player
    # retries through a momentary 404 without the viewer ever seeing an outage.
    if raw_ok:
        st["bad_streak"] = 0
    else:
        st["bad_streak"] += 1
    down = st["bad_streak"] >= _DOWN_STREAK_POLLS

    st["polls_total"] += 1
    if not down:
        st["polls_up"] += 1
        if st["cur_outage_start"] is not None:
            outage = now - st["cur_outage_start"]
            st["longest_outage_secs"] = max(st["longest_outage_secs"], outage)
            st["cur_outage_start"] = None
        if raw_ok:
            st["last_seen"] = now
    else:
        st["polls_down"] += 1
        st["downtime_secs"] += STATUS_POLL_SECS   # approx (poll granularity)
        if st["cur_outage_start"] is None:
            # Anchor the outage at the FIRST bad fetch, not the Nth, so the
            # measured outage duration is honest.
            st["cur_outage_start"] = now - _DOWN_STREAK_POLLS * STATUS_POLL_SECS
    st["dirty"] = True


def _doc_from_session(st: dict, ttl_now: datetime) -> dict:
    doc = {k: st.get(k) for k in _PERSIST_FIELDS}
    total = st["polls_total"] or 1
    doc["uptime_pct"] = round(st["polls_up"] / total * 100, 2)
    doc["ttl_at"] = ttl_now
    return doc


async def _flush(only_slug: Optional[str] = None) -> None:
    col = await _get_col()
    if col is None:
        return
    ttl_now = datetime.now(timezone.utc)
    slugs = [only_slug] if only_slug else list(_sessions.keys())
    for slug in slugs:
        st = _sessions.get(slug)
        if st is None or not st.get("dirty"):
            continue
        try:
            await col.update_one({"_id": st["_id"]},
                                 {"$set": _doc_from_session(st, ttl_now)},
                                 upsert=True)
            st["dirty"] = False
        except Exception as e:
            logger.debug("status: flush failed for %s: %s", slug, e)


async def _end_session(slug: str) -> None:
    """Stream is no longer live: finalize the doc and drop it from memory.
    ttl_at freezes at end time → Mongo purges it STATUS_TTL_DAYS later."""
    st = _sessions.get(slug)
    if st is None:
        return
    st["status"] = "ended"
    st["ended_at"] = time.time()
    if st["cur_outage_start"] is not None:
        outage = time.time() - st["cur_outage_start"]
        st["longest_outage_secs"] = max(st["longest_outage_secs"], outage)
        st["cur_outage_start"] = None
    st["dirty"] = True
    await _flush(only_slug=slug)
    _sessions.pop(slug, None)
    logger.info("status: '%s' ended — finalized", slug)


# ── Public read API (used by the /status page + admin tab) ────────────────────
def _live_snapshot() -> list[dict]:
    out = []
    for st in _sessions.values():
        total = st["polls_total"] or 1
        out.append({
            "slug":                st["slug"],
            "display_name":        st["display_name"],
            "source":              st["source"],
            "status":              "live",
            "session_start":       st["session_start"],
            "last_seen":           st["last_seen"],
            "event_end":           st["event_end"],
            "uptime_pct":          round(st["polls_up"] / total * 100, 2),
            "polls_total":         st["polls_total"],
            "polls_up":            st["polls_up"],
            "polls_down":          st["polls_down"],
            "downtime_secs":       round(st["downtime_secs"], 1),
            "longest_outage_secs": round(st["longest_outage_secs"], 1),
            "irregular_count":     st["irregular_count"],
            "irregular_samples":   st["irregular_samples"][-_MAX_IRREGULAR_SAMPLES:],
            "stall_total":         st["stall_total"],
            "currently_down":      st["cur_outage_start"] is not None,
        })
    out.sort(key=lambda d: d["display_name"].lower())
    return out


async def get_status_data() -> dict:
    """Return {'live': [...], 'past': [...]}. Live from memory (fresh); past from
    Mongo (ended sessions, still within the TTL window). Fully guarded."""
    live = _live_snapshot()
    live_ids = {s["slug"] for s in live}
    past = []
    try:
        col = await _get_col()
        if col is not None:
            cur = col.find({"status": "ended"}).sort("ended_at", -1).limit(200)
            async for d in cur:
                d.pop("_id", None)
                d.pop("ttl_at", None)
                # Don't show an ended row for a slug that's live again
                if d.get("slug") in live_ids:
                    continue
                past.append(d)
    except Exception as e:
        logger.debug("status: past query failed: %s", e)
    return {"live": live, "past": past,
            "poll_secs": STATUS_POLL_SECS, "ttl_days": STATUS_TTL_DAYS}


# ── Main loop ─────────────────────────────────────────────────────────────────
async def run_status_monitor() -> None:
    """Single background task. Master-mode only. Any failure is contained here."""
    global _client, _started
    if os.getenv("LEAF_MODE", "standalone") != "master":
        return
    if _started:
        return
    _started = True

    await asyncio.sleep(20)  # let workers register + streams come up first

    _client = httpx.AsyncClient(
        timeout=STATUS_REQ_TIMEOUT,
        limits=httpx.Limits(max_connections=STATUS_CONCURRENCY * 2,
                            max_keepalive_connections=STATUS_CONCURRENCY),
        follow_redirects=True,
    )
    try:
        await _ensure_ttl()
    except Exception as e:
        logger.warning("status: ttl init error: %s", e)

    sem = asyncio.Semaphore(STATUS_CONCURRENCY)
    last_flush = 0.0
    logger.info("📊 status monitor started (poll=%ss, concurrency=%d, ttl=%dd)",
                STATUS_POLL_SECS, STATUS_CONCURRENCY, STATUS_TTL_DAYS)

    while True:
        loop_start = time.time()
        try:
            live = _enumerate_live()

            # Begin sessions for newly-live slugs
            for slug, meta in live.items():
                if slug not in _sessions:
                    _start_session(slug, meta)

            # End sessions for slugs that vanished (event over / stopped)
            for slug in list(_sessions.keys()):
                if slug not in live:
                    await _end_session(slug)

            # Poll all live slugs concurrently, capped by the semaphore.
            async def _guarded(_s, _m):
                async with sem:
                    try:
                        await _poll_one(_s, _m)
                    except Exception:
                        pass

            if live:
                await asyncio.gather(*[_guarded(s, m) for s, m in live.items()],
                                     return_exceptions=True)

            if time.time() - last_flush >= STATUS_FLUSH_SECS:
                await _flush()
                last_flush = time.time()
        except Exception as e:
            logger.warning("status monitor loop error (contained): %s", e)

        # Keep a steady cadence regardless of how long the polls took.
        elapsed = time.time() - loop_start
        await asyncio.sleep(max(1.0, STATUS_POLL_SECS - elapsed))
