"""
accounts.py - IPTV provider accounts for the ClearTV leaf.

Accounts live in a JSON file under DATA_DIR. No database: one leaf owns one
file, and that is the whole deployment model.

An account is an Xtream Codes login (server + username + password). Providers cap
concurrent connections per account, so the leaf records which channel an account
is currently serving and refuses to double-book it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Optional

import httpx

import config

logger = logging.getLogger("leaf.accounts")

_BASE_HEADERS = {"User-Agent": config.UPSTREAM_USER_AGENT, "Accept": "*/*"}

# What a provider calls an account that can no longer stream.
_DEAD_STATUSES = {"expired", "disabled", "banned"}

_accounts: dict[str, dict] = {}
_loaded = False

# Serialises claim/release so two concurrent "start channel" clicks cannot both
# take the same account.
_claim_lock: asyncio.Lock | None = None

# account_id -> (fetched_at, [channel, ...])
_channel_cache: dict[str, tuple[float, list[dict]]] = {}


def _lock() -> asyncio.Lock:
    global _claim_lock
    if _claim_lock is None:
        _claim_lock = asyncio.Lock()
    return _claim_lock

# ── persistence ───────────────────────────────────────────────────────────────

def _load() -> dict[str, dict]:
    """Read accounts from disk once, then serve from memory."""
    global _accounts, _loaded
    if _loaded:
        return _accounts
    try:
        with open(config.ACCOUNTS_FILE, encoding="utf-8") as fh:
            data = json.load(fh)
        _accounts = data if isinstance(data, dict) else {}
    except FileNotFoundError:
        _accounts = {}
    except Exception as exc:
        # Never start with a half-parsed file: an unreadable store is treated as
        # empty and left on disk so it can be inspected rather than overwritten.
        logger.error("accounts file unreadable (%s) — starting empty", exc)
        _accounts = {}
    _loaded = True
    return _accounts


def _save() -> None:
    """Write accounts atomically so a crash mid-write cannot corrupt the file."""
    os.makedirs(os.path.dirname(config.ACCOUNTS_FILE) or ".", exist_ok=True)
    tmp = config.ACCOUNTS_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(_accounts, fh, indent=2)
        os.replace(tmp, config.ACCOUNTS_FILE)
        # Credentials — keep it owner-only.
        os.chmod(config.ACCOUNTS_FILE, 0o600)
    except Exception as exc:
        logger.error("accounts save failed: %s", exc)


def _fields_from_info(info: dict) -> dict:
    """
    Turn a fetch_account_info result into the fields stored on an account.

    An unreachable provider writes nothing but the check timestamp: we cannot
    tell a bad password from a dead server, so the last known good status is left
    alone rather than being replaced with blanks.
    """
    if info.get("auth_failed"):
        return {"auth_ok": False, "status": "Rejected", "info_checked_at": int(time.time())}
    if info.get("error"):
        return {"info_checked_at": int(time.time())}
    return {
        "auth_ok": True,
        "status": info.get("status", ""),
        "exp_date": info.get("exp_date"),
        "max_connections": info.get("max_connections", ""),
        "info_checked_at": int(time.time()),
    }


def is_usable(acc: dict) -> bool:
    """
    False when the provider says this account cannot stream.

    A never-checked account (no status yet, e.g. added while the provider was
    briefly down) gets the benefit of the doubt — only an explicit rejection or a
    dead status disqualifies it, so a momentary outage cannot empty the pool.
    """
    if acc.get("auth_ok") is False:
        return False
    if str(acc.get("status", "")).strip().lower() in _DEAD_STATUSES:
        return False
    exp = acc.get("exp_date")
    if exp:
        try:
            if int(exp) < time.time():
                return False
        except (TypeError, ValueError):
            pass
    return True


# ── public API ────────────────────────────────────────────────────────────────

def list_accounts() -> list[dict]:
    """Accounts for the panel. Passwords are never included."""
    out = []
    for acc_id, acc in _load().items():
        out.append({
            "id": acc_id,
            "name": acc.get("name", acc_id),
            "server": acc.get("server", ""),
            "username": acc.get("username", ""),
            "has_password": bool(acc.get("password")),
            "added_at": acc.get("added_at", ""),
            "status": acc.get("status", ""),
            "exp_date": acc.get("exp_date"),
            "max_connections": acc.get("max_connections", ""),
            "usable": is_usable(acc),
            "current_slug": acc.get("current_slug"),
            "busy": bool(acc.get("current_slug")),
            "checked_at": acc.get("info_checked_at"),
        })
    return sorted(out, key=lambda a: a["name"].lower())


def get_account(account_id: str) -> Optional[dict]:
    return _load().get(account_id)


def probe_credentials(server: str, username: str, password: str) -> dict:
    """
    Check credentials against the provider without saving anything.

    Callers use this before persisting, so a mistyped password can never
    overwrite the working one on an account that is already stored. Blocking —
    run it in an executor.
    """
    return fetch_account_info({
        "server": server.rstrip("/"),
        "username": username,
        "password": password,
    })


def add_account(name: str, server: str, username: str, password: str, info: dict | None = None) -> str:
    """
    Add or update an account. Returns its id.

    Re-adding the same server+username updates the existing record rather than
    creating a duplicate, so fixing a typo'd password does not orphan a claim.

    `info` is a probe_credentials() result. When supplied, its status/expiry are
    stored in the same write, so an account is never briefly present with unknown
    state.
    """
    accounts = _load()
    probed = _fields_from_info(info) if info else {}

    for acc_id, acc in accounts.items():
        if acc.get("server", "").rstrip("/") == server.rstrip("/") and acc.get("username") == username:
            acc.update({"name": name, "password": password, **probed})
            _save()
            _channel_cache.pop(acc_id, None)
            logger.info("account updated: %s", acc_id)
            return acc_id

    base = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:40] or "account"
    acc_id = base
    n = 2
    while acc_id in accounts:
        acc_id = f"{base}-{n}"
        n += 1

    accounts[acc_id] = {
        "name": name,
        "server": server.rstrip("/"),
        "username": username,
        "password": password,
        "added_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "current_slug": None,
        "busy_since": None,
        **probed,
    }
    _save()
    logger.info("account added: %s", acc_id)
    return acc_id


def remove_account(account_id: str) -> bool:
    accounts = _load()
    if account_id not in accounts:
        return False
    del accounts[account_id]
    _save()
    _channel_cache.pop(account_id, None)
    logger.info("account removed: %s", account_id)
    return True


def capacity() -> dict:
    accounts = _load()
    usable = [a for a in accounts.values() if is_usable(a)]
    busy = sum(1 for a in usable if a.get("current_slug"))
    return {
        "total": len(accounts),
        "usable": len(usable),
        "busy": busy,
        "free": len(usable) - busy,
    }


# ── claiming ──────────────────────────────────────────────────────────────────

async def claim(slug: str, preferred: str = "") -> tuple[str, Optional[dict]]:
    """
    Reserve an account for `slug`. Returns (account_id, account) or ("", None).

    Held under a lock: without it two concurrent starts can both read the same
    account as free and double-book it, which the provider answers by dropping
    one of the streams mid-playback.
    """
    async with _lock():
        accounts = _load()

        if preferred:
            acc = accounts.get(preferred)
            if acc is None:
                return "", None
            if acc.get("current_slug") not in (None, slug):
                return "", None
            if not is_usable(acc):
                return "", None
            acc["current_slug"] = slug
            acc["busy_since"] = int(time.time())
            _save()
            return preferred, acc

        for acc_id, acc in accounts.items():
            if acc.get("current_slug"):
                continue
            if not is_usable(acc):
                continue
            acc["current_slug"] = slug
            acc["busy_since"] = int(time.time())
            _save()
            logger.info("account %s claimed for %s", acc_id, slug)
            return acc_id, acc

        return "", None


async def release(account_id: str) -> None:
    async with _lock():
        acc = _load().get(account_id)
        if acc is None:
            return
        acc["current_slug"] = None
        acc["busy_since"] = None
        _save()
        logger.info("account %s released", account_id)


async def release_slug(slug: str) -> None:
    """Release whichever account is held for this channel."""
    async with _lock():
        changed = False
        for acc in _load().values():
            if acc.get("current_slug") == slug:
                acc["current_slug"] = None
                acc["busy_since"] = None
                changed = True
        if changed:
            _save()


def reset_claims() -> None:
    """
    Clear every claim. Called at startup before channels are restored: nothing is
    streaming yet, so any claim left in the file is stale from the last run and
    would otherwise permanently shrink the usable pool.
    """
    accounts = _load()
    stale = [a for a in accounts.values() if a.get("current_slug")]
    for acc in stale:
        acc["current_slug"] = None
        acc["busy_since"] = None
    if stale:
        _save()
        logger.info("cleared %d stale account claim(s) from previous run", len(stale))


# ── provider API ──────────────────────────────────────────────────────────────

def stream_url(acc: dict, stream_id: str) -> str:
    """
    The provider's HLS URL for one channel.

    Contains the account username and password, so this must never be sent to a
    client. The relay fetches it server-side and rewrites what clients see.
    """
    server = acc.get("server", "").rstrip("/")
    return f"{server}/live/{acc.get('username', '')}/{acc.get('password', '')}/{stream_id}.m3u8"


def _client() -> httpx.Client:
    return httpx.Client(
        verify=config.UPSTREAM_VERIFY_TLS,
        timeout=config.UPSTREAM_TIMEOUT,
        follow_redirects=True,
        headers=_BASE_HEADERS,
    )


def fetch_channels(account_id: str, search: str = "", force: bool = False) -> dict:
    """
    Channel list for an account, cached for CHANNEL_CACHE_TTL.

    Blocking httpx by design — callers run it in an executor. Search filters the
    cached list locally, so typing in the panel does not re-hit the provider.
    """
    acc = get_account(account_id)
    if acc is None:
        return {"error": "unknown account", "channels": [], "total": 0}

    cached = _channel_cache.get(account_id)
    fresh = cached and not force and (time.time() - cached[0]) < config.CHANNEL_CACHE_TTL

    if fresh:
        channels = cached[1]
    else:
        try:
            with _client() as client:
                resp = client.get(
                    f"{acc['server']}/player_api.php",
                    params={
                        "username": acc.get("username", ""),
                        "password": acc.get("password", ""),
                        "action": "get_live_streams",
                    },
                )
            if resp.status_code != 200:
                if cached:
                    channels = cached[1]
                else:
                    return {"error": f"provider returned HTTP {resp.status_code}", "channels": [], "total": 0}
            else:
                raw = resp.json()
                if not isinstance(raw, list):
                    if cached:
                        channels = cached[1]
                    else:
                        return {"error": "unexpected provider response", "channels": [], "total": 0}
                else:
                    channels = []
                    for ch in raw:
                        sid = str(ch.get("stream_id", "")).strip()
                        if not sid:
                            continue
                        channels.append({
                            "stream_id": sid,
                            "name": ch.get("name", "") or sid,
                            "category": ch.get("category_name", "") or "",
                            "logo": ch.get("stream_icon", "") or "",
                        })
                    _channel_cache[account_id] = (time.time(), channels)
        except Exception as exc:
            if cached:
                logger.warning("channel fetch failed for %s (%s) — serving cache", account_id, exc)
                channels = cached[1]
            else:
                return {"error": str(exc), "channels": [], "total": 0}

    total = len(channels)
    if search:
        needle = search.strip().lower()
        channels = [
            c for c in channels
            if needle in c["name"].lower() or needle in c["category"].lower() or needle == c["stream_id"]
        ]

    age = time.time() - _channel_cache.get(account_id, (0, []))[0]
    return {
        "channels": channels[:400],
        "total": total,
        "matched": len(channels),
        "cache_age": int(age) if age < 1e9 else None,
    }


def channel_name(account_id: str, stream_id: str) -> str:
    """Provider's label for one channel, from cache only (no network call)."""
    cached = _channel_cache.get(account_id)
    if not cached:
        return ""
    for ch in cached[1]:
        if ch["stream_id"] == stream_id:
            return ch["name"]
    return ""


def fetch_account_info(acc: dict) -> dict:
    """Status and expiry from the provider. Blocking — run in an executor."""
    if not (acc.get("server") and acc.get("username")):
        return {"error": "incomplete account"}
    try:
        with _client() as client:
            resp = client.get(
                f"{acc['server']}/player_api.php",
                params={"username": acc.get("username", ""), "password": acc.get("password", "")},
            )
        if resp.status_code != 200:
            return {"error": f"HTTP {resp.status_code}"}
        info = (resp.json() or {}).get("user_info") or {}
    except Exception as exc:
        # Transport-level failure: the provider may simply be down right now, which
        # is different from it rejecting the credentials.
        return {"error": str(exc), "unreachable": True}

    if not info:
        return {"error": "no user_info in response"}

    # An Xtream panel answers bad credentials with HTTP 200 and auth=0 rather
    # than a 401. Without this check the caller sees a successful response with
    # empty fields and treats the account as fine.
    if str(info.get("auth", "1")).strip() in ("0", "false", "False"):
        return {"error": "provider rejected these credentials", "auth_failed": True}

    try:
        exp = info.get("exp_date")
        exp_date = int(exp) if exp not in (None, "", "null") else None
    except (TypeError, ValueError):
        exp_date = None

    return {
        "status": str(info.get("status") or ""),
        "exp_date": exp_date,
        "max_connections": str(info.get("max_connections", "")),
        "active_cons": str(info.get("active_cons", "")),
    }


def refresh_account_info() -> int:
    """
    Update status/expiry for every account. Blocking — run in an executor.

    Goes through _fields_from_info so an auth rejection marks the account
    unusable here exactly as it does at add time, and an unreachable provider
    leaves the last known status intact.
    """
    updated = 0
    for acc_id, acc in list(_load().items()):
        info = fetch_account_info(acc)
        if info.get("auth_failed"):
            logger.warning("account %s: provider rejected the stored credentials", acc_id)
        elif info.get("error"):
            logger.warning("account info refresh failed for %s: %s", acc_id, info["error"])
        acc.update(_fields_from_info(info))
        updated += 1
    if updated:
        _save()
    return updated


async def refresh_loop() -> None:
    """Keep provider status fresh so the panel shows real expiry, not stale data."""
    loop = asyncio.get_running_loop()
    await asyncio.sleep(5)
    while True:
        try:
            if _load():
                count = await loop.run_in_executor(None, refresh_account_info)
                if count:
                    logger.info("refreshed provider info for %d account(s)", count)
        except Exception as exc:
            logger.warning("account refresh loop error: %s", exc)
        await asyncio.sleep(config.ACCOUNT_INFO_INTERVAL)
