"""
iptv_accounts.py — IPTV Account Manager

Stores IPTV accounts (Xtream Codes panel credentials) in MongoDB Atlas,
collection: leaf_iptv_accounts (separate from the main sportsaddon data).
Falls back to local JSON if MongoDB is unreachable.

Supports fetching live channel lists via the Xtream Codes player API.
Also supports raw M3U playlist URLs as an account type.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import urllib.parse
from typing import Optional

import httpx

logger = logging.getLogger("leaf.iptv_accounts")

# ── MongoDB setup ─────────────────────────────────────────────────────────────
_MONGO_URI        = os.getenv("MONGODB_URI", "")
_MONGO_COLLECTION = "leaf_iptv_accounts"
_mongo_db         = None
_mongo_col        = None

def _get_mongo_col():
    """Lazy-init MongoDB collection. Returns None if unavailable."""
    global _mongo_db, _mongo_col
    if _mongo_col is not None:
        return _mongo_col
    if not _MONGO_URI:
        return None
    try:
        from pymongo import MongoClient
        client     = MongoClient(_MONGO_URI, serverSelectionTimeoutMS=3000, socketTimeoutMS=5000, connectTimeoutMS=5000)
        _mongo_db  = client["stremio"]
        _mongo_col = _mongo_db[_MONGO_COLLECTION]
        logger.info("iptv_accounts: connected to MongoDB collection %s", _MONGO_COLLECTION)
        return _mongo_col
    except Exception as exc:
        logger.warning("iptv_accounts: MongoDB unavailable (%s), falling back to JSON", exc)
        return None


# ── Fallback: local JSON ──────────────────────────────────────────────────────
_ACCOUNTS_FILE = os.path.join("data", "iptv_accounts.json")

def _load_local() -> dict:
    os.makedirs("data", exist_ok=True)
    if not os.path.exists(_ACCOUNTS_FILE):
        return {}
    try:
        with open(_ACCOUNTS_FILE, "r") as f:
            return json.load(f)
    except Exception as exc:
        logger.warning("Failed to load local accounts: %s", exc)
        return {}

def _save_local(accounts: dict) -> None:
    os.makedirs("data", exist_ok=True)
    with open(_ACCOUNTS_FILE, "w") as f:
        json.dump(accounts, f, indent=2)


# ── In-memory channel cache ───────────────────────────────────────────────────
_channel_cache: dict[str, dict] = {}
_CHANNEL_REFRESH_INTERVAL = 4 * 3600  # refresh every 4 hours automatically
_CACHE_TTL = _CHANNEL_REFRESH_INTERVAL  # keep cache valid until next background refresh

# ── Account expiry / status ───────────────────────────────────────────────────
# Xtream player_api.php (no action) returns user_info.{status, exp_date, ...}.
# We fetch this periodically, persist it on each account, block claiming of
# expired/disabled accounts, and surface it in the admin UI.
_ACCOUNT_INFO_REFRESH_INTERVAL = 30 * 60   # re-check every 30 min
_ACCOUNT_EXPIRY_WARN_SECS       = 72 * 3600  # ⚠️ warn when <72h to expiry
# Statuses the provider reports for an account that must NOT be used.
_DEAD_ACCOUNT_STATUSES = {"Expired", "Disabled", "Banned"}


def _is_account_usable(acc: dict) -> bool:
    """False if the provider has expired/disabled/banned this account, or its
    exp_date has passed. Accounts with no status info yet are treated as usable
    (fail-open) so a probe failure never takes the whole pool offline."""
    # Reject shell accounts — a deleted account can be resurrected as a partial
    # doc (status/exp_date only, no credentials) by a stale targeted $set. Such a
    # shell has no server/username/password and can never stream, so it must never
    # be claimed or probed. m3u accounts use `server` as the URL (no username).
    if not acc.get("server"):
        return False
    if acc.get("type", "xtream") == "xtream" and (not acc.get("username") or not acc.get("password")):
        return False
    status = (acc.get("status") or "").strip()
    if status in _DEAD_ACCOUNT_STATUSES:
        return False
    exp = acc.get("exp_date")
    try:
        if exp and float(exp) > 0 and float(exp) < time.time():
            return False
    except (TypeError, ValueError):
        pass
    return True

_BASE_HEADERS = {
    "user-agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
    "accept":          "*/*",
    "accept-language": "en-US,en;q=0.9",
}


# ══════════════════════════════════════════════════════════════════════════════
# Account storage — MongoDB primary, JSON fallback
# ══════════════════════════════════════════════════════════════════════════════

_accounts_cache: dict = {}
_accounts_cache_at: float = 0.0
_ACCOUNTS_CACHE_TTL = 5  # seconds — short read-through cache for display / sync callers.
                         # NOT an authority: MongoDB is the single source of truth.
                         # The hot claim path (async_claim_free_account) always reads
                         # fresh from Mongo under a lock, so a stale entry here can
                         # never cause a double-booking.


def _apply_cache_slug(account_id: str, slug) -> None:
    """Mirror a slug change into the read-through cache so display / sync callers
    reflect it immediately without waiting for the next refresh. Convenience only —
    MongoDB remains the source of truth."""
    if account_id in _accounts_cache:
        _accounts_cache[account_id]["current_slug"] = slug
        _accounts_cache[account_id]["busy_since"]   = int(time.time()) if slug else None


def _load_accounts() -> dict:
    """Synchronous read for display / non-hot-path callers. 5s read-through cache;
    on miss it does a blocking pymongo read. Hot paths (tick, reconcile, claim,
    rotate) must use async_load_accounts() so they never block the event loop."""
    global _accounts_cache, _accounts_cache_at
    now = time.time()
    if _accounts_cache and (now - _accounts_cache_at) < _ACCOUNTS_CACHE_TTL:
        return _accounts_cache
    col = _get_mongo_col()
    if col is not None:
        try:
            doc = col.find_one({"_id": "accounts"})
            if doc and "accounts" in doc:
                accounts = doc["accounts"]
                try:
                    _save_local(accounts)
                except Exception:
                    pass
                _accounts_cache = accounts
                _accounts_cache_at = now
                return accounts
            return {}
        except Exception as exc:
            logger.warning("iptv_accounts: MongoDB read failed (%s), using local JSON", exc)
    accounts = _load_local()
    _accounts_cache = accounts
    _accounts_cache_at = now
    return accounts



def _save_accounts(accounts: dict) -> None:
    """Full-document write — used by add/remove account only.
    For busy/free changes use mark_account_busy / mark_account_free (targeted $set)."""
    global _accounts_cache, _accounts_cache_at
    _accounts_cache = accounts
    _accounts_cache_at = time.time()
    _save_local(accounts)

    col = _get_mongo_col()
    if col is not None:
        try:
            col.update_one(
                {"_id": "accounts"},
                {"$set": {"accounts": accounts, "updated_at": int(time.time())}},
                upsert=True,
            )
        except Exception as exc:
            logger.warning("iptv_accounts: MongoDB write failed (%s)", exc)


def list_accounts() -> list[dict]:
    accounts = _load_accounts()
    # Never expose raw password in listings
    return [
        {
            "id":           aid,
            "name":         a.get("name", aid),
            "type":         a.get("type", "xtream"),
            "server":       a.get("server", ""),
            "username":     a.get("username", ""),
            "has_password": bool(a.get("password")),
            "added_at":     a.get("added_at", ""),
            # Account status/expiry (from the Xtream player_api refresh loop).
            "status":       a.get("status", ""),
            "exp_date":     a.get("exp_date"),
            "usable":       _is_account_usable(a),
            "info_checked_at": a.get("info_checked_at"),
        }
        for aid, a in accounts.items()
    ]


def get_account(account_id: str) -> Optional[dict]:
    return _load_accounts().get(account_id)


def add_account(
    name: str,
    account_type: str,   # "xtream" or "m3u"
    server: str,         # for xtream: base URL (http://host:port), for m3u: full playlist URL
    username: str = "",
    password: str = "",
) -> str:
    """Add or update an account. Returns the generated account_id."""
    accounts = _load_accounts()
    # Use a slug of the name as ID
    base_id    = name.lower().replace(" ", "_").replace("/", "_")[:32]
    account_id = base_id
    i = 2
    while account_id in accounts and accounts[account_id].get("server") != server:
        account_id = f"{base_id}_{i}"
        i += 1

    accounts[account_id] = {
        "name":         name,
        "type":         account_type,
        "server":       server.rstrip("/"),
        "username":     username,
        "password":     password,
        "added_at":     time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "current_slug": None,   # set by iptv_scheduler when recording
        "busy_since":   None,   # timestamp when it became busy
    }
    _save_accounts(accounts)
    logger.info("Account saved: %s (%s)", account_id, account_type)
    return account_id


def remove_account(account_id: str) -> bool:
    accounts = _load_accounts()
    if account_id not in accounts:
        return False
    del accounts[account_id]
    _save_accounts(accounts)
    _channel_cache.pop(account_id, None)
    logger.info("Account removed: %s", account_id)
    return True


# ── Scheduler helpers ─────────────────────────────────────────────────────────

def get_account_capacity() -> dict:
    """Return capacity stats for all accounts."""
    accounts = _load_accounts()
    total = len(accounts)
    busy  = sum(1 for a in accounts.values() if a.get("current_slug"))
    return {"total": total, "busy": busy, "free": total - busy}


def mark_account_busy(account_id: str, slug: str) -> bool:
    """Mark an account busy via a targeted MongoDB $set. Only mirrors to cache on success."""
    col = _get_mongo_col()
    if col is not None:
        try:
            # Guard on the account key existing — a targeted $set on a dotted path
            # would RECREATE a deleted account as an empty shell. The $exists filter
            # makes the write a no-op if the account was removed.
            res = col.update_one(
                {"_id": "accounts", f"accounts.{account_id}": {"$exists": True}},
                {"$set": {
                    f"accounts.{account_id}.current_slug": slug,
                    f"accounts.{account_id}.busy_since":   int(time.time()),
                }},
            )
            if res.matched_count == 0:
                logger.warning("mark_account_busy: account %s no longer exists — skipping (was deleted)", account_id)
                return False
            _apply_cache_slug(account_id, slug)
        except Exception as exc:
            logger.warning("mark_account_busy: MongoDB write failed (%s) — cache NOT updated", exc)
            return False
    else:
        _apply_cache_slug(account_id, slug)
    return True


def mark_account_free(account_id: str) -> bool:
    """Release an account via a targeted MongoDB $set. Only mirrors to cache on success."""
    col = _get_mongo_col()
    if col is not None:
        try:
            # $exists guard — never recreate a deleted account as an empty shell.
            res = col.update_one(
                {"_id": "accounts", f"accounts.{account_id}": {"$exists": True}},
                {"$set": {
                    f"accounts.{account_id}.current_slug": None,
                    f"accounts.{account_id}.busy_since":   None,
                }},
            )
            if res.matched_count == 0:
                logger.warning("mark_account_free: account %s no longer exists — skipping (was deleted)", account_id)
                return False
            _apply_cache_slug(account_id, None)
        except Exception as exc:
            logger.warning("mark_account_free: MongoDB write failed (%s) — cache NOT updated", exc)
            return False
    else:
        _apply_cache_slug(account_id, None)
    return True


def get_free_account() -> Optional[tuple]:
    """Return (account_id, account_dict) for first free AND usable account, or None."""
    accounts = _load_accounts()
    for acc_id, acc in accounts.items():
        if not acc.get("current_slug") and _is_account_usable(acc):
            return acc_id, acc
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Async motor API — non-blocking MongoDB for hot paths
# ══════════════════════════════════════════════════════════════════════════════
#
# Why motor instead of pymongo + run_in_executor:
#   - pymongo in run_in_executor still blocks one thread; under load many concurrent
#     writes exhaust the thread-pool and queue behind each other.
#   - motor is fully async — each await yields control to the event loop, so 40
#     concurrent writes don't starve each other or the playlist proxy.
#   - No in-memory bridge (hard-lock dict) is needed: every claim is an atomic
#     MongoDB write that all other async code sees on the next await.
#   - An asyncio.Lock gates the find-then-claim sequence so two concurrent ticks
#     can never double-book the same account.

_motor_client = None
_motor_col    = None
# Lock that serialises the "find free account → write claim" window.
# asyncio.Lock is process-local but that's fine: one master process owns all claims.
_claim_lock: asyncio.Lock | None = None

# Set of account_ids that reconcile has confirmed are ACTUALLY streaming on a
# worker right now (ffmpeg running=true per /internal/status). Published by the
# scheduler's reconcile tick via publish_live_accounts() and read by the claim
# guard as a second line of defence: even if MongoDB's current_slug drifts to
# None (probe cycling, restart lag), we won't hand a genuinely-live account to a
# new slug and open a 2nd connection → HTTP 458.
#
# IMPORTANT: this must be the RUNNING set, not the raw leaf_workers.assigned_streams
# map — that map is the persistent restart-recovery record and keeps stale/dead
# entries, which would block free accounts forever ("account full" while the panel
# shows them free). Reconcile already filters to running=true, so we mirror that.
_live_account_ids: set[str] = set()


def publish_live_accounts(account_ids: set[str]) -> None:
    """Called by reconcile each tick with the set of account_ids confirmed to be
    actively streaming on a worker (running=true). The claim guard reads this."""
    global _live_account_ids
    _live_account_ids = set(account_ids or ())


def _get_motor_col():
    """Lazy-init the motor (async) collection. Safe to call from any async context."""
    global _motor_client, _motor_col
    if _motor_col is not None:
        return _motor_col
    if not _MONGO_URI:
        return None
    try:
        from motor.motor_asyncio import AsyncIOMotorClient
        _motor_client = AsyncIOMotorClient(
            _MONGO_URI,
            serverSelectionTimeoutMS=3000,
            socketTimeoutMS=5000,
            connectTimeoutMS=5000,
        )
        _motor_col = _motor_client["stremio"][_MONGO_COLLECTION]
        logger.info("iptv_accounts: motor (async) client initialised")
        return _motor_col
    except Exception as exc:
        logger.warning("iptv_accounts: motor init failed (%s)", exc)
        return None


def _get_claim_lock() -> asyncio.Lock:
    global _claim_lock
    if _claim_lock is None:
        _claim_lock = asyncio.Lock()
    return _claim_lock


async def async_load_accounts() -> dict:
    """Non-blocking read via motor. Always fetches fresh from MongoDB (no TTL cache).
    Use this in the scheduler tick, reconcile, and rotation so those never block
    the event loop on a slow MongoDB connection."""
    global _accounts_cache, _accounts_cache_at
    col = _get_motor_col()
    if col is not None:
        try:
            doc = await col.find_one({"_id": "accounts"})
            if doc and "accounts" in doc:
                accounts = doc["accounts"]
                # Keep the sync cache warm so display/channel-name callers
                # don't see stale state between their own 5s refreshes.
                _accounts_cache    = accounts
                _accounts_cache_at = time.time()
                return accounts
        except Exception as exc:
            logger.warning("async_load_accounts: motor read failed (%s), using cache", exc)
    # Fallback: return the last known cache (no pymongo — never block the event loop).
    # If cache is empty, load from local JSON file only.
    if _accounts_cache:
        return _accounts_cache
    return _load_local()


async def async_mark_busy(account_id: str, slug: str) -> None:
    """Non-blocking targeted $set to mark an account busy. Mirrors to cache ONLY on success."""
    col = _get_motor_col()
    if col is not None:
        try:
            # $exists guard — never recreate a deleted account as an empty shell.
            res = await col.update_one(
                {"_id": "accounts", f"accounts.{account_id}": {"$exists": True}},
                {"$set": {
                    f"accounts.{account_id}.current_slug": slug,
                    f"accounts.{account_id}.busy_since":   int(time.time()),
                }},
            )
            if res.matched_count == 0:
                logger.warning("async_mark_busy: account %s no longer exists — skipping (was deleted)", account_id)
                return
            _apply_cache_slug(account_id, slug)  # only on success — mirrors async_mark_free
        except Exception as exc:
            logger.warning(
                "async_mark_busy %s: %s — cache NOT updated to prevent split-brain",
                account_id, exc,
            )
            return  # do NOT mark busy in cache — keep consistent with MongoDB state
    else:
        _apply_cache_slug(account_id, slug)


async def async_mark_free(account_id: str) -> None:
    """Non-blocking targeted $set to release an account. Mirrors to cache."""
    col = _get_motor_col()
    if col is not None:
        try:
            # $exists guard — never recreate a deleted account as an empty shell.
            res = await col.update_one(
                {"_id": "accounts", f"accounts.{account_id}": {"$exists": True}},
                {"$set": {
                    f"accounts.{account_id}.current_slug": None,
                    f"accounts.{account_id}.busy_since":   None,
                }},
            )
            if res.matched_count == 0:
                logger.warning("async_mark_free: account %s no longer exists — skipping (was deleted)", account_id)
                return
            _apply_cache_slug(account_id, None)  # only update cache on success
        except Exception as exc:
            logger.warning("async_mark_free %s: %s — cache NOT updated to prevent split-brain", account_id, exc)
            return  # do NOT mark free in cache — keep consistent with MongoDB state
    else:
        _apply_cache_slug(account_id, None)


async def async_claim_specific_account(account_id: str, slug: str) -> bool:
    """Claim a SPECIFIC account for slug under the claim lock, but only if it's
    free or already held by this same slug. Used by restore paths that re-attach
    a recording to the account it used before a restart. Returns True on success,
    False if the account is now busy with a different slug (caller must re-pick)."""
    async with _get_claim_lock():
        accounts = await async_load_accounts()
        acc = accounts.get(account_id)
        if acc is None:
            return False
        cur = acc.get("current_slug")
        if cur and cur != slug:
            return False  # taken by someone else — don't steal it
        # Don't re-attach to an expired/disabled account on restore — force the
        # caller to re-pick a fresh one via async_claim_free_account.
        if not _is_account_usable(acc):
            return False
        await async_mark_busy(account_id, slug)
        return True


async def async_claim_free_account(
    slug: str, exclude: set[str] | None = None,
) -> tuple[str, dict] | tuple[None, None]:
    """Atomically find and claim a free xtream account for slug.

    Holds _claim_lock for the entire find→write window so two concurrent
    callers (tick + probe, or two rapid ticks) can never pick the same account.
    Returns (account_id, account_dict) on success, (None, None) if all busy.
    The caller must call async_mark_free(account_id) on any failure path after
    this returns, or the account stays locked with nothing recording.

    `exclude` is a set of account_ids to skip on this pass. Rotation passes the
    accounts it has already tried for this slug so a flaky/dead channel spreads
    its attempts across DISTINCT accounts instead of ping-ponging between the
    same two (which wastes the rotation budget and produces log noise). If every
    non-excluded account is busy, the excluded ones are tried as a last resort so
    a genuinely free account is never refused just because it was tried before.
    """
    exclude = exclude or set()
    async with _get_claim_lock():
        # Always read fresh from MongoDB — never from the cache — so a claim
        # written by a previous lock holder is immediately visible.
        accounts = await async_load_accounts()

        # Second line of defence against handing out a genuinely-live account:
        # skip any account reconcile has confirmed is ACTUALLY streaming (ffmpeg
        # running=true) right now, even if MongoDB's current_slug drifted to None.
        # We read the reconcile-published running set — NOT raw assigned_streams,
        # which keeps stale/dead entries and would block free accounts forever.
        _live_on_workers = _live_account_ids

        fallback: tuple[str, dict] | None = None
        for acc_id, acc in accounts.items():
            if acc.get("current_slug"):
                continue
            if acc.get("type", "xtream") != "xtream":
                continue
            # Never claim an expired/disabled/banned account. Skipped entirely so
            # it's not even eligible as the excluded-fallback below.
            if not _is_account_usable(acc):
                continue
            # Even if MongoDB says free, skip accounts a worker is actively using.
            # Reconcile may not have re-marked them yet (probe-cycling, restart lag, etc.)
            if acc_id in _live_on_workers:
                logger.warning(
                    "iptv_accounts: skip %s for '%s' — leaf_workers shows it active on a worker despite MongoDB free (drift guard)",
                    acc_id, slug,
                )
                continue
            if acc_id in exclude:
                # Remember the first free-but-excluded account; only use it if no
                # fresh account exists, so we never refuse a genuinely free slot.
                if fallback is None:
                    fallback = (acc_id, acc)
                continue
            # Claim atomically inside the lock
            await async_mark_busy(acc_id, slug)
            logger.info("iptv_accounts: claimed %s for slug '%s'", acc_id, slug)
            return acc_id, acc
        if fallback is not None:
            acc_id, acc = fallback
            await async_mark_busy(acc_id, slug)
            logger.info("iptv_accounts: claimed %s for slug '%s' (excluded fallback — no fresh account)", acc_id, slug)
            return acc_id, acc
    logger.warning("iptv_accounts: all accounts busy, cannot claim for '%s'", slug)
    return None, None


# ══════════════════════════════════════════════════════════════════════════════
# Channel fetching
# ══════════════════════════════════════════════════════════════════════════════

def _build_stream_url(account: dict, stream_id: str) -> str:
    """Build the HLS stream URL for a given stream_id."""
    if account.get("type") == "xtream":
        server = account["server"]
        user   = account["username"]
        pwd    = account["password"]
        return f"{server}/live/{user}/{pwd}/{stream_id}.m3u8"
    return ""


def _fetch_xtream_channels(account: dict, search: str = "", page: int = 1, per_page: int = 100) -> dict:
    """Fetch live channels via Xtream Codes player_api."""
    server   = account["server"]
    username = account["username"]
    password = account["password"]

    api_url = f"{server}/player_api.php"
    params  = {"username": username, "password": password, "action": "get_live_streams"}

    try:
        with httpx.Client(verify=False, timeout=15, follow_redirects=True) as client:
            resp = client.get(api_url, params=params, headers=_BASE_HEADERS)
        if resp.status_code != 200:
            return {"error": f"API returned HTTP {resp.status_code}", "channels": [], "total": 0}
        data = resp.json()
        if not isinstance(data, list):
            return {"error": "Unexpected API response format", "channels": [], "total": 0}
    except Exception as exc:
        return {"error": str(exc), "channels": [], "total": 0}

    channels = []
    for ch in data:
        name      = ch.get("name", "")
        cat       = ch.get("category_name", "")
        stream_id = str(ch.get("stream_id", ""))
        if not stream_id:
            continue
        channels.append({
            "stream_id":  stream_id,
            "name":       name,
            "category":   cat,
            "stream_url": _build_stream_url(account, stream_id),
            "epg_id":     ch.get("epg_channel_id", ""),
            "logo":       ch.get("stream_icon", ""),
        })

    return {"channels": channels, "total": len(channels)}


def fetch_account_info(account: dict) -> dict:
    """Fetch Xtream account status/expiry via player_api.php (no action param).
    Returns {status, exp_date, is_trial, max_connections, active_cons} or
    {"error": ...}. Blocking httpx — call from a thread/executor."""
    if account.get("type", "xtream") != "xtream":
        return {"error": "not xtream"}
    server   = account.get("server", "")
    username = account.get("username", "")
    password = account.get("password", "")
    if not (server and username and password):
        return {"error": "missing credentials"}
    api_url = f"{server}/player_api.php"
    params  = {"username": username, "password": password}
    try:
        with httpx.Client(verify=False, timeout=15, follow_redirects=True) as client:
            resp = client.get(api_url, params=params, headers=_BASE_HEADERS)
        if resp.status_code != 200:
            return {"error": f"HTTP {resp.status_code}"}
        data = resp.json()
    except Exception as exc:
        return {"error": str(exc)}
    ui = (data or {}).get("user_info") or {}
    if not ui:
        return {"error": "no user_info"}
    # exp_date may be a string or int (unix seconds); normalise to int or None.
    exp_raw = ui.get("exp_date")
    try:
        exp_date = int(exp_raw) if exp_raw not in (None, "", "null") else None
    except (TypeError, ValueError):
        exp_date = None
    return {
        "status":          ui.get("status") or "",
        "exp_date":        exp_date,
        "is_trial":        str(ui.get("is_trial", "")),
        "max_connections": str(ui.get("max_connections", "")),
        "active_cons":     str(ui.get("active_cons", "")),
    }


def refresh_all_account_info() -> None:
    """Blocking sweep: fetch fresh status/expiry for every xtream account and
    persist it via a targeted $set (never touching current_slug/busy_since).
    exp_date is per-username so we do NOT dedupe by server."""
    accounts = _load_accounts()
    if not accounts:
        return
    col = _get_mongo_col()
    checked = 0
    for acc_id, acc in accounts.items():
        if acc.get("type", "xtream") != "xtream":
            continue
        info = fetch_account_info(acc)
        if "error" in info:
            logger.warning("account_info_refresh: %s — %s", acc_id, info["error"])
            continue
        fields = {
            f"accounts.{acc_id}.status":          info["status"],
            f"accounts.{acc_id}.exp_date":        info["exp_date"],
            f"accounts.{acc_id}.is_trial":        info["is_trial"],
            f"accounts.{acc_id}.max_connections": info["max_connections"],
            f"accounts.{acc_id}.info_checked_at": int(time.time()),
        }
        # Update in-memory cache too so readers see it before the next load.
        acc.update({
            "status":          info["status"],
            "exp_date":        info["exp_date"],
            "is_trial":        info["is_trial"],
            "max_connections": info["max_connections"],
            "info_checked_at": int(time.time()),
        })
        if col is not None:
            try:
                # $exists guard — a targeted $set must never recreate a deleted account.
                col.update_one(
                    {"_id": "accounts", f"accounts.{acc_id}": {"$exists": True}},
                    {"$set": fields},
                )
            except Exception as exc:
                logger.warning("account_info_refresh: %s persist failed — %s", acc_id, exc)
        checked += 1
        if not _is_account_usable(acc):
            logger.warning("account_info_refresh: %s is UNUSABLE (status=%s exp_date=%s)",
                           acc_id, info["status"], info["exp_date"])
            # If this expired account is currently streaming, free it immediately so
            # the reconcile loop can reassign the slug to a fresh account. Without this,
            # the stream keeps looping rc=0 on the dead credentials indefinitely because
            # rc=0 never triggers rotation in m3u_recorder.
            cur_slug = acc.get("current_slug")
            if cur_slug and not str(cur_slug).startswith("__probe__"):
                logger.warning("account_info_refresh: force-freeing %s from expired account %s — will rotate to fresh account",
                               cur_slug, acc_id)
                try:
                    mark_account_free(acc_id)
                except Exception as _fe:
                    logger.warning("account_info_refresh: could not free %s: %s", acc_id, _fe)
    # Mirror to local JSON fallback so state survives a Mongo outage.
    try:
        _save_accounts(accounts)
    except Exception:
        pass
    logger.info("account_info_refresh: checked %d xtream account(s)", checked)


async def run_account_info_refresh_loop() -> None:
    """Async background task: refresh account status/expiry every
    _ACCOUNT_INFO_REFRESH_INTERVAL seconds. Immediate warm-up, then repeat.
    Register once from leaf_main.py."""
    import concurrent.futures
    loop = asyncio.get_running_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="acct-info")
    logger.info("account_info_refresh: background loop started (interval=%dm)",
                _ACCOUNT_INFO_REFRESH_INTERVAL // 60)
    while True:
        try:
            await loop.run_in_executor(executor, refresh_all_account_info)
        except Exception as exc:
            logger.exception("account_info_refresh: unexpected error: %s", exc)
        await asyncio.sleep(_ACCOUNT_INFO_REFRESH_INTERVAL)


def _fetch_m3u_channels(account: dict) -> dict:
    """Fetch channels from a raw M3U playlist URL."""
    url = account["server"]
    try:
        with httpx.Client(verify=False, timeout=20, follow_redirects=True) as client:
            resp = client.get(url, headers=_BASE_HEADERS)
        if resp.status_code != 200:
            return {"error": f"HTTP {resp.status_code}", "channels": [], "total": 0}
        text = resp.text
    except Exception as exc:
        return {"error": str(exc), "channels": [], "total": 0}

    channels = []
    cur_name  = ""
    cur_logo  = ""
    cur_group = ""
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF:"):
            cur_name  = line.split(",", 1)[-1].strip() if "," in line else ""
            cur_logo  = ""
            cur_group = ""
            if 'tvg-logo="' in line:
                cur_logo = line.split('tvg-logo="', 1)[1].split('"')[0]
            if 'group-title="' in line:
                cur_group = line.split('group-title="', 1)[1].split('"')[0]
        elif line.startswith("http") and cur_name:
            parsed    = urllib.parse.urlparse(line)
            seg       = parsed.path.strip("/").split("/")
            stream_id = seg[-1].replace(".m3u8", "").replace(".ts", "") if seg else cur_name
            channels.append({
                "stream_id":  stream_id,
                "name":       cur_name,
                "category":   cur_group,
                "stream_url": line,
                "epg_id":     "",
                "logo":       cur_logo,
            })
            cur_name = ""

    return {"channels": channels, "total": len(channels)}


def fetch_channels(account_id: str, search: str = "", force_refresh: bool = False) -> dict:
    account = get_account(account_id)
    if not account:
        return {"error": "Account not found", "channels": [], "total": 0}

    # Xtream accounts on the same server have identical channel lists — use server as cache key
    is_m3u = account.get("type") == "m3u"
    cache_key = account_id if is_m3u else account["server"]

    now = time.time()
    cached = _channel_cache.get(cache_key)
    if not force_refresh and cached and (now - cached["fetched_at"]) < _CACHE_TTL:
        all_channels = cached["channels"]
    else:
        result = _fetch_m3u_channels(account) if is_m3u else _fetch_xtream_channels(account)
        if "error" in result and not result.get("channels"):
            return result
        all_channels = result.get("channels", [])
        _channel_cache[cache_key] = {"channels": all_channels, "fetched_at": now}
        logger.info("Fetched %d channels (key=%s)", len(all_channels), cache_key)

    # Apply search filter
    if search:
        q = search.lower()
        all_channels = [
            c for c in all_channels
            if q in (c.get("name") or "").lower()
            or q in (c.get("category") or "").lower()
            or q in (c.get("epg_id") or "").lower()
        ]

    return {"channels": all_channels, "total": len(all_channels)}


def get_stream_url(account_id: str, stream_id: str) -> Optional[str]:
    """Get the direct HLS URL for a stream_id from an account."""
    account = get_account(account_id)
    if not account:
        return None

    # For xtream accounts, channels are cached by server (not account_id)
    is_m3u = account.get("type") == "m3u"
    cache_key = account_id if is_m3u else account.get("server", account_id)
    cached = _channel_cache.get(cache_key, {}).get("channels", [])
    for ch in cached:
        if ch["stream_id"] == stream_id:
            return ch["stream_url"]

    if account.get("type") == "xtream":
        return _build_stream_url(account, stream_id)

    return None


def get_channel_name(account_id: str, stream_id: str) -> Optional[str]:
    """Get the original channel name (e.g. FOXTV 4k) for a stream_id from an account.
    Falls back to fetching the channel list live if the cache is empty.
    """
    cached = _channel_cache.get(account_id, {}).get("channels", [])
    for ch in cached:
        if ch["stream_id"] == stream_id:
            return ch.get("name")

    # Cache miss — fetch channels now so we can look it up
    account = get_account(account_id)
    if not account:
        return None
    try:
        result = fetch_channels(account_id, force_refresh=False)
        for ch in result.get("channels", []):
            if ch["stream_id"] == stream_id:
                return ch.get("name")
    except Exception as exc:
        logger.warning("get_channel_name: fetch failed for %s/%s: %s", account_id, stream_id, exc)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Background channel cache refresh loop
# ══════════════════════════════════════════════════════════════════════════════

def refresh_all_channels_cache() -> None:
    accounts = _load_accounts()
    if not accounts:
        return
    # Group by server — same server = same channel list, fetch once
    seen_servers: set = set()
    for acc_id, acc in accounts.items():
        is_m3u = acc.get("type") == "m3u"
        key = acc_id if is_m3u else acc.get("server", "")
        if key in seen_servers:
            continue
        seen_servers.add(key)
        try:
            result = fetch_channels(acc_id, force_refresh=True)
            if "error" in result and not result.get("channels"):
                logger.warning("channel_cache_refresh: %s — %s", acc_id, result["error"])
            else:
                logger.info("channel_cache_refresh: %s — cached %d channels", key, result.get("total", 0))
        except Exception as exc:
            logger.warning("channel_cache_refresh: %s — %s", acc_id, exc)
    logger.info("channel_cache_refresh: done (%d unique servers)", len(seen_servers))


async def run_channel_cache_refresh_loop() -> None:
    """Async background task: refreshes channel lists every _CHANNEL_REFRESH_INTERVAL seconds.
    Does an immediate warm-up on first run, then repeats every 4 hours.
    Call this once from leaf_main.py: asyncio.create_task(run_channel_cache_refresh_loop())
    """
    import concurrent.futures
    loop = asyncio.get_running_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="chan-refresh")

    logger.info("channel_cache_refresh: background loop started (interval=%dh)",
                _CHANNEL_REFRESH_INTERVAL // 3600)

    while True:
        try:
            # Run the blocking fetch in a thread so we don't block the event loop
            await loop.run_in_executor(executor, refresh_all_channels_cache)
        except Exception as exc:
            logger.exception("channel_cache_refresh: unexpected error: %s", exc)
        await asyncio.sleep(_CHANNEL_REFRESH_INTERVAL)
