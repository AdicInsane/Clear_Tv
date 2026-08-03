"""
iptv_scheduler.py — Smart IPTV auto-scheduler for the Leaf Bridge.

Runs a background loop every 60 seconds:
  1. Fetches live + upcoming events from the sportsaddon backend
     (sports_live + sports_today catalogs, token-authenticated)
  2. For each live event (or event starting within PRE_START_BUFFER):
       - Looks up assigned IPTV channel slugs from MongoDB (leaf_event_channels)
       - For each assigned slug not already recording:
           * Finds a free IPTV account (1 account = 1 connection)
           * Starts the M3U recording with full event metadata
  3. For events no longer in the live feed: stops recordings + frees accounts
  4. Hard cap of 4.5h per recording as safety net
  5. If an event is already live and you assign a channel via the admin panel,
     it will be picked up within 60 seconds (next tick) automatically

Backend event fields (from streamed.pk via sportsaddon):
  id       — match ID (e.g. "streamed:1234567" → Stremio meta ID)
  title    — "Bayern Munich vs Arsenal"
  category — "football"
  date     — Unix timestamp in MILLISECONDS (0 or missing = 24/7 channel)
  teams    — {home: {name, badge}, away: {name, badge}}
  popular  — bool
  sources  — [{source, id}]

The catalog response shape is: {"metas": [{...meta_item...}]}
Meta item id prefix may be "streamed:", "leaf:", or "sf:"
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

import httpx

logger = logging.getLogger("leaf.iptv_scheduler")

_LEAF_MODE = os.getenv("LEAF_MODE", "standalone")

# ── Config ────────────────────────────────────────────────────────────────────

PRE_START_BUFFER_SECS   = 30 * 60       # start 30 min before event goes live
MAX_EVENT_DURATION_SECS = 24 * 3600  # 24 hr hard safety cap — user removes events manually
# Default auto-end window used when an event has NO per-event/team end-offset
# configured. Before this existed, such events got auto_end_at=None and their
# ends_at fell back to time.time()+MAX_EVENT_DURATION_SECS — RECOMPUTED on every
# restart, so the clock reset each time and the event effectively never ended.
# Now every scheduler event gets an ABSOLUTE, PINNED auto-end (see
# _resolve_pinned_auto_end), so a restart reuses the exact same timestamp.
DEFAULT_END_OFFSET_HOURS = float(os.getenv("DEFAULT_END_OFFSET_HOURS", "24"))

_BACKEND_URL    = os.getenv("ADDON_API_BASE",   "https://premium.highfly.dev")
_SCHED_TOKEN    = os.getenv("SCHEDULER_TOKEN",  "")  # ⚠️ expires ~June 2027

# Catalog endpoints that give us live + today events (used by scheduler)
_LIVE_CATALOG_URL  = f"{_BACKEND_URL}/{_SCHED_TOKEN}/catalog/sports/sports_live.json"
_TODAY_CATALOG_URL = f"{_BACKEND_URL}/{_SCHED_TOKEN}/catalog/sports/sports_today.json"

# All sport-category catalog IDs available in the backend manifest
_ALL_SPORT_CATALOGS = [
    "sports_live",
    "sports_today",
    "sports_basketball",
    "sports_football",
    "sports_american_football",
    "sports_hockey",
    "sports_baseball",
    "sports_motor_sports",
    "sports_fight",
    "sports_tennis",
    "sports_rugby",
    "sports_golf",
    "sports_billiards",
    "sports_afl",
    "sports_darts",
    "sports_cricket",
    "sports_other",
]

# ── State ─────────────────────────────────────────────────────────────────────

# slug → {event_id, account_id, started_at, shared_events}  — scheduler-managed recordings only
# shared_events: set of event_ids that also need this channel (for duplicate stream handling)
_scheduler_recordings: dict[str, dict] = {}

# Capacity warnings to show in admin panel
_capacity_warnings: list[str] = []

# ── Stop-grace: consecutive miss counts per recording ───────────────────────
# Prevents a recording from being killed the moment a match momentarily
# disappears from the live API feed (transient glitch, streamed.pk lag).
# The recording is only stopped after STOP_GRACE_TICKS consecutive misses.
_stop_miss_counts: dict[str, int] = {}   # ch_slug → consecutive miss count
STOP_GRACE_TICKS  = 15                   # ~15 minutes at 60s tick interval
                                         # (raised from 5: streamed.pk drops LIVE events
                                         #  from its feed for several minutes mid-match;
                                         #  a genuine end still stops via auto_end_at /
                                         #  the 4.5h hard cap, so this fallback can be lax)

# ── Orphan-grace: streams running on a worker but missing from all tracking ──
# A master restart starts with empty in-memory state (_scheduler_recordings,
# _recording_meta), so on the first reconcile passes every live scheduler
# stream looks "orphaned".  Require _ORPHAN_STOP_TICKS consecutive reconcile
# passes seeing it as orphaned before stopping — this gives _tick() time to
# re-register scheduler streams and prevents mass-stopping live streams on
# a restart.
_orphan_miss_counts: dict[str, int] = {}  # slug → consecutive orphan-seen count
_ORPHAN_STOP_TICKS = 3                     # ~3 minutes at 60s reconcile interval

# ── Ghost-free grace: an account whose current_slug is set but whose slug is not
# seen running on ANY worker this pass. A SINGLE stalled/slow /internal/status
# (all-worker status stalls happen — 11 times in one day observed) makes a LIVE
# account look idle for one pass; freeing it immediately then lets the very next
# claim hand the still-live account to a second stream → provider 458 → both
# storm-rotate → account-reuse cascade. Mirror the orphan-grace design: only free
# after the account's slug is seen absent for _GHOST_FREE_TICKS CONSECUTIVE
# passes, so one bad poll can never free a live account. This is derived purely
# from live worker observation persisting over time — NOT from trusting MongoDB.
_ghost_miss_counts: dict[str, int] = {}    # account_id → consecutive ghost-seen count
_GHOST_FREE_TICKS = 3                      # ~3 min at 60s reconcile interval

# ── Admin force controls ─────────────────────────────────────────────────────
# Force-deleted: scheduler will never (re)start these event IDs
_force_deleted: set[str] = set()          # event_id strings
# Force-started: scheduler treats these event IDs as always-live until expiry
_force_started: dict[str, float] = {}    # event_id → monotonic expiry time
FORCE_START_DURATION_SECS = 15 * 60      # 15-minute pin window

# ── Admin events cache ────────────────────────────────────────────────────────
# Keyed by category string → {"ts": float, "data": list}
_events_cache: dict[str, dict] = {}
_EVENTS_CACHE_TTL = 45  # seconds — fast reloads hit cache, stale after 45s
_events_cache_lock = asyncio.Lock()


# ── Backend fetching ──────────────────────────────────────────────────────────

async def _fetch_metas(url: str) -> list[dict]:
    """Fetch a catalog endpoint and return the metas list (handles pagination)."""
    try:
        base_url = url[:-5] if url.endswith(".json") else url
        all_metas = []
        skip = 0
        
        async with httpx.AsyncClient(timeout=8, verify=False) as client:
            while True:
                page_url = f"{base_url}/skip={skip}.json" if skip > 0 else url
                resp = await client.get(page_url)
                
                if resp.status_code == 200:
                    data = resp.json()
                    metas = data.get("metas", [])
                    if not metas:
                        break
                    all_metas.extend(metas)
                    if len(metas) < 20:  # PAGE_SIZE is 20
                        break
                    skip += 20
                else:
                    if skip == 0:
                        raise Exception(f"HTTP {resp.status_code}")
                    else:
                        logger.warning("Scheduler: HTTP %d for %s", resp.status_code, page_url)
                        break
        return all_metas
    except Exception as exc:
        logger.warning("Scheduler: fetch failed %s: %s", url, exc)
        raise exc


async def _fetch_backend_events() -> tuple[list[dict], list[dict]]:
    """
    Returns (live_metas, today_metas) — both as lists of Stremio meta dicts.
    live_metas  = currently live (releaseInfo == "LIVE")
    today_metas = scheduled for today (has a date timestamp)
    """
    live_raw, all_events = await asyncio.gather(
        _fetch_metas(_LIVE_CATALOG_URL),
        fetch_all_events_for_admin("all"),
    )

    live_events = [m for m in live_raw if not _is_always_on(m)]
    live_ids    = {m.get("id") for m in live_events}
    today_events = [
        m for m in all_events
        # Keep events NOT in sports_live feed regardless of their releaseInfo.
        # Removing `not _is_live(m)` fixes the case where an event is marked LIVE
        # in a sport-specific catalog (sports_football etc.) but absent from the
        # sports_live feed due to backend lag or a transient glitch — previously
        # these fell through both lists and the scheduler never started them.
        if not _is_always_on(m) and m.get("id") not in live_ids
    ]

    return live_events, today_events


async def fetch_all_events_for_admin(category: str = "", force_refresh: bool = False) -> list[dict]:
    """
    Fetch events across all sport categories for the admin schedule panel.
    Results are cached for _EVENTS_CACHE_TTL seconds — subsequent calls within
    that window return instantly from memory without any HTTP requests.
    On error the stale cache is returned rather than an empty list.
    """
    cache_key = category or "all"
    now = time.monotonic()

    if not force_refresh:
        async with _events_cache_lock:
            cached = _events_cache.get(cache_key)
            if cached and (now - cached["ts"]) < _EVENTS_CACHE_TTL:
                return cached["data"]

    # Cache miss or expired — fetch fresh data
    if category and category != "all":
        catalogs = [category]
    else:
        catalogs = _ALL_SPORT_CATALOGS

    urls = [
        f"{_BACKEND_URL}/{_SCHED_TOKEN}/catalog/sports/{cat}.json"
        for cat in catalogs
    ]

    # return_exceptions=True so ONE slow/failing catalog (e.g. a pathologically
    # slow upstream category timing out at 40s+) can't collapse the entire event
    # list to empty. Each catalog that raises is treated as empty and skipped;
    # the rest still populate the schedule.
    try:
        raw_results = await asyncio.gather(*[_fetch_metas(url) for url in urls],
                                           return_exceptions=True)
    except Exception as exc:
        logger.warning("Scheduler: fetch_all_events_for_admin failed: %s", exc)
        cached = _events_cache.get(cache_key)
        return cached["data"] if cached else []

    results = []
    _n_failed = 0
    for cat, res in zip(catalogs, raw_results):
        if isinstance(res, Exception):
            logger.warning("Scheduler: catalog %s failed, skipping: %s", cat, res)
            results.append([])
            _n_failed += 1
        else:
            results.append(res)

    # If EVERY catalog failed, don't cache an empty list over good stale data —
    # return the last-known events so a transient upstream outage doesn't blank
    # the schedule tab. A partial failure (some catalogs OK) proceeds normally.
    if _n_failed == len(catalogs) and _n_failed > 0:
        logger.warning("Scheduler: ALL %d catalogs failed — returning stale cache", _n_failed)
        cached = _events_cache.get(cache_key)
        if cached:
            return cached["data"]

    seen: dict[str, dict] = {}  # id → meta
    for cat, metas in zip(catalogs, results):
        for m in metas:
            if _is_always_on(m):
                continue
            mid = m.get("id", "")
            if mid not in seen:
                m["_category"] = cat
                m["_is_live"]  = _has_started(m)
                seen[mid] = m
            elif cat not in ("sports_live", "sports_today"):
                # Upgrade generic aggregator category to sport-specific label
                seen[mid]["_category"] = cat

    # Sort: live first, then by releaseInfo string
    events = sorted(seen.values(), key=lambda m: (0 if m["_is_live"] else 1, m.get("releaseInfo", "")))

    # Store in cache
    async with _events_cache_lock:
        _events_cache[cache_key] = {"ts": time.monotonic(), "data": events}

    return events


def _is_always_on(meta: dict) -> bool:
    """True for leaf: and sf: prefixed IDs — managed by CineArena loop."""
    mid = meta.get("id", "")
    return mid.startswith("leaf:") or mid.startswith("sf:")


def _is_live(meta: dict) -> bool:
    """True if meta is currently live (releaseInfo == LIVE)."""
    return meta.get("releaseInfo", "").upper() == "LIVE"


def _has_started(meta: dict) -> bool:
    """True if backend says LIVE, or start time has passed and is within max duration.
    Used for admin display so events that started (but backend is slow to mark LIVE)
    still appear in the Live Now section instead of Today.
    """
    if _is_live(meta):
        return True
    starts_at = _parse_event_starts_at(meta)
    if starts_at <= 0:
        return False
    return starts_at <= time.time() <= starts_at + MAX_EVENT_DURATION_SECS


def _starts_within_buffer(meta: dict) -> bool:
    """
    True if a scheduled event starts within PRE_START_BUFFER_SECS.
    The backend puts the start time as the description or releaseInfo string.
    The raw streamed.pk 'date' field (Unix ms) isn't in the catalog meta,
    so we use releaseInfo: if it's not "LIVE" and not empty, it's a time string
    like "18 Jun 2026 · 21:00 UTC". We parse that to compare against now.
    """
    info = meta.get("releaseInfo", "")
    if not info or info.upper() == "LIVE":
        return False
    try:
        from datetime import datetime, timezone
        # Format: "18 Jun 2026 · 21:00 UTC"
        info_clean = info.replace(" UTC", "").strip()
        # Remove the · separator
        info_clean = info_clean.replace(" · ", " ")
        dt = datetime.strptime(info_clean, "%d %b %Y %H:%M")
        dt = dt.replace(tzinfo=timezone.utc)
        secs_until = dt.timestamp() - time.time()
        # Also catch events whose scheduled time just passed but aren't marked LIVE yet
        return -MAX_EVENT_DURATION_SECS <= secs_until <= PRE_START_BUFFER_SECS
    except Exception:
        return False


def _is_event_live_or_soon(meta: dict) -> bool:
    """True if meta is currently live or starting soon."""
    return _is_live(meta) or _starts_within_buffer(meta)


def _parse_event_starts_at(meta: dict) -> float:
    """
    Parse releaseInfo ("18 Jun 2026 · 21:00 UTC") into a unix timestamp.
    Returns 0.0 if unparseable (matches behave like live/24-7).
    """
    info = meta.get("releaseInfo", "")
    if not info or info.upper() == "LIVE":
        return 0.0
    try:
        from datetime import datetime, timezone
        info_clean = info.replace(" UTC", "").strip()
        info_clean = info_clean.replace(" · ", " ")
        dt = datetime.strptime(info_clean, "%d %b %Y %H:%M")
        dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


def _event_id_from_meta(meta: dict) -> str:
    """
    Extract the raw event ID from a meta item.
    e.g. "streamed:1234567" → "1234567"
    We use the full stremio id as the event_id key for consistency.
    """
    return meta.get("id", "")


def _event_title_from_meta(meta: dict) -> str:
    return meta.get("name", meta.get("id", ""))


def _resolve_pinned_auto_end(event_id: str, event_title: str, event_starts_at: float) -> Optional[float]:
    """Return the ABSOLUTE, PERSISTENT auto-end epoch-seconds for an event, or None for 24/7.

    This is the single source of truth for when a scheduler recording ends, and
    it is designed so a master/worker restart NEVER moves the end time:

      1. If a pinned timestamp already exists (event_channel_store), reuse it
         verbatim. This is what survives restarts — the whole point.
      2. Otherwise compute it ONCE and pin it:
         - anchor = the event's real UTC kickoff (event_starts_at) when known,
           so the 30-min pre-start buffer is never counted; else "now" on first
           sight for an already-LIVE event (releaseInfo == "LIVE", no parseable
           start — the anchor is within one tick of the true start and, crucially,
           the pinned value never moves again).
         - offset = the per-event/team end-offset if configured, else
           DEFAULT_END_OFFSET_HOURS. Either way it is ALWAYS set — never None —
           so ends_at can never fall back to the restart-recomputed
           time.time()+MAX_EVENT_DURATION_SECS path again.
         - UNLESS offset == 0 (the "24/7 no limit" admin option) → return None
           to mark this as a permanent recording that never auto-ends.

    Returns None for 24/7 channels (end_offset_hours == 0), or a real epoch timestamp."""
    from event_channel_store import (
        get_end_offset_for_event, get_auto_end_ts, set_auto_end_ts,
    )
    pinned = get_auto_end_ts(event_id)
    if pinned:
        return float(pinned)

    offset_hours = get_end_offset_for_event(event_id, event_title, event_id)
    if offset_hours is None:
        offset_hours = DEFAULT_END_OFFSET_HOURS

    # 24/7 permanent channel: end_offset_hours == 0 means "never auto-end"
    if offset_hours == 0:
        logger.info("Scheduler: event %s marked as 24/7 (end_offset_hours=0) — no auto-end", event_id)
        return None

    anchor = event_starts_at if event_starts_at and event_starts_at > 0 else time.time()
    auto_end_at = anchor + offset_hours * 3600
    # If the computed end is already in the past (e.g. event started hours ago and
    # end_offset ran out), anchor from NOW instead — otherwise a force-start or
    # re-assignment would pin a stale past timestamp and the recording dies immediately.
    _anchor_label = "kickoff" if (event_starts_at and event_starts_at > 0) else "now"
    if auto_end_at <= time.time():
        auto_end_at = time.time() + offset_hours * 3600
        _anchor_label = "now (kickoff+offset already passed)"
    set_auto_end_ts(event_id, auto_end_at)
    logger.info(
        "Scheduler: pinned auto-end for %s → %s (anchor=%s, offset=%.1fh)",
        event_id,
        time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(auto_end_at)),
        _anchor_label,
        offset_hours,
    )
    return auto_end_at


# ── Account pool helpers ──────────────────────────────────────────────────────

def _get_accounts() -> dict:
    try:
        from iptv_accounts import _load_accounts
        return _load_accounts()
    except Exception:
        return {}


async def _claim_account_for_slug(ch_slug: str) -> tuple[str, str] | tuple[None, None]:
    """Atomically claim a free xtream account for ch_slug via motor and return
    (account_id, stream_url). Returns (None, None) if all accounts are busy.

    Uses async_claim_free_account which holds an asyncio.Lock for the
    find→write window — so concurrent ticks and the probe can never
    double-book the same account (no in-memory bridge needed)."""
    from iptv_accounts import async_claim_free_account
    acc_id, acc = await async_claim_free_account(ch_slug)
    if not acc_id:
        logger.warning("Scheduler: all accounts busy, cannot start '%s'", ch_slug)
        return None, None
    server   = acc.get("server", "").rstrip("/")
    username = acc.get("username", "")
    password = acc.get("password", "")
    if not server or not username:
        # Bad account data — release the claim we just made (await so it can't be
        # dropped if the loop is shutting down).
        from iptv_accounts import async_mark_free
        await async_mark_free(acc_id)
        logger.warning("Scheduler: account %s has no server/username — skipping", acc_id)
        return None, None
    stream_url = f"{server}/live/{username}/{password}/{ch_slug}.m3u8"
    logger.info("Scheduler: claimed %s on acc %s for slug '%s'", acc_id, acc_id, ch_slug)
    return acc_id, stream_url


def _persist_recording_meta(slug: str, meta: dict) -> None:
    """Persist a scheduler recording's metadata to MongoDB in a background thread.
    Without this, scheduler recordings live only in-memory: a master restart wipes
    them, they show as bare slug numbers with no name in the Recordings tab, and the
    orphan detector kills the still-live worker stream because it's tracked nowhere."""
    snapshot = dict(meta)  # copy so a later mutation can't race the write
    def _do():
        try:
            from leaf_admin import _save_recording_meta
            _save_recording_meta(slug, snapshot)
        except Exception as exc:
            logger.warning("Scheduler: could not persist recording meta for %s: %s", slug, exc)
    asyncio.get_event_loop().run_in_executor(None, _do)


def _delete_recording_meta_bg(slug: str) -> None:
    """Remove a scheduler recording's metadata from MongoDB in a background thread,
    so a stopped stream doesn't get restored / re-shown after a master restart."""
    def _do():
        try:
            from leaf_admin import _delete_recording_meta
            _delete_recording_meta(slug)
        except Exception as exc:
            logger.warning("Scheduler: could not delete recording meta for %s: %s", slug, exc)
    asyncio.get_event_loop().run_in_executor(None, _do)


# ── Stream URL resolution ─────────────────────────────────────────────────────

# ── Recording management ──────────────────────────────────────────────────────

async def _pick_least_loaded_worker() -> Optional[tuple[str, str]]:
    """Return (worker_id, worker_url) for worker with fewest active recordings."""
    from leaf_server import _parse_workers
    from worker_client import status_of_worker

    workers = _parse_workers()
    if not workers:
        return None

    best_id, best_url, best_count = None, None, float("inf")
    for wid, wurl in workers.items():
        status = await status_of_worker(wurl)
        count = len(status) if status is not None else float("inf")
        if count < best_count:
            best_id, best_url, best_count = wid, wurl, count

    return (best_id, best_url) if best_id else None


async def _start_scheduler_recording(
    ch_slug:    str,
    stream_url: str,
    meta:       dict,
    acc_id:     str,
    is_pre_start: bool = False,
) -> None:
    """Start an M3U recording managed by the scheduler.

    is_pre_start=True means the recording started 30 min before the event's
    scheduled live time (event is in today_metas, not yet in live_metas).
    The flag is exposed via _recording_meta → leaf catalog → backend
    cinearena_channels so it can gate the stream display per plan.
    """
    from m3u_recorder import active_m3u_tasks, m3u_recordings, start_m3u_recording
    from config import RESIDENTIAL_PROXIES
    from leaf_admin import _recording_meta

    if ch_slug in active_m3u_tasks:
        # Account was already claimed by caller — release it, nothing new is recording it.
        if acc_id:
            from iptv_accounts import async_mark_free
            await async_mark_free(acc_id)
        return

    # Starting a recording is an explicit "we want this" — clear any stale hide
    # left over from a previous auto-end/remove-all. Otherwise reconcile would
    # see the slug in _hidden_slugs and immediately kill this fresh recording
    # (and leave a zombie in _scheduler_recordings that blocks all future restarts).
    try:
        from leaf_admin import _hidden_slugs, _save_hidden_slugs
        if ch_slug in _hidden_slugs:
            _hidden_slugs.pop(ch_slug, None) if isinstance(_hidden_slugs, dict) else _hidden_slugs.discard(ch_slug)
            _save_hidden_slugs()
            logger.info("Scheduler: cleared stale hide for '%s' on (re)start", ch_slug)
    except Exception as _exc:
        logger.warning("Scheduler: could not clear hidden slug for '%s': %s", ch_slug, _exc)

    title      = _event_title_from_meta(meta)
    categories = meta.get("genres") or ["Sports"]
    headers    = {"user-agent": "okhttp/5.0.0-alpha.2"}

    if _LEAF_MODE == "master":
        pick = await _pick_least_loaded_worker()
        if not pick:
            logger.warning("Scheduler: no workers available for %s", ch_slug)
            # Release the account we claimed — nothing is recording it.
            if acc_id:
                from iptv_accounts import async_mark_free
                await async_mark_free(acc_id)
            return
        worker_id, worker_url = pick
        from worker_client import start_on_worker
        ok = await start_on_worker(worker_url, ch_slug, stream_url, headers, list(RESIDENTIAL_PROXIES))
        if not ok:
            logger.error("Scheduler: failed to start %s on worker %s", ch_slug, worker_id)
            if acc_id:
                from iptv_accounts import async_mark_free
                await async_mark_free(acc_id)
            return
        from leaf_server import _slug_to_worker
        _slug_to_worker[ch_slug] = worker_url
        logger.info("Scheduler: ▶ routed '%s' to worker %s", ch_slug, worker_id)
        # Persist slug→worker mapping to MongoDB so master restart recovery works.
        # Without this, _load_slug_to_worker() can't find streams that were never
        # rotated, causing the scheduler to re-launch them on a NEW worker → duplicates.
        # Run the blocking pymongo write in a thread so it never blocks the event loop.
        # A direct MongoClient() call in async code was freezing the entire server
        # when MongoDB was slow or workers went unreachable simultaneously.
        def _persist_worker_assignment():
            try:
                from config import MONGODB_URI
                if MONGODB_URI:
                    from pymongo import MongoClient
                    _wk_col = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=3000, socketTimeoutMS=5000, connectTimeoutMS=5000)["stremio"]["leaf_workers"]
                    _wk_col.update_one(
                        {"_id": worker_id},
                        {"$set": {f"assigned_streams.{ch_slug}.stream_url": stream_url,
                                  f"assigned_streams.{ch_slug}.account_id": acc_id}},
                        upsert=True,
                    )
            except Exception as _exc:
                logger.warning("Scheduler: could not persist worker assignment to MongoDB: %s", _exc)
        asyncio.get_event_loop().run_in_executor(None, _persist_worker_assignment)
    else:
        start_m3u_recording(ch_slug, stream_url, headers, RESIDENTIAL_PROXIES)

    # Set display metadata once recorder initialises
    async def _set_meta():
        for _ in range(30):
            if ch_slug in m3u_recordings:
                m3u_recordings[ch_slug].title      = title
                m3u_recordings[ch_slug].categories = categories
                break
            await asyncio.sleep(0.2)

    asyncio.create_task(_set_meta())

    # Register in admin meta (so it shows in Recordings tab)
    # event_id stored here so leaf_server can set streamed_id → backend injects
    # this stream UNDER the matching streamed.pk event instead of as standalone

    try:
        from iptv_accounts import get_channel_name
        chan_name = get_channel_name(acc_id, ch_slug) or ""
    except Exception:
        chan_name = ""

    event_starts_at = _parse_event_starts_at(meta)

    _ev_id = _event_id_from_meta(meta)
    # Absolute, pinned auto-end — computed from UTC kickoff (or now on first sight),
    # reused verbatim across every restart. None when end_offset_hours==0 (24/7 channel).
    auto_end_at = _resolve_pinned_auto_end(_ev_id, _event_title_from_meta(meta), event_starts_at)
    is_permanent = (auto_end_at is None)  # end_offset_hours==0 → 24/7, never auto-stops

    _recording_meta[ch_slug] = {
        "display_name":    title,
        "channel_name":    chan_name,
        "account_id":      acc_id,
        "stream_id":       "",
        "ends_at":         auto_end_at,
        "categories":      categories,
        "scheduler":       True,
        "event_id":        _ev_id,
        "is_pre_start":    is_pre_start,
        "event_starts_at": event_starts_at,
        "auto_end_at":     auto_end_at,
        "permanent":       is_permanent,  # 24/7: skip auto-end, hard-cap, and feed-absence stops
    }

    # Persist scheduler meta to MongoDB so it survives a master restart — otherwise
    # the stream shows as a bare slug with no name and gets orphan-killed on reboot.
    _persist_recording_meta(ch_slug, _recording_meta[ch_slug])

    # Persist state
    _scheduler_recordings[ch_slug] = {
        "event_id":   _event_id_from_meta(meta),
        "account_id": acc_id,
        "started_at": time.time(),
        "shared_events": {_event_id_from_meta(meta)},
    }
    # Account already claimed atomically by _claim_account_for_slug before this
    # function was called — no second mark_busy needed here.

    if is_pre_start:
        logger.info(
            "Scheduler: ⏰ PRE-START '%s' for '%s' via account %s (starts in %d min)",
            ch_slug, title, acc_id, max(1, int((event_starts_at - time.time()) / 60)) if event_starts_at else 0,
        )
    else:
        logger.info("Scheduler: ▶ started '%s' for '%s' via account %s",
                    ch_slug, title, acc_id)


def _free_orphan_account(ch_slug: str) -> None:
    """Free whichever account holds ch_slug when account_id was not recorded (bug4).
    Runs in a thread via run_in_executor — never called directly from the event loop."""
    try:
        from iptv_accounts import _load_accounts, mark_account_free
        for aid, a in _load_accounts().items():
            if a.get("current_slug") == ch_slug:
                mark_account_free(aid)
                logger.info("Scheduler: freed orphan-locked account %s (held %s)", aid, ch_slug)
                break
    except Exception as exc:
        logger.warning("Scheduler: orphan free failed for %s: %s", ch_slug, exc)


def _stop_scheduler_recording(ch_slug: str) -> None:
    """Stop a scheduler-managed recording and free its account."""
    from m3u_recorder import active_m3u_tasks, stop_m3u_recording
    from leaf_admin import _recording_meta

    # ── Protect admin-owned / permanent (24/7) channels ──────────────────────
    # A 24/7 channel can be an ADMIN recording (started from the "Start Recording"
    # panel with duration 0 → meta has no "scheduler" flag) or a scheduler 24/7
    # (assigned to an event with end-time "24/7 no limit" → "permanent": True).
    # When an admin 24/7 channel is assigned to an event, the tick's re-register
    # path adopts it into _scheduler_recordings for display/sharing — but it must
    # NEVER be stopped by the scheduler (event auto-end, feed-absence, hard cap).
    # Only a manual admin stop (which calls stop_m3u_recording / the worker stop
    # endpoint directly, not this function) may end it. Detach it from scheduler
    # stop-tracking instead of stopping it, so a sibling event's auto-end or a
    # feed-absence sweep can't kill a channel the admin owns.
    _meta = _recording_meta.get(ch_slug, {})
    _is_admin_owned = not _meta.get("scheduler", False)
    _is_permanent   = _meta.get("permanent", False)
    if _meta and (_is_admin_owned or _is_permanent):
        _scheduler_recordings.pop(ch_slug, None)   # stop tracking for events
        _stop_miss_counts.pop(ch_slug, None)
        logger.info(
            "Scheduler: ⛔ refusing to stop protected 24/7 channel '%s' "
            "(admin_owned=%s permanent=%s) — detached from event tracking, stream kept alive",
            ch_slug, _is_admin_owned, _is_permanent,
        )
        return

    rec_info = _scheduler_recordings.pop(ch_slug, {})
    acc_id   = rec_info.get("account_id")
    _recording_meta.pop(ch_slug, None)
    _delete_recording_meta_bg(ch_slug)

    # Only force-delete the primary event that caused this stop.
    # Shared events must NOT be force-deleted — they should start fresh
    # on the next tick once the channel is free.
    event_id = rec_info.get("event_id", "")
    if event_id:
        _force_deleted.add(event_id)

    if _LEAF_MODE == "master":
        from leaf_server import _slug_to_worker
        worker_url = _slug_to_worker.pop(ch_slug, None)
        if worker_url:
            from worker_client import stop_on_worker
            # bug1+3: delay account free until worker confirms stop, not fire-and-forget
            async def _stop_and_free(_wu=worker_url, _cs=ch_slug, _aid=acc_id):
                ok = await stop_on_worker(_wu, _cs)
                if not ok:
                    # Worker unreachable — try once more after 10s, then give up.
                    await asyncio.sleep(10)
                    ok = await stop_on_worker(_wu, _cs)
                if not ok:
                    logger.error(
                        "Scheduler: ⚠️ stop_on_worker failed for '%s' on %s after retry — "
                        "account '%s' may stay locked until reconcile or manual free",
                        _cs, _wu, _aid or "unknown",
                    )
                    return
                if _aid:
                    from iptv_accounts import async_mark_free
                    await async_mark_free(_aid)
                else:
                    await asyncio.get_event_loop().run_in_executor(None, _free_orphan_account, _cs)
            asyncio.create_task(_stop_and_free())
            logger.info("Scheduler: ■ routed stop '%s' to %s", ch_slug, worker_url)
            return
    else:
        if ch_slug in active_m3u_tasks:
            stop_m3u_recording(ch_slug)
            logger.info("Scheduler: ■ stopped '%s'", ch_slug)

    if acc_id:
        from iptv_accounts import async_mark_free
        # Await directly — _stop_scheduler_recording is called from async contexts
        # (tick, reconcile). create_task risks task being dropped on SIGTERM.
        # Note: this function is sync but callers always run it from async frames;
        # the outer create_task in _stop_and_free handles the async boundary.
        asyncio.get_event_loop().create_task(async_mark_free(acc_id))
    else:
        _free_orphan_account(ch_slug)


# ── Capacity status (for admin panel) ────────────────────────────────────────

def get_capacity_status() -> dict:
    """Return current account pool status."""
    accounts = _get_accounts()
    try:
        from iptv_accounts import _is_account_usable
    except Exception:
        _is_account_usable = lambda a: True
    details  = [
        {
            "id":           acc_id,
            "name":         acc.get("name", acc_id),
            "current_slug": acc.get("current_slug"),
            "busy_since":   acc.get("busy_since"),
            "status":       acc.get("status", ""),
            "exp_date":     acc.get("exp_date"),
            "usable":       _is_account_usable(acc),
        }
        for acc_id, acc in accounts.items()
    ]
    total = len(details)
    busy  = sum(1 for d in details if d["current_slug"])
    return {
        "total":    total,
        "busy":     busy,
        "free":     total - busy,
        "warnings": list(_capacity_warnings),
        "accounts": details,
    }


# ── Account reconciliation ───────────────────────────────────────────────────

async def _reconcile_accounts() -> None:
    """
    Free any accounts that are marked busy in MongoDB but whose recording
    is no longer running.
    Runs on startup and at the start of every tick to prevent ghost locks.
    """
    try:
        from iptv_accounts import _load_accounts, mark_account_free
    except Exception:
        return

    if _LEAF_MODE == "master":
        from iptv_accounts import async_load_accounts  # used at lines below — must be in scope
        from leaf_server import _parse_workers
        from worker_client import status_of_worker, stop_on_worker
        from leaf_admin import _hidden_slugs
        from leaf_server import _slug_to_worker
        from iptv_accounts import mark_account_busy
        workers = _parse_workers()
        active: set = set()
        slug_workers: dict[str, list[str]] = {}   # slug → [worker_url, ...] running it
        failed_workers = 0
        for wurl in workers.values():
            st = await status_of_worker(wurl)
            if st is not None:   # empty dict {} = valid response (no streams), None = unreachable
                # Only count slugs whose ffmpeg is ACTUALLY running. The worker keeps
                # a slug in m3u_recordings (and thus in the status payload) after its
                # ffmpeg dies, with running=False. Counting those zombie entries as
                # active kept dead slugs "live" — poisoning the account map, blocking
                # ghost-free, and producing phantom double-use. Trust the running flag.
                for _slug, _info in st.items():
                    _running = _info.get("running") if isinstance(_info, dict) else True
                    if _running:
                        active.add(_slug)
                        slug_workers.setdefault(_slug, []).append(wurl)
            else:
                failed_workers += 1
        if failed_workers:
            logger.warning(
                "Scheduler: reconcile — %d/%d worker(s) unreachable, skipping ghost-free to avoid false positives",
                failed_workers, len(workers),
            )

        # ── Duplicate-WORKER detection: same slug live on 2+ workers ─────────────
        # A master/worker restart can re-exec a slug from leaf_workers on a new
        # worker while the old copy is still running elsewhere → the SAME channel
        # streams on TWO accounts at once. Their rotations then fight each other
        # (each frees an account the other still uses → provider 458 → more
        # rotations) and the account bookkeeping drifts. Keep the copy the master
        # actually routes viewers to (_slug_to_worker); stop every other copy so
        # one slug = one worker = one account. Only act when all workers responded.
        if not failed_workers:
            for _dup_slug, _wurls in slug_workers.items():
                if len(_wurls) < 2:
                    continue
                _keep = _slug_to_worker.get(_dup_slug)
                if _keep not in _wurls:
                    _keep = _wurls[0]   # master has no route — keep first, stop rest
                for _wu in _wurls:
                    if _wu == _keep:
                        continue
                    logger.error(
                        "Scheduler: reconcile — '%s' live on %d workers %s — stopping "
                        "duplicate on %s, keeping %s (one slug = one worker)",
                        _dup_slug, len(_wurls), _wurls, _wu, _keep,
                    )
                    asyncio.create_task(stop_on_worker(_wu, _dup_slug))

        # ── Authoritative slug→account map from leaf_workers.assigned_streams ────
        # This is the ONLY trustworthy source of "which account is actually
        # streaming this slug" — it is written atomically when a worker starts a
        # stream and on rotation, and it matches the live ffmpeg process. The
        # in-memory _recording_meta / _scheduler_recordings account_ids drift on
        # any failed-rotation persist and MUST NOT be used to decide ownership,
        # or reconcile re-marks the wrong account busy → duplicate lock → the
        # wrong account gets freed → probe/tick reuses a live account → HTTP 458.
        # CRITICAL: only include slugs that are ACTUALLY RUNNING (in `active`, the
        # set of slugs confirmed live via each worker's /internal/status). leaf_workers
        # accumulates STALE assignments when a stream stops/moves without the doc being
        # cleaned — those dead entries can map an account to a slug it no longer serves,
        # and (worse) the same account to two slugs. Filtering by `active` keeps only
        # the assignment that matches a live ffmpeg process, so worker_acct is an
        # accurate slug→account map of what's streaming right now.
        worker_acct: dict[str, str] = {}   # slug → account_id, live streams only
        acct_seen: dict[str, str] = {}     # account_id → slug, to detect real double-use
        # username → account_id map, to self-heal a leaf_workers entry whose
        # `account_id` field is missing (e.g. written by an older worker build that
        # clobbered the master's account_id). The stream_url embeds the account's
        # username (…/live/<username>/<password>/<id>.m3u8), so we can recover the
        # real owner from the URL even when account_id is absent. Without this, a
        # live account maps to no slug → shows FREE → probe grabs it → HTTP 458.
        _user_to_acct: dict[str, str] = {}
        for _aid_u, _acc_u in (await async_load_accounts()).items():
            _un = _acc_u.get("username")
            if _un:
                _user_to_acct[_un] = _aid_u
        def _acct_from_url(_url: str) -> str:
            # …/live/<username>/<password>/<id>.(m3u8|ts) → username → account_id
            try:
                _parts = _url.split("/live/", 1)[1].split("/")
                return _user_to_acct.get(_parts[0], "")
            except Exception:
                return ""
        try:
            import iptv_accounts as _ia
            _ia._get_motor_col()
            if _ia._motor_client is not None:
                _wcol = _ia._motor_client["stremio"]["leaf_workers"]
                async for _wdoc in _wcol.find({}):
                    _wid       = _wdoc.get("_id")
                    _this_wurl = workers.get(_wid)
                    for _s, _info in (_wdoc.get("assigned_streams") or {}).items():
                        if _s not in active:
                            continue  # dead/stale leaf_workers entry — not actually streaming
                        # Only trust this entry if THIS doc's own worker is the one
                        # actually running the slug (running=true per /internal/status).
                        # leaf_workers is never pruned when a stream moves worker or
                        # dies without an explicit /stop, so a slug accumulates stale
                        # entries across multiple worker docs — each with a DIFFERENT
                        # account_id. Without this check worker_acct[slug] resolves to
                        # whichever doc MongoDB returns LAST (nondeterministic), which
                        # can map a live slug to a stale account so reconcile then frees
                        # the REAL streaming account → 2nd connection → HTTP 458.
                        # slug_workers is the authoritative slug→running-worker(s) map.
                        if _this_wurl not in slug_workers.get(_s, []):
                            continue
                        _a = (_info or {}).get("account_id")
                        if not _a:
                            # Self-heal: no account_id on this entry (clobbered by an
                            # older worker build). Recover it from the stream_url's
                            # username and persist it back so reconcile can map the
                            # slug → account and never leave the live account showing
                            # FREE (the exact gap that caused the probe → HTTP 458).
                            _a = _acct_from_url((_info or {}).get("stream_url", ""))
                            if not _a:
                                continue
                            try:
                                await _wcol.update_one(
                                    {"_id": _wid},
                                    {"$set": {f"assigned_streams.{_s}.account_id": _a}},
                                )
                                logger.warning(
                                    "Scheduler: reconcile — healed missing account_id for '%s' → '%s' "
                                    "(recovered from stream_url username)",
                                    _s, _a,
                                )
                            except Exception as _hexc:
                                logger.warning("Scheduler: reconcile — could not persist healed account_id for '%s': %s", _s, _hexc)
                        worker_acct[_s] = _a
                        if _a in acct_seen and acct_seen[_a] != _s:
                            # Two live slugs share the same account → genuine double-use.
                            # Auto-resolve immediately by stopping the SECOND slug on its
                            # worker so it restarts and claims a different account.
                            # The first slug (acct_seen[_a]) keeps the account.
                            # Previously this just logged "needs manual check" and looped
                            # forever — that left the double-use unresolved for 90+ min,
                            # and with MongoDB current_slug=None (drifted) a probe or tick
                            # could then claim the same account a third time → 458 cascade.
                            _first = acct_seen[_a]
                            logger.error(
                                "Scheduler: reconcile — account '%s' used by TWO live slugs "
                                "('%s' and '%s') — auto-stopping '%s' so it restarts on a free account",
                                _a, _first, _s, _s,
                            )
                            _dup_wurl = _slug_to_worker.get(_s)
                            if _dup_wurl:
                                asyncio.create_task(stop_on_worker(_dup_wurl, _s))
                            # Keep acct_seen pointing at the original/first holder only.
                        else:
                            acct_seen[_a] = _s
        except Exception as _exc:
            logger.warning("Scheduler: reconcile — could not load leaf_workers map: %s", _exc)

        # Publish the set of account_ids actually streaming right now (running=true,
        # filtered by `active`) so async_claim_free_account can refuse to hand out a
        # genuinely-live account whose MongoDB current_slug has drifted to None.
        # Only publish when every worker responded: if a worker is unreachable its
        # accounts are absent from worker_acct, and overwriting the set would wrongly
        # un-block them (→ a claim could reuse a still-live account → 458). Keeping
        # the last-good set is the safe direction.
        if not failed_workers:
            try:
                from iptv_accounts import publish_live_accounts
                publish_live_accounts(set(worker_acct.values()))
            except Exception as _pexc:
                logger.warning("Scheduler: reconcile — publish_live_accounts failed: %s", _pexc)

        for acc_id, acc in (await async_load_accounts()).items():
            slug = acc.get("current_slug")
            if slug:
                # Probe holds: a live /probe request claimed this account for a
                # few seconds (sentinel slug __probe__*). It manages its own
                # release in a finally. Reconcile must NOT free it — doing so
                # while the probe's connection is open lets the same tick start
                # a real recording on it → 2nd connection → HTTP 458.
                if str(slug).startswith("__probe__"):
                    continue
                # Slug confirmed live on a worker this pass → clear any ghost strike
                # so a brief earlier status stall can't accumulate toward a false
                # free. Only a run of CONSECUTIVE misses may free the account.
                if slug in active:
                    _ghost_miss_counts.pop(acc_id, None)
                if slug not in active:
                    if not failed_workers:  # only free if ALL workers responded
                        # Ghost-free GRACE: do NOT free on the first miss. A single
                        # stalled/slow /internal/status makes a live account look
                        # idle for one pass; freeing it immediately lets the next
                        # claim hand the still-live account to a 2nd stream → 458 →
                        # reuse cascade. Require _GHOST_FREE_TICKS consecutive
                        # passes seeing the slug absent from the live worker set
                        # before freeing, exactly like the orphan detector.
                        _ghost_miss_counts[acc_id] = _ghost_miss_counts.get(acc_id, 0) + 1
                        if _ghost_miss_counts[acc_id] < _GHOST_FREE_TICKS:
                            logger.info(
                                "Scheduler: reconcile — '%s' (slug='%s') not on any worker "
                                "(%d/%d) — grace before freeing",
                                acc_id, slug, _ghost_miss_counts[acc_id], _GHOST_FREE_TICKS,
                            )
                        else:
                            logger.warning(
                                "Scheduler: reconcile — freeing ghost lock on '%s' (slug='%s' not on any worker, %d ticks)",
                                acc_id, slug, _ghost_miss_counts[acc_id],
                            )
                            from iptv_accounts import async_mark_free
                            await async_mark_free(acc_id)
                            _scheduler_recordings.pop(slug, None)
                            _slug_to_worker.pop(slug, None)
                            _ghost_miss_counts.pop(acc_id, None)
                elif slug in worker_acct and worker_acct[slug] != acc_id:
                    # Impostor lock: this account holds a slug that leaf_workers
                    # says belongs to a DIFFERENT account (the one actually
                    # streaming). Free this one — keeping it would leave the real
                    # streaming account looking free, so a probe/tick would reuse
                    # it and open a 2nd connection → HTTP 458. This is the exact
                    # bug that let the probe grab a live account.
                    if not failed_workers:
                        logger.warning(
                            "Scheduler: reconcile — freeing impostor lock '%s' on slug='%s' "
                            "(leaf_workers says real owner is '%s')",
                            acc_id, slug, worker_acct[slug],
                        )
                        from iptv_accounts import async_mark_free
                        await async_mark_free(acc_id)
                elif slug in _hidden_slugs:
                    worker_url = _slug_to_worker.get(slug)
                    if worker_url:
                        asyncio.create_task(stop_on_worker(worker_url, slug))
                        _slug_to_worker.pop(slug, None)
                    logger.warning(
                        "Scheduler: reconcile — stopping hidden recording '%s' on worker and freeing '%s'",
                        slug, acc_id,
                    )
                    from iptv_accounts import async_mark_free as _amf
                    await _amf(acc_id)
                    # Purge in-memory tracking so this doesn't become a zombie that
                    # the tick's "already recording" skip refuses to ever restart.
                    _scheduler_recordings.pop(slug, None)
                    _stop_miss_counts.pop(slug, None)
                    try:
                        from leaf_admin import _recording_meta as _rmeta_hid
                        _rmeta_hid.pop(slug, None)
                        _delete_recording_meta_bg(slug)
                    except Exception:
                        pass
            else:
                # Account is free in MongoDB (current_slug=None) → no ghost lock to
                # track. Drop any stale strike so a later reuse of this account_id
                # starts from a clean count.
                _ghost_miss_counts.pop(acc_id, None)
                # Reverse ghost: account shows free in MongoDB but leaf_workers
                # says it is actually streaming a slug on a worker. Use worker_acct
                # (authoritative) as the ONLY source — never _recording_meta /
                # _scheduler_recordings account_ids, which drift and caused the
                # wrong account to be re-marked busy (→ duplicate → 458).
                candidate = None
                for _s, _a in worker_acct.items():
                    if _a == acc_id and _s in active:
                        candidate = _s
                        break
                if candidate:
                    logger.warning(
                        "Scheduler: reconcile — re-marking '%s' busy (slug='%s' active on worker per leaf_workers but MongoDB shows free)",
                        acc_id, candidate,
                    )
                    from iptv_accounts import async_mark_busy
                    await async_mark_busy(acc_id, candidate)

        # ── Duplicate lock detection: multiple accounts holding same slug ────────
        from collections import defaultdict as _dd
        slug_holders: dict = _dd(list)
        _fresh = await async_load_accounts()
        for acc_id2, acc2 in _fresh.items():
            s = acc2.get("current_slug")
            if s and not str(s).startswith("__probe__"):
                slug_holders[s].append(acc_id2)
        for slug, holders in slug_holders.items():
            if len(holders) > 1:
                # Keep the holder leaf_workers confirms is actually streaming this
                # slug; free the rest. NEVER free arbitrarily — the previous code
                # freed holders[1:] blindly, which could free the live account and
                # leave an idle impostor holding the slug (→ probe reuses the live
                # account → HTTP 458). Fall back to holders[0] only if leaf_workers
                # has no record (worker doc lost).
                keep = worker_acct.get(slug)
                if keep not in holders:
                    keep = holders[0]
                for dup_acc in holders:
                    if dup_acc == keep:
                        continue
                    logger.warning(
                        "Scheduler: reconcile — freeing duplicate lock on '%s' (slug='%s' held by %d accounts, keeping real owner '%s')",
                        dup_acc, slug, len(holders), keep,
                    )
                    from iptv_accounts import async_mark_free as _amfd
                    await _amfd(dup_acc)

        # ── Sync _recording_meta account_ids from real MongoDB source ────────────
        # After rotations or master restarts, _recording_meta[slug]["account_id"]
        # can drift from what leaf_iptv_accounts actually shows. Fix by syncing
        # from the authoritative source (slug → account from _load_accounts).
        from leaf_admin import _recording_meta as _rmeta2
        slug_to_real_acc = {
            acc2.get("current_slug"): aid2
            for aid2, acc2 in (await async_load_accounts()).items()
            if acc2.get("current_slug")
        }
        for _slug, _real_acc in slug_to_real_acc.items():
            if _slug in _rmeta2 and _rmeta2[_slug].get("account_id") != _real_acc:
                _rmeta2[_slug]["account_id"] = _real_acc
                # The account moved (rotation or heal) without going through the
                # rotate endpoint, so the last_rotated_from/to display fields are
                # now stale and point at an account this slug no longer uses.
                # Clear them so the admin panel doesn't show a phantom
                # "🔄 X → Y" that makes it look like an account is shared.
                if _rmeta2[_slug].get("last_rotated_to") not in (None, "", _real_acc):
                    _rmeta2[_slug].pop("last_rotated_from", None)
                    _rmeta2[_slug].pop("last_rotated_to", None)

        # ── Orphan stream detection (with grace period) ──────────────────────
        # Streams still running on workers but removed from both _scheduler_recordings
        # and _recording_meta. A stream must be seen as orphan across
        # _ORPHAN_STOP_TICKS consecutive reconcile passes before being stopped.
        # This prevents mass-stops right after a master restart, when the in-memory
        # state is empty and legit streams haven't been re-registered by _tick() yet.
        if not failed_workers:
            from leaf_server import _slug_to_worker as _s2w
            from worker_client import stop_on_worker as _sow
            current_orphans: set = set()
            for _slug in list(active):
                if _slug not in _scheduler_recordings and _slug not in _rmeta2:
                    current_orphans.add(_slug)

            # Reset counters for slugs no longer orphaned (got re-registered)
            for _s in list(_orphan_miss_counts.keys()):
                if _s not in current_orphans:
                    _orphan_miss_counts.pop(_s, None)

            for _slug in current_orphans:
                _orphan_miss_counts[_slug] = _orphan_miss_counts.get(_slug, 0) + 1
                if _orphan_miss_counts[_slug] < _ORPHAN_STOP_TICKS:
                    logger.info(
                        "Scheduler: reconcile — '%s' looks orphaned (%d/%d), grace before stop",
                        _slug, _orphan_miss_counts[_slug], _ORPHAN_STOP_TICKS,
                    )
                    continue
                _wu = _s2w.get(_slug)
                if _wu:
                    logger.warning(
                        "Scheduler: reconcile — stopping confirmed orphan '%s' on %s (%d ticks)",
                        _slug, _wu, _orphan_miss_counts[_slug],
                    )
                    async def _kill_orphan(_s=_slug, _w=_wu):
                        ok = await _sow(_w, _s)
                        if ok:
                            _s2w.pop(_s, None)
                            _delete_recording_meta_bg(_s)
                            from iptv_accounts import async_load_accounts as _ala, async_mark_free as _amf
                            _accs = await _ala()
                            for _aid, _acc in _accs.items():
                                if _acc.get("current_slug") == _s:
                                    await _amf(_aid)
                                    break
                    asyncio.create_task(_kill_orphan())
                    _orphan_miss_counts.pop(_slug, None)
        return

    try:
        from m3u_recorder import active_m3u_tasks
    except Exception:
        return

    try:
        from leaf_admin import _hidden_slugs
        from m3u_recorder import stop_m3u_recording
    except Exception:
        _hidden_slugs = set()
        stop_m3u_recording = None

    from iptv_accounts import async_load_accounts as _ala_sa, async_mark_free as _amf_sa
    for acc_id, acc in (await _ala_sa()).items():
        slug = acc.get("current_slug")
        if not slug:
            continue
        if str(slug).startswith("__probe__"):
            continue  # transient probe hold — probe frees it in its own finally
        if slug not in active_m3u_tasks:
            logger.warning(
                "Scheduler: reconcile — freeing ghost lock on '%s' (slug='%s' not recording)",
                acc_id, slug,
            )
            await _amf_sa(acc_id)
        elif slug in _hidden_slugs:
            # Hidden recording is still running and holding an account — stop it
            logger.warning(
                "Scheduler: reconcile — stopping hidden recording '%s' and freeing '%s'",
                slug, acc_id,
            )
            if stop_m3u_recording:
                stop_m3u_recording(slug)
            await _amf_sa(acc_id)


# ── Main loop ─────────────────────────────────────────────────────────────────

async def run_scheduler_loop() -> None:
    """Background coroutine — starts with leaf_main.py, runs forever."""
    logger.info("📅 IPTV Scheduler started (pre-buffer=%ds, interval=60s)",
                PRE_START_BUFFER_SECS)
    # Free any ghost locks left over from previous run
    await _reconcile_accounts()
    while True:
        try:
            await _tick()
        except Exception as exc:
            logger.exception("Scheduler tick error: %s", exc)
        await asyncio.sleep(60)


async def _tick() -> None:
    global _capacity_warnings
    _capacity_warnings = []

    # Import at function top so _recording_meta is always bound as a local variable
    # for the entire function scope — avoids UnboundLocalError when any code path
    # later reassigns it via another `from leaf_admin import _recording_meta`.
    from leaf_admin import _recording_meta

    # Auto-heal any ghost locks before doing anything else
    await _reconcile_accounts()

    from event_channel_store import get_channels_for_event, cleanup_expired_overrides

    live_metas, today_metas = await _fetch_backend_events()
    logger.info("Scheduler tick: %d live, %d today events", len(live_metas), len(today_metas))

    # Build set of currently live event IDs (to detect ended events)
    live_event_ids: set[str] = {_event_id_from_meta(m) for m in live_metas}

    # Also include upcoming events within pre-start buffer — prevents stopping a recording
    # when the primary event ends but a shared event is about to start (e.g. Argentina ends,
    # Colombia starts in 14min — don't stop and restart, keep the recording alive)
    upcoming_event_ids: set[str] = {
        _event_id_from_meta(m) for m in today_metas
        if _is_live(m) or _starts_within_buffer(m)
    }
    # "Active" = live right now OR starting soon
    active_event_ids: set[str] = live_event_ids | upcoming_event_ids

    # Also treat force-started events as live (until their expiry)
    now_mono = time.monotonic()
    expired_fs = [eid for eid, exp in _force_started.items() if now_mono >= exp]
    for eid in expired_fs:
        del _force_started[eid]
        logger.info("Scheduler: force-start window expired for %s", eid)
    live_event_ids |= set(_force_started.keys())

    # Load accounts fresh from MongoDB (non-blocking via motor) once per tick.
    from iptv_accounts import async_load_accounts
    accounts = await async_load_accounts()

    # ── Process events that should be recording ─────────────────────────────
    # Events to act on = live now + starting within PRE_START_BUFFER
    # Also include today_metas entries that are LIVE in sport catalogs but
    # absent from sports_live feed (backend lag) — _is_live catches those.
    actionable = live_metas + [
        m for m in today_metas if _is_live(m) or _starts_within_buffer(m)
    ]
    # Add force-started events from today's list even if not yet within buffer
    force_today = [
        m for m in today_metas
        if _event_id_from_meta(m) in _force_started
        and _event_id_from_meta(m) not in {_event_id_from_meta(x) for x in actionable}
    ]
    actionable.extend(force_today)

    # Also handle force-started events that have COMPLETELY LEFT the catalog
    # (e.g. ended events — streamed.pk removes them hours later, so they're in
    # neither live_metas nor today_metas). Build a synthetic meta from the persisted
    # _recording_meta so the start loop can restart their channels.
    already_in_actionable = {_event_id_from_meta(x) for x in actionable}
    for _fs_eid in list(_force_started.keys()):
        if _fs_eid in already_in_actionable:
            continue
        # Look up event info from persisted recording metadata
        from leaf_admin import _recording_meta as _rmeta_fs
        _found_meta = None
        for _slug, _m in list(_rmeta_fs.items()):
            if _m.get("event_id") == _fs_eid:
                _found_meta = _m
                break
        if _found_meta:
            synthetic_meta = {
                "id":          _fs_eid,
                "name":        _found_meta.get("display_name", _fs_eid),
                "genres":      _found_meta.get("categories", ["Sports"]),
                "releaseInfo": "LIVE",  # treat as live so is_pre_start=False
            }
            actionable.append(synthetic_meta)
            logger.info("Scheduler: force-start — added synthetic meta for off-catalog event '%s'",
                        _found_meta.get("display_name", _fs_eid))
        else:
            # No recording meta — try event_channel_store for the title
            from event_channel_store import get_channels_for_event as _gce
            _chs = _gce(_fs_eid, _fs_eid, _fs_eid)
            if _chs:
                actionable.append({
                    "id":          _fs_eid,
                    "name":        _fs_eid,
                    "genres":      ["Sports"],
                    "releaseInfo": "LIVE",
                })
                logger.info("Scheduler: force-start — added bare meta for off-catalog event %s", _fs_eid)

    for meta in actionable:
        event_id    = _event_id_from_meta(meta)
        event_title = _event_title_from_meta(meta)
        event_slug  = event_id  # slug for matching

        if not event_id:
            continue

        # Skip force-deleted events entirely
        if event_id in _force_deleted:
            logger.debug("Scheduler: skipping force-deleted event %s", event_id)
            continue

        # Pre-start flag: True when the event is in today_metas (not yet live)
        # False when it's already in live_metas. Used by leaf catalog + backend
        # to gate the "🕐 Starting in X min" UI.
        is_pre_start = event_id not in live_event_ids

        # Team rules fire for both upcoming and already-live events
        channels = get_channels_for_event(event_id, event_title, event_slug, use_team_rules=True)
        if not channels:
            continue  # No assignment yet

        logger.info("Scheduler: event '%s' has %d assigned channel(s): %s (pre_start=%s)",
                    event_title[:40], len(channels), channels, is_pre_start)

        for ch_slug in channels:
            # Zombie guard: a slug can linger in _scheduler_recordings after its
            # stream died on the worker (reconcile hidden-kill, worker restart,
            # ffmpeg crash). In master mode a live scheduler recording ALWAYS has
            # a _slug_to_worker entry, so if that's missing the dict entry is stale.
            # Purge it here so the code below can cleanly restart the channel
            # instead of skipping it forever via the "already recording" branch.
            if _LEAF_MODE == "master" and ch_slug in _scheduler_recordings:
                from leaf_server import _slug_to_worker as _s2w_chk
                if ch_slug not in _s2w_chk:
                    logger.warning(
                        "Scheduler: '%s' in _scheduler_recordings but not on any worker — clearing zombie, will restart",
                        ch_slug,
                    )
                    _scheduler_recordings.pop(ch_slug, None)
                    _stop_miss_counts.pop(ch_slug, None)
                    from leaf_admin import _recording_meta as _rmeta_z
                    _rmeta_z.pop(ch_slug, None)
                    # Also delete the persisted Mongo doc. If the restart below
                    # succeeds, _start_scheduler_recording re-persists (idempotent).
                    # If it can't restart (e.g. all accounts busy → continue), we
                    # must NOT leave the stale doc behind or it resurrects as a
                    # ghost recording on the next master restart.
                    _delete_recording_meta_bg(ch_slug)

            if ch_slug in _scheduler_recordings:
                # Channel is already recording — add this event to the shared set
                rec_info = _scheduler_recordings[ch_slug]
                shared = rec_info.setdefault("shared_events", set())
                if event_id not in shared:
                    shared.add(event_id)
                    logger.info("Scheduler: '%s' already recording — added event '%s' to shared set (now %d events)",
                                ch_slug, event_title[:40], len(shared))
                    # Store this event's metadata so catalog can show it under its own name
                    from leaf_admin import _recording_meta
                    if ch_slug in _recording_meta:
                        shared_list = _recording_meta[ch_slug].setdefault("shared_events_meta", [])
                        if not any(m.get("event_id") == event_id for m in shared_list):
                            shared_list.append({
                                "event_id":        event_id,
                                "display_name":    event_title,
                                "categories":      meta.get("genres") or ["Sports"],
                                "event_starts_at": _parse_event_starts_at(meta),
                                "is_pre_start":    is_pre_start,
                            })
                        # ── Auto-end extension: if this new event ends LATER than the
                        # current pin, extend the recording to cover it.  Without this,
                        # a channel assigned to event B while already recording event A
                        # gets killed at A's auto_end even though B runs longer.
                        _new_auto_end = _resolve_pinned_auto_end(
                            event_id, event_title, _parse_event_starts_at(meta)
                        )
                        _cur_auto_end = _recording_meta[ch_slug].get("auto_end_at")
                        if _new_auto_end and (_cur_auto_end is None or _new_auto_end > _cur_auto_end):
                            _recording_meta[ch_slug]["auto_end_at"] = _new_auto_end
                            _recording_meta[ch_slug]["ends_at"]     = _new_auto_end
                            logger.info(
                                "Scheduler: '%s' auto-end extended to %s (new event '%s' runs longer)",
                                ch_slug,
                                time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(_new_auto_end)),
                                event_title[:40],
                            )
                        # Re-persist so the extended auto_end survives a restart.
                        _persist_recording_meta(ch_slug, _recording_meta[ch_slug])
                continue

            if ch_slug in _get_active_m3u_tasks():
                continue

            # Master mode: stream may already be running on a worker (e.g. after master restart).
            # Re-register it so scheduler can track/stop it and catalog can see it.
            if _LEAF_MODE == "master":
                from leaf_server import _slug_to_worker
                if ch_slug in _slug_to_worker:
                    acc_id_for_slug = next(
                        (aid for aid, a in accounts.items() if a.get("current_slug") == ch_slug),
                        None,
                    )
                    from leaf_admin import _recording_meta  # must be before the dict below
                    _scheduler_recordings[ch_slug] = {
                        "event_id":     event_id,
                        "account_id":   acc_id_for_slug,
                        # Use the persisted started_at if available — resetting to
                        # time.time() on every restart would give the hard-cap a fresh
                        # window and let a channel that already ran 7h run another 24h.
                        "started_at":   (_recording_meta.get(ch_slug) or {}).get("started_at", time.time()),
                        "shared_events": {event_id},
                    }
                    if ch_slug not in _recording_meta:
                        _ev_starts = _parse_event_starts_at(meta)
                        # Same absolute pinned auto-end as the start path — on a
                        # master restart this reuses the ALREADY-PINNED timestamp
                        # (get_auto_end_ts), so the end time does not move.
                        _auto_end = _resolve_pinned_auto_end(event_id, event_title, _ev_starts)
                        _recording_meta[ch_slug] = {
                            "display_name":    event_title,
                            "channel_name":    "",
                            "account_id":      acc_id_for_slug or "",
                            "stream_id":       "",
                            "ends_at":         _auto_end,
                            "categories":      meta.get("genres") or ["Sports"],
                            "scheduler":       True,
                            "event_id":        event_id,
                            "is_pre_start":    is_pre_start,
                            "event_starts_at": _ev_starts,
                            "auto_end_at":     _auto_end,
                            "permanent":       (_auto_end is None),
                        }
                        _persist_recording_meta(ch_slug, _recording_meta[ch_slug])
                    # Ensure MongoDB reflects this account as busy after restart.
                    # Must await — fire-and-forget risks a concurrent tick claiming
                    # the same account before the write lands (double-booking).
                    if acc_id_for_slug:
                        from iptv_accounts import async_mark_busy
                        try:
                            await async_mark_busy(acc_id_for_slug, ch_slug)
                        except Exception as _e:
                            logger.error(
                                "Scheduler: re-confirm busy failed for '%s' (acc=%s): %s — skipping",
                                ch_slug, acc_id_for_slug, _e,
                            )
                            continue
                    logger.info("Scheduler: re-registered '%s' on existing worker (master restart recovery)", ch_slug)
                    continue
            acc_id, stream_url = await _claim_account_for_slug(ch_slug)
            if not acc_id:
                warn = f"All accounts busy — can't start: {event_title} ({ch_slug})"
                logger.warning("Scheduler: %s", warn)
                _capacity_warnings.append(warn)
                continue

            # Account is already claimed atomically inside _claim_account_for_slug.
            await _start_scheduler_recording(
                ch_slug, stream_url, meta, acc_id, is_pre_start=is_pre_start,
            )

    # ── Transition detection: pre-start → live ───────────────────────────────
    # When an event we started in pre-start mode appears in live_metas,
    # flip its is_pre_start flag to False so the UI stops showing the
    # countdown and shows the normal "Leaf · small delay" label instead.
    # The recording itself continues uninterrupted.
    from leaf_admin import _recording_meta
    for ch_slug, rec_info in list(_scheduler_recordings.items()):
        event_id = rec_info.get("event_id", "")
        if not event_id:
            continue
        meta_entry = _recording_meta.get(ch_slug, {})
        if not meta_entry.get("is_pre_start"):
            continue
        if event_id in live_event_ids:
            meta_entry["is_pre_start"] = False
            _recording_meta[ch_slug] = meta_entry
            _persist_recording_meta(ch_slug, meta_entry)  # keep flag across restart
            logger.info(
                "Scheduler: ⏰→🔴 '%s' transitioned to LIVE — countdown disabled",
                ch_slug,
            )

    # ── Re-adopt orphaned scheduler recordings ───────────────────────────────
    # A scheduler recording that is running on a worker and present in
    # _recording_meta (restored from Mongo after a master restart) but MISSING
    # from _scheduler_recordings can never be evaluated by the stop loop below —
    # so an event that ENDED while the master was down (or that ended and then the
    # master restarted) lingers forever: it shows in the panel with an "Ends in
    # Xm" but nothing can stop it, because the stop loop only iterates
    # _scheduler_recordings. The normal re-register path (above) only re-adopts
    # events that are still in the live feed; ended ones are never re-adopted.
    # Fix: adopt every worker-backed scheduler meta into _scheduler_recordings so
    # the stop loop can end it (via feed-absence grace, auto_end, or hard cap).
    if _LEAF_MODE == "master":
        from leaf_server import _slug_to_worker as _s2w_adopt
        for _cs, _m in list(_recording_meta.items()):
            if not _m.get("scheduler"):
                continue
            if _cs in _scheduler_recordings:
                continue
            if _cs not in _s2w_adopt:
                continue  # not actually on a worker — leave to orphan/restore paths
            _eid = _m.get("event_id", "")
            _scheduler_recordings[_cs] = {
                "event_id":      _eid,
                "account_id":    _m.get("account_id"),
                "started_at":    _m.get("started_at", time.time()),
                "shared_events": {_eid} if _eid else set(),
            }
            logger.info(
                "Scheduler: re-adopted orphaned recording '%s' (event=%s) into stop-tracking",
                _cs, _eid or "(none)",
            )

    # ── Stop recordings for events no longer live ───────────────────────────
    from leaf_admin import _recording_meta
    for ch_slug, rec_info in list(_scheduler_recordings.items()):
        if ch_slug not in _scheduler_recordings:
            continue  # already stopped by a sibling auto-end in this loop
        event_id = rec_info.get("event_id", "")
        started  = rec_info.get("started_at", 0)

        # A channel is PROTECTED from any scheduler stop if it is admin-owned
        # (no `scheduler` flag) or explicitly permanent (24/7). Assigning such a
        # channel to an event adopts it into _scheduler_recordings for display,
        # but the scheduler must NEVER end it — only a manual admin stop can.
        def _is_protected(_s: str) -> bool:
            _m = _recording_meta.get(_s, {})
            # No meta at all → NOT protected. An empty dict must not read as
            # protected (the old `not _m.get("scheduler")` returned True for {}),
            # or a scheduler channel that lost its meta would become unstoppable
            # and leak forever. Only a channel with real meta that is admin-owned
            # (no scheduler flag) or explicitly permanent is protected.
            if not _m:
                return False
            return _m.get("permanent", False) or not _m.get("scheduler", False)

        # Auto-end: event_starts_at + end_offset_hours — stops ALL channels of the event
        auto_end = _recording_meta.get(ch_slug, {}).get("auto_end_at")
        if auto_end and time.time() >= auto_end:
            # Only stop channels whose PRIMARY event is this one AND that have no
            # live secondary events. A channel shared with a still-live event B must
            # NOT be swept away when event A's auto_end fires — it was extended to
            # cover B (see the shared-set auto_end extension above) but the primary
            # pointer may still be A, so the filter must check shared_events too.
            event_slugs = [
                s for s, r in list(_scheduler_recordings.items())
                if r.get("event_id") == event_id
                and not _is_protected(s)
                and not any(
                    eid for eid in r.get("shared_events", set()) - {event_id}
                    if eid in active_event_ids
                )
            ]
            for s in event_slugs:
                logger.info("Scheduler: auto-end '%s' (event %s end_offset_hours)", s, event_id)
                _stop_miss_counts.pop(s, None)
                _stop_scheduler_recording(s)
            # Event genuinely reached its scheduled end — clear the pinned auto-end
            # timestamp so if this event is ever re-added it gets a fresh window.
            try:
                from event_channel_store import clear_auto_end_ts
                clear_auto_end_ts(event_id)
            except Exception:
                pass
            # _hidden_slugs is a dict[slug -> expiry]; add each slug with a TTL.
            # (Was `.update(list)` which throws ValueError on a dict and crashed the tick.)
            from leaf_admin import _hidden_slugs, _save_hidden_slugs, _HIDDEN_SLUG_TTL
            _exp = time.time() + _HIDDEN_SLUG_TTL
            for s in event_slugs:
                _hidden_slugs[s] = _exp
            _save_hidden_slugs()
            # Detach any PROTECTED (admin/24-7) channels that were only sharing this
            # ended event — they keep recording, but must stop pinning the ended
            # event in live_registry so it can leave Live Now. Drop the ended
            # event from their shared set; if it was their primary, they revert to
            # a standalone 24/7 channel with no event association.
            for _ps, _pr in list(_scheduler_recordings.items()):
                if not _is_protected(_ps):
                    continue
                _shared = _pr.get("shared_events") or set()
                if event_id in _shared:
                    _shared.discard(event_id)
                    _pr["shared_events"] = _shared
                if _pr.get("event_id") == event_id:
                    _pr["event_id"] = next(iter(_shared), "")
                    logger.info("Scheduler: detached ended event %s from protected 24/7 channel '%s' (keeps running)",
                                event_id, _ps)
            continue

        # Protected channels (admin-owned or explicitly permanent 24/7): never
        # stopped by hard-cap or feed-absence — only a manual admin stop can end
        # them. _stop_scheduler_recording also refuses to stop these as a final
        # guard. BUT we must still DETACH ended events from the channel so it stops
        # pinning them in live_registry (otherwise an event assigned only to a 24/7
        # channel — whose auto_end_at is None so the auto-end branch never runs —
        # stays stuck in Live Now forever). Keep the channel alive; just drop
        # events that are gone from the live feed (with the same grace as normal
        # channels to ride out transient streamed.pk glitches).
        if _is_protected(ch_slug):
            _shared = rec_info.get("shared_events") or ({event_id} if event_id else set())
            _absent = {e for e in _shared if e and e not in active_event_ids}
            if _absent:
                miss = _stop_miss_counts.get(ch_slug, 0) + 1
                _stop_miss_counts[ch_slug] = miss
                if miss >= STOP_GRACE_TICKS:
                    _kept = _shared - _absent
                    rec_info["shared_events"] = _kept
                    if rec_info.get("event_id") in _absent:
                        rec_info["event_id"] = next(iter(_kept), "")
                    if ch_slug in _recording_meta:
                        _recording_meta[ch_slug]["event_id"] = rec_info.get("event_id", "")
                        _persist_recording_meta(ch_slug, _recording_meta[ch_slug])
                    _stop_miss_counts.pop(ch_slug, None)
                    logger.info(
                        "Scheduler: protected 24/7 '%s' — detached ended event(s) %s after %d ticks "
                        "(channel keeps running, events leave Live Now)",
                        ch_slug, _absent, miss,
                    )
                else:
                    logger.debug(
                        "Scheduler: protected 24/7 '%s' — event(s) %s absent %d/%d ticks (grace)",
                        ch_slug, _absent, miss, STOP_GRACE_TICKS,
                    )
            else:
                _stop_miss_counts.pop(ch_slug, None)
            continue

        # Hard cap: 8 hours (always respected, no grace)
        if time.time() - started > MAX_EVENT_DURATION_SECS:
            logger.info("Scheduler: hard timeout for '%s' (8h elapsed)", ch_slug)
            _stop_miss_counts.pop(ch_slug, None)
            _stop_scheduler_recording(ch_slug)
            continue

        # Pre-start recordings are intentionally NOT in live_metas yet — they
        # are buffering ahead of the scheduled kickoff time.  Never count them
        # as absent; they will transition to live (and the flag will flip) once
        # the event appears in the live catalog.
        if _recording_meta.get(ch_slug, {}).get("is_pre_start", False):
            _stop_miss_counts.pop(ch_slug, None)  # reset any stale miss count
            logger.debug("Scheduler: pre-start '%s' — skipping absent check", ch_slug)
            continue

        # NOTE: the pinned auto_end (checked above) is the ABSOLUTE CEILING and the
        # 8h hard cap is the backstop. But feed-absence below is the PRIMARY early
        # stop — an event that drops from the live feed for STOP_GRACE_TICKS (~15
        # min, enough to ride out streamed.pk mid-match glitches) is genuinely over
        # and is stopped now rather than lingering hours until its auto_end ceiling.
        # (Previously an `if auto_end: continue` here made auto_end authoritative and
        # ignored feed-absence entirely, which left ended events "live" for hours.)

        # Event dropped from live feed — use grace period to avoid stopping
        # on transient API glitches.  Only stop after STOP_GRACE_TICKS consecutive misses.
        # For shared recordings (multiple events using same channel), only stop when
        # ALL events are absent from the live feed AND none are starting soon.
        if event_id and event_id not in active_event_ids:
            # Check if ANY of the shared events are still live or starting soon
            shared_events = rec_info.get("shared_events", {event_id})
            still_live_events = [eid for eid in shared_events if eid in active_event_ids]

            if still_live_events:
                # Primary event ended but shared events remain — promote new primary
                if event_id not in live_event_ids:
                    new_primary = still_live_events[0]
                    rec_info["event_id"] = new_primary
                    rec_info["shared_events"] = set(still_live_events)
                    if ch_slug in _recording_meta:
                        _recording_meta[ch_slug]["event_id"] = new_primary
                        # Also update display_name, ends_at, auto_end_at to reflect
                        # the new primary — without this the catalog shows the wrong
                        # event name and live_registry reports the stale end time.
                        _new_primary_meta = next(
                            (m for m in (live_metas + today_metas)
                             if _event_id_from_meta(m) == new_primary), None
                        )
                        if _new_primary_meta:
                            _recording_meta[ch_slug]["display_name"] = _event_title_from_meta(_new_primary_meta)
                            _new_end = _resolve_pinned_auto_end(
                                new_primary,
                                _event_title_from_meta(_new_primary_meta),
                                _parse_event_starts_at(_new_primary_meta),
                            )
                            if _new_end:
                                _recording_meta[ch_slug]["auto_end_at"] = _new_end
                                _recording_meta[ch_slug]["ends_at"]     = _new_end
                        _persist_recording_meta(ch_slug, _recording_meta[ch_slug])
                    logger.info("Scheduler: '%s' primary %s ended, promoted %s from shared set",
                                ch_slug, event_id, new_primary)
                _stop_miss_counts.pop(ch_slug, None)
                logger.debug(
                    "Scheduler: '%s' event '%s' absent but %d other event(s) still live — keeping recording",
                    ch_slug, event_id, len(still_live_events)
                )
            else:
                # All shared events are absent — apply grace period
                miss = _stop_miss_counts.get(ch_slug, 0) + 1
                _stop_miss_counts[ch_slug] = miss
                if miss >= STOP_GRACE_TICKS:
                    logger.info(
                        "Scheduler: all events for '%s' absent %d/%d ticks (shared: %s) — stopping",
                        ch_slug, miss, STOP_GRACE_TICKS, shared_events,
                    )
                    _stop_miss_counts.pop(ch_slug, None)
                    _stop_scheduler_recording(ch_slug)
                else:
                    logger.info(
                        "Scheduler: all events for '%s' miss %d/%d (shared: %s) — grace, not stopping yet",
                        ch_slug, miss, STOP_GRACE_TICKS, shared_events,
                    )
        else:
            # Event is live — reset grace counter
            _stop_miss_counts.pop(ch_slug, None)

    # ── Cleanup expired event overrides from MongoDB ──────────────────────────
    cleanup_expired_overrides()

    # ── Publish the live-event registry ───────────────────────────────────────
    # Build the authoritative set of events we are ACTIVELY recording and sync it
    # to leaf_live_registry. The main backends read this to keep those events in
    # their "Live Now" page regardless of streamed.pk hiccups, and to remove them
    # ONLY when we mark them ended here (auto-end / hard cap / real stop).
    # One event can span multiple channels (shared) — dedupe by event_id, and for
    # a shared channel register every event_id in its shared set so none is
    # prematurely marked ended while its channel is still live.
    try:
        from leaf_admin import _recording_meta as _rmeta_reg
        import live_registry as _reg
        _active_events: dict[str, dict] = {}
        for _cs, _rinfo in list(_scheduler_recordings.items()):
            _meta = _rmeta_reg.get(_cs, {})
            # Skip pre-start recordings — the event is not live in the feed yet, so
            # it's not on the Live Now page and must not be pinned there.
            if _meta.get("is_pre_start", False):
                continue
            _shared = _rinfo.get("shared_events") or {_rinfo.get("event_id", "")}
            for _eid in _shared:
                if not _eid or str(_eid).startswith("__probe__"):
                    continue
                # match_id = raw streamed id (strip the "streamed:" prefix if present)
                _mid = _eid.split(":", 1)[1] if ":" in str(_eid) else _eid
                _ends = _meta.get("ends_at") or 0.0
                _prev = _active_events.get(_eid)
                # If several channels carry the same event, keep the latest ends_at.
                if _prev and _prev.get("ends_at", 0) >= _ends:
                    continue
                _active_events[_eid] = {
                    "match_id":    _mid,
                    "title":       _meta.get("display_name", ""),
                    "category":    str(_meta.get("categories", "")),
                    "started_at":  _rinfo.get("started_at", time.time()),
                    "ends_at":     _ends,
                    "auto_end_at": _meta.get("auto_end_at"),
                }
        _reg.sync_active(_active_events)
    except Exception as _rexc:
        logger.warning("Scheduler: live_registry sync failed: %s", _rexc)


def _get_active_m3u_tasks() -> set:
    try:
        from m3u_recorder import active_m3u_tasks
        return set(active_m3u_tasks.keys())
    except Exception:
        return set()
