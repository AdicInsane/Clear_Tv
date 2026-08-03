"""
leaf_main.py - Leaf Bridge daemon entry point.

Orchestration loop:
  1. Start aiohttp server on port 8087
  2. Every LEAF_POLL_INTERVAL seconds:
     a. Fetch SKTech live events (decrypt from API)
     b. Fetch streamed.pk live matches (from our addon API)
     c. Match events using fuzzy + Groq AI
     d. For each confident match:
        - Fetch stream list from SKTech API
        - Pick best HLS URL
        - Start virtual-clock recording if not already running
  3. Cancel recordings for events that are no longer live

Usage:
  python leaf_main.py

Environment variables (see config.py and .env):
  SKTECH_FIREBASE_API_KEY, SKTECH_FIREBASE_APP_ID, SKTECH_FIREBASE_PROJECT_NUMBER
  SKLIVE_V23_KEY, SKLIVE_V23_IV
  GROQ_API_KEY, MONGODB_URI
  R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT, R2_BUCKET_NAME
  ADDON_API_BASE, ADDON_ADMIN_TOKEN
  LEAF_SERVER_PORT (default 8087)
  LEAF_POLL_INTERVAL (default 60)
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from config import (
    LEAF_POLL_INTERVAL,
    MAX_CONCURRENT_LEAF,
    M3U_WHITELIST,
    R2_ACCESS_KEY_ID,
    R2_ENDPOINT,
    R2_PUBLIC_DOMAIN,
    R2_SECRET_ACCESS_KEY,
)
from ics_parser import get_active_match

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s - %(message)s",
)
logger = logging.getLogger("leaf.main")


def _build_leaf_r2_worker() -> dict:
    """
    Build the R2 worker config for Leaf.
    Uses the rumtv08 worker (cdn.kitomi33.eu.cc) if CF_WORKER_POOL is set,
    otherwise falls back to the main R2 credentials.
    """
    raw_pool = os.getenv("CF_WORKER_POOL", "")
    if raw_pool:
        for item in raw_pool.split(","):
            parts = [p.strip() for p in item.strip().split("|")]
            if len(parts) == 6:
                worker = {
                    "url":                  parts[0].rstrip("/"),
                    "secret":               parts[1],
                    "r2_domain":            parts[2].rstrip("/"),
                    "r2_access_key_id":     parts[3],
                    "r2_secret_access_key": parts[4],
                    "r2_endpoint":          parts[5],
                }
                # Pick the rumtv08 worker (cdn.kitomi33.eu.cc) for Leaf
                if "kitomi33" in worker["r2_domain"] or "rumtv08" in worker["url"]:
                    logger.info("Leaf using R2 worker: %s", worker["r2_domain"])
                    return worker

        # If rumtv08 not found, use the second entry (index 1) if available
        entries = [e.strip() for e in raw_pool.split(",") if e.strip()]
        if len(entries) >= 2:
            parts = [p.strip() for p in entries[1].split("|")]
            if len(parts) == 6:
                worker = {
                    "url":                  parts[0].rstrip("/"),
                    "secret":               parts[1],
                    "r2_domain":            parts[2].rstrip("/"),
                    "r2_access_key_id":     parts[3],
                    "r2_secret_access_key": parts[4],
                    "r2_endpoint":          parts[5],
                }
                logger.info("Leaf using R2 worker (fallback index 1): %s", worker["r2_domain"])
                return worker

    # Final fallback: main R2 credentials
    logger.warning("Leaf using main R2 credentials as fallback")
    return {
        "url":                  "",
        "secret":               "",
        "r2_domain":            R2_PUBLIC_DOMAIN,
        "r2_access_key_id":     R2_ACCESS_KEY_ID,
        "r2_secret_access_key": R2_SECRET_ACCESS_KEY,
        "r2_endpoint":          R2_ENDPOINT,
    }


async def _check_if_sports(state, title: str, summary: str) -> None:
    """Uses Groq LLM to check if a program is a sporting event or sports show."""
    from config import GROQ_API_KEY
    if not GROQ_API_KEY:
        state.is_sports = True
        return
        
    try:
        lower_title = title.lower()
        lower_summary = summary.lower()
        banned_phrases = ["review", "highlights", "replay", "another chance to view", "a look back", "sportscentre", "sc with jay", "classic match"]
        
        for phrase in banned_phrases:
            if phrase in lower_title or phrase in lower_summary:
                logger.info("Local Filter: Dropping '%s' (matches '%s')", title, phrase)
                state.is_sports = False
                return

        from groq import AsyncGroq
        import random
        keys = [k.strip() for k in GROQ_API_KEY.split(",") if k.strip()]
        selected_key = random.choice(keys) if keys else GROQ_API_KEY
        client = AsyncGroq(api_key=selected_key)
        prompt = (
            f"Evaluate the following TV program:\n"
            f"Title: {title}\n"
            f"Summary: {summary}\n\n"
            f"Is this a CURRENT, NEW, or LIVE sporting event/broadcast? "
            f"If it is a replay, highlight show, talk show, news broadcast, 'another chance to view', 'A look back', season review, retrospective, classic match, or non-sports, respond with EXACTLY 'NO'. "
            f"CRITICAL RULES: Shows like 'SC with Jay Onrait', 'SportsCentre', 'SportsCenter', or any post-game analysis/news desks MUST be answered with 'NO'. "
            f"If it is an active/new LIVE sporting event or match, respond with EXACTLY 'YES'."
        )
        resp = await client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=10,
        )
        content = resp.choices[0].message.content.strip().upper()
        
        if "NO" in content:
            logger.info("Groq Filter: Dropping '%s' (Not Sports)", title)
            state.is_sports = False
        else:
            logger.info("Groq Filter: Keeping '%s' (Sports)", title)
            state.is_sports = True
    except Exception as exc:
        logger.error("Groq check failed for '%s': %s", title, exc)
        state.is_sports = True  # Default to true on failure


async def _m3u_loop() -> None:
    import httpx
    import re
    from config import M3U_SOURCE_URL, RESIDENTIAL_PROXIES
    from m3u_recorder import start_m3u_recording, stop_m3u_recording, active_m3u_tasks, m3u_recordings
    
    logger.info("📺 M3U loop started — sourcing from %s", M3U_SOURCE_URL)
    
    while True:
        try:
            async with httpx.AsyncClient(verify=False, timeout=15) as client:
                resp = await client.get(M3U_SOURCE_URL)
            if resp.status_code == 200:
                lines = resp.text.splitlines()
                
                parsed_channels = {}
                current_title = ""
                current_headers = {}
                
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                        
                    if line.startswith("#EXTINF:"):
                        current_title = line.split(",")[-1].strip()
                        current_headers = {}
                    elif line.startswith("#EXTVLCOPT:"):
                        opt = line[len("#EXTVLCOPT:"):].strip()
                        if "=" in opt:
                            k, v = opt.split("=", 1)
                            if k == "http-user-agent":
                                current_headers["User-Agent"] = v
                            elif k == "http-referrer":
                                current_headers["Referer"] = v
                            elif k == "http-origin":
                                current_headers["Origin"] = v
                    elif not line.startswith("#"):
                        # It's a URL
                        if current_title and line.startswith("http"):
                            # Sanitize title to slug
                            slug = re.sub(r'[^a-z0-9]', '-', current_title.lower())
                            slug = re.sub(r'-+', '-', slug).strip('-')
                            
                            parsed_channels[slug] = {
                                "url": line,
                                "headers": current_headers,
                                "title": current_title
                            }
                        current_title = ""
                        current_headers = {}

                # Check the ICS Schedule
                match = get_active_match()
                
                if not match:
                    # If no match is playing, stop World-Cup M3U tasks — but NEVER stop
                    # scheduler-managed or admin-managed recordings.
                    if active_m3u_tasks:
                        try:
                            from iptv_scheduler import _scheduler_recordings as _sched_recs
                        except Exception:
                            _sched_recs = {}
                        try:
                            from leaf_admin import _recording_meta as _admin_meta
                        except Exception:
                            _admin_meta = {}
                        wc_slugs_to_stop = [
                            s for s in list(active_m3u_tasks.keys())
                            if s not in _sched_recs and s not in _admin_meta
                        ]
                        if wc_slugs_to_stop:
                            logger.info("M3U: No active World Cup match found in schedule! Stopping WC recordings to save proxy bandwidth.")
                            for active_slug in wc_slugs_to_stop:
                                stop_m3u_recording(active_slug)
                else:
                    logger.info("M3U: Active match detected '%s' - proceeding with recording", match["summary"])
                    
                    # Stop removed channels — but NEVER stop admin-started recordings
                    try:
                        from leaf_admin import _recording_meta as _admin_meta
                    except Exception:
                        _admin_meta = {}
                    for active_slug in list(active_m3u_tasks.keys()):
                        if active_slug in _admin_meta:
                            continue  # admin-managed, leave it alone
                        if active_slug not in parsed_channels:
                            logger.info("M3U: stopping recording for '%s' (removed from playlist)", active_slug)
                            stop_m3u_recording(active_slug)
                            
                    # Start new channels
                    for slug, info in parsed_channels.items():
                        if slug not in M3U_WHITELIST:
                            continue
                        if slug not in active_m3u_tasks:
                            start_m3u_recording(slug, info["url"], info["headers"], RESIDENTIAL_PROXIES)
                        if slug in m3u_recordings:
                            m3u_recordings[slug].title = info["title"]
                            # Only update stream_url if it actually changed to a different base URL
                            # Avoid clearing the recorder's seen_urls on every 60s poll
                            current = m3u_recordings[slug].stream_url
                            if current and info["url"] != current:
                                m3u_recordings[slug].stream_url = info["url"]
                            
        except Exception as exc:
            logger.error("M3U loop error: %s", exc)
            
        await asyncio.sleep(60)  # check every 60s


async def _stream_info_refresh_loop() -> None:
    """
    Master-mode background loop: polls all workers every 30s and writes
    stream_info into _recording_meta so catalog.json always has it populated,
    regardless of whether the admin panel has been visited.
    """
    import asyncio as _aio
    _INTERVAL = 30

    await _aio.sleep(15)  # short initial delay — let workers register first

    while True:
        try:
            from leaf_server import _parse_workers
            from worker_client import status_of_worker
            from leaf_admin import _recording_meta

            workers = list(_parse_workers().items())
            if workers:
                statuses = await _aio.gather(
                    *[status_of_worker(wurl) for _, wurl in workers],
                    return_exceptions=True,
                )
                updated = 0
                for (_wid, _wurl), status in zip(workers, statuses):
                    if not isinstance(status, dict):
                        continue
                    for slug, info in status.items():
                        si = info.get("stream_info")
                        if si and slug in _recording_meta:
                            if _recording_meta[slug].get("stream_info") != si:
                                _recording_meta[slug]["stream_info"] = si
                                updated += 1
                if updated:
                    logger.debug("stream_info refresh: updated %d slug(s)", updated)
        except Exception as exc:
            logger.warning("stream_info refresh loop error: %s", exc)

        await _aio.sleep(_INTERVAL)


async def _yt_schedule_poller() -> None:
    """
    Master-mode background loop: fires SCHEDULED YouTube streams when their
    start time arrives. A scheduled stream is persisted in Mongo with
    pending_start=True + scheduled_start_at=<UTC epoch>. Comparison uses
    time.time() (UTC epoch), so the VPS's local timezone is irrelevant — the
    scrape stored an absolute epoch and we compare against absolute epoch.
    """
    import asyncio as _aio, time as _t
    _INTERVAL = 30
    await _aio.sleep(20)  # let restore re-arm pending schedules first

    while True:
        try:
            from leaf_admin import _yt_meta, _yt_launch_now
            now = _t.time()
            # snapshot: launching mutates _yt_meta
            for slug, meta in list(_yt_meta.items()):
                if not meta.get("pending_start"):
                    continue
                sa = meta.get("scheduled_start_at")
                if sa is None or now < sa:
                    continue
                # Time reached — launch now. Guard against double-fire: flip the
                # flag before awaiting so a slow launch can't be re-entered.
                meta["pending_start"] = False
                ends_at = meta.get("ends_at")
                dur_h = 0.0
                if ends_at:
                    dur_h = max(0.0, (ends_at - now) / 3600.0)
                logger.info("yt schedule: firing scheduled stream '%s' (was due %d UTC)", slug, int(sa))
                ok = await _yt_launch_now(
                    slug, meta.get("youtube_url", ""), meta.get("display_name", slug),
                    meta.get("category", "Sports"), dur_h, ends_at=ends_at,
                )
                if not ok:
                    # Launch failed (worker down / stream not live yet) — re-arm so
                    # the next pass retries rather than silently dropping it.
                    meta["pending_start"] = True
                    logger.warning("yt schedule: launch failed for '%s', will retry next pass", slug)
        except Exception as exc:
            logger.warning("yt schedule poller error: %s", exc)

        await _aio.sleep(_INTERVAL)


async def _iptv_schedule_poller() -> None:
    """
    Master-mode background loop: fires manually-SCHEDULED IPTV recordings when
    their start time arrives. A scheduled IPTV channel is persisted in Mongo with
    pending_start=True + scheduled_start_at=<UTC epoch>. It holds NO account until
    this poller launches it (accounts are scarce). Comparison uses time.time()
    (UTC epoch) so the VPS's local timezone is irrelevant.
    """
    import asyncio as _aio, time as _t
    _INTERVAL = 30
    await _aio.sleep(22)  # let restore re-arm pending schedules first

    while True:
        try:
            from leaf_admin import _recording_meta, _iptv_launch_now
            now = _t.time()
            for slug, meta in list(_recording_meta.items()):
                if not meta.get("pending_start"):
                    continue
                if meta.get("source") == "youtube":
                    continue  # YouTube schedules are fired by _yt_schedule_poller
                sa = meta.get("scheduled_start_at")
                if sa is None or now < sa:
                    continue
                # Time reached — flip the flag before awaiting so a slow launch can't
                # be re-entered by the next tick, then launch.
                meta["pending_start"] = False
                logger.info("iptv schedule: firing scheduled recording '%s' (was due %d UTC)", slug, int(sa))
                ok = await _iptv_launch_now(slug)
                if not ok:
                    # Launch failed (no free account / no worker) — re-arm to retry.
                    m = _recording_meta.get(slug)
                    if m is not None:
                        m["pending_start"] = True
                    logger.warning("iptv schedule: launch failed for '%s', will retry next pass", slug)
        except Exception as exc:
            logger.warning("iptv schedule poller error: %s", exc)

        await _aio.sleep(_INTERVAL)


async def _orchestrator_loop() -> None:
    from leaf_client import get_live_events, get_stream_list, pick_best_stream
    from leaf_matcher import fetch_streamed_live_matches, match_leaf_events
    from leaf_recorder import (
        active_leaf_recordings,
        set_leaf_r2_worker,
        start_leaf_recording,
        stop_leaf_recording,
    )
    from leaf_server import start_leaf_server

    # Configure R2 worker for Leaf
    worker = _build_leaf_r2_worker()
    set_leaf_r2_worker(worker)

    # Start HTTP server
    runner = await start_leaf_server()
    logger.info("🍃 Leaf Bridge orchestrator started (poll every %ds)", LEAF_POLL_INTERVAL)

    # (M3U playlist scraper loop removed — was polling a dead ICS URL every 60s
    #  and logging harmless connection errors. M3U_WHITELIST was empty so it never
    #  started anything. Admin/scheduler streams replaced it entirely.)

    # Start IPTV event scheduler (auto-starts/stops recordings based on backend events)
    from iptv_scheduler import run_scheduler_loop
    asyncio.create_task(run_scheduler_loop())

    # Pre-warm + keep channel list cache fresh (runs immediately, then every 4h)
    from iptv_accounts import run_channel_cache_refresh_loop
    asyncio.create_task(run_channel_cache_refresh_loop())

    # Keep account expiry/status fresh from the Xtream API (immediately, then every
    # 30m). Feeds the "never claim expired/disabled account" filter + admin UI.
    from iptv_accounts import run_account_info_refresh_loop
    asyncio.create_task(run_account_info_refresh_loop())

    # Master mode: keep stream_info in _recording_meta fresh (polls workers every 30s)
    if os.getenv("LEAF_MODE", "standalone") == "master":
        asyncio.create_task(_stream_info_refresh_loop())
        # Fire scheduled YouTube streams when their UTC start time arrives.
        asyncio.create_task(_yt_schedule_poller())
        # Fire manually-scheduled IPTV recordings at their UTC start time.
        asyncio.create_task(_iptv_schedule_poller())
        # Stream-stability status monitor — isolated task, polls public playlist
        # URLs like a player. Fully exception-guarded so it can NEVER slow down or
        # hang the streaming path; if it crashes it dies alone.
        try:
            from status_monitor import run_status_monitor
            asyncio.create_task(run_status_monitor())
        except Exception as _smexc:
            logger.warning("status monitor failed to launch (contained): %s", _smexc)

    # Track which slugs are currently matched so we can cancel stale ones
    try:
        while True:
            await asyncio.sleep(3600)

    finally:
        await runner.cleanup()
        logger.info("🍃 Leaf server stopped")


if __name__ == "__main__":
    try:
        asyncio.run(_orchestrator_loop())
    except KeyboardInterrupt:
        logger.info("🍃 Leaf Bridge shut down by user")