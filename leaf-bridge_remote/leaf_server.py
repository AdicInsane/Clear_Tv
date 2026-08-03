"""
leaf_server.py - aiohttp HTTP server for Leaf Bridge.

Serves live HLS playlists from memory (same virtual clock as recorder).
Nginx on the VPS proxies:
  recaps.highfly.dev/leaf/ → localhost:8087

Endpoints:
  GET /leaf/{slug}/live.m3u8      → rolling live playlist (200 or 404 if buffering)
  GET /leaf/{slug}/metadata.json  → recording state JSON
  GET /cinearena/{slug}/live.m3u8 → CineArena live playlist
  GET /m3u/{slug}/live.m3u8       → scheduler / admin M3U recording playlist
  GET /leaf/health                → health check + active slugs
  GET /leaf/catalog.json          → all active recordings (for stream injection)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time

import httpx
from aiohttp import web

from config import LEAF_SERVER_HOST, LEAF_SERVER_PORT, LEAF_SERVER_PUBLIC_URL, ADDON_API_BASE, DELAY_SECONDS
from leaf_recorder import build_live_playlist, leaf_recordings
from m3u_recorder import m3u_recordings, build_m3u_playlist

logger = logging.getLogger("leaf.server")

# ── Master mode routing ────────────────────────────────────────────────────────

_LEAF_MODE = os.getenv("LEAF_MODE", "standalone")

# slug → worker_url  (populated on master startup, updated on worker_up)
_slug_to_worker: dict[str, str] = {}

# Lock prevents two workers grabbing the same free account simultaneously
_rotation_lock = asyncio.Lock()

# Recently-tried accounts per slug, so a flaky channel's rotation cycles through
# DISTINCT accounts instead of ping-ponging between the same 2. Without this, a
# dead channel frees an account back to the front of the free pool and immediately
# re-picks it next cycle — burning the rotation budget re-testing the same account
# and creating log noise. We remember the last few and exclude them from the next
# claim. Keyed by slug → list of recently-used account_ids (most-recent last).
_recent_rotation_accts: dict[str, list[str]] = {}
_RECENT_ROTATION_KEEP = 4   # remember the last 4 accounts a slug rotated through

# Playlist cache: slug → (bytes, expires_monotonic)
# Caches each playlist for 2s so 400+ concurrent viewers don't all hit the worker
_playlist_cache: dict[str, tuple[bytes, float]] = {}
_PLAYLIST_CACHE_TTL = 2.0

# Single-flight: slug → in-flight asyncio.Future for the current upstream fetch.
# Without this, when the 2s cache expires a viral channel's hundreds of concurrent
# viewers ALL miss the cache on the same tick and ALL fire upstream GETs at once
# (a cache stampede / thundering herd) — every 2s. That floods the worker and
# stalls the master event loop for 20-57s (observed hang at 15k req/min on a
# single 900-viewer channel). With single-flight, only the FIRST misser fetches;
# every other concurrent request for that slug awaits the same Future and shares
# the result, collapsing ~900 upstream reqs/2s into 1 upstream req/2s per channel.
_playlist_inflight: dict[str, "asyncio.Future[tuple[int, bytes]]"] = {}

# Persistent pooled HTTP client for worker proxy requests (avoids new TCP conn per request)
_worker_client: httpx.AsyncClient | None = None

def _get_worker_client() -> httpx.AsyncClient:
    global _worker_client
    if _worker_client is None or _worker_client.is_closed:
        _worker_client = httpx.AsyncClient(
            timeout=8,
            limits=httpx.Limits(
                max_connections=1000,           # was 200 — exhausted at ~6k req/min under high latency
                max_keepalive_connections=200,  # was 50
            ),
        )
    return _worker_client


def _parse_workers() -> dict[str, str]:
    """Parse LEAF_WORKERS=worker1=http://w1:8088,worker2=http://w2:8089 → {id: url}."""
    result = {}
    for item in os.getenv("LEAF_WORKERS", "").split(","):
        item = item.strip()
        if "=" in item:
            wid, url = item.split("=", 1)
            result[wid.strip()] = url.strip()
    return result


async def _load_slug_to_worker() -> None:
    """Build _slug_to_worker from MongoDB leaf_workers on master startup (async motor)."""
    workers = _parse_workers()
    if not workers:
        return
    from config import MONGODB_URI
    if not MONGODB_URI:
        return
    try:
        import iptv_accounts as _ia
        _ia._get_motor_col()  # ensure the shared motor client is initialised
        if _ia._motor_client is None:
            return
        col = _ia._motor_client["stremio"]["leaf_workers"]
        for wid, wurl in workers.items():
            doc = await col.find_one({"_id": wid})
            if doc:
                for slug in (doc.get("assigned_streams") or {}):
                    _slug_to_worker[slug] = wurl
        logger.info("Master routing map loaded: %d slugs across %d worker(s)",
                    len(_slug_to_worker), len(workers))
    except Exception as exc:
        logger.warning("_load_slug_to_worker failed: %s", exc)


async def _handle_worker_up(request: web.Request) -> web.Response:
    """Worker registration endpoint — re-syncs routing map for that worker."""
    try:
        body = await request.json()
        worker_id = body.get("worker_id", "")
    except Exception:
        return web.Response(status=400, text="Invalid JSON")

    worker_url = _parse_workers().get(worker_id, "")
    if not worker_url:
        logger.warning("worker_up: unknown worker_id %r", worker_id)
        return web.Response(status=200, text=json.dumps({"ok": True}), content_type="application/json")

    from config import MONGODB_URI
    if MONGODB_URI:
        try:
            import iptv_accounts as _ia
            _ia._get_motor_col()  # ensure motor client initialised
            if _ia._motor_client is not None:
                col = _ia._motor_client["stremio"]["leaf_workers"]
                doc = await col.find_one({"_id": worker_id})
                if doc:
                    for slug in (doc.get("assigned_streams") or {}):
                        _slug_to_worker[slug] = worker_url
        except Exception as exc:
            logger.warning("worker_up sync failed: %s", exc)

    logger.info("Worker %s (%s) registered", worker_id, worker_url)
    return web.Response(status=200, text=json.dumps({"ok": True}), content_type="application/json")

# ── Token validation cache ────────────────────────────────────────────────────
_token_cache: dict[str, tuple[bool, float]] = {}  # token → (valid, cache_expires_mono)
_TOKEN_CACHE_TTL = 300.0  # 5 minutes


_verify_client: httpx.AsyncClient | None = None

def _get_verify_client() -> httpx.AsyncClient:
    global _verify_client
    if _verify_client is None or _verify_client.is_closed:
        _verify_client = httpx.AsyncClient(
            verify=False,
            timeout=5,
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=10),
        )
    return _verify_client


async def _validate_token(token: str) -> bool:
    now = time.monotonic()
    cached = _token_cache.get(token)
    if cached and now < cached[1]:
        return cached[0]
    try:
        resp = await _get_verify_client().get(f"{ADDON_API_BASE}/{token}/verify.json")
        valid = resp.status_code == 200 and resp.json().get("active") in (True, 1)
    except Exception:
        valid = False
    _token_cache[token] = (valid, now + _TOKEN_CACHE_TTL)
    return valid


def _client_ip(request: web.Request) -> str:
    """Real client IP behind Cloudflare + nginx.

    leaf.highfly.dev is Cloudflare-proxied, so request.remote / X-Real-IP are the
    CF edge IP. Cloudflare sets CF-Connecting-IP to the true client; nginx passes
    it through. Fall back through X-Forwarded-For (first hop) → X-Real-IP →
    request.remote for non-CF paths."""
    cf = request.headers.get("CF-Connecting-IP")
    if cf:
        return cf.strip()
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.headers.get("X-Real-IP") or (request.remote or "")


async def _handle_hidden_matches(request: web.Request) -> web.Response:
    try:
        from leaf_admin import _hidden_match_ids
        now = time.time()
        active = [mid for mid, exp in _hidden_match_ids.items() if exp > now]
    except Exception:
        active = []
    return web.Response(
        status=200,
        text=json.dumps({"hidden": active}),
        content_type="application/json",
    )


async def _handle_live_registry(request: web.Request) -> web.Response:
    """Expose the live-event registry so main backends WITHOUT MongoDB access
    (the free VPS) can read which events the leaf node is actively recording and
    keep them pinned in Live Now. Premium backends read the Mongo collection
    directly; this endpoint is the HTTP mirror of the same data."""
    try:
        import live_registry
        rows = live_registry.get_all()
    except Exception as exc:
        logger.warning("leaf: live_registry endpoint error: %s", exc)
        rows = []
    return web.Response(
        status=200,
        text=json.dumps({"events": rows}),
        content_type="application/json",
        headers={"Cache-Control": "no-store"},
    )


async def _handle_live_playlist(request: web.Request) -> web.Response:
    slug  = request.match_info["slug"]
    state = leaf_recordings.get(slug)

    if state is None:
        return web.Response(status=404, text="Not found")

    playlist = build_live_playlist(state)
    if playlist is None:
        return web.Response(status=404, text="Buffering - please wait")

    return web.Response(
        status=200,
        body=playlist.encode("utf-8"),
        headers={"Content-Type": "application/vnd.apple.mpegurl"},
    )


async def _handle_metadata(request: web.Request) -> web.Response:
    slug  = request.match_info["slug"]
    state = leaf_recordings.get(slug)

    if state is None:
        return web.Response(status=404, text="Not found")

    good = [s for s in state.segments if s.done and not s.failed]
    meta = {
        "slug":           state.slug,
        "streamed_id":    state.streamed_id,
        "title":          state.title,
        "category":       state.category,
        "status":         "recording",
        "segment_count":  len(good),
        "total_duration": round(sum(s.duration for s in good), 2),
        "buffering":      build_live_playlist(state) is None,
        "started_at":     state.started_at,
    }
    return web.Response(
        status=200,
        text=json.dumps(meta, indent=2),
        content_type="application/json",
    )


async def _handle_health(request: web.Request) -> web.Response:
    active_leaf = list(leaf_recordings.keys())
    from m3u_recorder import active_m3u_tasks as _m3u_tasks
    active_m3u = list(_m3u_tasks.keys())

    return web.Response(
        status=200,
        text=json.dumps({
            "status": "ok",
            "service": "🍃 Leaf Bridge",
            "active_leaf_recordings": active_leaf,
            "active_m3u_recordings": active_m3u,
            "count": len(active_leaf) + len(active_m3u),
        }, indent=2),
        content_type="application/json",
    )


async def _handle_catalog(request: web.Request) -> web.Response:
    """
    GET /leaf/catalog.json

    Returns all ACTIVELY recording streams for injection into the sportsaddon backends.
    Only entries that are currently recording and have a playlist available are included.
    No hardcoded channels, no offline entries, no whitelists.
    """
    entries = []

    try:
        from leaf_admin import _hidden_slugs
    except Exception:
        _hidden_slugs = set()

    # ── 1. SKTech leaf recordings ────────────────────────────────────────
    for slug, state in leaf_recordings.items():
        if slug in _hidden_slugs:
            continue
        buffering = build_live_playlist(state) is None
        entries.append({
            "source":       "leaf",
            "slug":         slug,
            "channel":      slug,
            "title":        state.title,
            "summary":      getattr(state, "summary", ""),
            "description":  getattr(state, "description", ""),
            "image_url":    getattr(state, "image_url", ""),
            "categories":   getattr(state, "categories", []),
            "streamed_id":  state.streamed_id,
            "status":       "recording",
            "buffering":    buffering,
            "playlist_url": f"{LEAF_SERVER_PUBLIC_URL}/leaf/{slug}/live.m3u8",
            "started_at":   state.started_at,
            "stream_info":  getattr(state, "stream_info", None),
        })

    # ── 3. Scheduler + admin M3U recordings ──────────────────────────────────────
    try:
        from leaf_admin import _recording_meta
    except Exception:
        _recording_meta = {}

    try:
        from iptv_scheduler import _scheduler_recordings
    except Exception:
        _scheduler_recordings = {}

    def _build_m3u_entry(slug, meta, started_at=0, buffering=False, stream_info=None):
        title        = meta.get("display_name", slug)
        channel_name = meta.get("channel_name") or ""
        categories   = meta.get("categories") or ["Sports"]
        is_scheduler = meta.get("scheduler", False)
        event_id     = meta.get("event_id", "")
        # If channel_name is empty (cache was cold at recording start), do a live lookup
        if not channel_name:
            try:
                from iptv_accounts import get_channel_name as _gcn
                acc_id = meta.get("account_id", "")
                if acc_id:
                    fetched = _gcn(acc_id, slug)
                    if fetched:
                        channel_name = fetched
                        # Backfill meta so future catalog builds skip the lookup
                        meta["channel_name"] = fetched
            except Exception:
                pass
        if not channel_name:
            channel_name = title
        try:
            import channel_db as db
            ch_db = db.get_channel(slug)
            if ch_db and ch_db.get("label"):
                channel_name = ch_db["label"]
        except Exception:
            pass
        raw_match_id = event_id.split(":", 1)[-1] if ":" in event_id else event_id
        evt_starts   = float(meta.get("event_starts_at", 0))
        entry = {
            "source":          "m3u",
            "slug":            slug,
            "channel":         slug,
            "channel_name":    channel_name,
            "title":           title,
            "summary":         title,
            "description":     "Live IPTV Recording",
            "image_url":       "",
            "categories":      categories,
            "status":          "recording",
            "buffering":       buffering,
            "playlist_url":    f"{LEAF_SERVER_PUBLIC_URL}/m3u/{slug}/live.m3u8",
            "started_at":      started_at,
            "stream_info":     stream_info,
            "is_pre_start":    bool(meta.get("is_pre_start", False)),
            "event_starts_at": evt_starts,
            "starts_in_secs":  max(0, int(evt_starts - time.time())) if evt_starts > 0 else 0,
        }
        if is_scheduler and raw_match_id:
            entry["streamed_id"] = raw_match_id
        else:
            entry["standalone"] = True
        return entry

    if _LEAF_MODE == "master":
        # m3u_recordings is empty on master — recordings live on workers.
        # Build catalog from _recording_meta for every slug assigned to a worker.
        for slug in list(_slug_to_worker.keys()):
            if slug in _hidden_slugs:
                continue
            meta = _recording_meta.get(slug)
            if not meta:
                continue
            started_at = _scheduler_recordings.get(slug, {}).get("started_at", 0) or meta.get("started_at", 0)
            buffering = (started_at > 0 and time.time() - started_at < DELAY_SECONDS + 15) and meta.get("stream_info") is None
            entries.append(_build_m3u_entry(slug, meta, started_at=started_at, buffering=buffering,
                                            stream_info=meta.get("stream_info")))
            # Also add catalog entries for any co-events sharing this channel
            for shared_meta in meta.get("shared_events_meta", []):
                merged = {**meta, **shared_meta}  # overlay shared event's title/id over base meta
                entries.append(_build_m3u_entry(slug, merged, started_at=started_at, buffering=buffering,
                                                stream_info=meta.get("stream_info")))
    else:
        for slug, state in m3u_recordings.items():
            if slug in _hidden_slugs:
                continue
            meta = _recording_meta.get(slug, {})
            entries.append(_build_m3u_entry(
                slug, meta,
                started_at=getattr(state, "started_at", 0),
                buffering=build_m3u_playlist(state) is None,
                stream_info=getattr(state, "stream_info", None),
            ))

    return web.Response(
        status=200,
        text=json.dumps(entries, indent=2, default=str),
        content_type="application/json",
    )




async def _handle_match_playlist(request: web.Request) -> web.Response:
    match_id = request.match_info["match_id"]
    state = None
    for s in leaf_recordings.values():
        if s.streamed_id == match_id:
            state = s
            break

    if state is None:
        return web.Response(status=404, text="No active recording for this match")

    playlist = build_live_playlist(state)
    if playlist is None:
        return web.Response(status=404, text="Buffering - please wait")

    return web.Response(
        status=200,
        body=playlist.encode("utf-8"),
        headers={"Content-Type": "application/vnd.apple.mpegurl"},
    )


async def _handle_m3u_playlist(request: web.Request) -> web.Response:
    slug = request.match_info["slug"]

    # Scheduled-but-not-started recording: it holds no worker yet (starts at its
    # UTC time). Return a clear 425 "Too Early" with the scheduled time instead of
    # the generic "No worker assigned" (which players render as endless buffering).
    if _LEAF_MODE == "master":
        from leaf_admin import _recording_meta as _rm_sched
        _pm = _rm_sched.get(slug) or {}
        if _pm.get("pending_start"):
            _sa = _pm.get("scheduled_start_at")
            import datetime as _dt
            _when = (_dt.datetime.utcfromtimestamp(_sa).strftime("%Y-%m-%d %H:%M UTC")
                     if _sa else "a scheduled time")
            return web.Response(status=425,
                                text=f"Scheduled recording — not started yet. Starts at {_when}.")

    if _LEAF_MODE == "master" and request.method != "HEAD":
        from leaf_admin import _recording_meta
        si = (_recording_meta.get(slug) or {}).get("stream_info") or {}
        if si.get("height", 0) > 1080:
            token = request.rel_url.query.get("token", "")
            if not token or not await _validate_token(token):
                return web.Response(status=403, text="Premium token required")

    # Master mode: proxy to the worker that owns this slug
    if _LEAF_MODE == "master":
        worker_url = _slug_to_worker.get(slug)
        if not worker_url:
            return web.Response(status=404, text="No worker assigned for this stream")

        # HEAD requests bypass cache (used by players to check availability)
        if request.method == "HEAD":
            try:
                client = _get_worker_client()
                resp = await client.head(f"{worker_url}/m3u/{slug}/live.m3u8")
                if resp.status_code == 404:
                    return web.Response(status=404, text="Buffering - please wait")
                return web.Response(status=resp.status_code, body=b"",
                                    headers={"Content-Type": "application/vnd.apple.mpegurl"})
            except Exception as exc:
                logger.warning("proxy HEAD %s failed: %s", slug, exc)
                return web.Response(status=502, text="Worker unavailable")

        # GET: serve from cache if fresh (playlist changes every 6s, 2s cache is safe)
        now = time.monotonic()
        cached = _playlist_cache.get(slug)
        if cached and now < cached[1]:
            return web.Response(status=200, body=cached[0],
                                headers={"Content-Type": "application/vnd.apple.mpegurl"})

        # Single-flight: collapse concurrent cache-misses for the same slug into
        # ONE upstream fetch. The first misser creates the Future and fetches;
        # every other concurrent request for this slug awaits the same Future and
        # shares its (status, body). This turns a ~900-request stampede every 2s
        # (which stalled the event loop for 20-57s) into a single upstream fetch
        # per channel per cache window. There is no await between the cache miss
        # above and this lookup, so the check-or-create is atomic under asyncio.
        inflight = _playlist_inflight.get(slug)
        if inflight is not None:
            status, body = await inflight
            if status == 404:
                return web.Response(status=404, text="Buffering - please wait")
            if status == 502:
                return web.Response(status=502, text="Worker unavailable")
            return web.Response(status=status, body=body,
                                headers={"Content-Type": "application/vnd.apple.mpegurl"})

        fut: "asyncio.Future[tuple[int, bytes]]" = asyncio.get_running_loop().create_future()
        _playlist_inflight[slug] = fut
        status_out, body_out = 502, b""
        try:
            client = _get_worker_client()
            url = f"{worker_url}/m3u/{slug}/live.m3u8"
            try:
                resp = await client.get(url)
            except Exception:
                # Brief retry. With single-flight only ONE coroutine reaches here
                # per slug, so this no longer piles up hundreds of 2s sleeps — a
                # short pause is enough to ride out a momentary worker blip.
                await asyncio.sleep(0.3)
                resp = await client.get(url)
            status_out = resp.status_code
            if resp.status_code == 404:
                _playlist_cache.pop(slug, None)   # clear stale cache on 404
                body_out = b""
                return web.Response(status=404, text="Buffering - please wait")
            if resp.status_code == 200:
                _playlist_cache[slug] = (resp.content, now + _PLAYLIST_CACHE_TTL)
            body_out = resp.content
            return web.Response(status=resp.status_code, body=resp.content,
                                headers={"Content-Type": "application/vnd.apple.mpegurl"})
        except Exception as exc:
            logger.warning("proxy %s → %s failed: %s", slug, worker_url, exc)
            status_out, body_out = 502, b""
            return web.Response(status=502, text="Worker unavailable")
        finally:
            # Always resolve the Future (success or failure) and clear it, or the
            # awaiters would hang and the next window would never start a fetch.
            if not fut.done():
                fut.set_result((status_out, body_out))
            _playlist_inflight.pop(slug, None)

    state = m3u_recordings.get(slug)

    if state is None:
        return web.Response(status=404, text="Not found")

    # Token-protect 1080p+ streams
    si = getattr(state, "stream_info", None) or {}
    if si.get("height", 0) > 1080 and request.method != "HEAD":
        token = request.rel_url.query.get("token", "")
        if not token or not await _validate_token(token):
            return web.Response(status=403, text="Premium token required")

    playlist = build_m3u_playlist(state)
    if playlist is None:
        return web.Response(status=404, text="Buffering - please wait")

    return web.Response(
        status=200,
        body=playlist.encode("utf-8"),
        headers={"Content-Type": "application/vnd.apple.mpegurl"},
    )


async def _handle_rotate_account(request: web.Request) -> web.Response:
    """Worker calls this when a stream gets repeated rc=0/rc=8 drops."""
    try:
        body = await request.json()
        slug        = body.get("slug", "").strip()
        current_url = body.get("current_url", "").strip()
    except Exception:
        return web.Response(status=400, text="Invalid JSON")
    if not slug:
        return web.Response(status=400, text="slug required")

    from iptv_accounts import async_load_accounts, async_mark_free, async_mark_busy, async_claim_free_account
    from iptv_scheduler import _scheduler_recordings
    from leaf_admin import _recording_meta

    async with _rotation_lock:
        # Fresh read from MongoDB under the rotation lock so we never see a
        # stale snapshot that still shows the incoming account as free.
        accounts = await async_load_accounts()
        current_acc_id = next((aid for aid, acc in accounts.items() if acc.get("current_slug") == slug), None)
        # Exclude accounts this slug rotated through recently (incl. the current one)
        # so a flaky channel cycles through DISTINCT accounts instead of ping-ponging
        # between the same 2. If every account is excluded (small pool, long storm),
        # fall back to allowing the current one so rotation never dead-ends.
        _recent = list(_recent_rotation_accts.get(slug, []))
        if current_acc_id and current_acc_id not in _recent:
            _recent.append(current_acc_id)
        # Use the atomic claim helper — it acquires the claim lock internally,
        # reads fresh, and writes to MongoDB before returning. `exclude` skips
        # recently-tried accounts; it self-relaxes inside the helper if the only
        # free accounts are excluded ones.
        new_acc_id, new_acc = await async_claim_free_account(slug, exclude=set(_recent))
        if not new_acc_id:
            return web.Response(status=409, text=json.dumps({"error": "no_free_account"}),
                                content_type="application/json")
        # Record the account we just picked (most-recent last, capped).
        _recent.append(new_acc_id)
        _recent_rotation_accts[slug] = _recent[-_RECENT_ROTATION_KEEP:]
        # Guard against malformed account record — a KeyError would ghost-lock new_acc_id.
        try:
            import re as _re
            _ch_match = _re.search(r'/live/[^/]+/[^/]+/([^.?]+)', current_url)
            channel_id = _ch_match.group(1) if _ch_match else slug
            new_url = f"{new_acc['server'].rstrip('/')}/live/{new_acc['username']}/{new_acc['password']}/{channel_id}.m3u8"
        except Exception as _exc:
            logger.error("rotate_account: malformed account record for %s: %s — releasing claim", new_acc_id, _exc)
            await async_mark_free(new_acc_id)
            return web.Response(status=500, text=json.dumps({"error": "malformed_account"}),
                                content_type="application/json")
        # Release the old account after claiming the new one.
        if current_acc_id and current_acc_id != new_acc_id:
            await async_mark_free(current_acc_id)

    if slug in _scheduler_recordings:
        _scheduler_recordings[slug]["account_id"] = new_acc_id
    if slug in _recording_meta:
        _recording_meta[slug]["account_id"]        = new_acc_id
        _recording_meta[slug]["last_rotated_from"] = current_acc_id or "unknown"
        _recording_meta[slug]["last_rotated_to"]   = new_acc_id
        # Persist the new account_id back to the recording-meta MongoDB doc, else a
        # master restart reloads the pre-rotation account_id and ghost-locks the old one.
        try:
            from leaf_admin import async_save_recording_meta
            await async_save_recording_meta(slug, _recording_meta[slug])
        except Exception as exc:
            # Roll back the in-memory + account state so a restart doesn't see a
            # stale doc and the pool doesn't lose an account.
            logger.error(
                "rotate_account: recording-meta persist failed for '%s' — rolling back to %s: %s",
                slug, current_acc_id or "?", exc,
            )
            if current_acc_id:
                _recording_meta[slug]["account_id"] = current_acc_id
                await async_mark_busy(current_acc_id, slug)  # re-hold old account
                await async_mark_free(new_acc_id)            # release the one we just claimed
                return web.Response(status=500, text=json.dumps({"error": "persist_failed"}),
                                    content_type="application/json")

    worker_url = _slug_to_worker.get(slug)
    if worker_url:
        workers   = _parse_workers()
        worker_id = next((wid for wid, wurl in workers.items() if wurl == worker_url), None)
        if worker_id:
            # Non-blocking motor write to leaf_workers (was blocking pymongo in a thread).
            from iptv_accounts import _get_motor_col  # reuse the motor client factory
            try:
                from motor.motor_asyncio import AsyncIOMotorClient  # noqa: F401 (ensure motor present)
                from config import MONGODB_URI
                if MONGODB_URI:
                    import iptv_accounts as _ia
                    if _ia._motor_client is not None:
                        wk_col = _ia._motor_client["stremio"]["leaf_workers"]
                        await wk_col.update_one({"_id": worker_id},
                                                {"$set": {f"assigned_streams.{slug}.stream_url": new_url,
                                                          f"assigned_streams.{slug}.account_id": new_acc_id}})
            except Exception as exc:
                logger.error(
                    "rotate_account: worker MongoDB update failed — leaf_workers doc for '%s' "
                    "is stale (still points at old account %s); reconcile/restart required: %s",
                    slug, current_acc_id or "?", exc,
                )

    logger.info("rotate_account: %s  %s → %s", slug, current_acc_id or "?", new_acc_id)
    return web.Response(status=200,
                        text=json.dumps({"stream_url": new_url, "account_id": new_acc_id}),
                        content_type="application/json")


async def start_leaf_server() -> web.AppRunner:
    import os
    os.makedirs("media/m3u", exist_ok=True)

    app = web.Application()
    app.router.add_get("/leaf/{slug}/live.m3u8",          _handle_live_playlist)
    app.router.add_get("/leaf/{slug}/metadata.json",      _handle_metadata)
    app.router.add_get("/leaf/match/{match_id}/live.m3u8", _handle_match_playlist)
    app.router.add_get("/m3u/{slug}/live.m3u8",           _handle_m3u_playlist)
    app.router.add_get("/leaf/health",                    _handle_health)
    app.router.add_get("/leaf/catalog.json",              _handle_catalog)
    app.router.add_get("/leaf/hidden_matches.json",       _handle_hidden_matches)
    app.router.add_get("/leaf/live_registry.json",        _handle_live_registry)
    app.router.add_static("/m3u", "media/m3u")

    from leaf_admin import register_admin_routes
    register_admin_routes(app)

    if _LEAF_MODE == "master":
        await _load_slug_to_worker()
        app.router.add_post("/internal/worker_up",      _handle_worker_up)
        app.router.add_post("/internal/rotate_account", _handle_rotate_account)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, LEAF_SERVER_HOST, LEAF_SERVER_PORT, backlog=1024)
    await site.start()
    logger.info("🍃 Leaf server listening on %s:%d  [mode=%s]", LEAF_SERVER_HOST, LEAF_SERVER_PORT, _LEAF_MODE)
    return runner
