"""
worker_node.py — Leaf Bridge worker node entry point.

Run with LEAF_MODE=worker on a recording-only VPS.

Env vars:
  WORKER_ID          unique name  (default: worker1)
  WORKER_PORT        HTTP port    (default: 8088)
  WORKER_MASTER_URL  master base URL for registration (e.g. http://master:8087)
  MONGODB_URI        shared Atlas URI (same as master)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time

import httpx
from aiohttp import web

from config import MONGODB_URI, RESIDENTIAL_PROXIES
from m3u_recorder import (
    active_m3u_tasks,
    build_m3u_playlist,
    m3u_recordings,
    start_m3u_recording,
    stop_m3u_recording,
    run_r2_cleanup_loop,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s - %(message)s",
)
logger = logging.getLogger("leaf.worker")

WORKER_ID         = os.getenv("WORKER_ID",        "worker1")
WORKER_PORT       = int(os.getenv("WORKER_PORT",   "8088"))
WORKER_MASTER_URL = os.getenv("WORKER_MASTER_URL", "")

# ── MongoDB ────────────────────────────────────────────────────────────────────

_workers_col = None

def _get_workers_col():
    global _workers_col
    if _workers_col is not None:
        return _workers_col
    if not MONGODB_URI:
        return None
    try:
        from pymongo import MongoClient
        _workers_col = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=3000, socketTimeoutMS=5000, connectTimeoutMS=5000)["stremio"]["leaf_workers"]
        return _workers_col
    except Exception as exc:
        logger.warning("MongoDB unavailable: %s", exc)
        return None


def _load_assigned() -> dict:
    col = _get_workers_col()
    if col is None:
        return {}
    try:
        doc = col.find_one({"_id": WORKER_ID})
        return (doc or {}).get("assigned_streams", {})
    except Exception as exc:
        logger.warning("load assigned_streams failed: %s", exc)
        return {}


def _slugs_still_wanted() -> set[str] | None:
    """Return the set of slugs the master still considers ACTIVE, from the
    authoritative leaf_iptv_accounts collection (an account's current_slug points
    at the stream it's recording). When a stream genuinely ends, the master frees
    the account (current_slug -> None), so a stale/ended slug has NO account and
    must NOT be resurrected on restart.

    assigned_streams is a persistent restart-recovery list that is only pruned on
    an explicit /stop — it accumulates dead entries (ended events, moved streams)
    and ballooned to 100+ slugs, so blindly re-execing all of it on boot spawns
    dozens of dead ffmpeg reconnect-loops that saturate the CPU. Cross-checking
    against live account holdings fixes that at the root.

    Returns None if MongoDB is unreachable — caller then falls back to recovering
    everything (old behaviour) rather than recovering nothing.
    """
    if not MONGODB_URI:
        return None
    try:
        from pymongo import MongoClient
        db = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=3000,
                         socketTimeoutMS=5000, connectTimeoutMS=5000)["stremio"]
        doc = db["leaf_iptv_accounts"].find_one({"_id": "accounts"})
        accts = (doc or {}).get("accounts", {})
        wanted = {
            a.get("current_slug")
            for a in accts.values()
            if a.get("current_slug") and not str(a.get("current_slug")).startswith("__probe__")
        }
        return wanted
    except Exception as exc:
        logger.warning("[worker/%s] could not load account holdings: %s — recovering all", WORKER_ID, exc)
        return None


def _save_assigned(assigned: dict) -> None:
    col = _get_workers_col()
    if col is None:
        return
    try:
        col.update_one(
            {"_id": WORKER_ID},
            {"$set": {"assigned_streams": assigned, "last_heartbeat": int(time.time())}},
            upsert=True,
        )
    except Exception as exc:
        logger.warning("save assigned_streams failed: %s", exc)


def _save_assigned_slug(slug: str, entry: dict) -> None:
    """Persist ONE slug's assigned-stream entry with field-scoped $set, so we never
    clobber fields written by the master (notably `account_id`, which the worker is
    never told and must not overwrite) or other slugs' entries.

    The old whole-dict `$set: {"assigned_streams": <dict>}` replaced the entire
    sub-document from the worker's own view — which has no `account_id` — wiping the
    master's slug→account mapping for EVERY slug on this worker. Reconcile then
    couldn't map the slug to its account, so a live account showed FREE, the probe
    grabbed it, and the provider returned HTTP 458 (connection-limit). Writing only
    the fields the worker actually owns (dotted paths) leaves the master's
    account_id intact and never touches sibling slugs.
    """
    col = _get_workers_col()
    if col is None:
        return
    try:
        col.update_one(
            {"_id": WORKER_ID},
            {"$set": {
                f"assigned_streams.{slug}.stream_url":  entry.get("stream_url", ""),
                f"assigned_streams.{slug}.headers":     entry.get("headers", {}),
                f"assigned_streams.{slug}.proxies":     entry.get("proxies", []),
                f"assigned_streams.{slug}.assigned_at": entry.get("assigned_at", int(time.time())),
                # Persist the standalone flag so worker-restart recovery re-launches
                # admin 24x7 channels with the never-give-up behavior. Without this,
                # a recovered 24x7 stream loses standalone=True and the zombie guard
                # would kill it forever on the next provider blip.
                f"assigned_streams.{slug}.standalone":  bool(entry.get("standalone", False)),
                "last_heartbeat": int(time.time()),
            }},
            upsert=True,
        )
    except Exception as exc:
        logger.warning("save assigned_stream slug %s failed: %s", slug, exc)


def _touch_heartbeat() -> None:
    col = _get_workers_col()
    if col is None:
        return
    try:
        col.update_one(
            {"_id": WORKER_ID},
            {"$set": {"last_heartbeat": int(time.time())}},
            upsert=True,
        )
    except Exception:
        pass


# ── Startup recovery ───────────────────────────────────────────────────────────

async def _startup_recovery() -> None:
    assigned = _load_assigned()
    if not assigned:
        logger.info("[worker/%s] No assigned streams to recover", WORKER_ID)
        await _notify_master_up()
        return

    # Only recover streams the master STILL wants (an account is holding the slug).
    # assigned_streams accumulates dead entries (ended events, moved streams) because
    # it's only pruned on explicit /stop — blindly re-execing all of it spawns dozens
    # of dead ffmpeg reconnect-loops that saturate the CPU and make /internal/status
    # time out. Cross-check against the authoritative live account holdings.
    wanted = _slugs_still_wanted()
    if wanted is None:
        # MongoDB unreachable — fall back to recovering everything (old behaviour)
        # rather than recovering nothing and going dark.
        logger.warning("[worker/%s] account holdings unavailable — recovering ALL %d assigned",
                       WORKER_ID, len(assigned))
        to_recover = dict(assigned)
        stale: dict = {}
    else:
        to_recover = {s: i for s, i in assigned.items() if s in wanted}
        stale      = {s: i for s, i in assigned.items() if s not in wanted}
        logger.info("[worker/%s] recovering %d live stream(s), pruning %d stale entr(ies)",
                    WORKER_ID, len(to_recover), len(stale))

    # Prune stale entries from our own doc so they never resurrect again.
    if stale:
        try:
            col = _get_workers_col()
            if col is not None:
                col.update_one(
                    {"_id": WORKER_ID},
                    {"$unset": {f"assigned_streams.{s}": 1 for s in stale}},
                )
                logger.info("[worker/%s] pruned stale assigned_streams: %s",
                            WORKER_ID, sorted(stale.keys()))
        except Exception as exc:
            logger.warning("[worker/%s] stale prune failed: %s", WORKER_ID, exc)

    for slug, info in to_recover.items():
        stream_url = info.get("stream_url", "")
        headers    = info.get("headers", {})
        proxies    = info.get("proxies") or RESIDENTIAL_PROXIES
        standalone = bool(info.get("standalone", False))
        if stream_url:
            start_m3u_recording(slug, stream_url, headers, proxies, standalone)
            logger.info("[worker/%s] Recovered: %s (standalone=%s)", WORKER_ID, slug, standalone)
            await asyncio.sleep(3)  # stagger startup to avoid R2 upload storm
        else:
            logger.warning("[worker/%s] No stream_url for %s, skipping", WORKER_ID, slug)

    await _notify_master_up()


async def _notify_master_up() -> None:
    if not WORKER_MASTER_URL:
        return
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            await client.post(
                f"{WORKER_MASTER_URL}/internal/worker_up",
                json={"worker_id": WORKER_ID},
            )
        logger.info("[worker/%s] Registered with master %s", WORKER_ID, WORKER_MASTER_URL)
    except Exception as exc:
        logger.warning("[worker/%s] Could not reach master: %s", WORKER_ID, exc)


# ── Internal API ───────────────────────────────────────────────────────────────

async def _handle_start(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:
        return web.Response(status=400, text="Invalid JSON")

    slug       = body.get("slug", "")
    stream_url = body.get("stream_url", "")
    headers    = body.get("headers", {})
    proxies    = body.get("proxies") or RESIDENTIAL_PROXIES
    standalone = bool(body.get("standalone", False))

    if not slug or not stream_url:
        return web.Response(status=400, text="slug and stream_url required")

    start_m3u_recording(slug, stream_url, headers, proxies, standalone)

    # Field-scoped write — never clobbers the master's account_id or sibling slugs.
    _save_assigned_slug(slug, {
        "stream_url":  stream_url,
        "headers":     headers,
        "proxies":     proxies,
        "assigned_at": int(time.time()),
        "standalone":  standalone,
    })

    logger.info("[worker/%s] Started: %s", WORKER_ID, slug)
    return web.Response(status=200, text=json.dumps({"ok": True}), content_type="application/json")


async def _handle_stop(request: web.Request) -> web.Response:
    slug = request.match_info["slug"]
    stop_m3u_recording(slug)

    assigned = _load_assigned()
    if slug in assigned:
        del assigned[slug]
        _save_assigned(assigned)

    logger.info("[worker/%s] Stopped: %s", WORKER_ID, slug)
    return web.Response(status=200, text=json.dumps({"ok": True}), content_type="application/json")


async def _handle_status(request: web.Request) -> web.Response:
    status = {
        slug: {
            "session_id":  state.session_id,
            "segments":    len(state.segments),
            "running":     slug in active_m3u_tasks,
            "started_at":  state.started_at,
            "stream_info": state.stream_info,
            "stream_url":  state.stream_url,   # current URL (updated on rotation) —
                                               # was omitted, so master always saw it MISSING
        }
        for slug, state in m3u_recordings.items()
    }
    return web.Response(status=200, text=json.dumps(status), content_type="application/json")


# ── Client-facing playlist ─────────────────────────────────────────────────────

async def _handle_m3u_playlist(request: web.Request) -> web.Response:
    slug  = request.match_info["slug"]
    state = m3u_recordings.get(slug)
    if state is None:
        return web.Response(status=404, text="Not found")
    playlist = build_m3u_playlist(state)
    if playlist is None:
        return web.Response(status=404, text="Buffering - please wait")
    return web.Response(
        status=200,
        body=playlist.encode("utf-8"),
        headers={"Content-Type": "application/vnd.apple.mpegurl"},
    )


# ── Heartbeat loop ─────────────────────────────────────────────────────────────

async def _heartbeat_loop() -> None:
    while True:
        await asyncio.sleep(30)
        _touch_heartbeat()


# ── Main ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    app = web.Application()
    app.router.add_post(  "/internal/start",       _handle_start)
    app.router.add_delete("/internal/stop/{slug}", _handle_stop)
    app.router.add_get(   "/internal/status",      _handle_status)
    app.router.add_get(   "/m3u/{slug}/live.m3u8", _handle_m3u_playlist)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", WORKER_PORT, backlog=1024)
    await site.start()
    logger.info("Leaf worker %s listening on port %d", WORKER_ID, WORKER_PORT)

    asyncio.create_task(_heartbeat_loop())
    asyncio.create_task(run_r2_cleanup_loop())
    await _startup_recovery()

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
