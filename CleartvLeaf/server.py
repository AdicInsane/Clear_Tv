"""
server.py - the ClearTV leaf's HTTP surface.

Public (read by the addon and by players):
  GET  /leaf/catalog.json        the channel lineup
  GET  /leaf/health              liveness + counts
  GET  /m3u/{slug}/live.m3u8     a channel's playlist, rewritten
  GET  /seg/{ref}                one segment, proxied

Admin (session-gated, registered from admin.py):
  /admin, /admin/panel, /admin/api/...
"""

from __future__ import annotations

import json
import logging

from aiohttp import web

import accounts
import config
import relay
from channels import store

logger = logging.getLogger("leaf.server")


def _json(payload, status: int = 200, cache: str = "no-store") -> web.Response:
    return web.Response(
        status=status,
        text=json.dumps(payload, indent=2, default=str),
        content_type="application/json",
        headers={"Cache-Control": cache, "Access-Control-Allow-Origin": "*"},
    )


def _catalog_authorised(request: web.Request) -> bool:
    """
    Check the optional catalog bearer token.

    When CATALOG_API_KEY is unset the catalog is public — that is the documented
    default for a local leaf, and config.warnings() says so at startup.
    """
    if not config.CATALOG_API_KEY:
        return True
    header = request.headers.get("Authorization", "")
    if header.startswith("Bearer "):
        return header[7:].strip() == config.CATALOG_API_KEY
    # Also accept ?key= so a browser can be pointed at it while debugging.
    return request.rel_url.query.get("key", "") == config.CATALOG_API_KEY


# ── public endpoints ──────────────────────────────────────────────────────────

async def handle_catalog(request: web.Request) -> web.Response:
    """
    GET /leaf/catalog.json

    Every enabled channel, in the shape the ClearTV addon expects. Only enabled
    channels appear, so anything listed here is meant to be playable right now.
    """
    if not _catalog_authorised(request):
        return _json({"error": "unauthorized"}, status=401)
    return _json(store.catalog_entries())


async def handle_health(request: web.Request) -> web.Response:
    stats = store.stats()
    return _json({
        "status": "ok",
        "service": "ClearTV leaf",
        "channels": stats,
        "accounts": accounts.capacity(),
        "cache": relay.cache_stats(),
    })


async def handle_playlist(request: web.Request) -> web.Response:
    """
    GET /m3u/{slug}/live.m3u8

    Fetches the provider playlist server-side and returns it with every URI
    rewritten to an opaque /seg/ link. No quality gating and no token requirement:
    if a channel is in the lineup, it plays.
    """
    slug = request.match_info["slug"]

    channel = store.get(slug)
    if channel is None:
        return web.Response(status=404, text="Unknown channel")
    if not channel.enabled:
        return web.Response(status=404, text="Channel is stopped")

    account = accounts.get_account(channel.account_id) if channel.account_id else None
    if account is None:
        # Enabled but unclaimed — restore could not get an account, or one was
        # deleted underneath it. Say so plainly rather than serving nothing.
        store.update(slug, last_error="no provider account assigned")
        return web.Response(status=503, text="No provider account assigned to this channel")

    upstream = accounts.stream_url(account, channel.stream_id)

    try:
        playlist, info = await relay.playlist_for(slug, upstream)
    except Exception as exc:
        store.update(slug, last_error=str(exc))
        logger.warning("playlist failed for %s: %s", slug, exc)
        return web.Response(status=502, text=f"Upstream error: {exc}")

    # Record what we learned about the stream so the panel and the addon can show
    # a real quality label. Only write on change to avoid a save per request.
    if info and info != channel.stream_info:
        store.update(slug, stream_info=info)
    if channel.last_error:
        store.update(slug, last_error="")

    if request.method == "HEAD":
        return web.Response(status=200, headers={"Content-Type": "application/vnd.apple.mpegurl"})

    return web.Response(
        status=200,
        body=playlist.encode(),
        headers={
            "Content-Type": "application/vnd.apple.mpegurl",
            "Cache-Control": "no-cache, no-store",
            "Access-Control-Allow-Origin": "*",
        },
    )


async def handle_segment(request: web.Request) -> web.Response:
    """
    GET /seg/{ref}

    Proxies one media segment. `ref` is an opaque id this leaf issued when it
    rewrote the playlist; the provider URL it maps to is held server-side. There
    is no client-supplied URL anywhere in this path, so the endpoint cannot be
    used as an open proxy and the provider credentials never travel.
    """
    token = request.match_info["token"]

    try:
        url = relay.resolve_ref(token)
    except ValueError as exc:
        # 410 rather than 403: the id was either never issued or has aged out,
        # and in both cases the right client behaviour is to re-fetch the
        # playlist for fresh ids.
        return web.Response(status=410, text=str(exc))

    try:
        body, content_type = await relay.segment(url)
    except Exception as exc:
        return web.Response(status=502, text=f"Segment error: {exc}")

    return web.Response(
        status=200,
        body=body,
        headers={
            "Content-Type": content_type,
            "Cache-Control": "public, max-age=30",
            "Access-Control-Allow-Origin": "*",
        },
    )


async def handle_root(request: web.Request) -> web.Response:
    stats = store.stats()
    return web.Response(
        text=f"ClearTV leaf — {stats['enabled']} channel(s) live. /admin to manage.\n",
        content_type="text/plain",
    )


# ── app ───────────────────────────────────────────────────────────────────────

def build_app() -> web.Application:
    import admin

    app = web.Application()

    app.router.add_get("/", handle_root)
    app.router.add_get("/leaf/catalog.json", handle_catalog)
    app.router.add_get("/leaf/health", handle_health)
    # allow_head is on by default, so players can probe availability with HEAD
    # before committing to a GET.
    app.router.add_get("/m3u/{slug}/live.m3u8", handle_playlist)
    app.router.add_get("/seg/{token}", handle_segment)

    admin.register_routes(app)

    return app
