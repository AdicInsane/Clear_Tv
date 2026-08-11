"""
admin.py - the ClearTV leaf admin panel.

Two things only: the channels you have added, and the ability to stop them.
Everything else the old leaf panel did (scheduling, event matching, worker
rebalancing, hidden matches, YouTube) is deliberately absent.

Routes:
  GET    /admin                          login page (or redirect to panel)
  POST   /admin/login                    authenticate
  GET    /admin/logout                   drop the session
  GET    /admin/panel                    the panel

  GET    /admin/api/accounts             list provider accounts
  POST   /admin/api/accounts             add / update an account
  DELETE /admin/api/accounts/{id}        remove an account
  GET    /admin/api/channels             browse a provider's channel list
  GET    /admin/api/lineup               what is in the lineup right now
  POST   /admin/api/lineup               add a channel to the lineup
  POST   /admin/api/lineup/{slug}/stop   stop a channel, release its account
  POST   /admin/api/lineup/{slug}/start  start a stopped channel again
  DELETE /admin/api/lineup/{slug}        remove a channel entirely
  PATCH  /admin/api/lineup/{slug}        rename / recategorise
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
from typing import Optional

from aiohttp import web

import accounts
import config
import recorder
from channels import Channel, store

logger = logging.getLogger("leaf.admin")

# ── auth ──────────────────────────────────────────────────────────────────────

_sessions: dict[str, float] = {}          # token -> created_at
_login_attempts: dict[str, list[float]] = {}   # ip -> recent failure times

_COOKIE = "cleartv_admin"


def _new_session() -> str:
    token = secrets.token_hex(32)
    _sessions[token] = time.time()
    # Opportunistic prune so the dict cannot grow forever.
    cutoff = time.time() - config.SESSION_TTL
    for stale in [t for t, created in _sessions.items() if created < cutoff]:
        _sessions.pop(stale, None)
    return token


def _valid_session(token: Optional[str]) -> bool:
    if not token:
        return False
    created = _sessions.get(token)
    if created is None:
        return False
    if time.time() - created > config.SESSION_TTL:
        _sessions.pop(token, None)
        return False
    return True


def _client_ip(request: web.Request) -> str:
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.headers.get("X-Real-IP") or (request.remote or "unknown")


def _throttled(ip: str) -> bool:
    """True when this IP has failed too many logins recently."""
    now = time.time()
    tries = [t for t in _login_attempts.get(ip, []) if now - t < config.LOGIN_WINDOW]
    _login_attempts[ip] = tries
    return len(tries) >= config.LOGIN_MAX_ATTEMPTS


def _record_failure(ip: str) -> None:
    _login_attempts.setdefault(ip, []).append(time.time())


def require_auth(handler):
    """Gate a handler behind a valid session."""

    async def wrapper(request: web.Request) -> web.Response:
        if not _valid_session(request.cookies.get(_COOKIE)):
            if request.path.startswith("/admin/api"):
                return _json({"error": "unauthorized"}, status=401)
            raise web.HTTPFound("/admin")
        return await handler(request)

    wrapper.__name__ = getattr(handler, "__name__", "wrapped")
    return wrapper


def _json(payload, status: int = 200) -> web.Response:
    return web.Response(
        status=status,
        text=json.dumps(payload, default=str),
        content_type="application/json",
        headers={"Cache-Control": "no-store"},
    )


async def _body(request: web.Request) -> dict:
    try:
        data = await request.json()
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


# ── login / panel ─────────────────────────────────────────────────────────────

async def handle_root(request: web.Request) -> web.Response:
    import panel

    if _valid_session(request.cookies.get(_COOKIE)):
        raise web.HTTPFound("/admin/panel")
    return web.Response(text=panel.login_html(), content_type="text/html")


async def handle_login(request: web.Request) -> web.Response:
    ip = _client_ip(request)

    if _throttled(ip):
        logger.warning("login throttled for %s", ip)
        return _json({"error": "Too many attempts. Wait a few minutes."}, status=429)

    data = await _body(request)
    if not data:
        post = await request.post()
        data = {"password": post.get("password", "")}

    supplied = str(data.get("password", ""))

    # Constant-time compare so a wrong password cannot be found by timing.
    if not secrets.compare_digest(supplied, config.ADMIN_PASSWORD):
        _record_failure(ip)
        logger.warning("failed admin login from %s", ip)
        return _json({"error": "Wrong password"}, status=401)

    _login_attempts.pop(ip, None)
    token = _new_session()
    logger.info("admin logged in from %s", ip)

    resp = _json({"ok": True})
    resp.set_cookie(
        _COOKIE,
        token,
        max_age=config.SESSION_TTL,
        httponly=True,
        samesite="Strict",
        secure=config.COOKIE_SECURE,
        path="/",
    )
    return resp


async def handle_logout(request: web.Request) -> web.Response:
    _sessions.pop(request.cookies.get(_COOKIE, ""), None)
    resp = web.HTTPFound("/admin")
    resp.del_cookie(_COOKIE, path="/")
    return resp


@require_auth
async def handle_panel(request: web.Request) -> web.Response:
    import panel

    return web.Response(text=panel.panel_html(), content_type="text/html")


# ── accounts ──────────────────────────────────────────────────────────────────

@require_auth
async def handle_list_accounts(request: web.Request) -> web.Response:
    return _json({"accounts": accounts.list_accounts(), "capacity": accounts.capacity()})


@require_auth
async def handle_add_account(request: web.Request) -> web.Response:
    data = await _body(request)

    name = str(data.get("name", "")).strip()
    server = str(data.get("server", "")).strip()
    username = str(data.get("username", "")).strip()
    password = str(data.get("password", "")).strip()

    if not (name and server and username and password):
        return _json({"error": "name, server, username and password are all required"}, status=400)
    if not server.startswith(("http://", "https://")):
        return _json({"error": "server must start with http:// or https://"}, status=400)

    # Check the credentials BEFORE writing anything. Re-adding an existing
    # server+username updates that record, so persisting first would let a
    # mistyped password overwrite a working one and take its channels down with
    # it. The probe stores nothing.
    loop = asyncio.get_running_loop()
    info = await loop.run_in_executor(None, accounts.probe_credentials, server, username, password)

    if info.get("auth_failed"):
        return _json({"error": "the provider rejected those credentials — nothing was saved"}, status=400)

    # An unreachable provider still gets saved: a dead server is indistinguishable
    # from a bad password from here, and the response says the check did not
    # complete so the panel can show it.
    acc_id = accounts.add_account(name, server, username, password, info)

    return _json({
        "ok": True,
        "id": acc_id,
        "info": info,
        "unverified": bool(info.get("error")),
    })


@require_auth
async def handle_delete_account(request: web.Request) -> web.Response:
    acc_id = request.match_info["id"]

    # Refuse while it is serving something, otherwise that channel keeps
    # streaming with no way to trace which account it belongs to.
    in_use = [c.slug for c in store.enabled() if c.account_id == acc_id]
    if in_use:
        return _json(
            {"error": f"account is serving {', '.join(in_use)} — stop those channels first"},
            status=409,
        )

    if not accounts.remove_account(acc_id):
        return _json({"error": "not found"}, status=404)
    return _json({"ok": True})


# ── provider channel browser ──────────────────────────────────────────────────

@require_auth
async def handle_browse_channels(request: web.Request) -> web.Response:
    """
    GET /admin/api/channels?account_id=…&search=…&refresh=1

    Reads the provider's channel list (thousands of entries), cached per account.
    The blocking fetch runs in an executor so the event loop keeps serving
    playlists while the panel is being browsed.
    """
    account_id = request.rel_url.query.get("account_id", "").strip()
    search = request.rel_url.query.get("search", "").strip()
    refresh = request.rel_url.query.get("refresh", "") == "1"

    if not account_id:
        return _json({"error": "account_id required", "channels": []}, status=400)

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, accounts.fetch_channels, account_id, search, refresh)

    # Flag anything already in the lineup so the panel can grey it out instead of
    # letting an admin add the same channel twice on two different accounts.
    taken = {c.stream_id for c in store.all()}
    for ch in result.get("channels", []):
        ch["in_lineup"] = ch["stream_id"] in taken

    return _json(result)


# ── lineup ────────────────────────────────────────────────────────────────────

def _channel_json(ch: Channel) -> dict:
    acc = accounts.get_account(ch.account_id) if ch.account_id else None
    return {
        "slug": ch.slug,
        "display_name": ch.display_name,
        "stream_id": ch.stream_id,
        "account_id": ch.account_id,
        "account_name": (acc or {}).get("name", "") if acc else "",
        "categories": list(ch.categories),
        "logo": ch.logo,
        "enabled": ch.enabled,
        "started_at": ch.started_at or None,
        "uptime": int(time.time() - ch.started_at) if ch.enabled and ch.started_at else None,
        "quality": f"{ch.stream_info.get('height')}p" if ch.stream_info.get("height") else "",
        "stream_info": ch.stream_info or None,
        "last_error": ch.last_error or None,
        "playlist_url": ch.playlist_url(),
    }


@require_auth
async def handle_lineup(request: web.Request) -> web.Response:
    return _json({
        "channels": [_channel_json(c) for c in store.all()],
        "stats": store.stats(),
        "capacity": accounts.capacity(),
        "recordings": len(recorder.recordings),
    })


async def _verify_stream(account_id: str, stream_id: str) -> tuple[bool, str]:
    """
    Quick reachability check before committing a channel to the lineup.

    Checks the SAME .ts URL the recorder will hand to ffmpeg, not the .m3u8 —
    verifying a different URL than the one that gets recorded proves nothing.

    Two things matter here:
      * redirects must be followed. Xtream panels routinely 302 from the catalog
        URL to the edge that actually serves the stream, so treating a 302 as a
        failure rejects perfectly good channels.
      * the response must be streamed and closed after the first chunk. A live
        .ts URL never ends, so a plain GET would download until the timeout.
    """
    import httpx as _hx

    acc = accounts.get_account(account_id)
    if acc is None:
        return False, "account disappeared"

    url = recorder.to_ts_url(accounts.stream_url(acc, stream_id))

    try:
        async with _hx.AsyncClient(
            verify=config.UPSTREAM_VERIFY_TLS,
            timeout=10.0,
            follow_redirects=True,
        ) as cl:
            async with cl.stream(
                "GET", url, headers={"User-Agent": config.UPSTREAM_USER_AGENT}
            ) as resp:
                if resp.status_code not in (200, 206):
                    return False, f"provider returned HTTP {resp.status_code}"
                # Read one chunk only. A 200 with an empty body is a dead channel
                # that the status code alone would pass.
                async for chunk in resp.aiter_bytes():
                    if chunk:
                        return True, ""
                    break
                return False, "provider sent no data"
    except Exception as exc:
        return False, str(exc)


@require_auth
async def handle_add_channel(request: web.Request) -> web.Response:
    """
    POST /admin/api/lineup

    Body: {stream_id, display_name, account_id?, categories?, logo?, verify?}

    Claims a provider account, verifies the stream actually plays, then puts the
    channel in the lineup where the addon will pick it up on its next refresh.
    """
    data = await _body(request)

    stream_id = str(data.get("stream_id", "")).strip()
    display_name = str(data.get("display_name", "")).strip()
    preferred = str(data.get("account_id", "")).strip()
    logo = str(data.get("logo", "")).strip()
    verify = data.get("verify", True) is not False

    raw_cats = data.get("categories")
    if isinstance(raw_cats, str):
        raw_cats = [raw_cats]
    categories = [str(c).strip() for c in (raw_cats or []) if str(c).strip()] or ["General"]

    if not stream_id:
        return _json({"error": "stream_id required"}, status=400)
    if not display_name:
        display_name = accounts.channel_name(preferred, stream_id) or f"Channel {stream_id}"

    slug = str(data.get("slug", "")).strip() or store.unique_slug(display_name)
    if store.exists(slug):
        return _json({"error": f"'{slug}' is already in the lineup"}, status=409)

    account_id, acc = await accounts.claim(slug, preferred)
    if not account_id:
        if preferred:
            return _json({"error": "that account is busy, expired or unknown"}, status=409)
        cap = accounts.capacity()
        return _json(
            {"error": f"no free account available ({cap['busy']}/{cap['usable']} in use)"},
            status=409,
        )

    stream_info: dict = {}
    if verify:
        ok, err = await _verify_stream(account_id, stream_id)
        if not ok:
            await accounts.release(account_id)
            return _json({"error": f"stream check failed: {err}"}, status=502)

    acc_full = accounts.get_account(account_id)
    up_url = accounts.stream_url(acc_full, stream_id) if acc_full else ""

    channel = await store.add(Channel(
        slug=slug,
        display_name=display_name,
        stream_id=stream_id,
        account_id=account_id,
        categories=categories,
        logo=logo,
        enabled=True,
        stream_info=stream_info,
    ))

    # Start the recorder immediately.
    recorder.start_recording(slug, up_url, display_name)

    logger.info("channel live: %s → account %s (stream_id=%s)", slug, account_id, stream_id)
    return _json({"ok": True, "channel": _channel_json(channel)})


@require_auth
async def handle_stop_channel(request: web.Request) -> web.Response:
    """
    POST /admin/api/lineup/{slug}/stop

    The stopper. Drops the channel out of the catalog and releases its provider
    account so something else can use it. The record stays, so starting it again
    is one click and does not need re-picking from the provider list.
    """
    slug = request.match_info["slug"]

    ch = store.get(slug)
    if ch is None:
        return _json({"error": "not found"}, status=404)
    if not ch.enabled:
        return _json({"ok": True, "already_stopped": True, "channel": _channel_json(ch)})

    account_id = ch.account_id

    await store.set_enabled(slug, False)
    recorder.stop_recording(slug)

    if account_id:
        await accounts.release(account_id)
    await accounts.release_slug(slug)

    logger.info("channel stopped: %s (account %s freed)", slug, account_id or "?")
    return _json({"ok": True, "channel": _channel_json(store.get(slug))})


@require_auth
async def handle_start_channel(request: web.Request) -> web.Response:
    """POST /admin/api/lineup/{slug}/start — bring a stopped channel back."""
    slug = request.match_info["slug"]

    ch = store.get(slug)
    if ch is None:
        return _json({"error": "not found"}, status=404)
    if ch.enabled:
        return _json({"ok": True, "already_running": True, "channel": _channel_json(ch)})

    data = await _body(request)
    preferred = str(data.get("account_id", "")).strip()

    account_id, _acc = await accounts.claim(slug, preferred)
    if not account_id:
        cap = accounts.capacity()
        return _json(
            {"error": f"no free account available ({cap['busy']}/{cap['usable']} in use)"},
            status=409,
        )

    ok, err = await _verify_stream(account_id, ch.stream_id)
    if not ok:
        await accounts.release(account_id)
        await store.update(slug, last_error=err)
        return _json({"error": f"stream check failed: {err}"}, status=502)

    acc_full = accounts.get_account(account_id)
    up_url = accounts.stream_url(acc_full, ch.stream_id) if acc_full else ""

    await store.set_enabled(slug, True)
    await store.update(slug, account_id=account_id, last_error="")
    recorder.start_recording(slug, up_url, ch.display_name)

    logger.info("channel restarted: %s → account %s", slug, account_id)
    return _json({"ok": True, "channel": _channel_json(store.get(slug))})


@require_auth
async def handle_remove_channel(request: web.Request) -> web.Response:
    """DELETE /admin/api/lineup/{slug} — stop it if running, then forget it."""
    slug = request.match_info["slug"]

    ch = store.get(slug)
    if ch is None:
        return _json({"error": "not found"}, status=404)

    account_id = ch.account_id
    await store.remove(slug)
    recorder.stop_recording(slug)

    if account_id:
        await accounts.release(account_id)
    await accounts.release_slug(slug)

    logger.info("channel removed: %s", slug)
    return _json({"ok": True})


@require_auth
async def handle_patch_channel(request: web.Request) -> web.Response:
    """PATCH /admin/api/lineup/{slug} — rename or recategorise."""
    slug = request.match_info["slug"]

    if store.get(slug) is None:
        return _json({"error": "not found"}, status=404)

    data = await _body(request)
    fields: dict = {}

    if "display_name" in data:
        name = str(data["display_name"]).strip()
        if not name:
            return _json({"error": "display_name cannot be empty"}, status=400)
        fields["display_name"] = name

    if "categories" in data:
        raw = data["categories"]
        if isinstance(raw, str):
            raw = [raw]
        cats = [str(c).strip() for c in (raw or []) if str(c).strip()]
        fields["categories"] = cats or ["General"]

    if "logo" in data:
        fields["logo"] = str(data["logo"]).strip()

    if not fields:
        return _json({"error": "nothing to update"}, status=400)

    ch = await store.update(slug, **fields)
    return _json({"ok": True, "channel": _channel_json(ch)})


# ── startup restore ───────────────────────────────────────────────────────────

async def restore() -> None:
    """
    Re-claim provider accounts for channels that were live before a restart.

    accounts.reset_claims() has already cleared every claim from the last run
    (nothing is streaming yet at this point), so this rebuilds the claims from
    the lineup itself — the file is the source of truth, not the stale claims.

    A channel whose account can no longer be claimed is left enabled but marked
    with an error, so it shows up in the panel as needing attention instead of
    silently disappearing from the catalog.
    """
    store.load()
    enabled = store.enabled()
    if not enabled:
        return

    restored = 0
    for ch in enabled:
        account_id, _acc = await accounts.claim(ch.slug, ch.account_id)
        if not account_id:
            # Try any free account before giving up — the original may have been
            # deleted or expired while the leaf was down.
            account_id, _acc = await accounts.claim(ch.slug)

        if not account_id:
            await store.update(ch.slug, last_error="no account available at startup")
            logger.warning("channel %s: could not re-claim an account", ch.slug)
            continue

        await store.update(ch.slug, account_id=account_id, last_error="")
        # Re-start the recorder for this channel.
        acc_full = accounts.get_account(account_id)
        up_url = accounts.stream_url(acc_full, ch.stream_id) if acc_full else ""
        if up_url:
            recorder.start_recording(ch.slug, up_url, ch.display_name)
        restored += 1

    logger.info("restored %d/%d live channel(s)", restored, len(enabled))


# ── routes ────────────────────────────────────────────────────────────────────

def register_routes(app: web.Application) -> None:
    app.router.add_get("/admin", handle_root)
    app.router.add_post("/admin/login", handle_login)
    app.router.add_get("/admin/logout", handle_logout)
    app.router.add_get("/admin/panel", handle_panel)

    app.router.add_get("/admin/api/accounts", handle_list_accounts)
    app.router.add_post("/admin/api/accounts", handle_add_account)
    app.router.add_delete("/admin/api/accounts/{id}", handle_delete_account)

    app.router.add_get("/admin/api/channels", handle_browse_channels)

    app.router.add_get("/admin/api/lineup", handle_lineup)
    app.router.add_post("/admin/api/lineup", handle_add_channel)
    app.router.add_post("/admin/api/lineup/{slug}/stop", handle_stop_channel)
    app.router.add_post("/admin/api/lineup/{slug}/start", handle_start_channel)
    app.router.add_delete("/admin/api/lineup/{slug}", handle_remove_channel)
    app.router.add_route("PATCH", "/admin/api/lineup/{slug}", handle_patch_channel)
