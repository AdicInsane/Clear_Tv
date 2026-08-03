"""
admin_panel.py - Password-protected admin panel for Leaf Bridge.

Routes added to the aiohttp app:
  GET  /admin                  → login page (if not authed) or dashboard
  POST /admin/login            → authenticate
  GET  /admin/logout           → clear session
  GET  /admin/accounts         → list IPTV accounts
  POST /admin/accounts/add     → add account
  POST /admin/accounts/delete  → delete account
  POST /admin/accounts/test    → test account connection
  GET  /admin/channels         → list channels
  POST /admin/channels/add     → add/update channel
  POST /admin/channels/delete  → delete channel
  POST /admin/channels/toggle  → enable/disable channel
  GET  /admin/status           → live recording status (JSON)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import uuid

from aiohttp import web

import channel_db as db

logger = logging.getLogger("leaf.admin")

# ── Auth ──────────────────────────────────────────────────────────────────────
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "highfly2026")
_sessions: dict[str, float] = {}   # token → expires_at
_SESSION_TTL = 3600 * 8            # 8 hours


def _make_token() -> str:
    return hashlib.sha256(os.urandom(32)).hexdigest()


def _check_session(request: web.Request) -> bool:
    token = request.cookies.get("leaf_admin")
    if not token:
        return False
    exp = _sessions.get(token, 0)
    if time.time() > exp:
        _sessions.pop(token, None)
        return False
    return True


def _require_auth(handler):
    async def wrapper(request: web.Request):
        if not _check_session(request):
            raise web.HTTPFound("/admin")
        return await handler(request)
    return wrapper


# ── HTML helpers ──────────────────────────────────────────────────────────────

def _page(title: str, body: str, flash: str = "") -> web.Response:
    flash_html = f'<div class="flash">{flash}</div>' if flash else ""
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} — Leaf Admin</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f1117;color:#e2e8f0;min-height:100vh}}
  nav{{background:#1a1d2e;padding:12px 24px;display:flex;align-items:center;gap:16px;border-bottom:1px solid #2d3148}}
  nav a{{color:#7c85f5;text-decoration:none;font-size:14px;font-weight:500}}
  nav a:hover{{color:#a5b4fc}}
  nav .brand{{color:#fff;font-weight:700;font-size:16px;margin-right:auto}}
  .container{{max-width:1100px;margin:0 auto;padding:24px}}
  h1{{font-size:22px;font-weight:700;margin-bottom:20px;color:#fff}}
  h2{{font-size:16px;font-weight:600;margin-bottom:12px;color:#a5b4fc}}
  .flash{{background:#1e3a5f;border:1px solid #3b82f6;color:#93c5fd;padding:10px 16px;border-radius:8px;margin-bottom:16px;font-size:14px}}
  .flash.err{{background:#3b1f1f;border-color:#ef4444;color:#fca5a5}}
  .card{{background:#1a1d2e;border:1px solid #2d3148;border-radius:12px;padding:20px;margin-bottom:20px}}
  table{{width:100%;border-collapse:collapse;font-size:13px}}
  th{{text-align:left;padding:8px 12px;color:#94a3b8;font-weight:500;border-bottom:1px solid #2d3148}}
  td{{padding:8px 12px;border-bottom:1px solid #1e2235;vertical-align:middle}}
  tr:last-child td{{border-bottom:none}}
  .badge{{display:inline-block;padding:2px 8px;border-radius:99px;font-size:11px;font-weight:600}}
  .badge.on{{background:#14532d;color:#86efac}}
  .badge.off{{background:#3b1f1f;color:#fca5a5}}
  .badge.live{{background:#1e3a5f;color:#93c5fd}}
  .badge.buf{{background:#3b2f0a;color:#fcd34d}}
  input,select{{background:#0f1117;border:1px solid #2d3148;color:#e2e8f0;padding:8px 12px;border-radius:6px;font-size:13px;width:100%}}
  input:focus,select:focus{{outline:none;border-color:#7c85f5}}
  .btn{{display:inline-block;padding:8px 16px;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;border:none;text-decoration:none}}
  .btn-primary{{background:#4f46e5;color:#fff}}
  .btn-primary:hover{{background:#4338ca}}
  .btn-danger{{background:#dc2626;color:#fff}}
  .btn-danger:hover{{background:#b91c1c}}
  .btn-sm{{padding:4px 10px;font-size:12px}}
  .btn-ghost{{background:transparent;border:1px solid #2d3148;color:#94a3b8}}
  .btn-ghost:hover{{border-color:#7c85f5;color:#a5b4fc}}
  .grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px}}
  .form-row{{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:12px;margin-bottom:12px}}
  label{{font-size:12px;color:#94a3b8;display:block;margin-bottom:4px}}
  .status-dot{{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:6px}}
  .dot-green{{background:#22c55e}}
  .dot-red{{background:#ef4444}}
  .dot-yellow{{background:#f59e0b}}
  .login-wrap{{display:flex;align-items:center;justify-content:center;min-height:100vh}}
  .login-box{{background:#1a1d2e;border:1px solid #2d3148;border-radius:16px;padding:40px;width:360px}}
  .login-box h1{{text-align:center;margin-bottom:24px}}
  .login-box input{{margin-bottom:12px}}
  .login-box .btn{{width:100%;text-align:center;padding:10px}}
  code{{background:#0f1117;padding:2px 6px;border-radius:4px;font-size:12px;color:#a5b4fc}}
</style>
</head>
<body>
<nav>
  <span class="brand">🍃 Leaf Admin</span>
  <a href="/admin">Dashboard</a>
  <a href="/admin/accounts">IPTV Accounts</a>
  <a href="/admin/channels">Channels</a>
  <a href="/admin/logout">Logout</a>
</nav>
<div class="container">
{flash_html}
{body}
</div>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


# ── Login ─────────────────────────────────────────────────────────────────────

async def handle_admin_root(request: web.Request) -> web.Response:
    if _check_session(request):
        raise web.HTTPFound("/admin/channels")
    body = """
<div class="login-wrap">
<div class="login-box">
  <h1>🍃 Leaf Admin</h1>
  <form method="POST" action="/admin/login">
    <label>Password</label>
    <input type="password" name="password" autofocus placeholder="Enter admin password">
    <button class="btn btn-primary" type="submit">Login</button>
  </form>
</div>
</div>"""
    return web.Response(text=f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<style>*{{box-sizing:border-box;margin:0;padding:0}}body{{font-family:sans-serif;background:#0f1117;color:#e2e8f0}}
.login-wrap{{display:flex;align-items:center;justify-content:center;min-height:100vh}}
.login-box{{background:#1a1d2e;border:1px solid #2d3148;border-radius:16px;padding:40px;width:360px}}
h1{{text-align:center;margin-bottom:24px;font-size:22px}}
input{{background:#0f1117;border:1px solid #2d3148;color:#e2e8f0;padding:10px 12px;border-radius:6px;font-size:14px;width:100%;margin-bottom:12px}}
.btn{{display:block;width:100%;padding:10px;background:#4f46e5;color:#fff;border:none;border-radius:6px;font-size:14px;font-weight:600;cursor:pointer;text-align:center}}
label{{font-size:12px;color:#94a3b8;display:block;margin-bottom:4px}}</style>
<title>Leaf Admin</title></head><body>
<div class="login-wrap"><div class="login-box">
<h1>🍃 Leaf Admin</h1>
<form method="POST" action="/admin/login">
<label>Password</label>
<input type="password" name="password" autofocus placeholder="Enter admin password">
<button class="btn" type="submit">Login</button>
</form></div></div></body></html>""", content_type="text/html")


async def handle_login(request: web.Request) -> web.Response:
    data = await request.post()
    pw = data.get("password", "")
    if pw == ADMIN_PASSWORD:
        token = _make_token()
        _sessions[token] = time.time() + _SESSION_TTL
        resp = web.HTTPFound("/admin/channels")
        resp.set_cookie("leaf_admin", token, max_age=_SESSION_TTL, httponly=True, samesite="Lax")
        raise resp
    raise web.HTTPFound("/admin?err=1")


async def handle_logout(request: web.Request) -> web.Response:
    token = request.cookies.get("leaf_admin")
    if token:
        _sessions.pop(token, None)
    resp = web.HTTPFound("/admin")
    resp.del_cookie("leaf_admin")
    raise resp


# ── Dashboard / Status ────────────────────────────────────────────────────────

@_require_auth
async def handle_dashboard(request: web.Request) -> web.Response:
    raise web.HTTPFound("/admin/channels")


@_require_auth
async def handle_status_json(request: web.Request) -> web.Response:
    from m3u_recorder import m3u_recordings, active_m3u_tasks, build_m3u_playlist
    from cinearena_recorder import cinearena_recordings, build_cinearena_playlist

    out = {}
    for slug, state in m3u_recordings.items():
        good = [s for s in state.segments if s.done and not s.failed]
        out[slug] = {
            "type": "m3u",
            "title": state.title,
            "segments": len(good),
            "duration": round(sum(s.duration for s in good), 1),
            "buffering": build_m3u_playlist(state) is None,
        }
    for slug, state in cinearena_recordings.items():
        good = [s for s in state.segments if s.done and not s.failed]
        out[slug] = {
            "type": "cinearena",
            "title": state.title,
            "segments": len(good),
            "duration": round(sum(s.duration for s in good), 1),
            "buffering": build_cinearena_playlist(state) is None,
        }
    return web.Response(text=json.dumps(out, indent=2), content_type="application/json")


# ── Accounts ──────────────────────────────────────────────────────────────────

@_require_auth
async def handle_accounts(request: web.Request) -> web.Response:
    accounts = db.get_accounts()
    flash = request.rel_url.query.get("msg", "")
    rows = ""
    for acc in accounts.values():
        rows += f"""<tr>
          <td><b>{acc['label']}</b></td>
          <td><code>{acc['host']}:{acc.get('port',80)}</code></td>
          <td><code>{acc['username']}</code></td>
          <td>
            <form method="POST" action="/admin/accounts/test" style="display:inline">
              <input type="hidden" name="id" value="{acc['id']}">
              <button class="btn btn-ghost btn-sm" type="submit">Test</button>
            </form>
            <form method="POST" action="/admin/accounts/delete" style="display:inline" onsubmit="return confirm('Delete?')">
              <input type="hidden" name="id" value="{acc['id']}">
              <button class="btn btn-danger btn-sm" type="submit">Delete</button>
            </form>
          </td>
        </tr>"""

    body = f"""
<h1>IPTV Accounts</h1>
<div class="card">
  <h2>Add Account</h2>
  <form method="POST" action="/admin/accounts/add">
    <div class="form-row">
      <div><label>Label (e.g. AlphaX 8K)</label><input name="label" required placeholder="My IPTV Account"></div>
      <div><label>Host</label><input name="host" required placeholder="alphax8k.top"></div>
      <div><label>Port</label><input name="port" value="80" placeholder="80"></div>
      <div><label>Username</label><input name="username" required placeholder="abc123"></div>
      <div><label>Password</label><input name="password" required placeholder="xyz456"></div>
      <div><label>Protocol</label>
        <select name="protocol"><option value="http">HTTP</option><option value="https">HTTPS</option></select>
      </div>
    </div>
    <button class="btn btn-primary" type="submit">Add Account</button>
  </form>
</div>
<div class="card">
  <h2>Saved Accounts ({len(accounts)})</h2>
  <table>
    <tr><th>Label</th><th>Host</th><th>Username</th><th>Actions</th></tr>
    {rows if rows else '<tr><td colspan="4" style="color:#64748b;text-align:center;padding:20px">No accounts yet</td></tr>'}
  </table>
</div>"""
    return _page("Accounts", body, flash)


@_require_auth
async def handle_accounts_add(request: web.Request) -> web.Response:
    data = await request.post()
    acc = {
        "id":       str(uuid.uuid4())[:8],
        "label":    data.get("label", "").strip(),
        "host":     data.get("host", "").strip().rstrip("/"),
        "port":     int(data.get("port", 80) or 80),
        "username": data.get("username", "").strip(),
        "password": data.get("password", "").strip(),
        "protocol": data.get("protocol", "http"),
    }
    db.save_account(acc)
    raise web.HTTPFound("/admin/accounts?msg=Account+added")


@_require_auth
async def handle_accounts_delete(request: web.Request) -> web.Response:
    data = await request.post()
    db.delete_account(data.get("id", ""))
    raise web.HTTPFound("/admin/accounts?msg=Account+deleted")


@_require_auth
async def handle_accounts_test(request: web.Request) -> web.Response:
    data = await request.post()
    acc = db.get_account(data.get("id", ""))
    if not acc:
        raise web.HTTPFound("/admin/accounts?msg=Account+not+found")
    url = db.build_stream_url(acc, "1", "m3u8")
    try:
        import aiohttp
        async with aiohttp.ClientSession() as sess:
            async with sess.get(
                f"{acc.get('protocol','http')}://{acc['host']}:{acc.get('port',80)}/player_api.php"
                f"?username={acc['username']}&password={acc['password']}",
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                body = await resp.json()
                if body.get("user_info"):
                    exp = body["user_info"].get("exp_date", "?")
                    active = body["user_info"].get("active_cons", "?")
                    max_c = body["user_info"].get("max_connections", "?")
                    msg = f"OK — expires {exp}, connections {active}/{max_c}"
                else:
                    msg = f"Connected but unexpected response"
    except Exception as e:
        msg = f"Error: {e}"
    raise web.HTTPFound(f"/admin/accounts?msg={msg}")


# ── Channels ──────────────────────────────────────────────────────────────────

SPORT_OPTIONS = [
    ("football", "Football / Soccer"),
    ("basketball", "Basketball"),
    ("cricket", "Cricket"),
    ("tennis", "Tennis"),
    ("american-football", "American Football"),
    ("rugby", "Rugby"),
    ("baseball", "Baseball"),
    ("hockey", "Hockey"),
    ("motorsport", "Motorsport"),
    ("golf", "Golf"),
    ("boxing", "Boxing"),
    ("mma", "MMA / UFC"),
    ("other", "Other"),
]


@_require_auth
async def handle_channels(request: web.Request) -> web.Response:
    channels = db.get_channels()
    accounts = db.get_accounts()
    flash = request.rel_url.query.get("msg", "")

    # Live recording status
    from m3u_recorder import m3u_recordings, build_m3u_playlist
    from cinearena_recorder import cinearena_recordings, build_cinearena_playlist

    def _status_badge(slug):
        state = m3u_recordings.get(slug) or cinearena_recordings.get(slug)
        if not state:
            return '<span class="badge off">Stopped</span>'
        buffering = (build_m3u_playlist(state) or build_cinearena_playlist(state)) is None
        if buffering:
            return '<span class="badge buf">Buffering</span>'
        return '<span class="badge on">Live</span>'

    rows = ""
    for ch in channels.values():
        acc = accounts.get(ch.get("account_id", ""), {})
        acc_label = acc.get("label", "—")
        enabled_badge = '<span class="badge on">On</span>' if ch.get("enabled", True) else '<span class="badge off">Off</span>'
        mode_badge = f'<span class="badge live">{ch.get("mode","24x7")}</span>'
        rows += f"""<tr>
          <td><b>{ch['label']}</b><br><code style="font-size:11px">{ch['slug']}</code></td>
          <td>{acc_label}</td>
          <td><code>{ch.get('stream_id','')}</code></td>
          <td>{mode_badge}</td>
          <td>{ch.get('sport','—')}</td>
          <td>{enabled_badge}</td>
          <td>{_status_badge(ch['slug'])}</td>
          <td>
            <form method="POST" action="/admin/channels/toggle" style="display:inline">
              <input type="hidden" name="slug" value="{ch['slug']}">
              <button class="btn btn-ghost btn-sm" type="submit">{'Disable' if ch.get('enabled',True) else 'Enable'}</button>
            </form>
            <a href="/admin/channels/edit?slug={ch['slug']}" class="btn btn-ghost btn-sm">Edit</a>
            <form method="POST" action="/admin/channels/delete" style="display:inline" onsubmit="return confirm('Delete?')">
              <input type="hidden" name="slug" value="{ch['slug']}">
              <button class="btn btn-danger btn-sm" type="submit">Del</button>
            </form>
          </td>
        </tr>"""

    # Account options for select
    acc_opts = "".join(f'<option value="{a["id"]}">{a["label"]}</option>' for a in accounts.values())
    if not acc_opts:
        acc_opts = '<option value="">— Add an account first —</option>'

    sport_opts = "".join(f'<option value="{v}">{l}</option>' for v, l in SPORT_OPTIONS)

    body = f"""
<h1>Channels <span style="font-size:14px;color:#64748b;font-weight:400">({len(channels)} total)</span></h1>
<div class="card">
  <h2>Add Channel</h2>
  <form method="POST" action="/admin/channels/add">
    <div class="form-row">
      <div><label>Slug (unique ID, no spaces)</label><input name="slug" required placeholder="sky-sports-2"></div>
      <div><label>Display Label</label><input name="label" required placeholder="Sky Sports 2"></div>
      <div><label>IPTV Account</label><select name="account_id">{acc_opts}</select></div>
      <div><label>Stream ID</label><input name="stream_id" required placeholder="2006132"></div>
      <div><label>Mode</label>
        <select name="mode">
          <option value="24x7">24x7 (always recording)</option>
          <option value="live-only">Live-only (only when show is in Live Now)</option>
        </select>
      </div>
      <div><label>Sport Category</label><select name="sport">{sport_opts}</select></div>
    </div>
    <button class="btn btn-primary" type="submit">Add Channel</button>
  </form>
</div>
<div class="card">
  <table>
    <tr><th>Channel</th><th>Account</th><th>Stream ID</th><th>Mode</th><th>Sport</th><th>Enabled</th><th>Status</th><th>Actions</th></tr>
    {rows if rows else '<tr><td colspan="8" style="color:#64748b;text-align:center;padding:20px">No channels yet — add one above</td></tr>'}
  </table>
</div>"""
    return _page("Channels", body, flash)


@_require_auth
async def handle_channels_add(request: web.Request) -> web.Response:
    data = await request.post()
    slug = data.get("slug", "").strip().lower().replace(" ", "-")
    if not slug:
        raise web.HTTPFound("/admin/channels?msg=Slug+required")
    ch = {
        "slug":       slug,
        "label":      data.get("label", slug).strip(),
        "account_id": data.get("account_id", "").strip(),
        "stream_id":  data.get("stream_id", "").strip(),
        "mode":       data.get("mode", "24x7"),
        "sport":      data.get("sport", "other"),
        "enabled":    True,
        "extra_headers": {},
    }
    db.save_channel(ch)
    raise web.HTTPFound(f"/admin/channels?msg=Channel+{slug}+added")


@_require_auth
async def handle_channels_edit_get(request: web.Request) -> web.Response:
    slug = request.rel_url.query.get("slug", "")
    ch = db.get_channel(slug)
    if not ch:
        raise web.HTTPFound("/admin/channels?msg=Not+found")
    accounts = db.get_accounts()
    acc_opts = "".join(
        f'<option value="{a["id"]}" {"selected" if a["id"]==ch.get("account_id") else ""}>{a["label"]}</option>'
        for a in accounts.values()
    )
    sport_opts = "".join(
        f'<option value="{v}" {"selected" if v==ch.get("sport") else ""}>{l}</option>'
        for v, l in SPORT_OPTIONS
    )
    body = f"""
<h1>Edit Channel: {ch['label']}</h1>
<div class="card">
  <form method="POST" action="/admin/channels/add">
    <input type="hidden" name="slug" value="{ch['slug']}">
    <div class="form-row">
      <div><label>Slug</label><input name="slug_display" value="{ch['slug']}" disabled></div>
      <div><label>Display Label</label><input name="label" value="{ch['label']}" required></div>
      <div><label>IPTV Account</label><select name="account_id">{acc_opts}</select></div>
      <div><label>Stream ID</label><input name="stream_id" value="{ch.get('stream_id','')}" required></div>
      <div><label>Mode</label>
        <select name="mode">
          <option value="24x7" {"selected" if ch.get("mode")=="24x7" else ""}>24x7</option>
          <option value="live-only" {"selected" if ch.get("mode")=="live-only" else ""}>Live-only</option>
        </select>
      </div>
      <div><label>Sport</label><select name="sport">{sport_opts}</select></div>
    </div>
    <button class="btn btn-primary" type="submit">Save Changes</button>
    <a href="/admin/channels" class="btn btn-ghost" style="margin-left:8px">Cancel</a>
  </form>
</div>"""
    return _page(f"Edit {ch['label']}", body)


@_require_auth
async def handle_channels_delete(request: web.Request) -> web.Response:
    data = await request.post()
    slug = data.get("slug", "")
    db.delete_channel(slug)
    raise web.HTTPFound(f"/admin/channels?msg=Channel+{slug}+deleted")


@_require_auth
async def handle_channels_toggle(request: web.Request) -> web.Response:
    data = await request.post()
    slug = data.get("slug", "")
    ch = db.get_channel(slug)
    if ch:
        ch["enabled"] = not ch.get("enabled", True)
        db.save_channel(ch)
    raise web.HTTPFound("/admin/channels")


# ── Register routes ───────────────────────────────────────────────────────────

def register_admin_routes(app: web.Application) -> None:
    app.router.add_get("/admin",                    handle_admin_root)
    app.router.add_get("/admin/",                   handle_admin_root)
    app.router.add_post("/admin/login",             handle_login)
    app.router.add_get("/admin/logout",             handle_logout)
    app.router.add_get("/admin/accounts",           handle_accounts)
    app.router.add_post("/admin/accounts/add",      handle_accounts_add)
    app.router.add_post("/admin/accounts/delete",   handle_accounts_delete)
    app.router.add_post("/admin/accounts/test",     handle_accounts_test)
    app.router.add_get("/admin/channels",           handle_channels)
    app.router.add_post("/admin/channels/add",      handle_channels_add)
    app.router.add_get("/admin/channels/edit",      handle_channels_edit_get)
    app.router.add_post("/admin/channels/delete",   handle_channels_delete)
    app.router.add_post("/admin/channels/toggle",   handle_channels_toggle)
    app.router.add_get("/admin/status.json",        handle_status_json)
