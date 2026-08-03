"""
leaf_admin.py — Admin panel for the Leaf Bridge IPTV recorder.

Routes (all under /admin):
  GET  /admin                   → HTML admin panel (password-protected via session cookie)
  POST /admin/login             → Verify password, set session cookie
  GET  /admin/logout            → Clear session
  GET  /admin/api/accounts      → List accounts
  POST /admin/api/accounts      → Add account
  DELETE /admin/api/accounts/{id} → Remove account
  GET  /admin/api/channels?account_id=&search=&refresh=  → Fetch channel list
  GET  /admin/api/recordings    → List active M3U recordings
  POST /admin/api/recordings    → Start a recording
  DELETE /admin/api/recordings/{slug} → Stop a recording
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import time
import asyncio
from typing import Optional

from aiohttp import web

from iptv_accounts import (
    list_accounts,
    get_account,
    add_account,
    remove_account,
    fetch_channels,
    get_stream_url,
)
from m3u_recorder import (
    active_m3u_tasks,
    m3u_recordings,
    start_m3u_recording,
    stop_m3u_recording,
    build_m3u_playlist,
)
from config import RESIDENTIAL_PROXIES

logger = logging.getLogger("leaf.admin")

_LEAF_MODE = os.getenv("LEAF_MODE", "standalone")

# ── Auth ──────────────────────────────────────────────────────────────────────
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "leafadmin2026")
_sessions: dict[str, float] = {}   # token → created_at
_SESSION_TTL = 86400               # 24 hours


def _new_session() -> str:
    token = secrets.token_hex(32)
    _sessions[token] = time.time()
    return token


def _valid_session(token: Optional[str]) -> bool:
    if not token:
        return False
    created = _sessions.get(token)
    if created is None:
        return False
    if time.time() - created > _SESSION_TTL:
        del _sessions[token]
        return False
    return True


def _get_session(request: web.Request) -> Optional[str]:
    return request.cookies.get("admin_session")


def _require_auth(handler):
    async def wrapper(request: web.Request):
        if not _valid_session(_get_session(request)):
            if request.path.startswith("/admin/api"):
                return web.Response(status=401, text=json.dumps({"error": "Unauthorized"}),
                                    content_type="application/json")
            raise web.HTTPFound("/admin")
        return await handler(request)
    return wrapper


# ── Active recording meta (stores display info beyond m3u_recorder state) ─────
_recording_meta: dict[str, dict] = {}  # slug → {display_name, account_id, stream_id, ends_at}
_RECORDING_COLLECTION = "leaf_admin_recordings"
_mongo_col = None

# ── Hidden slugs (removed from catalog / Live Now on all backends) ────────────
_hidden_slugs: dict[str, float] = {}
_HIDDEN_SLUG_TTL = 6 * 3600
_HIDDEN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hidden_slugs.json")

_hidden_match_ids: dict[str, float] = {}   # match_id → expiry unix timestamp
_HIDDEN_MATCHES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hidden_match_ids.json")
_HIDDEN_MATCH_TTL = 8 * 3600   # 8 hours


def _load_hidden_slugs() -> None:
    global _hidden_slugs
    try:
        with open(_HIDDEN_FILE) as f:
            data = json.load(f)
        now = time.time()
        if isinstance(data, list):
            _hidden_slugs = {}  # old format — discard (no expiry info)
        else:
            _hidden_slugs = {s: exp for s, exp in data.items() if exp > now}
    except FileNotFoundError:
        pass
    except Exception as exc:
        logger.warning("hidden_slugs load error: %s", exc)


def _save_hidden_slugs() -> None:
    try:
        tmp = _HIDDEN_FILE + ".tmp"
        now = time.time()
        active = {s: exp for s, exp in _hidden_slugs.items() if exp > now}
        with open(tmp, "w") as f:
            json.dump(active, f)
        os.replace(tmp, _HIDDEN_FILE)
    except Exception as exc:
        logger.warning("hidden_slugs save error: %s", exc)


def _load_hidden_match_ids() -> None:
    global _hidden_match_ids
    try:
        with open(_HIDDEN_MATCHES_FILE) as f:
            data = json.load(f)
        now = time.time()
        if isinstance(data, list):
            _hidden_match_ids = {}
        else:
            _hidden_match_ids = {mid: exp for mid, exp in data.items() if exp > now}
    except FileNotFoundError:
        pass
    except Exception as exc:
        logger.warning("hidden_match_ids load error: %s", exc)


def _save_hidden_match_ids() -> None:
    try:
        now = time.time()
        active = {mid: exp for mid, exp in _hidden_match_ids.items() if exp > now}
        tmp = _HIDDEN_MATCHES_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(active, f)
        os.replace(tmp, _HIDDEN_MATCHES_FILE)
    except Exception as exc:
        logger.warning("hidden_match_ids save error: %s", exc)


def _get_mongo_col():
    """Get MongoDB collection for admin recordings."""
    global _mongo_col
    if _mongo_col is not None:
        return _mongo_col
    try:
        from pymongo import MongoClient
        uri = os.getenv("MONGODB_URI", "")
        if not uri:
            logger.warning("MONGODB_URI not set, admin recordings won't persist")
            return None
        client = MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=5000, connectTimeoutMS=5000)
        _mongo_col = client["stremio"][_RECORDING_COLLECTION]
        logger.info("Admin recordings: connected to MongoDB")
        return _mongo_col
    except Exception as exc:
        logger.warning("Admin recordings: MongoDB unavailable: %s", exc)
        return None


_motor_rec_col = None

def _get_motor_rec_col():
    """Get the async motor collection for admin recordings (shared motor client)."""
    global _motor_rec_col
    if _motor_rec_col is not None:
        return _motor_rec_col
    try:
        import iptv_accounts as _ia
        _ia._get_motor_col()  # ensure the shared motor client exists
        if _ia._motor_client is None:
            return None
        _motor_rec_col = _ia._motor_client["stremio"][_RECORDING_COLLECTION]
        return _motor_rec_col
    except Exception as exc:
        logger.warning("Admin recordings: motor unavailable: %s", exc)
        return None


def _save_recording_meta(slug: str, meta: dict):
    """Persist one recording to MongoDB (SYNC — startup/restore only; blocks event loop)."""
    col = _get_mongo_col()
    if col is None:
        return
    try:
        col.replace_one({"_id": slug}, {"_id": slug, **meta}, upsert=True)
    except Exception as e:
        logger.error("Failed to save recording meta for %s: %s", slug, e)


def _delete_recording_meta(slug: str):
    """Remove one recording from MongoDB (SYNC — startup/restore only)."""
    col = _get_mongo_col()
    if col is None:
        return
    try:
        col.delete_one({"_id": slug})
    except Exception as e:
        logger.error("Failed to delete recording meta for %s: %s", slug, e)


async def async_save_recording_meta(slug: str, meta: dict):
    """Persist one recording to MongoDB via motor — non-blocking, for async handlers."""
    col = _get_motor_rec_col()
    if col is None:
        return
    try:
        await col.replace_one({"_id": slug}, {"_id": slug, **meta}, upsert=True)
    except Exception as e:
        logger.error("async_save_recording_meta %s: %s", slug, e)


async def async_delete_recording_meta(slug: str):
    """Remove one recording from MongoDB via motor — non-blocking, for async handlers."""
    col = _get_motor_rec_col()
    if col is None:
        return
    try:
        await col.delete_one({"_id": slug})
    except Exception as e:
        logger.error("async_delete_recording_meta %s: %s", slug, e)


def _load_all_recording_meta() -> dict[str, dict]:
    """Load all admin recordings from MongoDB."""
    col = _get_mongo_col()
    if col is None:
        return {}
    try:
        docs = col.find({})
        result = {}
        for doc in docs:
            slug = doc.pop("_id")
            result[slug] = doc
        return result
    except Exception as e:
        logger.error("Failed to load recording meta: %s", e)
        return {}


def _restore_admin_recordings():
    """Re-start admin recordings that were running before backend restart."""
    from m3u_recorder import start_m3u_recording, active_m3u_tasks, m3u_recordings
    from iptv_accounts import get_stream_url

    global _recording_meta
    _recording_meta = _load_all_recording_meta()

    if not _recording_meta:
        return

    logger.info("Restoring %d admin recording(s)...", len(_recording_meta))

    to_remove = []
    for slug, meta in _recording_meta.items():
        # Skip hidden recordings — don't re-lock accounts for them
        if _hidden_slugs.get(slug, 0) > time.time():
            logger.info("Skipping hidden recording on restore: %s", slug)
            to_remove.append(slug)
            continue

        # YouTube streams — re-register with the yt-worker instead of claiming an
        # IPTV account. If the duration already expired while we were down, drop it.
        if meta.get("source") == "youtube":
            ends_at = meta.get("ends_at")
            if ends_at and time.time() > ends_at:
                logger.info("Dropping expired YouTube recording on restore: %s", slug)
                to_remove.append(slug)
                continue
            # Scheduled (pending) stream: it has NOT started yet. Keep the pending
            # meta in memory so the yt schedule poller fires it at scheduled_start_at
            # — do NOT start it on the worker now. If its start time already passed
            # while the master was down (and it hasn't ended), leave pending_start
            # set: the poller will start it on its next pass (within 30s).
            if meta.get("pending_start"):
                _sa = meta.get("scheduled_start_at")
                _yt_meta[slug] = {
                    "youtube_url":  meta.get("youtube_url", ""),
                    "display_name": meta.get("display_name", slug),
                    "category":     (meta.get("categories") or ["Sports"])[0],
                    "ends_at":      ends_at,
                    "started_at":   None,
                    "scheduled_start_at": _sa,
                    "pending_start":      True,
                }
                logger.info("Re-armed scheduled YouTube stream on restore: %s (start_at=%s UTC)", slug, _sa)
                continue
            _yt_url = meta.get("youtube_url", "")
            _yt_worker = os.getenv("YT_WORKER_URL", "http://100.69.9.26:8089")
            if _yt_url:
                async def _restore_yt(_s=slug, _u=_yt_url, _dn=meta.get("display_name", slug), _wu=_yt_worker):
                    import httpx as _hx
                    try:
                        async with _hx.AsyncClient(timeout=15) as _c:
                            await _c.post(f"{_wu}/internal/start",
                                          json={"slug": _s, "stream_url": _u, "display_name": _dn})
                        from leaf_server import _slug_to_worker as _s2w
                        _s2w[_s] = _wu
                        logger.info("Restored YouTube recording %s → %s", _s, _wu)
                    except Exception as _e:
                        logger.warning("Restore YouTube %s failed: %s", _s, _e)
                asyncio.create_task(_restore_yt())
            continue
        # Pending SCHEDULED IPTV recording (manual schedule, not yet started): keep
        # the pending meta in memory so the IPTV schedule poller fires it at its
        # scheduled_start_at. Do NOT start it or claim an account here. If its start
        # time already passed while the master was down, leave pending_start set —
        # the poller starts it on its next pass (within ~30s).
        if meta.get("pending_start"):
            logger.info("Re-armed scheduled IPTV recording on restore: %s (start_at=%s UTC)",
                        slug, meta.get("scheduled_start_at"))
            continue
        # Scheduler recordings are handled by the scheduler, not restore.
        # But drop any that already ended while we were down, so they don't
        # linger as dead ghost entries in the Recordings tab. Live ones are
        # left in place: the scheduler tick re-registers them from the worker
        # (master-restart recovery) or restarts them from the event store.
        if meta.get("scheduler"):
            sched_end = meta.get("auto_end_at") or meta.get("ends_at")
            if sched_end and time.time() > sched_end:
                logger.info("Dropping expired scheduler recording on restore: %s", slug)
                to_remove.append(slug)
            continue

        # Check if the account is still usable — don't restore a recording onto an
        # expired account. The scheduler will re-create it with a fresh account on
        # next tick. Without this, restore silently re-uses the dead URL and the
        # stream loops rc=0 forever without ever rotating to a good account.
        acc_id = meta.get("account_id", "")
        if acc_id:
            try:
                from iptv_accounts import _load_accounts, _is_account_usable
                _all_accts = _load_accounts()
                _acc = _all_accts.get(acc_id, {})
                if _acc and not _is_account_usable(_acc):
                    logger.warning("Skipping restore %s: account %s is expired/unusable", slug, acc_id)
                    from iptv_accounts import mark_account_free
                    mark_account_free(acc_id)
                    to_remove.append(slug)
                    continue
            except Exception as _e:
                logger.warning("Could not check account usability for %s: %s", slug, _e)

        # Check if duration expired
        ends_at = meta.get("ends_at")
        if ends_at and time.time() > ends_at:
            logger.info("Skipping expired recording: %s", slug)
            to_remove.append(slug)
            continue

        # Check if already running (shouldn't happen, but safety check)
        if slug in active_m3u_tasks:
            logger.info("Recording already active: %s", slug)
            continue

        # Resolve stream URL
        stream_id = meta.get("stream_id", "")
        stream_url = get_stream_url(acc_id, stream_id) if (acc_id and stream_id) else None

        if not stream_url:
            logger.warning("Cannot restore recording %s: no stream URL", slug)
            # Free the account so it doesn't stay ghost-locked with nothing recording.
            if acc_id:
                from iptv_accounts import mark_account_free
                mark_account_free(acc_id)
            to_remove.append(slug)
            continue

        # Start recording
        headers = {"user-agent": "okhttp/5.0.0-alpha.2"}
        if _LEAF_MODE == "master":
            # Capture loop vars for the closure
            _slug, _url, _meta, _ends_at = slug, stream_url, meta, ends_at

            async def _restore_on_worker(_s=_slug, _u=_url, _m=_meta, _ea=_ends_at):
                from iptv_scheduler import _pick_least_loaded_worker
                from worker_client import start_on_worker
                from leaf_server import _slug_to_worker
                await asyncio.sleep(5)  # let workers register via worker_up first
                if _s in _slug_to_worker:
                    logger.info("Restore %s: already on worker %s (auto-recovered), skipping", _s, _slug_to_worker[_s])
                    return
                pick = await _pick_least_loaded_worker()
                if not pick:
                    logger.warning("Restore %s: no workers available", _s)
                    return
                _, worker_url = pick
                # Claim the account BEFORE starting the worker — without this the
                # scheduler tick can grab it during the start_on_worker round-trip.
                _aid = (_m or {}).get("account_id", "")
                if _aid:
                    try:
                        from iptv_accounts import async_claim_specific_account, async_mark_free
                        claimed = await async_claim_specific_account(_aid, _s)
                    except Exception:
                        claimed = False
                    if not claimed:
                        logger.warning("Restore %s: account %s already taken — aborting restore to prevent double-booking", _s, _aid)
                        return
                ok = await start_on_worker(worker_url, _s, _u, headers, list(RESIDENTIAL_PROXIES), standalone=True)
                if not ok:
                    if _aid:
                        from iptv_accounts import async_mark_free
                        await async_mark_free(_aid)
                    logger.warning("Restore %s: worker failed to start", _s)
                    return
                _slug_to_worker[_s] = worker_url
                _recording_meta[_s]["started_at"] = time.time()
                logger.info("Restored admin recording %s → worker %s", _s, worker_url)
                if _ea:
                    remaining = int(_ea - time.time())
                    if remaining > 0:
                        async def _auto_stop_worker(_sw=_s, _wu=worker_url):
                            await asyncio.sleep(int(_ea - time.time()))
                            from leaf_server import _slug_to_worker as s2w
                            from worker_client import stop_on_worker
                            wurl = s2w.pop(_sw, None)
                            if wurl:
                                await stop_on_worker(wurl, _sw)
                            # Free the account unconditionally (worker reachable or not).
                            _m = _recording_meta.pop(_sw, None)
                            await async_delete_recording_meta(_sw)
                            _aid = (_m or {}).get("account_id")
                            if _aid:
                                from iptv_accounts import async_mark_free
                                await async_mark_free(_aid)
                        asyncio.create_task(_auto_stop_worker())

            asyncio.create_task(_restore_on_worker())
        else:
            try:
                start_m3u_recording(slug, stream_url, headers, RESIDENTIAL_PROXIES,
                                    standalone=(not ends_at))

                async def _set_meta_on_restore():
                    for _ in range(20):
                        if slug in m3u_recordings:
                            m3u_recordings[slug].title = meta.get("display_name", slug)
                            if hasattr(m3u_recordings[slug], "categories"):
                                m3u_recordings[slug].categories = meta.get("categories", [])
                            break
                        await asyncio.sleep(0.1)

                asyncio.create_task(_set_meta_on_restore())

                if ends_at:
                    remaining_sec = int(ends_at - time.time())
                    if remaining_sec > 0:
                        async def _auto_stop(_slug=slug, _acc_id=acc_id):
                            await asyncio.sleep(remaining_sec)
                            _m = _recording_meta.pop(_slug, None)
                            _aid = (_m or {}).get("account_id") or _acc_id
                            if _slug in active_m3u_tasks:
                                from m3u_recorder import stop_m3u_recording
                                stop_m3u_recording(_slug)
                            await async_delete_recording_meta(_slug)
                            if _aid:
                                from iptv_accounts import async_mark_free
                                await async_mark_free(_aid)
                            logger.info("Auto-stopped restored recording '%s'", _slug)
                        asyncio.create_task(_auto_stop())

                logger.info("Restored admin recording: %s → %s", slug, stream_url[:80])
            except Exception as e:
                logger.error("Failed to restore recording %s: %s", slug, e)
                # Free account so it doesn't stay ghost-locked with nothing recording.
                if acc_id:
                    from iptv_accounts import mark_account_free
                    mark_account_free(acc_id)
                to_remove.append(slug)

    # Clean up expired/failed recordings
    for slug in to_remove:
        _recording_meta.pop(slug, None)
        _delete_recording_meta(slug)


# ── HTML panel ────────────────────────────────────────────────────────────────

_LOGIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Leaf Bridge Admin</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  :root{--bg:#0d0f14;--card:#161a23;--border:#252a36;--accent:#6c6fff;--accent2:#4e4ecc;--text:#e8eaf0;--muted:#8b91a8;--danger:#ff4d6d;--success:#2dd4bf;}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center}
  .card{background:var(--card);border:1px solid var(--border);border-radius:16px;padding:40px;width:360px;box-shadow:0 24px 60px rgba(0,0,0,.5)}
  h1{font-size:1.4rem;font-weight:700;margin-bottom:8px}
  .sub{color:var(--muted);font-size:.85rem;margin-bottom:28px}
  label{display:block;font-size:.8rem;font-weight:600;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.05em}
  input{width:100%;background:#1e2330;border:1px solid var(--border);border-radius:8px;padding:10px 14px;color:var(--text);font-size:.9rem;outline:none;transition:border .2s}
  input:focus{border-color:var(--accent)}
  .btn{width:100%;background:var(--accent);color:#fff;border:none;border-radius:8px;padding:12px;font-size:.95rem;font-weight:600;cursor:pointer;transition:background .2s;margin-top:16px}
  .btn:hover{background:var(--accent2)}
  .err{color:var(--danger);font-size:.82rem;margin-top:12px;display:none}
  .logo{font-size:2rem;margin-bottom:12px}
</style>
</head>
<body>
<div class="card">
  <div class="logo">🍃</div>
  <h1>Leaf Bridge Admin</h1>
  <p class="sub">IPTV Recording Manager</p>
  <form id="lf">
    <label>Password</label>
    <input type="password" id="pw" placeholder="Enter admin password" autofocus>
    <button class="btn" type="submit">Sign In</button>
    <div class="err" id="err">Incorrect password</div>
  </form>
</div>
<script>
document.getElementById('lf').addEventListener('submit',async e=>{
  e.preventDefault();
  const r=await fetch('/admin/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:document.getElementById('pw').value})});
  if(r.ok){location.href='/admin/panel';}else{document.getElementById('err').style.display='block';}
});
</script>
</body></html>"""


def _panel_html() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Leaf Bridge Admin Panel</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  :root{--bg:#0d0f14;--card:#161a23;--card2:#1c2133;--border:#252a36;--accent:#6c6fff;--accent2:#4e4ecc;--text:#e8eaf0;--muted:#8b91a8;--danger:#ff4d6d;--success:#2dd4bf;--warn:#f59e0b}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh}
  .header{background:var(--card);border-bottom:1px solid var(--border);padding:0 24px;display:flex;align-items:center;gap:12px;height:56px;position:sticky;top:0;z-index:100}
  .header h1{font-size:1rem;font-weight:700;flex:1}
  .nav{display:flex;gap:4px}
  .nav button{background:none;border:none;color:var(--muted);padding:8px 14px;border-radius:8px;cursor:pointer;font-size:.85rem;font-weight:500;transition:all .15s}
  .nav button.active,.nav button:hover{background:#252a36;color:var(--text)}
  .logout{background:none;border:1px solid var(--border);color:var(--muted);padding:6px 12px;border-radius:8px;cursor:pointer;font-size:.8rem}
  .logout:hover{border-color:var(--danger);color:var(--danger)}
  .main{padding:24px;max-width:1200px;margin:0 auto}
  .tab{display:none}.tab.active{display:block}
  .section-title{font-size:1rem;font-weight:700;margin-bottom:16px;display:flex;align-items:center;gap:8px}
  .card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:16px}
  .grid2{display:grid;grid-template-columns:1fr 1fr;gap:16px}
  @media(max-width:700px){.grid2{grid-template-columns:1fr}}
  label{display:block;font-size:.75rem;font-weight:600;color:var(--muted);margin-bottom:5px;text-transform:uppercase;letter-spacing:.05em}
  input,select,textarea{width:100%;background:#1e2330;border:1px solid var(--border);border-radius:8px;padding:9px 12px;color:var(--text);font-size:.875rem;outline:none;font-family:inherit;transition:border .15s}
  input:focus,select:focus{border-color:var(--accent)}
  .btn{background:var(--accent);color:#fff;border:none;border-radius:8px;padding:9px 18px;font-size:.875rem;font-weight:600;cursor:pointer;transition:background .2s;white-space:nowrap}
  .btn:hover{background:var(--accent2)}
  .btn:disabled{opacity:.4;cursor:default}
  .btn-sm{padding:6px 12px;font-size:.8rem}
  .btn-danger{background:#2a1a1f;color:var(--danger);border:1px solid #3d1a24}
  .btn-danger:hover{background:#3d1a24}
  .btn-success{background:#0f2a27;color:var(--success);border:1px solid #1a3d38}
  .btn-success:hover{background:#1a3d38}
  .btn-outline{background:transparent;color:var(--accent);border:1px solid var(--accent)}
  .btn-outline:hover{background:var(--accent);color:#fff}
  .row{display:flex;gap:12px;align-items:flex-end;flex-wrap:wrap}
  .badge{display:inline-flex;align-items:center;gap:4px;padding:3px 8px;border-radius:20px;font-size:.72rem;font-weight:600}
  .badge-green{background:#0f2a27;color:var(--success)}
  .badge-yellow{background:#2a2010;color:var(--warn)}
  .badge-red{background:#2a1a1f;color:var(--danger)}
  .badge-blue{background:#181b2e;color:var(--accent)}
  .account-card{background:var(--card2);border:1px solid var(--border);border-radius:10px;padding:14px 16px;display:flex;align-items:center;gap:12px;margin-bottom:8px}
  .account-card .name{font-weight:600;font-size:.9rem;flex:1}
  .account-card .meta{color:var(--muted);font-size:.78rem}
  .ch-list{height:480px;overflow-y:auto;border:1px solid var(--border);border-radius:8px}
  .ch-item{display:flex;align-items:center;gap:10px;padding:10px 14px;border-bottom:1px solid #1a1f2e;cursor:pointer;transition:background .1s}
  .ch-item:hover{background:#1c2133}
  .ch-item:last-child{border-bottom:none}
  .ch-logo{width:32px;height:32px;border-radius:6px;object-fit:contain;background:#1e2330;flex-shrink:0}
  .ch-name{flex:1;font-size:.875rem;font-weight:500}
  .ch-cat{color:var(--muted);font-size:.75rem}
  .rec-card{background:var(--card2);border:1px solid var(--border);border-radius:10px;padding:14px 16px;margin-bottom:8px;display:flex;align-items:center;gap:12px}
  .rec-info{flex:1}
  .rec-name{font-weight:600;font-size:.9rem}
  .rec-meta{color:var(--muted);font-size:.77rem;margin-top:2px}
  .dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
  .dot-green{background:var(--success);box-shadow:0 0 6px var(--success)}
  .dot-yellow{background:var(--warn);box-shadow:0 0 6px var(--warn)}
  .dot-red{background:var(--danger);box-shadow:0 0 6px var(--danger)}
  .empty{text-align:center;color:var(--muted);padding:40px;font-size:.9rem}
  .modal-bg{display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:200;align-items:center;justify-content:center}
  .modal-bg.open{display:flex}
  .modal{background:var(--card);border:1px solid var(--border);border-radius:16px;padding:28px;width:520px;max-width:95vw;max-height:90vh;overflow-y:auto}
  .modal h2{font-size:1.05rem;font-weight:700;margin-bottom:20px}
  .field{margin-bottom:14px}
  .toast{position:fixed;bottom:24px;right:24px;background:var(--card2);border:1px solid var(--border);border-radius:10px;padding:12px 18px;font-size:.85rem;z-index:999;transform:translateY(20px);opacity:0;transition:all .3s;pointer-events:none}
  .toast.show{transform:translateY(0);opacity:1}
  .toast.ok{border-color:var(--success);color:var(--success)}
  .toast.err{border-color:var(--danger);color:var(--danger)}
  .search-row{display:flex;gap:8px;margin-bottom:12px;align-items:center}
  .spin{display:none;width:18px;height:18px;border:2px solid var(--border);border-top-color:var(--accent);border-radius:50%;animation:spin .6s linear infinite}
  @keyframes spin{to{transform:rotate(360deg)}}
  .sel-ch-preview{background:var(--card2);border:1px solid var(--accent);border-radius:8px;padding:12px;margin-bottom:14px;display:none}
  .sel-ch-name{font-weight:600;margin-bottom:2px}
  .sel-ch-meta{color:var(--muted);font-size:.78rem}
  select option{background:#1e2330}
  .type-tabs{display:flex;gap:4px;margin-bottom:16px}
  .type-tab{flex:1;padding:8px;background:#1e2330;border:1px solid var(--border);border-radius:8px;cursor:pointer;font-size:.82rem;font-weight:600;color:var(--muted);text-align:center;transition:all .15s}
  .type-tab.active{background:#1e2533;border-color:var(--accent);color:var(--accent)}
  .cat-pill{background:#1e2330;border:1px solid var(--border);border-radius:20px;padding:5px 12px;font-size:.78rem;font-weight:600;color:var(--muted);cursor:pointer;transition:all .15s;white-space:nowrap}
  .cat-pill:hover{border-color:var(--accent);color:var(--accent)}
  .cat-pill.active{background:#1e2533;border-color:var(--accent);color:var(--accent)}
</style>
</head>
<body>
<header class="header">
  <span style="font-size:1.3rem">🍃</span>
  <h1>Leaf Bridge Admin</h1>
  <nav class="nav">
    <button class="active" onclick="showTab('recordings',this)">🔴 Recordings</button>
    <button onclick="showTab('accounts',this)">🔑 Accounts</button>
    <button onclick="showTab('browse',this)">📺 Browse &amp; Record</button>
    <button onclick="showTab('schedule',this)">📅 Schedule</button>
    <button onclick="showTab('groups',this)">📁 Groups</button>
    <button onclick="showTab('rules',this)">📋 Auto Rules</button>
    <button onclick="showTab('youtube',this)">🎬 YouTube</button>
    <button onclick="showTab('status',this)">📊 Status</button>
  </nav>
  <button class="logout" onclick="logout()">Sign out</button>
</header>

<main class="main">

  <!-- ── RECORDINGS TAB ── -->
  <div class="tab active" id="tab-recordings">
    <div class="section-title">🔴 Active Recordings
      <button class="btn btn-sm btn-outline" onclick="loadRecordings()">↻ Refresh</button>
    </div>
    <div id="rec-list"><div class="empty">Loading…</div></div>
  </div>

  <!-- ── ACCOUNTS TAB ── -->
  <div class="tab" id="tab-accounts">
    <div class="section-title">🔑 IPTV Accounts
      <button class="btn btn-sm" onclick="openAddAccount()">+ Add Account</button>
    </div>
    <div id="acc-list"><div class="empty">Loading…</div></div>
  </div>

  <!-- ── BROWSE TAB ── -->
  <div class="tab" id="tab-browse">
    <div class="grid2">
      <div>
        <div class="section-title">📺 Browse Channels</div>
        <div class="card" style="padding:14px">
          <div class="field">
            <label>Account</label>
            <select id="br-account" onchange="onAccountChange()">
              <option value="">— Select account —</option>
            </select>
          </div>
          <div class="search-row">
            <input id="br-search" placeholder="Search channels…" oninput="debounceSearch()" style="flex:1">
            <div class="spin" id="br-spin"></div>
            <button class="btn btn-sm btn-outline" onclick="loadChannels(true)">↻</button>
          </div>
          <div class="ch-list" id="ch-list"><div class="empty">Select an account to browse channels</div></div>
          <div id="ch-count" style="color:var(--muted);font-size:.75rem;margin-top:8px;text-align:right"></div>
        </div>
      </div>
      <div>
        <div class="section-title">▶ Start Recording</div>
        <div class="card" style="padding:14px">
          <div class="sel-ch-preview" id="sel-preview">
            <div class="sel-ch-name" id="sel-name">—</div>
            <div class="sel-ch-meta" id="sel-meta">No channel selected</div>
          </div>
          <div class="field">
            <label>Display Name (in catalog)</label>
            <input id="rec-display-name" placeholder="e.g. Ghana vs Panama 4K">
          </div>
          <div class="field">
            <label>Duration</label>
            <select id="rec-duration">
              <option value="0">24/7 (no limit)</option>
              <option value="90">90 minutes</option>
              <option value="120">2 hours</option>
              <option value="180">3 hours</option>
              <option value="240">4 hours</option>
              <option value="360">6 hours</option>
              <option value="480">8 hours</option>
              <option value="720">12 hours</option>
            </select>
          </div>
          <div class="field">
            <label>Start at (UTC) — leave blank to start now</label>
            <input id="rec-startat" type="datetime-local" step="60" oninput="recUpdateBtn()">
            <div style="color:var(--muted);font-size:.73rem;margin-top:4px">Set a future UTC time to schedule; it auto-starts at that exact time.</div>
          </div>
          <div class="field">
            <label>Slug (URL identifier)</label>
            <input id="rec-slug" placeholder="e.g. fox-4k-ghana-panama" style="font-family:monospace">
            <div style="color:var(--muted);font-size:.73rem;margin-top:4px">leaf.highfly.dev/m3u/<span id="slug-preview" style="color:var(--accent)">slug</span>/live.m3u8</div>
          </div>
          <div class="field">
            <label>Sport Category</label>
            <select id="rec-category">
              <option value="">— Not specified —</option>
              <option value="football">⚽ Football / Soccer</option>
              <option value="basketball">🏀 Basketball</option>
              <option value="american-football">🏈 American Football (NFL)</option>
              <option value="hockey">🏒 Hockey</option>
              <option value="baseball">⚾ Baseball</option>
              <option value="fight">🥊 Fight (UFC / Boxing)</option>
              <option value="tennis">🎾 Tennis</option>
              <option value="rugby">🏉 Rugby</option>
              <option value="golf">⛳ Golf</option>
              <option value="motor-sports">🏎 Motor Sports</option>
              <option value="cricket">🏏 Cricket</option>
              <option value="afl">🦘 AFL</option>
              <option value="darts">🎯 Darts</option>
              <option value="billiards">🎱 Billiards / Snooker</option>
              <option value="other">🏅 Other Sports</option>
            </select>
          </div>
          <button class="btn" style="width:100%" id="start-rec-btn" disabled onclick="startRecording()">
            Select a channel first
          </button>
        </div>
      </div>
    </div>
  </div>

  
  <!-- ── GROUPS TAB ── -->
  <div class="tab" id="tab-groups">
    <div class="section-title">📁 Channel Groups
      <button class="btn btn-sm" onclick="openAddGroup()">+ Create Group</button>
      <button class="btn btn-sm btn-outline" onclick="loadGroups()">↻ Refresh</button>
    </div>
    <div id="group-list"><div class="empty">Loading Groups…</div></div>
  </div>

  <!-- ── RULES TAB ── -->
  <div class="tab" id="tab-rules">
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:16px">
      <div class="section-title" style="margin:0;flex:1">📋 Auto-Assignment Rules</div>
      <button class="btn btn-sm" onclick="openRuleModal()">+ Add Rule</button>
      <button class="btn btn-sm btn-outline" onclick="loadRules()">↻ Refresh</button>
    </div>
    <div style="color:var(--muted);font-size:.82rem;margin-bottom:16px;padding:10px 14px;background:var(--card2);border-radius:8px;border-left:3px solid var(--accent)">
      Keyword rules auto-assign channels to <b>upcoming</b> events whose titles contain the keyword. Already-live events are not affected — assign those manually in the Schedule tab.
    </div>
    <div id="rules-list"><div class="empty">Loading…</div></div>
  </div>

  <!-- ── SCHEDULE TAB ── -->
  <div class="tab" id="tab-schedule">
    <div id="cap-banner" style="display:none;background:#2a1e0a;border:1px solid #f59e0b;border-radius:10px;padding:12px 16px;margin-bottom:16px;color:#f59e0b;font-size:.85rem"></div>
    <!-- Top bar -->
    <div style="display:flex;gap:10px;margin-bottom:14px;align-items:center;flex-wrap:wrap">
      <div class="section-title" style="margin:0;flex:1">📅 Schedule Manager</div>
      <div style="display:flex;gap:8px;flex:2;min-width:240px">
        <input id="sched-search" placeholder="🔍 Search events…" oninput="schedDebounceSearch()" style="flex:1">
        <button class="btn btn-sm btn-outline" onclick="loadSchedule(true)">↻ Refresh</button>
      </div>
    </div>
    <!-- Category pills -->
    <div id="sched-cat-pills" style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:14px">
      <button class="cat-pill active" data-cat="all" onclick="setCat('all',this)">🌐 All</button>
      <button class="cat-pill" data-cat="sports_live" onclick="setCat('sports_live',this)">🔴 Live</button>
      <button class="cat-pill" data-cat="sports_today" onclick="setCat('sports_today',this)">📆 Today</button>
      <button class="cat-pill" data-cat="sports_basketball" onclick="setCat('sports_basketball',this)">🏀 Basketball</button>
      <button class="cat-pill" data-cat="sports_football" onclick="setCat('sports_football',this)">⚽ Football</button>
      <button class="cat-pill" data-cat="sports_american_football" onclick="setCat('sports_american_football',this)">🏈 NFL</button>
      <button class="cat-pill" data-cat="sports_hockey" onclick="setCat('sports_hockey',this)">🏒 Hockey</button>
      <button class="cat-pill" data-cat="sports_baseball" onclick="setCat('sports_baseball',this)">⚾ Baseball</button>
      <button class="cat-pill" data-cat="sports_tennis" onclick="setCat('sports_tennis',this)">🎾 Tennis</button>
      <button class="cat-pill" data-cat="sports_cricket" onclick="setCat('sports_cricket',this)">🏏 Cricket</button>
      <button class="cat-pill" data-cat="sports_motor_sports" onclick="setCat('sports_motor_sports',this)">🏎 Motorsport</button>
      <button class="cat-pill" data-cat="sports_fight" onclick="setCat('sports_fight',this)">🥊 Fight</button>
      <button class="cat-pill" data-cat="sports_rugby" onclick="setCat('sports_rugby',this)">🏉 Rugby</button>
      <button class="cat-pill" data-cat="sports_golf" onclick="setCat('sports_golf',this)">⛳ Golf</button>
      <button class="cat-pill" data-cat="sports_afl" onclick="setCat('sports_afl',this)">🦘 AFL</button>
      <button class="cat-pill" data-cat="sports_darts" onclick="setCat('sports_darts',this)">🎯 Darts</button>
      <button class="cat-pill" data-cat="sports_billiards" onclick="setCat('sports_billiards',this)">🎱 Billiards</button>
      <button class="cat-pill" data-cat="sports_other" onclick="setCat('sports_other',this)">🏅 Other</button>
    </div>
    <!-- Capacity bar -->
    <div id="cap-bar" class="card" style="padding:14px;margin-bottom:16px">
      <div style="font-size:.78rem;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.05em;margin-bottom:10px">⚡ Account Capacity</div>
      <div id="cap-accounts" style="display:flex;flex-wrap:wrap;gap:8px"></div>
    </div>
    <!-- Live Now -->
    <div class="section-title">🔴 Live Now</div>
    <div id="sched-live"><div class="empty">Loading…</div></div>
    <!-- Today / Upcoming -->
    <div class="section-title" style="margin-top:20px">📆 Today / Upcoming</div>
    <div id="sched-today"><div class="empty">Loading…</div></div>
    <!-- Ended (leaf marked ended, streamed.pk may still say LIVE) -->
    <div class="section-title" style="margin-top:20px">🏁 Ended</div>
    <div id="sched-ended"><div class="empty">None ended</div></div>
    <!-- Hidden from Live Now -->
    <div class="section-title" style="margin-top:20px">🚫 Hidden from Live Now</div>
    <div id="sched-hidden"><div class="empty">None hidden</div></div>
  </div>

  <!-- ── YOUTUBE TAB ── -->
  <div class="tab" id="tab-youtube">
    <div class="section-title">🎬 YouTube Live Streams</div>
    <div style="background:var(--card2);border:1px solid var(--border);border-radius:12px;padding:18px;margin-bottom:20px">
      <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end">
        <div style="flex:3;min-width:200px">
          <label style="font-size:.78rem;color:var(--muted);display:block;margin-bottom:4px">YouTube URL</label>
          <input id="yt-url" placeholder="https://www.youtube.com/watch?v=..." oninput="ytFetchInfo()" style="width:100%">
        </div>
        <div style="flex:2;min-width:140px">
          <label style="font-size:.78rem;color:var(--muted);display:block;margin-bottom:4px">Slug (stream ID)</label>
          <input id="yt-slug" placeholder="yt-australia-wcup" style="width:100%">
        </div>
        <div style="flex:2;min-width:140px">
          <label style="font-size:.78rem;color:var(--muted);display:block;margin-bottom:4px">Title</label>
          <input id="yt-title" placeholder="Auto-fetched…" style="width:100%">
        </div>
        <div style="flex:2;min-width:130px">
          <label style="font-size:.78rem;color:var(--muted);display:block;margin-bottom:4px">Category</label>
          <select id="yt-category" style="width:100%">
            <option value="football">⚽ Football</option>
            <option value="basketball">🏀 Basketball</option>
            <option value="american_football">🏈 American Football</option>
            <option value="hockey">🏒 Hockey</option>
            <option value="baseball">⚾ Baseball</option>
            <option value="tennis">🎾 Tennis</option>
            <option value="cricket">🏏 Cricket</option>
            <option value="fight">🥊 Fight</option>
            <option value="motor_sports">🏎️ Motor Sports</option>
            <option value="rugby">🏉 Rugby</option>
            <option value="afl">🏉 AFL</option>
            <option value="golf">⛳ Golf</option>
            <option value="darts">🎯 Darts</option>
            <option value="other" selected>🌐 Other</option>
          </select>
        </div>
        <div style="flex:1;min-width:90px">
          <label style="font-size:.78rem;color:var(--muted);display:block;margin-bottom:4px">Duration (h)</label>
          <input id="yt-hours" type="number" value="24" min="1" max="72" style="width:100%">
        </div>
        <div style="flex:2;min-width:190px">
          <label style="font-size:.78rem;color:var(--muted);display:block;margin-bottom:4px">Start at (UTC) — blank = now</label>
          <input id="yt-startat" type="datetime-local" step="60" style="width:100%" oninput="ytUpdateBtn()">
        </div>
        <div>
          <button class="btn" id="yt-start-btn" onclick="ytStart()" style="height:38px">▶ Start Stream</button>
        </div>
      </div>
      <div id="yt-info-bar" style="margin-top:10px;font-size:.8rem;color:var(--muted)"></div>
    </div>
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
      <div class="section-title" style="margin-bottom:0">Active YouTube Streams</div>
      <button class="btn btn-sm btn-outline" id="yt-refresh-list-btn" onclick="loadYouTube()">🔄 Refresh</button>
    </div>
    <div id="yt-list"><div class="empty">Loading…</div></div>
  </div>

  <!-- ── STATUS TAB ── -->
  <div class="tab" id="tab-status">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
      <div class="section-title" style="margin-bottom:0">📊 Stream Stability</div>
      <button class="btn btn-sm btn-outline" id="status-refresh-btn" onclick="loadStatus()">🔄 Refresh</button>
    </div>
    <div id="status-meta" style="font-size:.78rem;color:var(--muted);margin-bottom:14px"></div>
    <div class="section-title" style="font-size:.9rem;margin-bottom:10px">● Live Now</div>
    <div id="status-live"><div class="empty">Loading…</div></div>
    <div class="section-title" style="font-size:.9rem;margin:22px 0 10px">🏁 Past Events</div>
    <div id="status-past"><div class="empty">Loading…</div></div>
  </div>

</main>

<!-- Add Account Modal -->
<div class="modal-bg" id="modal-acc">
  <div class="modal">
    <h2>Add IPTV Account</h2>
    <div class="type-tabs">
      <div class="type-tab active" id="tt-xtream" onclick="setAccType('xtream')">Xtream Codes</div>
      <div class="type-tab" id="tt-m3u" onclick="setAccType('m3u')">Raw M3U URL</div>
    </div>
    <div class="field">
      <label>Account Name</label>
      <input id="acc-name" placeholder="e.g. My IPTV Provider">
    </div>
    <div id="xtream-fields">
      <div class="field">
        <label>Server URL</label>
        <input id="acc-server" placeholder="http://yourserver.com:8080">
      </div>
      <div class="field">
        <label>Username</label>
        <input id="acc-user" placeholder="username">
      </div>
      <div class="field">
        <label>Password</label>
        <input type="password" id="acc-pass" placeholder="password">
      </div>
    </div>
    <div id="m3u-fields" style="display:none">
      <div class="field">
        <label>M3U Playlist URL</label>
        <input id="acc-m3u-url" placeholder="http://yourserver.com/get.php?...">
      </div>
    </div>
    <div style="display:flex;gap:10px;margin-top:20px">
      <button class="btn btn-outline" style="flex:1" onclick="closeModal('modal-acc')">Cancel</button>
      <button class="btn" style="flex:1" onclick="saveAccount()">Save Account</button>
    </div>
  </div>
</div>

<!-- Generic Modal (used by openAddGroup and other dynamic forms) -->
<div class="modal-bg" id="modal">
  <div class="modal">
    <div id="modal-inner"></div>
    <div style="margin-top:16px;text-align:right">
      <button class="btn btn-outline btn-sm" onclick="closeModal('modal')">✕ Close</button>
    </div>
  </div>
</div>


<!-- Assign Channel Modal -->
<div class="modal-bg" id="modal-assign">
  <div class="modal">
    <h2>Assign IPTV Channels</h2>
    <p style="color:var(--muted);font-size:.83rem;margin-bottom:16px">Event: <b id="assign-event-name"></b></p>
    <p style="color:var(--muted);font-size:.78rem;margin-bottom:12px;padding:8px 12px;background:var(--card2);border-radius:8px;border-left:3px solid var(--accent)">
      🤖 Account is <b>auto-selected</b> at recording time — pick one or more channels below.
    </p>
    <div class="field">
      <label>Team Key <span style="font-weight:400;color:var(--muted)">(optional — reuses across future games of same team)</span></label>
      <input id="assign-team-key" placeholder="e.g. atlanta-braves (leave blank for one-off)">
    </div>
    <div class="field" style="margin-top:14px">
      <label>Auto-end after <span style="font-weight:400;color:var(--muted)">(hours from event scheduled start — leave blank for no auto-end)</span></label>
      <input id="assign-end-hours" type="number" min="0.5" max="12" step="0.5" placeholder="e.g. 3" style="width:140px">
      <div style="color:var(--muted);font-size:.72rem;margin-top:3px">Timer starts at the event's scheduled kick-off time, not the recording start.</div>
    </div>
    <div class="field" style="margin-top:14px">
      <label style="display:flex;align-items:center;justify-content:space-between">
        <span>Search &amp; Pick TV Channels <span style="font-weight:400;color:var(--muted)">(click rows to select, multi-select)</span></span>
        <span id="assign-sel-count" style="display:none;background:var(--accent);color:#000;font-size:.72rem;font-weight:700;padding:2px 8px;border-radius:20px">0 selected</span>
      </label>
      <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px;margin-top:6px">
        <input id="assign-ch-search" placeholder="e.g. beIN Sports, TNT, Sky…" oninput="assignDebounceSearch()" style="flex:1" autofocus>
        <div class="spin" id="assign-spin"></div>
      </div>
      <div id="assign-ch-list" style="height:280px;overflow-y:auto;border:1px solid var(--border);border-radius:8px">
        <div class="empty">Type to search channels across all accounts</div>
      </div>
    </div>
    <div style="display:flex;gap:10px;margin-top:16px">
      <button class="btn btn-outline" style="flex:1" onclick="closeModal('modal-assign')">Cancel</button>
      <button class="btn" style="flex:2" id="assign-save-btn" onclick="saveAssignment()">Add Selected Channels</button>
    </div>
    <div style="display:flex;align-items:center;justify-content:flex-end;gap:8px;margin-top:10px">
      <span style="font-size:.76rem;color:var(--muted)">Refresh schedule after saving</span>
      <div id="assign-refresh-toggle"
        onclick="_assignToggleRefresh()"
        title="Toggle: refresh the schedule list after saving"
        style="width:40px;height:22px;border-radius:11px;background:var(--accent);cursor:pointer;position:relative;transition:background .2s;flex-shrink:0">
        <div id="assign-refresh-knob" style="position:absolute;top:3px;left:21px;width:16px;height:16px;border-radius:50%;background:#fff;transition:left .2s"></div>
      </div>
    </div>
  </div>
</div>

<!-- Rule Modal -->
<div class="modal-bg" id="modal-rule">
  <div class="modal">
    <h2 id="rule-modal-title">Add Auto Rule</h2>
    <input type="hidden" id="rule-edit-key">
    <div class="field">
      <label>Keyword(s) <span style="font-weight:400;color:var(--muted)">(comma-separated — matched against event titles)</span></label>
      <input id="rule-keywords" placeholder="e.g. Boston Red Sox, New York Yankees">
    </div>
    <div class="field" style="margin-top:14px">
      <label>Auto-end after <span style="font-weight:400;color:var(--muted)">(hours from scheduled start — optional)</span></label>
      <input id="rule-end-hours" type="number" min="0.5" max="12" step="0.5" placeholder="e.g. 3" style="width:140px">
    </div>
    <div class="field" style="margin-top:14px">
      <label style="display:flex;align-items:center;justify-content:space-between">
        <span>Channels <span style="font-weight:400;color:var(--muted)">(search and select)</span></span>
        <span id="rule-sel-count" style="display:none;background:var(--accent);color:#000;font-size:.72rem;font-weight:700;padding:2px 8px;border-radius:20px">0 selected</span>
      </label>
      <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px;margin-top:6px">
        <input id="rule-ch-search" placeholder="e.g. beIN Sports, TNT, Sky…" oninput="ruleDebounceSearch()" style="flex:1">
        <div class="spin" id="rule-spin"></div>
      </div>
      <div id="rule-ch-list" style="height:240px;overflow-y:auto;border:1px solid var(--border);border-radius:8px">
        <div class="empty">Type to search channels</div>
      </div>
    </div>
    <div id="rule-sel-chips" style="display:flex;flex-wrap:wrap;gap:6px;margin-top:10px;min-height:24px"></div>
    <div style="display:flex;gap:10px;margin-top:16px">
      <button class="btn btn-outline" style="flex:1" onclick="closeModal('modal-rule')">Cancel</button>
      <button class="btn" style="flex:2" id="rule-save-btn" onclick="saveRule()">Save Rule</button>
    </div>
  </div>
</div>

<!-- Edit Recording Modal -->
<div class="modal-bg" id="modal-edit-rec">
  <div class="modal">
    <h2>Edit Recording</h2>
    <input type="hidden" id="edit-rec-slug">
    <div class="field">
      <label>Display Name</label>
      <input id="edit-rec-name" placeholder="e.g. Ghana vs Panama 4K">
    </div>
    <div class="field" style="margin-top:12px">
      <label>Sport Category</label>
      <select id="edit-rec-category">
        <option value="">— Not specified —</option>
        <option value="football">⚽ Football / Soccer</option>
        <option value="basketball">🏀 Basketball</option>
        <option value="american-football">🏈 American Football (NFL)</option>
        <option value="hockey">🏒 Hockey</option>
        <option value="baseball">⚾ Baseball</option>
        <option value="fight">🥊 Fight (UFC / Boxing)</option>
        <option value="tennis">🎾 Tennis</option>
        <option value="rugby">🏉 Rugby</option>
        <option value="golf">⛳ Golf</option>
        <option value="motor-sports">🏎 Motor Sports</option>
        <option value="cricket">🏏 Cricket</option>
        <option value="afl">🦘 AFL</option>
        <option value="darts">🎯 Darts</option>
        <option value="billiards">🎱 Billiards / Snooker</option>
        <option value="other">🏅 Other Sports</option>
      </select>
    </div>
    <div class="field" style="margin-top:12px">
      <label>Add More Time <span style="font-weight:400;color:var(--muted)">(extend end time)</span></label>
      <select id="edit-rec-extend">
        <option value="0">No change</option>
        <option value="30">+30 minutes</option>
        <option value="60">+1 hour</option>
        <option value="90">+1.5 hours</option>
        <option value="120">+2 hours</option>
        <option value="180">+3 hours</option>
        <option value="240">+4 hours</option>
        <option value="-1">Remove end time (24/7)</option>
      </select>
    </div>
    <div style="display:flex;gap:10px;margin-top:20px">
      <button class="btn btn-outline" style="flex:1" onclick="closeModal('modal-edit-rec')">Cancel</button>
      <button class="btn" style="flex:2" onclick="saveRecordingEdit()">Save Changes</button>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
const api = (path, opts={}) => fetch('/admin/api'+path, {headers:{'Content-Type':'application/json'},...opts}).then(r=>r.json());
// Encode a value for safe embedding inside a single-quoted onclick="fn('...')" attribute.
// encodeURIComponent does NOT escape apostrophes, so names like "Connah's Quay" break the
// JS string and crash with "missing ) after argument list". Escape ' (and ") to %27/%22 —
// decodeURIComponent restores them, so JSON.parse round-trips cleanly.
const encAttr = (obj) => encodeURIComponent(typeof obj === 'string' ? obj : JSON.stringify(obj)).replace(/'/g, '%27').replace(/"/g, '%22');
let _selChannel = null;
let _accType = 'xtream';
let _searchTimer = null;
let _chData = [];  // channel objects stored here; onclick uses index

// ── Tabs ──
function showTab(name, btn) {
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.nav button').forEach(b=>b.classList.remove('active'));
  document.getElementById('tab-'+name).classList.add('active');
  btn.classList.add('active');
  if(name==='recordings') loadRecordings();
  if(name==='accounts') loadAccounts();
  if(name==='groups') loadGroups();
  if(name==='browse') { loadBrowseAccounts(); }
  if(name==='schedule') loadSchedule();
  if(name==='youtube') loadYouTube();
  if(name==='status') loadStatus();
}

// ── Toast ──
function toast(msg, ok=true) {
  const t=document.getElementById('toast');
  t.textContent=msg; t.className='toast '+(ok?'ok':'err'); t.classList.add('show');
  setTimeout(()=>t.classList.remove('show'),3000);
}

// ── Modal ──
function openModal(id){document.getElementById(id).classList.add('open')}
function closeModal(id){document.getElementById(id).classList.remove('open')}

// ── Logout ──
async function logout(){await fetch('/admin/logout');location.href='/admin';}

// ═══════════════════ RECORDINGS ═══════════════════
let _recMap = {};  // slug → full recording object (for edit modal)

async function loadRecordings() {
  const data = await api('/recordings');
  const el = document.getElementById('rec-list');
  if(!data.recordings||!data.recordings.length){
    _recMap = {};
    el.innerHTML='<div class="empty">No active M3U recordings</div>'; return;
  }
  _recMap = {};
  data.recordings.forEach(r => { _recMap[r.slug] = r; });
  el.innerHTML = data.recordings.map((r,ri)=>{
    // Scheduled (pending) recording: not started yet, no worker, no account.
    // Render as a distinct "Scheduled" card with countdown + Cancel — never "Live".
    if (r.pending) {
      const sa = r.scheduled_start_at || 0;
      const mins = Math.max(0, Math.round((sa - Date.now()/1000)/60));
      const when = sa ? (mins>60 ? Math.floor(mins/60)+'h '+(mins%60)+'m' : mins+'m') : '?';
      return `<div class="rec-card" style="border-color:#fbbf24;background:rgba(251,191,36,0.05)">
        <div class="dot" style="background:#fbbf24"></div>
        <div class="rec-info">
          <div class="rec-name">${r.display_name||r.slug}</div>
          <div class="rec-meta">
            <span class="badge" style="background:#2a2410;color:#fbbf24;border:1px solid #fbbf24">⏳ Scheduled</span>
            &nbsp;Starts in ${when} &nbsp;·&nbsp; ${sa?fmtUtc(sa):'time unknown'}
            &nbsp;·&nbsp; <code style="font-size:.75rem">${r.slug}</code>
          </div>
        </div>
        <button class="btn btn-sm btn-danger" onclick="stopRecording('${r.slug}')">✕ Cancel</button>
      </div>`;
    }
    const dur = r.ends_at ? `Ends in ${Math.max(0,Math.round((r.ends_at-Date.now()/1000)/60))}m` : '24/7';
    const status = r.dead ? 'red' : (r.buffering ? 'yellow' : 'green');
    const label  = r.dead ? 'Dead' : (r.buffering ? 'Buffering' : 'Live');
    // Low-bitrate concern: a healthy sports stream is usually 4+ Mbps. If ffprobe
    // reports 0-3 Mbps the stream is likely a low-quality/placeholder feed — flag it.
    const mbps = (typeof r.required_mbps === 'number') ? r.required_mbps : null;
    const lowBitrate = mbps !== null && mbps <= 3;
    const mbpsBadge = mbps !== null
      ? `&nbsp;·&nbsp; <span title="${lowBitrate ? 'Low bitrate — stream may be poor quality or a placeholder feed' : 'Bitrate looks healthy'}" style="font-size:.72rem;font-weight:600;color:${lowBitrate ? '#f59e0b' : 'var(--muted)'}">${lowBitrate ? '⚠️ ' : ''}${mbps} Mbps${r.resolution ? ' · '+r.resolution : ''}</span>`
      : '';
        const startedTxt = r.started_at ? new Date(r.started_at).toISOString().slice(0,19).replace('T',' ')+' UTC' : '—';
        const detailRows = [
          ['Channel name', r.display_name||'—'],
          ['Slug', r.slug],
          ['Stream ID', r.stream_id||'—'],
          ['Account', r.account_id||'—'],
          ['Worker', r.worker||'—'],
          ['Resolution', r.resolution||'—'],
          ['Bitrate', mbps!==null ? mbps+' Mbps' : '—'],
          ['Status', label],
          ['Ends', r.ends_at ? fmtUtc(r.ends_at) : '24/7'],
          ['Started', startedTxt],
          ['Rotated', (r.last_rotated_from&&r.last_rotated_to)?`${r.last_rotated_from} → ${r.last_rotated_to}`:'—'],
          ['Event ID', r.event_id||'—'],
          ['Playlist', `<a href="${r.playlist_url}" target="_blank" style="color:var(--accent)">${r.playlist_url}</a>`],
        ].map(([k,v])=>`<div style="display:flex;gap:8px;padding:2px 0"><span style="color:var(--muted);min-width:110px;font-size:.74rem">${k}</span><span style="font-size:.76rem;word-break:break-all">${v}</span></div>`).join('');
        return `<div class="rec-card" style="flex-wrap:wrap;${r.hidden?'opacity:.55':''}${lowBitrate?';border-color:#f59e0b':''}">
      <div class="dot dot-${status}"></div>
      <div class="rec-info" style="cursor:pointer" onclick="toggleRecDetail(${ri})">
        <div class="rec-name">${r.display_name||r.slug} ${r.hidden?'<span style="color:#f59e0b;font-size:.72rem">⚠ Hidden from backends</span>':''}
          <span id="rec-caret-${ri}" style="color:var(--muted);font-size:.7rem;margin-left:4px">▸</span></div>
        <div class="rec-meta">
          <span class="badge badge-${status}">${label}</span>
          &nbsp;${dur} &nbsp;·&nbsp; <code style="font-size:.75rem">${r.slug}</code>
          ${r.account_id ? `&nbsp;·&nbsp; <span style="color:var(--accent);font-size:.75rem">🔑 ${r.account_id}</span>` : ''}
          ${mbpsBadge}
          ${(r.last_rotated_from && r.last_rotated_to === r.account_id) ? `&nbsp;·&nbsp; <span style="color:#f59e0b;font-size:.72rem" title="Account rotated due to repeated drops">🔄 ${r.last_rotated_from} → ${r.last_rotated_to}</span>` : ''}
          &nbsp;·&nbsp; <a href="${r.playlist_url}" target="_blank" style="color:var(--accent);text-decoration:none" onclick="event.stopPropagation()">▶ playlist</a>
        </div>
      </div>
      <button class="btn btn-sm btn-outline" style="margin-right:4px" onclick="openEditRecording('${r.slug}')">✎ Edit</button>
      <button class="btn btn-sm" style="background:#f59e0b;color:#000;margin-right:4px" onclick="toggleHide('${r.slug}',${r.hidden})">${r.hidden?'Unhide':'Hide'}</button>
      ${r.match_id ? `<button class="btn btn-sm" style="background:${r.match_hidden?'#10b981':'#ef4444'};color:#fff;margin-right:4px" onclick="toggleHideMatch('${encAttr(r.match_id)}',${r.match_hidden})">${r.match_hidden?'Show in Live Now':'Hide from Live Now'}</button>` : ''}
      <button class="btn btn-sm btn-danger" onclick="stopRecording('${r.slug}')">Stop</button>
      <div id="rec-detail-${ri}" style="display:none;width:100%;margin-top:10px;padding:10px 12px;background:#12151c;border-radius:8px;border:1px solid var(--border)">${detailRows}</div>
    </div>`;
  }).join('');
}
function toggleRecDetail(ri){
  const d=document.getElementById('rec-detail-'+ri);
  const c=document.getElementById('rec-caret-'+ri);
  if(!d) return;
  const open = d.style.display==='none';
  d.style.display = open ? 'block' : 'none';
  if(c) c.textContent = open ? '▾' : '▸';
}

async function stopRecording(slug){
  if(!confirm('Stop recording '+slug+'?')) return;
  const r=await api('/recordings/'+slug,{method:'DELETE'});
  toast(r.ok?'Stopped: '+slug:'Error: '+r.error, !!r.ok);
  loadRecordings();
}

async function toggleHide(slug, currentlyHidden){
  const action = currentlyHidden ? 'unhide' : 'hide';
  const r = await api('/recordings/'+slug+'/'+action, {method:'POST'});
  toast(r.ok ? (currentlyHidden?'Visible on all backends':'Hidden from all backends') : 'Error: '+r.error, !!r.ok);
  loadRecordings();
}

function openEditRecording(slug) {
  const rec = _recMap[slug];
  if (!rec) { toast('Recording not found', false); return; }
  document.getElementById('edit-rec-slug').value = rec.slug;
  document.getElementById('edit-rec-name').value = rec.display_name || rec.slug;
  document.getElementById('edit-rec-category').value = '';
  document.getElementById('edit-rec-extend').value = '0';
  openModal('modal-edit-rec');
  setTimeout(() => document.getElementById('edit-rec-name').focus(), 150);
}

async function saveRecordingEdit() {
  const slug      = document.getElementById('edit-rec-slug').value;
  const name      = document.getElementById('edit-rec-name').value.trim();
  const category  = document.getElementById('edit-rec-category').value;
  const extend    = parseInt(document.getElementById('edit-rec-extend').value) || 0;
  if (!name) { toast('Display name required', false); return; }
  const body = { display_name: name };
  if (category) body.categories = [category];
  if (extend !== 0) body.extend_minutes = extend;
  const r = await api('/recordings/'+slug, {method:'PATCH', body: JSON.stringify(body)});
  if (r.ok) {
    closeModal('modal-edit-rec');
    toast('Updated: ' + slug);
    loadRecordings();
  } else {
    toast('Error: ' + (r.error || 'unknown'), false);
  }
}

async function toggleHideMatch(matchIdEnc, currentlyHidden){
  const matchId = decodeURIComponent(matchIdEnc);
  const action = currentlyHidden ? 'unhide' : 'hide';
  const r = await api('/hidden_matches/'+encodeURIComponent(matchId)+'/'+action, {method:'POST'});
  toast(r.ok ? (currentlyHidden?'Match visible in Live Now':'Match hidden from Live Now on all backends') : 'Error: '+r.error, !!r.ok);
  try { loadRecordings(); } catch(e) {}
  try { loadSchedule(); } catch(e) {}
}

// ═══════════════════ ACCOUNTS ═══════════════════

// ── GROUPS ──
let groupsData = [];
async function loadGroups(){
  try {
    const res = await fetch('/admin/api/groups');
    const json = await res.json();
    groupsData = json.groups || [];
    renderGroups();
    
    // Also update the group dropdown in Schedule
    const schedG = document.getElementById('sched-g-sel');
    if(schedG) {
        schedG.innerHTML = '<option value="">-- Or Quick Add Group --</option>' + 
          groupsData.map(g => `<option value="${g.id}">${g.label} (${g.channels.length} ch)</option>`).join('');
    }
  } catch(e) {}
}
function renderGroups(){
  const out = document.getElementById('group-list');
  if(!groupsData.length){ out.innerHTML = '<div class="empty">No groups created yet.</div>'; return; }
  
  let h = '<div class="grid2">';
  for(let g of groupsData) {
    const chStr = g.channels.map(c => typeof c === 'object' ? c.slug : c).join(', ');
    h += `
    <div class="card">
      <div class="section-title" style="margin-bottom:8px">${g.label}</div>
      <div style="font-size:0.8rem;color:var(--muted);margin-bottom:12px;font-family:monospace">${g.id}</div>
      <div style="font-size:0.85rem;color:var(--text);margin-bottom:16px">Channels: ${chStr}</div>
      <button class="btn btn-danger btn-sm" onclick="deleteGroup('${g.id}')">Delete Group</button>
    </div>
    `;
  }
  h += '</div>';
  out.innerHTML = h;
}
async function deleteGroup(id) {
    if(!confirm("Delete this group?")) return;
    await fetch('/admin/api/groups/' + id, {method:'DELETE'});
    loadGroups();
}
// Group builder state
let _groupChannels = [];  // [{stream_id, name}] picked for current group

function openAddGroup() {
    _groupChannels = [];
    const out = document.getElementById('modal-inner');
    out.innerHTML = `
      <h2>Create Channel Group</h2>
      <div class="field"><label>Group ID <span style="font-weight:400;color:var(--muted)">(short slug, e.g. sports-pack)</span></label><input id="g-id" placeholder="sports-pack" required></div>
      <div class="field"><label>Display Label</label><input id="g-label" placeholder="Sports Pack A" required></div>
      <div class="field" style="margin-top:10px">
        <label>Add Channels <span style="font-weight:400;color:var(--muted)">(search all accounts)</span></label>
        <div style="display:flex;gap:8px;align-items:center;margin-bottom:6px">
          <input id="g-ch-search" placeholder="Search channel name…" style="flex:1" oninput="gGroupDebounce()">
          <div class="spin" id="g-ch-spin"></div>
        </div>
        <div id="g-ch-list" style="height:180px;overflow-y:auto;border:1px solid var(--border);border-radius:8px;margin-bottom:8px">
          <div class="empty">Type to search channels</div>
        </div>
        <div style="margin-bottom:4px;font-size:.77rem;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.05em">Added to group</div>
        <div id="g-picked" style="display:flex;flex-wrap:wrap;gap:6px;min-height:28px;padding:6px;border:1px solid var(--border);border-radius:8px;background:var(--card2)">
          <span style="color:var(--muted);font-size:.76rem">No channels added yet</span>
        </div>
      </div>
      <div style="display:flex;gap:10px;margin-top:16px">
        <button class="btn btn-outline" style="flex:1" onclick="closeModal('modal')">Cancel</button>
        <button class="btn" style="flex:1" onclick="submitGroup()">Create Group</button>
      </div>
    `;
    document.getElementById('modal').classList.add('open');
    setTimeout(() => { const el = document.getElementById('g-ch-search'); if(el) el.focus(); }, 150);
}

let _gGroupTimer = null;
function gGroupDebounce() {
  clearTimeout(_gGroupTimer);
  const q = document.getElementById('g-ch-search').value.trim();
  if (!q) { document.getElementById('g-ch-list').innerHTML = '<div class="empty">Type to search channels</div>'; return; }
  _gGroupTimer = setTimeout(() => gGroupSearch(q), 350);
}

let _gGroupAllCh = [];
async function gGroupSearch(q) {
  document.getElementById('g-ch-spin').style.display = 'block';
  document.getElementById('g-ch-list').innerHTML = '<div class="empty">Searching…</div>';
  const data = await api(`/channels?account_id=all&search=${encodeURIComponent(q)}`);
  document.getElementById('g-ch-spin').style.display = 'none';
  _gGroupAllCh = data.channels || [];
  gGroupRenderList();
}

function gGroupRenderList() {
  const el = document.getElementById('g-ch-list');
  if (!_gGroupAllCh.length) { el.innerHTML = '<div class="empty">No channels found</div>'; return; }
  const pickedIds = new Set(_groupChannels.map(c => c.stream_id));
  el.innerHTML = _gGroupAllCh.slice(0, 200).map((c, i) => {
    const picked = pickedIds.has(c.stream_id);
    const chipId = `probe-g-${Math.random().toString(36).slice(2,8)}`;
    return `<div class="ch-item${picked?' selected-ch':''}"
      style="${picked?'background:var(--card2);border-left:3px solid var(--accent);padding-left:11px;':''}">
      ${c.logo ? `<img class="ch-logo" src="${c.logo}" onerror="this.style.display='none'">` : '<div class="ch-logo"></div>'}
      <div style="flex:1;min-width:0" onclick="gGroupToggle(${i})">
        <div class="ch-name">${c.name.replace(/</g,'&lt;')}</div>
        <div class="ch-cat"><span>${(c.category||'—')}</span><code style="font-size:.67rem;color:var(--muted)">#${c.stream_id}</code></div>
      </div>
      <span id="${chipId}" onclick="event.stopPropagation(); probeChannel('${c.stream_id}','${chipId}')" title="Test if stream is live"
        style="cursor:pointer;font-size:1.4rem;padding:4px 10px;opacity:.9;background:#1a3a1a;border-radius:6px;color:#4ade80;border:1px solid #2a5a2a"
        onmouseover="this.style.opacity=1;this.style.background='#2a5a2a'" onmouseout="this.style.opacity=.9;this.style.background='#1a3a1a'">▶</span>
      <span style="color:${picked?'var(--accent)':'var(--muted)'};font-size:1.1rem;padding-right:4px;cursor:pointer" onclick="gGroupToggle(${i})">${picked?'✓':'+' }</span>
    </div>`;
  }).join('');
}

function gGroupToggle(idx) {
  const c = _gGroupAllCh.slice(0, 200)[idx];
  if (!c) return;
  const existing = _groupChannels.findIndex(x => x.stream_id === c.stream_id);
  if (existing >= 0) {
    _groupChannels.splice(existing, 1);
  } else {
    _groupChannels.push({stream_id: c.stream_id, name: c.name});
  }
  gGroupRenderPicked();
  gGroupRenderList();
}

function gGroupRenderPicked() {
  const el = document.getElementById('g-picked');
  if (!_groupChannels.length) {
    el.innerHTML = '<span style="color:var(--muted);font-size:.76rem">No channels added yet</span>';
    return;
  }
  el.innerHTML = _groupChannels.map((c, i) =>
    `<span class="badge badge-blue" style="cursor:pointer;gap:4px" onclick="gGroupRemove(${i})">${c.name} ✕</span>`
  ).join(' ');
}

function gGroupRemove(i) {
  _groupChannels.splice(i, 1);
  gGroupRenderPicked();
  gGroupRenderList();
}

async function submitGroup() {
  const gId = document.getElementById('g-id').value.trim();
  const gLabel = document.getElementById('g-label').value.trim();
  if (!gId) { toast('Group ID required', false); return; }
  if (!gLabel) { toast('Display Label required', false); return; }
  if (!_groupChannels.length) { toast('Add at least one channel', false); return; }
  const channels = _groupChannels.map(c => c.stream_id);
  const r = await fetch('/admin/api/groups', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({id: gId, label: gLabel, channels})
  });
  const data = await r.json();
  if (data.ok || r.ok) {
    closeModal('modal');
    toast(`Group "${gLabel}" created with ${channels.length} channels!`);
    loadGroups();
  } else {
    toast('Error creating group', false);
  }
}
// ─────────────

function _expiryInfo(a){
  // Returns {line, icon, dead} describing an xtream account's expiry/status.
  if(a.type==='m3u') return {line:'', icon:'', dead:false};
  const dead = (a.usable===false);
  let icon='', line='';
  const now=Math.floor(Date.now()/1000);
  if(a.exp_date){
    const d=new Date(a.exp_date*1000);
    const days=Math.floor((a.exp_date-now)/86400);
    const dstr=d.toLocaleDateString(undefined,{year:'numeric',month:'short',day:'numeric'});
    if(a.exp_date<now){ line=`⛔ Expired ${dstr}`; icon='⛔'; }
    else if(days<3){ line=`⚠️ Expires ${dstr} (${days<=0?'today':days+'d'})`; icon='⚠️'; }
    else { line=`Expires ${dstr} (${days}d)`; }
  }
  if(a.status && a.status!=='Active'){
    line = (line?line+' · ':'') + a.status;
    if(a.status!=='Active') icon = icon || '⛔';
  }
  return {line, icon, dead};
}
async function loadAccounts(){
  const data=await api('/accounts');
  const el=document.getElementById('acc-list');
  if(!data.accounts||!data.accounts.length){
    el.innerHTML='<div class="empty">No accounts yet. Add one to get started.</div>'; return;
  }
  el.innerHTML=data.accounts.map(a=>{
    const ex=_expiryInfo(a);
    // Black out expired/disabled accounts: dimmed, struck, clearly not in use.
    const cardStyle = ex.dead ? 'opacity:.45;filter:grayscale(1);' : '';
    const expLine = ex.line
      ? `<div class="meta" style="${ex.dead?'color:#ff6b6b;font-weight:600':(ex.icon==='⚠️'?'color:#e0a800;font-weight:600':'')}">${ex.line}</div>`
      : '';
    const deadBadge = ex.dead ? '<span class="badge badge-red">DISABLED</span>' : '';
    return `
    <div class="account-card" style="${cardStyle}">
      <div style="font-size:1.3rem">${ex.icon || (a.type==='m3u'?'📋':'🔑')}</div>
      <div style="flex:1">
        <div class="name">${a.name}</div>
        <div class="meta">${a.type==='m3u'?'M3U Playlist':'Xtream Codes'} · ${a.server}${a.username ? ' · ' + a.username : ''}</div>
        <div class="meta">Added ${a.added_at}</div>
        ${expLine}
      </div>
      ${deadBadge}
      <span class="badge badge-blue">${a.type}</span>
      <button class="btn btn-sm btn-danger" onclick="deleteAccount('${a.id}')">Remove</button>
    </div>`;}).join('');
}

function setAccType(t){
  _accType=t;
  document.getElementById('tt-xtream').classList.toggle('active',t==='xtream');
  document.getElementById('tt-m3u').classList.toggle('active',t==='m3u');
  document.getElementById('xtream-fields').style.display=t==='xtream'?'block':'none';
  document.getElementById('m3u-fields').style.display=t==='m3u'?'block':'none';
}
function openAddAccount(){
  document.getElementById('acc-name').value='';
  document.getElementById('acc-server').value='';
  document.getElementById('acc-user').value='';
  document.getElementById('acc-pass').value='';
  document.getElementById('acc-m3u-url').value='';
  openModal('modal-acc');
}
async function saveAccount(){
  const name=document.getElementById('acc-name').value.trim();
  if(!name){toast('Name is required',false);return;}
  const body={name,type:_accType};
  if(_accType==='xtream'){
    body.server=document.getElementById('acc-server').value.trim();
    body.username=document.getElementById('acc-user').value.trim();
    body.password=document.getElementById('acc-pass').value.trim();
    if(!body.server||!body.username){toast('Server and username required',false);return;}
  } else {
    body.server=document.getElementById('acc-m3u-url').value.trim();
    if(!body.server){toast('M3U URL required',false);return;}
  }
  const r=await api('/accounts',{method:'POST',body:JSON.stringify(body)});
  if(r.ok){closeModal('modal-acc');toast('Account saved!');loadAccounts();
  loadGroups();}
  else toast('Error: '+r.error,false);
}
async function deleteAccount(id){
  if(!confirm('Remove account '+id+'?'))return;
  const r=await api('/accounts/'+id,{method:'DELETE'});
  toast(r.ok?'Removed':'Error: '+r.error,!!r.ok);
  loadAccounts();
  loadGroups();
}

// ═══════════════════ BROWSE ═══════════════════
async function loadBrowseAccounts(){
  const data=await api('/accounts');
  const sel=document.getElementById('br-account');
  const prev=sel.value;
  sel.innerHTML='<option value="">— Select account —</option>';
  (data.accounts||[]).forEach(a=>{
    const opt=document.createElement('option');
    opt.value=a.id; opt.textContent=`${a.name} (${a.type})`;
    sel.appendChild(opt);
  });
  if(prev) sel.value=prev;
}

function onAccountChange(){
  _selChannel=null;
  updateSelPreview();
  document.getElementById('ch-list').innerHTML='<div class="empty">Loading channels…</div>';
  const id=document.getElementById('br-account').value;
  if(id) loadChannels(false);
}

function debounceSearch(){
  clearTimeout(_searchTimer);
  _searchTimer=setTimeout(()=>loadChannels(false),400);
}

async function loadChannels(refresh=false){
  const acct=document.getElementById('br-account').value;
  if(!acct) return;
  const q=document.getElementById('br-search').value.trim();
  document.getElementById('br-spin').style.display='block';
  const url=`/channels?account_id=${encodeURIComponent(acct)}&search=${encodeURIComponent(q)}${refresh?'&refresh=1':''}`;
  const data=await api(url);
  document.getElementById('br-spin').style.display='none';
  const el=document.getElementById('ch-list');
  if(data.error){el.innerHTML=`<div class="empty" style="color:var(--danger)">${data.error}</div>`;return;}
  const chs=data.channels||[];
  document.getElementById('ch-count').textContent=`${chs.length} channel${chs.length!==1?'s':''} shown`;
  _chData = chs.slice(0, 500);
  el.innerHTML = _chData.map((c, i) => {
    const chipId = `probe-br-${Math.random().toString(36).slice(2,8)}`;
    return `
    <div class="ch-item">
      ${c.logo ? `<img class="ch-logo" src="${c.logo}" onerror="this.style.display='none'">` : '<div class="ch-logo"></div>'}
      <div style="flex:1;cursor:pointer" onclick="selectChannel(${i})">
        <div class="ch-name">${c.name.replace(/</g,'&lt;')}</div>
        <div class="ch-cat">${(c.category||'—').replace(/</g,'&lt;')}</div>
      </div>
      <span id="${chipId}" onclick="event.stopPropagation(); probeChannel('${c.stream_id}','${chipId}')" title="Test if stream is live"
        style="cursor:pointer;font-size:1.4rem;padding:4px 10px;opacity:.9;background:#1a3a1a;border-radius:6px;color:#4ade80;border:1px solid #2a5a2a"
        onmouseover="this.style.opacity=1;this.style.background='#2a5a2a'" onmouseout="this.style.opacity=.9;this.style.background='#1a3a1a'">▶</span>
    </div>`;
  }).join('');
}

function selectChannel(idx) {
  const c = _chData[idx];
  if (!c) return;
  _selChannel = c;
  updateSelPreview();
  // Auto-fill slug and name
  const slug = c.name.toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-|-$/g,'').slice(0,48);
  document.getElementById('rec-slug').value = slug;
  document.getElementById('slug-preview').textContent = slug;
  document.getElementById('rec-display-name').value = c.name;
  document.getElementById('start-rec-btn').disabled = false;
  document.getElementById('start-rec-btn').textContent = '▶ Start Recording';
}

function updateSelPreview(){
  const p=document.getElementById('sel-preview');
  if(!_selChannel){p.style.display='none';return;}
  p.style.display='block';
  document.getElementById('sel-name').textContent=_selChannel.name;
  document.getElementById('sel-meta').textContent=(_selChannel.category||'Unknown')+ ' · '+_selChannel.stream_id;
}

document.getElementById('rec-slug').addEventListener('input',function(){
  document.getElementById('slug-preview').textContent=this.value||'slug';
});

// Flip the Start button label to Schedule when a future UTC time is set.
// Reuses utcInputToEpoch() defined in the YouTube section.
function recUpdateBtn(){
  const btn=document.getElementById('start-rec-btn');
  if(!btn || btn.disabled) return;
  const ep=utcInputToEpoch(document.getElementById('rec-startat').value);
  btn.textContent = (ep && ep > Date.now()/1000) ? '⏱ Schedule Recording' : '▶ Start Recording';
}

async function startRecording(){
  if(!_selChannel){toast('Select a channel first',false);return;}
  const slug=document.getElementById('rec-slug').value.trim();
  const displayName=document.getElementById('rec-display-name').value.trim();
  const duration=parseInt(document.getElementById('rec-duration').value)||0;
  const category=document.getElementById('rec-category').value.trim();
  const acctId=document.getElementById('br-account').value;
  const startAt=utcInputToEpoch(document.getElementById('rec-startat').value);
  if(!slug){toast('Slug is required',false);return;}
  if(!displayName){toast('Display name is required',false);return;}
  const scheduling = startAt && startAt > Date.now()/1000;
  const btn=document.getElementById('start-rec-btn');
  btn.disabled=true; btn.textContent = scheduling ? 'Scheduling…' : 'Starting…';
  const body={
    stream_id:  _selChannel.stream_id,
    stream_url: _selChannel.stream_url,
    slug, display_name: displayName,
    duration_minutes: duration,
    categories: category?[category]:[],
  };
  if(scheduling) body.start_at = startAt;
  const r=await api('/recordings',{method:'POST',body:JSON.stringify(body)});
  btn.disabled=false; recUpdateBtn();
  if(r.ok){
    toast(r.scheduled ? ('⏱ Scheduled for '+fmtUtc(r.scheduled_start_at)) : ('Recording started: '+slug));
    document.getElementById('rec-startat').value='';
    const recBtn = document.querySelector('.nav button');
    showTab('recordings', recBtn);
    recBtn.classList.add('active');
  } else {
    toast('Error: '+r.error, false);
  }
}

// ═══════════════════ STATUS ═══════════════════
let _statusTimer = null;
function _fmtDur(s){
  s = Math.max(0, Math.round(s||0));
  if(s < 60) return s+'s';
  const m = Math.floor(s/60), sec = s%60;
  if(m < 60) return m+'m'+(sec?' '+sec+'s':'');
  const h = Math.floor(m/60);
  return h+'h '+(m%60)+'m';
}
function _statusCard(s, past){
  const up = (typeof s.uptime_pct==='number') ? s.uptime_pct : 0;
  const upColor = up >= 99 ? 'var(--success)' : (up >= 95 ? '#f59e0b' : '#ef4444');
  const dot = past ? '#6b7280' : (s.currently_down ? '#ef4444' : 'var(--success)');
  const dotLabel = past ? 'Ended' : (s.currently_down ? '● Down now' : '● Live');
  const irr = s.irregular_count || 0;
  const samples = (s.irregular_samples||[]).slice(0,6).map(x=>x.dur+'s').join(', ');
  const dn = s.downtime_secs || 0;
  const longest = s.longest_outage_secs || 0;
  const stalls = s.stall_total || 0;
  // Yellow the whole card when the stream had irregular segment durations —
  // those cause visible rewinds/jumps for viewers, so it's a quality warning
  // even when uptime is 100%.
  const warn = irr > 0 && !past;
  const cardBorder = warn ? 'border-color:#f59e0b;background:rgba(245,158,11,0.05)' : '';
  return `<div class="rec-card" style="flex-direction:column;align-items:stretch;gap:8px;${past?'opacity:.72':''}${cardBorder ? ';'+cardBorder : ''}">
    <div style="display:flex;align-items:center;gap:10px">
      <span style="color:${dot};font-size:.72rem;font-weight:700;white-space:nowrap">${dotLabel}</span>
      <span class="rec-name" style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${warn?'⚠️ ':''}${s.display_name||s.slug}</span>
      <span style="font-size:1.05rem;font-weight:700;color:${upColor}">${up}%</span>
    </div>
    <div style="height:6px;background:#1e2330;border-radius:4px;overflow:hidden">
      <div style="height:100%;width:${up}%;background:${upColor}"></div>
    </div>
    <div style="display:flex;gap:14px;flex-wrap:wrap;font-size:.72rem;color:var(--muted)">
      <span title="Total time the public playlist was unreachable/frozen">⬇ Downtime: <b style="color:var(--text)">${_fmtDur(dn)}</b></span>
      <span title="Longest single continuous outage">📉 Worst outage: <b style="color:var(--text)">${_fmtDur(longest)}</b></span>
      <span title="Polls where the playlist media-sequence was frozen">🧊 Stalls: <b style="color:var(--text)">${stalls}</b></span>
      <span title="Segments whose #EXTINF duration deviated from 6.0s">⏱ Irregular segs: <b style="color:${irr?'#f59e0b':'var(--text)'}">${irr}</b></span>
    </div>
    ${irr ? `<div style="font-size:.68rem;color:#f59e0b;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="Sample of off-duration segments">bad durations: ${samples}${irr>6?' …':''}</div>` : ''}
    <div style="font-size:.66rem;color:var(--muted)">
      <code>${s.slug}</code> · ${s.polls_up||0}/${s.polls_total||0} polls ok
      ${s.event_end ? ' · event ends '+new Date(s.event_end*1000).toISOString().slice(0,16).replace('T',' ')+' UTC' : (past?'':' · 24/7')}
      ${past && s.ended_at ? ' · ended '+new Date(s.ended_at*1000).toISOString().slice(0,16).replace('T',' ')+' UTC' : ''}
    </div>
  </div>`;
}
async function loadStatus(){
  const liveEl = document.getElementById('status-live');
  const pastEl = document.getElementById('status-past');
  const meta   = document.getElementById('status-meta');
  if(!liveEl) return;
  try{
    const d = await api('/status');
    const live = d.live||[], past = d.past||[];
    meta.textContent = `${live.length} live · ${past.length} past (kept ${d.ttl_days||2}d) · polling every ${d.poll_secs||6}s via public link`;
    liveEl.innerHTML = live.length ? live.map(s=>_statusCard(s,false)).join('') : '<div class="empty">No live streams being monitored</div>';
    pastEl.innerHTML = past.length ? past.map(s=>_statusCard(s,true)).join('') : '<div class="empty">No past events in the retention window</div>';
  }catch(e){ liveEl.innerHTML = '<div class="empty">Error loading status</div>'; }
}
// Auto-refresh the status tab every 6s only while it is the visible tab.
function _statusAutoRefresh(){
  const tab = document.getElementById('tab-status');
  if(tab && tab.classList.contains('active')) loadStatus();
}
setInterval(_statusAutoRefresh, 6000);

// ── Init ──
loadRecordings();
loadRules();
loadYouTube();

// ═══════════════════ YOUTUBE ═══════════════════
let _ytInfoTimer = null;

async function loadYouTube() {
  const el = document.getElementById('yt-list');
  if (!el) return;
  const rb = document.getElementById('yt-refresh-list-btn');
  if (rb) { rb.disabled = true; rb.textContent = '⏳ Refreshing…'; }
  try {
    const r = await api('/yt/list');
    const streams = r.streams || [];
    if (rb) { rb.disabled = false; rb.textContent = '🔄 Refresh'; }
    if (!streams.length) { el.innerHTML = '<div class="empty">No YouTube streams running</div>'; return; }
    el.innerHTML = streams.map(s => {
      const now = Date.now()/1000;
      let statusBadge, subline, actions;
      if (s.pending) {
        const sa = s.scheduled_start_at || 0;
        const mins = Math.max(0, Math.round((sa-now)/60));
        const when = sa ? (mins>60 ? Math.floor(mins/60)+'h '+(mins%60)+'m' : mins+'m') : '?';
        statusBadge = `<span class="badge" style="background:#2a2410;color:#fbbf24;border:1px solid #fbbf24">⏳ Scheduled</span>`;
        subline = `<span style="color:var(--muted);font-size:.72rem">Starts in ${when} · ${sa?fmtUtc(sa):'time unknown'}</span>`;
        actions = `<button class="btn btn-sm" style="background:#3a1a1a;color:#f87171;border:1px solid #f87171"
            onclick="ytStop('${encAttr(s.slug)}')">✕ Cancel</button>`;
      } else {
        statusBadge = `<span class="badge ${s.running?'badge-green':'badge-red'}">${s.running?'● Live':'⚠ Buffering'}</span>`;
        subline = `<span style="color:var(--muted);font-size:.72rem">${s.segments} segs</span>
            ${s.ends_at ? `<span style="color:var(--muted);font-size:.72rem">Ends in ${Math.max(0,Math.round((s.ends_at-now)/60))}m</span>` : '<span style="color:var(--muted);font-size:.72rem">24/7</span>'}`;
        actions = `<a href="/m3u/${encAttr(s.slug)}/live.m3u8" target="_blank" class="btn btn-sm btn-outline">▶ playlist</a>
          <button class="btn btn-sm btn-outline" onclick="ytRefresh('${encAttr(s.slug)}')">🔄 Refresh</button>
          <button class="btn btn-sm" style="background:#3a1a1a;color:#f87171;border:1px solid #f87171"
            onclick="ytStop('${encAttr(s.slug)}')">■ Stop</button>`;
      }
      return `
      <div class="rec-card" style="flex-direction:row;align-items:center;gap:12px">
        <div style="flex:1;min-width:0">
          <div class="rec-name">${s.display_name || s.slug}</div>
          <div class="rec-meta" style="margin-top:3px;display:flex;gap:6px;flex-wrap:wrap;align-items:center">
            ${statusBadge}
            <span class="badge badge-blue">yt</span>
            ${s.category ? `<span class="badge" style="background:#1e2330;color:var(--muted)">${s.category}</span>` : ''}
            ${subline}
          </div>
          <div style="font-size:.7rem;color:var(--muted);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${s.youtube_url||''}</div>
        </div>
        <div style="display:flex;gap:8px;flex-shrink:0">${actions}</div>
      </div>`;
    }).join('');
  } catch(e) {
    el.innerHTML = '<div class="empty">Error loading YouTube streams</div>';
  } finally {
    if (rb) { rb.disabled = false; rb.textContent = '🔄 Refresh'; }
  }
}

// Convert a UTC epoch (seconds) → value string for <input type=datetime-local>,
// rendered in UTC (NOT browser local) so what you type is what the server uses.
function epochToUtcInput(epoch) {
  const d = new Date(epoch * 1000);
  const p = n => String(n).padStart(2,'0');
  return d.getUTCFullYear()+'-'+p(d.getUTCMonth()+1)+'-'+p(d.getUTCDate())+'T'+p(d.getUTCHours())+':'+p(d.getUTCMinutes());
}
// Parse a datetime-local value as UTC → epoch seconds (Date.UTC ignores TZ).
function utcInputToEpoch(val) {
  if (!val) return 0;
  const m = val.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/);
  if (!m) return 0;
  return Math.floor(Date.UTC(+m[1], +m[2]-1, +m[3], +m[4], +m[5]) / 1000);
}
function fmtUtc(epoch) {
  const d = new Date(epoch*1000);
  const p = n => String(n).padStart(2,'0');
  return d.getUTCFullYear()+'-'+p(d.getUTCMonth()+1)+'-'+p(d.getUTCDate())+' '+p(d.getUTCHours())+':'+p(d.getUTCMinutes())+' UTC';
}
function ytUpdateBtn() {
  const btn = document.getElementById('yt-start-btn');
  if (!btn) return;
  const ep = utcInputToEpoch(document.getElementById('yt-startat').value);
  btn.textContent = (ep && ep > Date.now()/1000) ? '⏱ Schedule' : '▶ Start Stream';
}

let _ytFetchTimer = null;
function ytFetchInfo() {
  clearTimeout(_ytFetchTimer);
  _ytFetchTimer = setTimeout(async () => {
    const url = document.getElementById('yt-url').value.trim();
    if (!url || !url.includes('youtube.com') && !url.includes('youtu.be')) return;
    document.getElementById('yt-info-bar').textContent = 'Fetching info…';
    try {
      const r = await api('/yt/info?url=' + encodeURIComponent(url));
      let bar = r.title ? '📺 ' + r.title : '';
      if (r.title && !document.getElementById('yt-title').value)
        document.getElementById('yt-title').value = r.title;
      if (r.id && !document.getElementById('yt-slug').value)
        document.getElementById('yt-slug').value = 'yt-' + r.id;
      // Upcoming stream: prefill the UTC start field from the page's scheduled time.
      if (r.scheduled_start_at) {
        const sa = r.scheduled_start_at;
        if (!document.getElementById('yt-startat').value)
          document.getElementById('yt-startat').value = epochToUtcInput(sa);
        ytUpdateBtn();
        bar += (bar?'  ':'') + '⏱ Scheduled start: ' + fmtUtc(sa)
             + (sa > Date.now()/1000 ? ' (in ' + Math.round((sa-Date.now()/1000)/60) + ' min)' : ' (already started)');
      } else if (r.is_upcoming) {
        bar += (bar?'  ':'') + '⏳ Upcoming (no time found — set Start at manually)';
      }
      document.getElementById('yt-info-bar').textContent = bar;
    } catch(e) { document.getElementById('yt-info-bar').textContent = ''; }
  }, 800);
}

async function ytStart() {
  const url   = document.getElementById('yt-url').value.trim();
  const slug  = document.getElementById('yt-slug').value.trim();
  const title = document.getElementById('yt-title').value.trim();
  const hours = parseInt(document.getElementById('yt-hours').value) || 24;
  const category = document.getElementById('yt-category').value;
  const startAt = utcInputToEpoch(document.getElementById('yt-startat').value);
  if (!url)  { toast('YouTube URL required', false); return; }
  if (!slug) { toast('Slug required', false); return; }
  const body = {youtube_url:url, slug, display_name:title||slug, duration_hours:hours, category};
  if (startAt && startAt > Date.now()/1000) body.start_at = startAt;
  const r = await api('/yt/start', {method:'POST', body: JSON.stringify(body)});
  const msg = r.ok ? (r.scheduled ? '⏱ Scheduled for '+fmtUtc(r.scheduled_start_at) : '▶ YouTube stream started!')
                   : 'Error: '+(r.error||'unknown');
  toast(msg, !!r.ok);
  if (r.ok) {
    document.getElementById('yt-url').value='';
    document.getElementById('yt-slug').value='';
    document.getElementById('yt-title').value='';
    document.getElementById('yt-startat').value='';
    ytUpdateBtn();
    loadYouTube();
  }
}

async function ytStop(slug) {
  if (!confirm('Stop YouTube stream "'+slug+'"?')) return;
  const r = await api('/yt/stop/'+encodeURIComponent(slug), {method:'DELETE'});
  toast(r.ok ? 'Stream stopped' : 'Error: '+(r.error||'?'), !!r.ok);
  loadYouTube();
}

async function ytRefresh(slug) {
  toast('🔄 Refreshing stream…', true);
  const r = await api('/yt/refresh/'+encodeURIComponent(slug), {method:'POST'});
  toast(r.ok ? '🔄 Stream refreshed (re-extracting URL)' : 'Error: '+(r.error||'?'), !!r.ok);
  setTimeout(loadYouTube, 2500);
}


// ═══════════════════ AUTO RULES ═══════════════════
let _ruleSelSlugs = new Set();
let _ruleAllCh = [];
let _ruleSearchTimer = null;

async function loadRules() {
  const el = document.getElementById('rules-list');
  if (!el) return;
  el.innerHTML = '<div class="empty">Loading…</div>';
  const data = await api('/rules');
  if (!data.rules || !data.rules.length) {
    el.innerHTML = '<div class="empty">No rules yet — click "+ Add Rule" to create one.</div>';
    return;
  }
  el.innerHTML = data.rules.map(r => {
    const chs = (r.channels || []).map(c => `<code style="font-size:.74rem;background:var(--card2);padding:1px 6px;border-radius:4px">${typeof c==='object'?c.slug:c}</code>`).join(' ');
    const endTag = r.end_offset_hours ? `<span style="color:var(--muted);font-size:.77rem">⏱ Auto-end: ${r.end_offset_hours}h</span>` : '';
    return `<div class="card" style="padding:14px;margin-bottom:10px;display:flex;align-items:flex-start;gap:14px">
      <div style="flex:1;min-width:0">
        <div style="font-weight:700;margin-bottom:5px">${(r.match_keys||[]).join(', ')}</div>
        <div style="display:flex;flex-wrap:wrap;gap:4px;align-items:center">${chs} ${endTag}</div>
      </div>
      <div style="display:flex;gap:6px;flex-shrink:0">
        <button class="btn btn-sm btn-outline" onclick="openRuleModal('${encAttr(r)}')">Edit</button>
        <button class="btn btn-sm btn-outline" style="color:var(--danger);border-color:var(--danger)" onclick="deleteRule('${r.id}')">✕</button>
      </div>
    </div>`;
  }).join('');
}

function openRuleModal(ruleJsonEnc) {
  _ruleSelSlugs = new Set();
  _ruleAllCh = [];
  document.getElementById('rule-modal-title').textContent = ruleJsonEnc ? 'Edit Rule' : 'Add Auto Rule';
  document.getElementById('rule-edit-key').value = '';
  document.getElementById('rule-keywords').value = '';
  document.getElementById('rule-end-hours').value = '';
  document.getElementById('rule-ch-search').value = '';
  document.getElementById('rule-ch-list').innerHTML = '<div class="empty">Type to search channels</div>';
  document.getElementById('rule-sel-chips').innerHTML = '';
  _ruleUpdateCount();
  if (ruleJsonEnc) {
    const r = JSON.parse(decodeURIComponent(ruleJsonEnc));
    document.getElementById('rule-edit-key').value = (r.id || '').replace('team:', '');
    document.getElementById('rule-keywords').value = (r.match_keys || []).join(', ');
    if (r.end_offset_hours) document.getElementById('rule-end-hours').value = r.end_offset_hours;
    _ruleSelSlugs = new Set((r.channels || []).map(c => typeof c === 'object' ? c.slug : c));
    _ruleUpdateCount();
    _ruleRenderChips();
  }
  openModal('modal-rule');
  setTimeout(() => { const el = document.getElementById('rule-keywords'); if (el) el.focus(); }, 150);
}

function ruleDebounceSearch() {
  clearTimeout(_ruleSearchTimer);
  const q = document.getElementById('rule-ch-search').value.trim();
  if (!q) { document.getElementById('rule-ch-list').innerHTML = '<div class="empty">Type to search channels</div>'; return; }
  _ruleSearchTimer = setTimeout(() => _ruleSearch(q), 350);
}

async function _ruleSearch(q) {
  document.getElementById('rule-spin').style.display = 'block';
  document.getElementById('rule-ch-list').innerHTML = '<div class="empty">Searching…</div>';
  const data = await api(`/channels?account_id=all&search=${encodeURIComponent(q)}`);
  document.getElementById('rule-spin').style.display = 'none';
  _ruleAllCh = data.channels || [];
  _ruleRenderList();
}

function _ruleRenderList() {
  const el = document.getElementById('rule-ch-list');
  if (!_ruleAllCh.length) { el.innerHTML = '<div class="empty">No channels found</div>'; return; }
  el.innerHTML = _ruleAllCh.slice(0, 300).map((c, i) => {
    const sel = _ruleSelSlugs.has(c.stream_id);
    const bg = sel ? 'background:rgba(99,102,241,.18);border-left:3px solid var(--accent);padding-left:11px;' : '';
    const chkBg = sel ? 'var(--accent)' : 'transparent';
    const chkBdr = sel ? 'var(--accent)' : 'var(--border)';
    const chkMark = sel ? '<span style="color:#000;font-size:.75rem;font-weight:900;line-height:1">&#10003;</span>' : '';
    const logo = c.logo ? `<img class="ch-logo" src="${c.logo}" onerror="this.style.display='none'">` : '<div class="ch-logo"></div>';
    return `<div class="ch-item" style="cursor:pointer;${bg}" onclick="_ruleToggle(${i})">
      <div style="display:flex;align-items:center;justify-content:center;width:20px;height:20px;border-radius:5px;border:2px solid ${chkBdr};background:${chkBg};flex-shrink:0;margin-right:8px;transition:all .15s">${chkMark}</div>
      ${logo}
      <div style="flex:1;min-width:0">
        <div class="ch-name">${c.name.replace(/</g,'&lt;')}</div>
        <div class="ch-cat"><code style="font-size:.67rem;color:var(--muted)">#${c.stream_id}</code></div>
      </div>
    </div>`;
  }).join('');
}

function _ruleToggle(idx) {
  const c = _ruleAllCh.slice(0, 300)[idx];
  if (!c) return;
  if (_ruleSelSlugs.has(c.stream_id)) _ruleSelSlugs.delete(c.stream_id);
  else _ruleSelSlugs.add(c.stream_id);
  _ruleUpdateCount();
  _ruleRenderList();
  _ruleRenderChips();
}

function _ruleUpdateCount() {
  const n = _ruleSelSlugs.size;
  const badge = document.getElementById('rule-sel-count');
  if (badge) { badge.textContent = `${n} selected`; badge.style.display = n > 0 ? 'inline-block' : 'none'; }
}

function _ruleRenderChips() {
  const el = document.getElementById('rule-sel-chips');
  if (!el) return;
  el.innerHTML = [..._ruleSelSlugs].map(s =>
    `<span style="background:var(--card2);border:1px solid var(--border);border-radius:14px;padding:3px 10px;font-size:.75rem;display:inline-flex;align-items:center;gap:6px">
      <code>${s}</code>
      <span onclick="_ruleRemoveChip('${s}')" style="cursor:pointer;color:var(--muted);font-size:.9rem">✕</span>
    </span>`
  ).join('');
}

function _ruleRemoveChip(slug) {
  _ruleSelSlugs.delete(slug);
  _ruleUpdateCount();
  _ruleRenderList();
  _ruleRenderChips();
}

async function saveRule() {
  const kw = document.getElementById('rule-keywords').value.trim();
  if (!kw) { toast('Enter at least one keyword', false); return; }
  if (_ruleSelSlugs.size === 0) { toast('Select at least one channel', false); return; }
  const editKey = document.getElementById('rule-edit-key').value.trim();
  const matchKeys = kw.split(',').map(k => k.trim()).filter(Boolean);
  const teamKey = editKey || matchKeys[0].toLowerCase().replace(/\s+/g, '-');
  const endHours = parseFloat(document.getElementById('rule-end-hours').value) || null;
  const btn = document.getElementById('rule-save-btn');
  btn.disabled = true; btn.textContent = 'Saving…';
  const res = await api('/rules', {method: 'POST', body: JSON.stringify({
    team_key: teamKey, label: matchKeys[0], match_keys: matchKeys,
    channels: [..._ruleSelSlugs], end_offset_hours: endHours,
  })});
  btn.disabled = false; btn.textContent = 'Save Rule';
  if (res.ok) { closeModal('modal-rule'); toast('Rule saved!'); loadRules(); }
  else toast(res.error || 'Save failed', false);
}

async function deleteRule(ruleId) {
  if (!confirm('Delete this rule?')) return;
  const res = await api(`/rules/${encodeURIComponent(ruleId)}`, {method: 'DELETE'});
  if (res.ok) { toast('Rule deleted'); loadRules(); }
  else toast(res.error || 'Delete failed', false);
}

// ═══════════════════ SCHEDULE ═══════════════════
let _schedCat = 'all';
let _schedSearchTimer = null;
let _schedChanNames = {};   // slug → friendly channel name, from /schedule/events
let _assignEventData = null;
let _assignSelSlugs = new Set();  // multi-select: set of stream_id strings
let _assignAllCh = [];
let _assignSearchTimer = null;
let _assignDoRefresh = true;  // refresh toggle state

function setCat(cat, btn) {
  _schedCat = cat;
  document.querySelectorAll('.cat-pill').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  loadSchedule();
}

function schedDebounceSearch() {
  clearTimeout(_schedSearchTimer);
  _schedSearchTimer = setTimeout(loadSchedule, 400);
}

async function loadSchedule(forceRefresh = false) {
  const search = (document.getElementById('sched-search')||{}).value || '';
  const liveEl = document.getElementById('sched-live');
  const todayEl = document.getElementById('sched-today');
  liveEl.innerHTML = '<div class="empty">Loading…</div>';
  todayEl.innerHTML = '<div class="empty">Loading…</div>';

  let evRes = {events:[]}, capRes = {accounts:[], warnings:[]};
  try {
    const params = {category: _schedCat, search};
    if (forceRefresh) params.refresh = "1";
    const qs = new URLSearchParams(params);
    evRes = await api('/schedule/events?' + qs);
    _schedChanNames = evRes.channel_names || {};
  } catch (err) { console.error('Failed to load schedule events:', err); }
  try {
    capRes = await api('/schedule/capacity');
  } catch (err) { console.error('Failed to load capacity:', err); }

  // Capacity bar
  const capEl = document.getElementById('cap-accounts');
  const banner = document.getElementById('cap-banner');
  if (capRes.accounts) {
    capEl.innerHTML = capRes.accounts.map(a => {
      // Expired/disabled accounts are blacked out — shown but never counted as usable.
      if (a.usable === false) {
        const why = a.status && a.status !== 'Active' ? a.status : 'expired';
        return `<span class="badge" title="${a.name} — ${why}, not used" `
             + `style="opacity:.4;filter:grayscale(1);text-decoration:line-through">`
             + `${a.name}: ⛔ ${why}</span>`;
      }
      const busy = !!a.current_slug;
      const cls = busy ? 'badge-red' : 'badge-green';
      const lbl = busy ? `● ${a.current_slug}` : '○ free';
      // ⚠️ warn marker when nearing expiry (<3d) but still usable
      let warn = '';
      if (a.exp_date) {
        const days = Math.floor((a.exp_date - Math.floor(Date.now()/1000)) / 86400);
        if (days < 3) warn = ' ⚠️';
      }
      return `<span class="badge ${cls}" title="${a.name}">${a.name}: ${lbl}${warn}</span>`;
    }).join('') || '<span style="color:var(--muted);font-size:.8rem">No accounts added yet</span>';
  }
  if (capRes.warnings && capRes.warnings.length) {
    banner.style.display = 'block';
    banner.innerHTML = '⚠️ <b>Capacity warnings:</b><br>' + capRes.warnings.map(w => `• ${w}`).join('<br>');
  } else { banner.style.display = 'none'; }

  const all = evRes.events || [];
  let hiddenSet = new Set();
  try {
    const hRes = await api('/hidden_matches');
    hiddenSet = new Set(hRes.hidden || []);
  } catch(e) {}
  all.forEach(e => { e._match_hidden = hiddenSet.has(e.id); });
  const live = all.filter(e => e._is_live);
  const ended = all.filter(e => e._is_ended);
  // Upcoming = not live and not ended (ended events get their own section so they
  // don't clutter Today/Upcoming looking like they're about to start).
  const upcoming = all.filter(e => !e._is_live && !e._is_ended).sort((a,b) => {
    const ta = a.releaseInfo ? new Date(a.releaseInfo.replace('·','')) : NaN;
    const tb = b.releaseInfo ? new Date(b.releaseInfo.replace('·','')) : NaN;
    if(isNaN(ta) && isNaN(tb)) return 0;
    if(isNaN(ta)) return 1;
    if(isNaN(tb)) return -1;
    return ta - tb;
  });
  const nameMap = Object.fromEntries(all.map(e => [e.id, e.name||e.title||e.id]));

  liveEl.innerHTML = live.length ? live.map(renderSchedEvent).join('') : '<div class="empty">No live events right now</div>';
  todayEl.innerHTML = upcoming.length ? upcoming.map(renderSchedEvent).join('') : '<div class="empty">No upcoming events in this category</div>';

  const endedEl = document.getElementById('sched-ended');
  if (endedEl) {
    endedEl.innerHTML = ended.length ? ended.map(renderSchedEvent).join('') : '<div class="empty">No recently ended events</div>';
  }

  const hiddenEl = document.getElementById('sched-hidden');
  if (hiddenEl) {
    const hiddenIds = Array.from(hiddenSet);
    hiddenEl.innerHTML = hiddenIds.length ? hiddenIds.map(id =>
      `<div class="rec-card" style="flex-direction:row;align-items:center;gap:10px">
        <div style="flex:1;min-width:0">
          <div class="rec-name" style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${nameMap[id]||id}</div>
          <div class="rec-meta" style="margin-top:2px;color:var(--muted);font-size:.72rem">ID: ${id}</div>
        </div>
        <button class="btn btn-sm" style="background:#10b981;color:#fff;flex-shrink:0" onclick="toggleHideMatch('${encAttr(id)}',true)">Show in Live Now</button>
      </div>`
    ).join('') : '<div class="empty">None hidden</div>';
  }
}

function renderSchedEvent(ev) {
  const channels = ev.assigned_channels || [];
  const hasChannels = channels.length > 0;
  const cat = ev._category ? `<span style="color:var(--muted);font-size:.72rem">${ev._category.replace('sports_','').replace('_',' ')}</span>` : '';
  const time = ev._is_live ? '' : (ev.releaseInfo ? `<span style="color:var(--muted);font-size:.72rem">🕐 ${ev.releaseInfo}</span>` : '');
  const statusBadge = ev._is_ended
    ? '<span class="badge" style="background:#3a1a1a;color:#f87171">🏁 Ended</span>'
    : (ev._is_live
        ? '<span class="badge badge-red">🔴 LIVE</span>'
        : '<span class="badge badge-blue">⏳ Soon</span>');
  const recBadge = ev._is_recording
    ? '<span class="badge badge-green">● Recording</span>'
    : (hasChannels ? '<span class="badge badge-yellow">⏸ Assigned</span>' : '<span class="badge" style="background:#1e2330;color:var(--muted)">No channel</span>');
  // Long-run warning: recording active for 8+ hours — highlight so you can review/stop
  const _recSecs = ev._recording_started_at ? (Date.now()/1000 - ev._recording_started_at) : 0;
  const _recHrs  = Math.floor(_recSecs / 3600);
  const longRunBadge = (ev._is_recording && _recSecs > 8 * 3600)
    ? `<span class="badge" style="background:#3a2200;color:#fbbf24;border:1px solid #f59e0b" title="Recording has been running for ${_recHrs}h — review if still needed">⚠️ ${_recHrs}h running</span>`
    : '';
  const chips = channels.map(ch => {
    const chipId = `probe-${Math.random().toString(36).slice(2,8)}`;
    const nm = _schedChanNames[ch];
    const label = nm ? `${nm.replace(/</g,'&lt;')} <span style="opacity:.55;font-size:.68rem">#${ch}</span>` : ch;
    return `<span class="badge badge-blue" style="gap:4px;align-items:center;display:inline-flex">
      <span onclick="removeChannel('${encAttr(ev.id)}','${encAttr(ch)}')" style="cursor:pointer" title="${nm?encAttr(nm)+' — click to remove':'click to remove'}">${label} ✕</span>
      <span id="${chipId}" onclick="probeChannel('${encAttr(ch)}','${chipId}')" title="Test if stream is live"
        style="cursor:pointer;font-size:.85rem;padding:0 2px;opacity:.7" onmouseover="this.style.opacity=1" onmouseout="this.style.opacity=.7">▶</span>
    </span>`;
  }).join(' ');
  // Action buttons — context-sensitive
  const forceDeleteBtn = ev._is_live
    ? `<button class="btn btn-sm" style="background:#3a1a1a;color:#f87171;border:1px solid #f87171" onclick="forceDelete('${encAttr(ev.id)}')" title="Immediately stop recording and block this event from restarting">🗑 Force Delete</button>`
    : '';
  const forceStartBtn = hasChannels && !ev._is_recording
    ? `<button class="btn btn-sm" style="background:#1a3a1a;color:#4ade80;border:1px solid #4ade80" onclick="forceStart('${encAttr(ev.id)}','${encAttr(ev.name||ev.title||ev.id)}')" title="Force this event into Live Now and start recording for 15 minutes">▶ Force Start</button>`
    : '';
  const removeAllBtn = hasChannels
    ? `<button class="btn btn-sm" style="background:#3a1a1a;color:#f59e0b;border:1px solid #f59e0b" onclick="removeAllChannels('${encAttr(ev.id)}')" title="Remove all assigned channels">✕ Remove All</button>`
    : '';
  return `<div class="rec-card" style="flex-direction:column;align-items:flex-start;gap:6px;${longRunBadge ? 'border-color:#f59e0b;background:rgba(245,158,11,0.04)' : ''}">
    <div style="display:flex;width:100%;align-items:center;gap:8px">
      <div style="flex:1;min-width:0">
        <div class="rec-name" style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${ev.name||ev.title||ev.id}</div>
        <div class="rec-meta" style="margin-top:3px;display:flex;gap:6px;flex-wrap:wrap;align-items:center">${statusBadge} ${recBadge} ${longRunBadge} ${cat} ${time}</div>
      </div>
      <div style="display:flex;gap:6px;flex-shrink:0;flex-wrap:wrap;justify-content:flex-end">
        ${forceDeleteBtn}${forceStartBtn}${removeAllBtn}
        <button class="btn btn-sm" style="background:${ev._match_hidden?'#10b981':'#ef4444'};color:#fff" onclick="toggleHideMatch('${encAttr(ev.id)}',${!!ev._match_hidden})">${ev._match_hidden?'Show in Live Now':'Hide from Live Now'}</button>
        <button class="btn btn-sm btn-outline" onclick="openAssignModal('${encAttr({id:ev.id,name:ev.name||ev.title,slug:ev.slug||ev.id})}')" title="Assign single channel">+ Channel</button>
        <button class="btn btn-sm" style="background:var(--card2);color:var(--accent);border:1px solid var(--accent)" onclick="openAssignGroupModal('${encAttr({id:ev.id,name:ev.name||ev.title})}')" title="Assign all channels from a group">+ Group</button>
      </div>
    </div>
    <div style="display:flex;flex-wrap:wrap;gap:6px;align-items:center;min-height:18px">${chips||'<span style="color:var(--muted);font-size:.76rem">No channels assigned yet</span>'}${ev.end_offset_hours ? `<span style="color:var(--muted);font-size:.76rem;margin-left:4px">⏱ Auto-end: ${ev.end_offset_hours}h</span>` : ''}</div>
  </div>`;
}

// ── Force Delete (live events) ─────────────────────────────────────────────
async function forceDelete(eventIdEnc) {
  const eventId = decodeURIComponent(eventIdEnc);
  if (!confirm(`Force-delete "${eventId}"?\n\nThis will:\n• Stop the recording immediately\n• Block this event from restarting\n\nIt will NOT restart until you refresh the page (or redeploy).`)) return;
  try {
    const r = await api('/schedule/force-delete', {method:'POST', body: JSON.stringify({event_id: eventId})});
    if (r.ok) {
      toast('🗑 Force-deleted — recording stopped and blocked', true);
      loadSchedule(true);
    } else {
      toast('Force-delete failed: ' + (r.error || 'unknown'), false);
    }
  } catch(e) { toast('Force-delete error: ' + e, false); }
}

// ── Force Start (scheduled events) ────────────────────────────────────────
async function forceStart(eventIdEnc, nameEnc) {
  const eventId = decodeURIComponent(eventIdEnc);
  const name    = decodeURIComponent(nameEnc);
  if (!confirm(`Force-start "${name}"?\n\nThis will:\n• Immediately start the recording\n• Keep it in Live Now for 15 minutes\n• Auto-stop if it's still absent from the live feed after 15 min`)) return;
  try {
    const r = await api('/schedule/force-start', {method:'POST', body: JSON.stringify({event_id: eventId})});
    if (r.ok) {
      toast('▶ Force-started — recording will begin within 60 seconds', true);
      loadSchedule(true);
    } else {
      toast('Force-start failed: ' + (r.error || 'unknown'), false);
    }
  } catch(e) { toast('Force-start error: ' + e, false); }
}

// ── Assign Group Modal ──
let _assignGroupEventData = null;
function openAssignGroupModal(evDataEnc) {
  _assignGroupEventData = JSON.parse(decodeURIComponent(evDataEnc));
  if (!groupsData || !groupsData.length) {
    toast('No groups created yet — go to the Groups tab first', false);
    return;
  }
  const out = document.getElementById('modal-inner');
  out.innerHTML = `
    <h2>Assign Group to Event</h2>
    <p style="color:var(--muted);font-size:.83rem;margin-bottom:14px">Event: <b>${_assignGroupEventData.name || _assignGroupEventData.id}</b></p>
    <p style="color:var(--muted);font-size:.78rem;margin-bottom:12px;padding:8px 12px;background:var(--card2);border-radius:8px;border-left:3px solid var(--accent)">
      🤖 All channels in the selected group will be assigned. Accounts are auto-selected at match time.
    </p>
    <div class="field">
      <label>Select Group</label>
      <div style="display:flex;flex-direction:column;gap:8px;margin-top:6px">
        ${groupsData.map(g => `
          <div class="ch-item" onclick="assignGroupNow('${g.id}')" style="cursor:pointer">
            <div style="flex:1">
              <div class="ch-name">${g.label}</div>
              <div class="ch-cat">${g.channels.length} channel(s): <code style="font-size:.7rem;color:var(--muted)">${g.channels.slice(0,4).join(', ')}${g.channels.length>4?'…':''}</code></div>
            </div>
            <span style="color:var(--accent);font-size:1.1rem">→</span>
          </div>`).join('')}
      </div>
    </div>
    <div class="field" style="margin-top:14px">
      <label>Auto-end after <span style="font-weight:400;color:var(--muted)">(hours from scheduled start — optional)</span></label>
      <input id="group-assign-end-hours" type="number" min="0" step="0.5" placeholder="e.g. 3" style="width:120px">
    </div>
    <div style="margin-top:14px;text-align:right">
      <button class="btn btn-outline btn-sm" onclick="closeModal('modal')">Cancel</button>
    </div>
  `;
  document.getElementById('modal').classList.add('open');
}

async function assignGroupNow(groupId) {
  if (!_assignGroupEventData) return;
  const ev = _assignGroupEventData;
  const endHours = parseFloat(document.getElementById('group-assign-end-hours')?.value) || null;
  const r = await api('/schedule/assign', {
    method: 'POST',
    body: JSON.stringify({
      event_id:         ev.id,
      event_name:       ev.name || ev.id,
      event_slug:       ev.id,
      group_id:         groupId,
      end_offset_hours: endHours,
    })
  });
  closeModal('modal');
  if (r.ok) {
    const g = groupsData.find(x => x.id === groupId || x.id === 'group:' + groupId);
    toast(`Group "${g ? g.label : groupId}" assigned! Accounts auto-selected at match time.`);
    loadSchedule();
  } else {
    toast('Error: ' + r.error, false);
  }
}

async function removeChannel(evIdEnc, slugEnc) {
  const evId = decodeURIComponent(evIdEnc);
  const slug = decodeURIComponent(slugEnc);
  if (!confirm(`Remove channel ${slug} from this event?`)) return;
  const r = await api('/schedule/assign', {method:'DELETE', body: JSON.stringify({event_id: evId, slug})});
  toast(r.ok ? 'Removed' : 'Error: '+r.error, !!r.ok);
  loadSchedule();
}

async function removeAllChannels(evIdEnc) {
  const evId = decodeURIComponent(evIdEnc);
  if (!confirm(`Remove ALL channels from this event?`)) return;
  const r = await api('/schedule/assign', {method:'DELETE', body: JSON.stringify({event_id: evId, slug: '*'})});
  toast(r.ok ? 'All channels removed' : 'Error: '+r.error, !!r.ok);
  loadSchedule();
}

async function probeChannel(slugEnc, chipId) {
  const slug = decodeURIComponent(slugEnc);
  const el = document.getElementById(chipId);
  if (!el) return;
  el.textContent = '⏳';
  el.style.opacity = '1';
  el.onclick = null;
  try {
    const r = await api(`/schedule/probe?slug=${encodeURIComponent(slug)}`);
    if (r.live) {
      el.textContent = '✅';
      el.title = `Live! Segment responded in ${r.latency_ms}ms`;
      el.parentElement.style.background = 'rgba(34,197,94,.25)';
      el.parentElement.style.borderColor = '#22c55e';
    } else {
      el.textContent = '❌';
      el.title = `Dead: ${r.error || 'No segment data'}`;
      el.parentElement.style.background = 'rgba(239,68,68,.2)';
      el.parentElement.style.borderColor = '#ef4444';
    }
  } catch(e) {
    el.textContent = '⚠️';
    el.title = 'Probe request failed';
  }
  setTimeout(() => {
    if(el) { el.onclick = () => probeChannel(slug, chipId); el.style.opacity = '.7'; }
  }, 4000);
}

// ── Assign Modal ──
function openAssignModal(evDataEnc) {
  _assignEventData = JSON.parse(decodeURIComponent(evDataEnc));
  _assignSelSlugs = new Set();
  _assignAllCh = [];
  _assignDoRefresh = true;  // reset toggle to ON each time modal opens
  document.getElementById('assign-event-name').textContent = _assignEventData.name || _assignEventData.id;
  document.getElementById('assign-team-key').value = '';
  document.getElementById('assign-end-hours').value = '';
  document.getElementById('assign-ch-search').value = '';
  document.getElementById('assign-ch-list').innerHTML = '<div class="empty">Type to search channels across all accounts</div>';
  _assignUpdateCount();
  _assignSyncRefreshUI();
  openModal('modal-assign');
  setTimeout(() => { const el = document.getElementById('assign-ch-search'); if(el) el.focus(); }, 150);
}

function _assignToggleRefresh() {
  _assignDoRefresh = !_assignDoRefresh;
  _assignSyncRefreshUI();
}

function _assignSyncRefreshUI() {
  const tog  = document.getElementById('assign-refresh-toggle');
  const knob = document.getElementById('assign-refresh-knob');
  if (!tog) return;
  tog.style.background  = _assignDoRefresh ? 'var(--accent)' : 'var(--border)';
  knob.style.left       = _assignDoRefresh ? '21px' : '3px';
}

function assignDebounceSearch() {
  clearTimeout(_assignSearchTimer);
  const q = document.getElementById('assign-ch-search').value.trim();
  if (!q) {
    document.getElementById('assign-ch-list').innerHTML = '<div class="empty">Type to search channels across all accounts</div>';
    return;
  }
  _assignSearchTimer = setTimeout(() => assignSearchAll(q), 350);
}

async function assignSearchAll(q) {
  document.getElementById('assign-spin').style.display = 'block';
  document.getElementById('assign-ch-list').innerHTML = '<div class="empty">Searching all accounts…</div>';
  // Search across all accounts at once
  const data = await api(`/channels?account_id=all&search=${encodeURIComponent(q)}`);
  document.getElementById('assign-spin').style.display = 'none';
  if (data.error) {
    document.getElementById('assign-ch-list').innerHTML = `<div class="empty" style="color:var(--danger)">${data.error}</div>`;
    return;
  }
  _assignAllCh = data.channels || [];
  assignRenderChannels(_assignAllCh);
}

function assignRenderChannels(chs) {
  const el = document.getElementById('assign-ch-list');
  if (!chs.length) { el.innerHTML = '<div class="empty">No channels found</div>'; return; }
  el.innerHTML = chs.slice(0, 300).map((c, i) => {
    const isSelected = _assignSelSlugs.has(c.stream_id);
    const chipId = `probe-a-${Math.random().toString(36).slice(2,8)}`;
    const checkBg    = isSelected ? 'var(--accent)' : 'transparent';
    const checkBdr   = isSelected ? 'var(--accent)' : 'var(--border)';
    const checkMark  = isSelected ? '<span style="color:#000;font-size:.75rem;font-weight:900;line-height:1">&#10003;</span>' : '';
    const rowBg      = isSelected ? 'background:rgba(99,102,241,.18);border-left:3px solid var(--accent);padding-left:11px;' : '';
    const logo       = c.logo ? `<img class="ch-logo" src="${c.logo}" onerror="this.style.display='none'">` : '<div class="ch-logo"></div>';
    const acct       = c.account_name ? `<span style="color:var(--muted);font-size:.67rem">${c.account_name}</span>` : '';
    return `<div class="ch-item" id="ch-row-${i}" style="cursor:pointer;${rowBg}" onclick="assignToggleCh(${i})">
      <div style="display:flex;align-items:center;justify-content:center;width:20px;height:20px;border-radius:5px;border:2px solid ${checkBdr};background:${checkBg};flex-shrink:0;margin-right:8px;transition:all .15s">
        ${checkMark}
      </div>
      ${logo}
      <div style="flex:1;min-width:0">
        <div class="ch-name" style="white-space:normal;word-break:break-word">${c.name.replace(/</g,'&lt;')}</div>
        <div class="ch-cat" style="display:flex;gap:6px;flex-wrap:wrap;align-items:center">
          <span>${(c.category||'\u2014').replace(/</g,'&lt;')}</span>
          <code style="font-size:.67rem;color:var(--muted)">#${c.stream_id}</code>
          ${acct}
        </div>
      </div>
      <span id="${chipId}" onclick="event.stopPropagation(); probeChannel('${c.stream_id}','${chipId}')" title="Test if stream is live"
        style="cursor:pointer;font-size:1.4rem;padding:4px 10px;opacity:.9;background:#1a3a1a;border-radius:6px;color:#4ade80;border:1px solid #2a5a2a"
        onmouseover="this.style.opacity=1;this.style.background='#2a5a2a'" onmouseout="this.style.opacity=.9;this.style.background='#1a3a1a'">▶</span>
    </div>`;
  }).join('');
}

function assignToggleCh(idx) {
  const c = _assignAllCh.slice(0, 300)[idx];
  if (!c) return;
  const sid = c.stream_id || '';
  if (_assignSelSlugs.has(sid)) {
    _assignSelSlugs.delete(sid);
  } else {
    _assignSelSlugs.add(sid);
  }
  _assignUpdateCount();
  assignRenderChannels(_assignAllCh);
}

function _assignUpdateCount() {
  const n = _assignSelSlugs.size;
  const badge = document.getElementById('assign-sel-count');
  const btn   = document.getElementById('assign-save-btn');
  if (badge) {
    badge.textContent = `${n} selected`;
    badge.style.display = n > 0 ? 'inline-block' : 'none';
  }
  if (btn) btn.textContent = n > 1 ? `Add ${n} Channels` : 'Add Selected Channel';
}

async function saveAssignment() {
  if (!_assignEventData) return;
  const teamKey = document.getElementById('assign-team-key').value.trim();
  if (_assignSelSlugs.size === 0) { toast('Select at least one channel first', false); return; }

  const btn = document.getElementById('assign-save-btn');
  btn.disabled = true;
  btn.textContent = 'Saving…';

  const endHours = parseFloat(document.getElementById('assign-end-hours').value) || null;
  const baseBody = {
    event_id:         _assignEventData.id,
    event_name:       _assignEventData.name || _assignEventData.id,
    event_slug:       _assignEventData.slug || _assignEventData.id,
    team_key:         teamKey || null,
    end_offset_hours: endHours,
  };

  // Build slug→name map for the selected channels so the backend can store
  // friendly names for the schedule chips (falls back to slug if unknown).
  const nameMap = {};
  _assignAllCh.forEach(c => { if (c && c.stream_id) nameMap[String(c.stream_id)] = c.name || ''; });

  // Send all slugs in parallel
  const slugs = [..._assignSelSlugs];
  const results = await Promise.all(
    slugs.map(slug => api('/schedule/assign', {method:'POST', body: JSON.stringify({...baseBody, slug, channel_names: {[slug]: nameMap[slug] || ''}})}))
  );

  btn.disabled = false;
  const failed = results.filter(r => !r.ok);
  if (failed.length === 0) {
    closeModal('modal-assign');
    toast(`✅ ${slugs.length} channel${slugs.length > 1 ? 's' : ''} assigned! Accounts auto-selected at match time.`);
    loadSchedule();
  } else {
    const ok = results.length - failed.length;
    toast(`⚠️ ${ok} assigned, ${failed.length} failed: ${failed[0].error || 'unknown'}`, false);
    _assignUpdateCount();
  }
}
</script>
</body></html>"""


# ══════════════════════════════════════════════════════════════════════════════
# API Handlers
# ══════════════════════════════════════════════════════════════════════════════

async def _handle_login(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:
        return web.Response(status=400, text=json.dumps({"error": "Bad request"}),
                            content_type="application/json")
    if body.get("password") == ADMIN_PASSWORD:
        token = _new_session()
        resp = web.Response(status=200, text=json.dumps({"ok": True}),
                            content_type="application/json")
        resp.set_cookie("admin_session", token, max_age=_SESSION_TTL, httponly=True)
        return resp
    return web.Response(status=401, text=json.dumps({"error": "Wrong password"}),
                        content_type="application/json")


async def _handle_logout(request: web.Request) -> web.Response:
    token = _get_session(request)
    if token:
        _sessions.pop(token, None)
    resp = web.HTTPFound("/admin")
    resp.del_cookie("admin_session")
    return resp


async def _handle_root(request: web.Request) -> web.Response:
    if _valid_session(_get_session(request)):
        raise web.HTTPFound("/admin/panel")
    return web.Response(status=200, text=_LOGIN_HTML, content_type="text/html")


@_require_auth
async def _handle_panel(request: web.Request) -> web.Response:
    return web.Response(status=200, text=_panel_html(), content_type="text/html")


@_require_auth
async def _handle_list_accounts(request: web.Request) -> web.Response:
    return web.Response(
        text=json.dumps({"accounts": list_accounts()}),
        content_type="application/json",
    )


@_require_auth
async def _handle_add_account(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:
        return web.Response(status=400, text=json.dumps({"error": "Bad JSON"}),
                            content_type="application/json")
    name   = body.get("name", "").strip()
    atype  = body.get("type", "xtream")
    server = body.get("server", "").strip()
    user   = body.get("username", "").strip()
    pwd    = body.get("password", "").strip()
    if not name or not server:
        return web.Response(status=400, text=json.dumps({"error": "name and server required"}),
                            content_type="application/json")
    acc_id = add_account(name, atype, server, user, pwd)
    return web.Response(text=json.dumps({"ok": True, "id": acc_id}),
                        content_type="application/json")


@_require_auth
async def _handle_delete_account(request: web.Request) -> web.Response:
    acc_id = request.match_info["id"]
    ok = remove_account(acc_id)
    return web.Response(text=json.dumps({"ok": ok, "error": "" if ok else "Not found"}),
                        content_type="application/json")


@_require_auth
async def _handle_channels(request: web.Request) -> web.Response:
    acc_id  = request.rel_url.query.get("account_id", "")
    search  = request.rel_url.query.get("search", "")
    refresh = request.rel_url.query.get("refresh", "") == "1"
    if not acc_id:
        return web.Response(status=400, text=json.dumps({"error": "account_id required"}),
                            content_type="application/json")
    loop = asyncio.get_running_loop()

    try:
        from iptv_accounts import _load_accounts, fetch_channels
        if acc_id == "all":
            accounts = _load_accounts()
            if not accounts:
                return web.Response(text=json.dumps({"channels": [], "error": "No accounts configured"}),
                                    content_type="application/json")
            from iptv_accounts import _channel_cache
            warm_id = next((a for a in accounts if a in _channel_cache), next(iter(accounts)))
            result = await loop.run_in_executor(None, fetch_channels, warm_id, search, refresh)
            for ch in result.get("channels", []):
                ch["account_name"] = "any free account"
            return web.Response(text=json.dumps(result), content_type="application/json")

        result = await loop.run_in_executor(None, fetch_channels, acc_id, search, refresh)
        return web.Response(text=json.dumps(result), content_type="application/json")

    except Exception as exc:
        logger.exception("_handle_channels error for account=%s search=%r: %s", acc_id, search, exc)
        return web.Response(
            status=500,
            text=json.dumps({"error": str(exc), "channels": [], "total": 0}),
            content_type="application/json",
        )


@_require_auth
async def _handle_list_recordings(request: web.Request) -> web.Response:
    from config import LEAF_SERVER_PUBLIC_URL
    recordings = []

    if _LEAF_MODE == "master":
        from leaf_server import _parse_workers, _slug_to_worker
        from worker_client import status_of_worker
        workers = list(_parse_workers().items())
        statuses = await asyncio.gather(*[status_of_worker(wurl) for _, wurl in workers])
        # Display-only slug→name map so a numeric-slug channel (e.g. 1527629) shows
        # a friendly name in the recordings list when its meta display_name is
        # missing or is just the slug itself. Never read by the recording path.
        try:
            from event_channel_store import get_channel_names
            _chan_names = get_channel_names()
        except Exception:
            _chan_names = {}
        seen_slugs: set = set()
        for (wid, wurl), status in zip(workers, statuses):
            if not status:
                continue
            for slug, info in status.items():
                if slug in seen_slugs:
                    logger.warning("list_recordings: '%s' on multiple workers — skipping duplicate from %s", slug, wid)
                    continue
                seen_slugs.add(slug)
                # Heal missing routing entry so playlist links work without restart
                if slug not in _slug_to_worker:
                    logger.warning("list_recordings: healing _slug_to_worker '%s' → %s", slug, wurl)
                    _slug_to_worker[slug] = wurl
                if info.get("stream_info") and slug in _recording_meta:
                    _recording_meta[slug]["stream_info"] = info["stream_info"]
                meta = _recording_meta.get(slug, {})
                event_id = meta.get("event_id", "")
                raw_match_id = event_id.split(":", 1)[-1] if ":" in event_id else event_id
                segs = info.get("segments", 0)
                si   = info.get("stream_info") or {}
                try:
                    from datetime import datetime, timezone
                    age = (datetime.now(timezone.utc) - datetime.fromisoformat(info["started_at"].replace("Z", "+00:00"))).total_seconds()
                except Exception:
                    age = 0
                dead = (segs == 0 and age > 90) or (bool(si) and si.get("video_bitrate_kbps", 9999) < 300)
                _dn = meta.get("display_name")
                if not _dn or _dn == slug:
                    _dn = _chan_names.get(slug) or _dn or slug
                recordings.append({
                    "slug":              slug,
                    "display_name":      _dn,
                    "account_id":        meta.get("account_id", ""),
                    "last_rotated_from": meta.get("last_rotated_from", ""),
                    "last_rotated_to":   meta.get("last_rotated_to", ""),
                    "stream_id":         meta.get("stream_id", ""),
                    "ends_at":           meta.get("ends_at"),
                    "dead":              dead,
                    "buffering":         not dead and segs < 3,
                    "started_at":        info.get("started_at", ""),
                    "playlist_url":      f"{LEAF_SERVER_PUBLIC_URL}/m3u/{slug}/live.m3u8",
                    "hidden":            _hidden_slugs.get(slug, 0) > time.time(),
                    "worker":            wid,
                    "required_mbps":     si.get("required_mbps"),
                    "video_bitrate_kbps": si.get("video_bitrate_kbps"),
                    "resolution":        si.get("resolution", ""),
                    "event_id":          event_id,
                    "match_id":          raw_match_id,
                    "match_hidden": (_hidden_match_ids.get(raw_match_id, 0) > time.time()) if raw_match_id else False,
                })
        # Append manually-SCHEDULED IPTV recordings that haven't started yet. They
        # are not on any worker (no account claimed until start), so the worker-status
        # scan above never sees them — surface them here so they show as "Scheduled"
        # and can be cancelled. Skip youtube (handled by the YouTube tab) and any
        # slug already listed as live above.
        for slug, meta in _recording_meta.items():
            if not meta.get("pending_start"):
                continue
            if meta.get("source") == "youtube" or slug in seen_slugs:
                continue
            _pdn = meta.get("display_name")
            if not _pdn or _pdn == slug:
                _pdn = _chan_names.get(slug) or _pdn or slug
            recordings.append({
                "slug":              slug,
                "display_name":      _pdn,
                "account_id":        "",
                "stream_id":         meta.get("stream_id", ""),
                "ends_at":           meta.get("ends_at"),
                "dead":              False,
                "buffering":         False,
                "pending":           True,
                "scheduled_start_at": meta.get("scheduled_start_at"),
                "started_at":        "",
                "playlist_url":      f"{LEAF_SERVER_PUBLIC_URL}/m3u/{slug}/live.m3u8",
                "hidden":            False,
                "worker":            "",
                "categories":        meta.get("categories", []),
            })
        return web.Response(text=json.dumps({"recordings": recordings}),
                            content_type="application/json")

    for slug, state in m3u_recordings.items():
        meta     = _recording_meta.get(slug, {})
        ends_at  = meta.get("ends_at")
        buffering = build_m3u_playlist(state) is None
        event_id  = meta.get("event_id", "")
        raw_match_id = event_id.split(":", 1)[-1] if ":" in event_id else event_id
        si = meta.get("stream_info") or {}
        recordings.append({
            "slug":              slug,
            "display_name":      meta.get("display_name") or getattr(state, "title", slug),
            "account_id":        meta.get("account_id", ""),
            "last_rotated_from": meta.get("last_rotated_from", ""),
            "last_rotated_to":   meta.get("last_rotated_to", ""),
            "stream_id":         meta.get("stream_id", ""),
            "ends_at":           ends_at,
            "dead":              False,
            "buffering":         buffering,
            "started_at":        getattr(state, "started_at", ""),
            "playlist_url":      f"{LEAF_SERVER_PUBLIC_URL}/m3u/{slug}/live.m3u8",
            "hidden":            _hidden_slugs.get(slug, 0) > time.time(),
            "required_mbps":     si.get("required_mbps"),
            "video_bitrate_kbps": si.get("video_bitrate_kbps"),
            "resolution":        si.get("resolution", ""),
            "event_id":          event_id,
            "match_id":          raw_match_id,
            "match_hidden": (_hidden_match_ids.get(raw_match_id, 0) > time.time()) if raw_match_id else False,
        })
    return web.Response(text=json.dumps({"recordings": recordings}),
                        content_type="application/json")


async def _iptv_launch_now(slug: str) -> bool:
    """Launch a SCHEDULED IPTV recording whose start time has arrived.

    Reads the pending meta for `slug` (persisted by _handle_start_recording with
    pending_start=True), claims a free account NOW (not at schedule time — accounts
    are scarce and must not be held idle until kickoff), builds the stream URL,
    starts it on the least-loaded worker, and arms auto-stop. Returns True on
    success. On failure leaves pending_start set so the poller retries next pass.
    Master-mode only (the poller only runs on master).
    """
    meta = _recording_meta.get(slug)
    if not meta:
        return False
    stream_id    = meta.get("stream_id", "")
    display_name = meta.get("display_name", slug)
    categories   = meta.get("categories", [])
    duration_min = int(meta.get("duration_minutes", 0) or 0)
    if not stream_id:
        logger.warning("iptv schedule: '%s' has no stream_id — cannot launch", slug)
        return False

    from iptv_accounts import async_claim_free_account, async_mark_free
    acc_id, claimed_acc = await async_claim_free_account(slug)
    if not acc_id:
        logger.warning("iptv schedule: no free account for '%s' — will retry", slug)
        return False
    server   = claimed_acc.get("server", "").rstrip("/")
    username = claimed_acc.get("username", "")
    password = claimed_acc.get("password", "")
    stream_url = f"{server}/live/{username}/{password}/{stream_id}.m3u8"
    headers = {"user-agent": "okhttp/5.0.0-alpha.2"}

    from iptv_scheduler import _pick_least_loaded_worker
    from worker_client import start_on_worker
    from leaf_server import _slug_to_worker
    pick = await _pick_least_loaded_worker()
    if not pick:
        await async_mark_free(acc_id)
        logger.warning("iptv schedule: no workers available for '%s' — will retry", slug)
        return False
    worker_id, worker_url = pick
    # 24x7 (no duration) admin channels are standalone → the recorder must never
    # give up on them via the zombie guard. Fixed-duration events can still give up.
    _standalone = (duration_min == 0)
    ok = await start_on_worker(worker_url, slug, stream_url, headers, list(RESIDENTIAL_PROXIES), standalone=_standalone)
    if not ok:
        await async_mark_free(acc_id)
        logger.warning("iptv schedule: worker %s failed to start '%s' — will retry", worker_id, slug)
        return False
    _slug_to_worker[slug] = worker_url

    ends_at = (time.time() + duration_min * 60) if duration_min > 0 else None
    _recording_meta[slug] = {
        "display_name": display_name,
        "account_id":   acc_id,
        "stream_id":    stream_id,
        "ends_at":      ends_at,
        "categories":   categories,
        "started_at":   time.time(),
        "standalone":   True,
        "scheduled_start_at": None,
        "pending_start":      False,
        "duration_minutes":   duration_min,
    }
    await async_save_recording_meta(slug, _recording_meta[slug])

    if ends_at:
        async def _auto_stop(_slug=slug, _dur=duration_min):
            await asyncio.sleep(max(1, _dur * 60))
            from leaf_server import _slug_to_worker as s2w
            from worker_client import stop_on_worker
            wurl = s2w.pop(_slug, None)
            if wurl:
                await stop_on_worker(wurl, _slug)
            _m = _recording_meta.pop(_slug, None)
            await async_delete_recording_meta(_slug)
            _aid = (_m or {}).get("account_id")
            if _aid:
                from iptv_accounts import async_mark_free as _amf
                await _amf(_aid)
            logger.info("Auto-stopped scheduled recording '%s' after %dm", _slug, _dur)
        asyncio.create_task(_auto_stop())

    logger.info("IPTV scheduled recording STARTED: %s → worker %s (acc=%s)", slug, worker_id, acc_id)
    return True


@_require_auth
async def _handle_start_recording(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:
        return web.Response(status=400, text=json.dumps({"error": "Bad JSON"}),
                            content_type="application/json")

    slug         = body.get("slug", "").strip().lower().replace(" ", "-")
    display_name = body.get("display_name", slug)
    stream_id    = body.get("stream_id", "")
    stream_url   = body.get("stream_url", "")
    duration_min = int(body.get("duration_minutes", 0))
    categories   = body.get("categories", [])
    # start_at: optional UTC epoch seconds. If set and in the future, the channel
    # is SCHEDULED — persisted to Mongo as pending WITHOUT claiming an account or
    # touching a worker (accounts are scarce; we claim only when it actually starts).
    # The IPTV schedule poller fires it via _iptv_launch_now when now >= start_at.
    try:
        start_at = int(body.get("start_at") or 0) or None
    except (TypeError, ValueError):
        start_at = None

    if not slug:
        return web.Response(text=json.dumps({"error": "slug required"}),
                            content_type="application/json")

    # ── Scheduled path: persist pending and return without claiming/starting ────
    if start_at and start_at > time.time() + 5:
        if not stream_id:
            return web.Response(text=json.dumps({"error": "stream_id required to schedule"}),
                                content_type="application/json")
        _recording_meta[slug] = {
            "display_name": display_name,
            "account_id":   "",
            "stream_id":    stream_id,
            "ends_at":      None,   # computed at launch from duration
            "categories":   categories,
            "started_at":   None,
            "standalone":   True,
            "scheduled_start_at": start_at,
            "pending_start":      True,
            "duration_minutes":   duration_min,
        }
        await async_save_recording_meta(slug, _recording_meta[slug])
        logger.info("IPTV recording SCHEDULED: %s (stream_id=%s start_at=%d UTC dur=%dm)",
                    slug, stream_id, start_at, duration_min)
        return web.Response(text=json.dumps({"ok": True, "slug": slug, "scheduled": True,
                                             "scheduled_start_at": start_at}),
                            content_type="application/json")

    # Auto-pick a free account atomically via motor — stream_id always wins over client-provided stream_url
    acc_id = ""
    if stream_id:
        from iptv_accounts import async_claim_free_account, async_mark_free
        # Claim the account under `slug` (the name the worker records under and
        # that reconcile matches against the workers' active set), NOT `stream_id`.
        # For admin channels slug is a friendly name (e.g. uk-sky-sports-golf-hevc-hd)
        # while stream_id is the numeric channel id (621647). Marking the account
        # busy under stream_id made reconcile see 'slug not on any worker' and
        # ghost-free the account, letting another stream steal it mid-recording.
        # The stream URL still uses stream_id — that's the provider's channel id.
        acc_id, _claimed_acc = await async_claim_free_account(slug)
        if not acc_id:
            return web.Response(text=json.dumps({"error": "No free accounts available — all accounts are busy"}),
                                content_type="application/json")
        server   = _claimed_acc.get("server", "").rstrip("/")
        username = _claimed_acc.get("username", "")
        password = _claimed_acc.get("password", "")
        stream_url = f"{server}/live/{username}/{password}/{stream_id}.m3u8"
    if not stream_url:
        return web.Response(text=json.dumps({"error": "stream_id required"}),
                            content_type="application/json")

    if _LEAF_MODE != "master" and slug in active_m3u_tasks:
        # Release the account we just claimed — nothing new will record on it.
        if acc_id:
            from iptv_accounts import async_mark_free
            await async_mark_free(acc_id)
        return web.Response(text=json.dumps({"error": f"Recording '{slug}' already running"}),
                            content_type="application/json")

    headers = {"user-agent": "okhttp/5.0.0-alpha.2"}

    if _LEAF_MODE == "master":
        from iptv_scheduler import _pick_least_loaded_worker
        from worker_client import start_on_worker
        from leaf_server import _slug_to_worker
        pick = await _pick_least_loaded_worker()
        if not pick:
            if acc_id:
                from iptv_accounts import async_mark_free as _amf
                await _amf(acc_id)
            return web.Response(text=json.dumps({"error": "No workers available"}),
                                content_type="application/json")
        worker_id, worker_url = pick
        # Admin-added 24x7 channel (no duration) → standalone: the worker's zombie
        # guard must never permanently kill it on a provider slate/blip.
        _is_standalone = duration_min <= 0
        ok = await start_on_worker(worker_url, slug, stream_url, headers, list(RESIDENTIAL_PROXIES), standalone=_is_standalone)
        if not ok:
            if acc_id:
                from iptv_accounts import async_mark_free as _amf2
                await _amf2(acc_id)
            return web.Response(text=json.dumps({"error": f"Worker {worker_id} failed to start"}),
                                content_type="application/json")
        _slug_to_worker[slug] = worker_url
        logger.info("Admin routed recording %s to worker %s", slug, worker_id)
    else:
        start_m3u_recording(slug, stream_url, headers, RESIDENTIAL_PROXIES, standalone=(duration_min <= 0))

    if _LEAF_MODE != "master":
        async def _set_meta():
            for _ in range(20):
                if slug in m3u_recordings:
                    m3u_recordings[slug].title = display_name
                    if hasattr(m3u_recordings[slug], "categories"):
                        m3u_recordings[slug].categories = categories
                    break
                await asyncio.sleep(0.1)
        asyncio.create_task(_set_meta())

    ends_at = (time.time() + duration_min * 60) if duration_min > 0 else None

    _recording_meta[slug] = {
        "display_name": display_name,
        "account_id":   acc_id,
        "stream_id":    stream_id,
        "ends_at":      ends_at,
        "categories":   categories,
        "started_at":   time.time(),
    }
    await async_save_recording_meta(slug, _recording_meta[slug])

    # Account was already claimed atomically by async_claim_free_account above.

    # Schedule auto-stop if duration set
    if ends_at:
        async def _auto_stop():
            await asyncio.sleep(duration_min * 60)
            if _LEAF_MODE == "master":
                from leaf_server import _slug_to_worker
                from worker_client import stop_on_worker
                wurl = _slug_to_worker.pop(slug, None)
                if wurl:
                    await stop_on_worker(wurl, slug)
            elif slug in active_m3u_tasks:
                stop_m3u_recording(slug)
            # Free the account unconditionally on auto-stop expiry.
            _meta = _recording_meta.pop(slug, None)
            await async_delete_recording_meta(slug)
            _acc = (_meta or {}).get("account_id")
            if _acc:
                from iptv_accounts import async_mark_free
                await async_mark_free(_acc)
            logger.info("Auto-stopped recording '%s' after %dm", slug, duration_min)
        asyncio.create_task(_auto_stop())

    logger.info("Admin started recording: %s → %s (duration=%dm)", slug, stream_url[:80], duration_min)
    return web.Response(text=json.dumps({"ok": True, "slug": slug}),
                        content_type="application/json")


@_require_auth
async def _handle_stop_recording(request: web.Request) -> web.Response:
    slug = request.match_info["slug"]

    if _LEAF_MODE == "master":
        import iptv_scheduler as _sched
        from leaf_server import _slug_to_worker
        # Pending schedule that hasn't started yet: no worker, no account claimed.
        # Just drop the pending meta so the poller never fires it. Must be checked
        # FIRST — it's not in _scheduler_recordings and not on any worker.
        if _recording_meta.get(slug, {}).get("pending_start"):
            _recording_meta.pop(slug, None)
            await async_delete_recording_meta(slug)
            logger.info("Cancelled scheduled recording '%s' (was pending)", slug)
            return web.Response(text=json.dumps({"ok": True, "cancelled_pending": True}),
                                content_type="application/json")
        if slug in _sched._scheduler_recordings:
            _sched._stop_scheduler_recording(slug)
            _sched._stop_miss_counts.pop(slug, None)
            _slug_to_worker.pop(slug, None)
        else:
            from worker_client import stop_on_worker
            from leaf_server import _parse_workers
            from worker_client import status_of_worker
            worker_url = _slug_to_worker.pop(slug, None)
            if not worker_url:
                # Fallback: scan all workers for this slug
                for wurl in _parse_workers().values():
                    st = await status_of_worker(wurl)
                    if st and slug in st:
                        worker_url = wurl
                        break
            if not worker_url:
                return web.Response(text=json.dumps({"error": "Not found"}),
                                    content_type="application/json")
            # Free the account whether or not the worker was reachable — an
            # unreachable worker must not leave the account ghost-locked forever.
            await stop_on_worker(worker_url, slug)
            from iptv_accounts import async_load_accounts, async_mark_free
            for aid, acc in (await async_load_accounts()).items():
                if acc.get("current_slug") == slug:
                    await async_mark_free(aid)
        _recording_meta.pop(slug, None)
        await async_delete_recording_meta(slug)
        return web.Response(text=json.dumps({"ok": True}), content_type="application/json")

    if slug not in active_m3u_tasks:
        return web.Response(text=json.dumps({"error": "Not found"}),
                            content_type="application/json")
    import iptv_scheduler as _sched
    if slug in _sched._scheduler_recordings:
        _sched._stop_scheduler_recording(slug)
        _sched._stop_miss_counts.pop(slug, None)
    else:
        # Admin-started recording — free account if it was tracked
        meta = _recording_meta.get(slug, {})
        acc_id = meta.get("account_id")
        stop_m3u_recording(slug)
        if acc_id:
            from iptv_accounts import async_mark_free
            await async_mark_free(acc_id)
    _recording_meta.pop(slug, None)
    await async_delete_recording_meta(slug)
    return web.Response(text=json.dumps({"ok": True}), content_type="application/json")


@_require_auth
async def _handle_patch_recording(request: web.Request) -> web.Response:
    """PATCH /admin/api/recordings/{slug} — update display name, categories, or end time."""
    slug = request.match_info["slug"]
    try:
        body = await request.json()
    except Exception:
        return web.Response(status=400, text=json.dumps({"error": "Bad JSON"}),
                            content_type="application/json")

    if slug not in _recording_meta:
        return web.Response(text=json.dumps({"error": "Recording not found"}),
                            content_type="application/json")

    meta = _recording_meta[slug]

    # Update display name
    if "display_name" in body:
        new_name = str(body["display_name"]).strip()
        if new_name:
            meta["display_name"] = new_name
            # Also update the in-memory recorder title if running locally
            try:
                from m3u_recorder import m3u_recordings
                if slug in m3u_recordings:
                    m3u_recordings[slug].title = new_name
            except Exception:
                pass

    # Update categories (sport)
    if "categories" in body:
        cats = body["categories"]
        if isinstance(cats, list):
            meta["categories"] = cats
            try:
                from m3u_recorder import m3u_recordings
                if slug in m3u_recordings and hasattr(m3u_recordings[slug], "categories"):
                    m3u_recordings[slug].categories = cats
            except Exception:
                pass

    # Extend / clear end time
    if "extend_minutes" in body:
        mins = int(body["extend_minutes"])
        if mins == -1:
            # Remove end time → 24/7
            meta["ends_at"] = None
            meta["auto_end_at"] = None
        elif mins > 0:
            # Extend from now (or current end, whichever is later)
            base = max(meta.get("ends_at") or 0, time.time())
            meta["ends_at"] = base + mins * 60

    _recording_meta[slug] = meta
    await async_save_recording_meta(slug, meta)
    logger.info("Admin patched recording '%s': %s", slug, {k: v for k, v in body.items() if k != "stream_url"})
    return web.Response(text=json.dumps({"ok": True}), content_type="application/json")


@_require_auth
async def _handle_hide_recording(request: web.Request) -> web.Response:
    """
    Hide a recording from the catalog AND immediately stop it + free its account.
    Hiding used to only remove from catalog but left the ffmpeg process running
    and the account locked — causing ghost locks that survived restarts.
    Now: hide = stop + catalog-hide.  The scheduler will not restart hidden slugs.
    """
    slug = request.match_info["slug"]
    _hidden_slugs[slug] = time.time() + _HIDDEN_SLUG_TTL
    _save_hidden_slugs()
    logger.info("Admin: hiding + stopping slug '%s'", slug)

    if _LEAF_MODE == "master":
        import iptv_scheduler as _sched
        from leaf_server import _slug_to_worker

        # Stop via scheduler path (also frees account)
        if slug in _sched._scheduler_recordings:
            _sched._stop_scheduler_recording(slug)
            _sched._stop_miss_counts.pop(slug, None)
            _slug_to_worker.pop(slug, None)  # bug2: scheduler path never cleared _slug_to_worker
        else:
            # Admin-started recording — stop on worker + free account.
            # Free unconditionally: an unreachable worker must not ghost-lock the account.
            from worker_client import stop_on_worker
            from iptv_accounts import async_mark_free
            worker_url = _slug_to_worker.pop(slug, None)
            acc_id = _recording_meta.get(slug, {}).get("account_id")
            if worker_url:
                await stop_on_worker(worker_url, slug)
            if acc_id:
                await async_mark_free(acc_id)

        _recording_meta.pop(slug, None)
        await async_delete_recording_meta(slug)
    else:
        # Standalone mode
        import iptv_scheduler as _sched
        from m3u_recorder import active_m3u_tasks, stop_m3u_recording as _stop
        if slug in _sched._scheduler_recordings:
            _sched._stop_scheduler_recording(slug)
            _sched._stop_miss_counts.pop(slug, None)
        elif slug in active_m3u_tasks:
            meta = _recording_meta.get(slug, {})
            acc_id = meta.get("account_id")
            _stop(slug)
            if acc_id:
                from iptv_accounts import async_mark_free
                await async_mark_free(acc_id)
        _recording_meta.pop(slug, None)
        await async_delete_recording_meta(slug)

    return web.Response(text=json.dumps({"ok": True, "hidden": True}), content_type="application/json")


@_require_auth
async def _handle_unhide_recording(request: web.Request) -> web.Response:
    slug = request.match_info["slug"]
    _hidden_slugs.pop(slug, None)
    _save_hidden_slugs()
    logger.info("Admin unhidden slug from catalog: %s", slug)
    return web.Response(text=json.dumps({"ok": True, "hidden": False}), content_type="application/json")


@_require_auth
async def _handle_get_hidden_matches(request: web.Request) -> web.Response:
    now = time.time()
    active = [mid for mid, exp in _hidden_match_ids.items() if exp > now]
    return web.Response(text=json.dumps({"hidden": active}), content_type="application/json")


@_require_auth
async def _handle_hide_match(request: web.Request) -> web.Response:
    match_id = request.match_info["match_id"]
    _hidden_match_ids[match_id] = time.time() + _HIDDEN_MATCH_TTL
    _save_hidden_match_ids()
    logger.info("Admin hidden match from Live Now: %s (expires 8h)", match_id)
    return web.Response(text=json.dumps({"ok": True, "hidden": True}), content_type="application/json")


@_require_auth
async def _handle_unhide_match(request: web.Request) -> web.Response:
    match_id = request.match_info["match_id"]
    _hidden_match_ids.pop(match_id, None)
    _save_hidden_match_ids()
    logger.info("Admin unhidden match from Live Now: %s", match_id)
    return web.Response(text=json.dumps({"ok": True, "hidden": False}), content_type="application/json")


# ══════════════════════════════════════════════════════════════════════════════
# Schedule API Handlers
# ══════════════════════════════════════════════════════════════════════════════

@_require_auth
async def _handle_schedule_events(request: web.Request) -> web.Response:
    """GET /admin/api/schedule/events?category=&search= — fetch events from all catalogs + annotate."""
    from event_channel_store import get_channels_for_event, get_end_offset_for_event, _load_all
    from iptv_scheduler import fetch_all_events_for_admin, _is_event_live_or_soon, _scheduler_recordings, _ALL_SPORT_CATALOGS

    category = request.rel_url.query.get("category", "all")
    search   = request.rel_url.query.get("search", "").strip().lower()
    refresh  = request.rel_url.query.get("refresh") in ("1", "true")

    events = await fetch_all_events_for_admin(category, force_refresh=refresh)

    # Leaf-truth override: the admin "Live Now" is built from streamed.pk's feed
    # via _has_started(), which keeps an event "LIVE" for up to 8h regardless of
    # whether it actually ended (streamed.pk holds ended events LIVE for hours).
    # The leaf's own live_registry knows when WE ended a recording — cross-ref it
    # and demote any event the leaf marked ended so it leaves Live Now here too.
    # Events with no registry row (leaf never recorded them) keep the 8h behavior.
    _ended_event_ids: set[str] = set()
    try:
        import live_registry as _reg
        for _row in _reg.get_all():
            if _row.get("status") == "ended":
                _ended_event_ids.add(str(_row.get("_id", "")))
    except Exception as _rexc:
        logger.warning("Schedule: live_registry read failed, using streamed.pk truth only: %s", _rexc)

    result = []
    for ev in events[:300]:  # cap at 300 events across all categories
        event_id    = str(ev.get("id", ""))
        event_title = ev.get("name") or ev.get("title") or ""
        event_slug  = str(ev.get("slug") or ev.get("stremioId") or event_id)

        # Apply search filter
        if search:
            haystack = f"{event_title} {event_slug} {ev.get('genres', [])}".lower()
            if search not in haystack:
                continue

        assigned = get_channels_for_event(event_id, event_title, event_slug)
        is_live  = ev.get("_is_live", False)
        # If the leaf already marked this event ended, trust that over streamed.pk's
        # stale 8h LIVE window — demote it out of Live Now into its own Ended section.
        is_ended = event_id in _ended_event_ids
        if is_live and is_ended:
            is_live = False
        is_rec   = any(
            event_id in rec.get("shared_events", {rec.get("event_id", "")})
            for rec in _scheduler_recordings.values()
        )
        end_offset = get_end_offset_for_event(event_id, event_title, event_slug)

        # Find earliest started_at for this event's channels (for long-run warning)
        _started_at = None
        for _cs, _rm in _recording_meta.items():
            if (_rm.get("event_id") == event_id or
                    event_id in (_rm.get("shared_events_meta") or []) or
                    any(m.get("event_id") == event_id for m in (_rm.get("shared_events_meta") or []))):
                _sa = _rm.get("started_at")
                if _sa and (_started_at is None or _sa < _started_at):
                    _started_at = _sa
        # Also check scheduler_recordings
        for _cs, _rinfo in _scheduler_recordings.items():
            if event_id in _rinfo.get("shared_events", {_rinfo.get("event_id", "")}):
                _sa = _rinfo.get("started_at")
                if _sa and (_started_at is None or _sa < _started_at):
                    _started_at = _sa

        result.append({
            **ev,
            "id":                 event_id,
            "assigned_channels":  assigned,
            "_is_live":           is_live,
            "_is_ended":          is_ended,
            "_is_recording":      is_rec,
            "_category":          ev.get("_category", ""),
            "end_offset_hours":   end_offset,
            "_recording_started_at": _started_at,
        })

    # Attach the persisted slug→name map once so the schedule tab can label
    # assigned-channel chips with a friendly name instead of the numeric slug.
    try:
        from event_channel_store import get_channel_names
        _chan_names = get_channel_names()
    except Exception:
        _chan_names = {}

    return web.Response(
        text=json.dumps({"events": result, "catalogs": _ALL_SPORT_CATALOGS,
                         "channel_names": _chan_names}),
        content_type="application/json",
    )


@_require_auth
async def _handle_schedule_capacity(request: web.Request) -> web.Response:
    """GET /admin/api/schedule/capacity — return account pool status."""
    from iptv_scheduler import get_capacity_status
    return web.Response(
        text=json.dumps(get_capacity_status()),
        content_type="application/json",
    )


@_require_auth
async def _handle_schedule_assign(request: web.Request) -> web.Response:
    """POST /admin/api/schedule/assign — assign an IPTV channel to an event."""
    from event_channel_store import save_event_override, save_team_rule, _load_all, get_channels_for_event

    try:
        body = await request.json()
    except Exception:
        return web.Response(status=400, text=json.dumps({"error": "Bad JSON"}),
                            content_type="application/json")

    event_id   = str(body.get("event_id", "")).strip()
    event_name = body.get("event_name", event_id)
    event_slug = body.get("event_slug", event_id)
    slug       = body.get("slug", "").strip()
    # stream_url intentionally ignored — account auto-selected at recording time
    team_key   = (body.get("team_key") or "").strip()
    group_id   = body.get("group_id", "").strip()
    # Display-only: {slug: friendly_name} sent by the assign UI so the schedule
    # chips + recordings cards can show a channel name instead of the numeric id.
    channel_names = body.get("channel_names") or {}
    if channel_names:
        try:
            from event_channel_store import set_channel_names
            set_channel_names(channel_names)
        except Exception as _cnexc:
            logger.warning("Schedule: channel-name persist failed: %s", _cnexc)

    if not event_id or (not slug and not group_id):
        return web.Response(text=json.dumps({"error": "event_id and either slug or group_id required"}),
                            content_type="application/json")

    # Get existing channels for this event and append (avoid duplicates by slug)
    existing = get_channels_for_event(event_id, event_name, event_slug)
    # existing is a list of plain slugs (from get_channels_for_event normalization)
    # We need the raw docs to preserve stream_urls that are already stored
    all_docs = _load_all()
    raw_channels: list = []
    if team_key:
        doc = all_docs.get(f"team:{team_key}")
        if doc:
            raw_channels = list(doc.get("channels", []))
    else:
        doc = all_docs.get(f"event:{event_id}")
        if doc:
            raw_channels = list(doc.get("channels", []))

    end_offset_hours = body.get("end_offset_hours")
    try:
        end_offset_hours = float(end_offset_hours) if end_offset_hours else None
    except (TypeError, ValueError):
        end_offset_hours = None

    # Determine channels to add (plain slugs only — end_offset_hours lives on the event doc)
    to_add = []
    if group_id:
        from event_channel_store import get_group
        group_doc = get_group(group_id if group_id.startswith("group:") else f"group:{group_id}")
        if group_doc:
            to_add.extend(group_doc.get("channels", []))
    elif slug:
        to_add.append(slug)

    # Remove old entries for the slugs we are adding, then append new
    add_slugs = [c["slug"] if isinstance(c, dict) else c for c in to_add]
    raw_channels = [c for c in raw_channels if (c["slug"] if isinstance(c, dict) else c) not in add_slugs]
    raw_channels.extend(to_add)

    # Preserve existing end_offset_hours if not explicitly overriding
    existing_doc = all_docs.get(f"team:{team_key}" if team_key else f"event:{event_id}", {})
    final_end_offset = end_offset_hours if end_offset_hours is not None else existing_doc.get("end_offset_hours")

    # Assigning a channel is an explicit request to record this event, so clear
    # any force-delete blocklist entry left over from a prior "remove all"/stop.
    # Without this the scheduler tick skips the event and never starts the new channel.
    try:
        import iptv_scheduler as _sched
        if event_id in _sched._force_deleted:
            _sched._force_deleted.discard(event_id)
            logger.info("Schedule: cleared force-delete blocklist for event %s (re-assigned)", event_id)
    except Exception:
        pass

    # Save as team rule (reusable) if team_key given, else event override
    if team_key:
        save_team_rule(
            team_key=team_key,
            label=event_name,
            channels=raw_channels,
            match_keys=[team_key],
            end_offset_hours=final_end_offset,
        )
    else:
        save_event_override(event_id=event_id, channels=raw_channels, end_offset_hours=final_end_offset)

    logger.info("Schedule: assigned slug %s to event %s (team_key=%s) [account auto-selected at runtime]",
                slug, event_id, team_key or "none")
    return web.Response(text=json.dumps({"ok": True}), content_type="application/json")


async def _stop_channel_for_event(slug: str, event_id: str) -> bool:
    """Stop a running recording for `slug` on behalf of `event_id`.

    Respects shared streams: if the channel is still needed by another event,
    this event is just dropped from its shared set and the stream keeps running.
    Only when no other event needs it is the stream actually stopped, the
    account freed, and the meta cleared.

    Returns True if the stream was actually stopped, False if it was kept alive
    for another event (or wasn't running at all).
    """
    import iptv_scheduler as _sched
    from m3u_recorder import active_m3u_tasks as _active

    if slug in _sched._scheduler_recordings:
        rec_info = _sched._scheduler_recordings[slug]
        shared = rec_info.get("shared_events", {rec_info.get("event_id", "")})
        other_events = shared - {event_id}
        if other_events:
            rec_info["shared_events"] = other_events
            logger.info("Admin unassign: removed event '%s' from shared set of '%s' (still shared with %s)",
                        event_id, slug, other_events)
            return False
        _sched._stop_scheduler_recording(slug)
        _sched._stop_miss_counts.pop(slug, None)
        _recording_meta.pop(slug, None)
        await async_delete_recording_meta(slug)
        logger.info("Admin unassign: stopped scheduler recording for '%s'", slug)
        return True

    if slug in _active:
        meta = _recording_meta.get(slug, {})
        acc_id = meta.get("account_id")
        stop_m3u_recording(slug)
        if acc_id:
            from iptv_accounts import async_mark_free
            await async_mark_free(acc_id)
        _recording_meta.pop(slug, None)
        await async_delete_recording_meta(slug)
        logger.info("Admin unassign: stopped manual recording for '%s'", slug)
        return True

    if _LEAF_MODE == "master":
        from leaf_server import _slug_to_worker
        from worker_client import stop_on_worker
        worker_url = _slug_to_worker.pop(slug, None)
        if worker_url:
            # Free the account whether or not the worker was reachable.
            await stop_on_worker(worker_url, slug)
            acc_id = _recording_meta.get(slug, {}).get("account_id")
            if acc_id:
                from iptv_accounts import async_mark_free
                await async_mark_free(acc_id)
            _recording_meta.pop(slug, None)
            await async_delete_recording_meta(slug)
            logger.info("Admin unassign: stopped worker recording for '%s'", slug)
            return True

    return False


@_require_auth
async def _handle_schedule_unassign(request: web.Request) -> web.Response:
    """DELETE /admin/api/schedule/assign — remove a channel from an event."""
    from event_channel_store import (
        save_event_override, save_team_rule, get_channels_for_event,
        _load_all, _delete
    )

    try:
        body = await request.json()
    except Exception:
        return web.Response(status=400, text=json.dumps({"error": "Bad JSON"}),
                            content_type="application/json")

    event_id = str(body.get("event_id", "")).strip()
    slug     = body.get("slug", "").strip()

    if not event_id or not slug:
        return web.Response(text=json.dumps({"error": "event_id and slug required"}),
                            content_type="application/json")

    # Remove slug from both team rules and event overrides that match
    all_docs = _load_all()

    # Special case: slug='*' means remove ALL channels from this event
    if slug == '*':
        # Collect every channel this event is responsible for: those listed on
        # the event override doc PLUS any running recording whose shared set
        # includes this event (covers streams started before the doc changed).
        import iptv_scheduler as _sched
        slugs_to_stop: set = set()
        for doc_id, doc in all_docs.items():
            if doc_id == f"event:{event_id}":
                for c in doc.get("channels", []):
                    slugs_to_stop.add(c["slug"] if isinstance(c, dict) else c)
        for _s, _info in list(_sched._scheduler_recordings.items()):
            _shared = _info.get("shared_events", {_info.get("event_id", "")})
            if event_id in _shared:
                slugs_to_stop.add(_s)

        # Delete the override doc first so the scheduler tick won't restart it.
        for doc_id in list(all_docs.keys()):
            if doc_id == f"event:{event_id}":
                _delete(doc_id)
        logger.info("Removed all channels from event %s (%d channel(s) to stop)",
                    event_id, len(slugs_to_stop))

        stopped = 0
        for _s in slugs_to_stop:
            if await _stop_channel_for_event(_s, event_id):
                stopped += 1
        logger.info("Admin unassign (remove-all): stopped %d/%d stream(s) for event %s",
                    stopped, len(slugs_to_stop), event_id)
        return web.Response(text=json.dumps({"ok": True, "stopped": stopped}),
                            content_type="application/json")

    for doc_id, doc in all_docs.items():
        channels = doc.get("channels", [])
        slugs_in_channels = [(c["slug"] if isinstance(c, dict) else c) for c in channels]
        if slug not in slugs_in_channels:
            continue
        channels = [c for c in channels if (c["slug"] if isinstance(c, dict) else c) != slug]
        if channels:
            if doc_id.startswith("team:"):
                save_team_rule(
                    team_key=doc_id[5:],
                    label=doc.get("label", doc_id),
                    channels=channels,
                    match_keys=doc.get("match_keys", []),
                )
            else:
                save_event_override(event_id=event_id, channels=channels)
        else:
            _delete(doc_id)

    # Immediately stop the running recording for the unassigned channel.
    # Shared streams are kept alive for their other events (handled in helper).
    await _stop_channel_for_event(slug, event_id)

    return web.Response(text=json.dumps({"ok": True}), content_type="application/json")



@_require_auth
async def _handle_force_delete(request: web.Request) -> web.Response:
    """POST /admin/api/schedule/force-delete — stop + blocklist a live event."""
    try:
        body = await request.json()
    except Exception:
        return web.Response(status=400, text=json.dumps({"error": "Bad JSON"}),
                            content_type="application/json")

    event_id = str(body.get("event_id", "")).strip()
    if not event_id:
        return web.Response(text=json.dumps({"error": "event_id required"}),
                            content_type="application/json")

    import iptv_scheduler as _sched
    # Add to blocklist so scheduler never restarts it this session
    _sched._force_deleted.add(event_id)

    # Find and stop any active scheduler recording for this event_id
    stopped: list[str] = []
    for ch_slug, rec_info in list(_sched._scheduler_recordings.items()):
        if rec_info.get("event_id") == event_id:
            _sched._stop_scheduler_recording(ch_slug)
            _sched._stop_miss_counts.pop(ch_slug, None)
            # bug6: _recording_meta and _slug_to_worker were never cleared here
            _recording_meta.pop(ch_slug, None)
            await async_delete_recording_meta(ch_slug)
            try:
                from leaf_server import _slug_to_worker
                _slug_to_worker.pop(ch_slug, None)
            except Exception:
                pass
            stopped.append(ch_slug)

    logger.info("Admin force-delete: event=%s stopped=%s", event_id, stopped)
    return web.Response(
        text=json.dumps({"ok": True, "event_id": event_id, "stopped_slugs": stopped}),
        content_type="application/json",
    )


@_require_auth
async def _handle_force_start(request: web.Request) -> web.Response:
    """POST /admin/api/schedule/force-start — pin event as live for 15 min and trigger recording."""
    try:
        body = await request.json()
    except Exception:
        return web.Response(status=400, text=json.dumps({"error": "Bad JSON"}),
                            content_type="application/json")

    event_id = str(body.get("event_id", "")).strip()
    if not event_id:
        return web.Response(text=json.dumps({"error": "event_id required"}),
                            content_type="application/json")

    import time as _time
    import iptv_scheduler as _sched

    # Remove from blocklist if it was previously force-deleted
    _sched._force_deleted.discard(event_id)

    # Clear _hidden_slugs for all channels assigned to this event so the
    # scheduler tick doesn't skip them. Auto-end adds channels to _hidden_slugs
    # which prevents the tick from restarting them even after force-start.
    try:
        from event_channel_store import get_channels_for_event
        _ch_slugs = get_channels_for_event(event_id, event_id, event_id)
        if _ch_slugs:
            for _s in _ch_slugs:
                _hidden_slugs.pop(_s, None)
            _save_hidden_slugs()
            logger.info("Admin force-start: cleared hidden state for %d channel(s): %s", len(_ch_slugs), _ch_slugs)
    except Exception as _e:
        logger.warning("Admin force-start: could not clear hidden slugs: %s", _e)

    # Clear the pinned auto-end so _resolve_pinned_auto_end recomputes it fresh
    # from the REAL kickoff time when the recording actually starts.
    # DO NOT pre-pin a new value here — pre-pinning from now+offset causes the
    # auto-end to be calculated from the force-start click time, not the match
    # kickoff, so a stream force-started 48min before a match ends 48min early.
    # _resolve_pinned_auto_end handles the "already-past kickoff" edge case itself.
    try:
        from event_channel_store import clear_auto_end_ts
        clear_auto_end_ts(event_id)
        logger.info("Admin force-start: cleared auto-end pin for %s — scheduler will recompute from kickoff", event_id)
    except Exception as _e:
        logger.warning("Admin force-start: could not clear auto-end: %s", _e)

    # Immediately flip live_registry from 'ended' → 'recording' so the admin panel
    # shows it in Live Now on the next load without waiting for the next tick (~60s).
    try:
        import live_registry as _reg
        _reg.upsert_recording(
            event_id   = event_id,
            match_id   = event_id.split(":", 1)[1] if ":" in event_id else event_id,
            title      = event_id,
            category   = "sports",
            started_at = _time.time(),
            ends_at    = 0.0,
            auto_end_at= None,
        )
    except Exception as _e:
        logger.warning("Admin force-start: could not pre-flip live_registry: %s", _e)

    # Pin as live for FORCE_START_DURATION_SECS
    expiry = _time.monotonic() + _sched.FORCE_START_DURATION_SECS
    _sched._force_started[event_id] = expiry

    logger.info(
        "Admin force-start: event=%s pinned live for %.0f seconds",
        event_id, _sched.FORCE_START_DURATION_SECS,
    )
    return web.Response(
        text=json.dumps({
            "ok": True,
            "event_id": event_id,
            "live_until_secs": _sched.FORCE_START_DURATION_SECS,
        }),
        content_type="application/json",
    )


@_require_auth
async def _handle_probe_channel(request: web.Request) -> web.Response:
    """GET /admin/api/schedule/probe?slug=<stream_id> — test if an IPTV stream is live."""
    import time as _time
    slug = request.rel_url.query.get("slug", "").strip()
    if not slug:
        return web.Response(text=json.dumps({"error": "slug required"}),
                            content_type="application/json")

    from iptv_accounts import async_claim_free_account, async_mark_free, async_mark_busy

    # Build the set of account_ids ACTUALLY streaming on a worker right now, so we
    # never probe with an account MongoDB wrongly thinks is free (drift / zombie).
    # This is defense-in-depth: even if the accounts collection is momentarily out
    # of sync with reality, the probe cannot open a 2nd connection on a live account
    # (which returns HTTP 458 and kills the real stream). Source of truth = the
    # per-worker /internal/status running slugs cross-referenced with leaf_workers.
    async def _live_account_slugs() -> dict[str, str]:
        """Return {account_id: real_slug} for every account ACTUALLY streaming on a
        worker right now (per /internal/status running flag + leaf_workers map)."""
        live: dict[str, str] = {}
        if _LEAF_MODE != "master":
            return live
        try:
            from leaf_server import _parse_workers
            from worker_client import status_of_worker
            import iptv_accounts as _ia
            _ia._get_motor_col()
            slug_to_acct: dict[str, str] = {}
            if _ia._motor_client is not None:
                _wcol = _ia._motor_client["stremio"]["leaf_workers"]
                async for _wdoc in _wcol.find({}):
                    for _s, _info in (_wdoc.get("assigned_streams") or {}).items():
                        _a = (_info or {}).get("account_id")
                        if _a:
                            slug_to_acct[_s] = _a
            for _wurl in _parse_workers().values():
                _st = await status_of_worker(_wurl)
                if not _st:
                    continue
                for _s, _info in _st.items():
                    running = _info.get("running") if isinstance(_info, dict) else True
                    if running and _s in slug_to_acct:
                        live[slug_to_acct[_s]] = _s   # account_id -> the slug it serves
        except Exception as _exc:
            logger.warning("probe: could not build live-account map: %s", _exc)
        return live

    live_accts = await _live_account_slugs()

    # Atomically claim a free account, then verify it isn't actually streaming.
    # Retry up to a few times: if we claim a drifted account (free in MongoDB but
    # live on a worker), heal it back to busy and try the next one.
    probe_acc_id, acc = None, None
    for _attempt in range(8):
        _cand_id, _cand = await async_claim_free_account(f"__probe__{slug}")
        if _cand is None:
            break  # genuinely no free account
        if _cand_id in live_accts:
            # Drifted account — actually streaming. Re-mark busy with the slug it is
            # REALLY serving (not the probe slug) so MongoDB matches reality and the
            # next claim skips it. Do NOT probe with it (would open a 2nd conn → 458).
            _real_slug = live_accts[_cand_id]
            logger.warning(
                "probe: claimed '%s' but it is actually streaming slug '%s' on a worker — "
                "healing to busy and retrying with another account",
                _cand_id, _real_slug,
            )
            await async_mark_busy(_cand_id, _real_slug)
            continue
        probe_acc_id, acc = _cand_id, _cand
        break

    if acc is None:
        return web.Response(text=json.dumps({
            "live": False,
            "error": "All accounts are busy recording — probe skipped to protect active streams"
        }), content_type="application/json")

    try:
        server   = acc.get("server", "").rstrip("/")
        username = acc.get("username", "")
        password = acc.get("password", "")
        if not server or not username:
            return web.Response(text=json.dumps({"live": False, "error": "Account missing server/username"}),
                                content_type="application/json")

        m3u8_url = f"{server}/live/{username}/{password}/{slug}.m3u8"
        import httpx
        t0 = _time.monotonic()
        async with httpx.AsyncClient(timeout=7, follow_redirects=True,
                                     headers={"user-agent": "okhttp/5.0.0-alpha.2"}) as client:
            # Step 1: fetch the M3U8 playlist
            r = await client.get(m3u8_url)
            if r.status_code != 200:
                return web.Response(text=json.dumps({
                    "live": False,
                    "error": f"M3U8 returned HTTP {r.status_code}",
                    "url": m3u8_url,
                }), content_type="application/json")

            # Step 2: find first segment URI
            base = m3u8_url.rsplit("/", 1)[0]
            seg_url = None
            for line in r.text.splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    seg_url = line if line.startswith("http") else f"{base}/{line}"
                    break

            if not seg_url:
                return web.Response(text=json.dumps({
                    "live": False,
                    "error": "Playlist has no segments yet (stream buffering or offline)",
                    "url": m3u8_url,
                }), content_type="application/json")

            # Step 3: HEAD the segment to check it's being served
            seg_r = await client.head(seg_url)
            latency_ms = round((_time.monotonic() - t0) * 1000)
            live = seg_r.status_code in (200, 206)
            return web.Response(text=json.dumps({
                "live":           live,
                "latency_ms":     latency_ms,
                "segment_url":    seg_url,
                "segment_status": seg_r.status_code,
                "error":          None if live else f"Segment HTTP {seg_r.status_code}",
            }), content_type="application/json")

    except Exception as exc:
        return web.Response(text=json.dumps({"live": False, "error": str(exc)}),
                            content_type="application/json")
    finally:
        # Always release the probe hold on every return/exception path.
        if probe_acc_id:
            from iptv_accounts import async_mark_free as _probe_free
            await _probe_free(probe_acc_id)


_STATUS_PAGE_HTML = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Leaf · Stream Status</title>
<style>
:root{--bg:#0d1017;--card:#151a24;--card2:#1a2029;--border:#242c39;--text:#e6ebf2;
--muted:#8a93a3;--accent:#4ea1ff;--green:#22c55e;--red:#ef4444;--amber:#f59e0b}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;padding:20px;max-width:1400px;margin:0 auto}
h1{font-size:1.3rem;display:flex;align-items:center;gap:10px;margin-bottom:4px}
.sub{color:var(--muted);font-size:.8rem;margin-bottom:18px}
.sub a{color:var(--accent);text-decoration:none}
.section-title{font-size:1rem;font-weight:700;margin:22px 0 12px;display:flex;align-items:center;gap:8px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(330px,1fr));gap:12px}
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:14px 16px}
.card.down{border-color:var(--red)}
.card.past{opacity:.82}
.row{display:flex;align-items:center;gap:8px}
.name{font-weight:600;font-size:.95rem;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.dot{width:9px;height:9px;border-radius:50%;flex-shrink:0}
.dot-green{background:var(--green);box-shadow:0 0 6px var(--green)}
.dot-red{background:var(--red);box-shadow:0 0 6px var(--red)}
.dot-grey{background:var(--muted)}
.badge{font-size:.68rem;padding:2px 7px;border-radius:20px;font-weight:600}
.b-src{background:#1e2330;color:var(--muted)}
.b-live{background:#0f2a1c;color:var(--green)}
.b-down{background:#2a1010;color:var(--red)}
.b-end{background:#22252e;color:var(--muted)}
.bar{height:7px;border-radius:4px;background:#2a3140;overflow:hidden;margin:10px 0 6px}
.bar>i{display:block;height:100%;border-radius:4px}
.stats{display:grid;grid-template-columns:1fr 1fr;gap:4px 14px;font-size:.76rem;color:var(--muted);margin-top:8px}
.stats b{color:var(--text);font-weight:600}
.irr{margin-top:9px;font-size:.72rem;color:var(--amber)}
.irr-samples{color:var(--muted);font-size:.68rem;margin-top:2px;max-height:52px;overflow:auto;font-family:monospace}
.empty{color:var(--muted);padding:20px;text-align:center;background:var(--card);border:1px solid var(--border);border-radius:12px}
.refresh{margin-left:auto;background:var(--card2);border:1px solid var(--border);color:var(--text);
padding:6px 12px;border-radius:8px;cursor:pointer;font-size:.8rem}
.topbar{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
</style></head><body>
<div class="topbar">
  <div>
    <h1>📊 Stream Status <span id="live-count" class="badge b-live"></span></h1>
    <div class="sub" id="meta">loading… · <a href="/admin/panel">← back to panel</a></div>
  </div>
  <button class="refresh" onclick="load()">🔄 Refresh</button>
  <label class="sub" style="display:flex;align-items:center;gap:6px"><input type="checkbox" id="auto" checked> auto</label>
</div>
<div class="section-title">🔴 Live now</div>
<div id="live" class="grid"><div class="empty">loading…</div></div>
<div class="section-title">🏁 Past events <span class="sub" id="ttl-note"></span></div>
<div id="past" class="grid"><div class="empty">loading…</div></div>
<script>
function fmtDur(s){s=Math.round(s||0);if(s<60)return s+'s';const m=Math.floor(s/60),ss=s%60;
if(m<60)return m+'m'+(ss?' '+ss+'s':'');const h=Math.floor(m/60);return h+'h '+(m%60)+'m';}
function upColor(p){return p>=99?'var(--green)':p>=95?'#a3e635':p>=85?'var(--amber)':'var(--red)';}
function card(s,past){
  const down=s.currently_down&&!past;
  const dot=past?'dot-grey':(down?'dot-red':'dot-green');
  const stat=past?`<span class="badge b-end">ended</span>`:(down?`<span class="badge b-down">● DOWN</span>`:`<span class="badge b-live">● live</span>`);
  const up=s.uptime_pct!=null?s.uptime_pct:0;
  let irr='';
  if(s.irregular_count>0){
    const samp=(s.irregular_samples||[]).slice(-8).map(x=>x.dur+'s').join(', ');
    irr=`<div class="irr">⚠ ${s.irregular_count} irregular segment${s.irregular_count>1?'s':''}</div>
         <div class="irr-samples">${samp}${s.irregular_count>8?' …':''}</div>`;
  }
  const dur=past
    ? (s.ended_at&&s.session_start?fmtDur(s.ended_at-s.session_start):'—')
    : (s.session_start?fmtDur(Date.now()/1000-s.session_start):'—');
  return `<div class="card ${down?'down':''} ${past?'past':''}">
    <div class="row"><span class="dot ${dot}"></span>
      <span class="name" title="${s.display_name||s.slug}">${s.display_name||s.slug}</span>
      <span class="badge b-src">${s.source||'iptv'}</span> ${stat}</div>
    <div class="bar"><i style="width:${up}%;background:${upColor(up)}"></i></div>
    <div class="row" style="font-size:.78rem"><b style="color:${upColor(up)}">${up}%</b>
      <span style="color:var(--muted)">&nbsp;uptime · monitored ${dur}</span></div>
    <div class="stats">
      <div>Downtime: <b>${fmtDur(s.downtime_secs)}</b></div>
      <div>Longest outage: <b>${fmtDur(s.longest_outage_secs)}</b></div>
      <div>Polls: <b>${s.polls_up}/${s.polls_total}</b></div>
      <div>Stalls: <b>${s.stall_total||0}</b></div>
    </div>${irr}
    <div class="sub" style="margin-top:8px;font-family:monospace;font-size:.66rem">${s.slug}</div>
  </div>`;
}
async function load(){
  try{
    const r=await fetch('/admin/api/status',{credentials:'same-origin'});
    const d=await r.json();
    const live=d.live||[],past=d.past||[];
    document.getElementById('live-count').textContent=live.length+' live';
    document.getElementById('meta').innerHTML=`polling every ${d.poll_secs}s via public links · <a href="/admin/panel">← back to panel</a>`;
    document.getElementById('ttl-note').textContent=`(auto-cleared after ${d.ttl_days} day${d.ttl_days>1?'s':''})`;
    document.getElementById('live').innerHTML=live.length?live.map(s=>card(s,false)).join(''):'<div class="empty">No live streams</div>';
    document.getElementById('past').innerHTML=past.length?past.map(s=>card(s,true)).join(''):'<div class="empty">No past events in the retention window</div>';
  }catch(e){document.getElementById('meta').textContent='error loading status';}
}
load();
setInterval(()=>{if(document.getElementById('auto').checked)load();},6000);
</script></body></html>"""


@_require_auth
async def _handle_status_data(request: web.Request) -> web.Response:
    """GET /admin/api/status — live + past stream-stability data. Fully guarded:
    a failure here returns empty data, never touches the streaming path."""
    try:
        from status_monitor import get_status_data
        data = await get_status_data()
    except Exception as exc:
        logger.warning("status data fetch failed: %s", exc)
        data = {"live": [], "past": [], "poll_secs": 6, "ttl_days": 2}
    return web.Response(text=json.dumps(data), content_type="application/json")


async def _handle_status_page(request: web.Request) -> web.Response:
    """GET /status — standalone stability dashboard. Session-gated like the panel."""
    if not _valid_session(_get_session(request)):
        raise web.HTTPFound("/admin")
    return web.Response(text=_STATUS_PAGE_HTML, content_type="text/html")


@_require_auth
async def _handle_list_groups(request: web.Request) -> web.Response:
    from event_channel_store import list_groups
    return web.Response(text=json.dumps({"groups": list_groups()}), content_type="application/json")

@_require_auth
async def _handle_add_group(request: web.Request) -> web.Response:
    from event_channel_store import save_group
    try:
        body = await request.json()
        gid      = body["id"]
        glabel   = body["label"]
        gchannels = body["channels"]
    except Exception:
        return web.Response(status=400, text=json.dumps({"error": "Bad JSON or missing id/label/channels"}),
                            content_type="application/json")
    save_group(gid, glabel, gchannels)
    return web.Response(text=json.dumps({"ok": True}), content_type="application/json")

@_require_auth
async def _handle_delete_group(request: web.Request) -> web.Response:
    from event_channel_store import _delete
    group_id = request.match_info["id"]
    doc_id = group_id if group_id.startswith("group:") else f"group:{group_id}"
    _delete(doc_id)
    return web.Response(text=json.dumps({"ok": True}), content_type="application/json")


@_require_auth
async def _handle_list_rules(request: web.Request) -> web.Response:
    from event_channel_store import list_team_rules
    return web.Response(text=json.dumps({"rules": list_team_rules()}), content_type="application/json")


@_require_auth
async def _handle_save_rule(request: web.Request) -> web.Response:
    from event_channel_store import save_team_rule
    try:
        body = await request.json()
    except Exception:
        return web.Response(status=400, text=json.dumps({"error": "Bad JSON"}), content_type="application/json")
    team_key  = str(body.get("team_key", "")).strip()
    label     = str(body.get("label", team_key)).strip()
    match_keys = body.get("match_keys") or [team_key]
    channels  = body.get("channels", [])
    if not team_key or not channels:
        return web.Response(text=json.dumps({"error": "team_key and channels required"}), content_type="application/json")
    end_offset = body.get("end_offset_hours")
    try:
        end_offset = float(end_offset) if end_offset else None
    except (TypeError, ValueError):
        end_offset = None
    save_team_rule(team_key=team_key, label=label, channels=channels,
                   match_keys=match_keys, end_offset_hours=end_offset)
    return web.Response(text=json.dumps({"ok": True}), content_type="application/json")


@_require_auth
async def _handle_delete_rule(request: web.Request) -> web.Response:
    from event_channel_store import delete_rule
    rule_id = request.match_info["id"]
    doc_id = rule_id if rule_id.startswith("team:") else f"team:{rule_id}"
    delete_rule(doc_id)
    return web.Response(text=json.dumps({"ok": True}), content_type="application/json")


# ── YouTube stream handlers ───────────────────────────────────────────────────
_YT_WORKER_URL  = os.getenv("YT_WORKER_URL", "http://100.69.9.26:8089")
_yt_meta: dict[str, dict] = {}   # slug → {youtube_url, display_name, ends_at, started_at}


async def _yt_launch_now(slug: str, youtube_url: str, display_name: str,
                         category: str, duration_h: float,
                         ends_at: Optional[float] = None) -> bool:
    """Start a YouTube recording on the yt-worker NOW and persist its live meta.

    Shared by the immediate-start handler, the schedule poller, and restore so the
    start path stays identical everywhere. `ends_at` may be pre-computed (scheduler
    fires with start_at+duration); if None it's derived from duration_h from now.
    Returns True on success. Arms the auto-stop task when there's an end time.
    """
    import httpx as _hx, time as _t
    try:
        async with _hx.AsyncClient(timeout=15) as c:
            r = await c.post(f"{_YT_WORKER_URL}/internal/start", json={
                "slug":         slug,
                "stream_url":   youtube_url,
                "display_name": display_name,
            })
        if r.status_code != 200:
            logger.warning("yt launch %s: worker returned %s", slug, r.status_code)
            return False
    except Exception as exc:
        logger.warning("yt launch %s: worker unreachable: %s", slug, exc)
        return False
    from leaf_server import _slug_to_worker
    _slug_to_worker[slug] = _YT_WORKER_URL
    if ends_at is None:
        ends_at = (_t.time() + duration_h * 3600) if duration_h > 0 else None
    _yt_meta[slug] = {
        "youtube_url":  youtube_url,
        "display_name": display_name,
        "category":     category,
        "ends_at":      ends_at,
        "started_at":   _t.time(),
        "scheduled_start_at": None,
        "pending_start":      False,
    }
    _recording_meta[slug] = {
        "display_name": display_name,
        "channel_name": display_name,
        "categories":   [category],
        "source":       "youtube",
        "youtube_url":  youtube_url,
        "started_at":   _t.time(),
        "ends_at":      ends_at,
        "auto_end_at":  ends_at,
        "account_id":   "",
        "stream_id":    "",
        "standalone":   True,
        "scheduled_start_at": None,
        "pending_start":      False,
        "duration_hours":     duration_h,
    }
    await async_save_recording_meta(slug, _recording_meta[slug])
    if ends_at:
        async def _yt_auto_stop(_slug=slug, _ea=ends_at):
            await asyncio.sleep(max(1, int(_ea - _t.time())))
            try:
                async with _hx.AsyncClient(timeout=10) as _c:
                    await _c.delete(f"{_YT_WORKER_URL}/internal/stop/{_slug}")
            except Exception:
                pass
            from leaf_server import _slug_to_worker as _s2w
            _s2w.pop(_slug, None)
            _yt_meta.pop(_slug, None)
            _recording_meta.pop(_slug, None)
            await async_delete_recording_meta(_slug)
        asyncio.create_task(_yt_auto_stop())
    logger.info("YouTube stream started: %s → %s (cat=%s)", slug, youtube_url[:60], category)
    return True


@_require_auth
async def _handle_yt_start(request: web.Request) -> web.Response:
    """POST /admin/api/yt/start — start a YouTube live stream recording."""
    try:
        body = await request.json()
    except Exception:
        return web.Response(status=400, text=json.dumps({"error": "Bad JSON"}),
                            content_type="application/json")
    youtube_url   = body.get("youtube_url", "").strip()
    slug          = body.get("slug", "").strip().lower().replace(" ", "-")
    display_name  = body.get("display_name", slug)
    duration_h    = float(body.get("duration_hours", 24))
    category      = body.get("category", "").strip() or "Sports"
    # start_at: optional UTC epoch seconds. If set and in the future, the stream is
    # SCHEDULED (persisted to Mongo, not started now) — the yt schedule poller fires
    # it when the wall clock (UTC epoch, timezone-independent) reaches start_at.
    try:
        start_at = int(body.get("start_at") or 0) or None
    except (TypeError, ValueError):
        start_at = None
    if not youtube_url or not slug:
        return web.Response(text=json.dumps({"error": "youtube_url and slug required"}),
                            content_type="application/json")
    import httpx as _hx, time as _t

    # ── Scheduled path: persist as pending and return without starting ──────────
    if start_at and start_at > _t.time() + 5:
        ends_at = (start_at + duration_h * 3600) if duration_h > 0 else None
        _yt_meta[slug] = {
            "youtube_url":  youtube_url,
            "display_name": display_name,
            "category":     category,
            "ends_at":      ends_at,
            "started_at":   None,
            "scheduled_start_at": start_at,
            "pending_start":      True,
        }
        _recording_meta[slug] = {
            "display_name": display_name,
            "channel_name": display_name,
            "categories":   [category],
            "source":       "youtube",
            "youtube_url":  youtube_url,
            "started_at":   None,
            "ends_at":      ends_at,
            "auto_end_at":  ends_at,
            "account_id":   "",
            "stream_id":    "",
            "standalone":   True,
            "scheduled_start_at": start_at,
            "pending_start":      True,
            "duration_hours":     duration_h,
        }
        await async_save_recording_meta(slug, _recording_meta[slug])
        logger.info("YouTube stream SCHEDULED: %s → %s (start_at=%d UTC, %.0fh cat=%s)",
                    slug, youtube_url[:60], start_at, duration_h, category)
        return web.Response(text=json.dumps({"ok": True, "slug": slug, "scheduled": True,
                                             "scheduled_start_at": start_at}),
                            content_type="application/json")
    ok = await _yt_launch_now(slug, youtube_url, display_name, category, duration_h)
    if not ok:
        return web.Response(text=json.dumps({"error": "yt-worker unreachable or refused"}),
                            content_type="application/json")
    return web.Response(text=json.dumps({"ok": True, "slug": slug}),
                        content_type="application/json")


@_require_auth
async def _handle_yt_stop(request: web.Request) -> web.Response:
    """DELETE /admin/api/yt/stop/{slug} — stop a YouTube stream."""
    slug = request.match_info["slug"]
    import httpx as _hx
    try:
        async with _hx.AsyncClient(timeout=10) as c:
            await c.delete(f"{_YT_WORKER_URL}/internal/stop/{slug}")
    except Exception as exc:
        logger.warning("yt stop request failed for %s: %s", slug, exc)
    from leaf_server import _slug_to_worker
    _slug_to_worker.pop(slug, None)
    _yt_meta.pop(slug, None)
    _recording_meta.pop(slug, None)
    await async_delete_recording_meta(slug)
    return web.Response(text=json.dumps({"ok": True}), content_type="application/json")


@_require_auth
async def _handle_yt_refresh(request: web.Request) -> web.Response:
    """POST /admin/api/yt/refresh/{slug} — force the yt-worker to re-extract the
    stream URL (recovers a buffering/black stream on a stale CDN token)."""
    slug = request.match_info["slug"]
    import httpx as _hx
    try:
        async with _hx.AsyncClient(timeout=20) as c:
            r = await c.post(f"{_YT_WORKER_URL}/internal/refresh/{slug}")
        if r.status_code != 200:
            return web.Response(text=json.dumps({"error": f"yt-worker returned {r.status_code}"}),
                                content_type="application/json")
    except Exception as exc:
        return web.Response(text=json.dumps({"error": f"yt-worker unreachable: {exc}"}),
                            content_type="application/json")
    logger.info("YouTube stream refresh requested: %s", slug)
    return web.Response(text=json.dumps({"ok": True}), content_type="application/json")


@_require_auth
async def _handle_yt_list(request: web.Request) -> web.Response:
    """GET /admin/api/yt/list — list active YouTube streams from yt-worker."""
    import httpx as _hx
    try:
        async with _hx.AsyncClient(timeout=8) as c:
            r = await c.get(f"{_YT_WORKER_URL}/internal/status")
        worker_status = r.json() if r.status_code == 200 else {}
    except Exception:
        worker_status = {}
    streams = []
    for slug, meta in _yt_meta.items():
        ws = worker_status.get(slug, {})
        pending = bool(meta.get("pending_start"))
        streams.append({
            "slug":         slug,
            "display_name": meta.get("display_name", slug),
            "youtube_url":  meta.get("youtube_url", ""),
            "category":     meta.get("category", "Sports"),
            "ends_at":      meta.get("ends_at"),
            "started_at":   meta.get("started_at"),
            "running":      (not pending) and ws.get("running", False),
            "segments":     ws.get("segments", 0),
            "stream_info":  ws.get("stream_info"),
            "pending":      pending,
            "scheduled_start_at": meta.get("scheduled_start_at"),
        })
    return web.Response(text=json.dumps({"streams": streams}), content_type="application/json")


@_require_auth
async def _handle_yt_info(request: web.Request) -> web.Response:
    """GET /admin/api/yt/info?url= — fetch YouTube title via yt-worker."""
    url = request.rel_url.query.get("url", "")
    if not url:
        return web.Response(status=400, text=json.dumps({"error": "url required"}),
                            content_type="application/json")
    import httpx as _hx
    try:
        async with _hx.AsyncClient(timeout=20) as c:
            r = await c.get(f"{_YT_WORKER_URL}/internal/info",
                            params={"url": url})
        return web.Response(text=r.text, content_type="application/json")
    except Exception as exc:
        return web.Response(text=json.dumps({"error": str(exc), "title": ""}),
                            content_type="application/json")


async def _handle_rebalance_workers(request: web.Request) -> web.Response:
    """Move excess streams from overloaded workers to the least-loaded one.

    Stops scheduler-managed streams that are over the per-worker target;
    the scheduler restarts them on the least-loaded worker on the next tick.
    """
    import math
    import iptv_scheduler as _sched
    from leaf_server import _parse_workers, _slug_to_worker
    from worker_client import status_of_worker

    workers = _parse_workers()
    if not workers:
        return web.Response(text=json.dumps({"error": "No workers configured"}),
                            content_type="application/json")

    # Fetch live stream counts for each worker
    statuses: dict[str, dict] = {}
    for wid, wurl in workers.items():
        s = await status_of_worker(wurl)
        statuses[wid] = s or {}

    total = sum(len(s) for s in statuses.values())
    if total == 0:
        return web.Response(text=json.dumps({"moved": 0, "msg": "Nothing to rebalance"}),
                            content_type="application/json")

    target = math.ceil(total / len(workers))
    moved = []

    # Build reverse map: slug → worker_id
    slug_to_wid = {slug: wid for wid, wurl in workers.items()
                   for slug in statuses.get(wid, {})}

    for wid, wurl in workers.items():
        count = len(statuses.get(wid, {}))
        excess = count - target
        if excess <= 0:
            continue
        # Pick scheduler-managed slugs to evict (they will be restarted by scheduler)
        candidates = [sl for sl in statuses.get(wid, {})
                      if sl in _sched._scheduler_recordings]
        for slug in candidates[:excess]:
            _sched._stop_scheduler_recording(slug)
            _sched._stop_miss_counts.pop(slug, None)
            moved.append({"slug": slug, "from": wid})

    return web.Response(
        text=json.dumps({"moved": len(moved), "target_per_worker": target, "slugs": moved}),
        content_type="application/json",
    )


# ══════════════════════════════════════════════════════════════════════════════
# Register routes into an existing aiohttp app
# ══════════════════════════════════════════════════════════════════════════════

def register_admin_routes(app: web.Application) -> None:
    """Call this from leaf_server.py / start_leaf_server() to mount admin routes."""
    _load_hidden_slugs()
    _load_hidden_match_ids()
    app.router.add_get("/admin",                            _handle_root)
    app.router.add_get("/admin/panel",                      _handle_panel)
    app.router.add_post("/admin/login",                     _handle_login)
    app.router.add_get("/admin/logout",                     _handle_logout)
    app.router.add_get("/admin/api/accounts",               _handle_list_accounts)
    app.router.add_post("/admin/api/accounts",              _handle_add_account)
    app.router.add_delete("/admin/api/accounts/{id}",       _handle_delete_account)
    app.router.add_get("/admin/api/channels",               _handle_channels)
    app.router.add_get("/admin/api/recordings",             _handle_list_recordings)
    app.router.add_post("/admin/api/recordings",            _handle_start_recording)
    app.router.add_delete("/admin/api/recordings/{slug}",   _handle_stop_recording)
    app.router.add_route("PATCH", "/admin/api/recordings/{slug}", _handle_patch_recording)
    app.router.add_post("/admin/api/recordings/{slug}/hide",   _handle_hide_recording)
    app.router.add_post("/admin/api/recordings/{slug}/unhide", _handle_unhide_recording)
    app.router.add_get("/admin/api/hidden_matches",                     _handle_get_hidden_matches)
    app.router.add_post("/admin/api/hidden_matches/{match_id}/hide",   _handle_hide_match)
    app.router.add_post("/admin/api/hidden_matches/{match_id}/unhide", _handle_unhide_match)
    # Schedule routes
    app.router.add_get("/admin/api/schedule/events",        _handle_schedule_events)
    app.router.add_get("/admin/api/schedule/capacity",      _handle_schedule_capacity)
    app.router.add_get("/admin/api/schedule/probe",         _handle_probe_channel)
    app.router.add_post("/admin/api/schedule/assign",       _handle_schedule_assign)
    app.router.add_delete("/admin/api/schedule/assign",     _handle_schedule_unassign)
    app.router.add_post("/admin/api/schedule/force-delete", _handle_force_delete)
    app.router.add_post("/admin/api/schedule/force-start",  _handle_force_start)
    app.router.add_get("/admin/api/groups",                 _handle_list_groups)
    app.router.add_post("/admin/api/groups",                _handle_add_group)
    app.router.add_delete("/admin/api/groups/{id}",         _handle_delete_group)
    app.router.add_get("/admin/api/rules",            _handle_list_rules)
    app.router.add_post("/admin/api/rules",           _handle_save_rule)
    app.router.add_delete("/admin/api/rules/{id}",    _handle_delete_rule)
    app.router.add_post("/admin/api/workers/rebalance",     _handle_rebalance_workers)
    # YouTube stream routes
    app.router.add_post("/admin/api/yt/start",              _handle_yt_start)
    app.router.add_delete("/admin/api/yt/stop/{slug}",      _handle_yt_stop)
    app.router.add_post("/admin/api/yt/refresh/{slug}",     _handle_yt_refresh)
    app.router.add_get("/admin/api/yt/list",                _handle_yt_list)
    app.router.add_get("/admin/api/yt/info",                _handle_yt_info)
    # Stream-stability status monitor (isolated — never touches streaming path)
    app.router.add_get("/admin/api/status",                 _handle_status_data)
    app.router.add_get("/status",                           _handle_status_page)
    logger.info("Admin panel registered at /admin (password: %s…)",
                ADMIN_PASSWORD[:3] + "***")

    # Restore admin-started recordings from disk
    _restore_admin_recordings()
