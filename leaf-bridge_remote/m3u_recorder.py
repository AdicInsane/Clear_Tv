"""
m3u_recorder.py — ffmpeg .ts → R2 segmentation recorder.

Connects once to the raw .ts stream, re-segments every 6s via ffmpeg stream-copy.
Uses -copyts + -avoid_negative_ts disabled (from cinearena fix) to keep timestamps
continuous across segments — avoids the 6s glitch caused by -reset_timestamps 1.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone as _tz
from typing import Optional

import httpx

from config import (
    DELAY_SECONDS,
    LIVE_WINDOW_SEGMENTS,
    MAX_FAILURES,
    R2_BUCKET_NAME,
    R2_PUBLIC_DOMAIN,
    R2_ACCESS_KEY_ID,
    R2_SECRET_ACCESS_KEY,
    R2_ENDPOINT,
)

logger = logging.getLogger("leaf.m3u_recorder")

_EXECUTOR = ThreadPoolExecutor(max_workers=100, thread_name_prefix="m3u-seg")

# ── R2 single-account upload ──────────────────────────────────────────────────
# Reads the first (and only) slot from R2_POOL, falls back to individual env vars.
# Failover pool removed — only one paid R2 account, backups were unreliable.
def _parse_r2_pool() -> dict:
    raw = os.getenv("R2_POOL", "").strip()
    if raw:
        parts = [p.strip() for p in raw.split(",")[0].split("|")]
        if len(parts) >= 4:
            return {
                "access_key_id":     parts[0],
                "secret_access_key": parts[1],
                "endpoint":          parts[2].rstrip("/"),
                "public_domain":     parts[3].rstrip("/"),
                "bucket":            parts[4] if len(parts) >= 5 else R2_BUCKET_NAME,
            }
    return {
        "access_key_id":     R2_ACCESS_KEY_ID,
        "secret_access_key": R2_SECRET_ACCESS_KEY,
        "endpoint":          R2_ENDPOINT,
        "public_domain":     R2_PUBLIC_DOMAIN,
        "bucket":            R2_BUCKET_NAME,
    }

_R2_SLOT: dict = _parse_r2_pool()


def _active_r2() -> dict:
    """Return the single configured R2 account."""
    return _R2_SLOT

active_m3u_tasks:  dict[str, asyncio.Task]       = {}
m3u_recordings:    dict[str, "M3URecordingState"] = {}

_SEG_DURATION = 6
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0"

# ── Account rotation ──────────────────────────────────────────────────────────
_WORKER_MASTER_URL  = os.getenv("WORKER_MASTER_URL", "").rstrip("/")
_last_rotation_at:  dict[str, float] = {}
_rotation_count:    dict[str, int]   = {}
_ROTATION_COOLDOWN  = 5.0    # reduced from 30s — 30s cooldown caused visible hangs on rc=0 drops
# Quick-exit count needed before we ask the master to rotate the account.
# Split by ffmpeg return code:
#   rc=8 / rc=251 = server ACTIVELY rejected us (auth fail, connection-limit,
#     ban). The account is bad right now — retrying the same one is pointless,
#     so rotate on the FIRST quick exit.
#   rc=0 = clean EOF / pipe drop. This is NEVER a reason to rotate the account:
#     the account is fine, the provider/CDN just dropped the connection. Rotating
#     on rc=0 only churns scarce accounts and makes a flaky-source stream WORSE
#     (a bad source keeps dropping on every account, so it burns through all
#     _MAX_ROTATIONS and still fails). rc=0 always reconnects the SAME account.
_ROTATION_TRIGGER          = 2   # rc=0 (transient drop) — informational only; the
                                 # can_rotate gate below requires rc in (8,251), so
                                 # rc=0 never actually rotates regardless of this value.
_ROTATION_TRIGGER_REJECT   = 1   # rc=8 / rc=251 (server reject — rotate at once
                                 # to a free account; a switch is instant and we
                                 # have free accounts, so no point waiting on the
                                 # rejected one)
_MAX_ROTATIONS      = 5

# Stall guard: ffmpeg can stay UP and keep DISPATCHING segments while none of them
# ever become servable (R2 uploads silently failing / stalling) — the playlist then
# returns 404 forever and viewers buffer, but the dispatch-based watchdog never fires
# (segments ARE being dispatched) and ffmpeg never exits (so no rc=0/rc=8 to trigger
# a rotation). This is the France-vs-Spain 4K-Fox hang. Guard: if no segment has
# UPLOADED SUCCESSFULLY (become servable) for this many seconds while ffmpeg is up,
# kill ffmpeg and force a rotation to a fresh account on the next loop iteration.
#
# 20s = ~3 missed 6s upload cycles. A healthy stream uploads every ~6s (last_upload_ok_at
# re-seeds on each ffmpeg restart, so reconnect gaps don't count — this only measures
# upload latency within a live session). 20s sits just ABOVE the 15s dispatch watchdog,
# so if BOTH dispatch and upload stall the dispatch watchdog fires first (normal restart);
# only the uploads-fail-while-dispatch-continues case reaches this guard. 45s was too
# slow — viewers should not buffer that long before we act.
_UPLOAD_STALL_ROTATE_SECS = 20.0
# Hard give-up ceiling: after this many consecutive quick exits (each <30s uptime)
# the stream is genuinely dead — no account can carry it (provider reject / offline
# channel). The recorder must EXIT (break the loop) so cleanup runs and the slot is
# freed, instead of looping forever reporting running=true with dead ffmpeg (a
# "zombie" that corrupts the master's account map and can expose a live account to
# the probe → HTTP 458). ~25 * (2–15s backoff) ≈ several minutes of hard failure.
#
# EXCEPTION: standalone=True (admin-added 24x7 channels). These must NEVER be
# permanently killed by the zombie guard — a provider slate/blip lasting a few
# minutes would otherwise delete a 24x7 channel forever until the operator
# manually re-adds it. For standalone streams the recorder instead backs off to
# _STANDALONE_RETRY_BACKOFF and keeps trying indefinitely (the account is briefly
# freed during the long sleep so a healthy account can be re-claimed on the next
# attempt). Scheduled events still give up at _MAX_QUICK_EXITS so their slot frees.
_MAX_QUICK_EXITS    = 25
# Long backoff a standalone 24x7 stream waits after hitting _MAX_QUICK_EXITS before
# trying again — long enough to not hammer a dead provider, short enough to recover
# quickly once the provider's feed comes back.
_STANDALONE_RETRY_BACKOFF = 60.0


def _persist_rotated_url(slug: str, new_url: str) -> None:
    """Update worker's own MongoDB assigned_streams so restart recovery uses the rotated URL."""
    try:
        from config import MONGODB_URI
        import os
        if not MONGODB_URI:
            return
        worker_id = os.getenv("WORKER_ID", "worker1")
        from pymongo import MongoClient
        col = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=3000, socketTimeoutMS=5000, connectTimeoutMS=5000)["stremio"]["leaf_workers"]
        col.update_one(
            {"_id": worker_id},
            {"$set": {f"assigned_streams.{slug}.stream_url": new_url}},
        )
        logger.info("[m3u/%s] persisted rotated URL to worker MongoDB", slug)
    except Exception as exc:
        logger.warning("[m3u/%s] could not persist rotated URL: %s", slug, exc)


async def _request_account_rotation(slug: str, current_url: str, bypass_cooldown: bool = False) -> Optional[str]:
    if not _WORKER_MASTER_URL:
        return None
    now = time.monotonic()
    # bypass_cooldown is set for reject codes (rc=8/251): the account we're on is
    # actively being rejected by the provider, so staying on it is pointless — we
    # must be allowed to rotate to a different account immediately. Without this,
    # a freshly-rotated account that itself gets rejected would trap the stream on
    # a dead account for the whole cooldown window (this caused a 5-min outage).
    # _MAX_ROTATIONS still caps total attempts, so this can't loop forever.
    if not bypass_cooldown and now - _last_rotation_at.get(slug, 0) < _ROTATION_COOLDOWN:
        logger.info("[m3u/%s] rotation cooldown active — skipping", slug)
        return None
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(
                f"{_WORKER_MASTER_URL}/internal/rotate_account",
                json={"slug": slug, "current_url": current_url},
            )
        if resp.status_code == 200:
            data    = resp.json()
            new_url = data.get("stream_url", "")
            if new_url:
                _last_rotation_at[slug] = now
                logger.info("[m3u/%s] account rotated → new acc=%s", slug, data.get("account_id", "?"))
                # Also update worker's own MongoDB so restart recovery uses new URL
                # (master may fail to update it if _slug_to_worker was empty post-restart)
                _persist_rotated_url(slug, new_url)
                return new_url
        elif resp.status_code == 409:
            logger.info("[m3u/%s] rotation: no free accounts available", slug)
        else:
            logger.warning("[m3u/%s] rotation: master returned HTTP %d", slug, resp.status_code)
    except Exception as exc:
        logger.warning("[m3u/%s] rotation request failed: %s", slug, exc)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Data model
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class M3USegment:
    index:         int
    filename:      str
    duration:      float
    r2_key:        str
    registered_at: float = field(default_factory=time.monotonic)
    dispatched_at: float = field(default_factory=time.monotonic)
    r2_domain:     str   = ""
    done:          bool  = False
    failed:        bool  = False
    discontinuity: bool  = False


@dataclass
class M3URecordingState:
    slug:             str
    title:            str  = ""
    stream_url:       str  = ""
    started_at:       str  = field(default_factory=lambda: datetime.now(_tz.utc).isoformat())
    session_id:       str  = field(default_factory=lambda: str(int(time.time())))
    segments:         list[M3USegment] = field(default_factory=list)
    first_segment_at: Optional[float]  = None
    stream_info:      Optional[dict]   = None
    trimmed_good:     int = 0   # good segments dropped during trim (offsets MEDIA-SEQUENCE)
    trimmed_disc:     int = 0   # failed segments dropped during trim (offsets DISCONTINUITY-SEQUENCE)
    last_upload_ok_at: Optional[float] = None  # monotonic time a segment last uploaded OK
                                               # (became servable). Drives the stall guard.


# ══════════════════════════════════════════════════════════════════════════════
# Segment trim — keeps memory bounded for long-running streams
# ══════════════════════════════════════════════════════════════════════════════

# How many segments to keep: full live window + delay buffer + small headroom.
# At 6s/seg and DELAY_SECONDS≈30, this is typically ~20-25 segments max,
# instead of growing without bound for multi-hour streams.
_KEEP_SEGS = LIVE_WINDOW_SEGMENTS + math.ceil(DELAY_SECONDS / _SEG_DURATION) + 5


def _trim_segments(state: M3URecordingState) -> None:
    excess = len(state.segments) - _KEEP_SEGS
    if excess <= 0:
        return
    drop = state.segments[:excess]
    state.trimmed_good += sum(1 for s in drop if not s.failed)
    state.trimmed_disc += sum(1 for s in drop if s.failed or s.discontinuity)
    del state.segments[:excess]


# ══════════════════════════════════════════════════════════════════════════════
# Playlist builder
# ══════════════════════════════════════════════════════════════════════════════

def build_m3u_playlist(state: M3URecordingState) -> Optional[str]:
    if not state.segments:
        return None

    now = time.monotonic()

    for seg in state.segments:
        if not seg.done and (now - seg.registered_at) >= DELAY_SECONDS:
            seg.done   = True
            seg.failed = True
            logger.warning("[m3u/%s] %s force-failed after %.0fs", state.slug, seg.filename, now - seg.registered_at)

    last_safe_idx = -1
    for i, s in enumerate(state.segments):
        if (now - s.registered_at) >= DELAY_SECONDS:
            last_safe_idx = i

    if last_safe_idx == -1:
        return None

    safe      = state.segments[:last_safe_idx + 1]
    good_safe = [s for s in safe if not s.failed]

    if not good_safe:
        return None

    good_window = good_safe[-LIVE_WINDOW_SEGMENTS:]
    first_good  = good_window[0]
    last_good   = good_window[-1]
    window      = [s for s in safe if first_good.index <= s.index <= last_good.index]

    target_dur  = max(math.ceil(s.duration) for s in good_window)
    good_before = state.trimmed_good + len([s for s in good_safe if s.index < first_good.index])
    disc_before = state.trimmed_disc + sum(1 for s in safe if s.index < first_good.index and (s.failed or s.discontinuity))

    _mono_now = time.monotonic()
    _wall_now = datetime.now(_tz.utc)
    _seg_wall = _wall_now - timedelta(
        seconds=_mono_now - first_good.registered_at
    )
    _pdt = _seg_wall.strftime("%Y-%m-%dT%H:%M:%S.") + f"{_seg_wall.microsecond // 1000:03d}Z"

    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{target_dur}",
        f"#EXT-X-MEDIA-SEQUENCE:{good_before}",
        f"#EXT-X-DISCONTINUITY-SEQUENCE:{disc_before}",
        "#EXT-X-ALLOW-CACHE:NO",
        f"#EXT-X-PROGRAM-DATE-TIME:{_pdt}",
    ]

    prev_index: Optional[int] = None
    for seg in window:
        if seg.failed:
            prev_index = None
            continue
        if seg.discontinuity:
            lines.append("#EXT-X-DISCONTINUITY")
        elif prev_index is not None and seg.index != prev_index + 1:
            lines.append("#EXT-X-DISCONTINUITY")
        elif prev_index is None and seg.index != first_good.index:
            lines.append("#EXT-X-DISCONTINUITY")
        lines.append(f"#EXTINF:{seg.duration:.3f},")
        domain = seg.r2_domain or _active_r2()["public_domain"]
        lines.append(f"{domain}/{seg.r2_key}")
        prev_index = seg.index

    return "\n".join(lines) + "\n"


# ══════════════════════════════════════════════════════════════════════════════
# R2 upload
# ══════════════════════════════════════════════════════════════════════════════

def _make_r2_client():
    import boto3
    from botocore.config import Config as BotoConfig
    slot = _active_r2()
    return boto3.client(
        "s3",
        endpoint_url=slot["endpoint"],
        aws_access_key_id=slot["access_key_id"],
        aws_secret_access_key=slot["secret_access_key"],
        config=BotoConfig(
            signature_version="s3v4",
            retries={"max_attempts": 2, "mode": "standard"},
            connect_timeout=5,
            read_timeout=15,
        ),
        region_name="auto",
    )


def _upload_ts_blocking(ts_data: bytes, r2_key: str) -> bool:
    slot = _active_r2()
    if not (slot["access_key_id"] and slot["secret_access_key"] and slot["endpoint"]):
        logger.error("R2 credentials missing — cannot upload %s", r2_key)
        return False
    try:
        _make_r2_client().put_object(
            Bucket=slot["bucket"],
            Key=r2_key,
            Body=ts_data,
            ContentType="video/MP2T",
            CacheControl="public, max-age=31536000, immutable",
        )
        return True
    except Exception as exc:
        logger.warning("R2 upload error (%s): %s", r2_key, exc)
        return False


# ══════════════════════════════════════════════════════════════════════════════
# ffmpeg-based recorder
# ══════════════════════════════════════════════════════════════════════════════

def _to_ts_url(url: str) -> str:
    return re.sub(r'\.m3u8(\?.*)?$', r'.ts', url)


def _probe_duration(path: str) -> float:
    """Return actual duration of a .ts file. Used for the last segment of a session
    which may be shorter than _SEG_DURATION if ffmpeg was killed mid-segment."""
    import subprocess
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
             "-of", "csv=p=0", path],
            capture_output=True, timeout=5,
        )
        v = float(r.stdout.strip())
        return v if v > 0 else float(_SEG_DURATION)
    except Exception:
        return float(_SEG_DURATION)


def _probe_segment(path: str) -> Optional[dict]:
    import subprocess, json
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", "-show_format", path],
            capture_output=True, timeout=8,
        )
        data    = json.loads(out.stdout)
        streams = data.get("streams", [])
        fmt     = data.get("format", {})
    except Exception:
        return None
    v = next((s for s in streams if s.get("codec_type") == "video"), None)
    a = next((s for s in streams if s.get("codec_type") == "audio"), None)
    if not v:
        return None
    w, h = v.get("width", 0), v.get("height", 0)
    res = f"{w}x{h}" if w and h else "unknown"
    bw  = int(v.get("bit_rate") or 0)
    if not bw:
        bw = int(v.get("tags", {}).get("BPS", 0) or 0)
    if not bw:
        bw = int(fmt.get("bit_rate") or 0)
    ch = int(a.get("channels", 2)) if a else 2
    return {
        "resolution":         res,
        "width":              w,
        "height":             h,
        "video_codec":        v.get("codec_name", ""),
        "audio_codec":        a.get("codec_name", "") if a else "",
        "audio":              "5.1 Surround" if ch >= 6 else "Stereo",
        "audio_channels":     ch,
        "video_bitrate_kbps": round(bw / 1000) if bw else 0,
        "required_mbps":      round(bw / 1_000_000 * 1.2, 1) if bw else None,
    }


async def record_m3u_channel(
    slug:       str,
    stream_url: str,
    headers:    dict,
    proxies:    list[str],
    standalone: bool = False,
) -> None:
    state = M3URecordingState(slug=slug, stream_url=stream_url)
    m3u_recordings[slug] = state

    ts_url     = _to_ts_url(stream_url)
    tmpdir     = f"/tmp/leaf_segs/{slug}"
    loop       = asyncio.get_running_loop()
    session_id = state.session_id
    next_index = 1

    ff_hdrs = f"User-Agent: {_UA}\r\n"
    for k, v in headers.items():
        if k.lower() != "user-agent":
            ff_hdrs += f"{k}: {v}\r\n"

    logger.info("[m3u/%s] ffmpeg recorder started → %s", slug, ts_url[:80])

    import random
    await asyncio.sleep(random.uniform(0, 30))  # stagger startup to prevent thundering herd

    consecutive_quick_exits = 0
    _last_ffmpeg_died_at: float = 0.0   # monotonic time when ffmpeg last exited
    force_rotate_next = False           # set by the stall guard: rotate on next loop
                                        # even though ffmpeg didn't rc-exit on its own
    while True:
        if os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir, ignore_errors=True)
        os.makedirs(tmpdir, exist_ok=True)

        proc = None
        ffmpeg_started_at = time.monotonic()
        # No pre-emptive restart — test data shows provider holds connections indefinitely.
        # We only reconnect when the provider drops us (rc=0) or rejects us (rc=8).
        try:
            # -copyts + -avoid_negative_ts disabled preserves continuous timestamps
            # across segment boundaries — prevents the 6s glitch players show with
            # -reset_timestamps 1 (timestamps jump back to 0 on every segment).
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y",
                "-reconnect",           "1",
                "-reconnect_delay_max", "2",
                "-rw_timeout",          "5000000",
                "-headers",             ff_hdrs,
                "-copyts",
                "-i",                   ts_url,
                "-c",                   "copy",
                "-avoid_negative_ts",   "disabled",
                "-f",                   "segment",
                "-segment_time",        str(_SEG_DURATION),
                "-segment_format",      "mpegts",
                os.path.join(tmpdir, "seg_%05d.ts"),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )

            seg_counter          = 0
            in_flight:  list[asyncio.Task] = []
            last_seg_in_session: Optional[float] = None
            last_probe_time: float = 0.0
            # Seed the upload-stall clock for THIS session so a fresh start gets a
            # full grace window before the stall guard can fire.
            state.last_upload_ok_at = time.monotonic()

            while True:
                exited  = proc.returncode is not None
                current = os.path.join(tmpdir, f"seg_{seg_counter:05d}.ts")
                nxt     = os.path.join(tmpdir, f"seg_{seg_counter+1:05d}.ts")

                seg_ready = os.path.exists(current) and (os.path.exists(nxt) or exited)

                if seg_ready:
                    try:
                        if state.stream_info is None or (time.monotonic() - last_probe_time) >= 900:
                            info = await loop.run_in_executor(_EXECUTOR, _probe_segment, current)
                            if info:
                                state.stream_info = info
                                last_probe_time = time.monotonic()
                                logger.info("[m3u/%s] stream info: %s %s ~%s Mbps",
                                            slug, info["resolution"], info["audio"], info["required_mbps"])
                        with open(current, "rb") as _f:
                            data = _f.read()
                        # Probe actual duration before deleting — only for the last
                        # segment of a session which may be short if ffmpeg was killed.
                        _is_last = exited and not os.path.exists(nxt)
                        _actual_dur = (
                            await loop.run_in_executor(_EXECUTOR, _probe_duration, current)
                            if _is_last else float(_SEG_DURATION)
                        )
                        os.unlink(current)
                    except Exception as e:
                        logger.warning("[m3u/%s] read seg %d: %s", slug, seg_counter, e)
                        seg_counter += 1
                        if exited and not os.path.exists(os.path.join(tmpdir, f"seg_{seg_counter:05d}.ts")):
                            break
                        continue

                    idx        = next_index
                    next_index += 1
                    filename   = f"seg_{idx}.ts"
                    r2_key     = f"leaf/{slug}/{session_id}/{filename}"
                    now_mono   = time.monotonic()
                    # Stagger registered_at by exactly _SEG_DURATION from the previous segment
                    # so burst dispatches (ffmpeg catching up on buffered content) don't make
                    # multiple segments safe simultaneously and cause window jumps.
                    if state.segments:
                        _reg_at = min(state.segments[-1].registered_at + _SEG_DURATION, now_mono)
                    else:
                        _reg_at = now_mono - _SEG_DURATION
                    # First segment of a restart: only mark discontinuity if ffmpeg was
                    # down long enough for source timestamps to genuinely break (>= 15s).
                    # Pre-emptive kills and fast rc=0 reconnects (< 15s downtime) preserve
                    # -copyts timestamps — skipping the tag eliminates the player rewind.
                    _is_restart_boundary = seg_counter == 0 and bool(state.segments)
                    _downtime = (time.monotonic() - _last_ffmpeg_died_at) if _last_ffmpeg_died_at > 0 else 99.0
                    _mark_disc = _is_restart_boundary and _downtime >= 15.0
                    if _is_restart_boundary and not _mark_disc:
                        logger.info("[m3u/%s] fast restart (%.1fs down) — no discontinuity tag, seamless resume",
                                    slug, _downtime)
                    elif _mark_disc:
                        logger.info("[m3u/%s] slow restart (%.1fs down) — marking discontinuity",
                                    slug, _downtime)
                    seg = M3USegment(
                        index=idx,
                        filename=filename,
                        duration=_actual_dur,
                        r2_key=r2_key,
                        registered_at=_reg_at,
                        dispatched_at=now_mono,
                        discontinuity=_mark_disc,
                    )
                    state.segments.append(seg)
                    _trim_segments(state)
                    if state.first_segment_at is None:
                        state.first_segment_at = now_mono

                    async def _upload(s=seg, d=data):
                        ok = await loop.run_in_executor(_EXECUTOR, _upload_ts_blocking, d, s.r2_key)
                        s.r2_domain = _active_r2()["public_domain"] if ok else ""
                        s.done      = True
                        s.failed    = not ok
                        if not ok:
                            logger.warning("[m3u/%s] ✗ R2 upload failed: %s", slug, s.filename)
                        else:
                            # Stamp last successful (servable) upload for the stall guard.
                            state.last_upload_ok_at = time.monotonic()
                            logger.debug("[m3u/%s] ✓ %s", slug, s.filename)

                    t = asyncio.create_task(_upload())
                    in_flight.append(t)
                    in_flight[:] = [t for t in in_flight if not t.done()]

                    logger.info("[m3u/%s] seg %d dispatched", slug, idx)
                    last_seg_in_session = time.monotonic()
                    seg_counter += 1

                    if exited and not os.path.exists(os.path.join(tmpdir, f"seg_{seg_counter:05d}.ts")):
                        break
                elif exited:
                    break
                else:
                    now = time.monotonic()
                    # Watchdog: kill ffmpeg if it's been running but silent for 15s THIS session.
                    # 15s ≈ 2.5 missed 6s segments — tight enough to cut frozen-stream detection
                    # (was 20s), loose enough not to false-kill a healthy stream during a normal
                    # bursty gap (network jitter / R2 lag / ffmpeg's own -reconnect_delay_max 2).
                    if last_seg_in_session is not None and now - last_seg_in_session > 15:
                        logger.warning("[m3u/%s] no segment for %.0fs — killing hung ffmpeg",
                                       slug, now - last_seg_in_session)
                        proc.kill()
                        try:
                            await asyncio.wait_for(proc.wait(), timeout=3)
                        except asyncio.TimeoutError:
                            pass
                    # Stall guard: ffmpeg is UP and may even be dispatching segments,
                    # but NONE are becoming servable (R2 uploads failing/stalling) →
                    # playlist 404s forever and the dispatch watchdog above never fires
                    # because segments ARE flowing. If no segment has uploaded OK for
                    # _UPLOAD_STALL_ROTATE_SECS, the account/edge is bad in a way a
                    # same-account reconnect can't fix — kill ffmpeg and force a rotate
                    # to a fresh account on the next loop iteration.
                    elif (
                        state.last_upload_ok_at is not None
                        and now - state.last_upload_ok_at > _UPLOAD_STALL_ROTATE_SECS
                        and _WORKER_MASTER_URL
                        and _rotation_count.get(slug, 0) < _MAX_ROTATIONS
                    ):
                        logger.warning(
                            "[m3u/%s] no servable segment for %.0fs (uploads stalled, ffmpeg up) "
                            "— killing ffmpeg to force account rotation",
                            slug, now - state.last_upload_ok_at,
                        )
                        force_rotate_next = True
                        proc.kill()
                        try:
                            await asyncio.wait_for(proc.wait(), timeout=3)
                        except asyncio.TimeoutError:
                            pass
                    await asyncio.sleep(0.3)

            if in_flight:
                # Wait at most 2s; remaining uploads continue in background threads
                await asyncio.wait(in_flight, timeout=2.0)

            rc = proc.returncode if proc else -1
            _last_ffmpeg_died_at = time.monotonic()   # stamp exact death time for discontinuity logic

            # Pick up URL change if leaf_main.py rotated the token
            injected = m3u_recordings.get(slug)
            if injected and injected.stream_url and injected.stream_url != stream_url:
                stream_url = injected.stream_url
                ts_url     = _to_ts_url(stream_url)
                logger.info("[m3u/%s] URL rotated, restarting with new ts_url", slug)

            uptime = time.monotonic() - ffmpeg_started_at

            # Stall-guard rotation: ffmpeg was killed because uploads stalled while it
            # stayed up (a long-uptime kill, so the uptime<30 path below won't catch
            # it). Rotate to a fresh account NOW, bypassing the quick-exit counter.
            if force_rotate_next:
                force_rotate_next = False
                if _rotation_count.get(slug, 0) < _MAX_ROTATIONS:
                    new_url = await _request_account_rotation(slug, stream_url, bypass_cooldown=True)
                    if new_url:
                        stream_url = new_url
                        ts_url     = _to_ts_url(new_url)
                        injected_sr = m3u_recordings.get(slug)
                        if injected_sr:
                            injected_sr.stream_url = new_url
                        _rotation_count[slug] = _rotation_count.get(slug, 0) + 1
                        consecutive_quick_exits = 0
                        logger.info("[m3u/%s] stall-guard rotated account, restarting", slug)
                        await asyncio.sleep(random.uniform(0, 0.3))
                        continue
                    # No free account / cooldown — fall through and reconnect same
                    # account after a short delay rather than hammering instantly.
                    logger.warning("[m3u/%s] stall-guard rotation found no account — reconnecting same", slug)
                    await asyncio.sleep(3 + random.uniform(0, 1))
                    continue

            if uptime < 30:
                consecutive_quick_exits += 1

                # Per-code rotation threshold: reject codes (8/251) rotate on the
                # first quick exit; a clean drop (0) waits for _ROTATION_TRIGGER.
                is_reject     = rc in (8, 251)
                rotate_after  = _ROTATION_TRIGGER_REJECT if is_reject else _ROTATION_TRIGGER

                # Pre-reconnect backoff: first couple of retries ~3-4s, then back off
                # to ~5-6s once we've failed repeatedly (protects the provider from a
                # tight reconnect loop). Capped at 5s base — 8s left live streams dark
                # too long during provider blips; 2s was too tight (risks pushing a
                # flaky provider from rc=0 drops into rc=8 rejections).
                delay  = 5 if consecutive_quick_exits >= 3 else 3
                jitter = random.uniform(0, 1)

                # ONLY rotate the account on a genuine rejection (rc=8/251 = auth
                # fail / connection-limit / ban — the account is actually bad).
                # A clean drop (rc=0) means the account is FINE and the provider
                # just dropped the connection (CDN blip) — reconnect on the SAME
                # account instead of switching. Rotating on rc=0 churns accounts
                # (burns the _MAX_ROTATIONS budget then keeps exiting anyway on a
                # bad source) and makes the stream WORSE, not better — a bad source
                # drops on every account, so switching accounts never helps rc=0.
                can_rotate = (
                    rc in (8, 251)
                    and consecutive_quick_exits >= rotate_after
                    and _WORKER_MASTER_URL
                )

                if can_rotate and _rotation_count.get(slug, 0) < _MAX_ROTATIONS:
                    # Reject codes mean the current account is refused by the
                    # provider — bypass the cooldown so we can jump to a different
                    # account at once instead of retrying the dead one for minutes.
                    new_url = await _request_account_rotation(slug, stream_url, bypass_cooldown=is_reject)
                    if new_url:
                        stream_url = new_url
                        ts_url     = _to_ts_url(new_url)
                        injected2  = m3u_recordings.get(slug)
                        if injected2:
                            injected2.stream_url = new_url
                        _rotation_count[slug] = _rotation_count.get(slug, 0) + 1
                        consecutive_quick_exits = 0
                        delay  = 0
                        jitter = random.uniform(0, 0.3)
                    # else: rotation returned nothing (no free account / cooldown).
                    # Fall through with the backoff delay set above so we don't
                    # hammer the dead account every 2s while waiting for a slot.
                elif can_rotate and _rotation_count.get(slug, 0) >= _MAX_ROTATIONS:
                    logger.warning("[m3u/%s] rotation limit (%d) reached — stream likely dead",
                                   slug, _MAX_ROTATIONS)

                # ── Zombie guard: give up and EXIT so the slot is freed ──────────
                # If we've had too many consecutive quick exits, no account can
                # carry this stream (provider reject / channel offline). Keeping the
                # loop alive makes the worker report running=true with dead ffmpeg —
                # a zombie that poisons the master's account map and can leak a live
                # account to the probe. Break out so the cleanup below runs
                # (m3u_recordings.pop / active_m3u_tasks.pop) and the account frees.
                if consecutive_quick_exits >= _MAX_QUICK_EXITS:
                    if standalone:
                        # Admin-added 24x7 channel: NEVER give up. The provider is
                        # likely serving a slate/blip. Back off for a long interval
                        # (account briefly freed during the sleep so a fresh account
                        # can be re-claimed via rotation on the next attempt) and keep
                        # retrying. Reset the counter so the next batch of failures
                        # gets a full quota before backing off again.
                        logger.warning(
                            "[m3u/%s] %d consecutive quick exits (rc=%s) — standalone 24x7 "
                            "stream, NOT giving up; backing off %.0fs and retrying",
                            slug, consecutive_quick_exits, rc, _STANDALONE_RETRY_BACKOFF,
                        )
                        consecutive_quick_exits = 0
                        _rotation_count.pop(slug, None)
                        await asyncio.sleep(_STANDALONE_RETRY_BACKOFF)
                        continue
                    logger.error(
                        "[m3u/%s] giving up after %d consecutive quick exits (rc=%s) — "
                        "exiting recorder so the account/slot is freed (was zombie-looping)",
                        slug, consecutive_quick_exits, rc,
                    )
                    break
            else:
                consecutive_quick_exits = 0
                _rotation_count.pop(slug, None)
                delay = 0
                jitter = random.uniform(0, 0.3)
            logger.warning("[m3u/%s] ffmpeg exited rc=%s (uptime=%.0fs, attempt=%d) — restarting in %.1fs",
                           slug, rc, uptime, consecutive_quick_exits, delay + jitter)
            await asyncio.sleep(delay + jitter)

        except asyncio.CancelledError:
            logger.info("[m3u/%s] Recording cancelled", slug)
            if proc and proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except asyncio.TimeoutError:
                    proc.kill()
            break
        except Exception as exc:
            logger.exception("[m3u/%s] Unexpected error: %s", slug, exc)
            if proc and proc.returncode is None:
                proc.terminate()
            await asyncio.sleep(5)

    shutil.rmtree(tmpdir, ignore_errors=True)
    m3u_recordings.pop(slug, None)
    active_m3u_tasks.pop(slug, None)
    logger.info("[m3u/%s] Recording finished", slug)


# ══════════════════════════════════════════════════════════════════════════════
# Public control functions
# ══════════════════════════════════════════════════════════════════════════════

def start_m3u_recording(slug: str, stream_url: str, headers: dict, proxies: list[str],
                        standalone: bool = False) -> None:
    if slug in active_m3u_tasks:
        state = m3u_recordings.get(slug)
        if state and stream_url and state.stream_url != stream_url:
            state.stream_url = stream_url
            logger.info("[m3u/%s] URL rotated into running recorder", slug)
        return
    task = asyncio.create_task(record_m3u_channel(slug, stream_url, headers, proxies, standalone))
    active_m3u_tasks[slug] = task


def stop_m3u_recording(slug: str) -> None:
    task = active_m3u_tasks.pop(slug, None)
    if task:
        task.cancel()
    # Remove from m3u_recordings immediately so /internal/status is clean right away.
    # The coroutine also pops on CancelledError, but that's async — this is synchronous
    # and ensures the admin panel never sees a stopped slug as still "running".
    m3u_recordings.pop(slug, None)


# ══════════════════════════════════════════════════════════════════════════════
# R2 segment cleanup — deletes objects older than 2 hours from the active slot
# ══════════════════════════════════════════════════════════════════════════════

_R2_CLEANUP_AGE_SECS = 2 * 3600   # 2 hours
_R2_CLEANUP_INTERVAL = 30 * 60    # run every 30 minutes
_R2_CLEANUP_PREFIX   = "leaf/"    # only touch our own prefix


def _cleanup_old_r2_segments_blocking() -> None:
    """Delete R2 objects under leaf/ that are older than 2 hours. Runs in executor."""
    import datetime
    slot = _active_r2()
    if not (slot["access_key_id"] and slot["secret_access_key"] and slot["endpoint"]):
        return
    try:
        import boto3
        from botocore.config import Config as BotoConfig
        client = boto3.client(
            "s3",
            endpoint_url=slot["endpoint"],
            aws_access_key_id=slot["access_key_id"],
            aws_secret_access_key=slot["secret_access_key"],
            config=BotoConfig(signature_version="s3v4",
                              connect_timeout=10, read_timeout=30),
            region_name="auto",
        )
        bucket   = slot["bucket"]
        cutoff   = datetime.datetime.now(datetime.timezone.utc) - \
                   datetime.timedelta(seconds=_R2_CLEANUP_AGE_SECS)
        paginator = client.get_paginator("list_objects_v2")
        to_delete: list[dict] = []
        total_scanned = 0

        for page in paginator.paginate(Bucket=bucket, Prefix=_R2_CLEANUP_PREFIX):
            for obj in page.get("Contents", []):
                total_scanned += 1
                if obj["LastModified"] < cutoff:
                    to_delete.append({"Key": obj["Key"]})

                # Delete in batches of 1000 (S3 limit)
                if len(to_delete) >= 1000:
                    client.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": to_delete, "Quiet": True},
                    )
                    logger.info("R2 cleanup: deleted %d objects (batch)", len(to_delete))
                    to_delete = []

        if to_delete:
            client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": to_delete, "Quiet": True},
            )

        logger.info(
            "R2 cleanup: scanned %d objects, deleted %d older than 2h (bucket=%s)",
            total_scanned, len(to_delete), bucket,
        )
    except Exception as exc:
        logger.warning("R2 cleanup error: %s", exc)


async def run_r2_cleanup_loop() -> None:
    """Async background task — cleans up old R2 segments every 30 minutes."""
    import asyncio as _aio
    loop = _aio.get_running_loop()
    logger.info("R2 cleanup loop started (age=2h, interval=30min, prefix=%s)", _R2_CLEANUP_PREFIX)
    while True:
        await _aio.sleep(_R2_CLEANUP_INTERVAL)
        try:
            await loop.run_in_executor(_EXECUTOR, _cleanup_old_r2_segments_blocking)
        except Exception as exc:
            logger.warning("R2 cleanup loop error: %s", exc)
