"""
recorder.py - ffmpeg .ts → Cloudflare R2 segmentation recorder.

Architecture:
  1. ffmpeg connects to the Xtream raw .ts URL and splits it into 6s segments
     on local disk using stream-copy (no re-encode).
  2. Each segment is uploaded to R2 in a background thread.
  3. A virtual clock (registered_at + DELAY_SECONDS) controls when a segment
     is safe to serve — gives R2 upload time to complete.
  4. build_playlist() returns an HLS playlist whose segment lines are absolute
     R2 CDN URLs.  The leaf never serves segment bytes — R2/CDN does.

Ported from leaf-bridge_remote/m3u_recorder.py with the multi-worker, account-
rotation, and proxy layers stripped out (single-instance leaf, no master/worker
split).
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

import config

logger = logging.getLogger("leaf.recorder")

_EXECUTOR = ThreadPoolExecutor(max_workers=40, thread_name_prefix="leaf-seg")

# ── active state ──────────────────────────────────────────────────────────────
active_tasks:    dict[str, asyncio.Task]        = {}
recordings:      dict[str, "RecordingState"]    = {}

# ── constants ─────────────────────────────────────────────────────────────────
_SEG_DURATION  = config.SEG_DURATION
_DELAY         = config.DELAY_SECONDS
_WINDOW        = config.LIVE_WINDOW_SEGMENTS
_MAX_FAILURES  = config.MAX_FAILURES

# Keep enough segments for the live window + delay buffer + small headroom.
_KEEP_SEGS = _WINDOW + math.ceil(_DELAY / _SEG_DURATION) + 5

# How long with no new servable segment before we consider the stream stalled.
_STALL_SECS = 20.0

@dataclass
class Segment:
    index:         int
    filename:      str
    duration:      float
    r2_key:        str
    registered_at: float = field(default_factory=time.monotonic)
    done:          bool  = False
    failed:        bool  = False
    discontinuity: bool  = False


@dataclass
class RecordingState:
    slug:             str
    stream_url:       str  = ""
    title:            str  = ""
    started_at:       str  = field(default_factory=lambda: datetime.now(_tz.utc).isoformat())
    session_id:       str  = field(default_factory=lambda: str(int(time.time())))
    segments:         list[Segment] = field(default_factory=list)
    first_segment_at: Optional[float] = None
    stream_info:      Optional[dict]  = None
    trimmed_good:     int = 0
    trimmed_disc:     int = 0
    last_upload_ok_at: Optional[float] = None


# ── helpers ───────────────────────────────────────────────────────────────────

def _trim(state: RecordingState) -> None:
    excess = len(state.segments) - _KEEP_SEGS
    if excess <= 0:
        return
    drop = state.segments[:excess]
    state.trimmed_good += sum(1 for s in drop if not s.failed)
    state.trimmed_disc += sum(1 for s in drop if s.failed or s.discontinuity)
    del state.segments[:excess]


def to_ts_url(url: str) -> str:
    """
    Convert an .m3u8 URL to its .ts equivalent (Xtream raw stream).

    Public because admin.py's pre-add check must verify the exact URL ffmpeg will
    be given — checking the .m3u8 while recording the .ts proves nothing.
    """
    return re.sub(r"\.m3u8(\?.*)?$", ".ts", url)


# Kept so existing internal call sites stay valid.
_to_ts_url = to_ts_url


# ── playlist builder ──────────────────────────────────────────────────────────

def build_playlist(state: RecordingState) -> Optional[str]:
    """
    Build the HLS playlist served at /m3u/{slug}/live.m3u8.

    Segment lines are absolute R2 CDN URLs — the leaf is not in the segment path.
    Returns None while still buffering (before DELAY_SECONDS worth of segments).
    """
    if not state.segments:
        return None

    now = time.monotonic()

    # Force-fail segments that have been waiting longer than the delay window
    # without completing their upload — they are blocking the window.
    for seg in state.segments:
        if not seg.done and (now - seg.registered_at) >= _DELAY:
            seg.done   = True
            seg.failed = True
            logger.warning("[%s] seg %d force-failed after %.0fs wait",
                           state.slug, seg.index, now - seg.registered_at)

    last_safe_idx = -1
    for i, s in enumerate(state.segments):
        if (now - s.registered_at) >= _DELAY:
            last_safe_idx = i

    if last_safe_idx == -1:
        return None   # still buffering

    safe      = state.segments[: last_safe_idx + 1]
    good_safe = [s for s in safe if not s.failed]
    if not good_safe:
        return None

    good_window = good_safe[-_WINDOW:]
    first_good  = good_window[0]
    last_good   = good_window[-1]
    window      = [s for s in safe if first_good.index <= s.index <= last_good.index]

    target_dur  = max(math.ceil(s.duration) for s in good_window)
    good_before = state.trimmed_good + len([s for s in good_safe if s.index < first_good.index])
    disc_before = state.trimmed_disc + sum(
        1 for s in safe if s.index < first_good.index and (s.failed or s.discontinuity)
    )

    mono_now = time.monotonic()
    wall_now = datetime.now(_tz.utc)
    seg_wall = wall_now - timedelta(seconds=mono_now - first_good.registered_at)
    pdt = seg_wall.strftime("%Y-%m-%dT%H:%M:%S.") + f"{seg_wall.microsecond // 1000:03d}Z"

    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{target_dur}",
        f"#EXT-X-MEDIA-SEQUENCE:{good_before}",
        f"#EXT-X-DISCONTINUITY-SEQUENCE:{disc_before}",
        "#EXT-X-ALLOW-CACHE:NO",
        f"#EXT-X-PROGRAM-DATE-TIME:{pdt}",
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
        lines.append(f"{config.R2_PUBLIC_DOMAIN}/{seg.r2_key}")
        prev_index = seg.index

    return "\n".join(lines) + "\n"


# ── R2 upload ─────────────────────────────────────────────────────────────────

def _make_r2_client():
    import boto3
    from botocore.config import Config as BotoConfig
    return boto3.client(
        "s3",
        endpoint_url=config.R2_ENDPOINT,
        aws_access_key_id=config.R2_ACCESS_KEY_ID,
        aws_secret_access_key=config.R2_SECRET_ACCESS_KEY,
        config=BotoConfig(
            signature_version="s3v4",
            retries={"max_attempts": 2, "mode": "standard"},
            connect_timeout=5,
            read_timeout=15,
        ),
        region_name="auto",
    )


def _upload_ts_blocking(ts_data: bytes, r2_key: str) -> bool:
    try:
        _make_r2_client().put_object(
            Bucket=config.R2_BUCKET_NAME,
            Key=r2_key,
            Body=ts_data,
            ContentType="video/MP2T",
            CacheControl="public, max-age=31536000, immutable",
        )
        return True
    except Exception as exc:
        logger.warning("R2 upload error (%s): %s", r2_key, exc)
        return False


# ── segment probing ───────────────────────────────────────────────────────────

def _probe_duration(path: str) -> float:
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
    import subprocess, json as _json
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_streams", "-show_format", path],
            capture_output=True, timeout=8,
        )
        data    = _json.loads(out.stdout)
        streams = data.get("streams", [])
        fmt     = data.get("format", {})

        video = next((s for s in streams if s.get("codec_type") == "video"), {})
        audio = next((s for s in streams if s.get("codec_type") == "audio"), {})

        width  = video.get("width", 0)
        height = video.get("height", 0)
        vbr    = int(video.get("bit_rate") or fmt.get("bit_rate") or 0)
        abr    = int(audio.get("bit_rate") or 0)
        total  = (vbr + abr) / 1_000_000

        return {
            "width":  width,
            "height": height,
            "resolution": f"{width}x{height}" if width else "unknown",
            "video_codec": video.get("codec_name", ""),
            "audio_codec": audio.get("codec_name", ""),
            "video_bitrate_kbps": vbr // 1000 if vbr else None,
            "required_mbps": round(total, 2) if total else None,
        }
    except Exception:
        return None


# ── ffmpeg recording loop ─────────────────────────────────────────────────────

async def record_channel(slug: str, stream_url: str, title: str = "") -> None:
    """
    Core recording coroutine. Runs until cancelled (stop_recording) or the
    task is torn down. Restarts ffmpeg automatically on disconnect.
    """
    import random

    state = RecordingState(slug=slug, stream_url=stream_url, title=title or slug)
    recordings[slug] = state

    ts_url     = _to_ts_url(stream_url)
    tmpdir     = f"/tmp/cleartv_segs/{slug}"
    loop       = asyncio.get_running_loop()
    session_id = state.session_id
    next_index = 1

    ff_hdrs = f"User-Agent: {config.UPSTREAM_USER_AGENT}\r\n"

    logger.info("[%s] recorder started → %s", slug, ts_url[:80])

    # Stagger startup across multiple channels so they don't all hammer R2
    # and the provider simultaneously on a restart.
    await asyncio.sleep(random.uniform(0, 10))

    consecutive_quick_exits = 0
    last_ffmpeg_died_at: float = 0.0

    while True:
        if os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir, ignore_errors=True)
        os.makedirs(tmpdir, exist_ok=True)

        proc = None
        ffmpeg_started_at = time.monotonic()

        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y",
                "-reconnect",            "1",
                "-reconnect_delay_max",  "2",
                "-rw_timeout",           "5000000",
                "-headers",              ff_hdrs,
                "-copyts",
                "-i",                    ts_url,
                "-c",                    "copy",
                "-avoid_negative_ts",    "disabled",
                "-f",                    "segment",
                "-segment_time",         str(_SEG_DURATION),
                "-segment_format",       "mpegts",
                os.path.join(tmpdir, "seg_%05d.ts"),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )

            seg_counter       = 0
            in_flight:  list[asyncio.Task] = []
            last_seg_time: Optional[float] = None
            last_probe_time: float         = 0.0
            state.last_upload_ok_at        = time.monotonic()

            while True:
                exited  = proc.returncode is not None
                current = os.path.join(tmpdir, f"seg_{seg_counter:05d}.ts")
                nxt     = os.path.join(tmpdir, f"seg_{seg_counter + 1:05d}.ts")

                seg_ready = os.path.exists(current) and (os.path.exists(nxt) or exited)

                if seg_ready:
                    try:
                        if state.stream_info is None or (time.monotonic() - last_probe_time) >= 900:
                            info = await loop.run_in_executor(_EXECUTOR, _probe_segment, current)
                            if info:
                                state.stream_info = info
                                last_probe_time = time.monotonic()
                                # Update channel store so the catalog shows quality
                                try:
                                    from channels import store as _cs
                                    _cs.update(slug, stream_info=info)
                                except Exception:
                                    pass

                        with open(current, "rb") as fh:
                            data = fh.read()

                        is_last   = exited and not os.path.exists(nxt)
                        actual_dur = (
                            await loop.run_in_executor(_EXECUTOR, _probe_duration, current)
                            if is_last else float(_SEG_DURATION)
                        )
                        os.unlink(current)
                    except Exception as exc:
                        logger.warning("[%s] read seg %d: %s", slug, seg_counter, exc)
                        seg_counter += 1
                        if exited and not os.path.exists(os.path.join(tmpdir, f"seg_{seg_counter:05d}.ts")):
                            break
                        continue

                    idx        = next_index
                    next_index += 1
                    r2_key     = f"{config.R2_SEGMENT_PREFIX}/{slug}/{session_id}/seg_{idx}.ts"
                    now_mono   = time.monotonic()

                    if state.segments:
                        reg_at = min(state.segments[-1].registered_at + _SEG_DURATION, now_mono)
                    else:
                        reg_at = now_mono - _SEG_DURATION

                    is_restart = seg_counter == 0 and bool(state.segments)
                    downtime   = (time.monotonic() - last_ffmpeg_died_at) if last_ffmpeg_died_at > 0 else 99.0
                    mark_disc  = is_restart and downtime >= 15.0

                    seg = Segment(
                        index=idx, filename=f"seg_{idx}.ts", duration=actual_dur,
                        r2_key=r2_key, registered_at=reg_at, discontinuity=mark_disc,
                    )
                    state.segments.append(seg)
                    _trim(state)
                    if state.first_segment_at is None:
                        state.first_segment_at = now_mono

                    async def _upload(s=seg, d=data):
                        ok = await loop.run_in_executor(_EXECUTOR, _upload_ts_blocking, d, s.r2_key)
                        s.done   = True
                        s.failed = not ok
                        if ok:
                            state.last_upload_ok_at = time.monotonic()
                            logger.debug("[%s] ✓ seg %d uploaded", slug, s.index)
                        else:
                            logger.warning("[%s] ✗ seg %d R2 upload failed", slug, s.index)

                    t = asyncio.create_task(_upload())
                    in_flight.append(t)
                    in_flight[:] = [t for t in in_flight if not t.done()]
                    last_seg_time = time.monotonic()
                    seg_counter  += 1

                    if exited and not os.path.exists(os.path.join(tmpdir, f"seg_{seg_counter:05d}.ts")):
                        break
                elif exited:
                    break
                else:
                    now = time.monotonic()
                    if last_seg_time and now - last_seg_time > 15:
                        logger.warning("[%s] no seg for %.0fs — killing hung ffmpeg", slug, now - last_seg_time)
                        proc.kill()
                        try:
                            await asyncio.wait_for(proc.wait(), timeout=3)
                        except asyncio.TimeoutError:
                            pass
                    await asyncio.sleep(0.3)

            if in_flight:
                await asyncio.wait(in_flight, timeout=2.0)

        except asyncio.CancelledError:
            if proc and proc.returncode is None:
                proc.kill()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=3)
                except asyncio.TimeoutError:
                    pass
            recordings.pop(slug, None)
            shutil.rmtree(tmpdir, ignore_errors=True)
            logger.info("[%s] recorder stopped", slug)
            return

        except Exception as exc:
            logger.error("[%s] recorder error: %s", slug, exc)

        finally:
            if proc and proc.returncode is None:
                proc.kill()

        last_ffmpeg_died_at = time.monotonic()
        uptime = last_ffmpeg_died_at - ffmpeg_started_at

        if uptime < 30:
            consecutive_quick_exits += 1
        else:
            consecutive_quick_exits = 0

        if consecutive_quick_exits >= 5:
            logger.error("[%s] ffmpeg keeps dying quickly (%d times), giving up", slug, consecutive_quick_exits)
            recordings.pop(slug, None)
            active_tasks.pop(slug, None)
            shutil.rmtree(tmpdir, ignore_errors=True)
            return

        wait = min(2.0 * consecutive_quick_exits, 30.0)
        logger.info("[%s] ffmpeg exited (uptime=%.0fs), restarting in %.1fs", slug, uptime, wait)
        await asyncio.sleep(wait)


# ── public API ────────────────────────────────────────────────────────────────

def start_recording(slug: str, stream_url: str, title: str = "") -> None:
    if slug in active_tasks and not active_tasks[slug].done():
        return
    task = asyncio.create_task(record_channel(slug, stream_url, title))
    active_tasks[slug] = task


def stop_recording(slug: str) -> None:
    task = active_tasks.pop(slug, None)
    if task:
        task.cancel()
    recordings.pop(slug, None)


# ── R2 cleanup ────────────────────────────────────────────────────────────────

def _cleanup_old_r2_blocking() -> None:
    import datetime, boto3
    from botocore.config import Config as BotoConfig
    try:
        client = boto3.client(
            "s3",
            endpoint_url=config.R2_ENDPOINT,
            aws_access_key_id=config.R2_ACCESS_KEY_ID,
            aws_secret_access_key=config.R2_SECRET_ACCESS_KEY,
            config=BotoConfig(signature_version="s3v4", connect_timeout=10, read_timeout=30),
            region_name="auto",
        )
        prefix  = f"{config.R2_SEGMENT_PREFIX}/"
        cutoff  = datetime.datetime.now(datetime.timezone.utc) - \
                  datetime.timedelta(seconds=config.R2_CLEANUP_AGE_SECS)
        pager   = client.get_paginator("list_objects_v2")
        to_del: list[dict] = []
        scanned = 0
        for page in pager.paginate(Bucket=config.R2_BUCKET_NAME, Prefix=prefix):
            for obj in page.get("Contents", []):
                scanned += 1
                if obj["LastModified"] < cutoff:
                    to_del.append({"Key": obj["Key"]})
                if len(to_del) >= 1000:
                    client.delete_objects(Bucket=config.R2_BUCKET_NAME,
                                          Delete={"Objects": to_del, "Quiet": True})
                    to_del = []
        if to_del:
            client.delete_objects(Bucket=config.R2_BUCKET_NAME,
                                  Delete={"Objects": to_del, "Quiet": True})
        logger.info("R2 cleanup: scanned %d objects", scanned)
    except Exception as exc:
        logger.warning("R2 cleanup error: %s", exc)


async def r2_cleanup_loop() -> None:
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(config.R2_CLEANUP_INTERVAL)
        try:
            await loop.run_in_executor(_EXECUTOR, _cleanup_old_r2_blocking)
        except Exception as exc:
            logger.warning("R2 cleanup loop error: %s", exc)
