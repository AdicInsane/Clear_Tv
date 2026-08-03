"""
leaf_recorder.py - Virtual-clock HLS recorder for Leaf Bridge.

Heavily based on recorderservice/recorder.py but:
  - Segments stored under leaf/{slug}/ prefix in R2
  - Uses the rumtv08 R2 worker (cdn.kitomi33.eu.cc) — separate from main recorder
  - Stream URL comes from SKTech API (leaf_client.py), not from our addon API
  - No ADMIN_TOKEN dependency — fully self-contained
  - Served via leaf_server.py on port 8087 at /leaf/{slug}/live.m3u8
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone as _tz
from typing import Optional

import httpx

from config import (
    DELAY_SECONDS,
    LIVE_WINDOW_SEGMENTS,
    MAX_CONCURRENT_LEAF,
    MAX_FAILURES,
    R2_BUCKET_NAME,
    R2_PUBLIC_DOMAIN,
)

# ── Domains that block datacenter IPs (same list as main recorder) ────────────
_DATACENTER_BLOCKED_DOMAINS = (
    "feijing-xzbforanchor",
    "tiktokcdn.com",
    "tiktokcdn-us.com",
    "ibyteimg.com",
    "byteoversea.com",
)


def _is_blocked(url: str) -> bool:
    return any(d in url for d in _DATACENTER_BLOCKED_DOMAINS)


def _wrap_via_worker(url: str, headers: dict) -> str:
    """
    Wrap a geo-blocked URL through the CF Worker segment proxy.
    Uses the same HMAC token format as the sportsaddon backend.
    Returns original URL if worker not configured.
    """
    import base64, hashlib, hmac, json as _json, os
    worker_url = os.getenv("SEG_WORKER_URL", "").rstrip("/")
    worker_secret = os.getenv("SEG_WORKER_SECRET", "")
    if not worker_url or not worker_secret:
        # Fall back to the first CF_WORKER_POOL entry's URL
        raw_pool = os.getenv("CF_WORKER_POOL", "")
        if raw_pool:
            first = raw_pool.split(",")[0].strip()
            parts = [p.strip() for p in first.split("|")]
            if len(parts) == 6 and parts[0].startswith("http"):
                worker_url = parts[0].rstrip("/")
                worker_secret = parts[1]
    if not worker_url or not worker_secret:
        return url
    payload = _json.dumps({"url": url, "headers": headers}, separators=(",", ":"))
    payload_b64 = base64.urlsafe_b64encode(payload.encode()).rstrip(b"=").decode()
    sig = hmac.new(worker_secret.encode(), msg=payload_b64.encode(), digestmod=hashlib.sha256).hexdigest()[:32]
    return f"{worker_url}/seg/{payload_b64}.{sig}"

logger = logging.getLogger("leaf.recorder")

# ── Thread pool for blocking segment download+upload ──────────────────────────
_EXECUTOR = ThreadPoolExecutor(max_workers=64, thread_name_prefix="leaf-seg")

# ── Global state ──────────────────────────────────────────────────────────────
# slug → asyncio.Task
active_leaf_recordings: dict[str, asyncio.Task] = {}

# slug → LeafRecordingState  (read by leaf_server.py)
leaf_recordings: dict[str, "LeafRecordingState"] = {}

# ── R2 worker config for Leaf (uses rumtv08 — cdn.kitomi33.eu.cc) ─────────────
# Populated at startup by leaf_main.py from the CF_WORKER_POOL env var.
# Falls back to the main R2 credentials if not set.
_leaf_r2_worker: dict = {}


def set_leaf_r2_worker(worker: dict) -> None:
    """Called at startup to configure which R2 bucket Leaf uses."""
    global _leaf_r2_worker
    _leaf_r2_worker = worker
    logger.info("Leaf R2 worker set: domain=%s", worker.get("r2_domain", "?"))


def _get_leaf_worker() -> dict:
    return _leaf_r2_worker


# ══════════════════════════════════════════════════════════════════════════════
# Data model (mirrors recorder.py)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class LeafSegment:
    index:         int
    filename:      str
    duration:      float
    r2_key:        str
    registered_at: float = field(default_factory=time.monotonic)
    r2_domain:     str   = ""
    done:          bool  = False
    failed:        bool  = False


@dataclass
class LeafRecordingState:
    slug:            str
    streamed_id:     str   = ""   # matched streamed.pk match ID
    title:           str   = ""
    category:        str   = ""
    started_at:      str   = field(default_factory=lambda: datetime.now(_tz.utc).isoformat())
    segments:        list[LeafSegment] = field(default_factory=list)
    first_segment_at: Optional[float] = None


# ══════════════════════════════════════════════════════════════════════════════
# Playlist generation (identical logic to recorder.py)
# ══════════════════════════════════════════════════════════════════════════════

def build_live_playlist(state: LeafRecordingState) -> Optional[str]:
    """
    Build the rolling live.m3u8 served to Stremio.
    Returns None (→ 404) during the initial DELAY_SECONDS boot buffer.
    """
    if not state.segments:
        return None

    now = time.monotonic()

    # Force-fail any segment past the delay window still in-flight
    for seg in state.segments:
        if not seg.done and (now - seg.registered_at) >= DELAY_SECONDS:
            seg.done   = True
            seg.failed = True
            logger.warning("[%s] %s force-failed after %.0fs", state.slug, seg.filename, now - seg.registered_at)

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

    import math
    target_dur  = max(math.ceil(s.duration) for s in good_window)
    good_before = len([s for s in good_safe if s.index < first_good.index])
    disc_before = sum(1 for s in safe if s.index < first_good.index and s.failed)

    _mono_now  = time.monotonic()
    _wall_now  = datetime.now(_tz.utc)
    _seg_wall  = _wall_now - __import__("datetime").timedelta(seconds=_mono_now - first_good.registered_at)
    _pdt       = _seg_wall.strftime("%Y-%m-%dT%H:%M:%S.") + f"{_seg_wall.microsecond // 1000:03d}Z"

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
        if prev_index is not None and seg.index != prev_index + 1:
            lines.append("#EXT-X-DISCONTINUITY")
        elif prev_index is None and seg.index != first_good.index:
            lines.append("#EXT-X-DISCONTINUITY")
        lines.append(f"#EXTINF:{seg.duration:.3f},")
        domain = seg.r2_domain or R2_PUBLIC_DOMAIN
        lines.append(f"{domain}/leaf/{state.slug}/{seg.filename}")
        prev_index = seg.index

    return "\n".join(lines) + "\n"


# ══════════════════════════════════════════════════════════════════════════════
# R2 upload helpers
# ══════════════════════════════════════════════════════════════════════════════

def _make_s3_client(worker: dict):
    import boto3
    from botocore.config import Config as BotoConfig
    return boto3.client(
        "s3",
        endpoint_url=worker["r2_endpoint"],
        aws_access_key_id=worker["r2_access_key_id"],
        aws_secret_access_key=worker["r2_secret_access_key"],
        config=BotoConfig(
            signature_version="s3v4",
            retries={"max_attempts": 2, "mode": "standard"},
        ),
        region_name="auto",
    )


def _fetch_with_chrome_tls(url: str, headers: dict, timeout: int = 20) -> bytes | None:
    """
    Fetch a URL using curl_cffi Chrome TLS impersonation.
    Bypasses CDN fingerprinting that blocks plain httpx/requests.
    Used for both playlist and segment fetches from blocked domains.
    """
    from curl_cffi.requests import Session as CurlSession
    try:
        with CurlSession(impersonate="chrome120") as s:
            resp = s.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            return resp.content
        logger.warning("curl_cffi HTTP %d: %s", resp.status_code, url[:80])
        return None
    except Exception as exc:
        logger.warning("curl_cffi fetch error (%s): %s", url[:60], exc)
        return None


def _upload_segment_blocking(source_url: str, r2_key: str, req_headers: dict, worker: dict) -> bool:
    """Download one .ts segment and upload to R2. Runs in thread pool."""
    fetch_hdrs = {
        "user-agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
        "accept":          "*/*",
        "accept-language": "en-US,en;q=0.9",
    }
    fetch_hdrs.update({k.lower(): v for k, v in req_headers.items()})

    # Blocked domains: use Chrome TLS impersonation via curl_cffi
    data = None
    if _is_blocked(source_url):
        logger.debug("Fetching blocked segment via Chrome TLS: %s", source_url[:80])
        data = _fetch_with_chrome_tls(source_url, fetch_hdrs)
    else:
        import httpx as _httpx
        _TIMEOUT = _httpx.Timeout(connect=8.0, read=25.0, write=8.0, pool=5.0)
        try:
            with _httpx.Client(verify=False, timeout=_TIMEOUT, follow_redirects=True) as client:
                resp = client.get(source_url, headers=fetch_hdrs)
            if resp.status_code == 200:
                data = resp.content
            else:
                logger.warning("Segment HTTP %d: %s", resp.status_code, source_url[:80])
        except Exception as exc:
            logger.warning("Segment download error (%s): %s", source_url[:60], exc)

    if data is None:
        return False

    if not (worker.get("r2_access_key_id") and worker.get("r2_secret_access_key") and worker.get("r2_endpoint")):
        logger.error("R2 credentials missing — cannot upload %s", r2_key)
        return False

    try:
        s3 = _make_s3_client(worker)
        s3.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=r2_key,
            Body=data,
            ContentType="video/MP2T",
            CacheControl="public, max-age=31536000, immutable",
            ContentDisposition="inline",
        )
        return True
    except Exception as exc:
        logger.warning("R2 upload error (%s): %s", r2_key, exc)
        return False


async def _upload_segment_async(source_url: str, r2_key: str, req_headers: dict) -> Optional[str]:
    """Async wrapper. Returns r2_domain on success, None on failure."""
    worker = _get_leaf_worker()
    loop   = asyncio.get_running_loop()
    try:
        ok = await loop.run_in_executor(
            _EXECUTOR, _upload_segment_blocking, source_url, r2_key, req_headers, worker
        )
        return worker.get("r2_domain", R2_PUBLIC_DOMAIN) if ok else None
    except Exception as exc:
        logger.warning("upload_segment_async error (%s): %s", r2_key, exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# HLS playlist parser
# ══════════════════════════════════════════════════════════════════════════════

def parse_media_playlist(text: str, base_url: str) -> tuple[int, list[tuple[float, str]]]:
    target_duration = 6
    segments: list[tuple[float, str]] = []
    cur_dur = 0.0

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#EXT-X-TARGETDURATION:"):
            try:
                target_duration = int(line.split(":", 1)[1])
            except (ValueError, IndexError):
                pass
        elif line.startswith("#EXTINF:"):
            try:
                cur_dur = float(line.split(":", 1)[1].split(",")[0])
            except (ValueError, IndexError):
                cur_dur = 0.0
        elif not line.startswith("#"):
            abs_url = line if line.startswith("http") else urllib.parse.urljoin(base_url, line)
            if len(abs_url) < 2000 and " " not in abs_url:
                segments.append((cur_dur, abs_url))
            cur_dur = 0.0

    return target_duration, segments


# ══════════════════════════════════════════════════════════════════════════════
# Per-stream recording task
# ══════════════════════════════════════════════════════════════════════════════

async def record_leaf_stream(
    slug: str,
    stream_url: str,
    stream_headers: dict,
    streamed_id: str = "",
    title: str = "",
    category: str = "",
) -> None:
    """
    Record one Leaf HLS stream until it ends or is cancelled.
    Segments uploaded to R2 under leaf/{slug}/ prefix.
    State stored in leaf_recordings[slug] for the HTTP server.
    """
    logger.info("[leaf/%s] Recording started — %s", slug, stream_url[:80])

    state = LeafRecordingState(
        slug=slug,
        streamed_id=streamed_id,
        title=title,
        category=category,
    )
    leaf_recordings[slug] = state

    seen_urls:       set[str]      = set()
    recent_filenames: list[str]    = []
    next_index:      int           = 1
    target_duration: int           = 6
    failures:        int           = 0
    in_flight:       list[asyncio.Task] = []

    async def _handle_segment(seg_url: str, seg: LeafSegment, hdrs: dict) -> None:
        r2_domain = await _upload_segment_async(seg_url, seg.r2_key, hdrs)
        if r2_domain:
            seg.r2_domain = r2_domain
            seg.done      = True
            logger.debug("[leaf/%s] ✓ %s", slug, seg.filename)
        else:
            seg.done   = True
            seg.failed = True
            logger.warning("[leaf/%s] ✗ %s (skipped)", slug, seg.filename)

    current_url     = stream_url
    current_headers = dict(stream_headers)
    use_chrome_tls  = _is_blocked(current_url)

    if use_chrome_tls:
        logger.info("[leaf/%s] Blocked domain detected — using Chrome TLS impersonation for playlist", slug)

    async def _fetch_playlist(url: str, hdrs: dict) -> str | None:
        """Fetch playlist text, using Chrome TLS for blocked domains."""
        loop = asyncio.get_running_loop()
        if use_chrome_tls or _is_blocked(url):
            raw = await loop.run_in_executor(
                _EXECUTOR, _fetch_with_chrome_tls, url, hdrs, 12
            )
            return raw.decode("utf-8", errors="replace") if raw else None
        else:
            try:
                async with httpx.AsyncClient(verify=False, timeout=12) as _c:
                    r = await _c.get(url, headers=hdrs, follow_redirects=True)
                return r.text if r.status_code == 200 else None
            except Exception as exc:
                logger.warning("[leaf/%s] Playlist fetch error: %s", slug, exc)
                return None

    async with httpx.AsyncClient(verify=False, timeout=15) as http:
        while failures < MAX_FAILURES:
            try:
                # 1. Fetch playlist
                text_raw = await _fetch_playlist(current_url, current_headers)
                if text_raw is None:
                    failures += 1
                    logger.warning("[leaf/%s] Playlist fetch failed (%d/%d)", slug, failures, MAX_FAILURES)
                    await asyncio.sleep(5)
                    continue

                text = text_raw.strip()

                # 3. Validate
                if not text.startswith("#EXTM3U"):
                    failures += 1
                    logger.warning("[leaf/%s] Not an HLS playlist", slug)
                    await asyncio.sleep(5)
                    continue

                # 4. Follow master playlist (pick highest bandwidth variant)
                if "#EXT-X-STREAM-INF" in text:
                    _, variants = parse_media_playlist(text, current_url)
                    if variants:
                        current_url = variants[0][1]  # parse_media_playlist returns (dur, url) tuples
                        logger.info("[leaf/%s] Following variant → %s", slug, current_url[:80])
                    await asyncio.sleep(1)
                    continue

                # 5. Parse new segments
                failures = 0
                target_duration, all_segs = parse_media_playlist(text, current_url)
                
                new_segs = []
                for dur, url in all_segs:
                    fname = urllib.parse.urlparse(url).path.split('/')[-1]
                    if url not in seen_urls and fname not in recent_filenames:
                        new_segs.append((dur, url))

                # 6. Register + dispatch all new segments
                cumulative_offset = sum(dur for dur, _ in new_segs)
                elapsed = 0.0
                for duration, seg_url in new_segs:
                    seen_urls.add(seg_url)
                    fname = urllib.parse.urlparse(seg_url).path.split('/')[-1]
                    recent_filenames.append(fname)
                    if len(recent_filenames) > 100:
                        recent_filenames.pop(0)
                        
                    idx      = next_index
                    next_index += 1
                    filename = f"seg_{idx}.ts"
                    r2_key   = f"leaf/{slug}/{filename}"
                    backdate = cumulative_offset - elapsed
                    seg      = LeafSegment(
                        index=idx, filename=filename,
                        duration=duration, r2_key=r2_key,
                        registered_at=time.monotonic() - backdate,
                    )
                    elapsed += duration
                    state.segments.append(seg)
                    if state.first_segment_at is None:
                        state.first_segment_at = time.monotonic()

                    t = asyncio.create_task(_handle_segment(seg_url, seg, dict(current_headers)))
                    in_flight.append(t)

                # Prune completed tasks
                in_flight[:] = [t for t in in_flight if not t.done()]

                if new_segs:
                    logger.info("[leaf/%s] Dispatched %d new segment(s)", slug, len(new_segs))

                # 7. Poll interval
                await asyncio.sleep(max(1.0, target_duration / 2))

            except asyncio.CancelledError:
                logger.info("[leaf/%s] Recording cancelled", slug)
                break
            except Exception as exc:
                logger.exception("[leaf/%s] Unexpected error: %s", slug, exc)
                failures += 1
                await asyncio.sleep(5)

    # Wait for in-flight uploads
    if in_flight:
        logger.info("[leaf/%s] Waiting for %d in-flight uploads …", slug, len(in_flight))
        await asyncio.gather(*in_flight, return_exceptions=True)

    # Cleanup
    leaf_recordings.pop(slug, None)
    active_leaf_recordings.pop(slug, None)
    logger.info("[leaf/%s] Recording finished", slug)


# ══════════════════════════════════════════════════════════════════════════════
# Public control functions
# ══════════════════════════════════════════════════════════════════════════════

def start_leaf_recording(
    slug: str,
    stream_url: str,
    stream_headers: dict,
    streamed_id: str = "",
    title: str = "",
    category: str = "",
) -> bool:
    """
    Spawn a recording task for the given slug.
    Returns True if started, False if already running or limit reached.
    """
    if slug in active_leaf_recordings:
        return False

    if len(active_leaf_recordings) >= MAX_CONCURRENT_LEAF:
        logger.warning("Leaf recording limit (%d) reached — skipping %s", MAX_CONCURRENT_LEAF, slug)
        return False

    task = asyncio.create_task(
        record_leaf_stream(slug, stream_url, stream_headers, streamed_id, title, category)
    )
    active_leaf_recordings[slug] = task
    logger.info("Started Leaf recording: %s", slug)
    return True


def stop_leaf_recording(slug: str) -> None:
    """Cancel a running Leaf recording task."""
    task = active_leaf_recordings.get(slug)
    if task:
        task.cancel()
        active_leaf_recordings.pop(slug, None)
        logger.info("Stopped Leaf recording: %s", slug)
