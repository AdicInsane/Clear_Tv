"""
relay.py - serves HLS to clients without ever exposing the provider.

An Xtream stream URL embeds the account username and password. Handing that to a
player would leak a shared, connection-capped credential to every viewer, so the
leaf does this instead:

  1. fetches the upstream playlist server-side, using the claimed account
  2. rewrites every segment / key / map URI into an opaque `/seg/<id>` link,
     keeping the provider URL server-side so its credentials never travel
  3. serves the rewritten playlist, and proxies the segment bytes itself

Two caches keep upstream load flat regardless of viewer count: playlists are
cached for a couple of seconds and segments for ~30s, both with single-flight so
a cache miss triggers exactly one upstream fetch no matter how many viewers hit
it at the same instant.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import re
import time
from urllib.parse import urljoin

import httpx

import config

logger = logging.getLogger("leaf.relay")

_client: httpx.AsyncClient | None = None

# slug -> (expires_monotonic, playlist_text)
_playlist_cache: dict[str, tuple[float, str]] = {}
_playlist_inflight: dict[str, "asyncio.Future[str]"] = {}

# upstream segment URL -> (expires_monotonic, body, content_type)
_segment_cache: dict[str, tuple[float, bytes, str]] = {}
_segment_inflight: dict[str, "asyncio.Future[tuple[bytes, str]]"] = {}
_segment_bytes = 0

class RelayError(RuntimeError):
    """Upstream could not be fetched or returned something unusable."""


def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            verify=config.UPSTREAM_VERIFY_TLS,
            timeout=config.UPSTREAM_TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": config.UPSTREAM_USER_AGENT, "Accept": "*/*"},
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=30),
        )
    return _client


async def close_client() -> None:
    global _client
    if _client is not None and not _client.is_closed:
        await _client.aclose()
    _client = None


# ── opaque segment references ─────────────────────────────────────────────────
#
# A provider segment URL is `…/live/<user>/<pass>/seg123.ts` — it carries the
# account credentials. It therefore must not appear in anything a client sees,
# in any form: encoding it (base64, hex, whatever) is not hiding it, since a
# client can trivially decode it back.
#
# So the URL never leaves this process. Each one is registered here under an
# opaque id and the playlist carries only that id. Two consequences:
#
#   * clients cannot recover the provider URL or its credentials, and
#   * /seg/ cannot be used as an open proxy, because there is no client-supplied
#     URL anywhere in the path — an unknown id is simply a miss.
#
# The id is derived with HMAC over the URL, so the same segment gets the same id
# across playlist refreshes (which keeps the segment cache warm) while remaining
# unguessable without the secret.

# id -> (expires_monotonic, upstream_url)
_seg_refs: dict[str, tuple[float, str]] = {}


def ref_url(url: str) -> str:
    """Register an upstream URL and return the opaque id clients will use."""
    ref = hmac.new(config.SEGMENT_SECRET.encode(), url.encode(), hashlib.sha256).hexdigest()[:32]
    _seg_refs[ref] = (time.monotonic() + config.SEGMENT_TTL, url)
    return ref


def resolve_ref(ref: str) -> str:
    """
    Return the upstream URL for an id, or raise ValueError.

    Only ids this leaf issued are present, so an unknown id means either a
    fabricated request or one whose playlist has aged out.
    """
    entry = _seg_refs.get(ref)
    if entry is None:
        raise ValueError("unknown or expired segment reference")

    expires, url = entry
    if time.monotonic() > expires:
        _seg_refs.pop(ref, None)
        raise ValueError("expired segment reference")

    return url


def _evict_refs(now: float) -> None:
    for ref in [r for r, (exp, _) in _seg_refs.items() if exp <= now]:
        _seg_refs.pop(ref, None)


# ── playlist rewriting ────────────────────────────────────────────────────────

_ATTR_URI = re.compile(r'(URI=")([^"]+)(")')


def _stream_info(line: str) -> dict:
    """Pull resolution and bandwidth out of an #EXT-X-STREAM-INF line."""
    info: dict = {}
    res = re.search(r"RESOLUTION=(\d+)x(\d+)", line)
    if res:
        info["width"] = int(res.group(1))
        info["height"] = int(res.group(2))
    bw = re.search(r"BANDWIDTH=(\d+)", line)
    if bw:
        info["video_bitrate_kbps"] = int(bw.group(1)) // 1000
    return info


def rewrite(playlist: str, base_url: str) -> tuple[str, dict]:
    """
    Rewrite every URI in a media playlist to a signed /seg/ link.

    Returns (rewritten_playlist, stream_info). Relative URIs are resolved against
    base_url first, since the client has no way to resolve them once the playlist
    is being served from a different host.
    """
    out: list[str] = []
    info: dict = {}

    for line in playlist.splitlines():
        stripped = line.strip()

        if not stripped:
            out.append(line)
            continue

        if stripped.startswith("#"):
            if stripped.startswith("#EXT-X-STREAM-INF"):
                info.update(_stream_info(stripped))
            # #EXT-X-KEY and #EXT-X-MAP carry a URI= attribute that must be
            # rewritten too, or the player fetches the provider directly for the
            # decryption key and leaks the credentials we are hiding.
            if "URI=" in stripped:
                def _sub(match: re.Match) -> str:
                    absolute = urljoin(base_url, match.group(2))
                    return f"{match.group(1)}{config.PUBLIC_URL}/seg/{ref_url(absolute)}{match.group(3)}"

                out.append(_ATTR_URI.sub(_sub, line))
            else:
                out.append(line)
            continue

        absolute = urljoin(base_url, stripped)
        out.append(f"{config.PUBLIC_URL}/seg/{ref_url(absolute)}")

    return "\n".join(out) + "\n", info


def _is_master(playlist: str) -> bool:
    return "#EXT-X-STREAM-INF" in playlist


def _pick_variant(playlist: str, base_url: str) -> tuple[str, dict]:
    """
    Choose the best variant from a master playlist.

    Highest bandwidth wins — no quality capping anywhere in ClearTV.
    """
    best_url = ""
    best_bw = -1
    best_info: dict = {}

    lines = playlist.splitlines()
    for i, line in enumerate(lines):
        if not line.strip().startswith("#EXT-X-STREAM-INF"):
            continue
        info = _stream_info(line.strip())
        bw = info.get("video_bitrate_kbps", 0)
        for candidate in lines[i + 1:]:
            candidate = candidate.strip()
            if candidate and not candidate.startswith("#"):
                if bw > best_bw:
                    best_bw = bw
                    best_url = urljoin(base_url, candidate)
                    best_info = info
                break

    return best_url, best_info


# ── playlist fetch ────────────────────────────────────────────────────────────

async def _fetch_playlist(upstream_url: str) -> tuple[str, dict]:
    """
    Fetch and rewrite one channel's playlist.

    A master playlist costs one extra round trip: fetch it, pick the highest
    bandwidth variant, then fetch that. Its stream_info is kept because the
    master advertises resolution the media playlist does not.
    """
    client = get_client()

    try:
        resp = await client.get(upstream_url)
    except Exception as exc:
        raise RelayError(f"upstream unreachable: {exc}") from exc

    if resp.status_code != 200:
        raise RelayError(f"upstream returned HTTP {resp.status_code}")

    text = resp.text
    if not text.lstrip().startswith("#EXTM3U"):
        raise RelayError("upstream did not return a playlist")

    base = str(resp.url)
    master_info: dict = {}

    if _is_master(text):
        variant_url, master_info = _pick_variant(text, base)
        if not variant_url:
            raise RelayError("master playlist had no usable variant")
        try:
            resp = await client.get(variant_url)
        except Exception as exc:
            raise RelayError(f"variant unreachable: {exc}") from exc
        if resp.status_code != 200:
            raise RelayError(f"variant returned HTTP {resp.status_code}")
        text = resp.text
        base = str(resp.url)

    rewritten, info = rewrite(text, base)
    merged = {**master_info, **info}
    return rewritten, merged


async def playlist_for(slug: str, upstream_url: str) -> tuple[str, dict]:
    """
    The rewritten playlist for a channel, cached and single-flighted.

    Single-flight matters here: the cache window is ~2s, so without it every
    viewer of a popular channel misses the cache on the same tick and they all
    hit the provider at once — which is exactly how an account gets its
    connection limit tripped.
    """
    now = time.monotonic()

    cached = _playlist_cache.get(slug)
    if cached and now < cached[0]:
        return cached[1], {}

    inflight = _playlist_inflight.get(slug)
    if inflight is not None:
        return await asyncio.shield(inflight), {}

    loop = asyncio.get_running_loop()
    future: "asyncio.Future[str]" = loop.create_future()
    _playlist_inflight[slug] = future

    try:
        rewritten, info = await _fetch_playlist(upstream_url)
        _playlist_cache[slug] = (time.monotonic() + config.PLAYLIST_CACHE_TTL, rewritten)
        if not future.done():
            future.set_result(rewritten)
        return rewritten, info
    except Exception as exc:
        if not future.done():
            future.set_exception(exc)
        raise
    finally:
        _playlist_inflight.pop(slug, None)
        # A future that failed and was never awaited by anyone else would log
        # "exception was never retrieved" noise on garbage collection.
        if future.done() and not future.cancelled() and future.exception() is not None:
            future.exception()


def drop_playlist(slug: str) -> None:
    """Forget a channel's cached playlist — called when it is stopped."""
    _playlist_cache.pop(slug, None)


# ── segment fetch ─────────────────────────────────────────────────────────────

def _evict_segments(now: float) -> None:
    """Drop expired entries, then oldest-first until under the size cap."""
    global _segment_bytes

    for key in [k for k, (exp, _, _) in _segment_cache.items() if exp <= now]:
        _, body, _ = _segment_cache.pop(key)
        _segment_bytes -= len(body)

    cap = config.SEGMENT_CACHE_MAX_MB * 1024 * 1024
    if _segment_bytes <= cap:
        return

    for key in sorted(_segment_cache, key=lambda k: _segment_cache[k][0]):
        if _segment_bytes <= cap:
            break
        _, body, _ = _segment_cache.pop(key)
        _segment_bytes -= len(body)


async def segment(url: str) -> tuple[bytes, str]:
    """
    Fetch one segment, cached and single-flighted.

    Several viewers a second or two apart request the same segment; caching it
    briefly means the provider serves each segment once rather than once per
    viewer.
    """
    global _segment_bytes
    now = time.monotonic()

    cached = _segment_cache.get(url)
    if cached and now < cached[0]:
        return cached[1], cached[2]

    inflight = _segment_inflight.get(url)
    if inflight is not None:
        return await asyncio.shield(inflight)

    loop = asyncio.get_running_loop()
    future: "asyncio.Future[tuple[bytes, str]]" = loop.create_future()
    _segment_inflight[url] = future

    try:
        client = get_client()
        resp = await client.get(url)
        if resp.status_code != 200:
            raise RelayError(f"segment returned HTTP {resp.status_code}")

        body = resp.content
        ctype = resp.headers.get("content-type", "video/mp2t")

        _segment_cache[url] = (time.monotonic() + config.SEGMENT_CACHE_TTL, body, ctype)
        _segment_bytes += len(body)
        _evict_segments(time.monotonic())

        result = (body, ctype)
        if not future.done():
            future.set_result(result)
        return result
    except Exception as exc:
        if not future.done():
            future.set_exception(exc)
        raise
    finally:
        _segment_inflight.pop(url, None)
        if future.done() and not future.cancelled() and future.exception() is not None:
            future.exception()


def cache_stats() -> dict:
    return {
        "playlists_cached": len(_playlist_cache),
        "segments_cached": len(_segment_cache),
        "segment_cache_mb": round(_segment_bytes / 1024 / 1024, 1),
        "segment_cache_cap_mb": config.SEGMENT_CACHE_MAX_MB,
        "segment_refs": len(_seg_refs),
    }


async def sweep_loop() -> None:
    """
    Periodically evict expired segments and stale references, so memory does not
    creep up on a leaf that has been running for weeks with nobody watching.
    """
    while True:
        await asyncio.sleep(30)
        try:
            now = time.monotonic()
            _evict_segments(now)
            _evict_refs(now)
        except Exception as exc:
            logger.warning("segment sweep error: %s", exc)
