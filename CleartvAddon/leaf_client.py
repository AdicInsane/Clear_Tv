"""
leaf_client.py - HTTP client for the ClearTV leaf node.

The leaf node is the single source of truth for what channels exist and where
their playlists live. This module is the only place that talks to it.

Endpoints used:
  GET {LEAF_BASE_URL}/leaf/catalog.json  → list of channel entries
  GET {LEAF_BASE_URL}/leaf/health        → liveness + active slug count
"""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import urlsplit, urlunsplit

import httpx

from config import (
    LEAF_API_KEY,
    LEAF_BASE_URL,
    LEAF_CATALOG_PATH,
    LEAF_HEALTH_PATH,
    LEAF_PUBLIC_URL,
    LEAF_TIMEOUT,
    LEAF_VERIFY_TLS,
)

logger = logging.getLogger("cleartv.leaf")

_client: httpx.AsyncClient | None = None

# One retry on a transport error. The leaf node is normally on the same
# provider, so a failure is either a momentary blip (retry fixes it) or the node
# is down (a second attempt costs almost nothing and we fall back to the last
# good snapshot either way).
_RETRY_DELAY = 0.4


class LeafUnavailable(RuntimeError):
    """The leaf node could not be reached or returned an unusable response."""


def _headers() -> dict[str, str]:
    headers = {"Accept": "application/json", "User-Agent": "ClearTV-Addon"}
    if LEAF_API_KEY:
        headers["Authorization"] = f"Bearer {LEAF_API_KEY}"
    return headers


async def init_client() -> None:
    """Create the pooled client. Called once from the app lifespan."""
    global _client
    if _client is not None and not _client.is_closed:
        return
    _client = httpx.AsyncClient(
        timeout=LEAF_TIMEOUT,
        verify=LEAF_VERIFY_TLS,
        follow_redirects=True,
        headers=_headers(),
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
    )
    logger.info("leaf client ready → %s", LEAF_BASE_URL)


async def close_client() -> None:
    global _client
    if _client is not None and not _client.is_closed:
        await _client.aclose()
    _client = None


def _get_client() -> httpx.AsyncClient:
    # Lazily built so tests and one-off scripts can use this module without
    # running the full app lifespan.
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=LEAF_TIMEOUT,
            verify=LEAF_VERIFY_TLS,
            follow_redirects=True,
            headers=_headers(),
        )
    return _client


async def _get_json(path: str):
    url = f"{LEAF_BASE_URL}{path}"
    client = _get_client()

    last_exc: Exception | None = None
    for attempt in (1, 2):
        try:
            resp = await client.get(url)
        except Exception as exc:
            last_exc = exc
            if attempt == 1:
                await asyncio.sleep(_RETRY_DELAY)
                continue
            raise LeafUnavailable(f"GET {url} failed: {exc}") from exc

        if resp.status_code == 200:
            try:
                return resp.json()
            except Exception as exc:
                raise LeafUnavailable(f"GET {url} returned invalid JSON: {exc}") from exc

        # 5xx is worth one retry; 4xx is a configuration problem and will not
        # fix itself, so surface it immediately.
        if resp.status_code >= 500 and attempt == 1:
            last_exc = LeafUnavailable(f"GET {url} → HTTP {resp.status_code}")
            await asyncio.sleep(_RETRY_DELAY)
            continue

        raise LeafUnavailable(f"GET {url} → HTTP {resp.status_code}")

    raise LeafUnavailable(str(last_exc) if last_exc else f"GET {url} failed")


async def fetch_catalog() -> list[dict]:
    """
    Return the raw channel entries from the leaf node.

    The leaf returns a bare JSON array; a dict with an "entries" or "channels"
    key is also accepted so the leaf can grow a wrapper later without breaking
    this addon.
    """
    data = await _get_json(LEAF_CATALOG_PATH)

    if isinstance(data, dict):
        for key in ("entries", "channels", "catalog"):
            if isinstance(data.get(key), list):
                data = data[key]
                break
        else:
            raise LeafUnavailable(f"catalog response has no channel list (keys: {list(data)[:6]})")

    if not isinstance(data, list):
        raise LeafUnavailable(f"catalog response is {type(data).__name__}, expected a list")

    return [e for e in data if isinstance(e, dict)]


async def fetch_health() -> dict:
    """Return the leaf node's health payload. Raises LeafUnavailable on failure."""
    data = await _get_json(LEAF_HEALTH_PATH)
    return data if isinstance(data, dict) else {"raw": data}


def public_playlist_url(url: str) -> str:
    """
    Map a playlist URL from the leaf onto the public leaf domain.

    A leaf node behind a reverse proxy may advertise an internal address in
    playlist_url. Setting LEAF_PUBLIC_URL swaps the scheme/host/port while
    keeping the path and query, so clients get a reachable URL. With
    LEAF_PUBLIC_URL unset the leaf's own URL is returned untouched.
    """
    if not url:
        return ""
    if not LEAF_PUBLIC_URL:
        return url

    try:
        target = urlsplit(LEAF_PUBLIC_URL)
        parts = urlsplit(url)
        return urlunsplit((target.scheme, target.netloc, parts.path, parts.query, parts.fragment))
    except Exception:
        return url
