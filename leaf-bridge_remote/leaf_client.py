"""
leaf_client.py - SKTech API client for the Leaf Bridge service.

Flow:
  1. POST to Firebase Remote Config to get the live base API URL.
     Falls back to SKTECH_DEFAULT_API_URL if Firebase fails.
  2. GET {api_url}/events.txt  → decrypt → parse event list
  3. GET {web_url}/{slug}.txt  → decrypt → parse stream list

All responses are AES-encrypted blobs decrypted by leaf_crypto.py.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any
from uuid import uuid4

import httpx

from config import (
    SKTECH_DEFAULT_API_URL,
    SKTECH_DEFAULT_WEB_URL,
    SKTECH_FIREBASE_API_KEY,
    SKTECH_FIREBASE_APP_ID,
    SKTECH_FIREBASE_PROJECT_NUMBER,
    SKTECH_PACKAGE_NAME,
)
from leaf_crypto import decrypt_sklive

logger = logging.getLogger("leaf.client")

# ── Cached base URLs (refreshed every 30 min) ─────────────────────────────────
_cached_api_url: str = ""
_cached_web_url: str = ""
_cache_fetched_at: float = 0.0
_URL_CACHE_TTL = 1800  # 30 minutes

_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=5.0)
_USER_AGENT   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


# ══════════════════════════════════════════════════════════════════════════════
# Firebase Remote Config
# ══════════════════════════════════════════════════════════════════════════════

async def _fetch_firebase_config() -> dict[str, str] | None:
    """
    POST to Firebase Remote Config impersonating the SKTech Android app.
    Returns the entries dict or None on failure.
    """
    if not SKTECH_FIREBASE_API_KEY or not SKTECH_FIREBASE_APP_ID:
        return None

    url = (
        f"https://firebaseremoteconfig.googleapis.com/v1/projects/"
        f"{SKTECH_FIREBASE_PROJECT_NUMBER}/namespaces/firebase:fetch"
    )
    payload = {
        "appInstanceId":      uuid4().hex,
        "appInstanceIdToken": "",
        "appId":              SKTECH_FIREBASE_APP_ID,
        "countryCode":        "US",
        "languageCode":       "en-US",
        "platformVersion":    "30",
        "timeZone":           "UTC",
        "appVersion":         "5.0",
        "appBuild":           "50",
        "packageName":        SKTECH_PACKAGE_NAME,
        "sdkVersion":         "22.1.0",
        "analyticsUserProperties": {},
    }
    headers = {
        "Content-Type":       "application/json",
        "Accept":             "application/json",
        "X-Android-Package":  SKTECH_PACKAGE_NAME,
        "X-Goog-Api-Key":     SKTECH_FIREBASE_API_KEY,
        "X-Google-GFE-Can-Retry": "yes",
    }

    try:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
            resp = await client.post(url, json=payload, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            entries = data.get("entries") or {}
            logger.info("Firebase Remote Config fetched: %s", list(entries.keys()))
            return entries
        else:
            logger.warning("Firebase Remote Config HTTP %d", resp.status_code)
    except Exception as exc:
        logger.warning("Firebase Remote Config fetch failed: %s", exc)

    return None


async def _refresh_base_urls() -> None:
    """Refresh cached API + web URLs from Firebase (or use defaults)."""
    global _cached_api_url, _cached_web_url, _cache_fetched_at

    entries = await _fetch_firebase_config()
    if entries:
        api = entries.get("api_url", "").rstrip("/")
        web = entries.get("web_url", "").rstrip("/")
        if api:
            _cached_api_url = api
        if web:
            _cached_web_url = web

    # Fall back to defaults for anything still empty
    if not _cached_api_url:
        _cached_api_url = SKTECH_DEFAULT_API_URL.rstrip("/")
    if not _cached_web_url:
        _cached_web_url = SKTECH_DEFAULT_WEB_URL.rstrip("/")

    # sktech.live doesn't resolve from datacenter IPs — fall back to api_url
    # which is the same backend and serves both events.txt and slug.txt files
    if _cached_web_url and "sktech.live" in _cached_web_url:
        _cached_web_url = _cached_api_url

    _cache_fetched_at = time.monotonic()
    logger.info("Leaf API URL: %s  |  Web URL: %s", _cached_api_url, _cached_web_url)


async def _get_api_url() -> str:
    if not _cached_api_url or (time.monotonic() - _cache_fetched_at) > _URL_CACHE_TTL:
        await _refresh_base_urls()
    return _cached_api_url


async def _get_web_url() -> str:
    if not _cached_web_url or (time.monotonic() - _cache_fetched_at) > _URL_CACHE_TTL:
        await _refresh_base_urls()
    return _cached_web_url


# ══════════════════════════════════════════════════════════════════════════════
# Encrypted blob fetch + decrypt helper
# ══════════════════════════════════════════════════════════════════════════════

async def _fetch_and_decrypt(url: str) -> Any | None:
    """
    Fetch an encrypted blob from url, decrypt it, and JSON-parse the result.
    Returns parsed Python object or None on any failure.
    """
    try:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
            resp = await client.get(url, headers={"User-Agent": _USER_AGENT})
        if resp.status_code != 200:
            logger.warning("fetch_and_decrypt: HTTP %d for %s", resp.status_code, url)
            return None

        encrypted = resp.text.strip()
        if not encrypted:
            logger.warning("fetch_and_decrypt: empty response from %s", url)
            return None

        decrypted = decrypt_sklive(encrypted)
        if not decrypted:
            logger.warning("fetch_and_decrypt: decryption failed for %s", url)
            return None

        return json.loads(decrypted)

    except json.JSONDecodeError as exc:
        logger.warning("fetch_and_decrypt: JSON parse error for %s: %s", url, exc)
        return None
    except Exception as exc:
        logger.warning("fetch_and_decrypt: error for %s: %s", url, exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

async def get_live_events() -> list[dict]:
    """
    Fetch and decrypt the events list from {api_url}/events.txt.

    Returns a flat list of event dicts with normalised keys:
      event_name, team_a, team_b, team_a_flag, team_b_flag,
      category, slug, start_time, end_time, visible, link_names
    """
    api_url = await _get_api_url()
    url     = f"{api_url}/events.txt"
    logger.info("Fetching Leaf events from %s", url)

    raw = await _fetch_and_decrypt(url)
    if not isinstance(raw, list):
        logger.warning("get_live_events: unexpected response type: %s", type(raw))
        return []

    events: list[dict] = []
    for i, wrapper in enumerate(raw):
        try:
            # Each item is {"event": "<json string>"}
            if isinstance(wrapper, dict) and "event" in wrapper:
                inner = json.loads(wrapper["event"])
            elif isinstance(wrapper, dict):
                inner = wrapper
            else:
                continue

            if inner.get("visible") is False:
                continue

            slug = ""
            links_raw = inner.get("links", "")
            if links_raw:
                # links field is the slug filename e.g. "chelsea-vs-arsenal.txt"
                slug = links_raw.replace(".txt", "").strip("/")

            events.append({
                "event_name":  inner.get("eventName", ""),
                "team_a":      inner.get("teamAName", ""),
                "team_b":      inner.get("teamBName", ""),
                "team_a_flag": inner.get("teamAFlag", ""),
                "team_b_flag": inner.get("teamBFlag", ""),
                "category":    (inner.get("category") or "").lower(),
                "slug":        slug,
                "start_time":  inner.get("date", "") + " " + inner.get("time", ""),
                "end_time":    inner.get("end_date", "") + " " + inner.get("end_time", ""),
                "link_names":  inner.get("link_names") or [],
                "visible":     inner.get("visible", True),
            })
        except Exception as exc:
            logger.debug("get_live_events: error parsing event %d: %s", i, exc)

    logger.info("get_live_events: %d visible events", len(events))
    return events


async def get_stream_list(slug: str) -> list[dict]:
    """
    Fetch and decrypt the stream list for a specific event slug.
    GET {web_url}/{slug}.txt

    Returns a list of stream dicts:
      name, url, headers (dict), drm_kid, drm_key, token_api (dict|None)

    The `url` field is already cleaned (pipe-separated headers stripped out).
    """
    web_url = await _get_web_url()
    url     = f"{web_url}/{slug}.txt"
    logger.info("Fetching Leaf stream list from %s", url)

    raw = await _fetch_and_decrypt(url)
    if not isinstance(raw, list):
        logger.warning("get_stream_list: unexpected response type for %s: %s", slug, type(raw))
        return []

    streams: list[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue

        link_raw = item.get("link") or ""
        token_api_raw = item.get("tokenApi")

        # Parse pipe-separated headers from link field
        # Format: https://example.com/stream.m3u8|User-Agent=Foo&Referer=Bar
        stream_url = ""
        headers: dict[str, str] = {}

        if link_raw:
            if "|" in link_raw:
                parts = link_raw.split("|", 1)
                stream_url = parts[0].strip()
                for pair in parts[1].split("&"):
                    kv = pair.split("=", 1)
                    if len(kv) == 2:
                        headers[kv[0].strip()] = kv[1].strip()
            else:
                stream_url = link_raw.strip()

        # Parse DRM info from api field (format: "kid_hex:key_hex")
        drm_kid = ""
        drm_key = ""
        api_raw = item.get("api") or ""
        if api_raw and ":" in api_raw:
            parts = api_raw.split(":", 1)
            drm_kid = parts[0].strip()
            drm_key = parts[1].strip()

        # Parse tokenApi JSON if present
        token_api: dict | None = None
        if token_api_raw:
            try:
                if isinstance(token_api_raw, str):
                    token_api = json.loads(token_api_raw)
                elif isinstance(token_api_raw, dict):
                    token_api = token_api_raw
            except Exception:
                pass

        streams.append({
            "name":      item.get("name") or "Server",
            "url":       stream_url,
            "headers":   headers,
            "drm_kid":   drm_kid,
            "drm_key":   drm_key,
            "scheme":    item.get("scheme", 0),
            "token_api": token_api,
        })

    logger.info("get_stream_list: %d streams for slug '%s'", len(streams), slug)
    return streams


def pick_best_stream(streams: list[dict]) -> dict | None:
    """
    Pick the best stream from a list for recording purposes.

    Priority:
      1. Direct HLS (.m3u8) with no tokenApi and no DRM — simplest to record
      2. Direct HLS with headers but no tokenApi
      3. Direct HLS with tokenApi (needs extra resolution step)
      4. Anything else

    Returns the best stream dict or None if list is empty.
    """
    if not streams:
        return None

    # Domains confirmed to block datacenter IPs — deprioritize these
    _BLOCKED_DOMAINS = (
        "feijing-xzbforanchor",
        "tiktokcdn.com",
        "tiktokcdn-us.com",
        "ibyteimg.com",
        "byteoversea.com",
    )

    def score(s: dict) -> int:
        url           = s.get("url", "")
        has_m3u8      = ".m3u8" in url or "m3u8" in url
        has_token_api = s.get("token_api") is not None
        has_drm       = bool(s.get("drm_kid"))
        is_blocked    = any(d in url for d in _BLOCKED_DOMAINS)
        # Blocked domains get a +10 penalty so they're only used as last resort
        penalty = 10 if is_blocked else 0
        if has_drm:
            return 20 + penalty
        if has_m3u8 and not has_token_api:
            return 0 + penalty
        if has_m3u8 and has_token_api:
            return 2 + penalty
        return 3 + penalty

    return min(streams, key=score)
