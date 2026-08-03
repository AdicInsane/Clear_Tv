"""
config.py - ClearTV addon settings.

Everything comes from the environment (see .env.example). No secrets are
hardcoded here — the addon refuses to start if a required value is missing.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env(name, "true" if default else "false").lower()
    return raw in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError:
        return default


def _env_list(name: str, default: str = "") -> list[str]:
    return [p.strip() for p in _env(name, default).split(",") if p.strip()]


# ── Server ────────────────────────────────────────────────────────────────────
HOST = _env("HOST", "0.0.0.0")
PORT = _env_int("PORT", 7100)

# Public URL of this addon — used to build install links on the configure page.
BASE_URL = _env("BASE_URL", f"http://localhost:{PORT}").rstrip("/")

# ── Addon identity ────────────────────────────────────────────────────────────
ADDON_ID = _env("ADDON_ID", "top.nowclear.cleartv")
ADDON_NAME = _env("ADDON_NAME", "ClearTV")
ADDON_VERSION = _env("ADDON_VERSION", "1.0.0")
ADDON_DESCRIPTION = _env(
    "ADDON_DESCRIPTION",
    "Live TV channels, streamed clean and direct.",
)
ADDON_LOGO = _env("ADDON_LOGO", f"{BASE_URL}/logo.png")
ADDON_BACKGROUND = _env("ADDON_BACKGROUND", "")

# Stremio content type. Live TV uses "tv".
STREMIO_TYPE = "tv"

# Prefix for every Stremio id this addon owns → "cleartv:<slug>".
ID_PREFIX = _env("ID_PREFIX", "cleartv")

# ── Leaf node ─────────────────────────────────────────────────────────────────
# The leaf node that owns the channels and serves the HLS playlists.
# Required — there is no default, so a misconfigured deploy fails loudly at
# startup instead of silently pointing at somebody else's leaf.
LEAF_BASE_URL = _env("LEAF_BASE_URL").rstrip("/")

# Path of the channel catalog on the leaf node.
LEAF_CATALOG_PATH = _env("LEAF_CATALOG_PATH", "/leaf/catalog.json")
LEAF_HEALTH_PATH = _env("LEAF_HEALTH_PATH", "/leaf/health")

# Optional shared secret. When set it is sent as a bearer token on every leaf
# request so the leaf node can restrict its catalog to this addon.
LEAF_API_KEY = _env("LEAF_API_KEY")

# Network behaviour when talking to the leaf node.
LEAF_TIMEOUT = float(_env("LEAF_TIMEOUT", "10"))
LEAF_VERIFY_TLS = _env_bool("LEAF_VERIFY_TLS", True)

# Rewrites the playlist host the leaf reports. Leave empty to use the URL the
# leaf node returns verbatim (the normal case). Set it when the leaf advertises
# an internal address but clients must reach it on a public domain.
LEAF_PUBLIC_URL = _env("LEAF_PUBLIC_URL").rstrip("/")

# ── Refresh / cache ───────────────────────────────────────────────────────────
# How often the background loop pulls a fresh channel list from the leaf node.
REFRESH_INTERVAL = _env_int("REFRESH_INTERVAL", 45)

# How long a fetched snapshot is served before it is considered stale. Must be
# comfortably larger than REFRESH_INTERVAL so a single slow or failed refresh
# never leaves the catalog empty.
CACHE_TTL = _env_int("CACHE_TTL", 180)

# Retry delay used after a failed refresh, so a flaky leaf recovers quickly
# instead of waiting out the full interval.
REFRESH_RETRY_INTERVAL = _env_int("REFRESH_RETRY_INTERVAL", 15)

# ── Catalog behaviour ─────────────────────────────────────────────────────────
CATALOG_ID = "cleartv_channels"
CATALOG_NAME = _env("CATALOG_NAME", "Live TV")

# Channels per catalog page.
PAGE_SIZE = _env_int("PAGE_SIZE", 100)

# Category applied when the leaf node reports none.
DEFAULT_CATEGORY = _env("DEFAULT_CATEGORY", "General")

# Channels still filling their buffer are playable within a few seconds. Shown
# by default; users can hide them from the configure page.
SHOW_BUFFERING_DEFAULT = _env_bool("SHOW_BUFFERING_DEFAULT", True)

# Slugs never shown, regardless of what the leaf reports.
BLOCKED_SLUGS: frozenset[str] = frozenset(_env_list("BLOCKED_SLUGS"))

# Placeholder poster for channels with no artwork.
FALLBACK_POSTER = _env("FALLBACK_POSTER", "")

# ── Rate limiting ─────────────────────────────────────────────────────────────
RATE_LIMIT_ENABLED = _env_bool("RATE_LIMIT_ENABLED", True)
RATE_LIMIT_MAX = _env_int("RATE_LIMIT_MAX", 240)
RATE_LIMIT_WINDOW = _env_int("RATE_LIMIT_WINDOW", 60)
RATE_LIMIT_BYPASS_IPS: frozenset[str] = frozenset(_env_list("RATE_LIMIT_BYPASS_IPS"))

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL = _env("LOG_LEVEL", "INFO").upper()


class ConfigError(RuntimeError):
    """Raised when the environment is missing something the addon needs."""


def validate() -> None:
    """Fail fast on a bad deploy. Called from main.py before the app starts."""
    missing: list[str] = []
    if not LEAF_BASE_URL:
        missing.append("LEAF_BASE_URL (local dev: http://127.0.0.1:8087)")
    if missing:
        raise ConfigError(
            "Missing required configuration: "
            + ", ".join(missing)
            + ". Copy .env.example to .env and fill it in."
        )

    if not LEAF_BASE_URL.startswith(("http://", "https://")):
        raise ConfigError(f"LEAF_BASE_URL must start with http:// or https:// (got {LEAF_BASE_URL!r})")

    if CACHE_TTL <= REFRESH_INTERVAL:
        raise ConfigError(
            f"CACHE_TTL ({CACHE_TTL}s) must exceed REFRESH_INTERVAL ({REFRESH_INTERVAL}s), "
            "otherwise the catalog goes cold between refreshes."
        )
