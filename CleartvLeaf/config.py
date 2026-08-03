"""
config.py - ClearTV leaf node settings.

Everything comes from the environment (see .env.example). Nothing is defaulted
to a production value, so a misconfigured start fails loudly rather than
pointing at somebody else's infrastructure.
"""

from __future__ import annotations

import os
import secrets

from dotenv import load_dotenv

load_dotenv()


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    return _env(name, "true" if default else "false").lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)))
    except ValueError:
        return default


# ── Server ────────────────────────────────────────────────────────────────────
HOST = _env("LEAF_HOST", "127.0.0.1")
PORT = _env_int("LEAF_PORT", 8090)

# Public base URL of this leaf. Goes into playlist_url in the catalog, so the
# addon (and therefore Stremio) must be able to reach it.
PUBLIC_URL = _env("LEAF_PUBLIC_URL", f"http://{HOST}:{PORT}").rstrip("/")

# ── Admin panel ───────────────────────────────────────────────────────────────
# Required. The panel manages provider credentials and what goes out to users,
# so there is deliberately no default password to fall back on.
ADMIN_PASSWORD = _env("ADMIN_PASSWORD")

# How long a login lasts before the panel asks again.
SESSION_TTL = _env_int("ADMIN_SESSION_TTL", 86400)

# Set true only when the panel is served over HTTPS — it marks the session
# cookie Secure, which a browser will then refuse to send over plain HTTP.
COOKIE_SECURE = _env_bool("ADMIN_COOKIE_SECURE", False)

# Failed-login throttle: attempts allowed per window, per IP.
LOGIN_MAX_ATTEMPTS = _env_int("ADMIN_LOGIN_MAX_ATTEMPTS", 8)
LOGIN_WINDOW = _env_int("ADMIN_LOGIN_WINDOW", 300)

# ── Catalog API ───────────────────────────────────────────────────────────────
# Optional bearer token the addon must present to read the catalog. Empty means
# the catalog is open to anyone who can reach this leaf.
CATALOG_API_KEY = _env("CATALOG_API_KEY")

# ── Segment relay ─────────────────────────────────────────────────────────────
# Clients are never given the provider URL, because it contains the account
# username and password. Instead the leaf rewrites every segment URI to a signed
# /seg/ link it serves itself. This secret signs those links.
#
# A random secret is generated when unset, which is fine for local use: links
# simply stop working across restarts. Set it explicitly for anything long-lived.
SEGMENT_SECRET = _env("SEGMENT_SECRET") or secrets.token_hex(32)
SEGMENT_SECRET_IS_EPHEMERAL = not _env("SEGMENT_SECRET")

# How long a signed segment link stays valid. Long enough to cover a player's
# buffer, short enough that a leaked link is worthless quickly.
SEGMENT_TTL = _env_int("SEGMENT_TTL", 120)

# Playlist cache window. The upstream playlist only changes once per segment
# duration, so a short cache collapses many viewers into one upstream fetch.
PLAYLIST_CACHE_TTL = _env_float("PLAYLIST_CACHE_TTL", 2.0)

# Segment cache: how long fetched segment bytes are kept, and the cap on total
# cache size so memory cannot run away.
SEGMENT_CACHE_TTL = _env_float("SEGMENT_CACHE_TTL", 30.0)
SEGMENT_CACHE_MAX_MB = _env_int("SEGMENT_CACHE_MAX_MB", 256)

# ── Upstream provider requests ────────────────────────────────────────────────
UPSTREAM_TIMEOUT = _env_float("UPSTREAM_TIMEOUT", 15.0)
UPSTREAM_USER_AGENT = _env("UPSTREAM_USER_AGENT", "okhttp/5.0.0-alpha.2")

# Xtream panels routinely have broken or self-signed certificates. Verification
# is off by default because the alternative is most providers simply not working;
# the credentials are still sent over TLS, just unvalidated.
UPSTREAM_VERIFY_TLS = _env_bool("UPSTREAM_VERIFY_TLS", False)

# ── Channel list cache ────────────────────────────────────────────────────────
# A provider's channel list is thousands of entries and rarely changes, so it is
# cached per account and refreshed in the background.
CHANNEL_CACHE_TTL = _env_int("CHANNEL_CACHE_TTL", 4 * 3600)

# How often account status/expiry is re-checked against the provider.
ACCOUNT_INFO_INTERVAL = _env_int("ACCOUNT_INFO_INTERVAL", 30 * 60)

# ── Storage ───────────────────────────────────────────────────────────────────
DATA_DIR = _env("DATA_DIR", "data")
ACCOUNTS_FILE = os.path.join(DATA_DIR, "accounts.json")
CHANNELS_FILE = os.path.join(DATA_DIR, "channels.json")

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL = _env("LOG_LEVEL", "INFO").upper()


class ConfigError(RuntimeError):
    """Raised when the environment is missing something the leaf needs."""


def validate() -> None:
    """Fail fast on a bad deploy. Called before the server binds."""
    if not ADMIN_PASSWORD:
        raise ConfigError(
            "ADMIN_PASSWORD is not set. The admin panel stores IPTV provider "
            "credentials and controls what users can watch, so it will not start "
            "without one. Add it to .env (see .env.example)."
        )
    if len(ADMIN_PASSWORD) < 8:
        raise ConfigError("ADMIN_PASSWORD must be at least 8 characters.")
    if not PUBLIC_URL.startswith(("http://", "https://")):
        raise ConfigError(f"LEAF_PUBLIC_URL must start with http:// or https:// (got {PUBLIC_URL!r})")

    os.makedirs(DATA_DIR, exist_ok=True)


def warnings() -> list[str]:
    """Non-fatal things worth saying out loud at startup."""
    out: list[str] = []
    if SEGMENT_SECRET_IS_EPHEMERAL:
        out.append(
            "SEGMENT_SECRET unset — generated a random one. Existing segment "
            "links break on every restart; set it in .env to keep them stable."
        )
    if not CATALOG_API_KEY:
        out.append(
            "CATALOG_API_KEY unset — /leaf/catalog.json is readable by anyone who "
            "can reach this leaf."
        )
    if not COOKIE_SECURE and not PUBLIC_URL.startswith("https://"):
        out.append(
            "Admin panel is served over plain HTTP — the password and session "
            "cookie travel unencrypted. Fine on localhost, not over a network."
        )
    return out
