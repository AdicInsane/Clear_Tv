"""
config.py - Leaf Bridge service configuration.
All secrets loaded from environment / .env file.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── SKTech / Leaf API ─────────────────────────────────────────────────────────
# Firebase Remote Config credentials (impersonates com.live.sktechtv)
SKTECH_FIREBASE_API_KEY        = os.getenv("SKTECH_FIREBASE_API_KEY",        "AIzaSyClGjK1EBL-ZLbCoep1z5QSmwMyHshimSk")
SKTECH_FIREBASE_APP_ID         = os.getenv("SKTECH_FIREBASE_APP_ID",         "1:330162934410:android:0d81c4732e3d206d6cd373")
SKTECH_FIREBASE_PROJECT_NUMBER = os.getenv("SKTECH_FIREBASE_PROJECT_NUMBER", "330162934410")
SKTECH_PACKAGE_NAME            = "com.live.sktechtv"

# AES decryption keys extracted from libnative-lib.so
# V23 algorithm (tried first — current API version)
SKLIVE_V23_KEY = os.getenv("SKLIVE_V23_KEY", "V9LQR42pNKc7smaX")
SKLIVE_V23_IV  = os.getenv("SKLIVE_V23_IV",  "d2WT1lR4ckEvUsdk")

# Legacy algorithm (fallback if V23 fails — keys not yet extracted)
SKLIVE_LEGACY_KEY = os.getenv("SKLIVE_LEGACY_KEY", "")
SKLIVE_LEGACY_IV  = os.getenv("SKLIVE_LEGACY_IV",  "")

# Default API URLs (from remote_config_defaults.xml — used if Firebase fails)
SKTECH_DEFAULT_API_URL = os.getenv("SKTECH_DEFAULT_API_URL", "https://nontonboladis.store")
SKTECH_DEFAULT_WEB_URL = os.getenv("SKTECH_DEFAULT_WEB_URL", "https://sktech.live")

# How often to refresh the events list and try to match (seconds)
LEAF_POLL_INTERVAL = int(os.getenv("LEAF_POLL_INTERVAL", "60"))

# Set to False to completely disable Groq parsing (will fall back to generic categories)
GROQ_ENABLED = os.getenv("GROQ_ENABLED", "true").lower() == "true"
GROQ_API_KEY = "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,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,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,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"
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

# ── MongoDB (reuse same Atlas cluster as sportsaddon for shared Groq cache) ──
MONGODB_URI = os.getenv("MONGODB_URI", "")

# ── Cloudflare R2 (same bucket as recorder — segments go under leaf/ prefix) ─
R2_ACCOUNT_ID        = os.getenv("R2_ACCOUNT_ID",        "")
R2_ACCESS_KEY_ID     = os.getenv("R2_ACCESS_KEY_ID",     "")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_ENDPOINT          = os.getenv("R2_ENDPOINT",          "")
R2_BUCKET_NAME       = os.getenv("R2_BUCKET_NAME",       "recaps")
R2_PUBLIC_DOMAIN     = os.getenv("R2_PUBLIC_DOMAIN",     "https://cdn.recaps.dev").rstrip("/")

# ── Virtual clock HTTP server ─────────────────────────────────────────────────
# Serves /leaf/{slug}/live.m3u8 from memory.
# Nginx on the VPS should proxy recaps.highfly.dev/leaf/ → this port.
LEAF_SERVER_HOST = os.getenv("LEAF_SERVER_HOST", "0.0.0.0")
LEAF_SERVER_PORT = int(os.getenv("LEAF_SERVER_PORT", "8087"))
# Public URL of this Leaf server (used to build playlist_url in /leaf/catalog.json)
LEAF_SERVER_PUBLIC_URL = os.getenv("LEAF_SERVER_PUBLIC_URL", "https://leaf.highfly.dev").rstrip("/")

# Seconds to buffer before serving live.m3u8 (same as recorder)
DELAY_SECONDS        = float(os.getenv("DELAY_SECONDS",        "20"))
LIVE_WINDOW_SEGMENTS = int(os.getenv("LIVE_WINDOW_SEGMENTS",   "6"))

# Max consecutive playlist fetch failures before giving up on a stream
MAX_FAILURES = int(os.getenv("MAX_FAILURES", "30"))

# Max simultaneous Leaf recordings running at once
MAX_CONCURRENT_LEAF = int(os.getenv("MAX_CONCURRENT_LEAF", "20"))

# ── Sportsaddon premium backend (to fetch streamed.pk live match list) ────────
# Used by the matcher to get the current live match list for Groq matching.
# Any premium VPS works — we just need the catalog endpoint.
ADDON_API_BASE  = os.getenv("ADDON_API_BASE",  "https://premium.highfly.dev")
ADDON_ADMIN_TOKEN = os.getenv("ADDON_ADMIN_TOKEN", "")

# ── M3U Playlist Source & Proxies ─────────────────────────────────────────────
M3U_SOURCE_URL = os.getenv("M3U_SOURCE_URL", "http://127.0.0.1:9797/fox4k.m3u")
_proxy_str = os.environ.get("RESIDENTIAL_PROXIES", "")
RESIDENTIAL_PROXIES = [p.strip() for p in _proxy_str.split(",") if p.strip()]
M3U_WHITELIST: list[str] = []  # No auto-started ICS channels (was: fox-sports-fifa-2026)
