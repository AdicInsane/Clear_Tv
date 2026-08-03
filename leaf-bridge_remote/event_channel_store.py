"""
event_channel_store.py — MongoDB store for event→IPTV channel mappings.

Collection: leaf_event_channels
Document shape:
{
  "_id":      "team:atlanta-braves",          # team-level key (reusable across games)
  "label":    "Atlanta Braves",               # human label
  "channels": ["us-mlb-atlanta-braves"],      # IPTV slugs to record
  "sport":    "baseball",
  "match_keys": ["atlanta-braves"],           # strings to match in streamed event slugs/titles
  "updated_at": 1234567890
}

Also stores per-event overrides (one-off):
{
  "_id":      "event:braves-vs-giants-2387973",
  "channels": ["us-mlb-atlanta-braves"],
  "expires_at": 1234567890,   # cleaned up after event ends
}
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Optional

logger = logging.getLogger("leaf.event_channel_store")

_COLLECTION  = "leaf_event_channels"
_col         = None

# JSON file fallback path \u2014 used when MongoDB is not configured (e.g. on the leaf server)
_JSON_PATH   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "event_channels.json")


def _get_col():
    global _col
    if _col is not None:
        return _col
    try:
        from pymongo import MongoClient
        uri = os.getenv("MONGODB_URI", "")
        if not uri:
            return None
        client = MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=5000, connectTimeoutMS=5000)
        _col = client["stremio"][_COLLECTION]
        logger.info("event_channel_store: connected to MongoDB")
        return _col
    except Exception as exc:
        logger.warning("event_channel_store: MongoDB unavailable: %s", exc)
        return None


# ── JSON file persistence (fallback when MongoDB not available) ───────────────

def _load_json() -> dict[str, dict]:
    """Load all records from the local JSON file."""
    try:
        with open(_JSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as exc:
        logger.warning("event_channel_store: JSON read error: %s", exc)
        return {}


def _save_json(data: dict[str, dict]) -> None:
    """Atomically write all records to the local JSON file."""
    tmp = _JSON_PATH + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, _JSON_PATH)
    except Exception as exc:
        logger.warning("event_channel_store: JSON write error: %s", exc)


# ── In-memory cache (populated from JSON on first read) ───────────────────────
_mem: dict[str, dict] = {}
_mem_loaded: bool = False

# ── Short-lived read cache for _load_all (avoids N×MongoDB per schedule load) ─
_load_all_cache: dict = {}
_load_all_cache_at: float = 0.0
_LOAD_ALL_TTL = 3.0  # seconds


def _ensure_mem_loaded() -> None:
    global _mem, _mem_loaded
    if not _mem_loaded:
        _mem = _load_json()
        _mem_loaded = True
        if _mem:
            logger.info("event_channel_store: loaded %d records from JSON file", len(_mem))


def _load_all() -> dict[str, dict]:
    global _load_all_cache, _load_all_cache_at
    import time
    if _load_all_cache and (time.time() - _load_all_cache_at) < _LOAD_ALL_TTL:
        return _load_all_cache
    col = _get_col()
    if col is not None:
        try:
            result = {doc["_id"]: doc for doc in col.find({})}
            _load_all_cache = result
            _load_all_cache_at = time.time()
            return result
        except Exception as exc:
            logger.warning("event_channel_store: read error: %s", exc)
    # JSON file fallback
    _ensure_mem_loaded()
    return dict(_mem)


def _save(doc_id: str, doc: dict) -> None:
    global _load_all_cache_at
    _load_all_cache_at = 0.0  # invalidate read cache on any write
    col = _get_col()
    if col is not None:
        try:
            col.replace_one({"_id": doc_id}, doc, upsert=True)
            return
        except Exception as exc:
            logger.warning("event_channel_store: write error: %s", exc)
    # JSON file fallback \u2014 update in-memory then flush to disk atomically
    _ensure_mem_loaded()
    _mem[doc_id] = doc
    _save_json(dict(_mem))


def _delete(doc_id: str) -> None:
    col = _get_col()
    if col is not None:
        try:
            col.delete_one({"_id": doc_id})
            return
        except Exception as exc:
            logger.warning("event_channel_store: delete error: %s", exc)
    # JSON file fallback
    _ensure_mem_loaded()
    _mem.pop(doc_id, None)
    _save_json(dict(_mem))


# ── Public API ────────────────────────────────────────────────────────────────

def list_team_rules() -> list[dict]:
    """Return all team-level reusable rules."""
    all_docs = _load_all()
    return [
        {**v, "id": k}
        for k, v in all_docs.items()
        if k.startswith("team:")
    ]


def list_event_overrides() -> list[dict]:
    """Return all one-off event overrides."""
    all_docs = _load_all()
    now = time.time()
    return [
        {**v, "id": k}
        for k, v in all_docs.items()
        if k.startswith("event:") and v.get("expires_at", now + 1) > now
    ]


def list_groups() -> list[dict]:
    """Return all channel groups."""
    all_docs = _load_all()
    return [
        {**v, "id": k}
        for k, v in all_docs.items()
        if k.startswith("group:")
    ]


def get_group(group_id: str) -> Optional[dict]:
    """Return a specific channel group."""
    all_docs = _load_all()
    return all_docs.get(group_id)


def save_group(group_id: str, label: str, channels: list) -> str:
    """Save a channel group."""
    doc_id = group_id if group_id.startswith("group:") else f"group:{group_id}"
    doc = {
        "_id":        doc_id,
        "label":      label,
        "channels":   channels,
        "updated_at": int(time.time()),
    }
    _save(doc_id, doc)
    logger.info("Saved group: %s → %s", doc_id, channels)
    return doc_id


def save_team_rule(
    team_key: str,
    label: str,
    channels: list,
    sport: str = "sports",
    match_keys: Optional[list[str]] = None,
    end_offset_hours: Optional[float] = None,
) -> str:
    doc_id = f"team:{team_key}"
    doc = {
        "_id":        doc_id,
        "label":      label,
        "channels":   channels,
        "sport":      sport,
        "match_keys": match_keys or [team_key],
        "updated_at": int(time.time()),
    }
    if end_offset_hours is not None:
        doc["end_offset_hours"] = end_offset_hours
    _save(doc_id, doc)
    logger.info("Saved team rule: %s → %s", team_key, channels)
    return doc_id


def save_event_override(
    event_id: str,
    channels: list,
    expires_at: Optional[float] = None,
    end_offset_hours: Optional[float] = None,
) -> str:
    doc_id = f"event:{event_id}"
    doc = {
        "_id":        doc_id,
        "channels":   channels,
        "expires_at": expires_at or (time.time() + 36 * 3600),
        "updated_at": int(time.time()),
    }
    if end_offset_hours is not None:
        doc["end_offset_hours"] = end_offset_hours
    _save(doc_id, doc)
    logger.info("Saved event override: %s → %s", event_id, channels)
    return doc_id


def delete_rule(doc_id: str) -> None:
    _delete(doc_id)


# ── Auto-end pin ──────────────────────────────────────────────────────────────
# The absolute auto-end timestamp for an event, computed ONCE from
# event_starts_at + offset and persisted here. On any restart the scheduler
# reuses this pinned value instead of recomputing (which would reset the clock
# from time.time() whenever the event has dropped from the live feed).

def get_auto_end_ts(event_id: str) -> Optional[float]:
    """Return the pinned absolute auto-end epoch seconds for an event, or None."""
    doc = _load_all().get(f"autoend:{event_id}")
    if doc:
        ts = doc.get("auto_end_ts")
        return float(ts) if ts else None
    return None


def set_auto_end_ts(event_id: str, ts: float) -> None:
    """Pin the absolute auto-end timestamp for an event (idempotent — first
    writer wins is enforced by the caller, which only pins when unset)."""
    doc_id = f"autoend:{event_id}"
    _save(doc_id, {
        "_id":         doc_id,
        "auto_end_ts": float(ts),
        # keep the doc around until well after the scheduled end, then it can be
        # cleaned up like any expiring override
        "expires_at":  float(ts) + 6 * 3600,
        "updated_at":  int(time.time()),
    })


def clear_auto_end_ts(event_id: str) -> None:
    """Remove the pin when the event genuinely ends, so a later re-add of the
    same event gets a fresh auto-end window."""
    _delete(f"autoend:{event_id}")


def _slug_of(ch) -> str:
    """Accept either a plain string slug or a {slug, stream_url} dict."""
    return ch["slug"] if isinstance(ch, dict) else ch


def get_channels_for_event(event_id: str, event_title: str, event_slug: str, use_team_rules: bool = True) -> list[str]:
    """
    Return the list of IPTV channel slugs for a given streamed.pk event.
    Priority: event override > team rule match.
    Returns plain slug strings (backward-compatible).
    use_team_rules=False skips team rule matching (used for already-live events).
    """
    all_docs = _load_all()

    # 1. Check for explicit event override
    override = all_docs.get(f"event:{event_id}")
    if override and override.get("expires_at", 0) > time.time():
        return [_slug_of(c) for c in override.get("channels", [])]

    if not use_team_rules:
        return []

    # 2. Check team rules — collect channels from ALL matching rules (deduped, order preserved)
    search_text = f"{event_slug} {event_title}".lower()
    seen: set[str] = set()
    result: list[str] = []
    for doc_id, doc in all_docs.items():
        if not doc_id.startswith("team:"):
            continue
        for key in doc.get("match_keys", []):
            if key.lower() in search_text:
                for ch in doc.get("channels", []):
                    slug = _slug_of(ch)
                    if slug not in seen:
                        seen.add(slug)
                        result.append(slug)
                break  # matched this rule, move to next rule

    return result


def get_end_offset_for_event(event_id: str, event_title: str, event_slug: str) -> Optional[float]:
    """Return end_offset_hours for an event (top-level field, applies to all its channels)."""
    all_docs = _load_all()
    override = all_docs.get(f"event:{event_id}")
    if override and override.get("expires_at", 0) > time.time():
        return override.get("end_offset_hours")
    search_text = f"{event_slug} {event_title}".lower()
    for doc_id, doc in all_docs.items():
        if not doc_id.startswith("team:"):
            continue
        for key in doc.get("match_keys", []):
            if key.lower() in search_text:
                return doc.get("end_offset_hours")
    return None


def get_stream_url_for_channel(
    event_id: str, event_title: str, event_slug: str, ch_slug: str
) -> str:
    """
    Return the stored stream_url for a specific channel slug assigned to this event.
    Returns empty string if not stored (scheduler will resolve via Xtream API).
    """
    all_docs = _load_all()

    # Check event override first
    override = all_docs.get(f"event:{event_id}")
    if override and override.get("expires_at", 0) > time.time():
        for ch in override.get("channels", []):
            if isinstance(ch, dict) and ch.get("slug") == ch_slug:
                return ch.get("stream_url", "")

    # Check team rules
    search_text = f"{event_slug} {event_title}".lower()
    for doc_id, doc in all_docs.items():
        if not doc_id.startswith("team:"):
            continue
        for key in doc.get("match_keys", []):
            if key.lower() in search_text:
                for ch in doc.get("channels", []):
                    if isinstance(ch, dict) and ch.get("slug") == ch_slug:
                        return ch.get("stream_url", "")

    return ""


def cleanup_expired_overrides() -> None:
    """Remove expired event overrides from MongoDB.

    Uses a 30-minute grace period beyond the expires_at timestamp before
    actually deleting — this prevents an assignment from being wiped out
    by a transient API blip right as a match kicks off.
    """
    all_docs = _load_all()
    now = time.time()
    grace = 30 * 60  # 30 minutes grace
    for doc_id, doc in all_docs.items():
        if doc_id.startswith("event:") and doc.get("expires_at", now + 1) <= (now - grace):
            _delete(doc_id)
            logger.info("Cleaned up expired event override: %s", doc_id)


# ── Channel slug → human name map ─────────────────────────────────────────────
# Scheduler channel slugs are numeric provider IDs (e.g. "1527629"). The admin UI
# should show a friendly name ("Sky Sports F1") next to that ID. We persist a
# single map doc (meta:channel_names) written whenever a channel is assigned with
# a known name. It is display-only — nothing in the recording path reads it, so a
# missing entry just falls back to showing the slug.
_CHANNEL_NAMES_DOC = "meta:channel_names"


def get_channel_names() -> dict:
    """Return the full {slug: name} display map (empty dict if none stored)."""
    doc = _load_all().get(_CHANNEL_NAMES_DOC) or {}
    return dict(doc.get("names", {}))


def set_channel_names(names: dict) -> None:
    """Merge {slug: name} pairs into the persistent display map. Blank names are
    ignored so we never overwrite a real name with an empty string."""
    clean = {str(k): str(v).strip() for k, v in (names or {}).items()
             if k and str(v).strip()}
    if not clean:
        return
    current = get_channel_names()
    current.update(clean)
    _save(_CHANNEL_NAMES_DOC, {"_id": _CHANNEL_NAMES_DOC, "names": current,
                               "updated_at": int(time.time())})
