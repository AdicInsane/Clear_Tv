"""
live_registry.py — Authoritative registry of events the leaf node is ACTIVELY
recording, so the main sportsaddon backends can decide when to remove an event
from their "Live Now" page WITHOUT depending on the flaky streamed.pk live feed.

Problem this solves:
  The main backends built Live Now from streamed.pk's /matches/live feed and
  removed a match as soon as streamed.pk stopped reporting it (+ a 5-min sticky).
  streamed.pk hiccups constantly, so live matches flickered / vanished mid-event.

New rule (leaf-recorded events only):
  - An event the leaf node is recording is STICKY in Live Now — the main backend
    never removes it on a streamed.pk hiccup.
  - It is removed ONLY when the leaf node marks it ended (scheduled auto-end,
    hard cap, or a real stop) OR its ends_at passes.
  - Events with no leaf channel keep the backend's existing behaviour.

Collection: leaf_live_registry
Document shape (one per event_id):
{
  "_id":         "streamed:1234567",     # full stremio event id (matches leaf meta id)
  "match_id":    "1234567",              # streamed.pk raw id (main matches on this)
  "title":       "Team A vs Team B",
  "category":    "basketball",
  "started_at":  1783051200.0,           # unix ts recording began
  "ends_at":     1783065600.0,           # unix ts event should leave Live Now
  "auto_end_at": 1783065600.0,           # scheduled end (may be null)
  "status":      "recording" | "ended",
  "updated_at":  1783051260.0
}

Storage: MongoDB (primary). JSON-file fallback so a Mongo blip never wipes state.
Leaf always has MONGODB_URI, so Mongo is the live path; the main backends read
this same collection (premium) or the /leaf/live_registry.json HTTP endpoint (free).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Optional

logger = logging.getLogger("leaf.live_registry")

_COLLECTION = "leaf_live_registry"
_col = None
_col_tried = False

# Keep ended events around long enough that a main backend NEVER falls back to
# streamed.pk's stale "live" signal for an event the leaf already ended.
# streamed.pk keeps ended events in its live feed for HOURS (england-vs-argentina
# was stuck 7.6h), so a 15-min retain let the "ended" row vanish while streamed.pk
# still said live → the event resurrected. 24h comfortably outlives any streamed.pk
# staleness; the row is pruned once past this window (event long gone everywhere).
_ENDED_RETAIN_SECS = 24 * 60 * 60

# JSON fallback (same dir as code) — only used if Mongo is unreachable.
_JSON_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "live_registry.json")
_lock = threading.Lock()


def _get_col():
    global _col, _col_tried
    if _col is not None:
        return _col
    if _col_tried:
        return None
    _col_tried = True
    try:
        from pymongo import MongoClient
        uri = os.getenv("MONGODB_URI", "")
        if not uri:
            logger.warning("live_registry: MONGODB_URI not set — using JSON fallback")
            return None
        client = MongoClient(uri, serverSelectionTimeoutMS=3000,
                             socketTimeoutMS=5000, connectTimeoutMS=5000)
        _col = client["stremio"][_COLLECTION]
        logger.info("live_registry: connected to MongoDB")
        return _col
    except Exception as exc:
        logger.warning("live_registry: MongoDB unavailable: %s — JSON fallback", exc)
        return None


# ── JSON fallback ─────────────────────────────────────────────────────────────

def _load_json() -> dict[str, dict]:
    try:
        with open(_JSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as exc:
        logger.warning("live_registry: JSON read error: %s", exc)
        return {}


def _save_json(data: dict[str, dict]) -> None:
    tmp = _JSON_PATH + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, _JSON_PATH)
    except Exception as exc:
        logger.warning("live_registry: JSON write error: %s", exc)


# ── Public API ────────────────────────────────────────────────────────────────

def upsert_recording(event_id: str, match_id: str, title: str, category: str,
                     started_at: float, ends_at: float,
                     auto_end_at: Optional[float]) -> None:
    """Mark an event as actively recording. Idempotent — safe to call every tick.
    Never downgrades an already-'recording' row; refreshes its timing fields."""
    if not event_id:
        return
    now = time.time()
    doc = {
        "_id":         event_id,
        "match_id":    match_id,
        "title":       title,
        "category":    category,
        "started_at":  started_at,
        "ends_at":     ends_at,
        "auto_end_at": auto_end_at,
        "status":      "recording",
        "updated_at":  now,
    }
    col = _get_col()
    if col is not None:
        try:
            col.replace_one({"_id": event_id}, doc, upsert=True)
            return
        except Exception as exc:
            logger.warning("live_registry: upsert %s failed: %s — JSON fallback", event_id, exc)
    with _lock:
        data = _load_json()
        data[event_id] = doc
        _save_json(data)


def mark_ended(event_id: str) -> None:
    """Mark an event ended. The main backend then allows it to leave Live Now.
    The row is retained for _ENDED_RETAIN_SECS so late pollers see the transition."""
    if not event_id:
        return
    now = time.time()
    col = _get_col()
    if col is not None:
        try:
            col.update_one(
                {"_id": event_id},
                {"$set": {"status": "ended", "ended_at": now, "updated_at": now}},
            )
            return
        except Exception as exc:
            logger.warning("live_registry: mark_ended %s failed: %s — JSON fallback", event_id, exc)
    with _lock:
        data = _load_json()
        if event_id in data:
            data[event_id]["status"]     = "ended"
            data[event_id]["ended_at"]   = now
            data[event_id]["updated_at"] = now
            _save_json(data)


def get_all() -> list[dict]:
    """Return every registry row (recording + recently-ended). Used by the HTTP
    endpoint and by the main backend's Mongo reader. Prunes stale ended rows."""
    now = time.time()
    col = _get_col()
    if col is not None:
        try:
            rows = list(col.find({}))
            # Prune ended rows older than the retain window (best-effort, non-blocking).
            stale = [r["_id"] for r in rows
                     if r.get("status") == "ended"
                     and now - r.get("ended_at", now) > _ENDED_RETAIN_SECS]
            if stale:
                try:
                    col.delete_many({"_id": {"$in": stale}})
                except Exception:
                    pass
            return [r for r in rows if r["_id"] not in stale]
        except Exception as exc:
            logger.warning("live_registry: get_all failed: %s — JSON fallback", exc)
    with _lock:
        data = _load_json()
        stale = [k for k, v in data.items()
                 if v.get("status") == "ended"
                 and now - v.get("ended_at", now) > _ENDED_RETAIN_SECS]
        if stale:
            for k in stale:
                data.pop(k, None)
            _save_json(data)
        return list(data.values())


# Event IDs THIS process has observed as recording. We only ever mark an event
# 'ended' if we previously saw it 'recording' in this process — never purely
# because it's absent from `active`. This is the guard against the restart race:
#   On a leaf restart _scheduler_recordings starts EMPTY, and the first tick only
#   re-registers events still present in the streamed.pk feed. If a genuinely-live
#   recording's event momentarily dropped from that feed at restart time (the exact
#   hiccup this feature exists to survive), a naive "absent -> ended" would wrongly
#   mark it ended and the main backend would drop it from Live Now. By requiring
#   "seen recording by THIS process first", restart-time absences can't end an
#   event; only a real stop (which re-registers then pops it) can.
# Events that genuinely ended while the leaf was down stay 'recording' in Mongo but
# are bounded by the main reader's ends_at ceiling (always set: auto_end_at or
# started+hard-cap), so they stop pinning at their scheduled end — safe direction.
_seen_recording: set[str] = set()


def sync_active(active: dict[str, dict]) -> None:
    """Reconcile the registry against the set of events currently recording.

    `active` maps event_id -> {match_id, title, category, started_at, ends_at, auto_end_at}.
    - Upserts every active event as 'recording' (and records it as seen).
    - Marks 'ended' ONLY events this process has seen recording that are now absent.

    Called once per scheduler tick — self-healing across rotations and shared
    channels, and restart-safe (see _seen_recording note above).
    """
    for eid, info in active.items():
        upsert_recording(
            event_id=eid,
            match_id=info.get("match_id", ""),
            title=info.get("title", ""),
            category=info.get("category", ""),
            started_at=info.get("started_at", time.time()),
            ends_at=info.get("ends_at", 0.0),
            auto_end_at=info.get("auto_end_at"),
        )
        _seen_recording.add(eid)

    active_ids = set(active.keys())
    for eid in list(_seen_recording):
        if eid not in active_ids:
            # This process saw it recording and it's now gone from the active set
            # → a real stop (auto-end / hard cap / grace / force-delete). End it.
            logger.info("live_registry: event '%s' stopped recording — marking ended", eid)
            mark_ended(eid)
            _seen_recording.discard(eid)
