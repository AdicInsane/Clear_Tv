"""
channels.py - the channels an admin has added to ClearTV.

This is the leaf's channel lineup: whatever is in here and enabled is what the
addon shows. There is no scheduling and no event matching. An admin picks a
channel from a provider, it goes live; they stop it, it goes away.

Stopping keeps the record (so it can be started again without re-picking it from
a list of thousands) but releases the provider account, because accounts are the
scarce resource. Removing deletes the record outright.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass, field

import config

logger = logging.getLogger("leaf.channels")


@dataclass
class Channel:
    """One channel in the lineup."""

    slug: str
    display_name: str
    stream_id: str
    account_id: str = ""
    categories: list[str] = field(default_factory=lambda: ["General"])
    logo: str = ""
    enabled: bool = True
    started_at: float = 0.0
    added_at: float = 0.0
    # Filled in by the relay once it has seen the upstream playlist, so the
    # addon can show a real quality label instead of guessing.
    stream_info: dict = field(default_factory=dict)
    # Last relay error, surfaced in the panel so a dead channel is visible
    # rather than silently serving nothing.
    last_error: str = ""

    def playlist_url(self) -> str:
        return f"{config.PUBLIC_URL}/m3u/{self.slug}/live.m3u8"


def slugify(text: str) -> str:
    """'Sky Sports Main Event HD' → 'sky-sports-main-event-hd'."""
    slug = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    return re.sub(r"-+", "-", slug)[:80]


class ChannelStore:
    """The lineup, persisted to a JSON file under DATA_DIR."""

    def __init__(self) -> None:
        self._items: dict[str, Channel] = {}
        self._loaded = False

    # ── persistence ───────────────────────────────────────────────────────────

    def load(self) -> None:
        if self._loaded:
            return
        try:
            with open(config.CHANNELS_FILE, encoding="utf-8") as fh:
                raw = json.load(fh)
        except FileNotFoundError:
            raw = {}
        except Exception as exc:
            # Treated as empty and left on disk for inspection rather than
            # overwritten, so a bad edit does not silently lose the lineup.
            logger.error("channels file unreadable (%s) — starting empty", exc)
            raw = {}

        items: dict[str, Channel] = {}
        if isinstance(raw, dict):
            for slug, data in raw.items():
                if not isinstance(data, dict):
                    continue
                try:
                    items[slug] = Channel(
                        slug=slug,
                        display_name=data.get("display_name") or slug,
                        stream_id=str(data.get("stream_id", "")),
                        account_id=data.get("account_id", "") or "",
                        categories=[c for c in (data.get("categories") or []) if isinstance(c, str)]
                        or ["General"],
                        logo=data.get("logo", "") or "",
                        enabled=bool(data.get("enabled", True)),
                        started_at=float(data.get("started_at") or 0),
                        added_at=float(data.get("added_at") or 0),
                        stream_info=data.get("stream_info") or {},
                        last_error=data.get("last_error", "") or "",
                    )
                except Exception as exc:
                    logger.warning("skipping malformed channel %r: %s", slug, exc)

        self._items = items
        self._loaded = True
        if items:
            live = sum(1 for c in items.values() if c.enabled)
            logger.info("loaded %d channel(s), %d enabled", len(items), live)

    def save(self) -> None:
        os.makedirs(os.path.dirname(config.CHANNELS_FILE) or ".", exist_ok=True)
        tmp = config.CHANNELS_FILE + ".tmp"
        try:
            payload = {}
            for slug, ch in self._items.items():
                data = asdict(ch)
                data.pop("slug", None)   # it is the key
                payload[slug] = data
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)
            os.replace(tmp, config.CHANNELS_FILE)
        except Exception as exc:
            logger.error("channels save failed: %s", exc)

    # ── reads ─────────────────────────────────────────────────────────────────

    def all(self) -> list[Channel]:
        self.load()
        return sorted(self._items.values(), key=lambda c: c.display_name.lower())

    def enabled(self) -> list[Channel]:
        return [c for c in self.all() if c.enabled]

    def get(self, slug: str) -> Channel | None:
        self.load()
        return self._items.get(slug)

    def exists(self, slug: str) -> bool:
        self.load()
        return slug in self._items

    def unique_slug(self, base: str) -> str:
        """A slug not already in use, suffixing -2, -3 … if needed."""
        self.load()
        base = slugify(base) or "channel"
        slug = base
        n = 2
        while slug in self._items:
            slug = f"{base}-{n}"
            n += 1
        return slug

    # ── writes ────────────────────────────────────────────────────────────────

    def add(self, channel: Channel) -> Channel:
        self.load()
        channel.added_at = channel.added_at or time.time()
        if channel.enabled and not channel.started_at:
            channel.started_at = time.time()
        self._items[channel.slug] = channel
        self.save()
        logger.info("channel added: %s (%s)", channel.slug, channel.display_name)
        return channel

    def update(self, slug: str, **fields) -> Channel | None:
        self.load()
        ch = self._items.get(slug)
        if ch is None:
            return None
        for key, value in fields.items():
            if hasattr(ch, key):
                setattr(ch, key, value)
        self.save()
        return ch

    def set_enabled(self, slug: str, enabled: bool) -> Channel | None:
        self.load()
        ch = self._items.get(slug)
        if ch is None:
            return None
        ch.enabled = enabled
        ch.started_at = time.time() if enabled else 0.0
        if not enabled:
            ch.account_id = ""
            ch.last_error = ""
        self.save()
        logger.info("channel %s: %s", slug, "started" if enabled else "stopped")
        return ch

    def remove(self, slug: str) -> Channel | None:
        self.load()
        ch = self._items.pop(slug, None)
        if ch is not None:
            self.save()
            logger.info("channel removed: %s", slug)
        return ch

    # ── catalog ───────────────────────────────────────────────────────────────

    def catalog_entries(self) -> list[dict]:
        """
        The payload served at /leaf/catalog.json.

        Field names match what the ClearTV addon's normaliser reads. Only enabled
        channels appear: the addon treats the catalog as "things that play right
        now", so a stopped channel must not be listed.
        """
        entries = []
        for ch in self.enabled():
            entries.append({
                "source": "leaf",
                "slug": ch.slug,
                "channel": ch.slug,
                "channel_name": ch.display_name,
                "title": ch.display_name,
                "summary": ch.display_name,
                "description": "",
                "image_url": ch.logo,
                "categories": list(ch.categories),
                "status": "live",
                "buffering": False,
                "playlist_url": ch.playlist_url(),
                "started_at": ch.started_at,
                "stream_info": ch.stream_info or None,
            })
        return entries

    def stats(self) -> dict:
        all_items = self.all()
        return {
            "total": len(all_items),
            "enabled": sum(1 for c in all_items if c.enabled),
            "stopped": sum(1 for c in all_items if not c.enabled),
            "errored": sum(1 for c in all_items if c.enabled and c.last_error),
        }


store = ChannelStore()
