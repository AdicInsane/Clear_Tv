"""
channel_store.py - in-memory channel list, refreshed from the leaf node.

Design notes:

* The leaf node is authoritative. This module never invents a channel and never
  keeps one the leaf has dropped for longer than the stale window.
* A refresh failure NEVER clears the last good snapshot. A leaf restart or a
  momentary network blip would otherwise empty every user's catalog mid-session.
  We keep serving the last known channel list until CACHE_TTL passes, and only
  then admit we have nothing.
* Reads are plain dict lookups against an immutable snapshot, so catalog and
  stream requests never wait on I/O.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

import leaf_client
from config import (
    BLOCKED_SLUGS,
    CACHE_TTL,
    DEFAULT_CATEGORY,
    FALLBACK_POSTER,
    ID_PREFIX,
)

logger = logging.getLogger("cleartv.channels")


@dataclass(frozen=True)
class Channel:
    """One playable live channel, normalised from a leaf catalog entry."""

    slug: str
    name: str
    playlist_url: str
    categories: tuple[str, ...] = ()
    description: str = ""
    poster: str = ""
    buffering: bool = False
    source: str = ""
    started_at: float = 0.0
    quality: str = ""
    stream_info: dict = field(default_factory=dict, compare=False)

    @property
    def stremio_id(self) -> str:
        return f"{ID_PREFIX}:{self.slug}"

    @property
    def primary_category(self) -> str:
        return self.categories[0] if self.categories else DEFAULT_CATEGORY


def _first_str(entry: dict, *keys: str) -> str:
    """First non-empty string among the given keys."""
    for key in keys:
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _categories(entry: dict) -> tuple[str, ...]:
    raw = entry.get("categories")
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        raw = []

    seen: list[str] = []
    for item in raw:
        if not isinstance(item, str):
            continue
        label = item.strip()
        if label and label.lower() not in {s.lower() for s in seen}:
            seen.append(label)

    return tuple(seen) if seen else (DEFAULT_CATEGORY,)


def _quality_label(stream_info: dict) -> str:
    """Human label from the leaf's ffprobe data, e.g. '1080p'. Empty if unknown."""
    if not isinstance(stream_info, dict):
        return ""
    height = stream_info.get("height")
    try:
        height = int(height)
    except (TypeError, ValueError):
        return ""
    if height <= 0:
        return ""
    return f"{height}p"


def normalise(entry: dict) -> Channel | None:
    """
    Convert one leaf catalog entry into a Channel, or None if it is unusable.

    An entry needs a slug and a playlist URL to be playable; anything else is
    cosmetic and gets a sensible fallback.
    """
    slug = _first_str(entry, "slug", "channel")
    if not slug or slug in BLOCKED_SLUGS:
        return None

    playlist_url = leaf_client.public_playlist_url(_first_str(entry, "playlist_url"))
    if not playlist_url:
        return None

    # channel_name is the friendly IPTV label; title is the recording name.
    name = _first_str(entry, "channel_name", "title", "summary") or slug

    stream_info = entry.get("stream_info")
    if not isinstance(stream_info, dict):
        stream_info = {}

    try:
        started_at = float(entry.get("started_at") or 0.0)
    except (TypeError, ValueError):
        started_at = 0.0

    return Channel(
        slug=slug,
        name=name,
        playlist_url=playlist_url,
        categories=_categories(entry),
        description=_first_str(entry, "description", "summary"),
        poster=_first_str(entry, "image_url", "poster") or FALLBACK_POSTER,
        buffering=bool(entry.get("buffering")),
        source=_first_str(entry, "source"),
        started_at=started_at,
        quality=_quality_label(stream_info),
        stream_info=stream_info,
    )


class ChannelStore:
    """Holds the current snapshot and the bookkeeping around refreshing it."""

    def __init__(self) -> None:
        self._by_slug: dict[str, Channel] = {}
        self._ordered: tuple[Channel, ...] = ()
        self._fetched_at: float = 0.0
        self._last_error: str = ""
        self._refresh_count: int = 0
        self._fail_count: int = 0

    # ── refresh ───────────────────────────────────────────────────────────────

    async def refresh(self) -> bool:
        """
        Pull a fresh catalog from the leaf node.

        Returns True on success. On failure the previous snapshot is left intact
        and False is returned, so the caller can retry sooner.
        """
        try:
            entries = await leaf_client.fetch_catalog()
        except Exception as exc:
            self._fail_count += 1
            self._last_error = str(exc)
            logger.warning(
                "channel refresh failed (%s) — serving last good snapshot of %d channel(s)",
                exc,
                len(self._ordered),
            )
            return False

        channels: dict[str, Channel] = {}
        skipped = 0
        for entry in entries:
            channel = normalise(entry)
            if channel is None:
                skipped += 1
                continue
            # A slug can appear twice when one IPTV channel carries co-events.
            # The catalog is a channel list, so first entry wins and the rest
            # are duplicates of the same playable stream.
            channels.setdefault(channel.slug, channel)

        self._by_slug = channels
        self._ordered = tuple(
            sorted(channels.values(), key=lambda c: (c.primary_category.lower(), c.name.lower()))
        )
        self._fetched_at = time.time()
        self._last_error = ""
        self._refresh_count += 1

        logger.info(
            "channels refreshed: %d usable%s",
            len(self._ordered),
            f", {skipped} entr{'y' if skipped == 1 else 'ies'} skipped" if skipped else "",
        )
        return True

    # ── reads ─────────────────────────────────────────────────────────────────

    @property
    def is_stale(self) -> bool:
        """True when the snapshot is older than CACHE_TTL (or never loaded)."""
        if not self._fetched_at:
            return True
        return (time.time() - self._fetched_at) > CACHE_TTL

    def all(self, *, include_buffering: bool = True) -> list[Channel]:
        """Every channel, ordered by category then name. Empty once stale."""
        if self.is_stale:
            return []
        if include_buffering:
            return list(self._ordered)
        return [c for c in self._ordered if not c.buffering]

    def get(self, slug: str) -> Channel | None:
        """
        Look up one channel by slug.

        Deliberately ignores staleness: if a player still holds a stream URL for
        a channel we have not re-confirmed yet, handing back the known playlist
        is better than a hard failure — the leaf node itself will 404 if the
        channel is genuinely gone.
        """
        return self._by_slug.get(slug)

    def categories(self) -> list[str]:
        """Sorted unique category labels across all channels."""
        labels: dict[str, str] = {}
        for channel in self.all():
            for label in channel.categories:
                labels.setdefault(label.lower(), label)
        return [labels[key] for key in sorted(labels)]

    def status(self) -> dict:
        """Snapshot health, surfaced on /health."""
        age = time.time() - self._fetched_at if self._fetched_at else None
        return {
            "channels": len(self._ordered),
            "categories": len(self.categories()),
            "age_seconds": round(age, 1) if age is not None else None,
            "stale": self.is_stale,
            "refreshes": self._refresh_count,
            "failures": self._fail_count,
            "last_error": self._last_error or None,
        }


store = ChannelStore()
