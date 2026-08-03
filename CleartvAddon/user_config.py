"""
user_config.py - per-user settings carried in the addon URL.

Stremio has no per-user storage, so settings travel in the install URL:

    https://cleartv.nowclear.top/<config>/manifest.json

<config> is a base64url-encoded JSON object. It is cosmetic data only — never
credentials — so it does not need signing. An unreadable value falls back to
defaults rather than erroring, because a user with a stale install link should
still get a working addon.
"""

from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass, field

from config import SHOW_BUFFERING_DEFAULT

logger = logging.getLogger("cleartv.userconfig")

SORT_NAME = "name"
SORT_CATEGORY = "category"
SORT_OPTIONS = (SORT_CATEGORY, SORT_NAME)


@dataclass
class UserConfig:
    """Decoded user settings. All fields optional with usable defaults."""

    hidden_categories: frozenset[str] = field(default_factory=frozenset)
    show_buffering: bool = SHOW_BUFFERING_DEFAULT
    show_quality: bool = True
    sort: str = SORT_CATEGORY

    def allows(self, categories: tuple[str, ...]) -> bool:
        """True if a channel with these categories should be shown."""
        if not self.hidden_categories:
            return True
        return not any(c.lower() in self.hidden_categories for c in categories)

    def to_dict(self) -> dict:
        return {
            "hidden": sorted(self.hidden_categories),
            "buffering": self.show_buffering,
            "quality": self.show_quality,
            "sort": self.sort,
        }

    def encode(self) -> str:
        raw = json.dumps(self.to_dict(), separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")


DEFAULT = UserConfig()


def decode(value: str | None) -> UserConfig:
    """
    Decode a config segment from the URL path.

    Returns defaults for empty, malformed, or partially-populated input so a bad
    link degrades to "the whole catalog" instead of a 500.
    """
    if not value:
        return UserConfig()

    try:
        padded = value + "=" * (-len(value) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded).decode())
    except Exception:
        logger.debug("user_config: undecodable value %r — using defaults", value[:32])
        return UserConfig()

    if not isinstance(data, dict):
        return UserConfig()

    hidden_raw = data.get("hidden")
    if isinstance(hidden_raw, str):
        hidden_raw = [hidden_raw]
    hidden = frozenset(
        item.strip().lower()
        for item in (hidden_raw or [])
        if isinstance(item, str) and item.strip()
    )

    sort = data.get("sort")
    if sort not in SORT_OPTIONS:
        sort = SORT_CATEGORY

    def _flag(key: str, default: bool) -> bool:
        raw = data.get(key)
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)):
            return bool(raw)
        if isinstance(raw, str):
            return raw.strip().lower() in ("1", "true", "yes", "on")
        return default

    return UserConfig(
        hidden_categories=hidden,
        show_buffering=_flag("buffering", SHOW_BUFFERING_DEFAULT),
        show_quality=_flag("quality", True),
        sort=sort,
    )


