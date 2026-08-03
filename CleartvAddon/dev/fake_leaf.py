"""
dev/fake_leaf.py - a stub leaf node for local testing.

Serves the same two endpoints the real leaf does, with a handful of made-up
channels, so the addon can be exercised end to end without a leaf running.

    python dev/fake_leaf.py          # listens on 127.0.0.1:8087

Then point the addon at it:  LEAF_BASE_URL=http://127.0.0.1:8087
"""

from __future__ import annotations

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOST = "127.0.0.1"
PORT = 8087
PUBLIC = f"http://{HOST}:{PORT}"

_STARTED = time.time()


def _channels() -> list[dict]:
    """Entries shaped exactly like the leaf bridge's /leaf/catalog.json."""
    return [
        {
            "source": "m3u",
            "slug": "sky-sports-main-event",
            "channel": "sky-sports-main-event",
            "channel_name": "Sky Sports Main Event",
            "title": "Sky Sports Main Event",
            "summary": "Sky Sports Main Event",
            "description": "Live IPTV Recording",
            "image_url": "",
            "categories": ["Sports"],
            "status": "recording",
            "buffering": False,
            "playlist_url": f"{PUBLIC}/m3u/sky-sports-main-event/live.m3u8",
            "started_at": _STARTED,
            "stream_info": {"height": 1080, "width": 1920, "video_bitrate_kbps": 6000},
        },
        {
            "source": "m3u",
            "slug": "tnt-sports-1",
            "channel": "tnt-sports-1",
            "channel_name": "TNT Sports 1",
            "title": "TNT Sports 1",
            "categories": ["Sports"],
            "status": "recording",
            "buffering": True,
            "playlist_url": f"{PUBLIC}/m3u/tnt-sports-1/live.m3u8",
            "started_at": _STARTED,
            "stream_info": {"height": 720, "width": 1280},
        },
        {
            "source": "m3u",
            "slug": "bbc-news",
            "channel": "bbc-news",
            "channel_name": "BBC News",
            "title": "BBC News",
            "categories": ["News"],
            "status": "recording",
            "buffering": False,
            "playlist_url": f"{PUBLIC}/m3u/bbc-news/live.m3u8",
            "started_at": _STARTED,
            "stream_info": {"height": 1080},
        },
        {
            "source": "leaf",
            "slug": "discovery-hd",
            "channel": "discovery-hd",
            "title": "Discovery HD",
            "summary": "Documentaries around the clock",
            "categories": ["Documentary", "Entertainment"],
            "status": "recording",
            "buffering": False,
            "playlist_url": f"{PUBLIC}/leaf/discovery-hd/live.m3u8",
            "started_at": _STARTED,
            "stream_info": {"height": 2160},
        },
        {
            # No playlist_url — the addon must skip this one.
            "source": "m3u",
            "slug": "broken-channel",
            "channel_name": "Broken Channel",
            "categories": ["Sports"],
            "status": "recording",
            "playlist_url": "",
        },
        {
            # No categories — should fall back to DEFAULT_CATEGORY.
            "source": "m3u",
            "slug": "random-local",
            "channel_name": "Random Local Channel",
            "status": "recording",
            "buffering": False,
            "playlist_url": f"{PUBLIC}/m3u/random-local/live.m3u8",
        },
    ]


_PLAYLIST = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:1
#EXTINF:6.0,
seg1.ts
#EXTINF:6.0,
seg2.ts
"""


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, status: int, body: bytes, ctype: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, payload) -> None:
        self._send(status, json.dumps(payload, indent=2).encode(), "application/json")

    def do_GET(self) -> None:  # noqa: N802  (stdlib naming)
        path = self.path.split("?", 1)[0]

        if path == "/leaf/catalog.json":
            self._json(200, _channels())
        elif path == "/leaf/health":
            slugs = [c["slug"] for c in _channels() if c.get("playlist_url")]
            self._json(200, {"status": "ok", "service": "fake leaf", "active": slugs, "count": len(slugs)})
        elif path.endswith("/live.m3u8"):
            self._send(200, _PLAYLIST.encode(), "application/vnd.apple.mpegurl")
        else:
            self._json(404, {"error": "not found", "path": path})

    def log_message(self, fmt: str, *args) -> None:
        print(f"fake-leaf  {fmt % args}")


if __name__ == "__main__":
    print(f"fake leaf node on http://{HOST}:{PORT}  (Ctrl-C to stop)")
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
