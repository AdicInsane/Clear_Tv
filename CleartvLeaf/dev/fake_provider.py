"""
dev/fake_provider.py - a stub Xtream Codes provider for local testing.

Implements just enough of the real thing to exercise the leaf end to end:

  GET /player_api.php?username=&password=                    → account info
  GET /player_api.php?...&action=get_live_streams             → channel list
  GET /live/{user}/{pass}/{stream_id}.m3u8                    → media playlist
  GET /hls/{stream_id}/{name}.ts                              → segment bytes

    python dev/fake_provider.py        # listens on 127.0.0.1:9100

Credentials are demo/demo. Anything else gets the same 401-ish response a real
panel gives, so the leaf's error handling can be tested too.
"""

from __future__ import annotations

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

HOST = "127.0.0.1"
PORT = 9100
BASE = f"http://{HOST}:{PORT}"

# Two logins so multi-account behaviour (claiming, capacity, exhaustion) can be
# exercised against a single stub.
USERS = {"demo": "demo", "demo2": "demo2"}

USERNAME = "demo"   # kept for the legacy single-user checks below
PASSWORD = "demo"


def _auth_ok(user: str, pwd: str) -> bool:
    return user in USERS and USERS[user] == pwd

# stream_id → (name, category)
CHANNELS = {
    "101": ("Sky Sports Main Event HD", "UK | SPORTS"),
    "102": ("Sky Sports Premier League", "UK | SPORTS"),
    "103": ("TNT Sports 1 HD", "UK | SPORTS"),
    "201": ("BBC One HD", "UK | ENTERTAINMENT"),
    "202": ("BBC News HD", "UK | NEWS"),
    "203": ("ITV1 HD", "UK | ENTERTAINMENT"),
    "301": ("Discovery Channel HD", "UK | DOCUMENTARY"),
    "302": ("National Geographic HD", "UK | DOCUMENTARY"),
    "401": ("CNN International", "NEWS | WORLD"),
    "402": ("Al Jazeera English", "NEWS | WORLD"),
    # Deliberately broken: the playlist route 500s for this one so the leaf's
    # "stream check failed" path can be tested.
    "999": ("Broken Test Channel", "TEST"),
}

# A master playlist is returned for this id, to exercise variant selection.
MASTER_ID = "101"


def _media_playlist(stream_id: str) -> str:
    """Rolling live playlist with relative segment URIs."""
    seq = int(time.time() // 6)
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        "#EXT-X-TARGETDURATION:6",
        f"#EXT-X-MEDIA-SEQUENCE:{seq}",
    ]
    for i in range(4):
        lines.append("#EXTINF:6.000,")
        # Relative on purpose — the leaf must resolve it against the playlist URL.
        lines.append(f"seg{seq + i}.ts")
    return "\n".join(lines) + "\n"


def _master_playlist(stream_id: str) -> str:
    return (
        "#EXTM3U\n"
        "#EXT-X-VERSION:3\n"
        '#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1280x720,CODECS="avc1.4d401f,mp4a.40.2"\n'
        f"{BASE}/hls/{stream_id}/720/index.m3u8\n"
        '#EXT-X-STREAM-INF:BANDWIDTH=6000000,RESOLUTION=1920x1080,CODECS="avc1.640028,mp4a.40.2"\n'
        f"{BASE}/hls/{stream_id}/1080/index.m3u8\n"
    )


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, status: int, body: bytes, ctype: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, payload) -> None:
        self._send(status, json.dumps(payload).encode(), "application/json")

    def _text(self, status: int, text: str, ctype: str = "text/plain") -> None:
        self._send(status, text.encode(), ctype)

    # ── routes ────────────────────────────────────────────────────────────────

    def do_GET(self) -> None:  # noqa: N802  (stdlib naming)
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/player_api.php":
            self._player_api(query)
        elif path.startswith("/live/"):
            self._live(path)
        elif path.startswith("/hls/"):
            self._hls(path)
        else:
            self._json(404, {"error": "not found", "path": path})

    def _player_api(self, query: dict) -> None:
        user = (query.get("username") or [""])[0]
        pwd = (query.get("password") or [""])[0]

        if not _auth_ok(user, pwd):
            # What a real panel returns for bad credentials: HTTP 200 with auth=0,
            # not a 401. The leaf has to detect this explicitly.
            self._json(200, {"user_info": {"auth": 0}})
            return

        action = (query.get("action") or [""])[0]

        if action == "get_live_streams":
            self._json(200, [
                {
                    "num": i + 1,
                    "name": name,
                    "stream_id": int(sid),
                    "stream_icon": f"{BASE}/logos/{sid}.png",
                    "category_name": cat,
                    "epg_channel_id": f"{sid}.epg",
                }
                for i, (sid, (name, cat)) in enumerate(CHANNELS.items())
            ])
            return

        if not action:
            self._json(200, {
                "user_info": {
                    "username": user,
                    "auth": 1,
                    "status": "Active",
                    "exp_date": str(int(time.time()) + 45 * 86400),
                    "is_trial": "0",
                    "active_cons": "0",
                    "max_connections": "2",
                },
                "server_info": {"url": HOST, "port": str(PORT), "https_port": ""},
            })
            return

        self._json(200, [])

    def _live(self, path: str) -> None:
        # /live/{user}/{pass}/{stream_id}.m3u8
        parts = [p for p in path.split("/") if p]
        if len(parts) != 4:
            self._text(400, "bad path")
            return

        _, user, pwd, filename = parts
        if not _auth_ok(user, pwd):
            self._text(401, "unauthorized")
            return

        # A media playlist served from /live/ has relative segment URIs, so the
        # player resolves them back to this same directory.
        if filename.endswith(".ts"):
            self._send(200, b"\x47" + b"\x00" * 8191, "video/mp2t")
            return

        stream_id = filename.rsplit(".", 1)[0]
        if stream_id not in CHANNELS:
            self._text(404, "no such stream")
            return
        if stream_id == "999":
            self._text(500, "provider error (intentional, for testing)")
            return

        if stream_id == MASTER_ID:
            self._text(200, _master_playlist(stream_id), "application/vnd.apple.mpegurl")
        else:
            self._text(200, _media_playlist(stream_id), "application/vnd.apple.mpegurl")

    def _hls(self, path: str) -> None:
        # /hls/{stream_id}/{variant}/index.m3u8  or  /hls/{stream_id}/segN.ts
        if path.endswith("index.m3u8"):
            stream_id = path.split("/")[2]
            self._text(200, _media_playlist(stream_id), "application/vnd.apple.mpegurl")
            return
        if path.endswith(".ts"):
            # 8 KB of filler — enough to prove the relay passes bytes through.
            self._send(200, b"\x47" + b"\x00" * 8191, "video/mp2t")
            return
        self._text(404, "not found")

    def log_message(self, fmt: str, *args) -> None:
        print("fake-provider  " + (fmt % args))


if __name__ == "__main__":
    print(f"fake Xtream provider on {BASE}  (demo/demo, Ctrl-C to stop)")
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
