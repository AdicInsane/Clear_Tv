# ClearTV

A Stremio addon for live TV. Channels come from a leaf node you control; the
addon turns them into a Stremio catalog and hands the leaf's HLS URLs straight
to the player.

Local development only right now — nothing here is deployed.

## How it works

```
leaf node  ──/leaf/catalog.json──▶  ClearTV  ──manifest/catalog/meta/stream──▶  Stremio
                                       │
                                       └─ stream URLs point at the leaf directly
```

The addon is **not in the media path**. It serves small JSON documents and tells
Stremio where the playlist lives; video never flows through it. That keeps it
cheap to run and means a restart never interrupts playback.

A single background loop pulls the channel list every `REFRESH_INTERVAL`
seconds into an in-memory snapshot. Every request is answered from that
snapshot, so no request ever waits on the leaf node.

**If the leaf goes down, the last good snapshot keeps serving** until it exceeds
`CACHE_TTL`. Refresh failures retry on the shorter `REFRESH_RETRY_INTERVAL` and
are reported on `/health`.

## Running locally

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env          # then edit LEAF_BASE_URL
```

Without a leaf node handy, start the stub in one terminal:

```bash
.venv/bin/python dev/fake_leaf.py     # 127.0.0.1:8087
```

and the addon in another:

```bash
.venv/bin/python main.py              # 127.0.0.1:7100
```

Open <http://127.0.0.1:7100/configure> to pick settings and get an install link.

## Configuration

Everything is environment-driven; see `.env.example` for the annotated list.
`LEAF_BASE_URL` is the only required value — there is no default, so a
misconfigured start fails immediately instead of pointing somewhere unintended.

The addon also refuses to start if `CACHE_TTL` is not greater than
`REFRESH_INTERVAL`, since that would let the catalog go cold between refreshes.

## Endpoints

| Path | Purpose |
|---|---|
| `/manifest.json` | Stremio manifest |
| `/catalog/tv/cleartv_channels.json` | channel list (supports `skip`, `genre`, `search`) |
| `/meta/tv/cleartv:{slug}.json` | one channel's detail page |
| `/stream/tv/cleartv:{slug}.json` | the playable leaf URL |
| `/configure` | settings page + install link |
| `/health` | snapshot age, refresh count, last error |
| `/channels.json` | flat channel dump, handy for checking the leaf |
| `/categories.json` | category list used by the configure page |

Each protocol route also exists as `/{config}/...`, where `{config}` is the
base64url settings blob the configure page generates.

## User settings

Stremio has no per-user storage, so settings ride in the install URL. They are
cosmetic only — hidden categories, whether to show buffering channels, whether
to append quality to names, and sort order. No credentials, nothing to sign. An
unreadable blob falls back to defaults rather than erroring, so a stale install
link still works.

## Channel identity

A channel's Stremio id is `cleartv:{slug}`, where the slug is the leaf's own.
That makes ids stable across restarts on both sides and keeps the mapping
obvious when reading logs.
