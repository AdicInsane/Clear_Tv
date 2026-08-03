# ClearTV leaf

The channel source behind the ClearTV addon. An admin picks channels from an IPTV
provider; the leaf serves them, and the addon turns them into a Stremio catalog.

Local development only right now — nothing here is deployed.

## What the panel does

Two things, deliberately:

- **Channels you have added** — the lineup, with what each one is playing on
- **The stopper** — pull a channel out of the catalog and release its account

Everything the old sports leaf panel did beyond that is gone: no scheduling, no
event matching, no Groq, no worker pool, no hidden matches, no YouTube, no
MongoDB, no R2. The old `leaf_admin.py` was 4,300 lines; this one is 570.

## How a channel becomes watchable

```
provider  ──credentials stay here──▶  leaf  ──/m3u/{slug}/live.m3u8──▶  addon ──▶ Stremio
                                       │
                                       └─ segments proxied via opaque /seg/{ref}
```

Adding a channel claims a provider account, verifies the stream actually plays,
then puts it in the lineup. The addon picks it up on its next refresh.

**Provider credentials never leave the leaf.** An Xtream stream URL looks like
`…/live/<user>/<pass>/1234.m3u8` — handing that to a player would give every
viewer a shared, connection-capped login. So the leaf fetches upstream itself and
rewrites every segment, key and map URI to an opaque `/seg/<ref>` id. The real
URL is held in memory; the id maps back to it server-side only.

Encoding the URL instead (base64 or similar) would **not** be enough — a client
can decode it. There is no client-supplied URL anywhere in the segment path,
which also means `/seg/` cannot be used as an open proxy.

One account serves one channel at a time, because that is the provider's
connection limit. Accounts are claimed under a lock so two concurrent adds cannot
double-book one and get each other's stream dropped.

## Caching

Upstream load stays flat regardless of how many people are watching:

| | window | why |
|---|---|---|
| playlists | 2s, single-flight | hundreds of viewers → one upstream fetch per channel |
| segments | 30s, capped at 256 MB | each segment fetched once, not once per viewer |

## Running locally

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env          # set ADMIN_PASSWORD
```

Without a real provider, start the stub in one terminal:

```bash
.venv/bin/python dev/fake_provider.py    # 127.0.0.1:9100, demo/demo
```

and the leaf in another:

```bash
.venv/bin/python main.py                 # 127.0.0.1:8090
```

Then open <http://127.0.0.1:8090/admin>, add the provider
(`http://127.0.0.1:9100`, `demo`/`demo`), search for a channel, and add it.

Point the addon at it with `LEAF_BASE_URL=http://127.0.0.1:8090`.

## Configuration

All environment-driven; `.env.example` is annotated.

`ADMIN_PASSWORD` is **required** with no default — the panel holds provider
credentials and controls what users can watch, so the leaf refuses to start
without one. Minimum 8 characters.

Worth setting for anything long-lived:

- `SEGMENT_SECRET` — without it a random one is generated per run, so segment
  links break on every restart
- `CATALOG_API_KEY` — otherwise the channel list is readable by anyone who can
  reach the leaf (set the same value as the addon's `LEAF_API_KEY`)

The leaf prints a warning at startup for each of these when unset.

## Endpoints

| Path | Purpose |
|---|---|
| `/leaf/catalog.json` | the lineup, in the shape the addon reads |
| `/leaf/health` | channel counts, account capacity, cache stats |
| `/m3u/{slug}/live.m3u8` | a channel's playlist, rewritten |
| `/seg/{ref}` | one segment, proxied |
| `/admin` | panel |

## Storage

`data/accounts.json` (chmod 600 — it holds provider passwords) and
`data/channels.json`. No database. One leaf owns its files, and that is the whole
deployment model.

Account claims are cleared at startup: nothing is streaming yet at that point, so
any claim left on disk is a leftover from the previous run that would otherwise
permanently shrink the usable pool. The lineup is then restored and each live
channel re-claims an account.

## Security notes

- The panel is session-cookie auth (`HttpOnly`, `SameSite=Strict`), with a
  constant-time password compare and a per-IP failed-login throttle.
- Sessions are in memory, so a restart logs you out. That is intentional — there
  is no session store to leak.
- Over plain HTTP the password and cookie travel unencrypted. Fine on localhost;
  put it behind TLS and set `ADMIN_COOKIE_SECURE=true` before exposing it.
- Provider channel names are third-party input, so the panel builds every table
  row with `textContent`, never `innerHTML`.
