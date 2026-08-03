"""
configure.py - the ClearTV configure page.

A single self-contained HTML page: pick categories to hide, a couple of display
toggles, then install. The settings are encoded into the install URL by the
page's own JavaScript, in the same base64url-JSON shape user_config.decode reads.
"""

from __future__ import annotations

from config import ADDON_DESCRIPTION, ADDON_NAME, ADDON_VERSION, BASE_URL, SHOW_BUFFERING_DEFAULT


def configure_html() -> str:
    """Render the page. Server-side values are injected as a JS config object."""
    return _TEMPLATE.replace("__ADDON_NAME__", ADDON_NAME) \
        .replace("__ADDON_DESC__", ADDON_DESCRIPTION) \
        .replace("__ADDON_VERSION__", ADDON_VERSION) \
        .replace("__BASE_URL__", BASE_URL) \
        .replace("__SHOW_BUFFERING__", "true" if SHOW_BUFFERING_DEFAULT else "false")


_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__ADDON_NAME__ · Setup</title>
<link rel="icon" href="/favicon.ico">
<style>
  :root {
    --bg: #0b0f14;
    --panel: #141a22;
    --panel-2: #1b232d;
    --line: #26313d;
    --text: #e8eef5;
    --muted: #8fa1b3;
    --accent: #37d7a0;
    --accent-dark: #1fa87a;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font: 15px/1.55 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    padding: 32px 16px 64px;
  }
  .wrap { max-width: 760px; margin: 0 auto; }
  header { text-align: center; margin-bottom: 28px; }
  h1 { margin: 0 0 6px; font-size: 30px; letter-spacing: -0.4px; }
  h1 span { color: var(--accent); }
  .sub { color: var(--muted); margin: 0; font-size: 14px; }
  .ver { color: var(--muted); font-size: 12px; margin-top: 6px; }
  section {
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 12px;
    padding: 20px;
    margin-bottom: 18px;
  }
  h2 {
    margin: 0 0 4px;
    font-size: 15px;
    text-transform: uppercase;
    letter-spacing: 0.7px;
    color: var(--accent);
  }
  .hint { color: var(--muted); font-size: 13px; margin: 0 0 14px; }
  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(190px, 1fr));
    gap: 8px;
  }
  label.chk {
    display: flex;
    align-items: center;
    gap: 9px;
    background: var(--panel-2);
    border: 1px solid var(--line);
    border-radius: 8px;
    padding: 9px 11px;
    cursor: pointer;
    font-size: 14px;
  }
  label.chk:hover { border-color: var(--accent-dark); }
  input[type=checkbox] { accent-color: var(--accent); width: 16px; height: 16px; cursor: pointer; }
  .row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    padding: 11px 0;
    border-bottom: 1px solid var(--line);
  }
  .row:last-child { border-bottom: none; }
  .row .label { font-size: 14px; }
  .row .desc { color: var(--muted); font-size: 12.5px; margin-top: 2px; }
  select {
    background: var(--panel-2);
    color: var(--text);
    border: 1px solid var(--line);
    border-radius: 7px;
    padding: 7px 10px;
    font-size: 14px;
  }
  .actions { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 4px; }
  button {
    border: none;
    border-radius: 9px;
    padding: 13px 22px;
    font-size: 15px;
    font-weight: 600;
    cursor: pointer;
    font-family: inherit;
  }
  .primary { background: var(--accent); color: #06231a; flex: 1 1 240px; }
  .primary:hover { background: var(--accent-dark); color: #fff; }
  a.primary {
    display: flex;
    align-items: center;
    justify-content: center;
    text-decoration: none;
  }
  .ghost { background: var(--panel-2); color: var(--text); border: 1px solid var(--line); }
  .ghost:hover { border-color: var(--accent-dark); }
  textarea#url {
    display: block;
    width: 100%;
    background: #080b0f;
    border: 1px solid var(--line);
    border-radius: 8px;
    padding: 12px;
    margin-top: 12px;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 12.5px;
    color: var(--muted);
    resize: vertical;
  }
  .status { text-align: center; color: var(--muted); font-size: 13px; padding: 10px 0; }
  .empty { color: var(--muted); font-size: 13.5px; }
  footer { text-align: center; color: var(--muted); font-size: 12px; margin-top: 26px; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>Clear<span>TV</span></h1>
    <p class="sub">__ADDON_DESC__</p>
    <p class="ver">v__ADDON_VERSION__ · <span id="count">…</span></p>
  </header>

  <section>
    <h2>Categories</h2>
    <p class="hint">Every category is included by default. Tick one to hide it.</p>
    <div id="cats" class="grid"><span class="empty">Loading channels…</span></div>
  </section>

  <section>
    <h2>Display</h2>
    <div class="row">
      <div>
        <div class="label">Show channels that are still buffering</div>
        <div class="desc">They usually start playing within a few seconds.</div>
      </div>
      <input type="checkbox" id="buffering">
    </div>
    <div class="row">
      <div>
        <div class="label">Show quality in the channel name</div>
        <div class="desc">Adds the resolution the leaf node reported, e.g. 1080p.</div>
      </div>
      <input type="checkbox" id="quality" checked>
    </div>
    <div class="row">
      <div>
        <div class="label">Sort channels by</div>
        <div class="desc">How the Live TV catalog is ordered.</div>
      </div>
      <select id="sort">
        <option value="category">Category, then name</option>
        <option value="name">Name only</option>
      </select>
    </div>
  </section>

  <section>
    <h2>Install</h2>
    <p class="hint">Opens Stremio and adds ClearTV with the settings above.</p>
    <div class="actions">
      <a class="primary" id="install" href="#">Install in Stremio</a>
      <button class="ghost" id="copy">Copy link</button>
    </div>
    <textarea id="url" rows="2" readonly></textarea>
  </section>

  <div class="status" id="status"></div>
  <footer>__ADDON_NAME__ · configure once, change it any time</footer>
</div>
<script>
var CFG = { base: "__BASE_URL__", showBuffering: __SHOW_BUFFERING__ };

var elCats   = document.getElementById('cats');
var elBuf    = document.getElementById('buffering');
var elQual   = document.getElementById('quality');
var elSort   = document.getElementById('sort');
var elUrl    = document.getElementById('url');
var elStatus = document.getElementById('status');

var categories = [];

// base64url without padding — matches user_config.decode on the server.
function b64url(obj) {
  var bytes = new TextEncoder().encode(JSON.stringify(obj));
  var bin = '';
  for (var i = 0; i < bytes.length; i++) { bin += String.fromCharCode(bytes[i]); }
  return btoa(bin).split('+').join('-').split('/').join('_').split('=').join('');
}

function unb64url(text) {
  var s = text.split('-').join('+').split('_').join('/');
  while (s.length % 4) { s += '='; }
  var bin = atob(s);
  var bytes = new Uint8Array(bin.length);
  for (var i = 0; i < bin.length; i++) { bytes[i] = bin.charCodeAt(i); }
  return JSON.parse(new TextDecoder().decode(bytes));
}

function stremioLink(u) {
  var i = u.indexOf('://');
  return 'stremio://' + (i === -1 ? u : u.substring(i + 3));
}

function hiddenCategories() {
  var out = [];
  var boxes = elCats.querySelectorAll('input[type=checkbox]');
  for (var i = 0; i < boxes.length; i++) {
    if (boxes[i].checked) { out.push(boxes[i].value); }
  }
  return out;
}

function currentConfig() {
  return {
    hidden: hiddenCategories(),
    buffering: elBuf.checked,
    quality: elQual.checked,
    sort: elSort.value
  };
}
function isDefault(cfg) {
  return cfg.hidden.length === 0
    && cfg.buffering === CFG.showBuffering
    && cfg.quality === true
    && cfg.sort === 'category';
}

function render() {
  var cfg = currentConfig();
  // An all-default install needs no config segment, so the URL stays clean.
  var url = isDefault(cfg)
    ? CFG.base + '/manifest.json'
    : CFG.base + '/' + b64url(cfg) + '/manifest.json';
  elUrl.value = url;
  document.getElementById('install').href = stremioLink(url);
}

function renderCategories(hidden) {
  var lower = {};
  for (var i = 0; i < hidden.length; i++) { lower[String(hidden[i]).toLowerCase()] = true; }

  if (!categories.length) {
    elCats.innerHTML = '<span class="empty">No categories reported by the leaf node yet.</span>';
    return;
  }

  elCats.innerHTML = '';
  for (var j = 0; j < categories.length; j++) {
    var name = categories[j];
    var label = document.createElement('label');
    label.className = 'chk';
    var box = document.createElement('input');
    box.type = 'checkbox';
    box.value = name;
    box.checked = !!lower[name.toLowerCase()];
    box.addEventListener('change', render);
    label.appendChild(box);
    label.appendChild(document.createTextNode(name));
    elCats.appendChild(label);
  }
}

function loadExisting() {
  var hash = window.location.hash.replace('#', '');
  if (!hash) { return []; }
  try {
    var cfg = unb64url(hash);
    if (typeof cfg.buffering === 'boolean') { elBuf.checked = cfg.buffering; }
    if (typeof cfg.quality === 'boolean') { elQual.checked = cfg.quality; }
    if (cfg.sort === 'name' || cfg.sort === 'category') { elSort.value = cfg.sort; }
    return Array.isArray(cfg.hidden) ? cfg.hidden : [];
  } catch (e) {
    return [];
  }
}

elBuf.checked = CFG.showBuffering;
elBuf.addEventListener('change', render);
elQual.addEventListener('change', render);
elSort.addEventListener('change', render);

document.getElementById('copy').addEventListener('click', function () {
  elUrl.select();
  var ok = false;
  try { ok = document.execCommand('copy'); } catch (e) { ok = false; }
  if (navigator.clipboard) { navigator.clipboard.writeText(elUrl.value); ok = true; }
  elStatus.textContent = ok ? 'Copied.' : 'Select the URL and copy it manually.';
  setTimeout(function () { elStatus.textContent = ''; }, 2500);
});

var preHidden = loadExisting();

fetch('/categories.json')
  .then(function (r) { return r.json(); })
  .then(function (data) {
    categories = (data && data.categories) || [];
    var count = (data && data.channels) || 0;
    document.getElementById('count').textContent =
      count + (count === 1 ? ' channel' : ' channels') + ' live now';
    renderCategories(preHidden);
    render();
  })
  .catch(function () {
    document.getElementById('count').textContent = 'Leaf node unreachable';
    renderCategories(preHidden);
    render();
  });
</script>
</body>
</html>
"""
