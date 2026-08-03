"""
panel.py - HTML for the ClearTV leaf admin panel.

Kept apart from admin.py so the handlers stay readable. Two pages: a password
prompt and the panel itself.

Provider channel names are untrusted input — they come from a third-party API —
so the panel's JavaScript builds every row with createElement + textContent and
never with innerHTML. A channel called `<img onerror=...>` is then just text.
"""

from __future__ import annotations

_CSS = """
  :root {
    --bg: #0b0f14;
    --panel: #141a22;
    --panel-2: #1b232d;
    --line: #26313d;
    --text: #e8eef5;
    --muted: #8fa1b3;
    --accent: #37d7a0;
    --accent-dark: #1fa87a;
    --danger: #e2564d;
    --warn: #e0a53b;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font: 14.5px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    padding: 26px 16px 70px;
  }
  .wrap { max-width: 1080px; margin: 0 auto; }
  h1 { margin: 0; font-size: 25px; letter-spacing: -0.3px; }
  h1 span { color: var(--accent); }
  h2 {
    margin: 0 0 3px;
    font-size: 13px;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: var(--accent);
  }
  .hint { color: var(--muted); font-size: 12.5px; margin: 0 0 14px; }
  section {
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 12px;
    padding: 18px;
    margin-bottom: 16px;
  }
"""
_CSS += """
  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 12px;
    margin-bottom: 20px;
  }
  .stats { color: var(--muted); font-size: 12.5px; margin-top: 4px; }
  .stats b { color: var(--text); font-weight: 600; }
  table { width: 100%; border-collapse: collapse; }
  th {
    text-align: left;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    color: var(--muted);
    padding: 0 10px 8px;
    font-weight: 600;
  }
  td { padding: 9px 10px; border-top: 1px solid var(--line); vertical-align: middle; }
  tr.off td { opacity: 0.55; }
  .name { font-weight: 600; }
  .sub { color: var(--muted); font-size: 12px; margin-top: 2px; }
  .pill {
    display: inline-block;
    font-size: 11px;
    padding: 2px 8px;
    border-radius: 20px;
    border: 1px solid var(--line);
    background: var(--panel-2);
    color: var(--muted);
  }
  .pill.live { color: var(--accent); border-color: var(--accent-dark); }
  .pill.off { color: var(--muted); }
  .pill.err { color: var(--danger); border-color: var(--danger); }
  input, select {
    background: var(--panel-2);
    color: var(--text);
    border: 1px solid var(--line);
    border-radius: 7px;
    padding: 9px 11px;
    font-size: 14px;
    font-family: inherit;
    width: 100%;
  }
  input:focus, select:focus { outline: none; border-color: var(--accent-dark); }
  label.f { display: block; margin-bottom: 12px; }
  label.f span { display: block; font-size: 12px; color: var(--muted); margin-bottom: 5px; }
  .row2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .row3 { display: grid; grid-template-columns: 2fr 1fr 1fr; gap: 12px; }
"""
_CSS += """
  button {
    border: none;
    border-radius: 7px;
    padding: 9px 15px;
    font-size: 13.5px;
    font-weight: 600;
    cursor: pointer;
    font-family: inherit;
  }
  button:disabled { opacity: 0.5; cursor: not-allowed; }
  .primary { background: var(--accent); color: #06231a; }
  .primary:hover:enabled { background: var(--accent-dark); color: #fff; }
  .ghost { background: var(--panel-2); color: var(--text); border: 1px solid var(--line); }
  .ghost:hover:enabled { border-color: var(--accent-dark); }
  .stop { background: var(--warn); color: #241a05; }
  .stop:hover:enabled { filter: brightness(1.1); }
  .del { background: transparent; color: var(--danger); border: 1px solid var(--line); }
  .del:hover:enabled { border-color: var(--danger); }
  .acts { display: flex; gap: 6px; justify-content: flex-end; }
  .acts button { padding: 6px 11px; font-size: 12.5px; }
  .empty { color: var(--muted); font-size: 13px; padding: 14px 10px; }
  .results { max-height: 340px; overflow-y: auto; margin-top: 12px; }
  .results table { font-size: 13.5px; }
  #toast {
    position: fixed;
    left: 50%;
    bottom: 22px;
    transform: translateX(-50%);
    background: var(--panel-2);
    border: 1px solid var(--line);
    border-radius: 9px;
    padding: 11px 18px;
    font-size: 13.5px;
    max-width: 90vw;
    opacity: 0;
    pointer-events: none;
    transition: opacity 0.18s;
    z-index: 50;
  }
  #toast.show { opacity: 1; }
  #toast.ok { border-color: var(--accent-dark); }
  #toast.bad { border-color: var(--danger); }
  .login { max-width: 340px; margin: 14vh auto 0; text-align: center; }
  .login h1 { margin-bottom: 22px; font-size: 30px; }
  .login button { width: 100%; padding: 12px; margin-top: 4px; }
  .err { color: var(--danger); font-size: 13px; min-height: 19px; margin-top: 10px; }
  a.logout { color: var(--muted); font-size: 12.5px; text-decoration: none; }
  a.logout:hover { color: var(--accent); }
  code { font-family: ui-monospace, Menlo, monospace; font-size: 12px; color: var(--muted); }
"""

_LOGIN = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ClearTV Leaf</title>
<style>__CSS__</style>
</head>
<body>
<div class="login">
  <h1>Clear<span>TV</span></h1>
  <form id="f">
    <input type="password" id="pw" placeholder="Admin password" autofocus autocomplete="current-password">
    <button class="primary" type="submit">Sign in</button>
  </form>
  <div class="err" id="err"></div>
</div>
<script>
document.getElementById('f').addEventListener('submit', function (ev) {
  ev.preventDefault();
  var err = document.getElementById('err');
  err.textContent = '';
  fetch('/admin/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ password: document.getElementById('pw').value })
  }).then(function (r) {
    return r.json().then(function (d) { return { ok: r.ok, d: d }; });
  }).then(function (res) {
    if (res.ok) { window.location = '/admin/panel'; }
    else { err.textContent = res.d.error || 'Sign in failed'; }
  }).catch(function () { err.textContent = 'Network error'; });
});
</script>
</body>
</html>
"""


def login_html() -> str:
    return _LOGIN.replace("__CSS__", _CSS)


_PANEL = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ClearTV Leaf · Admin</title>
<style>__CSS__</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <h1>Clear<span>TV</span> leaf</h1>
      <div class="stats" id="stats">Loading…</div>
    </div>
    <a class="logout" href="/admin/logout">Sign out</a>
  </header>

  <section>
    <h2>Channels</h2>
    <p class="hint">What the addon is serving right now. Stop a channel to pull it
      from the catalog and free its provider account.</p>
    <table>
      <thead>
        <tr>
          <th>Channel</th>
          <th>Category</th>
          <th>Account</th>
          <th>Status</th>
          <th></th>
        </tr>
      </thead>
      <tbody id="lineup"></tbody>
    </table>
    <div class="empty" id="lineup-empty">Loading…</div>
  </section>

  <section>
    <h2>Add a channel</h2>
    <p class="hint">Search a provider's channel list, then add it. The stream is
      checked before it goes into the catalog.</p>
    <div class="row3">
      <label class="f"><span>Provider account</span>
        <select id="browse-account"></select>
      </label>
      <label class="f"><span>Search</span>
        <input id="browse-search" placeholder="sky sports, bbc…">
      </label>
      <label class="f"><span>&nbsp;</span>
        <button class="ghost" id="browse-btn" style="width:100%">Search</button>
      </label>
    </div>
    <div class="results" id="results"></div>
  </section>
<!--ACCOUNTS-->
</div>
<div id="toast"></div>
<!--SCRIPT-->
</body>
</html>
"""
_ACCOUNTS = """
  <section>
    <h2>Provider accounts</h2>
    <p class="hint">Each account can serve one channel at a time — that is the
      provider's connection limit, not a ClearTV one.</p>
    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th>Server</th>
          <th>Status</th>
          <th>In use by</th>
          <th></th>
        </tr>
      </thead>
      <tbody id="accounts"></tbody>
    </table>
    <div class="empty" id="accounts-empty">Loading…</div>

    <div style="margin-top:18px;padding-top:16px;border-top:1px solid var(--line)">
      <div class="row2">
        <label class="f"><span>Label</span>
          <input id="acc-name" placeholder="Provider 1">
        </label>
        <label class="f"><span>Server</span>
          <input id="acc-server" placeholder="http://provider.example:8080">
        </label>
      </div>
      <div class="row2">
        <label class="f"><span>Username</span>
          <input id="acc-user" autocomplete="off">
        </label>
        <label class="f"><span>Password</span>
          <input id="acc-pass" type="password" autocomplete="new-password">
        </label>
      </div>
      <button class="primary" id="acc-add">Add account</button>
    </div>
  </section>
"""

_SCRIPT = """<script>
// ── helpers ──────────────────────────────────────────────────────────────────

function api(path, opts) {
  opts = opts || {};
  opts.headers = opts.headers || {};
  if (opts.body) { opts.headers['Content-Type'] = 'application/json'; }
  return fetch(path, opts).then(function (r) {
    return r.json().catch(function () { return {}; }).then(function (d) {
      if (!r.ok) { throw new Error(d.error || ('HTTP ' + r.status)); }
      return d;
    });
  });
}

var toastTimer = null;
function toast(msg, kind) {
  var el = document.getElementById('toast');
  el.textContent = msg;
  el.className = 'show ' + (kind || '');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(function () { el.className = ''; }, 4000);
}

// Always textContent — provider channel names are third-party input.
function cell(text, cls) {
  var td = document.createElement('td');
  if (cls) { td.className = cls; }
  td.textContent = (text === null || text === undefined) ? '' : String(text);
  return td;
}

function pill(text, kind) {
  var span = document.createElement('span');
  span.className = 'pill ' + (kind || '');
  span.textContent = text;
  return span;
}

function btn(label, cls, onClick) {
  var b = document.createElement('button');
  b.className = cls;
  b.textContent = label;
  b.addEventListener('click', onClick);
  return b;
}

function uptime(secs) {
  if (!secs || secs < 0) { return ''; }
  var h = Math.floor(secs / 3600), m = Math.floor((secs % 3600) / 60);
  if (h) { return h + 'h ' + m + 'm'; }
  if (m) { return m + 'm'; }
  return secs + 's';
}
// ── lineup ───────────────────────────────────────────────────────────────────

var accountsCache = [];

function renderLineup(data) {
  var tbody = document.getElementById('lineup');
  var empty = document.getElementById('lineup-empty');
  tbody.innerHTML = '';

  var list = data.channels || [];
  if (!list.length) {
    empty.textContent = 'No channels yet. Add one below.';
    empty.style.display = '';
    return;
  }
  empty.style.display = 'none';

  list.forEach(function (ch) {
    var tr = document.createElement('tr');
    if (!ch.enabled) { tr.className = 'off'; }

    var first = document.createElement('td');
    var name = document.createElement('div');
    name.className = 'name';
    name.textContent = ch.display_name;
    var sub = document.createElement('div');
    sub.className = 'sub';
    sub.textContent = ch.slug + '  ·  id ' + ch.stream_id + (ch.quality ? '  ·  ' + ch.quality : '');
    first.appendChild(name);
    first.appendChild(sub);
    tr.appendChild(first);

    tr.appendChild(cell((ch.categories || []).join(', ')));
    tr.appendChild(cell(ch.account_name || ch.account_id || '—'));

    var status = document.createElement('td');
    if (ch.last_error) {
      status.appendChild(pill('error', 'err'));
      var e = document.createElement('div');
      e.className = 'sub';
      e.textContent = ch.last_error;
      status.appendChild(e);
    } else if (ch.enabled) {
      status.appendChild(pill('live', 'live'));
      if (ch.uptime) {
        var u = document.createElement('div');
        u.className = 'sub';
        u.textContent = uptime(ch.uptime);
        status.appendChild(u);
      }
    } else {
      status.appendChild(pill('stopped', 'off'));
    }
    tr.appendChild(status);

    var acts = document.createElement('td');
    var box = document.createElement('div');
    box.className = 'acts';
    if (ch.enabled) {
      box.appendChild(btn('Stop', 'stop', function () { stopChannel(ch.slug); }));
    } else {
      box.appendChild(btn('Start', 'ghost', function () { startChannel(ch.slug); }));
    }
    box.appendChild(btn('Remove', 'del', function () { removeChannel(ch.slug, ch.display_name); }));
    acts.appendChild(box);
    tr.appendChild(acts);

    tbody.appendChild(tr);
  });
}

function renderStats(data) {
  var s = data.stats || {}, c = data.capacity || {};
  var el = document.getElementById('stats');
  el.innerHTML = '';
  var parts = [
    ['live', s.enabled || 0],
    ['stopped', s.stopped || 0],
    ['accounts free', (c.free || 0) + '/' + (c.usable || 0)]
  ];
  parts.forEach(function (p, i) {
    if (i) { el.appendChild(document.createTextNode('  ·  ')); }
    var b = document.createElement('b');
    b.textContent = String(p[1]);
    el.appendChild(b);
    el.appendChild(document.createTextNode(' ' + p[0]));
  });
}

function loadLineup() {
  return api('/admin/api/lineup').then(function (data) {
    renderLineup(data);
    renderStats(data);
  }).catch(function (e) { toast(e.message, 'bad'); });
}
// ── channel actions ──────────────────────────────────────────────────────────

function stopChannel(slug) {
  api('/admin/api/lineup/' + encodeURIComponent(slug) + '/stop', { method: 'POST' })
    .then(function () { toast('Stopped ' + slug + ' — account freed', 'ok'); return loadAll(); })
    .catch(function (e) { toast(e.message, 'bad'); });
}

function startChannel(slug) {
  api('/admin/api/lineup/' + encodeURIComponent(slug) + '/start', {
    method: 'POST', body: JSON.stringify({})
  })
    .then(function () { toast('Started ' + slug, 'ok'); return loadAll(); })
    .catch(function (e) { toast(e.message, 'bad'); });
}

function removeChannel(slug, name) {
  if (!window.confirm('Remove "' + name + '" from the lineup?')) { return; }
  api('/admin/api/lineup/' + encodeURIComponent(slug), { method: 'DELETE' })
    .then(function () { toast('Removed ' + slug, 'ok'); return loadAll(); })
    .catch(function (e) { toast(e.message, 'bad'); });
}

function addChannel(streamId, name, logo, category, button) {
  button.disabled = true;
  button.textContent = 'Checking…';
  api('/admin/api/lineup', {
    method: 'POST',
    body: JSON.stringify({
      stream_id: streamId,
      display_name: name,
      logo: logo || '',
      account_id: document.getElementById('browse-account').value,
      categories: category ? [category] : ['General']
    })
  }).then(function () {
    toast('Added ' + name, 'ok');
    button.textContent = 'Added';
    return loadAll();
  }).catch(function (e) {
    toast(e.message, 'bad');
    button.disabled = false;
    button.textContent = 'Add';
  });
}

// ── provider browser ─────────────────────────────────────────────────────────

function renderResults(data) {
  var box = document.getElementById('results');
  box.innerHTML = '';

  var list = data.channels || [];
  if (!list.length) {
    var none = document.createElement('div');
    none.className = 'empty';
    none.textContent = data.error ? data.error : 'Nothing matched.';
    box.appendChild(none);
    return;
  }

  var head = document.createElement('div');
  head.className = 'hint';
  head.textContent = 'Showing ' + list.length + ' of ' + (data.matched || list.length) +
    ' matches (' + (data.total || 0) + ' channels on this account)';
  box.appendChild(head);

  var table = document.createElement('table');
  var tbody = document.createElement('tbody');

  list.forEach(function (ch) {
    var tr = document.createElement('tr');

    var first = document.createElement('td');
    var nm = document.createElement('div');
    nm.className = 'name';
    nm.textContent = ch.name;
    first.appendChild(nm);
    if (ch.category) {
      var cat = document.createElement('div');
      cat.className = 'sub';
      cat.textContent = ch.category;
      first.appendChild(cat);
    }
    tr.appendChild(first);

    tr.appendChild(cell(ch.stream_id, 'sub'));

    var act = document.createElement('td');
    var box2 = document.createElement('div');
    box2.className = 'acts';
    if (ch.in_lineup) {
      box2.appendChild(pill('in lineup', 'live'));
    } else {
      var add = btn('Add', 'primary', function () {
        addChannel(ch.stream_id, ch.name, ch.logo, ch.category, add);
      });
      box2.appendChild(add);
    }
    act.appendChild(box2);
    tr.appendChild(act);

    tbody.appendChild(tr);
  });

  table.appendChild(tbody);
  box.appendChild(table);
}

function browse(refresh) {
  var accId = document.getElementById('browse-account').value;
  if (!accId) { toast('Add a provider account first', 'bad'); return; }

  var b = document.getElementById('browse-btn');
  b.disabled = true;
  b.textContent = 'Searching…';

  var q = encodeURIComponent(document.getElementById('browse-search').value.trim());
  api('/admin/api/channels?account_id=' + encodeURIComponent(accId) + '&search=' + q +
      (refresh ? '&refresh=1' : ''))
    .then(renderResults)
    .catch(function (e) { toast(e.message, 'bad'); })
    .then(function () { b.disabled = false; b.textContent = 'Search'; });
}
// ── accounts ─────────────────────────────────────────────────────────────────

function expiryText(acc) {
  if (!acc.exp_date) { return acc.status || 'unknown'; }
  var d = new Date(acc.exp_date * 1000);
  var days = Math.round((acc.exp_date * 1000 - Date.now()) / 86400000);
  return d.toISOString().slice(0, 10) + (days >= 0 ? ' (' + days + 'd)' : ' (expired)');
}

function renderAccounts(data) {
  accountsCache = data.accounts || [];

  var tbody = document.getElementById('accounts');
  var empty = document.getElementById('accounts-empty');
  tbody.innerHTML = '';

  if (!accountsCache.length) {
    empty.textContent = 'No accounts yet. Add one below to start picking channels.';
    empty.style.display = '';
  } else {
    empty.style.display = 'none';
    accountsCache.forEach(function (acc) {
      var tr = document.createElement('tr');
      if (!acc.usable) { tr.className = 'off'; }

      var first = document.createElement('td');
      var nm = document.createElement('div');
      nm.className = 'name';
      nm.textContent = acc.name;
      var sub = document.createElement('div');
      sub.className = 'sub';
      sub.textContent = acc.username;
      first.appendChild(nm);
      first.appendChild(sub);
      tr.appendChild(first);

      tr.appendChild(cell(acc.server, 'sub'));

      var st = document.createElement('td');
      st.appendChild(pill(acc.usable ? 'active' : (acc.status || 'unusable'),
                          acc.usable ? 'live' : 'err'));
      var exp = document.createElement('div');
      exp.className = 'sub';
      exp.textContent = expiryText(acc);
      st.appendChild(exp);
      tr.appendChild(st);

      tr.appendChild(cell(acc.current_slug || '—', acc.current_slug ? '' : 'sub'));

      var act = document.createElement('td');
      var box = document.createElement('div');
      box.className = 'acts';
      box.appendChild(btn('Delete', 'del', function () { deleteAccount(acc.id, acc.name); }));
      act.appendChild(box);
      tr.appendChild(act);

      tbody.appendChild(tr);
    });
  }

  // Keep the browser's account picker in sync, preserving the current choice.
  var sel = document.getElementById('browse-account');
  var previous = sel.value;
  sel.innerHTML = '';
  accountsCache.filter(function (a) { return a.usable; }).forEach(function (acc) {
    var opt = document.createElement('option');
    opt.value = acc.id;
    opt.textContent = acc.name + (acc.current_slug ? ' (busy: ' + acc.current_slug + ')' : '');
    sel.appendChild(opt);
  });
  if (previous) { sel.value = previous; }
}

function deleteAccount(id, name) {
  if (!window.confirm('Delete account "' + name + '"?')) { return; }
  api('/admin/api/accounts/' + encodeURIComponent(id), { method: 'DELETE' })
    .then(function () { toast('Deleted ' + name, 'ok'); return loadAll(); })
    .catch(function (e) { toast(e.message, 'bad'); });
}

function addAccount() {
  var b = document.getElementById('acc-add');
  var body = {
    name: document.getElementById('acc-name').value.trim(),
    server: document.getElementById('acc-server').value.trim(),
    username: document.getElementById('acc-user').value.trim(),
    password: document.getElementById('acc-pass').value
  };
  if (!body.name || !body.server || !body.username || !body.password) {
    toast('Fill in all four fields', 'bad');
    return;
  }

  b.disabled = true;
  b.textContent = 'Checking…';
  api('/admin/api/accounts', { method: 'POST', body: JSON.stringify(body) })
    .then(function (d) {
      if (d.info && d.info.error) {
        toast('Saved, but the provider check failed: ' + d.info.error, 'bad');
      } else {
        toast('Added ' + body.name, 'ok');
      }
      ['acc-name', 'acc-server', 'acc-user', 'acc-pass'].forEach(function (id) {
        document.getElementById(id).value = '';
      });
      return loadAll();
    })
    .catch(function (e) { toast(e.message, 'bad'); })
    .then(function () { b.disabled = false; b.textContent = 'Add account'; });
}

function loadAccounts() {
  return api('/admin/api/accounts').then(renderAccounts)
    .catch(function (e) { toast(e.message, 'bad'); });
}

function loadAll() {
  return Promise.all([loadAccounts(), loadLineup()]);
}

// ── wire up ──────────────────────────────────────────────────────────────────

document.getElementById('browse-btn').addEventListener('click', function () { browse(false); });
document.getElementById('browse-search').addEventListener('keydown', function (ev) {
  if (ev.key === 'Enter') { browse(false); }
});
document.getElementById('acc-add').addEventListener('click', addAccount);

loadAll();
// Refresh the lineup periodically so uptime and account state stay current
// without the admin having to reload the page.
setInterval(loadLineup, 15000);
</script>"""


def panel_html() -> str:
    return (
        _PANEL
        .replace("__CSS__", _CSS)
        .replace("<!--ACCOUNTS-->", _ACCOUNTS)
        .replace("<!--SCRIPT-->", _SCRIPT)
    )

