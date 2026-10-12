// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// About: who made it, which version runs, the licence text and the notices of everything vendored into the UI.
import { html, useState, useAsync } from '../core/h.js';
import { api } from '../api/client.js';
import { Btn, Badge, Card, Kv, Loading, ErrorBox, PageHead } from '../ui/basic.js';
import { Table } from '../ui/data.js';
import { BrandMark } from '../ui/icons.js';
import { fmtDuration } from '../core/util.js';

/** Read the "Libraries" table of vendor/VENDOR.md into rows. Plain text only; nothing from the file is ever rendered as HTML. */
function parseVendor(md) {
  const rows = [];
  let inLibs = false;
  for (const line of md.split('\n')) {
    if (line.startsWith('## ')) { inLibs = /^##\s+Libraries/i.test(line); continue; }
    if (!inLibs || !line.startsWith('|')) continue;
    const c = line.replace(/^\||\|$/g, '').split('|').map((x) => x.trim().replace(/`/g, ''));
    if (c.length < 5 || c[0] === 'Library' || /^-+$/.test(c[0])) continue;
    rows.push({ id: c[0], name: c[0], version: c[1], licence: c[2], source: c[3], notes: c[4] });
  }
  return rows;
}
const LICENCE_FILES = { codemirror: ['codemirror/LICENSE'], fonts: ['fonts/LICENSE-jetbrains-mono.txt', 'fonts/LICENSE-schibsted-grotesk.txt'], htm: ['htm/LICENSE'], preact: ['preact/LICENSE'], 'swagger-ui': ['swagger-ui/LICENSE', 'swagger-ui/NOTICE'] };

function LicenceText({ path, title }) {
  const [open, setOpen] = useState(false);
  const t = useAsync(() => (open ? fetch(path).then((r) => (r.ok ? r.text() : Promise.reject(new Error(`${r.status}`)))) : Promise.resolve(null)), [open, path]);
  return html`<div><button class="linkbtn" aria-expanded=${open} onClick=${() => setOpen(!open)}>${open ? 'Hide' : 'Show'} ${title}</button>
    ${open ? (t.loading ? html`<${Loading} />` : t.error ? html`<p class="small muted">The text could not be loaded.</p>` : html`<pre class="licence pre mt-2" tabindex="0" aria-label=${title}>${t.data}</pre>`) : null}</div>`;
}

export function AboutPage() {
  const info = useAsync(() => api.get('/admin/info').catch(() => fetch('/v1/health').then((r) => r.json())), []);
  const vendor = useAsync(() => fetch('/vendor/VENDOR.md').then((r) => (r.ok ? r.text() : '')).then(parseVendor).catch(() => []), []);
  const i = info.data || {};
  return html`<div class="page" data-testid="about"><${PageHead} icon="info" title="About" sub="Open Lakehouse Compute Fabric runs Spark and SQL on your own hardware, and plugs into Microsoft Fabric." />
    <div class="about-hero"><${BrandMark} size=${72} /><div><h2>${i.product || 'Open Lakehouse Compute Fabric'}</h2>
      <p class="mt-1">Version <b class="mono" data-testid="about-version">${i.version || '…'}</b></p><p class="muted mt-1">Created by <b data-testid="about-author">Justin Dodrill</b>. Free software under the Apache License 2.0.</p></div></div>
    <div class="grid-2 mt-4"><${Card} title="This installation">${info.loading ? html`<${Loading} />` : html`<${Kv} items=${[['Version', html`<span class="mono">${i.version}</span>`], i.node && ['Node', i.node], i.python && ['Python', i.python], i.platform && ['Platform', i.platform], i.database && ['Database', i.database.dialect],
        i.uptime_seconds != null && ['Uptime', fmtDuration(i.uptime_seconds)], i.modules && ['Modules', i.modules.length]]} />`}
        <div class="row mt-3"><a class="btn" href="/v1/docs" target="_blank" rel="noopener"><span>API reference</span></a><a class="btn" href="#/agents">AI agents (MCP)</a></div></${Card}>
      <${Card} title="Licence" actions=${html`<${Badge} kind="ok">Apache-2.0<//>`}><p class="small">Copyright 2026 Justin Dodrill. Licensed under the Apache License, Version 2.0. You may use, modify and distribute it, including commercially, under the terms of the licence. It comes without warranty.</p>
        <div class="mt-2"><${LicenceText} path="/LICENSE.txt" title="the full licence text" /></div></${Card}></div>
    <section class="mt-6" aria-labelledby="third-h"><h2 id="third-h" class="h3">Third-party notices</h2><p class="muted small mt-1 mb-2">The interface bundles these libraries, served from this server. Nothing is loaded from the internet.</p>
      ${vendor.loading ? html`<${Loading} />` : html`<${Table} caption="Vendored libraries" rows=${vendor.data || []} empty=${html`<p class="muted">The vendor list is not available.</p>`} columns=${[
      { key: 'name', label: 'Library', render: (r) => html`<b>${r.name}</b>` }, { key: 'version', label: 'Version', render: (r) => html`<span class="mono">${r.version}</span>` },
      { key: 'licence', label: 'Licence', render: (r) => html`<${Badge}>${r.licence}<//>` }, { key: 'notes', label: 'Used for', sortable: false, render: (r) => html`<span class="small">${r.notes}</span>` },
      { key: 'texts', label: 'Licence text', sortable: false, render: (r) => html`<span class="col">${(LICENCE_FILES[r.name] || []).map((f) => html`<a key=${f} class="small" href=${`/vendor/${f}`} target="_blank" rel="noopener">${f.split('/').pop()}</a>`)}</span>` },
    ]} />`}</section></div>`;
}
