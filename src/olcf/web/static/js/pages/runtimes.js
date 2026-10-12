// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Runtimes: catalog with per-node install state, install/verify/repair/retire/rollback as tracked operations, compatibility matrix, offline bundles.
import { html, useState, useAsync, useMemo, useRef } from '../core/h.js';
import { api, can } from '../api/client.js';
import { PageHead, Card, Btn, Badge, StatusBadge, Light, Banner, Empty, ErrorBox, Loading, Tabs, TabPanel, Check, Select, Textarea, Input, Kv, Progress, Async } from '../ui/basic.js';
import { Icon } from '../ui/icons.js';
import { Table } from '../ui/data.js';
import { Dialog, MenuButton, openOverlay, confirmDialog, act, toast } from '../ui/overlay.js';
import { openOperation } from './common.js';
import { fmtBytes, ago, titleCase, shortId } from '../core/util.js';
import { prefs } from '../core/store.js';

const MODE_LABEL = { install: 'Install', verify: 'Verify', repair: 'Repair', retire: 'Retire', rollback: 'Roll back' };
const MODE_PAST = { install: 'installed', verify: 'verified', repair: 'repaired', retire: 'retired', rollback: 'rolled back' };

export async function runOperation(runtime, mode, body = {}) {
  const r = await act(() => api.post(`/runtimes/${encodeURIComponent(runtime.id)}/${mode}`, body), { fail: `${MODE_LABEL[mode]} could not start` });
  if (!r) return null;
  return openOperation(r.operation_id, `${MODE_LABEL[mode]} ${runtime.id} on ${r.targets.length} node${r.targets.length === 1 ? '' : 's'}`);
}

function InstallDialog({ runtime, workers, bundles, close }) {
  const online = workers.filter((w) => w.state === 'online' || w.state === 'degraded');
  const have = new Set((runtime.nodes || []).filter((n) => n.state === 'ready').map((n) => n.worker_id));
  const [sel, setSel] = useState(() => new Set(online.filter((w) => !have.has(w.id)).map((w) => w.id)));
  const [bundle, setBundle] = useState('');
  const own = bundles.filter((b) => !b.runtime_id || b.runtime_id === runtime.id);
  const toggle = (id) => setSel((s) => { const n = new Set(s); if (n.has(id)) n.delete(id); else n.add(id); return n; });
  return html`<${Dialog} title=${`Install ${runtime.id}`} onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" icon="download" disabled=${!sel.size} onClick=${() => close({ worker_ids: [...sel], bundle_id: bundle || undefined })}>Install on ${sel.size} node${sel.size === 1 ? '' : 's'}<//>`}>
    <p class="muted">Each node downloads the pinned, hash-checked packages, builds the environment next to the active one and verifies it before switching. A failure leaves the previous state untouched.</p>
    ${online.length ? html`<fieldset class="fieldset"><legend>Nodes</legend><div class="col gap-1">${online.map((w) => html`<${Check} key=${w.id} checked=${sel.has(w.id)} onChange=${() => toggle(w.id)} label=${html`${w.name} <span class="muted small">${have.has(w.id) ? 'already installed' : w.pool}</span>`} />`)}</div></fieldset>` : html`<${Banner} kind="warn" title="No node is online" remediation="Add a machine on the Compute page first." />`}
    ${own.length ? html`<${Select} label="Offline bundle (optional)" value=${bundle} onChange=${setBundle} placeholder="Download from the package index" options=${own.map((b) => ({ value: b.id, label: `${b.filename} (${fmtBytes(b.size)})` }))} hint="Use a bundle for air-gapped nodes." />` : null}</${Dialog}>`;
}

function RegisterDialog({ close }) {
  const [text, setText] = useState('');
  const [err, setErr] = useState(null);
  const [busy, setBusy] = useState(false);
  async function go() {
    setErr(null);
    let manifest;
    try { manifest = JSON.parse(text); } catch (e) { setErr({ message: 'This is not valid JSON: ' + e.message }); return; }
    setBusy(true);
    const r = await act(() => api.post('/runtimes', { manifest }), { ok: (x) => `Registered ${x.id}` });
    setBusy(false);
    if (r) close(r);
  }
  const onFile = async (e) => { const f = e.target.files && e.target.files[0]; if (f) setText(await f.text()); };
  return html`<${Dialog} title="Register a runtime" size="wide" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!text.trim()} onClick=${go}>Register<//>`}>
    <p class="muted">A runtime manifest pins Spark, Delta, Java, Scala and Python and every package by hash. Signed manifests are verified against the trusted keys; unsigned ones need the setting <code>compute.allow_unsigned_runtimes</code>.</p>
    <div class="field"><label for="rt-file">Load from a file</label><input id="rt-file" type="file" accept="application/json,.json" onChange=${onFile} /></div>
    <${Textarea} label="Manifest (JSON)" mono=${true} rows=${12} value=${text} onInput=${setText} error=${err && err.message} />
  </${Dialog}>`;
}

function RuntimeCard({ rt, workers, bundles, reload, canManage }) {
  const [open, setOpen] = useState(false);
  const nodes = rt.nodes || [];
  const ready = nodes.filter((n) => n.state === 'ready' && n.verified).length;
  const failed = nodes.filter((n) => n.state === 'failed').length;
  const light = rt.state === 'retired' ? 'unknown' : failed ? 'down' : ready ? 'ok' : 'degraded';
  const builtin = !!rt.builtin; // olcf-host-python: the worker's own Python interpreter, no Spark
  async function install() {
    const body = await openOverlay(({ close }) => html`<${InstallDialog} runtime=${rt} workers=${workers} bundles=${bundles} close=${close} />`);
    if (body) { await runOperation(rt, 'install', body); reload(); }
  }
  async function op(mode) {
    if (mode === 'retire') {
      const ok = await confirmDialog({ title: `Retire ${rt.id}?`, danger: true, confirmLabel: 'Retire', body: 'Nodes stop offering this runtime and new jobs will not be placed on it. Running work is not interrupted; the retire is refused while jobs use it. You can roll back later.' });
      if (!ok) return;
    }
    await runOperation(rt, mode, {});
    reload();
  }
  return html`<article class=${'rt-card' + (rt.state === 'retired' ? ' retired' : '')} aria-label=${`Runtime ${rt.id}`}>
    <header class="rt-head"><${Light} state=${light} size="lg" glyph=${true} label=${`${rt.id}: ${ready} of ${nodes.length} nodes ready`} /><div class="grow"><h3>${rt.title || rt.id}</h3><div class="small muted mono">${rt.id}</div></div>
      <div class="rt-actions">${builtin ? html`<${Badge} kind="ok" title="Part of every worker: nothing to install, verify or retire">Built in<//>` : canManage ? html`<${Btn} size="sm" kind="primary" icon="download" disabled=${rt.state === 'retired'} onClick=${install}>Install<//><${Btn} size="sm" icon="shield-check" disabled=${!nodes.length} onClick=${() => op('verify')}>Verify<//>
        <${MenuButton} label=${`More actions for ${rt.id}`} items=${[{ label: 'Repair', icon: 'wrench', disabled: !nodes.length || rt.state === 'retired', onClick: () => op('repair') }, rt.state === 'retired' ? { label: 'Roll back (make available again)', icon: 'undo', onClick: () => op('rollback') } : { label: 'Retire', icon: 'power', danger: true, disabled: !nodes.length, onClick: () => op('retire') }]} />` : null}</div></header>
    <p class="rt-desc">${rt.description}</p>
    <div class="chips rt-facts"><${Badge} kind="info">${rt.family}<//>${rt.no_spark ? html`<${Badge} kind="warn" title="Runs Python only: Spark, Spark SQL and Delta on Spark need a Spark runtime">No Spark<//>` : null}${rt.spark ? html`<${Badge}>Spark ${rt.spark}<//>` : null}${rt.delta ? html`<${Badge}>Delta ${rt.delta}<//>` : null}${rt.iceberg ? html`<${Badge}>Iceberg ${rt.iceberg}<//>` : null}${rt.java ? html`<${Badge}>Java ${rt.java}<//>` : null}${rt.scala ? html`<${Badge}>Scala ${rt.scala}<//>` : null}${rt.python ? html`<${Badge}>Python ${rt.python}<//>` : null}
      ${builtin ? null : rt.signed ? html`<${Badge} kind="ok"><${Icon} name="shield-check" size="sm" /> Signed<//>` : html`<${Badge} kind="warn" title="The manifest carries no trusted signature">Unsigned<//>`}${rt.state === 'retired' ? html`<${Badge} kind="down">Retired<//>` : null}</div>
    <div class="rt-nodes"><button class="linklike" aria-expanded=${open} onClick=${() => setOpen(!open)}><${Icon} name=${open ? 'chevron-down' : 'chevron-right'} size="sm" /> ${nodes.length ? `${builtin ? 'Available on' : 'Installed on'} ${ready} of ${nodes.length} node${nodes.length === 1 ? '' : 's'}${failed ? `, ${failed} failed` : ''}` : (builtin ? 'Not offered by any online node yet' : 'Not installed on any node')}</button>
      ${open && nodes.length ? html`<${Table} dense=${true} columns=${[{ key: 'worker', label: 'Node' }, { key: 'state', label: 'State', render: (n) => html`<${StatusBadge} state=${n.state} />` }, { key: 'verified', label: 'Verified', render: (n) => (n.verified ? 'Yes' : 'No'), sort: (n) => (n.verified ? 1 : 0) }, { key: 'fingerprint', label: 'Fingerprint', render: (n) => html`<code title=${n.fingerprint}>${String(n.fingerprint || '').slice(0, 23)}</code>` }, { key: 'detail', label: 'Detail', render: (n) => n.detail || '' }, { key: 'updated_at', label: 'Updated', render: (n) => ago(n.updated_at) }]} rows=${nodes} rowKey=${(n) => n.worker_id} />` : null}
      ${failed && open ? html`<div class="mt-2"><${Banner} kind="warn" title="Some nodes failed verification" remediation="Run Repair to re-resolve the pinned packages. If the message says the signing key is untrusted, start the agent with --runtime-trust-dir pointing at your trusted public keys." /></div>` : null}</div>
  </article>`;
}

function Matrix() {
  const m = useAsync(() => api.get('/runtimes/matrix'), []);
  return html`<${Async} state=${m}>${(d) => html`<div class="col gap-4">
    <${Card} title="Runtime families" flush=${true}><${Table} caption="Runtime families and component versions" columns=${[
      { key: 'family', label: 'Family', render: (f) => html`<b>${f.family}</b>${f.fabric_interop ? html` <${Badge} kind="info">Fabric interop<//>` : null}` },
      { key: 'spark', label: 'Spark', render: (f) => f.spark.join(', ') }, { key: 'delta', label: 'Delta', render: (f) => f.delta.join(', ') || '–' }, { key: 'iceberg', label: 'Iceberg', render: (f) => f.iceberg.join(', ') || '–' },
      { key: 'java', label: 'Java', render: (f) => f.java.join(', ') }, { key: 'scala', label: 'Scala', render: (f) => f.scala.join(', ') }, { key: 'python', label: 'Python', render: (f) => f.python.join(', ') },
      { key: 'rt', label: 'Runtimes', sortable: false, render: (f) => html`<div class="chips">${f.runtimes.map((r) => html`<span class="badge" key=${r.id} title=${`${r.ready_nodes} node(s) ready`}><${Light} state=${r.state === 'retired' ? 'unknown' : r.ready_nodes ? 'ok' : 'degraded'} label="" />${r.version}</span>`)}</div>` },
    ]} rows=${d.families} rowKey=${(f) => f.family} /></${Card}>
    <${Card} title="Delta table features by family" sub="Features that affect readers can make tables unreadable in older engines" flush=${true}>
      <div class="table-wrap"><table class="table dense matrix-table"><caption class="sr-only">Delta features supported by each runtime family</caption><thead><tr><th scope="col">Feature</th>${d.families.map((f) => html`<th scope="col" key=${f.family}>${f.family}</th>`)}</tr></thead>
      <tbody>${d.delta_features.map((x) => html`<tr key=${x.feature}><th scope="row">${x.feature}${x.affects_readers ? html` <${Badge} kind="warn" title="Readers must support this feature">reader<//>` : null}</th>${d.families.map((f) => html`<td key=${f.family}>${x.families[f.family] ? html`<span class="yes"><${Icon} name="check" size="sm" /><span class="sr-only">Supported</span></span>` : html`<span class="muted" aria-label="Not supported">–</span>`}</td>`)}</tr>`)}</tbody></table></div></${Card}>
    <p class="small muted">${d.note}</p></div>`}</${Async}>`;
}

function Bundles({ canManage }) {
  const b = useAsync(() => (can('runtime.manage') ? api.get('/runtimes/bundles') : { items: [] }), []);
  const rts = useAsync(() => api.get('/runtimes'), []);
  const [up, setUp] = useState(null);
  const [runtime, setRuntime] = useState('');
  const file = useRef();
  async function upload(e) {
    const f = e.target.files && e.target.files[0];
    if (!f) return;
    setUp(0);
    const r = await act(() => api.upload('/runtimes/bundles', f, { query: runtime ? { runtime_id: runtime } : undefined, onProgress: setUp }), { ok: 'Bundle uploaded' });
    setUp(null); e.target.value = '';
    if (r) b.reload();
  }
  async function del(x) {
    if (!(await confirmDialog({ title: `Delete ${x.filename}?`, danger: true, confirmLabel: 'Delete', body: 'Nodes can no longer install from this bundle.' }))) return;
    if (await act(() => api.del(`/runtimes/bundles/${encodeURIComponent(x.id)}`), { ok: 'Bundle deleted' }) !== undefined) b.reload();
  }
  return html`<${Card} title="Offline bundles" sub="For machines without internet access" actions=${canManage ? html`<div class="row"><${Select} size="sm" value=${runtime} onChange=${setRuntime} placeholder="Any runtime" options=${(rts.data ? rts.data.items : []).map((r) => r.id)} aria-label="Runtime the bundle belongs to" /><input ref=${file} type="file" hidden onChange=${upload} aria-label="Bundle file" /><${Btn} size="sm" icon="upload" busy=${up != null} onClick=${() => file.current.click()}>${up != null ? `Uploading ${Math.round(up * 100)}%` : 'Upload bundle'}<//></div>` : null} flush=${true}>
    <${Async} state=${b}>${(d) => d.items.length ? html`<${Table} columns=${[{ key: 'filename', label: 'File' }, { key: 'runtime_id', label: 'Runtime', render: (x) => x.runtime_id || 'any' }, { key: 'size', label: 'Size', num: true, render: (x) => fmtBytes(x.size) }, { key: 'sha256', label: 'SHA-256', render: (x) => html`<code title=${x.sha256}>${x.sha256.slice(0, 16)}</code>` }, { key: 'created_at', label: 'Uploaded', render: (x) => ago(x.created_at) }, { key: 'a', label: '', sortable: false, render: (x) => canManage ? html`<${Btn} size="sm" kind="ghost" iconOnly icon="trash" label=${`Delete ${x.filename}`} onClick=${() => del(x)} />` : null }]} rows=${d.items} />` : html`<${Empty} icon="package" title="No offline bundles">Build one with the OLCF CLI on a connected machine and upload it here; nodes then install from this control plane.<//>`}<//></${Card}>`;
}

export function RuntimesPanel() {
  const rts = useAsync(() => api.get('/runtimes'), []);
  const workers = useAsync(() => api.get('/workers').then((r) => r.items), []);
  const bundles = useAsync(() => (can('runtime.manage') ? api.get('/runtimes/bundles').then((r) => r.items) : []), []);
  const [sub, setSub] = useState(prefs.get('rt.tab', 'catalog'));
  const canManage = can('runtime.manage');
  const [showRetired, setShowRetired] = useState(false);
  const setTab = (t) => { setSub(t); prefs.set('rt.tab', t); };
  const items = (rts.data ? rts.data.items : []).filter((r) => showRetired || r.state !== 'retired');
  async function register() { const r = await openOverlay(({ close }) => html`<${RegisterDialog} close=${close} />`); if (r) rts.reloadLoud(); }
  return html`<div class="col gap-4">
    <div class="row-wrap"><${Tabs} label="Runtime views" value=${sub} onChange=${setTab} tabs=${[{ id: 'catalog', label: 'Catalog', count: rts.data ? rts.data.items.length : null }, { id: 'matrix', label: 'Compatibility' }, { id: 'bundles', label: 'Offline bundles' }]} />
      <span class="grow"></span>${sub === 'catalog' ? html`<${Check} label="Show retired" checked=${showRetired} onChange=${setShowRetired} />${canManage ? html`<${Btn} size="sm" icon="plus" onClick=${register}>Register runtime<//>` : null}` : null}</div>
    <${TabPanel} id="catalog" value=${sub}><${Async} state=${rts}>${() => items.length ? html`<div class="rt-grid">${items.map((r) => html`<${RuntimeCard} key=${r.id} rt=${r} workers=${workers.data || []} bundles=${bundles.data || []} reload=${rts.reload} canManage=${canManage} />`)}</div>` : html`<${Empty} icon="layers" title="No runtimes registered">Register a runtime manifest to run Spark jobs and notebooks.<//>`}<//></${TabPanel}>
    <${TabPanel} id="matrix" value=${sub}><${Matrix} /></${TabPanel}>
    <${TabPanel} id="bundles" value=${sub}><${Bundles} canManage=${canManage} /></${TabPanel}></div>`;
}

export function RuntimesPage() {
  return html`<div class="page"><${PageHead} icon="layers" title="Runtimes" sub="Pinned Spark, Delta and Python environments, installed and verified on each node" /><${RuntimesPanel} /></div>`;
}
