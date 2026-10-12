// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Lakehouse explorer: tables (schema, preview with time travel, history, protocol, maintenance) and files (browse, preview, upload, folders, move, delete).
import { html, useState, useEffect, useMemo, useRef, useAsync } from '../core/h.js';
import { api, can } from '../api/client.js';
import { Btn, Badge, Card, Banner, ErrorBox, Loading, Empty, Tabs, TabPanel, Kv, Steps, Select, Input, Switch, PageHead, Tag } from '../ui/basic.js';
import { Table, VirtualGrid } from '../ui/data.js';
import { Icon } from '../ui/icons.js';
import { act, toast, confirmDialog, promptDialog, openOverlay, Dialog, MenuButton } from '../ui/overlay.js';
import { enc, fmtBytes, fmtNum, fmtTime, ago, jsonPretty, downloadBlob, copyText, fmtMs } from '../core/util.js';
import { useWorkspaces, useQueryState, Ago } from './common.js';
import { navigate } from '../core/util.js';

const fmtType = (t) => (typeof t === 'string' ? t : jsonPretty(t));

function LakehouseTest({ lh, close }) {
  const r = useAsync(() => api.post(`/lakehouses/${enc(lh.id)}/test`, {}), [lh.id]);
  return html`<${Dialog} title=${`Storage test: ${lh.name}`} onClose=${close} footer=${html`<${Btn} onClick=${r.reloadLoud}>Run again<//><${Btn} kind="primary" onClick=${close}>Close<//>`}>
    ${r.loading ? html`<${Loading} label="Probing storage" />` : r.error ? html`<${ErrorBox} error=${r.error} />` : html`<${Banner} kind=${r.data.ok ? 'ok' : 'bad'} title=${r.data.ok ? 'Storage is reachable and writable' : 'Storage check failed'}>Provider ${lh.provider}. Each step ran against the lakehouse root.<//><div class="mt-3"><${Steps} steps=${r.data.steps} /></div>`}</${Dialog}>`;
}

// ---- tables -----------------------------------------------------------------------------------------------------------------------------------
function SchemaTab({ t }) {
  const cols = t.schema || [];
  return html`<${Table} dense caption="Columns" rows=${cols.map((c, i) => ({ ...c, id: c.name, pos: i + 1, part: (t.partition_columns || []).includes(c.name) }))} columns=${[
    { key: 'pos', label: '#', num: true, width: '48px' },
    { key: 'name', label: 'Column', render: (c) => html`<span class="mono">${c.name}</span> ${c.part ? html`<${Badge} kind="info">partition<//>` : null}` },
    { key: 'type', label: 'Type', render: (c) => html`<span class="mono">${fmtType(c.type)}</span>`, sort: (c) => fmtType(c.type) },
    { key: 'nullable', label: 'Nullable', render: (c) => (c.nullable ? 'yes' : 'no') },
  ]} />`;
}

function PreviewTab({ lh, t, history }) {
  const [version, setVersion] = useState('');
  const [rows, setRows] = useState(200);
  const pv = useAsync(() => api.get(`/lakehouses/${enc(lh)}/tables/${enc(t.name)}/preview`, { rows, version: version === '' ? undefined : version }), [lh, t.name, version, rows]);
  const vers = (history || []).map((h) => h.version);
  const d = pv.data;
  return html`<div class="toolbar mb-3">
    <${Select} label="Version (time travel)" size="sm" value=${version} onChange=${setVersion} placeholder=${`Latest (v${t.version})`} options=${vers.map((v) => ({ value: String(v), label: `Version ${v}` }))} />
    <${Select} label="Rows" size="sm" value=${String(rows)} onChange=${(v) => setRows(Number(v))} options=${[50, 200, 1000, 5000].map((n) => ({ value: String(n), label: String(n) }))} />
    <span class="grow"></span>${d ? html`<span class="small muted">${fmtNum(d.row_count)} rows shown${d.truncated ? ', more in the table' : ''} · version ${d.version}</span>` : null}
    <${Btn} size="sm" icon="terminal" onClick=${() => navigate(`/sql?q=${enc(`SELECT * FROM ${t.name} LIMIT 100`)}&lakehouse=${enc(lh)}`)}>Query in SQL editor<//></div>
    ${pv.loading ? html`<${Loading} label="Reading table" />` : pv.error ? html`<${ErrorBox} error=${pv.error} onRetry=${pv.reloadLoud} />` : html`<${VirtualGrid} caption=${`Preview of ${t.name}`} columns=${d.columns.map((c) => ({ name: c.name, type: c.type, numeric: ['integer', 'float', 'decimal'].includes(c.kind) }))} rows=${d.rows} height=${460}
      onCopy=${(v) => { copyText(String(v)); toast.ok('Copied'); }} />`}`;
}

function HistoryTab({ rows }) {
  return html`<${Table} dense caption="Table history" rows=${rows.map((h) => ({ ...h, id: h.version }))} columns=${[
    { key: 'version', label: 'Version', num: true, width: '80px' },
    { key: 'timestamp', label: 'When', render: (h) => fmtTime(h.timestamp), sort: (h) => h.timestamp },
    { key: 'operation', label: 'Operation', render: (h) => html`<${Badge}>${h.operation}<//>` },
    { key: 'rows', label: 'Rows added', num: true, render: (h) => fmtNum(h.operation_metrics && h.operation_metrics.num_added_rows), sort: (h) => (h.operation_metrics && h.operation_metrics.num_added_rows) || 0 },
    { key: 'files', label: 'Files +/-', render: (h) => `${(h.operation_metrics && h.operation_metrics.num_added_files) ?? 0} / ${(h.operation_metrics && h.operation_metrics.num_removed_files) ?? 0}` },
    { key: 'engine', label: 'Engine', render: (h) => html`<span class="mono small">${h.engine || '–'}</span>` },
    { key: 'params', label: 'Parameters', render: (h) => html`<span class="small muted">${Object.entries(h.operation_parameters || {}).map(([k, v]) => `${k}=${typeof v === 'string' ? v : JSON.stringify(v)}`).join(', ')}</span>` },
  ]} />`;
}

function ProtocolTab({ lh, t }) {
  const p = useAsync(() => api.get(`/lakehouses/${enc(lh)}/tables/${enc(t.name)}/protocol`), [lh, t.name]);
  if (p.loading) return html`<${Loading} />`;
  if (p.error) return html`<${ErrorBox} error=${p.error} />`;
  const d = p.data;
  const eng = (side) => Object.entries(d[side] || {}).map(([name, v]) => html`<tr key=${side + name}><td>${side}</td><td class="mono">${name}</td><td><${Badge} kind=${v.allowed ? 'ok' : 'bad'}>${v.allowed ? 'allowed' : 'blocked'}<//></td><td>${(v.findings || []).map((f) => (typeof f === 'string' ? f : f.message || jsonPretty(f))).join('; ') || html`<span class="muted">No findings</span>`}</td></tr>`);
  return html`<${Kv} items=${[['Reader version', d.protocol.min_reader_version], ['Writer version', d.protocol.min_writer_version], ['Reader features', (d.effective_reader_features || []).join(', ') || 'none'], ['Writer features', (d.effective_writer_features || []).join(', ') || 'none'], ['Compatibility profile', html`<span class="mono">${d.profile}</span>`], ['Policy version', html`<span class="mono">${d.policy_version}</span>`]]} />
    <h3 class="mt-4">Engine compatibility</h3><div class="table-wrap"><table class="table dense"><thead><tr><th>Operation</th><th>Engine</th><th>Result</th><th>Findings</th></tr></thead><tbody>${eng('read')}${eng('write')}</tbody></table></div>`;
}

function MaintenanceTab({ lh, t, onDone, canWrite }) {
  const [busy, setBusy] = useState('');
  const [out, setOut] = useState(null);
  const [retention, setRetention] = useState(168);
  async function go(kind, body) {
    setBusy(kind);
    const r = await act(() => api.post(`/lakehouses/${enc(lh)}/tables/${enc(t.name)}/${kind}`, body), { ok: kind === 'vacuum' && body.dry_run ? 'Dry run finished' : `${kind === 'optimize' ? 'Compaction' : 'Vacuum'} finished` });
    setBusy('');
    if (r) { setOut({ kind, r }); if (!(kind === 'vacuum' && body.dry_run)) onDone(); }
  }
  return html`<div class="grid c2">
    <${Card} title="Compact small files" sub="OPTIMIZE"><p class="muted mt-0">Rewrites many small files into fewer large ones. Readers keep working; older versions stay available for time travel.</p>
      <${Btn} icon="layers" busy=${busy === 'optimize'} disabled=${!canWrite} onClick=${() => go('optimize', {})}>Compact now<//></${Card}>
    <${Card} title="Remove unreferenced files" sub="VACUUM"><p class="muted mt-0">Deletes data files no longer referenced by versions inside the retention window. Time travel to removed versions stops working.</p>
      <${Input} label="Retention (hours)" type="number" min="0" value=${retention} onInput=${(v) => setRetention(Number(v))} />
      <div class="row gap-2 mt-3"><${Btn} busy=${busy === 'vacuum-dry'} onClick=${() => go('vacuum', { retention_hours: retention, dry_run: true })}>Dry run<//>
      <${Btn} kind="danger" icon="trash" disabled=${!canWrite} onClick=${async () => { if (await confirmDialog({ title: `Vacuum ${t.name}?`, danger: true, confirmLabel: 'Delete files', body: `Files older than ${retention} hours that are not referenced are deleted for good.` })) go('vacuum', { retention_hours: retention, dry_run: false }); }}>Vacuum<//></div></${Card}>
    ${out ? html`<div style="grid-column:1/-1"><${Card} title=${`Result of ${out.kind}`}><pre class="code" tabindex="0">${jsonPretty(out.r)}</pre></${Card}></div>` : null}</div>`;
}

function TableView({ lh, name, canWrite }) {
  const [tab, setTab] = useQueryState('tt', 'preview');
  const t = useAsync(() => api.get(`/lakehouses/${enc(lh)}/tables/${enc(name)}`), [lh, name]);
  const hist = useAsync(() => api.get(`/lakehouses/${enc(lh)}/tables/${enc(name)}/history`, { limit: 100 }).then((r) => r.items || []).catch(() => []), [lh, name]);
  if (t.loading) return html`<${Loading} label="Reading table metadata" />`;
  if (t.error) return html`<${ErrorBox} error=${t.error} onRetry=${t.reloadLoud} />`;
  const d = t.data;
  const tabs = [{ id: 'preview', label: 'Preview' }, { id: 'schema', label: `Schema (${(d.schema || []).length})` }, { id: 'history', label: `History (${(hist.data || []).length})` }, { id: 'details', label: 'Details' }, { id: 'protocol', label: 'Delta protocol' }, { id: 'maint', label: 'Maintenance' }];
  return html`<div>
    <div class="row gap-3 mb-3 wrap"><h2 class="m-0"><${Icon} name="table" /> ${d.name}</h2><${Badge}>${d.format}<//><${Badge} kind="outline">version ${d.version}<//>${(d.partition_columns || []).length ? html`<${Badge} kind="info">partitioned by ${d.partition_columns.join(', ')}<//>` : null}</div>
    <${Tabs} tabs=${tabs} value=${tab} onChange=${setTab} label="Table sections" />
    <div class="tab-body">
    <${TabPanel} id="preview" value=${tab}><${PreviewTab} lh=${lh} t=${d} history=${hist.data} /></${TabPanel}>
    <${TabPanel} id="schema" value=${tab}><${SchemaTab} t=${d} /></${TabPanel}>
    <${TabPanel} id="history" value=${tab}><${HistoryTab} rows=${hist.data || []} /></${TabPanel}>
    <${TabPanel} id="details" value=${tab}><${Kv} items=${[['Name', d.name], ['Format', d.format], ['Path', html`<span class="mono">${d.path}</span>`], ['Location', html`<span class="mono small">${d.location}</span>`], ['Current version', d.version], ['Partition columns', (d.partition_columns || []).join(', ') || 'none'], ['Partitions', d.partitions ? `${d.partitions.count}` : null], ['Canonical URI', html`<span class="mono small">olcf://…/${d.lakehouse}/${d.path}</span>`]]} /></${TabPanel}>
    <${TabPanel} id="protocol" value=${tab}><${ProtocolTab} lh=${lh} t=${d} /></${TabPanel}>
    <${TabPanel} id="maint" value=${tab}><${MaintenanceTab} lh=${lh} t=${d} canWrite=${canWrite} onDone=${() => { t.reload(); hist.reload(); }} /></${TabPanel}></div></div>`;
}

// ---- files ------------------------------------------------------------------------------------------------------------------------------------
function FilePreview({ lh, path, close }) {
  const p = useAsync(() => api.get(`/lakehouses/${enc(lh)}/files/preview`, { path, rows: 100, bytes: 65536 }), [lh, path]);
  const d = p.data;
  return html`<${Dialog} title=${path} size="lg" onClose=${close} footer=${html`<${Btn} icon="download" onClick=${async () => { const b = await act(() => api.blob(`/lakehouses/${enc(lh)}/files/content`, { path })); if (b) downloadBlob(b, path.split('/').pop()); }}>Download<//><${Btn} kind="primary" onClick=${close}>Close<//>`}>
    ${p.loading ? html`<${Loading} />` : p.error ? html`<${ErrorBox} error=${p.error} />` : html`<div class="small muted mb-2">${d.media_type || ''} · ${fmtBytes(d.size)} · ${d.kind}</div>
      ${d.kind === 'text' ? html`<pre class="code" tabindex="0" style="max-height:60vh;overflow:auto">${d.text}</pre>` : d.columns ? html`<${VirtualGrid} columns=${d.columns.map((c) => (typeof c === 'string' ? { name: c } : c))} rows=${d.rows || []} height=${380} />` : html`<pre class="code" tabindex="0">${jsonPretty(d)}</pre>`}`}</${Dialog}>`;
}

function Files({ lh, canWrite }) {
  const [path, setPath] = useQueryState('fp', 'Files');
  const list = useAsync(() => api.get(`/lakehouses/${enc(lh)}/files`, { path, limit: 500 }), [lh, path]);
  const input = useRef();
  const [up, setUp] = useState(null);
  const parts = path.split('/').filter(Boolean);
  const items = (list.data && list.data.items) || [];
  const sorted = [...items].sort((a, b) => (b.is_dir - a.is_dir) || a.name.localeCompare(b.name));
  async function upload(files) {
    for (const f of files) {
      setUp({ name: f.name, p: 0 });
      try { await api.upload(`/lakehouses/${enc(lh)}/files/upload`, f, { query: { path, overwrite: true }, onProgress: (p) => setUp({ name: f.name, p }) }); toast.ok(`Uploaded ${f.name}`); } catch (e) { toast.bad(`Could not upload ${f.name}`, e.message); }
    }
    setUp(null); list.reload();
  }
  async function mkdir() { const n = await promptDialog({ title: 'New folder', label: 'Name', confirmLabel: 'Create', validate: (v) => (/[\\/]/.test(v) ? 'No slashes' : null) }); if (n && (await act(() => api.post(`/lakehouses/${enc(lh)}/files/mkdir`, { path: `${path}/${n}` }), { ok: 'Folder created' }))) list.reload(); }
  async function del(it) { if (await confirmDialog({ title: `Delete ${it.name}?`, danger: true, confirmLabel: 'Delete', body: it.is_dir ? 'The folder and everything in it are deleted.' : 'The file is deleted.' }) && (await act(() => api.del(`/lakehouses/${enc(lh)}/files`, { path: it.path, recursive: it.is_dir || undefined }), { ok: 'Deleted' })) !== undefined) list.reload(); }
  async function mv(it) { const n = await promptDialog({ title: 'Move or rename', label: 'New path', value: it.path, confirmLabel: 'Move' }); if (n && n !== it.path && (await act(() => api.post(`/lakehouses/${enc(lh)}/files/move`, { src: it.path, dst: n }), { ok: 'Moved' }))) list.reload(); }
  return html`<div>
    <div class="toolbar mb-3"><nav class="row gap-1" aria-label="Path">${parts.map((p, i) => html`<span key=${i} class="row gap-1">${i ? html`<${Icon} name="chevron-right" size="sm" />` : null}<button class="linklike" onClick=${() => setPath(parts.slice(0, i + 1).join('/'))}>${p}</button></span>`)}</nav><span class="grow"></span>
      ${canWrite ? html`<${Btn} size="sm" icon="folder" onClick=${mkdir}>New folder<//><${Btn} size="sm" icon="upload" onClick=${() => input.current.click()}>Upload<//><input ref=${input} type="file" multiple hidden onChange=${(e) => { upload([...e.target.files]); e.target.value = ''; }} />` : null}</div>
    ${up ? html`<div class="banner info mb-3"><div class="grow">Uploading ${up.name}: ${Math.round(up.p * 100)}%</div></div>` : null}
    ${list.loading ? html`<${Loading} />` : list.error ? html`<${ErrorBox} error=${list.error} onRetry=${list.reloadLoud} />` : sorted.length === 0 ? html`<${Empty} icon="folder-open" title="This folder is empty" action=${canWrite ? html`<${Btn} icon="upload" onClick=${() => input.current.click()}>Upload files<//>` : null}>Upload files or create a folder.<//>` : html`
    <${Table} dense caption=${`Contents of ${path}`} rows=${sorted} rowKey=${(r) => r.path} columns=${[
      { key: 'name', label: 'Name', sort: (r) => r.name, render: (r) => r.is_dir ? html`<button class="linklike row gap-2" onClick=${() => setPath(r.path)}><${Icon} name="folder" size="sm" /> ${r.name}</button>` : html`<button class="linklike row gap-2" onClick=${() => openOverlay(({ close }) => html`<${FilePreview} lh=${lh} path=${r.path} close=${close} />`)}><${Icon} name="file" size="sm" /> ${r.name}</button>` },
      { key: 'size', label: 'Size', num: true, render: (r) => (r.is_dir ? '–' : fmtBytes(r.size)), sort: (r) => r.size },
      { key: 'modified', label: 'Modified', render: (r) => html`<${Ago} ts=${r.modified} />`, sort: (r) => r.modified },
      { key: 'act', label: '', sortable: false, width: '48px', render: (r) => html`<${MenuButton} label=${`Actions for ${r.name}`} items=${[!r.is_dir && { label: 'Preview', icon: 'eye', onClick: () => openOverlay(({ close }) => html`<${FilePreview} lh=${lh} path=${r.path} close=${close} />`) }, !r.is_dir && { label: 'Download', icon: 'download', onClick: async () => { const b = await act(() => api.blob(`/lakehouses/${enc(lh)}/files/content`, { path: r.path })); if (b) downloadBlob(b, r.name); } }, { label: 'Copy path', icon: 'copy', onClick: () => copyText(r.path) }, canWrite && { sep: true }, canWrite && { label: 'Move or rename', icon: 'move', onClick: () => mv(r) }, canWrite && { label: 'Delete', icon: 'trash', danger: true, onClick: () => del(r) }]} />` },
    ]} />`}
    ${list.data && list.data.scan_capped ? html`<div class="small muted mt-2">The listing was capped; narrow into a sub folder.</div>` : null}</div>`;
}

// ---- page -------------------------------------------------------------------------------------------------------------------------------------
export function LakehousePage({ params }) {
  const lhs = useAsync(() => api.get('/lakehouses', { limit: 200 }).then((r) => r.items || []), []);
  const [section, setSection] = useQueryState('s', 'tables');
  const [table, setTable] = useQueryState('t', '');
  const [filter, setFilter] = useState('');
  const items = lhs.data || [];
  const id = params.id || (items[0] && items[0].id);
  const lh = items.find((x) => x.id === id);
  const cat = useAsync(() => (id ? api.get('/catalog/tables', { lakehouse_id: id, limit: 500 }).then((r) => r.items || []).catch(() => []) : []), [id]);
  const tabs = useAsync(() => (id ? api.get(`/lakehouses/${enc(id)}/tables`).then((r) => r.items || []) : []), [id]);
  const canWrite = lh ? can('lakehouse.write', lh.workspace_id) : false;
  useEffect(() => { if (tabs.data && tabs.data.length && !table && section === 'tables') setTable(tabs.data[0].name); }, [tabs.data]);
  if (lhs.loading) return html`<${Loading} label="Loading lakehouses" />`;
  if (lhs.error) return html`<div class="page"><${ErrorBox} error=${lhs.error} onRetry=${lhs.reloadLoud} /></div>`;
  if (!items.length) return html`<div class="page"><${PageHead} title="Lakehouse" icon="database" /><${Empty} icon="database" title="No lakehouses yet" action=${html`<${Btn} kind="primary" href="#/">Install the sample lakehouse from Home<//>`}>Create a lakehouse in a workspace, or install the sample to explore.<//></div>`;
  const catBy = Object.fromEntries((cat.data || []).map((c) => [c.name, c]));
  const list = (tabs.data || []).filter((x) => x.name.toLowerCase().includes(filter.toLowerCase()));
  async function refresh() { if (await act(() => api.post(`/lakehouses/${enc(id)}/refresh`, {}), { ok: 'Catalog refreshed' })) { tabs.reload(); cat.reload(); } }
  return html`<div class="page wide">
    <${PageHead} title="Lakehouse" icon="database" sub=${lh ? html`${lh.workspace_name} · <span class="mono">${lh.canonical_uri}</span>` : ''} actions=${html`
      <${Select} label="Lakehouse" size="sm" value=${id} onChange=${(v) => navigate(`/lakehouse/${enc(v)}`)} options=${items.map((x) => ({ value: x.id, label: `${x.name} (${x.workspace_name})` }))} />
      <${Btn} icon="refresh" onClick=${refresh}>Rescan tables<//><${Btn} icon="activity" onClick=${() => openOverlay(({ close }) => html`<${LakehouseTest} lh=${lh} close=${close} />`)}>Test storage<//>`} />
    ${lh ? html`<div class="chips mb-4"><${Badge}>${lh.provider}<//><${Badge}>${lh.default_format}<//><${Badge} kind=${lh.schema_enabled ? 'info' : 'outline'}>${lh.schema_enabled ? 'schemas enabled' : 'no schemas'}<//>${lh.description ? html`<span class="muted small">${lh.description}</span>` : null}</div>` : null}
    <div class="lh-layout">
      <aside class="lh-side card" aria-label="Lakehouse navigation"><div class="card-body flush">
        <div class="seg-row"><button class=${'seg-tab' + (section === 'tables' ? ' on' : '')} aria-pressed=${section === 'tables'} onClick=${() => setSection('tables')}><${Icon} name="table" size="sm" /> Tables <span class="muted">${(tabs.data || []).length}</span></button>
        <button class=${'seg-tab' + (section === 'files' ? ' on' : '')} aria-pressed=${section === 'files'} onClick=${() => setSection('files')}><${Icon} name="folder" size="sm" /> Files</button></div>
        ${section === 'tables' ? html`<div class="p-3"><input class="input sm" placeholder="Filter tables" aria-label="Filter tables" value=${filter} onInput=${(e) => setFilter(e.target.value)} /></div>
          ${tabs.loading ? html`<${Loading} />` : tabs.error ? html`<div class="p-3"><${ErrorBox} error=${tabs.error} /></div>` : html`<ul class="tbl-list" aria-label="Tables">${list.map((x) => { const c = catBy[x.name]; return html`<li key=${x.name}><button class=${table === x.name ? 'on' : ''} aria-current=${table === x.name ? 'true' : undefined} onClick=${() => setTable(x.name)}><span class="row gap-2"><${Icon} name="table" size="sm" /><b>${x.name}</b></span>${c ? html`<span class="small muted trunc">${c.description || ''}</span><span class="chips">${(c.tags || []).slice(0, 3).map((t) => html`<${Tag} key=${t}>${t}<//>`)}</span>` : null}</button></li>`; })}</ul>`}`
          : html`<div class="p-3 small muted">Browse and manage files under <span class="mono">Files/</span> and the raw table folders.</div><ul class="tbl-list"><li><button onClick=${() => { setSection('files'); }}><span class="row gap-2"><${Icon} name="folder" size="sm" /><b>Files</b></span></button></li><li><button onClick=${() => { setSection('files'); setPath('Tables'); }}><span class="row gap-2"><${Icon} name="folder" size="sm" /><b>Tables (raw)</b></span></button></li></ul>`}
      </div></aside>
      <main class="lh-main">${section === 'tables' ? (table ? html`<${TableView} key=${id + table} lh=${id} name=${table} canWrite=${canWrite} />` : html`<${Empty} icon="table" title="Pick a table">Choose a table on the left to see its schema and data.<//>`) : html`<${Files} key=${id} lh=${id} canWrite=${canWrite} />`}</main>
    </div></div>`;
  function setPath(p) { setQueryFp(p); }
}
function setQueryFp(p) { import('../core/router.js').then((m) => m.setQuery({ fp: p })); }
