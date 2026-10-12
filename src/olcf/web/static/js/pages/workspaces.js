// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Workspaces and items: workspace list, item tree with search/tags, create dialogs, RBAC members, recycle bin, version history with diff, import/export, Git sync.
import { html, useState, useEffect, useMemo, useRef, useAsync, usePoll } from '../core/h.js';
import { api, can } from '../api/client.js';
import { PageHead, Card, Btn, Badge, StatusBadge, Light, Banner, Empty, ErrorBox, Loading, Tabs, TabPanel, Seg, Select, Input, Textarea, Switch, Check, Kv, Code, Async, Tag } from '../ui/basic.js';
import { Icon } from '../ui/icons.js';
import { Table } from '../ui/data.js';
import { Dialog, Drawer, MenuButton, openOverlay, confirmDialog, promptDialog, act, toast } from '../ui/overlay.js';
import { DiffView } from '../ui/diffview.js';
import { invalidateWorkspaces, useWorkspaces, useQueryState, Ago } from './common.js';
import { ITEM_TYPES, itemMeta, itemHref, wsHref } from '../core/links.js';
import { enc, ago, fmtTime, titleCase, downloadBlob, jsonPretty, debounce, fmtNum } from '../core/util.js';

const CREATABLE = ['notebook', 'sql_query', 'lakehouse', 'pipeline', 'environment', 'spark_job_definition', 'variable_set', 'report'];
const splitTags = (s) => s.split(',').map((x) => x.trim()).filter(Boolean);

// ------------------------------------------------------------------ workspaces list
function CreateWorkspace({ close }) {
  const [f, setF] = useState({ name: '', display_name: '', description: '', tags: '' });
  const [busy, setBusy] = useState(false);
  const set = (k) => (v) => setF((s) => ({ ...s, [k]: v }));
  const valid = /^[A-Za-z0-9][A-Za-z0-9 _.-]{0,62}$/.test(f.name);
  async function go() {
    setBusy(true);
    const r = await act(() => api.post('/workspaces', { name: f.name.trim(), display_name: f.display_name || null, description: f.description, tags: splitTags(f.tags) }), { ok: 'Workspace created' });
    setBusy(false);
    if (r) { invalidateWorkspaces(); close(r); }
  }
  return html`<${Dialog} title="Create workspace" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!valid} onClick=${go}>Create workspace<//>`}>
    <${Input} label="Name" value=${f.name} onInput=${set('name')} error=${f.name && !valid ? 'Use letters, digits, spaces, dots, dashes or underscores.' : null} data-autofocus=${true} hint="Used in URLs and APIs." />
    <${Input} label="Display name" value=${f.display_name} onInput=${set('display_name')} /><${Textarea} label="Description" rows=${3} value=${f.description} onInput=${set('description')} /><${Input} label="Tags" value=${f.tags} onInput=${set('tags')} hint="Comma separated." /></${Dialog}>`;
}
function ImportWorkspace({ close }) {
  const [file, setFile] = useState(null);
  const [mode, setMode] = useState('rename');
  const [name, setName] = useState('');
  const [busy, setBusy] = useState(false);
  const [res, setRes] = useState(null);
  const [err, setErr] = useState(null);
  async function go() {
    setBusy(true); setErr(null);
    try {
      const fd = new FormData(); fd.append('file', file, file.name);
      const { data } = await api.full('POST', '/workspaces/import', { query: { on_conflict: mode, new_name: name || undefined }, form: fd });
      setRes(data); invalidateWorkspaces();
    } catch (e) { setErr(e); } finally { setBusy(false); }
  }
  return html`<${Dialog} title="Import a workspace" onClose=${() => close(res)} footer=${res ? html`<${Btn} kind="primary" onClick=${() => close(res)}>Close<//>` : html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!file} onClick=${go}>Import<//>`}>
    ${res ? html`<${Banner} kind="ok" title="Workspace imported">${res.workspace_id ? html`<a href=${wsHref(res.workspace_id)}>Open the workspace</a>` : null} ${res.summary || ''}<//><pre class="code mt-3">${jsonPretty(res)}</pre>` : html`<div>
      <p class="muted">Import an archive created with Export (.olcf.zip). Items, lakehouse definitions and role bindings are restored; data files are included when the archive carries them.</p>
      <div class="field"><label for="imp-file">Archive</label><input id="imp-file" type="file" accept=".zip,application/zip" onChange=${(e) => setFile(e.target.files[0] || null)} /></div>
      <${Select} label="If the workspace already exists" value=${mode} onChange=${setMode} options=${[{ value: 'rename', label: 'Import under a new name' }, { value: 'skip', label: 'Skip existing items' }, { value: 'overwrite', label: 'Overwrite existing items' }, { value: 'fail', label: 'Stop with an error' }]} />
      ${mode === 'rename' ? html`<${Input} label="New name (optional)" value=${name} onInput=${setName} />` : null}${err ? html`<${ErrorBox} error=${err} />` : null}</div>`}</${Dialog}>`;
}

export function WorkspacesPage() {
  const [q, setQ] = useState('');
  const [showDeleted, setSD] = useState(false);
  const ws = useAsync(() => api.get('/workspaces', { q: q || undefined, deleted: showDeleted || undefined, limit: 200 }).then((r) => r.items), [q, showDeleted]);
  async function create() { const r = await openOverlay(({ close }) => html`<${CreateWorkspace} close=${close} />`); if (r) window.location.hash = wsHref(r.id); }
  async function imp() { const r = await openOverlay(({ close }) => html`<${ImportWorkspace} close=${close} />`); if (r) ws.reloadLoud(); }
  async function restore(w) { if (await act(() => api.post(`/workspaces/${enc(w.id)}/restore`), { ok: 'Workspace restored' })) { invalidateWorkspaces(); ws.reloadLoud(); } }
  const items = ws.data || [];
  return html`<div class="page"><${PageHead} icon="folder" title="Workspaces" sub="Notebooks, queries, lakehouses, pipelines and dashboards live in workspaces, each with its own members"
      actions=${html`${can('workspace.import') ? html`<${Btn} icon="upload" onClick=${imp}>Import<//>` : null}<${Btn} kind="primary" icon="plus" onClick=${create}>Create workspace<//>`} />
    <div class="toolbar"><${Input} size="sm" type="search" placeholder="Search workspaces" aria-label="Search workspaces" value=${q} onInput=${debounce(setQ, 250)} /><span class="grow"></span><${Check} label="Show deleted" checked=${showDeleted} onChange=${setSD} /></div>
    <${Async} state=${ws}>${() => items.length ? html`<div class="ws-grid">${items.map((w) => html`<article class=${'ws-card' + (w.deleted_at ? ' deleted' : '')} key=${w.id}>
      <h3><a href=${wsHref(w.id)}>${w.display_name || w.name}</a></h3><p class="muted">${w.description || 'No description.'}</p>
      <div class="chips">${(w.tags || []).map((t) => html`<${Tag} key=${t}>${t}<//>`)}</div>
      <div class="ws-foot small muted">${w.deleted_at ? html`Deleted ${ago(w.deleted_at)} <${Btn} size="sm" onClick=${() => restore(w)}>Restore<//>` : html`Created ${ago(w.created_at)}`}</div></article>`)}</div>` : html`<${Empty} icon="folder" title=${q ? 'No workspace matches' : 'No workspaces yet'} action=${html`<${Btn} kind="primary" icon="plus" onClick=${create}>Create workspace<//>`}>${q ? 'Try another search.' : 'Create one, or install the sample from Home to get a populated example.'}<//>`}<//></div>`;
}

// ------------------------------------------------------------------ items
function CreateItem({ ws, type: t0, folder: f0, close }) {
  const [type, setType] = useState(t0 || 'notebook');
  const [f, setF] = useState({ name: '', description: '', folder: f0 || '', tags: '' });
  const [busy, setBusy] = useState(false);
  const set = (k) => (v) => setF((s) => ({ ...s, [k]: v }));
  const lhs = useAsync(() => api.get(`/workspaces/${enc(ws)}/items`, { type: 'lakehouse', limit: 100 }).then((r) => r.items), [ws]);
  const [lh, setLh] = useState('');
  async function go() {
    setBusy(true);
    const base = { name: f.name.trim(), description: f.description, folder: f.folder, tags: splitTags(f.tags) };
    let r;
    if (type === 'lakehouse') r = await act(() => api.post(`/workspaces/${enc(ws)}/lakehouses`, { ...base }), { ok: 'Lakehouse created' });
    else {
      const lakehouse_id = lh || (lhs.data && lhs.data[0] && lhs.data[0].id) || null;
      const def = { sql_query: { sql: 'SELECT 1 AS one', lakehouse_id, engine: 'duckdb' }, spark_job_definition: { language: 'python', code: "from pyspark.sql import SparkSession\nspark = SparkSession.builder.getOrCreate()\nprint(spark.range(10).count())\n" }, report: { title: f.name.trim(), filters: [], tiles: [{ id: 'hello', title: 'Hello', type: 'kpi', sql: 'SELECT 42 AS answer', lakehouse_id, format: 'integer', engine: 'duckdb', width: 3, height: 1 }] } }[type];
      r = await act(() => api.post(`/workspaces/${enc(ws)}/items`, { type, ...base, ...(def ? { definition: def } : {}) }), { ok: `${itemMeta(type).label} created` });
    }
    setBusy(false);
    if (r) close(r);
  }
  const meta = itemMeta(type);
  return html`<${Dialog} title="Create item" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!f.name.trim()} onClick=${go}>Create ${meta.label.toLowerCase()}<//>`}>
    <div class="type-pick" role="radiogroup" aria-label="Item type">${CREATABLE.map((k) => html`<button key=${k} role="radio" aria-checked=${type === k} class=${type === k ? 'on' : ''} onClick=${() => setType(k)}><${Icon} name=${itemMeta(k).icon} /><span>${itemMeta(k).label}</span></button>`)}</div>
    <${Input} label="Name" value=${f.name} onInput=${set('name')} data-autofocus=${true} /><div class="form-grid"><${Input} label="Folder" value=${f.folder} onInput=${set('folder')} placeholder="optional, e.g. etl/daily" /><${Input} label="Tags" value=${f.tags} onInput=${set('tags')} hint="Comma separated." /></div>
    <${Input} label="Description" value=${f.description} onInput=${set('description')} />
    ${['sql_query', 'report'].includes(type) && lhs.data && lhs.data.length > 1 ? html`<${Select} label="Lakehouse" value=${lh} onChange=${setLh} options=${lhs.data.map((x) => ({ value: x.id, label: x.name }))} placeholder="First lakehouse" />` : null}</${Dialog}>`;
}

function VersionDrawer({ item, onClose, reload }) {
  const vs = useAsync(() => api.get(`/items/${enc(item.id)}/versions`).then((r) => r.items), [item.id]);
  const [sel, setSel] = useState(null);
  const cur = useAsync(() => api.get(`/items/${enc(item.id)}`), [item.id]);
  const old = useAsync(() => (sel ? api.get(`/items/${enc(item.id)}/versions/${sel}`) : null), [item.id, sel]);
  async function revert(v) {
    if (!(await confirmDialog({ title: `Revert to version ${v}?`, confirmLabel: 'Revert', body: 'The current content is kept as a new version, so you can undo this.' }))) return;
    if (await act(() => api.post(`/items/${enc(item.id)}/versions/${v}/revert`), { ok: `Reverted to version ${v}` })) { vs.reloadLoud(); cur.reloadLoud(); reload(); }
  }
  const text = (x) => (x ? jsonPretty(x.definition ?? x.content ?? x) : '');
  return html`<${Drawer} wide=${true} onClose=${onClose} title=${html`<span class="row"><${Icon} name="history" />Version history of ${item.name}</span>`}>
    <${Async} state=${vs}>${(list) => html`<div class="split-v"><ul class="ver-list" aria-label="Versions">${list.map((v) => html`<li key=${v.id} class=${sel === v.version ? 'on' : ''}><button class="linklike" onClick=${() => setSel(v.version)}><b>Version ${v.version}</b><span class="small muted">${ago(v.created_at)} · ${v.comment || ''}</span></button>${v.version !== (cur.data && cur.data.version) ? html`<${Btn} size="sm" kind="ghost" onClick=${() => revert(v.version)}>Revert<//>` : html`<${Badge} kind="info">current<//>`}</li>`)}</ul>
      <div class="grow">${sel ? (old.data && cur.data ? html`<h3 class="mb-2">Version ${sel} compared with the current version</h3><${DiffView} a=${text(old.data)} b=${text(cur.data)} />` : html`<${Loading} />`) : html`<div class="muted">Choose a version to compare it with the current one.</div>`}</div></div>`}<//></${Drawer}>`;
}

function ItemRow({ it, reload, ws, onHistory, onOpenTag }) {
  const meta = itemMeta(it.type);
  const id = enc(it.id);
  async function rename() { const n = await promptDialog({ title: 'Rename', label: 'Name', value: it.name, confirmLabel: 'Rename' }); if (n && n !== it.name && (await act(() => api.put(`/items/${id}`, { name: n, comment: 'renamed' }, { ifMatch: it.etag }), { ok: 'Renamed' }))) reload(); }
  async function move() { const n = await promptDialog({ title: 'Move to folder', label: 'Folder', value: it.folder || '', confirmLabel: 'Move', hint: 'Empty moves it to the top level.' }); if (n != null && (await act(() => api.put(`/items/${id}`, { folder: n, comment: 'moved' }, { ifMatch: it.etag }), { ok: 'Moved' }))) reload(); }
  async function tags() { const n = await promptDialog({ title: 'Tags', label: 'Tags', value: (it.tags || []).join(', '), confirmLabel: 'Save', hint: 'Comma separated.' }); if (n != null && (await act(() => api.put(`/items/${id}`, { tags: splitTags(n), comment: 'tags' }, { ifMatch: it.etag }), { ok: 'Tags saved' }))) reload(); }
  async function del() { if (await confirmDialog({ title: `Delete ${it.name}?`, danger: true, confirmLabel: 'Move to recycle bin', body: 'You can restore it from the recycle bin of this workspace.' }) && (await act(() => api.del(`/items/${id}`), { ok: 'Moved to the recycle bin' })) !== undefined) reload(); }
  const canEdit = can('item.update', ws);
  return html`<li class="item-row" key=${it.id}><a class="item-main" href=${itemHref(it)}><span class="item-ic"><${Icon} name=${meta.icon} /></span><span class="grow" style="min-width:0"><b class="trunc">${it.name}</b>${it.description ? html`<span class="small muted trunc">${it.description}</span>` : null}</span></a>
    <span class="chips item-tags">${(it.tags || []).slice(0, 3).map((t) => html`<button class="tag tag-btn" key=${t} onClick=${() => onOpenTag(t)} title=${`Filter by ${t}`}>${t}</button>`)}</span><span class="badge outline">${meta.label}</span><span class="small muted item-when"><${Ago} ts=${it.updated_at} /></span>
    <${MenuButton} label=${`Actions for ${it.name}`} items=${[{ label: 'Open', icon: 'external', onClick: () => { window.location.hash = itemHref(it); } }, { label: 'Version history', icon: 'history', onClick: () => onHistory(it) }, canEdit && { sep: true }, canEdit && { label: 'Rename', icon: 'edit', onClick: rename }, canEdit && { label: 'Move to folder', icon: 'folder', onClick: move }, canEdit && { label: 'Edit tags', icon: 'tag', onClick: tags }, can('item.delete', ws) && { sep: true }, can('item.delete', ws) && { label: 'Delete', icon: 'trash', danger: true, onClick: del }]} /></li>`;
}

function Items({ ws, wsObj }) {
  const [q, setQ] = useQueryState('q', '');
  const [type, setType] = useQueryState('type', '');
  const [tag, setTag] = useQueryState('tag', '');
  const [hist, setHist] = useState(null);
  const [text, setText] = useState(q);
  const items = useAsync(() => api.get(`/workspaces/${enc(ws)}/items`, { q: q || undefined, type: type || undefined, tag: tag || undefined, limit: 500 }).then((r) => r.items), [ws, q, type, tag]);
  const all = items.data || [];
  const folders = useMemo(() => { const m = {}; all.forEach((i) => { (m[i.folder || ''] ||= []).push(i); }); return Object.entries(m).sort(([a], [b]) => (a === '' ? -1 : b === '' ? 1 : a.localeCompare(b))); }, [all]);
  const [collapsed, setCollapsed] = useState({});
  const allTags = [...new Set(all.flatMap((i) => i.tags || []))].sort();
  useEffect(() => { const t = setTimeout(() => text !== q && setQ(text), 250); return () => clearTimeout(t); }, [text]);
  async function create(t, folder) { const r = await openOverlay(({ close }) => html`<${CreateItem} ws=${ws} type=${t} folder=${folder} close=${close} />`); if (r) { items.reloadLoud(); if (r.type) window.location.hash = itemHref(r); } }
  const typesPresent = Object.keys(ITEM_TYPES);
  return html`<div class="col gap-3"><div class="toolbar"><${Input} size="sm" type="search" placeholder="Search items" aria-label="Search items" value=${text} onInput=${setText} />
      <${Select} size="sm" value=${type} onChange=${setType} aria-label="Filter by type" placeholder="All types" options=${typesPresent.map((k) => ({ value: k, label: ITEM_TYPES[k].plural }))} />
      ${allTags.length || tag ? html`<${Select} size="sm" value=${tag} onChange=${setTag} aria-label="Filter by tag" placeholder="All tags" options=${[...new Set([...(tag ? [tag] : []), ...allTags])]} />` : null}
      <span class="grow"></span>${can('item.create', ws) ? html`<${MenuButton} label="New item" icon="plus" kind="primary" size="" iconOnly=${false} items=${CREATABLE.map((k) => ({ label: itemMeta(k).label, icon: itemMeta(k).icon, onClick: () => create(k) }))}>New item<//>` : null}</div>
    <${Async} state=${items}>${() => all.length ? html`<${Card} flush=${true} head=${false}><div class="item-tree">${folders.map(([folder, list]) => html`<section key=${folder}>${folder ? html`<button class="folder-head" aria-expanded=${!collapsed[folder]} onClick=${() => setCollapsed((c) => ({ ...c, [folder]: !c[folder] }))}><${Icon} name=${collapsed[folder] ? 'chevron-right' : 'chevron-down'} size="sm" /><${Icon} name="folder" size="sm" />${folder}<span class="muted small">${list.length}</span></button>` : null}
      ${collapsed[folder] ? null : html`<ul class="item-list">${list.map((it) => html`<${ItemRow} key=${it.id} it=${it} ws=${ws} reload=${items.reload} onHistory=${setHist} onOpenTag=${setTag} />`)}</ul>`}</section>`)}</div></${Card}>` : html`<${Empty} icon="folder-open" title=${q || type || tag ? 'No item matches' : 'This workspace is empty'} action=${can('item.create', ws) ? html`<${Btn} kind="primary" icon="plus" onClick=${() => create('notebook')}>Create a notebook<//>` : null}>${q || type || tag ? 'Clear the search or filters.' : 'Create a notebook, SQL query or lakehouse to get started.'}<//>`}<//>
    ${hist ? html`<${VersionDrawer} item=${hist} onClose=${() => setHist(null)} reload=${items.reload} />` : null}</div>`;
}

// ------------------------------------------------------------------ members
function AddMember({ ws, close }) {
  const users = useAsync(() => api.get('/identity/users', { limit: 200 }).then((r) => r.items.filter((u) => u.kind !== 'worker' && u.is_active)), []);
  const roles = useAsync(() => api.get('/roles').then((r) => r.roles), []);
  const [p, setP] = useState('');
  const [role, setRole] = useState('Contributor');
  const [busy, setBusy] = useState(false);
  async function go() { setBusy(true); const r = await act(() => api.post('/identity/bindings', { principal_id: p, role, scope_type: 'workspace', scope_id: ws }), { ok: 'Member added' }); setBusy(false); if (r) close(r); }
  const wsRoles = (roles.data || []).filter((r) => !['PlatformAdmin', 'SecurityAdmin', 'RuntimeAdmin'].includes(r.name));
  return html`<${Dialog} title="Add a member" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!p} onClick=${go}>Add member<//>`}>
    <${Select} label="Person or service principal" value=${p} onChange=${setP} placeholder="Choose" options=${(users.data || []).map((u) => ({ value: u.id, label: `${u.display_name || u.name}${u.kind === 'service' ? ' (service)' : ''}` }))} />
    <${Select} label="Role" value=${role} onChange=${setRole} options=${wsRoles.map((r) => r.name)} hint=${(wsRoles.find((r) => r.name === role) || { permissions: [] }).permissions.length + ' permissions'} /></${Dialog}>`;
}
function Members({ ws }) {
  const m = useAsync(() => api.get(`/workspaces/${enc(ws)}/members`).then((r) => r.items), [ws]);
  const manage = can('workspace.members.manage', ws);
  async function add() { if (await openOverlay(({ close }) => html`<${AddMember} ws=${ws} close=${close} />`)) m.reloadLoud(); }
  async function remove(b) { if (!(await confirmDialog({ title: `Remove ${b.display_name || b.principal_name}?`, danger: true, confirmLabel: 'Remove', body: `They lose the ${b.role} role in this workspace.` }))) return; if ((await act(() => api.del(`/identity/bindings/${enc(b.id)}`), { ok: 'Member removed' })) !== undefined) m.reloadLoud(); }
  return html`<${Card} title="Members" sub="Who can do what in this workspace" actions=${manage ? html`<${Btn} size="sm" icon="plus" onClick=${add}>Add member<//>` : null} flush=${true}>
    <${Async} state=${m}>${(items) => html`<${Table} caption="Workspace members" rowKey=${(b) => b.id} columns=${[{ key: 'p', label: 'Member', sort: (b) => b.principal_name, render: (b) => html`<b>${b.display_name || b.principal_name}</b><div class="small muted">${b.principal_name} · ${b.principal_kind}</div>` }, { key: 'role', label: 'Role', render: (b) => html`<${Badge} kind="info">${b.role}<//>` }, { key: 'source', label: 'Source', render: (b) => titleCase(b.source) }, { key: 'created_at', label: 'Since', render: (b) => ago(b.created_at) }, { key: 'a', label: '', sortable: false, render: (b) => manage && b.source !== 'system' ? html`<${Btn} size="sm" kind="ghost" iconOnly icon="trash" label=${`Remove ${b.principal_name}`} onClick=${() => remove(b)} />` : null }]} rows=${items} />`}<//></${Card}>`;
}

// ------------------------------------------------------------------ recycle bin
function Bin({ ws }) {
  const it = useAsync(() => api.get(`/workspaces/${enc(ws)}/items`, { deleted: true, limit: 200 }).then((r) => r.items.filter((x) => x.deleted_at)), [ws]);
  async function restore(i) { if (await act(() => api.post(`/items/${enc(i.id)}/restore`), { ok: `${i.name} restored` })) it.reloadLoud(); }
  return html`<${Card} title="Recycle bin" sub="Deleted items can be restored" flush=${true}><${Async} state=${it}>${(items) => items.length ? html`<${Table} caption="Deleted items" rowKey=${(i) => i.id} columns=${[{ key: 'name', label: 'Item', render: (i) => html`<span class="row"><${Icon} name=${itemMeta(i.type).icon} />${i.name}</span>` }, { key: 'type', label: 'Type', render: (i) => itemMeta(i.type).label }, { key: 'deleted_at', label: 'Deleted', render: (i) => ago(i.deleted_at) }, { key: 'a', label: '', sortable: false, render: (i) => can('item.update', ws) ? html`<${Btn} size="sm" icon="undo" onClick=${() => restore(i)}>Restore<//>` : null }]} rows=${items} />` : html`<${Empty} icon="recycle" title="The recycle bin is empty">Deleted items appear here until you restore them.<//>`}<//></${Card}>`;
}

// ------------------------------------------------------------------ git
function GitPanel({ ws }) {
  const caps = useAsync(() => api.get(`/workspaces/${enc(ws)}/git/capabilities`), [ws]);
  const st = useAsync(() => api.get(`/workspaces/${enc(ws)}/git/status`), [ws]);
  const [f, setF] = useState({ repo_url: '', branch: 'main', folder: '', notebook_format: 'ipynb', create_branch: false });
  const [msg, setMsg] = useState('');
  const [busy, setBusy] = useState('');
  const [pull, setPull] = useState(null);
  const base = `/workspaces/${enc(ws)}/git`;
  const run = async (name, fn, ok) => { setBusy(name); const r = await act(fn, { ok }); setBusy(''); if (r !== undefined) { st.reloadLoud(); caps.reload(); } return r; };
  if (caps.loading && !caps.data) return html`<${Loading} />`;
  const c = caps.data;
  const s = st.data;
  if (c && !c.available) return html`<${Banner} kind="warn" title="Git is not available on the control plane" remediation=${c.remediation || 'Install the git command line tool on the machine running the API and restart it.'}>${c.detail}<//>`;
  return html`<div class="col gap-4">
    <${Card} title="Repository" sub=${c ? `git ${c.version}` : ''}>${s && s.connected ? html`<div class="col gap-3"><${Kv} items=${[['Repository', html`<code>${s.repo_url}</code>`], ['Branch', s.branch], ['Folder', s.folder || '/'], ['Notebook format', s.notebook_format], ['Synced commit', s.synced_commit ? html`<code>${s.synced_commit.slice(0, 10)}</code>` : 'none yet'], ['Remote commit', s.remote_commit ? html`<code>${s.remote_commit.slice(0, 10)}</code>` : null], ['In sync', s.in_sync == null ? null : s.in_sync ? 'Yes' : 'No']]} />
        <div class="row-wrap"><${Btn} icon="refresh" busy=${busy === 'fetch'} onClick=${() => run('fetch', () => api.get(`${base}/status`, { fetch: true }).then((x) => { st.setData(x); return x; }), 'Fetched')}>Fetch<//><${Btn} icon="download" busy=${busy === 'dry'} onClick=${async () => { const r = await run('dry', () => api.post(`${base}/pull`, { dry_run: true })); if (r) setPull(r); }}>Preview pull<//><${Btn} icon="download" kind="primary" busy=${busy === 'pull'} onClick=${async () => { const r = await run('pull', () => api.post(`${base}/pull`, {}), 'Pulled'); if (r) setPull(r); }}>Pull<//>
          <${Btn} kind="danger" icon="x" onClick=${async () => { if (await confirmDialog({ title: 'Disconnect from Git?', danger: true, confirmLabel: 'Disconnect', body: 'Items stay as they are; syncing stops.' })) run('dc', () => api.post(`${base}/disconnect`), 'Disconnected'); }}>Disconnect<//></div></div>` : html`<div>
        <p class="muted" style="margin-top:0">Connect the workspace to a Git repository to keep notebooks and definitions under version control. ${c && c.allow_file_urls ? 'Local file:// repositories are allowed on this server.' : ''}</p>
        <div class="form-grid"><${Input} label="Repository URL" value=${f.repo_url} onInput=${(v) => setF({ ...f, repo_url: v })} placeholder="https://git.example.com/team/lakehouse.git" /><${Input} label="Branch" value=${f.branch} onInput=${(v) => setF({ ...f, branch: v })} /><${Input} label="Folder in the repository" value=${f.folder} onInput=${(v) => setF({ ...f, folder: v })} /><${Select} label="Notebook format" value=${f.notebook_format} onChange=${(v) => setF({ ...f, notebook_format: v })} options=${[{ value: 'ipynb', label: 'Jupyter (.ipynb)' }, { value: 'py', label: 'Python (.py)' }, { value: 'fabric', label: 'Fabric Git format' }]} /></div>
        <${Switch} label="Create the branch if it does not exist" checked=${f.create_branch} onChange=${(v) => setF({ ...f, create_branch: v })} /><div class="mt-3"><${Btn} kind="primary" icon="git" busy=${busy === 'connect'} disabled=${!f.repo_url} onClick=${() => run('connect', () => api.post(`${base}/connect`, f), 'Connected')}>Connect<//></div></div>`}</${Card}>
    ${s && s.connected ? html`<${Card} title="Changes" sub=${`${(s.changes || []).length} pending`} flush=${true}>${(s.changes || []).length ? html`<${Table} dense=${true} rowKey=${(c) => c.path || c.item_id || c.name} columns=${[{ key: 'status', label: 'Change', render: (c) => html`<${Badge} kind=${c.status === 'deleted' ? 'bad' : c.status === 'added' ? 'ok' : 'info'}>${c.status || c.change}<//>` }, { key: 'path', label: 'Item', render: (c) => c.name || c.path }, { key: 'dir', label: 'Direction', render: (c) => c.direction || c.side || '' }]} rows=${s.changes} />
        <div class="commit-box"><${Input} label="Commit message" value=${msg} onInput=${setMsg} /><${Btn} kind="primary" icon="git" busy=${busy === 'commit'} disabled=${!msg.trim()} onClick=${async () => { const r = await run('commit', () => api.post(`${base}/commit`, { message: msg }), 'Committed'); if (r) setMsg(''); }}>Commit and push<//></div>` : html`<${Empty} icon="check-circle" title="Nothing to commit">The workspace matches the repository.<//>`}</${Card}>` : null}
    ${pull ? html`<${Card} title="Pull result"><pre class="code" tabindex="0">${jsonPretty(pull)}</pre></${Card}>` : null}</div>`;
}

// ------------------------------------------------------------------ settings
function Settings({ w, reload }) {
  const [f, setF] = useState({ display_name: w.display_name || '', description: w.description || '', tags: (w.tags || []).join(', ') });
  const lhs = useAsync(() => api.get(`/workspaces/${enc(w.id)}/items`, { type: 'lakehouse', limit: 100 }).then((r) => r.items), [w.id]);
  const [dl, setDl] = useState((w.settings && w.settings.default_lakehouse) || '');
  const [busy, setBusy] = useState(false);
  async function save() { setBusy(true); const r = await act(() => api.patch(`/workspaces/${enc(w.id)}`, { display_name: f.display_name, description: f.description, tags: splitTags(f.tags), settings: { ...(w.settings || {}), default_lakehouse: dl || null } }, { ifMatch: w.etag }), { ok: 'Workspace saved' }); setBusy(false); if (r) { invalidateWorkspaces(); reload(); } }
  return html`<${Card} title="Settings"><div class="form-grid"><${Input} label="Display name" value=${f.display_name} onInput=${(v) => setF({ ...f, display_name: v })} /><${Input} label="Tags" value=${f.tags} onInput=${(v) => setF({ ...f, tags: v })} />
    <${Select} label="Default lakehouse" value=${dl} onChange=${setDl} placeholder="None" options=${(lhs.data || []).map((l) => ({ value: l.id, label: l.name }))} hint="Notebooks and SQL use it when no lakehouse is chosen." /></div>
    <${Textarea} label="Description" rows=${3} value=${f.description} onInput=${(v) => setF({ ...f, description: v })} /><${Btn} kind="primary" busy=${busy} onClick=${save}>Save changes<//></${Card}>`;
}

export function WorkspaceDetailPage({ params }) {
  const wsId = params.ws;
  const w = useAsync(() => api.get(`/workspaces/${enc(wsId)}`), [wsId]);
  const [tab, setTab] = useQueryState('tab', 'items');
  async function exp() { const b = await act(() => api.blob(`/workspaces/${enc(wsId)}/export`), {}); if (b) downloadBlob(b, `${(w.data && w.data.name) || 'workspace'}.olcf.zip`); }
  async function del() { if (!(await confirmDialog({ title: `Delete workspace ${w.data.name}?`, danger: true, confirmLabel: 'Delete workspace', body: 'It moves to the deleted list and can be restored from the Workspaces page.' }))) return; if ((await act(() => api.del(`/workspaces/${enc(wsId)}`), { ok: 'Workspace deleted' })) !== undefined) { invalidateWorkspaces(); window.location.hash = '#/workspaces'; } }
  const d = w.data;
  if (w.error && !d) return html`<div class="page"><${ErrorBox} error=${w.error} /></div>`;
  if (!d) return html`<div class="page"><${Loading} /></div>`;
  return html`<div class="page"><${PageHead} icon="folder" title=${d.display_name || d.name} sub=${d.description || null}
      badge=${html`<span class="chips" style="margin-left:8px">${(d.tags || []).map((t) => html`<${Tag} key=${t}>${t}<//>`)}</span>`}
      actions=${html`${can('workspace.export', wsId) ? html`<${Btn} icon="download" onClick=${exp}>Export<//>` : null}${can('workspace.delete', wsId) ? html`<${Btn} kind="danger" icon="trash" onClick=${del}>Delete<//>` : null}`} />
    <${Tabs} label="Workspace sections" value=${tab} onChange=${setTab} tabs=${[{ id: 'items', label: 'Items' }, { id: 'members', label: 'Members' }, { id: 'bin', label: 'Recycle bin' }, { id: 'git', label: 'Git' }, { id: 'settings', label: 'Settings' }]} />
    <div class="tab-body"><${TabPanel} id="items" value=${tab}><${Items} ws=${wsId} wsObj=${d} /></${TabPanel}><${TabPanel} id="members" value=${tab}><${Members} ws=${wsId} /></${TabPanel}><${TabPanel} id="bin" value=${tab}><${Bin} ws=${wsId} /></${TabPanel}><${TabPanel} id="git" value=${tab}><${GitPanel} ws=${wsId} /></${TabPanel}><${TabPanel} id="settings" value=${tab}><${Settings} w=${d} reload=${w.reload} /></${TabPanel}></div></div>`;
}
