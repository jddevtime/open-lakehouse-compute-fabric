// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Environments (locked dependencies), variable sets (typed variables with per-stage overrides) and Spark job definitions.
import { html, useState, useEffect, useRef, useMemo, useAsync } from '../core/h.js';
import { api, can } from '../api/client.js';
import { Btn, Badge, Banner, Empty, ErrorBox, Loading, Tabs, TabPanel, PageHead, Select, Input, Textarea, Check, Switch, Field, Kv, Card, Code, Steps } from '../ui/basic.js';
import { Table, JsonView } from '../ui/data.js';
import { Icon } from '../ui/icons.js';
import { Dialog, MenuButton, openOverlay, confirmDialog, promptDialog, act, toast } from '../ui/overlay.js';
import { enc, fmtTime, titleCase, shortId, clone, jsonPretty } from '../core/util.js';
import { useQueryState, Ago, useWorkspaceId, InlineWorkspaceSelect, useItems, openOperation } from './common.js';
import { itemHref, jobHref } from '../core/links.js';
import { CodeEditor } from '../notebook/cm.js';

// ---------------------------------------------------------------------------------------------------------------------------------------- shared editing helpers
function useItemEditor(id) {
  const item = useAsync(() => api.get(`/items/${enc(id)}`), [id]);
  const [draft, setDraft] = useState(null);
  const [saving, setSaving] = useState(false);
  const saved = useRef('');
  useEffect(() => { if (item.data) { const d = clone(item.data.definition || {}); saved.current = JSON.stringify(d); setDraft(d); } }, [item.data && item.data.etag]);
  const dirty = !!draft && JSON.stringify(draft) !== saved.current;
  const mutate = (fn) => setDraft((d) => { const n = clone(d); fn(n); return n; });
  async function save() {
    setSaving(true);
    const r = await act(() => api.put(`/items/${enc(id)}`, { definition: draft, comment: 'edited in the console' }, { ifMatch: item.data.etag }), { ok: 'Saved' });
    setSaving(false);
    if (r) { await item.reload(); return true; }
    return false;
  }
  return { item, draft, setDraft, mutate, dirty, saving, save, revert: () => setDraft(JSON.parse(saved.current)) };
}
function EditActions({ ed, canEdit }) {
  return canEdit ? html`<${Btn} disabled=${!ed.dirty} onClick=${ed.revert}>Revert<//><${Btn} kind="primary" icon="save" busy=${ed.saving} disabled=${!ed.dirty} onClick=${ed.save} data-testid="item-save">Save<//>` : null;
}
const DetailBadge = ({ ed }) => (ed.dirty ? html`<${Badge} kind="warn">Unsaved changes<//>` : html`<${Badge}>${'Version ' + ed.item.data.version}<//>`);

/** One entry per line <-> array. */
function Lines({ label, value, onChange, hint, rows = 4, mono = true, placeholder }) {
  const [text, setText] = useState((value || []).join('\n'));
  const last = useRef((value || []).join('\n'));
  useEffect(() => { const t = (value || []).join('\n'); if (t !== last.current) { last.current = t; setText(t); } }, [value]);
  return html`<${Textarea} label=${label} mono=${mono} rows=${rows} value=${text} hint=${hint} placeholder=${placeholder} onInput=${(t) => { setText(t); const arr = t.split('\n').map((x) => x.trim()).filter(Boolean); last.current = arr.join('\n'); onChange(arr); }} />`;
}
/** Editable object of string values. */
function KvEditor({ label, value, onChange, keyLabel = 'Name', valueLabel = 'Value', hint }) {
  const entries = Object.entries(value || {});
  const [rows, setRows] = useState(entries.length ? entries : []);
  const last = useRef(JSON.stringify(value || {}));
  useEffect(() => { const j = JSON.stringify(value || {}); if (j !== last.current) { last.current = j; setRows(Object.entries(value || {})); } }, [value]);
  function commit(rs) { setRows(rs); const o = {}; rs.forEach(([k, v]) => { if (k.trim()) o[k.trim()] = v; }); last.current = JSON.stringify(o); onChange(o); }
  return html`<fieldset class="fieldset"><legend>${label}</legend>${hint ? html`<div class="small muted mb-2">${hint}</div>` : null}
    <div class="col gap-2">${rows.map(([k, v], i) => html`<div key=${i} class="kv-row"><input class="input mono" aria-label=${keyLabel} placeholder=${keyLabel} value=${k} onInput=${(e) => commit(rows.map((r, j) => (j === i ? [e.target.value, r[1]] : r)))} /><input class="input mono" aria-label=${valueLabel} placeholder=${valueLabel} value=${v} onInput=${(e) => commit(rows.map((r, j) => (j === i ? [r[0], e.target.value] : r)))} /><${Btn} size="sm" iconOnly icon="x" label=${`Remove ${k || 'row'}`} onClick=${() => commit(rows.filter((_, j) => j !== i))} /></div>`)}
      <div><${Btn} size="sm" icon="plus" onClick=${() => setRows([...rows, ['', '']])}>Add<//></div></div></fieldset>`;
}
function JsonBox({ value, onCommit, label, readOnly }) {
  const [text, setText] = useState(() => jsonPretty(value));
  const [err, setErr] = useState('');
  const last = useRef(JSON.stringify(value));
  useEffect(() => { const t = JSON.stringify(value); if (t !== last.current) { last.current = t; setText(jsonPretty(value)); setErr(''); } }, [value]);
  function change(t) { setText(t); try { const v = JSON.parse(t); setErr(''); last.current = JSON.stringify(v); onCommit(v); } catch (e) { setErr(e.message); } }
  return html`<div><div class=${'json-field' + (err ? ' bad' : '')}><${CodeEditor} value=${text} mode="json" onChange=${change} autoHeight=${true} lineNumbers=${true} readOnly=${readOnly} label=${label} /></div>${err ? html`<div class="err p-2" role="alert">Not valid JSON yet: ${err}</div>` : null}</div>`;
}

function ItemListPage({ type, title, icon, sub, empty, columns, createBody, createLabel, extra }) {
  const [ws, setWs, wss] = useWorkspaceId();
  const items = useItems(ws, type);
  const canCreate = can('item.create', ws);
  const enrich = useAsync(async () => (extra && items.data ? extra(items.data) : {}), [items.data]);
  async function create() {
    const n = await promptDialog({ title: `New ${createLabel}`, label: 'Name', confirmLabel: 'Create', hint: 'Letters, digits, dots, dashes and underscores.' });
    if (!n) return;
    const r = await act(() => api.post(`/workspaces/${enc(ws)}/items`, { type, name: n.trim(), definition: createBody() }), { ok: `${titleCase(createLabel)} created` });
    if (r) window.location.hash = itemHref(r);
  }
  return html`<div class="page wide"><${PageHead} title=${title} icon=${icon} sub=${sub} actions=${html`<${InlineWorkspaceSelect} value=${ws} onChange=${setWs} items=${wss} />${canCreate ? html`<${Btn} kind="primary" icon="plus" onClick=${create} data-testid="new-item">New ${createLabel}<//>` : null}`} />
    ${items.loading && !items.data ? html`<${Loading} />` : items.error ? html`<${ErrorBox} error=${items.error} onRetry=${items.reloadLoud} />` : html`
    <${Table} caption=${title} rows=${items.data} onRowClick=${(i) => { window.location.hash = itemHref(i); }} empty=${html`<${Empty} icon=${icon} title=${`No ${title.toLowerCase()} in this workspace`} action=${canCreate ? html`<${Btn} kind="primary" icon="plus" onClick=${create}>New ${createLabel}<//>` : null}>${empty}<//>`} columns=${[
      { key: 'name', label: 'Name', render: (i) => html`<a href=${itemHref(i)}><b>${i.name}</b></a><div class="small muted">${i.description || ''}</div>`, sort: (i) => i.name },
      ...columns.map((c) => ({ ...c, render: (i) => c.render(i, enrich.data || {}) })),
      { key: 'updated', label: 'Edited', render: (i) => html`<${Ago} ts=${i.updated_at} />`, sort: (i) => i.updated_at },
    ]} />`}</div>`;
}
function DetailShell({ ed, title, icon, children, actions, canEdit }) {
  if (ed.item.loading && !ed.item.data) return html`<div class="page"><${Loading} /></div>`;
  if (ed.item.error) return html`<div class="page"><${ErrorBox} error=${ed.item.error} onRetry=${ed.item.reloadLoud} /></div>`;
  if (!ed.draft) return html`<div class="page"><${Loading} /></div>`;
  return html`<div class="page wide"><${PageHead} title=${ed.item.data.name} icon=${icon} sub=${title} badge=${html`<${DetailBadge} ed=${ed} />`} actions=${html`${actions}<${EditActions} ed=${ed} canEdit=${canEdit} />`} />${typeof children === 'function' ? children() : children}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------------------------------- environments
const ENV_DEFAULT = () => ({ runtime: { family: 'fabric-2.0-compatible' }, python: { requirements: [] }, jars: { coordinates: [] }, variables: {}, spark_conf: {} });
export function EnvironmentsPage() {
  return html`<${ItemListPage} type="environment" title="Environments" icon="box" createLabel="environment" createBody=${ENV_DEFAULT}
    sub="Pinned Python packages, JARs, variables and Spark settings that notebooks and jobs can share. Publishing locks exact versions so every run gets the same set."
    empty="Create one to pin the libraries your notebooks need."
    extra=${async (list) => Object.fromEntries(await Promise.all(list.slice(0, 60).map(async (i) => { try { const d = await api.get(`/items/${enc(i.id)}`); return [i.id, d.definition]; } catch { return [i.id, null]; } })))}
    columns=${[
      { key: 'rt', label: 'Runtime', sortable: false, render: (i, e) => { const d = e[i.id]; return d ? html`<span class="mono small">${(d.runtime || {}).runtime_id || (d.runtime || {}).family || 'default'}</span>` : '–'; } },
      { key: 'pk', label: 'Packages', sortable: false, render: (i, e) => { const d = e[i.id]; return d ? ((d.python || {}).requirements || []).length + ((d.jars || {}).coordinates || []).length : '–'; } },
      { key: 'pub', label: 'Published', sortable: false, render: (i, e) => { const d = e[i.id]; const p = d && (d.publications || [])[0]; return p ? html`<${Badge} kind="ok">v${p.version}<//> <span class="small muted"><${Ago} ts=${p.published_at} /></span>` : d ? html`<${Badge}>Not published<//>` : '–'; } },
    ]} />`;
}

function PublishDialog({ env, close }) {
  const [comment, setComment] = useState('');
  const [dry, setDry] = useState(false);
  const [force, setForce] = useState(false);
  const [verify, setVerify] = useState(false);
  const [busy, setBusy] = useState(false);
  async function go() {
    setBusy(true);
    const r = await act(() => api.post(`/environments/${enc(env.id)}/publish`, { comment, dry_run: dry, force, verify, wait: false }));
    setBusy(false);
    if (r && r.operation_id) { close(true); await openOperation(r.operation_id, dry ? 'Checking the environment' : `Publishing ${env.name}`); }
  }
  return html`<${Dialog} title=${`Publish ${env.name}`} onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" icon="rocket" busy=${busy} onClick=${go} data-testid="publish-confirm">${dry ? 'Check only' : 'Publish'}<//>`}>
    <p class="muted">Publishing resolves every package to an exact version with file hashes and stores an immutable lock. Jobs that use this environment install exactly that lock.</p>
    <div class="col gap-3"><${Input} label="What changed" value=${comment} onInput=${setComment} placeholder="Added pandas" />
      <${Check} label="Check only, do not publish" checked=${dry} onChange=${setDry} /><${Check} label="Publish even if nothing changed" checked=${force} onChange=${setForce} /><${Check} label="Also test-install the lock on the control plane" checked=${verify} onChange=${setVerify} /></div></${Dialog}>`;
}

function LockView({ lock }) {
  const pk = (lock.python && lock.python.packages) || [];
  return html`<div class="col gap-3"><${Kv} items=${[['Runtime', lock.runtime && (lock.runtime.runtime_id || lock.runtime.family)], ['Python', lock.python && `${lock.python.series} (${lock.python.abi})`], ['Platforms', ((lock.python || {}).platforms || []).join(', ')], ['Lock hash', html`<span class="mono small">${lock.lock_hash}</span>`], ['Resolved', lock.meta && lock.meta.resolved_at ? fmtTime(lock.meta.resolved_at) : null]]} />
    <${Table} dense caption="Locked packages" rows=${pk.map((p, i) => ({ ...p, id: p.name + i }))} empty=${html`<div class="small muted">No Python packages are pinned.</div>`} columns=${[{ key: 'name', label: 'Package', render: (p) => p.name }, { key: 'version', label: 'Version', render: (p) => p.version }, { key: 'files', label: 'Artifacts', render: (p) => ((p.artifacts || p.files || []).length || '–') }]} />
    ${((lock.jars || {}).resolved || []).length ? html`<${Table} dense caption="Locked JARs" rows=${lock.jars.resolved.map((j, i) => ({ ...j, id: i }))} columns=${[{ key: 'c', label: 'Coordinate', render: (j) => j.coordinate || j.coordinates || j.name || JSON.stringify(j) }, { key: 's', label: 'SHA-256', render: (j) => html`<span class="mono small">${(j.sha256 || '').slice(0, 16)}</span>` }]} />` : null}</div>`;
}

export function EnvironmentDetailPage({ params }) {
  const id = params.id;
  const ed = useItemEditor(id);
  const [tab, setTab] = useQueryState('tab', 'definition');
  const [selVer, setSelVer] = useState(null);
  const pubs = useAsync(() => api.get(`/environments/${enc(id)}/publications`).then((r) => r.items).catch(() => []), [id, ed.item.data && ed.item.data.version]);
  const rts = useAsync(() => api.get('/runtimes').then((r) => r.items).catch(() => []), []);
  const pol = useAsync(() => api.get('/environments/policy').catch(() => null), []);
  const lock = useAsync(() => (selVer ? api.get(`/environments/${enc(id)}/publications/${selVer}`) : Promise.resolve(null)), [id, selVer]);
  useEffect(() => { if (selVer == null && pubs.data && pubs.data[0]) setSelVer(pubs.data[0].version); }, [pubs.data]);
  const it = ed.item.data;
  const ws = it && it.workspace_id;
  const canEdit = ws ? can('item.update', ws) : false;
  const d = ed.draft;
  async function publish() { if (ed.dirty) { toast.warn('Save your changes first', 'Publishing uses the saved definition.'); return; } if (await openOverlay(({ close }) => html`<${PublishDialog} env=${it} close=${close} />`)) { ed.item.reload(); pubs.reload(); } }
  async function verify() { const r = await act(() => api.full('POST', `/environments/${enc(id)}/verify`, { query: selVer ? { version: selVer } : undefined, body: {} }).then((x) => x.data)); if (r) await openOverlay(({ close }) => html`<${Dialog} title="Install test" onClose=${() => close()} footer=${html`<${Btn} kind="primary" onClick=${() => close()}>Close<//>`}><${Banner} kind=${r.ok === false ? 'bad' : 'ok'} title=${r.ok === false ? 'The lock did not install' : 'The lock installs cleanly'}>${r.detail || r.message || ''}<//><div class="mt-3"><${JsonView} value=${r} depth=${2} /></div></${Dialog}>`); }
  async function fromNotebook() {
    const nbs = await api.get(`/workspaces/${enc(ws)}/items`, { type: 'notebook', limit: 200 }).then((r) => r.items);
    const picked = await openOverlay(({ close }) => html`<${NotebookPicker} items=${nbs} close=${close} />`);
    if (!picked) return;
    const r = await act(() => api.post(`/environments/${enc(id)}/requirements/from-notebook`, { notebook_id: picked, apply: true }), { ok: 'Requirements imported' });
    if (r) ed.item.reload();
  }
  const fams = [...new Set((rts.data || []).map((r) => r.family))];
  const req = (d && d.python && d.python.requirements) || [];
  return html`<${DetailShell} ed=${ed} icon="box" title="Environment" canEdit=${canEdit} actions=${can('item.update', ws) ? html`<${Btn} icon="rocket" onClick=${publish} data-testid="env-publish">Publish<//>` : null}>${() => html`
    <${Tabs} value=${tab} onChange=${setTab} tabs=${[{ id: 'definition', label: 'Definition' }, { id: 'publications', label: 'Publications', count: (pubs.data || []).length || undefined }, { id: 'json', label: 'JSON' }, { id: 'policy', label: 'Package policy' }]} />
    <div class="mt-4">
    <${TabPanel} id="definition" value=${tab}><div class="grid c2">
      <${Card} title="Runtime and packages"><div class="col gap-3">
        <${Select} label="Runtime family" value=${(d.runtime || {}).family || ''} onChange=${(v) => ed.mutate((x) => { x.runtime = { ...(x.runtime || {}), family: v || undefined }; })} placeholder="Platform default" options=${fams} disabled=${!canEdit} />
        <${Select} label="Pin to a runtime build (optional)" value=${(d.runtime || {}).runtime_id || ''} onChange=${(v) => ed.mutate((x) => { x.runtime = { ...(x.runtime || {}), runtime_id: v || undefined }; })} placeholder="Any build of the family" options=${(rts.data || []).filter((r) => !(d.runtime || {}).family || r.family === d.runtime.family).map((r) => ({ value: r.id, label: r.id }))} disabled=${!canEdit} />
        <${Lines} label="Python requirements" value=${req} onChange=${(a) => ed.mutate((x) => { x.python = { ...(x.python || {}), requirements: a }; })} hint="One per line, for example pandas>=2,<3" placeholder="pandas>=2,<3" />
        <${Lines} label="JAR coordinates" value=${(d.jars || {}).coordinates || []} onChange=${(a) => ed.mutate((x) => { x.jars = { ...(x.jars || {}), coordinates: a }; })} hint="group:artifact:version" />
        <${Select} label="Installing packages at run time" value=${d.pip_policy || ''} onChange=${(v) => ed.mutate((x) => { if (v) x.pip_policy = v; else delete x.pip_policy; })} placeholder="Platform default" options=${[{ value: 'block', label: 'Block' }, { value: 'declared', label: 'Only what is declared' }, { value: 'allow', label: 'Allow any' }]} /></div>
        ${canEdit ? html`<div class="mt-3"><${Btn} size="sm" icon="notebook" onClick=${fromNotebook}>Import from a notebook's %pip lines<//></div>` : null}<//>
      <${Card} title="Variables and Spark settings"><div class="col gap-3">
        <${KvEditor} label="Environment variables" value=${d.variables} onChange=${(o) => ed.mutate((x) => { x.variables = o; })} keyLabel="NAME" hint="Set in every process that uses this environment. Do not put secrets here." />
        <${KvEditor} label="Spark configuration" value=${d.spark_conf} onChange=${(o) => ed.mutate((x) => { x.spark_conf = o; })} keyLabel="spark.sql.shuffle.partitions" /></div><//></div><//>
    <${TabPanel} id="publications" value=${tab}><div class="grid c2">
      <${Card} title="Versions" sub="Every publish creates an immutable version.">${pubs.loading ? html`<${Loading} />` : html`<${Table} dense caption="Publications" rows=${(pubs.data || []).map((p) => ({ ...p, id: p.version }))} selected=${(p) => p.version === selVer} onRowClick=${(p) => setSelVer(p.version)} empty=${html`<${Empty} icon="rocket" title="Not published yet">Publish to pin exact package versions.<//>`} columns=${[{ key: 'v', label: 'Version', render: (p) => html`<b>v${p.version}</b>` }, { key: 'at', label: 'Published', render: (p) => html`<${Ago} ts=${p.published_at} /> <span class="small muted">by ${p.published_by}</span>` }, { key: 'c', label: 'Comment', render: (p) => p.comment || '' }]} />`}<//>
      <${Card} title=${selVer ? `Lock of v${selVer}` : 'Lock'} actions=${selVer && canEdit ? html`<${Btn} size="sm" icon="check" onClick=${verify}>Test install<//>` : null}>${!selVer ? html`<div class="muted small">Select a version.</div>` : lock.loading ? html`<${Loading} />` : lock.error ? html`<${ErrorBox} error=${lock.error} />` : lock.data ? html`<${LockView} lock=${lock.data.lock} />` : null}<//></div><//>
    <${TabPanel} id="json" value=${tab}><${JsonBox} value=${d} onCommit=${(v) => ed.setDraft(v)} label="Environment definition" readOnly=${!canEdit} /><//>
    <${TabPanel} id="policy" value=${tab}><${Card} title="Package sources" sub="Set by administrators. Environments cannot loosen it.">${pol.data ? html`<${Kv} items=${[['Public package index allowed', pol.data.allow_public_package_install ? 'Yes' : 'No'], ['Allowed indexes', (pol.data.allowed_package_indexes || []).join(', ') || 'Any'], ['Wheelhouses', (pol.data.wheelhouses || []).join(', ') || 'None'], ['Source distributions', pol.data.allow_sdist ? 'Allowed' : 'Wheels only'], ['Default run-time installs', pol.data.pip_policy_default], ['Strictest allowed', pol.data.pip_policy_max]]} />` : html`<${Loading} />`}<//><//>
    </div>`}</${DetailShell}>`;
}
function NotebookPicker({ items, close }) {
  const [v, setV] = useState(items[0] ? items[0].id : '');
  return html`<${Dialog} title="Import requirements" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" disabled=${!v} onClick=${() => close(v)}>Import<//>`}><${Select} label="Notebook" value=${v} onChange=${setV} options=${items.map((i) => ({ value: i.id, label: i.name }))} hint="Adds the packages from its %pip install lines to this environment." /></${Dialog}>`;
}

// ---------------------------------------------------------------------------------------------------------------------------------------- variable sets
const VTYPES = ['string', 'integer', 'number', 'boolean', 'array', 'object', 'secret'];
const NAME_OK = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/;
function ValueInput({ type, value, onChange, secrets, label, placeholder }) {
  const [text, setText] = useState(() => (type === 'array' || type === 'object') && value !== undefined ? JSON.stringify(value) : '');
  const [bad, setBad] = useState(false);
  useEffect(() => { if (type === 'array' || type === 'object') setText(value === undefined ? '' : JSON.stringify(value)); }, [type, value === undefined]);
  if (type === 'boolean') return html`<select class="select sm" aria-label=${label} value=${value === undefined ? '' : String(value)} onChange=${(e) => onChange(e.target.value === '' ? undefined : e.target.value === 'true')}><option value="" selected=${value === undefined}>${placeholder || 'Not set'}</option><option value="true" selected=${value === true}>true</option><option value="false" selected=${value === false}>false</option></select>`;
  if (type === 'secret') return html`<select class="select sm" aria-label=${label} value=${value || ''} onChange=${(e) => onChange(e.target.value || undefined)}><option value="" selected=${!value}>${placeholder || 'Not set'}</option>${secrets.map((s) => html`<option key=${s.id} value=${s.id} selected=${s.id === value}>${s.name}</option>`)}</select>`;
  if (type === 'array' || type === 'object') return html`<input class=${'input sm mono' + (bad ? ' bad' : '')} aria-label=${label} aria-invalid=${bad} placeholder=${placeholder || (type === 'array' ? '[]' : '{}')} value=${text} onInput=${(e) => { const t = e.target.value; setText(t); if (!t.trim()) { setBad(false); onChange(undefined); return; } try { const v = JSON.parse(t); if ((type === 'array') !== Array.isArray(v) || (type === 'object' && (v === null || typeof v !== 'object'))) throw new Error('type'); setBad(false); onChange(v); } catch { setBad(true); } }} />`;
  const num = type === 'integer' || type === 'number';
  return html`<input class="input sm" aria-label=${label} type=${num ? 'number' : 'text'} step=${type === 'number' ? 'any' : undefined} placeholder=${placeholder || ''} value=${value === undefined ? '' : value} onInput=${(e) => { const t = e.target.value; onChange(t === '' ? undefined : num ? Number(t) : t); }} />`;
}
export function VariableSetsPage() {
  return html`<${ItemListPage} type="variable_set" title="Variable sets" icon="sliders" createLabel="variable set" createBody=${() => ({ stages: ['dev', 'test', 'prod'], default_stage: 'dev', variables: [] })}
    sub="Named values with a different setting per stage. Notebooks, pipelines and Spark jobs read them, and promoting a workspace to another stage switches them all at once."
    empty="Create one to keep dev, test and prod settings apart." columns=${[]} />`;
}
export function VariableSetDetailPage({ params }) {
  const id = params.id;
  const ed = useItemEditor(id);
  const secrets = useAsync(() => api.get('/secrets').then((r) => r.items).catch(() => []), []);
  const [stage, setStage] = useState('');
  const d = ed.draft;
  const ws = ed.item.data && ed.item.data.workspace_id;
  const canEdit = ws ? can('item.update', ws) : false;
  const stages = (d && d.stages) || [];
  useEffect(() => { if (d && !stage) setStage(d.default_stage || stages[0] || ''); }, [!!d]);
  const resolved = useAsync(() => (stage && !ed.dirty ? api.get(`/variable-sets/${enc(id)}/resolve`, { stage }) : Promise.resolve(null)), [id, stage, ed.item.data && ed.item.data.etag]);
  const vars = (d && d.variables) || [];
  const setVar = (i, fn) => ed.mutate((x) => { fn(x.variables[i]); });
  async function addVar() {
    const n = await promptDialog({ title: 'New variable', label: 'Name', confirmLabel: 'Add', validate: (v) => (NAME_OK.test(v) ? null : 'Start with a letter or underscore; letters, digits and underscores only.') });
    if (n) ed.mutate((x) => { x.variables = [...(x.variables || []), { name: n, type: 'string', default: '', overrides: {}, description: '' }]; });
  }
  async function addStage() {
    const n = await promptDialog({ title: 'New stage', label: 'Stage name', confirmLabel: 'Add', validate: (v) => (/^[A-Za-z0-9_-]{1,32}$/.test(v) && !stages.includes(v) ? null : 'Use up to 32 letters, digits, dashes or underscores, not already used.') });
    if (n) ed.mutate((x) => { x.stages = [...x.stages, n]; });
  }
  const sn = Object.fromEntries((secrets.data || []).map((s) => [s.id, s.name]));
  return html`<${DetailShell} ed=${ed} icon="sliders" title="Variable set" canEdit=${canEdit}>${() => html`
    <div class="row-wrap mb-3"><span class="muted small">Stages</span>${stages.map((s) => html`<span key=${s} class="chip on-static">${s}${s === d.default_stage ? html` <span class="small muted">default</span>` : ''}</span>`)}${canEdit ? html`<${Btn} size="sm" icon="plus" onClick=${addStage}>Add stage<//>` : null}
      <${Select} label="Default stage" size="sm" value=${d.default_stage || ''} onChange=${(v) => ed.mutate((x) => { x.default_stage = v; })} options=${stages} disabled=${!canEdit} /></div>
    <${Card} flush title="Variables" sub="Leave a stage empty to use the default." actions=${canEdit ? html`<${Btn} size="sm" icon="plus" onClick=${addVar} data-testid="add-variable">Add variable<//>` : null}>
      ${vars.length ? html`<div class="table-wrap"><table class="table dense vars-table"><caption class="sr-only">Variables</caption><thead><tr><th scope="col">Name</th><th scope="col">Type</th><th scope="col">Default</th>${stages.map((s) => html`<th key=${s} scope="col">${s}</th>`)}<th scope="col"><span class="sr-only">Remove</span></th></tr></thead>
        <tbody>${vars.map((v, i) => html`<tr key=${v.name + i}><td><b class="mono">${v.name}</b>${v.description ? html`<div class="small muted">${v.description}</div>` : ''}</td>
          <td><select class="select sm" aria-label=${`Type of ${v.name}`} disabled=${!canEdit} onChange=${(e) => setVar(i, (x) => { x.type = e.target.value; x.default = e.target.value === 'boolean' ? false : e.target.value === 'string' ? '' : undefined; x.overrides = {}; })}>${VTYPES.map((t) => html`<option key=${t} value=${t} selected=${t === v.type}>${t}</option>`)}</select></td>
          <td><${ValueInput} key=${v.type} type=${v.type} label=${`Default of ${v.name}`} value=${v.default} secrets=${secrets.data || []} onChange=${(val) => setVar(i, (x) => { if (val === undefined) delete x.default; else x.default = val; })} /></td>
          ${stages.map((s) => html`<td key=${s}><${ValueInput} key=${v.type + s} type=${v.type} label=${`${s} value of ${v.name}`} placeholder="default" value=${(v.overrides || {})[s]} secrets=${secrets.data || []} onChange=${(val) => setVar(i, (x) => { x.overrides = x.overrides || {}; if (val === undefined) delete x.overrides[s]; else x.overrides[s] = val; })} /></td>`)}
          <td>${canEdit ? html`<${Btn} size="sm" iconOnly icon="trash" label=${`Remove ${v.name}`} onClick=${() => ed.mutate((x) => { x.variables.splice(i, 1); })} />` : null}</td></tr>`)}</tbody></table></div>` : html`<${Empty} icon="sliders" title="No variables yet" action=${canEdit ? html`<${Btn} kind="primary" icon="plus" onClick=${addVar}>Add variable<//>` : null}>Add typed variables with values per stage. Secret variables point to a stored secret and never contain its value.<//>`}<//>
    <${Card} class="mt-4" title="Resolved values" sub="What a notebook, pipeline or job sees at one stage." actions=${html`<${Select} label="Stage" size="sm" value=${stage} onChange=${setStage} options=${stages} />`}>
      ${ed.dirty ? html`<div class="small muted">Save your changes to preview the resolved values.</div>` : resolved.loading ? html`<${Loading} />` : resolved.error ? html`<${ErrorBox} error=${resolved.error} />` : resolved.data ? html`<${Table} dense caption="Resolved variables" rows=${resolved.data.variables.map((v) => ({ ...v, id: v.name }))} columns=${[{ key: 'n', label: 'Variable', render: (v) => html`<span class="mono">${v.name}</span>` }, { key: 'v', label: 'Value', render: (v) => (v.type === 'secret' ? html`<span class="row"><${Icon} name="lock" size="sm" /> ${sn[v.value] || 'secret'} <span class="small muted">(value hidden)</span></span>` : html`<span class="mono">${typeof v.value === 'object' ? JSON.stringify(v.value) : String(v.value)}</span>`) }, { key: 's', label: 'From', render: (v) => html`<${Badge} kind=${v.source === 'override' ? 'warn' : ''}>${v.source}<//>` }]} />` : null}<//>`}</${DetailShell}>`;
}

// ---------------------------------------------------------------------------------------------------------------------------------------- spark job definitions
export function SparkJobsPage() {
  return html`<${ItemListPage} type="spark_job_definition" title="Spark job definitions" icon="zap" createLabel="Spark job" createBody=${() => ({ language: 'python', code: "df = spark.range(100)\nprint(df.count())\n", args: [], settings: {} })}
    sub="Saved Spark applications (Python, Scala or Java) you can run on demand, from a pipeline or on a schedule."
    empty="Create one to run a Spark application with fixed settings." columns=${[]} />`;
}
function SparkRunDialog({ item, close }) {
  const [name, setName] = useState('');
  const [params, setParams] = useState('');
  const [conf, setConf] = useState({});
  const [busy, setBusy] = useState(false);
  let pj = {}; let perr = '';
  if (params.trim()) { try { pj = JSON.parse(params); if (!pj || typeof pj !== 'object' || Array.isArray(pj)) throw new Error('not an object'); } catch (e) { perr = 'Parameters must be a JSON object'; } }
  async function go() {
    setBusy(true);
    const r = await act(() => api.post(`/spark-job-definitions/${enc(item.id)}/run`, { parameters: pj, conf, ...(name.trim() ? { name: name.trim() } : {}) }), { ok: 'Job submitted' });
    setBusy(false);
    if (r) { close(r); window.location.hash = jobHref(r.id); }
  }
  return html`<${Dialog} title=${`Run ${item.name}`} onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" icon="play" busy=${busy} disabled=${!!perr} onClick=${go} data-testid="spark-run-confirm">Run job<//>`}>
    <div class="col gap-3"><${Input} label="Job name (optional)" value=${name} onInput=${setName} /><${Textarea} label="Parameters (JSON)" mono rows=${3} value=${params} onInput=${setParams} error=${perr} placeholder='{"run_date": "2026-01-01"}' hint="Available to the job as variables." />
      <${KvEditor} label="Extra Spark settings for this run" value=${conf} onChange=${setConf} keyLabel="spark.executor.memory" /></div></${Dialog}>`;
}
export function SparkJobDetailPage({ params }) {
  const id = params.id;
  const ed = useItemEditor(id);
  const [tab, setTab] = useQueryState('tab', 'definition');
  const runs = useAsync(() => api.get(`/spark-job-definitions/${enc(id)}/runs`, { limit: 30 }).then((r) => r.items), [id, tab]);
  const lhs = useAsync(() => (ed.item.data ? api.get(`/workspaces/${enc(ed.item.data.workspace_id)}/items`, { type: 'lakehouse', limit: 100 }).then((r) => r.items) : []), [ed.item.data && ed.item.data.workspace_id]);
  const envs = useItems(ed.item.data && ed.item.data.workspace_id, 'environment');
  const d = ed.draft;
  const it = ed.item.data;
  const ws = it && it.workspace_id;
  const canEdit = ws ? can('item.update', ws) : false;
  const [val, setVal] = useState(null);
  const s = (d && d.settings) || {};
  const setS = (fn) => ed.mutate((x) => { x.settings = x.settings || {}; fn(x.settings); });
  const inline = d && d.code !== undefined && d.language === 'python';
  async function validate() { setVal(null); const r = await act(() => api.get(`/spark-job-definitions/${enc(id)}/validate`)); if (r) { setVal(r); setTab('validate'); } }
  async function run() { if (ed.dirty) { toast.warn('Save your changes first', 'A run uses the saved definition.'); return; } await openOverlay(({ close }) => html`<${SparkRunDialog} item=${it} close=${close} />`); }
  const res = s.resources || {};
  const num = (v) => (v === '' ? undefined : Number(v));
  return html`<${DetailShell} ed=${ed} icon="zap" title="Spark job definition" canEdit=${canEdit} actions=${html`<${Btn} icon="check" onClick=${validate} data-testid="spark-validate">Validate<//>${can('session.use', ws) || can('job.submit', ws) ? html`<${Btn} icon="play" onClick=${run} data-testid="spark-run">Run<//>` : null}`}>${() => html`
    <${Tabs} value=${tab} onChange=${setTab} tabs=${[{ id: 'definition', label: 'Definition' }, { id: 'settings', label: 'Settings' }, { id: 'validate', label: 'Validation' }, { id: 'runs', label: 'Runs' }, { id: 'json', label: 'JSON' }]} />
    <div class="mt-4">
    <${TabPanel} id="definition" value=${tab}><div class="col gap-3">
      <div class="grid c3"><${Select} label="Language" value=${d.language || 'python'} onChange=${(v) => ed.mutate((x) => { x.language = v; if (v === 'python') { delete x.main_class; if (!x.main_file && x.code === undefined) x.code = ''; } else { delete x.code; } })} options=${['python', 'scala', 'java']} disabled=${!canEdit} />
        ${(d.language || 'python') === 'python' ? html`<${Select} label="Source" value=${inline ? 'inline' : 'file'} onChange=${(v) => ed.mutate((x) => { if (v === 'inline') { delete x.main_file; x.code = x.code || ''; } else { delete x.code; x.main_file = x.main_file || 'Files/jobs/main.py'; } })} options=${[{ value: 'inline', label: 'Code written here' }, { value: 'file', label: 'A file in a lakehouse' }]} disabled=${!canEdit} />` : html`<${Input} label="Main class" mono value=${d.main_class || ''} onInput=${(v) => ed.mutate((x) => { x.main_class = v; })} placeholder="com.example.Main" />`}
        ${!inline ? html`<${Input} label=${(d.language || 'python') === 'python' ? 'Main file (.py)' : 'Main file (.jar)'} mono value=${d.main_file || ''} onInput=${(v) => ed.mutate((x) => { x.main_file = v; })} placeholder="Files/jobs/main.py" />` : null}</div>
      ${inline ? html`<${Field} label="Code"><div class="json-field"><${CodeEditor} value=${d.code || ''} mode="python" onChange=${(v) => ed.mutate((x) => { x.code = v; })} autoHeight=${true} lineNumbers=${true} readOnly=${!canEdit} label="Spark job code" height=${360} /></div><//>` : null}
      <div class="grid c2"><${Lines} label="Command-line arguments" value=${d.args || []} onChange=${(a) => ed.mutate((x) => { x.args = a; })} hint="One per line." /><${Lines} label="Reference files" value=${d.reference_files || []} onChange=${(a) => ed.mutate((x) => { x.reference_files = a; })} hint="Extra .py, .zip or .jar files, one per line." /></div></div><//>
    <${TabPanel} id="settings" value=${tab}><div class="grid c2">
      <${Card} title="Where it runs"><div class="col gap-3">
        <${Select} label="Default lakehouse" value=${((s.default_lakehouse || {}).name) || ''} onChange=${(v) => setS((x) => { if (v) x.default_lakehouse = { name: v }; else delete x.default_lakehouse; })} placeholder="None" options=${(lhs.data || []).map((l) => ({ value: l.name, label: l.name }))} disabled=${!canEdit} />
        <${Select} label="Environment" value=${s.environment_id || ''} onChange=${(v) => setS((x) => { if (v) x.environment_id = v; else delete x.environment_id; })} placeholder="None" options=${(envs.data || []).map((e) => ({ value: e.id, label: e.name }))} disabled=${!canEdit} />
        <div class="grid c2"><${Input} label="Retries" type="number" min="0" max="10" value=${((s.policy || {}).retries) ?? ''} onInput=${(v) => setS((x) => { x.policy = { ...(x.policy || {}) }; if (v === '') delete x.policy.retries; else x.policy.retries = Number(v); })} /><${Input} label="Timeout (seconds)" type="number" min="1" value=${((s.policy || {}).timeout_seconds) ?? ''} onInput=${(v) => setS((x) => { x.policy = { ...(x.policy || {}) }; if (v === '') delete x.policy.timeout_seconds; else x.policy.timeout_seconds = Number(v); })} /></div></div><//>
      <${Card} title="Resources"><div class="grid c2">${[['driver_memory_mb', 'Driver memory (MB)'], ['executor_memory_mb', 'Executor memory (MB)'], ['driver_cores', 'Driver cores'], ['executor_cores', 'Executor cores'], ['min_executors', 'Minimum executors'], ['max_executors', 'Maximum executors']].map(([k, l]) => html`<${Input} key=${k} label=${l} type="number" min="1" value=${res[k] ?? ''} onInput=${(v) => setS((x) => { x.resources = { ...(x.resources || {}) }; if (v === '') delete x.resources[k]; else x.resources[k] = num(v); })} disabled=${!canEdit} />`)}</div><//>
      <${Card} title="Spark configuration" class="span-2"><${KvEditor} label="Settings" value=${((s.runtime || {}).spark_conf) || {}} onChange=${(o) => setS((x) => { x.runtime = { ...(x.runtime || {}), spark_conf: o }; })} keyLabel="spark.sql.shuffle.partitions" /><//></div><//>
    <${TabPanel} id="validate" value=${tab}>${val ? html`<${Banner} kind="ok" title="The definition is valid">This is the job a run would submit. Nothing was started.<//><div class="grid c2 mt-3"><${Card} title="Normalised definition"><${JsonView} value=${val.definition} depth=${3} /><//><${Card} title="Job submission"><${JsonView} value=${val.submission} depth=${2} /><//></div>` : html`<${Empty} icon="check" title="Not validated yet" action=${html`<${Btn} kind="primary" onClick=${validate}>Validate now<//>`}>Validation shows exactly the job a run would submit.<//>`}<//>
    <${TabPanel} id="runs" value=${tab}>${runs.loading ? html`<${Loading} />` : runs.error ? html`<${ErrorBox} error=${runs.error} />` : html`<${Table} caption="Runs" rows=${runs.data} onRowClick=${(j) => { window.location.hash = jobHref(j.id); }} empty=${html`<${Empty} icon="play-all" title="Not run yet">Use Run to submit it.<//>`} columns=${[{ key: 's', label: 'State', render: (j) => html`<${Badge} kind=${j.state === 'succeeded' ? 'ok' : j.state === 'failed' ? 'bad' : ''}>${titleCase(j.state)}<//>` }, { key: 'id', label: 'Job', render: (j) => html`<a href=${jobHref(j.id)} class="mono small">${shortId(j.id, 10)}</a>` }, { key: 'c', label: 'Submitted', render: (j) => html`<${Ago} ts=${j.created_at} />` }]} />`}<//>
    <${TabPanel} id="json" value=${tab}><${JsonBox} value=${d} onCommit=${(v) => ed.setDraft(v)} label="Spark job definition" readOnly=${!canEdit} /><//>
    </div>`}</${DetailShell}>`;
}
