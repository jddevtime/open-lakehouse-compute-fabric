// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Pipelines: list and runs, the designer (palette, graph, inspector, JSON, live validation), run detail with the run graph and timeline,
// schedules and event triggers.
import { html, useState, useEffect, useRef, useMemo, useAsync, usePoll, useDebounced } from '../core/h.js';
import { api, can } from '../api/client.js';
import { Btn, Badge, Banner, Empty, ErrorBox, Loading, Tabs, TabPanel, Kv, Card, PageHead, Select, Input, Textarea, Check, Switch, Field, Code } from '../ui/basic.js';
import { Table, JsonView } from '../ui/data.js';
import { Icon } from '../ui/icons.js';
import { Dag, activityCategory, hasNode } from '../ui/dag.js';
import { Dialog, MenuButton, openOverlay, confirmDialog, promptDialog, act, toast } from '../ui/overlay.js';
import { enc, fmtDuration, fmtTime, durationBetween, titleCase, shortId, clone, jsonPretty, downloadText, copyText } from '../core/util.js';
import { useQueryState, Ago, useWorkspaceId, InlineWorkspaceSelect, useItems } from './common.js';
import { jobHref, runHref, itemHref } from '../core/links.js';
import { JobState } from './jobs.js';
import { CodeEditor } from '../notebook/cm.js';
import { SchedulesPanel, TriggersPanel } from './triggers.js';

// ---------------------------------------------------------------------------------------------------------------------------------------- definitions
const SCHEMA = 'olcf.pipeline/1';
const SKELETON = {
  wait: { waitTimeInSeconds: 5 },
  sql: { sql: 'SELECT 1 AS ok', lakehouse: '' },
  notebook: { notebook: '', parameters: {} },
  spark_job: { sparkJob: '', parameters: {} },
  script: { language: 'python', code: "print('hello from a pipeline')" },
  copy: { source: { lakehouse: '', path: 'Files/in', format: 'csv' }, sink: { lakehouse: '', table: 'target', format: 'delta', mode: 'overwrite' } },
  http: { url: 'https://example.com/health', method: 'GET' },
  webhook_wait: { url: 'https://example.com/start', method: 'POST', timeoutSeconds: 600 },
  set_variable: { variable: '', value: '' },
  fail: { message: 'Stopped on purpose', errorCode: 'E_STOP' },
  if: { expression: "@equals(1, 1)", ifTrueActivities: [], ifFalseActivities: [] },
  switch: { on: "@pipeline().parameters.mode", cases: [{ value: 'a', activities: [] }], defaultActivities: [] },
  for_each: { items: '@array(pipeline().parameters.items)', activities: [] },
  until: { expression: "@equals(1, 1)", activities: [], maxIterations: 10 },
  child_pipeline: { pipeline: '', parameters: {}, waitOnCompletion: true },
};
const GROUPS = [['Compute', ['notebook', 'spark_job', 'script', 'sql']], ['Data and services', ['copy', 'http', 'webhook_wait']], ['Flow', ['if', 'switch', 'for_each', 'until', 'child_pipeline']], ['Utilities', ['wait', 'set_variable', 'fail']]];
const TEMPLATES = {
  blank: { label: 'Blank', def: () => ({ schema: SCHEMA, description: '', parameters: {}, variables: {}, activities: [{ name: 'Wait', type: 'wait', typeProperties: { waitTimeInSeconds: 1 }, dependsOn: [] }], outputs: {} }) },
  query: { label: 'Query then branch', def: () => ({ schema: SCHEMA, description: 'Count rows, then take one of two paths.', parameters: { year: { type: 'int', default: 2025 } }, variables: { msg: { type: 'string', default: '' } },
    activities: [{ name: 'Count orders', type: 'sql', typeProperties: { sql: 'SELECT count(*) AS n FROM fact_sales WHERE year = @{pipeline().parameters.year}' }, dependsOn: [] },
      { name: 'Set message', type: 'set_variable', typeProperties: { variable: 'msg', value: 'done' }, dependsOn: [{ activity: 'Count orders', conditions: ['Succeeded'] }] },
      { name: 'Stop on failure', type: 'fail', typeProperties: { message: 'counting failed', errorCode: 'E_COUNT' }, dependsOn: [{ activity: 'Count orders', conditions: ['Failed'] }] }], outputs: { msg: "@variables('msg')" } }) },
};
const SCOPE_KEYS = ['ifTrueActivities', 'ifFalseActivities', 'activities', 'defaultActivities'];
const uniqueName = (base, taken) => { let n = base; let i = 2; while (taken.includes(n)) { n = `${base} ${i}`; i += 1; } return n; };

/** All activity arrays of a definition, as [array, parentPath] pairs. */
function scopesOf(act) {
  const tp = act.typeProperties || {};
  const out = [];
  SCOPE_KEYS.forEach((k) => { if (Array.isArray(tp[k])) out.push(tp[k]); });
  if (Array.isArray(tp.cases)) tp.cases.forEach((c) => { if (c && Array.isArray(c.activities)) out.push(c.activities); });
  return out;
}
/** Find an activity by node id ("Parent/Child"): returns {arr, idx, act}. */
function findAct(def, nodeId) {
  if (!nodeId) return null;
  const parts = nodeId.split('/');
  let arrs = [def.activities || []];
  let found = null;
  for (const part of parts) {
    found = null;
    for (const arr of arrs) { const i = arr.findIndex((a) => a && a.name === part); if (i >= 0) { found = { arr, idx: i, act: arr[i] }; break; } }
    if (!found) return null;
    arrs = scopesOf(found.act);
  }
  return found;
}
function nodeIdOfPath(def, path) {
  const toks = [...String(path).matchAll(/([A-Za-z_][A-Za-z0-9_]*)|\[(\d+)\]/g)].map((m) => (m[2] != null ? Number(m[2]) : m[1]));
  let cur = def; let id = null;
  for (const t of toks) {
    if (cur == null || typeof cur !== 'object') break;
    cur = cur[t];
    if (typeof t === 'number' && cur && typeof cur === 'object' && !Array.isArray(cur) && typeof cur.name === 'string' && typeof cur.type === 'string') id = id ? `${id}/${cur.name}` : cur.name;
  }
  return id;
}
const condList = (c) => (Array.isArray(c) ? c : [c]).filter(Boolean).map((x) => titleCase(String(x).toLowerCase()));

// ---------------------------------------------------------------------------------------------------------------------------------------- shared
export function RunsTable({ runs, showPipeline = true, empty }) {
  return html`<${Table} caption="Pipeline runs" rows=${runs} onRowClick=${(r) => { window.location.hash = runHref(r.id); }} empty=${empty || html`<${Empty} icon="workflow" title="No runs yet">Start a pipeline and its runs appear here.<//>`} initialSort=${{ key: 'created', dir: -1 }} columns=${[
    { key: 'state', label: 'State', render: (r) => html`<${JobState} state=${r.state} />`, sort: (r) => r.state },
    showPipeline && { key: 'pipeline', label: 'Pipeline', render: (r) => html`<a href=${`#/pipelines/${enc(r.pipeline_id)}`}><b>${r.pipeline_name}</b></a><div class="small muted">version ${r.pipeline_version}${r.depth ? ' · child run' : ''}</div>`, sort: (r) => r.pipeline_name },
    { key: 'id', label: 'Run', render: (r) => html`<a href=${runHref(r.id)} class="mono small">${shortId(r.id, 10)}</a>`, sortable: false },
    { key: 'trigger', label: 'Started by', render: (r) => html`${titleCase(r.trigger_type)}<div class="small muted">${(r.trigger || {}).by || ''}</div>`, sort: (r) => r.trigger_type },
    { key: 'created', label: 'Started', render: (r) => html`<${Ago} ts=${r.created_at} />`, sort: (r) => r.created_at },
    { key: 'dur', label: 'Duration', num: true, render: (r) => (r.duration_seconds != null ? fmtDuration(r.duration_seconds) : r.started_at ? fmtDuration(durationBetween(r.started_at, null)) : '–'), sort: (r) => r.duration_seconds || 0 },
  ].filter(Boolean)} />`;
}

function paramInit(spec) { const v = spec.default; return v == null ? (spec.type === 'bool' ? false : '') : (typeof v === 'object' ? jsonPretty(v) : v); }
function RunDialog({ pipeline, definition, close }) {
  const spec = (definition && definition.parameters) || {};
  const names = Object.keys(spec);
  const [vals, setVals] = useState(() => Object.fromEntries(names.map((n) => [n, paramInit(spec[n])])));
  const [policy, setPolicy] = useState('queue');
  const [busy, setBusy] = useState(false);
  const errs = {};
  const out = {};
  names.forEach((n) => {
    const t = spec[n].type; const v = vals[n];
    if (t === 'int') { if (v === '' || v == null) { if (spec[n].default == null) errs[n] = 'Required'; } else if (!/^-?\d+$/.test(String(v))) errs[n] = 'Whole number'; else out[n] = Number(v); }
    else if (t === 'float') { if (v === '' || v == null) { if (spec[n].default == null) errs[n] = 'Required'; } else if (Number.isNaN(Number(v))) errs[n] = 'Number'; else out[n] = Number(v); }
    else if (t === 'bool') out[n] = !!v;
    else if (t === 'array' || t === 'object') { try { const p = JSON.parse(v === '' ? 'null' : v); if (p != null) out[n] = p; } catch { errs[n] = 'Not valid JSON'; } }
    else if (v !== '' && v != null) out[n] = v;
  });
  async function go() {
    setBusy(true);
    const r = await act(() => api.post(`/pipelines/${enc(pipeline.id)}/runs`, { parameters: out, queue_policy: policy }), { ok: 'Run started' });
    setBusy(false);
    if (r) { close(r); window.location.hash = runHref(r.id); }
  }
  return html`<${Dialog} title=${`Run ${pipeline.name}`} onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" icon="play" busy=${busy} disabled=${Object.keys(errs).length > 0} onClick=${go} data-testid="run-confirm">Run pipeline<//>`}>
    ${names.length ? html`<div class="col gap-3">${names.map((n) => { const s = spec[n]; const lab = `${n} (${s.type})`;
      return s.type === 'bool' ? html`<${Check} key=${n} label=${lab} checked=${!!vals[n]} onChange=${(v) => setVals({ ...vals, [n]: v })} />`
        : s.type === 'array' || s.type === 'object' ? html`<${Textarea} key=${n} label=${lab} mono rows=${3} value=${vals[n]} onInput=${(v) => setVals({ ...vals, [n]: v })} error=${errs[n]} />`
        : html`<${Input} key=${n} label=${lab} type=${s.type === 'int' || s.type === 'float' ? 'number' : 'text'} step=${s.type === 'float' ? 'any' : undefined} value=${vals[n]} onInput=${(v) => setVals({ ...vals, [n]: v })} error=${errs[n]} hint=${s.default != null ? `Default ${JSON.stringify(s.default)}` : undefined} />`; })}</div>` : html`<p class="muted">This pipeline has no parameters.</p>`}
    <div class="mt-3"><${Select} label="If it is already running" value=${policy} onChange=${setPolicy} options=${[{ value: 'queue', label: 'Queue this run' }, { value: 'skip', label: 'Skip this run' }, { value: 'cancel_previous', label: 'Cancel the earlier run' }]} /></div></${Dialog}>`;
}
export const openRunDialog = (pipeline, definition) => openOverlay(({ close }) => html`<${RunDialog} pipeline=${pipeline} definition=${definition} close=${close} />`);

// ---------------------------------------------------------------------------------------------------------------------------------------- list
function NewPipeline({ ws, close }) {
  const [name, setName] = useState('');
  const [tpl, setTpl] = useState('query');
  const [busy, setBusy] = useState(false);
  async function go() {
    setBusy(true);
    const r = await act(() => api.post(`/workspaces/${enc(ws)}/items`, { type: 'pipeline', name: name.trim(), definition: TEMPLATES[tpl].def() }), { ok: 'Pipeline created' });
    setBusy(false);
    if (r) close(r);
  }
  return html`<${Dialog} title="New pipeline" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!name.trim()} onClick=${go}>Create pipeline<//>`}>
    <div class="col gap-3"><${Input} label="Name" value=${name} onInput=${setName} placeholder="daily_refresh" required hint="Letters, digits, dots, dashes and underscores." />
      <${Select} label="Start from" value=${tpl} onChange=${setTpl} options=${Object.entries(TEMPLATES).map(([value, t]) => ({ value, label: t.label }))} /></div></${Dialog}>`;
}

export function PipelinesPage() {
  const [ws, setWs, wss] = useWorkspaceId();
  const [tab, setTab] = useQueryState('tab', 'pipelines');
  const [state, setState] = useQueryState('state', '');
  const items = useItems(ws, 'pipeline');
  const runs = usePoll(() => (ws ? api.get('/pipeline-runs', { workspace_id: ws, limit: 100, state: state || undefined }).then((r) => r.items) : []), 4000, [ws, state]);
  const canCreate = can('item.create', ws);
  const last = useMemo(() => { const m = {}; (runs.data || []).forEach((r) => { (m[r.pipeline_id] ||= []).push(r); }); return m; }, [runs.data]);
  async function create() { const r = await openOverlay(({ close }) => html`<${NewPipeline} ws=${ws} close=${close} />`); if (r) window.location.hash = itemHref(r); }
  async function runIt(p) { const d = await api.get(`/items/${enc(p.id)}`).catch(() => null); await openRunDialog(p, d && d.definition); }
  const list = items.data || [];
  return html`<div class="page wide"><${PageHead} title="Pipelines" icon="workflow" sub="Chain notebooks, Spark jobs, SQL and file copies into reliable, schedulable workflows." actions=${html`<${InlineWorkspaceSelect} value=${ws} onChange=${setWs} items=${wss} />${canCreate ? html`<${Btn} kind="primary" icon="plus" onClick=${create} data-testid="new-pipeline">New pipeline<//>` : null}`} />
    <${Tabs} value=${tab} onChange=${setTab} tabs=${[{ id: 'pipelines', label: 'Pipelines', count: list.length }, { id: 'runs', label: 'Runs' }, { id: 'schedules', label: 'Schedules' }, { id: 'triggers', label: 'Triggers' }]} />
    <div class="mt-4">
    <${TabPanel} id="pipelines" value=${tab}>${items.loading && !items.data ? html`<${Loading} />` : items.error ? html`<${ErrorBox} error=${items.error} onRetry=${items.reloadLoud} />` : html`
      <${Table} caption="Pipelines" rows=${list} empty=${html`<${Empty} icon="workflow" title="No pipelines in this workspace" action=${canCreate ? html`<${Btn} kind="primary" icon="plus" onClick=${create}>New pipeline<//>` : null}>Create one, or install the sample from the Home page.<//>`} columns=${[
        { key: 'name', label: 'Pipeline', render: (p) => html`<a href=${itemHref(p)}><b>${p.name}</b></a><div class="small muted">${p.description || ''}</div>`, sort: (p) => p.name },
        { key: 'last', label: 'Last run', render: (p) => { const r = (last[p.id] || [])[0]; return r ? html`<a href=${runHref(r.id)} class="nodecor"><${JobState} state=${r.state} /></a> <span class="small muted"><${Ago} ts=${r.created_at} /></span>` : html`<span class="muted">never</span>`; }, sortable: false },
        { key: 'ok', label: 'Recent runs', render: (p) => { const rs = (last[p.id] || []).slice(0, 12).reverse(); return rs.length ? html`<span class="runstrip" role="img" aria-label=${`${rs.filter((r) => r.state === 'succeeded').length} of ${rs.length} recent runs succeeded`}>${rs.map((r) => html`<i key=${r.id} class=${'rs-' + r.state} title=${`${titleCase(r.state)} · ${fmtTime(r.created_at)}`}></i>`)}</span>` : html`<span class="muted">–</span>`; }, sortable: false },
        { key: 'updated', label: 'Edited', render: (p) => html`<${Ago} ts=${p.updated_at} />`, sort: (p) => p.updated_at },
        { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (p) => html`<div class="row">${can('pipeline.run', ws) ? html`<${Btn} size="sm" icon="play" onClick=${() => runIt(p)} label=${`Run ${p.name}`}>Run<//>` : null}</div>` },
      ]} />`}<//>
    <${TabPanel} id="runs" value=${tab}>
      <div class="toolbar"><${Select} label="State" size="sm" value=${state} onChange=${setState} placeholder="Any state" options=${['queued', 'running', 'succeeded', 'failed', 'cancelled'].map((s) => ({ value: s, label: titleCase(s) }))} /></div>
      ${runs.loading && !runs.data ? html`<${Loading} />` : runs.error && !runs.data ? html`<${ErrorBox} error=${runs.error} onRetry=${runs.reloadLoud} />` : html`<${RunsTable} runs=${runs.data || []} />`}<//>
    <${TabPanel} id="schedules" value=${tab}>${ws ? html`<${SchedulesPanel} ws=${ws} />` : null}<//>
    <${TabPanel} id="triggers" value=${tab}>${ws ? html`<${TriggersPanel} ws=${ws} />` : null}<//>
    </div></div>`;
}

// ---------------------------------------------------------------------------------------------------------------------------------------- designer
function JsonField({ label, value, onCommit, rows = 8, mode = 'json' }) {
  const [text, setText] = useState(() => jsonPretty(value));
  const [err, setErr] = useState('');
  const last = useRef(jsonPretty(value));
  useEffect(() => { const t = jsonPretty(value); if (t !== last.current) { last.current = t; setText(t); setErr(''); } }, [value]);
  function change(t) {
    setText(t);
    try { const v = JSON.parse(t); setErr(''); last.current = jsonPretty(v); onCommit(v); } catch (e) { setErr(e.message); }
  }
  return html`<div class="field"><label>${label}</label><div class=${'json-field' + (err ? ' bad' : '')}><${CodeEditor} value=${text} mode=${mode} onChange=${change} autoHeight=${true} label=${label} /></div>${err ? html`<div class="err" role="alert">${err}</div>` : null}</div>`;
}

function NameField({ value, onCommit, disabled }) {
  const [v, setV] = useState(value);
  useEffect(() => setV(value), [value]);
  return html`<${Input} label="Name" value=${v} onInput=${setV} disabled=${disabled} onBlur=${() => onCommit(v)} onKeyDown=${(e) => { if (e.key === 'Enter') onCommit(v); }} hint="Expressions that mention the old name are not rewritten." />`;
}
function Inspector({ def, nodeId, mutate, errors, catalog, onClose, readOnly }) {
  const f = findAct(def, nodeId);
  if (!f) return html`<${Empty} icon="workflow" title="Select an activity">Click a box in the graph to edit its settings, or add one from the palette.<//>`;
  const a = f.act;
  const meta = (catalog || {})[a.type] || {};
  const sibs = f.arr.filter((x) => x !== a).map((x) => x.name);
  const deps = a.dependsOn || [];
  const myErrs = errors.filter((e) => nodeIdOfPath(def, e.path) === nodeId);
  const at = (fn) => mutate((d) => { const x = findAct(d, nodeId); if (x) fn(x); });
  function rename(v) {
    const nn = v.trim();
    if (!nn || nn === a.name) return;
    at((x) => { x.arr.forEach((o) => (o.dependsOn || []).forEach((dd) => { if (dd.activity === x.act.name) dd.activity = nn; })); x.act.name = nn; });
  }
  const setDep = (name, on, conds) => at((x) => {
    const cur = (x.act.dependsOn || []).filter((d) => d.activity !== name);
    if (on) cur.push({ activity: name, conditions: conds && conds.length ? conds : ['Succeeded'] });
    x.act.dependsOn = cur;
  });
  const pol = a.policy || {};
  const setPol = (k, v) => at((x) => { const p = { ...(x.act.policy || {}) }; if (v === '' || v == null) delete p[k]; else p[k] = Number(v); x.act.policy = p; });
  function remove() {
    at((x) => { const nm = x.act.name; x.arr.splice(x.idx, 1); x.arr.forEach((o) => { if (o.dependsOn) o.dependsOn = o.dependsOn.filter((d) => d.activity !== nm); }); });
    onClose();
  }
  return html`<div class="inspector-body" data-testid="inspector">
    <div class="row"><span class=${'cat-dot cat-' + activityCategory(a.type)}></span><b class="grow">${a.name}</b><${Badge} mono>${a.type}<//></div>
    ${meta.description ? html`<p class="small muted mt-2">${meta.description}</p>` : null}
    ${myErrs.length ? html`<div class="col gap-1 mt-2">${myErrs.map((e, i) => html`<${Banner} key=${i} kind=${e.severity === 'warning' ? 'warn' : 'bad'}><span class="mono small">${e.path.replace(/^.*?typeProperties\./, '')}</span> ${e.message}<//>`)}</div>` : null}
    <div class="col gap-3 mt-3">
      <${NameField} key=${nodeId} value=${a.name} onCommit=${rename} disabled=${readOnly} />
      ${sibs.length ? html`<fieldset class="fieldset"><legend>Runs after</legend><div class="col gap-2">${sibs.map((s) => { const d = deps.find((x) => x.activity === s); const cs = d ? condList(d.conditions) : [];
        return html`<div key=${s} class="dep-row"><${Check} label=${s} checked=${!!d} disabled=${readOnly} onChange=${(on) => setDep(s, on)} />
          ${d ? html`<span class="chips" role="group" aria-label=${`Conditions for ${s}`}>${['Succeeded', 'Failed', 'Skipped', 'Completed'].map((c) => html`<button key=${c} type="button" class="chip sm" disabled=${readOnly} aria-pressed=${cs.includes(c)} onClick=${() => { const n = cs.includes(c) ? cs.filter((x) => x !== c) : [...cs, c]; setDep(s, true, n.length ? n : ['Succeeded']); }}>${c}</button>`)}</span>` : null}</div>`; })}</div></fieldset>` : null}
      <${JsonField} key=${nodeId + 'tp'} label="Settings (typeProperties)" value=${a.typeProperties || {}} onCommit=${(v) => at((x) => { x.act.typeProperties = v; })} />
      <div class="grid c3"><${Input} label="Retries" type="number" min="0" value=${pol.retries ?? ''} onInput=${(v) => setPol('retries', v)} disabled=${readOnly} /><${Input} label="Retry wait (s)" type="number" min="0" value=${pol.retryBackoffSeconds ?? ''} onInput=${(v) => setPol('retryBackoffSeconds', v)} disabled=${readOnly} /><${Input} label="Timeout (s)" type="number" min="1" value=${pol.timeoutSeconds ?? ''} onInput=${(v) => setPol('timeoutSeconds', v)} disabled=${readOnly} /></div>
      ${readOnly ? null : html`<div class="row"><${Btn} kind="danger" icon="trash" onClick=${remove}>Remove activity<//></div>`}
    </div></div>`;
}

function Palette({ catalog, onAdd, disabled }) {
  return html`<div class="act-pal" role="group" aria-label="Add an activity">${GROUPS.map(([g, types]) => html`<div key=${g} class="pal-group"><div class="pal-title small muted">${g}</div>
    ${types.filter((t) => catalog[t]).map((t) => html`<button key=${t} type="button" class="pal-item" disabled=${disabled} title=${catalog[t].description} onClick=${() => onAdd(t)} data-testid=${'add-' + t}><span class=${'cat-dot cat-' + activityCategory(t)}></span><span>${catalog[t].title}</span><${Icon} name="plus" size="sm" /></button>`)}</div>`)}</div>`;
}

export function PipelineDetailPage({ params }) {
  const id = params.id;
  const item = useAsync(() => api.get(`/items/${enc(id)}`), [id]);
  const types = useAsync(() => api.get('/pipelines/activity-types'), []);
  const [tab, setTab] = useQueryState('tab', 'design');
  const [draft, setDraft] = useState(null);
  const [sel, setSel] = useState(null);
  const [saving, setSaving] = useState(false);
  const saved = useRef('');
  const undo = useRef([]);
  const ws = item.data && item.data.workspace_id;
  const canEdit = ws ? can('item.update', ws) : false;
  useEffect(() => { if (item.data && item.data.definition) { const d = clone(item.data.definition); saved.current = JSON.stringify(d); setDraft(d); undo.current = []; } }, [item.data && item.data.etag]);
  const text = draft ? JSON.stringify(draft) : '';
  const dirty = !!draft && text !== saved.current;
  const dtext = useDebounced(text, 350);
  const lastGood = useRef(null);
  const val = useAsync(async () => (dtext ? api.post('/pipelines/validate', { definition: JSON.parse(dtext) }) : null), [dtext]);
  if (val.data && val.data.graph) lastGood.current = val.data.graph;
  const catalog = useMemo(() => Object.fromEntries(((types.data && types.data.items) || []).map((t) => [t.type, t])), [types.data]);
  const graph = lastGood.current || (draft ? { nodes: [], edges: [] } : null);
  const problems = (val.data && [...(val.data.errors || []), ...(val.data.warnings || [])]) || [];
  const errNodes = useMemo(() => new Set(problems.filter((e) => e.severity !== 'warning').map((e) => nodeIdOfPath(draft || {}, e.path)).filter(Boolean)), [val.data, text]);
  function mutate(fn) { setDraft((d) => { undo.current.push(JSON.stringify(d)); if (undo.current.length > 60) undo.current.shift(); const n = clone(d); fn(n); return n; }); }
  function doUndo() { const t = undo.current.pop(); if (t) setDraft(JSON.parse(t)); }
  function add(type) {
    mutate((d) => {
      const taken = (d.activities || []).map((a) => a.name);
      const prior = sel && findAct(d, sel);
      const name = uniqueName(catalog[type] ? catalog[type].title : titleCase(type), taken);
      const a = { name, type, typeProperties: clone(SKELETON[type] || {}), dependsOn: [], policy: {} };
      if (prior && prior.arr === d.activities) a.dependsOn = [{ activity: prior.act.name, conditions: ['Succeeded'] }];
      else if (!prior && d.activities.length) a.dependsOn = [];
      d.activities = d.activities || [];
      d.activities.push(a);
      setSel(name);
    });
  }
  async function save() {
    const comment = 'edited in the console';
    setSaving(true);
    const r = await act(() => api.put(`/items/${enc(id)}`, { definition: draft, comment }, { ifMatch: item.data.etag }), { ok: 'Pipeline saved' });
    setSaving(false);
    if (r) item.reload();
  }
  async function rename() { const n = await promptDialog({ title: 'Rename pipeline', label: 'Name', value: item.data.name, confirmLabel: 'Rename' }); if (n && n !== item.data.name && (await act(() => api.put(`/items/${enc(id)}`, { name: n, comment: 'renamed' }, { ifMatch: item.data.etag }), { ok: 'Renamed' }))) item.reload(); }
  async function del() { if (await confirmDialog({ title: `Delete ${item.data.name}?`, danger: true, confirmLabel: 'Move to recycle bin', body: 'Its schedules stop firing. You can restore it from the workspace recycle bin.' }) && (await act(() => api.del(`/items/${enc(id)}`), { ok: 'Moved to the recycle bin' })) !== undefined) window.location.hash = '#/pipelines'; }
  async function yaml() { const t = await api.text(`/pipelines/${enc(id)}/definition`, { format: 'yaml' }).catch(() => null); if (t) downloadText(t, `${item.data.name}.yaml`, 'application/yaml'); else toast.bad('Export failed', 'The YAML export is not available.'); }
  if (item.loading && !item.data) return html`<div class="page"><${Loading} /></div>`;
  if (item.error) return html`<div class="page"><${ErrorBox} error=${item.error} onRetry=${item.reloadLoud} /></div>`;
  const it = item.data;
  if (it.type !== 'pipeline') return html`<div class="page"><${Banner} kind="warn" title="This item is not a pipeline">Open it from its workspace.<//></div>`;
  const nerr = problems.filter((e) => e.severity !== 'warning').length;
  const valid = val.data ? val.data.valid : null;
  return html`<div class="page wide"><${PageHead} title=${it.name} icon="workflow" sub=${(draft && draft.description) || it.description || 'Pipeline'} badge=${html`${dirty ? html`<${Badge} kind="warn">Unsaved changes<//>` : html`<${Badge}>${"Version " + it.version}<//>`} ${valid === true ? html`<${Badge} kind="ok">Valid<//>` : valid === false ? html`<${Badge} kind="bad">${nerr} problem${nerr === 1 ? '' : 's'}<//>` : null}`}
    actions=${html`${can('pipeline.run', ws) ? html`<${Btn} icon="play" onClick=${() => openRunDialog(it, it.definition)} data-testid="run-pipeline" title=${dirty ? 'Runs the saved version' : undefined}>Run<//>` : null}
      ${canEdit ? html`<${Btn} icon="undo" disabled=${!undo.current.length} onClick=${doUndo}>Undo<//><${Btn} disabled=${!dirty} onClick=${() => { setDraft(JSON.parse(saved.current)); undo.current = []; }}>Revert<//><${Btn} kind="primary" icon="save" busy=${saving} disabled=${!dirty} onClick=${save} data-testid="pipeline-save">Save<//>` : null}
      <${MenuButton} label="More actions" items=${[{ label: 'Download as YAML', icon: 'download', onClick: yaml }, canEdit && { label: 'Rename', icon: 'edit', onClick: rename }, can('item.delete', ws) && { sep: true }, can('item.delete', ws) && { label: 'Delete', icon: 'trash', danger: true, onClick: del }]} />`} />
    <${Tabs} value=${tab} onChange=${setTab} tabs=${[{ id: 'design', label: 'Design' }, { id: 'json', label: 'Definition' }, { id: 'runs', label: 'Runs' }, { id: 'schedules', label: 'Schedules' }, { id: 'triggers', label: 'Triggers' }]} />
    <div class="mt-4">
    <${TabPanel} id="design" value=${tab}>${!draft ? html`<${Loading} />` : html`
      <div class="designer-pipe">
        <aside class="pipe-pal" aria-label="Activities"><h3 class="small muted">Add activity</h3>${types.data ? html`<${Palette} catalog=${catalog} onAdd=${add} disabled=${!canEdit} />` : html`<${Loading} />`}</aside>
        <div class="pipe-canvas"><${Dag} graph=${graph} selected=${sel} onSelect=${setSel} errors=${errNodes} height=${560} label=${`Graph of ${it.name}`} empty=${html`<${Empty} icon="workflow" title="No activities">Add the first activity from the palette.<//>`} /></div>
        <aside class="pipe-inspector" aria-label="Activity settings"><${Inspector} def=${draft} nodeId=${sel} mutate=${mutate} errors=${problems} catalog=${catalog} onClose=${() => setSel(null)} readOnly=${!canEdit} /></aside></div>
      <${Problems} problems=${problems} def=${draft} onPick=${(n) => setSel(n)} loading=${val.loading} />`}<//>
    <${TabPanel} id="json" value=${tab}>${draft ? html`<div class="row mb-2"><span class="muted small grow">The definition is plain JSON (${SCHEMA}). Changes here update the designer instantly.</span><${Btn} size="sm" onClick=${() => copyText(JSON.stringify(draft, null, 2))}>Copy<//></div><div class="json-field"><${JsonDefinition} value=${draft} onCommit=${(v) => mutate((d) => { Object.keys(d).forEach((k) => delete d[k]); Object.assign(d, v); })} readOnly=${!canEdit} /></div><${Problems} problems=${problems} def=${draft} onPick=${(n) => { setSel(n); setTab('design'); }} loading=${val.loading} />` : null}<//>
    <${TabPanel} id="runs" value=${tab}><${PipelineRuns} id=${id} /><//>
    <${TabPanel} id="schedules" value=${tab}><${SchedulesPanel} ws=${ws} targetId=${id} /><//>
    <${TabPanel} id="triggers" value=${tab}><${TriggersPanel} ws=${ws} targetId=${id} /><//>
    </div></div>`;
}
function JsonDefinition({ value, onCommit, readOnly }) {
  const [text, setText] = useState(() => JSON.stringify(value, null, 2));
  const [err, setErr] = useState('');
  const last = useRef(JSON.stringify(value));
  useEffect(() => { const t = JSON.stringify(value); if (t !== last.current) { last.current = t; setText(JSON.stringify(value, null, 2)); setErr(''); } }, [value]);
  function change(t) {
    setText(t);
    try { const v = JSON.parse(t); setErr(''); last.current = JSON.stringify(v); onCommit(v); } catch (e) { setErr(e.message); }
  }
  return html`<div><${CodeEditor} value=${text} mode="json" onChange=${change} autoHeight=${true} lineNumbers=${true} readOnly=${readOnly} label="Pipeline definition" />${err ? html`<div class="err p-3" role="alert">Not valid JSON yet: ${err}</div>` : null}</div>`;
}
function Problems({ problems, def, onPick, loading }) {
  if (!problems.length) return html`<div class="small muted mt-3" aria-live="polite">${loading ? 'Checking the definition…' : 'The definition is valid.'}</div>`;
  return html`<${Card} class="mt-4" title="Problems" sub=${`${problems.length} found while checking the definition`}><ul class="problems" data-testid="problems">${problems.map((e, i) => { const n = nodeIdOfPath(def, e.path);
    return html`<li key=${i} class=${e.severity === 'warning' ? 'warn' : 'bad'}><${Icon} name=${e.severity === 'warning' ? 'alert-triangle' : 'alert-circle'} size="sm" /><span class="grow"><span class="mono small muted">${e.path}</span><br />${e.message}</span>${n ? html`<${Btn} size="sm" onClick=${() => onPick(n)}>Show<//>` : null}</li>`; })}</ul><//>`;
}
function PipelineRuns({ id }) {
  const runs = usePoll(() => api.get(`/pipelines/${enc(id)}/runs`, { limit: 50 }).then((r) => r.items), 4000, [id]);
  if (runs.loading && !runs.data) return html`<${Loading} />`;
  if (runs.error && !runs.data) return html`<${ErrorBox} error=${runs.error} onRetry=${runs.reloadLoud} />`;
  return html`<${RunsTable} runs=${runs.data} showPipeline=${false} />`;
}

// ---------------------------------------------------------------------------------------------------------------------------------------- run detail
const FINISHED = ['succeeded', 'failed', 'cancelled', 'timed_out', 'skipped'];

function Gantt({ items, total }) {
  if (!items.length) return html`<${Empty} icon="clock" title="Nothing has started yet" />`;
  const t0 = Math.min(...items.map((i) => new Date(i.started_at).getTime()));
  const t1 = Math.max(...items.map((i) => new Date(i.finished_at || Date.now()).getTime()), t0 + 1);
  const W = 900; const LW = 190; const RH = 26; const span = t1 - t0;
  const x = (t) => LW + ((t - t0) / span) * (W - LW - 16);
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((f) => ({ f, t: t0 + f * span }));
  return html`<div class="gantt-wrap"><svg class="gantt" viewBox=${`0 0 ${W} ${items.length * RH + 30}`} role="img" aria-label=${`Timeline of ${items.length} activities, ${fmtDuration(span / 1000)} in total`}>
    ${ticks.map((k) => html`<g key=${k.f}><line class="gt-grid" x1=${x(k.t)} x2=${x(k.t)} y1="18" y2=${items.length * RH + 24} /><text class="gt-tick" x=${x(k.t)} y="11" text-anchor=${k.f === 1 ? 'end' : k.f === 0 ? 'start' : 'middle'}>${fmtDuration((k.t - t0) / 1000)}</text></g>`)}
    ${items.map((it, i) => { const a = new Date(it.started_at).getTime(); const b = new Date(it.finished_at || Date.now()).getTime();
      return html`<g key=${it.id} transform=${`translate(0,${24 + i * RH})`}><text class="gt-label" x=${LW - 8} y="14" text-anchor="end">${(it.node || '').split('/').pop().slice(0, 26)}</text>
        <rect class=${'gt-bar s-' + it.state} x=${x(a)} y="3" width=${Math.max(3, x(b) - x(a))} height="16" rx="3"><title>${`${it.node}: ${titleCase(it.state)}, ${fmtDuration((it.duration_ms || b - a) / 1000)}`}</title></rect>
        <text class="gt-dur" x=${Math.min(W - 40, x(b) + 6)} y="15">${fmtDuration((it.duration_ms || b - a) / 1000)}</text></g>`; })}</svg></div>`;
}

function ResumeDialog({ run, graph, close }) {
  const failed = (graph.nodes || []).filter((n) => !n.parent && ['failed', 'cancelled', 'timed_out', 'skipped'].includes(n.state));
  const [sel, setSel] = useState(() => new Set((graph.nodes || []).filter((n) => !n.parent && ['failed', 'timed_out'].includes(n.state)).map((n) => n.name)));
  const [latest, setLatest] = useState(false);
  const [busy, setBusy] = useState(false);
  const toggle = (n) => setSel((s) => { const x = new Set(s); if (x.has(n)) x.delete(n); else x.add(n); return x; });
  async function go() {
    setBusy(true);
    const r = await act(() => api.post(`/pipeline-runs/${enc(run.id)}/resume`, { rerun: [...sel], latest_definition: latest, reuse: true }), { ok: 'Resumed as a new run' });
    setBusy(false);
    if (r) close(r);
  }
  return html`<${Dialog} title="Resume from failure" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" icon="play" busy=${busy} onClick=${go} data-testid="resume-confirm">Resume<//>`}>
    <p class="muted">A new run starts in the same group and reuses every activity that succeeded. Pick the activities that should run again; anything that depends on them runs again too.</p>
    ${failed.length ? html`<fieldset class="fieldset"><legend>Run again</legend><div class="col gap-1">${failed.map((n) => html`<${Check} key=${n.id} label=${html`${n.name} <span class="muted small">${titleCase(n.state)}</span>`} checked=${sel.has(n.name)} onChange=${() => toggle(n.name)} />`)}</div></fieldset>` : html`<p class="small muted">No failed activities were found; the unfinished ones run again.</p>`}
    <div class="mt-3"><${Switch} label="Use the latest saved definition" checked=${latest} onChange=${setLatest} /></div></${Dialog}>`;
}

function NodePanel({ node, run }) {
  if (!node) return html`<div class="small muted p-3">Select an activity in the graph to see its attempts, errors and jobs.</div>`;
  const rows = node.rows || [];
  return html`<div class="node-panel" data-testid="run-node">
    <div class="row"><b class="grow">${node.name}</b><${JobState} state=${node.state} /></div>
    <${Kv} items=${[['Type', node.type], ['Attempts', node.attempts], node.instances > 1 ? ['Iterations', node.instances] : null, ['Started', node.started_at ? fmtTime(node.started_at) : null], ['Duration', node.duration_ms != null ? fmtDuration(node.duration_ms / 1000) : null], node.reused ? ['Reused', 'Result taken from the earlier run'] : null]} />
    ${(node.job_ids || []).length ? html`<div class="mt-3"><div class="small muted mb-1">Jobs</div><div class="col gap-1">${node.job_ids.map((j) => html`<a key=${j} href=${jobHref(j)} class="mono small">${shortId(j, 12)}</a>`)}</div></div>` : null}
    ${(node.child_runs || []).length ? html`<div class="mt-3"><div class="small muted mb-1">Child runs</div><div class="col gap-1">${node.child_runs.map((c) => html`<a key=${c.id || c} href=${runHref(c.id || c)} class="mono small">${shortId(c.id || c, 12)}</a>`)}</div></div>` : null}
    ${rows.filter((r) => r.error).map((r) => html`<div key=${r.id} class="mt-3"><${Banner} kind="bad" title=${`${titleCase(r.error.category || 'error')}${r.iteration != null ? ` (iteration ${r.iteration})` : ''}`}>${r.error.message}${r.error.details && r.error.details.remediation ? html`<div class="small mt-1">${r.error.details.remediation}</div>` : null}<//></div>`)}
    ${rows.length > 1 || (rows[0] && rows[0].attempts && rows[0].attempts.length > 1) ? html`<details class="mt-3"><summary class="small">Attempts and iterations</summary><${Table} dense caption="Activity attempts" rows=${rows.flatMap((r) => (r.attempts || [{ attempt: 1, state: r.state }]).map((a) => ({ ...a, id: r.id + ':' + a.attempt, iteration: r.iteration })))} columns=${[
      { key: 'it', label: 'Iteration', render: (a) => (a.iteration == null ? '–' : a.iteration) }, { key: 'at', label: 'Attempt', render: (a) => a.attempt }, { key: 'st', label: 'State', render: (a) => html`<${JobState} state=${a.state} />` }, { key: 'dur', label: 'Time', render: (a) => (a.started_at ? fmtDuration(durationBetween(a.started_at, a.finished_at)) : '–') }]} /></details>` : null}</div>`;
}

export function RunDetailPage({ params }) {
  const id = params.id;
  const [tab, setTab] = useQueryState('tab', 'graph');
  const [finished, setFinished] = useState(false);
  const g = usePoll(() => api.get(`/pipeline-runs/${enc(id)}/graph`), finished ? 60000 : 1500, [id, finished]);
  const kids = useAsync(() => api.get(`/pipeline-runs/${enc(id)}/children`).then((r) => r.items), [id, finished]);
  const [sel, setSel] = useState(null);
  const run = g.data && g.data.run;
  useEffect(() => { if (run && run.finished !== finished) setFinished(!!run.finished); }, [run && run.finished]);
  useEffect(() => { if (g.data && sel == null) { const bad = (g.data.nodes || []).find((n) => n.state === 'failed'); if (bad) setSel(bad.id); } }, [g.data && g.data.run.state]);
  const states = useMemo(() => Object.fromEntries(((g.data && g.data.nodes) || []).map((n) => [n.id, n.state])), [g.data]);
  if (g.loading && !g.data) return html`<div class="page"><${Loading} /></div>`;
  if (g.error && !g.data) return html`<div class="page"><${ErrorBox} error=${g.error} onRetry=${g.reloadLoud} /></div>`;
  const gr = g.data;
  const ws = run.workspace_id;
  const active = !run.finished;
  const node = (gr.nodes || []).find((n) => n.id === sel);
  const failedActs = (run.error && run.error.failed_activities) || [];
  async function cancel() { if (await confirmDialog({ title: 'Cancel this run?', danger: true, confirmLabel: 'Cancel run', cancelLabel: 'Keep running', body: 'Running activities, jobs and queries are stopped.' }) && (await act(() => api.post(`/pipeline-runs/${enc(id)}/cancel`, { reason: 'cancelled from the console' }), { ok: 'Cancellation requested' }))) g.reload(); }
  async function resume() { const r = await openOverlay(({ close }) => html`<${ResumeDialog} run=${run} graph=${gr} close=${close} />`); if (r) window.location.hash = runHref(r.id); }
  async function rerun() { const r = await act(() => api.post(`/pipeline-runs/${enc(id)}/rerun`, {}), { ok: 'Started a full re-run' }); if (r) window.location.hash = runHref(r.id); }
  const dur = run.duration_seconds != null ? run.duration_seconds : run.started_at ? durationBetween(run.started_at, null) : null;
  const timeline = (gr.timeline || []).filter((t) => t.started_at);
  return html`<div class="page wide"><${PageHead} title=${html`${run.pipeline_name} <span class="muted mono small">${shortId(run.id, 10)}</span>`} icon="workflow" sub=${`Version ${run.pipeline_version} · started by ${titleCase(run.trigger_type)}${(run.trigger || {}).by ? ' (' + run.trigger.by + ')' : ''}`} badge=${html`<${JobState} state=${run.state} />`}
    actions=${html`<a class="btn" href=${`#/pipelines/${enc(run.pipeline_id)}`}>Open pipeline</a>
      ${active && can('pipeline.run', ws) ? html`<${Btn} kind="danger" icon="stop" onClick=${cancel} data-testid="run-cancel">Cancel run<//>` : null}
      ${!active && run.state !== 'succeeded' && can('pipeline.run', ws) ? html`<${Btn} kind="primary" icon="play" onClick=${resume} data-testid="run-resume">Resume<//>` : null}
      ${!active && can('pipeline.run', ws) ? html`<${Btn} icon="refresh" onClick=${rerun}>Run again<//>` : null}`} />
    ${run.state === 'failed' ? html`<${Banner} kind="bad" title="This run failed">${run.error && run.error.message}${failedActs.map((f, i) => html`<div key=${i} class="mt-2 small"><b>${f.activity}</b> (${f.type}): ${(f.error || {}).message}${(f.error || {}).details && f.error.details.remediation ? html`<br /><span class="muted">${f.error.details.remediation}</span>` : null}</div>`)}<//>` : null}
    ${run.state === 'cancelled' ? html`<${Banner} kind="warn" title="Cancelled">${run.cancel_reason || run.state_reason || 'The run was cancelled.'}<//>` : null}
    <div class="grid c4 mb-4"><div class="stat"><div class="stat-top"><span class="stat-label">Duration</span></div><div class="stat-value">${dur != null ? fmtDuration(dur) : '–'}</div></div>
      <div class="stat"><div class="stat-top"><span class="stat-label">Succeeded</span></div><div class="stat-value">${(gr.counts || {}).succeeded || 0}</div></div>
      <div class="stat"><div class="stat-top"><span class="stat-label">Failed</span></div><div class="stat-value">${(gr.counts || {}).failed || 0}</div></div>
      <div class="stat"><div class="stat-top"><span class="stat-label">Critical path</span></div><div class="stat-value">${gr.critical_path_ms != null ? fmtDuration(gr.critical_path_ms / 1000) : '–'}</div></div></div>
    <${Tabs} value=${tab} onChange=${setTab} tabs=${[{ id: 'graph', label: 'Run graph' }, { id: 'timeline', label: 'Timeline' }, { id: 'data', label: 'Parameters and outputs' }, { id: 'children', label: 'Child runs', count: (kids.data || []).length || undefined }, { id: 'raw', label: 'Raw' }]} />
    <div class="mt-4">
    <${TabPanel} id="graph" value=${tab}><div class="run-split"><div class="pipe-canvas"><${Dag} graph=${gr} states=${states} selected=${sel} onSelect=${setSel} height=${520} label=${`Run graph of ${run.pipeline_name}`} /></div><aside class="pipe-inspector"><${NodePanel} node=${node} run=${run} /></aside></div><//>
    <${TabPanel} id="timeline" value=${tab}><${Card} title="Activity timeline" sub=${`${timeline.length} activit${timeline.length === 1 ? 'y' : 'ies'} started`}><${Gantt} items=${timeline} /><//><//>
    <${TabPanel} id="data" value=${tab}><div class="grid c2"><${Card} title="Parameters"><${JsonView} value=${run.parameters || {}} depth=${3} /><//><${Card} title="Variables"><${JsonView} value=${run.variables || {}} depth=${3} /><//><${Card} title="Outputs"><${JsonView} value=${run.outputs || {}} depth=${3} /><//></div><//>
    <${TabPanel} id="children" value=${tab}>${kids.loading ? html`<${Loading} />` : html`<${RunsTable} runs=${kids.data || []} empty=${html`<${Empty} icon="workflow" title="No child runs">Runs started by Execute pipeline activities appear here.<//>`} />`}<//>
    <${TabPanel} id="raw" value=${tab}><${Card} title="Run record"><${JsonView} value=${run} depth=${2} /><//><//>
    </div></div>`;
}
