// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// SQL editor: tabs, schema-aware completion, named parameters, run/cancel/explain, virtual result grid, chart view, export, history and saved queries.
import { html, useState, useEffect, useRef, useMemo, useAsync, useCallback } from '../core/h.js';
import { api, can, describeError, getAccessToken, ApiError } from '../api/client.js';
import { Btn, Badge, Banner, ErrorBox, Loading, Empty, Tabs, TabPanel, Select, Input, Seg, Switch, Card, Field } from '../ui/basic.js';
import { VirtualGrid } from '../ui/data.js';
import { Chart } from '../charts/Chart.js';
import { Icon } from '../ui/icons.js';
import { act, toast, promptDialog, confirmDialog, MenuButton } from '../ui/overlay.js';
import { enc, hex, fmtNum, fmtMs, ago, fmtTime, downloadBlob, downloadText, toCsv, copyText, jsonPretty } from '../core/util.js';
import { prefs as P } from '../core/store.js';
import { CodeEditor, loadCodeMirror } from '../notebook/cm.js';
import { useWorkspaces, useWorkspaceId, InlineWorkspaceSelect, Ago } from './common.js';
import { useRoute, setQuery } from '../core/router.js';
import { itemHref } from '../core/links.js';

const STARTER = 'SELECT category, round(sum(net_revenue), 0) AS revenue\nFROM retail_lakehouse.gold_category_summary\nGROUP BY category\nORDER BY revenue DESC';
let tabSeq = 0;
const newTab = (over = {}) => ({ id: 't' + Date.now().toString(36) + (tabSeq++), name: 'Query ' + (tabSeq), sql: '', engine: 'duckdb', lakehouse: '', params: {}, savedId: null, savedEtag: null, savedDef: null, ...over });

// ---- parameters -----------------------------------------------------------------------------------------------------------------------------
export function detectParams(sql) {
  const clean = String(sql).replace(/--[^\n]*/g, ' ').replace(/\/\*[\s\S]*?\*\//g, ' ').replace(/'(?:[^']|'')*'/g, "''").replace(/"(?:[^"]|"")*"/g, '""');
  const names = [];
  for (const m of clean.matchAll(/(?<![:\w]):([A-Za-z_]\w*)/g)) if (!names.includes(m[1])) names.push(m[1]);
  return names;
}
function coerce(v) {
  if (v === '' || v == null) return null;
  if (/^-?\d+$/.test(v)) return Number(v);
  if (/^-?\d*\.\d+$/.test(v)) return Number(v);
  if (v === 'true') return true;
  if (v === 'false') return false;
  return v;
}

// ---- completion -------------------------------------------------------------------------------------------------------------------------------
const KEYWORDS = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT', 'JOIN', 'LEFT JOIN', 'INNER JOIN', 'ON', 'AS', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'DISTINCT', 'HAVING', 'UNION ALL', 'WITH', 'OVER', 'PARTITION BY', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'ROUND', 'CAST', 'COALESCE', 'DATE_TRUNC', 'EXTRACT', 'DESC', 'ASC', 'NULL', 'IS NULL', 'IS NOT NULL'];
function makeHint(schema, getCtx) {
  const fn = (cm) => {
    const cur = cm.getCursor();
    const line = cm.getLine(cur.line).slice(0, cur.ch);
    const m = /([\w.]*)$/.exec(line);
    const word = m ? m[1] : '';
    const start = { line: cur.line, ch: cur.ch - word.length };
    const lhs = (schema && schema.lakehouses) || [];
    const text = cm.getValue();
    const aliasMap = {}; // alias or table name -> table obj
    const allTables = lhs.flatMap((l) => (l.tables || []).map((t) => ({ ...t, lh: l.name })));
    for (const mm of text.matchAll(/\b(?:from|join)\s+([\w.]+)(?:\s+(?:as\s+)?(?!on\b|where\b|group\b|order\b|left\b|inner\b|join\b|limit\b|union\b)(\w+))?/gi)) {
      const ref = mm[1];
      const t = allTables.find((x) => x.reference === ref || x.name === ref || `${x.lh}.${x.name}` === ref);
      if (t) { aliasMap[ref] = t; aliasMap[t.name] = t; if (mm[2]) aliasMap[mm[2]] = t; }
    }
    let list = [];
    let from = start;
    if (word.includes('.')) {
      const parts = word.split('.');
      const last = parts.pop();
      const head = parts.join('.');
      from = { line: cur.line, ch: cur.ch - last.length };
      const lh = lhs.find((l) => l.name === head);
      if (lh) list = (lh.tables || []).map((t) => ({ text: t.name, displayText: `${t.name}  (${t.format})`, className: 'hint-table' }));
      else if (aliasMap[head]) list = aliasMap[head].columns.map((c) => ({ text: c.name, displayText: `${c.name}  ${c.type}` }));
      list = list.filter((x) => x.text.toLowerCase().startsWith(last.toLowerCase()));
    } else {
      const w = word.toLowerCase();
      const cols = [...new Map(Object.values(aliasMap).flatMap((t) => t.columns).map((c) => [c.name, c])).values()];
      list = [
        ...cols.map((c) => ({ text: c.name, displayText: `${c.name}  ${c.type}` })),
        ...allTables.map((t) => ({ text: t.reference, displayText: `${t.reference}  (table)` })),
        ...lhs.map((l) => ({ text: l.name, displayText: `${l.name}  (lakehouse)` })),
        ...KEYWORDS.map((k) => ({ text: k, displayText: k, className: 'hint-kw' })),
      ].filter((x) => x.text.toLowerCase().startsWith(w) && x.text.toLowerCase() !== w);
    }
    return { list: list.slice(0, 60), from, to: cur };
  };
  return fn;
}

// ---- results ----------------------------------------------------------------------------------------------------------------------------------
function ChartView({ res }) {
  const cols = res.columns;
  const numeric = cols.filter((c) => ['integer', 'float', 'decimal'].includes(c.kind));
  const cats = cols.filter((c) => !['integer', 'float', 'decimal'].includes(c.kind));
  const [type, setType] = useState('bar');
  const [x, setX] = useState((cats[0] || cols[0] || {}).name);
  const [y, setY] = useState((numeric[0] || {}).name);
  const [series, setSeries] = useState('');
  if (!numeric.length) return html`<${Empty} icon="chart" title="Nothing to plot">Charts need at least one numeric column.<//>`;
  const view = { x, y: [y], series: series || undefined, format: /revenue|amount|price|profit|cost|sales/i.test(y) ? 'currency' : undefined };
  const rows = res.rows.slice(0, 2000);
  return html`<div><div class="toolbar mb-3">
    <${Seg} label="Chart type" value=${type} onChange=${setType} options=${[{ value: 'bar', label: 'Bar' }, { value: 'line', label: 'Line' }, { value: 'area', label: 'Area' }, { value: 'donut', label: 'Donut' }, { value: 'scatter', label: 'Scatter' }]} />
    <${Select} label="X" size="sm" value=${x} onChange=${setX} options=${cols.map((c) => ({ value: c.name, label: c.name }))} />
    <${Select} label="Y" size="sm" value=${y} onChange=${setY} options=${numeric.map((c) => ({ value: c.name, label: c.name }))} />
    <${Select} label="Series" size="sm" value=${series} onChange=${setSeries} placeholder="None" options=${cats.filter((c) => c.name !== x).map((c) => ({ value: c.name, label: c.name }))} /></div>
    <${Chart} type=${type} columns=${cols} rows=${rows} view=${view} height=${320} title=${`${y} by ${x}`} subtitle=${rows.length < res.rows.length ? 'first 2,000 rows' : undefined} /></div>`;
}

function Results({ res, err, busy, plan, onExport, tab, setTab, elapsed }) {
  if (busy) return html`<div class="loading" role="status"><span class="spinner lg"></span><span>Running${elapsed ? ` · ${(elapsed / 1000).toFixed(1)} s` : ''}</span></div>`;
  if (err) {
    const d = describeError(err);
    return html`<${ErrorBox} error=${err} />`;
  }
  if (plan) return html`<div class="small muted mb-2">Plan from ${plan.engine} for ${plan.statement}${plan.tables && plan.tables.length ? ` over ${plan.tables.join(', ')}` : ''}</div><pre class="code" tabindex="0" style="max-height:420px;overflow:auto">${plan.plan}</pre>`;
  if (!res) return html`<${Empty} icon="terminal" title="Run a query to see results">Press <kbd>Ctrl</kbd> + <kbd>Enter</kbd> to run, or use Explain to see the plan first.<//>`;
  const st = res.stats || {};
  return html`<div>
    <div class="row gap-3 mb-2 wrap"><${Badge} kind="ok">${fmtNum(res.row_count)} row${res.row_count === 1 ? '' : 's'}${res.truncated ? ' (truncated)' : ''}<//><span class="small muted">${fmtMs(st.elapsed_ms)} · ${res.engine}${st.engine_version ? ' ' + st.engine_version : ''}${(st.tables || []).length ? ' · ' + st.tables.join(', ') : ''} · <span class="mono">${res.query_id}</span></span><span class="grow"></span>
      <${MenuButton} label="Export" icon="download" iconOnly=${false} kind="" size="sm" items=${[{ label: 'CSV', onClick: () => onExport('csv') }, { label: 'JSON', onClick: () => onExport('json') }, { label: 'Newline-delimited JSON', onClick: () => onExport('ndjson') }, { label: 'Parquet', onClick: () => onExport('parquet') }, { sep: true }, { label: 'Copy as CSV (shown rows)', onClick: () => { copyText(toCsv(res.columns, res.rows)); toast.ok('Copied'); } }]}>Export<//></div>
    ${res.truncated ? html`<${Banner} kind="warn">Only the first ${fmtNum(res.row_count)} rows are shown. Raise the row limit, add a LIMIT, or export to get everything.<//>` : null}
    ${(res.warnings || []).map((w, i) => html`<${Banner} key=${i} kind="warn">${typeof w === 'string' ? w : jsonPretty(w)}<//>`)}
    <${Tabs} tabs=${[{ id: 'grid', label: 'Table' }, { id: 'chart', label: 'Chart' }]} value=${tab} onChange=${setTab} label="Result views" />
    <div class="tab-body">
      <${TabPanel} id="grid" value=${tab}><${VirtualGrid} caption="Query result" columns=${res.columns.map((c) => ({ name: c.name, type: c.type, numeric: ['integer', 'float', 'decimal'].includes(c.kind) }))} rows=${res.rows} height=${440} onCopy=${(v) => { copyText(String(v)); toast.ok('Copied'); }} /></${TabPanel}>
      <${TabPanel} id="chart" value=${tab}><${ChartView} res=${res} /></${TabPanel}></div></div>`;
}

// ---- side panels ------------------------------------------------------------------------------------------------------------------------------
function SchemaPanel({ schema, onInsert, loading, error }) {
  const [open, setOpen] = useState({});
  const [q, setQ] = useState('');
  if (loading) return html`<${Loading} />`;
  if (error) return html`<${ErrorBox} error=${error} compact=${true} />`;
  const lhs = (schema && schema.lakehouses) || [];
  if (!lhs.length) return html`<${Empty} icon="database" title="No lakehouses">Install the sample from Home to get tables to query.<//>`;
  return html`<div class="p-3"><input class="input sm" placeholder="Filter tables and columns" aria-label="Filter schema" value=${q} onInput=${(e) => setQ(e.target.value)} /></div>
    <ul class="schema-tree" role="tree" aria-label="Schema">${lhs.map((l) => html`<li key=${l.id} role="treeitem" aria-expanded="true"><div class="st-lh"><${Icon} name="database" size="sm" /> <b>${l.name}</b> <span class="muted small">${l.workspace}</span></div>
      <ul role="group">${(l.tables || []).filter((t) => !q || t.name.includes(q.toLowerCase()) || t.columns.some((c) => c.name.includes(q.toLowerCase()))).map((t) => { const k = l.id + t.name; const isOpen = open[k] || !!q; return html`<li key=${k} role="treeitem" aria-expanded=${isOpen}>
        <div class="st-row"><button class="st-tog" aria-label=${(isOpen ? 'Collapse ' : 'Expand ') + t.name} onClick=${() => setOpen((o) => ({ ...o, [k]: !o[k] }))}><${Icon} name=${isOpen ? 'chevron-down' : 'chevron-right'} size="sm" /></button><button class="st-name" title=${`Insert ${t.reference}`} onClick=${() => onInsert(t.reference)}><${Icon} name="table" size="sm" /> ${t.name}</button></div>
        ${isOpen ? html`<ul role="group" class="st-cols">${t.columns.filter((c) => !q || c.name.includes(q.toLowerCase()) || t.name.includes(q.toLowerCase())).map((c) => html`<li key=${c.name} role="treeitem"><button class="st-col" onClick=${() => onInsert(c.name)} title=${`Insert ${c.name}`}><span class="mono">${c.name}</span><span class="muted small mono">${c.type}</span></button></li>`)}</ul>` : null}</li>`; })}</ul></li>`)}</ul>`;
}

function SavedPanel({ ws, onOpen }) {
  const q = useAsync(() => (ws ? api.get(`/workspaces/${enc(ws)}/items`, { type: 'sql_query', limit: 200 }).then((r) => r.items || []) : []), [ws]);
  if (q.loading) return html`<${Loading} />`;
  if (q.error) return html`<${ErrorBox} error=${q.error} compact=${true} />`;
  if (!q.data.length) return html`<${Empty} icon="file-code" title="No saved queries">Save a query to reuse it and to use it in dashboards.<//>`;
  return html`<ul class="tbl-list">${q.data.map((it) => html`<li key=${it.id}><button onClick=${() => onOpen(it)}><span class="row gap-2"><${Icon} name="file-code" size="sm" /><b>${it.name}</b></span><span class="small muted trunc">${it.description || ''}</span></button></li>`)}</ul>`;
}

function HistoryPanel({ ws, onOpen, refreshKey }) {
  const h = useAsync(() => api.get('/sql/history', { mine: true, limit: 60 }).then((r) => r.items || []), [refreshKey]);
  if (h.loading) return html`<${Loading} />`;
  if (h.error) return html`<${ErrorBox} error=${h.error} compact=${true} />`;
  if (!h.data.length) return html`<${Empty} icon="history" title="No history yet">Queries you run appear here.<//>`;
  return html`<ul class="tbl-list">${h.data.map((q) => html`<li key=${q.id}><button onClick=${() => onOpen(q)} title=${q.sql_text}><span class="row gap-2"><${Badge} kind=${q.status === 'ok' ? 'ok' : 'bad'}>${q.status}<//><span class="small muted">${q.elapsed_ms != null ? fmtMs(q.elapsed_ms) : ''} · <${Ago} ts=${q.started_at} /></span></span><span class="mono small trunc" style="max-width:100%">${(q.sql_text || '').replace(/\s+/g, ' ').slice(0, 90)}</span></button></li>`)}</ul>`;
}

// ---- page ---------------------------------------------------------------------------------------------------------------------------------------
export function SqlPage({ params }) {
  const [wsId, setWsId, workspaces] = useWorkspaceId();
  const eng = useAsync(() => api.get('/sql/engines'), []);
  const schemaQ = useAsync(() => (wsId ? api.get('/sql/schema', { workspace_id: wsId }) : { lakehouses: [] }), [wsId]);
  const route = useRoute();
  const [tabs, setTabs] = useState(() => { const t = P.get('sql.tabs', null); return Array.isArray(t) && t.length ? t : [newTab({ name: 'Revenue by category', sql: STARTER })]; });
  const [active, setActive] = useState(() => P.get('sql.active', null));
  const [panel, setPanel] = useState('schema');
  const [results, setResults] = useState({}); // tabId -> {res, err, plan, busy, started}
  const [rtab, setRtab] = useState('grid');
  const [hk, setHk] = useState(0);
  const [, tick] = useState(0);
  const cm = useRef(null);
  const running = useRef({});
  const cur = tabs.find((t) => t.id === active) || tabs[0];
  const lhs = (schemaQ.data && schemaQ.data.lakehouses) || [];
  const engines = (eng.data && eng.data.items) || [];
  const limits = (eng.data && eng.data.limits) || {};
  const [maxRows, setMaxRows] = useState(() => P.get('sql.maxRows', 10000));
  const R = results[cur.id] || {};

  useEffect(() => { P.set('sql.tabs', tabs.map((t) => ({ ...t }))); P.set('sql.active', cur.id); }, [tabs, cur.id]);
  useEffect(() => { if (!R.busy) return undefined; const t = setInterval(() => tick((n) => n + 1), 500); return () => clearInterval(t); }, [R.busy]);
  const upd = (patch, id = cur.id) => setTabs((ts) => ts.map((t) => (t.id === id ? { ...t, ...patch } : t)));
  const setRes = (id, v) => setResults((r) => ({ ...r, [id]: { ...(r[id] || {}), ...v } }));

  // deep link from the lakehouse explorer: #/sql?q=...&lakehouse=...
  useEffect(() => {
    const q = route.query.q;
    if (q) { const t = newTab({ name: 'Query', sql: q, lakehouse: '' }); setTabs((ts) => [...ts, t]); setActive(t.id); setQuery({ q: undefined, lakehouse: undefined }); }
  }, [route.query.q]);
  // a saved query opened via #/sql/<id>
  useEffect(() => { if (params.id) openSaved({ id: params.id }); }, [params.id]);

  const names = useMemo(() => detectParams(cur.sql), [cur.sql]);
  const hint = useMemo(() => makeHint(schemaQ.data, null), [schemaQ.data]);
  const engineInfo = engines.find((e) => e.id === cur.engine);

  const body = () => {
    const parameters = {};
    for (const n of names) parameters[n] = coerce(cur.params[n]);
    return { sql: cur.sql, engine: cur.engine, workspace_id: wsId, lakehouse: cur.lakehouse || undefined, parameters, max_rows: Number(maxRows) || undefined, timeout_seconds: undefined };
  };
  async function run(how = 'run') {
    if (!cur.sql.trim()) { toast.info('Write a query first'); return; }
    if (!wsId) { toast.bad('Pick a workspace', 'Queries run in the context of a workspace.'); return; }
    const id = cur.id;
    const qid = 'qry_' + hex(20);
    running.current[id] = qid;
    setRes(id, { busy: true, err: null, plan: null, started: Date.now(), qid });
    try {
      if (how === 'explain') { const plan = await api.post('/sql/explain', { ...body(), query_id: qid }); setRes(id, { busy: false, plan, res: null }); } else {
        const res = await api.post('/sql/execute', { ...body(), query_id: qid });
        setRes(id, { busy: false, res, plan: null }); setRtab('grid'); setHk((n) => n + 1);
      }
    } catch (e) {
      setRes(id, { busy: false, err: e.name === 'AbortError' ? null : e }); setHk((n) => n + 1);
    }
    delete running.current[id];
  }
  async function cancel() {
    const qid = running.current[cur.id];
    if (!qid) return;
    try { await api.post(`/sql/cancel/${enc(qid)}`, {}); toast.info('Cancel requested'); } catch (e) { toast.bad('Could not cancel', describeError(e).title); }
  }
  async function doExport(format) {
    if (!cur.sql.trim()) return;
    const blob = await act(() => api.full('POST', '/sql/export', { body: { ...body(), format, filename: cur.name.replace(/[^\w.-]+/g, '_') }, raw: true }).then((r) => r.data.blob()), { ok: `Exported ${format.toUpperCase()}` });
    if (blob) downloadBlob(blob, `${cur.name.replace(/[^\w.-]+/g, '_') || 'result'}.${format === 'ndjson' ? 'ndjson' : format}`);
  }
  async function save() {
    const lakehouse_id = (lhs.find((l) => l.name === cur.lakehouse) || lhs[0] || {}).id;
    const definition = { sql: cur.sql, engine: cur.engine, ...(lakehouse_id ? { lakehouse_id } : {}), parameters: names.map((n) => ({ name: n, type: typeof coerce(cur.params[n]) === 'number' ? 'number' : 'string', default: coerce(cur.params[n]), required: false })) };
    if (cur.savedId) {
      const r = await act(() => api.put(`/items/${enc(cur.savedId)}`, { definition, comment: 'saved from the SQL editor' }, { ifMatch: cur.savedEtag }), { ok: 'Query saved' });
      if (r) upd({ savedEtag: r.etag || cur.savedEtag });
    } else {
      const name = await promptDialog({ title: 'Save query', label: 'Name', value: cur.name, confirmLabel: 'Save' });
      if (!name) return;
      const r = await act(() => api.post(`/workspaces/${enc(wsId)}/items`, { type: 'sql_query', name, definition }), { ok: 'Query saved' });
      if (r) upd({ savedId: r.id, savedEtag: r.etag, name });
    }
  }
  async function openSaved(it) {
    try {
      const full = await api.full('GET', `/items/${enc(it.id)}`);
      const d = full.data.definition || {};
      const existing = tabs.find((t) => t.savedId === it.id);
      if (existing) { setActive(existing.id); return; }
      const t = newTab({ name: full.data.name, sql: d.sql || '', engine: d.engine || 'duckdb', savedId: it.id, savedEtag: full.data.etag, params: Object.fromEntries((d.parameters || []).map((p) => [p.name, p.default == null ? '' : String(p.default)])), lakehouse: (lhs.find((l) => l.id === d.lakehouse_id) || {}).name || '' });
      setTabs((ts) => [...ts, t]); setActive(t.id);
    } catch (e) { toast.bad('Could not open the query', describeError(e).title); }
  }
  const addTab = () => { const t = newTab({ sql: '' }); setTabs((ts) => [...ts, t]); setActive(t.id); };
  const closeTab = (id) => { if (tabs.length === 1) { upd({ sql: '', name: 'Query 1', savedId: null }); return; } const i = tabs.findIndex((t) => t.id === id); const rest = tabs.filter((t) => t.id !== id); setTabs(rest); if (active === id || cur.id === id) setActive(rest[Math.max(0, i - 1)].id); };
  const insert = (text) => { const e = cm.current; if (e) { e.replaceSelection(text); e.focus(); } else upd({ sql: cur.sql + text }); };
  const elapsed = R.busy ? Date.now() - R.started : null;

  return html`<div class="page wide sql-page">
    <header class="page-head"><div class="titles"><h1><${Icon} name="terminal" size="lg" />SQL editor</h1><div class="sub">Query lakehouse tables with DuckDB, or run Spark SQL on the compute fabric. Reference tables as <span class="mono">lakehouse.table</span>.</div></div>
      <div class="actions"><${InlineWorkspaceSelect} value=${wsId} onChange=${setWsId} items=${workspaces} /></div></header>
    <div class="sql-layout">
      <aside class="sql-side card" aria-label="Schema, saved queries and history"><div class="card-body flush">
        <${Tabs} tabs=${[{ id: 'schema', label: 'Schema' }, { id: 'saved', label: 'Saved' }, { id: 'history', label: 'History' }]} value=${panel} onChange=${setPanel} label="Side panel" />
        <div class="side-scroll"><${TabPanel} id="schema" value=${panel}><${SchemaPanel} schema=${schemaQ.data} loading=${schemaQ.loading} error=${schemaQ.error} onInsert=${insert} /></${TabPanel}>
        <${TabPanel} id="saved" value=${panel}><${SavedPanel} ws=${wsId} onOpen=${openSaved} /></${TabPanel}>
        <${TabPanel} id="history" value=${panel}><${HistoryPanel} ws=${wsId} refreshKey=${hk} onOpen=${(q) => { const t = newTab({ name: 'From history', sql: q.sql_text || '', engine: q.engine || 'duckdb' }); setTabs((ts) => [...ts, t]); setActive(t.id); }} /></${TabPanel}></div></div></aside>
      <main class="sql-main">
        <div class="qtabs" role="tablist" aria-label="Query tabs">${tabs.map((t) => html`<div key=${t.id} class=${'qtab' + (t.id === cur.id ? ' on' : '')}><button role="tab" aria-selected=${t.id === cur.id} id=${'qt-' + t.id} onClick=${() => setActive(t.id)} onDblClick=${async () => { const n = await promptDialog({ title: 'Rename tab', label: 'Name', value: t.name, confirmLabel: 'Rename' }); if (n) upd({ name: n }, t.id); }} title="Double-click to rename">${t.savedId ? html`<${Icon} name="file-code" size="sm" />` : null}${t.name}${results[t.id] && results[t.id].busy ? html` <span class="spinner"></span>` : null}</button>
          <button class="x" aria-label=${`Close ${t.name}`} onClick=${() => closeTab(t.id)}><${Icon} name="x" size="sm" /></button></div>`)}
          <button class="qtab-add" aria-label="New query tab" onClick=${addTab}><${Icon} name="plus" size="sm" /></button></div>
        <div class="card sql-editor"><div class="card-body flush">
          <div class="toolbar sql-bar">
            <${Btn} kind="primary" icon="play" disabled=${R.busy} onClick=${() => run('run')} data-testid="sql-run">Run<//>
            ${R.busy ? html`<${Btn} icon="stop" onClick=${cancel} data-testid="sql-cancel">Cancel<//>` : null}
            <${Btn} icon="route" disabled=${R.busy} onClick=${() => run('explain')}>Explain<//>
            <span class="sep"></span>
            <${Select} label="Engine" size="sm" value=${cur.engine} onChange=${(v) => upd({ engine: v })} options=${(engines.length ? engines : [{ id: 'duckdb', label: 'DuckDB' }]).map((e) => ({ value: e.id, label: e.label + (e.available === false ? ' (unavailable)' : '') }))} />
            <${Select} label="Default lakehouse" size="sm" value=${cur.lakehouse} onChange=${(v) => upd({ lakehouse: v })} placeholder="None (qualify tables)" options=${lhs.map((l) => ({ value: l.name, label: l.name }))} />
            <${Select} label="Row limit" size="sm" value=${String(maxRows)} onChange=${(v) => { setMaxRows(Number(v)); P.set('sql.maxRows', Number(v)); }} options=${[100, 1000, 10000, 100000].filter((n) => !limits.max_rows_cap || n <= limits.max_rows_cap).map((n) => ({ value: String(n), label: fmtNum(n) }))} />
            <span class="grow"></span>
            ${can('item.create', wsId) || can('item.update', wsId) ? html`<${Btn} icon="save" onClick=${save}>${cur.savedId ? 'Save changes' : 'Save query'}<//>` : null}
          </div>
          ${engineInfo && engineInfo.available === false ? html`<${Banner} kind="warn" title=${`${engineInfo.label} is not available`}>${engineInfo.detail || 'Pick another engine.'}<//>` : null}
          ${engineInfo && engineInfo.read_only && /^\s*(insert|update|delete|merge|create|drop|alter)\b/i.test(cur.sql) ? html`<${Banner} kind="warn" title="This engine is read-only">${engineInfo.label} cannot change data. Switch the engine to Spark SQL for INSERT, MERGE, UPDATE, DELETE and CREATE TABLE AS.<//>` : null}
          <div class="sql-cm"><${CodeEditor} key=${cur.id} value=${cur.sql} mode="sql" onChange=${(v) => upd({ sql: v })} onRun=${() => run('run')} hint=${hint} editorRef=${cm} autoHeight=${false} height="220px" lineNumbers=${true} placeholder="SELECT ... FROM lakehouse.table" label="SQL" /></div>
          ${names.length ? html`<div class="param-bar" role="group" aria-label="Query parameters"><span class="small muted">Parameters</span>${names.map((n) => html`<label key=${n} class="param-in"><span class="mono">:${n}</span><input class="input sm mono" value=${cur.params[n] ?? ''} onInput=${(e) => upd({ params: { ...cur.params, [n]: e.target.value } })} aria-label=${`Value of ${n}`} /></label>`)}</div>` : null}
        </div></div>
        <div class="card mt-4"><div class="card-body" aria-live="polite" data-testid="sql-results"><${Results} res=${R.res} err=${R.err} busy=${R.busy} plan=${R.plan} onExport=${doExport} tab=${rtab} setTab=${setRtab} elapsed=${elapsed} /></div></div>
      </main></div></div>`;
}
