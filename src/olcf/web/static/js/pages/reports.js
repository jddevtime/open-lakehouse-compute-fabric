// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Dashboards: list, viewer (filters, auto refresh, per-tile export) and designer (tile and filter editor with live preview). Charts come from the in-house SVG library.
import { html, useState, useEffect, useRef, useMemo, useAsync, useCallback } from '../core/h.js';
import { api, can, describeError } from '../api/client.js';
import { Btn, Badge, Banner, ErrorBox, Loading, Empty, Select, Input, Textarea, Seg, Switch, Card, PageHead, Field, Check, Tabs, TabPanel } from '../ui/basic.js';
import { Icon } from '../ui/icons.js';
import { Chart, DataTable } from '../charts/Chart.js';
import { act, toast, confirmDialog, promptDialog, MenuButton, openOverlay, Dialog } from '../ui/overlay.js';
import { enc, ago, fmtMs, fmtNum, clone, uid, hex, jsonPretty, navigate, downloadText, toCsv } from '../core/util.js';
import { prefs as P } from '../core/store.js';
import { useWorkspaces, useWorkspaceId, InlineWorkspaceSelect, Ago } from './common.js';
import { CodeEditor } from '../notebook/cm.js';
import { detectParams } from './sql.js';
import { itemHref } from '../core/links.js';

const TILE_TYPES = [['kpi', 'Single number'], ['line', 'Line'], ['area', 'Area'], ['bar', 'Bar'], ['stacked', 'Stacked bar'], ['donut', 'Donut'], ['table', 'Table'], ['heatmap', 'Heatmap'], ['scatter', 'Scatter']];
const FORMATS = [['', 'Automatic'], ['integer', 'Integer'], ['number', 'Decimal'], ['currency', 'Currency'], ['percent', 'Percent'], ['compact', 'Compact']];
const iso = (d) => d.toISOString().slice(0, 10);

// ---- filters ------------------------------------------------------------------------------------------------------------------------------------
function rangePresets(def) {
  const to = def && def.to ? new Date(def.to + 'T00:00:00Z') : new Date();
  const from = (days) => { const d = new Date(to); d.setUTCDate(d.getUTCDate() - days + 1); return iso(d); };
  const yStart = new Date(Date.UTC(to.getUTCFullYear(), 0, 1));
  const prevY = to.getUTCFullYear() - 1;
  return [['Last 30 days', { from: from(30), to: iso(to) }], ['Last 90 days', { from: from(90), to: iso(to) }], ['Year to date', { from: iso(yStart), to: iso(to) }], [String(prevY), { from: `${prevY}-01-01`, to: `${prevY}-12-31` }], ['Everything', { from: '2000-01-01', to: iso(to) }]];
}
function FilterControl({ f, value, onChange }) {
  if (f.type === 'date_range') {
    const v = value || f.default || {};
    return html`<div class="fl-group" role="group" aria-label=${f.label || f.name}><span class="fl-label">${f.label || f.name}</span>
      <input type="date" class="input sm" aria-label=${`${f.label || f.name} from`} value=${v.from || ''} onChange=${(e) => onChange({ ...v, from: e.target.value })} /><span class="muted">to</span>
      <input type="date" class="input sm" aria-label=${`${f.label || f.name} to`} value=${v.to || ''} onChange=${(e) => onChange({ ...v, to: e.target.value })} />
      <${MenuButton} label=${`${f.label || f.name} presets`} icon="calendar" items=${rangePresets(f.default).map(([l, val]) => ({ label: l, onClick: () => onChange(val) }))} /></div>`;
  }
  if (f.type === 'select' && f.multiple) {
    const sel = Array.isArray(value) ? value : [];
    return html`<div class="fl-group" role="group" aria-label=${f.label || f.name}><span class="fl-label">${f.label || f.name}</span>${(f.options || []).map((o) => html`<${Check} key=${o} label=${String(o)} checked=${sel.includes(o)} onChange=${(c) => onChange(c ? [...sel, o] : sel.filter((x) => x !== o))} />`)}</div>`;
  }
  if (f.type === 'select') return html`<label class="fl-group"><span class="fl-label">${f.label || f.name}</span><select class="select sm" value=${value ?? ''} onChange=${(e) => onChange(e.target.value === '' ? null : e.target.value)} aria-label=${f.label || f.name}><option value="">All</option>${(f.options || []).map((o) => html`<option key=${o} value=${o} selected=${value === o}>${o}</option>`)}</select></label>`;
  if (f.type === 'boolean') return html`<div class="fl-group"><${Switch} label=${f.label || f.name} checked=${!!value} onChange=${onChange} /></div>`;
  return html`<label class="fl-group"><span class="fl-label">${f.label || f.name}</span><input class="input sm" type=${f.type === 'number' ? 'number' : f.type === 'date' ? 'date' : 'text'} value=${value ?? ''} onChange=${(e) => onChange(e.target.value === '' ? null : f.type === 'number' ? Number(e.target.value) : e.target.value)} aria-label=${f.label || f.name} /></label>`;
}

// ---- a tile ------------------------------------------------------------------------------------------------------------------------------------
export function Tile({ tile, onOpenSql, onRetry, selected, onSelect, designer }) {
  const v = tile.view || {};
  const span = Math.max(1, Math.min(12, v.width || 4));
  const rows = Math.max(1, Math.min(8, v.height || 2));
  const h = rows * 104 + (rows - 1) * 16;
  const chartH = tile.type === "kpi" ? h : h - 92;
  return html`<section class=${`tile${tile.type === 'kpi' ? ' kpi-tile' : ''}${selected ? ' selected' : ''}${designer ? ' design' : ''}`} style=${`--span:${span};--rows:${rows}`} aria-label=${tile.title} data-tile=${tile.id} onClick=${onSelect ? () => onSelect(tile.id) : undefined}>
    ${tile.error ? html`<div class="tile-err" role="alert"><${Icon} name="alert-circle" /><div><b>${tile.title}</b><div class="small">${typeof tile.error === 'string' ? tile.error : tile.error.message || jsonPretty(tile.error)}</div>${onRetry ? html`<${Btn} size="sm" onClick=${onRetry}>Try again<//>` : null}</div></div>`
      : tile.loading ? html`<div class="tile-load" role="status"><div class="skel skel-t"></div><div class="skel skel-b"></div><span class="sr-only">Loading ${tile.title}</span></div>`
      : html`<${Chart} type=${tile.type} columns=${tile.columns || []} rows=${tile.rows || []} view=${{ ...v, y: v.y ? [].concat(v.y) : undefined }} title=${tile.title} height=${chartH > 120 ? chartH : 120} subtitle=${tile.row_count > (tile.rows || []).length ? 'first rows only' : undefined} />`}
    ${!designer && onOpenSql && tile.sql ? html`<button class="tile-sql" title="Open the query in the SQL editor" aria-label=${`Open ${tile.title} query in the SQL editor`} onClick=${() => onOpenSql(tile)}><${Icon} name="terminal" size="sm" /></button>` : null}
    ${!designer && !tile.loading && !tile.error && tile.elapsed_ms != null ? html`<span class="tile-meta" title=${tile.from_cache ? 'Served from the result cache' : 'Fresh result'}>${tile.from_cache ? 'cached · ' : ''}${fmtMs(tile.elapsed_ms)}</span>` : null}</section>`;
}

// ---- list ---------------------------------------------------------------------------------------------------------------------------------------
export function ReportsPage() {
  const [wsId, setWsId, workspaces] = useWorkspaceId();
  const list = useAsync(() => api.get('/reports', { workspace_id: wsId || undefined }).then((r) => r.items || []), [wsId]);
  const items = list.data || [];
  async function create() {
    const name = await promptDialog({ title: 'New dashboard', label: 'Name', confirmLabel: 'Create' });
    if (!name) return;
    const lhs = await api.get(`/workspaces/${enc(wsId)}/lakehouses`).catch(() => ({ items: [] }));
    const lakehouse_id = ((lhs.items || [])[0] || {}).id;
    const r = await act(() => api.post(`/workspaces/${enc(wsId)}/items`, { type: 'report', name, definition: { title: name, filters: [], tiles: [{ id: 'hello', title: 'Hello', type: 'kpi', sql: 'SELECT 42 AS answer', ...(lakehouse_id ? { lakehouse_id } : {}), format: 'integer', engine: 'duckdb', width: 3, height: 1 }] } }), { ok: 'Dashboard created' });
    if (r) navigate(`/dashboards/${enc(r.id)}/edit`);
  }
  return html`<div class="page"><${PageHead} title="Dashboards" icon="chart" sub="Live dashboards built from SQL tiles. Filters apply to every tile; each tile exports as PNG, SVG or CSV." actions=${html`<${InlineWorkspaceSelect} value=${wsId} onChange=${setWsId} items=${workspaces} />${can('item.create', wsId) ? html`<${Btn} kind="primary" icon="plus" onClick=${create}>New dashboard<//>` : null}`} />
    ${list.loading ? html`<${Loading} />` : list.error ? html`<${ErrorBox} error=${list.error} onRetry=${list.reloadLoud} />` : !items.length ? html`<${Empty} icon="chart" title="No dashboards in this workspace" action=${html`<${Btn} href="#/">Install the sample from Home<//>`}>Create a dashboard, or install the sample lakehouse to get Retail Sales Overview.<//>` : html`
      <div class="ws-grid">${items.map((r) => html`<article class="card ws-card" key=${r.id}><div class="card-body"><div class="row gap-2"><${Icon} name="chart" /><h3 class="m-0 grow"><a href=${`#/dashboards/${enc(r.id)}`}>${r.name}</a></h3></div>
        <p class="muted small">${r.description || 'No description.'}</p><div class="chips"><${Badge}>${r.tile_count} tiles<//>${(r.filters || []).map((f) => html`<${Badge} key=${f.name} kind="outline">filter: ${f.label || f.name}<//>`)}</div>
        <div class="row gap-2 mt-3"><${Btn} kind="primary" size="sm" href=${`#/dashboards/${enc(r.id)}`}>Open<//>${can('item.update', r.workspace_id) ? html`<${Btn} size="sm" href=${`#/dashboards/${enc(r.id)}/edit`}>Design<//>` : null}<span class="grow"></span><span class="small muted">updated <${Ago} ts=${r.updated_at} /></span></div></div></article>`)}</div>`}</div>`;
}

// ---- viewer -------------------------------------------------------------------------------------------------------------------------------------
export function ReportViewPage({ params }) {
  const id = params.id;
  const storeKey = 'dash.' + id;
  const [filters, setFilters] = useState(() => P.get(storeKey, null));
  const [run, setRun] = useState({ loading: true });
  const [auto, setAuto] = useState(() => P.get('dash.auto', 0));
  const seq = useRef(0);
  const item = useAsync(() => api.get(`/items/${enc(id)}`), [id]);
  const load = useCallback(async (refresh) => {
    const my = ++seq.current;
    setRun((r) => (r.data ? { ...r, busy: true } : { loading: true }));
    try {
      const body = { filters: filters || {}, refresh: !!refresh };
      const data = await api.post(`/reports/${enc(id)}/run`, body);
      if (my === seq.current) { setRun({ data, at: new Date() }); if (!filters) setFilters(Object.fromEntries(data.filters.map((f) => [f.name, f.value]))); }
    } catch (e) { if (my === seq.current) setRun((r) => ({ ...r, loading: false, busy: false, error: e })); }
  }, [id, JSON.stringify(filters)]);
  useEffect(() => { const t = setTimeout(() => load(false), filters ? 250 : 0); return () => clearTimeout(t); }, [load]);
  useEffect(() => { if (!auto) return undefined; const t = setInterval(() => { if (!document.hidden) load(true); }, auto * 1000); return () => clearInterval(t); }, [auto, load]);
  const setF = (name, v) => { const n = { ...(filters || {}), [name]: v }; setFilters(n); P.set(storeKey, n); };
  const d = run.data;
  const canEdit = item.data ? can('item.update', item.data.workspace_id) : false;
  if (run.loading && !d) return html`<${Loading} label="Running dashboard" />`;
  if (run.error && !d) return html`<div class="page"><${ErrorBox} error=${run.error} onRetry=${() => load(true)} /></div>`;
  const tiles = d.tiles;
  async function exportAll() {
    const parts = tiles.filter((t) => !t.error).map((t) => `# ${t.title}\n${toCsv(t.columns, t.rows)}`);
    downloadText(parts.join('\n\n'), `${d.report.title.replace(/\W+/g, '_')}.csv`, 'text/csv');
  }
  return html`<div class="page wide dash-page">
    <header class="dash-head"><div><h1>${d.report.title}</h1><div class="sub">${item.data && item.data.description ? item.data.description + ' · ' : ''}${run.busy ? 'Refreshing…' : `Updated ${run.at ? run.at.toLocaleTimeString('en-GB') : ''}`} · ${tiles.length} tiles · ${fmtMs(d.elapsed_ms)}${d.cache ? ` · ${d.cache.hits} cached` : ''}</div></div>
      <div class="actions"><${Select} label="Auto refresh" size="sm" value=${String(auto)} onChange=${(v) => { setAuto(Number(v)); P.set('dash.auto', Number(v)); }} options=${[['0', 'Off'], ['30', 'Every 30 s'], ['60', 'Every minute'], ['300', 'Every 5 minutes']].map(([value, label]) => ({ value, label }))} />
        <${Btn} icon="refresh" busy=${run.busy} onClick=${() => load(true)} data-testid="dash-refresh">Refresh<//>
        <${MenuButton} label="Export dashboard" icon="download" iconOnly=${false} kind="" size="" items=${[{ label: 'All tile data (CSV)', onClick: exportAll }, { label: 'Print or save as PDF', icon: 'file', onClick: () => window.print() }]}>Export<//>
        ${canEdit ? html`<${Btn} icon="edit" href=${`#/dashboards/${enc(id)}/edit`}>Design<//>` : null}</div></header>
    ${d.filters.length ? html`<div class="filter-bar" role="group" aria-label="Dashboard filters">${d.filters.map((f) => html`<${FilterControl} key=${f.name} f=${f} value=${filters ? filters[f.name] : f.value} onChange=${(v) => setF(f.name, v)} />`)}<${Btn} size="sm" kind="ghost" icon="undo" onClick=${() => { const n = Object.fromEntries(d.filters.map((f) => [f.name, f.default ?? null])); setFilters(n); P.set(storeKey, n); }}>Reset<//></div>` : null}
    ${d.partial ? html`<${Banner} kind="warn" title="Some tiles failed">${d.errors} tile${d.errors === 1 ? '' : 's'} could not be computed. The rest are current.<//>` : null}
    ${run.error ? html`<${ErrorBox} error=${run.error} />` : null}
    <div class="dash-grid" data-testid="dash-grid">${tiles.map((t) => html`<${Tile} key=${t.id} tile=${{ ...t, sql: (item.data && item.data.definition.tiles.find((x) => x.id === t.id) || {}).sql }} onRetry=${() => load(true)} onOpenSql=${(tl) => navigate(`/sql?q=${enc(tl.sql)}`)} />`)}</div></div>`;
}

// ---- designer -----------------------------------------------------------------------------------------------------------------------------------
function paramValues(filters, vals) {
  const p = {};
  for (const f of filters || []) {
    const v = vals[f.name] !== undefined ? vals[f.name] : f.default;
    if (f.type === 'date_range') { p[`${f.name}_from`] = (v || {}).from ?? null; p[`${f.name}_to`] = (v || {}).to ?? null; } else p[f.name] = v === undefined ? null : v;
  }
  return p;
}

export function ReportDesignerPage({ params }) {
  const id = params.id;
  const item = useAsync(() => api.full('GET', `/items/${enc(id)}`), [id]);
  const lhs = useAsync(async () => { const it = (await api.get(`/items/${enc(id)}`)); return (await api.get(`/workspaces/${enc(it.workspace_id)}/lakehouses`).catch(() => ({ items: [] }))).items || []; }, [id]);
  const [def, setDef] = useState(null);
  const [etag, setEtag] = useState(null);
  const [sel, setSel] = useState(null); // tile id | '__report'
  const [prev, setPrev] = useState({}); // tile id -> {columns, rows, error, loading}
  const [dirty, setDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [vals, setVals] = useState({});
  const [problems, setProblems] = useState(null);
  useEffect(() => { if (item.data && !def) { setDef(clone(item.data.data.definition)); setEtag(item.data.data.etag || item.data.etag); setSel(((item.data.data.definition.tiles || [])[0] || {}).id || '__report'); } }, [item.data]);
  const edit = (fn) => { setDef((d) => { const n = clone(d); fn(n); return n; }); setDirty(true); };
  const runTile = useCallback(async (tile, d, v) => {
    setPrev((p) => ({ ...p, [tile.id]: { ...(p[tile.id] || {}), loading: true } }));
    try {
      const lhName = ((lhs.data || []).find((l) => l.id === tile.lakehouse_id) || {}).name;
      const names = detectParams(tile.sql || '');
      const pv = paramValues(d.filters, v); const parameters = {}; for (const n of names) parameters[n] = pv[n] ?? null;
      const wsid = item.data.data.workspace_id;
      const r = await api.post('/sql/execute', { sql: tile.sql, engine: tile.engine || 'duckdb', workspace_id: wsid, lakehouse: lhName, parameters, max_rows: 2000 });
      setPrev((p) => ({ ...p, [tile.id]: { columns: r.columns, rows: r.rows, row_count: r.row_count, elapsed_ms: (r.stats || {}).elapsed_ms } }));
    } catch (e) { setPrev((p) => ({ ...p, [tile.id]: { error: describeError(e).title + (describeError(e).remediation ? ' ' + describeError(e).remediation : '') } })); }
  }, [lhs.data, item.data]);
  // preview: run every tile once, then re-run a tile when its SQL or filters change
  const lastSql = useRef({});
  useEffect(() => {
    if (!def || !lhs.data || !item.data) return undefined;
    const t = setTimeout(() => { for (const tile of def.tiles) { const key = tile.sql + '|' + (tile.lakehouse_id || '') + '|' + JSON.stringify(vals); if (lastSql.current[tile.id] !== key) { lastSql.current[tile.id] = key; runTile(tile, def, vals); } } }, 500);
    return () => clearTimeout(t);
  }, [def, lhs.data, item.data, JSON.stringify(vals)]);
  if (item.loading || !def) return html`<${Loading} label="Opening designer" />`;
  if (item.error) return html`<div class="page"><${ErrorBox} error=${item.error} /></div>`;
  const tile = def.tiles.find((t) => t.id === sel);
  const upTile = (patch) => edit((d) => { const t = d.tiles.find((x) => x.id === sel); Object.assign(t, patch); for (const k of Object.keys(patch)) if (patch[k] === '' || patch[k] === undefined) delete t[k]; });
  const addTile = (type) => {
    const tid = 'tile_' + hex(4);
    const first = (lhs.data || [])[0];
    edit((d) => { d.tiles.push({ id: tid, title: 'New tile', type, sql: type === 'kpi' ? 'SELECT 42 AS value' : 'SELECT 1 AS x, 2 AS y', ...(first ? { lakehouse_id: first.id } : {}), engine: 'duckdb', width: type === 'kpi' ? 3 : 6, height: type === 'kpi' ? 1 : 3, ...(type !== 'kpi' && type !== 'table' ? { x: 'x', y: 'y' } : {}) }); });
    setSel(tid);
  };
  const move = (i, dlt) => edit((d) => { const j = i + dlt; if (j < 0 || j >= d.tiles.length) return; [d.tiles[i], d.tiles[j]] = [d.tiles[j], d.tiles[i]]; });
  async function save() {
    setSaving(true); setProblems(null);
    try {
      const { data, etag: et } = await api.full('PUT', `/items/${enc(id)}`, { body: { definition: def, comment: 'edited in the dashboard designer' }, ifMatch: etag });
      setEtag(data.etag || (et || '').replace(/"/g, '')); setDirty(false); toast.ok('Dashboard saved');
    } catch (e) { setProblems(e); } finally { setSaving(false); }
  }
  const tiles = def.tiles.map((t) => ({ ...t, ...(prev[t.id] || {}), view: { x: t.x, y: t.y, series: t.series, format: t.format, width: t.width, height: t.height, orientation: t.orientation, stack: t.stack }, loading: !prev[t.id] || prev[t.id].loading }));
  const cols = tile && prev[tile.id] && prev[tile.id].columns ? prev[tile.id].columns.map((c) => c.name) : [];
  const selFilter = null;
  return html`<div class="page wide designer">
    <header class="page-head"><div class="titles"><h1><${Icon} name="edit" size="lg" />Designing: ${def.title}</h1><div class="sub">Changes preview live with the filter values below. Save to publish them to the dashboard.</div></div>
      <div class="actions">${dirty ? html`<${Badge} kind="warn">Unsaved changes<//>` : html`<${Badge} kind="ok">Saved<//>`}<${Btn} href=${`#/dashboards/${enc(id)}`}>View dashboard<//><${Btn} kind="primary" icon="save" busy=${saving} disabled=${!dirty} onClick=${save} data-testid="dash-save">Save<//></div></header>
    ${problems ? html`<${ErrorBox} error=${problems} />` : null}
    <div class="designer-grid">
      <aside class="card" aria-label="Tiles"><div class="card-head"><h3>Tiles</h3><${MenuButton} label="Add tile" icon="plus" iconOnly=${false} kind="" size="sm" items=${TILE_TYPES.map(([t, l]) => ({ label: l, onClick: () => addTile(t) }))}>Add<//></div>
        <ul class="tile-list">${def.tiles.map((t, i) => html`<li key=${t.id} class=${sel === t.id ? 'on' : ''}><button class="grow" onClick=${() => setSel(t.id)}><${Icon} name=${{ kpi: 'gauge', table: 'table', donut: 'pie', line: 'chart-line', area: 'chart-line' }[t.type] || 'chart'} size="sm" /> ${t.title}<span class="small muted">${t.type}</span></button>
          <${Btn} size="sm" kind="ghost" iconOnly icon="arrow-up" label=${`Move ${t.title} up`} disabled=${i === 0} onClick=${() => move(i, -1)} /><${Btn} size="sm" kind="ghost" iconOnly icon="arrow-down" label=${`Move ${t.title} down`} disabled=${i === def.tiles.length - 1} onClick=${() => move(i, 1)} /></li>`)}</ul>
        <div class="card-body"><${Btn} size="sm" icon="sliders" onClick=${() => setSel('__report')}>Dashboard and filters<//></div></aside>
      <main><div class="filter-bar mb-3" role="group" aria-label="Preview filters">${(def.filters || []).map((f) => html`<${FilterControl} key=${f.name} f=${f} value=${vals[f.name] !== undefined ? vals[f.name] : f.default} onChange=${(v) => setVals((s) => ({ ...s, [f.name]: v }))} />`)}${(def.filters || []).length ? null : html`<span class="small muted">No filters yet. Add one under Dashboard and filters.</span>`}</div>
        <div class="dash-grid">${tiles.map((t) => html`<${Tile} key=${t.id} tile=${t} selected=${sel === t.id} onSelect=${setSel} designer=${true} />`)}</div></main>
      <aside class="card inspector" aria-label="Properties"><div class="card-body">${sel === '__report' || !tile ? html`<${ReportProps} def=${def} edit=${edit} lhs=${lhs.data || []} />` : html`
        <h3 class="mt-0">Tile properties</h3>
        <${Input} label="Title" value=${tile.title} onInput=${(v) => upTile({ title: v })} />
        <div class="form-grid"><${Select} label="Type" value=${tile.type} onChange=${(v) => upTile({ type: v })} options=${TILE_TYPES.map(([value, label]) => ({ value, label }))} />
        <${Select} label="Number format" value=${tile.format || ''} onChange=${(v) => upTile({ format: v })} options=${FORMATS.map(([value, label]) => ({ value, label }))} /></div>
        <div class="form-grid"><${Select} label="Lakehouse" value=${tile.lakehouse_id || ''} onChange=${(v) => upTile({ lakehouse_id: v })} placeholder="None" options=${(lhs.data || []).map((l) => ({ value: l.id, label: l.name }))} /><${Select} label="Engine" value=${tile.engine || 'duckdb'} onChange=${(v) => upTile({ engine: v })} options=${[{ value: 'duckdb', label: 'DuckDB' }, { value: 'spark', label: 'Spark SQL' }]} /></div>
        <div class="field"><label>Query</label><${CodeEditor} key=${tile.id} value=${tile.sql || ''} mode="sql" onChange=${(v) => upTile({ sql: v })} autoHeight=${false} height="150px" label="Tile query" /><div class="hint">Use :filter_name for filter values. Date ranges give :name_from and :name_to.</div></div>
        ${!['kpi', 'table'].includes(tile.type) ? html`<div class="form-grid"><${Select} label="X (category)" value=${tile.x || ''} onChange=${(v) => upTile({ x: v })} placeholder="Automatic" options=${cols.map((c) => ({ value: c, label: c }))} /><${Select} label="Y (measure)" value=${[].concat(tile.y || [])[0] || ''} onChange=${(v) => upTile({ y: v })} placeholder="Automatic" options=${cols.map((c) => ({ value: c, label: c }))} /></div>
          <${Select} label="Series or colour" value=${tile.series || ''} onChange=${(v) => upTile({ series: v })} placeholder="None" options=${cols.map((c) => ({ value: c, label: c }))} />` : null}
        <div class="form-grid"><${Input} label="Width (1 to 12)" type="number" min="1" max="12" value=${tile.width ?? 4} onInput=${(v) => upTile({ width: Math.max(1, Math.min(12, Number(v) || 1)) })} /><${Input} label="Height (rows)" type="number" min="1" max="8" value=${tile.height ?? 2} onInput=${(v) => upTile({ height: Math.max(1, Math.min(8, Number(v) || 1)) })} /></div>
        <div class="row gap-2 mt-3"><${Btn} size="sm" icon="refresh" onClick=${() => { lastSql.current[tile.id] = null; runTile(tile, def, vals); }}>Re-run<//><${Btn} size="sm" kind="danger" icon="trash" onClick=${async () => { if (await confirmDialog({ title: `Remove ${tile.title}?`, danger: true, confirmLabel: 'Remove', body: 'The tile is removed when you save.' })) { edit((d) => { d.tiles = d.tiles.filter((x) => x.id !== tile.id); }); setSel('__report'); } }}>Remove tile<//></div>`}</div></aside>
    </div></div>`;
}

function ReportProps({ def, edit, lhs }) {
  return html`<h3 class="mt-0">Dashboard</h3><${Input} label="Title" value=${def.title} onInput=${(v) => edit((d) => { d.title = v; })} />
    <${Select} label="Auto refresh" value=${String(def.refresh_seconds || '')} onChange=${(v) => edit((d) => { d.refresh_seconds = v ? Number(v) : null; })} placeholder="Off" options=${[['30', 'Every 30 s'], ['60', 'Every minute'], ['300', 'Every 5 minutes']].map(([value, label]) => ({ value, label }))} />
    <h3 class="mt-4">Filters</h3>
    ${(def.filters || []).map((f, i) => html`<div class="fieldset mb-3" key=${i}><div class="form-grid"><${Input} label="Name" value=${f.name} mono=${true} onInput=${(v) => edit((d) => { d.filters[i].name = v.replace(/\W+/g, '_'); })} /><${Input} label="Label" value=${f.label || ''} onInput=${(v) => edit((d) => { d.filters[i].label = v; })} /></div>
      <${Select} label="Type" value=${f.type} onChange=${(v) => edit((d) => { d.filters[i].type = v; })} options=${['date_range', 'select', 'text', 'number', 'date', 'boolean'].map((t) => ({ value: t, label: t.replace('_', ' ') }))} />
      ${f.type === 'select' ? html`<${Textarea} label="Options query (first column)" mono=${true} rows=${2} value=${f.options_sql || ''} onInput=${(v) => edit((d) => { d.filters[i].options_sql = v; })} /><${Select} label="Options lakehouse" value=${f.options_lakehouse_id || ''} onChange=${(v) => edit((d) => { d.filters[i].options_lakehouse_id = v; })} placeholder="None" options=${lhs.map((l) => ({ value: l.id, label: l.name }))} />` : null}
      ${f.type === 'date_range' ? html`<div class="form-grid"><${Input} label="Default from" type="date" value=${(f.default || {}).from || ''} onInput=${(v) => edit((d) => { d.filters[i].default = { ...(d.filters[i].default || {}), from: v }; })} /><${Input} label="Default to" type="date" value=${(f.default || {}).to || ''} onInput=${(v) => edit((d) => { d.filters[i].default = { ...(d.filters[i].default || {}), to: v }; })} /></div>` : null}
      <${Btn} size="sm" kind="ghost" icon="trash" onClick=${() => edit((d) => { d.filters.splice(i, 1); })}>Remove filter<//></div>`)}
    <${Btn} size="sm" icon="plus" onClick=${() => edit((d) => { (d.filters ||= []).push({ name: 'filter_' + (d.filters.length + 1), label: 'Filter', type: 'select', default: null, options_sql: '' }); })}>Add filter<//>`;
}
