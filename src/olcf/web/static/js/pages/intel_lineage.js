// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Lineage explorer: a layered, left-to-right picture of where data comes from and where it goes, with evidence for every connection. The component is used by the lineage
// page and by the asset page; the layout (intel/layout.js) and the drawing rules (intel/graphdraw.js) are plain modules with their own tests.
import { html, useState, useEffect, useRef, useMemo, useCallback, useAsync, useDebounced } from '../core/h.js';
import { api } from '../api/client.js';
import { setQuery, useRoute } from '../core/router.js';
import { Btn, Banner, Empty, ErrorBox, Loading, PageHead, Seg, Select, Check, Badge, Input } from '../ui/basic.js';
import { Icon } from '../ui/icons.js';
import { MenuButton, toast } from '../ui/overlay.js';
import { Table } from '../ui/data.js';
import { downloadBlob, downloadText, fmtNum, enc, plural } from '../core/util.js';
import { layoutLineage } from '../intel/layout.js';
import { EDGE_DASH, EDGE_WIDTH, graphToSvg, layerHeadings, nodeLines } from '../intel/graphdraw.js';
import { mergeGraphs, adjacency, related, neighbour, matchNodes, fitView, initialView, zoomAt, ensureVisible, centreOn, mappingRows, columnLabel, edgeSentence, graphJson, HEAD } from '../intel/graphops.js';
import { typeMeta, typeLabel, categoryOf, isProcess, CONFIDENCE, confLabel, CONF_HELP, layerOf, assetHref, lineageHref, impactHref, gapMeta, pathTail, TYPE_META } from '../intel/model.js';
import { AssetLink, ConfBadge, TypeChip, Ago } from './intel_common.js';

const DIRS = [{ value: 'up', label: 'Upstream', title: 'Where the data comes from' }, { value: 'down', label: 'Downstream', title: 'What uses the data' }, { value: 'both', label: 'Both', title: 'Both directions' }];
const LEVELS = [{ value: 'table', label: 'Tables', title: 'Which tables, notebooks and reports connect' }, { value: 'column', label: 'Columns', title: 'Which columns feed which columns' }];
const CONF_OPTIONS = [{ value: '', label: 'Show everything' }, { value: 'inferred', label: 'Inferred or better' }, { value: 'derived', label: 'Derived or better' }, { value: 'verified', label: 'Verified only' }];
const NON_PROCESS_TYPES = Object.keys(TYPE_META).filter((t) => !isProcess(t) && t !== 'column');       // every kind of asset that is not a step in between (model tables and measures included)

function colorsFromCss(el) {
  const cs = getComputedStyle(el);
  const v = (n) => cs.getPropertyValue(n).trim();
  return { bg: v('--surface-2'), surface: v('--surface'), ink: v('--ink'), ink3: v('--ink-3'), lineStrong: v('--line-strong'), accent: v('--accent'), font: cs.fontFamily,
    source: v('--lx-source'), dataset: v('--lx-dataset'), process: v('--lx-process'), model: v('--lx-model'), report: v('--lx-report'), container: v('--lx-container'), column: v('--lx-column') };
}

async function svgToPng(svgText, scale = 2) {
  const m = /width="(\d+)" height="(\d+)"/.exec(svgText);
  const w = m ? +m[1] : 800; const h = m ? +m[2] : 600;
  const s = Math.min(scale, 16000 / Math.max(w, h));
  const img = new Image();
  img.src = 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svgText);
  await img.decode();
  const canvas = document.createElement('canvas');
  canvas.width = Math.round(w * s); canvas.height = Math.round(h * s);
  const ctx = canvas.getContext('2d');
  ctx.scale(s, s);
  ctx.drawImage(img, 0, 0, w, h);
  return new Promise((resolve, reject) => canvas.toBlob((b) => (b ? resolve(b) : reject(new Error('The picture could not be made'))), 'image/png'));
}

/** "cell 3, line 12" from an evidence location object. */
export function formatLocation(loc) {
  if (!loc || typeof loc !== 'object') return '';
  return Object.entries(loc).filter(([, v]) => v != null && v !== '').map(([k, v]) => `${k.replace(/_/g, ' ')} ${typeof v === 'object' ? JSON.stringify(v) : v}`).join(', ');
}

// ---------------------------------------------------------------------------------------------------------------- evidence of one connection
function Evidence({ detail }) {
  const ev = detail.evidence || [];
  return html`<div>
    ${detail.transformation ? html`<div class="ev"><b>What happens to the data</b><code class="mono wrap-any">${detail.transformation}</code></div>` : null}
    <div class="ev"><div class="small muted">Seen ${fmtNum(detail.observation_count || 0)} time${detail.observation_count === 1 ? '' : 's'}${detail.last_seen_at ? html`, last <${Ago} ts=${detail.last_seen_at} />` : ''}.${detail.last_verified_at ? html` Last confirmed while data was processed <${Ago} ts=${detail.last_verified_at} />.` : ' Never confirmed while data was being processed.'}</div></div>
    ${ev.length ? ev.map((x, i) => html`<div class="ev" key=${i}>
      <div class="row-wrap"><${Badge} mono>${x.kind}<//><${ConfBadge} level=${x.confidence} /><span class="small muted">found by ${x.source}${x.observed_at ? html` · <${Ago} ts=${x.observed_at} />` : ''}</span></div>
      ${formatLocation(x.location) ? html`<div class="small"><span class="muted">Where:</span> ${formatLocation(x.location)}</div>` : null}
      ${x.snippet ? html`<pre class="snip">${x.snippet}</pre>` : null}</div>`) : html`<div class="ev muted">No evidence is stored for this connection.</div>`}</div>`;
}

function EdgeItem({ edge, label, open: initialOpen, always }) {
  const [open, setOpen] = useState(!!initialOpen);
  const st = useAsync(() => (open && !edge.virtual ? api.get(`/intelligence/edges/${enc(edge.id)}`) : Promise.resolve(null)), [open, edge.id]);
  const body = edge.virtual ? html`<div class="ev muted">This link skips over steps that are hidden by the current filters, so it has no evidence of its own.</div>`
    : st.error ? html`<div class="ev"><${ErrorBox} error=${st.error} compact /></div>` : st.data ? html`<${Evidence} detail=${st.data} />` : open ? html`<div class="ev"><${Loading} label="Loading the evidence" /></div>` : null;
  return html`<details open=${open || always} data-testid="edge-evidence" onToggle=${(e) => setOpen(e.currentTarget.open)}><summary><span class="grow wrap-any">${label}</span><${ConfBadge} level=${edge.confidence} /></summary>${body}</details>`;
}

// ---------------------------------------------------------------------------------------------------------------- side panel
function NodePanel({ node, edges, nodeById, adj, expanded, canExpand, onExpand, onSelectEdge, onClose, rootId }) {
  const ins = adj.inn.get(node.id) || [];
  const outs = adj.out.get(node.id) || [];
  const layer = layerOf(node);
  const nameOf = (id) => (nodeById.get(id) || {}).name || id;
  const first = ins[0] || outs[0];
  return html`<aside class="lx-panel" aria-label="Details of the selected asset" data-testid="lineage-panel">
    <div class="pad col gap-2">
      <div class="row"><h3 class="grow">${node.name}</h3><${Btn} kind="ghost" size="sm" iconOnly icon="x" label="Close details" onClick=${onClose} /></div>
      <div class="row-wrap"><${TypeChip} type=${node.type} />${layer ? html`<${Badge} title="Based on the name of the asset, not something the platform checked">${layer} (by name)<//>` : null}${node.id === rootId ? html`<${Badge} kind="accent">Starting point<//>` : null}${node.placeholder ? html`<${Badge} kind="warn" title="Something refers to this, but no scan has found it yet.">Not found by a scan<//>` : null}</div>
      ${node.path ? html`<div class="small muted wrap-any">${node.path}</div>` : null}
      ${node.description ? html`<div class="small">${node.description}</div>` : null}
      <div class="row-wrap"><${Btn} size="sm" icon="external" href=${assetHref(node.id)}>Open asset<//>
        ${node.id !== rootId ? html`<${Btn} size="sm" icon="route" href=${lineageHref(node.id)}>Start from here<//>` : null}
        <${Btn} size="sm" icon="activity" href=${impactHref(node.id)}>Impact<//>
        ${canExpand && !expanded ? html`<${Btn} size="sm" icon="plus" onClick=${onExpand}>Show what is connected<//>` : null}</div>
    </div>
    <div class="sect"><h4>Data comes from (${ins.length})</h4>${ins.length ? ins.map((e, i) => html`<${EdgeItem} key=${e.id} edge=${e} open=${e === first} label=${edgeSentence(e, nameOf)} />`) : html`<div class="small muted">Nothing upstream was found${node.direction === 'downstream' ? ' within this view' : ''}. ${isProcess(node.type) ? '' : 'That can mean this is a true source, or that the scans could not see what feeds it.'}</div>`}</div>
    <div class="sect"><h4>Data goes to (${outs.length})</h4>${outs.length ? outs.map((e) => html`<${EdgeItem} key=${e.id} edge=${e} open=${e === first} label=${edgeSentence(e, nameOf)} />`) : html`<div class="small muted">Nothing downstream was found within this view.</div>`}</div>
  </aside>`;
}

function EdgePanel({ edge, nodeById, onClose }) {
  const nameOf = (id) => (nodeById.get(id) || {}).name || id;
  return html`<aside class="lx-panel" aria-label="Details of the selected connection" data-testid="lineage-panel">
    <div class="pad col gap-2"><div class="row"><h3 class="grow">Connection</h3><${Btn} kind="ghost" size="sm" iconOnly icon="x" label="Close details" onClick=${onClose} /></div>
      <div class="wrap-any">${edgeSentence(edge, nameOf)}</div>
      <div class="small muted">${CONF_HELP[edge.confidence] || ''}</div></div>
    <div class="sect"><h4>Evidence</h4><${EdgeItem} edge=${edge} open=${true} always=${true} label="Why we think so" /></div></aside>`;
}

// ---------------------------------------------------------------------------------------------------------------- legend
export function LineageLegend() {
  return html`<div class="lx-legend" aria-label="Legend">
    ${CONFIDENCE.map((c) => html`<span class="item" key=${c} title=${CONF_HELP[c]}><svg width="30" height="8" aria-hidden="true"><line x1="1" y1="4" x2="29" y2="4" stroke="currentColor" stroke-width=${EDGE_WIDTH[c]} stroke-dasharray=${EDGE_DASH[c] || undefined} stroke-linecap=${c === 'unknown' ? 'round' : undefined} /></svg>${confLabel(c)}</span>`)}
    <span class="item muted">Line style shows how sure we are. Process steps sit between the tables they read and write.</span></div>`;
}

// ---------------------------------------------------------------------------------------------------------------- column level
function ColumnLineage({ assetId, root, opts }) {
  const [column, setColumn] = useState('');
  const [filter, setFilter] = useState('');
  const isTable = root && (root.type === 'table' || root.type === 'view' || root.type === 'model_table');
  const cols = useAsync(() => (isTable ? api.get(`/intelligence/assets/${enc(assetId)}`).then((a) => ((a.attrs && a.attrs.columns) || []).map((c) => c.name)) : Promise.resolve([])), [assetId, isTable]);
  const st = useAsync(() => api.get(`/intelligence/assets/${enc(assetId)}/lineage`, { direction: opts.direction, depth: opts.depth, level: 'column', min_confidence: opts.minConf || undefined, column: column || undefined }), [assetId, opts.direction, opts.depth, opts.minConf, column]);
  const rows = useMemo(() => (st.data ? mappingRows(st.data, assetId) : []), [st.data, assetId]);
  const shown = useMemo(() => { const f = filter.trim().toLowerCase(); return f ? rows.filter((r) => `${columnLabel(r.from)} ${columnLabel(r.to)} ${r.transformation}`.toLowerCase().includes(f)) : rows; }, [rows, filter]);
  const own = st.data ? st.data.nodes.filter((n) => n.parent_id === assetId) : [];
  const mapped = new Set();
  for (const r of rows) { if (r.from.parent_id === assetId) mapped.add(r.from.id); if (r.to.parent_id === assetId) mapped.add(r.to.id); }
  const columns = [
    { key: 'from', label: 'From column', sort: (r) => columnLabel(r.from), render: (r) => html`<span><${AssetLink} asset=${r.from} /><div class="small muted wrap-any">${pathTail(r.from.path, 3)}</div></span>` },
    { key: 'to', label: 'To column', sort: (r) => columnLabel(r.to), render: (r) => html`<span><${AssetLink} asset=${r.to} /><div class="small muted wrap-any">${pathTail(r.to.path, 3)}</div></span>` },
    { key: 'transformation', label: 'What happens to it', render: (r) => (r.transformation ? html`<code class="mono wrap-any">${r.transformation}</code>` : html`<span class="muted">Not known</span>`) },
    { key: 'via', label: 'Done by', sort: (r) => (r.via ? r.via.name : ''), render: (r) => (r.via ? html`<${AssetLink} asset=${r.via} />` : html`<span class="muted">Not known</span>`) },
    { key: 'confidence', label: 'How sure', sort: (r) => CONFIDENCE.indexOf(r.confidence), render: (r) => html`<${ConfBadge} level=${r.confidence} />` },
  ];
  return html`<div class="col gap-3" data-testid="column-lineage">
    <div class="filter-row">${isTable && (cols.data || []).length ? html`<${Select} label="Column" value=${column} onChange=${setColumn} placeholder="All columns" options=${cols.data} />` : null}
      <div class="grow"><${Input} label="Filter the mappings" value=${filter} onInput=${setFilter} placeholder="Column name or text of the calculation" /></div></div>
    ${st.error && !st.data ? html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />` : !st.data ? html`<${Loading} label="Loading column lineage" />` : html`
      ${st.data.truncated ? html`<${Banner} kind="warn" title="Only part of the column lineage is shown">The answer was cut at ${st.data.stats && st.data.stats.max_nodes} columns. Pick one column or reduce the depth.<//>` : null}
      ${isTable && own.length ? html`<div class="small ${mapped.size < own.length ? '' : 'muted'}" role="status">${mapped.size} of ${own.length} columns of this table have a recorded mapping${mapped.size < own.length ? '. The others may still be connected: the code that moves them could not be followed.' : '.'}</div>` : null}
      ${shown.length ? html`<div class="lx-table-wrap"><${Table} caption="Column mappings" rows=${shown} columns=${columns} dense=${true} /></div>` : html`<${Empty} icon="list" title=${rows.length ? 'No mapping matches the filter' : 'No column-level connections were found'}>${rows.length ? 'Clear the filter to see all mappings.' : 'Column lineage needs the analysers to understand the SQL or code that moves the data. Where they could not, a gap explains why. Table-level lineage may still show the connection.'}</${Empty}>`}
      ${(st.data.gaps || []).length ? html`<details><summary class="small">${plural(st.data.gaps.length, 'thing')} could not be read near here</summary>${st.data.gaps.map((g) => html`<div class="gap-item" key=${g.id}><div><b>${gapMeta(g.kind).label}</b> · ${g.subject}</div><div class="small">${g.detail}</div><div class="rem">${g.remediation}</div></div>`)}</details>` : null}`}
  </div>`;
}

// ---------------------------------------------------------------------------------------------------------------- the explorer
export function LineageExplorer({ assetId, initial = {}, onState, height, embedded, root: rootAsset }) {
  const [direction, setDirection] = useState(initial.direction || 'both');
  const [depth, setDepth] = useState(Math.min(10, Math.max(1, +initial.depth || 4)));
  const [level, setLevel] = useState(initial.level === 'column' ? 'column' : 'table');
  const [minConf, setMinConf] = useState(initial.conf || '');
  const [procs, setProcs] = useState(initial.procs !== '0');
  const [view, setView] = useState('graph');
  const [search, setSearch] = useState('');
  const [sel, setSel] = useState(null);
  const [extra, setExtra] = useState([]);
  const [expanded, setExpanded] = useState(() => new Set());
  const [matchAt, setMatchAt] = useState(0);
  const dDepth = useDebounced(depth, 300);
  useEffect(() => { onState && onState({ direction, depth: dDepth, level, conf: minConf, procs: procs ? '' : '0' }); }, [direction, dDepth, level, minConf, procs]);

  const sig = [assetId, direction, dDepth, minConf, procs].join('|');
  const base = useAsync(() => (level === 'table' ? api.get(`/intelligence/assets/${enc(assetId)}/lineage`, { direction, depth: dDepth, level: 'table', min_confidence: minConf || undefined, types: procs ? undefined : NON_PROCESS_TYPES }) : Promise.resolve(null)), [sig, level]);
  useEffect(() => { setExtra([]); setExpanded(new Set()); setSel(null); }, [sig]);

  const graph = useMemo(() => (base.data ? mergeGraphs([base.data, ...extra]) : { nodes: [], edges: [] }), [base.data, extra]);
  const nodeById = useMemo(() => new Map(graph.nodes.map((n) => [n.id, n])), [graph]);
  const adj = useMemo(() => adjacency(graph.edges), [graph]);
  const layout = useMemo(() => {
    // many lines leaving one asset need room to fan out: widen the gap between steps with the busiest asset's connection count
    const deg = new Map();
    for (const e of graph.edges) { deg.set(e.from, (deg.get(e.from) || 0) + 1); deg.set(e.to, (deg.get(e.to) || 0) + 1); }
    const busiest = Math.max(0, ...deg.values());
    const gapX = Math.min(170, 40 + Math.max(0, busiest - 6) * 6);
    // boxes only as wide as the longest name needs (up to the usual width): short names leave room for a bigger, more readable picture
    const longest = Math.max(0, ...graph.nodes.filter((n) => !isProcess(n.type)).map((n) => String(n.name || '').length));
    const nodeW = Math.max(156, Math.min(208, Math.round(longest * 7.4 + 44)));
    return layoutLineage(graph.nodes.map((n) => ({ id: n.id, name: n.name, type: n.type, process: isProcess(n.type) })), graph.edges.map((e) => ({ id: e.id, from: e.from, to: e.to })), { gapX, nodeW });
  }, [graph]);
  const layoutNode = useMemo(() => new Map(layout.nodes.map((n) => [n.id, n])), [layout]);
  const edgeById = useMemo(() => new Map(graph.edges.map((e) => [e.id, e])), [graph]);
  const heads = useMemo(() => layerHeadings(layout, nodeById), [layout, nodeById]);
  const rootId = base.data && base.data.root ? base.data.root.id : assetId;
  const frontier = useMemo(() => new Set(graph.nodes.filter((n) => n.level != null && n.level >= dDepth && n.direction !== 'root').map((n) => n.id)), [graph, dDepth]);
  const rel = useMemo(() => (sel && sel.kind === 'node' ? related(adj, sel.id) : null), [adj, sel]);
  const matches = useMemo(() => matchNodes(graph.nodes, layout, search), [graph, layout, search]);

  // ---- view (zoom and pan)
  const canvas = useRef(null);
  const [size, setSize] = useState({ w: 900, h: 560 });
  const [vw, setVw] = useState({ k: 1, tx: 0, ty: 0 });
  const vref = useRef(vw); vref.current = vw;
  const fitted = useRef(null);
  const touched = useRef(false);                    // once the person has moved or zoomed the picture it is no longer re-fitted when the window changes size
  const [measured, setMeasured] = useState(false);
  const [partial, setPartial] = useState(null);     // set while only the area around the starting asset is in view: how many assets that is
  useEffect(() => {
    if (!canvas.current || typeof ResizeObserver === 'undefined') return undefined;
    const ro = new ResizeObserver((es) => { const r = es[0].contentRect; if (r.width > 20 && r.height > 20) { setSize({ w: r.width, h: r.height }); setMeasured(true); } });
    ro.observe(canvas.current);
    return () => ro.disconnect();
  }, [view, level, base.data]);
  const refit = () => {
    const r = initialView(layout, rootId, size.w, size.h);
    setVw(r.view); setPartial(r.partial ? { shown: r.shown, total: layout.nodes.length } : null);
  };
  useEffect(() => {
    // fit the picture to the window whenever a new answer arrives (first load, direction, depth, filters), not when one node is expanded, and again if the window is still settling
    // and nobody has touched the picture; a picture too big to read when fitted opens around the starting asset instead (see initialView)
    if (!base.data || !measured) return;
    if (fitted.current === base.data && touched.current) return;
    if (fitted.current !== base.data) touched.current = false;
    fitted.current = base.data;
    refit();
  }, [base.data, layout, measured, size.w, size.h]);  // eslint-disable-line
  const fit = () => { touched.current = false; setVw(fitView(layout, size.w, size.h)); setPartial(null); };
  const zoom = (f) => { touched.current = true; setPartial(null); setVw((v) => zoomAt(v, f, size.w / 2, size.h / 2)); };
  const drag = useRef(null);
  const onPointerDown = (e) => {
    if (e.button !== 0 || e.target.closest('.lx-node, .lx-tools, .lx-hit')) return;
    touched.current = true;
    drag.current = { x: e.clientX, y: e.clientY, tx: vref.current.tx, ty: vref.current.ty, moved: false };
    e.currentTarget.setPointerCapture(e.pointerId);
    e.currentTarget.classList.add('panning');
  };
  const onPointerMove = (e) => {
    const d = drag.current;
    if (!d) return;
    const dx = e.clientX - d.x; const dy = e.clientY - d.y;
    if (Math.abs(dx) + Math.abs(dy) > 3) d.moved = true;
    setVw((v) => ({ ...v, tx: d.tx + dx, ty: d.ty + dy }));
  };
  const onPointerUp = (e) => {
    const d = drag.current; drag.current = null;
    e.currentTarget.classList.remove('panning');
    if (d && !d.moved) setSel(null);
  };
  const onWheel = (e) => {
    if (!(e.ctrlKey || e.metaKey)) return;
    e.preventDefault();
    touched.current = true; setPartial(null);
    const r = canvas.current.getBoundingClientRect();
    setVw((v) => zoomAt(v, e.deltaY < 0 ? 1.15 : 1 / 1.15, e.clientX - r.left, e.clientY - r.top));
  };

  // ---- actions
  const select = useCallback((id, focus) => {
    touched.current = true;
    setSel({ kind: 'node', id });
    const n = layoutNode.get(id);
    if (n) setVw((v) => ensureVisible(v, n, size.w, size.h));
    if (focus) requestAnimationFrame(() => { const el = canvas.current && canvas.current.querySelector(`[data-node="${CSS.escape(id)}"]`); if (el) el.focus(); });
  }, [layoutNode, size]);
  const expand = async (id) => {
    if (expanded.has(id)) return;
    touched.current = true;
    setExpanded((s) => new Set(s).add(id));
    try {
      const r = await api.get(`/intelligence/assets/${enc(id)}/lineage`, { direction: 'both', depth: 1, level: 'table', min_confidence: minConf || undefined, types: procs ? undefined : NON_PROCESS_TYPES });
      const before = graph.nodes.length;
      setExtra((x) => [...x, r]);
      const added = mergeGraphs([graph, r]).nodes.length - before;
      toast.info(added ? `Added ${added} connected asset${added === 1 ? '' : 's'}` : 'Nothing more is connected to this asset');
    } catch (e) { setExpanded((s) => { const n = new Set(s); n.delete(id); return n; }); toast.bad('Could not load what is connected', e && e.message); }
  };
  const onKey = (e) => {
    if (e.key !== 'Tab') touched.current = true;
    const nodeEl = e.target.closest('[data-node]');
    const id = nodeEl ? nodeEl.getAttribute('data-node') : null;
    if (e.key === '+' || e.key === '=') { e.preventDefault(); zoom(1.2); return; }
    if (e.key === '-' || e.key === '_') { e.preventDefault(); zoom(1 / 1.2); return; }
    if (e.key === '0') { e.preventDefault(); fit(); return; }
    if (!id) return;
    if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); select(id); return; }
    if (e.key === 'Escape') { setSel(null); return; }
    if ((e.key === 'e' || e.key === 'E') && !isProcess((nodeById.get(id) || {}).type)) { e.preventDefault(); expand(id); return; }
    if (e.key.startsWith('Arrow')) {
      e.preventDefault();
      const next = neighbour(layout, adj, id, e.key);
      if (next) { const n = layoutNode.get(next); if (n) setVw((v) => ensureVisible(v, n, size.w, size.h)); requestAnimationFrame(() => { const el = canvas.current.querySelector(`[data-node="${CSS.escape(next)}"]`); if (el) el.focus(); }); }
    }
  };
  const gotoMatch = (step) => {
    if (!matches.length) return;
    const i = (matchAt + step + matches.length) % matches.length;
    setMatchAt(i);
    const n = layoutNode.get(matches[i]);
    if (n) setVw((v) => centreOn(v, n, size.w, size.h));
    setSel({ kind: 'node', id: matches[i] });
  };
  useEffect(() => { setMatchAt(0); }, [search]);

  const exportSvg = () => graphToSvg({ layout, nodes: graph.nodes, edges: graph.edges, selected: sel && sel.kind === 'node' ? sel.id : null, colors: colorsFromCss(canvas.current || document.body), title: `Lineage of ${(base.data && base.data.root && base.data.root.name) || assetId}` });
  const fname = `lineage-${((base.data && base.data.root && base.data.root.name) || 'asset').replace(/[^A-Za-z0-9_-]+/g, '_')}`;
  const doExport = async (kind) => {
    try {
      if (kind === 'svg') downloadText(exportSvg(), fname + '.svg', 'image/svg+xml');
      else if (kind === 'png') downloadBlob(await svgToPng(exportSvg()), fname + '.png');
      else downloadText(graphJson({ root: base.data.root, direction, depth: dDepth, level: 'table', minConfidence: minConf, nodes: graph.nodes, edges: graph.edges, truncated: base.data.truncated }), fname + '.json', 'application/json');
    } catch (e) { toast.bad('The export failed', e && e.message); }
  };

  const data = base.data;
  const selNode = sel && sel.kind === 'node' ? nodeById.get(sel.id) : null;
  const selEdge = sel && sel.kind === 'edge' ? edgeById.get(sel.id) : null;
  const nameOf = (id) => (nodeById.get(id) || {}).name || id;
  const tabId = selNode ? selNode.id : rootId;
  const tableRows = useMemo(() => graph.edges.map((e) => ({ ...e, a: nodeById.get(e.src), b: nodeById.get(e.dst) })).sort((x, y) => (layoutNode.get(x.from) || { layer: 0 }).layer - (layoutNode.get(y.from) || { layer: 0 }).layer || String(x.id).localeCompare(String(y.id))), [graph, nodeById, layoutNode]);

  const bar = html`<div class="lx-bar" role="group" aria-label="Lineage options">
    <div><span class="lbl" id="lx-l-dir">Direction</span><${Seg} label="Direction" options=${DIRS} value=${direction} onChange=${setDirection} /></div>
    <div><span class="lbl" id="lx-l-lvl">Detail</span><${Seg} label="Detail" options=${LEVELS} value=${level} onChange=${setLevel} /></div>
    <div><label class="lbl" for="lx-depth">How many steps (${depth})</label><div class="depth"><input id="lx-depth" type="range" min="1" max="10" step="1" value=${depth} onInput=${(e) => setDepth(+e.target.value)} aria-valuetext=${`${depth} steps`} /><output>${depth}</output></div></div>
    <div style="min-width:170px"><${Select} label="How sure" value=${minConf} onChange=${setMinConf} options=${CONF_OPTIONS} /></div>
    ${level === 'table' ? html`<div><${Check} label="Show notebooks and queries between tables" checked=${procs} onChange=${setProcs} /></div>` : null}
    ${level === 'table' ? html`<div class="grow" style="min-width:180px"><label class="lbl" for="lx-search">Find in the picture</label><div class="row"><input id="lx-search" class="input" type="search" value=${search} onInput=${(e) => setSearch(e.target.value)} onKeyDown=${(e) => { if (e.key === 'Enter') { e.preventDefault(); gotoMatch(e.shiftKey ? -1 : 1); } }} placeholder="Name or type" />
      ${search ? html`<span class="small muted nowrap" role="status">${matches.length ? `${matchAt + 1} of ${matches.length}` : 'No match'}</span>` : null}</div></div>` : null}
    ${level === 'table' ? html`<div><span class="lbl">View</span><${Seg} label="View" options=${[{ value: 'graph', label: 'Picture', icon: 'route' }, { value: 'table', label: 'Table', icon: 'list' }]} value=${view} onChange=${setView} /></div>` : null}
    ${level === 'table' ? html`<${MenuButton} label="Export" icon="download" kind="" size="" iconOnly=${false} items=${[{ label: 'Picture (SVG)', onClick: () => doExport('svg') }, { label: 'Picture (PNG)', onClick: () => doExport('png') }, { label: 'Data (JSON)', onClick: () => doExport('json') }]}>Export<//>` : null}
  </div>`;

  if (level === 'column') return html`<div class="lx">${bar}<${ColumnLineage} assetId=${assetId} root=${rootAsset || (data && data.root)} opts=${{ direction, depth: dDepth, minConf }} /></div>`;

  const summary = data ? `${graph.nodes.length} asset${graph.nodes.length === 1 ? '' : 's'} and ${graph.edges.length} connection${graph.edges.length === 1 ? '' : 's'} across ${layout.layers.length} step${layout.layers.length === 1 ? '' : 's'}${data.truncated ? ', cut off at the size limit' : ''}` : '';
  const panel = selNode ? html`<${NodePanel} node=${selNode} edges=${graph.edges} nodeById=${nodeById} adj=${adj} expanded=${expanded.has(selNode.id)} canExpand=${!isProcess(selNode.type)} onExpand=${() => expand(selNode.id)} onClose=${() => setSel(null)} rootId=${rootId} />`
    : selEdge ? html`<${EdgePanel} edge=${selEdge} nodeById=${nodeById} onClose=${() => setSel(null)} />` : null;

  return html`<div class="lx" data-testid="lineage-explorer">${bar}
    ${base.error && !data ? html`<${ErrorBox} error=${base.error} onRetry=${base.reloadLoud} />` : !data ? html`<${Loading} label="Tracing the lineage" />` : html`
      ${data.truncated ? html`<${Banner} kind="warn" title="Only part of the graph is shown">The picture was cut at ${data.stats && data.stats.max_nodes} assets. Use fewer steps, raise the 'How sure' filter, or start from a more specific asset and use 'Show what is connected'.<//>` : null}
      ${!graph.edges.length ? html`<${Empty} icon="route" title="No connections were found">Nothing was found that feeds this asset or uses it${minConf ? ' at this confidence' : ''}. That does not prove there is none: look at the gaps below to see what the scans could not read.</${Empty}>` : null}
      <p class="sr-only" role="status" aria-live="polite" data-testid="lineage-summary">${summary}</p>
      ${view === 'graph' && graph.edges.length ? html`<div class=${'lx-main' + (panel ? '' : ' nopanel')} style=${height ? `--lx-h:${height}` : undefined}>
        <div class="lx-canvas" ref=${canvas} onPointerDown=${onPointerDown} onPointerMove=${onPointerMove} onPointerUp=${onPointerUp} onPointerCancel=${onPointerUp} onWheel=${onWheel}>
          <svg role="group" aria-label=${`Lineage picture: ${summary}. Use the arrow keys to move between assets, Enter to see details, E to show what is connected, plus and minus to zoom, 0 to fit.`} onKeyDown=${onKey} data-testid="lineage-svg">
            <defs><marker id="lx-ah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path class="lx-arrow" d="M0,0 L10,5 L0,10 z" /></marker></defs>
            <g transform=${`translate(${vw.tx},${vw.ty}) scale(${vw.k})`}>
              ${layout.layers.map((l, i) => (heads[i] ? html`<text key=${'h' + i} class="lx-head" x=${l.x + l.w / 2} y="18">${heads[i]}</text>` : null))}
              <g transform=${`translate(0,${HEAD})`}>
                <g class="lx-edges">${layout.edges.map((le) => { const e = edgeById.get(le.id) || {}; const on = rel && (le.from === sel.id || le.to === sel.id || ((rel.up.has(le.from) || le.from === sel.id) && (rel.up.has(le.to) || le.to === sel.id)) || ((rel.down.has(le.to) || le.to === sel.id) && (rel.down.has(le.from) || le.from === sel.id))); const isSel = sel && sel.kind === 'edge' && sel.id === le.id;
                  return html`<g key=${le.id}><path class=${`lx-edge ${e.confidence || 'unknown'}${on ? ' rel' : ''}${isSel ? ' sel' : ''}${rel && !on ? ' dimmed' : ''}`} d=${le.d} marker-end="url(#lx-ah)" data-edge=${le.id} />
                    <path class="lx-hit" d=${le.d} onClick=${(ev) => { ev.stopPropagation(); setSel({ kind: 'edge', id: le.id }); }}><title>${edgeSentence(e, nameOf)} (${confLabel(e.confidence)})</title></path></g>`; })}</g>
                <g class="lx-nodes">${layout.nodes.map((ln) => {
                  const n = nodeById.get(ln.id); if (!n) return null;
                  const cat = categoryOf(n.type); const { title, sub } = nodeLines(n);
                  const isSel = sel && sel.kind === 'node' && sel.id === n.id;
                  const dim = rel && !isSel && !rel.up.has(n.id) && !rel.down.has(n.id);
                  const ins = (adj.inn.get(n.id) || []).length; const outs = (adj.out.get(n.id) || []).length;
                  const tab = (isSel || (!sel && n.id === rootId)) ? 0 : -1;
                  return html`<g key=${n.id} class=${`lx-node ${ln.process ? 'process' : ''}${n.id === rootId ? ' root' : ''}${isSel ? ' sel' : ''}${dim ? ' dimmed' : ''}${search && matches.includes(n.id) ? ' match' : ''}`} transform=${`translate(${ln.x},${ln.y})`} role="button" tabindex=${tab} data-node=${n.id} aria-pressed=${isSel ? 'true' : 'false'}
                    aria-label=${`${n.name}, ${typeLabel(n.type)}${layerOf(n) ? `, ${layerOf(n)} by name` : ''}, ${ins} upstream and ${outs} downstream connection${ins + outs === 1 ? '' : 's'}`}
                    onClick=${(ev) => { ev.stopPropagation(); select(n.id); }} onDblClick=${(ev) => { ev.stopPropagation(); if (!ln.process) expand(n.id); }}>
                    <rect class="box" width=${ln.w} height=${ln.h} rx=${ln.process ? ln.h / 2 : 8} />
                    ${ln.process ? null : html`<rect class=${'stripe lx-cat-' + cat} width="5" height=${ln.h} rx="2" />`}
                    ${ln.process ? html`<text class="title" x=${ln.w / 2} y=${ln.h / 2 - 2}>${title}</text><text class="sub" x=${ln.w / 2} y=${ln.h / 2 + 11}>${sub}</text>`
                      : html`<text class="title" x="16" y=${ln.h / 2 - 4}>${title}</text><text class="sub" x="16" y=${ln.h / 2 + 12}>${sub}</text>`}
                    ${!ln.process && frontier.has(n.id) && !expanded.has(n.id) ? html`<g class="plus-g" transform=${`translate(${ln.w},${ln.h / 2})`} onClick=${(ev) => { ev.stopPropagation(); expand(n.id); }}><circle class="plus" r="9" /><text class="plus-t" y="4">+</text><title>Show what is connected to ${n.name}</title></g>` : null}
                    <title>${(n.path || n.name) + ' (' + typeLabel(n.type) + ')'}</title></g>`; })}</g>
              </g></g></svg>
          <div class="lx-tools"><${Btn} size="sm" iconOnly icon="plus" label="Zoom in" onClick=${() => zoom(1.25)} /><${Btn} size="sm" iconOnly icon="minus" label="Zoom out" onClick=${() => zoom(1 / 1.25)} /><${Btn} size="sm" iconOnly icon="expand" label="Fit to window" onClick=${fit} /></div>
          <div class="lx-hint">${partial ? html`<b>Showing the area around this asset (${partial.shown} of ${partial.total}).</b> Zoom out or press Fit to see all of it. ` : null}Drag to move. Ctrl and scroll, or the buttons, to zoom. Click an asset or a line for details.</div>
        </div>
        ${panel}</div>` : null}
      ${view === 'table' && graph.edges.length ? html`<div class="lx-table-wrap"><${Table} caption="Connections" dense=${true} rows=${tableRows} rowKey=${(r) => r.id}
        columns=${[
          { key: 'a', label: 'This', sortable: false, render: (r) => html`<${AssetLink} asset=${r.a} showType=${true} />` },
          { key: 'type', label: 'Relationship', sort: (r) => r.type, render: (r) => html`<span>${edgeSentence(r, () => '').trim()}</span>` },
          { key: 'b', label: 'That', sortable: false, render: (r) => html`<${AssetLink} asset=${r.b} showType=${true} />` },
          { key: 'confidence', label: 'How sure', sort: (r) => CONFIDENCE.indexOf(r.confidence), render: (r) => html`<${ConfBadge} level=${r.confidence} />` },
          { key: 'last_seen_at', label: 'Last seen', render: (r) => html`<${Ago} ts=${r.last_seen_at} />` },
        ]} /></div>` : null}
      <${LineageLegend} />
      ${(data.gaps || []).length ? html`<details class="blind" data-testid="lineage-gaps"><summary><b>${plural(data.gaps.length, 'thing')}</b> the scans could not read near here. The picture may be missing connections.</summary>
        ${data.gaps.map((g) => html`<div class="gap-item" key=${g.id}><div><b>${gapMeta(g.kind).label}</b> · ${g.subject}</div><div class="small">${g.detail}</div><div class="rem">${g.remediation}</div></div>`)}</details>` : html`<div class="small muted">Lineage only shows what scans could read. A picture without gaps is not proof that nothing is missing.</div>`}`}
  </div>`;
}

// ---------------------------------------------------------------------------------------------------------------- the page
const PICK_TYPES = 'table,view,semantic_model,model_table,report,dashboard,notebook,pipeline,sql_query,spark_job,file';

function AssetPicker() {
  const [q, setQ] = useState('');
  const dq = useDebounced(q, 300);
  const res = useAsync(() => api.get('/intelligence/assets', { q: dq || undefined, type: PICK_TYPES, limit: 30 }).then((r) => r.items || []), [dq]);
  return html`<div class="col gap-3"><div class="intel-search"><div class="grow"><input class="input" type="search" value=${q} onInput=${(e) => setQ(e.target.value)} placeholder="Find a table, report or notebook" aria-label="Find an asset to trace" /></div></div>
    ${res.error && !res.data ? html`<${ErrorBox} error=${res.error} onRetry=${res.reloadLoud} />` : !res.data ? html`<${Loading} />` : !res.data.length ? html`<${Empty} icon="search" title="Nothing found">Try another word, or run a scan first.</${Empty}>` : html`<ul class="results">${res.data.map((a) => html`<li class="asset-card" key=${a.id}><div class="ic"><${Icon} name=${typeMeta(a.type).icon} /></div><div class="grow"><h3><a href=${lineageHref(a.id)}>${a.name}</a></h3><div class="path">${a.path || ''}</div></div><${TypeChip} type=${a.type} /></li>`)}</ul>`}</div>`;
}

export function LineagePage({ params }) {
  const id = params && params.id;
  const loc = useRoute();
  const q = loc.query;
  const asset = useAsync(() => (id ? api.get(`/intelligence/assets/${enc(id)}`) : Promise.resolve(null)), [id]);
  const a = asset.data;
  const onState = useCallback((s) => setQuery({ direction: s.direction === 'both' ? '' : s.direction, depth: s.depth === 4 ? '' : s.depth, level: s.level === 'table' ? '' : s.level, conf: s.conf, procs: s.procs }), []);
  if (!id) return html`<div class="page"><${PageHead} icon="route" title="Lineage" sub="Pick something to see where its data comes from and what depends on it." /><${AssetPicker} /></div>`;
  return html`<div class="page wide"><${PageHead} icon="route" title=${a ? `Lineage of ${a.name}` : 'Lineage'} sub=${a ? html`<span class="wrap-any">${a.path || typeLabel(a.type)}</span>` : ''}
      actions=${a ? html`<${Btn} icon="external" href=${assetHref(id)}>Open asset<//><${Btn} icon="activity" href=${impactHref(id)}>Impact analysis<//><${Btn} icon="search" href="#/intelligence/lineage">Choose another<//>` : null} />
    ${asset.error && !a ? html`<${ErrorBox} error=${asset.error} onRetry=${asset.reloadLoud} />` : !a ? html`<${Loading} />` : html`<${LineageExplorer} assetId=${id} root=${a} initial=${{ direction: q.direction, depth: q.depth, level: q.level, conf: q.conf, procs: q.procs }} onState=${onState} height="calc(100vh - 360px)" />`}</div>`;
}
