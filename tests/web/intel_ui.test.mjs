// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Unit tests of the pure parts of the Intelligence UI: the lineage layout, the graph helpers, the formatters and the SVG export. Run by test_intel_ui.py under Node.
import test from 'node:test';
import assert from 'node:assert/strict';
import { layoutLineage, countCrossings, isotonic, overlaps, smoothPath } from './js/intel/layout.js';
import { mergeGraphs, adjacency, related, neighbour, matchNodes, fitView, initialView, LEGIBLE_ZOOM, zoomAt, ensureVisible, centreOn, mappingRows, columnLabel, edgeSentence, graphJson, MIN_ZOOM, MAX_ZOOM, HEAD } from './js/intel/graphops.js';
import * as M from './js/intel/model.js';
import { graphToSvg, edgeDash, EDGE_DASH, nodeLines, trunc } from './js/intel/graphdraw.js';
import { renderMarkdown } from './js/notebook/markdown.js';

// ---- helpers ----------------------------------------------------------------------------------------------------------------------
function rng(seed) { let s = seed >>> 0; return () => { s = (s * 1664525 + 1013904223) >>> 0; return s / 4294967296; }; }
/** A random layered DAG: `n` nodes, about `deg` forward edges per node, some of them long. */
function dag(n, deg, seed) {
  const r = rng(seed);
  const nodes = Array.from({ length: n }, (_, i) => ({ id: `n${String(i).padStart(4, '0')}`, name: `Node ${i}`, type: i % 7 === 0 ? 'notebook' : 'table', process: i % 7 === 0 }));
  const edges = [];
  for (let i = 1; i < n; i += 1) {
    for (let k = 0; k < deg; k += 1) {
      const span = r() < 0.15 ? Math.floor(r() * 40) + 2 : Math.floor(r() * 6) + 1;
      const j = Math.max(0, i - span);
      edges.push({ id: `e${i}-${j}-${k}`, from: nodes[j].id, to: nodes[i].id });
    }
  }
  return { nodes, edges };
}
const noOverlap = (lay, gap = 0) => {
  const byLayer = new Map();
  for (const n of lay.nodes) (byLayer.get(n.layer) || byLayer.set(n.layer, []).get(n.layer)).push(n);
  for (const list of byLayer.values()) {
    list.sort((a, b) => a.y - b.y);
    for (let i = 1; i < list.length; i += 1) assert.ok(list[i].y >= list[i - 1].y + list[i - 1].h + gap - 1e-6, `nodes ${list[i - 1].id} and ${list[i].id} overlap`);
  }
  // and nothing at all overlaps across the whole picture
  for (let i = 0; i < lay.nodes.length; i += 1) for (let j = i + 1; j < lay.nodes.length; j += 1) if (lay.nodes[i].layer === lay.nodes[j].layer) assert.ok(!overlaps(lay.nodes[i], lay.nodes[j], gap - 0.001), 'overlap');
};

// ---- layout -----------------------------------------------------------------------------------------------------------------------
test('layout: a chain runs left to right, one layer per step', () => {
  const lay = layoutLineage([{ id: 'a' }, { id: 'b' }, { id: 'c' }], [{ id: '1', from: 'a', to: 'b' }, { id: '2', from: 'b', to: 'c' }]);
  const at = Object.fromEntries(lay.nodes.map((n) => [n.id, n]));
  assert.ok(at.a.x < at.b.x && at.b.x < at.c.x);
  assert.deepEqual(lay.layers.map((l) => l.count), [1, 1, 1]);
  assert.ok(lay.width > 0 && lay.height > 0);
  for (const e of lay.edges) assert.ok(e.d.startsWith('M'));
});

test('layout: every edge points forward and nodes never overlap (800 and 1200 nodes)', () => {
  for (const [n, deg, seed] of [[800, 2, 7], [1200, 3, 99]]) {
    const { nodes, edges } = dag(n, deg, seed);
    const t0 = Date.now();
    const lay = layoutLineage(nodes, edges);
    const ms = Date.now() - t0;
    assert.equal(lay.nodes.length, n);
    const at = new Map(lay.nodes.map((v) => [v.id, v]));
    for (const e of edges) assert.ok(at.get(e.from).layer < at.get(e.to).layer, `edge ${e.id} must run left to right`);
    noOverlap(lay, 0);
    assert.ok(ms < 20000, `layout of ${n} nodes took ${ms} ms`);
    assert.ok(lay.stats.dummies <= 6000, 'the number of stop-over points is bounded');
  }
});

test('layout: the input order does not change the picture', () => {
  const { nodes, edges } = dag(150, 2, 5);
  const a = layoutLineage(nodes, edges);
  const b = layoutLineage([...nodes].reverse(), [...edges].reverse());
  assert.equal(JSON.stringify(a), JSON.stringify(b));
  const c = layoutLineage(nodes, edges);
  assert.equal(JSON.stringify(a), JSON.stringify(c));
});

test('layout: cycles, self loops, duplicates and dangling edges are survived', () => {
  const lay = layoutLineage([{ id: 'a' }, { id: 'b' }, { id: 'c' }, { id: 'a' }], [
    { id: '1', from: 'a', to: 'b' }, { id: '2', from: 'b', to: 'c' }, { id: '3', from: 'c', to: 'a' }, { id: '4', from: 'a', to: 'a' }, { id: '5', from: 'a', to: 'ghost' }, { id: '1', from: 'a', to: 'b' }]);
  assert.equal(lay.nodes.length, 3);
  assert.equal(lay.edges.length, 3, 'self loop, dangling and duplicate edges are dropped');
  assert.equal(lay.edges.filter((e) => e.cycle).length, 1, 'exactly one edge closes the loop');
  noOverlap(lay);
});

test('layout: a long edge is routed through stop-over points, not through the columns between', () => {
  const nodes = Array.from({ length: 6 }, (_, i) => ({ id: `n${i}`, name: `n${i}` }));
  const chain = [[0, 1], [1, 2], [2, 3], [3, 4]].map(([a, b], i) => ({ id: `c${i}`, from: `n${a}`, to: `n${b}` }));
  const lay = layoutLineage(nodes, [...chain, { id: 'long', from: 'n0', to: 'n4' }]);
  const long = lay.edges.find((e) => e.id === 'long');
  assert.equal(long.points.length, 5, 'one point per column it crosses');
  const at = new Map(lay.nodes.map((n) => [n.id, n]));
  for (const [x, y] of long.points.slice(1, -1)) for (const n of lay.nodes) assert.ok(!(x > n.x && x < n.x + n.w && y > n.y && y < n.y + n.h), `the route crosses ${n.id}`);
  assert.equal(at.get('n5').layer, 0, 'an unconnected node sits in the first column');
});

test('layout: empty and single node graphs', () => {
  const e = layoutLineage([], []);
  assert.deepEqual([e.nodes.length, e.edges.length], [0, 0]);
  const s = layoutLineage([{ id: 'x', name: 'only' }], []);
  assert.equal(s.nodes.length, 1);
  assert.ok(s.width > 0);
});

test('layout: process nodes use their own size and a wide hub does not overlap', () => {
  const nodes = [{ id: 'hub', name: 'hub' }, ...Array.from({ length: 60 }, (_, i) => ({ id: `d${i}`, name: `d${i}`, process: i % 2 === 0 }))];
  const edges = nodes.slice(1).map((n) => ({ id: `h${n.id}`, from: 'hub', to: n.id }));
  const lay = layoutLineage(nodes, edges);
  noOverlap(lay);
  const p = lay.nodes.find((n) => n.id === 'd0'); const t = lay.nodes.find((n) => n.id === 'd1');
  assert.ok(p.process && p.h < t.h, 'processes are slimmer than datasets');
});

test('layout: crossing counter and isotonic fit', () => {
  assert.equal(countCrossings([[0, 1], [1, 0]]), 1);
  assert.equal(countCrossings([[0, 0], [1, 1], [2, 2]]), 0);
  const fit = isotonic([3, 1, 2], [1, 1, 1]);
  for (let i = 1; i < fit.length; i += 1) assert.ok(fit[i] >= fit[i - 1]);
  assert.match(smoothPath([[0, 0], [100, 50]]), /^M0,0 C50,0 50,50 100,50$/);
});

// ---- graph helpers ----------------------------------------------------------------------------------------------------------------
const G = { nodes: [{ id: 'a', name: 'A', path: 'W / a', type: 'table' }, { id: 'b', name: 'B', path: 'W / b', type: 'table' }, { id: 'c', name: 'C Report', path: 'W / c', type: 'report' }, { id: 'd', name: 'D', type: 'table' }],
  edges: [{ id: '1', from: 'a', to: 'b', type: 'READS_FROM', confidence: 'verified' }, { id: '2', from: 'b', to: 'c', type: 'DISPLAYS', confidence: 'derived' }, { id: '3', from: 'd', to: 'b', type: 'READS_FROM', confidence: 'inferred' }] };

test('graphops: merge keeps the nearest level and drops edges with a missing end', () => {
  const m = mergeGraphs([{ nodes: [{ id: 'a', level: 2, direction: 'up' }, { id: 'b', level: 1 }], edges: [{ id: '1', from: 'a', to: 'b' }] }, { nodes: [{ id: 'a', level: 1, direction: 'up' }], edges: [{ id: '1', from: 'a', to: 'b' }, { id: '9', from: 'a', to: 'zz' }] }, null]);
  assert.equal(m.nodes.length, 2);
  assert.equal(m.nodes.find((n) => n.id === 'a').level, 1);
  assert.deepEqual(m.edges.map((e) => e.id), ['1']);
});

test('graphops: upstream and downstream sets, and arrow-key navigation', () => {
  const adj = adjacency(G.edges);
  const r = related(adj, 'b');
  assert.deepEqual([...r.up].sort(), ['a', 'd']);
  assert.deepEqual([...r.down], ['c']);
  const lay = layoutLineage(G.nodes, G.edges);
  assert.equal(neighbour(lay, adj, 'b', 'ArrowRight'), 'c');
  assert.ok(['a', 'd'].includes(neighbour(lay, adj, 'b', 'ArrowLeft')));
  assert.equal(neighbour(lay, adj, 'c', 'ArrowRight'), null);
  const up = neighbour(lay, adj, 'a', 'ArrowDown') || neighbour(lay, adj, 'a', 'ArrowUp');
  assert.ok(up === 'd' || up === null);
  assert.equal(neighbour(lay, adj, 'nope', 'ArrowLeft'), null);
});

test('graphops: search matches every word in reading order', () => {
  const lay = layoutLineage(G.nodes, G.edges);
  assert.deepEqual(matchNodes(G.nodes, lay, 'report'), ['c']);
  assert.deepEqual(matchNodes(G.nodes, lay, 'w table'), ['a', 'b'].filter((x) => matchNodes(G.nodes, lay, 'w table').includes(x)));
  assert.deepEqual(matchNodes(G.nodes, lay, '   '), []);
  assert.deepEqual(matchNodes(G.nodes, lay, 'zzz'), []);
});

test('graphops: view maths keeps things on screen', () => {
  const lay = layoutLineage(G.nodes, G.edges);
  const v = fitView(lay, 800, 400);
  assert.ok(v.k <= 1 && v.k >= MIN_ZOOM);
  const big = { width: 20000, height: 9000, nodes: [], layers: [] };
  assert.equal(fitView(big, 800, 400).k, MIN_ZOOM);
  const z = zoomAt({ k: 1, tx: 10, ty: 20 }, 2, 100, 100);
  assert.equal(z.k, 2);
  assert.equal(z.tx, 100 - (100 - 10) * 2);                       // the point under the pointer stays put
  assert.equal(zoomAt({ k: 2.4, tx: 0, ty: 0 }, 5, 0, 0).k, MAX_ZOOM);
  const node = { x: 2000, y: 0, w: 200, h: 50 };
  const view = ensureVisible({ k: 1, tx: 0, ty: 0 }, node, 800, 400);
  assert.ok(view.tx + node.x + node.w <= 800 - 24 + 1e-6);
  assert.equal(ensureVisible(view, node, 800, 400), view, 'already visible: unchanged');
  const c = centreOn({ k: 1, tx: 0, ty: 0 }, node, 800, 400);
  assert.equal(c.tx + node.x + node.w / 2, 400);
  assert.equal(c.ty + node.y + HEAD + node.h / 2, 200);
});

test('graphops: column lineage becomes mapping rows with the transformation text', () => {
  const data = { nodes: [{ id: 'c1', name: 'amount', path: 'W / lh / src / amount', parent_id: 't1' }, { id: 'c2', name: 'net', path: 'W / lh / fact / net', parent_id: 't2' }, { id: 'c3', name: 'other', path: 'W / lh / fact / other', parent_id: 't2' }, { id: 'p', name: 'job', type: 'notebook' }],
    edges: [{ id: 'x', type: 'TRANSFORMS', from: 'c1', to: 'c2', via: 'p', transformation: 'amount * 0.8', confidence: 'derived' }, { id: 'y', type: 'READS_FROM', from: 'c1', to: 'c3' }] };
  const rows = mappingRows(data);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].transformation, 'amount * 0.8');
  assert.equal(rows[0].via.name, 'job');
  assert.equal(columnLabel(data.nodes[0]), 'src.amount');
  assert.equal(mappingRows(data, 't2').length, 1);
  assert.equal(mappingRows(data, 'zzz').length, 0);
  assert.equal(edgeSentence({ src: 'a', dst: 'b', type: 'READS_FROM' }, (x) => x.toUpperCase()), 'A reads from B');
  const j = JSON.parse(graphJson({ root: 'a', direction: 'both', depth: 2, level: 'table', nodes: [{ id: 'a' }], edges: [], truncated: true, generatedAt: 'now' }));
  assert.deepEqual([j.root, j.truncated, j.min_confidence, j.generated_at], ['a', true, null, 'now']);
});

// ---- model: formatters, links, labels --------------------------------------------------------------------------------------------
test('model: type names, plurals and counts read as plain English', () => {
  assert.equal(M.typeLabel('sql_query'), 'SQL query');
  assert.equal(M.typeNoun('sql_query'), 'SQL query', 'an acronym keeps its capitals');
  assert.equal(M.typeNoun('table'), 'table');
  assert.equal(M.typePlural('sql_query', 2), 'SQL queries');
  assert.equal(M.typePlural('table', 1), 'table');
  assert.equal(M.typePlural('table', 3), 'tables');
  assert.equal(M.typeLabel('something_new'), 'something new', 'unknown types are still shown');
  assert.equal(M.countsPhrase({ table: 2, report: 1, visual: 0 }), '2 tables, 1 report');
  assert.equal(M.countsPhrase(null), '');
});

test('model: confidence order, weakest link and filters', () => {
  assert.deepEqual(M.CONFIDENCE, ['verified', 'derived', 'inferred', 'unknown']);
  assert.equal(M.weakest(['verified', 'inferred', 'derived']), 'inferred');
  assert.equal(M.weakest([]), 'verified');
  assert.ok(M.atLeast('verified', 'derived') && M.atLeast('derived', 'derived') && !M.atLeast('inferred', 'derived') && M.atLeast('unknown', ''));
  assert.equal(M.confLabel('nonsense'), 'Unknown');
});

test('model: scan states, gaps and source kinds have words and lights', () => {
  assert.deepEqual(['succeeded', 'partial', 'failed', 'running', 'cancelled', undefined].map(M.scanLight), ['ok', 'degraded', 'down', 'busy', 'unknown', 'unknown']);
  assert.equal(M.scanLabel(undefined), 'Never scanned');
  assert.ok(M.gapMeta('parse_error').label.length > 3);
  assert.equal(M.gapMeta('brand_new_kind').label, 'brand new kind');
  assert.equal(M.sourceKindLabel('fabric'), 'Microsoft Fabric and Power BI');
  for (const k of Object.keys(M.GAP_KINDS)) assert.ok(M.GAP_KINDS[k].help, `gap kind ${k} explains itself`);
});

test('model: links are escaped and carry their filters', () => {
  assert.equal(M.assetHref('ast_1'), '#/intelligence/assets/ast_1');
  assert.equal(M.assetHref('ast_1', 'columns'), '#/intelligence/assets/ast_1?tab=columns');
  assert.equal(M.assetHref('a b/c'), '#/intelligence/assets/a%20b%2Fc');
  assert.equal(M.impactHref('x'), '#/intelligence/impact/x');
  assert.match(M.lineageHref('x', { direction: 'up' }), /^#\/intelligence\/lineage\/x\?direction=up$/);
  assert.equal(M.discoveryHref('gaps', { kind: 'parse_error' }), '#/intelligence/discovery?tab=gaps&kind=parse_error');
  assert.equal(M.discoveryHref(), '#/intelligence/discovery');
  assert.equal(M.governanceHref('review'), '#/intelligence/governance/review');
  assert.equal(M.governanceHref(''), '#/intelligence/governance');
  assert.match(M.catalogHref({ type: 'table', q: 'a b' }), /^#\/intelligence\/catalog\?/);
});

test('model: small helpers', () => {
  assert.equal(M.pct(1, 3), 33);
  assert.equal(M.pct(1, 0), null);
  assert.equal(M.pct(2, 3, 1), 66.7);
  assert.equal(M.layerOf({ name: 'gold_daily_sales' }), 'Gold');
  assert.equal(M.layerOf({ name: 'goldfish' }), null, 'only a whole word counts');
  assert.equal(M.layerOf({ name: 'x', path: 'W / Silver / x' }), 'Silver');
  assert.equal(M.pathTail('A / b / c / d'), 'c / d');
  assert.equal(M.pathTail(''), '');
  assert.equal(M.edgeVerb('READS_FROM'), 'reads from');
  assert.equal(M.edgeVerb('SOMETHING_NEW'), 'something new');
  assert.equal(M.escapeRegExp('a.b*c'), 'a\\.b\\*c');
});

// ---- export -----------------------------------------------------------------------------------------------------------------------
test('graphdraw: the SVG export is built from the data, escaped, and shows the confidence styles', () => {
  const nodes = [{ id: 'a', name: 'sales <raw> & "co"', type: 'table', path: 'W / a' }, { id: 'p', name: 'job', type: 'notebook' }, { id: 'b', name: 'report', type: 'report' }];
  const edges = [{ id: '1', from: 'a', to: 'p', confidence: 'verified' }, { id: '2', from: 'p', to: 'b', confidence: 'inferred' }, { id: '3', from: 'a', to: 'b', confidence: 'unknown' }];
  const layout = layoutLineage(nodes.map((n) => ({ ...n, process: n.type === 'notebook' })), edges);
  const colors = { bg: '#fff', surface: '#fafafa', ink: '#111', ink3: '#666', accent: '#06f', lineStrong: '#999', dataset: '#06c', font: 'Inter' };
  const svg = graphToSvg({ layout, nodes, edges, selected: 'a', colors, title: 'My <graph>' });
  assert.ok(svg.startsWith('<svg') && svg.endsWith('</svg>'));
  assert.ok(!svg.includes('<raw>') && svg.includes('&lt;raw&gt;') && svg.includes('&amp;'));
  assert.ok(svg.includes(`stroke-dasharray="${EDGE_DASH.inferred}"`) && svg.includes(`stroke-dasharray="${EDGE_DASH.unknown}"`));
  assert.equal((svg.match(/marker-end/g) || []).length, 3);
  assert.equal(svg, graphToSvg({ layout, nodes, edges, selected: 'a', colors, title: 'My <graph>' }), 'deterministic');
  assert.equal(edgeDash('verified'), '');
  assert.equal(edgeDash('bogus'), EDGE_DASH.unknown);
  assert.equal(trunc('abcdefghij', 5), 'abcd…');
  assert.ok(nodeLines({ name: 'gold_sales', type: 'table' }).sub.includes('Gold'));
});

// ---- markdown (used for documentation pages and glossary terms) -----------------------------------------------------------------
function flatten(v, out = { text: '', tags: [] }) {
  if (v == null || typeof v === 'boolean') return out;
  if (Array.isArray(v)) { v.forEach((x) => flatten(x, out)); return out; }
  if (typeof v === 'string' || typeof v === 'number') { out.text += String(v); return out; }
  if (typeof v.type === 'string') out.tags.push(v.type);
  flatten(v.props && v.props.children, out);
  return out;
}
test('markdown: underscores inside words are not emphasis, real emphasis still works', () => {
  const lit = flatten(renderMarkdown('The column gross_amount * discount_pct is used, and __init__ stays.'));
  assert.ok(lit.text.includes('gross_amount * discount_pct'), lit.text);
  assert.ok(!lit.tags.includes('em'));
  const em = flatten(renderMarkdown('This is _important_ and **very** bold, also __this__ and *that*.'));
  assert.equal(em.tags.filter((t) => t === 'em').length, 2);
  assert.equal(em.tags.filter((t) => t === 'strong').length, 2);
});

test('graphops: links that skip hidden steps have no id and must not collapse into one', () => {
  const m = mergeGraphs([{ nodes: [{ id: 'a' }, { id: 'b' }, { id: 'c' }], edges: [{ id: null, from: 'a', to: 'c', virtual: true }, { id: null, from: 'b', to: 'c', virtual: true }, { id: 'e1', from: 'a', to: 'b' }] }]);
  assert.equal(m.edges.length, 3);
  assert.equal(new Set(m.edges.map((e) => e.id)).size, 3);
  assert.ok(m.edges.every((e) => e.id != null));
  assert.equal(mergeGraphs([m, m]).edges.length, 3);
});

test('graphops: a small picture opens fitted whole, a huge one opens around the starting asset at a readable zoom and says so', () => {
  const chain = (n) => { const nodes = Array.from({ length: n }, (_, i) => ({ id: `n${i}`, name: `n${i}`, type: 'table' })); return layoutLineage(nodes, nodes.slice(1).map((x, i) => ({ id: `e${i}`, from: `n${i}`, to: x.id }))); };
  const small = chain(4);
  const a = initialView(small, 'n3', 1000, 500);
  assert.equal(a.partial, false);
  assert.deepEqual(a.view, fitView(small, 1000, 500));
  const big = chain(30);
  assert.ok(fitView(big, 1000, 500).k < LEGIBLE_ZOOM);
  const b = initialView(big, 'n29', 1000, 500);
  assert.equal(b.partial, true);
  assert.ok(b.view.k >= LEGIBLE_ZOOM);
  const root = big.nodes.find((n) => n.id === 'n29');
  const x0 = b.view.tx + root.x * b.view.k; const y0 = b.view.ty + (root.y + HEAD) * b.view.k;
  assert.ok(x0 >= 0 && x0 + root.w * b.view.k <= 1000, 'the starting asset is inside the window horizontally');
  assert.ok(y0 >= 0 && y0 + root.h * b.view.k <= 500, 'and vertically');
  assert.ok(b.shown < big.nodes.length);
  // deterministic
  assert.deepEqual(initialView(big, 'n29', 1000, 500), b);
  // an unknown starting asset falls back to the whole picture
  assert.equal(initialView(big, 'nope', 1000, 500).partial, false);
});
