// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Pure graph helpers for the lineage explorer: merging expansions, flow adjacency, keyboard navigation, searching, view maths and the column mapping rows. No DOM, no network.
import { edgeVerb, confRank, typeLabel } from './model.js';

/** Merge several lineage answers into one graph. Nodes are deduplicated by id (the smallest `level` wins), edges by id; edges whose ends are missing are dropped. */
export function mergeGraphs(parts) {
  const nodes = new Map();
  const edges = new Map();
  for (const p of parts || []) {
    if (!p) continue;
    for (const n of p.nodes || []) {
      const have = nodes.get(n.id);
      if (!have) nodes.set(n.id, n);
      else if ((n.level ?? 99) < (have.level ?? 99)) nodes.set(n.id, { ...have, level: n.level, direction: have.direction === 'root' ? 'root' : n.direction });
    }
    // a link that skips hidden steps (virtual) has no id of its own: give it one made of its ends, or every such link would collapse into the first
    for (const e of p.edges || []) { const id = e.id != null ? e.id : `virtual:${e.from}>${e.to}`; if (!edges.has(id)) edges.set(id, e.id != null ? e : { ...e, id }); }
  }
  return { nodes: [...nodes.values()], edges: [...edges.values()].filter((e) => nodes.has(e.from) && nodes.has(e.to)) };
}

/** Flow adjacency: out.get(id) are edges leaving it (data flows from -> to), inn.get(id) the edges arriving. */
export function adjacency(edges) {
  const out = new Map();
  const inn = new Map();
  for (const e of edges) {
    if (!out.has(e.from)) out.set(e.from, []);
    out.get(e.from).push(e);
    if (!inn.has(e.to)) inn.set(e.to, []);
    inn.get(e.to).push(e);
  }
  return { out, inn };
}

/** Everything upstream and everything downstream of `id` (not including it), following the data flow. */
export function related(adj, id) {
  const walk = (start, next) => {
    const seen = new Set();
    const stack = [start];
    while (stack.length) {
      const cur = stack.pop();
      for (const e of next.get(cur) || []) {
        const other = next === adj.out ? e.to : e.from;
        if (other !== id && !seen.has(other)) { seen.add(other); stack.push(other); }
      }
    }
    return seen;
  };
  return { up: walk(id, adj.inn), down: walk(id, adj.out) };
}

/** The node an arrow key moves to: left = something upstream, right = something downstream, up and down = the neighbour in the same column. */
export function neighbour(layout, adj, id, key) {
  const byId = new Map(layout.nodes.map((n) => [n.id, n]));
  const cur = byId.get(id);
  if (!cur) return null;
  const closest = (ids) => {
    let best = null;
    for (const i of ids) {
      const n = byId.get(i);
      if (!n) continue;
      const d = Math.abs(n.y - cur.y);
      if (!best || d < best.d || (d === best.d && n.id < best.n.id)) best = { n, d };
    }
    return best ? best.n.id : null;
  };
  if (key === 'ArrowLeft') return closest((adj.inn.get(id) || []).map((e) => e.from));
  if (key === 'ArrowRight') return closest((adj.out.get(id) || []).map((e) => e.to));
  const col = layout.nodes.filter((n) => n.layer === cur.layer).sort((a, b) => a.y - b.y || (a.id < b.id ? -1 : 1));
  const i = col.findIndex((n) => n.id === id);
  if (key === 'ArrowUp') return i > 0 ? col[i - 1].id : null;
  if (key === 'ArrowDown') return i >= 0 && i < col.length - 1 ? col[i + 1].id : null;
  return null;
}

/** Nodes whose name, path or type match every word of `q`, in reading order (left to right, top to bottom). */
export function matchNodes(nodes, layout, q) {
  const words = String(q || '').toLowerCase().split(/\s+/).filter(Boolean);
  if (!words.length) return [];
  const pos = new Map(layout.nodes.map((n) => [n.id, n]));
  return nodes
    .filter((n) => { const hay = `${n.name || ''} ${n.path || ''} ${typeLabel(n.type)}`.toLowerCase(); return words.every((w) => hay.includes(w)); })
    .sort((a, b) => { const pa = pos.get(a.id) || { layer: 0, y: 0 }; const pb = pos.get(b.id) || { layer: 0, y: 0 }; return pa.layer - pb.layer || pa.y - pb.y; })
    .map((n) => n.id);
}

export const clampNum = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
export const MIN_ZOOM = 0.1;
export const MAX_ZOOM = 2.5;
export const HEAD = 34;

/** The view {k, tx, ty} that fits the whole graph (never magnified past 1) and centres it in a w x h box. */
export function fitView(layout, w, h, pad = 24) {
  const gw = Math.max(1, layout.width); const gh = Math.max(1, layout.height + HEAD);
  const k = clampNum(Math.min((w - pad * 2) / gw, (h - pad * 2) / gh, 1), MIN_ZOOM, 1);
  return { k, tx: (w - gw * k) / 2, ty: Math.max(pad / 2, (h - gh * k) / 2) };
}
/** Below this zoom a node label is too small to read, so a picture that would need less is not fitted whole. */
export const LEGIBLE_ZOOM = 0.6;
/**
 * The view a lineage picture opens with. The whole picture when it fits at a readable zoom; otherwise the starting asset and what is up to `reach` layers either side of it, at
 * the smallest readable zoom, with `partial: true` so the page can say that more is outside the window. Always returns a view in which the starting asset is fully visible.
 */
export function initialView(layout, rootId, w, h, { pad = 24 } = {}) {
  const whole = fitView(layout, w, h, pad);
  const root = layout.nodes.find((n) => n.id === rootId);
  if (whole.k >= LEGIBLE_ZOOM || !root) return { view: whole, partial: false, shown: layout.nodes.length };
  const reachMax = Math.max(...layout.nodes.map((n) => Math.abs(n.layer - root.layer)));
  const box = (reach) => {
    const near = layout.nodes.filter((n) => Math.abs(n.layer - root.layer) <= reach);
    const x0 = Math.min(...near.map((n) => n.x)); const x1 = Math.max(...near.map((n) => n.x + n.w));
    const y0 = Math.min(...near.map((n) => n.y + HEAD)); const y1 = Math.max(...near.map((n) => n.y + HEAD + n.h));
    const bw = Math.max(1, x1 - x0); const bh = Math.max(1, y1 - y0);
    return { near, x0, y0, bw, bh, k: Math.min((w - pad * 2) / bw, (h - pad * 2) / bh, 1) };
  };
  // as many steps either side of the starting asset as still fit at a readable zoom (at least one)
  let reach = reachMax - 1;
  while (reach > 1 && box(reach).k < LEGIBLE_ZOOM) reach -= 1;
  const b = box(Math.max(1, reach));
  const k = clampNum(b.k, LEGIBLE_ZOOM, 1);
  const view = ensureVisible({ k, tx: (w - b.bw * k) / 2 - b.x0 * k, ty: (h - b.bh * k) / 2 - b.y0 * k }, root, w, h, pad);
  return { view, partial: true, shown: b.near.length };
}
/** Zoom by `factor` keeping the point (px, py) of the box where it is. */
export function zoomAt(view, factor, px, py) {
  const k = clampNum(view.k * factor, MIN_ZOOM, MAX_ZOOM);
  const f = k / view.k;
  return { k, tx: px - (px - view.tx) * f, ty: py - (py - view.ty) * f };
}
/** Pan just enough that a node is fully visible in a w x h box (with a margin); the view is returned unchanged when it already is. */
export function ensureVisible(view, node, w, h, margin = 24) {
  const x0 = view.tx + node.x * view.k; const x1 = x0 + node.w * view.k;
  const y0 = view.ty + (node.y + HEAD) * view.k; const y1 = y0 + node.h * view.k;
  let { tx, ty } = view;
  if (x0 < margin) tx += margin - x0; else if (x1 > w - margin) tx -= x1 - (w - margin);
  if (y0 < margin) ty += margin - y0; else if (y1 > h - margin) ty -= y1 - (h - margin);
  return tx === view.tx && ty === view.ty ? view : { ...view, tx, ty };
}
/** Centre a node in the box. */
export function centreOn(view, node, w, h) {
  return { ...view, tx: w / 2 - (node.x + node.w / 2) * view.k, ty: h / 2 - (node.y + HEAD + node.h / 2) * view.k };
}

/** One sentence per connection, for the table view and for screen readers. */
export function edgeSentence(e, nameOf) {
  return `${nameOf(e.src)} ${edgeVerb(e.type)} ${nameOf(e.dst)}`;
}

/** "table.column" from a column asset's path ("Workspace / lakehouse / table / column"). */
export function columnLabel(node) {
  if (!node) return '';
  const parts = String(node.path || node.name || '').split(' / ');
  return parts.length >= 2 ? parts.slice(-2).join('.') : parts[0];
}

/**
 * Column lineage as mapping rows: one row per TRANSFORMS edge, "source column -> target column", with the transformation text and the process that does it.
 * `focusId` (a table or a column) limits the rows to those that touch it or one of its columns when given.
 */
export function mappingRows(data, focusId) {
  const nodes = new Map((data.nodes || []).map((n) => [n.id, n]));
  const rows = [];
  for (const e of data.edges || []) {
    if (e.type !== 'TRANSFORMS') continue;
    const from = nodes.get(e.from); const to = nodes.get(e.to);
    if (!from || !to) continue;
    rows.push({ id: e.id, from, to, via: e.via ? nodes.get(e.via) || { id: e.via, name: 'a process', type: 'script' } : null, transformation: e.transformation || '', confidence: e.confidence, last_seen_at: e.last_seen_at, edge: e });
  }
  const touches = (n) => n && (n.id === focusId || n.parent_id === focusId);
  const out = focusId ? rows.filter((r) => touches(r.from) || touches(r.to)) : rows;
  return out.sort((a, b) => columnLabel(a.to).localeCompare(columnLabel(b.to)) || columnLabel(a.from).localeCompare(columnLabel(b.from)) || confRank(a.confidence) - confRank(b.confidence));
}

/** The JSON a person exports: only what the screen shows, plus how it was asked for. */
export function graphJson({ root, direction, depth, level, minConfidence, nodes, edges, truncated, generatedAt }) {
  return JSON.stringify({ root, direction, depth, level, min_confidence: minConfidence || null, truncated: !!truncated, generated_at: generatedAt || new Date().toISOString(), nodes, edges }, null, 2);
}
