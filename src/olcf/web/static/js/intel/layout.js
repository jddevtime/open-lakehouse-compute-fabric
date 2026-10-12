// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Deterministic layered (left to right) layout for lineage graphs, in plain JavaScript with no dependency and no DOM.
//
// Steps: (1) break cycles, (2) give every node a layer by longest path from the sources, (3) put long edges through invisible stop-over nodes so they never cross a real node,
// (4) order each layer to reduce crossings (barycentre sweeps, the best ordering seen wins), (5) place nodes vertically with an optimal, order-preserving fit to their neighbours
// (weighted isotonic regression), (6) lay the layers out as columns. The same input always gives the same picture, whatever order it arrives in, and nodes never overlap.
//
// layoutLineage(nodes, edges, options) -> { nodes, edges, layers, width, height }
//   nodes: [{ id, name?, type?, process? }]  edges: [{ id, from, to }] where from -> to is the direction the DATA flows.

const DEF = { nodeW: 208, nodeH: 52, processW: 176, processH: 40, gapX: 76, gapY: 16, pad: 24, dummyH: 6, sweeps: 8, placePasses: 8, maxDummies: 6000 };

const cmp = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
const sortKey = (n) => String(n.name || '').toLowerCase() + '\u0001' + String(n.id);

/** Number of pairs that cross between two adjacent layers, given [posInUpper, posInLower] for each edge (merge-sort inversion count). */
export function countCrossings(pairs) {
  const a = pairs.slice().sort((x, y) => x[0] - y[0] || x[1] - y[1]).map((p) => p[1]);
  let count = 0;
  const tmp = new Array(a.length);
  (function sort(lo, hi) {
    if (hi - lo < 2) return;
    const mid = (lo + hi) >> 1;
    sort(lo, mid); sort(mid, hi);
    let i = lo; let j = mid; let k = lo;
    while (i < mid && j < hi) {
      if (a[i] <= a[j]) tmp[k++] = a[i++];
      else { count += mid - i; tmp[k++] = a[j++]; }
    }
    while (i < mid) tmp[k++] = a[i++];
    while (j < hi) tmp[k++] = a[j++];
    for (let t = lo; t < hi; t += 1) a[t] = tmp[t];
  })(0, a.length);
  return count;
}

/** Weighted isotonic regression (pool adjacent violators): the non-decreasing sequence closest to `z` in weighted least squares. */
export function isotonic(z, w) {
  const blocks = [];
  for (let i = 0; i < z.length; i += 1) {
    blocks.push({ sum: z[i] * w[i], w: w[i], n: 1 });
    while (blocks.length > 1) {
      const b = blocks[blocks.length - 1]; const a = blocks[blocks.length - 2];
      if (a.sum / a.w <= b.sum / b.w) break;
      blocks.pop();
      a.sum += b.sum; a.w += b.w; a.n += b.n;
    }
  }
  const out = [];
  for (const b of blocks) for (let k = 0; k < b.n; k += 1) out.push(b.sum / b.w);
  return out;
}

export function layoutLineage(nodesIn, edgesIn, options = {}) {
  const o = { ...DEF, ...options };
  // ---------------------------------------------------------------- 1. normalise (input order must not matter)
  const nodeMap = new Map();
  for (const n of nodesIn || []) if (n && n.id != null && !nodeMap.has(n.id)) nodeMap.set(n.id, n);
  const nodes = [...nodeMap.values()].sort((a, b) => cmp(sortKey(a), sortKey(b)));
  const edgeMap = new Map();
  for (const e of edgesIn || []) {
    if (!e || e.from === e.to || !nodeMap.has(e.from) || !nodeMap.has(e.to)) continue;
    const id = e.id != null ? String(e.id) : `${e.from}>${e.to}`;
    if (!edgeMap.has(id)) edgeMap.set(id, { id, from: e.from, to: e.to });
  }
  const edges = [...edgeMap.values()].sort((a, b) => cmp(a.from, b.from) || cmp(a.to, b.to) || cmp(a.id, b.id));
  const idx = new Map(nodes.map((n, i) => [n.id, i]));
  const N = nodes.length;
  if (!N) return { nodes: [], edges: [], layers: [], width: o.pad * 2, height: o.pad * 2 };

  // unique connections (a pair can carry several edges); every edge is later drawn along its pair's route
  const out = Array.from({ length: N }, () => []);
  const pairKey = new Set();
  for (const e of edges) {
    const k = `${idx.get(e.from)}>${idx.get(e.to)}`;
    if (pairKey.has(k)) continue;
    pairKey.add(k);
    out[idx.get(e.from)].push(idx.get(e.to));
  }
  out.forEach((l) => l.sort((a, b) => a - b));

  // ---------------------------------------------------------------- 2. break cycles (iterative DFS; edges into a node still on the stack are reversed)
  const state = new Uint8Array(N);
  const reversed = new Set();
  const indeg = new Array(N).fill(0);
  out.forEach((l) => l.forEach((v) => { indeg[v] += 1; }));
  const starts = nodes.map((_, i) => i).sort((a, b) => (indeg[a] === 0 ? 0 : 1) - (indeg[b] === 0 ? 0 : 1) || a - b);
  for (const s of starts) {
    if (state[s]) continue;
    const stack = [[s, 0]];
    state[s] = 1;
    while (stack.length) {
      const top = stack[stack.length - 1];
      const [u, i] = top;
      if (i >= out[u].length) { state[u] = 2; stack.pop(); continue; }
      top[1] += 1;
      const v = out[u][i];
      if (state[v] === 1) reversed.add(`${u}>${v}`);
      else if (state[v] === 0) { state[v] = 1; stack.push([v, 0]); }
    }
  }
  const dag = Array.from({ length: N }, () => []);
  const rdag = Array.from({ length: N }, () => []);
  for (let u = 0; u < N; u += 1) {
    for (const v of out[u]) {
      const [a, b] = reversed.has(`${u}>${v}`) ? [v, u] : [u, v];
      if (!dag[a].includes(b)) { dag[a].push(b); rdag[b].push(a); }
    }
  }

  // ---------------------------------------------------------------- 3. layers: longest path from the sources, then pull pure sources next to their first consumer
  const layer = new Array(N).fill(0);
  const deg = rdag.map((l) => l.length);
  const queue = [];
  for (let i = 0; i < N; i += 1) if (!deg[i]) queue.push(i);
  for (let qi = 0; qi < queue.length; qi += 1) {
    const u = queue[qi];
    for (const v of dag[u]) {
      if (layer[v] < layer[u] + 1) layer[v] = layer[u] + 1;
      deg[v] -= 1;
      if (!deg[v]) queue.push(v);
    }
  }
  for (let i = 0; i < N; i += 1) {
    if (!rdag[i].length && dag[i].length) layer[i] = Math.min(...dag[i].map((v) => layer[v])) - 1;
  }
  const minL = Math.min(...layer);
  for (let i = 0; i < N; i += 1) layer[i] -= minL;
  const L = Math.max(...layer) + 1;

  // ---------------------------------------------------------------- 4. stop-over nodes for edges that span several layers
  // vertex: { key, idx (real node index) | -1, layer, h, w, dummy }
  const verts = nodes.map((n, i) => {
    const proc = !!n.process;
    return { key: n.id, node: n, i, layer: layer[i], w: proc ? o.processW : o.nodeW, h: proc ? o.processH : o.nodeH, dummy: false };
  });
  const chains = new Map(); // "u>v" (real indexes, as drawn: flow direction) -> [vertex indexes from lower layer to higher layer]
  let dummies = 0;
  const upN = []; const downN = []; // neighbours between consecutive layers, by vertex index
  verts.forEach(() => { upN.push([]); downN.push([]); });
  const link = (a, b) => { downN[a].push(b); upN[b].push(a); };
  const tooMany = (() => { let c = 0; for (let u = 0; u < N; u += 1) for (const v of dag[u]) c += Math.max(0, layer[v] - layer[u] - 1); return c > o.maxDummies; })();
  for (let u = 0; u < N; u += 1) {
    for (const v of dag[u]) {
      const span = layer[v] - layer[u];
      const chain = [u];
      if (!tooMany) {
        for (let l = layer[u] + 1; l < layer[v]; l += 1) {
          verts.push({ key: `~${u}>${v}@${l}`, node: null, i: -1, layer: l, w: 0, h: o.dummyH, dummy: true });
          upN.push([]); downN.push([]);
          chain.push(verts.length - 1); dummies += 1;
        }
      }
      chain.push(v);
      for (let k = 0; k + 1 < chain.length; k += 1) link(chain[k], chain[k + 1]);
      chains.set(`${u}>${v}`, chain);
      if (span < 1) throw new Error('layout invariant: edges point to a later layer');
    }
  }
  const layersV = Array.from({ length: L }, () => []);
  verts.forEach((v, vi) => layersV[v.layer].push(vi));

  // ---------------------------------------------------------------- 5. order within layers
  const pos = new Array(verts.length).fill(0);
  const setPos = () => layersV.forEach((l) => l.forEach((vi, k) => { pos[vi] = k; }));
  // initial: first layer by name, the others by barycentre of what is already placed
  layersV[0].sort((a, b) => a - b); // real nodes are already sorted by name
  setPos();
  for (let l = 1; l < L; l += 1) {
    const bary = (vi) => (upN[vi].length ? upN[vi].reduce((s, p) => s + pos[p], 0) / upN[vi].length : Infinity);
    layersV[l].sort((a, b) => (bary(a) - bary(b)) || (a - b));
    layersV[l].forEach((vi, k) => { pos[vi] = k; });
  }
  const crossings = () => {
    let c = 0;
    for (let l = 0; l + 1 < L; l += 1) {
      const pairs = [];
      for (const vi of layersV[l]) for (const w of downN[vi]) pairs.push([pos[vi], pos[w]]);
      c += countCrossings(pairs);
    }
    return c;
  };
  let best = crossings();
  let bestOrder = layersV.map((l) => l.slice());
  for (let s = 0; s < o.sweeps && best > 0; s += 1) {
    const down = s % 2 === 0;
    const seq = down ? Array.from({ length: L - 1 }, (_, k) => k + 1) : Array.from({ length: L - 1 }, (_, k) => L - 2 - k);
    for (const l of seq) {
      const nb = down ? upN : downN;
      const keyOf = (vi) => {
        const ns = nb[vi];
        if (!ns.length) return pos[vi];
        const ps = ns.map((p) => pos[p]).sort((a, b) => a - b);
        const m = ps.length >> 1;
        return ps.length % 2 ? ps[m] : (ps[m - 1] + ps[m]) / 2; // median, robust against one far-away neighbour
      };
      const bary = (vi) => (nb[vi].length ? nb[vi].reduce((a, p) => a + pos[p], 0) / nb[vi].length : keyOf(vi));
      const cur = new Map(layersV[l].map((vi, k) => [vi, k]));
      layersV[l].sort((a, b) => (bary(a) - bary(b)) || (keyOf(a) - keyOf(b)) || (cur.get(a) - cur.get(b)));
      layersV[l].forEach((vi, k) => { pos[vi] = k; });
    }
    const c = crossings();
    if (c < best) { best = c; bestOrder = layersV.map((l) => l.slice()); }
  }
  for (let l = 0; l < L; l += 1) layersV[l] = bestOrder[l];
  setPos();

  // ---------------------------------------------------------------- 6. vertical placement (centre y per vertex)
  const cy = new Array(verts.length).fill(0);
  const colH = layersV.map((l) => l.reduce((a, vi) => a + verts[vi].h, 0) + o.gapY * Math.max(0, l.length - 1));
  const tallest = Math.max(...colH);
  layersV.forEach((l, li) => {
    let y = (tallest - colH[li]) / 2;
    l.forEach((vi) => { cy[vi] = y + verts[vi].h / 2; y += verts[vi].h + o.gapY; });
  });
  const place = (l) => {
    const list = layersV[l];
    if (!list.length) return;
    const offs = []; let acc = 0;
    list.forEach((vi) => { offs.push(acc); acc += verts[vi].h + o.gapY; });
    const z = []; const w = [];
    list.forEach((vi, k) => {
      const ns = [...upN[vi], ...downN[vi]];
      const want = ns.length ? ns.reduce((a, p) => a + cy[p], 0) / ns.length : cy[vi];
      z.push(want - verts[vi].h / 2 - offs[k]);
      w.push(verts[vi].dummy ? 4 : ns.length ? 1 + Math.min(ns.length, 4) * 0.25 : 0.25);
    });
    const t = isotonic(z, w);
    list.forEach((vi, k) => { cy[vi] = offs[k] + t[k] + verts[vi].h / 2; });
  };
  for (let p = 0; p < o.placePasses; p += 1) {
    if (p % 2 === 0) for (let l = 0; l < L; l += 1) place(l); else for (let l = L - 1; l >= 0; l -= 1) place(l);
  }
  let minTop = Infinity; let maxBottom = -Infinity;
  verts.forEach((v, vi) => { minTop = Math.min(minTop, cy[vi] - v.h / 2); maxBottom = Math.max(maxBottom, cy[vi] + v.h / 2); });
  const dy = o.pad - minTop;

  // ---------------------------------------------------------------- 7. columns
  const colW = layersV.map((l) => Math.max(0, ...l.map((vi) => verts[vi].w)));
  const colX = []; let x = o.pad;
  for (let l = 0; l < L; l += 1) { colX.push(x); x += (colW[l] || 0) + o.gapX; }
  const width = x - o.gapX + o.pad;
  const height = maxBottom - minTop + o.pad * 2;
  const vx = (vi) => colX[verts[vi].layer] + (colW[verts[vi].layer] - verts[vi].w) / 2;

  const outNodes = nodes.map((n, i) => ({
    id: n.id, x: vx(i), y: cy[i] + dy - verts[i].h / 2, w: verts[i].w, h: verts[i].h, layer: layer[i], order: pos[i], process: !!n.process,
  }));
  // edges: one route per connection; the route is a list of points (right side of the source, stop-overs, left side of the target)
  const routeOf = (u, v) => {
    const rev = reversed.has(`${u}>${v}`);
    const [a, b] = rev ? [v, u] : [u, v];
    const chain = chains.get(`${a}>${b}`);
    const pts = chain.map((vi, k) => {
      const V = verts[vi];
      if (k === 0) return [vx(vi) + V.w, cy[vi] + dy];
      if (k === chain.length - 1) return [vx(vi), cy[vi] + dy];
      return [colX[V.layer] + colW[V.layer] / 2, cy[vi] + dy];
    });
    return { pts: rev ? pts.slice().reverse() : pts, rev };
  };
  const outEdges = edges.map((e) => {
    const u = idx.get(e.from); const v = idx.get(e.to);
    const { pts, rev } = routeOf(u, v);
    return { id: e.id, from: e.from, to: e.to, points: pts, d: smoothPath(pts, rev), cycle: rev };
  });
  const layers = layersV.map((l, li) => ({ index: li, x: colX[li], w: colW[li], count: l.filter((vi) => !verts[vi].dummy).length }));
  return { nodes: outNodes, edges: outEdges, layers, width, height, stats: { layers: L, dummies, crossings: best } };
}

/** SVG path through the route's points with horizontal tangents at every point (an "S" between neighbours). Edges that run backwards (cycles) loop out and round. */
export function smoothPath(pts, backwards = false) {
  const f = (n) => Math.round(n * 10) / 10;
  if (pts.length < 2) return '';
  if (backwards) {
    const [a, b] = [pts[0], pts[pts.length - 1]];
    const out = Math.max(40, Math.abs(a[0] - b[0]) / 3);
    return `M${f(a[0])},${f(a[1])} C${f(a[0] + out)},${f(a[1])} ${f(b[0] - out)},${f(b[1])} ${f(b[0])},${f(b[1])}`;
  }
  let d = `M${f(pts[0][0])},${f(pts[0][1])}`;
  for (let k = 1; k < pts.length; k += 1) {
    const [x0, y0] = pts[k - 1]; const [x1, y1] = pts[k];
    const mx = (x1 - x0) / 2;
    d += ` C${f(x0 + mx)},${f(y0)} ${f(x1 - mx)},${f(y1)} ${f(x1)},${f(y1)}`;
  }
  return d;
}

/** Axis aligned overlap test used by the tests and by keyboard navigation. */
export function overlaps(a, b, gap = 0) {
  return a.x < b.x + b.w + gap && b.x < a.x + a.w + gap && a.y < b.y + b.h + gap && b.y < a.y + a.h + gap;
}
