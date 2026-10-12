// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Directed-graph renderer for pipelines (SVG, no dependencies). Lays activities out left to right by dependency depth, draws containers
// (If, Switch, ForEach, Until) with their nested scopes inside, and colours nodes by run state. Keyboard operable.
import { html, useState, useRef, useEffect, useMemo, useId } from '../core/h.js';
import { Btn } from './basic.js';

const NW = 196; const NH = 54; const GX = 58; const GY = 18; const PAD = 14; const HEAD = 30; const LABEL = 18;
const CONTAINERS = new Set(['if', 'switch', 'for_each', 'until']);
const CATEGORY = { copy: 'data', notebook: 'compute', spark_job: 'compute', script: 'compute', sql: 'compute', http: 'io', webhook_wait: 'io', if: 'control', switch: 'control', for_each: 'control', until: 'control', child_pipeline: 'control', wait: 'misc', set_variable: 'misc', fail: 'misc' };
export const activityCategory = (t) => CATEGORY[t] || 'misc';
const GLYPH = { succeeded: '✓', failed: '✕', running: '●', queued: '○', pending: '○', skipped: '−', cancelled: '⊘', timed_out: '✕', cancelling: '⊘' };

function groupBy(arr, f) { const m = new Map(); arr.forEach((x) => { const k = f(x); if (!m.has(k)) m.set(k, []); m.get(k).push(x); }); return m; }

/** Compute positions. graph = {nodes:[{id,name,type,parent,scope}], edges:[{from,to,conditions}]}. */
export function layoutGraph(graph) {
  const nodes = (graph && graph.nodes) || [];
  const edges = (graph && graph.edges) || [];
  const byParent = groupBy(nodes, (n) => n.parent || '');
  const out = { nodes: [], edges: [], labels: [], w: 0, h: 0 };

  function scope(list, ox, oy) {
    // returns {w,h} and pushes absolute entries
    const ids = new Set(list.map((n) => n.id));
    const rank = {}; list.forEach((n) => { rank[n.id] = 0; });
    const es = edges.filter((e) => ids.has(e.from) && ids.has(e.to));
    for (let i = 0; i < list.length; i += 1) {
      let changed = false;
      es.forEach((e) => { if (rank[e.to] < rank[e.from] + 1 && rank[e.from] + 1 < list.length) { rank[e.to] = rank[e.from] + 1; changed = true; } });
      if (!changed) break;
    }
    // size every node (containers contain their children)
    const size = {};
    const kids = {};
    list.forEach((n) => {
      if (CONTAINERS.has(n.type) && byParent.has(n.id)) {
        const groups = [...groupBy(byParent.get(n.id), (c) => c.scope).entries()];
        const sub = groups.map(([sc, cl]) => ({ sc, cl, m: measure(cl) }));
        const w = Math.max(NW, ...sub.map((s) => s.m.w)) + PAD * 2;
        const h = HEAD + sub.reduce((a, s) => a + s.m.h + LABEL + 6, 0) + PAD;
        size[n.id] = { w, h }; kids[n.id] = sub;
      } else size[n.id] = { w: NW, h: NH };
    });
    const cols = groupBy(list, (n) => rank[n.id]);
    const keys = [...cols.keys()].sort((a, b) => a - b);
    let x = 0; const colH = {}; const colX = {}; const colW = {};
    keys.forEach((k) => {
      const col = cols.get(k);
      colW[k] = Math.max(...col.map((n) => size[n.id].w));
      colH[k] = col.reduce((a, n) => a + size[n.id].h, 0) + GY * (col.length - 1);
      colX[k] = x; x += colW[k] + GX;
    });
    const H = Math.max(0, ...keys.map((k) => colH[k]));
    const pos = {};
    keys.forEach((k) => {
      let y = (H - colH[k]) / 2;
      cols.get(k).forEach((n) => { pos[n.id] = { x: colX[k], y, ...size[n.id] }; y += size[n.id].h + GY; });
    });
    list.forEach((n) => {
      const p = pos[n.id];
      const ax = ox + p.x; const ay = oy + p.y;
      out.nodes.push({ node: n, x: ax, y: ay, w: p.w, h: p.h, container: !!kids[n.id] });
      if (kids[n.id]) {
        let cy = ay + HEAD;
        kids[n.id].forEach((s) => {
          out.labels.push({ x: ax + PAD, y: cy + 12, text: scopeLabel(s.sc) });
          scope(s.cl, ax + PAD, cy + LABEL);
          cy += s.m.h + LABEL + 6;
        });
      }
    });
    es.forEach((e) => {
      const a = pos[e.from]; const b = pos[e.to];
      const sx = ox + a.x + a.w; const sy = oy + a.y + a.h / 2; const tx = ox + b.x; const ty = oy + b.y + b.h / 2;
      const dx = Math.max(24, (tx - sx) / 2);
      out.edges.push({ from: e.from, to: e.to, conditions: e.conditions || [], d: `M${sx},${sy} C${sx + dx},${sy} ${tx - dx},${ty} ${tx},${ty}` });
    });
    return { w: Math.max(0, x - GX), h: H };
  }
  // measuring needs a dry run of the same algorithm, so run it into a scratch result
  function measure(list) {
    const keep = { n: out.nodes, e: out.edges, l: out.labels };
    out.nodes = []; out.edges = []; out.labels = [];
    const m = scope(list, 0, 0);
    out.nodes = keep.n; out.edges = keep.e; out.labels = keep.l;
    return m;
  }
  const top = byParent.get('') || [];
  const m = scope(top, 12, 12);
  out.w = m.w + 24; out.h = m.h + 24;
  return out;
}
function scopeLabel(sc) {
  const last = String(sc || '').split('.').pop();
  return ({ ifTrueActivities: 'If true', ifFalseActivities: 'If false', defaultActivities: 'Default', cases: 'Cases', activities: 'Activities' }[last] || last.replace(/\[\d+\]$/, '')).toString();
}
const trunc = (s, n) => (s.length > n ? s.slice(0, n - 1) + '…' : s);
const edgeKind = (c) => { const l = (c || []).map((x) => String(x).toLowerCase()); return l.includes('failed') && !l.includes('succeeded') ? 'failed' : l.includes('skipped') && l.length === 1 ? 'skipped' : l.includes('completed') ? 'completed' : 'ok'; };

/**
 * <Dag graph states selected onSelect errors label/>
 * `states` maps node id to a run state; `errors` is a Set of node ids with problems.
 */
export function Dag({ graph, states, selected, onSelect, errors, label = 'Pipeline graph', height = 420, empty }) {
  const lay = useMemo(() => layoutGraph(graph), [graph]);
  const [wrapEl, setWrapEl] = useState(null);
  const [zoom, setZoom] = useState(null); // null = fit
  const [avail, setAvail] = useState(900);
  const uid = useId().replace(/[^a-z0-9]/gi, '');
  useEffect(() => {
    if (!wrapEl || typeof ResizeObserver === 'undefined') return undefined;
    setAvail(wrapEl.clientWidth || 900);
    const ro = new ResizeObserver((es) => setAvail(es[0].contentRect.width));
    ro.observe(wrapEl);
    return () => ro.disconnect();
  }, [wrapEl]);
  if (!lay.nodes.length) return empty || html`<div class="dag-empty muted">No activities yet.</div>`;
  const fit = Math.min(1, Math.max(0.3, (avail - 8) / lay.w));
  const z = zoom == null ? fit : zoom;
  const nodeAt = (id) => lay.nodes.find((n) => n.node.id === id);
  function key(e, n) {
    if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onSelect && onSelect(n.node.id); }
  }
  const stateOf = (id) => (states && states[id]) || '';
  return html`<div class="dag" data-testid="dag">
    <div class="dag-tools"><${Btn} size="sm" iconOnly icon="minus" label="Zoom out" onClick=${() => setZoom(Math.max(0.3, z - 0.15))} /><${Btn} size="sm" onClick=${() => setZoom(null)}>${Math.round(z * 100)}%<//><${Btn} size="sm" iconOnly icon="plus" label="Zoom in" onClick=${() => setZoom(Math.min(2, z + 0.15))} />
      <span class="dag-legend small muted" aria-hidden="true"><i class="lg ok"></i>on success <i class="lg failed"></i>on failure <i class="lg completed"></i>always</span></div>
    <div class="dag-scroll" ref=${setWrapEl} style=${`max-height:${height}px`}>
      <svg class="dag-svg" width=${lay.w * z} height=${lay.h * z} viewBox=${`0 0 ${lay.w} ${lay.h}`} role="group" aria-label=${label}>
        <defs>${['ok', 'failed', 'skipped', 'completed'].map((k) => html`<marker key=${k} id=${`${uid}-${k}`} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" class=${'dag-arrow ' + k} /></marker>`)}</defs>
        ${lay.nodes.filter((n) => n.container).map((n) => html`<rect key=${'c' + n.node.id} class="dag-group" x=${n.x} y=${n.y} width=${n.w} height=${n.h} rx="10" />`)}
        ${lay.labels.map((l, i) => html`<text key=${i} class="dag-scope" x=${l.x} y=${l.y}>${l.text}</text>`)}
        ${lay.edges.map((e, i) => { const k = edgeKind(e.conditions); return html`<path key=${i} class=${'dag-edge ' + k} d=${e.d} marker-end=${`url(#${uid}-${k})`}><title>${`${e.from.split('/').pop()} to ${e.to.split('/').pop()} ${(e.conditions || []).join(', ')}`}</title></path>`; })}
        ${lay.nodes.map((n) => {
    const st = stateOf(n.node.id); const bad = errors && errors.has(n.node.id);
    const name = n.node.name || n.node.id.split('/').pop();
    return html`<g key=${n.node.id} class=${`dag-n cat-${activityCategory(n.node.type)}${st ? ' s-' + st : ''}${selected === n.node.id ? ' sel' : ''}${bad ? ' err' : ''}${n.container ? ' cont' : ''}`}
        transform=${`translate(${n.x},${n.y})`} tabindex="0" role="button" aria-pressed=${selected === n.node.id} data-node=${n.node.id}
        aria-label=${`${name}, ${n.node.type}${st ? ', ' + st : ''}${bad ? ', has validation problems' : ''}`}
        onClick=${() => onSelect && onSelect(n.node.id)} onKeyDown=${(e) => key(e, n)}>
        <rect class="dag-box" width=${n.container ? n.w : NW} height=${n.container ? HEAD - 4 : NH} rx="8" />
        <rect class="dag-cat" x="0" y="0" width="5" height=${n.container ? HEAD - 4 : NH} rx="2" />
        <text class="dag-name" x="16" y=${n.container ? 14 : 22}>${trunc(name, n.container ? 34 : 23)}</text>
        <text class="dag-type" x="16" y=${n.container ? 14 : 40} dx=${n.container ? Math.min(name.length, 34) * 7.2 + 8 : 0}>${n.node.type}</text>
        ${st || bad ? html`<text class="dag-glyph" x=${(n.container ? n.w : NW) - 14} y=${n.container ? 14 : 22} text-anchor="end">${bad ? '!' : GLYPH[st] || ''}</text>` : null}
        <title>${`${name} (${n.node.type})${st ? ' - ' + st : ''}`}</title>
      </g>`;
  })}
      </svg></div></div>`;
}

export const hasNode = (graph, id) => !!((graph && graph.nodes) || []).find((n) => n.id === id);
