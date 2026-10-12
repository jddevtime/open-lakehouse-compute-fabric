// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Drawing rules of the lineage graph that must be identical on screen and in exports: edge styles by confidence, node labels, and a pure SVG serialiser
// (the export is built from the layout data, not copied from the DOM, so it is the same picture in any theme and can be tested without a browser).
import { categoryOf, typeLabel, confLabel, layerOf, CATEGORY_HEADING } from './model.js';

/** Stroke pattern by confidence: solid = verified, long dashes = derived, short dashes = inferred, dots = unknown. */
export const EDGE_DASH = { verified: '', derived: '8 4', inferred: '3 3', unknown: '1.5 4' };
export const edgeDash = (c) => (EDGE_DASH[c] !== undefined ? EDGE_DASH[c] : EDGE_DASH.unknown);
export const EDGE_WIDTH = { verified: 2.2, derived: 1.8, inferred: 1.6, unknown: 1.6 };

export const trunc = (s, n) => { const t = String(s ?? ''); return t.length > n ? t.slice(0, Math.max(1, n - 1)) + '…' : t; };
const x = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

/** Text lines drawn in a node: name, and the type (with a bronze / silver / gold note when the NAME says so). */
export function nodeLines(n) {
  const layer = layerOf(n);
  const proc = categoryOf(n.type) === 'process';
  return { title: trunc(n.name || n.id, proc ? 22 : 26), sub: trunc(typeLabel(n.type) + (layer ? ` · ${layer} (by name)` : ''), 34), layer };
}

/** Column headings: the dominant kind of thing in each layer ("Sources", "Tables", "Notebooks, queries and pipelines", ...). */
export function layerHeadings(layout, nodeById) {
  const per = layout.layers.map(() => ({}));
  for (const ln of layout.nodes) {
    const n = nodeById.get(ln.id);
    if (!n) continue;
    const c = categoryOf(n.type);
    per[ln.layer][c] = (per[ln.layer][c] || 0) + 1;
  }
  return per.map((counts) => {
    const top = Object.entries(counts).sort((a, b) => b[1] - a[1])[0];
    return top ? CATEGORY_HEADING[top[0]] || '' : '';
  });
}

/** Standalone SVG text of a laid-out graph. `colors` holds resolved colour strings (see colorsFromCss in the component). */
export function graphToSvg({ layout, nodes, edges, selected, colors, title = 'Lineage' }) {
  const nodeById = new Map(nodes.map((n) => [n.id, n]));
  const edgeById = new Map(edges.map((e) => [e.id, e]));
  const headings = layerHeadings(layout, nodeById);
  const W = Math.max(Math.ceil(layout.width), 440); const H = Math.ceil(layout.height) + 34 + 28;
  const font = colors.font || 'system-ui, sans-serif';
  const parts = [];
  parts.push(`<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" font-family="${x(font)}">`);
  parts.push(`<title>${x(title)}</title>`);
  parts.push(`<rect width="${W}" height="${H}" fill="${x(colors.bg)}"/>`);
  parts.push(`<defs><marker id="ah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="${x(colors.ink3)}"/></marker></defs>`);
  layout.layers.forEach((l, i) => { if (headings[i]) parts.push(`<text x="${l.x + l.w / 2}" y="18" text-anchor="middle" font-size="11" font-weight="600" fill="${x(colors.ink3)}">${x(headings[i])}</text>`); });
  parts.push('<g transform="translate(0,34)">');
  for (const le of layout.edges) {
    const e = edgeById.get(le.id) || {};
    const conf = e.confidence || 'unknown';
    const dash = edgeDash(conf);
    parts.push(`<path d="${le.d}" fill="none" stroke="${x(colors.ink3)}" stroke-width="${EDGE_WIDTH[conf] || 1.6}"${dash ? ` stroke-dasharray="${dash}"` : ''} marker-end="url(#ah)"/>`);
  }
  for (const ln of layout.nodes) {
    const n = nodeById.get(ln.id);
    if (!n) continue;
    const cat = categoryOf(n.type);
    const { title: t1, sub } = nodeLines(n);
    const isSel = selected === n.id;
    const rx = ln.process ? ln.h / 2 : 8;
    parts.push(`<g transform="translate(${ln.x},${ln.y})"><rect width="${ln.w}" height="${ln.h}" rx="${rx}" fill="${x(colors.surface)}" stroke="${x(isSel ? colors.accent : colors.lineStrong)}" stroke-width="${isSel ? 2.4 : 1.2}"/>`);
    if (!ln.process) parts.push(`<rect width="5" height="${ln.h}" rx="2" fill="${x(colors[cat] || colors.dataset)}"/>`);
    const tx = ln.process ? ln.w / 2 : 16;
    const anchor = ln.process ? 'middle' : 'start';
    if (ln.process) {
      parts.push(`<text x="${tx}" y="${ln.h / 2 - 2}" text-anchor="${anchor}" font-size="12" font-weight="600" fill="${x(colors.ink)}">${x(t1)}</text>`);
      parts.push(`<text x="${tx}" y="${ln.h / 2 + 11}" text-anchor="${anchor}" font-size="10.5" fill="${x(colors.ink3)}">${x(sub)}</text>`);
    } else {
      parts.push(`<text x="${tx}" y="${ln.h / 2 - 4}" font-size="12.5" font-weight="600" fill="${x(colors.ink)}">${x(t1)}</text>`);
      parts.push(`<text x="${tx}" y="${ln.h / 2 + 12}" font-size="10.5" fill="${x(colors.ink3)}">${x(sub)}</text>`);
    }
    parts.push(`<title>${x((n.path || n.name) + ' (' + typeLabel(n.type) + ')')}</title></g>`);
  }
  parts.push('</g>');
  // legend
  const ly = H - 12;
  let lx = 16;
  for (const c of ['verified', 'derived', 'inferred', 'unknown']) {
    const dash = edgeDash(c);
    parts.push(`<line x1="${lx}" y1="${ly}" x2="${lx + 26}" y2="${ly}" stroke="${x(colors.ink3)}" stroke-width="${EDGE_WIDTH[c]}"${dash ? ` stroke-dasharray="${dash}"` : ''}/><text x="${lx + 32}" y="${ly + 4}" font-size="10.5" fill="${x(colors.ink3)}">${x(confLabel(c))}</text>`);
    lx += 100;
  }
  parts.push('</svg>');
  return parts.join('');
}
