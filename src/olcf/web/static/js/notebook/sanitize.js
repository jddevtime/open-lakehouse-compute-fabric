// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Allow-list HTML sanitiser for notebook outputs (pandas tables, rich reprs). Untrusted markup is parsed into an inert document (DOMParser never runs
// scripts or loads resources) and only known-safe elements and attributes are rebuilt as Preact vnodes. There is no innerHTML anywhere.
import { h } from '../core/h.js';
import { safeHref, safeImgSrc } from './markdown.js';

const VOID = new Set(['br', 'hr', 'img', 'wbr']);
const TAGS = new Set(['a', 'abbr', 'b', 'bdi', 'blockquote', 'br', 'caption', 'cite', 'code', 'col', 'colgroup', 'dd', 'del', 'details', 'dfn', 'div', 'dl', 'dt', 'em', 'figcaption', 'figure', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'hr', 'i', 'img', 'ins', 'kbd', 'li', 'mark', 'ol', 'p', 'pre', 'q', 's', 'samp', 'small', 'span', 'strong', 'sub', 'summary', 'sup', 'table', 'tbody', 'td', 'tfoot', 'th', 'thead', 'time', 'tr', 'u', 'ul', 'var', 'wbr', 'center', 'font', 'big', 'tt']);
const DROP_WITH_CONTENT = new Set(['script', 'style', 'iframe', 'object', 'embed', 'applet', 'template', 'noscript', 'link', 'meta', 'base', 'form', 'input', 'button', 'select', 'textarea', 'svg', 'math', 'audio', 'video', 'canvas', 'frame', 'frameset', 'title', 'head']);
const ATTRS = {
  '*': new Set(['title', 'lang', 'dir', 'align', 'valign', 'colspan', 'rowspan', 'scope', 'headers', 'span', 'border', 'cellpadding', 'cellspacing', 'width', 'height', 'datetime', 'open', 'start', 'reversed', 'type']),
  a: new Set(['href', 'name']), img: new Set(['src', 'alt']),
};
const CSS_OK = new Set(['color', 'background-color', 'background', 'font-weight', 'font-style', 'font-size', 'font-family', 'text-align', 'text-decoration', 'vertical-align', 'white-space', 'width', 'height', 'min-width', 'max-width', 'padding', 'padding-left', 'padding-right', 'padding-top', 'padding-bottom', 'margin', 'margin-left', 'margin-right', 'margin-top', 'margin-bottom', 'border', 'border-top', 'border-bottom', 'border-left', 'border-right', 'border-color', 'border-style', 'border-width', 'border-collapse', 'border-radius', 'display', 'line-height', 'text-overflow', 'overflow', 'float']);
const CSS_VALUE_OK = /^[\w\s#%.,()+\-/!"']*$/;

export function cleanStyle(s) {
  const out = [];
  for (const decl of String(s || '').split(';')) {
    const i = decl.indexOf(':');
    if (i < 1) continue;
    const prop = decl.slice(0, i).trim().toLowerCase();
    const val = decl.slice(i + 1).trim();
    if (!CSS_OK.has(prop) || !val || val.length > 120 || !CSS_VALUE_OK.test(val) || /url\s*\(|expression|javascript|@import|\\/i.test(val)) continue;
    if (prop === 'display' && !/^(block|inline|inline-block|none|table|table-cell|flex)$/.test(val)) continue;
    out.push(`${prop}:${val}`);
  }
  return out.join(';');
}

const MAX_NODES = 12000;
function convert(node, ctx) {
  if (ctx.count++ > MAX_NODES) { ctx.truncated = true; return null; }
  if (node.nodeType === 3) return node.nodeValue;
  if (node.nodeType !== 1) return null;
  const tag = node.nodeName.toLowerCase();
  if (DROP_WITH_CONTENT.has(tag)) return null;
  const kids = [];
  for (const c of node.childNodes) { const v = convert(c, ctx); if (v != null) kids.push(v); }
  if (!TAGS.has(tag)) return kids.length ? kids : null; // unknown element: keep its text, lose the wrapper
  const props = {};
  for (const a of node.attributes) {
    const name = a.name.toLowerCase();
    const v = a.value;
    if (name.startsWith('on')) continue;
    if (name === 'style') { const st = cleanStyle(v); if (st) props.style = st; continue; }
    if (name === 'class') { props.class = 'x-' + v.split(/\s+/).filter((c) => /^[\w-]{1,40}$/.test(c)).join(' x-'); if (props.class === 'x-') delete props.class; continue; }
    if (!(ATTRS['*'].has(name) || (ATTRS[tag] && ATTRS[tag].has(name)))) continue;
    if (name === 'href') { const u = safeHref(v); if (u) { props.href = u; if (/^https?:/i.test(u)) { props.target = '_blank'; props.rel = 'noopener noreferrer'; } } continue; }
    if (name === 'src') { const u = safeImgSrc(v); if (u) props.src = u; else { props['data-blocked'] = '1'; } continue; }
    props[name === 'colspan' ? 'colSpan' : name === 'rowspan' ? 'rowSpan' : name === 'cellpadding' ? 'cellPadding' : name === 'cellspacing' ? 'cellSpacing' : name] = v;
  }
  if (tag === 'img' && !props.src) return h('span', { class: 'md-img-blocked' }, `[image: ${props.alt || 'blocked'}]`);
  return h(tag === 'center' ? 'div' : tag === 'font' || tag === 'big' || tag === 'tt' ? 'span' : tag, props, VOID.has(tag) ? undefined : kids);
}

/** Sanitise an HTML string to vnodes. Returns {nodes, truncated}. */
export function sanitizeHtml(src, { maxChars = 800_000 } = {}) {
  const text = String(src ?? '');
  const clipped = text.length > maxChars;
  let doc;
  try { doc = new DOMParser().parseFromString(clipped ? text.slice(0, maxChars) : text, 'text/html'); } catch { return { nodes: [text.slice(0, 2000)], truncated: true }; }
  const ctx = { count: 0, truncated: clipped };
  const nodes = [];
  for (const c of doc.body.childNodes) { const v = convert(c, ctx); if (v != null) nodes.push(v); }
  return { nodes, truncated: ctx.truncated };
}

export function SafeHtml({ source, class: cls = '' }) {
  const { nodes, truncated } = sanitizeHtml(source);
  return h('div', { class: 'nb-html ' + cls }, nodes, truncated ? h('div', { class: 'small muted' }, 'Output truncated for display.') : null);
}
