// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// A small, safe Markdown renderer. It produces Preact vnodes (never an HTML string), so nothing the author types can become markup:
// raw HTML in the source is shown as text, link and image URLs go through an allow-list, and external images are not fetched (CSP blocks them anyway).
import { h, Fragment } from '../core/h.js';

const SAFE_LINK = /^(https?:|mailto:|#|\/(?!\/)|\.{0,2}\/|[a-z0-9_.~-]+(?:\/|$)(?!.*:))/i;
const SAFE_IMG = /^(data:image\/(png|jpe?g|gif|webp|svg\+xml|bmp)[;,]|\/(?!\/)|\.{0,2}\/|blob:)/i;

/** Returns a safe href or null. `javascript:`, `vbscript:`, `data:` and protocol-relative URLs are refused. */
export function safeHref(u) {
  const s = String(u || '').trim().replace(/[\u0000-\u001f\u007f\s]+/g, '');
  if (!s) return null;
  if (/^(javascript|vbscript|data|file|blob):/i.test(s)) return null;
  if (s.startsWith('//')) return null;
  return SAFE_LINK.test(s) ? String(u).trim() : null;
}
export function safeImgSrc(u) {
  const s = String(u || '').trim();
  return SAFE_IMG.test(s.replace(/[\u0000-\u001f]/g, '')) ? s : null;
}

const INLINE = [
  ['code', /(`+)([\s\S]*?[^`])\1(?!`)/],
  ['img', /!\[([^\]]*)\]\(\s*([^)\s]+)(?:\s+"([^"]*)")?\s*\)/],
  ['link', /\[((?:[^\[\]]|\[[^\]]*\])*)\]\(\s*([^)\s]+)(?:\s+"([^"]*)")?\s*\)/],
  ['auto', /<((?:https?:\/\/|mailto:)[^>\s]+)>/],
  ['strong', /(\*\*)(?=\S)([\s\S]*?\S)\1/],
  ['strongu', /(?<![\p{L}\p{N}_])(__)(?=\S)([\s\S]*?\S)\1(?![\p{L}\p{N}_])/u],     // underscores inside a word (gross_amount) are not emphasis
  ['del', /~~(?=\S)([\s\S]*?\S)~~/],
  ['em', /(\*)(?=\S)([\s\S]*?\S)\1/],
  ['emu', /(?<![\p{L}\p{N}_])(_)(?=\S)([\s\S]*?\S)\1(?![\p{L}\p{N}_])/u],
  ['br', /( {2,}|\\)\n/],
  ['esc', /\\([\\`*_{}\[\]()#+\-.!|~<>])/],
  ['url', /(https?:\/\/[^\s<>()]+[^\s<>().,;:!?'"])/],
];

function inline(src, key = 'i') {
  const out = [];
  let rest = src;
  let n = 0;
  while (rest) {
    let best = null;
    for (const [type, re] of INLINE) {
      const m = re.exec(rest);
      if (m && (!best || m.index < best.m.index)) best = { type, m };
    }
    if (!best) { out.push(rest); break; }
    const { type, m } = best;
    if (m.index > 0) out.push(rest.slice(0, m.index));
    const k = `${key}${n++}`;
    switch (type) {
      case 'code': out.push(h('code', { key: k }, m[2].replace(/^ (.*) $/, '$1'))); break;
      case 'img': {
        const src2 = safeImgSrc(m[2]);
        out.push(src2 ? h('img', { key: k, src: src2, alt: m[1], title: m[3], loading: 'lazy', class: 'md-img' })
          : h('span', { key: k, class: 'md-img-blocked', title: 'External images are not loaded' }, `[image: ${m[1] || m[2]}]`));
        break;
      }
      case 'link': {
        const href = safeHref(m[2]);
        out.push(href ? h('a', { key: k, href, title: m[3], ...(/^https?:/i.test(href) ? { target: '_blank', rel: 'noopener noreferrer' } : {}) }, inline(m[1], k)) : h('span', { key: k }, inline(m[1], k)));
        break;
      }
      case 'auto': case 'url': {
        const href = safeHref(m[1]);
        out.push(href ? h('a', { key: k, href, target: '_blank', rel: 'noopener noreferrer' }, m[1]) : m[1]);
        break;
      }
      case 'strong': case 'strongu': out.push(h('strong', { key: k }, inline(m[2], k))); break;
      case 'em': case 'emu': out.push(h('em', { key: k }, inline(m[2], k))); break;
      case 'del': out.push(h('del', { key: k }, inline(m[1], k))); break;
      case 'br': out.push(h('br', { key: k })); break;
      case 'esc': out.push(m[1]); break;
      default: out.push(m[0]);
    }
    rest = rest.slice(m.index + m[0].length);
  }
  return out;
}

const isBlank = (l) => /^\s*$/.test(l);
const HR = /^ {0,3}([-*_])( *\1){2,} *$/;
const HEAD = /^ {0,3}(#{1,6})\s+(.*?)(?:\s+#+)?\s*$/;
const FENCE = /^ {0,3}(`{3,}|~{3,})\s*([\w+-]*)/;
const LIST = /^(\s*)([-*+]|\d{1,9}[.)])\s+(.*)$/;
const QUOTE = /^ {0,3}>\s?(.*)$/;
const TABLE_SEP = /^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)*\|?\s*$/;

function splitRow(l) {
  let s = l.trim();
  if (s.startsWith('|')) s = s.slice(1);
  if (s.endsWith('|') && !s.endsWith('\\|')) s = s.slice(0, -1);
  return s.split(/(?<!\\)\|/).map((c) => c.trim().replace(/\\\|/g, '|'));
}

function blocks(lines, key = 'b') {
  const out = [];
  let i = 0;
  let n = 0;
  while (i < lines.length) {
    const line = lines[i];
    const k = `${key}${n++}`;
    if (isBlank(line)) { i++; continue; }
    let m = FENCE.exec(line);
    if (m) {
      const fence = m[1]; const body = [];
      i++;
      while (i < lines.length && !new RegExp(`^ {0,3}${fence[0]}{${fence.length},}\\s*$`).test(lines[i])) body.push(lines[i++]);
      i++;
      out.push(h('pre', { key: k, class: 'md-code', 'data-lang': m[2] || undefined, tabindex: 0 }, h('code', null, body.join('\n'))));
      continue;
    }
    if ((m = HEAD.exec(line))) { out.push(h('h' + m[1].length, { key: k }, inline(m[2], k))); i++; continue; }
    if (HR.test(line)) { out.push(h('hr', { key: k })); i++; continue; }
    if (QUOTE.test(line)) {
      const q = [];
      while (i < lines.length && (QUOTE.test(lines[i]) || (!isBlank(lines[i]) && q.length && !LIST.test(lines[i]) && !HEAD.test(lines[i])))) { q.push((QUOTE.exec(lines[i]) || [0, lines[i]])[1]); i++; }
      out.push(h('blockquote', { key: k }, blocks(q, k)));
      continue;
    }
    if (i + 1 < lines.length && line.includes('|') && TABLE_SEP.test(lines[i + 1])) {
      const head = splitRow(line);
      const align = splitRow(lines[i + 1]).map((c) => (/^:-+:$/.test(c) ? 'center' : /-:$/.test(c) ? 'right' : /^:-/.test(c) ? 'left' : undefined));
      i += 2;
      const rows = [];
      while (i < lines.length && !isBlank(lines[i]) && lines[i].includes('|')) rows.push(splitRow(lines[i++]));
      out.push(h('div', { key: k, class: 'md-table' }, h('table', null,
        h('thead', null, h('tr', null, head.map((c, j) => h('th', { key: j, style: align[j] ? `text-align:${align[j]}` : undefined }, inline(c, `${k}h${j}`))))),
        h('tbody', null, rows.map((r, ri) => h('tr', { key: ri }, head.map((_, j) => h('td', { key: j, style: align[j] ? `text-align:${align[j]}` : undefined }, inline(r[j] ?? '', `${k}r${ri}c${j}`)))))))));
      continue;
    }
    if (LIST.test(line)) {
      const first = LIST.exec(line);
      const ordered = /\d/.test(first[2]);
      const baseIndent = first[1].length;
      const items = [];
      while (i < lines.length) {
        const lm = LIST.exec(lines[i]);
        if (lm && lm[1].length <= baseIndent + 1 && /\d/.test(lm[2]) === ordered) {
          const body = [lm[3]];
          i++;
          while (i < lines.length) {
            const l = lines[i];
            if (isBlank(l)) { if (i + 1 < lines.length && /^\s{2,}\S/.test(lines[i + 1]) && !(LIST.exec(lines[i + 1]) && LIST.exec(lines[i + 1])[1].length <= baseIndent + 1)) { body.push(''); i++; continue; } break; }
            const l2 = LIST.exec(l);
            if (l2 && l2[1].length <= baseIndent + 1) break;
            if (HEAD.test(l) || FENCE.test(l) && !/^\s/.test(l)) break;
            body.push(l.replace(new RegExp(`^ {0,${baseIndent + 2}}`), ''));
            i++;
          }
          items.push(body);
        } else break;
      }
      const startNum = ordered ? parseInt(first[2], 10) : undefined;
      out.push(h(ordered ? 'ol' : 'ul', { key: k, start: startNum && startNum !== 1 ? startNum : undefined }, items.map((body, ii) => {
        const task = /^\[([ xX])\]\s+/.exec(body[0]);
        if (task) body[0] = body[0].slice(task[0].length);
        const kids = blocks(body, `${k}-${ii}`);
        const tight = kids.length === 1 && kids[0].type === 'p' ? kids[0].props.children : kids;
        return h('li', { key: ii, class: task ? 'task' : undefined }, task ? h('input', { type: 'checkbox', checked: task[1] !== ' ', disabled: true, 'aria-label': task[1] !== ' ' ? 'done' : 'not done' }) : null, tight);
      })));
      continue;
    }
    // paragraph (also swallows setext headings)
    const para = [];
    while (i < lines.length && !isBlank(lines[i]) && !HEAD.test(lines[i]) && !FENCE.test(lines[i]) && !HR.test(lines[i]) && !QUOTE.test(lines[i]) && !(para.length && LIST.test(lines[i]))) {
      if (para.length && /^ {0,3}(=+|-+)\s*$/.test(lines[i])) { const lvl = lines[i].trim()[0] === '=' ? 1 : 2; out.push(h('h' + lvl, { key: k }, inline(para.join(' '), k))); para.length = 0; i++; break; }
      if (para.length && lines[i].includes('|') && i + 1 < lines.length && TABLE_SEP.test(lines[i + 1])) break;
      para.push(lines[i].replace(/^\s+/, '')); i++;
    }
    if (para.length) out.push(h('p', { key: k }, inline(para.join('\n'), k)));
  }
  return out;
}

/** Render markdown source to vnodes. */
export function Markdown({ source, class: cls = '' }) {
  const text = String(source ?? '').replace(/\r\n?/g, '\n');
  return h('div', { class: 'md ' + cls }, blocks(text.split('\n')));
}
export const renderMarkdown = (source) => h(Fragment, null, blocks(String(source ?? '').replace(/\r\n?/g, '\n').split('\n')));
