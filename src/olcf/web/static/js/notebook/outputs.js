// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Notebook output rendering: stream text with ANSI colours and carriage returns, rich display data picked by MIME priority, errors with tracebacks.
import { html, useState } from '../core/h.js';
import { Markdown } from './markdown.js';
import { SafeHtml } from './sanitize.js';
import { Btn } from '../ui/basic.js';
import { fmtBytes } from '../core/util.js';

const join = (v) => (Array.isArray(v) ? v.join('') : v == null ? '' : typeof v === 'string' ? v : JSON.stringify(v, null, 2));
const MAX_TEXT = 200_000;

// ---- ANSI -----------------------------------------------------------------------------------------------------------------------------------
const ANSI_RE = /\u001b\[([0-9;]*)m/g;
/** Apply backspaces and carriage returns the way a terminal would (progress bars rewrite their line). */
export function collapseCr(text) {
  if (!/[\r\b]/.test(text)) return text;
  return text.split('\n').map((line) => {
    if (line.includes('\r')) { const parts = line.split('\r'); let cur = ''; for (const p of parts) { cur = p.length >= cur.length ? p : p + cur.slice(p.length); } line = cur; }
    while (/[^\n\b]\u0008/.test(line)) line = line.replace(/[^\n\b]\u0008/g, '');
    return line;
  }).join('\n');
}
/** Split a string with SGR escapes into [{text, cls}] runs. */
export function ansiRuns(text) {
  const runs = [];
  let st = { fg: null, bg: null, b: false, i: false, u: false, d: false };
  let last = 0;
  const cls = (s) => [s.fg != null ? 'ansi-fg-' + s.fg : '', s.bg != null ? 'ansi-bg-' + s.bg : '', s.b ? 'ansi-b' : '', s.i ? 'ansi-i' : '', s.u ? 'ansi-u' : '', s.d ? 'ansi-d' : ''].filter(Boolean).join(' ');
  let m;
  ANSI_RE.lastIndex = 0;
  while ((m = ANSI_RE.exec(text))) {
    if (m.index > last) runs.push({ text: text.slice(last, m.index), cls: cls(st) });
    last = m.index + m[0].length;
    const codes = m[1] === '' ? [0] : m[1].split(';').map((x) => parseInt(x || '0', 10));
    st = { ...st };
    for (let i = 0; i < codes.length; i++) {
      const c = codes[i];
      if (c === 0) st = { fg: null, bg: null, b: false, i: false, u: false, d: false };
      else if (c === 1) st.b = true; else if (c === 2) st.d = true; else if (c === 3) st.i = true; else if (c === 4) st.u = true;
      else if (c === 22) { st.b = false; st.d = false; } else if (c === 23) st.i = false; else if (c === 24) st.u = false;
      else if (c >= 30 && c <= 37) st.fg = c - 30; else if (c >= 90 && c <= 97) st.fg = c - 90 + 8; else if (c === 39) st.fg = null;
      else if (c >= 40 && c <= 47) st.bg = c - 40; else if (c >= 100 && c <= 107) st.bg = c - 100 + 8; else if (c === 49) st.bg = null;
      else if (c === 38 || c === 48) { // 256-colour / truecolour: map to the nearest of the 16 theme colours
        const key = c === 38 ? 'fg' : 'bg';
        if (codes[i + 1] === 5) { st[key] = codes[i + 2] % 16; i += 2; } else if (codes[i + 1] === 2) { i += 4; }
      }
    }
  }
  if (last < text.length) runs.push({ text: text.slice(last), cls: cls(st) });
  return runs.map((r) => ({ ...r, text: r.text.replace(/\u001b\[[0-9;?]*[A-Za-ln-z]/g, '') }));
}
export function Ansi({ text }) {
  const runs = ansiRuns(text);
  return html`${runs.map((r, i) => (r.cls ? html`<span key=${i} class=${r.cls}>${r.text}</span>` : r.text))}`;
}
export const stripAnsi = (s) => String(s ?? '').replace(/\u001b\[[0-9;?]*[A-Za-z]/g, '');

// ---- one output ---------------------------------------------------------------------------------------------------------------------------
function TextBlock({ text, cls, label }) {
  const [all, setAll] = useState(false);
  const long = text.length > MAX_TEXT && !all;
  const shown = long ? text.slice(0, MAX_TEXT) : text;
  return html`<div class=${'out-text ' + cls} role="group" aria-label=${label}><pre><${Ansi} text=${collapseCr(shown)} /></pre>
    ${long ? html`<div class="out-more">Showing the first ${fmtBytes(MAX_TEXT)} of ${fmtBytes(text.length)}. <${Btn} size="sm" onClick=${() => setAll(true)}>Show everything<//></div>` : null}</div>`;
}

const IMG_MIME = ['image/png', 'image/jpeg', 'image/gif', 'image/webp'];
const B64 = /^[A-Za-z0-9+/=\s]+$/;
export function pickMime(data) {
  const order = ['application/vnd.olcf.table+json', 'text/html', 'image/svg+xml', ...IMG_MIME, 'text/markdown', 'text/latex', 'application/json', 'text/plain'];
  return order.find((m) => data[m] != null) || Object.keys(data)[0] || null;
}
function RichData({ data, metadata }) {
  const mime = pickMime(data);
  const v = data[mime];
  if (mime === 'text/html') return html`<${SafeHtml} source=${join(v)} />`;
  if (mime === 'image/svg+xml') return html`<img class="out-img" alt=${data['text/plain'] ? join(data['text/plain']) : 'SVG output'} src=${'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(join(v))} />`;
  if (IMG_MIME.includes(mime)) {
    const b64 = join(v).replace(/\s+/g, '');
    if (!B64.test(b64)) return html`<${TextBlock} text="[image data could not be displayed]" cls="stream" label="image" />`;
    const dim = metadata && metadata[mime];
    return html`<img class="out-img" alt=${data['text/plain'] ? join(data['text/plain']) : 'Figure output'} src=${`data:${mime};base64,${b64}`} width=${dim && dim.width} height=${dim && dim.height} />`;
  }
  if (mime === 'text/markdown') return html`<${Markdown} source=${join(v)} />`;
  if (mime === 'application/json') { return html`<${TextBlock} text=${typeof v === 'string' ? v : JSON.stringify(v, null, 2)} cls="rich" label="JSON output" />`; }
  return html`<${TextBlock} text=${join(v)} cls="rich" label="Text output" />`;
}

function OneOutput({ o }) {
  switch (o.output_type) {
    case 'stream': return html`<${TextBlock} text=${join(o.text)} cls=${'stream ' + (o.name === 'stderr' ? 'stderr' : '')} label=${o.name === 'stderr' ? 'Standard error' : 'Standard output'} />`;
    case 'error': {
      const tb = (o.traceback || []).join('\n') || `${o.ename}: ${o.evalue}`;
      return html`<div class="out-error" role="alert"><div class="out-err-head"><b>${o.ename || 'Error'}</b>${o.evalue ? html`: ${stripAnsi(o.evalue)}` : null}</div><${TextBlock} text=${tb} cls="traceback" label="Traceback" /></div>`;
    }
    case 'execute_result': case 'display_data': return html`<div class="out-rich"><${RichData} data=${o.data || {}} metadata=${o.metadata} /></div>`;
    default: return html`<${TextBlock} text=${JSON.stringify(o, null, 2)} cls="rich" label="Output" />`;
  }
}

/** Merge consecutive stream outputs of the same name so a long print loop is one block. */
export function mergeStreams(outputs) {
  const out = [];
  for (const o of outputs || []) {
    const prev = out[out.length - 1];
    if (o.output_type === 'stream' && prev && prev.output_type === 'stream' && prev.name === o.name) out[out.length - 1] = { ...prev, text: join(prev.text) + join(o.text) };
    else out.push(o);
  }
  return out;
}

export function Outputs({ outputs, collapsed, onToggle }) {
  const merged = mergeStreams(outputs);
  if (!merged.length) return null;
  if (collapsed) return html`<div class="outputs collapsed"><${Btn} size="sm" kind="ghost" icon="chevron-right" onClick=${onToggle}>Show ${merged.length} output${merged.length === 1 ? '' : 's'}<//></div>`;
  return html`<div class="outputs" role="region" aria-label="Cell output">${merged.map((o, i) => html`<${OneOutput} key=${i} o=${o} />`)}</div>`;
}
