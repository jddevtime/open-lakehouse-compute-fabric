// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Dependency-free SVG chart library following the OLCF data-visualisation rules:
//   thin marks (bars <= 24px, 4px rounded data end, 2px surface gaps), 2px lines with ringed end dots, hairline grids, one axis,
//   fixed-order categorical hues (never cycled; tails fold into "Other"), sequential single-hue ramps, legends for >= 2 series,
//   tooltips on hover AND keyboard focus, a table view for every chart, text always in ink tokens (never the series colour).
// All text goes through Preact (escaped); nothing here uses innerHTML.
import { html, useState, useRef, useEffect, useLayoutEffect, useMemo, useId } from '../core/h.js';
import { fmtValue, niceTicks, linear, isNum, toNum, colIsNumeric, inkFor, textW, truncate } from './format.js';
import { Icon } from '../ui/icons.js';
import { fmtNum, fmtCompact, downloadText, downloadBlob, toCsv } from '../core/util.js';

export const SERIES = (i) => `var(--series-${(i % 8) + 1})`;
const MAX_SERIES = 8;

export function useSize(ref, dflt = 480) {
  const [w, setW] = useState(dflt);
  useLayoutEffect(() => {
    if (!ref.current) return undefined;
    setW(Math.max(120, Math.floor(ref.current.getBoundingClientRect().width)));
    const ro = new ResizeObserver((es) => { const x = Math.floor(es[0].contentRect.width); if (x > 40) setW(x); });
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);
  return w;
}

// ---------------------------------------------------------------- data shaping
function cell(r, c, i) { return Array.isArray(r) ? r[i] : r[c.name]; }
function colIndex(cols, name) { const i = cols.findIndex((c) => c.name === name); return i; }

/** Turn tabular rows + view hints into {cats, series[]} for category charts. */
export function shapeCategory(columns, rows, view = {}) {
  const cols = columns.map((c) => (typeof c === 'string' ? { name: c } : c));
  const numeric = cols.map((c, i) => colIsNumeric(c, rows, i));
  let xi = view.x ? colIndex(cols, view.x) : -1;
  if (xi < 0) xi = numeric.findIndex((n) => !n);
  if (xi < 0) xi = 0;
  const yNames = [].concat(view.y || []).filter((n) => colIndex(cols, n) >= 0);
  let groupCol = -1;
  const seriesCol = view.series ? colIndex(cols, view.series) : -1;
  let measures = yNames.map((n) => colIndex(cols, n));
  if (seriesCol >= 0 && seriesCol !== xi) { if (numeric[seriesCol]) { if (!measures.includes(seriesCol)) measures.push(seriesCol); } else groupCol = seriesCol; }
  if (!measures.length) { const first = numeric.findIndex((n, i) => n && i !== xi); if (first >= 0) measures = [first]; }
  const cats = [];
  const catIdx = new Map();
  for (const r of rows) { const k = String(cell(r, cols[xi], xi) ?? '∅'); if (!catIdx.has(k)) { catIdx.set(k, cats.length); cats.push(k); } }
  let series = [];
  if (groupCol >= 0 && measures.length) {
    const m = measures[0];
    const groups = new Map();
    for (const r of rows) {
      const g = String(cell(r, cols[groupCol], groupCol) ?? '∅');
      if (!groups.has(g)) groups.set(g, new Array(cats.length).fill(null));
      const v = toNum(cell(r, cols[m], m));
      const ci = catIdx.get(String(cell(r, cols[xi], xi) ?? '∅'));
      groups.get(g)[ci] = (groups.get(g)[ci] ?? 0) + (v ?? 0);
    }
    series = [...groups.entries()].map(([name, values]) => ({ name, values }));
  } else {
    series = measures.map((m) => {
      const values = new Array(cats.length).fill(null);
      for (const r of rows) { const ci = catIdx.get(String(cell(r, cols[xi], xi) ?? '∅')); const v = toNum(cell(r, cols[m], m)); if (v != null) values[ci] = (values[ci] ?? 0) + v; }
      return { name: cols[m].name, values };
    });
  }
  if (series.length > MAX_SERIES) {
    // fold the tail into Other; never generate a ninth hue
    const head = series.slice(0, MAX_SERIES - 1);
    const tail = series.slice(MAX_SERIES - 1);
    head.push({ name: 'Other', values: cats.map((_, i) => tail.reduce((s, x) => s + (x.values[i] ?? 0), 0)), other: true });
    series = head;
  }
  return { cats, series, xName: cols[xi].name };
}

// ---------------------------------------------------------------- shared bits
export function Legend({ items, kind = 'rect' }) {
  if (!items || items.length < 2) return null;
  return html`<ul class="legend" aria-label="Legend">${items.map((s, i) => html`<li key=${s.name}><span class=${'key ' + kind} style=${`--k:${s.other ? 'var(--chart-muted)' : SERIES(i)}`}></span><span>${s.name}</span></li>`)}</ul>`;
}

function Tip({ tip }) {
  if (!tip) return null;
  const left = tip.flip ? undefined : tip.x + 12;
  const right = tip.flip ? tip.w - tip.x + 12 : undefined;
  return html`<div class="ctip" role="status" style=${`top:${Math.max(0, tip.y - 8)}px;${left != null ? `left:${left}px` : `right:${right}px`}`}>
    ${tip.title != null ? html`<div class="ctip-title">${tip.title}</div>` : null}
    ${tip.rows.map((r, i) => html`<div class="ctip-row" key=${i}><span class=${'key ' + (r.key || 'line')} style=${`--k:${r.color}`}></span><b>${r.value}</b><span class="ctip-name">${r.name}</span></div>`)}</div>`;
}

function Frame({ title, subtitle, legend, children, height, table, exportable, id, onExportName, empty, loading }) {
  const [view, setView] = useState('chart');
  const ref = useRef();
  const hid = useId();
  const doExport = async (kind) => {
    const svg = ref.current && ref.current.querySelector('svg.chart-svg');
    const name = (onExportName || title || 'chart').toString().replace(/[^\w.-]+/g, '_');
    if (kind === 'csv' && table) { downloadText(toCsv(table.columns, table.rows), name + '.csv', 'text/csv'); return; }
    if (!svg) return;
    const { svgText, w, h } = serializeSvg(svg);
    if (kind === 'svg') downloadText(svgText, name + '.svg', 'image/svg+xml');
    if (kind === 'png') {
      const img = new Image();
      img.onload = () => {
        const c = document.createElement('canvas'); const s = 2; c.width = w * s; c.height = h * s;
        const g = c.getContext('2d'); g.scale(s, s); g.fillStyle = getComputedStyle(ref.current).getPropertyValue('--chart-surface') || '#fff'; g.fillRect(0, 0, w, h); g.drawImage(img, 0, 0, w, h);
        c.toBlob((b) => b && downloadBlob(b, name + '.png'));
      };
      img.src = 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svgText);
    }
  };
  return html`<figure class="chart" ref=${ref} aria-labelledby=${hid} data-chart-id=${id}>
    ${title || exportable !== false ? html`<figcaption class="chart-head"><div class="grow"><div id=${hid} class="chart-title">${title}</div>${subtitle ? html`<div class="chart-sub">${subtitle}</div>` : null}</div>
      ${table ? html`<div class="chart-tools"><button class="ctool" aria-pressed=${view === 'table'} title=${view === 'table' ? 'Show chart' : 'Show data table'} onClick=${() => setView(view === 'table' ? 'chart' : 'table')}><${Icon} name=${view === 'table' ? 'chart' : 'table'} size="sm" /><span class="sr-only">${view === 'table' ? 'Show chart' : 'Show data table'}</span></button>
        <button class="ctool" title="Download PNG" onClick=${() => doExport('png')}><${Icon} name="image" size="sm" /><span class="sr-only">Download PNG</span></button>
        <button class="ctool" title="Download SVG" onClick=${() => doExport('svg')}><${Icon} name="code" size="sm" /><span class="sr-only">Download SVG</span></button>
        <button class="ctool" title="Download CSV" onClick=${() => doExport('csv')}><${Icon} name="download" size="sm" /><span class="sr-only">Download CSV</span></button></div>` : null}</figcaption>` : null}
    ${view === 'table' && table ? html`<${DataTable} columns=${table.columns} rows=${table.rows} height=${height} />` : html`${children}${legend}`}</figure>`;
}

/** Clone an SVG and inline resolved colours so exports render outside the page's CSS. */
export function serializeSvg(svg) {
  const clone = svg.cloneNode(true);
  const src = [svg, ...svg.querySelectorAll('*')];
  const dst = [clone, ...clone.querySelectorAll('*')];
  src.forEach((s, i) => {
    const cs = getComputedStyle(s);
    const d = dst[i];
    for (const p of ['fill', 'stroke', 'stroke-width', 'opacity', 'fill-opacity', 'font-size', 'font-weight', 'font-family', 'text-anchor']) {
      const v = cs.getPropertyValue(p);
      if (v) d.style.setProperty(p, v);
    }
    d.removeAttribute('class');
  });
  const b = svg.getBoundingClientRect();
  clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg');
  clone.setAttribute('width', b.width); clone.setAttribute('height', b.height);
  return { svgText: new XMLSerializer().serializeToString(clone), w: b.width, h: b.height };
}

export function DataTable({ columns, rows, height }) {
  const cols = columns.map((c) => (typeof c === 'string' ? { name: c } : c));
  return html`<div class="chart-table" style=${height ? `max-height:${height}px` : undefined} tabindex="0"><table class="table dense"><thead><tr>${cols.map((c, i) => html`<th scope="col" key=${i} class=${colIsNumeric(c, rows, i) ? 'num' : ''}>${c.name}</th>`)}</tr></thead>
    <tbody>${rows.slice(0, 500).map((r, ri) => html`<tr key=${ri}>${cols.map((c, i) => { const v = cell(r, c, i); return html`<td key=${i} class=${isNum(v) ? 'num' : ''}>${v == null ? '–' : isNum(v) ? fmtNum(v, Math.abs(v) < 100 ? 2 : 0) : String(v)}</td>`; })}</tr>`)}</tbody></table></div>`;
}

function Empty({ height, text = 'No data for the current filters' }) {
  return html`<div class="chart-empty" style=${`height:${height}px`}><${Icon} name="chart" /><span>${text}</span></div>`;
}

// ---------------------------------------------------------------- axes
function useAxisBox(maxLabel, h, opts = {}) {
  return { l: opts.l ?? Math.max(36, Math.ceil(textW(maxLabel) + 12)), r: opts.r ?? 16, t: opts.t ?? 10, b: opts.b ?? 26, h };
}

function YGrid({ ticks, y, x0, x1, fmt }) {
  return html`<g class="grid" aria-hidden="true">${ticks.map((t) => html`<g key=${t}><line x1=${x0} x2=${x1} y1=${y(t)} y2=${y(t)} class=${t === 0 ? 'axis' : 'gridline'} /><text x=${x0 - 8} y=${y(t)} dy="0.32em" text-anchor="end" class="ct">${fmt(t)}</text></g>`)}</g>`;
}

// ---------------------------------------------------------------- bar / column (grouped or stacked, auto orientation)
export function BarChart({ columns, rows, view = {}, height = 260, title, subtitle, stacked, tableOverride }) {
  const wrap = useRef();
  const W = useSize(wrap);
  const sh = useMemo(() => shapeCategory(columns, rows, view), [columns, rows, view.x, view.y, view.series]);
  const [tip, setTip] = useState(null);
  const [hot, setHot] = useState(null);
  const fmt = (v) => fmtValue(v, view.format, { compact: true });
  const fmtFull = (v) => fmtValue(v, view.format);
  const { cats, series } = sh;
  const isStack = stacked ?? view.stack ?? false;
  const nSeries = series.length;
  const empty = !cats.length || !nSeries;
  const longest = Math.max(0, ...cats.map((c) => c.length));
  const horizontal = view.orientation ? view.orientation === 'horizontal' : cats.length > 7 || longest > 11 || (cats.length > 4 && W < 520);
  const totals = cats.map((_, i) => series.reduce((s, x) => s + (x.values[i] ?? 0), 0));
  const maxV = isStack ? Math.max(...totals, 0) : Math.max(0, ...series.flatMap((s) => s.values.filter(isNum)));
  const minV = Math.min(0, ...series.flatMap((s) => s.values.filter(isNum)));
  const ticks = niceTicks(minV, maxV, horizontal ? 4 : 4);
  const top = ticks[ticks.length - 1]; const bot = ticks[0];
  const hit = (name, i, ev, val, color, key = 'rect') => {
    const r = wrap.current.getBoundingClientRect();
    const px = ev.clientX != null && ev.type !== 'focus' ? ev.clientX - r.left : (ev.target.getBoundingClientRect().left - r.left + ev.target.getBoundingClientRect().width / 2);
    const py = ev.clientY != null && ev.type !== 'focus' ? ev.clientY - r.top : (ev.target.getBoundingClientRect().top - r.top);
    const rowsT = isStack || nSeries > 1 ? series.map((s, si) => ({ key: 'rect', color: s.other ? 'var(--chart-muted)' : SERIES(si), name: s.name, value: fmtFull(s.values[i]) })) : [{ key, color, name, value: fmtFull(val) }];
    setTip({ x: px, y: py, w: r.width, flip: px > r.width * 0.6, title: cats[i], rows: rowsT });
    setHot(`${name}|${i}`);
  };
  const clear = () => { setTip(null); setHot(null); };
  let body;
  if (empty) body = html`<${Empty} height=${height} />`;
  else if (horizontal) {
    const maxLab = Math.min(22, longest);
    const m = { l: Math.ceil(textW('x'.repeat(maxLab)) + 14), r: 56, t: 6, b: 22 };
    const slot = Math.max(18, Math.min(34, (height - m.t - m.b) / cats.length));
    const H = m.t + m.b + slot * cats.length;
    const x = linear(bot, top, m.l, W - m.r);
    const th = Math.min(20, slot * (isStack ? 0.62 : nSeries > 1 ? 0.8 / nSeries : 0.62));
    body = html`<svg class="chart-svg" width="100%" height=${H} viewBox=${`0 0 ${W} ${H}`} role="img" aria-label=${(title || 'Bar chart') + '. ' + cats.length + ' categories.'}>
      <g aria-hidden="true">${ticks.map((t) => html`<g key=${t}><line y1=${m.t} y2=${H - m.b} x1=${x(t)} x2=${x(t)} class=${t === 0 ? 'axis' : 'gridline'} /><text x=${x(t)} y=${H - m.b + 15} text-anchor="middle" class="ct">${fmt(t)}</text></g>`)}</g>
      ${cats.map((c, i) => {
        const cy = m.t + slot * i + slot / 2;
        let acc = 0;
        return html`<g key=${c}><text x=${m.l - 8} y=${cy} dy="0.32em" text-anchor="end" class="ct ink">${truncate(c, maxLab)}<title>${c}</title></text>
          ${series.map((s, si) => {
            const v = s.values[i]; if (v == null) return null;
            const color = s.other ? 'var(--chart-muted)' : SERIES(si);
            let x0; let x1; let yy = cy - th / 2;
            if (isStack) { x0 = x(acc); acc += v; x1 = x(acc); } else { x0 = x(Math.min(0, v)); x1 = x(Math.max(0, v)); yy = cy - (th * nSeries + 2 * (nSeries - 1)) / 2 + si * (th + 2); }
            const w = Math.max(1, x1 - x0 - (isStack && si < nSeries - 1 ? 2 : 0));
            const last = !isStack || si === nSeries - 1 || series.slice(si + 1).every((q) => !q.values[i]);
            return html`<g key=${s.name}><path d=${barPath(x0, yy, w, th, last ? 4 : 0, 'h')} style=${`fill:${color}`} class=${'bar' + (hot === `${s.name}|${i}` ? ' hot' : '')} tabindex="0" role="img" aria-label=${`${c}, ${s.name}: ${fmtFull(v)}`}
              onPointerMove=${(e) => hit(s.name, i, e, v, color)} onPointerLeave=${clear} onFocus=${(e) => hit(s.name, i, e, v, color)} onBlur=${clear} />
              ${!isStack && w > 0 ? html`<text x=${x1 + 6} y=${yy + th / 2} dy="0.32em" class="ct ink strong" aria-hidden="true">${fmt(v)}</text>` : null}</g>`;
          })}</g>`;
      })}</svg>`;
  } else {
    const m = useAxisBox(fmt(top), height, { b: 26, t: 14 });
    const H = height;
    const y = linear(bot, top, H - m.b, m.t);
    const slot = (W - m.l - m.r) / cats.length;
    const th = Math.max(4, Math.min(24, slot * (isStack ? 0.6 : 0.78 / nSeries)));
    const step = Math.max(1, Math.ceil(textW('x'.repeat(Math.min(12, longest)), 11) / slot));
    body = html`<svg class="chart-svg" width="100%" height=${H} viewBox=${`0 0 ${W} ${H}`} role="img" aria-label=${(title || 'Column chart') + '. ' + cats.length + ' categories.'}>
      <${YGrid} ticks=${ticks} y=${y} x0=${m.l} x1=${W - m.r} fmt=${fmt} />
      ${cats.map((c, i) => {
        const cx = m.l + slot * i + slot / 2;
        let acc = 0;
        return html`<g key=${c}>${i % step === 0 ? html`<text x=${cx} y=${H - m.b + 15} text-anchor="middle" class="ct" aria-hidden="true">${truncate(c, 12)}</text>` : null}
          ${series.map((s, si) => {
            const v = s.values[i]; if (v == null) return null;
            const color = s.other ? 'var(--chart-muted)' : SERIES(si);
            let y0; let y1; let xx = cx - th / 2;
            if (isStack) { y0 = y(acc); acc += v; y1 = y(acc); } else { y0 = y(Math.max(0, v)); y1 = y(Math.min(0, v)); xx = cx - (th * nSeries + 2 * (nSeries - 1)) / 2 + si * (th + 2); }
            const h = Math.max(1, y1 - y0 - (isStack && si < nSeries - 1 ? 2 : 0));
            const last = !isStack || si === nSeries - 1 || series.slice(si + 1).every((q) => !q.values[i]);
            const labelFits = !isStack && nSeries === 1 && cats.length <= 12 && slot > 36;
            return html`<g key=${s.name}><path d=${barPath(xx, isStack ? y0 : y0, th, h, last ? 4 : 0, 'v')} style=${`fill:${color}`} class=${'bar' + (hot === `${s.name}|${i}` ? ' hot' : '')} tabindex="0" role="img" aria-label=${`${c}, ${s.name}: ${fmtFull(v)}`}
              onPointerMove=${(e) => hit(s.name, i, e, v, color)} onPointerLeave=${clear} onFocus=${(e) => hit(s.name, i, e, v, color)} onBlur=${clear} />
              ${labelFits ? html`<text x=${xx + th / 2} y=${y0 - 5} text-anchor="middle" class="ct ink strong" aria-hidden="true">${fmt(v)}</text>` : null}</g>`;
          })}</g>`;
      })}</svg>`;
  }
  return html`<${Frame} title=${title} subtitle=${subtitle} height=${height} table=${tableOverride || { columns, rows }} legend=${html`<${Legend} items=${series} />`}>
    <div class="chart-body" ref=${wrap}>${body}<${Tip} tip=${tip} /></div><//>`;
}

/** Bar path with a rounded data end and square baseline. dir 'v' grows up from y+h; 'h' grows right from x. */
function barPath(x, y, w, h, r, dir) {
  r = Math.min(r, w / 2, h / 2);
  if (dir === 'v') return r <= 0 ? `M${x} ${y}h${w}v${h}h${-w}z` : `M${x} ${y + h}V${y + r}a${r} ${r} 0 0 1 ${r} ${-r}h${w - 2 * r}a${r} ${r} 0 0 1 ${r} ${r}V${y + h}z`;
  return r <= 0 ? `M${x} ${y}h${w}v${h}h${-w}z` : `M${x} ${y}H${x + w - r}a${r} ${r} 0 0 1 ${r} ${r}V${y + h - r}a${r} ${r} 0 0 1 ${-r} ${r}H${x}z`;
}

// ---------------------------------------------------------------- line / area
export function LineChart({ columns, rows, view = {}, height = 260, title, subtitle, area }) {
  const wrap = useRef();
  const W = useSize(wrap);
  const sh = useMemo(() => shapeCategory(columns, rows, view), [columns, rows, view.x, view.y, view.series]);
  const [idx, setIdx] = useState(null);
  const [tip, setTip] = useState(null);
  const { cats, series } = sh;
  const fmt = (v) => fmtValue(v, view.format, { compact: true });
  const fmtFull = (v) => fmtValue(v, view.format);
  if (!cats.length || !series.length) return html`<${Frame} title=${title} subtitle=${subtitle} exportable=${false}><${Empty} height=${height} /><//>`;
  const all = series.flatMap((s) => s.values.filter(isNum));
  const lo = Math.min(0, ...all); const hi = Math.max(...all, 1);
  const ticks = niceTicks(lo, hi, 4);
  const m = useAxisBox(fmt(ticks[ticks.length - 1]), height, { r: Math.max(16, series.length <= 4 ? Math.ceil(textW(fmt(hi)) + 22) : 16), t: 14 });
  const H = height;
  const y = linear(ticks[0], ticks[ticks.length - 1], H - m.b, m.t);
  const n = cats.length;
  const x = (i) => m.l + (n === 1 ? (W - m.l - m.r) / 2 : (i / (n - 1)) * (W - m.l - m.r));
  const step = Math.max(1, Math.ceil(textW('x'.repeat(Math.min(10, Math.max(...cats.map((c) => c.length)))), 11) / ((W - m.l - m.r) / Math.max(1, n - 1)) + 0.2));
  const path = (vals) => { let d = ''; let pen = false; vals.forEach((v, i) => { if (!isNum(v)) { pen = false; return; } d += (pen ? 'L' : 'M') + x(i).toFixed(1) + ' ' + y(v).toFixed(1); pen = true; }); return d; };
  const move = (clientX) => {
    const r = wrap.current.getBoundingClientRect();
    const px = clientX - r.left;
    const i = Math.max(0, Math.min(n - 1, Math.round(((px - m.l) / (W - m.l - m.r)) * (n - 1))));
    show(i, r);
  };
  const show = (i, r = wrap.current.getBoundingClientRect()) => {
    setIdx(i);
    setTip({ x: x(i), y: m.t + 8, w: r.width, flip: x(i) > r.width * 0.6, title: cats[i], rows: series.map((s, si) => ({ key: 'line', color: s.other ? 'var(--chart-muted)' : SERIES(si), name: s.name, value: fmtFull(s.values[i]) })) });
  };
  const keyNav = (e) => {
    if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') { e.preventDefault(); show(Math.max(0, Math.min(n - 1, (idx ?? (e.key === 'ArrowRight' ? -1 : n)) + (e.key === 'ArrowRight' ? 1 : -1)))); }
    if (e.key === 'Escape') { setIdx(null); setTip(null); }
  };
  const showEnd = series.length <= 4;
  const body = html`<svg class="chart-svg" width="100%" height=${H} viewBox=${`0 0 ${W} ${H}`} role="group" aria-label=${(title || 'Line chart') + '. Use left and right arrow keys to read values.'} tabindex="0" onKeyDown=${keyNav} onFocus=${() => idx == null && show(n - 1)} onBlur=${() => { setIdx(null); setTip(null); }}
      onPointerMove=${(e) => move(e.clientX)} onPointerLeave=${() => { setIdx(null); setTip(null); }}>
    <${YGrid} ticks=${ticks} y=${y} x0=${m.l} x1=${W - m.r} fmt=${fmt} />
    <g aria-hidden="true">${cats.map((c, i) => (i % step === 0 || i === n - 1 && step > 1 && (n - 1) % step === 0 ? html`<text key=${i} x=${x(i)} y=${H - m.b + 15} text-anchor=${i === 0 && n > 2 ? 'start' : i === n - 1 && n > 2 ? 'end' : 'middle'} class="ct">${truncate(c, 10)}</text>` : null))}</g>
    ${(area || view.area) && series.length === 1 ? html`<path d=${path(series[0].values) + `L${x(n - 1)} ${y(Math.max(0, ticks[0]))}L${x(0)} ${y(Math.max(0, ticks[0]))}Z`} style="fill:var(--series-1)" class="area" />` : null}
    ${series.map((s, si) => html`<path key=${s.name} d=${path(s.values)} class="line" style=${`stroke:${s.other ? 'var(--chart-muted)' : SERIES(si)}`} />`)}
    ${idx != null ? html`<line x1=${x(idx)} x2=${x(idx)} y1=${m.t} y2=${H - m.b} class="crosshair" />` : null}
    ${series.map((s, si) => {
      const lastI = s.values.map(isNum).lastIndexOf(true);
      const color = s.other ? 'var(--chart-muted)' : SERIES(si);
      return html`<g key=${s.name} aria-hidden="true">
        ${lastI >= 0 ? html`<circle cx=${x(lastI)} cy=${y(s.values[lastI])} r="4" class="dot" style=${`fill:${color}`} />` : null}
        ${showEnd && lastI >= 0 ? html`<text x=${x(lastI) + 9} y=${y(s.values[lastI])} dy="0.32em" class="ct ink strong">${fmt(s.values[lastI])}</text>` : null}
        ${idx != null && isNum(s.values[idx]) ? html`<circle cx=${x(idx)} cy=${y(s.values[idx])} r="4.5" class="dot" style=${`fill:${color}`} />` : null}</g>`;
    })}</svg>`;
  return html`<${Frame} title=${title} subtitle=${subtitle} height=${height} table=${{ columns, rows }} legend=${html`<${Legend} items=${series} kind="line" />`}><div class="chart-body" ref=${wrap}>${body}<${Tip} tip=${tip} /></div><//>`;
}

// ---------------------------------------------------------------- donut
export function DonutChart({ columns, rows, view = {}, height = 260, title, subtitle }) {
  const wrap = useRef();
  const W = useSize(wrap);
  const [tip, setTip] = useState(null);
  const [hot, setHot] = useState(null);
  const sh = useMemo(() => shapeCategory(columns, rows, { ...view, series: null }), [columns, rows, view.x, view.y]);
  const vals = sh.cats.map((c, i) => ({ name: c, v: Math.max(0, sh.series[0] ? sh.series[0].values[i] ?? 0 : 0) })).filter((d) => d.v > 0).sort((a, b) => b.v - a.v);
  let slices = vals;
  if (vals.length > 7) { const head = vals.slice(0, 6); head.push({ name: 'Other', v: vals.slice(6).reduce((s, d) => s + d.v, 0), other: true }); slices = head; }
  const total = slices.reduce((s, d) => s + d.v, 0);
  const fmt = (v) => fmtValue(v, view.format);
  if (!total) return html`<${Frame} title=${title} subtitle=${subtitle} exportable=${false}><${Empty} height=${height} /><//>`;
  const size = Math.min(height, Math.max(140, W * 0.5)); const R = size / 2 - 4; const r = R * 0.62;
  const cx = size / 2; const cy = size / 2;
  let a0 = -Math.PI / 2;
  const arcs = slices.map((d, i) => {
    const frac = d.v / total; const gap = slices.length > 1 ? 0.012 : 0;
    const s = a0 + gap; const e = a0 + frac * Math.PI * 2 - gap; a0 += frac * Math.PI * 2;
    const large = e - s > Math.PI ? 1 : 0;
    const p = (rad, a) => [cx + rad * Math.cos(a), cy + rad * Math.sin(a)];
    const [x0, y0] = p(R, s); const [x1, y1] = p(R, e); const [x2, y2] = p(r, e); const [x3, y3] = p(r, s);
    return { ...d, i, frac, d: `M${x0} ${y0}A${R} ${R} 0 ${large} 1 ${x1} ${y1}L${x2} ${y2}A${r} ${r} 0 ${large} 0 ${x3} ${y3}Z` };
  });
  const show = (a, ev) => { const rect = wrap.current.getBoundingClientRect(); const bx = ev.target.getBoundingClientRect(); setHot(a.i); setTip({ x: (ev.type === 'focus' ? bx.left + bx.width / 2 : ev.clientX) - rect.left, y: (ev.type === 'focus' ? bx.top : ev.clientY) - rect.top, w: rect.width, flip: false, title: a.name, rows: [{ key: 'rect', color: a.other ? 'var(--chart-muted)' : SERIES(a.i), name: `${(a.frac * 100).toFixed(1)}% of total`, value: fmt(a.v) }] }); };
  const body = html`<div class="donut-wrap"><svg class="chart-svg" width=${size} height=${size} viewBox=${`0 0 ${size} ${size}`} role="img" aria-label=${(title || 'Donut chart') + ': ' + slices.map((s) => `${s.name} ${(s.v / total * 100).toFixed(0)}%`).join(', ')}>
    ${arcs.map((a) => html`<path key=${a.name} d=${a.d} class=${'slice' + (hot === a.i ? ' hot' : '')} style=${`fill:${a.other ? 'var(--chart-muted)' : SERIES(a.i)}`} tabindex="0" role="img" aria-label=${`${a.name}: ${fmt(a.v)}, ${(a.frac * 100).toFixed(1)}%`} onPointerMove=${(e) => show(a, e)} onPointerLeave=${() => { setTip(null); setHot(null); }} onFocus=${(e) => show(a, e)} onBlur=${() => { setTip(null); setHot(null); }} />`)}
    <text x=${cx} y=${cy - 4} text-anchor="middle" class="donut-total">${fmtValue(total, view.format, { compact: true })}</text><text x=${cx} y=${cy + 14} text-anchor="middle" class="ct">total</text></svg>
    <ul class="donut-legend" aria-label="Legend">${arcs.map((a) => html`<li key=${a.name}><span class="key rect" style=${`--k:${a.other ? 'var(--chart-muted)' : SERIES(a.i)}`}></span><span class="grow trunc">${a.name}</span><b>${(a.frac * 100).toFixed(a.frac < 0.1 ? 1 : 0)}%</b></li>`)}</ul></div>`;
  return html`<${Frame} title=${title} subtitle=${subtitle} height=${height} table=${{ columns, rows }}><div class="chart-body" ref=${wrap}>${body}<${Tip} tip=${tip} /></div><//>`;
}

// ---------------------------------------------------------------- heatmap (sequential single hue)
export function Heatmap({ columns, rows, view = {}, height = 260, title, subtitle }) {
  const wrap = useRef();
  const W = useSize(wrap);
  const [tip, setTip] = useState(null);
  const cols = columns.map((c) => (typeof c === 'string' ? { name: c } : c));
  const xi = Math.max(0, colIndex(cols, view.x)); const yi = Math.max(0, colIndex(cols, view.y));
  let vi = view.series ? colIndex(cols, view.series) : -1;
  if (vi < 0) vi = cols.findIndex((c, i) => i !== xi && i !== yi && colIsNumeric(c, rows, i));
  const xs = []; const ys = []; const map = new Map();
  for (const r of rows) { const a = String(cell(r, cols[xi], xi)); const b = String(cell(r, cols[yi], yi)); if (!xs.includes(a)) xs.push(a); if (!ys.includes(b)) ys.push(b); map.set(a + '\u0000' + b, toNum(cell(r, cols[vi], vi))); }
  const vals = [...map.values()].filter(isNum);
  if (!vals.length) return html`<${Frame} title=${title} subtitle=${subtitle} exportable=${false}><${Empty} height=${height} /><//>`;
  const lo = Math.min(...vals); const hi = Math.max(...vals);
  const ml = Math.ceil(textW('x'.repeat(Math.min(14, Math.max(...ys.map((s) => s.length))))) + 12);
  const mt = 24; const mb = 34;
  const cw = (W - ml - 8) / xs.length; const ch = Math.min(40, (height - mt - mb) / ys.length);
  const H = mt + ch * ys.length + mb;
  const fmt = (v) => fmtValue(v, view.format, { compact: true });
  const rampVar = (t) => `var(--seq-${Math.min(7, Math.max(1, Math.round(1 + t * 6)))})`;
  const swatchRgb = (t) => { const el = wrap.current; if (!el) return 'rgb(60,60,60)'; return getComputedStyle(el).getPropertyValue(`--seq-${Math.min(7, Math.max(1, Math.round(1 + t * 6)))}`); };
  const hexToRgb = (h) => { const m = /#?([0-9a-f]{6})/i.exec(h.trim()); if (!m) return 'rgb(128,128,128)'; const n = parseInt(m[1], 16); return `rgb(${n >> 16},${(n >> 8) & 255},${n & 255})`; };
  const show = (a, b, v, ev) => { const rect = wrap.current.getBoundingClientRect(); const bx = ev.target.getBoundingClientRect(); setTip({ x: bx.left + bx.width / 2 - rect.left, y: bx.top - rect.top, w: rect.width, flip: bx.left - rect.left > rect.width * 0.6, title: `${b} · ${a}`, rows: [{ key: 'rect', color: rampVar((v - lo) / (hi - lo || 1)), name: cols[vi].name, value: fmtValue(v, view.format) }] }); };
  const body = html`<svg class="chart-svg" width="100%" height=${H} viewBox=${`0 0 ${W} ${H}`} role="img" aria-label=${(title || 'Heatmap') + `. ${ys.length} rows by ${xs.length} columns.`}>
    ${xs.map((a, i) => html`<text key=${a} x=${ml + cw * i + cw / 2} y=${mt - 8} text-anchor="middle" class="ct" aria-hidden="true">${truncate(a, Math.max(3, Math.floor(cw / 6.6)))}</text>`)}
    ${ys.map((b, j) => html`<g key=${b}><text x=${ml - 8} y=${mt + ch * j + ch / 2} dy="0.32em" text-anchor="end" class="ct ink" aria-hidden="true">${truncate(b, 14)}</text>
      ${xs.map((a, i) => { const v = map.get(a + '\u0000' + b); if (!isNum(v)) return html`<rect key=${a} x=${ml + cw * i + 1} y=${mt + ch * j + 1} width=${cw - 2} height=${ch - 2} rx="3" class="hm-empty" />`; const t = (v - lo) / (hi - lo || 1);
        return html`<g key=${a}><rect x=${ml + cw * i + 1} y=${mt + ch * j + 1} width=${cw - 2} height=${ch - 2} rx="3" class="hm-cell" style=${`fill:${rampVar(t)}`} tabindex="0" role="img" aria-label=${`${b}, ${a}: ${fmtValue(v, view.format)}`} onPointerMove=${(e) => show(a, b, v, e)} onPointerLeave=${() => setTip(null)} onFocus=${(e) => show(a, b, v, e)} onBlur=${() => setTip(null)} />
          ${cw > 52 && ch > 22 ? html`<text x=${ml + cw * i + cw / 2} y=${mt + ch * j + ch / 2} dy="0.32em" text-anchor="middle" class="ct" style=${`fill:${inkFor(hexToRgb(swatchRgb(t)))}`} aria-hidden="true" pointer-events="none">${fmt(v)}</text>` : null}</g>`; })}</g>`)}
    <g aria-hidden="true" transform=${`translate(${ml},${H - mb + 14})`}><text x="0" y="9" class="ct">${fmt(lo)}</text>${[1, 2, 3, 4, 5, 6, 7].map((k) => html`<rect key=${k} x=${34 + (k - 1) * 22} y="0" width="21" height="10" rx="2" style=${`fill:var(--seq-${k})`} />`)}<text x=${34 + 7 * 22 + 6} y="9" class="ct">${fmt(hi)}</text></g></svg>`;
  return html`<${Frame} title=${title} subtitle=${subtitle} height=${height} table=${{ columns, rows }}><div class="chart-body" ref=${wrap}>${body}<${Tip} tip=${tip} /></div><//>`;
}

// ---------------------------------------------------------------- scatter (nearest-point hit testing, <= 3 colour groups + Other)
export function ScatterChart({ columns, rows, view = {}, height = 260, title, subtitle }) {
  const wrap = useRef();
  const W = useSize(wrap);
  const [tip, setTip] = useState(null);
  const [hot, setHot] = useState(null);
  const cols = columns.map((c) => (typeof c === 'string' ? { name: c } : c));
  const nums = cols.map((c, i) => colIsNumeric(c, rows, i));
  let xi = view.x ? colIndex(cols, view.x) : nums.indexOf(true);
  let yi = view.y ? colIndex(cols, view.y) : nums.findIndex((n, i) => n && i !== xi);
  const gi = view.series ? colIndex(cols, view.series) : -1;
  const pts = rows.map((r) => ({ x: toNum(cell(r, cols[xi], xi)), y: toNum(cell(r, cols[yi], yi)), g: gi >= 0 ? String(cell(r, cols[gi], gi)) : null })).filter((p) => isNum(p.x) && isNum(p.y));
  if (!pts.length) return html`<${Frame} title=${title} subtitle=${subtitle} exportable=${false}><${Empty} height=${height} /><//>`;
  const groups = [...new Set(pts.map((p) => p.g))];
  const top3 = groups.length > 3 ? groups.slice(0, 3) : groups;
  const gIdx = (g) => { const i = top3.indexOf(g); return i < 0 ? -1 : i; };
  const xt = niceTicks(Math.min(...pts.map((p) => p.x)), Math.max(...pts.map((p) => p.x)), 5);
  const yt = niceTicks(Math.min(0, ...pts.map((p) => p.y)), Math.max(...pts.map((p) => p.y)), 4);
  const m = useAxisBox(fmtValue(yt[yt.length - 1], view.format, { compact: true }), height, { t: 12, b: 34 });
  const H = height;
  const x = linear(xt[0], xt[xt.length - 1], m.l, W - m.r); const y = linear(yt[0], yt[yt.length - 1], H - m.b, m.t);
  const near = (cx, cy) => { let best = null; let bd = 24 * 24; for (const p of pts) { const dx = x(p.x) - cx; const dy = y(p.y) - cy; const d = dx * dx + dy * dy; if (d < bd) { bd = d; best = p; } } return best; };
  const onMove = (e) => { const r = wrap.current.getBoundingClientRect(); const p = near(e.clientX - r.left, e.clientY - r.top); if (!p) { setTip(null); setHot(null); return; } setHot(p); setTip({ x: x(p.x), y: y(p.y), w: r.width, flip: x(p.x) > r.width * 0.6, title: p.g, rows: [{ key: 'rect', color: gIdx(p.g) < 0 ? 'var(--chart-muted)' : SERIES(gIdx(p.g)), name: cols[xi].name, value: fmtValue(p.x, view.xformat) }, { key: 'rect', color: gIdx(p.g) < 0 ? 'var(--chart-muted)' : SERIES(gIdx(p.g)), name: cols[yi].name, value: fmtValue(p.y, view.format) }] }); };
  const body = html`<svg class="chart-svg" width="100%" height=${H} viewBox=${`0 0 ${W} ${H}`} role="img" aria-label=${(title || 'Scatter plot') + `: ${pts.length} points of ${cols[yi].name} against ${cols[xi].name}`} onPointerMove=${onMove} onPointerLeave=${() => { setTip(null); setHot(null); }}>
    <${YGrid} ticks=${yt} y=${y} x0=${m.l} x1=${W - m.r} fmt=${(v) => fmtValue(v, view.format, { compact: true })} />
    <g aria-hidden="true">${xt.map((t) => html`<text key=${t} x=${x(t)} y=${H - m.b + 15} text-anchor="middle" class="ct">${fmtCompact(t)}</text>`)}<text x=${(m.l + W - m.r) / 2} y=${H - 4} text-anchor="middle" class="ct ink">${cols[xi].name}</text></g>
    ${pts.slice(0, 5000).map((p, i) => html`<circle key=${i} cx=${x(p.x)} cy=${y(p.y)} r=${hot === p ? 6 : 4} class="pt" style=${`fill:${gIdx(p.g) < 0 ? 'var(--chart-muted)' : SERIES(gIdx(p.g))}`} />`)}</svg>`;
  return html`<${Frame} title=${title} subtitle=${subtitle} height=${height} table=${{ columns, rows }} legend=${html`<${Legend} items=${top3.map((g) => ({ name: g || cols[yi].name }))} />`}><div class="chart-body" ref=${wrap}>${body}<${Tip} tip=${tip} /></div><//>`;
}

// ---------------------------------------------------------------- KPI tile
export function KpiTile({ columns, rows, view = {}, title, subtitle, compact }) {
  const cols = columns.map((c) => (typeof c === 'string' ? { name: c } : c));
  const first = rows[0];
  const nums = cols.map((c, i) => colIsNumeric(c, rows, i));
  const vi = Math.max(0, nums.indexOf(true));
  const raw = first ? cell(first, cols[vi], vi) : null;
  const deltaCol = cols.findIndex((c) => /^(delta|change|diff)(_pct)?$/i.test(c.name));
  const priorCol = cols.findIndex((c) => /^(prior|previous|prev)/i.test(c.name));
  let delta = null;
  if (deltaCol >= 0 && first) delta = toNum(cell(first, cols[deltaCol], deltaCol));
  else if (priorCol >= 0 && first) { const p = toNum(cell(first, cols[priorCol], priorCol)); const c = toNum(raw); if (p && c != null) delta = ((c - p) / Math.abs(p)) * 100; }
  const trend = rows.length > 2 ? rows.map((r) => toNum(cell(r, cols[vi], vi))).filter(isNum) : null;
  const value = rows.length > 2 && trend ? trend[trend.length - 1] : raw;
  const up = delta != null && delta >= 0;
  return html`<figure class=${'kpi' + (compact ? ' compact' : '')} aria-label=${`${title}: ${fmtValue(value, view.format)}`}>
    <figcaption class="kpi-label">${title}${subtitle ? html` <span class="muted">${subtitle}</span>` : null}</figcaption>
    <div class="kpi-value">${fmtValue(value, view.format, { compact: Math.abs(toNum(value) || 0) >= 1e7 })}</div>
    ${delta != null ? html`<div class=${'kpi-delta ' + (up ? 'up' : 'down')}><${Icon} name=${up ? 'arrow-up' : 'arrow-down'} size="sm" /><span>${Math.abs(delta).toFixed(1)}%</span><span class="muted"> vs prior</span></div>` : null}
    ${trend && trend.length > 2 ? html`<${Spark} values=${trend} />` : null}</figure>`;
}
export function Spark({ values, height = 28 }) {
  const lo = Math.min(...values); const hi = Math.max(...values);
  const W = 120;
  const pts = values.map((v, i) => [(i / (values.length - 1)) * W, height - 3 - ((v - lo) / (hi - lo || 1)) * (height - 8)]);
  const d = pts.map((p, i) => (i ? 'L' : 'M') + p[0].toFixed(1) + ' ' + p[1].toFixed(1)).join('');
  const last = pts[pts.length - 1];
  return html`<svg class="kpi-spark" viewBox=${`0 0 ${W + 8} ${height}`} preserveAspectRatio="none" aria-hidden="true"><path d=${d} class="line" /><circle cx=${last[0]} cy=${last[1]} r="3" class="dot" /></svg>`;
}

// ---------------------------------------------------------------- table tile with in-cell bars
export function TableTile({ columns, rows, view = {}, height = 260, title, subtitle }) {
  const cols = columns.map((c) => (typeof c === 'string' ? { name: c } : c));
  const nums = cols.map((c, i) => colIsNumeric(c, rows, i));
  const barCol = view.y ? colIndex(cols, view.y) : (() => { let best = -1; let bv = -1; cols.forEach((c, i) => { if (!nums[i]) return; const mx = Math.max(...rows.map((r) => Math.abs(toNum(cell(r, c, i)) || 0))); if (/revenue|amount|total|sales|count|units/i.test(c.name) && mx >= bv) { best = i; bv = mx; } }); return best; })();
  const maxBar = barCol >= 0 ? Math.max(...rows.map((r) => Math.abs(toNum(cell(r, cols[barCol], barCol)) || 0)), 1) : 1;
  const fmtCell = (v, c, i) => (v == null ? '–' : nums[i] ? fmtValue(v, i === barCol ? view.format : /pct|percent|margin|rate/i.test(c.name) ? 'percent' : /revenue|amount|price|cost|profit/i.test(c.name) ? 'currency' : 'integer') : String(v));
  return html`<${Frame} title=${title} subtitle=${subtitle} height=${height} table=${{ columns, rows }} exportable=${true}>
    <div class="chart-table" style=${`max-height:${height}px`} tabindex="0"><table class="table dense"><thead><tr>${cols.map((c, i) => html`<th key=${i} scope="col" class=${nums[i] ? 'num' : ''}>${c.name}</th>`)}</tr></thead>
    <tbody>${rows.map((r, ri) => html`<tr key=${ri}>${cols.map((c, i) => { const v = cell(r, c, i); return html`<td key=${i} class=${nums[i] ? 'num' : ''}>${i === barCol ? html`<span class="cellbar"><i style=${`width:${Math.max(1, (Math.abs(toNum(v) || 0) / maxBar) * 100)}%`}></i><span>${fmtCell(v, c, i)}</span></span>` : fmtCell(v, c, i)}</td>`; })}</tr>`)}</tbody></table></div><//>`;
}

/** Dispatch by tile type. */
export function Chart({ type, ...p }) {
  switch (type) {
    case 'kpi': return html`<${KpiTile} ...${p} />`;
    case 'bar': case 'column': return html`<${BarChart} ...${p} />`;
    case 'stacked': return html`<${BarChart} ...${p} stacked=${true} />`;
    case 'line': return html`<${LineChart} ...${p} />`;
    case 'area': return html`<${LineChart} ...${p} area=${true} />`;
    case 'donut': case 'pie': return html`<${DonutChart} ...${p} />`;
    case 'heatmap': return html`<${Heatmap} ...${p} />`;
    case 'scatter': return html`<${ScatterChart} ...${p} />`;
    case 'table': return html`<${TableTile} ...${p} />`;
    default: return html`<${TableTile} ...${p} />`;
  }
}

/** Compact stat tile for dashboards of platform metrics: label, value, delta, sparkline. */
export function StatTile({ label, value, sub, delta, spark, state, icon, onClick, href }) {
  const body = html`<div class="stat-top"><span class="stat-label">${label}</span>${state ? html`<span class=${'light ' + state} aria-hidden="true"></span>` : null}</div><div class="stat-value">${value}</div>${sub ? html`<div class="stat-sub">${sub}</div>` : null}${delta != null ? html`<div class=${'kpi-delta ' + (delta >= 0 ? 'up' : 'down')}>${delta >= 0 ? '+' : ''}${delta}</div>` : null}${spark && spark.length > 2 ? html`<${Spark} values=${spark} />` : null}`;
  return href ? html`<a class="stat" href=${href}>${body}</a>` : html`<div class="stat" onClick=${onClick}>${body}</div>`;
}
