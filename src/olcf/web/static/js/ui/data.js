// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Data components: sortable table, virtualised grid, JSON viewer, load-more pager.
import { html, useState, useMemo, useRef, useEffect, useLayoutEffect } from '../core/h.js';
import { Icon } from './icons.js';
import { Btn, Empty } from './basic.js';
import { fmtNum } from '../core/util.js';

/** Sortable table. columns: [{key, label, render(row), sort(row), num, width, mono}] */
export function Table({ columns, rows, onRowClick, rowKey = (r) => r.id, empty, dense, caption, initialSort, selected, rowProps }) {
  const [sort, setSort] = useState(initialSort || null);
  const sorted = useMemo(() => {
    if (!sort) return rows;
    const c = columns.find((x) => x.key === sort.key);
    if (!c) return rows;
    const f = c.sort || ((r) => r[c.key]);
    return [...rows].sort((a, b) => { const x = f(a); const y = f(b); return (x == null ? 1 : y == null ? -1 : x > y ? 1 : x < y ? -1 : 0) * sort.dir; });
  }, [rows, sort]);
  if (!rows.length && empty) return empty;
  return html`<div class="table-wrap"><table class=${'table' + (dense ? ' dense' : '')}>${caption ? html`<caption class="sr-only">${caption}</caption>` : null}
    <thead><tr>${columns.map((c) => html`<th key=${c.key} scope="col" class=${c.num ? 'num' : ''} style=${c.width ? `width:${c.width}` : undefined} aria-sort=${c.sortable === false ? undefined : sort && sort.key === c.key ? (sort.dir > 0 ? 'ascending' : 'descending') : 'none'}
      onClick=${c.sortable === false ? undefined : () => setSort((s) => (s && s.key === c.key ? (s.dir > 0 ? { key: c.key, dir: -1 } : null) : { key: c.key, dir: 1 }))}>${c.label}${c.sortable === false ? null : html`<span class="sort">${sort && sort.key === c.key ? (sort.dir > 0 ? '▲' : '▼') : ''}</span>`}</th>`)}</tr></thead>
    <tbody>${sorted.map((r) => html`<tr key=${rowKey(r)} class=${selected && selected(r) ? 'sel' : ''} data-click=${onRowClick ? '1' : undefined} tabindex=${onRowClick ? 0 : undefined}
      onClick=${onRowClick ? (e) => { if (!e.target.closest('a,button,input,select,label')) onRowClick(r); } : undefined}
      onKeyDown=${onRowClick ? (e) => { if (e.key === 'Enter' && e.target === e.currentTarget) onRowClick(r); } : undefined} ...${rowProps ? rowProps(r) : {}}>${columns.map((c) => html`<td key=${c.key} class=${(c.num ? 'num ' : '') + (c.mono ? 'mono' : '')}>${c.render ? c.render(r) : r[c.key]}</td>`)}</tr>`)}</tbody></table></div>`;
}

/**
 * Virtualised data grid for large result sets: only visible rows are in the DOM.
 * columns: [{name, type?}], rows: array of arrays or objects. Supports column resize and cell copy.
 */
export function VirtualGrid({ columns, rows, rowHeight = 28, height = 420, onCopy, formatCell, caption }) {
  const [top, setTop] = useState(0);
  const [widths, setWidths] = useState({});
  const box = useRef();
  const cols = columns.map((c) => (typeof c === 'string' ? { name: c } : c));
  const total = rows.length;
  const vh = height;
  const start = Math.max(0, Math.floor(top / rowHeight) - 6);
  const end = Math.min(total, Math.ceil((top + vh) / rowHeight) + 6);
  const w = (c, i) => widths[i] ?? c.width ?? (isNumeric(c) ? 140 : 170);
  const totalW = cols.reduce((s, c, i) => s + w(c, i), 52);
  const cell = (r, c, i) => {
    const v = Array.isArray(r) ? r[i] : r[c.name];
    return formatCell ? formatCell(v, c) : v == null ? null : typeof v === 'object' ? JSON.stringify(v) : String(v);
  };
  const startResize = (e, i) => {
    e.preventDefault(); e.stopPropagation();
    const x0 = e.clientX; const w0 = w(cols[i], i);
    const mv = (ev) => setWidths((s) => ({ ...s, [i]: Math.max(48, w0 + ev.clientX - x0) }));
    const up = () => { document.removeEventListener('mousemove', mv); document.removeEventListener('mouseup', up); };
    document.addEventListener('mousemove', mv); document.addEventListener('mouseup', up);
  };
  return html`<div class="vgrid" role="grid" aria-label=${caption || 'Results'} aria-rowcount=${total + 1} aria-colcount=${cols.length + 1} style=${`height:${Math.min(vh, (total + 1) * rowHeight + 34)}px`}>
    <div class="vgrid-scroll" ref=${box} onScroll=${(e) => setTop(e.target.scrollTop)} tabindex="0">
      <div style=${`width:${totalW}px;height:${(total + 1) * rowHeight}px;position:relative`}>
        <div class="vgrid-head" role="row" style=${`height:${rowHeight}px;width:${totalW}px`}>
          <div class="vg-cell vg-idx" role="columnheader" style="width:52px"></div>
          ${cols.map((c, i) => html`<div class="vg-cell" role="columnheader" key=${i} style=${`width:${w(c, i)}px`} title=${c.type ? `${c.name} (${c.type})` : c.name}><span class="trunc">${c.name}</span>${c.type ? html`<span class="vg-type">${c.type}</span>` : null}<span class="vg-resize" onMouseDown=${(e) => startResize(e, i)} aria-hidden="true"></span></div>`)}
        </div>
        <div style=${`position:absolute;top:${(start + 1) * rowHeight}px;left:0;width:${totalW}px`}>
          ${rows.slice(start, end).map((r, k) => html`<div class="vg-row" role="row" aria-rowindex=${start + k + 2} key=${start + k} style=${`height:${rowHeight}px`}>
            <div class="vg-cell vg-idx" role="gridcell" style="width:52px">${start + k + 1}</div>
            ${cols.map((c, i) => { const v = cell(r, c, i); return html`<div class=${'vg-cell' + (isNumeric(c) ? ' num' : '') + (v == null ? ' null' : '')} role="gridcell" key=${i} style=${`width:${w(c, i)}px`} title=${v ?? ''} onDblClick=${() => onCopy && onCopy(v)}><span class="trunc">${v == null ? 'null' : v}</span></div>`; })}</div>`)}
        </div>
      </div>
    </div>
    <div class="vgrid-foot">${fmtNum(total)} row${total === 1 ? '' : 's'}</div></div>`;
}
const NUM_RE = /int|long|short|byte|float|double|decimal|numeric|number|bigint|real|hugeint/i;
function isNumeric(c) { return c.numeric ?? (c.type ? NUM_RE.test(c.type) : false); }

/** Collapsible JSON tree for diagnostics payloads. Never uses innerHTML. */
export function JsonView({ value, depth = 2, name }) {
  return html`<div class="jsonv" role="tree"><${JNode} k=${name} v=${value} d=${0} open=${depth} /></div>`;
}
function JNode({ k, v, d, open }) {
  const isObj = v && typeof v === 'object';
  const [o, setO] = useState(d < open);
  const label = k != null ? html`<span class="jk">${k}</span><span class="jp">: </span>` : null;
  if (!isObj) return html`<div class="jl" role="treeitem">${label}<span class=${'jv ' + (v === null ? 'jnull' : typeof v)}>${typeof v === 'string' ? JSON.stringify(v) : String(v)}</span></div>`;
  const entries = Array.isArray(v) ? v.map((x, i) => [i, x]) : Object.entries(v);
  const br = Array.isArray(v) ? ['[', ']'] : ['{', '}'];
  return html`<div role="treeitem" aria-expanded=${o}><div class="jl jtog" onClick=${() => setO(!o)} tabindex="0" onKeyDown=${(e) => (e.key === 'Enter' || e.key === ' ') && (e.preventDefault(), setO(!o))}><${Icon} name=${o ? 'chevron-down' : 'chevron-right'} size="sm" />${label}<span class="jp">${br[0]}${o ? '' : entries.length ? ` ${entries.length} ${Array.isArray(v) ? 'items' : 'keys'} ` + br[1] : br[1]}</span></div>
    ${o ? html`<div class="jchildren">${entries.map(([ek, ev]) => html`<${JNode} key=${ek} k=${ek} v=${ev} d=${d + 1} open=${open} />`)}<div class="jl jp">${br[1]}</div></div>` : null}</div>`;
}

export function LoadMore({ cursor, loading, onMore }) {
  if (!cursor) return null;
  return html`<div class="center mt-3"><${Btn} busy=${loading} onClick=${onMore}>Load more<//></div>`;
}
