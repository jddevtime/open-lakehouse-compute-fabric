// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Number formatting and axis maths for the chart library (no dependencies).
import { fmtCompact, fmtBytes, fmtNum, fmtPct } from '../core/util.js';

/** Format a value according to a tile `format` hint: currency | integer | percent | decimal | bytes | duration | compact | text. */
export function fmtValue(v, format, { compact = false } = {}) {
  if (v == null || (typeof v === 'number' && Number.isNaN(v))) return '–';
  if (typeof v !== 'number') { const n = Number(v); if (Number.isNaN(n) || v === '' || typeof v === 'boolean') return String(v); v = n; }
  switch (format) {
    case 'currency': return compact || Math.abs(v) >= 1e6 ? '$' + fmtCompact(v) : '$' + fmtNum(v, Math.abs(v) < 1000 ? 2 : 0);
    case 'integer': return compact ? fmtCompact(Math.round(v)) : fmtNum(Math.round(v));
    case 'percent': return fmtPct(v, Math.abs(v) < 10 ? 1 : 0);
    case 'bytes': return fmtBytes(v);
    case 'duration': return v < 1000 ? Math.round(v) + ' ms' : (v / 1000).toFixed(1) + ' s';
    case 'decimal': return fmtNum(v, 2);
    case 'compact': return fmtCompact(v);
    default: return compact ? fmtCompact(v) : Math.abs(v) >= 1000 ? fmtNum(v, 0) : fmtNum(v, Math.abs(v) < 10 ? 2 : 1);
  }
}

/** "Nice" tick values covering [lo, hi] with about `count` steps. */
export function niceTicks(lo, hi, count = 5) {
  if (!Number.isFinite(lo) || !Number.isFinite(hi)) return [0, 1];
  if (lo === hi) { if (lo === 0) return [0, 1]; hi = lo + Math.abs(lo) * 0.5; lo = Math.min(0, lo); }
  const span = hi - lo;
  const raw = span / Math.max(1, count);
  const mag = 10 ** Math.floor(Math.log10(raw));
  const norm = raw / mag;
  const step = (norm >= 5 ? 10 : norm >= 2 ? 5 : norm >= 1 ? 2 : 1) * mag;
  const start = Math.floor(lo / step) * step;
  const end = Math.ceil(hi / step) * step;
  const out = [];
  for (let v = start; v <= end + step * 1e-6; v += step) out.push(Math.abs(v) < step * 1e-9 ? 0 : +v.toPrecision(12));
  return out;
}

export function linear(d0, d1, r0, r1) {
  const k = (r1 - r0) / (d1 - d0 || 1);
  const f = (v) => r0 + (v - d0) * k;
  f.invert = (p) => d0 + (p - r0) / (k || 1);
  return f;
}

export const isNum = (v) => typeof v === 'number' && Number.isFinite(v);
export function toNum(v) { if (isNum(v)) return v; if (v == null || v === '') return null; const n = Number(v); return Number.isFinite(n) ? n : null; }

/** Is this column numeric? Uses the server type when given, else samples values. */
export function colIsNumeric(col, rows, idx) {
  if (col && col.type && /int|long|short|byte|float|double|decimal|numeric|real|bigint/i.test(col.type)) return true;
  if (col && col.kind && /int|float|decimal|number/i.test(col.kind)) return true;
  let seen = 0;
  for (const r of rows.slice(0, 30)) { const v = Array.isArray(r) ? r[idx] : r[col.name]; if (v == null) continue; seen++; if (!isNum(v)) return false; }
  return seen > 0;
}

/** Choose black/white text for a label drawn on a colour fill, by relative luminance of its hex/rgb value. */
export function inkFor(rgb) {
  const m = /rgba?\((\d+)[ ,]+(\d+)[ ,]+(\d+)/.exec(rgb) || [];
  const [r, g, b] = [m[1] || 0, m[2] || 0, m[3] || 0].map((x) => { const c = Number(x) / 255; return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4; });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b > 0.38 ? '#0b0b0b' : '#ffffff';
}

/** Estimate rendered text width in the 11px chart font. */
export const textW = (s, px = 11) => String(s).length * px * 0.58;
export function truncate(s, max) { s = String(s ?? ''); return s.length > max ? s.slice(0, Math.max(1, max - 1)) + '…' : s; }
