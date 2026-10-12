// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Formatting, escaping, clipboard, download and misc helpers shared by every page.

export const enc = encodeURIComponent;
export const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

export function uid(prefix = 'id') { return prefix + '-' + Math.random().toString(36).slice(2, 10); }
export function hex(n = 16) {
  const a = new Uint8Array(n / 2);
  (globalThis.crypto || window.crypto).getRandomValues(a);
  return Array.from(a, (b) => b.toString(16).padStart(2, '0')).join('');
}

export function fmtNum(n, digits = 0) {
  if (n == null || Number.isNaN(n)) return '–';
  return Number(n).toLocaleString('en-US', { maximumFractionDigits: digits, minimumFractionDigits: 0 });
}
export function fmtCompact(n) {
  if (n == null || Number.isNaN(n)) return '–';
  const a = Math.abs(n);
  if (a >= 1e9) return (n / 1e9).toFixed(a >= 1e10 ? 0 : 1).replace(/\.0$/, '') + 'B';
  if (a >= 1e6) return (n / 1e6).toFixed(a >= 1e7 ? 0 : 1).replace(/\.0$/, '') + 'M';
  if (a >= 1e3) return (n / 1e3).toFixed(a >= 1e4 ? 0 : 1).replace(/\.0$/, '') + 'k';
  return Number.isInteger(n) ? String(n) : n.toFixed(1).replace(/\.0$/, '');
}
export function fmtBytes(n) {
  if (n == null || Number.isNaN(n)) return '–';
  const u = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  let i = 0;
  let v = Number(n);
  while (Math.abs(v) >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return (i === 0 ? v.toFixed(0) : v >= 100 ? v.toFixed(0) : v.toFixed(1)) + ' ' + u[i];
}
export function fmtMb(mb) { return mb == null ? '–' : fmtBytes(mb * 1024 * 1024); }
export function fmtPct(n, d = 0) { return n == null || Number.isNaN(n) ? '–' : Number(n).toFixed(d) + '%'; }
export function fmtMs(ms) {
  if (ms == null) return '–';
  if (ms < 1) return '<1 ms';
  if (ms < 1000) return Math.round(ms) + ' ms';
  if (ms < 60000) return (ms / 1000).toFixed(ms < 10000 ? 2 : 1) + ' s';
  return fmtDuration(ms / 1000);
}
export function fmtDuration(sec) {
  if (sec == null || Number.isNaN(sec)) return '–';
  sec = Math.max(0, Math.round(sec));
  if (sec < 60) return sec + 's';
  const m = Math.floor(sec / 60);
  if (m < 60) return m + 'm ' + String(sec % 60).padStart(2, '0') + 's';
  const h = Math.floor(m / 60);
  if (h < 48) return h + 'h ' + String(m % 60).padStart(2, '0') + 'm';
  return Math.floor(h / 24) + 'd ' + (h % 24) + 'h';
}
export function parseTs(ts) { if (!ts) return null; const d = new Date(ts); return Number.isNaN(d.getTime()) ? null : d; }
export function ago(ts, now = Date.now()) {
  const d = parseTs(ts);
  if (!d) return '–';
  const s = Math.round((now - d.getTime()) / 1000);
  if (s < -5) return 'in ' + fmtDuration(-s);
  if (s < 5) return 'just now';
  if (s < 60) return s + 's ago';
  if (s < 3600) return Math.floor(s / 60) + 'm ago';
  if (s < 86400) return Math.floor(s / 3600) + 'h ago';
  if (s < 86400 * 14) return Math.floor(s / 86400) + 'd ago';
  return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
}
export function fmtTime(ts) {
  const d = parseTs(ts);
  return d ? d.toLocaleString('en-GB', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }) : '–';
}
export function fmtDate(ts) { const d = parseTs(ts); return d ? d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' }) : '–'; }
export function fmtClock(ts) { const d = parseTs(ts); return d ? d.toLocaleTimeString('en-GB', { hour12: false }) : '–'; }
export function durationBetween(a, b) {
  const x = parseTs(a); const y = parseTs(b) || new Date();
  return x ? (y - x) / 1000 : null;
}
export function plural(n, one, many) { return `${fmtNum(n)} ${n === 1 ? one : many || one + 's'}`; }
export function shortId(id, n = 8) { if (!id) return '–'; const s = String(id); const i = s.indexOf('_'); return i > 0 ? s.slice(0, i + 1) + s.slice(i + 1, i + 1 + n) : s.slice(0, n); }
export function titleCase(s) { return String(s ?? '').replace(/[_-]+/g, ' ').replace(/^\w/, (c) => c.toUpperCase()); }

export function debounce(fn, ms = 200) { let t; const f = (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; f.cancel = () => clearTimeout(t); return f; }
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export function clamp(v, lo, hi) { return Math.min(hi, Math.max(lo, v)); }
export const sum = (a) => a.reduce((x, y) => x + (Number(y) || 0), 0);
export const uniq = (a) => [...new Set(a)];
export function groupBy(arr, fn) { const m = {}; for (const x of arr) (m[fn(x)] ||= []).push(x); return m; }
export function sortBy(arr, fn, dir = 1) { return [...arr].sort((a, b) => { const x = fn(a); const y = fn(b); return (x > y ? 1 : x < y ? -1 : 0) * dir; }); }
export function isObj(v) { return v && typeof v === 'object' && !Array.isArray(v); }
export function get(o, path, d) { let c = o; for (const k of String(path).split('.')) { if (c == null) return d; c = c[k]; } return c ?? d; }
export function clone(o) { return o == null ? o : JSON.parse(JSON.stringify(o)); }
export function jsonPretty(o) { try { return JSON.stringify(o, null, 2); } catch { return String(o); } }

export async function copyText(text) {
  try {
    if (navigator.clipboard && window.isSecureContext) { await navigator.clipboard.writeText(text); return true; }
  } catch { /* fall through to the textarea fallback */ }
  try {
    const ta = document.createElement('textarea');
    ta.value = text; ta.setAttribute('readonly', ''); ta.style.position = 'fixed'; ta.style.opacity = '0';
    document.body.appendChild(ta); ta.select();
    const ok = document.execCommand('copy');
    ta.remove();
    return ok;
  } catch { return false; }
}

export function downloadBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = filename; a.rel = 'noopener';
  document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 4000);
}
export function downloadText(text, filename, type = 'text/plain') { downloadBlob(new Blob([text], { type: type + ';charset=utf-8' }), filename); }

export function toCsv(columns, rows) {
  const q = (v) => {
    if (v == null) return '';
    const s = typeof v === 'object' ? JSON.stringify(v) : String(v);
    return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  };
  const cols = columns.map((c) => (typeof c === 'string' ? c : c.name));
  return [cols.map(q).join(','), ...rows.map((r) => (Array.isArray(r) ? r : cols.map((c) => r[c])).map(q).join(','))].join('\n');
}

/** Minimal cron next-fire preview is done server-side; this validates a 5-field expression shape only. */
export function looksLikeCron(s) { return /^\s*(\S+\s+){4}\S+\s*$/.test(s || ''); }

export function qs(obj) {
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(obj || {})) {
    if (v == null || v === '' || v === false) continue;
    if (Array.isArray(v)) v.forEach((x) => p.append(k, x)); else p.append(k, String(v));
  }
  const s = p.toString();
  return s ? '?' + s : '';
}

/** Map any status word to one of the five light states used across the UI. */
export function lightState(s) {
  const v = String(s || 'unknown').toLowerCase();
  if (['ok', 'online', 'healthy', 'up', 'ready', 'succeeded', 'success', 'active', 'alive', 'verified', 'true', 'pass', 'passed', 'idle', 'resolved', 'available', 'running_ok'].includes(v)) return 'ok';
  if (['degraded', 'warn', 'warning', 'draining', 'cordoned', 'pending', 'installing', 'stale', 'partial', 'queued', 'retrying', 'firing_low', 'starting'].includes(v)) return 'degraded';
  if (['down', 'failed', 'error', 'offline', 'revoked', 'dead', 'critical', 'bad', 'false', 'fail', 'timed_out', 'firing', 'missing', 'unreachable'].includes(v)) return 'down';
  if (['busy', 'running', 'executing', 'dispatching', 'provisioning', 'validating', 'accepted'].includes(v)) return 'busy';
  return 'unknown';
}
export const statusLabel = (s) => ({ ok: 'Healthy', degraded: 'Degraded', down: 'Down', unknown: 'Unknown', busy: 'Busy' }[lightState(s)]);

export function worstState(states) {
  const rank = { down: 3, degraded: 2, unknown: 1, busy: 0, ok: 0 };
  let best = 'ok'; let br = -1; let anyKnown = false;
  for (const s of states) { const l = lightState(s); if (l !== 'unknown') anyKnown = true; if (rank[l] > br) { br = rank[l]; best = l; } }
  return anyKnown || !states.length ? best : 'unknown';
}

export function relUrl(path) { return path.startsWith('#') ? path : '#' + path; }
export function navigate(path) { window.location.hash = path.startsWith('#') ? path : '#' + path; }

/** Event bus for cross-cutting notifications (SSE events, session expiry...). */
const listeners = new Map();
export const bus = {
  on(name, fn) { if (!listeners.has(name)) listeners.set(name, new Set()); listeners.get(name).add(fn); return () => listeners.get(name).delete(fn); },
  emit(name, data) { (listeners.get(name) || []).forEach((f) => { try { f(data); } catch (e) { console.error(e); } }); },
};
