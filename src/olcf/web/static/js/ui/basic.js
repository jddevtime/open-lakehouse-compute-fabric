// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Basic component library: buttons, badges, status lights, cards, banners, tabs, form fields, meters, code blocks.
import { html, useState, useRef, useEffect, useId } from '../core/h.js';
import { Icon } from './icons.js';
import { copyText, lightState, statusLabel, fmtPct, titleCase } from '../core/util.js';
import { describeError } from '../api/client.js';

export function Btn({ kind = '', size = '', icon, iconOnly, busy, children, label, href, class: cls = '', ...rest }) {
  const c = ['btn', kind, size, iconOnly ? 'icon-only' : '', cls].filter(Boolean).join(' ');
  const inner = html`${busy ? html`<span class="spinner spin" aria-hidden="true"></span>` : icon ? html`<${Icon} name=${icon} size=${size === 'sm' ? 'sm' : undefined} />` : null}${iconOnly ? html`<span class="sr-only">${label || children}</span>` : children}`;
  if (href) return html`<a class=${c} href=${href} aria-label=${iconOnly ? label : undefined} title=${iconOnly ? label : rest.title} ...${rest}>${inner}</a>`;
  return html`<button type=${rest.type || 'button'} class=${c} aria-label=${iconOnly ? label : undefined} title=${iconOnly ? label : rest.title} ...${rest} disabled=${rest.disabled || busy}>${inner}</button>`;
}

const GLYPH = { ok: 'M5 12.5l4.5 4.5L19 7.5', degraded: 'M12 6v7M12 17.5h.01', down: 'M7 7l10 10M17 7 7 17', unknown: 'M9.5 9.5a2.5 2.5 0 1 1 3.5 2.3c-.7.4-1 1-1 1.7M12 17.5h.01', busy: 'M12 6v6l3.5 2' };
/** A status light. `glyph` adds an in-light symbol so state never relies on colour alone (use for sizes >= 16px). */
export function Light({ state, size = '', pulse, glyph, label }) {
  const s = lightState(state);
  const l = label || statusLabel(state);
  return html`<span class=${`light ${s} ${size}${pulse && s === 'ok' ? ' pulse' : ''}`} role="img" aria-label=${l} title=${l}>${glyph ? html`<span class="glyph"><svg viewBox="0 0 24 24"><path d=${GLYPH[s]} /></svg></span>` : null}</span>`;
}

export function StatusBadge({ state, label, dot = true }) {
  const s = String(state || 'unknown').toLowerCase();
  return html`<span class=${'badge ' + s}>${dot ? html`<${Light} state=${s} label="" />` : null}${label || titleCase(s)}</span>`;
}
export function Badge({ kind = '', children, mono, title }) { return html`<span class=${`badge ${kind}${mono ? ' mono' : ''}`} title=${title}>${children}</span>`; }
export function Tag({ children }) { return html`<span class="tag">${children}</span>`; }

export function Card({ title, sub, actions, children, flush, class: cls = '', head = true, id, foot }) {
  return html`<section class=${'card ' + cls} id=${id} aria-label=${typeof title === 'string' ? title : undefined}>
    ${head && (title || actions) ? html`<header class="card-head"><h3>${title}${sub ? html` <span class="sub">${sub}</span>` : null}</h3>${actions}</header>` : null}
    <div class=${'card-body' + (flush ? ' flush' : '')}>${children}</div>
    ${foot ? html`<footer class="card-foot">${foot}</footer>` : null}
  </section>`;
}

export function Banner({ kind = 'info', title, children, remediation, actions, icon }) {
  const ic = icon || { info: 'info', warn: 'alert-triangle', bad: 'alert-circle', ok: 'check-circle' }[kind];
  return html`<div class=${'banner ' + kind} role=${kind === 'bad' ? 'alert' : 'status'}><${Icon} name=${ic} />
    <div class="grow">${title ? html`<div class="b-title">${title}</div>` : null}${children ? html`<div class="b-body">${children}</div>` : null}${remediation ? html`<div class="b-rem"><b>What to do:</b> ${remediation}</div>` : null}</div>${actions}</div>`;
}

export function Empty({ icon = 'folder', title, children, action }) {
  return html`<div class="empty"><${Icon} name=${icon} /><h3>${title}</h3>${children ? html`<p>${children}</p>` : null}${action}</div>`;
}
export function Loading({ label = 'Loading' }) { return html`<div class="loading" role="status"><span class="spinner lg"></span><span>${label}</span></div>`; }
export function Spinner() { return html`<span class="spinner" role="status" aria-label="Loading"></span>`; }

/** Standard error presentation: message, server remediation, correlation id, optional retry. */
export function ErrorBox({ error, onRetry, compact }) {
  if (!error) return null;
  const d = describeError(error);
  return html`<${Banner} kind="bad" title=${d.title} remediation=${d.remediation} actions=${onRetry ? html`<${Btn} size="sm" onClick=${onRetry}>Try again<//>` : null}>
    ${d.body}${d.cid && !compact ? html`<div class="small muted mt-2">Reference <span class="mono">${d.cid}</span>${d.code ? html` · ${d.code}` : null}</div>` : null}</${Banner}>`;
}

/** Wrap an async result: loading / error / content. */
export function Async({ state, children, label, empty }) {
  if (state.loading && state.data === undefined) return html`<${Loading} label=${label} />`;
  if (state.error && state.data === undefined) return html`<${ErrorBox} error=${state.error} onRetry=${state.reloadLoud} />`;
  if (empty && state.data !== undefined && empty(state.data)) return null;
  return children(state.data);
}

export function CopyBtn({ text, label = 'Copy', size = 'sm', kind = '', iconOnly }) {
  const [done, setDone] = useState(false);
  const timer = useRef();
  useEffect(() => () => clearTimeout(timer.current), []);
  const go = async () => { const ok = await copyText(typeof text === 'function' ? text() : text); setDone(ok); clearTimeout(timer.current); timer.current = setTimeout(() => setDone(false), 1600); };
  return html`<${Btn} size=${size} kind=${kind} icon=${done ? 'check' : 'copy'} iconOnly=${iconOnly} label=${done ? 'Copied' : label} onClick=${go}>${done ? 'Copied' : label}<//>`;
}

export function Code({ children, text, copy = true, wrap, maxHeight, lang }) {
  const body = text ?? children;
  return html`<div class="code-wrap"><pre class=${'code' + (wrap ? ' wrap' : '')} style=${maxHeight ? `max-height:${maxHeight}px` : undefined} tabindex="0" data-lang=${lang}>${body}</pre>${copy ? html`<span class="copy"><${CopyBtn} text=${String(body)} /></span>` : null}</div>`;
}

export function Kv({ items }) {
  return html`<dl class="kv">${items.filter(Boolean).map(([k, v]) => html`<dt key=${k}>${k}</dt><dd>${v ?? html`<span class="muted">–</span>`}</dd>`)}</dl>`;
}

export function Tabs({ tabs, value, onChange, label = 'Sections' }) {
  const onKey = (e, i) => {
    if (e.key !== 'ArrowRight' && e.key !== 'ArrowLeft' && e.key !== 'Home' && e.key !== 'End') return;
    e.preventDefault();
    const n = e.key === 'Home' ? 0 : e.key === 'End' ? tabs.length - 1 : (i + (e.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
    onChange(tabs[n].id);
    requestAnimationFrame(() => document.getElementById('tab-' + tabs[n].id)?.focus());
  };
  const list = useRef(null);
  // on a narrow screen the strip scrolls sideways: bring the chosen tab into view (inside the strip only, the page itself must not move)
  useEffect(() => {
    const el = list.current && list.current.querySelector('[aria-selected="true"]');
    if (!el || list.current.scrollWidth <= list.current.clientWidth) return;
    const l = list.current;
    l.scrollLeft = Math.max(0, el.offsetLeft - (l.clientWidth - el.offsetWidth) / 2);
  }, [value]);
  return html`<div class="tabs" role="tablist" aria-label=${label} ref=${list}>${tabs.map((t, i) => html`<button key=${t.id} id=${'tab-' + t.id} role="tab" aria-selected=${value === t.id} aria-controls=${'panel-' + t.id} tabindex=${value === t.id ? 0 : -1} onClick=${() => onChange(t.id)} onKeyDown=${(e) => onKey(e, i)}>${t.icon ? html`<${Icon} name=${t.icon} size="sm" />` : null}${t.label}${t.count != null ? html`<span class="count">${t.count}</span>` : null}</button>`)}</div>`;
}
export function TabPanel({ id, value, children }) { return value === id ? html`<div role="tabpanel" id=${'panel-' + id} aria-labelledby=${'tab-' + id}>${children}</div>` : null; }

export function Seg({ options, value, onChange, label }) {
  return html`<div class="seg" role="group" aria-label=${label}>${options.map((o) => html`<button key=${o.value} type="button" aria-pressed=${value === o.value} title=${o.title || o.label} onClick=${() => onChange(o.value)}>${o.icon ? html`<${Icon} name=${o.icon} size="sm" />` : null}${o.label ? html`<span>${o.label}</span>` : html`<span class="sr-only">${o.title}</span>`}</button>`)}</div>`;
}

// ---- form fields ----
export function Field({ label, hint, error, children, class: cls = '', required }) {
  const id = useId();
  return html`<div class=${'field ' + cls}>${label ? html`<label for=${id}>${label}${required ? html`<span class="muted" aria-hidden="true">*</span>` : null}</label>` : null}${typeof children === 'function' ? children(id) : children}${hint ? html`<div class="hint">${hint}</div>` : null}${error ? html`<div class="err" role="alert">${error}</div>` : null}</div>`;
}
export function Input({ label, hint, error, value, onInput, onChange, mono, class: cls = '', required, size, ...rest }) {
  return html`<${Field} label=${label} hint=${hint} error=${error} required=${required}>${(id) => html`<input id=${id} class=${`input ${mono ? 'mono' : ''} ${size || ''} ${cls}`} value=${value ?? ''} onInput=${(e) => { onInput && onInput(e.target.value, e); onChange && onChange(e.target.value, e); }} aria-invalid=${error ? 'true' : undefined} required=${required} ...${rest} />`}<//>`;
}
export function Textarea({ label, hint, error, value, onInput, mono, rows = 4, ...rest }) {
  return html`<${Field} label=${label} hint=${hint} error=${error}>${(id) => html`<textarea id=${id} class=${'textarea' + (mono ? ' mono' : '')} rows=${rows} value=${value ?? ''} onInput=${(e) => onInput && onInput(e.target.value, e)} ...${rest}></textarea>`}<//>`;
}
export function Select({ label, hint, value, onChange, options, size, placeholder, ...rest }) {
  return html`<${Field} label=${label} hint=${hint}>${(id) => html`<select id=${id} class=${'select ' + (size || '')} value=${value ?? ''} onChange=${(e) => onChange && onChange(e.target.value, e)} ...${rest}>${placeholder != null ? html`<option value="">${placeholder}</option>` : null}${(options || []).map((o) => { const v = typeof o === 'object' ? o.value : o; const l = typeof o === 'object' ? o.label : o; return html`<option key=${v} value=${v} selected=${String(v) === String(value ?? '')}>${l}</option>`; })}</select>`}<//>`;
}
export function Check({ label, checked, onChange, ...rest }) {
  return html`<label class="check"><input type="checkbox" checked=${!!checked} onChange=${(e) => onChange && onChange(e.target.checked)} ...${rest} /> <span>${label}</span></label>`;
}
export function Switch({ label, checked, onChange, ...rest }) {
  return html`<label class="switch"><input type="checkbox" role="switch" checked=${!!checked} onChange=${(e) => onChange && onChange(e.target.checked)} ...${rest} /> <span>${label}</span></label>`;
}

// ---- meters ----
export function Progress({ value, kind = '', indeterminate, label }) {
  return html`<div class=${`progress ${kind}${indeterminate ? ' indeterminate' : ''}`} role="progressbar" aria-label=${label} aria-valuemin="0" aria-valuemax="100" aria-valuenow=${indeterminate ? undefined : Math.round(value || 0)}><i style=${`width:${Math.min(100, Math.max(0, value || 0))}%`}></i></div>`;
}
export function Gauge({ label, value, text, warn = 75, bad = 90, hint }) {
  const v = value == null ? null : Math.max(0, Math.min(100, value));
  const k = v == null ? '' : v >= bad ? 'bad' : v >= warn ? 'warn' : '';
  return html`<div class=${'gauge ' + k} title=${hint}><div class="g-top"><span>${label}</span><b>${text ?? fmtPct(v)}</b></div><div class="g-bar" role="meter" aria-label=${label} aria-valuemin="0" aria-valuemax="100" aria-valuenow=${v ?? undefined}><i style=${`width:${v ?? 0}%`}></i></div></div>`;
}
/** Sparkline with an optional fixed domain; drawn as one path + soft area. */
export function Sparkline({ values, min, max, height = 26, label, color }) {
  const vals = (values || []).filter((v) => v != null);
  if (vals.length < 2) return html`<svg class="spark" viewBox="0 0 100 26" preserveAspectRatio="none" role="img" aria-label=${(label || 'trend') + ': not enough data'}></svg>`;
  const lo = min ?? Math.min(...vals); const hi = max ?? Math.max(...vals, lo + 1);
  const W = 100; const H = height;
  const pts = vals.map((v, i) => [(i / (vals.length - 1)) * W, H - 2 - ((Math.min(hi, Math.max(lo, v)) - lo) / (hi - lo || 1)) * (H - 4)]);
  const d = pts.map((p, i) => (i ? 'L' : 'M') + p[0].toFixed(2) + ' ' + p[1].toFixed(2)).join(' ');
  const last = vals[vals.length - 1];
  return html`<svg class="spark" viewBox=${`0 0 ${W} ${H}`} preserveAspectRatio="none" role="img" aria-label=${`${label || 'trend'}: latest ${Math.round(last * 10) / 10}, range ${Math.round(Math.min(...vals))} to ${Math.round(Math.max(...vals))}`} style=${color ? `--series-1:${color}` : undefined}>
    <path class="area" d=${d + ` L${W} ${H} L0 ${H} Z`} /><path class="line" vector-effect="non-scaling-stroke" d=${d} /></svg>`;
}

/** Vertical pipeline of lights: used by every "Test connectivity" result. steps: [{name, ok, detail, ms, remediation, state}] */
export function Steps({ steps, pending = [] }) {
  return html`<ol class="steps" aria-label="Test steps">${[...steps.map((s) => ({ ...s, _s: s.state || (s.ok === true ? 'ok' : s.ok === false ? 'down' : s.skipped ? 'unknown' : 'unknown') })), ...pending.map((p) => ({ name: p, _s: 'pending' }))].map((s, i) => html`<li key=${i} class=${'step ' + (s._s === 'pending' ? 'pending' : '')}>
    <div class="s-light"><${s._s === 'pending' ? Spin : Light} state=${s._s === 'pending' ? 'unknown' : s._s} size="lg" glyph=${true} /></div>
    <div><div class="s-title">${s.label || s.name}${s.ms != null ? html`<span class="badge outline mono">${s.ms >= 1000 ? (s.ms / 1000).toFixed(2) + ' s' : Math.round(s.ms * 10) / 10 + ' ms'}</span>` : null}</div>
    ${s.detail ? html`<div class="s-detail">${typeof s.detail === 'string' ? s.detail : JSON.stringify(s.detail)}</div>` : null}
    ${s.remediation ? html`<div class="s-rem"><b>Fix: </b>${s.remediation}</div>` : null}</div></li>`)}</ol>`;
}
function Spin() { return html`<span class="spinner" style="width:16px;height:16px"></span>`; }

export function Kbd({ children }) { return html`<kbd>${children}</kbd>`; }
export function PageHead({ title, sub, actions, icon, badge }) {
  return html`<header class="page-head"><div class="titles"><h1>${icon ? html`<${Icon} name=${icon} size="lg" />` : null}${title}${badge}</h1>${sub ? html`<div class="sub">${sub}</div>` : null}</div>${actions ? html`<div class="actions">${actions}</div>` : null}</header>`;
}
