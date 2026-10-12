// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Overlays: dialogs, drawers, toasts, dropdown menus, confirm/prompt helpers. Dialogs trap focus, close on Escape and restore focus.
import { html, useEffect, useRef, useState, useStore, useId } from '../core/h.js';
import { createStore, toasts } from '../core/store.js';
import { Icon } from './icons.js';
import { Btn, ErrorBox, Input } from './basic.js';
import { describeError } from '../api/client.js';

const FOCUSABLE = 'a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex="-1"])';

function useTrap(ref, onClose) {
  useEffect(() => {
    const prev = document.activeElement;
    const el = ref.current;
    const first = el && (el.querySelector('[data-autofocus]') || el.querySelector(FOCUSABLE) || el);
    if (first) (first.focus ? first : el).focus();
    const key = (e) => {
      if (e.key === 'Escape') { e.stopPropagation(); onClose && onClose(); return; }
      if (e.key !== 'Tab' || !el) return;
      const f = [...el.querySelectorAll(FOCUSABLE)].filter((x) => x.offsetParent !== null);
      if (!f.length) { e.preventDefault(); return; }
      const a = f[0]; const z = f[f.length - 1];
      if (e.shiftKey && document.activeElement === a) { e.preventDefault(); z.focus(); } else if (!e.shiftKey && document.activeElement === z) { e.preventDefault(); a.focus(); }
    };
    document.addEventListener('keydown', key, true);
    return () => { document.removeEventListener('keydown', key, true); if (prev && prev.focus && document.contains(prev)) prev.focus(); };
  }, []);
}

export function Dialog({ title, onClose, children, footer, size = '', dismissable = true, labelledBy }) {
  const ref = useRef();
  const tid = useId();
  useTrap(ref, dismissable ? onClose : null);
  return html`<div class="scrim" onMouseDown=${(e) => { if (dismissable && e.target === e.currentTarget) onClose && onClose(); }}>
    <div class=${'dialog ' + size} role="dialog" aria-modal="true" aria-labelledby=${labelledBy || tid} ref=${ref} tabindex="-1">
      <div class="dialog-head"><h2 id=${tid}>${title}</h2>${dismissable ? html`<${Btn} kind="ghost" size="sm" iconOnly icon="x" label="Close dialog" onClick=${onClose} />` : null}</div>
      <div class="dialog-body">${children}</div>
      ${footer ? html`<div class="dialog-foot">${footer}</div>` : null}
    </div></div>`;
}

export function Drawer({ title, onClose, children, wide, actions }) {
  const ref = useRef();
  const tid = useId();
  useTrap(ref, onClose);
  return html`<div><div class="drawer-scrim" onMouseDown=${onClose}></div>
    <aside class=${'drawer' + (wide ? ' wide' : '')} role="dialog" aria-modal="true" aria-labelledby=${tid} ref=${ref} tabindex="-1">
      <div class="drawer-head"><h2 id=${tid} class="trunc">${title}</h2>${actions}<${Btn} kind="ghost" size="sm" iconOnly icon="x" label="Close panel" onClick=${onClose} /></div>
      <div class="drawer-body">${children}</div></aside></div>`;
}

// ---- imperative overlay host ----
const overlays = createStore({ items: [] });
let seq = 0;
/** Open any overlay; `render({close})` returns vdom. Resolves when closed with the value passed to close(). */
export function openOverlay(render) {
  return new Promise((resolve) => {
    const id = ++seq;
    const close = (value) => { overlays.set((s) => ({ ...s, items: s.items.filter((o) => o.id !== id) })); resolve(value); };
    overlays.set((s) => ({ ...s, items: [...s.items, { id, render: () => render({ close }) }] }));
  });
}
export function OverlayHost() {
  const { items } = useStore(overlays);
  return html`<${Fragment2}>${items.map((o) => html`<div key=${o.id}>${o.render()}</div>`)}</${Fragment2}>`;
}
function Fragment2({ children }) { return children; }

export function confirmDialog({ title, body, confirmLabel = 'Confirm', danger, cancelLabel = 'Cancel', details }) {
  return openOverlay(({ close }) => html`<${Dialog} title=${title} onClose=${() => close(false)}
    footer=${html`<${Btn} onClick=${() => close(false)}>${cancelLabel}<//><${Btn} kind=${danger ? 'danger' : 'primary'} data-autofocus=${true} onClick=${() => close(true)}>${confirmLabel}<//>`}>
    <div>${body}</div>${details}</${Dialog}>`);
}
export function promptDialog({ title, label, value = '', confirmLabel = 'OK', hint, placeholder, validate }) {
  return openOverlay(({ close }) => html`<${PromptBody} ...${{ title, label, value, confirmLabel, hint, placeholder, validate, close }} />`);
}
function PromptBody({ title, label, value, confirmLabel, hint, placeholder, validate, close }) {
  const [v, setV] = useState(value);
  const err = validate ? validate(v) : null;
  const submit = (e) => { e.preventDefault(); if (!err) close(v); };
  return html`<${Dialog} title=${title} onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" disabled=${!!err || !v} onClick=${submit}>${confirmLabel}<//>`}>
    <form onSubmit=${submit}><${Input} label=${label} value=${v} onInput=${setV} hint=${hint} placeholder=${placeholder} error=${v ? err : null} data-autofocus=${true} /></form></${Dialog}>`;
}

/** Run an async action with toast feedback; returns the result or undefined on failure. */
export async function act(fn, { ok, fail } = {}) {
  try {
    const r = await fn();
    if (ok) toast.ok(typeof ok === 'function' ? ok(r) : ok);
    return r;
  } catch (e) {
    if (e && e.name === 'AbortError') return undefined;
    const d = describeError(e);
    toast.bad(fail || d.title, [d.body, d.remediation].filter(Boolean).join(' '));
    return undefined;
  }
}

// ---- toasts ----
let tseq = 0;
export function toast(kind, title, body, ms = 5200) {
  const id = ++tseq;
  toasts.set((s) => ({ items: [...s.items, { id, kind, title, body }] }));
  if (ms) setTimeout(() => dismissToast(id), ms);
  return id;
}
toast.ok = (t, b) => toast('ok', t, b);
toast.bad = (t, b) => toast('bad', t, b, 9000);
toast.warn = (t, b) => toast('warn', t, b, 7000);
toast.info = (t, b) => toast('info', t, b);
export const dismissToast = (id) => toasts.set((s) => ({ items: s.items.filter((x) => x.id !== id) }));
export function Toasts() {
  const { items } = useStore(toasts);
  return html`<div class="toasts" role="region" aria-label="Notifications" aria-live="polite">${items.map((t) => html`<div class=${'toast ' + t.kind} key=${t.id} role=${t.kind === 'bad' ? 'alert' : 'status'}>
    <${Icon} name=${{ ok: 'check-circle', bad: 'alert-circle', warn: 'alert-triangle', info: 'info' }[t.kind]} /><div class="grow"><div class="t-title">${t.title}</div>${t.body ? html`<div class="t-body">${t.body}</div>` : null}</div>
    <button aria-label="Dismiss" onClick=${() => dismissToast(t.id)}><${Icon} name="x" size="sm" /></button></div>`)}</div>`;
}

// ---- dropdown menu ----
export function MenuButton({ items, label, icon = 'more', kind = 'ghost', size = 'sm', children, iconOnly = true, align = 'right' }) {
  const [open, setOpen] = useState(null);
  const btn = useRef();
  const toggle = () => {
    if (open) return setOpen(null);
    const r = btn.current.getBoundingClientRect();
    const w = 220;
    const left = align === 'right' ? Math.max(8, Math.min(window.innerWidth - w - 8, r.right - w)) : Math.min(r.left, window.innerWidth - w - 8);
    const below = window.innerHeight - r.bottom > 260 || r.top < 260;
    setOpen({ left, top: below ? r.bottom + 4 : undefined, bottom: below ? undefined : window.innerHeight - r.top + 4 });
  };
  return html`<span class="menu-anchor" ref=${btn}><${Btn} kind=${kind} size=${size} iconOnly=${iconOnly} icon=${icon} label=${label} aria-label=${label} aria-haspopup="menu" aria-expanded=${!!open} onClick=${toggle}>${children}<//>
    ${open ? html`<${MenuList} items=${items} pos=${open} onClose=${() => { setOpen(null); btn.current?.querySelector('button')?.focus(); }} />` : null}</span>`;
}
function MenuList({ items, pos, onClose }) {
  const ref = useRef();
  useEffect(() => {
    ref.current.querySelector('button:not([disabled]),a')?.focus();
    const out = (e) => { if (!ref.current.contains(e.target)) onClose(); };
    const key = (e) => {
      if (e.key === 'Escape') { e.stopPropagation(); onClose(); }
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        e.preventDefault();
        const b = [...ref.current.querySelectorAll('button:not([disabled]),a')];
        const i = b.indexOf(document.activeElement);
        b[(i + (e.key === 'ArrowDown' ? 1 : -1) + b.length) % b.length]?.focus();
      }
    };
    setTimeout(() => document.addEventListener('mousedown', out), 0);
    document.addEventListener('keydown', key, true);
    return () => { document.removeEventListener('mousedown', out); document.removeEventListener('keydown', key, true); };
  }, []);
  return html`<div class="menu" role="menu" ref=${ref} style=${`left:${pos.left}px;${pos.top != null ? `top:${pos.top}px` : `bottom:${pos.bottom}px`}`}>
    ${items.filter(Boolean).map((it, i) => it.sep ? html`<div class="sep" key=${i}></div>` : it.head ? html`<div class="head" key=${i}>${it.head}</div>` : html`<button role="menuitem" key=${i} class=${it.danger ? 'danger' : ''} disabled=${it.disabled} onClick=${() => { onClose(); it.onClick && it.onClick(); }}>${it.icon ? html`<${Icon} name=${it.icon} size="sm" />` : null}${it.label}</button>`)}</div>`;
}
