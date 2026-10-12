// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Rendering primitives: Preact + htm (vendored, no build step) and the hooks used across the UI.
import { h, render, Fragment, createContext, Component } from '../../vendor/preact/preact.module.js';
import { useState, useEffect, useRef, useMemo, useCallback, useContext, useReducer, useLayoutEffect, useId } from '../../vendor/preact/hooks.module.js';
import htm from '../../vendor/htm/htm.module.js';

export const html = htm.bind(h);
export { h, render, Fragment, createContext, Component, useState, useEffect, useRef, useMemo, useCallback, useContext, useReducer, useLayoutEffect, useId };

/** Run an async loader when `deps` change; returns {data, error, loading, reload, setData}. Stale responses are dropped. */
export function useAsync(fn, deps = []) {
  const [st, setSt] = useState({ data: undefined, error: null, loading: true });
  const seq = useRef(0);
  const run = useCallback((silent) => {
    const my = ++seq.current;
    if (!silent) setSt((s) => ({ ...s, loading: true, error: null }));
    return Promise.resolve()
      .then(fn)
      .then((data) => { if (seq.current === my) setSt({ data, error: null, loading: false }); return data; })
      .catch((error) => { if (seq.current === my) setSt((s) => ({ data: silent ? s.data : undefined, error, loading: false })); });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
  useEffect(() => { run(false); return () => { seq.current++; }; }, [run]);
  return { ...st, reload: () => run(true), reloadLoud: () => run(false), setData: (data) => setSt((s) => ({ ...s, data })) };
}

/** Poll `fn` every `ms` (backs off to 5x when the tab is hidden). Returns the same shape as useAsync. */
export function usePoll(fn, ms, deps = []) {
  const a = useAsync(fn, deps);
  const reload = useRef(a.reload);
  reload.current = a.reload;
  useEffect(() => {
    let timer;
    let stopped = false;
    const tick = () => {
      if (stopped) return;
      timer = setTimeout(() => { reload.current(); tick(); }, document.hidden ? ms * 5 : ms);
    };
    tick();
    const vis = () => { if (!document.hidden) reload.current(); };
    document.addEventListener('visibilitychange', vis);
    return () => { stopped = true; clearTimeout(timer); document.removeEventListener('visibilitychange', vis); };
  }, [ms, ...deps]);
  return a;
}

/** Subscribe a component to a store (see store.js). */
export function useStore(store, selector = (s) => s) {
  const [, force] = useReducer((n) => n + 1, 0);
  const sel = useRef(selector);
  sel.current = selector;
  const value = selector(store.get());
  const last = useRef(value);
  last.current = value;
  useEffect(() => {
    const check = () => { const next = sel.current(store.get()); if (next !== last.current) { last.current = next; force(); } };
    const off = store.subscribe(check);
    check(); // the store may have changed between render and subscription
    return off;
  }, [store]);
  return value;
}

export function useDebounced(value, ms = 250) {
  const [v, setV] = useState(value);
  useEffect(() => { const t = setTimeout(() => setV(value), ms); return () => clearTimeout(t); }, [value, ms]);
  return v;
}

/** Keep a callback fresh without re-subscribing effects. */
export function useLatest(v) { const r = useRef(v); r.current = v; return r; }

export function useInterval(fn, ms) {
  const f = useLatest(fn);
  useEffect(() => { if (ms == null) return undefined; const t = setInterval(() => f.current(), ms); return () => clearInterval(t); }, [ms]);
}

export function useEvent(target, name, handler, opts) {
  const h2 = useLatest(handler);
  useEffect(() => {
    const t = typeof target === 'function' ? target() : target;
    if (!t) return undefined;
    const fn = (e) => h2.current(e);
    t.addEventListener(name, fn, opts);
    return () => t.removeEventListener(name, fn, opts);
  }, [name]);
}
