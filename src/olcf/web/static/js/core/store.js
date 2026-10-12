// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// A tiny observable store (no dependency) plus guarded browser storage helpers.

export function createStore(initial) {
  let state = initial;
  const subs = new Set();
  return {
    get: () => state,
    set(patch) {
      const next = typeof patch === 'function' ? patch(state) : { ...state, ...patch };
      if (next === state) return;
      state = next;
      subs.forEach((f) => { try { f(state); } catch (e) { console.error(e); } });
    },
    subscribe(f) { subs.add(f); return () => subs.delete(f); },
  };
}

/** localStorage / sessionStorage can throw (private mode, blocked site data): every access is guarded. */
export const prefs = {
  get(key, dflt = null) {
    try { const v = window.localStorage.getItem('olcf.' + key); return v == null ? dflt : JSON.parse(v); } catch { return dflt; }
  },
  set(key, value) {
    try { window.localStorage.setItem('olcf.' + key, JSON.stringify(value)); } catch { /* storage unavailable */ }
  },
  del(key) { try { window.localStorage.removeItem('olcf.' + key); } catch { /* storage unavailable */ } },
};
export const sess = {
  get(key, dflt = null) {
    try { const v = window.sessionStorage.getItem('olcf.' + key); return v == null ? dflt : JSON.parse(v); } catch { return dflt; }
  },
  set(key, value) {
    try { window.sessionStorage.setItem('olcf.' + key, JSON.stringify(value)); } catch { /* storage unavailable */ }
  },
  del(key) { try { window.sessionStorage.removeItem('olcf.' + key); } catch { /* storage unavailable */ } },
};

// ---- global application state ----
export const ui = createStore({
  theme: prefs.get('theme', 'system'), // system | light | dark
  rail: prefs.get('rail', 'expanded'),
  workspaceId: prefs.get('workspace', null),
  route: { path: '/', params: {}, query: {} },
});
export const session = createStore({ user: null, bindings: [], permissions: [], ready: false, expired: false });
export const health = createStore({ status: 'unknown', components: [], checkedAt: null });
export const toasts = createStore({ items: [] });
