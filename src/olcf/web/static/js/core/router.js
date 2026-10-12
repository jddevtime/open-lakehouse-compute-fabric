// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Hash-based router. Routes are declared in routes.js; page modules are imported lazily on first visit.
import { html, useEffect, useState, useStore, Component } from './h.js';
import { ui } from './store.js';
import { Loading, ErrorBox } from '../ui/basic.js';

export function parseHash(hash = window.location.hash) {
  let h = hash.replace(/^#/, '') || '/';
  if (!h.startsWith('/')) h = '/' + h;
  const [path, search = ''] = h.split('?');
  const query = {};
  new URLSearchParams(search).forEach((v, k) => { query[k] = v; });
  return { path: path.replace(/\/+$/, '') || '/', query };
}

export function compile(pattern) {
  const keys = [];
  const re = new RegExp('^' + pattern.replace(/:(\w+)/g, (_, k) => { keys.push(k); return '([^/]+)'; }) + '$');
  return { re, keys };
}

export function matchRoute(routes, path) {
  for (const r of routes) {
    const c = r._c || (r._c = compile(r.path));
    const m = c.re.exec(path);
    if (m) { const params = {}; c.keys.forEach((k, i) => { params[k] = decodeURIComponent(m[i + 1]); }); return { route: r, params }; }
  }
  return null;
}

export function setQuery(patch, { replace = true } = {}) {
  const { path, query } = parseHash();
  const q = { ...query, ...patch };
  for (const k of Object.keys(q)) if (q[k] == null || q[k] === '') delete q[k];
  const s = new URLSearchParams(q).toString();
  const target = '#' + path + (s ? '?' + s : '');
  if (replace) window.history.replaceState(null, '', target); else window.location.hash = target;
  window.dispatchEvent(new HashChangeEvent('hashchange'));
}

export function useRoute() {
  const [loc, setLoc] = useState(parseHash());
  useEffect(() => {
    const f = () => setLoc(parseHash());
    window.addEventListener('hashchange', f);
    return () => window.removeEventListener('hashchange', f);
  }, []);
  return loc;
}

class Boundary extends Component {
  constructor(p) { super(p); this.state = { err: null }; }
  componentDidCatch(err) { console.error('page crashed', err); this.setState({ err }); }
  componentDidUpdate(prev) { if (prev.k !== this.props.k && this.state.err) this.setState({ err: null }); }
  render() {
    if (this.state.err) return html`<div class="page"><${ErrorBox} error=${this.state.err} onRetry=${() => this.setState({ err: null })} /></div>`;
    return this.props.children;
  }
}

const cache = new Map();
export function Outlet({ routes, loc }) {
  const m = matchRoute(routes, loc.path);
  const [mod, setMod] = useState(null);
  const [fail, setFail] = useState(null);
  const key = m ? m.route.path : '404';
  useEffect(() => {
    setFail(null);
    if (!m) { setMod(null); return undefined; }
    let alive = true;
    const loaded = cache.get(m.route.module);
    if (loaded) setMod({ k: key, m: loaded }); else {
      setMod(null);
      import(m.route.module).then((x) => { cache.set(m.route.module, x); if (alive) setMod({ k: key, m: x }); }).catch((e) => { if (alive) setFail(e); });
    }
    return () => { alive = false; };
  }, [key]);
  useEffect(() => { ui.set({ route: { path: loc.path, params: m ? m.params : {}, query: loc.query, nav: m ? m.route.nav : null, title: m ? m.route.title : 'Not found' } }); document.title = (m ? m.route.title : 'Not found') + ' · OLCF'; }, [loc.path, loc.hash]);
  if (!m) return html`<div class="page"><div class="empty"><h3>Page not found</h3><p>There is no page at <code>${loc.path}</code>.</p><a class="btn primary" href="#/">Go to Home</a></div></div>`;
  if (fail) return html`<div class="page"><${ErrorBox} error=${fail} /></div>`;
  if (!mod || mod.k !== key) return html`<${Loading} label="Loading page" />`;
  const Page = mod.m[m.route.export || 'default'];
  return html`<${Boundary} k=${loc.path}><${Page} key=${loc.path} params=${m.params} query=${loc.query} /></${Boundary}>`;
}
