// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Application shell: navigation rail, top bar with the platform heartbeat strip, command palette, theme and user menus.
import { html, useState, useEffect, useLayoutEffect, useRef, useStore, useMemo, usePoll } from './core/h.js';
import { ui, session, health, prefs } from './core/store.js';
import { t } from './core/i18n.js';
import { navGroups } from './routes.js';
import { Icon, BrandMark } from './ui/icons.js';
import { Btn, Light, Kbd, Badge } from './ui/basic.js';
import { Dialog, MenuButton, Drawer } from './ui/overlay.js';
import { api, logout, can, hasRoute } from './api/client.js';
import { lightState, worstState, ago, debounce, navigate } from './core/util.js';
import { itemHref, itemMeta } from './core/links.js';
import { version } from './version.js';

export function applyTheme(theme) {
  const root = document.documentElement;
  if (theme === 'light' || theme === 'dark') root.setAttribute('data-theme', theme); else root.removeAttribute('data-theme');
}
export function setTheme(theme) { ui.set({ theme }); prefs.set('theme', theme); applyTheme(theme); }

const ADMIN_NAV = { security: 'identity.read', admin: 'admin.settings', runtimes: 'runtime.read', compute: 'compute.read' };

/** Is the Intelligence module installed? A deployment without it has no /v1/intelligence routes, so its navigation group and its command-palette pages would only lead to errors and are
 *  left out. True until the API description says otherwise. */
export function useHasIntelligence() {
  const [has, setHas] = useState(true);
  useEffect(() => { let live = true; hasRoute('/v1/intelligence/summary', 'get').then((h) => { if (live) setHas(h); }); return () => { live = false; }; }, []);
  return has;
}

export function Rail() {
  const route = useStore(ui, (s) => s.route);
  const rail = useStore(ui, (s) => s.rail);
  const sessionSt = useStore(session);
  const visible = (key) => !ADMIN_NAV[key] || sessionSt.permissions.length === 0 || can(ADMIN_NAV[key]);
  const hasIntel = useHasIntelligence();
  const groups = hasIntel ? navGroups : navGroups.filter(([g]) => g !== 'group.intelligence');
  return html`<nav class="rail" aria-label="Main">
    <a class="rail-brand" href="#/" aria-label="OLCF - Open Lakehouse Compute Fabric, home" title="OLCF - Open Lakehouse Compute Fabric"><${BrandMark} /><span class="word"><b>OLCF</b><span class="full">Open Lakehouse Compute Fabric</span></span></a>
    <div class="rail-nav">${groups.map(([g, items]) => html`<div key=${g}><div class="rail-group">${t(g)}</div>${items.filter(([k]) => visible(k)).map(([key, label, icon, path, ext]) => html`<a key=${key} class="rail-link" href=${ext ? path : '#' + path} target=${ext ? '_blank' : undefined} rel=${ext ? 'noopener' : undefined} aria-current=${route.nav === key ? 'page' : undefined} title=${t(label)}><${Icon} name=${icon} /><span class="label">${t(label)}</span>${ext ? html`<${Icon} name="external" size="sm" class="ext" />` : null}</a>`)}</div>`)}</div>
    <div class="rail-foot">
      <button class="rail-link" style="background:none;border:0;cursor:pointer;font:inherit;width:100%" onClick=${() => { const v = rail === 'collapsed' ? 'expanded' : 'collapsed'; ui.set({ rail: v }); prefs.set('rail', v); }} aria-label=${rail === 'collapsed' ? 'Expand navigation' : 'Collapse navigation'}><${Icon} name="panel-left" /><span class="label">Collapse menu</span></button>
      <div class="ver">v${version} · by Justin Dodrill</div>
    </div></nav>`;
}

/** The platform heartbeat: one light per control-plane component; click for details. */
function HealthStrip() {
  const h = useStore(health);
  const [open, setOpen] = useState(false);
  const comps = h.components;
  const order = ['database', 'secrets', 'audit', 'leader', 'scheduler', 'worker-fleet', 'runtimes', 'mcp', 'fabric-adapter'];
  const key = comps.filter((c) => order.includes(c.name) || c.name.startsWith('storage:'));
  const shown = (key.length ? key : comps).slice(0, 9);
  const overall = h.status === 'unknown' ? 'unknown' : worstState(comps.map((c) => c.status));
  const label = { ok: 'All systems healthy', degraded: 'Needs attention', down: 'Problem detected', unknown: 'Checking', busy: 'Checking' }[overall];
  return html`<button class="hstrip" onClick=${() => setOpen(true)} aria-label=${'Platform health: ' + label}>
    <span class="lights" aria-hidden="true">${shown.length ? shown.map((c) => html`<${Light} key=${c.name} state=${c.status} label=${c.name + ': ' + c.status} />`) : html`<${Light} state="unknown" />`}</span><span class="txt">${label}</span></button>
    ${open ? html`<${HealthDrawer} onClose=${() => setOpen(false)} />` : null}`;
}
function HealthDrawer({ onClose }) {
  const h = useStore(health);
  const groups = {};
  h.components.forEach((c) => { (groups[c.category || 'other'] ||= []).push(c); });
  return html`<${Drawer} title="Platform health" onClose=${onClose}>
    <p class="muted">Quick checks from every control-plane component${h.checkedAt ? ', last updated ' + ago(h.checkedAt) : ''}. Open Compute to run deep connectivity tests.</p>
    ${Object.entries(groups).map(([g, cs]) => html`<h4 class="mt-4 mb-2" style="text-transform:capitalize">${g.replace('-', ' ')}</h4><div class="col gap-1">${cs.map((c) => html`<div key=${c.name} class="row" style="align-items:flex-start;padding:6px 0;border-bottom:1px solid var(--line)"><${Light} state=${c.status} /><div class="grow"><div style="font-weight:580">${c.name}</div><div class="small muted">${c.detail}</div></div></div>`)}</div>`)}
    <div class="form-actions"><${Btn} href="#/compute" onClick=${onClose}>Open Compute panel<//></div></${Drawer}>`;
}

function ThemeMenu() {
  const theme = useStore(ui, (s) => s.theme);
  const icon = theme === 'dark' ? 'moon' : theme === 'light' ? 'sun' : 'monitor';
  return html`<${MenuButton} label="Choose colour theme" icon=${icon} items=${[
    { head: 'Colour theme' },
    { label: 'Match system', icon: 'monitor', onClick: () => setTheme('system') },
    { label: 'Light', icon: 'sun', onClick: () => setTheme('light') },
    { label: 'Dark', icon: 'moon', onClick: () => setTheme('dark') },
  ]} />`;
}
function UserMenu() {
  const s = useStore(session);
  const u = s.user;
  const initials = (u && (u.display_name || u.name) || '?').split(/\s+/).map((x) => x[0]).join('').slice(0, 2).toUpperCase();
  return html`<${MenuButton} label=${'Account menu for ' + (u ? u.name : '')} icon="user" iconOnly=${false} kind="ghost" items=${[
    { head: u ? `${u.display_name || u.name}` : '' },
    { label: 'Security & tokens', icon: 'key', onClick: () => navigate('/security?tab=tokens') },
    { label: 'Change password', icon: 'lock', onClick: () => navigate('/security?tab=account') },
    { sep: true },
    { label: 'Sign out', icon: 'log-out', onClick: () => logout() },
  ]}><span aria-hidden="true" style="display:grid;place-items:center;width:24px;height:24px;border-radius:50%;background:var(--accent-soft);color:var(--accent-ink);font-weight:700;font-size:11px">${initials}</span><span class="small">${u ? u.name : ''}</span><//>`;
}

export function Topbar({ onSearch }) {
  const route = useStore(ui, (s) => s.route);
  return html`<header class="topbar">
    <div class="crumbs" aria-live="polite"><strong style="color:var(--ink);font-weight:620">${route.title || ''}</strong></div>
    <div class="spacer"></div>
    <button class="search-trigger" onClick=${onSearch} aria-label="Search (Ctrl+K)"><${Icon} name="search" size="sm" /><span class="lbl">Search items and pages</span><${Kbd}>Ctrl K<//></button>
    <${HealthStrip} /><${ThemeMenu} /><${UserMenu} /></header>`;
}

export function Palette({ onClose }) {
  const [q, setQ] = useState('');
  const [remote, setRemote] = useState([]);
  const [sel, setSel] = useState(0);
  const hasIntel = useHasIntelligence();
  const pages = useMemo(() => navGroups.filter(([g]) => hasIntel || g !== 'group.intelligence').flatMap(([, items]) => items.map(([k, l, i, p]) => ({ key: 'p-' + k, label: t(l), icon: i, kind: 'Page', href: '#' + p }))), [hasIntel]);
  const lookup = useMemo(() => debounce(async (term) => { try { const r = await api.get('/search', { q: term, limit: 12 }); setRemote(r.items || []); } catch { setRemote([]); } }, 200), []);
  useEffect(() => { if (q.trim().length >= 2) lookup(q.trim()); else setRemote([]); setSel(0); }, [q]);
  const local = q ? pages.filter((p) => p.label.toLowerCase().includes(q.toLowerCase())) : pages.slice(0, 8);
  const rem = remote.map((it) => ({ key: it.id, label: it.name, icon: itemMeta(it.type).icon, kind: itemMeta(it.type).label, href: itemHref(it) }));
  const all = [...local, ...rem];
  const go = (it) => { onClose(); window.location.hash = it.href; };
  const onKey = (e) => {
    if (e.key === 'ArrowDown') { e.preventDefault(); setSel((s) => Math.min(all.length - 1, s + 1)); }
    if (e.key === 'ArrowUp') { e.preventDefault(); setSel((s) => Math.max(0, s - 1)); }
    if (e.key === 'Enter' && all[sel]) { e.preventDefault(); go(all[sel]); }
    if (e.key === 'Escape') onClose();
  };
  const ref = useRef();
  // layout effect, not a passive one: the input must hold focus in the same frame the dialog appears. A passive effect runs after the next paint, and keystrokes
  // typed in between (keyboard users, assistive technology, automation) went to the previously focused control and were lost.
  useLayoutEffect(() => { ref.current && ref.current.focus(); }, []);
  return html`<div class="scrim" onMouseDown=${(e) => e.target === e.currentTarget && onClose()}><div class="palette" role="dialog" aria-modal="true" aria-label="Search">
    <input ref=${ref} role="combobox" aria-expanded="true" aria-controls="palette-list" aria-activedescendant=${all[sel] ? 'pal-' + all[sel].key : undefined} placeholder="Search pages, notebooks, queries, dashboards…" value=${q} onInput=${(e) => setQ(e.target.value)} onKeyDown=${onKey} />
    <ul id="palette-list" role="listbox">${all.length ? all.map((it, i) => html`<li id=${'pal-' + it.key} key=${it.key} role="option" aria-selected=${i === sel} onMouseEnter=${() => setSel(i)} onClick=${() => go(it)}><${Icon} name=${it.icon} /><span>${it.label}</span><span class="kind">${it.kind}</span></li>`) : html`<li class="muted" style="cursor:default">No matches</li>`}</ul></div></div>`;
}

/** Polls /v1/admin/health into the health store (falls back to the public liveness endpoint for non-admins). */
export function useHealthPolling() {
  usePoll(async () => {
    try {
      const r = await api.get('/admin/health');
      health.set({ status: r.status, components: r.components || [], checkedAt: r.checked_at });
    } catch (e) {
      if (e.status === 403) { try { const r = await api.get('/health'); health.set({ status: r.status, components: [{ name: 'api', status: r.status, detail: 'v' + r.version, category: 'control-plane' }], checkedAt: new Date().toISOString() }); } catch { /* ignore */ } }
    }
    return true;
  }, 20000, []);
}
