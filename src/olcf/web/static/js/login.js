// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Sign-in screen: local login, optional OIDC providers, first-run hint, forced password change, session-expiry notice.
import { html, useState, useEffect, useStore } from './core/h.js';
import { session } from './core/store.js';
import { t } from './core/i18n.js';
import { login, api, loadMe, setTokens, ApiError, describeError, routesMatching } from './api/client.js';
import { Btn, Input, Banner, Check, Field } from './ui/basic.js';
import { BrandMark, Icon } from './ui/icons.js';

function Fabric() {
  // A small lattice of nodes: the brand motif (a fabric of machines). Positions are fixed so the art is stable.
  const cols = 8; const rows = 5; const dx = 74; const dy = 70;
  const nodes = [];
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) nodes.push({ x: 40 + c * dx + (r % 2 ? dx / 2 : 0), y: 40 + r * dy, k: (r * 7 + c * 3) % 11 });
  const edges = [];
  const at = (r, c) => (r >= 0 && r < rows && c >= 0 && c < cols ? nodes[r * cols + c] : null);
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) {
    const n = at(r, c);
    const right = at(r, c + 1); if (right) edges.push([n, right]);
    const d1 = at(r + 1, c); const d2 = at(r + 1, r % 2 ? c + 1 : c - 1);
    if (d1) edges.push([n, d1]); if (d2) edges.push([n, d2]);
  }
  const col = (k) => (k === 3 ? 'var(--st-warn)' : k === 8 ? 'var(--st-off)' : 'var(--st-ok)');
  return html`<svg viewBox="0 0 640 380" class="login-art" aria-hidden="true">${edges.map(([a, b], i) => html`<line key=${i} x1=${a.x} y1=${a.y} x2=${b.x} y2=${b.y} />`)}
    ${nodes.map((n, i) => html`<g key=${i}><circle cx=${n.x} cy=${n.y} r="9" class="ring" /><circle cx=${n.x} cy=${n.y} r="4.2" fill=${col(n.k)} class=${'dot' + (n.k % 3 === 0 ? ' pulse' : '')} style=${`animation-delay:${(n.k * 0.37).toFixed(2)}s`} /></g>`)}</svg>`;
}

export function LoginScreen() {
  const s = useStore(session);
  const [user, setUser] = useState('admin');
  const [pw, setPw] = useState('');
  const [remember, setRemember] = useState(false);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [change, setChange] = useState(null);
  const [providers, setProviders] = useState([]);

  useEffect(() => {
    (async () => {
      try {
        // only a PUBLIC listing may be probed here: GET /v1/identity/providers is the administrators' list and answers 401 to an anonymous visitor (a console error on every first visit)
        const found = await routesMatching(/^\/v1\/(identity|auth)\/(oidc\/providers|providers\/public)$/);
        if (!found.length) return;
        const r = await fetch(found[0]);
        if (!r.ok) return;
        const b = await r.json();
        const list = Array.isArray(b) ? b : b.items || [];
        setProviders(list.filter((p) => p && (p.login_url || p.authorize_url || p.url)));
      } catch { /* optional feature */ }
    })();
  }, []);

  async function submit(e) {
    e.preventDefault();
    setBusy(true); setErr(null);
    try {
      const r = await login(user.trim(), pw, remember);
      if (r.must_change_password) { setChange({ current: pw, user: user.trim(), remember }); } else { await loadMe(); }
    } catch (ex) { setErr(ex); } finally { setBusy(false); }
  }
  if (change) return html`<${ChangePassword} current=${change.current} user=${change.user} remember=${change.remember} />`;
  const d = err ? describeError(err) : null;
  return html`<main class="login">
    <section class="login-art-panel" aria-hidden="true"><div class="login-brand"><${BrandMark} size=${36} /><div><b>OLCF</b><span>Open Lakehouse Compute Fabric</span></div></div>
      <${Fabric} /><div class="login-copy"><h2>Your lakehouse, on the compute you already own.</h2><p>Notebooks, SQL, pipelines and Microsoft Fabric jobs run on one fabric of machines you control. Every node is visible, every connection testable.</p></div></section>
    <section class="login-form-panel"><form class="login-card" onSubmit=${submit} aria-labelledby="login-h">
      <h1 id="login-h">${t('login.title')}</h1><p class="muted">to the OLCF control plane</p>
      ${s.expired ? html`<${Banner} kind="warn" title="Session ended">${t('session.expired')}<//>` : null}
      ${d ? html`<div class="mt-3"><${Banner} kind="bad" title=${err instanceof ApiError && err.status === 401 ? 'Sign-in failed' : d.title} remediation=${d.remediation}>${err instanceof ApiError && err.status === 401 ? 'The username or password is not correct.' : d.body}<//></div>` : null}
      <div class="mt-4"><${Input} label=${t('login.user')} value=${user} onInput=${setUser} autocomplete="username" autofocus=${true} required=${true} /></div>
      <${Input} label=${t('login.password')} type="password" value=${pw} onInput=${setPw} autocomplete="current-password" required=${true} />
      <div class="mb-4"><${Check} label=${t('login.remember')} checked=${remember} onChange=${setRemember} /></div>
      <${Btn} kind="primary" size="lg" type="submit" busy=${busy} class="w-full">Sign in<//>
      ${providers.length ? html`<div class="login-or"><span>or continue with</span></div>${providers.map((p) => html`<a key=${p.id || p.name} class="btn lg w-full" href=${p.login_url || p.authorize_url || p.url}><${Icon} name="key" />${p.display_name || p.name}</a>`)}` : null}
      <p class="small muted mt-4">First time here? The administrator account is created at first start. Its password is printed once in the server console and saved to <code>secrets/bootstrap-admin.txt</code> in the data directory.</p>
    </form></section></main>`;
}

function ChangePassword({ current, user, remember }) {
  const [a, setA] = useState('');
  const [b, setB] = useState('');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const mismatch = b && a !== b;
  async function go(e) {
    e.preventDefault();
    if (a !== b) return;
    setBusy(true); setErr(null);
    try { await api.post('/identity/password', { current_password: current, new_password: a }); await login(user, a, remember); await loadMe(); } catch (ex) { setErr(ex); } finally { setBusy(false); }
  }
  const d = err ? describeError(err) : null;
  return html`<main class="login"><section class="login-form-panel" style="grid-column:1/-1"><form class="login-card" onSubmit=${go}>
    <h1>Choose a new password</h1><p class="muted">Your administrator requires a new password before you continue.</p>
    ${d ? html`<${Banner} kind="bad" title=${d.title} remediation=${d.remediation}>${d.body}<//>` : null}
    <div class="mt-4"><${Input} label="New password" type="password" value=${a} onInput=${setA} autocomplete="new-password" hint="At least 12 characters." required=${true} /></div>
    <${Input} label="Repeat new password" type="password" value=${b} onInput=${setB} autocomplete="new-password" error=${mismatch ? 'The two passwords do not match.' : null} required=${true} />
    <${Btn} kind="primary" size="lg" type="submit" busy=${busy} disabled=${!a || mismatch}>Save and continue<//></form></section></main>`;
}
