// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Application entry point: boots theme, session, router, shell and global services (health polling, event stream).
import { html, render, useEffect, useState, useStore } from './js/core/h.js';
import { ui, session } from './js/core/store.js';
import { bootstrapSession, loadStoredAuth } from './js/api/client.js';
import { applyTheme, Rail, Topbar, Palette, useHealthPolling } from './js/shell.js';
import { LoginScreen } from './js/login.js';
import { Outlet, useRoute } from './js/core/router.js';
import { routes } from './js/routes.js';
import { OverlayHost, Toasts } from './js/ui/overlay.js';
import { startEvents, stopEvents } from './js/core/events.js';
import { Loading } from './js/ui/basic.js';

applyTheme(ui.get().theme);

function Shell() {
  const loc = useRoute();
  const rail = useStore(ui, (s) => s.rail);
  const [pal, setPal] = useState(false);
  useHealthPolling();
  useEffect(() => {
    startEvents();
    const key = (e) => { if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') { e.preventDefault(); setPal(true); } };
    window.addEventListener('keydown', key);
    return () => { stopEvents(); window.removeEventListener('keydown', key); };
  }, []);
  return html`<div class="app" data-rail=${rail}>
    <a class="skip-link" href="#main">Skip to content</a>
    <${Rail} /><${Topbar} onSearch=${() => setPal(true)} />
    <main class="main" id="main" tabindex="-1"><${Outlet} routes=${routes} loc=${loc} /></main>
    ${pal ? html`<${Palette} onClose=${() => setPal(false)} />` : null}</div>`;
}

function Root() {
  const s = useStore(session);
  return html`${!s.ready ? html`<${Loading} label="Starting" />` : s.user ? html`<${Shell} />` : html`<${LoginScreen} />`}<${OverlayHost} /><${Toasts} />`;
}

loadStoredAuth();
render(html`<${Root} />`, document.getElementById('root'));
bootstrapSession();
