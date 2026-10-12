// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Helpers shared by pages: workspace selection, item loading, the sample-lakehouse card and guided tour.
import { html, useState, useEffect, useRef, useAsync, useStore, useMemo } from '../core/h.js';
import { ui, prefs } from '../core/store.js';
import { useRoute, setQuery } from '../core/router.js';
import { api } from '../api/client.js';
import { Btn, Banner, Select, Card, Badge, ErrorBox, Loading, Steps, Progress, Kv, StatusBadge, Light } from '../ui/basic.js';
import { Icon } from '../ui/icons.js';
import { act, toast, openOverlay, Dialog } from '../ui/overlay.js';
import { itemHref, itemMeta } from '../core/links.js';
import { ago, fmtMs, sleep } from '../core/util.js';

let wsCache = null;
export function invalidateWorkspaces() { wsCache = null; }
export async function loadWorkspaces() {
  if (!wsCache) wsCache = api.get('/workspaces', { limit: 200 }).then((r) => r.items || []).catch((e) => { wsCache = null; throw e; });
  return wsCache;
}
export function useWorkspaces() {
  const st = useAsync(loadWorkspaces, []);
  return { ...st, items: st.data || [] };
}

/** The sample workspace (``olcf sample install``; "Sample Retail"), when it is installed. */
export const isSampleWorkspace = (w) => ['sample', 'sample retail'].includes(String(w.name || '').toLowerCase());

/**
 * The workspace a page starts in: the one the user chose last (persisted), else the sample workspace when it is installed (that is where the guided tour and the
 * examples live), else the first workspace. The fallback is not persisted, so installing the sample later moves a new user there and an explicit choice always wins.
 */
export function defaultWorkspaceId(items, stored) {
  if (stored && items.find((w) => w.id === stored)) return stored;
  const sample = items.find(isSampleWorkspace);
  return sample ? sample.id : items[0] ? items[0].id : null;
}

/** Selected workspace id (persisted). Falls back to the sample workspace, then the first one, when the stored one no longer exists or was never chosen. */
export function useWorkspaceId() {
  const id = useStore(ui, (s) => s.workspaceId);
  const { items } = useWorkspaces();
  const valid = defaultWorkspaceId(items, id);
  const set = (v) => { ui.set({ workspaceId: v }); prefs.set('workspace', v); };
  return [valid, set, items];
}
export function WorkspaceSelect({ value, onChange, items, label = 'Workspace', size }) {
  if (!items.length) return null;
  return html`<${Select} label=${label} size=${size} value=${value || ''} onChange=${onChange} options=${items.map((w) => ({ value: w.id, label: w.display_name || w.name }))} />`;
}
export function InlineWorkspaceSelect({ value, onChange, items }) {
  return html`<label class="row"><span class="small muted">Workspace</span><select class="select sm" style="width:auto;min-width:150px" value=${value || ''} onChange=${(e) => onChange(e.target.value)} aria-label="Workspace">${items.map((w) => html`<option key=${w.id} value=${w.id} selected=${w.id === value}>${w.display_name || w.name}</option>`)}</select></label>`;
}

export function useItems(workspaceId, type) {
  return useAsync(async () => {
    if (!workspaceId) return [];
    const r = await api.get(`/workspaces/${encodeURIComponent(workspaceId)}/items`, { type, limit: 500 });
    return r.items || [];
  }, [workspaceId, type]);
}

export const Ago = ({ ts }) => html`<time datetime=${ts} title=${ts}>${ago(ts)}</time>`;

/** Sample lakehouse status + install action. */
export function useSample() {
  const st = useAsync(() => api.get('/sample/status'), []);
  const [busy, setBusy] = useState(false);
  const install = async (size = 'standard') => {
    setBusy(true);
    const r = await act(() => api.post('/sample/install', { size }), { ok: 'Sample lakehouse installed' });
    setBusy(false);
    if (r) {
      invalidateWorkspaces(); st.reload();
      const wid = r.workspace_id || (await api.get('/sample/status').catch(() => ({}))).workspace_id;  // work in the sample workspace from here on
      if (wid) { ui.set({ workspaceId: wid }); prefs.set('workspace', wid); }
    }
    return r;
  };
  return { st, status: st.data, busy, install };
}

export function SampleCard({ compact }) {
  const { st, status, busy, install } = useSample();
  const [size, setSize] = useState('standard');
  if (st.loading && !status) return null;
  if (st.error) return null;
  if (status && status.installed) return html`<${SampleTour} status=${status} />`;
  return html`<section class="sample-hero"><div class="sample-art" aria-hidden="true"><svg viewBox="0 0 220 120"><g fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round">${[0, 1, 2, 3, 4, 5, 6, 7].map((i) => html`<rect key=${i} x=${14 + i * 24} y=${96 - (14 + ((i * 37) % 9) * 8)} width="14" height=${14 + ((i * 37) % 9) * 8} rx="3" fill="currentColor" opacity=${0.18 + (i % 4) * 0.12} stroke="none" />`)}<path d="M16 70 L50 52 L84 60 L118 34 L152 44 L186 22" /></g></svg></div>
    <div class="grow"><h2>Install the sample retail lakehouse</h2>
    <p>Creates a workspace with nine Delta tables (about 80,000 order lines), three notebooks, nine saved SQL queries and the Retail Sales Overview dashboard, so every part of the platform has something real to work on. ${compact ? '' : 'It takes a few seconds and can be removed by deleting the workspace.'}</p>
    <div class="row-wrap mt-3"><${Btn} kind="primary" busy=${busy} icon="download" onClick=${() => install(size)}>Install sample lakehouse<//>
      <label class="row small muted">Size <select class="select sm" style="width:auto" value=${size} onChange=${(e) => setSize(e.target.value)} aria-label="Sample size">${(status && status.sizes || ['small', 'standard']).map((s) => html`<option key=${s} value=${s} selected=${s === size}>${s}</option>`)}</select></label></div></div></section>`;
}

/** Guided tour shown once the sample is installed: open notebook, run it, open the dashboard, copy the MCP agent config. */
export function SampleTour({ status }) {
  const ids = useAsync(async () => {
    const r = await api.get(`/workspaces/${encodeURIComponent(status.workspace_id)}/items`, { limit: 100 });
    return r.items || [];
  }, [status.workspace_id]);
  const items = ids.data || [];
  const py = items.find((i) => i.type === 'notebook' && /explore_python/i.test(i.name));
  const etl = items.find((i) => i.type === 'notebook' && /etl/i.test(i.name));
  const rep = items.find((i) => i.type === 'report');
  const q = items.find((i) => i.type === 'sql_query');
  const steps = [
    { icon: 'notebook', title: 'Open the Python notebook', body: 'Explore the Delta tables with DuckDB, pandas and matplotlib.', href: py ? itemHref(py) : null },
    { icon: 'play-all', title: 'Run it', body: 'Open the notebook and choose Run all: a real kernel on your compute node executes every cell.', href: py ? itemHref(py) + '?run=all' : null },
    { icon: 'chart', title: 'Open the dashboard', body: 'Retail Sales Overview: KPIs, trends, channel mix and a region by weekday heatmap.', href: rep ? itemHref(rep) : null },
    { icon: 'terminal', title: 'Query with SQL', body: 'Nine saved queries ready to run against retail_lakehouse.', href: q ? itemHref(q) : '#/sql' },
    { icon: 'bot', title: 'Connect an AI agent', body: 'Copy the MCP configuration for Claude Desktop or Claude Code.', href: '#/agents' },
  ];
  return html`<${Card} title="Sample retail lakehouse is ready" sub=${`${status.workspace} · ${status.tables ? status.tables.length : 0} tables`} actions=${html`<${Badge} kind="ok">Installed<//>`}>
    <ol class="tour">${steps.map((s, i) => html`<li key=${i}><a class=${'tour-step' + (s.href ? '' : ' disabled')} href=${s.href || undefined}><span class="tour-n">${i + 1}</span><span class="tour-ic"><${Icon} name=${s.icon} /></span><span class="grow"><b>${s.title}</b><span class="small muted">${s.body}</span></span><${Icon} name="chevron-right" size="sm" /></a></li>`)}</ol></${Card}>`;
}


// ------------------------------------------------------------------ shared dialogs
/** Run the active end-to-end test of a health component (POST /admin/health/checks/{name}/test) and show its steps. */
export function openHealthTest(name) { return openOverlay(({ close }) => html`<${HealthTestDialog} name=${name} close=${close} />`); }
function HealthTestDialog({ name, close }) {
  const r = useAsync(() => api.post(`/admin/health/checks/${encodeURIComponent(name)}/test`), [name]);
  const d = r.data;
  return html`<${Dialog} title=${`Test: ${name}`} size="wide" onClose=${() => close()} footer=${html`<${Btn} onClick=${() => r.reloadLoud()}>Run again<//><${Btn} kind="primary" onClick=${() => close()}>Close<//>`}>
    ${r.loading ? html`<div class="loading"><span class="spinner lg"></span><span>Running the end-to-end test…</span></div>` : r.error ? html`<${ErrorBox} error=${r.error} />` : html`
      <${Banner} kind=${d.status === 'ok' ? 'ok' : d.status === 'degraded' || d.status === 'unknown' ? 'warn' : 'bad'} title=${d.status === 'ok' ? 'Everything checked out' : d.status === 'unknown' ? 'Nothing to test yet' : d.status === 'degraded' ? 'Works, with problems' : 'The test failed'}>${d.detail}${d.latency_ms != null ? ` (${fmtMs(d.latency_ms)})` : ''}<//>
      ${(d.steps || []).length ? html`<div class="mt-4"><${Steps} steps=${d.steps.map((s) => ({ name: s.step || s.name, ok: s.ok, detail: s.detail, ms: s.ms, remediation: s.remediation }))} /></div>` : null}`}</${Dialog}>`;
}

/** Follow a long-running operation (GET /operations/{id}) with a progress bar until it ends. Resolves with the final record. */
export function openOperation(oid, title) { return openOverlay(({ close }) => html`<${OperationDialog} oid=${oid} title=${title} close=${close} />`); }
function OperationDialog({ oid, title, close }) {
  const [op, setOp] = useState(null);
  const [err, setErr] = useState(null);
  const last = useRef(null);
  useEffect(() => {
    let stop = false;
    (async () => {
      for (let i = 0; i < 4000 && !stop; i += 1) {
        try { const o = await api.get(`/operations/${encodeURIComponent(oid)}`); last.current = o; setOp(o); if (o.state !== 'running') return; } catch (e) { if (!stop) setErr(e); return; }
        await sleep(i < 20 ? 700 : 2000);
      }
    })();
    return () => { stop = true; };
  }, [oid]);
  const running = !op || op.state === 'running';
  const res = op && op.result;
  const nodes = (res && res.nodes) || [];
  return html`<${Dialog} title=${title} size="wide" dismissable=${!running} onClose=${() => close(last.current)} footer=${html`<${Btn} kind="primary" disabled=${running} onClick=${() => close(last.current)}>${running ? 'Working…' : 'Close'}<//>`}>
    ${err ? html`<${ErrorBox} error=${err} />` : null}
    <div class="row mb-2"><${StatusBadge} state=${running ? 'running' : op.state} label=${running ? 'In progress' : op.state === 'succeeded' ? 'Finished' : 'Failed'} /><span class="muted small">${op ? op.message : 'Starting…'}</span></div>
    <${Progress} value=${op ? op.progress : 0} indeterminate=${running && (!op || !op.progress)} kind=${op && op.state === 'failed' ? 'bad' : op && op.state === 'succeeded' ? 'ok' : ''} label=${title} />
    ${nodes.length ? html`<div class="mt-4"><${Steps} steps=${nodes.map((n) => ({ name: n.name, ok: n.ok, detail: n.ok ? (n.state ? `state: ${n.state}` : 'done') + (n.data && n.data.detail ? ` · ${n.data.detail}` : '') : n.error }))} /></div>` : null}
    ${op && op.error && !nodes.length ? html`<div class="mt-4"><${Banner} kind="bad" title="The operation failed">${op.error.message}<//></div>` : null}</${Dialog}>`;
}

/** State kept in the URL query (shareable tabs, filters). */
export function useQueryState(key, dflt = '') {
  const loc = useRoute();
  const v = loc.query[key] != null ? loc.query[key] : dflt;
  const set = (x) => setQuery({ [key]: x === dflt || x === '' ? null : x });
  return [v, set];
}
