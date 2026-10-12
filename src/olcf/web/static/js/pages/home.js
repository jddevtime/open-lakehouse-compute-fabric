// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Home / overview: platform health lights, key figures, first-run checklist, live activity, recent jobs and Fabric submissions.
import { html, useStore, useAsync, usePoll, useState, useEffect } from '../core/h.js';
import { health, session } from '../core/store.js';
import { activity, describeEvent } from '../core/events.js';
import { api } from '../api/client.js';
import { PageHead, Card, Light, Badge, StatusBadge, Btn, Empty, ErrorBox, Progress } from '../ui/basic.js';
import { StatTile } from '../charts/Chart.js';
import { Icon } from '../ui/icons.js';
import { Table } from '../ui/data.js';
import { SampleCard, useSample, Ago } from './common.js';
import { fmtNum, titleCase, worstState, lightState, fmtDuration, durationBetween, bus } from '../core/util.js';
import { jobHref } from '../core/links.js';

const CAT = { 'control-plane': 'Control plane', compute: 'Compute', data: 'Data and storage', storage: 'Data and storage', integration: 'Integrations', items: 'Workspaces and items', security: 'Security', agents: 'Agents' };

function HealthGrid() {
  const h = useStore(health);
  const groups = {};
  h.components.forEach((c) => { (groups[CAT[c.category] || titleCase(c.category || 'other')] ||= []).push(c); });
  const order = ['Control plane', 'Compute', 'Data and storage', 'Integrations', 'Workspaces and items', 'Security'];
  const names = Object.keys(groups).sort((a, b) => (order.indexOf(a) + 1 || 99) - (order.indexOf(b) + 1 || 99));
  if (!h.components.length) return html`<div class="muted">Checking components</div>`;
  return html`<div class="hgroups">${names.map((g) => html`<div class="hgroup" key=${g}><h4>${g}</h4>${groups[g].map((c) => html`<div class="hrow" key=${c.name}><${Light} state=${c.status} pulse=${true} /><div class="grow" style="min-width:0"><div class="name">${c.name}</div><div class="detail" title=${c.detail}>${c.detail}</div></div></div>`)}</div>`)}</div>`;
}

function Figures({ topo, jobs, failed, fabric }) {
  const s = topo && topo.summary;
  const online = s ? `${s.nodes_online}` : '–';
  const coresPct = s && s.cores_total ? Math.round((s.cores_used / s.cores_total) * 100) : 0;
  const nodesState = !s ? 'unknown' : s.nodes_total === 0 ? 'down' : s.nodes_online < s.nodes_total ? 'degraded' : 'ok';
  return html`<div class="grid c4" style="grid-template-columns:repeat(auto-fit,minmax(210px,1fr))">
    <${StatTile} href="#/compute" label="Workers online" value=${html`${online}<span class="muted" style="font-size:16px;font-weight:500"> / ${s ? s.nodes_total : '–'}</span>`} sub=${s && s.nodes_total === 0 ? 'No compute node has enrolled yet' : s ? `${fmtNum(s.memory_total_mb / 1024, 1)} GB memory across the fleet` : null} state=${nodesState} />
    <${StatTile} href="#/compute" label="Cores in use" value=${html`${s ? s.cores_used : '–'}<span class="muted" style="font-size:16px;font-weight:500"> / ${s ? s.cores_total : '–'}</span>`} sub=${html`<${Progress} value=${coresPct} kind=${coresPct > 90 ? 'bad' : coresPct > 75 ? 'warn' : ''} label="Cores in use" />`} />
    <${StatTile} href="#/jobs" label="Jobs running" value=${s ? s.jobs_running : '–'} sub=${s ? `${s.jobs_queued} queued · ${s.executors || 0} executors` : null} />
    <${StatTile} href="#/jobs?state=failed" label="Failed in 24 hours" value=${failed == null ? '–' : fmtNum(failed)} sub=${failed === 0 ? 'No failures' : 'Open Jobs to investigate'} state=${failed == null ? null : failed ? 'down' : 'ok'} />
    <${StatTile} href="#/fabric" label="Fabric submissions (24 h)" value=${fabric ? fmtNum(fabric.counters.received) : '–'} sub=${fabric && fabric.counters.received ? `${fabric.counters.succeeded} succeeded · ${fabric.counters.failed} failed` : 'None received yet'} /></div>`;
}

function Checklist({ topo }) {
  const { status } = useSample();
  const fabric = useAsync(() => api.get('/integrations/fabric').catch(() => null), []);
  const tokens = useAsync(() => api.get('/identity/tokens').catch(() => ({ items: [] })), []);
  const runtimeReady = topo && topo.nodes.some((n) => (n.runtimes || []).some((r) => r.state === 'ready' && r.verified && !r.no_spark)); // the built-in host-Python runtime has no Spark
  const nodesOnline = topo && topo.summary.nodes_online > 0;
  const fabricOk = fabric.data && fabric.data.readiness && fabric.data.readiness.filter((r) => r.required).every((r) => r.ok);
  const steps = [
    { id: 'sample', done: !!(status && status.installed), title: 'Install the sample lakehouse', body: 'Tables, notebooks, queries and a dashboard to try everything with.', href: '#/', cta: 'Install' },
    { id: 'node', done: !!nodesOnline, title: 'Connect a compute node', body: 'Enrol a machine to run notebooks, SQL and jobs.', href: '#/compute?add=1', cta: 'Add a machine' },
    { id: 'runtime', done: !!runtimeReady, title: 'Install a Spark runtime (optional)', body: 'Python notebooks, SQL and dashboards already work. Spark notebooks and jobs need a Spark runtime on at least one node.', href: '#/runtimes', cta: 'Open runtimes' },
    { id: 'fabric', done: !!fabricOk, title: 'Connect Microsoft Fabric', body: 'Allow your tenant, map workspaces, then test the connection.', href: '#/fabric', cta: 'Set up' },
    { id: 'token', done: !!(tokens.data && tokens.data.items && tokens.data.items.length), title: 'Create an access token', body: 'For the CLI, the Fabric SDK and AI agents.', href: '#/security?tab=tokens', cta: 'Create token' },
  ];
  const done = steps.filter((s) => s.done).length;
  return html`<${Card} title="Getting started" sub=${`${done} of ${steps.length} complete`} actions=${html`<div style="width:110px"><${Progress} value=${(done / steps.length) * 100} kind=${done === steps.length ? 'ok' : ''} label="Setup progress" /></div>`}>
    <ul class="checklist">${steps.map((s) => html`<li key=${s.id} class=${s.done ? 'done' : ''}><span class="tick"><${Icon} name="check" size="sm" /></span><div class="grow"><b>${s.title}</b><span class="small muted">${s.body}</span></div>${s.done ? html`<${Badge} kind="ok">Done<//>` : html`<${Btn} size="sm" href=${s.href}>${s.cta}<//>`}</li>`)}</ul></${Card}>`;
}

function Feed() {
  const a = useStore(activity);
  const initial = useAsync(async () => (await api.get('/events', { limit: 15, descending: true })).items, []);
  const items = a.items.length ? [...a.items, ...(initial.data || []).filter((e) => !a.items.find((x) => x.seq === e.seq))] : (initial.data || []);
  return html`<${Card} title="Recent activity" actions=${html`<${Badge} kind=${a.status === 'open' ? 'ok' : 'off'}>${a.status === 'open' ? 'Live' : 'Reconnecting'}<//>`} flush>
    ${items.length ? html`<ul class="feed" style="padding:4px 16px;max-height:400px;overflow:auto">${items.slice(0, 30).map((e, i) => { const d = describeEvent(e); return html`<li key=${e.seq || i}><span class=${'ic ' + (d.kind === 'succeeded' ? 'succeeded' : d.kind === 'failed' ? 'failed' : d.kind === 'warn' ? 'warn' : d.kind === 'running' ? 'running' : '')}><${Icon} name=${d.icon} size="sm" /></span>${d.link ? html`<a href=${d.link} class="grow" style="color:var(--ink);text-decoration:none">${d.text}</a>` : html`<span class="grow">${d.text}</span>`}<span class="when"><${Ago} ts=${e.ts} /></span></li>`; })}</ul>` : html`<${Empty} icon="activity" title="Nothing yet">Events from jobs, nodes and alerts appear here as they happen.<//>`}</${Card}>`;
}

function RecentJobs() {
  const jobs = usePoll(() => api.get('/jobs', { limit: 8 }), 6000, []);
  useEffect(() => bus.on('event:job', () => jobs.reload()), []);
  const rows = (jobs.data && jobs.data.items) || [];
  return html`<${Card} title="Recent jobs" actions=${html`<${Btn} size="sm" kind="ghost" href="#/jobs">All jobs<//>`} flush>
    ${rows.length ? html`<${Table} dense=${true} columns=${[
      { key: 'name', label: 'Job', render: (j) => html`<a href=${jobHref(j.id)} class="cell-main" style="color:var(--ink)">${j.name || j.id}</a><div class="cell-sub">${titleCase(j.kind)} · ${j.origin && j.origin.kind}</div>`, sortable: false },
      { key: 'state', label: 'State', render: (j) => html`<${StatusBadge} state=${j.state} />`, sortable: false },
      { key: 'dur', label: 'Duration', num: true, render: (j) => fmtDuration(durationBetween(j.started_at || j.created_at, j.finished_at)), sortable: false },
      { key: 'when', label: 'Submitted', render: (j) => html`<${Ago} ts=${j.created_at} />`, sortable: false },
    ]} rows=${rows} />` : html`<${Empty} icon="play-all" title="No jobs yet">Run a notebook or submit a script to see it here.<//>`}</${Card}>`;
}

function RecentFabric() {
  const r = usePoll(() => api.get('/integrations/fabric/submissions', { limit: 6 }).catch(() => ({ items: [] })), 8000, []);
  const rows = (r.data && r.data.items) || [];
  return html`<${Card} title="Recent submissions from Fabric" actions=${html`<${Btn} size="sm" kind="ghost" href="#/fabric">Open Fabric<//>`} flush>
    ${rows.length ? html`<${Table} dense=${true} columns=${[
      { key: 'nb', label: 'Notebook', render: (s) => html`<div class="cell-main">${s.notebook_name || s.notebook_id || 'notebook'}</div><div class="cell-sub">${s.fabric_workspace_name || s.fabric_workspace_id || ''}</div>`, sortable: false },
      { key: 'st', label: 'State', render: (s) => html`<${StatusBadge} state=${s.state} />`, sortable: false },
      { key: 'w', label: 'Received', render: (s) => html`<${Ago} ts=${s.received_at} />`, sortable: false },
    ]} rows=${rows} />` : html`<${Empty} icon="cloud" title="No submissions yet">When a Fabric notebook calls the OLCF SDK, the run shows up here with exactly what Fabric sent.<//>`}</${Card}>`;
}

export function HomePage() {
  const user = useStore(session, (s) => s.user);
  const topo = usePoll(() => api.get('/compute/topology').catch(() => null), 5000, []);
  const failed = usePoll(async () => {
    try {
      const since = new Date(Date.now() - 864e5).toISOString();
      const r = await api.get('/jobs', { state: ['failed', 'timed_out'], since, limit: 200 });
      return (r.items || []).length;
    } catch { return null; }
  }, 15000, []);
  const fabric = usePoll(() => api.get('/integrations/fabric/metrics').catch(() => null), 15000, []);
  const hour = new Date().getHours();
  const greet = hour < 5 ? 'Working late' : hour < 12 ? 'Good morning' : hour < 18 ? 'Good afternoon' : 'Good evening';
  return html`<div class="page">
    <${PageHead} title=${`${greet}${user ? ', ' + user.name : ''}`} sub="Everything about your lakehouse compute fabric at a glance." actions=${html`<${Btn} icon="server" href="#/compute">Compute panel<//><${Btn} kind="primary" icon="notebook" href="#/workspaces">Open workspaces<//>`} />
    <${Figures} topo=${topo.data} failed=${failed.data} fabric=${fabric.data} />
    <div class="split mt-4" style="grid-template-columns:minmax(0,1.35fr) minmax(0,1fr);align-items:start"><${SampleCard} /><${Checklist} topo=${topo.data} /></div>
    <div class="split mt-4" style="grid-template-columns:minmax(0,1.5fr) minmax(0,1fr);align-items:start"><${Card} title="Platform health" sub=${`${health.get().components.length} components`}><${HealthGrid} /></${Card}><${Feed} /></div>
    <div class="split mt-4" style="grid-template-columns:minmax(0,1fr) minmax(0,1fr)"><${RecentJobs} /><${RecentFabric} /></div>
    <${Card} title="Quick actions" class="mt-4"><div class="quick">
      <a href="#/workspaces"><${Icon} name="notebook" />New notebook</a><a href="#/sql"><${Icon} name="terminal" />Open SQL editor</a><a href="#/lakehouse"><${Icon} name="database" />Browse the lakehouse</a>
      <a href="#/compute?add=1"><${Icon} name="server" />Add a machine</a><a href="#/fabric"><${Icon} name="cloud" />Test Fabric connection</a><a href="#/agents"><${Icon} name="bot" />Connect an AI agent</a></div></${Card}></div>`;
}
