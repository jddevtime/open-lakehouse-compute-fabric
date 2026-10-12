// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Compute control panel: fleet map of machines, Spark clusters, control plane health, queues and pools, runtimes, connectivity tests.
import { html, useState, useEffect, useMemo, useAsync, usePoll } from '../core/h.js';
import { api, can, getAccessToken } from '../api/client.js';
import { prefs } from '../core/store.js';
import { setQuery, parseHash } from '../core/router.js';
import { PageHead, Card, Btn, Badge, StatusBadge, Light, Banner, Empty, ErrorBox, Loading, Tabs, TabPanel, Seg, Select, Input, Gauge, Sparkline, Progress, Kv, Async } from '../ui/basic.js';
import { Icon } from '../ui/icons.js';
import { Table } from '../ui/data.js';
import { MenuButton, confirmDialog, act, toast, openOverlay, Dialog } from '../ui/overlay.js';
import { openHealthTest, openOperation } from './common.js';
import { NodeDrawer, openNodeTest, openClusterTest, openSmoke, openAddMachine, editLabels, assignPool, openPoolDialog, openQueueDialog } from './compute_dialogs.js';
import { RuntimesPanel } from './runtimes.js';
import { fmtNum, fmtMb, fmtPct, fmtDuration, ago, titleCase, lightState, worstState, bus } from '../core/util.js';
import { jobHref } from '../core/links.js';

const SCHED = { active: null, cordoned: ['warn', 'Cordoned'], draining: ['warn', 'Draining'] };
const COMP_SHORT = { agent: 'agent', 'control-plane-link': 'link', disk: 'disk', clock: 'clock' };
const shortComp = (n) => COMP_SHORT[n] || (n.startsWith('runtime:') ? 'rt ' + n.slice(8).replace(/^olcf-rt-/, '') : n);

/** Executors per node derived from the applications of every Spark cluster. */
function executorsByNode(clusters) {
  const m = {};
  (clusters || []).forEach((c) => (c.master.applications || []).forEach((a) => (a.executors || []).forEach((e) => { if (e.worker) m[e.worker] = (m[e.worker] || 0) + 1; })));
  return m;
}

// ------------------------------------------------------------------ node actions
function nodeActions(n, { reload, pools }) {
  const id = encodeURIComponent(n.id);
  const sched = async (verb, past, body) => {
    const r = await act(() => api.post(`/workers/${id}/${verb}`, { reason: 'Requested from the console' }), { ok: `${n.name} ${past}` });
    if (r) reload();
  };
  return [
    { label: 'Test connectivity', icon: 'activity', onClick: () => openNodeTest(n) },
    { sep: true },
    n.scheduling === 'active' ? { label: 'Cordon: stop new work', icon: 'pause', disabled: n.state === 'revoked', onClick: async () => { if (await confirmDialog({ title: `Cordon ${n.name}?`, confirmLabel: 'Cordon', body: 'No new jobs or sessions will be placed on this node. Work that is already running continues.' })) sched('cordon', 'cordoned'); } } : { label: 'Uncordon: accept work again', icon: 'play', onClick: () => sched('uncordon', 'accepts work again') },
    { label: 'Drain: finish, then stop', icon: 'minus', disabled: n.scheduling === 'draining' || n.state === 'revoked', onClick: async () => { if (await confirmDialog({ title: `Drain ${n.name}?`, confirmLabel: 'Drain', danger: true, body: 'Running work finishes, nothing new is placed here, and the node is released once it is idle. Use this before maintenance.' })) sched('drain', 'is draining'); } },
    { sep: true },
    { label: 'Edit labels', icon: 'tag', onClick: () => editLabels(n, reload) },
    { label: 'Move to a pool', icon: 'layers', onClick: () => assignPool(n, pools, reload) },
    { sep: true },
    n.state === 'revoked'
      ? { label: 'Delete node record', icon: 'trash', danger: true, onClick: async () => { if (await confirmDialog({ title: `Delete ${n.name}?`, danger: true, confirmLabel: 'Delete', body: 'The record and its history are removed. The machine can enrol again with a new token.' })) { const r = await act(() => api.del(`/workers/${id}`), { ok: 'Node deleted' }); if (r !== undefined) reload(); } } }
      : { label: 'Revoke access', icon: 'shield', danger: true, onClick: async () => { if (await confirmDialog({ title: `Revoke ${n.name}?`, danger: true, confirmLabel: 'Revoke', body: 'The node is disconnected and its credential stops working. Its running work fails over according to the retry policy. To bring the machine back, enrol it again.' })) sched('revoke', 'revoked'); } },
  ];
}

// ------------------------------------------------------------------ summary + queues
function SummaryBar({ topo }) {
  const s = topo.summary;
  const memPct = s.memory_total_mb ? (s.memory_used_mb / s.memory_total_mb) * 100 : 0;
  const corePct = s.cores_total ? (s.cores_used / s.cores_total) * 100 : 0;
  const nodeState = s.nodes_total === 0 ? 'unknown' : s.nodes_online === s.nodes_total ? 'ok' : s.nodes_online === 0 ? 'down' : 'degraded';
  const cell = (label, value, sub, extra) => html`<div class="sum-cell"><div class="sum-label">${label}</div><div class="sum-value">${value}</div>${sub}${extra}</div>`;
  return html`<section class="sum-bar" aria-label="Fleet summary">
    ${cell('Nodes online', html`<${Light} state=${nodeState} size="lg" glyph=${true} /> ${s.nodes_online}<span class="of"> / ${s.nodes_total}</span>`, html`<div class="sum-sub">${s.nodes_total - s.nodes_online > 0 ? `${s.nodes_total - s.nodes_online} not connected` : s.nodes_total ? 'all connected' : 'none enrolled yet'}</div>`)}
    ${cell('Cores in use', html`${s.cores_used}<span class="of"> / ${s.cores_total}</span>`, html`<${Progress} value=${corePct} kind=${corePct > 90 ? 'bad' : corePct > 75 ? 'warn' : ''} label="Cores in use" />`)}
    ${cell('Memory reserved', html`${fmtMb(s.memory_used_mb)}<span class="of"> / ${fmtMb(s.memory_total_mb)}</span>`, html`<${Progress} value=${memPct} kind=${memPct > 90 ? 'bad' : memPct > 75 ? 'warn' : ''} label="Memory reserved" />`)}
    ${cell('Jobs', html`${s.jobs_running}<span class="of"> running</span>`, html`<div class="sum-sub">${s.jobs_queued} queued</div>`)}
    ${cell('Executors', fmtNum(s.executors), html`<div class="sum-sub">across Spark clusters</div>`)}
    ${cell('Sessions', fmtNum(s.sessions_active), html`<div class="sum-sub">notebook kernels</div>`)}</section>`;
}

function QueueStrip({ queues }) {
  if (!queues.length) return null;
  return html`<ul class="queue-strip" aria-label="Queues">${queues.map((q) => html`<li key=${q.name} class=${q.paused ? 'paused' : q.depth ? 'busy' : ''}><${Icon} name="route" size="sm" /><b>${q.name}</b><span><b class="n">${q.depth}</b> waiting</span><span><b class="n">${q.running}</b> running</span>${q.depth ? html`<span class="muted">oldest ${fmtDuration(q.oldest_wait_seconds)}</span>` : null}${q.paused ? html`<${Badge} kind="warn">Paused<//>` : null}</li>`)}</ul>`;
}

// ------------------------------------------------------------------ fleet
function Lamps({ comps }) {
  return html`<ul class="lamps" aria-label="Component status">${(comps || []).map((c) => html`<li key=${c.name} title=${`${c.name}: ${c.detail || c.status}`}><${Light} state=${c.status} label=${`${c.name}: ${c.status}${c.detail ? ', ' + c.detail : ''}`} /><span>${shortComp(c.name)}</span></li>`)}</ul>`;
}
function LastTest({ n }) {
  const t = n.notes && n.notes.last_test;
  if (!t) return html`<span class="muted small">Never tested</span>`;
  return html`<span class=${'small ' + (t.ok ? 'ok-ink' : 'bad-ink')}><${Icon} name=${t.ok ? 'check-circle' : 'alert-circle'} size="sm" /> Test ${t.ok ? 'passed' : `failed: ${(t.failed || []).map(shortComp).join(', ')}`} ${ago(t.at)}</span>`;
}

function NodeCard({ n, exec, onOpen, ctx }) {
  const m = n.metrics || {};
  const r = n.resources;
  const corePct = r.cores ? (r.cores_allocated / r.cores) * 100 : 0;
  const sch = SCHED[n.scheduling];
  const work = [n.running_jobs.length && `${n.running_jobs.length} job${n.running_jobs.length === 1 ? '' : 's'}`, n.sessions.length && `${n.sessions.length} session${n.sessions.length === 1 ? '' : 's'}`, exec && `${exec} executor${exec === 1 ? '' : 's'}`].filter(Boolean);
  const offline = n.state === 'offline' || n.state === 'revoked' || n.state === 'pending';
  return html`<article class=${`node ${lightState(n.state)} ${n.scheduling !== 'active' ? 'sched' : ''}`} aria-label=${`Node ${n.name}, ${n.state}`}>
    <header class="node-head"><${Light} state=${n.state} size="xl" glyph=${true} pulse=${!offline} label=${`${n.name} is ${n.state}`} />
      <div class="grow" style="min-width:0"><h3><button class="linklike" onClick=${onOpen} title="Open node details">${n.name}</button></h3><div class="small muted trunc" title=${`${n.hostname} · ${n.address}`}>${n.hostname} · ${n.os}/${n.arch}</div></div>
      <${MenuButton} label=${`Actions for ${n.name}`} items=${nodeActions(n, ctx)} /></header>
    <div class="chips node-tags"><${Badge} kind="outline" title="Worker pool"><${Icon} name="layers" size="sm" /> ${n.pool}<//>${sch ? html`<${Badge} kind=${sch[0]}>${sch[1]}<//>` : null}${n.state !== 'online' ? html`<${StatusBadge} state=${n.state} />` : null}${n.embedded ? html`<${Badge}>Embedded<//>` : null}${Object.entries(n.labels || {}).slice(0, 3).map(([k, v]) => html`<span class="tag" key=${k}>${k}=${v}</span>`)}</div>
    ${offline ? html`<div class="node-off"><${Icon} name="wifi" /> ${n.state === 'revoked' ? 'Access revoked' : n.state === 'pending' ? 'Waiting for the first connection' : `Not connected. Last heartbeat ${n.heartbeat_age_seconds != null ? fmtDuration(n.heartbeat_age_seconds) + ' ago' : 'never'}.`}</div>` : html`
      <div class="node-gauges"><${Gauge} label="CPU" value=${m.cpu_percent} text=${fmtPct(m.cpu_percent)} /><${Gauge} label="Memory" value=${m.mem_percent} text=${fmtPct(m.mem_percent)} hint=${`${fmtMb(r.memory_mb)} total`} /><${Gauge} label="Disk" value=${m.disk_percent} text=${fmtPct(m.disk_percent)} warn=${85} bad=${95} /><${Gauge} label="Cores reserved" value=${corePct} text=${`${r.cores_allocated} / ${r.cores}`} warn=${101} /></div>
      <div class="node-spark"><div><span class="small muted">CPU</span><${Sparkline} values=${n.history.cpu} min=${0} max=${100} label=${`${n.name} CPU`} /></div><div><span class="small muted">Memory</span><${Sparkline} values=${n.history.mem} min=${0} max=${100} label=${`${n.name} memory`} /></div></div>`}
    <${Lamps} comps=${n.components} />
    <footer class="node-foot"><div class="small">${work.length ? html`<b>${work.join(' · ')}</b>` : html`<span class="muted">Idle</span>`}</div><${LastTest} n=${n} /></footer>
    <div class="node-btns"><${Btn} size="sm" icon="activity" disabled=${offline} onClick=${() => openNodeTest(n)}>Test<//><${Btn} size="sm" kind="ghost" onClick=${onOpen}>Details<//>
      ${n.scheduling === 'active' ? null : html`<${Btn} size="sm" icon="play" onClick=${() => nodeActions(n, ctx).find((x) => x.label && x.label.startsWith('Uncordon'))?.onClick()}>Accept work<//>`}</div>
  </article>`;
}

function NodeRows({ nodes, execs, onOpen, ctx }) {
  return html`<${Table} caption="Nodes" dense=${true} onRowClick=${onOpen} rowKey=${(n) => n.id} initialSort=${{ key: 'name', dir: 1 }} columns=${[
    { key: 'state', label: 'State', sort: (n) => ({ down: 0, degraded: 1, unknown: 2, busy: 3, ok: 4 }[lightState(n.state)]), render: (n) => html`<span class="row"><${Light} state=${n.state} glyph=${true} size="lg" label=${n.state} /><span>${titleCase(n.state)}</span></span>` },
    { key: 'name', label: 'Node', render: (n) => html`<b>${n.name}</b> <span class="muted small">${n.hostname}</span>` },
    { key: 'pool', label: 'Pool' },
    { key: 'scheduling', label: 'Scheduling', render: (n) => (SCHED[n.scheduling] ? html`<${Badge} kind="warn">${SCHED[n.scheduling][1]}<//>` : 'Active') },
    { key: 'cpu', label: 'CPU', num: true, sort: (n) => n.metrics.cpu_percent, render: (n) => html`<div class="cell-meter"><${Progress} value=${n.metrics.cpu_percent} kind=${n.metrics.cpu_percent > 90 ? 'bad' : n.metrics.cpu_percent > 75 ? 'warn' : ''} label=${`${n.name} CPU`} /><span>${fmtPct(n.metrics.cpu_percent)}</span></div>` },
    { key: 'mem', label: 'Memory', num: true, sort: (n) => n.metrics.mem_percent, render: (n) => html`<div class="cell-meter"><${Progress} value=${n.metrics.mem_percent} kind=${n.metrics.mem_percent > 90 ? 'bad' : n.metrics.mem_percent > 75 ? 'warn' : ''} label=${`${n.name} memory`} /><span>${fmtPct(n.metrics.mem_percent)}</span></div>` },
    { key: 'disk', label: 'Disk', num: true, sort: (n) => n.metrics.disk_percent, render: (n) => fmtPct(n.metrics.disk_percent) },
    { key: 'cores', label: 'Cores', num: true, sort: (n) => n.resources.cores, render: (n) => `${n.resources.cores_allocated} / ${n.resources.cores}` },
    { key: 'work', label: 'Work', sort: (n) => n.running_jobs.length + n.sessions.length, render: (n) => `${n.running_jobs.length} jobs, ${n.sessions.length} sessions${execs[n.id] ? `, ${execs[n.id]} exec` : ''}` },
    { key: 'comps', label: 'Components', sortable: false, render: (n) => html`<span class="dots">${(n.components || []).map((c) => html`<${Light} key=${c.name} state=${c.status} label=${`${c.name}: ${c.status}`} />`)}</span>` },
    { key: 'rt', label: 'Runtimes', sortable: false, render: (n) => (n.runtimes || []).map((r) => html`<span class=${'badge ' + (r.state === 'ready' && r.verified ? 'ok' : 'down')} key=${r.id} title=${`${r.id}: ${r.state}${r.no_spark ? ' (Python only, no Spark)' : ''}`}>${r.id.replace(/^olcf-rt-/, '').replace(/^olcf-host-python$/, 'host-python')}</span>`) },
    { key: 'hb', label: 'Heartbeat', num: true, sort: (n) => n.heartbeat_age_seconds, render: (n) => (n.heartbeat_age_seconds != null ? fmtDuration(n.heartbeat_age_seconds) : '–') },
    { key: 'act', label: '', sortable: false, render: (n) => html`<${MenuButton} label=${`Actions for ${n.name}`} items=${nodeActions(n, ctx)} />` },
  ]} rows=${nodes} />`;
}

function Fleet({ topo, reload, pools }) {
  const [view, setViewS] = useState(prefs.get('compute.view', topo.nodes.length > 8 ? 'list' : 'grid'));
  const setView = (v) => { setViewS(v); prefs.set('compute.view', v); };
  const [q, setQ] = useState('');
  const [stateF, setStateF] = useState('');
  const [poolF, setPoolF] = useState('');
  const [sel, setSel] = useState(null);
  const execs = useMemo(() => executorsByNode(topo.clusters), [topo]);
  const nodes = topo.nodes.filter((n) => (!q || (n.name + ' ' + n.hostname + ' ' + Object.entries(n.labels || {}).map(([k, v]) => k + '=' + v).join(' ')).toLowerCase().includes(q.toLowerCase())) && (!stateF || lightState(n.state) === stateF || n.scheduling === stateF) && (!poolF || n.pool === poolF));
  const selNode = sel && topo.nodes.find((n) => n.id === sel);
  const ctx = { reload, pools };
  const poolNames = [...new Set(topo.nodes.map((n) => n.pool))];
  if (!topo.nodes.length) return html`<${Empty} icon="server" title="No machines yet" action=${html`<${Btn} kind="primary" icon="plus" onClick=${() => openAddMachine(pools)}>Add a machine<//>`}>A machine joins by running the OLCF worker agent. It connects outward to this control plane, so nothing has to be opened in your firewall.<//>`;
  return html`<div class="col gap-4">
    <div class="toolbar"><${Input} size="sm" type="search" placeholder="Search nodes, hosts or labels" aria-label="Search nodes" value=${q} onInput=${setQ} />
      <${Select} size="sm" value=${stateF} onChange=${setStateF} aria-label="Filter by state" placeholder="All states" options=${[{ value: 'down', label: 'Down' }, { value: 'degraded', label: 'Degraded' }, { value: 'ok', label: 'Healthy' }, { value: 'cordoned', label: 'Cordoned' }, { value: 'draining', label: 'Draining' }]} />
      ${poolNames.length > 1 ? html`<${Select} size="sm" value=${poolF} onChange=${setPoolF} aria-label="Filter by pool" placeholder="All pools" options=${poolNames} />` : null}
      <span class="grow"></span><span class="small muted">${nodes.length === topo.nodes.length ? `${nodes.length} node${nodes.length === 1 ? '' : 's'}` : `${nodes.length} of ${topo.nodes.length}`}</span>
      <${Seg} label="Layout" value=${view} onChange=${setView} options=${[{ value: 'grid', icon: 'grid', title: 'Cards' }, { value: 'list', icon: 'list', title: 'Table' }]} /></div>
    ${!nodes.length ? html`<${Empty} icon="search" title="No node matches">Clear the search or filters to see all ${topo.nodes.length} nodes.<//>` : view === 'grid' ? html`<div class=${'node-grid' + (topo.nodes.length > 8 ? ' dense' : '')}>${nodes.map((n) => html`<${NodeCard} key=${n.id} n=${n} exec=${execs[n.id]} onOpen=${() => setSel(n.id)} ctx=${ctx} />`)}</div>` : html`<${NodeRows} nodes=${nodes} execs=${execs} onOpen=${(n) => setSel(n.id)} ctx=${ctx} />`}
    ${selNode ? html`<${NodeDrawer} node=${selNode} topoNode=${selNode} pools=${pools} reload=${reload} onClose=${() => setSel(null)} />` : null}</div>`;
}

// ------------------------------------------------------------------ Spark clusters
function openSparkUi(c) {
  const url = c.master.ui_url;
  if (!url) return;
  window.open(`${url}?access_token=${encodeURIComponent(getAccessToken() || '')}`, '_blank', 'noopener');
}
function ClusterCard({ c, nodes, reload }) {
  const m = c.master;
  const byId = Object.fromEntries(nodes.map((n) => [n.id, n]));
  const coresPct = m.cores_total ? (m.cores_used / m.cores_total) * 100 : 0;
  const memPct = m.memory_total_mb ? (m.memory_used_mb / m.memory_total_mb) * 100 : 0;
  async function stop() {
    if (!(await confirmDialog({ title: 'Stop this Spark cluster?', danger: true, confirmLabel: 'Stop cluster', body: 'The master and workers are stopped. Applications running on it fail. A new cluster starts on demand.' }))) return;
    if (await act(() => api.post(`/clusters/${encodeURIComponent(c.id)}/stop`), { ok: 'Cluster stopped' })) reload();
  }
  return html`<article class="cluster" aria-label=${`Spark cluster ${c.runtime_id} in pool ${c.pool}`}>
    <header class="cluster-head"><${Light} state=${c.state} size="xl" glyph=${true} /><div class="grow"><h3>${c.runtime_id}</h3><div class="small muted">${c.provider} · pool ${c.pool}${m.url ? html` · <code>${m.url}</code>` : null}</div></div>
      <${StatusBadge} state=${c.state} /><${Btn} size="sm" icon="external" disabled=${!m.ui_url} onClick=${() => openSparkUi(c)}>Spark UI<//>${can('compute.manage') ? html`<${Btn} size="sm" kind="ghost" iconOnly icon="power" label="Stop cluster" onClick=${stop} />` : null}</header>
    ${c.detail && c.state !== 'up' ? html`<div class="small muted mb-2">${c.detail}</div>` : null}
    <div class="grid c3 mb-3"><div class="gauge"><div class="g-top"><span>Spark workers alive</span><b>${fmtNum(m.alive_workers)}</b></div><div class="small muted">${c.workers.length} registered</div></div><${Gauge} label="Cores" value=${coresPct} text=${`${m.cores_used} / ${m.cores_total}`} warn=${101} /><${Gauge} label="Memory" value=${memPct} text=${`${fmtMb(m.memory_used_mb)} / ${fmtMb(m.memory_total_mb)}`} /></div>
    ${c.workers.length ? html`<div class="small muted mb-1">Spark workers on</div><div class="chips mb-3">${c.workers.map((w) => html`<span class="badge" key=${w}>${(byId[w] && byId[w].name) || w}</span>`)}</div>` : null}
    ${m.applications.length ? html`<${Table} dense=${true} caption="Applications" rowKey=${(a) => a.ref} columns=${[{ key: 'name', label: 'Application', render: (a) => html`<b>${a.name || a.id}</b><div class="small muted mono">${a.id}</div>` }, { key: 'state', label: 'State', render: (a) => html`<${StatusBadge} state=${a.state} />` }, { key: 'cores', label: 'Cores', num: true }, { key: 'ex', label: 'Executors on', sortable: false, render: (a) => html`<div class="chips">${Object.entries((a.executors || []).reduce((o, e) => { const k = (byId[e.worker] && byId[e.worker].name) || e.worker; o[k] = (o[k] || 0) + 1; return o; }, {})).map(([k, v]) => html`<span class="badge" key=${k}>${k} <b>×${v}</b></span>`)}</div>` }, { key: 'job', label: 'Job', sortable: false, render: (a) => (a.job_id ? html`<a href=${jobHref(a.job_id)}>${a.job_id.slice(0, 14)}</a>` : a.session_id ? 'notebook session' : '') }]} rows=${m.applications} />` : html`<div class="muted small">No application is running on this cluster.</div>`}
  </article>`;
}
function StartClusterDialog({ pools, runtimes, close }) {
  const [rt, setRt] = useState(runtimes[0] || '');
  const [pool, setPool] = useState('default');
  const [busy, setBusy] = useState(false);
  async function go() {
    setBusy(true);
    const r = await act(() => api.post('/clusters', { runtime_id: rt, pool, provider: 'standalone' }));
    setBusy(false);
    if (r) close(r);
  }
  return html`<${Dialog} title="Start a Spark cluster" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!rt} onClick=${go}>Start cluster<//>`}>
    <p class="muted">A standalone cluster runs a Spark master and one worker per node of the pool. Clusters also start on demand when a Spark job is submitted, so you rarely need to do this by hand.</p>
    <${Select} label="Runtime" value=${rt} onChange=${setRt} options=${runtimes} /><${Select} label="Worker pool" value=${pool} onChange=${setPool} options=${pools.map((p) => p.name)} /></${Dialog}>`;
}
function SparkSection({ topo, reload, pools }) {
  const runtimes = [...new Set(topo.nodes.flatMap((n) => (n.runtimes || []).filter((r) => r.state === 'ready' && r.verified && !r.no_spark).map((r) => r.id)))];
  async function start() {
    const r = await openOverlay(({ close }) => html`<${StartClusterDialog} pools=${pools} runtimes=${runtimes} close=${close} />`);
    if (r) { await openOperation(r.operation_id, 'Starting Spark cluster'); reload(); }
  }
  return html`<div class="col gap-4"><div class="row-wrap"><p class="muted grow" style="margin:0">Spark standalone clusters spread executors across your nodes. Check the whole path, from master to executors on every machine, with a smoke test.</p>
    ${can('compute.manage') ? html`<${Btn} icon="zap" onClick=${() => openSmoke(runtimes)}>Spark smoke test<//><${Btn} icon="plus" disabled=${!runtimes.length} onClick=${start}>Start cluster<//>` : null}</div>
    ${topo.clusters.length ? html`<div class="cluster-grid">${topo.clusters.map((c) => html`<${ClusterCard} key=${c.id} c=${c} nodes=${topo.nodes} reload=${reload} />`)}</div>` : html`<${Empty} icon="zap" title="No Spark cluster is running" action=${runtimes.length ? html`<${Btn} icon="zap" onClick=${() => openSmoke(runtimes)}>Run the smoke test<//>` : html`<${Btn} href="#/runtimes">Install a runtime<//>`}>Clusters start on demand when the first Spark job is submitted. ${runtimes.length ? 'Run the smoke test to start one now and prove that executors land on several nodes.' : 'First install and verify a runtime on at least one node.'}<//>`}</div>`;
}

// ------------------------------------------------------------------ control plane
const CAT_ORDER = ['control-plane', 'compute', 'security', 'data', 'storage', 'integration', 'items', 'agents'];
const CAT_LABEL = { 'control-plane': 'Control plane', compute: 'Compute', security: 'Security', data: 'Data', storage: 'Storage', integration: 'Integrations', items: 'Items', agents: 'Agents' };
function ControlPlane({ topo }) {
  const cp = topo.control_plane;
  const groups = {};
  cp.components.forEach((c) => { (groups[c.category || 'other'] ||= []).push(c); });
  const cats = Object.keys(groups).sort((a, b) => (CAT_ORDER.indexOf(a) + 1 || 99) - (CAT_ORDER.indexOf(b) + 1 || 99));
  return html`<div class="col gap-4">
    <${Card} title="Control plane replicas" sub="API replicas share the work; exactly one leads" flush=${true}><${Table} caption="Replicas" rowKey=${(r) => r.node_id} columns=${[
      { key: 'alive', label: 'Status', render: (r) => html`<${StatusBadge} state=${r.alive ? 'online' : 'offline'} />` }, { key: 'node_id', label: 'Replica', render: (r) => html`<b>${r.node_id}</b>` },
      { key: 'leader', label: 'Role', render: (r) => (r.leader ? html`<${Badge} kind="info">Leader<//>` : 'Follower') }, { key: 'version', label: 'Version' }, { key: 'uptime_seconds', label: 'Uptime', num: true, render: (r) => fmtDuration(r.uptime_seconds) },
      { key: 'heartbeat_age_seconds', label: 'Heartbeat', num: true, render: (r) => `${r.heartbeat_age_seconds}s ago` }, { key: 'workers_connected', label: 'Agents attached', num: true }]} rows=${cp.nodes} /></${Card}>
    ${cats.map((cat) => html`<${Card} key=${cat} title=${CAT_LABEL[cat] || titleCase(cat)} sub=${`${groups[cat].length} component${groups[cat].length === 1 ? '' : 's'}`} flush=${true}>
      <ul class="comp-list">${groups[cat].map((c) => html`<li key=${c.name}><${Light} state=${c.status} size="lg" glyph=${true} label=${`${c.name}: ${c.status}`} /><div class="grow" style="min-width:0"><div class="c-name">${c.name}</div><div class="small muted" title=${c.detail}>${c.detail}</div></div>
        <span class="small muted num">${c.latency_ms != null ? c.latency_ms + ' ms' : ''}</span><${Btn} size="sm" icon="activity" onClick=${() => openHealthTest(c.name)} aria-label=${`Run deep test for ${c.name}`}>Test<//></li>`)}</ul></${Card}>`)}</div>`;
}

// ------------------------------------------------------------------ queues and pools
function QueuesPools({ queues, pools, reload, topo }) {
  const manage = can('compute.manage');
  const stat = Object.fromEntries((topo.queues || []).map((q) => [q.name, q]));
  async function pause(q) { if (await act(() => api.post(`/queues/${encodeURIComponent(q.name)}/${q.paused ? 'resume' : 'pause'}`), { ok: q.paused ? `${q.name} resumed` : `${q.name} paused` })) reload(); }
  async function delQ(q) { if (!(await confirmDialog({ title: `Delete queue ${q.name}?`, danger: true, confirmLabel: 'Delete', body: 'Queues with active jobs cannot be deleted.' }))) return; if (await act(() => api.del(`/queues/${encodeURIComponent(q.name)}`), { ok: 'Queue deleted' }) !== undefined) reload(); }
  async function delP(p) { if (!(await confirmDialog({ title: `Delete pool ${p.name}?`, danger: true, confirmLabel: 'Delete', body: 'Pools that still contain nodes cannot be deleted. Move the nodes first.' }))) return; if (await act(() => api.del(`/worker-pools/${encodeURIComponent(p.name)}`), { ok: 'Pool deleted' }) !== undefined) reload(); }
  const lim = (v) => (v == null ? html`<span class="muted">∞</span>` : fmtNum(v));
  return html`<div class="col gap-4">
    <${Card} title="Queues" sub="Where jobs wait, ordered by priority and fair share" actions=${manage ? html`<${Btn} size="sm" icon="plus" onClick=${() => openQueueDialog(null, pools, reload)}>Create queue<//>` : null} flush=${true}>
      <${Table} caption="Queues" rowKey=${(q) => q.name} columns=${[
        { key: 'name', label: 'Queue', render: (q) => html`<b>${q.name}</b><div class="small muted">${q.description}</div>` }, { key: 'paused', label: 'State', render: (q) => (q.paused ? html`<${Badge} kind="warn">Paused<//>` : html`<${Badge} kind="ok">Open<//>`) },
        { key: 'depth', label: 'Waiting', num: true, render: (q) => q.depth }, { key: 'running', label: 'Running', num: true }, { key: 'wait', label: 'Oldest wait', num: true, sort: (q) => (stat[q.name] || {}).oldest_wait_seconds || 0, render: (q) => (q.depth ? fmtDuration((stat[q.name] || {}).oldest_wait_seconds) : '–') },
        { key: 'max_concurrency', label: 'Max jobs', num: true, render: (q) => lim(q.max_concurrency) }, { key: 'max_cores', label: 'Max cores', num: true, render: (q) => lim(q.max_cores) }, { key: 'default_priority', label: 'Priority', num: true },
        { key: 'allowed_pools', label: 'Pools', render: (q) => (q.allowed_pools.length ? q.allowed_pools.join(', ') : 'any') },
        { key: 'a', label: '', sortable: false, render: (q) => manage ? html`<div class="row"><${Btn} size="sm" kind="ghost" onClick=${() => pause(q)}>${q.paused ? 'Resume' : 'Pause'}<//><${Btn} size="sm" kind="ghost" iconOnly icon="edit" label=${`Edit ${q.name}`} onClick=${() => openQueueDialog(q, pools, reload)} />${q.name !== 'default' ? html`<${Btn} size="sm" kind="ghost" iconOnly icon="trash" label=${`Delete ${q.name}`} onClick=${() => delQ(q)} />` : null}</div>` : null }]} rows=${queues} /></${Card}>
    <${Card} title="Worker pools" sub="Group nodes for placement and isolation" actions=${manage ? html`<${Btn} size="sm" icon="plus" onClick=${() => openPoolDialog(null, reload)}>Create pool<//>` : null} flush=${true}>
      <${Table} caption="Worker pools" rowKey=${(p) => p.name} columns=${[{ key: 'name', label: 'Pool', render: (p) => html`<b>${p.name}</b><div class="small muted">${p.description}</div>` }, { key: 'workers', label: 'Nodes', num: true }, { key: 'reserved', label: 'Access', render: (p) => (p.reserved ? html`<${Badge} kind="warn">Reserved<//>` : 'Open to all workspaces') },
        { key: 'a', label: '', sortable: false, render: (p) => manage ? html`<div class="row"><${Btn} size="sm" kind="ghost" iconOnly icon="edit" label=${`Edit ${p.name}`} onClick=${() => openPoolDialog(p, reload)} />${p.name !== 'default' ? html`<${Btn} size="sm" kind="ghost" iconOnly icon="trash" label=${`Delete ${p.name}`} onClick=${() => delP(p)} />` : null}</div>` : null }]} rows=${pools} /></${Card}></div>`;
}

// ------------------------------------------------------------------ page
export function ComputePage() {
  const topo = usePoll(() => api.get('/compute/topology'), 3000);
  const queues = useAsync(() => api.get('/queues').then((r) => r.items), []);
  const pools = useAsync(() => api.get('/worker-pools').then((r) => r.items), []);
  const [tab, setTabS] = useState(prefs.get('compute.tab', 'fleet'));
  const setTab = (t) => { setTabS(t); prefs.set('compute.tab', t); };
  const reload = () => { topo.reload(); queues.reload(); pools.reload(); };
  useEffect(() => bus.on('event:worker', () => topo.reload()), []);
  useEffect(() => {
    const q = parseHash().query;
    if (q.add) { setQuery({ add: null }); openAddMachine(pools.data || []); }
  }, [pools.data]);
  const d = topo.data;
  const poolList = pools.data || [];
  const manage = can('compute.manage');
  const actions = html`${manage ? html`<${Btn} kind="primary" icon="plus" onClick=${() => openAddMachine(poolList)}>Add a machine<//>` : null}
    <${Btn} icon="activity" disabled=${!d || !d.summary.nodes_online} onClick=${() => openClusterTest()}>Test all nodes<//>`;
  const tabs = [{ id: 'fleet', label: 'Machines', count: d ? d.summary.nodes_total : null }, { id: 'spark', label: 'Spark clusters', count: d ? d.clusters.length : null }, { id: 'control', label: 'Control plane' }, { id: 'queues', label: 'Queues and pools' }, { id: 'runtimes', label: 'Runtimes' }];
  return html`<div class="page wide"><${PageHead} icon="server" title="Compute" sub="Every machine, Spark cluster and queue that runs your work, live" actions=${actions} />
    ${topo.error && !d ? html`<${ErrorBox} error=${topo.error} onRetry=${topo.reloadLoud} />` : !d ? html`<${Loading} label="Reading the fleet" />` : html`
      ${topo.error ? html`<${Banner} kind="warn" title="Live updates are interrupted">The last fleet reading may be out of date. Retrying every few seconds.<//>` : null}
      <${SummaryBar} topo=${d} /><${QueueStrip} queues=${d.queues} />
      <div class="mt-4"><${Tabs} label="Compute sections" tabs=${tabs} value=${tab} onChange=${setTab} /></div>
      <div class="tab-body">
        <${TabPanel} id="fleet" value=${tab}><${Fleet} topo=${d} reload=${reload} pools=${poolList} /></${TabPanel}>
        <${TabPanel} id="spark" value=${tab}><${SparkSection} topo=${d} reload=${reload} pools=${poolList} /></${TabPanel}>
        <${TabPanel} id="control" value=${tab}><${ControlPlane} topo=${d} /></${TabPanel}>
        <${TabPanel} id="queues" value=${tab}><${QueuesPools} queues=${queues.data || []} pools=${poolList} reload=${reload} topo=${d} /></${TabPanel}>
        <${TabPanel} id="runtimes" value=${tab}><${RuntimesPanel} /></${TabPanel}></div>`}</div>`;
}
