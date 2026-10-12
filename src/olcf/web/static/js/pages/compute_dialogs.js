// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Dialogs of the Compute control panel: add-a-machine wizard, connectivity tests (node, cluster matrix, Spark smoke test), labels, pools, queues, node details.
import { html, useState, useEffect, useRef, useAsync, usePoll } from '../core/h.js';
import { api, getAccessToken } from '../api/client.js';
import { Dialog, Drawer, openOverlay, act, toast, confirmDialog } from '../ui/overlay.js';
import { Btn, Input, Select, Field, Light, StatusBadge, Banner, Code, CopyBtn, Steps, Tabs, TabPanel, Kv, Badge, ErrorBox, Check, Seg, Gauge, Sparkline, Textarea, Switch, Progress, Loading } from '../ui/basic.js';
import { Icon } from '../ui/icons.js';
import { Table } from '../ui/data.js';
import { fmtMs, fmtMb, fmtPct, fmtDuration, ago, titleCase, lightState, sleep, fmtNum, jsonPretty, fmtTime } from '../core/util.js';
import { jobHref } from '../core/links.js';

// ---------------------------------------------------------------- helpers
export function osCommands({ url, token, name, pool, labels }) {
  const lab = labels ? ` --labels ${labels}` : '';
  const pl = pool ? ` --pool ${pool}` : '';
  const run = `olcf-worker run --url ${url} --token ${token} --name ${name}${pl}${lab}`;
  return {
    linux: `# Requires Python 3.11+ and Java 17 or 21 for Spark
python3 -m venv ~/.olcf-worker-venv
~/.olcf-worker-venv/bin/pip install olcf
~/.olcf-worker-venv/bin/${run}`,
    macos: `# Requires Python 3.11+ (brew install python openjdk@21)
python3 -m venv ~/.olcf-worker-venv
~/.olcf-worker-venv/bin/pip install olcf
~/.olcf-worker-venv/bin/${run}`,
    windows: `# PowerShell. Requires Python 3.11+ and a Java 17 or 21 JDK
py -3 -m venv $env:USERPROFILE\\.olcf-worker-venv
& "$env:USERPROFILE\\.olcf-worker-venv\\Scripts\\pip.exe" install olcf
& "$env:USERPROFILE\\.olcf-worker-venv\\Scripts\\${run.replace('olcf-worker run', 'olcf-worker.exe run')}"`,
    docker: `docker run -d --name olcf-worker-${name} --restart unless-stopped \\
  -v olcf-worker-${name}:/state \\
  python:3.13-slim sh -c "apt-get update -qq && apt-get install -y -qq default-jre-headless >/dev/null && pip install -q olcf && ${run} --state-dir /state"`,
  };
}

/** Result of one node test as a step pipeline with a summary. */
export function NodeTestResult({ result }) {
  const failed = result.steps.filter((s) => !s.ok);
  return html`<div><${Banner} kind=${result.ok ? 'ok' : 'bad'} title=${result.ok ? 'Every check passed' : `${failed.length} of ${result.steps.length} checks failed`}>
      Run from the node's own point of view in ${fmtMs(result.duration_ms)}${result.checked_at ? ', ' + ago(result.checked_at) : ''}.<//>
    <div class="mt-4"><${Steps} steps=${result.steps.map((s) => ({ ...s, label: stepLabel(s.name), remediation: s.ok ? null : remediationFor(s) }))} /></div></div>`;
}
const STEP_LABELS = { 'agent-link': 'Agent link (control channel)', 'dns:control-plane': 'DNS lookup of the control plane', 'tcp:control-plane': 'TCP connection to the control plane', 'tls:control-plane': 'TLS handshake with the control plane', 'http:control-plane': 'HTTP request to the control plane', disk: 'Disk space', clock: 'Clock skew against the controller' };
export function stepLabel(n) {
  if (STEP_LABELS[n]) return STEP_LABELS[n];
  if (n.startsWith('runtime:')) return `Runtime ${n.slice(8)} verification`;
  if (n.startsWith('spark-master:') || n.startsWith('spark_master:')) return `Spark master reachability (${n.split(':').slice(1).join(':')})`;
  if (n.startsWith('storage:')) return `Storage reachability: ${n.slice(8)}`;
  return titleCase(n);
}
export function remediationFor(s) {
  const n = s.name; const d = String(s.detail || '');
  if (n === 'agent-link') return 'The agent did not answer on its control channel. Check that olcf-worker is running on the node and that outbound WebSocket traffic to the control plane is allowed.';
  if (n.startsWith('dns')) return 'The node cannot resolve the control plane host name. Fix DNS on the node or enrol it with an address it can resolve.';
  if (n.startsWith('tcp')) return 'The node cannot open a TCP connection to the control plane port. Check firewalls, proxies and the --url the agent was started with.';
  if (n.startsWith('tls')) return 'TLS verification failed. Pin the control plane CA with --ca-pem or install the CA on the node.';
  if (n.startsWith('http')) return 'The control plane answered with an error. Check its health on the Control plane tab.';
  if (n === 'disk') return 'Free disk space on the node (job work directories and runtimes live in the state directory) or move the state directory to a larger volume.';
  if (n === 'clock') return 'Synchronise the node clock (NTP/chrony). Large skew breaks token validation and log ordering.';
  if (n.startsWith('runtime:')) return /untrusted key|signature/.test(d) ? 'The node does not trust the key that signed this runtime. Start the agent with --runtime-trust-dir pointing at the trusted public keys, or re-install the runtime.' : 'Run Repair for this runtime from the Runtimes page; it re-resolves and re-verifies the pinned artifacts transactionally.';
  if (n.startsWith('spark')) return 'The node cannot reach the Spark master. Open the master port (and the worker/driver port ranges) between the nodes.';
  if (n.startsWith('storage')) return 'The node cannot reach this storage. Check the endpoint, firewall/proxy rules and that the connection is correct. Credentials are tested from the control plane.';
  return null;
}

export function openNodeTest(node) {
  return openOverlay(({ close }) => html`<${NodeTestDialog} node=${node} close=${close} />`);
}
function NodeTestDialog({ node, close }) {
  const r = useAsync(() => api.post(`/workers/${encodeURIComponent(node.id)}/test`), []);
  return html`<${Dialog} title=${`Test connectivity: ${node.name}`} size="wide" onClose=${() => close()} footer=${html`<${Btn} onClick=${() => r.reloadLoud()}>Run again<//><${Btn} kind="primary" onClick=${() => close()}>Close<//>`}>
    ${r.loading ? html`<div class="loading"><span class="spinner lg"></span><span>Running checks on ${node.name}…</span></div>` : r.error ? html`<${ErrorBox} error=${r.error} />` : html`<${NodeTestResult} result=${r.data} />`}</${Dialog}>`;
}

/** Node x check matrix of lights from POST /v1/compute/test. */
export function ClusterMatrix({ matrix }) {
  const { checks, rows } = matrix;
  return html`<div class="matrix-wrap"><table class="matrix"><thead><tr><th scope="col">Node</th>${checks.map((c) => html`<th scope="col" key=${c} title=${c}><span class="mx-h">${stepLabel(c).replace(/^(Spark master reachability|Storage reachability): ?/, '$1 ').slice(0, 26)}</span></th>`)}<th scope="col">Result</th></tr></thead>
    <tbody>${rows.map((r) => html`<tr key=${r.node}><th scope="row">${r.node}</th>${checks.map((c) => { const x = r.results[c]; return html`<td key=${c}>${x ? html`<span class="mx-cell" title=${`${stepLabel(c)}: ${x.detail || ''} (${fmtMs(x.ms)})`} tabindex="0"><${Light} state=${x.ok ? 'ok' : 'down'} size="lg" glyph=${true} label=${`${r.node} ${stepLabel(c)}: ${x.ok ? 'passed' : 'failed'}`} /></span>` : html`<span class="muted" aria-label="not run">–</span>`}</td>`; })}<td><${StatusBadge} state=${r.ok ? 'ok' : 'failed'} label=${r.ok ? 'Pass' : 'Fail'} /></td></tr>`)}</tbody></table></div>`;
}
export function openClusterTest() { return openOverlay(({ close }) => html`<${ClusterTestDialog} close=${close} />`); }
function ClusterTestDialog({ close }) {
  const r = useAsync(() => api.post('/compute/test'), []);
  const [sel, setSel] = useState(null);
  const d = r.data;
  const failures = d ? Object.values(d.nodes).flatMap((n) => n.steps.filter((s) => !s.ok).map((s) => ({ node: n.name, ...s }))) : [];
  return html`<${Dialog} title="Cluster-wide connectivity test" size="xwide" onClose=${() => close()} footer=${html`<${Btn} onClick=${() => r.reloadLoud()}>Run again<//><${Btn} kind="primary" onClick=${() => close()}>Close<//>`}>
    ${r.loading ? html`<div class="loading"><span class="spinner lg"></span><span>Every node is running its probe suite concurrently…</span></div>` : r.error ? html`<${ErrorBox} error=${r.error} />` : html`
      <${Banner} kind=${d.ok ? 'ok' : 'bad'} title=${d.ok ? 'All nodes passed every check' : `${failures.length} failing check${failures.length === 1 ? '' : 's'} across ${new Set(failures.map((f) => f.node)).size} node(s)`}>All nodes tested concurrently in ${fmtMs(d.duration_ms)}.<//>
      <div class="mt-4"><${ClusterMatrix} matrix=${d.matrix} /></div>
      ${failures.length ? html`<h3 class="mt-6 mb-2">What failed and how to fix it</h3><div class="col gap-3">${failures.map((f, i) => html`<div class="panel" key=${i}><div class="row"><${Light} state="down" /><b>${f.node}</b><span class="muted">${stepLabel(f.name)}</span></div><div class="small mt-2">${f.detail}</div><div class="small mt-2"><b>Fix: </b>${remediationFor(f) || 'See the detail above.'}</div></div>`)}</div>` : null}`}</${Dialog}>`;
}

export function openSmoke(runtimes) { return openOverlay(({ close }) => html`<${SmokeDialog} runtimes=${runtimes} close=${close} />`); }
function SmokeDialog({ runtimes, close }) {
  const [rt, setRt] = useState(runtimes[0] || '');
  const [executors, setExecutors] = useState('2');
  const [busy, setBusy] = useState(false);
  const [res, setRes] = useState(null);
  const [err, setErr] = useState(null);
  const [prog, setProg] = useState(null);
  const alive = useRef(true);
  useEffect(() => () => { alive.current = false; }, []);
  // The controller runs the test as a job: POST answers 202 at once, the result is polled by id until it has finished (the controller enforces the deadline).
  async function run() {
    setBusy(true); setErr(null); setRes(null); setProg(null);
    try {
      let doc = await api.post('/compute/spark-smoke', { runtime_id: rt || null, executors: Number(executors) || 2, partitions: 8, timeout_seconds: 240 });
      while (alive.current && !doc.finished) {
        setProg(doc);
        await sleep(2000);
        doc = await api.get(`/compute/spark-smoke/${encodeURIComponent(doc.id)}`);
      }
      if (alive.current) setRes(doc);
    } catch (e) { if (alive.current) setErr(e); } finally { if (alive.current) { setBusy(false); setProg(null); } }
  }
  return html`<${Dialog} title="Spark smoke test" size="wide" onClose=${() => close()} footer=${html`<${Btn} onClick=${() => close()}>Close<//><${Btn} kind="primary" icon="zap" busy=${busy} onClick=${run}>Run smoke test<//>`}>
    <p class="muted">Submits a tiny distributed job to the cluster and reports which nodes hosted executors. It proves that the master, workers, network paths and the runtime really work together. It starts a Spark driver, so it can take a minute on a cold cluster.</p>
    <div class="form-grid"><${Select} label="Runtime" value=${rt} onChange=${setRt} options=${runtimes.length ? runtimes : ['']} placeholder=${runtimes.length ? null : 'No runtime available'} /><${Input} label="Executors" type="number" min="1" max="16" value=${executors} onInput=${setExecutors} /></div>
    ${busy ? html`<div class="mt-4"><${Progress} indeterminate=${true} label="Smoke test running" /><div class="small muted mt-2">${prog ? `${prog.message} (${fmtDuration(prog.elapsed_seconds || 0)})` : 'Starting the smoke test…'}</div></div>` : null}
    ${err ? html`<div class="mt-4"><${ErrorBox} error=${err} /></div>` : null}
    ${res ? html`<div class="mt-4"><${SmokeResult} res=${res} /></div>` : null}</${Dialog}>`;
}
export function SmokeResult({ res }) {
  const nodes = res.nodes_with_executors || [];
  const title = res.ok ? (res.distributed ? 'Spark smoke test passed: work ran on several nodes' : 'Spark smoke test passed on a single node') : `Spark smoke test ${res.state || 'failed'}`;
  const kind = res.ok ? (res.distributed ? 'ok' : 'warn') : 'bad';
  const f = res.failure;
  return html`<${Banner} kind=${kind} title=${title} remediation=${res.ok ? null : 'Open the job below for the driver log. Typical causes: the runtime failed verification on the nodes, no node has free memory for the driver and executors, or the Spark master port is blocked.'}>${res.note || ''}${f && f.message ? html`<div class="mt-2">${f.message}</div>` : null}<//>
    <div class="mt-3"><${Kv} items=${[['Job', res.job_id ? html`<a href=${jobHref(res.job_id)}>${res.job_id}</a>` : null], ['Runtime', res.runtime_id], ['Master', res.master ? html`<code>${res.master}</code>` : null], ['Application', res.application_id ? html`<code>${res.application_id}</code>` : null],
      ['Tasks / checksum', res.tasks != null ? `${res.tasks} tasks, sum ${res.sum}` : null], ['Elapsed', res.elapsed_seconds != null ? fmtDuration(res.elapsed_seconds) : null]]} /></div>
    ${nodes.length ? html`<h3 class="mt-4 mb-2">Nodes that hosted executors</h3><div class="chips">${nodes.map((n) => html`<span class="badge ok" key=${n.worker_id}><${Light} state="ok" label="" />${n.name}${n.tasks != null ? html` <span class="muted">${n.tasks} tasks</span>` : null}</span>`)}</div>` : null}
    ${(res.hosts || []).length ? html`<div class="small muted mt-3">Task hosts: ${res.hosts.map((h) => `${h.host} (${h.tasks})`).join(', ')}</div>` : null}`;
}

// ---------------------------------------------------------------- add a machine
export function openAddMachine(pools) { return openOverlay(({ close }) => html`<${AddMachine} pools=${pools} close=${close} />`); }
function AddMachine({ pools, close }) {
  const [step, setStep] = useState(1);
  const [name, setName] = useState('');
  const [pool, setPool] = useState('default');
  const [labels, setLabels] = useState('');
  const [ttl, setTtl] = useState('3600');
  const [tok, setTok] = useState(null);
  const [url, setUrl] = useState(window.location.origin);
  const [os, setOs] = useState('linux');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  async function create() {
    setBusy(true); setErr(null);
    try {
      const lab = {};
      labels.split(',').map((x) => x.trim()).filter(Boolean).forEach((kv) => { const [k, ...v] = kv.split('='); if (k) lab[k.trim()] = v.join('=').trim(); });
      const r = await api.post('/workers/enrollment-tokens', { name: name.trim() || null, ttl_seconds: Number(ttl) || 3600, pool: pool || null, labels: lab });
      setTok(r); setUrl(r.url || window.location.origin); setStep(2);
    } catch (e) { setErr(e); } finally { setBusy(false); }
  }
  const nodeName = (name.trim() || (tok && /--name (\S+)/.exec(tok.command || '')?.[1]) || 'my-node');
  const cmds = tok ? osCommands({ url, token: tok.token, name: nodeName, pool: pool !== 'default' ? pool : '', labels: labels.replace(/\s+/g, '') }) : {};
  const waiting = usePoll(async () => (tok ? (await api.get('/workers')).items.find((w) => w.name === nodeName && w.state !== 'pending') || null : null), 2500, [tok && tok.token]);
  const seen = waiting.data;
  return html`<${Dialog} title="Add a machine" size="wide" onClose=${() => close()} footer=${step === 1 ? html`<${Btn} onClick=${() => close()}>Cancel<//><${Btn} kind="primary" busy=${busy} onClick=${create}>Create enrollment token<//>` : html`<${Btn} onClick=${() => setStep(1)}>Back<//><${Btn} kind="primary" onClick=${() => close()}>${seen ? 'Done' : 'Close'}<//>`}>
    <ol class="wizard-steps" aria-label="Progress"><li class=${step >= 1 ? 'on' : ''}>Describe the machine</li><li class=${step >= 2 ? 'on' : ''}>Run the command</li><li class=${seen ? 'on' : ''}>Connected</li></ol>
    ${step === 1 ? html`<div>
      <p class="muted">A machine joins the fabric by running the OLCF worker agent. It connects outward to this control plane, so it needs no inbound firewall rules. The enrollment token is single-use.</p>
      <div class="form-grid mt-3"><${Input} label="Node name" value=${name} onInput=${setName} placeholder="e.g. spark-node-3" hint="Leave empty to use the machine's host name." />
        <${Select} label="Worker pool" value=${pool} onChange=${setPool} options=${(pools.length ? pools : [{ name: 'default' }]).map((p) => p.name)} />
        <${Input} label="Labels" value=${labels} onInput=${setLabels} placeholder="site=berlin,gpu=false" hint="Comma separated key=value pairs." />
        <${Select} label="Token valid for" value=${ttl} onChange=${setTtl} options=${[{ value: '600', label: '10 minutes' }, { value: '3600', label: '1 hour' }, { value: '86400', label: '24 hours' }]} /></div>
      ${err ? html`<${ErrorBox} error=${err} />` : null}</div>` : html`<div>
      <${Input} label="Control plane URL as the machine will see it" value=${url} onInput=${setUrl} hint="Change this if the machine reaches the control plane under another address than your browser does." />
      <${Seg} label="Operating system" value=${os} onChange=${setOs} options=${[{ value: 'linux', label: 'Linux' }, { value: 'macos', label: 'macOS' }, { value: 'windows', label: 'Windows' }, { value: 'docker', label: 'Docker' }]} />
      <div class="mt-3"><${Code} text=${cmds[os]} lang="shell" /></div>
      <div class="small muted mt-2">Token expires ${ago(tok.expires_at)}; it can be used once. ${os === 'docker' ? 'The container needs network access to PyPI and to this control plane. Replace the image with your own build if you have one.' : ''}</div>
      <div class="mt-4 panel row" role="status" aria-live="polite">${seen ? html`<${Light} state=${seen.state === 'online' ? 'ok' : 'degraded'} size="lg" glyph=${true} /><div><b>${seen.name} connected</b><div class="small muted">${seen.hostname} · ${seen.resources.cores} cores · ${fmtMb(seen.resources.memory_mb)} · ${seen.os}/${seen.arch}</div></div><span class="grow"></span><${StatusBadge} state=${seen.state} />` : html`<span class="spinner"></span><span class="muted">Waiting for <b>${nodeName}</b> to connect…</span>`}</div></div>`}</${Dialog}>`;
}

// ---------------------------------------------------------------- labels / pool / queue / pool dialogs
export async function editLabels(node, reload) {
  const init = Object.entries(node.labels || {}).map(([k, v]) => `${k}=${v}`).join('\n');
  const text = await openOverlay(({ close }) => html`<${LabelsDialog} node=${node} init=${init} close=${close} />`);
  if (text != null) {
    const labels = {};
    text.split('\n').map((l) => l.trim()).filter(Boolean).forEach((l) => { const [k, ...v] = l.split('='); if (k.trim()) labels[k.trim()] = v.join('=').trim(); });
    const r = await act(() => api.put(`/workers/${encodeURIComponent(node.id)}/labels`, labels), { ok: 'Labels updated' });
    if (r) reload();
  }
}
function LabelsDialog({ node, init, close }) {
  const [v, setV] = useState(init);
  return html`<${Dialog} title=${`Labels of ${node.name}`} onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" onClick=${() => close(v)}>Save changes<//>`}>
    <${Textarea} label="Labels" value=${v} onInput=${setV} mono=${true} rows=${6} hint="One key=value per line. Jobs can request nodes by label, for example site=berlin." /></${Dialog}>`;
}
export async function assignPool(node, pools, reload) {
  const pool = await openOverlay(({ close }) => html`<${PoolPick} node=${node} pools=${pools} close=${close} />`);
  if (pool) { const r = await act(() => api.put(`/workers/${encodeURIComponent(node.id)}/pool`, { pool }), { ok: `${node.name} moved to pool ${pool}` }); if (r) reload(); }
}
function PoolPick({ node, pools, close }) {
  const [p, setP] = useState(node.pool || 'default');
  return html`<${Dialog} title=${`Assign ${node.name} to a pool`} onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" onClick=${() => close(p)}>Assign<//>`}>
    <${Select} label="Worker pool" value=${p} onChange=${setP} options=${pools.map((x) => x.name)} hint="Pools group nodes for placement; queues can be restricted to certain pools." /></${Dialog}>`;
}

export function openPoolDialog(pool, reload) { return openOverlay(({ close }) => html`<${PoolDialog} pool=${pool} reload=${reload} close=${close} />`); }
function PoolDialog({ pool, reload, close }) {
  const [name, setName] = useState(pool ? pool.name : '');
  const [desc, setDesc] = useState(pool ? pool.description || '' : '');
  const [reserved, setReserved] = useState(pool ? !!pool.reserved : false);
  const [busy, setBusy] = useState(false);
  async function save() {
    setBusy(true);
    const body = { description: desc, reserved };
    const r = await act(() => (pool ? api.patch(`/worker-pools/${encodeURIComponent(pool.name)}`, body, { ifMatch: pool.etag }) : api.post('/worker-pools', { name, ...body })), { ok: pool ? 'Pool updated' : 'Pool created' });
    setBusy(false);
    if (r) { reload(); close(true); }
  }
  return html`<${Dialog} title=${pool ? `Pool ${pool.name}` : 'Create worker pool'} onClose=${() => close()} footer=${html`<${Btn} onClick=${() => close()}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!pool && !name} onClick=${save}>${pool ? 'Save changes' : 'Create pool'}<//>`}>
    ${pool ? null : html`<${Input} label="Name" value=${name} onInput=${setName} placeholder="gpu, batch, team-a" />`}
    <${Input} label="Description" value=${desc} onInput=${setDesc} /><${Switch} label="Reserved: only workspaces assigned to this pool can use it" checked=${reserved} onChange=${setReserved} /></${Dialog}>`;
}
export function openQueueDialog(queue, pools, reload) { return openOverlay(({ close }) => html`<${QueueDialog} queue=${queue} pools=${pools} reload=${reload} close=${close} />`); }
function QueueDialog({ queue, pools, reload, close }) {
  const q = queue || {};
  const [f, setF] = useState({ name: q.name || '', description: q.description || '', max_concurrency: q.max_concurrency ?? '', max_cores: q.max_cores ?? '', max_memory_mb: q.max_memory_mb ?? '', max_queued: q.max_queued ?? '', max_queue_wait_seconds: q.max_queue_wait_seconds ?? '', default_priority: q.default_priority ?? 50, allowed_pools: (q.allowed_pools || []).join(', ') });
  const [busy, setBusy] = useState(false);
  const set = (k) => (v) => setF((s) => ({ ...s, [k]: v }));
  const num = (v) => (v === '' || v == null ? null : Number(v));
  async function save() {
    setBusy(true);
    const body = { description: f.description, max_concurrency: num(f.max_concurrency), max_cores: num(f.max_cores), max_memory_mb: num(f.max_memory_mb), max_queued: num(f.max_queued), max_queue_wait_seconds: num(f.max_queue_wait_seconds), default_priority: num(f.default_priority) ?? 50, allowed_pools: f.allowed_pools.split(',').map((x) => x.trim()).filter(Boolean) };
    const r = await act(() => (queue ? api.patch(`/queues/${encodeURIComponent(queue.name)}`, body, { ifMatch: queue.etag }) : api.post('/queues', { name: f.name, ...body })), { ok: queue ? 'Queue updated' : 'Queue created' });
    setBusy(false);
    if (r) { reload(); close(true); }
  }
  return html`<${Dialog} title=${queue ? `Queue ${queue.name}` : 'Create queue'} size="wide" onClose=${() => close()} footer=${html`<${Btn} onClick=${() => close()}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!queue && !f.name} onClick=${save}>${queue ? 'Save changes' : 'Create queue'}<//>`}>
    <div class="form-grid">${queue ? null : html`<${Input} label="Name" value=${f.name} onInput=${set('name')} />`}<${Input} label="Description" value=${f.description} onInput=${set('description')}  />
      <${Input} label="Max concurrent jobs" type="number" min="1" value=${f.max_concurrency} onInput=${set('max_concurrency')} hint="Empty means unlimited." /><${Input} label="Max cores in use" type="number" min="1" value=${f.max_cores} onInput=${set('max_cores')} />
      <${Input} label="Max memory in use (MB)" type="number" min="256" value=${f.max_memory_mb} onInput=${set('max_memory_mb')} /><${Input} label="Max queued jobs" type="number" min="1" value=${f.max_queued} onInput=${set('max_queued')} />
      <${Input} label="Max wait before rejecting (seconds)" type="number" min="1" value=${f.max_queue_wait_seconds} onInput=${set('max_queue_wait_seconds')} /><${Input} label="Default priority" type="number" min="0" max="100" value=${f.default_priority} onInput=${set('default_priority')} hint="Higher runs first. Default 50." />
      <${Input} label="Allowed pools" value=${f.allowed_pools} onInput=${set('allowed_pools')} placeholder=${pools.map((p) => p.name).join(', ')} hint="Comma separated. Empty means any pool." /></div></${Dialog}>`;
}

// ---------------------------------------------------------------- node details drawer
export function NodeDrawer({ node, topoNode, onClose, reload, pools }) {
  const detail = useAsync(() => api.get(`/workers/${encodeURIComponent(node.id)}`), [node.id]);
  const d = detail.data;
  const inv = (d && d.inventory) || {};
  const h = topoNode.history || {};
  return html`<${Drawer} title=${html`<span class="row"><${Light} state=${topoNode.state} size="lg" glyph=${true} />${node.name}</span>`} onClose=${onClose} wide=${true}>
    ${detail.loading && !d ? html`<${Loading} />` : detail.error ? html`<${ErrorBox} error=${detail.error} />` : html`
    <div class="grid c3"><div><div class="small muted mb-2">CPU (last ${(h.cpu || []).length} samples)</div><${Sparkline} values=${h.cpu} min=${0} max=${100} height=${48} label="CPU" /></div><div><div class="small muted mb-2">Memory</div><${Sparkline} values=${h.mem} min=${0} max=${100} height=${48} label="Memory" /></div>
      <div><div class="small muted mb-2">Load / network</div><div class="small">load1 <b>${topoNode.metrics.load1 ?? '–'}</b><br/>rx <b>${fmtNum((topoNode.metrics.net_rx_bps || 0) / 1024, 0)} KB/s</b> · tx <b>${fmtNum((topoNode.metrics.net_tx_bps || 0) / 1024, 0)} KB/s</b></div></div></div>
    <h3 class="mt-6 mb-2">Components</h3><${Table} dense=${true} columns=${[{ key: 'n', label: 'Component', render: (c) => c.name, sortable: false }, { key: 's', label: 'Status', render: (c) => html`<${StatusBadge} state=${c.status} />`, sortable: false }, { key: 'd', label: 'Detail', render: (c) => c.detail, sortable: false }]} rows=${topoNode.components || []} rowKey=${(c) => c.name} />
    <h3 class="mt-6 mb-2">Inventory</h3><${Kv} items=${[['Host', `${inv.hostname || ''} (${inv.fqdn || ''})`], ['Address', inv.address], ['Operating system', `${inv.os_name || inv.os} · kernel ${inv.kernel || ''}`], ['CPU', `${inv.cpu_model} · ${inv.cores} cores (${inv.cores_physical} physical)`], ['Memory', `${fmtMb(inv.memory_mb)} (${fmtMb(inv.usable_memory_mb)} usable by jobs)`], ['Disk', `${inv.disk_gb} GB, ${inv.disk_free_gb} GB free`], ['Java', (inv.java || []).map((j) => `${j.version} (${j.vendor})`).join(', ') || 'none found'], ['Python', (inv.python || []).map((p) => p.version).join(', ')], ['Agent', `${d.agent_version} (protocol ${d.protocol})`], ['State directory', html`<code>${inv.state_dir}</code>`], ['Enrolled', fmtTime(d.enrolled_at)], ['Last connected', fmtTime(d.last_connected_at)]]} />
    <h3 class="mt-6 mb-2">Runtimes on this node</h3>${(topoNode.runtimes || []).length ? html`<${Table} dense=${true} columns=${[{ key: 'id', label: 'Runtime', render: (r) => r.id, sortable: false }, { key: 'st', label: 'State', render: (r) => html`<${StatusBadge} state=${r.state} />`, sortable: false }, { key: 'v', label: 'Verified', render: (r) => (r.verified ? 'Yes' : 'No'), sortable: false }, { key: 'f', label: 'Fingerprint', render: (r) => html`<code>${String(r.fingerprint || '').slice(0, 19)}</code>`, sortable: false }]} rows=${topoNode.runtimes} rowKey=${(r) => r.id} />` : html`<div class="muted">No runtime installed. Install one from the Runtimes tab.</div>`}
    <h3 class="mt-6 mb-2">Running work</h3>${(topoNode.running_jobs || []).length || (topoNode.sessions || []).length ? html`<ul class="plain">${(topoNode.running_jobs || []).map((j) => html`<li key=${j.job_id}><a href=${jobHref(j.job_id)}>${j.name || j.job_id}</a> <${StatusBadge} state=${j.state} /></li>`)}${(topoNode.sessions || []).map((s) => html`<li key=${s.id || s.session_id}>Notebook session ${s.id || s.session_id}</li>`)}</ul>` : html`<div class="muted">Idle.</div>`}
    <details class="mt-6"><summary class="small muted" style="cursor:pointer">Raw node record</summary><pre class="code mt-2" style="max-height:300px">${jsonPretty(d)}</pre></details>`}</${Drawer}>`;
}
