// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Jobs: filterable live list, detail with failure panel, follow-tail logs, timeline, output and artifacts, cancel and retry.
import { html, useState, useEffect, useRef, useMemo, useAsync, usePoll } from '../core/h.js';
import { api, can, getAccessToken } from '../api/client.js';
import { Btn, Badge, StatusBadge, Light, Banner, ErrorBox, Loading, Empty, Tabs, TabPanel, Kv, Card, PageHead, Select, Input, Check, Progress, Switch } from '../ui/basic.js';
import { Table, JsonView, LoadMore } from '../ui/data.js';
import { Icon } from '../ui/icons.js';
import { act, toast, confirmDialog, MenuButton } from '../ui/overlay.js';
import { enc, fmtDuration, fmtTime, ago, durationBetween, fmtBytes, fmtMb, jsonPretty, downloadBlob, titleCase, shortId, bus, lightState, copyText } from '../core/util.js';
import { useQueryState, Ago, useWorkspaces } from './common.js';
import { jobHref, runHref, itemHref } from '../core/links.js';
import { Ansi } from '../notebook/outputs.js';
import { useRoute, setQuery } from '../core/router.js';

const ACTIVE = ['accepted', 'validating', 'queued', 'provisioning', 'dispatching', 'running'];
const TERMINAL = ['succeeded', 'failed', 'cancelled', 'timed_out'];
const STATES = ['queued', 'running', 'succeeded', 'failed', 'cancelled', 'timed_out'];
const isActive = (s) => ACTIVE.includes(s) || s === 'cancelling';
const stateKind = (s) => ({ succeeded: 'ok', failed: 'bad', timed_out: 'bad', cancelled: 'warn', running: 'busy', queued: 'degraded', provisioning: 'busy', dispatching: 'busy', cancelling: 'warn' }[s] || '');
export const JobState = ({ state }) => html`<span class=${'badge ' + (stateKind(state) || 'unknown')}><${Light} state=${stateKind(state) === 'bad' ? 'down' : stateKind(state) === 'ok' ? 'ok' : stateKind(state) === 'warn' ? 'degraded' : stateKind(state) === 'degraded' ? 'degraded' : stateKind(state) === 'busy' ? 'busy' : 'unknown'} label="" /> ${titleCase(state)}</span>`;
const dur = (j) => { const s = durationBetween(j.started_at, j.finished_at || (isActive(j.state) ? null : j.updated_at)); return s == null ? '–' : fmtDuration(s); };

// ---- list ---------------------------------------------------------------------------------------------------------------------------------------
export function JobsPage() {
  const [state, setState] = useQueryState('state', '');
  const [kind, setKind] = useQueryState('kind', '');
  const [origin, setOrigin] = useQueryState('origin', '');
  const [text, setText] = useQueryState('q', '');
  const [window_, setWindow] = useQueryState('since', '');
  const [live, setLive] = useState(true);
  const [more, setMore] = useState({ items: [], cursor: null, loading: false });
  const sel = state ? state.split(',') : [];
  const since = window_ ? new Date(Date.now() - Number(window_) * 3600e3).toISOString() : undefined;
  const q = { state: sel.length ? sel : undefined, kind: kind || undefined, origin: origin || undefined, text: text || undefined, since, limit: 50 };
  const key = JSON.stringify(q);
  const list = usePoll(() => api.get('/jobs', q), live ? 3000 : 3600000, [key]);
  const stats = usePoll(() => api.get('/jobs/stats').catch(() => ({ counts: {}, queues: {} })), 5000, []);
  useEffect(() => bus.on('event:job', () => live && list.reload()), [live, key]);
  useEffect(() => { setMore({ items: [], cursor: null, loading: false }); }, [key]);
  const items = [...((list.data && list.data.items) || []), ...more.items];
  const cursor = more.items.length ? more.cursor : list.data && list.data.next_cursor;
  async function loadMore() { setMore((m) => ({ ...m, loading: true })); const r = await api.get('/jobs', { ...q, cursor }); setMore((m) => ({ items: [...m.items, ...r.items], cursor: r.next_cursor, loading: false })); }
  const toggle = (s) => setState((sel.includes(s) ? sel.filter((x) => x !== s) : [...sel, s]).join(','));
  const c = (stats.data && stats.data.counts) || {};
  return html`<div class="page wide"><${PageHead} title="Jobs" icon="play-all" sub="Every notebook, Spark job, SQL statement and script that ran or is waiting to run." actions=${html`<${Switch} label="Live" checked=${live} onChange=${setLive} />`} />
    <div class="grid c4 mb-4"><div class="stat"><div class="stat-top"><span class="stat-label">Running</span><span class="light busy"></span></div><div class="stat-value">${c.running ?? 0}</div></div>
      <div class="stat"><div class="stat-top"><span class="stat-label">Queued</span><span class=${'light ' + ((c.queued || 0) > 5 ? 'degraded' : 'ok')}></span></div><div class="stat-value">${c.queued ?? 0}</div></div>
      <div class="stat"><div class="stat-top"><span class="stat-label">Failed (shown)</span></div><div class="stat-value">${items.filter((j) => j.state === 'failed').length}</div></div>
      <div class="stat"><div class="stat-top"><span class="stat-label">Succeeded (shown)</span></div><div class="stat-value">${items.filter((j) => j.state === 'succeeded').length}</div></div></div>
    <div class="toolbar mb-3" role="search"><input class="input" style="max-width:260px" placeholder="Search by name or id" aria-label="Search jobs" value=${text} onInput=${(e) => setText(e.target.value)} />
      <${Select} label="Kind" size="sm" value=${kind} onChange=${setKind} placeholder="All kinds" options=${['notebook', 'spark_job', 'sql', 'capsule', 'script'].map((k) => ({ value: k, label: titleCase(k) }))} />
      <${Select} label="Origin" size="sm" value=${origin} onChange=${setOrigin} placeholder="Any origin" options=${['standalone', 'fabric', 'pipeline', 'schedule', 'api', 'mcp'].map((k) => ({ value: k, label: titleCase(k) }))} />
      <${Select} label="Time window" size="sm" value=${window_} onChange=${setWindow} placeholder="All time" options=${[['1', 'Last hour'], ['24', 'Last 24 hours'], ['168', 'Last 7 days']].map(([value, label]) => ({ value, label }))} />
      <div class="chips" role="group" aria-label="State filter">${STATES.map((s) => html`<button key=${s} class=${'chip' + (sel.includes(s) ? ' on' : '')} aria-pressed=${sel.includes(s)} onClick=${() => toggle(s)}>${titleCase(s)}</button>`)}</div></div>
    ${list.loading && !list.data ? html`<${Loading} />` : list.error && !list.data ? html`<${ErrorBox} error=${list.error} onRetry=${list.reloadLoud} />` : html`
    <${Table} caption="Jobs" rows=${items} onRowClick=${(j) => { window.location.hash = jobHref(j.id); }} empty=${html`<${Empty} icon="play-all" title="No jobs match" action=${html`<${Btn} onClick=${() => { setState(''); setKind(''); setOrigin(''); setText(''); setWindow(''); }}>Clear filters<//>`}>Run a notebook or submit a script, and it appears here.<//>`} columns=${[
      { key: 'state', label: 'State', render: (j) => html`<${JobState} state=${j.state} />`, sort: (j) => j.state },
      { key: 'name', label: 'Name', render: (j) => html`<a href=${jobHref(j.id)}><b>${j.name || shortId(j.id)}</b></a><div class="small muted mono">${shortId(j.id, 10)}</div>`, sort: (j) => j.name || '' },
      { key: 'kind', label: 'Kind', render: (j) => html`${titleCase(j.kind)} <span class="muted small">${j.engine}</span>` },
      { key: 'origin', label: 'Origin', render: (j) => titleCase((j.origin || {}).kind || '') },
      { key: 'runtime', label: 'Runtime', render: (j) => html`<span class="mono small">${j.runtime_id || '–'}</span>` },
      { key: 'queue', label: 'Queue', render: (j) => j.queue },
      { key: 'dur', label: 'Duration', num: true, render: dur, sort: (j) => durationBetween(j.started_at, j.finished_at) || 0 },
      { key: 'created', label: 'Submitted', render: (j) => html`<${Ago} ts=${j.created_at} />`, sort: (j) => j.created_at },
    ]} initialSort=${{ key: 'created', dir: -1 }} />
    <${LoadMore} cursor=${cursor} loading=${more.loading} onMore=${loadMore} />`}</div>`;
}

// ---- detail ---------------------------------------------------------------------------------------------------------------------------------------
export function useJobLogs(jobId, active, stream = 'combined') {
  const [text, setText] = useState('');
  const [size, setSize] = useState(0);
  const off = useRef(0);
  useEffect(() => {
    off.current = 0; setText(''); setSize(0);
    let stop = false; let timer;
    const tick = async () => {
      try {
        const p = await api.get(`/jobs/${enc(jobId)}/logs`, { offset: off.current, limit: 65536, stream });
        if (stop) return;
        if (p.data) setText((t) => (t + p.data).slice(-600000));
        off.current = p.next_offset ?? off.current; setSize(p.size ?? off.current);
        if (!p.eof) timer = setTimeout(tick, 100); else if (active) timer = setTimeout(tick, 1200);
      } catch { if (!stop) timer = setTimeout(tick, 4000); }
    };
    tick();
    return () => { stop = true; clearTimeout(timer); };
  }, [jobId, active, stream]);
  return { text, size };
}

export function LogView({ jobId, live }) {
  const [stream, setStream] = useState('combined');
  const [follow, setFollow] = useState(true);
  const [filter, setFilter] = useState('');
  const { text, size } = useJobLogs(jobId, live, stream);
  const ref = useRef();
  useEffect(() => { if (follow && ref.current) ref.current.scrollTop = ref.current.scrollHeight; }, [text, follow, filter]);
  const shown = filter ? text.split('\n').filter((l) => l.toLowerCase().includes(filter.toLowerCase())).join('\n') : text;
  return html`<div><div class="toolbar mb-2">
    <${Select} label="Stream" size="sm" value=${stream} onChange=${setStream} options=${[{ value: 'combined', label: 'Combined' }, { value: 'stdout', label: 'Standard output' }, { value: 'stderr', label: 'Standard error' }, { value: 'driver', label: 'Driver' }]} />
    <input class="input sm" style="max-width:220px" placeholder="Filter lines" aria-label="Filter log lines" value=${filter} onInput=${(e) => setFilter(e.target.value)} />
    <${Switch} label="Follow the end" checked=${follow} onChange=${setFollow} />${live ? html`<${Badge} kind="busy">live<//>` : null}<span class="grow"></span><span class="small muted">${fmtBytes(size)}</span>
    <${Btn} size="sm" icon="download" onClick=${() => { const b = new Blob([text], { type: 'text/plain' }); downloadBlob(b, `${jobId}.log`); }}>Download<//>
    <${Btn} size="sm" icon="copy" onClick=${() => copyText(text).then(() => toast.ok('Log copied'))}>Copy<//></div>
    <div class="code-wrap"><pre class="code logview" ref=${ref} tabindex="0" aria-label="Job log" onScroll=${(e) => { const el = e.target; const atEnd = el.scrollHeight - el.scrollTop - el.clientHeight < 24; if (!atEnd && follow) setFollow(false); }} data-testid="job-log"><${Ansi} text=${shown || (live ? 'Waiting for output…' : 'No log output.')} /></pre></div></div>`;
}

function FailurePanel({ job }) {
  const f = job.failure;
  if (!f) return null;
  return html`<${Banner} kind="bad" title=${`Failed: ${f.category || 'error'}`} remediation=${f.remediation}>
    <div class="mb-2">${f.message}</div>${f.cell != null || f.line != null ? html`<div class="small muted mb-2">${f.cell != null ? `Cell ${f.cell}` : ''}${f.line != null ? ` line ${f.line}` : ''}</div>` : null}
    ${f.traceback ? html`<details><summary>Traceback</summary><pre class="code mt-2" tabindex="0" style="max-height:320px;overflow:auto"><${Ansi} text=${f.traceback} /></pre></details>` : null}<//>`;
}

function Timeline({ jobId, live }) {
  const t = usePoll(() => api.get(`/jobs/${enc(jobId)}/timeline`), live ? 3000 : 3600000, [jobId]);
  if (t.loading && !t.data) return html`<${Loading} />`;
  if (t.error && !t.data) return html`<${ErrorBox} error=${t.error} />`;
  const items = t.data.items || [];
  return html`<ol class="timeline" aria-label="Job timeline">${items.map((e, i) => { const prev = items[i - 1]; const gap = prev ? durationBetween(prev.ts, e.ts) : null; return html`<li key=${e.seq} class=${e.kind}><span class="tl-dot"></span><div><div class="row gap-2">${e.kind === 'state' ? html`<b>${titleCase(e.to)}</b>` : html`<span class="mono">${e.event}</span>`}<span class="small muted">${fmtTime(e.ts)}${gap != null && gap > 0.05 ? ` · +${gap < 1 ? Math.round(gap * 1000) + ' ms' : fmtDuration(gap)}` : ''}</span></div>
    ${e.reason ? html`<div class="small muted">${e.reason}</div>` : null}${e.kind === 'event' && e.data && Object.keys(e.data).length ? html`<div class="small mono muted trunc" style="max-width:640px">${JSON.stringify(e.data)}</div>` : null}</div></li>`; })}</ol>`;
}

export function JobDetailPage({ params }) {
  const id = params.id;
  const [tab, setTab] = useQueryState('tab', 'logs');
  const job = usePoll(() => api.get(`/jobs/${enc(id)}`), 2000, [id]);
  const j = job.data;
  const live = !!j && isActive(j.state);
  useEffect(() => bus.on('event:job', (e) => { if (e.payload && e.payload.job_id === id) job.reload(); }), [id]);
  const out = useAsync(() => (j && TERMINAL.includes(j.state) ? api.get(`/jobs/${enc(id)}/output`).catch(() => null) : null), [id, j && j.state]);
  const arts = useAsync(() => (j && TERMINAL.includes(j.state) ? api.get(`/jobs/${enc(id)}/artifacts`).then((r) => r.items || []).catch(() => []) : []), [id, j && j.state]);
  const nodeName = useAsync(() => (j && j.driver_worker_id ? api.get(`/workers/${enc(j.driver_worker_id)}`).then((w) => w.name).catch(() => null) : null), [j && j.driver_worker_id]);
  if (job.loading && !j) return html`<${Loading} label="Loading job" />`;
  if (job.error && !j) return html`<div class="page"><${ErrorBox} error=${job.error} onRetry=${job.reloadLoud} /><div class="mt-3"><${Btn} href="#/jobs">Back to jobs<//></div></div>`;
  const canCancel = can('job.cancel', j.workspace_id); const canRetry = can('job.retry', j.workspace_id);
  async function cancel() { if (await confirmDialog({ title: 'Cancel this job?', danger: true, confirmLabel: 'Cancel job', cancelLabel: 'Keep running', body: 'The job is asked to stop. Work already written stays.' })) { await act(() => api.post(`/jobs/${enc(id)}/cancel`, {}), { ok: 'Cancel requested' }); job.reload(); } }
  async function retry() { const r = await act(() => api.post(`/jobs/${enc(id)}/retry`, {}), { ok: 'Retry submitted' }); if (r && r.id) window.location.hash = jobHref(r.id); }
  const tabs = [{ id: 'logs', label: 'Logs' }, { id: 'overview', label: 'Overview' }, { id: 'timeline', label: 'Timeline' }, { id: 'output', label: 'Output' + ((arts.data || []).length ? ` (${arts.data.length})` : '') }, { id: 'raw', label: 'Raw' }];
  const wait = j.state === 'queued' && j.wait_reason;
  return html`<div class="page wide">
    <${PageHead} title=${j.name || shortId(j.id)} icon="play-all" badge=${html`<span style="margin-left:10px"><${JobState} state=${j.state} /></span>`} sub=${html`<span class="mono">${j.id}</span> · ${titleCase(j.kind)} · ${j.engine} · submitted <${Ago} ts=${j.created_at} />`} actions=${html`
      <${Btn} icon="refresh" onClick=${job.reloadLoud}>Refresh<//>${canCancel && isActive(j.state) ? html`<${Btn} kind="danger" icon="stop" onClick=${cancel} data-testid="job-cancel">Cancel<//>` : null}${canRetry && ['failed', 'cancelled', 'timed_out'].includes(j.state) ? html`<${Btn} kind="primary" icon="refresh" onClick=${retry} data-testid="job-retry">Retry<//>` : null}
      ${j.item_id ? html`<${Btn} href=${j.kind === 'notebook' ? `#/notebook/${enc(j.item_id)}` : `#/workspaces/${enc(j.workspace_id)}`}>Open item<//>` : null}`} />
    ${j.state === 'failed' || j.state === 'timed_out' ? html`<${FailurePanel} job=${j} />` : null}
    ${j.state === 'queued' && j.queue_reason ? html`<${Banner} kind="warn" title=${`Waiting in the queue: ${titleCase(String(j.queue_reason.code).replace(/_/g, ' '))}`} remediation=${j.queue_reason.remediation}>${j.queue_reason.message}<//>` : wait ? html`<${Banner} kind="info" title="Waiting in the queue">${j.wait_reason}<//>` : null}
    ${j.runtime_id === 'olcf-host-python' ? html`<${Banner} kind="info" title="Running on the host Python runtime">This job uses the worker's own Python (no Spark). Install a Spark runtime to use PySpark or Spark SQL.<//>` : null}
    ${j.state === 'cancelled' ? html`<${Banner} kind="warn" title="This job was cancelled">${j.state_reason || ''}<//>` : null}
    <div class="grid c4 mb-4"><div class="stat"><div class="stat-top"><span class="stat-label">Duration</span></div><div class="stat-value">${dur(j)}</div><div class="stat-sub">${j.started_at ? 'started ' + ago(j.started_at) : 'not started'}</div></div>
      <div class="stat"><div class="stat-top"><span class="stat-label">Attempt</span></div><div class="stat-value">${j.attempt}<span class="muted" style="font-size:.6em"> of ${j.max_attempts}</span></div><div class="stat-sub">${j.retry_of ? 'retry of ' + shortId(j.retry_of) : 'first attempt'}</div></div>
      <div class="stat"><div class="stat-top"><span class="stat-label">Runtime</span></div><div class="stat-value" style="font-size:var(--fs-lg)">${j.runtime_id || '–'}</div><div class="stat-sub">${j.provider || ''}${j.cluster_id ? ' · ' + j.cluster_id : ''}</div></div>
      <div class="stat"><div class="stat-top"><span class="stat-label">Node</span></div><div class="stat-value" style="font-size:var(--fs-lg)">${j.driver_worker_id ? (nodeName.data || shortId(j.driver_worker_id, 8)) : '–'}</div><div class="stat-sub">queue ${j.queue}, priority ${j.priority}</div></div></div>
    <${Tabs} tabs=${tabs} value=${tab} onChange=${setTab} label="Job sections" />
    <div class="tab-body">
      <${TabPanel} id="logs" value=${tab}><${LogView} jobId=${id} live=${live} /></${TabPanel}>
      <${TabPanel} id="overview" value=${tab}><div class="grid c2"><${Card} title="Run"><${Kv} items=${[['State', html`<${JobState} state=${j.state} />`], ['Reason', j.state_reason], ['Origin', html`${titleCase((j.origin || {}).kind)} ${(j.origin || {}).details && Object.keys(j.origin.details).length ? html`<span class="small mono muted">${JSON.stringify(j.origin.details)}</span>` : ''}`], ['Queued', fmtTime(j.queued_at)], ['Started', fmtTime(j.started_at)], ['Finished', fmtTime(j.finished_at)], ['Timeout at', fmtTime(j.timeout_at)], ['Correlation id', html`<span class="mono small">${j.correlation_id}</span>`]]} /></${Card}>
        <${Card} title="Resources"><${Kv} items=${[['Driver', `${(j.resources_granted || j.resources_request || {}).driver_cores || '–'} cores · ${fmtMb((j.resources_granted || j.resources_request || {}).driver_memory_mb)}`], ['Executors', (j.resources_granted || {}).executors != null ? `${j.resources_granted.executors} × ${j.resources_granted.executor_cores} cores · ${fmtMb(j.resources_granted.executor_memory_mb)}` : '–'], ['Pool', (j.resources_granted || {}).pool], ['Application', j.application_id], ['Peak memory', (j.result && j.result.metrics && j.result.metrics.max_rss_mb) ? fmtMb(j.result.metrics.max_rss_mb) : null], ['Python', j.result && j.result.metrics && j.result.metrics.python]]} /></${Card}>
        <${Card} title="Parameters"><${JsonView} value=${j.parameters || {}} depth=${2} /></${Card}>
        <${Card} title="Tags"><${JsonView} value=${j.tags || {}} depth=${1} /></${Card}></div></${TabPanel}>
      <${TabPanel} id="timeline" value=${tab}><${Timeline} jobId=${id} live=${live} /></${TabPanel}>
      <${TabPanel} id="output" value=${tab}>${out.loading ? html`<${Loading} />` : html`${j.exit_value != null ? html`<${Kv} items=${[['Exit value', html`<span class="mono">${typeof j.exit_value === 'string' ? j.exit_value : JSON.stringify(j.exit_value)}</span>`]]} />` : null}
        ${(arts.data || []).length ? html`<${Table} dense caption="Artifacts" rows=${arts.data.map((a) => ({ ...a, id: a.name }))} columns=${[{ key: 'name', label: 'Name' }, { key: 'size', label: 'Size', num: true, render: (a) => fmtBytes(a.size) }, { key: 'sha256', label: 'sha256', render: (a) => html`<span class="mono small">${(a.sha256 || '').slice(0, 16)}</span>` }, { key: 'dl', label: '', sortable: false, render: (a) => html`<${Btn} size="sm" icon="download" onClick=${async () => { const b = await act(() => api.blob(`/jobs/${enc(id)}/artifacts/${enc(a.name)}`)); if (b) downloadBlob(b, a.name); }}>Download<//>` }]} />` : html`<${Empty} icon="package" title="No artifacts">This job produced no files.<//>`}`}</${TabPanel}>
      <${TabPanel} id="raw" value=${tab}><${JsonView} value=${j} depth=${1} /></${TabPanel}></div></div>`;
}
