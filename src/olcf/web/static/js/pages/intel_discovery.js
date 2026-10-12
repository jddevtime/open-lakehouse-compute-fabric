// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Discovery: where Intelligence learns from (sources), what scans found and could not see, what has changed between scans, and the sampling policy that decides whether
// anything may ever look at data values (off by default).
import { html, useState, useEffect, useRef, useAsync, usePoll } from '../core/h.js';
import { api, request, can } from '../api/client.js';
import { Btn, Badge, Banner, Card, Check, Empty, ErrorBox, Input, Loading, PageHead, Progress, Select, Steps, Switch, Tabs, TabPanel, Textarea, Kv, Seg } from '../ui/basic.js';
import { Icon } from '../ui/icons.js';
import { Dialog, Drawer, confirmDialog, toast, act } from '../ui/overlay.js';
import { Table } from '../ui/data.js';
import { fmtNum, fmtDuration, durationBetween, ago, enc, fmtBytes } from '../core/util.js';
import { useQueryState, loadWorkspaces } from './common.js';
import { typeLabel, gapMeta, GAP_KINDS, sourceKindLabel, scanLabel, edgeVerb, assetHref, discoveryHref } from '../intel/model.js';
import { AssetLink, ConfBadge, ScanBadge, Ago, ScanDialog, ScanProgress, useScan, scanCounts, useSources } from './intel_common.js';

const CODE_TYPES = ['notebook', 'pipeline', 'activity', 'sql_query', 'spark_job', 'script', 'dataflow'];
const UNREADABLE = new Set(['parse_error', 'unsupported_construct', 'dynamic_reference', 'not_inspected', 'permission_denied', 'api_unavailable']);
const MAX_IMPORT_MB = 50;

const stepState = (s) => ({ ...s, state: s.skipped ? 'unknown' : s.ok ? 'ok' : 'down', detail: s.skipped ? 'Not tried, because an earlier step failed.' : s.detail, remediation: s.ok || s.skipped ? null : s.remediation, ms: s.skipped ? null : s.duration_ms });

/** The stepwise result of a connectivity test, in the same shape as the Fabric page's connection test. */
function TestResult({ res }) {
  return html`<div data-testid="test-result"><${Banner} kind=${res.ok ? 'ok' : 'warn'} title=${res.ok ? 'Every step worked' : `It stopped at: ${res.failed_step}`}>${res.ok ? 'This source can be scanned.' : 'Steps after the first failure were not tried, so the first thing to repair is the one marked in red.'}<//>
    <div class="mt-3"><${Steps} steps=${(res.steps || []).map(stepState)} /></div></div>`;
}

// ---------------------------------------------------------------------------------------------------------------- source dialog
const INC = [['', 'Platform default'], ['0', 'Never, only when I ask'], ['300', 'Every 5 minutes'], ['900', 'Every 15 minutes'], ['1800', 'Every 30 minutes'], ['3600', 'Every hour'], ['21600', 'Every 6 hours'], ['86400', 'Every day']];
const FULL = [['', 'Platform default'], ['0', 'Never, only when I ask'], ['21600', 'Every 6 hours'], ['86400', 'Every day'], ['604800', 'Every week']];
const KINDS = [
  { value: 'fabric', label: 'Microsoft Fabric and Power BI', help: 'Reads workspaces, items, report and model definitions and table structure through Microsoft APIs, with a service principal you provide.' },
  { value: 'catalog_mirror', label: 'The OLCF catalog', help: 'Brings in the table descriptions, tags and lineage already recorded in the built-in catalog.' },
  { value: 'local', label: 'Another view of this installation', help: 'Reads the workspaces, lakehouses, notebooks and reports of this installation. One is built in; add another to scan only some workspaces.' },
];
const lines = (v) => (v || []).join('\n');
const unlines = (t) => String(t || '').split('\n').map((x) => x.trim()).filter(Boolean);

function SourceDialog({ source, onClose }) {
  const editing = !!source;
  const cfg = (source && source.config) || {};
  const [kind, setKind] = useState(source ? source.kind : 'fabric');
  const [name, setName] = useState(source ? source.name : '');
  const [enabled, setEnabled] = useState(source ? source.enabled : true);
  const [inc, setInc] = useState(source && source.schedule && source.schedule.incremental_seconds != null ? String(source.schedule.incremental_seconds) : '');
  const [full, setFull] = useState(source && source.schedule && source.schedule.full_seconds != null ? String(source.schedule.full_seconds) : '');
  const [tenant, setTenant] = useState((cfg.metadata_identity && cfg.metadata_identity.tenant_id) || cfg.tenant_id || '');
  const [client, setClient] = useState((cfg.metadata_identity && cfg.metadata_identity.client_id) || '');
  const [secret, setSecret] = useState('');
  const [filter, setFilter] = useState(lines(cfg.workspace_ids));
  const [definitions, setDefinitions] = useState(cfg.definitions !== false);
  const [tables, setTables] = useState(cfg.tables !== false);
  const [personal, setPersonal] = useState(!!cfg.include_personal);
  const [sep, setSep] = useState(!!cfg.sampling_identity);
  const [sTenant, setSTenant] = useState((cfg.sampling_identity && cfg.sampling_identity.tenant_id) || '');
  const [sClient, setSClient] = useState((cfg.sampling_identity && cfg.sampling_identity.client_id) || '');
  const [sSecret, setSSecret] = useState('');
  const [busy, setBusy] = useState(false);
  const [testing, setTesting] = useState(false);
  const [err, setErr] = useState(null);
  const [res, setRes] = useState(null);
  const [fieldErr, setFieldErr] = useState({});
  const ws = useAsync(() => loadWorkspaces().catch(() => []), []);
  const hadSampling = !!cfg.sampling_identity;

  const show = (e) => { setFieldErr(e); if (Object.keys(e).length) requestAnimationFrame(() => { const x = document.querySelector('[data-testid=source-dialog] .err'); if (x) x.scrollIntoView({ block: 'center' }); }); return Object.keys(e).length > 0; };
  function problems() {
    const e = {};
    if (!name.trim()) e.name = 'Give the source a name';
    if (kind === 'fabric') {
      if (!tenant.trim()) e.tenant = 'The tenant id (or domain) is needed';
      if (!client.trim()) e.client = 'The client id (application id) of the service principal is needed';
      if (!editing && !secret) e.secret = 'The client secret is needed. It is stored encrypted and never shown again';
      if (sep && (!sClient.trim() || (!hadSampling && !sSecret))) e.sampling = 'A separate sampling identity needs its client id and secret';
    }
    return e;
  }
  function payload(forTest) {
    const config = {};
    if (kind === 'fabric') {
      config.tenant_id = tenant.trim();
      config.metadata_identity = { tenant_id: tenant.trim(), client_id: client.trim() };
      const ids = unlines(filter);
      if (ids.length) config.workspace_ids = ids; else if (editing) config.workspace_ids = null;
      config.definitions = definitions; config.tables = tables; config.include_personal = personal;
      if (sep && (sSecret || !hadSampling || forTest)) config.sampling_identity = { tenant_id: (sTenant || tenant).trim(), client_id: sClient.trim() };
      else if (!sep && editing && hadSampling) config.sampling_identity = null;
    } else {
      const ids = unlines(filter);
      if (ids.length) config.workspace_ids = ids; else if (editing) config.workspace_ids = null;
    }
    const schedule = {};
    if (inc !== '') schedule.incremental_seconds = Number(inc);
    if (full !== '') schedule.full_seconds = Number(full);
    const body = { name: name.trim(), config, schedule, enabled };
    if (kind === 'fabric') {
      if (secret) body.credentials = { client_secret: secret };
      if (sep && sSecret) body.sampling_credentials = { client_secret: sSecret };
    }
    return body;
  }
  async function test() {
    const e = problems(); delete e.secret;
    if (show(e)) return;
    setTesting(true); setErr(null); setRes(null);
    try {
      if (editing && !secret) setRes(await api.post(`/intelligence/sources/${enc(source.id)}/test`));
      else { const b = payload(true); setRes(await api.post('/intelligence/sources/test', { kind, config: b.config, credentials: b.credentials, sampling_credentials: b.sampling_credentials })); }
    } catch (x) { setErr(x); } finally { setTesting(false); }
  }
  async function save() {
    if (show(problems())) return;
    setBusy(true); setErr(null);
    try {
      const b = payload(false);
      const r = editing ? await api.full('PATCH', `/intelligence/sources/${enc(source.id)}`, { body: b, ifMatch: source.etag }).then((x) => x.data) : await api.post('/intelligence/sources', { kind, ...b });
      setSecret(''); setSSecret('');
      toast.ok(editing ? 'Source saved' : 'Source added');
      onClose(r);
    } catch (x) { setErr(x); } finally { setBusy(false); }
  }
  const kindInfo = KINDS.find((k) => k.value === kind);
  return html`<${Dialog} title=${editing ? `Edit ${source.name}` : 'Add a source'} size="lg" onClose=${() => onClose(null)}
    footer=${html`<${Btn} onClick=${() => onClose(null)}>Cancel<//>${kind !== 'local' && kind !== 'catalog_mirror' ? html`<${Btn} icon="activity" busy=${testing} onClick=${test}>Test connectivity<//>` : null}<${Btn} kind="primary" busy=${busy} onClick=${save}>${editing ? 'Save changes' : 'Add source'}<//>`}>
    <div class="col gap-1" data-testid="source-dialog">
      ${err ? html`<div class="mb-3"><${ErrorBox} error=${err} /></div>` : null}
      ${!editing ? html`<fieldset class="fieldset"><legend>What kind of source</legend>${KINDS.map((k) => html`<label class="radio-row" key=${k.value}><input type="radio" name="src-kind" checked=${kind === k.value} onChange=${() => { setKind(k.value); setRes(null); }} /><span><b>${k.label}</b><span class="small muted block">${k.help}</span></span></label>`)}
        <div class="small muted mt-2">To bring in a saved Power BI scanner file, use the Import tab instead.</div></fieldset>` : html`<p class="small muted">${kindInfo ? kindInfo.help : sourceKindLabel(kind)}</p>`}
      <${Input} label="Name" value=${name} onInput=${setName} error=${fieldErr.name} required=${true} data-autofocus=${true} maxLength="128" />
      ${kind === 'fabric' ? html`
        <fieldset class="fieldset"><legend>Who to scan as</legend>
          <p class="small muted mt-0">A service principal (an app registration) that may read the Fabric or Power BI metadata. Scanning reads names, structure and definitions, not the data inside tables.</p>
          <${Input} label="Tenant id" value=${tenant} onInput=${setTenant} error=${fieldErr.tenant} placeholder="Directory (tenant) id" mono=${true} required=${true} />
          <${Input} label="Client id" value=${client} onInput=${setClient} error=${fieldErr.client} placeholder="Application (client) id" mono=${true} required=${true} />
          <${Input} label="Client secret" type="password" autocomplete="new-password" value=${secret} onInput=${setSecret} error=${fieldErr.secret} required=${!editing}
            hint=${editing ? (source.has_credentials ? 'A secret is stored and is never shown. Leave this empty to keep it, or type a new one to replace it.' : 'No secret is stored yet.') : 'Stored encrypted in the secret store. It cannot be read back, only replaced.'} /></fieldset>
        <${Textarea} label="Only these Fabric workspaces (optional)" value=${filter} onInput=${setFilter} rows=${3} placeholder="One workspace id or name per line. Leave empty for everything the identity can see." />
        <div class="row-wrap" style="gap:var(--s-5)"><${Switch} label="Read report, model and notebook definitions" checked=${definitions} onChange=${setDefinitions} /><${Switch} label="Read table structure from OneLake" checked=${tables} onChange=${setTables} /><${Switch} label="Include personal workspaces" checked=${personal} onChange=${setPersonal} /></div>
        <fieldset class="fieldset mt-3"><legend>Sampling identity (optional)</legend>
          <${Switch} label="Use a separate identity for reading sample rows" checked=${sep} onChange=${setSep} />
          <p class="small muted">Looking at table values is off unless an administrator allows it in Sampling policies. If you ever want it for Fabric tables, give it its own identity here, so the scanning identity above never needs permission to read data.</p>
          ${sep ? html`<${Input} label="Tenant id" value=${sTenant} onInput=${setSTenant} placeholder=${tenant || 'Same as above'} mono=${true} />
            <${Input} label="Client id" value=${sClient} onInput=${setSClient} mono=${true} />
            <${Input} label="Client secret" type="password" autocomplete="new-password" value=${sSecret} onInput=${setSSecret} error=${fieldErr.sampling} hint=${hadSampling ? 'A secret is stored and never shown. Leave empty to keep it.' : 'Stored encrypted and never shown again.'} />` : null}</fieldset>`
        : html`<div class="field"><label>Only these workspaces (optional)</label>
          <div class="col" style="max-height:160px;overflow:auto;border:1px solid var(--line);border-radius:var(--r-sm);padding:6px 10px">${(ws.data || []).map((w) => html`<label class="check" key=${w.id}><input type="checkbox" checked=${unlines(filter).includes(w.id)} onChange=${(e) => setFilter(lines(e.target.checked ? [...unlines(filter), w.id] : unlines(filter).filter((x) => x !== w.id)))} /> <span>${w.display_name || w.name}</span></label>`)}${(ws.data || []).length ? null : html`<span class="muted small">No workspaces yet.</span>`}</div><div class="hint">Leave all unticked to read everything.</div></div>`}
      <div class="grid c2"><${Select} label="Look for changes" value=${inc} onChange=${setInc} options=${INC.map(([value, label]) => ({ value, label }))} hint="Quick scans that only re-read what changed." />
        <${Select} label="Re-read everything" value=${full} onChange=${setFull} options=${FULL.map(([value, label]) => ({ value, label }))} hint="Slower. Also notices what disappeared." /></div>
      <${Switch} label="Scan this source on its schedule" checked=${enabled} onChange=${setEnabled} />
      ${res ? html`<div class="mt-3"><${TestResult} res=${res} /></div>` : null}
    </div></${Dialog}>`;
}

// ---------------------------------------------------------------------------------------------------------------- sources
function SourceRow({ s, manage, onScan, onEdit, onDelete, onToggle }) {
  const [test, setTest] = useState(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  async function run() {
    setBusy(true); setErr(null); setTest(null);
    try { setTest(await api.post(`/intelligence/sources/${enc(s.id)}/test`)); } catch (e) { setErr(e); } finally { setBusy(false); }
  }
  const sched = s.effective_schedule || {};
  const every = (sec) => (sec ? (sec % 86400 === 0 ? `${sec / 86400} d` : sec % 3600 === 0 ? `${sec / 3600} h` : `${Math.round(sec / 60)} min`) : 'never');
  return html`<div class="source-card-wrap" data-testid=${'source-' + s.name}>
    <div class="source-card"><div class="meta"><h3>${s.name} ${s.system ? html`<${Badge} kind="outline">built in<//>` : null}${s.enabled ? '' : html` <${Badge}>switched off<//>`}</h3>
      <div class="small muted">${sourceKindLabel(s.kind)}${s.kind === 'fabric' ? (s.has_credentials ? ' · secret stored' : ' · no secret') + (s.has_sampling_identity ? ' · separate sampling identity' : '') : ''}${s.kind === 'scanner_import' && s.import ? ` · ${fmtBytes(s.import.bytes)} stored` : ''}</div>
      <div class="small muted">${s.kind === 'scanner_import' ? 'Scanned when imported' : `Quick scan ${every(sched.incremental_seconds)}, full scan ${every(sched.full_seconds)}`}${s.next_due_at && s.enabled && s.kind !== 'scanner_import' ? html` · next <${Ago} ts=${s.next_due_at} />` : ''}</div></div>
      <div><${ScanBadge} status=${s.last_status} running=${s.running} /><div class="small muted mt-1">${s.last_scan_at ? html`<${Ago} ts=${s.last_scan_at} />` : 'never scanned'}</div></div>
      <div class="row-wrap">
        ${manage && s.kind !== 'scanner_import' ? html`<${Btn} size="sm" icon="activity" busy=${busy} onClick=${run}>Test connectivity<//>` : null}
        ${manage && s.enabled && s.kind !== 'scanner_import' ? html`<${Btn} size="sm" kind="primary" icon="refresh" onClick=${() => onScan(s)} disabled=${s.running}>Scan now<//>` : null}
        ${manage ? html`<${Btn} size="sm" icon="edit" onClick=${() => onEdit(s)} disabled=${s.kind === 'scanner_import'} title=${s.kind === 'scanner_import' ? 'Import the file again to replace it' : undefined}>Edit<//>` : null}
        ${manage && !s.system ? html`<${Btn} size="sm" kind="ghost" iconOnly icon="trash" label=${`Delete ${s.name}`} onClick=${() => onDelete(s)} />` : null}</div></div>
    ${err || test ? html`<div class="pad" style="padding:0 var(--s-4) var(--s-4)">${err ? html`<${ErrorBox} error=${err} />` : html`<${TestResult} res=${test} />`}</div>` : null}</div>`;
}

function SourcesTab({ setTab }) {
  const src = useSources();
  const manage = can('intelligence.manage');
  const [dialog, setDialog] = useState(null);
  const [scan, setScan] = useState(null);
  async function del(s) {
    const ok = await confirmDialog({ title: `Delete ${s.name}?`, body: `Assets that only this source reported are marked as removed. Their history is kept. Stored credentials of this source are deleted.`, confirmLabel: 'Delete the source', danger: true });
    if (!ok) return;
    await act(() => api.del(`/intelligence/sources/${enc(s.id)}`), { ok: 'Source deleted' });
    src.reload();
  }
  return html`<div class="col gap-4">
    <div class="row-wrap">${manage ? html`<${Btn} kind="primary" icon="plus" onClick=${() => setDialog({})}>Add a source<//><${Btn} icon="upload" onClick=${() => setTab('import')}>Import a scanner file<//>` : html`<span class="muted small">You can see the sources. Changing them needs permission to manage Intelligence.</span>`}</div>
    ${src.error && !src.data ? html`<${ErrorBox} error=${src.error} onRetry=${src.reloadLoud} />` : !src.data ? html`<${Loading} />` : html`<${Card} flush=${true} title="Sources" sub=${`${src.items.length}`}>${src.items.map((s) => html`<${SourceRow} key=${s.id} s=${s} manage=${manage} onScan=${(x) => setScan(x.id)} onEdit=${(x) => setDialog(x)} onDelete=${del} />`)}</${Card}>`}
    ${dialog ? html`<${SourceDialog} source=${dialog.id ? dialog : null} onClose=${(r) => { setDialog(null); if (r) src.reload(); }} />` : null}
    ${scan ? html`<${ScanDialog} sources=${src.items} sourceId=${scan} onClose=${() => { setScan(null); src.reload(); }} onDone=${() => src.reload()} />` : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- scans
function ScanDetail({ id, sources, onClose }) {
  const { scan, error } = useScan(id, { every: 1000 });
  const manage = can('intelligence.manage');
  const [busy, setBusy] = useState(false);
  const s = scan;
  const st = (s && s.stats) || {};
  const ap = st.apply || {};
  const src = s ? sources.find((x) => x.id === s.source_id) : null;
  async function cancel() { setBusy(true); try { await api.post(`/intelligence/scans/${enc(id)}/cancel`); toast.info('The scan stops after the batch in progress'); } catch (e) { toast.bad('Could not cancel', e && e.message); } finally { setBusy(false); } }
  return html`<${Drawer} title=${s ? `Scan ${s.version} of ${src ? src.name : 'a source'}` : 'Scan'} onClose=${onClose} wide=${true} actions=${s && s.status === 'running' && manage ? html`<${Btn} size="sm" kind="danger" icon="stop" busy=${busy} onClick=${cancel}>Cancel the scan<//>` : null}>
    ${error && !s ? html`<${ErrorBox} error=${error} />` : !s ? html`<${Loading} />` : html`<div class="col gap-4" data-testid="scan-detail">
      <${ScanProgress} scanId=${id} compact=${true} />
      <${Card} title="What it found"><${Kv} items=${[
        ['Assets seen', fmtNum(scanCounts(s).seen)], ['New assets', fmtNum(ap.assets_created || 0)], ['Changed assets', fmtNum(ap.assets_updated || 0)], ['Unchanged', fmtNum(ap.assets_unchanged || 0)],
        ['Placeholders created', fmtNum(ap.placeholders_created || 0)], ['Connections created', fmtNum(ap.edges_created || 0)], ['Connections updated', fmtNum(ap.edges_updated || 0)], ['Connections rejected', fmtNum(ap.edges_rejected || 0)],
        ['Definitions read', fmtNum((st.facts && st.facts.definitions) || 0)], ['Definitions skipped (unchanged)', fmtNum(ap.analysis_skipped || 0)],
        st.sweep ? ['Removed by this scan', `${fmtNum(st.sweep.assets_removed || 0)} assets, ${fmtNum(st.sweep.edges_removed || 0)} connections`] : null,
        ['Mode', `${s.mode}${st.promoted_to_full ? ' (promoted to a full scan)' : ''}`], ['Started by', s.trigger],
        ['Took', s.finished_at ? fmtDuration(durationBetween(s.started_at, s.finished_at)) : 'still running'], st.workspace_filter ? ['Limited to', JSON.stringify(st.workspace_filter)] : null]} /></${Card}>
      <${Card} title=${`What it could not see (${(s.gaps && s.gaps.total) || 0})`}>${s.gaps && s.gaps.total ? html`<ul class="change-list">${Object.entries(s.gaps.by_kind).map(([k, n]) => html`<li key=${k}><${Icon} name=${gapMeta(k).icon} size="sm" /><a href=${discoveryHref('gaps', { scan: id, kind: k })}>${gapMeta(k).label}: <b>${n}</b></a></li>`)}</ul>` : html`<span class="muted">Nothing it knows of.</span>`}</${Card}>
      ${ap.errors ? html`<${Card} title=${`Errors (${ap.errors})`}><ul class="small">${(ap.error_samples || []).map((e, i) => html`<li key=${i} class="wrap-any">${typeof e === 'string' ? e : JSON.stringify(e)}</li>`)}</ul></${Card}>` : null}
      ${s.error ? html`<${Banner} kind="bad" title="The scan stopped with an error">${typeof s.error === 'string' ? s.error : JSON.stringify(s.error)}<//>` : null}</div>`}</${Drawer}>`;
}

function ScansTab({ sources }) {
  const [source, setSource] = useState('');
  const [status, setStatus] = useState('');
  const [open, setOpen] = useState(null);
  const [more, setMore] = useState([]);
  const [cursor, setCursor] = useState(null);
  const poll = usePoll(() => api.get('/intelligence/scans', { limit: 25, source_id: source || undefined, status: status || undefined }).then((r) => { setCursor(r.next_cursor); return r; }), 3000, [source, status]);
  const items = poll.data ? [...poll.data.items, ...more.filter((m) => !poll.data.items.some((i) => i.id === m.id))] : [];
  useEffect(() => { setMore([]); }, [source, status]);
  async function loadMore() { const r = await api.get('/intelligence/scans', { limit: 25, cursor, source_id: source || undefined, status: status || undefined }); setMore((m) => [...m, ...r.items]); setCursor(r.next_cursor); }
  const name = (id) => (sources.find((s) => s.id === id) || {}).name || id;
  const columns = [
    { key: 'started_at', label: 'Started', sort: (s) => s.started_at, render: (s) => html`<${Ago} ts=${s.started_at} />` },
    { key: 'source', label: 'Source', sort: (s) => name(s.source_id), render: (s) => name(s.source_id) },
    { key: 'status', label: 'Result', sort: (s) => s.status, render: (s) => html`<${ScanBadge} status=${s.status} />` },
    { key: 'mode', label: 'Kind', render: (s) => `${s.mode}${s.trigger && s.trigger !== 'manual' && s.trigger !== 'api' ? ' · ' + s.trigger : ''}` },
    { key: 'dur', label: 'Took', sort: (s) => durationBetween(s.started_at, s.finished_at), render: (s) => (s.finished_at ? fmtDuration(durationBetween(s.started_at, s.finished_at)) : html`<span class="muted">running</span>`) },
    { key: 'assets', label: 'Assets seen', num: true, sort: (s) => scanCounts(s).seen, render: (s) => fmtNum(scanCounts(s).seen) },
    { key: 'new', label: 'New', num: true, sort: (s) => scanCounts(s).created, render: (s) => fmtNum(scanCounts(s).created) },
    { key: 'edges', label: 'Connections', num: true, sort: (s) => scanCounts(s).edges, render: (s) => fmtNum(scanCounts(s).edges) },
    { key: 'gaps', label: 'Gaps', num: true, sort: (s) => (s.stats || {}).gaps_reported || 0, render: (s) => fmtNum((s.stats || {}).gaps_reported || 0) },
  ];
  return html`<div class="col gap-3" data-testid="scans">
    <div class="filter-row"><div style="min-width:200px"><${Select} label="Source" value=${source} onChange=${setSource} placeholder="All sources" options=${sources.map((s) => ({ value: s.id, label: s.name }))} /></div>
      <div style="min-width:200px"><${Select} label="Result" value=${status} onChange=${setStatus} placeholder="Any" options=${['succeeded', 'partial', 'failed', 'running', 'cancelled'].map((v) => ({ value: v, label: scanLabel(v) }))} /></div></div>
    ${poll.error && !poll.data ? html`<${ErrorBox} error=${poll.error} onRetry=${poll.reloadLoud} />` : !poll.data ? html`<${Loading} />` : html`<${Card} flush=${true}>
      <${Table} caption="Scans" rows=${items} columns=${columns} initialSort=${{ key: 'started_at', dir: -1 }} onRowClick=${(s) => setOpen(s.id)} empty=${html`<${Empty} icon="refresh" title="No scans yet">Start one from the Sources tab.</${Empty}>`} /></${Card}>
      ${cursor ? html`<div><${Btn} onClick=${loadMore}>Show older scans<//></div>` : null}
      <div class="small muted">Select a scan to see its counts, errors and what it could not see. Running scans update by themselves.</div>`}
    ${open ? html`<${ScanDetail} id=${open} sources=${sources} onClose=${() => setOpen(null)} />` : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- gaps
function GapsTab({ sources, query, setQuery }) {
  const [kind, setKind] = useState(query.kind || '');
  const [source, setSource] = useState('');
  const [resolved, setResolved] = useState(false);
  const asset = query.asset || ''; const scan = query.scan || '';
  const [more, setMore] = useState([]);
  const [cursor, setCursor] = useState(null);
  const st = useAsync(() => api.get('/intelligence/gaps', { limit: 50, kind: kind || undefined, source_id: source || undefined, resolved: resolved || undefined, asset_id: asset || undefined, scan_id: scan || undefined }).then((r) => { setCursor(r.next_cursor); setMore([]); return r; }), [kind, source, resolved, asset, scan]);
  const sum = useAsync(() => api.get('/intelligence/summary').then((s) => s.gaps).catch(() => null), []);
  async function loadMore() { const r = await api.get('/intelligence/gaps', { limit: 50, cursor, kind: kind || undefined, source_id: source || undefined, resolved: resolved || undefined, asset_id: asset || undefined, scan_id: scan || undefined }); setMore((m) => [...m, ...r.items]); setCursor(r.next_cursor); }
  const name = (id) => (sources.find((s) => s.id === id) || {}).name || id;
  const items = st.data ? [...st.data.items, ...more] : [];
  const kinds = Object.keys(GAP_KINDS);
  return html`<div class="col gap-3" data-testid="gaps">
    <${Banner} kind="info" title="Gaps are part of the answer">A gap is something a scan could not read or understand. Every gap hides part of the picture, so lineage and impact answers near it may be incomplete. Each gap says how to fix it.<//>
    <div class="filter-row"><div style="min-width:220px"><${Select} label="Kind" value=${kind} onChange=${setKind} placeholder="All kinds" options=${kinds.map((k) => ({ value: k, label: `${gapMeta(k).label}${sum.data && sum.data.by_kind[k] ? ` (${sum.data.by_kind[k]})` : ''}` }))} /></div>
      <div style="min-width:200px"><${Select} label="Source" value=${source} onChange=${setSource} placeholder="All sources" options=${sources.map((s) => ({ value: s.id, label: s.name }))} /></div>
      <${Seg} label="State" options=${[{ value: false, label: 'Open' }, { value: true, label: 'Resolved' }]} value=${resolved} onChange=${setResolved} />
      ${asset || scan ? html`<${Btn} size="sm" icon="x" onClick=${() => setQuery({ asset: '', scan: '' })}>${asset ? 'Showing one asset' : 'Showing one scan'}: show all<//>` : null}</div>
    ${kind ? html`<div class="small muted">${gapMeta(kind).help}</div>` : null}
    ${st.error && !st.data ? html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />` : !st.data ? html`<${Loading} />` : !items.length ? html`<${Empty} icon="check-circle" title=${resolved ? 'No resolved gaps' : 'No open gaps'}>${resolved ? 'Gaps are marked resolved when a later scan can read what it could not before.' : 'The scans found nothing they could not read. That says nothing about what no scan covers: see Coverage.'}</${Empty}>` : html`
      <${Card} flush=${true}><div style="padding:0 var(--s-4)">${items.map((g) => html`<div class="gap-item" key=${g.id}>
        <div class="row-wrap"><${Icon} name=${gapMeta(g.kind).icon} /><b>${gapMeta(g.kind).label}</b><span class="muted">·</span><span class="wrap-any">${g.subject}</span>${g.resolved_at ? html`<${Badge} kind="ok">Resolved <${Ago} ts=${g.resolved_at} /><//>` : html`<${Badge} kind="warn">Open<//>`}</div>
        <div class="small wrap-any">${g.detail}</div>
        ${g.remediation ? html`<div class="rem"><b>What to do: </b>${g.remediation}</div>` : null}
        <div class="small muted row-wrap">Source ${name(g.source_id)} · first seen <${Ago} ts=${g.first_seen_at} />${g.asset_id ? html` · <a href=${assetHref(g.asset_id)}>Open the asset</a>` : ''}${g.origin ? ` · found by ${g.origin}` : ''}</div></div>`)}</div></${Card}>
      ${cursor ? html`<div><${Btn} onClick=${loadMore}>Show more<//></div>` : null}`}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- coverage
function Bar({ value, total, label, sub }) {
  const p = total ? Math.round((value / total) * 100) : null;
  const cls = p == null ? 'none' : p >= 90 ? '' : p >= 60 ? 'mid' : 'low';
  return html`<div class="cov-row"><div>${label}${sub ? html`<div class="small muted">${sub}</div>` : null}</div><div class=${'cov-bar ' + cls} role="meter" aria-label=${label} aria-valuemin="0" aria-valuemax="100" aria-valuenow=${p ?? undefined}>${p != null ? html`<i style=${`width:${p}%`}></i>` : null}</div><div class="num small">${total ? `${fmtNum(value)} of ${fmtNum(total)} (${p}%)` : 'none found'}</div></div>`;
}

/** Measure, from the recorded connections, how many tables have any lineage and how many columns have a recorded mapping. Runs on request: it reads each table once. */
function LineageCoverage() {
  const [state, setState] = useState(null);
  const stop = useRef(false);
  useEffect(() => () => { stop.current = true; }, []);
  async function run() {
    stop.current = false;
    setState({ running: true, done: 0, total: 0 });
    try {
      const tables = [];
      let cursor;
      do { const r = await api.get('/intelligence/assets', { type: 'table', limit: 100, cursor }); tables.push(...r.items); cursor = r.next_cursor; } while (cursor && tables.length < 200);
      const t = { tables: tables.length, capped: !!cursor, withTable: 0, withColumns: 0, cols: 0, colsMapped: 0 };
      let next = 0; let done = 0;
      const worker = async () => {
        while (next < tables.length && !stop.current) {
          const a = tables[next++];
          try {
            const [g, l] = await Promise.all([api.get(`/intelligence/assets/${enc(a.id)}`), api.get(`/intelligence/assets/${enc(a.id)}/lineage`, { level: 'column', depth: 1, direction: 'both' })]);
            if (g.lineage_edges && (g.lineage_edges.upstream || g.lineage_edges.downstream)) t.withTable += 1;
            const own = l.nodes.filter((n) => n.parent_id === a.id);
            const mapped = new Set();
            for (const e of l.edges) if (e.type === 'TRANSFORMS') { if (own.some((n) => n.id === e.from)) mapped.add(e.from); if (own.some((n) => n.id === e.to)) mapped.add(e.to); }
            t.cols += own.length || ((g.attrs && g.attrs.column_count) || 0); t.colsMapped += mapped.size; if (mapped.size) t.withColumns += 1;
          } catch { /* one table that cannot be read is simply not counted */ }
          done += 1;
          if (done % 3 === 0 || done === tables.length) setState({ running: true, done, total: tables.length });
        }
      };
      await Promise.all([worker(), worker(), worker(), worker()]);
      setState({ running: false, result: t });
    } catch (e) { setState({ running: false, error: e }); }
  }
  const r = state && state.result;
  return html`<${Card} title="How much of the lineage did we find?" sub="Measured on request" actions=${html`<${Btn} size="sm" icon="refresh" busy=${state && state.running} onClick=${run}>${r ? 'Measure again' : 'Measure now'}<//>`}>
    ${!state ? html`<p class="m-0 muted">Counts which tables have any recorded connection and how many columns have a recorded mapping. It reads each table once, so it only runs when you ask.</p>` : null}
    ${state && state.running ? html`<${Progress} value=${state.total ? (state.done / state.total) * 100 : 0} label="Measuring" /><div class="small muted mt-2">${state.done} of ${state.total || '?'} tables</div>` : null}
    ${state && state.error ? html`<${ErrorBox} error=${state.error} />` : null}
    ${r ? html`<div class="col gap-2" data-testid="lineage-coverage"><${Bar} label="Tables with a recorded connection" value=${r.withTable} total=${r.tables} sub="Something feeds them or uses them" />
      <${Bar} label="Tables with column-level mappings" value=${r.withColumns} total=${r.tables} sub="At least one column traced" />
      <${Bar} label="Columns with a recorded mapping" value=${r.colsMapped} total=${r.cols} />
      <p class="small muted m-0">${r.capped ? 'Measured on the first 200 tables only. ' : ''}A column without a mapping is not necessarily unconnected: the code that moves it may use constructs the analysers cannot follow.</p></div>` : null}</${Card}>`;
}

function CoverageTab({ sources, setTab }) {
  const cov = useAsync(() => api.get('/intelligence/coverage'), []);
  const gaps = useAsync(() => api.get('/intelligence/gaps', { limit: 200 }).then((r) => r.items || []).catch(() => []), []);
  const sum = useAsync(() => api.get('/intelligence/summary').catch(() => null), []);
  if (cov.error && !cov.data) return html`<${ErrorBox} error=${cov.error} onRetry=${cov.reloadLoud} />`;
  if (!cov.data) return html`<${Loading} />`;
  const never = cov.data.sources.filter((s) => !s.last_scan && s.enabled && s.kind !== 'scanner_import');
  const badBySource = {};
  for (const g of gaps.data || []) if (g.asset_id && UNREADABLE.has(g.kind)) (badBySource[g.source_id] ||= new Set()).add(g.asset_id);
  return html`<div class="col gap-4" data-testid="coverage">
    <${Banner} kind="info" title="What this page is for">It shows what the scans covered and, just as important, what they could not see. A source that was never scanned, a notebook that could not be read and a table name built at run time all leave holes in the lineage. They are listed here instead of being hidden.<//>
    ${cov.data.sources.map((s) => {
      const codeTypes = CODE_TYPES.filter((t) => s.assets.by_type[t]);
      const codeTotal = codeTypes.reduce((n, t) => n + s.assets.by_type[t], 0);
      const codeBad = Math.min(codeTotal, (badBySource[s.source_id] || new Set()).size);
      const max = Math.max(1, ...Object.values(s.assets.by_type));
      return html`<${Card} key=${s.source_id} title=${s.name} sub=${sourceKindLabel(s.kind)} actions=${html`<${ScanBadge} status=${s.last_status} running=${s.running} />`}>
        <div class="grid c2 top">
          <div><h4 class="mb-2" style="font-size:var(--fs-sm)">What was found (${fmtNum(s.assets.active)} assets)</h4>
            ${Object.entries(s.assets.by_type).sort((a, b) => b[1] - a[1]).map(([t, n]) => html`<div class="cov-row" key=${t}><div>${typeLabel(t)}</div><div class="cov-bar" role="presentation"><i style=${`width:${(n / max) * 100}%;background:var(--series-1)`}></i></div><div class="num small">${fmtNum(n)}</div></div>`)}
            ${s.assets.active ? null : html`<div class="muted small">Nothing yet.</div>`}</div>
          <div class="blind"><h4 class="mb-2" style="font-size:var(--fs-sm)">What we could NOT see</h4>
            ${codeTotal ? html`<${Bar} label="Code read in full" value=${codeTotal - codeBad} total=${codeTotal} sub="Notebooks, queries, pipelines, scripts" />` : html`<div class="small muted mb-2">No code was found in this source.</div>`}
            ${s.open_gaps.total ? html`<ul class="change-list mt-2">${Object.entries(s.open_gaps.by_kind).map(([k, n]) => html`<li key=${k}><${Icon} name=${gapMeta(k).icon} size="sm" /><a href=${discoveryHref('gaps', { kind: k })}>${gapMeta(k).label}: <b>${n}</b></a></li>`)}</ul>` : html`<div class="small muted">No open gaps from this source.</div>`}
            <div class="small muted mt-3">Last full scan ${s.last_full_at ? ago(s.last_full_at) : 'never'}${s.next_due_at && s.enabled ? `, next scan due ${ago(s.next_due_at)}` : ''}.</div></div></div></${Card}>`; })}
    <${LineageCoverage} />
    <${Card} title="Always outside the picture" sub="Not a bug, a limit">
      <ul class="m-0" style="padding-left:18px;line-height:1.7">
        <li>Anything in a source nobody scanned${never.length ? html`: <b>${never.map((s) => s.name).join(', ')}</b> ${never.length === 1 ? 'has' : 'have'} never been scanned` : ''}.</li>
        <li>Table or file names that code builds while it runs, and code in a language or style the analysers do not understand (see the gaps above).</li>
        <li>Connections that exist only inside other systems that are not a source here.</li>
        <li>The values inside tables. Looking at them is ${cov.data.sampling && cov.data.sampling.enabled ? html`allowed by at least one sampling policy` : html`switched off`}. <a href=${discoveryHref('sampling')} onClick=${(e) => { e.preventDefault(); setTab('sampling'); }}>Sampling policies</a></li></ul></${Card}>
    ${sum.data ? html`<div class="small muted">${fmtNum(sum.data.edges.total)} connections recorded in total, ${fmtNum(sum.data.edges.by_confidence.verified)} of them verified by observation.</div>` : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- import
function ImportTab({ sources }) {
  const manage = can('intelligence.manage');
  const [file, setFile] = useState(null);
  const [name, setName] = useState('');
  const [tenant, setTenant] = useState('');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [scanId, setScanId] = useState(null);
  const [over, setOver] = useState(false);
  const input = useRef(null);
  if (!manage) return html`<${Empty} icon="lock" title="You cannot import files">Importing a scanner file needs permission to manage Intelligence.</${Empty}>`;
  const tooBig = file && file.size > MAX_IMPORT_MB * 1024 * 1024;
  function pick(f) { setFile(f || null); setErr(null); setScanId(null); if (f && !name) setName(f.name.replace(/\.json$/i, '').slice(0, 100)); }
  async function go() {
    setBusy(true); setErr(null); setScanId(null);
    try {
      const text = await file.text();
      try { JSON.parse(text); } catch { throw Object.assign(new Error('This file is not valid JSON. Export the scanner result again and try once more.'), { local: true }); }
      const r = await request('POST', '/intelligence/import/scanner', { body: text, headers: { 'Content-Type': 'application/json' }, query: { name: name || undefined, tenant_id: tenant || undefined, scan: true } }).then((x) => x.data);
      toast.ok('File imported. Scanning it now');
      setScanId((r.scan && (r.scan.id || r.scan.scan_id)) || r.scan_id || r.id || null);
    } catch (e) { setErr(e); } finally { setBusy(false); }
  }
  return html`<div class="col gap-4" data-testid="import">
    <${Card} title="Import a Power BI scanner result">
      <p class="mt-0">If you cannot let OLCF call Microsoft APIs, save the result of the Power BI admin <b>scanner API</b> (the JSON returned by <span class="mono">GET /admin/workspaces/scanResult/{scanId}</span>) and import the file here. OLCF turns it into workspaces, reports, models, tables, columns and the connections between them, with the same gaps and confidence levels as a live scan.</p>
      <ul class="small muted"><li>Files up to <b>${MAX_IMPORT_MB} MB</b> are accepted. For a bigger estate, export fewer workspaces per file and import them one by one.</li><li>The file is checked before it is stored and is refused if it looks like it contains a credential.</li><li>Importing again under the same name replaces the earlier file.</li></ul>
      <div class=${'upload-drop' + (over ? ' over' : '')} onDragOver=${(e) => { e.preventDefault(); setOver(true); }} onDragLeave=${() => setOver(false)} onDrop=${(e) => { e.preventDefault(); setOver(false); pick(e.dataTransfer.files[0]); }}>
        <${Icon} name="upload" size="lg" /><div class="mt-2">${file ? html`<b>${file.name}</b> <span class="muted">(${fmtBytes(file.size)})</span>` : 'Drop the scanner JSON file here, or'}</div>
        <div class="mt-3"><input ref=${input} type="file" accept=".json,application/json" class="sr-only" id="scanner-file" onChange=${(e) => pick(e.target.files[0])} /><label class="btn" for="scanner-file">${file ? 'Choose another file' : 'Choose a file'}</label></div></div>
      ${tooBig ? html`<div class="mt-3"><${Banner} kind="bad" title=${`This file is ${fmtBytes(file.size)}, over the ${MAX_IMPORT_MB} MB limit`} remediation="Export fewer workspaces per scanner result, then import each file separately." /></div>` : null}
      <div class="grid c2 mt-3"><${Input} label="Name of this import" value=${name} onInput=${setName} hint="Importing again under this name replaces it." /><${Input} label="Tenant id (optional)" value=${tenant} onInput=${setTenant} mono=${true} /></div>
      ${err ? html`<div class="mt-3"><${ErrorBox} error=${err} /></div>` : null}
      <div class="mt-3"><${Btn} kind="primary" icon="upload" busy=${busy} disabled=${!file || tooBig} onClick=${go}>Import and scan<//></div></${Card}>
    ${scanId ? html`<${Card} title="Scan of the imported file"><${ScanProgress} scanId=${scanId} /></${Card}>` : null}
    ${sources.filter((s) => s.kind === 'scanner_import').length ? html`<${Card} title="Earlier imports"><ul class="change-list">${sources.filter((s) => s.kind === 'scanner_import').map((s) => html`<li key=${s.id}><b>${s.name}</b><span class="small muted">${s.import ? fmtBytes(s.import.bytes) : ''}</span><${ScanBadge} status=${s.last_status} /><span class="small muted"><${Ago} ts=${s.last_scan_at} /></span></li>`)}</ul></${Card}>` : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- sampling policies
const MASKING = [['partial', 'Partly hidden: first and last characters stay, the rest becomes stars'], ['hash', 'Replaced by a fingerprint: equal values look equal, nothing is readable'], ['redact', 'Fully hidden'], ['none', 'Not hidden at all (dangerous)']];
const SCOPES = [['global', 'Everything'], ['workspace', 'One workspace'], ['source', 'One source'], ['asset', 'One table']];

function PolicyDialog({ policy, sources, workspaces, onClose }) {
  const [scope, setScope] = useState(policy ? policy.scope_type : 'workspace');
  const [scopeId, setScopeId] = useState(policy ? policy.workspace_id || policy.source_id || policy.asset_id || policy.scope_id || '' : '');
  const [rows, setRows] = useState(policy ? policy.max_rows : 5);
  const [mask, setMask] = useState(policy ? policy.masking : 'partial');
  const [excl, setExcl] = useState(policy ? policy.exclude_classified : true);
  const [on, setOn] = useState(policy ? policy.enabled : true);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  async function save() {
    if (rows > 0) {
      const ok = await confirmDialog({ title: 'Allow looking at data values?', confirmLabel: 'Allow it', danger: true,
        body: html`<div class="col gap-2"><p class="m-0">This lets people with the sample permission read up to <b>${rows}</b> real row${rows === 1 ? '' : 's'} ${scope === 'global' ? 'of every table' : 'of the chosen scope'}.</p>${mask === 'none' ? html`<p class="m-0"><b>Values will not be hidden.</b></p>` : null}${!excl ? html`<p class="m-0"><b>Columns that look sensitive will not be left out.</b></p>` : null}<p class="m-0 muted">Every read is recorded in the audit log. Values are never stored.</p></div>` });
      if (!ok) return;
    }
    setBusy(true); setErr(null);
    try { await api.put('/intelligence/sampling-policies', { scope_type: scope, scope_id: scope === 'global' ? null : scopeId, max_rows: rows, masking: mask, exclude_classified: excl, enabled: on }); toast.ok('Policy saved'); onClose(true); } catch (e) { setErr(e); } finally { setBusy(false); }
  }
  const scopeOptions = scope === 'workspace' ? workspaces.map((w) => ({ value: w.id, label: w.display_name || w.name })) : scope === 'source' ? sources.map((s) => ({ value: s.id, label: s.name })) : null;
  return html`<${Dialog} title=${policy ? 'Change a sampling policy' : 'Add a sampling policy'} onClose=${() => onClose(false)} footer=${html`<${Btn} onClick=${() => onClose(false)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${scope !== 'global' && !scopeId} onClick=${save}>Save the policy<//>`}>
    <div class="col gap-1" data-testid="policy-dialog">${err ? html`<div class="mb-3"><${ErrorBox} error=${err} /></div>` : null}
      <${Select} label="Applies to" value=${scope} onChange=${(v) => { setScope(v); setScopeId(''); }} options=${SCOPES.map(([value, label]) => ({ value, label }))} disabled=${!!policy} hint="The narrowest policy wins: a table beats a source, a source beats a workspace, a workspace beats everything." />
      ${scope === 'global' ? null : scopeOptions ? html`<${Select} label=${scope === 'workspace' ? 'Workspace' : 'Source'} value=${scopeId} onChange=${setScopeId} placeholder="Choose" options=${scopeOptions} disabled=${!!policy} />` : html`<${Input} label="Table (asset id)" value=${scopeId} onInput=${setScopeId} mono=${true} placeholder="ast_..." hint="Open the table in the catalog and copy the id from the address." disabled=${!!policy} />`}
      <div class="field"><label for="pol-rows">Most rows anyone can fetch at once: <b>${rows === 0 ? '0 (off)' : rows}</b></label><input id="pol-rows" type="range" min="0" max="100" step="1" value=${rows} onInput=${(e) => setRows(+e.target.value)} aria-valuetext=${rows === 0 ? 'off' : `${rows} rows`} style="width:100%;accent-color:var(--accent)" /><div class="hint">0 switches sampling off for this scope.</div></div>
      <${Select} label="How values are hidden" value=${mask} onChange=${setMask} options=${MASKING.map(([value, label]) => ({ value, label }))} />
      <${Check} label="Leave out columns that look sensitive or are classified" checked=${excl} onChange=${setExcl} />
      <${Switch} label="Policy is in force" checked=${on} onChange=${setOn} />
      ${rows > 0 ? html`<div class="warn-box mt-3" role="status"><b>This allows reading real data.</b> ${mask === 'none' ? 'Values will be shown as they are. ' : ''}${!excl ? 'Sensitive-looking columns will be included. ' : ''}Reads are limited, audited and never stored, but they do expose values to anyone with the sample permission.</div>` : html`<div class="small muted mt-3">With 0 rows nobody can fetch sample rows for this scope.</div>`}</div></${Dialog}>`;
}

function SamplingTab({ sources }) {
  const manage = can('intelligence.manage');
  const st = useAsync(() => api.get('/intelligence/sampling-policies'), []);
  const ws = useAsync(() => loadWorkspaces().catch(() => []), []);
  const [dialog, setDialog] = useState(null);
  const policies = st.data ? st.data.items : [];
  const active = policies.filter((p) => p.enabled && p.max_rows > 0);
  const label = (p) => (p.scope_type === 'global' ? 'Everything' : p.scope_type === 'workspace' ? `Workspace ${((ws.data || []).find((w) => w.id === p.workspace_id || w.id === p.scope_id) || {}).name || p.scope_id}` : p.scope_type === 'source' ? `Source ${((sources || []).find((s) => s.id === p.source_id || s.id === p.scope_id) || {}).name || p.scope_id}` : `Table ${p.scope_id}`);
  async function del(p) {
    if (!(await confirmDialog({ title: 'Remove this policy?', body: `${label(p)} falls back to the next wider policy, and finally to off.`, confirmLabel: 'Remove it', danger: true }))) return;
    await act(() => api.del(`/intelligence/sampling-policies/${enc(p.id)}`), { ok: 'Policy removed' });
    st.reload();
  }
  if (st.error && !st.data) return html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />`;
  if (!st.data) return html`<${Loading} />`;
  return html`<div class="col gap-4" data-testid="sampling">
    ${active.length ? html`<${Banner} kind="warn" title="Looking at data values is switched ON for part of the estate">${active.map((p) => `${label(p)}: up to ${p.max_rows} rows, ${p.masking}`).join('; ')}.<//>`
      : html`<${Banner} kind="ok" title="Looking at data values is switched off everywhere">This is the default and it is on purpose. Scans only read names, structure and definitions. No table value is read, stored or shown.<//>`}
    <${Card} title="What sampling is" ><p class="m-0">Sampling shows a few real rows of a table so people can see what the data looks like. It is a separate permission, limited to the number of rows below, partly hidden, left out for sensitive-looking columns, recorded in the audit log, and the values are never stored. It stays off until you add a policy that allows it.</p></${Card}>
    <${Card} flush=${true} title="Policies" actions=${manage ? html`<${Btn} size="sm" kind="primary" icon="plus" onClick=${() => setDialog({})}>Add a policy<//>` : null}>
      <${Table} caption="Sampling policies" rows=${policies} columns=${[
        { key: 'scope', label: 'Applies to', sort: (p) => p.scope_type, render: (p) => label(p) },
        { key: 'max_rows', label: 'Rows', num: true, render: (p) => (p.max_rows === 0 ? html`<${Badge} kind="off">Off<//>` : p.max_rows) },
        { key: 'masking', label: 'Hidden how', render: (p) => p.masking },
        { key: 'excl', label: 'Sensitive columns', render: (p) => (p.exclude_classified ? 'Left out' : html`<${Badge} kind="warn">Included<//>`) },
        { key: 'enabled', label: 'State', render: (p) => (p.enabled ? html`<${Badge} kind="ok">In force<//>` : html`<${Badge}>Paused<//>`) },
        manage ? { key: 'act', label: '', sortable: false, render: (p) => html`<span class="row"><${Btn} size="sm" onClick=${() => setDialog(p)}>Change<//><${Btn} size="sm" kind="ghost" iconOnly icon="trash" label="Remove the policy" onClick=${() => del(p)} /></span>` } : null].filter(Boolean)}
        empty=${html`<${Empty} icon="shield" title="No policies, so nothing is ever sampled">Add one only if you want people to see sample rows.</${Empty}>`} /></${Card}>
    ${dialog ? html`<${PolicyDialog} policy=${dialog.id ? dialog : null} sources=${sources} workspaces=${ws.data || []} onClose=${(saved) => { setDialog(null); if (saved) st.reload(); }} />` : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- snapshots
function DiffList({ title, items, render, tone }) {
  return html`<div><h3 class="mb-2" style="font-size:var(--fs-md)"><span class=${tone}>${title}</span> <span class="muted small">${items.length}</span></h3>
    ${items.length ? html`<ul class="change-list">${items.slice(0, 100).map((x, i) => html`<li key=${x.id || i}>${render(x)}</li>`)}</ul>${items.length > 100 ? html`<div class="small muted mt-2">Showing 100 of ${items.length}.</div>` : null}` : html`<div class="small muted">None</div>`}</div>`;
}
function describeAssetChange(c) {
  const out = [];
  if ((c.columns_added || []).length) out.push(`+${plural(c.columns_added.length, 'column')}`);
  if ((c.columns_removed || []).length) out.push(`-${plural(c.columns_removed.length, 'column')}`);
  if ((c.columns_retyped || []).length) out.push(`${c.columns_retyped.length} retyped`);
  if ((c.attrs_changed || []).length) out.push(`details: ${c.attrs_changed.map((k) => k.replace(/_/g, ' ')).join(', ')}`);
  return out.join(', ');
}
function SnapshotsTab({ sources, query, setQuery }) {
  const [source, setSource] = useState(query.source || '');
  const snaps = useAsync(() => api.get('/intelligence/snapshots', { limit: 100, source_id: source || undefined }).then((r) => (r.items || []).filter((s) => s.status === 'succeeded' || s.status === 'partial')), [source]);
  const [from, setFrom] = useState(query.from || '');
  const [to, setTo] = useState(query.to || '');
  const list = snaps.data || [];
  useEffect(() => {
    if (!list.length) return;
    const withTwo = [...new Set(list.map((s) => s.source_id))].find((id) => list.filter((s) => s.source_id === id).length >= 2);       // newest source that has two scans to compare
    const same = list.filter((s) => (source ? s.source_id === source : s.source_id === withTwo));
    if ((!from || !to || !list.some((s) => s.id === from) || !list.some((s) => s.id === to)) && same.length >= 2) { setTo(same[0].id); setFrom(same[1].id); }
  }, [snaps.data, source]);
  const diff = useAsync(() => (from && to && from !== to ? api.get('/intelligence/snapshots/diff', { from, to, limit: 500 }) : Promise.resolve(null)), [from, to]);
  const name = (id) => (sources.find((s) => s.id === id) || {}).name || 'a removed source';
  const label = (s) => `${name(s.source_id)} · scan ${s.version} (${s.mode}) · ${ago(s.finished_at || s.started_at)}`;
  const d = diff.data;
  return html`<div class="col gap-4" data-testid="snapshots">
    <div class="filter-row"><div style="min-width:200px"><${Select} label="Source" value=${source} onChange=${(v) => { setSource(v); setFrom(''); setTo(''); }} placeholder="Any source" options=${sources.map((s) => ({ value: s.id, label: s.name }))} /></div>
      <div style="min-width:280px"><${Select} label="Earlier scan" value=${from} onChange=${setFrom} options=${list.map((s) => ({ value: s.id, label: label(s) }))} placeholder="Choose" /></div>
      <div style="min-width:280px"><${Select} label="Later scan" value=${to} onChange=${setTo} options=${list.map((s) => ({ value: s.id, label: label(s) }))} placeholder="Choose" /></div></div>
    ${snaps.error && !snaps.data ? html`<${ErrorBox} error=${snaps.error} onRetry=${snaps.reloadLoud} />` : !snaps.data ? html`<${Loading} />` : list.length < 2 ? html`<${Empty} icon="history" title="Only one scan so far">Comparing needs two finished scans of the same source. Scan again later to see what changes.</${Empty}>` : !d && !diff.error ? html`<div class="muted">${from && to && from === to ? 'Choose two different scans.' : 'Choose two scans of the same source to compare.'}</div>` : diff.error ? html`<${ErrorBox} error=${diff.error} />` : html`
      <div class="chips"><span class="badge ok">${fmtNum(d.summary.assets_added)} assets added</span><span class="badge warn">${fmtNum(d.summary.assets_changed)} changed</span><span class="badge bad">${fmtNum(d.summary.assets_removed)} removed</span><span class="badge info">${fmtNum(d.summary.edges_added)} connections added</span><span class="badge warn">${fmtNum(d.summary.edges_changed)} changed</span><span class="badge bad">${fmtNum(d.summary.edges_removed)} removed</span></div>
      ${d.truncated ? html`<${Banner} kind="warn" title="The lists are cut at 500 entries">The counts above are complete. Compare closer scans to see everything.<//>` : null}
      <${Card} title="Assets"><div class="diff-cols">
        <${DiffList} title="Added" tone="delta-add" items=${d.assets.added} render=${(a) => html`<${AssetLink} asset=${a} /><span class="small muted">${typeLabel(a.type)}</span>`} />
        <${DiffList} title="Removed" tone="delta-del" items=${d.assets.removed} render=${(a) => html`<${AssetLink} asset=${a} /><span class="small muted">${typeLabel(a.type)}</span>`} />
        <${DiffList} title="Changed" tone="delta-chg" items=${d.assets.changed} render=${(a) => html`<${AssetLink} asset=${a} /><span class="small muted wrap-any">${describeAssetChange(a.changes || {})}</span>`} /></div></${Card}>
      <${Card} title="Connections"><div class="diff-cols">
        ${[['Added', 'added', 'delta-add'], ['Removed', 'removed', 'delta-del'], ['Changed', 'changed', 'delta-chg']].map(([t, k, tone]) => html`<${DiffList} key=${k} title=${t} tone=${tone} items=${d.edges[k]} render=${(e) => html`<span class="wrap-any">${e.src_name} ${edgeVerb(e.type)} ${e.dst_name}</span><${ConfBadge} level=${e.confidence} />`} />`)}</div></${Card}>`}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- the page
export function DiscoveryPage({ query }) {
  const [tab, setTab] = useQueryState('tab', 'sources');
  const src = useSources();
  const manage = can('intelligence.manage');
  const gaps = useAsync(() => api.get('/intelligence/summary').then((s) => s.gaps.open).catch(() => null), []);
  const setQ = (patch) => { window.location.hash = '#/intelligence/discovery?' + new URLSearchParams(Object.fromEntries(Object.entries({ ...query, ...patch }).filter(([, v]) => v))).toString(); };
  const tabs = [
    { id: 'sources', label: 'Sources', icon: 'plug' },
    { id: 'scans', label: 'Scans', icon: 'refresh' },
    { id: 'gaps', label: 'Gaps', icon: 'alert-triangle', count: gaps.data || undefined },
    { id: 'coverage', label: 'Coverage', icon: 'gauge' },
    { id: 'snapshots', label: 'Changes', icon: 'history' },
    manage ? { id: 'import', label: 'Import', icon: 'upload' } : null,
    manage ? { id: 'sampling', label: 'Sampling', icon: 'eye' } : null,
  ].filter(Boolean);
  const cur = tabs.some((t) => t.id === tab) ? tab : 'sources';
  return html`<div class="page"><${PageHead} icon="radio" title="Discovery" sub="Where Intelligence learns from, what each scan found, and what it could not see." />
    ${src.error && !src.data ? html`<${ErrorBox} error=${src.error} onRetry=${src.reloadLoud} />` : html`
      <${Tabs} label="Discovery sections" tabs=${tabs} value=${cur} onChange=${setTab} />
      <div class="tab-body">
        <${TabPanel} id="sources" value=${cur}><${SourcesTab} setTab=${setTab} /></${TabPanel}>
        <${TabPanel} id="scans" value=${cur}><${ScansTab} sources=${src.items} /></${TabPanel}>
        <${TabPanel} id="gaps" value=${cur}><${GapsTab} sources=${src.items} query=${query || {}} setQuery=${setQ} /></${TabPanel}>
        <${TabPanel} id="coverage" value=${cur}><${CoverageTab} sources=${src.items} setTab=${setTab} /></${TabPanel}>
        <${TabPanel} id="snapshots" value=${cur}><${SnapshotsTab} sources=${src.items} query=${query || {}} setQuery=${setQ} /></${TabPanel}>
        <${TabPanel} id="import" value=${cur}><${ImportTab} sources=${src.items} /></${TabPanel}>
        <${TabPanel} id="sampling" value=${cur}><${SamplingTab} sources=${src.items} /></${TabPanel}></div>`}</div>`;
}
