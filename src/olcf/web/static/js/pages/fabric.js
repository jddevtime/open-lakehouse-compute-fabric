// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Microsoft Fabric integration: readiness, diagnostics chain, configuration, live submissions, metrics, SDK snippet, notebook analyzer, rule reference.
import { html, useState, useEffect, useRef, useMemo, useAsync, usePoll } from '../core/h.js';
import { api, can } from '../api/client.js';
import { PageHead, Card, Btn, Badge, StatusBadge, Light, Banner, Empty, ErrorBox, Loading, Tabs, TabPanel, Seg, Select, Input, Textarea, Switch, Check, Kv, Code, Steps, Progress, Async, CopyBtn } from '../ui/basic.js';
import { Icon } from '../ui/icons.js';
import { Table } from '../ui/data.js';
import { Drawer, act, toast, confirmDialog } from '../ui/overlay.js';
import { BarChart, DonutChart } from '../charts/Chart.js';
import { useWorkspaces, useQueryState, Ago } from './common.js';
import { fmtNum, fmtMs, fmtBytes, fmtPct, ago, titleCase, bus, jsonPretty, fmtTime, lightState, downloadText } from '../core/util.js';
import { jobHref } from '../core/links.js';

const TABS = [{ id: 'overview', label: 'Connection' }, { id: 'config', label: 'Configuration' }, { id: 'submissions', label: 'Submissions' }, { id: 'metrics', label: 'Metrics' }, { id: 'sdk', label: 'SDK setup' }, { id: 'analyzer', label: 'Notebook check' }, { id: 'rules', label: 'Compatibility rules' }];

// ------------------------------------------------------------------ connection: readiness + diagnostics
function Readiness({ data }) {
  const items = data.readiness;
  const req = items.filter((r) => r.required);
  const okReq = req.filter((r) => r.ok).length;
  return html`<${Card} title="Readiness" sub=${`${okReq} of ${req.length} required steps done`} actions=${html`<div style="width:120px"><${Progress} value=${(okReq / Math.max(1, req.length)) * 100} kind=${okReq === req.length ? 'ok' : ''} label="Required readiness steps" /></div>`}>
    <ul class="ready-list">${items.map((r) => html`<li key=${r.id} class=${r.ok ? 'ok' : r.required ? 'req' : 'opt'}><${Light} state=${r.ok ? 'ok' : r.required ? 'down' : 'degraded'} size="lg" glyph=${true} label=${`${r.title}: ${r.ok ? 'done' : r.required ? 'required, not done' : 'recommended, not done'}`} />
      <div class="grow"><div class="r-title">${r.title}${r.required ? '' : html` <${Badge} kind="outline">optional<//>`}</div><div class="small muted">${r.detail}</div>${!r.ok && r.remediation ? html`<div class="small mt-2"><b>Next: </b>${r.remediation}</div>` : null}</div></li>`)}</ul></${Card}>`;
}

function Diagnostics() {
  const [res, setRes] = useState(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  async function run() {
    setBusy(true); setErr(null);
    try { setRes(await api.post('/integrations/fabric/test')); } catch (e) { setErr(e); } finally { setBusy(false); }
  }
  return html`<${Card} title="Connection test" sub="DNS, TLS, proxy, Entra token, Fabric API, OneLake and the inbound path, in order" actions=${can('integration.fabric.manage') ? html`<${Btn} kind="primary" icon="activity" busy=${busy} onClick=${run}>${res ? 'Run again' : 'Run diagnostics'}<//>` : null}>
    ${err ? html`<${ErrorBox} error=${err} />` : null}
    ${!res && !busy && !err ? html`<p class="muted" style="margin:0">Each step explains what failed and how to fix it. Steps that depend on an earlier failure are skipped, so you always see the first thing to repair.</p>` : null}
    ${busy ? html`<div class="loading"><span class="spinner lg"></span><span>Checking each hop. Unreachable hosts time out after the configured network timeout.</span></div>` : null}
    ${res && !busy ? html`<div><${Banner} kind=${res.ok ? 'ok' : 'warn'} title=${res.ok ? 'The whole path works' : res.detail}>Checked ${ago(res.checked_at)}.<//>
      <div class="mt-4"><${Steps} steps=${res.steps.map((s) => ({ ...s, state: s.skipped ? 'unknown' : s.ok ? 'ok' : 'down', remediation: s.ok ? null : s.remediation }))} /></div>
      <details class="mt-4"><summary class="small muted" style="cursor:pointer">Network settings used for these checks</summary><${Kv} items=${Object.entries(res.network || {}).map(([k, v]) => [k, v == null ? null : String(v)])} /></details></div>` : null}</${Card}>`;
}

function Overview({ cfg, reload }) {
  const subs = usePoll(() => api.get('/integrations/fabric/submissions', { limit: 6 }), 6000, []);
  return html`<div class="grid c2 fabric-grid"><div class="col gap-4"><${Readiness} data=${cfg} /></div><div class="col gap-4"><${Diagnostics} />
    <${Card} title="Latest submissions" actions=${html`<${Btn} size="sm" kind="ghost" href="#/fabric?tab=submissions">All submissions<//>`} flush=${true}>
      <${Async} state=${subs}>${(d) => d.items.length ? html`<${SubTable} items=${d.items} compact=${true} onOpen=${() => { window.location.hash = '#/fabric?tab=submissions'; }} />` : html`<${Empty} icon="cloud" title="Nothing submitted yet">Paste the SDK cell into a Fabric notebook and run it. The run shows up here as it happens.<//>`}<//></${Card}></div></div>`;
}

// ------------------------------------------------------------------ configuration form
const lines = (v) => (v || []).join('\n');
const unlines = (t) => t.split('\n').map((x) => x.trim()).filter(Boolean);
const num = (v) => (v === '' || v == null ? null : Number(v));

function ListEditor({ rows, onChange, cols, add, empty }) {
  const set = (i, k, v) => onChange(rows.map((r, j) => (j === i ? { ...r, [k]: v } : r)));
  return html`<div class="list-editor">${rows.length ? html`<div class="le-head" style=${`grid-template-columns:${cols.map((c) => c.w || '1fr').join(' ')} 36px`}>${cols.map((c) => html`<span key=${c.key}>${c.label}</span>`)}<span></span></div>` : html`<div class="muted small">${empty}</div>`}
    ${rows.map((r, i) => html`<div class="le-row" key=${i} style=${`grid-template-columns:${cols.map((c) => c.w || '1fr').join(' ')} 36px`}>${cols.map((c) => c.options ? html`<select key=${c.key} class="select sm" aria-label=${c.label} value=${r[c.key] || ''} onChange=${(e) => set(i, c.key, e.target.value)}><option value="">${c.placeholder || 'Choose'}</option>${c.options.map((o) => html`<option key=${o.value} value=${o.value} selected=${o.value === r[c.key]}>${o.label}</option>`)}</select>` : html`<input key=${c.key} class=${'input sm' + (c.mono ? ' mono' : '')} aria-label=${c.label} placeholder=${c.placeholder || ''} value=${r[c.key] || ''} onInput=${(e) => set(i, c.key, e.target.value)} />`)}
      <${Btn} kind="ghost" size="sm" iconOnly icon="trash" label="Remove row" onClick=${() => onChange(rows.filter((_, j) => j !== i))} /></div>`)}
    <${Btn} size="sm" icon="plus" onClick=${() => onChange([...rows, {}])}>${add}<//></div>`;
}

function ConfigForm({ cfg, reload }) {
  const c0 = cfg.config;
  const { items: workspaces } = useWorkspaces();
  const conns = useAsync(() => api.get('/connections', { limit: 200 }).then((r) => r.items || r).catch(() => []), []);
  const secrets = useAsync(() => api.get('/secrets', { limit: 200 }).then((r) => r.items || []).catch(() => []), []);
  const queues = useAsync(() => api.get('/queues').then((r) => r.items), []);
  const mx = useAsync(() => api.get('/runtimes/matrix').catch(() => ({ families: [] })), []);
  const rts = useAsync(() => api.get('/runtimes').then((r) => r.items).catch(() => []), []);
  const [f, setF] = useState(() => ({ ...c0, tenants: lines(c0.allowed_tenants), auds: lines(c0.audiences), wm: c0.workspace_map.map((m) => ({ ...m })), lm: c0.lakehouse_map.map((m) => ({ ...m })), sg: c0.secret_grants.map((m) => ({ ...m })),
    policy: { ...c0.default_policy }, net: { ...c0.network }, runtime: { ...c0.default_runtime } }));
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [dirty, setDirty] = useState(false);
  const manage = can('integration.fabric.manage');
  const upd = (k) => (v) => { setF((s) => ({ ...s, [k]: v })); setDirty(true); };
  const updIn = (g, k) => (v) => { setF((s) => ({ ...s, [g]: { ...s[g], [k]: v } })); setDirty(true); };
  async function save() {
    setBusy(true); setErr(null);
    const body = {
      enabled: f.enabled, allowed_tenants: unlines(f.tenants), audiences: unlines(f.auds),
      workspace_map: f.wm.filter((m) => m.fabric_workspace_id).map((m) => ({ fabric_workspace_id: m.fabric_workspace_id, olcf_workspace: m.olcf_workspace || '', tenant_id: m.tenant_id || null, label: m.label || null })),
      lakehouse_map: f.lm.filter((m) => m.olcf_lakehouse).map((m) => ({ fabric_workspace_id: m.fabric_workspace_id || null, fabric_lakehouse_id: m.fabric_lakehouse_id || null, fabric_lakehouse_name: m.fabric_lakehouse_name || null, olcf_lakehouse: m.olcf_lakehouse })),
      broker_connection: f.broker_connection || null,
      default_runtime: Object.fromEntries(Object.entries(f.runtime).filter(([, v]) => v)),
      default_policy: { queue: f.policy.queue || 'default', priority: num(f.policy.priority) ?? 50, timeout_seconds: num(f.policy.timeout_seconds), retries: num(f.policy.retries) ?? 0, retention_days: num(f.policy.retention_days), allow_pip_install: !!f.policy.allow_pip_install, allow_shell: !!f.policy.allow_shell, max_executors: num(f.policy.max_executors), max_executor_memory_mb: num(f.policy.max_executor_memory_mb) },
      network: { proxy: f.net.proxy || null, ca_bundle: f.net.ca_bundle || null, timeout_seconds: num(f.net.timeout_seconds), trust_env: f.net.trust_env !== false, authority_host: f.net.authority_host || null, fabric_api: f.net.fabric_api || null, onelake_dfs: f.net.onelake_dfs || null },
      secret_grants: f.sg.filter((g) => g.vault && g.name && g.secret_id), auto_create_lakehouses: !!f.auto_create_lakehouses, enforce_data_scope: f.enforce_data_scope !== false, pull_timeout_seconds: num(f.pull_timeout_seconds) ?? 120, sdk_hints: c0.sdk_hints,
    };
    try { await api.put('/integrations/fabric', body); toast.ok('Fabric configuration saved'); setDirty(false); reload(); } catch (e) { setErr(e); } finally { setBusy(false); }
  }
  async function rotate() {
    if (!(await confirmDialog({ title: 'Rotate the capsule signing key?', confirmLabel: 'Rotate key', danger: true, body: 'Submissions in flight keep working; new SDK sessions use the new key. Use this if you suspect the key leaked.' }))) return;
    const r = await act(() => api.post('/integrations/fabric/signing/rotate'), { ok: 'Signing key rotated' });
    if (r) reload();
  }
  const wsOpts = workspaces.map((w) => ({ value: w.id, label: w.display_name || w.name }));
  const connOpts = (conns.data || []).filter((c) => !c.type || /onelake|fabric|adls|abfs/i.test(c.type) || true).map((c) => ({ value: c.id, label: `${c.name}${c.type ? ' (' + c.type + ')' : ''}` }));
  const families = (mx.data ? mx.data.families : []).map((x) => x.family);
  const rtOpts = (rts.data || []).filter((r) => !f.runtime.family || r.family === f.runtime.family).map((r) => r.id);
  const fieldErrs = err && err.details && err.details.errors;
  const dis = !manage;
  return html`<div class="col gap-4">
    ${err ? html`<div><${ErrorBox} error=${err} />${fieldErrs ? html`<ul class="field-errs">${fieldErrs.map((e, i) => html`<li key=${i}><code>${e.field}</code> ${e.message}</li>`)}</ul>` : null}</div>` : null}
    ${!manage ? html`<${Banner} kind="info" title="Read only">You can see this configuration but not change it. Changing it needs the permission integration.fabric.manage.<//>` : null}
    <${Card} title="Who may submit" sub="Notebooks are accepted only from these Entra tenants and for this API audience">
      <${Switch} label="Integration enabled" checked=${f.enabled} onChange=${upd('enabled')} disabled=${dis} />
      <div class="form-grid mt-3"><${Textarea} label="Allowed Entra tenants" mono=${true} rows=${3} value=${f.tenants} onInput=${upd('tenants')} disabled=${dis} hint="One tenant id (GUID) per line. Empty: Entra tokens are refused; platform tokens still work." />
        <${Textarea} label="API audiences" mono=${true} rows=${3} value=${f.auds} onInput=${upd('auds')} disabled=${dis} hint="Client id or application id URI (api://…) of the Entra app registration that represents OLCF." /></div></${Card}>
    <${Card} title="Workspace mapping" sub="Each Fabric workspace runs its notebooks in one OLCF workspace. Unmapped workspaces are rejected.">
      <${ListEditor} rows=${f.wm} onChange=${upd('wm')} add="Add mapping" empty="No Fabric workspace is mapped yet." cols=${[{ key: 'fabric_workspace_id', label: 'Fabric workspace id', placeholder: '00000000-0000-0000-0000-000000000000', mono: true, w: '1.4fr' }, { key: 'olcf_workspace', label: 'OLCF workspace', options: wsOpts, placeholder: 'Choose workspace' }, { key: 'label', label: 'Label', placeholder: 'optional' }]} /></${Card}>
    <${Card} title="Lakehouse mapping" sub="Optional: run against an OLCF lakehouse instead of the Fabric one, for example a local copy of the data">
      <${ListEditor} rows=${f.lm} onChange=${upd('lm')} add="Add lakehouse mapping" empty="Fabric lakehouse references are used as they are." cols=${[{ key: 'fabric_lakehouse_name', label: 'Fabric lakehouse name' }, { key: 'fabric_lakehouse_id', label: 'Fabric lakehouse id', mono: true }, { key: 'olcf_lakehouse', label: 'OLCF lakehouse (id or name)' }]} /></${Card}>
    <${Card} title="Identity toward Fabric and OneLake" sub="The control plane reads Fabric and OneLake with a service principal stored as an OLCF connection. Its secret is never shown.">
      <div class="form-grid"><${Select} label="Broker connection" value=${f.broker_connection || ''} onChange=${upd('broker_connection')} placeholder="None" options=${connOpts} disabled=${dis} hint="A connection of type onelake that holds the client secret or certificate." />
        <div class="field"><label>Capsule signing</label><div class="row"><${Btn} icon="refresh" onClick=${rotate} disabled=${dis}>Rotate signing key<//><span class="small muted">Capsules are signed per submission with a key derived from this.</span></div></div></div></${Card}>
    <${Card} title="What notebooks may do" sub="A submitted notebook can narrow these limits but never widen them">
      <div class="form-grid"><${Select} label="Default runtime family" value=${f.runtime.family || ''} onChange=${updIn('runtime', 'family')} options=${families.length ? families : ['fabric-2.0-compatible']} disabled=${dis} />
        <${Select} label="Pin a runtime (optional)" value=${f.runtime.runtime_id || ''} onChange=${updIn('runtime', 'runtime_id')} placeholder="Let the scheduler choose" options=${rtOpts} disabled=${dis} />
        <${Select} label="Queue" value=${f.policy.queue || 'default'} onChange=${updIn('policy', 'queue')} options=${(queues.data || [{ name: 'default' }]).map((q) => q.name)} disabled=${dis} />
        <${Input} label="Priority (0 to 100)" type="number" min="0" max="100" value=${f.policy.priority} onInput=${updIn('policy', 'priority')} disabled=${dis} />
        <${Input} label="Timeout (seconds)" type="number" min="1" value=${f.policy.timeout_seconds ?? ''} onInput=${updIn('policy', 'timeout_seconds')} disabled=${dis} hint="Empty: platform default." />
        <${Input} label="Automatic retries" type="number" min="0" max="10" value=${f.policy.retries} onInput=${updIn('policy', 'retries')} disabled=${dis} />
        <${Input} label="Max executors" type="number" min="1" value=${f.policy.max_executors ?? ''} onInput=${updIn('policy', 'max_executors')} disabled=${dis} />
        <${Input} label="Max executor memory (MB)" type="number" min="256" value=${f.policy.max_executor_memory_mb ?? ''} onInput=${updIn('policy', 'max_executor_memory_mb')} disabled=${dis} />
        <${Input} label="Keep results for (days)" type="number" min="0" value=${f.policy.retention_days ?? ''} onInput=${updIn('policy', 'retention_days')} disabled=${dis} /></div>
      <div class="col gap-1 mt-2"><${Switch} label="Allow %pip install from notebook code" checked=${!!f.policy.allow_pip_install} onChange=${updIn('policy', 'allow_pip_install')} disabled=${dis} /><${Switch} label="Allow shell escapes (!cmd, %%sh, os.system)" checked=${!!f.policy.allow_shell} onChange=${updIn('policy', 'allow_shell')} disabled=${dis} />
        <${Switch} label="Reject OneLake references to Fabric workspaces that are not mapped" checked=${f.enforce_data_scope !== false} onChange=${upd('enforce_data_scope')} disabled=${dis} /><${Switch} label="Create a lakehouse item the first time a mapped Fabric lakehouse is used" checked=${!!f.auto_create_lakehouses} onChange=${upd('auto_create_lakehouses')} disabled=${dis} /></div></${Card}>
    <${Card} title="Key Vault secrets" sub="Let notebooks read specific secrets through notebookutils.credentials.getSecret">
      <${ListEditor} rows=${f.sg} onChange=${upd('sg')} add="Grant a secret" empty="No secret is exposed to notebooks." cols=${[{ key: 'vault', label: 'Vault URL or name', placeholder: 'https://my-vault.vault.azure.net/' }, { key: 'name', label: 'Secret name' }, { key: 'secret_id', label: 'OLCF secret id', mono: true }]} /></${Card}>
    <${Card} title="Network" sub="Applies to calls from the control plane to Entra, Fabric and OneLake (corporate proxy, private CA, sovereign clouds)">
      <div class="form-grid"><${Input} label="HTTPS proxy" value=${f.net.proxy || ''} onInput=${updIn('net', 'proxy')} placeholder="http://proxy.corp:3128" disabled=${dis} /><${Input} label="CA bundle (PEM path)" value=${f.net.ca_bundle || ''} onInput=${updIn('net', 'ca_bundle')} disabled=${dis} />
        <${Input} label="Timeout (seconds)" type="number" min="1" max="120" value=${f.net.timeout_seconds ?? ''} onInput=${updIn('net', 'timeout_seconds')} disabled=${dis} /><${Input} label="Pull timeout (seconds)" type="number" min="5" max="900" value=${f.pull_timeout_seconds} onInput=${upd('pull_timeout_seconds')} disabled=${dis} />
        <${Input} label="Authority host" value=${f.net.authority_host || ''} onInput=${updIn('net', 'authority_host')} placeholder=${c0.network_effective.authority_host} disabled=${dis} /><${Input} label="Fabric API" value=${f.net.fabric_api || ''} onInput=${updIn('net', 'fabric_api')} placeholder=${c0.network_effective.fabric_api} disabled=${dis} />
        <${Input} label="OneLake DFS" value=${f.net.onelake_dfs || ''} onInput=${updIn('net', 'onelake_dfs')} placeholder=${c0.network_effective.onelake_dfs} disabled=${dis} /></div>
      <${Switch} label="Use proxy settings from the environment (HTTPS_PROXY, NO_PROXY)" checked=${f.net.trust_env !== false} onChange=${updIn('net', 'trust_env')} disabled=${dis} /></${Card}>
    ${manage ? html`<div class="sticky-save"><span class="small muted">${dirty ? 'You have unsaved changes.' : 'Everything is saved.'}</span><${Btn} kind="primary" icon="save" busy=${busy} disabled=${!dirty} onClick=${save}>Save configuration<//></div>` : null}</div>`;
}

// ------------------------------------------------------------------ submissions
const STATE_KIND = { succeeded: 'ok', failed: 'bad', rejected: 'bad', timed_out: 'bad', cancelled: 'off', running: 'busy', queued: 'warn', accepted: 'info' };
function SubTable({ items, onOpen, compact }) {
  return html`<${Table} caption="Fabric submissions" dense=${true} rowKey=${(s) => s.id} onRowClick=${onOpen} columns=${[
    { key: 'state', label: 'State', render: (s) => html`<${StatusBadge} state=${s.state} />` },
    { key: 'notebook_name', label: 'Notebook', render: (s) => html`<b>${s.notebook_name || s.notebook_id || 'notebook'}</b><div class="small muted">${s.fabric_workspace_name || s.fabric_workspace_id || ''}</div>` },
    ...(compact ? [] : [{ key: 'caller', label: 'Caller', sort: (s) => (s.caller || {}).name || '', render: (s) => (s.caller && (s.caller.name || s.caller.id)) || '' }, { key: 'source_mode', label: 'Mode' }]),
    { key: 'round_trip_ms', label: 'Duration', num: true, render: (s) => (s.round_trip_ms != null ? fmtMs(s.round_trip_ms) : s.job && s.job.started_at ? 'running' : '–') },
    { key: 'received_at', label: 'Received', num: true, render: (s) => html`<${Ago} ts=${s.received_at} />` },
  ]} rows=${items} />`;
}

function useLogs(sid, active) {
  const [text, setText] = useState('');
  const off = useRef(0);
  useEffect(() => {
    off.current = 0; setText('');
    let stop = false; let timer;
    const tick = async () => {
      try {
        const p = await api.get(`/integrations/fabric/submissions/${encodeURIComponent(sid)}/logs`, { offset: off.current, limit: 65536 });
        if (stop) return;
        if (p.data) setText((t) => (t + p.data).slice(-400000));
        off.current = p.next_offset ?? off.current;
        if (!p.eof || active) timer = setTimeout(tick, p.eof ? 1500 : 200);
      } catch { if (!stop) timer = setTimeout(tick, 4000); }
    };
    tick();
    return () => { stop = true; clearTimeout(timer); };
  }, [sid, active]);
  return text;
}


/** One readable line from the analyzer summary: cells analysed, what needed a shim or a rewrite, and whether anything blocks the run. */
function compatText(c) {
  if (!c) return null;
  if (typeof c === 'string') return c;
  if (c.summary) return String(c.summary);
  const counts = c.counts || {};
  const parts = Object.entries(counts).filter(([k, n]) => n > 0 && k !== 'native-portable').map(([k, n]) => `${n} ${k.replace(/-/g, ' ')}`);
  const shims = (c.shims_required || []).length ? `shims: ${c.shims_required.join(', ')}` : '';
  const head = `${c.cells != null ? c.cells + ' cell' + (c.cells === 1 ? '' : 's') : 'Analysed'}, ${c.blocked ? 'blocked' : 'nothing blocks the run'}`;
  return [head, ...parts, shims].filter(Boolean).join(' · ');
}

function SubmissionDrawer({ id, onClose, reload }) {
  const sub = usePoll(() => api.get(`/integrations/fabric/submissions/${encodeURIComponent(id)}`), 2500, [id]);
  const s = sub.data;
  const live = s && !['succeeded', 'failed', 'rejected', 'timed_out', 'cancelled'].includes(s.state);
  const out = useAsync(() => (s && !live ? api.get(`/integrations/fabric/submissions/${encodeURIComponent(id)}/output`) : null), [id, live, s && s.state]);
  const logs = useLogs(id, live);
  const logRef = useRef();
  useEffect(() => { if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight; }, [logs]);
  async function cancel() { if (await act(() => api.post(`/integrations/fabric/submissions/${encodeURIComponent(id)}/cancel`), { ok: 'Cancel requested' })) { sub.reload(); reload(); } }
  return html`<${Drawer} wide=${true} onClose=${onClose} title=${s ? html`<span class="row"><${StatusBadge} state=${s.state} />${s.notebook_name || s.notebook_id}</span>` : 'Submission'}>
    ${!s ? (sub.error ? html`<${ErrorBox} error=${sub.error} />` : html`<${Loading} />`) : html`<div class="col gap-4">
      ${s.error ? html`<${Banner} kind="bad" title=${s.failure_category ? titleCase(s.failure_category) : 'Failed'}>${typeof s.error === 'string' ? s.error : JSON.stringify(s.error)}<//>` : null}
      ${s.job && s.job.failure ? html`<${Banner} kind="bad" title=${`Job failed: ${s.job.failure.category || ''}`} remediation=${s.job.failure.remediation}>${s.job.failure.message}<//>` : null}
      <div class="row-wrap">${live && can('job.cancel') ? html`<${Btn} kind="danger" icon="stop" onClick=${cancel}>Cancel run<//>` : null}${s.job_id ? html`<${Btn} icon="external" href=${jobHref(s.job_id)}>Open job<//>` : null}</div>
      <${Kv} items=${[['Submission', html`<code>${s.id}</code>`], ['Received', fmtTime(s.received_at)], ['Finished', s.finished_at ? fmtTime(s.finished_at) : null], ['Duration', s.round_trip_ms != null ? fmtMs(s.round_trip_ms) : null],
        ['Fabric workspace', s.fabric_workspace_name ? `${s.fabric_workspace_name} (${s.fabric_workspace_id})` : s.fabric_workspace_id], ['Notebook', `${s.notebook_name || ''} ${s.notebook_id ? '(' + s.notebook_id + ')' : ''}`], ['Entra tenant', s.tenant_id], ['Caller', s.caller ? (s.caller.name || s.caller.id) + (s.caller.kind ? ' · ' + s.caller.kind : '') : null],
        ['Source', `${s.source_mode} · ${fmtBytes(s.capsule_bytes)}`], ['SDK', `${s.sdk_version} / adapter ${s.adapter_version}`], ['Pipeline run', s.is_for_pipeline ? s.parent_run_id || 'yes' : 'no'], ['Compatibility', compatText(s.compat_summary)], ['Exit value', s.job && s.job.exit_value != null ? html`<code>${String(s.job.exit_value)}</code>` : null]]} />
      ${s.parameters && Object.keys(s.parameters).length ? html`<div><h3 class="mb-2">Parameters</h3><${Code} text=${jsonPretty(s.parameters)} lang="json" /></div>` : null}
      <div><h3 class="mb-2">Log ${live ? html`<span class="badge busy">live</span>` : null}</h3><div class="code-wrap"><pre class="code logview" ref=${logRef} tabindex="0">${logs || (live ? 'Waiting for output…' : 'No log output.')}</pre></div></div>
      ${out.data && out.data.available ? html`<div><h3 class="mb-2">Notebook output</h3>${out.data.cells.map((c) => html`<div class="cell-out" key=${c.index ?? c.cell}><div class="small muted">Cell ${(c.index ?? c.cell) + 1}</div>${(c.outputs || []).map((o, i) => html`<pre class=${'code ' + (o.name === 'error' || o.label === 'error' ? 'err' : '')} key=${i} tabindex="0">${o.text}</pre>`)}</div>`)}${out.data.truncated ? html`<div class="small muted">Output was truncated.</div>` : null}</div>` : null}
    </div>`}</${Drawer}>`;
}

function Submissions() {
  const [stateF, setStateF] = useQueryState('state', '');
  const [sel, setSel] = useQueryState('sub', '');
  const q = usePoll(() => api.get('/integrations/fabric/submissions', { limit: 100, state: stateF || undefined }), 3000, [stateF]);
  useEffect(() => bus.on('event:fabric', () => q.reload()), []);
  const items = (q.data && q.data.items) || [];
  const inflight = items.filter((s) => !['succeeded', 'failed', 'rejected', 'timed_out', 'cancelled'].includes(s.state)).length;
  return html`<div class="col gap-3"><div class="toolbar"><${Select} size="sm" value=${stateF} onChange=${setStateF} placeholder="All states" aria-label="Filter by state" options=${['accepted', 'queued', 'running', 'succeeded', 'failed', 'rejected', 'timed_out', 'cancelled']} />
      <span class="grow"></span>${inflight ? html`<span class="badge busy"><${Light} state="busy" label="" />${inflight} in flight</span>` : null}<span class="small muted">Updates live</span></div>
    ${q.error && !q.data ? html`<${ErrorBox} error=${q.error} onRetry=${q.reloadLoud} />` : !q.data ? html`<${Loading} />` : items.length ? html`<${Card} flush=${true} head=${false}><${SubTable} items=${items} onOpen=${(s) => setSel(s.id)} /></${Card}>` : html`<${Empty} icon="cloud" title=${stateF ? 'No submission in this state' : 'Nothing has been submitted from Fabric yet'}>Add the SDK cell from <a href="#/fabric?tab=sdk">SDK setup</a> to a Fabric notebook and run it.<//>`}
    ${sel ? html`<${SubmissionDrawer} id=${sel} onClose=${() => setSel('')} reload=${q.reload} />` : null}</div>`;
}

// ------------------------------------------------------------------ metrics
function Metrics() {
  const [win, setWin] = useState('24h');
  const m = usePoll(() => api.get('/integrations/fabric/metrics', { window: win }), 10000, [win]);
  const d = m.data;
  const kv = (o) => Object.entries(o || {}).map(([k, v]) => ({ name: k, count: v }));
  return html`<div class="col gap-4"><div class="toolbar"><${Seg} label="Time window" value=${win} onChange=${setWin} options=${[{ value: '1h', label: '1 hour' }, { value: '24h', label: '24 hours' }, { value: '7d', label: '7 days' }]} /></div>
    ${!d ? (m.error ? html`<${ErrorBox} error=${m.error} />` : html`<${Loading} />`) : html`
    <div class="grid c4" style="grid-template-columns:repeat(auto-fit,minmax(190px,1fr))">
      ${[['Received', fmtNum(d.counters.received), `${d.counters.accepted} accepted · ${d.counters.rejected} rejected`], ['Success rate', d.counters.success_rate == null ? '–' : fmtPct(d.counters.success_rate * (d.counters.success_rate <= 1 ? 100 : 1)), `${d.counters.succeeded} succeeded · ${d.counters.failed} failed`], ['In flight', fmtNum(d.counters.in_flight), `${d.counters.timed_out} timed out · ${d.counters.cancelled} cancelled`], ['Round trip p50 / p95', d.latency_ms.p50 == null ? '–' : `${fmtMs(d.latency_ms.p50)} / ${fmtMs(d.latency_ms.p95)}`, d.latency_ms.n ? `${d.latency_ms.n} runs, p99 ${fmtMs(d.latency_ms.p99)}` : 'No completed runs'], ['Handshakes', fmtNum(d.counters.handshakes), d.last_handshake ? `last ${ago(d.last_handshake.at || d.last_handshake)}` : 'none yet']].map(([l, v, s]) => html`<div class="stat" key=${l}><div class="stat-top"><span class="stat-label">${l}</span></div><div class="stat-value">${v}</div><div class="stat-sub">${s}</div></div>`)}</div>
    <div class="grid c2"><${Card} title="Failures by category">${Object.keys(d.errors_by_category).length ? html`<${BarChart} columns=${['category', 'count']} rows=${kv(d.errors_by_category).map((x) => [x.name, x.count])} view=${{ x: 'category', y: 'count', orientation: 'horizontal' }} height=${200} />` : html`<div class="muted">No failures in this window.</div>`}</${Card}>
      <${Card} title="Top notebooks">${d.top_notebooks.length ? html`<${BarChart} columns=${['notebook', 'runs']} rows=${d.top_notebooks.map((n) => [n.name || n.notebook_name || n.notebook_id || n.id || '?', n.count ?? n.runs ?? n.n ?? 0])} view=${{ x: 'notebook', y: 'runs', orientation: 'horizontal' }} height=${200} />` : html`<div class="muted">No runs in this window.</div>`}</${Card}></div>
    ${Object.keys(d.rejections_by_code).length ? html`<${Card} title="Rejections by reason" flush=${true}><${Table} dense=${true} rowKey=${(r) => r.code} columns=${[{ key: 'code', label: 'Reason code', render: (r) => html`<code>${r.code}</code>` }, { key: 'count', label: 'Count', num: true }]} rows=${Object.entries(d.rejections_by_code).map(([code, count]) => ({ code, count }))} /></${Card}>` : null}
    <${Card} title="What Fabric passes along" sub="Aggregates of what arrives with submissions"><div class="grid c2"><${Kv} items=${[['SDK versions', Object.entries(d.passed_along.sdk_versions).map(([k, v]) => `${k} (${v})`).join(', ') || null], ['Source modes', Object.entries(d.passed_along.source_modes).map(([k, v]) => `${k} (${v})`).join(', ') || null], ['From pipelines', d.passed_along.pipeline_runs], ['With a default lakehouse', d.passed_along.with_default_lakehouse], ['Average parameters', d.passed_along.parameters_per_submission_avg]]} /><${Kv} items=${[['Average capsule', fmtBytes(d.passed_along.capsule_bytes_avg)], ['Largest capsule', fmtBytes(d.passed_along.capsule_bytes_max)], ['Tenants', Object.entries(d.passed_along.tenants).map(([k, v]) => `${k} (${v})`).join(', ') || null], ['Findings by class', Object.entries(d.passed_along.compatibility_findings_by_class).map(([k, v]) => `${k} (${v})`).join(', ') || null]]} /></div></${Card}>`}</div>`;
}

// ------------------------------------------------------------------ SDK snippet
function Snippet({ cfg }) {
  const [auth, setAuth] = useState('keyvault');
  const [ws, setWs] = useState('');
  const sn = useAsync(() => api.get('/integrations/fabric/snippet', { auth, workspace: ws || undefined }), [auth, ws]);
  const maps = cfg.config.workspace_map;
  return html`<div class="grid c2 fabric-grid"><${Card} title="Run a Fabric notebook here" sub="Paste this cell into a notebook in Microsoft Fabric">
      <div class="form-grid"><${Select} label="How the notebook authenticates" value=${auth} onChange=${setAuth} options=${[{ value: 'keyvault', label: 'Token in Azure Key Vault (recommended)' }, { value: 'token', label: 'API token' }, { value: 'client_credentials', label: 'Service principal' }, { value: 'notebookutils', label: 'Fabric session identity' }]} />
        <${Select} label="Fabric workspace" value=${ws} onChange=${setWs} placeholder="Not specified" options=${maps.map((m) => ({ value: m.fabric_workspace_id, label: m.label || m.fabric_workspace_id }))} /></div>
      <${Async} state=${sn}>${(d) => html`<${Code} text=${d.snippet} lang="python" /><ul class="notes">${d.notes.map((n, i) => html`<li key=${i}>${n}</li>`)}</ul>${d.endpoint.startsWith('http://') ? html`<${Banner} kind="warn" title="The endpoint is not HTTPS" remediation="Set server.public_url to an https URL that Fabric can reach; Fabric notebooks cannot call a local address.">The snippet points at ${d.endpoint}.<//>` : null}`}<//></${Card}>
    <div class="col gap-4"><${Card} title="Install the SDK" sub=${`olcf-fabric ${(sn.data && sn.data.sdk.version) || ''}`}><p class="muted" style="margin-top:0">The SDK is a small wheel. Attach it to a Fabric Environment, or install it for one session.</p><${Code} text=${`%pip install olcf_fabric-${(sn.data && sn.data.sdk.version) || '0.1.0'}-py3-none-any.whl`} lang="python" /><p class="small muted mt-3">What happens on <code>olcf.run_remote()</code>: the notebook is captured, checked for compatibility, signed and submitted. Output and the exit value stream back into the cell, and a failure raises <code>RemoteExecutionError</code> so pipelines see it.</p></${Card}>
      <${Card} title="Endpoints"><${Kv} items=${[['Control plane', html`<code>${cfg.public_url}</code>`], ...Object.entries(cfg.endpoints).map(([k, v]) => [titleCase(k), html`<code>${v}</code>`]), ['Adapter', cfg.adapter_version]]} /></${Card}></div></div>`;
}

// ------------------------------------------------------------------ analyzer
const CLASS_KIND = { 'native-portable': 'ok', 'compatibility-shim': 'info', 'deterministic-rewrite': 'info', warning: 'warn', 'fabric-only-fallback': 'warn', unsupported: 'bad' };
function Analyzer() {
  const [text, setText] = useState('');
  const [fmt, setFmt] = useState('');
  const [lake, setLake] = useState('');
  const [res, setRes] = useState(null);
  const [err, setErr] = useState(null);
  const [busy, setBusy] = useState(false);
  const file = useRef();
  async function run() {
    setBusy(true); setErr(null);
    try { setRes(await api.post('/integrations/fabric/analyze', { content: text, format: fmt || null, lakehouse: lake || null })); } catch (e) { setErr(e); setRes(null); } finally { setBusy(false); }
  }
  const onFile = async (e) => { const f = e.target.files && e.target.files[0]; if (f) { setText(await f.text()); if (/\.ipynb$/.test(f.name)) setFmt('ipynb'); else if (/\.py$/.test(f.name)) setFmt('py'); } e.target.value = ''; };
  return html`<div class="grid c2 fabric-grid"><${Card} title="Check a notebook" sub="See how a Fabric notebook would run here, before you submit it. No job is created.">
      <div class="row-wrap mb-3"><input ref=${file} type="file" hidden accept=".ipynb,.py,.json" onChange=${onFile} aria-label="Notebook file" /><${Btn} icon="upload" onClick=${() => file.current.click()}>Load a file<//>
        <${Select} size="sm" value=${fmt} onChange=${setFmt} placeholder="Detect format" aria-label="Notebook format" options=${[{ value: 'ipynb', label: 'Jupyter (.ipynb)' }, { value: 'fabric_git_source', label: 'Fabric Git source' }, { value: 'py', label: 'Python script' }]} />
        <${Select} size="sm" value=${lake} onChange=${setLake} placeholder="Lakehouse: as configured" aria-label="Lakehouse what-if" options=${[{ value: 'local', label: 'What if: local lakehouse' }, { value: 'remote', label: 'What if: Fabric lakehouse' }]} /></div>
      <${Textarea} label="Notebook content" mono=${true} rows=${16} value=${text} onInput=${setText} placeholder="Paste the notebook JSON, a Fabric Git source file, or a Python script" />
      <div class="mt-3"><${Btn} kind="primary" icon="search" busy=${busy} disabled=${!text.trim()} onClick=${run}>Analyze<//></div></${Card}>
    <div class="col gap-4">${err ? html`<${ErrorBox} error=${err} />` : null}
      ${!res && !err ? html`<${Empty} icon="flask" title="No analysis yet">Load or paste a notebook and choose Analyze. Every Fabric-specific construct is classified: portable, served by a shim, rewritten, or unsupported.<//>` : null}
      ${res ? html`<${AnalysisResult} r=${res} />` : null}</div></div>`;
}
function AnalysisResult({ r }) {
  const c = r.counts;
  return html`<${Banner} kind=${r.blocked ? 'bad' : c.warning || c['fabric-only-fallback'] ? 'warn' : 'ok'} title=${r.blocked ? 'This notebook cannot run as it is' : 'This notebook can run here'}>${r.summary}<//>
    <div class="chips">${Object.entries(c).map(([k, v]) => html`<span class=${'badge ' + CLASS_KIND[k]} key=${k}>${k} <b>${v}</b></span>`)}</div>
    ${r.blockers.length ? html`<${Card} title="Blockers"><ul class="plain">${r.blockers.map((b, i) => html`<li key=${i}><${Light} state="down" /> <b>${b.construct}</b> <span class="muted small">cell ${b.cell + 1}, line ${b.line}</span><div class="small">${b.message}</div>${b.remediation ? html`<div class="small mt-2"><b>Fix: </b>${b.remediation}</div>` : null}</li>`)}</ul></${Card}>` : null}
    <${Card} title="All findings" flush=${true}><${Table} dense=${true} rowKey=${(f, i) => f.rule + f.line + f.column} columns=${[{ key: 'class', label: 'Class', render: (f) => html`<span class=${'badge ' + CLASS_KIND[f.class]}>${f.class}</span>` }, { key: 'construct', label: 'Construct', render: (f) => html`<code>${f.construct}</code>` }, { key: 'loc', label: 'Where', sort: (f) => f.cell * 10000 + f.line, render: (f) => `cell ${f.cell + 1}, line ${f.line}` }, { key: 'message', label: 'What happens', render: (f) => f.message }]} rows=${r.findings} /></${Card}>
    <${Card} title="Details"><${Kv} items=${[['Format', `${r.source.format} · ${fmtBytes(r.source.bytes)}`], ['Cells', `${r.cells.total} (${r.cells.code} code, ${r.cells.markdown} markdown, ${r.cells.sql} SQL)`], ['Runtime family', r.runtime_family], ['Shims needed', (r.shims_required || []).join(', ') || 'none'], ['Packages', (r.packages || []).join(', ') || 'none'], ['Data references', (r.data_references.paths || []).map((p) => p.uri).join(', ') || 'none'], ['Ruleset', r.ruleset_version]]} /></${Card}>`;
}

// ------------------------------------------------------------------ rules
function Rules() {
  const r = useAsync(() => api.get('/integrations/fabric/compat/rules'), []);
  const [cls, setCls] = useState('');
  return html`<${Async} state=${r}>${(d) => {
    const rules = d.rules.filter((x) => !cls || x.class === cls);
    return html`<div class="col gap-3"><div class="toolbar"><${Select} size="sm" value=${cls} onChange=${setCls} placeholder="All classes" aria-label="Filter by class" options=${d.classes} /><span class="grow"></span><span class="small muted">Ruleset ${d.ruleset_version} · ${rules.length} rules · families: ${d.runtime_families.join(', ')}</span></div>
      <${Card} flush=${true} head=${false}><${Table} dense=${true} rowKey=${(x) => x.id} columns=${[{ key: 'class', label: 'Class', render: (x) => html`<span class=${'badge ' + CLASS_KIND[x.class]}>${x.class}</span>` }, { key: 'title', label: 'Rule', render: (x) => html`<b>${x.title}</b><div class="small muted mono">${x.id}</div>` }, { key: 'message', label: 'What happens', render: (x) => html`${x.message}${x.remediation ? html`<div class="small mt-2"><b>Fix: </b>${x.remediation}</div>` : null}` }, { key: 'm', label: 'Matches', sortable: false, render: (x) => html`<div class="chips">${Object.values(x.matchers || {}).flat().slice(0, 4).map((m) => html`<code class="tag" key=${m}>${String(m)}</code>`)}</div>` }]} rows=${rules} /></${Card}></div>`;
  }}<//>`;
}

export function FabricPage() {
  const [tab, setTab] = useQueryState('tab', 'overview');
  const cfg = useAsync(() => api.get('/integrations/fabric'), []);
  const d = cfg.data;
  const req = d ? d.readiness.filter((r) => r.required) : [];
  const ready = d && req.every((r) => r.ok);
  return html`<div class="page"><${PageHead} icon="cloud" title="Microsoft Fabric" sub="Run notebooks from Fabric on your own machines, with the same code and the same results"
      actions=${d ? html`<span class=${'badge ' + (ready ? 'ok' : 'warn')}><${Light} state=${ready ? 'ok' : 'degraded'} label="" />${ready ? 'Ready for notebooks' : `${req.filter((r) => !r.ok).length} step(s) left`}</span>` : null} />
    ${cfg.error && !d ? html`<${ErrorBox} error=${cfg.error} onRetry=${cfg.reloadLoud} />` : !d ? html`<${Loading} />` : html`
      <${Tabs} label="Fabric sections" tabs=${TABS} value=${tab} onChange=${setTab} />
      <div class="tab-body">
        <${TabPanel} id="overview" value=${tab}><${Overview} cfg=${d} reload=${cfg.reload} /></${TabPanel}>
        <${TabPanel} id="config" value=${tab}><${ConfigForm} cfg=${d} reload=${cfg.reload} /></${TabPanel}>
        <${TabPanel} id="submissions" value=${tab}><${Submissions} /></${TabPanel}>
        <${TabPanel} id="metrics" value=${tab}><${Metrics} /></${TabPanel}>
        <${TabPanel} id="sdk" value=${tab}><${Snippet} cfg=${d} /></${TabPanel}>
        <${TabPanel} id="analyzer" value=${tab}><${Analyzer} /></${TabPanel}>
        <${TabPanel} id="rules" value=${tab}><${Rules} /></${TabPanel}></div>`}</div>`;
}
