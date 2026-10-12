// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Schedules and event triggers: cron editor with a live preview of the next fire times, webhook / storage / dependency triggers, one-time credentials.
import { html, useState, useAsync, usePoll, useDebounced } from '../core/h.js';
import { api, can } from '../api/client.js';
import { Btn, Badge, Banner, Empty, ErrorBox, Loading, Input, Select, Check, Switch, Textarea, Field, Code, CopyBtn } from '../ui/basic.js';
import { Table } from '../ui/data.js';
import { Dialog, MenuButton, openOverlay, confirmDialog, act, toast } from '../ui/overlay.js';
import { enc, titleCase, fmtTime, jsonPretty, shortId } from '../core/util.js';
import { describeError } from '../api/client.js';
import { Ago, useItems } from './common.js';
import { runHref, itemHref } from '../core/links.js';

const PRESETS = [['Every 15 minutes', '*/15 * * * *'], ['Hourly', '0 * * * *'], ['Daily at 02:00', '0 2 * * *'], ['Weekdays at 08:00', '0 8 * * 1-5'], ['Mondays at 06:00', '0 6 * * 1'], ['Monthly, the 1st', '0 3 1 * *']];
const ZONES = ['UTC', 'Europe/London', 'Europe/Berlin', 'Europe/Paris', 'America/New_York', 'America/Chicago', 'America/Denver', 'America/Los_Angeles', 'America/Sao_Paulo', 'Asia/Kolkata', 'Asia/Singapore', 'Asia/Tokyo', 'Australia/Sydney'];
const TARGETS = [['pipeline', 'Pipeline', 'pipeline'], ['notebook', 'Notebook', 'notebook'], ['spark_job_definition', 'Spark job definition', 'spark_job_definition']];

function browserZone() { try { return Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC'; } catch { return 'UTC'; } }
function parseJson(text, label) {
  const t = (text || '').trim();
  if (!t) return { value: {} };
  try { const v = JSON.parse(t); if (!v || typeof v !== 'object' || Array.isArray(v)) return { error: `${label} must be a JSON object` }; return { value: v }; } catch (e) { return { error: `${label} is not valid JSON: ${e.message}` }; }
}

/** Cron input with presets, time zone and the next fire times as the server computes them. */
export function CronEditor({ cron, tz, onCron, onTz, count = 5 }) {
  const dc = useDebounced(cron, 350);
  const prev = useAsync(async () => {
    if (!dc || !dc.trim()) return null;
    return api.post('/schedules/preview', { cron: dc.trim(), timezone: tz || 'UTC', count });
  }, [dc, tz]);
  const err = prev.error ? describeError(prev.error) : null;
  return html`<div class="cron-editor">
    <div class="grid c2">
      <${Input} label="Cron expression" mono value=${cron} onInput=${onCron} placeholder="0 2 * * *" hint="Minute, hour, day of month, month, day of week." error=${err ? err.body || err.title : undefined} aria-describedby="cron-prev" />
      <${Field} label="Time zone" hint="Fire times follow daylight saving in this zone.">${(id) => html`<input id=${id} class="input" list="tz-list" value=${tz} onInput=${(e) => onTz(e.target.value)} /><datalist id="tz-list">${[browserZone(), ...ZONES].map((z) => html`<option key=${z} value=${z}></option>`)}</datalist>`}<//></div>
    <div class="chips mt-2" role="group" aria-label="Schedule presets">${PRESETS.map(([l, c]) => html`<button key=${c} type="button" class="chip" aria-pressed=${cron === c} onClick=${() => onCron(c)}>${l}</button>`)}</div>
    <div class="cron-prev mt-3" id="cron-prev" aria-live="polite"><div class="small muted">Next fire times</div>
      ${prev.loading ? html`<div class="small muted">Checking…</div>` : prev.data ? html`<ol class="cron-list">${prev.data.items.map((x) => html`<li key=${x.utc}><b>${fmtTime(x.local || x.utc)}</b> <span class="muted small mono">${x.utc.replace('.000000Z', 'Z')}</span></li>`)}</ol>` : html`<div class="small muted">${err ? 'Fix the expression to see fire times.' : 'Enter an expression.'}</div>`}</div></div>`;
}

// ---------------------------------------------------------------------------------------------------------------------------------------- schedules
function ScheduleDialog({ ws, schedule, targetId, close }) {
  const d = (schedule && schedule.definition) || {};
  const t = d.target || {};
  const [name, setName] = useState(schedule ? schedule.name : '');
  const [ttype, setTtype] = useState(t.type || 'pipeline');
  const [ref, setRef] = useState(t.ref || targetId || '');
  const [cron, setCron] = useState(d.cron || '0 2 * * *');
  const [tz, setTz] = useState(d.timezone || browserZone());
  const [catchup, setCatchup] = useState(d.catchup || 'skip');
  const [params, setParams] = useState(Object.keys(t.parameters || {}).length ? jsonPretty(t.parameters) : '');
  const [busy, setBusy] = useState(false);
  const items = useItems(ws, ttype);
  const pj = parseJson(params, 'Parameters');
  async function save() {
    if (pj.error) { toast.bad('Parameters need fixing', pj.error); return; }
    if (!ref) { toast.bad('Pick what to run', 'Choose the target of the schedule.'); return; }
    const definition = { ...d, cron: cron.trim(), timezone: tz.trim() || 'UTC', catchup, target: { type: ttype, ref, parameters: pj.value } };
    setBusy(true);
    const r = await act(() => (schedule ? api.put(`/items/${enc(schedule.id)}`, { definition, comment: 'edited in the console' }, { ifMatch: schedule.etag }) : api.post('/schedules', { workspace_id: ws, name: name.trim(), description: '', definition })), { ok: schedule ? 'Schedule saved' : 'Schedule created' });
    setBusy(false);
    if (r) close(r);
  }
  const list = items.data || [];
  return html`<${Dialog} title=${schedule ? `Edit ${schedule.name}` : 'New schedule'} size="wide" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!cron.trim() || (!schedule && !name.trim())} onClick=${save}>${schedule ? 'Save schedule' : 'Create schedule'}<//>`}>
    <div class="col gap-3">
      ${schedule ? null : html`<${Input} label="Name" value=${name} onInput=${setName} placeholder="Nightly refresh" required />`}
      <div class="grid c2"><${Select} label="Runs a" value=${ttype} onChange=${(v) => { setTtype(v); setRef(''); }} options=${TARGETS.map(([v, l]) => ({ value: v, label: l }))} />
        <${Select} label="Which one" value=${ref} onChange=${setRef} placeholder=${items.loading ? 'Loading…' : list.length ? 'Choose…' : 'None in this workspace'} options=${list.map((i) => ({ value: i.id, label: i.name }))} /></div>
      <${CronEditor} cron=${cron} tz=${tz} onCron=${setCron} onTz=${setTz} />
      <${Select} label="Missed runs" value=${catchup} onChange=${setCatchup} options=${[{ value: 'skip', label: 'Skip them' }, { value: 'run_once', label: 'Run once when the platform is back' }, { value: 'run_all', label: 'Run every missed slot' }]} hint="What happens to fire times that passed while the platform was stopped." />
      <${Textarea} label="Parameters (JSON, optional)" mono rows=${3} value=${params} onInput=${setParams} error=${pj.error} placeholder='{"year": 2025}' />
    </div></${Dialog}>`;
}
export const openScheduleDialog = (props) => openOverlay(({ close }) => html`<${ScheduleDialog} ...${props} close=${close} />`);

export function SchedulesPanel({ ws, targetId }) {
  const sc = usePoll(() => api.get('/schedules', { workspace_id: ws }).then((r) => r.items), 15000, [ws]);
  const canEdit = can('item.update', ws);
  const rows = (sc.data || []).filter((s) => !targetId || (s.trigger && s.trigger.target_id === targetId));
  async function call(s, verb, body, ok) { if (await act(() => api.post(`/schedules/${enc(s.id)}/${verb}`, body), { ok })) sc.reload(); }
  async function del(s) { if (await confirmDialog({ title: `Delete ${s.name}?`, danger: true, confirmLabel: 'Delete schedule', body: 'The schedule stops firing. Runs it already started are not affected.' }) && (await act(() => api.del(`/items/${enc(s.id)}`), { ok: 'Schedule deleted' })) !== undefined) sc.reload(); }
  async function edit(s) { if (await openScheduleDialog({ ws, schedule: s })) sc.reload(); }
  async function create() { if (await openScheduleDialog({ ws, targetId })) sc.reload(); }
  async function pause(s) {
    const m = await openOverlay(({ close }) => html`<${PauseDialog} name=${s.name} close=${close} />`);
    if (m) call(s, 'pause', { minutes: m }, `Paused for ${m >= 60 ? Math.round(m / 60) + ' h' : m + ' min'}`);
  }
  if (sc.loading && !sc.data) return html`<${Loading} />`;
  if (sc.error && !sc.data) return html`<${ErrorBox} error=${sc.error} onRetry=${sc.reloadLoud} />`;
  return html`<div data-testid="schedules"><div class="toolbar"><span class="muted small">${rows.length} schedule${rows.length === 1 ? '' : 's'}</span><span class="grow"></span>${canEdit ? html`<${Btn} kind="primary" icon="plus" onClick=${create}>New schedule<//>` : null}</div>
    <${Table} caption="Schedules" rows=${rows} empty=${html`<${Empty} icon="calendar" title="No schedules yet" action=${canEdit ? html`<${Btn} kind="primary" icon="plus" onClick=${create}>New schedule<//>` : null}>A schedule starts a pipeline, notebook or Spark job on a cron expression.<//>`} columns=${[
      { key: 'name', label: 'Schedule', render: (s) => html`<b>${s.name}</b><div class="small muted">${(s.definition.target || {}).type}</div>` },
      { key: 'cron', label: 'When', render: (s) => html`<code class="mono">${s.definition.cron}</code><div class="small muted">${s.definition.timezone}</div>` },
      { key: 'next', label: 'Next run', render: (s) => { const n = (s.next_fire_times || [])[0]; return s.paused_until ? html`<${Badge} kind="warn">Paused<//> <span class="small muted">until ${fmtTime(s.paused_until)}</span>` : !s.enabled ? html`<${Badge}>Disabled<//>` : n ? html`<b>${fmtTime(n.local || n.utc)}</b><div class="small muted"><${Ago} ts=${n.utc} /></div>` : html`<span class="muted">none</span>`; }, sortable: false },
      { key: 'last', label: 'Last fired', render: (s) => (s.trigger && s.trigger.last_fired_at ? html`<${Ago} ts=${s.trigger.last_fired_at} /> <span class="small muted">${s.trigger.last_status || ''}</span>` : html`<span class="muted">never</span>`), sortable: false },
      { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (s) => html`<${MenuButton} label=${`Actions for ${s.name}`} items=${[
        can('pipeline.run', ws) && { label: 'Run now', icon: 'play', onClick: async () => { const r = await act(() => api.post(`/schedules/${enc(s.id)}/run-now`), { ok: 'Run started' }); if (r && r.run_id) window.location.hash = runHref(r.run_id); } },
        canEdit && { label: 'Edit', icon: 'edit', onClick: () => edit(s) },
        canEdit && (s.paused_until ? { label: 'Resume', icon: 'play', onClick: () => call(s, 'resume', undefined, 'Resumed') } : { label: 'Pause for a while', icon: 'pause', onClick: () => pause(s) }),
        canEdit && (s.enabled ? { label: 'Disable', icon: 'stop', onClick: () => call(s, 'disable', undefined, 'Schedule disabled') } : { label: 'Enable', icon: 'check', onClick: () => call(s, 'enable', undefined, 'Schedule enabled') }),
        canEdit && { sep: true }, canEdit && { label: 'Delete', icon: 'trash', danger: true, onClick: () => del(s) }]} />` },
    ]} /></div>`;
}
function PauseDialog({ name, close }) {
  const [m, setM] = useState(60);
  return html`<${Dialog} title=${`Pause ${name}`} onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" onClick=${() => close(Number(m))}>Pause<//>`}>
    <${Select} label="Pause for" value=${String(m)} onChange=${(v) => setM(Number(v))} options=${[[15, '15 minutes'], [60, '1 hour'], [240, '4 hours'], [1440, '1 day'], [10080, '1 week']].map(([v, l]) => ({ value: String(v), label: l }))} hint="Fire times that pass while paused are not run." /></${Dialog}>`;
}

// ---------------------------------------------------------------------------------------------------------------------------------------- triggers
const TTYPE = { webhook: 'Webhook', storage_event: 'New files', dependency: 'After a pipeline', schedule: 'Schedule' };

function TriggerDialog({ ws, targetId, close }) {
  const [type, setType] = useState('webhook');
  const [name, setName] = useState('');
  const [ref, setRef] = useState(targetId || '');
  const [params, setParams] = useState('');
  const [auth, setAuth] = useState('hmac');
  const [lh, setLh] = useState('');
  const [path, setPath] = useState('Files/incoming');
  const [pattern, setPattern] = useState('*.csv');
  const [poll, setPoll] = useState('30');
  const [debounce, setDebounce] = useState('30');
  const [mode, setMode] = useState('batch');
  const [up, setUp] = useState('');
  const [on, setOn] = useState(['succeeded']);
  const [busy, setBusy] = useState(false);
  const pipes = useItems(ws, 'pipeline');
  const lhs = useItems(ws, 'lakehouse');
  const pj = parseJson(params, 'Parameters');
  const toggle = (v) => setOn((a) => (a.includes(v) ? a.filter((x) => x !== v) : [...a, v]));
  async function save() {
    if (pj.error) { toast.bad('Parameters need fixing', pj.error); return; }
    const config = type === 'webhook' ? { auth } : type === 'storage_event' ? { lakehouse: lh, path, pattern, poll_seconds: Number(poll), debounce_seconds: Number(debounce), mode } : { upstream: { pipeline: up }, on };
    setBusy(true);
    const r = await act(() => api.post('/triggers', { workspace_id: ws, name: name.trim(), type, target: { type: 'pipeline', ref }, parameters: pj.value, config }), { ok: 'Trigger created' });
    setBusy(false);
    if (r) close(r);
  }
  const ok = name.trim() && ref && (type !== 'storage_event' || lh) && (type !== 'dependency' || (up && on.length));
  const pl = (pipes.data || []).map((i) => ({ value: i.id, label: i.name }));
  return html`<${Dialog} title="New trigger" size="wide" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!ok} onClick=${save}>Create trigger<//>`}>
    <div class="col gap-3">
      <${Select} label="Start a pipeline when" value=${type} onChange=${setType} options=${[{ value: 'webhook', label: 'An external system calls a URL (webhook)' }, { value: 'storage_event', label: 'New files arrive in a lakehouse folder' }, { value: 'dependency', label: 'Another pipeline finishes' }]} />
      <div class="grid c2"><${Input} label="Name" value=${name} onInput=${setName} placeholder="On new orders file" required />
        <${Select} label="Pipeline to run" value=${ref} onChange=${setRef} placeholder="Choose…" options=${pl} /></div>
      ${type === 'webhook' ? html`<${Select} label="Authentication" value=${auth} onChange=${setAuth} options=${[{ value: 'hmac', label: 'Signed requests (HMAC-SHA256)' }, { value: 'bearer', label: 'Bearer token' }]} hint="The secret or token is shown once, right after you create the trigger." />` : null}
      ${type === 'storage_event' ? html`<div class="grid c2"><${Select} label="Lakehouse" value=${lh} onChange=${setLh} placeholder="Choose…" options=${(lhs.data || []).map((i) => ({ value: i.id, label: i.name }))} /><${Input} label="Folder" mono value=${path} onInput=${setPath} />
        <${Input} label="File pattern" mono value=${pattern} onInput=${setPattern} /><${Select} label="One run per" value=${mode} onChange=${setMode} options=${[{ value: 'batch', label: 'Batch of files' }, { value: 'per_file', label: 'File' }]} />
        <${Input} label="Check every (seconds)" type="number" min="5" value=${poll} onInput=${setPoll} /><${Input} label="Wait for quiet (seconds)" type="number" min="0" value=${debounce} onInput=${setDebounce} hint="Files are batched until nothing new arrives for this long." /></div>` : null}
      ${type === 'dependency' ? html`<div class="grid c2"><${Select} label="Upstream pipeline" value=${up} onChange=${setUp} placeholder="Choose…" options=${pl} />
        <fieldset class="fieldset"><legend>When it</legend><div class="col gap-1">${['succeeded', 'failed', 'cancelled'].map((v) => html`<${Check} key=${v} label=${titleCase(v)} checked=${on.includes(v)} onChange=${() => toggle(v)} />`)}</div></fieldset></div>` : null}
      <${Textarea} label="Parameters (JSON, optional)" mono rows=${3} value=${params} onInput=${setParams} error=${pj.error} placeholder='{"year": 2025}' />
    </div></${Dialog}>`;
}

/** Shown once after a webhook trigger is created or its secret rotated. */
export function CredentialsDialog({ trigger, creds, close }) {
  const hmac = (trigger.config || {}).auth !== 'bearer';
  const url = creds.url;
  const sample = hmac
    ? `SECRET='${creds.secret}'\nBODY='{"source":"demo"}'\nTS=$(date +%s)\nSIG=$(printf '%s.%s' "$TS" "$BODY" | openssl dgst -sha256 -hmac "$SECRET" -hex | sed 's/^.* //')\ncurl -X POST '${url}' \\\n  -H 'Content-Type: application/json' \\\n  -H "X-OLCF-Timestamp: $TS" \\\n  -H "X-OLCF-Signature: sha256=$SIG" \\\n  -d "$BODY"`
    : `curl -X POST '${url}' \\\n  -H 'Authorization: Bearer ${creds.token}' \\\n  -H 'Content-Type: application/json' \\\n  -d '{"source":"demo"}'`;
  return html`<${Dialog} title=${`Webhook ready: ${trigger.name}`} size="wide" onClose=${() => close()} footer=${html`<${Btn} kind="primary" onClick=${() => close()}>I have saved it<//>`}>
    <${Banner} kind="warn" title="Copy the ${hmac ? 'secret' : 'token'} now">It is not stored in a readable form and cannot be shown again. Rotate it if you lose it.<//>
    <div class="mt-3"><${Field} label="URL"><${Code} text=${url} /><//></div>
    <div class="mt-3"><${Field} label=${hmac ? 'Signing secret' : 'Bearer token'}><${Code} text=${creds.secret || creds.token} /><//></div>
    <div class="mt-3"><div class="small muted mb-2">Example call</div><${Code} text=${sample} lang="shell" /></div></${Dialog}>`;
}

export function TriggersPanel({ ws, targetId }) {
  const tr = usePoll(() => api.get('/triggers', { workspace_id: ws }).then((r) => r.items), 15000, [ws]);
  const canEdit = can('item.create', ws);
  const rows = (tr.data || []).filter((t) => t.type !== 'schedule' && (!targetId || t.target_id === targetId));
  const names = useAsync(() => api.get(`/workspaces/${enc(ws)}/items`, { type: 'pipeline', limit: 500 }).then((r) => Object.fromEntries(r.items.map((i) => [i.id, i]))), [ws]);
  async function create() {
    const r = await openOverlay(({ close }) => html`<${TriggerDialog} ws=${ws} targetId=${targetId} close=${close} />`);
    if (!r) return;
    tr.reload();
    if (r.credentials) await openOverlay(({ close }) => html`<${CredentialsDialog} trigger=${r} creds=${r.credentials} close=${close} />`);
  }
  async function rotate(t) {
    if (!(await confirmDialog({ title: `Rotate the credential of ${t.name}?`, confirmLabel: 'Rotate', danger: true, body: 'The current secret stops working immediately. Update every caller with the new one.' }))) return;
    const r = await act(() => api.post(`/triggers/${enc(t.id)}/rotate-secret`));
    if (r) await openOverlay(({ close }) => html`<${CredentialsDialog} trigger=${t} creds=${{ url: r.url || `${location.origin}/v1/hooks/pipelines/${t.id}`, secret: r.secret, token: r.token }} close=${close} />`);
  }
  async function call(t, verb, ok, body) { if (await act(() => api.post(`/triggers/${enc(t.id)}/${verb}`, body), { ok })) tr.reload(); }
  async function del(t) { if (await confirmDialog({ title: `Delete ${t.name}?`, danger: true, confirmLabel: 'Delete trigger', body: 'Callers using its URL will get an error.' }) && (await act(() => api.del(`/triggers/${enc(t.id)}`), { ok: 'Trigger deleted' })) !== undefined) tr.reload(); }
  async function showRuns(t) { await openOverlay(({ close }) => html`<${TriggerRuns} trigger=${t} close=${close} />`); }
  if (tr.loading && !tr.data) return html`<${Loading} />`;
  if (tr.error && !tr.data) return html`<${ErrorBox} error=${tr.error} onRetry=${tr.reloadLoud} />`;
  const nm = names.data || {};
  return html`<div data-testid="triggers"><div class="toolbar"><span class="muted small">${rows.length} trigger${rows.length === 1 ? '' : 's'}</span><span class="grow"></span>${canEdit ? html`<${Btn} kind="primary" icon="plus" onClick=${create}>New trigger<//>` : null}</div>
    <${Table} caption="Triggers" rows=${rows} empty=${html`<${Empty} icon="webhook" title="No event triggers" action=${canEdit ? html`<${Btn} kind="primary" icon="plus" onClick=${create}>New trigger<//>` : null}>Start pipelines from a webhook call, new files in a lakehouse, or the end of another pipeline.<//>`} columns=${[
      { key: 'name', label: 'Trigger', render: (t) => html`<b>${t.name}</b><div class="small muted">${TTYPE[t.type] || t.type}</div>` },
      { key: 'target', label: 'Runs', render: (t) => (nm[t.target_id] ? html`<a href=${itemHref(nm[t.target_id])}>${nm[t.target_id].name}</a>` : html`<span class="mono small">${shortId(t.target_id)}</span>`) },
      { key: 'cfg', label: 'Condition', render: (t) => (t.type === 'webhook' ? html`<span class="small">${t.config.auth === 'bearer' ? 'Bearer token' : 'HMAC signed'}, ${t.config.rate_limit_per_minute}/min</span>` : t.type === 'storage_event' ? html`<span class="small mono">${t.config.path}/${t.config.pattern}</span>` : t.type === 'dependency' ? html`<span class="small">upstream ${(t.config.on || []).join(' or ')}</span>` : '') },
      { key: 'state', label: 'State', render: (t) => (t.enabled ? html`<${Badge} kind="ok">Enabled<//>` : html`<${Badge}>Disabled<//>`) },
      { key: 'last', label: 'Last fired', render: (t) => (t.last_fired_at ? html`<${Ago} ts=${t.last_fired_at} /> <span class="small muted">${t.last_status || ''}</span>` : html`<span class="muted">never</span>`) },
      { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (t) => html`<${MenuButton} label=${`Actions for ${t.name}`} items=${[
        { label: 'Fire history', icon: 'history', onClick: () => showRuns(t) },
        can('pipeline.run', ws) && { label: 'Run now', icon: 'play', onClick: async () => { const r = await act(() => api.post(`/triggers/${enc(t.id)}/run-now`, {}), { ok: 'Run started' }); if (r && r.run_id) window.location.hash = runHref(r.run_id); } },
        canEdit && t.type === 'webhook' && { label: 'Rotate secret', icon: 'key', onClick: () => rotate(t) },
        canEdit && (t.enabled ? { label: 'Disable', icon: 'stop', onClick: () => call(t, 'disable', 'Trigger disabled') } : { label: 'Enable', icon: 'check', onClick: () => call(t, 'enable', 'Trigger enabled') }),
        canEdit && { sep: true }, canEdit && { label: 'Delete', icon: 'trash', danger: true, onClick: () => del(t) }]} />` },
    ]} /></div>`;
}
function TriggerRuns({ trigger, close }) {
  const r = useAsync(() => api.get(`/triggers/${enc(trigger.id)}/runs`, { limit: 50 }).then((x) => x.items), [trigger.id]);
  return html`<${Dialog} title=${`Fire history: ${trigger.name}`} size="wide" onClose=${() => close()} footer=${html`<${Btn} kind="primary" onClick=${() => close()}>Close<//>`}>
    ${r.loading ? html`<${Loading} />` : r.error ? html`<${ErrorBox} error=${r.error} />` : html`<${Table} caption="Fire history" rows=${r.data} empty=${html`<${Empty} icon="history" title="Never fired">Nothing has called this trigger yet.<//>`} columns=${[
      { key: 'time', label: 'When', render: (x) => fmtTime(x.fire_time) },
      { key: 'status', label: 'Result', render: (x) => html`<${Badge} kind=${x.status === 'started' ? 'ok' : x.status === 'skipped' ? '' : 'bad'}>${titleCase(x.status)}<//> ${x.reason ? html`<span class="small muted">${x.reason}</span>` : ''}` },
      { key: 'run', label: 'Run', render: (x) => (x.run_id ? html`<a href=${runHref(x.run_id)} class="mono small">${shortId(x.run_id, 10)}</a>` : html`<span class="muted">–</span>`) },
    ]} />`}</${Dialog}>`;
}
