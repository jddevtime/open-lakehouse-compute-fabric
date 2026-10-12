// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Alerts and webhooks: active alerts with acknowledge and silence, rules with live test, notification channels, outbound webhook subscriptions with delivery history.
import { html, useState, useEffect, useMemo, useAsync, usePoll } from '../core/h.js';
import { api, can } from '../api/client.js';
import { Btn, Badge, Banner, Empty, ErrorBox, Loading, Tabs, TabPanel, PageHead, Select, Input, Textarea, Check, Switch, Field, Code, Kv, Card } from '../ui/basic.js';
import { Table, JsonView } from '../ui/data.js';
import { Icon } from '../ui/icons.js';
import { Dialog, Drawer, MenuButton, openOverlay, confirmDialog, act, toast } from '../ui/overlay.js';
import { enc, fmtTime, fmtDuration, durationBetween, titleCase, shortId, bus, jsonPretty } from '../core/util.js';
import { useQueryState, Ago, useWorkspaces } from './common.js';

const SEV = { info: ['', 'info'], warning: ['warn', 'alert-triangle'], critical: ['bad', 'alert-circle'] };
const SevBadge = ({ s }) => html`<${Badge} kind=${(SEV[s] || [''])[0]}>${titleCase(s)}<//>`;
const AlertState = ({ a }) => (a.state === 'firing' ? html`<${Badge} kind=${a.severity === 'critical' ? 'bad' : 'warn'}>Firing<//>` : a.state === 'pending' ? html`<${Badge}>Pending<//>` : html`<${Badge} kind="ok">Resolved<//>`);

function paramSummary(r) {
  const p = r.params || {};
  const skip = new Set(['resolve_after_seconds', 'interval_seconds']);
  return Object.entries(p).filter(([k, v]) => !skip.has(k) && v != null && !(Array.isArray(v) && !v.length) && v !== 'none').map(([k, v]) => `${k.replace(/_/g, ' ')} ${Array.isArray(v) ? v.join(', ') : v}`).join(' · ');
}

// ---------------------------------------------------------------------------------------------------------------------------------------- active alerts
function AlertsTab() {
  const [state, setState] = useQueryState('astate', 'open');
  const [sev, setSev] = useQueryState('sev', '');
  const q = { state: state === 'open' ? 'firing,pending' : state === 'resolved' ? 'resolved' : undefined, severity: sev || undefined, limit: 100 };
  const list = usePoll(() => api.get('/alerts', q), 8000, [state, sev]);
  useEffect(() => bus.on('event:alert', () => list.reload()), [state, sev]);
  const canManage = can('alert.manage');
  async function call(a, verb, body, ok) { if (await act(() => api.post(`/alerts/${enc(a.id)}/${verb}`, body), { ok })) list.reload(); }
  async function evaluate() { const r = await act(() => api.post('/alerts/evaluate'), { ok: 'Rules evaluated' }); if (r) list.reload(); }
  const items = (list.data && list.data.items) || [];
  async function detail(a) { await openOverlay(({ close }) => html`<${AlertDrawer} id=${a.id} close=${close} />`); }
  return html`<div data-testid="alerts"><div class="toolbar">
    <div class="chips" role="group" aria-label="Alert state">${[['open', 'Open'], ['resolved', 'Resolved'], ['all', 'All']].map(([v, l]) => html`<button key=${v} class="chip" aria-pressed=${state === v} onClick=${() => setState(v)}>${l}</button>`)}</div>
    <${Select} label="Severity" size="sm" value=${sev} onChange=${setSev} placeholder="Any severity" options=${['critical', 'warning', 'info'].map((s) => ({ value: s, label: titleCase(s) }))} />
    <span class="grow"></span>${canManage ? html`<${Btn} icon="refresh" onClick=${evaluate}>Evaluate rules now<//>` : null}</div>
    ${list.loading && !list.data ? html`<${Loading} />` : list.error && !list.data ? html`<${ErrorBox} error=${list.error} onRetry=${list.reloadLoud} />` : html`
    <${Table} caption="Alerts" rows=${items} onRowClick=${detail} empty=${html`<${Empty} icon="bell" title=${state === 'open' ? 'Nothing is firing' : 'No alerts'}>${state === 'open' ? 'All alert rules are quiet. Resolved alerts stay under Resolved.' : 'Alerts appear here when a rule fires.'}<//>`} initialSort=${{ key: 'since', dir: -1 }} columns=${[
      { key: 'sev', label: 'Severity', render: (a) => html`<${SevBadge} s=${a.severity} />`, sort: (a) => ['info', 'warning', 'critical'].indexOf(a.severity) },
      { key: 'summary', label: 'Alert', render: (a) => html`<b>${a.summary}</b><div class="small muted">${a.rule_name}</div>`, sort: (a) => a.summary },
      { key: 'state', label: 'State', render: (a) => html`<${AlertState} a=${a} />${a.acknowledged_at ? html` <${Badge}>Acknowledged<//>` : ''}${a.silenced_until && new Date(a.silenced_until) > new Date() ? html` <${Badge}>Silenced<//>` : ''}`, sort: (a) => a.state },
      { key: 'since', label: 'Since', render: (a) => html`<${Ago} ts=${a.fired_at || a.started_at} /><div class="small muted">${a.resolved_at ? 'for ' + fmtDuration(durationBetween(a.fired_at || a.started_at, a.resolved_at)) : `notified ${a.notify_count}×`}</div>`, sort: (a) => a.fired_at || a.started_at || '' },
      { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (a) => (a.state === 'resolved' || !canManage ? null : html`<${MenuButton} label=${`Actions for ${a.summary}`} items=${[
        !a.acknowledged_at && { label: 'Acknowledge', icon: 'check', onClick: () => call(a, 'ack', undefined, 'Acknowledged') },
        { label: 'Silence for 1 hour', icon: 'pause', onClick: () => call(a, 'silence', { minutes: 60 }, 'Silenced for an hour') },
        { label: 'Silence for 1 day', icon: 'pause', onClick: () => call(a, 'silence', { minutes: 1440 }, 'Silenced for a day') },
        a.silenced_until && { label: 'Lift the silence', icon: 'play', onClick: () => call(a, 'silence', { minutes: 0 }, 'Silence lifted') }]} />`) },
    ]} />`}</div>`;
}
function AlertDrawer({ id, close }) {
  const r = useAsync(() => api.get(`/alerts/${enc(id)}`), [id]);
  const a = r.data;
  return html`<${Drawer} title="Alert details" onClose=${() => close()}>${r.loading ? html`<${Loading} />` : r.error ? html`<${ErrorBox} error=${r.error} />` : html`
    <div class="row mb-3"><${SevBadge} s=${a.severity} /><${AlertState} a=${a} /></div><h3>${a.summary}</h3>
    <${Kv} items=${[['Rule', a.rule_name], ['First seen', fmtTime(a.started_at)], ['Fired', a.fired_at ? fmtTime(a.fired_at) : null], ['Last seen', fmtTime(a.last_seen_at)], ['Resolved', a.resolved_at ? fmtTime(a.resolved_at) : null], ['Notifications', a.notify_count], ['Value', a.value != null ? String(a.value) : null], ['Acknowledged', a.acknowledged_at ? `${fmtTime(a.acknowledged_at)}` : null]]} />
    <h4 class="mt-4">Labels</h4><${JsonView} value=${a.labels || {}} depth=${2} /><h4 class="mt-4">Details</h4><${JsonView} value=${a.details || {}} depth=${2} />`}</${Drawer}>`;
}

// ---------------------------------------------------------------------------------------------------------------------------------------- rules
function ParamField({ name, spec, value, onChange }) {
  const label = titleCase(name);
  const hint = spec.min != null || spec.max != null ? `${spec.min != null ? 'min ' + spec.min : ''}${spec.min != null && spec.max != null ? ', ' : ''}${spec.max != null ? 'max ' + spec.max : ''}${spec.default != null ? ` · default ${spec.default}` : ''}` : spec.default != null && !Array.isArray(spec.default) ? `Default ${spec.default}` : undefined;
  if (spec.type === 'boolean') return html`<${Check} label=${label} checked=${value ?? spec.default ?? false} onChange=${onChange} />`;
  if (spec.choices) return html`<${Select} label=${label} value=${value ?? spec.default ?? ''} onChange=${onChange} options=${spec.choices} placeholder=${spec.default == null ? 'Any' : undefined} />`;
  if (spec.type === 'string_list') return html`<${Input} label=${label} value=${Array.isArray(value) ? value.join(', ') : value ?? ''} onInput=${(v) => onChange(v)} hint="Comma separated" />`;
  if (spec.type === 'integer' || spec.type === 'number') return html`<${Input} label=${label} type="number" step=${spec.type === 'number' ? 'any' : '1'} min=${spec.min} max=${spec.max} value=${value ?? ''} placeholder=${spec.default != null ? String(spec.default) : ''} onInput=${onChange} hint=${hint} />`;
  return html`<${Input} label=${label} value=${value ?? ''} onInput=${onChange} />`;
}
function RuleDialog({ rule, types, channels, workspaces, close }) {
  const [name, setName] = useState(rule ? rule.name : '');
  const [desc, setDesc] = useState(rule ? rule.description || '' : '');
  const [type, setType] = useState(rule ? rule.type : 'failed_jobs');
  const [sev, setSev] = useState(rule ? rule.severity : 'warning');
  const [ws, setWs] = useState(rule ? rule.workspace_id || '' : '');
  const [params, setParams] = useState(rule ? { ...rule.params } : {});
  const [forS, setForS] = useState(rule ? String(rule.for_seconds) : '0');
  const [rep, setRep] = useState(rule ? String(rule.repeat_seconds) : '3600');
  const [chs, setChs] = useState(rule ? rule.channels || [] : []);
  const [enabled, setEnabled] = useState(rule ? rule.enabled : true);
  const [busy, setBusy] = useState(false);
  const spec = types.find((t) => t.type === type) || { params: {} };
  const set = (k, v) => setParams((p) => ({ ...p, [k]: v }));
  async function save() {
    const cleaned = {};
    Object.entries(spec.params).forEach(([k, s]) => {
      const v = params[k];
      if (v === '' || v == null) return;
      cleaned[k] = s.type === 'integer' ? parseInt(v, 10) : s.type === 'number' ? Number(v) : s.type === 'string_list' ? (Array.isArray(v) ? v : String(v).split(',').map((x) => x.trim()).filter(Boolean)) : v;
    });
    const body = { name: name.trim(), description: desc, severity: sev, params: cleaned, for_seconds: Number(forS) || 0, repeat_seconds: Number(rep) || 0, channels: chs, enabled };
    if (!rule) { body.type = type; if (ws) body.workspace_id = ws; }
    setBusy(true);
    const r = await act(() => (rule ? api.patch(`/alerts/rules/${enc(rule.id)}`, body, { ifMatch: rule.etag }) : api.post('/alerts/rules', body)), { ok: rule ? 'Rule saved' : 'Rule created' });
    setBusy(false);
    if (r) close(r);
  }
  const toggle = (id) => setChs((a) => (a.includes(id) ? a.filter((x) => x !== id) : [...a, id]));
  return html`<${Dialog} title=${rule ? `Edit ${rule.name}` : 'New alert rule'} size="wide" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!name.trim()} onClick=${save}>${rule ? 'Save rule' : 'Create rule'}<//>`}>
    <div class="col gap-3">
      <div class="grid c2"><${Input} label="Name" value=${name} onInput=${setName} required />
        <${Select} label="Alert when" value=${type} disabled=${!!rule} onChange=${(v) => { setType(v); setParams({}); }} options=${types.map((t) => ({ value: t.type, label: t.title }))} /></div>
      ${spec.description ? html`<p class="small muted">${spec.description}</p>` : null}
      <div class="grid c3">${Object.entries(spec.params).map(([k, s]) => html`<${ParamField} key=${type + k} name=${k} spec=${s} value=${params[k]} onChange=${(v) => set(k, v)} />`)}</div>
      <div class="grid c3"><${Select} label="Severity" value=${sev} onChange=${setSev} options=${['info', 'warning', 'critical'].map((s) => ({ value: s, label: titleCase(s) }))} />
        <${Input} label="Hold for (s)" type="number" min="0" value=${forS} onInput=${setForS} hint="Condition must persist this long before firing." />
        <${Input} label="Repeat every (s)" type="number" min="0" value=${rep} onInput=${setRep} hint="How often to re-notify while firing." /></div>
      ${!rule && spec.workspace_scoped ? html`<${Select} label="Scope" value=${ws} onChange=${setWs} placeholder="Whole organization" options=${workspaces.map((w) => ({ value: w.id, label: w.display_name || w.name }))} />` : null}
      <fieldset class="fieldset"><legend>Notify</legend>${channels.length ? html`<div class="row-wrap">${channels.map((c) => html`<${Check} key=${c.id} label=${html`${c.name} <span class="muted small">${c.type}</span>`} checked=${chs.includes(c.id)} onChange=${() => toggle(c.id)} />`)}</div>` : html`<span class="small muted">No channels yet; alerts are still recorded and shown here. Add one under Channels.</span>`}</fieldset>
      <${Textarea} label="Description" rows=${2} value=${desc} onInput=${setDesc} />
      <${Switch} label="Enabled" checked=${enabled} onChange=${setEnabled} />
    </div></${Dialog}>`;
}
function RulesTab() {
  const rules = usePoll(() => api.get('/alerts/rules').then((r) => r.items), 15000, []);
  const types = useAsync(() => api.get('/alerts/rule-types').then((r) => r.items), []);
  const chans = useAsync(() => api.get('/alerts/channels').then((r) => r.items), []);
  const { items: workspaces } = useWorkspaces();
  const canManage = can('alert.manage');
  async function edit(rule) { if (await openOverlay(({ close }) => html`<${RuleDialog} rule=${rule} types=${types.data || []} channels=${chans.data || []} workspaces=${workspaces} close=${close} />`)) rules.reload(); }
  async function toggle(r) { if (await act(() => api.patch(`/alerts/rules/${enc(r.id)}`, { enabled: !r.enabled }, { ifMatch: r.etag }), { ok: r.enabled ? 'Rule disabled' : 'Rule enabled' })) rules.reload(); }
  async function test(r) {
    const res = await act(() => api.post(`/alerts/rules/${enc(r.id)}/test`));
    if (!res) return;
    await openOverlay(({ close }) => html`<${Dialog} title=${`Test: ${r.name}`} onClose=${() => close()} footer=${html`<${Btn} kind="primary" onClick=${() => close()}>Close<//>`}>
      <${Banner} kind=${res.firing ? 'warn' : 'ok'} title=${res.firing ? 'This rule would fire right now' : 'This rule would not fire right now'}>${res.firing ? `${res.conditions.length} condition${res.conditions.length === 1 ? '' : 's'} met.` : 'Nothing currently matches its condition.'}<//>
      ${res.conditions.length ? html`<div class="mt-3"><${JsonView} value=${res.conditions} depth=${3} /></div>` : null}</${Dialog}>`);
  }
  async function silence(r, minutes) { if (await act(() => api.post(`/alerts/rules/${enc(r.id)}/silence`, { minutes }), { ok: minutes ? 'Rule silenced' : 'Silence lifted' })) rules.reload(); }
  async function del(r) { if (await confirmDialog({ title: `Delete ${r.name}?`, danger: true, confirmLabel: 'Delete rule', body: 'Alerts it raised are kept in the history.' }) && (await act(() => api.del(`/alerts/rules/${enc(r.id)}`), { ok: 'Rule deleted' })) !== undefined) rules.reload(); }
  if (rules.loading && !rules.data) return html`<${Loading} />`;
  if (rules.error && !rules.data) return html`<${ErrorBox} error=${rules.error} onRetry=${rules.reloadLoud} />`;
  const tt = Object.fromEntries((types.data || []).map((t) => [t.type, t.title]));
  return html`<div data-testid="rules"><div class="toolbar"><span class="muted small">${rules.data.length} rules</span><span class="grow"></span>${canManage ? html`<${Btn} kind="primary" icon="plus" disabled=${!types.data} onClick=${() => edit(null)}>New rule<//>` : null}</div>
    <${Table} caption="Alert rules" rows=${rules.data} columns=${[
      { key: 'name', label: 'Rule', render: (r) => html`<b>${r.name}</b>${r.builtin ? html` <${Badge}>Built in<//>` : ''}<div class="small muted">${tt[r.type] || r.type}</div>`, sort: (r) => r.name },
      { key: 'cond', label: 'Condition', render: (r) => html`<span class="small">${paramSummary(r)}</span>`, sortable: false },
      { key: 'sev', label: 'Severity', render: (r) => html`<${SevBadge} s=${r.severity} />`, sort: (r) => ['info', 'warning', 'critical'].indexOf(r.severity) },
      { key: 'on', label: 'Status', render: (r) => (r.last_error ? html`<${Badge} kind="bad" title=${r.last_error}>Error<//>` : !r.enabled ? html`<${Badge}>Disabled<//>` : r.silenced_until && new Date(r.silenced_until) > new Date() ? html`<${Badge} kind="warn">Silenced<//>` : html`<${Badge} kind="ok">Active<//>`), sortable: false },
      { key: 'ch', label: 'Notifies', render: (r) => ((r.channels || []).length ? `${r.channels.length} channel${r.channels.length === 1 ? '' : 's'}` : html`<span class="muted">UI only</span>`), sortable: false },
      { key: 'ev', label: 'Checked', render: (r) => (r.last_evaluated_at ? html`<${Ago} ts=${r.last_evaluated_at} />` : '–'), sort: (r) => r.last_evaluated_at || '' },
      { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (r) => html`<${MenuButton} label=${`Actions for ${r.name}`} items=${[
        { label: 'Test now', icon: 'flask', onClick: () => test(r) },
        canManage && { label: 'Edit', icon: 'edit', onClick: () => edit(r) },
        canManage && { label: r.enabled ? 'Disable' : 'Enable', icon: r.enabled ? 'stop' : 'check', onClick: () => toggle(r) },
        canManage && { label: 'Silence for 1 hour', icon: 'pause', onClick: () => silence(r, 60) },
        canManage && r.silenced_until && { label: 'Lift the silence', icon: 'play', onClick: () => silence(r, 0) },
        canManage && !r.builtin && { sep: true }, canManage && !r.builtin && { label: 'Delete', icon: 'trash', danger: true, onClick: () => del(r) }]} />` },
    ]} /></div>`;
}

// ---------------------------------------------------------------------------------------------------------------------------------------- channels
function ChannelDialog({ close }) {
  const [name, setName] = useState('');
  const [type, setType] = useState('webhook');
  const [url, setUrl] = useState('');
  const [to, setTo] = useState('');
  const [secret, setSecret] = useState('');
  const [busy, setBusy] = useState(false);
  async function save() {
    const config = type === 'webhook' ? { url: url.trim() } : type === 'email' ? { to: to.split(',').map((x) => x.trim()).filter(Boolean) } : {};
    setBusy(true);
    const r = await act(() => api.post('/alerts/channels', { name: name.trim(), type, config, ...(secret ? { secret } : {}) }), { ok: 'Channel created' });
    setBusy(false);
    if (r) close(r);
  }
  const ok = name.trim() && (type !== 'webhook' || url.trim()) && (type !== 'email' || to.trim());
  return html`<${Dialog} title="New notification channel" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!ok} onClick=${save}>Create channel<//>`}>
    <div class="col gap-3"><${Input} label="Name" value=${name} onInput=${setName} required />
      <${Select} label="Type" value=${type} onChange=${setType} options=${[{ value: 'webhook', label: 'Webhook (JSON POST)' }, { value: 'email', label: 'Email' }, { value: 'log', label: 'Server log only' }]} />
      ${type === 'webhook' ? html`<${Input} label="URL" mono value=${url} onInput=${setUrl} placeholder="https://hooks.example.com/alerts" /><${Input} label="Signing secret (optional)" type="password" autocomplete="off" value=${secret} onInput=${setSecret} hint="Stored encrypted and never shown again." />` : null}
      ${type === 'email' ? html`<${Input} label="Recipients" value=${to} onInput=${setTo} placeholder="oncall@example.com, data@example.com" hint="Comma separated. Needs SMTP configured under Administration." />` : null}</div></${Dialog}>`;
}
function ChannelsTab() {
  const ch = usePoll(() => api.get('/alerts/channels').then((r) => r.items), 20000, []);
  const canManage = can('alert.manage');
  async function create() { if (await openOverlay(({ close }) => html`<${ChannelDialog} close=${close} />`)) ch.reload(); }
  async function test(c) { const r = await act(() => api.post(`/alerts/channels/${enc(c.id)}/test`)); if (r) toast[r.ok === false ? 'bad' : 'ok'](r.ok === false ? 'The channel did not accept the test' : 'Test notification sent', r.detail || r.error || ''); }
  async function toggle(c) { if (await act(() => api.patch(`/alerts/channels/${enc(c.id)}`, { enabled: !c.enabled }), { ok: c.enabled ? 'Channel disabled' : 'Channel enabled' })) ch.reload(); }
  async function del(c) { if (await confirmDialog({ title: `Delete ${c.name}?`, danger: true, confirmLabel: 'Delete channel', body: 'Rules that use it stop notifying through it.' }) && (await act(() => api.del(`/alerts/channels/${enc(c.id)}`), { ok: 'Channel deleted' })) !== undefined) ch.reload(); }
  if (ch.loading && !ch.data) return html`<${Loading} />`;
  if (ch.error && !ch.data) return html`<${ErrorBox} error=${ch.error} onRetry=${ch.reloadLoud} />`;
  return html`<div data-testid="channels"><div class="toolbar"><span class="muted small">${ch.data.length} channel${ch.data.length === 1 ? '' : 's'}</span><span class="grow"></span>${canManage ? html`<${Btn} kind="primary" icon="plus" onClick=${create}>New channel<//>` : null}</div>
    <${Table} caption="Notification channels" rows=${ch.data} empty=${html`<${Empty} icon="send" title="No channels yet" action=${canManage ? html`<${Btn} kind="primary" icon="plus" onClick=${create}>New channel<//>` : null}>Channels deliver alert notifications to a webhook, an email address or the server log.<//>`} columns=${[
      { key: 'name', label: 'Channel', render: (c) => html`<b>${c.name}</b>` },
      { key: 'type', label: 'Type', render: (c) => titleCase(c.type) },
      { key: 'to', label: 'Destination', render: (c) => html`<span class="mono small">${c.config.url || (c.config.to || []).join(', ') || '–'}</span>` },
      { key: 'on', label: 'State', render: (c) => (c.enabled ? html`<${Badge} kind="ok">Enabled<//>` : html`<${Badge}>Disabled<//>`) },
      { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (c) => html`<${MenuButton} label=${`Actions for ${c.name}`} items=${[{ label: 'Send a test', icon: 'send', onClick: () => test(c) }, canManage && { label: c.enabled ? 'Disable' : 'Enable', icon: c.enabled ? 'stop' : 'check', onClick: () => toggle(c) }, canManage && { sep: true }, canManage && { label: 'Delete', icon: 'trash', danger: true, onClick: () => del(c) }]} />` },
    ]} /></div>`;
}

// ---------------------------------------------------------------------------------------------------------------------------------------- webhooks
function SecretDialog({ hook, secret, close }) {
  return html`<${Dialog} title=${`Signing secret for ${hook.name}`} onClose=${() => close()} footer=${html`<${Btn} kind="primary" onClick=${() => close()}>I have saved it<//>`}>
    <${Banner} kind="warn" title="Copy the secret now">It is shown once. Receivers verify each delivery with <span class="mono">HMAC_SHA256(secret, timestamp + "." + body)</span> from the X-OLCF-Signature header.<//>
    <div class="mt-3"><${Code} text=${secret} /></div></${Dialog}>`;
}
function WebhookDialog({ hook, events, close }) {
  const [name, setName] = useState(hook ? hook.name : '');
  const [url, setUrl] = useState(hook ? hook.url : '');
  const [sel, setSel] = useState(() => new Set(hook ? hook.event_types : ['pipeline.run.failed']));
  const [custom, setCustom] = useState('');
  const [desc, setDesc] = useState(hook ? hook.description || '' : '');
  const [busy, setBusy] = useState(false);
  const toggle = (t) => setSel((s) => { const n = new Set(s); if (n.has(t)) n.delete(t); else n.add(t); return n; });
  async function save() {
    const types = [...sel, ...custom.split(',').map((x) => x.trim()).filter(Boolean)];
    const body = { name: name.trim(), url: url.trim(), event_types: types, description: desc };
    setBusy(true);
    const r = await act(() => (hook ? api.patch(`/webhooks/${enc(hook.id)}`, body, { ifMatch: hook.etag }) : api.post('/webhooks', body)), { ok: hook ? 'Webhook saved' : 'Webhook created' });
    setBusy(false);
    if (r) close(r);
  }
  const catalog = events.filter((e) => !e.type.startsWith('webhook.'));
  return html`<${Dialog} title=${hook ? `Edit ${hook.name}` : 'New webhook'} size="wide" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!name.trim() || !url.trim() || (!sel.size && !custom.trim())} onClick=${save}>${hook ? 'Save webhook' : 'Create webhook'}<//>`}>
    <div class="col gap-3"><div class="grid c2"><${Input} label="Name" value=${name} onInput=${setName} required /><${Input} label="URL" mono value=${url} onInput=${setUrl} placeholder="https://hooks.example.com/olcf" required hint="Private network addresses must be allow-listed by an administrator." /></div>
      <fieldset class="fieldset"><legend>Send these events</legend><div class="evgrid">${catalog.map((e) => html`<${Check} key=${e.type} label=${html`<span class="mono small">${e.type}</span>`} title=${e.description} checked=${sel.has(e.type)} onChange=${() => toggle(e.type)} />`)}</div></fieldset>
      <${Input} label="Other patterns" mono value=${custom} onInput=${setCustom} placeholder="job.*, pipeline.run.*" hint="Comma separated. * matches any text." />
      <${Textarea} label="Description" rows=${2} value=${desc} onInput=${setDesc} /></div></${Dialog}>`;
}
function DeliveriesDrawer({ hook, close }) {
  const d = usePoll(() => api.get(`/webhooks/${enc(hook.id)}/deliveries`, { limit: 100 }).then((r) => r.items), 5000, [hook.id]);
  async function open(x) { await openOverlay(({ close: c }) => html`<${DeliveryDialog} id=${x.id} close=${c} />`); d.reload(); }
  return html`<${Drawer} title=${`Deliveries: ${hook.name}`} wide onClose=${() => close()}>${d.loading && !d.data ? html`<${Loading} />` : d.error && !d.data ? html`<${ErrorBox} error=${d.error} />` : html`
    <${Table} caption="Webhook deliveries" rows=${d.data} onRowClick=${open} empty=${html`<${Empty} icon="send" title="No deliveries yet">Send a test to see one.<//>`} columns=${[
      { key: 'st', label: 'State', render: (x) => html`<${Badge} kind=${x.state === 'succeeded' ? 'ok' : x.state === 'dead' ? 'bad' : 'warn'}>${titleCase(x.state)}<//>` },
      { key: 'ev', label: 'Event', render: (x) => html`<span class="mono small">${x.event_type}</span>${x.is_test ? html` <${Badge}>Test<//>` : ''}` },
      { key: 'code', label: 'Response', render: (x) => (x.last_status_code != null ? x.last_status_code : html`<span class="small muted">${x.last_error ? 'error' : '–'}</span>`) },
      { key: 'at', label: 'Attempts', num: true, render: (x) => x.attempts },
      { key: 'when', label: 'Created', render: (x) => html`<${Ago} ts=${x.created_at} />` },
    ]} />`}</${Drawer}>`;
}
function DeliveryDialog({ id, close }) {
  const r = useAsync(() => api.get(`/webhooks/deliveries/${enc(id)}`), [id]);
  const x = r.data;
  async function again() { if (await act(() => api.post(`/webhooks/deliveries/${enc(id)}/redeliver`), { ok: 'Delivery queued again' })) r.reloadLoud(); }
  return html`<${Dialog} title="Delivery" size="wide" onClose=${() => close()} footer=${html`${x && x.state !== 'pending' && x.state !== 'delivering' ? html`<${Btn} icon="refresh" onClick=${again}>Redeliver<//>` : null}<${Btn} kind="primary" onClick=${() => close()}>Close<//>`}>
    ${r.loading ? html`<${Loading} />` : r.error ? html`<${ErrorBox} error=${r.error} />` : html`
      ${x.last_error ? html`<${Banner} kind="bad" title="Last attempt failed">${x.last_error}<//>` : null}
      <${Kv} items=${[['Event', x.event_type], ['State', titleCase(x.state)], ['Attempts', x.attempts], ['Response', x.last_status_code], ['Latency', x.response_ms != null ? x.response_ms + ' ms' : null], ['Delivered', x.delivered_at ? fmtTime(x.delivered_at) : null]]} />
      <h4 class="mt-4">Attempts</h4><${Table} dense caption="Attempts" rows=${(x.attempt_log || []).map((a, i) => ({ ...a, id: i }))} columns=${[{ key: 'a', label: '#', render: (a) => a.attempt }, { key: 't', label: 'When', render: (a) => fmtTime(a.at) }, { key: 's', label: 'Status', render: (a) => a.status ?? html`<span class="small muted">${a.error || '–'}</span>` }, { key: 'ms', label: 'Time', render: (a) => a.ms + ' ms' }]} />
      <h4 class="mt-4">Payload</h4><${JsonView} value=${x.payload || {}} depth=${3} />`}</${Dialog}>`;
}
function WebhooksTab() {
  const hooks = usePoll(() => api.get('/webhooks').then((r) => r.items), 15000, []);
  const ev = useAsync(() => api.get('/webhooks/event-types'), []);
  const canManage = can('webhook.manage');
  async function create() {
    const r = await openOverlay(({ close }) => html`<${WebhookDialog} events=${(ev.data && ev.data.items) || []} close=${close} />`);
    if (!r) return;
    hooks.reload();
    if (r.secret) await openOverlay(({ close }) => html`<${SecretDialog} hook=${r} secret=${r.secret} close=${close} />`);
  }
  async function edit(h) { if (await openOverlay(({ close }) => html`<${WebhookDialog} hook=${h} events=${(ev.data && ev.data.items) || []} close=${close} />`)) hooks.reload(); }
  async function test(h) {
    const r = await act(() => api.post(`/webhooks/${enc(h.id)}/test`));
    if (!r) return;
    toast[r.state === 'succeeded' ? 'ok' : 'bad'](r.state === 'succeeded' ? `Test delivered (${r.last_status_code})` : 'The test delivery failed', r.last_error || '');
    hooks.reload();
  }
  async function rotate(h) {
    if (!(await confirmDialog({ title: `Rotate the secret of ${h.name}?`, danger: true, confirmLabel: 'Rotate', body: 'Receivers must switch to the new secret straight away.' }))) return;
    const r = await act(() => api.post(`/webhooks/${enc(h.id)}/rotate-secret`, {}));
    if (r && r.secret) await openOverlay(({ close }) => html`<${SecretDialog} hook=${h} secret=${r.secret} close=${close} />`);
  }
  async function toggle(h) { if (await act(() => api.patch(`/webhooks/${enc(h.id)}`, { enabled: !h.enabled }, { ifMatch: h.etag }), { ok: h.enabled ? 'Webhook disabled' : 'Webhook enabled' })) hooks.reload(); }
  async function del(h) { if (await confirmDialog({ title: `Delete ${h.name}?`, danger: true, confirmLabel: 'Delete webhook', body: 'Its delivery history is deleted too.' }) && (await act(() => api.del(`/webhooks/${enc(h.id)}`), { ok: 'Webhook deleted' })) !== undefined) hooks.reload(); }
  if (hooks.loading && !hooks.data) return html`<${Loading} />`;
  if (hooks.error && !hooks.data) return html`<${ErrorBox} error=${hooks.error} onRetry=${hooks.reloadLoud} />`;
  return html`<div data-testid="webhooks"><div class="toolbar"><span class="muted small">Webhooks send platform events (job, pipeline, security, fabric) to your systems, signed and retried.</span><span class="grow"></span>${canManage ? html`<${Btn} kind="primary" icon="plus" onClick=${create}>New webhook<//>` : null}</div>
    <${Table} caption="Webhooks" rows=${hooks.data} empty=${html`<${Empty} icon="webhook" title="No webhooks yet" action=${canManage ? html`<${Btn} kind="primary" icon="plus" onClick=${create}>New webhook<//>` : null}>Create one to be told when a pipeline fails or a job finishes.<//>`} columns=${[
      { key: 'name', label: 'Webhook', render: (h) => html`<b>${h.name}</b><div class="small muted mono">${h.url}</div>` },
      { key: 'ev', label: 'Events', render: (h) => html`<span class="small">${h.event_types.slice(0, 3).join(', ')}${h.event_types.length > 3 ? ` +${h.event_types.length - 3}` : ''}</span>`, sortable: false },
      { key: 'st', label: 'State', render: (h) => (h.disabled_reason ? html`<${Badge} kind="bad" title=${h.disabled_reason}>Disabled by platform<//>` : !h.enabled ? html`<${Badge}>Disabled<//>` : h.consecutive_failures ? html`<${Badge} kind="warn">${h.consecutive_failures} failing<//>` : html`<${Badge} kind="ok">Healthy<//>`), sortable: false },
      { key: 'last', label: 'Last delivery', render: (h) => (h.last_delivery_at ? html`<${Ago} ts=${h.last_delivery_at} /> <span class="small muted">${h.last_status || ''}</span>` : html`<span class="muted">never</span>`), sortable: false },
      { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (h) => html`<${MenuButton} label=${`Actions for ${h.name}`} items=${[
        { label: 'Deliveries', icon: 'list', onClick: () => openOverlay(({ close }) => html`<${DeliveriesDrawer} hook=${h} close=${close} />`) },
        { label: 'Send a test event', icon: 'send', onClick: () => test(h) },
        canManage && { label: 'Edit', icon: 'edit', onClick: () => edit(h) },
        canManage && { label: 'Rotate secret', icon: 'key', onClick: () => rotate(h) },
        canManage && { label: h.enabled ? 'Disable' : 'Enable', icon: h.enabled ? 'stop' : 'check', onClick: () => toggle(h) },
        canManage && { sep: true }, canManage && { label: 'Delete', icon: 'trash', danger: true, onClick: () => del(h) }]} />` },
    ]} /></div>`;
}

export function AlertsPage() {
  const [tab, setTab] = useQueryState('tab', 'alerts');
  const open = usePoll(() => api.get('/alerts', { state: 'firing', limit: 100 }).then((r) => r.items.length).catch(() => undefined), 15000, []);
  return html`<div class="page wide"><${PageHead} title="Alerts and webhooks" icon="bell" sub="Know when jobs, pipelines, nodes or credentials need attention, and send platform events to the systems that care." />
    <${Tabs} value=${tab} onChange=${setTab} tabs=${[{ id: 'alerts', label: 'Alerts', count: open.data || undefined }, { id: 'rules', label: 'Rules' }, { id: 'channels', label: 'Channels' }, { id: 'webhooks', label: 'Webhooks' }]} />
    <div class="mt-4"><${TabPanel} id="alerts" value=${tab}><${AlertsTab} /><//><${TabPanel} id="rules" value=${tab}><${RulesTab} /><//><${TabPanel} id="channels" value=${tab}><${ChannelsTab} /><//><${TabPanel} id="webhooks" value=${tab}><${WebhooksTab} /><//></div></div>`;
}
