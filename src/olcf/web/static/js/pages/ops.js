// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Administration: platform health, settings, backup and restore, updates, diagnostics with support bundles, and data retention.
import { html, useState, useEffect, useMemo, useRef, useAsync, usePoll } from '../core/h.js';
import { api, can, describeError } from '../api/client.js';
import { Btn, Badge, Banner, Empty, ErrorBox, Loading, Tabs, TabPanel, PageHead, Select, Input, Textarea, Check, Switch, Code, Kv, Card, StatusBadge, Light, Progress } from '../ui/basic.js';
import { Table, JsonView } from '../ui/data.js';
import { Icon } from '../ui/icons.js';
import { Dialog, MenuButton, openOverlay, confirmDialog, promptDialog, act, toast } from '../ui/overlay.js';
import { enc, fmtTime, fmtBytes, fmtDuration, fmtMs, titleCase, shortId, jsonPretty, downloadBlob, groupBy, plural } from '../core/util.js';
import { useQueryState, Ago, openHealthTest, openOperation } from './common.js';

const lightOf = (s) => (s === 'ok' ? 'ok' : s === 'degraded' || s === 'warn' ? 'degraded' : s === 'down' || s === 'fail' ? 'down' : 'unknown');

// ----------------------------------------------------------------------------------------------------------------------------------- health
function HealthTab() {
  const h = usePoll(() => api.get('/admin/health'), 15000, []);
  const checks = useAsync(() => api.get('/admin/health/checks').then((r) => r.items), []);
  const info = useAsync(() => api.get('/admin/info'), []);
  const ports = useAsync(() => api.text('/admin/ports').catch(() => ''), []);
  const deep = new Set((checks.data || []).filter((c) => c.deep).map((c) => c.name));
  const comps = (h.data && h.data.components) || [];
  const groups = useMemo(() => groupBy(comps, (c) => c.category || 'other'), [comps]);
  const i = info.data;
  return html`<div data-testid="health" class="col gap-4">
    ${h.loading && !h.data ? html`<${Loading} />` : h.error && !h.data ? html`<${ErrorBox} error=${h.error} onRetry=${h.reloadLoud} />` : html`
    <${Banner} kind=${h.data.status === 'ok' ? 'ok' : h.data.status === 'degraded' ? 'warn' : 'bad'} title=${h.data.status === 'ok' ? 'Every component is healthy' : h.data.status === 'degraded' ? 'The platform works, but something needs attention' : 'A critical component is down'}
      actions=${html`<${Btn} size="sm" icon="refresh" onClick=${() => h.reloadLoud()}>Refresh<//>`}>Checked ${fmtTime(h.data.checked_at)}. ${comps.filter((c) => c.status !== 'ok').length} of ${comps.length} components need a look.<//>
    ${Object.entries(groups).map(([cat, list]) => html`<section key=${cat} aria-label=${titleCase(cat)}><h2 class="h4 mb-2">${titleCase(cat)}</h2>
      <div class="diag-grid">${list.map((c) => html`<div class="diag-item" key=${c.name}><div class="name"><${Light} state=${lightOf(c.status)} label=${titleCase(c.status)} /><span class="grow">${c.name}</span>
        ${deep.has(c.name) ? html`<${Btn} size="sm" icon="activity" onClick=${() => openHealthTest(c.name)} aria-label=${`Run deep test for ${c.name}`}>Test<//>` : null}</div>
        <div class="detail">${c.detail}${c.latency_ms != null ? ` · ${c.latency_ms} ms` : ''}</div></div>`)}</div></section>`)}`}
    <div class="grid-2"><${Card} title="This installation">${i ? html`<${Kv} items=${[['Product', i.product], ['Version', html`<span class="mono">${i.version}</span>`], ['Author', i.author], ['Node', i.node], ['Leader', i.leader ? 'Yes' : 'No'], ['Uptime', fmtDuration(i.uptime_seconds)], ['Python', i.python],
        ['Database', `${i.database.dialect}, ${Object.keys(i.database.migrations).length} schema groups`], ['Address', html`<span class="mono">${i.base_url}</span>`]]} />` : html`<${Loading} />`}</${Card}>
      <${Card} title="Network ports" sub="What must be reachable between nodes">${ports.data ? html`<pre class="pre small">${ports.data}</pre>` : html`<${Loading} />`}
        <div class="row mt-3"><a class="btn" href="/v1/docs" target="_blank" rel="noopener" data-testid="api-docs"><${Icon} name="external" size="sm" /> API reference</a><a class="btn" href="/v1/openapi.json" target="_blank" rel="noopener">OpenAPI document</a></div></${Card}></div>
    ${i ? html`<${Card} title="Modules"><span class="chips-static">${i.modules.map((m) => html`<span key=${m} class="tag">${m}</span>`)}</span></${Card}>` : null}</div>`;
}

// ----------------------------------------------------------------------------------------------------------------------------------- settings
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
function SettingEditor({ k, v, draft, setDraft }) {
  const t = Array.isArray(v.default) || Array.isArray(v.value) ? 'list' : typeof v.default === 'boolean' || typeof v.value === 'boolean' ? 'bool' : typeof v.default === 'number' || typeof v.value === 'number' ? 'num' : 'str';
  const cur = draft !== undefined ? draft : v.value;
  if (t === 'bool') return html`<${Switch} label=${cur ? 'On' : 'Off'} checked=${!!cur} onChange=${setDraft} aria-label=${k} />`;
  if (t === 'num') return html`<${Input} type="number" step="any" value=${cur ?? ''} onInput=${(x) => setDraft(x === '' ? null : Number(x))} aria-label=${k} />`;
  if (t === 'list') return html`<${Textarea} rows=${Math.min(4, Math.max(1, (cur || []).length))} mono value=${(cur || []).join('\n')} onInput=${(x) => setDraft(x.split('\n').map((s) => s.trim()).filter(Boolean))} aria-label=${k} placeholder="One entry per line" />`;
  return html`<${Input} value=${cur ?? ''} onInput=${setDraft} aria-label=${k} />`;
}
function SettingsTab() {
  const st = useAsync(() => api.get('/admin/settings').then((r) => r.settings), []);
  const [q, setQ] = useState('');
  const [drafts, setDrafts] = useState({});
  const canEdit = can('admin.settings');
  const entries = useMemo(() => Object.entries(st.data || {}).filter(([k, v]) => !q || k.includes(q.toLowerCase()) || (v.description || '').toLowerCase().includes(q.toLowerCase())), [st.data, q]);
  const groups = useMemo(() => groupBy(entries, ([k]) => k.split('.')[0]), [entries]);
  const dirty = Object.keys(drafts).filter((k) => st.data && !same(drafts[k], st.data[k].value));
  async function save(k) {
    if (await act(() => api.put(`/admin/settings/${enc(k)}`, { value: drafts[k] }), { ok: `Saved ${k}` })) { setDrafts((d) => { const n = { ...d }; delete n[k]; return n; }); st.reload(); }
  }
  async function reset(k) {
    if (await act(() => api.put(`/admin/settings/${enc(k)}`, { value: st.data[k].default }), { ok: `${k} restored to its default` })) { setDrafts((d) => { const n = { ...d }; delete n[k]; return n; }); st.reload(); }
  }
  if (st.loading && !st.data) return html`<${Loading} />`;
  if (st.error && !st.data) return html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />`;
  return html`<div data-testid="settings"><div class="toolbar"><${Input} label="Find a setting" size="sm" type="search" value=${q} onInput=${setQ} placeholder="e.g. smtp, retention, timeout" data-testid="settings-search" />
    <span class="muted small grow">${plural(entries.length, 'setting')}${dirty.length ? ` · ${dirty.length} unsaved` : ''}. Changes apply immediately and are written to the audit log.</span></div>
    ${!canEdit ? html`<${Banner} kind="info" title="Read only">Changing settings needs the platform administrator role.<//>` : null}
    ${Object.entries(groups).map(([g, list]) => html`<${Card} key=${g} title=${titleCase(g)} class="set-group">${list.map(([k, v]) => {
    const isDirty = drafts[k] !== undefined && !same(drafts[k], v.value);
    return html`<div key=${k} class=${'set-row' + (!same(v.value, v.default) ? ' changed' : '')}><div><div class="key">${k}</div><div class="desc">${v.description}</div></div>
      <div>${canEdit ? html`<${SettingEditor} k=${k} v=${v} draft=${drafts[k]} setDraft=${(x) => setDrafts((d) => ({ ...d, [k]: x }))} />` : html`<span class="mono small">${JSON.stringify(v.value)}</span>`}</div>
      <div class="row">${canEdit && isDirty ? html`<${Btn} size="sm" kind="primary" onClick=${() => save(k)} aria-label=${`Save ${k}`}>Save<//>` : null}${canEdit && !isDirty && !same(v.value, v.default) ? html`<${Btn} size="sm" kind="ghost" onClick=${() => reset(k)} aria-label=${`Restore default of ${k}`} title=${`Default: ${JSON.stringify(v.default)}`}>Reset<//>` : null}</div></div>`;
  })}</${Card}>`)}
    ${!entries.length ? html`<${Empty} icon="search" title="No settings match">Try a shorter search.<//>` : null}</div>`;
}

// ----------------------------------------------------------------------------------------------------------------------------------- backup
function BackupDialog({ cfg, close }) {
  const [incKeys, setIncKeys] = useState(false);
  const [pass, setPass] = useState('');
  const [busy, setBusy] = useState(false);
  async function go() {
    setBusy(true);
    const body = { ...(incKeys ? { include_keys: true } : {}), ...(pass ? { passphrase: pass } : {}) };
    const r = await act(() => api.post('/admin/backup', body), {});
    setBusy(false);
    if (r) close(r);
  }
  const needPass = incKeys && !pass && !(cfg && cfg.passphrase_configured);
  return html`<${Dialog} title="Back up now" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${needPass} onClick=${go} data-testid="backup-start">Start backup<//>`}>
    <div class="col gap-3"><p class="small">The backup contains the database and settings. It is encrypted and copied to every configured target.</p>
      <${Check} label="Include the CA and signing keys" checked=${incKeys} onChange=${setIncKeys} />
      ${incKeys ? html`<${Input} label="Passphrase" type="password" autocomplete="new-password" value=${pass} onInput=${setPass} hint=${cfg && cfg.passphrase_configured ? 'Leave empty to use the configured backup passphrase.' : 'Key material is never written without a passphrase.'} />` : null}</div></${Dialog}>`;
}
function ConfigDialog({ cfg, targets, close }) {
  const [sch, setSch] = useState(cfg.schedule.enabled);
  const [cron, setCron] = useState(cfg.schedule.cron);
  const [keep, setKeep] = useState(String(cfg.retention.keep_last));
  const [days, setDays] = useState(String(cfg.retention.keep_days));
  const [rt, setRt] = useState(cfg.restore_test.enabled);
  const [rtc, setRtc] = useState(cfg.restore_test.cron);
  const [sel, setSel] = useState(new Set(cfg.targets));
  const [pass, setPass] = useState('');
  const [busy, setBusy] = useState(false);
  async function save() {
    setBusy(true);
    const body = { schedule: { enabled: sch, cron }, retention: { keep_last: Number(keep), keep_days: Number(days) }, restore_test: { enabled: rt, cron: rtc }, targets: [...sel], ...(pass ? { passphrase: pass } : {}) };
    const r = await act(() => api.put('/admin/backup/config', body), { ok: 'Backup settings saved' });
    setBusy(false);
    if (r) close(r);
  }
  return html`<${Dialog} size="wide" title="Backup schedule and retention" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} onClick=${save} data-testid="backup-config-save">Save settings<//>`}>
    <div class="col gap-3"><div class="grid-2"><div class="col gap-2"><${Switch} label="Back up on a schedule" checked=${sch} onChange=${setSch} /><${Input} label="Cron (server time, UTC)" mono value=${cron} onInput=${setCron} /></div>
      <div class="col gap-2"><${Switch} label="Prove restorability on a schedule" checked=${rt} onChange=${setRt} /><${Input} label="Cron" mono value=${rtc} onInput=${setRtc} hint="Restores the newest backup into a throw-away database." /></div></div>
      <div class="grid-2"><${Input} label="Keep at least this many" type="number" min="1" value=${keep} onInput=${setKeep} /><${Input} label="Delete after (days)" type="number" min="1" value=${days} onInput=${setDays} /></div>
      <fieldset class="fieldset"><legend>Copy backups to</legend>${targets.length ? html`<div class="col gap-2">${targets.map((t) => html`<${Check} key=${t.name} label=${`${t.name} (${t.type})`} checked=${sel.has(t.name)} onChange=${(v) => setSel((s) => { const n = new Set(s); if (v) n.add(t.name); else n.delete(t.name); return n; })} />`)}</div>` : html`<span class="muted small">No targets yet. Backups stay on this server until you add one.</span>`}</fieldset>
      <${Input} label=${cfg.passphrase_configured ? 'Backup passphrase (set; type to replace)' : 'Backup passphrase (optional)'} type="password" autocomplete="new-password" value=${pass} onInput=${setPass} hint="Write-only. Without one, backups are encrypted with the platform master key." /></div></${Dialog}>`;
}
const TARGET_HELP = {
  local: [{ path: '/mnt/nas/olcf-backups' }, null],
  s3: [{ bucket: 'my-backups', prefix: 'olcf/', region: 'eu-west-1' }, { access_key_id: '', secret_access_key: '' }],
  sftp: [{ host: 'backup.example.com', port: 22, username: 'olcf', path: '/backups', host_key_sha256: '' }, { password: '' }],
  lakehouse: [{ lakehouse: 'workspace/lakehouse', prefix: 'Files/olcf-backups' }, null],
};
function TargetDialog({ types, close }) {
  const [type, setType] = useState(types[0] || 'local');
  const [name, setName] = useState('');
  const [config, setConfig] = useState(jsonPretty(TARGET_HELP[types[0] || 'local'][0]));
  const [secret, setSecret] = useState('');
  const [err, setErr] = useState(null);
  const [busy, setBusy] = useState(false);
  function pick(t) { setType(t); setConfig(jsonPretty(TARGET_HELP[t][0])); setSecret(TARGET_HELP[t][1] ? jsonPretty(TARGET_HELP[t][1]) : ''); }
  async function save() {
    let c; let s;
    try { c = JSON.parse(config); } catch (e) { setErr(`Settings are not valid JSON: ${e.message}`); return; }
    if (secret.trim()) { try { s = JSON.parse(secret); } catch (e) { setErr(`Credentials are not valid JSON: ${e.message}`); return; } }
    setBusy(true); setErr(null);
    try { close(await api.post('/admin/backup/targets', { name: name.trim(), type, config: c, ...(s ? { secret: s } : {}) })); toast.ok('Target added'); } catch (e) { setErr(describeError(e)); } finally { setBusy(false); }
  }
  return html`<${Dialog} title="New backup target" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!name.trim()} onClick=${save}>Add target<//>`}>
    <div class="col gap-3">${err ? html`<${Banner} kind="bad" title="The target was not added">${err}<//>` : null}
      <${Select} label="Type" value=${type} onChange=${pick} options=${types.map((t) => ({ value: t, label: ({ local: 'Directory on a server', s3: 'S3 or MinIO', sftp: 'SFTP', lakehouse: 'A lakehouse' })[t] || t }))} />
      <${Input} label="Name" value=${name} onInput=${setName} hint="Letters, digits, . _ -" required />
      <${Textarea} label="Settings (JSON)" mono rows=${6} value=${config} onInput=${setConfig} />
      ${TARGET_HELP[type][1] ? html`<${Textarea} label="Credentials (JSON, stored encrypted)" mono rows=${4} value=${secret} onInput=${setSecret} hint="Never shown again." autocomplete="off" />` : null}</div></${Dialog}>`;
}
function RestoreDialog({ b, close }) {
  const pre = useAsync(() => api.get(`/admin/backup/${enc(b.id)}/prerequisites`), [b.id]);
  const [word, setWord] = useState('');
  const [pass, setPass] = useState('');
  const [keys, setKeys] = useState(false);
  const [busy, setBusy] = useState(false);
  async function go() {
    setBusy(true);
    const r = await act(() => api.post(`/admin/backup/${enc(b.id)}/restore`, { confirm: 'RESTORE', ...(pass ? { passphrase: pass } : {}), restore_keys: keys }), {});
    setBusy(false);
    if (r) close(r);
  }
  return html`<${Dialog} size="wide" title=${`Restore ${b.filename}`} onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="danger" busy=${busy} disabled=${word !== 'RESTORE'} onClick=${go} data-testid="restore-confirm">Restore this backup<//>`}>
    <div class="col gap-3"><${Banner} kind="warn" title="This replaces the live database">Everything created after ${fmtTime(b.created_at)} is lost. Take a fresh backup first if you may need it. Running jobs and sessions are interrupted.<//>
      ${pre.loading ? html`<${Loading} />` : pre.error ? html`<${ErrorBox} error=${pre.error} />` : html`<details><summary class="small">What the restore needs</summary><pre class="pre small mt-2">${jsonPretty(pre.data)}</pre></details>`}
      <${Input} label="Passphrase (if one was used)" type="password" autocomplete="off" value=${pass} onInput=${setPass} />
      ${b.includes && b.includes.keys ? html`<${Check} label="Also restore the CA and signing keys (overwrites the keys on disk)" checked=${keys} onChange=${setKeys} />` : null}
      <${Input} label="Type RESTORE to confirm" value=${word} onInput=${setWord} autocomplete="off" data-testid="restore-word" /></div></${Dialog}>`;
}
function BackupTab() {
  const list = useAsync(() => api.get('/admin/backup'), []);
  const cfg = useAsync(() => api.get('/admin/backup/config'), []);
  const tg = useAsync(() => api.get('/admin/backup/targets'), []);
  const canB = can('admin.backup');
  const items = (list.data && list.data.items) || [];
  const hl = list.data && list.data.health;
  const refresh = () => { list.reload(); cfg.reload(); tg.reload(); };
  async function start() {
    const r = await openOverlay(({ close }) => html`<${BackupDialog} cfg=${cfg.data} close=${close} />`);
    if (r) { await openOperation(r.operation_id, 'Backing up'); refresh(); }
  }
  async function op(b, verb, title) {
    const r = await act(() => api.post(`/admin/backup/${enc(b.id)}/${verb}`, {}), {});
    if (r) { const o = await openOperation(r.operation_id, title); if (o && o.state === 'succeeded') toast.ok(`${title} finished`); refresh(); }
  }
  async function download(b) { const blob = await act(() => api.blob(`/admin/backup/${enc(b.id)}/download`), {}); if (blob) downloadBlob(blob, b.filename); }
  async function del(b) { if (await confirmDialog({ title: 'Delete this backup?', danger: true, confirmLabel: 'Delete backup', body: `${b.filename} is removed from this server and every target.` }) && (await act(() => api.del(`/admin/backup/${enc(b.id)}`), { ok: 'Backup deleted' })) !== undefined) refresh(); }
  async function restore(b) {
    const r = await openOverlay(({ close }) => html`<${RestoreDialog} b=${b} close=${close} />`);
    if (r) { await openOperation(r.operation_id, 'Restoring'); refresh(); }
  }
  async function editCfg() { if (await openOverlay(({ close }) => html`<${ConfigDialog} cfg=${cfg.data} targets=${(tg.data && tg.data.items) || []} close=${close} />`)) refresh(); }
  async function addTarget() { if (await openOverlay(({ close }) => html`<${TargetDialog} types=${(tg.data && tg.data.types) || ['local']} close=${close} />`)) refresh(); }
  async function testTarget(t) { const r = await act(() => api.post(`/admin/backup/targets/${enc(t.name)}/test`, {})); if (r) toast[r.ok === false ? 'bad' : 'ok'](r.ok === false ? 'The target failed its test' : 'The target accepted a test file', r.detail || r.error || ''); }
  async function delTarget(t) { if (await confirmDialog({ title: `Remove ${t.name}?`, danger: true, confirmLabel: 'Remove target', body: 'Files already copied there are not deleted.' }) && (await act(() => api.del(`/admin/backup/targets/${enc(t.name)}`), { ok: 'Target removed' })) !== undefined) refresh(); }
  if (list.loading && !list.data) return html`<${Loading} />`;
  if (list.error && !list.data) return html`<${ErrorBox} error=${list.error} onRetry=${list.reloadLoud} />`;
  const c = cfg.data;
  return html`<div data-testid="backup" class="col gap-4">
    <div class="grid-2"><${Card} title="Protection" actions=${canB ? html`<${Btn} kind="primary" icon="save" onClick=${start} data-testid="backup-now">Back up now<//>` : null}>
        ${hl && hl.last_success ? html`<div class="row"><${Badge} kind=${hl.last_failed ? 'warn' : 'ok'}>${hl.last_failed ? 'Last backup failed' : 'Protected'}<//><span class="small">Last good backup <${Ago} ts=${hl.last_success} /></span></div>` : html`<${Banner} kind="warn" title="No backup yet">Take a backup now, then schedule one so a lost server does not mean lost work.<//>`}
        ${hl && hl.last_restore_test ? html`<p class="small muted mt-2">Last restore test: ${fmtTime(hl.last_restore_test.at || hl.last_restore_test)}</p>` : html`<p class="small muted mt-2">A backup you have not restored is only a hope. Run a restore test from the menu of any backup.</p>`}</${Card}>
      <${Card} title="Schedule" actions=${canB && c ? html`<${Btn} icon="settings" onClick=${editCfg} data-testid="backup-config">Change<//>` : null}>
        ${c ? html`<${Kv} items=${[['Backups', c.schedule.enabled ? html`<${Badge} kind="ok">On<//> <span class="mono small">${c.schedule.cron}</span>` : html`<${Badge}>Off<//>`], ['Restore test', c.restore_test.enabled ? html`<${Badge} kind="ok">On<//> <span class="mono small">${c.restore_test.cron}</span>` : html`<${Badge}>Off<//>`],
          ['Keep', `${c.retention.keep_last} newest, up to ${c.retention.keep_days} days`], ['Encryption', c.passphrase_configured ? 'Your passphrase' : 'Platform master key'], ['Targets', c.targets.length ? c.targets.join(', ') : 'This server only']]} />` : html`<${Loading} />`}</${Card}></div>
    <section aria-labelledby="bk-h"><h2 id="bk-h" class="h3 mb-2">Backups</h2>
      <${Table} caption="Backups" rows=${items} empty=${html`<${Empty} icon="hard-drive" title="No backups yet">The first one takes a few seconds.<//>`} columns=${[
      { key: 'created_at', label: 'Taken', sort: (b) => b.created_at, render: (b) => html`<div><${Ago} ts=${b.created_at} /><div class="small muted">${fmtTime(b.created_at)}</div></div>` },
      { key: 'kind', label: 'Kind', render: (b) => titleCase(b.kind) },
      { key: 'state', label: 'State', render: (b) => (b.state === 'succeeded' ? html`<${Badge} kind="ok">Complete<//>` : b.state === 'failed' ? html`<${Badge} kind="bad">Failed<//>` : html`<${Badge}>${titleCase(b.state)}<//>`) },
      { key: 'size', label: 'Size', num: true, sort: (b) => b.size_bytes, render: (b) => fmtBytes(b.size_bytes) },
      { key: 'rows', label: 'Contents', render: (b) => html`<span class="small">${b.rows != null ? `${b.rows} rows, ${b.tables} tables` : ''}${b.includes && b.includes.keys ? ' + keys' : ''}</span>` },
      { key: 'chk', label: 'Verified', sortable: false, render: (b) => (b.last_restore_test ? html`<${Badge} kind=${b.last_restore_test.ok ? 'ok' : 'bad'}>Restore tested<//>` : b.last_validation ? html`<${Badge} kind=${b.last_validation.ok ? 'ok' : 'bad'}>${b.last_validation.ok ? 'Validated' : 'Invalid'}<//>` : html`<span class="muted small">not yet</span>`) },
      { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (b) => html`<${MenuButton} label=${`Actions for ${b.filename}`} items=${[
        { label: 'Download', icon: 'download', onClick: () => download(b) },
        canB && { label: 'Validate', icon: 'check-circle', onClick: () => op(b, 'validate', 'Validating') },
        canB && { label: 'Test restore', icon: 'flask', onClick: () => op(b, 'restore-test', 'Testing restore') },
        canB && { sep: true }, canB && { label: 'Restore…', icon: 'undo', onClick: () => restore(b) }, canB && { label: 'Delete', icon: 'trash', onClick: () => del(b) },
      ].filter(Boolean)} />` },
    ]} /></section>
    <section aria-labelledby="tg-h"><div class="row mb-2"><h2 id="tg-h" class="h3 grow">Targets</h2>${canB ? html`<${Btn} icon="plus" onClick=${addTarget} data-testid="new-target">New target<//>` : null}</div>
      ${tg.data ? html`<${Table} caption="Backup targets" rows=${tg.data.items} rowKey=${(t) => t.name} empty=${html`<${Empty} icon="cloud" title="No off-host targets">Copy encrypted backups to a directory, S3 or MinIO, SFTP, or a lakehouse.<//>`} columns=${[
        { key: 'name', label: 'Target', render: (t) => html`<b>${t.name}</b>` }, { key: 'type', label: 'Type', render: (t) => titleCase(t.type) },
        { key: 'cfg', label: 'Destination', sortable: false, render: (t) => html`<span class="mono small">${t.config ? t.config.path || t.config.bucket || t.config.host || t.config.lakehouse || '' : ''}</span>` },
        { key: 'on', label: 'State', render: (t) => (t.enabled === false ? html`<${Badge}>Disabled<//>` : html`<${Badge} kind="ok">Enabled<//>`) },
        { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (t) => (canB ? html`<${MenuButton} label=${`Actions for ${t.name}`} items=${[{ label: 'Test', icon: 'activity', onClick: () => testTarget(t) }, { label: 'Remove', icon: 'trash', onClick: () => delTarget(t) }]} />` : null) },
      ]} />` : html`<${Loading} />`}</section></div>`;
}

// ----------------------------------------------------------------------------------------------------------------------------------- updates
function UpdateConfigDialog({ cfg, close }) {
  const [channel, setChannel] = useState(cfg.channel);
  const [feed, setFeed] = useState(cfg.feed_url || '');
  const [auto, setAuto] = useState(cfg.auto_check);
  const [every, setEvery] = useState(String(cfg.check_interval_hours));
  const [proxy, setProxy] = useState(cfg.proxy || '');
  const [win, setWin] = useState(jsonPretty(cfg.maintenance_windows || []));
  const [err, setErr] = useState(null);
  const [busy, setBusy] = useState(false);
  async function save() {
    let w; try { w = JSON.parse(win); if (!Array.isArray(w)) throw new Error('must be a list'); } catch (e) { setErr(`Maintenance windows are not valid JSON: ${e.message}`); return; }
    setBusy(true); setErr(null);
    try { close(await api.put('/admin/updates/config', { channel, feed_url: feed.trim() || null, auto_check: auto, check_interval_hours: Number(every) || 24, proxy: proxy.trim() || null, maintenance_windows: w })); toast.ok('Update settings saved'); } catch (e) { setErr(describeError(e)); } finally { setBusy(false); }
  }
  return html`<${Dialog} size="wide" title="Update settings" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} onClick=${save} data-testid="update-config-save">Save settings<//>`}>
    <div class="col gap-3">${err ? html`<${Banner} kind="bad" title="Not saved">${err}<//>` : null}
      <div class="grid-2"><${Select} label="Channel" value=${channel} onChange=${setChannel} options=${['stable', 'beta', 'nightly'].map((v) => ({ value: v, label: titleCase(v) }))} /><${Input} label="Update feed URL" mono value=${feed} onInput=${setFeed} placeholder="https://updates.example.com/olcf" hint="Leave empty for air-gapped sites and import bundles instead." /></div>
      <div class="grid-2"><div class="col gap-2"><${Switch} label="Check automatically" checked=${auto} onChange=${setAuto} /><${Input} label="Check every (hours)" type="number" min="1" value=${every} onInput=${setEvery} /></div><${Input} label="Proxy" mono value=${proxy} onInput=${setProxy} placeholder="http://proxy.internal:3128" /></div>
      <${Textarea} label="Maintenance windows (JSON)" mono rows=${4} value=${win} onInput=${setWin} hint="Updates only apply inside a window. An empty list means any time." /></div></${Dialog}>`;
}
function UpdatesTab() {
  const st = useAsync(() => api.get('/admin/updates'), []);
  const cfg = useAsync(() => api.get('/admin/updates/config'), []);
  const trust = useAsync(() => api.get('/admin/updates/trust').catch(() => null), []);
  const canU = can('admin.update');
  const fileRef = useRef();
  const d = st.data;
  const refresh = () => { st.reload(); cfg.reload(); trust.reload(); };
  async function check() {
    const r = await act(() => api.post('/admin/updates/check', {}), {});
    if (r) { toast.ok(r.latest ? `Version ${r.latest.version} is available` : 'You are on the latest version'); refresh(); }
  }
  async function pickBundle(e) {
    const f = e.target.files && e.target.files[0]; e.target.value = '';
    if (f && (await act(() => api.upload('/admin/updates/import', f), { ok: 'Bundle imported and verified' }))) refresh();
  }
  async function stage(u, verb, title) { const r = await act(() => api.post(`/admin/updates/${enc(u.id)}/${verb}`, {}), {}); if (r && r.operation_id) { await openOperation(r.operation_id, title); refresh(); } }
  async function apply(u) {
    if (!(await confirmDialog({ title: `Apply ${u.version}?`, danger: true, confirmLabel: 'Apply update', body: 'Compute is drained, a backup is taken, then the control plane is upgraded and restarted. Sign-in is briefly unavailable.' }))) return;
    const r = await act(() => api.post(`/admin/updates/${enc(u.id)}/apply`, { confirm: 'APPLY' }), {});
    if (r && r.operation_id) { await openOperation(r.operation_id, 'Applying update'); refresh(); }
  }
  async function maint(on) { if (await act(() => api.post('/admin/updates/maintenance', { on }), { ok: on ? 'Compute is draining' : 'Compute resumed' })) refresh(); }
  async function addKey() {
    const name = await promptDialog({ title: 'Trust a release signing key', label: 'Name of the signer', validate: (v) => (v.trim() ? null : 'Enter a name') });
    if (!name) return;
    const key = await promptDialog({ title: 'Trust a release signing key', label: 'Public key (base64 Ed25519)', validate: (v) => (v.trim().length > 20 ? null : 'Paste the public key') });
    if (key && (await act(() => api.post('/admin/updates/trust', { name: name.trim(), public_key: key.trim() }), { ok: 'Key trusted' }))) refresh();
  }
  async function revokeKey(k) { if (await confirmDialog({ title: `Stop trusting ${k.name}?`, danger: true, confirmLabel: 'Revoke key', body: 'Releases signed only by this key are no longer accepted.' }) && (await act(() => api.del(`/admin/updates/trust/${enc(k.keyid || k.id)}`), { ok: 'Key revoked' })) !== undefined) refresh(); }
  if (st.loading && !st.data) return html`<${Loading} />`;
  if (st.error && !st.data) return html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />`;
  return html`<div data-testid="updates" class="col gap-4">
    <div class="grid-2"><${Card} title="Version" actions=${canU ? html`<${Btn} icon="refresh" onClick=${check} data-testid="update-check">Check for updates<//>` : null}>
        <${Kv} items=${[['Installed', html`<span class="mono">${d.installed}</span>`], ['Channel', titleCase(d.channel)], ['Update feed', d.feed_configured ? 'Configured' : 'Not configured (offline mode)'], ['Last check', d.last_check ? fmtTime(d.last_check.at) : 'Never']]} />
        ${d.available ? html`<div class="mt-3"><${Banner} kind="info" title=${`Version ${d.available.version} is ${d.available.state}`}>${d.available.notes || 'No release notes.'}<//></div>` : html`<p class="muted small mt-3">${d.feed_configured ? 'No newer version is known.' : 'Set an update feed or import an offline bundle.'}</p>`}</${Card}>
      <${Card} title="Maintenance" actions=${canU ? html`<${Btn} icon=${d.maintenance.active ? 'play' : 'pause'} onClick=${() => maint(!d.maintenance.active)}>${d.maintenance.active ? 'Resume compute' : 'Drain compute'}<//>` : null}>
        <${Kv} items=${[['Compute', d.maintenance.active ? html`<${Badge} kind="warn">Draining<//>` : html`<${Badge} kind="ok">Accepting work<//>`], ['Maintenance window', d.in_window ? 'Open now' : 'Closed'], ['Trusted signing keys', d.trusted_keys]]} />
        <div class="row mt-3">${canU ? html`<${Btn} icon="settings" onClick=${async () => { if (await openOverlay(({ close }) => html`<${UpdateConfigDialog} cfg=${cfg.data} close=${close} />`)) refresh(); }} disabled=${!cfg.data} data-testid="update-config">Settings<//><${Btn} icon="upload" onClick=${() => fileRef.current.click()}>Import offline bundle<//><input ref=${fileRef} type="file" hidden accept=".tar,.tar.gz,.tgz,.tar.zst,.zst" onChange=${pickBundle} aria-label="Offline update bundle" />` : null}</div></${Card}></div>
    <section aria-labelledby="up-h"><h2 id="up-h" class="h3 mb-2">Releases</h2><${Table} caption="Releases" rows=${d.items} empty=${html`<${Empty} icon="package" title="No releases known">Check the feed or import an offline bundle signed by a key you trust.<//>`} columns=${[
      { key: 'version', label: 'Version', render: (u) => html`<b class="mono">${u.version}</b>` }, { key: 'channel', label: 'Channel', render: (u) => titleCase(u.channel) },
      { key: 'state', label: 'State', render: (u) => html`<${Badge} kind=${u.state === 'applied' ? 'ok' : u.state === 'failed' ? 'bad' : ''}>${titleCase(u.state)}<//>` },
      { key: 'source', label: 'Source', render: (u) => titleCase(u.source) }, { key: 'rel', label: 'Released', sort: (u) => u.released_at || '', render: (u) => (u.released_at ? fmtTime(u.released_at) : '–') },
      { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (u) => (canU ? html`<${MenuButton} label=${`Actions for ${u.version}`} items=${[
        { label: 'Stage download', icon: 'download', onClick: () => stage(u, 'stage', 'Staging update') }, { label: 'Run preflight checks', icon: 'check-circle', onClick: () => stage(u, 'preflight', 'Preflight checks') },
        { label: 'Apply…', icon: 'rocket', onClick: () => apply(u) }, { sep: true }, { label: 'Cancel', icon: 'x', onClick: () => act(() => api.post(`/admin/updates/${enc(u.id)}/cancel`, {}), { ok: 'Update cancelled' }).then(refresh) }, { label: 'Roll back', icon: 'undo', onClick: () => act(() => api.post(`/admin/updates/${enc(u.id)}/rollback`, {}), { ok: 'Rolled back' }).then(refresh) }]} />` : null) },
    ]} /></section>
    ${trust.data ? html`<section aria-labelledby="tr-h"><div class="row mb-2"><h2 id="tr-h" class="h3 grow">Trusted signing keys</h2>${can('admin.settings') ? html`<${Btn} icon="plus" onClick=${addKey}>Trust a key<//>` : null}</div>
      <p class="muted small mb-2">A release is accepted only when ${trust.data.threshold} trusted key${trust.data.threshold === 1 ? '' : 's'} signed it.</p>
      <${Table} caption="Trusted keys" rows=${trust.data.items} rowKey=${(k) => k.keyid || k.id || k.name} empty=${html`<${Empty} icon="key" title="No trusted keys">Without one, no release can be verified.<//>`} columns=${[{ key: 'name', label: 'Signer', render: (k) => html`<b>${k.name}</b>` }, { key: 'keyid', label: 'Key id', render: (k) => html`<span class="mono small">${k.keyid || k.id}</span>` },
        { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (k) => (can('admin.settings') ? html`<${Btn} size="sm" kind="ghost" icon="trash" iconOnly label=${`Revoke ${k.name}`} onClick=${() => revokeKey(k)} />` : null) }]} /></section>` : null}</div>`;
}

// ----------------------------------------------------------------------------------------------------------------------------------- diagnostics and support bundles
const BUNDLE_OPTS = [['include_logs', 'Server logs'], ['include_config', 'Configuration (secrets removed)'], ['include_diagnostics', 'Latest diagnostics'], ['include_health', 'Health snapshot'], ['include_audit', 'Recent audit records (opt in)'], ['include_jobs', 'Recent job states (opt in)'], ['include_worker_diagnostics', 'Each node\'s own diagnostics (opt in)']];
function BundleDialog({ close }) {
  const [o, setO] = useState({ sanitize: true, include_logs: true, include_config: true, include_diagnostics: true, include_health: true, include_audit: false, include_jobs: false, include_worker_diagnostics: false });
  const [busy, setBusy] = useState(false);
  async function go() { setBusy(true); const r = await act(() => api.post('/admin/support-bundle', o), { ok: 'Support bundle created' }); setBusy(false); if (r) close(r); }
  return html`<${Dialog} title="Create a support bundle" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} onClick=${go} data-testid="bundle-create">Create bundle<//>`}>
    <div class="col gap-2"><p class="small muted">A zip you can attach to a support request. It is scanned for secrets before it is offered for download.</p>
      <${Check} label="Replace IP addresses, e-mail addresses and host names with stable pseudonyms" checked=${o.sanitize} onChange=${(v) => setO({ ...o, sanitize: v })} />
      ${BUNDLE_OPTS.map(([k, l]) => html`<${Check} key=${k} label=${l} checked=${o[k]} onChange=${(v) => setO({ ...o, [k]: v })} />`)}</div></${Dialog}>`;
}
function CheckItem({ c }) {
  return html`<div class="diag-item"><div class="name"><${Light} state=${lightOf(c.status)} label=${titleCase(c.status)} /><span class="grow">${c.name}</span><span class="muted small">${fmtMs(c.ms)}</span></div><div class="detail">${c.detail}</div></div>`;
}
function DiagnosticsTab() {
  // answers 200 with `last_run: null` on a fresh install (no 404, no console error)
  const last = useAsync(() => api.get('/admin/diagnostics').then((d) => (d && d.last_run ? d : null)), []);
  const bundles = useAsync(() => api.get('/admin/support-bundle').then((r) => r.items), []);
  const [opts, setOpts] = useState({ spark: false, worker_probes: false, network_time: false, deep_health: true });
  const [busy, setBusy] = useState(false);
  const [rep, setRep] = useState(null);
  const canM = can('compute.manage');
  const r = rep || last.data;
  async function run() {
    setBusy(true);
    try { setRep(await api.post('/admin/diagnostics', opts)); } catch (e) { toast.bad('Diagnostics failed', describeError(e)); } finally { setBusy(false); }
  }
  const groups = useMemo(() => groupBy((r && r.checks) || [], (c) => c.category), [r]);
  async function makeBundle() { if (await openOverlay(({ close }) => html`<${BundleDialog} close=${close} />`)) bundles.reload(); }
  async function dl(b) { const blob = await act(() => api.blob(`/admin/support-bundle/${enc(b.id)}/download`), {}); if (blob) downloadBlob(blob, b.filename); }
  async function del(b) { if ((await act(() => api.del(`/admin/support-bundle/${enc(b.id)}`), { ok: 'Bundle deleted' })) !== undefined) bundles.reload(); }
  return html`<div data-testid="diagnostics" class="col gap-4">
    <${Card} title="Run diagnostics" sub="Checks the platform from the inside">
      <div class="row-wrap gap-3"><${Check} label="Deep health tests" checked=${opts.deep_health} onChange=${(v) => setOpts({ ...opts, deep_health: v })} />
        <${Check} label="Probe every node" checked=${opts.worker_probes} onChange=${(v) => setOpts({ ...opts, worker_probes: v })} />
        ${canM ? html`<${Check} label="Distributed Spark smoke job" checked=${opts.spark} onChange=${(v) => setOpts({ ...opts, spark: v })} />` : null}
        <${Check} label="Compare clock with the network" checked=${opts.network_time} onChange=${(v) => setOpts({ ...opts, network_time: v })} />
        <span class="grow"></span><${Btn} kind="primary" icon="play" busy=${busy} onClick=${run} data-testid="run-diagnostics">Run diagnostics<//></div></${Card}>
    ${last.loading && !r ? html`<${Loading} />` : r ? html`<div data-testid="diag-report"><${Banner} kind=${r.status === 'ok' ? 'ok' : r.status === 'degraded' ? 'warn' : 'bad'} title=${r.status === 'ok' ? 'All checks passed' : r.status === 'degraded' ? 'Some checks need attention' : 'Some checks failed'}>
        ${r.summary.ok} passed, ${r.summary.warn} warnings, ${r.summary.fail} failed, ${r.summary.skipped} skipped. Ran ${fmtTime(r.started_at)} in ${fmtMs(r.duration_ms)} on ${r.node}.<//>
      ${Object.entries(groups).map(([cat, list]) => html`<section key=${cat} class="mt-3" aria-label=${titleCase(cat)}><h3 class="h4 mb-2">${titleCase(cat)}</h3><div class="diag-grid">${list.map((c) => html`<${CheckItem} key=${c.name} c=${c} />`)}</div></section>`)}</div>` : html`<${Empty} icon="activity" title="Diagnostics have not been run">Run them to see versions, storage, time, certificates and more in one place.<//>`}
    <section aria-labelledby="sb-h"><div class="row mb-2"><h2 id="sb-h" class="h3 grow">Support bundles</h2><${Btn} icon="package" onClick=${makeBundle} data-testid="new-bundle">Create a bundle<//></div>
      <${Table} caption="Support bundles" rows=${bundles.data || []} empty=${bundles.loading ? html`<${Loading} />` : html`<${Empty} icon="package" title="No support bundles">Create one to share diagnostics with a support engineer without exposing secrets.<//>`} columns=${[
      { key: 'created_at', label: 'Created', sort: (b) => b.created_at, render: (b) => html`<${Ago} ts=${b.created_at} />` }, { key: 'filename', label: 'File', render: (b) => html`<span class="mono small">${b.filename}</span>` },
      { key: 'size', label: 'Size', num: true, sort: (b) => b.size_bytes, render: (b) => fmtBytes(b.size_bytes) },
      { key: 'scan', label: 'Secret scan', sortable: false, render: (b) => (b.scan && b.scan.ok ? html`<${Badge} kind="ok">Clean<//>` : html`<${Badge} kind="bad">${b.scan ? plural(b.scan.finding_count, 'finding') : 'Unknown'}<//>`) },
      { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (b) => html`<span class="row"><${Btn} size="sm" icon="download" onClick=${() => dl(b)} aria-label=${`Download ${b.filename}`}>Download<//><${Btn} size="sm" kind="ghost" icon="trash" iconOnly label=${`Delete ${b.filename}`} onClick=${() => del(b)} /></span>` },
    ]} /></section></div>`;
}

// ----------------------------------------------------------------------------------------------------------------------------------- retention
function RetentionTab() {
  const st = useAsync(() => api.get('/admin/retention'), []);
  const runs = useAsync(() => api.get('/admin/retention/runs', { limit: 10 }).then((r) => r.items), []);
  const [rep, setRep] = useState(null);
  const [busy, setBusy] = useState('');
  const canRun = can('admin.settings');
  async function run(dry) {
    if (!dry && !(await confirmDialog({ title: 'Delete expired data now?', danger: true, confirmLabel: 'Run retention', body: 'Records older than the limits below are removed. Audit records are archived first when archiving is on.' }))) return;
    setBusy(dry ? 'dry' : 'run');
    try { setRep(await api.post('/admin/retention/run', { dry_run: dry })); if (!dry) { st.reload(); runs.reload(); } } catch (e) { toast.bad('Retention failed', describeError(e)); } finally { setBusy(''); }
  }
  if (st.loading && !st.data) return html`<${Loading} />`;
  if (st.error && !st.data) return html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />`;
  const d = st.data;
  const LIMITS = [['event_days', 'Platform events'], ['metric_days', 'Metric samples'], ['job_history_days', 'Finished jobs'], ['log_days', 'Job logs'], ['audit_days', 'Audit records'], ['recycle_bin_days', 'Deleted items'], ['token_grace_days', 'Expired tokens'], ['operations_days', 'Finished operations'], ['support_bundle_days', 'Support bundles'], ['run_history_days', 'Retention reports']];
  return html`<div data-testid="retention" class="col gap-4">
    <div class="grid-2"><${Card} title="Schedule" actions=${canRun ? html`<span class="row"><${Btn} icon="eye" busy=${busy === 'dry'} onClick=${() => run(true)} data-testid="retention-dry">Preview<//><${Btn} kind="primary" icon="recycle" busy=${busy === 'run'} onClick=${() => run(false)}>Run now<//></span>` : null}>
        <${Kv} items=${[['Scheduled', d.enabled ? html`<${Badge} kind="ok">Every ${d.interval_hours} hours<//>` : html`<${Badge}>Off<//>`], ['Last run', d.last_run ? html`<${Ago} ts=${d.last_run.finished_at} />` : 'Never'], ['Next run after', d.next_run_after ? fmtTime(d.next_run_after) : '–'], ['Audit archive', html`<span class="mono small">${d.archive_dir}</span>`]]} />
        <p class="muted small mt-2">Change the limits under Settings, in the retention group.</p></${Card}>
      <${Card} title="How long data is kept"><${Kv} items=${LIMITS.map(([k, l]) => [l, `${d.settings[k]} days`])} /></${Card}></div>
    ${d.audit ? html`<${Banner} kind=${d.audit.ok ? 'ok' : 'bad'} title=${d.audit.ok ? 'The audit chain is intact' : 'The audit chain is broken'}>${d.audit.checked} records checked${d.audit.first_seq > 1 ? ` from sequence ${d.audit.first_seq}` : ''}, ${d.audit.purged_records} purged, ${d.audit.anchors} archive anchors.<//>` : null}
    ${rep ? html`<${Card} title=${rep.dry_run ? 'Preview: what a run would delete' : 'Result of this run'} sub=${`${rep.total_candidates} candidates, ${rep.total_deleted} deleted, ${rep.seconds}s`}>
      <${Table} dense caption="Retention steps" rows=${Object.entries(rep.steps).map(([k, v]) => ({ id: k, step: k, ...v }))} columns=${[{ key: 'step', label: 'Data', render: (s) => titleCase(s.step) }, { key: 'candidates', label: 'Expired', num: true }, { key: 'deleted', label: 'Deleted', num: true }, { key: 'cutoff', label: 'Older than', render: (s) => (s.cutoff ? fmtTime(s.cutoff) : '–') }]} /></${Card}>` : null}
    <section aria-labelledby="rr-h"><h2 id="rr-h" class="h3 mb-2">Recent runs</h2><${Table} caption="Retention runs" rows=${runs.data || []} empty=${html`<${Empty} icon="history" title="No runs yet">The first scheduled run happens shortly after start-up.<//>`} columns=${[
      { key: 'started_at', label: 'Started', sort: (r) => r.started_at, render: (r) => html`<${Ago} ts=${r.started_at} />` }, { key: 'dry', label: 'Mode', render: (r) => (r.dry_run ? 'Preview' : 'Delete') },
      { key: 'trigger', label: 'Trigger', render: (r) => titleCase((r.report && r.report.trigger) || '') }, { key: 'del', label: 'Deleted', num: true, sort: (r) => (r.report ? r.report.total_deleted : 0), render: (r) => (r.report ? r.report.total_deleted : '–') },
      { key: 'err', label: 'Outcome', render: (r) => (r.error ? html`<${Badge} kind="bad">Failed<//>` : html`<${Badge} kind="ok">Done<//>`) }]} /></section></div>`;
}

// ----------------------------------------------------------------------------------------------------------------------------------- page
export function AdminPage() {
  const [tab, setTab] = useQueryState('tab', 'health');
  const tabs = [
    { id: 'health', label: 'Health' },
    can('admin.settings') && { id: 'settings', label: 'Settings' },
    can('admin.backup') && { id: 'backup', label: 'Backup and restore' },
    can('admin.update') && { id: 'updates', label: 'Updates' },
    can('admin.diagnostics') && { id: 'diagnostics', label: 'Diagnostics' },
    can('admin.diagnostics') && { id: 'retention', label: 'Data retention' },
  ].filter(Boolean);
  const cur = tabs.find((t) => t.id === tab) ? tab : tabs[0].id;
  return html`<div class="page" data-testid="admin"><${PageHead} icon="settings" title="Administration" sub="Keep the platform healthy, recoverable and up to date." />
    <${Tabs} tabs=${tabs} value=${cur} onChange=${setTab} label="Administration sections" />
    <${TabPanel} id="health" value=${cur}><${HealthTab} /><//><${TabPanel} id="settings" value=${cur}><${SettingsTab} /><//><${TabPanel} id="backup" value=${cur}><${BackupTab} /><//>
    <${TabPanel} id="updates" value=${cur}><${UpdatesTab} /><//><${TabPanel} id="diagnostics" value=${cur}><${DiagnosticsTab} /><//><${TabPanel} id="retention" value=${cur}><${RetentionTab} /><//></div>`;
}
