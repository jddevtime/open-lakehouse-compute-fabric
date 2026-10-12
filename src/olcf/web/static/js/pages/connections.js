// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Connections and secrets: provider-driven connection forms, credentials that are stored once and never shown again, connectivity tests, secret rotation.
import { html, useState, useMemo, useAsync, usePoll } from '../core/h.js';
import { api, can, describeError } from '../api/client.js';
import { Btn, Badge, Banner, Empty, ErrorBox, Loading, Tabs, TabPanel, PageHead, Select, Input, Textarea, Check, Field, Steps, Kv } from '../ui/basic.js';
import { Table } from '../ui/data.js';
import { Icon } from '../ui/icons.js';
import { Dialog, MenuButton, openOverlay, confirmDialog, act, toast } from '../ui/overlay.js';
import { enc, fmtTime, titleCase, shortId, fmtMs } from '../core/util.js';
import { useQueryState, Ago, useWorkspaces } from './common.js';

/** What each provider expects inside its secret, by authentication mode. Mirrors the storage providers' documented secret shapes. */
const SECRET_SHAPES = {
  adls: { account_key: [['account_key', 'Account key']], sas: [['sas_token', 'SAS token']], service_principal: [['client_secret', 'Client secret']] },
  onelake: { service_principal: [['client_secret', 'Client secret']], user_token: [['access_token', 'Access token']] },
  s3: { static: [['access_key_id', 'Access key id'], ['secret_access_key', 'Secret access key'], ['session_token', 'Session token (optional)', true]] },
};
const shapeFor = (provider, auth) => (SECRET_SHAPES[provider] || {})[auth || (provider === 's3' ? 'static' : '')] || null;
const visible = (f, cfg) => !f.visible_when || Object.entries(f.visible_when).every(([k, vs]) => vs.includes(cfg[k]));

function useProviders() { return useAsync(() => api.get('/storage/providers').then((r) => r.items), []); }

function ConnectionDialog({ conn, providers, workspaces, secrets, close }) {
  const edit = !!conn;
  const initProv = edit ? providers.find((p) => p.connection_types.includes(conn.type)) : providers.find((p) => p.id === 's3') || providers[0];
  const [pid, setPid] = useState(initProv ? initProv.id : '');
  const prov = providers.find((p) => p.id === pid) || providers[0];
  const [ctype, setCtype] = useState(edit ? conn.type : prov.connection_types[0]);
  const [name, setName] = useState(edit ? conn.name : '');
  const [desc, setDesc] = useState(edit ? conn.description || '' : '');
  const [scope, setScope] = useState(edit ? (conn.workspace_id || '') : (workspaces[0] ? workspaces[0].id : ''));
  const [cfg, setCfg] = useState(() => {
    const base = {}; (prov.fields || []).forEach((f) => { if (f.default != null && f.type !== 'secret') base[f.name] = f.default; });
    return edit ? { ...base, ...conn.config } : base;
  });
  const [secretMode, setSecretMode] = useState(edit && conn.secret_id ? 'keep' : 'new');
  const [secretId, setSecretId] = useState(edit ? conn.secret_id || '' : '');
  const [sv, setSv] = useState({});
  const [raw, setRaw] = useState('');
  const [busy, setBusy] = useState(false);
  const fields = (prov.fields || []).filter((f) => f.type !== 'secret' && visible(f, cfg));
  const secretField = (prov.fields || []).find((f) => f.type === 'secret');
  const needsSecret = secretField && visible(secretField, cfg);
  const shape = shapeFor(prov.id, cfg.auth);
  const set = (k, v) => setCfg((c) => ({ ...c, [k]: v }));
  const scoped = scope ? workspaces.find((w) => w.id === scope) : null;
  const missing = fields.filter((f) => f.required && (cfg[f.name] === undefined || cfg[f.name] === '')).map((f) => f.label);
  const newSecretReady = shape ? shape.every(([k, , opt]) => opt || sv[k]) : !!raw.trim();
  function changeProv(id) {
    const p = providers.find((x) => x.id === id);
    setPid(id); setCtype(p.connection_types[0]);
    const base = {}; (p.fields || []).forEach((f) => { if (f.default != null && f.type !== 'secret') base[f.name] = f.default; });
    setCfg(base); setSv({});
  }
  async function save() {
    setBusy(true);
    try {
      let sid = secretId || null;
      if (needsSecret && secretMode === 'new' && newSecretReady) {
        let value = raw;
        if (shape) { value = {}; shape.forEach(([k]) => { if (sv[k]) value[k] = sv[k]; }); } else { try { value = JSON.parse(raw); } catch { value = raw; } }
        const meta = await api.post('/secrets', { name: `conn-${name.trim() || 'connection'}-${Date.now().toString(36)}`, value, scope_type: scope ? 'workspace' : 'org', scope_id: scope || '', meta: { purpose: 'connection', provider: prov.id } });
        sid = meta.id;
      }
      const clean = {}; fields.forEach((f) => { if (cfg[f.name] !== undefined && cfg[f.name] !== '') clean[f.name] = f.type === 'bool' ? !!cfg[f.name] : cfg[f.name]; });
      const body = edit ? { name: name.trim(), description: desc, config: clean, ...(sid ? { secret_id: sid } : {}) } : { workspace_id: scope || null, name: name.trim(), type: ctype, description: desc, config: clean, secret_id: sid, shared: !scope };
      const r = await act(() => (edit ? api.patch(`/connections/${enc(conn.id)}`, body, { ifMatch: conn.etag }) : api.post('/connections', body)), { ok: edit ? 'Connection saved' : 'Connection created' });
      if (r) close(r);
    } catch (e) {
      const d = describeError(e); toast.bad(d.title, [d.body, d.remediation].filter(Boolean).join(' '));
    } finally { setBusy(false); }
  }
  const scopeSecrets = secrets.filter((s) => (scope ? s.scope_type === 'workspace' && s.scope_id === scope : s.scope_type === 'org'));
  const ok = name.trim() && !missing.length && (!needsSecret || secretMode === 'keep' || secretMode === 'none' || (secretMode === 'existing' && secretId) || (secretMode === 'new' && newSecretReady));
  return html`<${Dialog} title=${edit ? `Edit ${conn.name}` : 'New connection'} size="wide" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!ok} onClick=${save} data-testid="conn-save">${edit ? 'Save connection' : 'Create connection'}<//>`}>
    <div class="col gap-3">
      ${edit ? null : html`<div class="prov-grid" role="radiogroup" aria-label="Connection kind">${providers.map((p) => html`<button key=${p.id} type="button" role="radio" aria-checked=${pid === p.id} class=${'prov' + (pid === p.id ? ' on' : '')} onClick=${() => changeProv(p.id)}><b>${p.label}</b><span class="small muted">${p.schemes.join(', ')}://</span></button>`)}</div>`}
      <p class="small muted">${prov.description}</p>
      <div class="grid c2"><${Input} label="Name" value=${name} onInput=${setName} required data-testid="conn-name" />
        ${prov.connection_types.length > 1 && !edit ? html`<${Select} label="Type" value=${ctype} onChange=${setCtype} options=${prov.connection_types} />` : html`<${Field} label="Type"><div class="pt-2"><${Badge} mono>${ctype}<//></div><//>`}</div>
      ${edit ? null : html`<${Select} label="Available to" value=${scope} onChange=${setScope} placeholder="Everyone (organization-wide)" options=${workspaces.map((w) => ({ value: w.id, label: `Workspace: ${w.display_name || w.name}` }))} hint="Organization-wide connections need the connection manager role." />`}
      <div class="grid c2">${fields.map((f) => f.type === 'enum' ? html`<${Select} key=${f.name} label=${f.label} value=${cfg[f.name] ?? ''} onChange=${(v) => set(f.name, v)} options=${f.options} hint=${f.help || undefined} />`
        : f.type === 'bool' ? html`<${Check} key=${f.name} label=${f.label} checked=${!!cfg[f.name]} onChange=${(v) => set(f.name, v)} />`
        : html`<${Input} key=${f.name} label=${f.label} value=${cfg[f.name] ?? ''} onInput=${(v) => set(f.name, v)} required=${f.required} hint=${f.help || undefined} mono=${/path|url|root|endpoint/i.test(f.name)} />`)}</div>
      ${needsSecret ? html`<fieldset class="fieldset" data-testid="conn-secret"><legend>Credentials</legend>
        <div class="seg-row mb-3"><div class="chips" role="group" aria-label="Credentials source">${[edit && conn.secret_id && ['keep', 'Keep the stored credentials'], ['new', 'Enter new credentials'], scopeSecrets.length && ['existing', 'Use a saved secret'], ['none', 'None (ambient identity)']].filter(Boolean).map(([v, l]) => html`<button key=${v} type="button" class="chip" aria-pressed=${secretMode === v} onClick=${() => setSecretMode(v)}>${l}</button>`)}</div></div>
        ${secretMode === 'keep' ? html`<p class="small muted">Credentials are stored encrypted (${shortId(conn.secret_id)}). They are never shown again.</p>` : null}
        ${secretMode === 'new' ? html`<div class="col gap-3"><${Banner} kind="info">Credentials are written to the encrypted secret store when you save and can never be read back through the console or the API.<//>
          ${shape ? html`<div class="grid c2">${shape.map(([k, l, opt]) => html`<${Input} key=${k} label=${l} type="password" autocomplete="new-password" value=${sv[k] || ''} onInput=${(v) => setSv({ ...sv, [k]: v })} required=${!opt} />`)}</div>` : html`<${Textarea} label="Secret value" rows=${3} value=${raw} onInput=${setRaw} hint=${(secretField && secretField.help) || 'JSON object or plain text.'} autocomplete="off" />`}</div>` : null}
        ${secretMode === 'existing' ? html`<${Select} label="Secret" value=${secretId} onChange=${setSecretId} placeholder="Choose…" options=${scopeSecrets.map((s) => ({ value: s.id, label: `${s.name} (v${s.version})` }))} />` : null}
        ${secretMode === 'none' ? html`<p class="small muted">The connection uses the identity of the machine that runs the work, for example an instance role.</p>` : null}</fieldset>` : null}
      <${Textarea} label="Description" rows=${2} value=${desc} onInput=${setDesc} />
    </div></${Dialog}>`;
}

function TestDialog({ conn, close }) {
  const r = useAsync(() => api.post(`/connections/${enc(conn.id)}/test`), [conn.id]);
  const d = r.data;
  return html`<${Dialog} title=${`Test ${conn.name}`} size="wide" onClose=${() => close()} footer=${html`<${Btn} onClick=${() => r.reloadLoud()}>Run again<//><${Btn} kind="primary" onClick=${() => close()}>Close<//>`}>
    ${r.loading ? html`<div class="loading"><span class="spinner lg"></span><span>Connecting…</span></div>` : r.error ? html`<${ErrorBox} error=${r.error} />` : html`
      <${Banner} kind=${d.ok ? 'ok' : 'bad'} title=${d.ok ? 'Connection works' : 'Connection failed'}>${d.detail}${d.latency_ms != null ? ` (${fmtMs(d.latency_ms)})` : ''}<//>
      ${(d.steps || []).length ? html`<div class="mt-4"><${Steps} steps=${d.steps.map((s) => ({ name: s.name || s.step, ok: s.ok, detail: s.detail, ms: s.ms, remediation: s.remediation }))} /></div>` : null}`}</${Dialog}>`;
}

function ConnectionsTab({ providers }) {
  const conns = usePoll(() => api.get('/connections', { limit: 200 }).then((r) => r.items), 20000, []);
  const secrets = useAsync(() => api.get('/secrets').then((r) => r.items).catch(() => []), []);
  const { items: workspaces } = useWorkspaces();
  const canManage = can('connection.manage') || workspaces.some((w) => can('connection.manage', w.id));
  const wsName = useMemo(() => Object.fromEntries(workspaces.map((w) => [w.id, w.display_name || w.name])), [workspaces]);
  const sn = useMemo(() => Object.fromEntries((secrets.data || []).map((s) => [s.id, s])), [secrets.data]);
  async function edit(c) { if (await openOverlay(({ close }) => html`<${ConnectionDialog} conn=${c} providers=${providers} workspaces=${workspaces} secrets=${secrets.data || []} close=${close} />`)) { conns.reload(); secrets.reload(); } }
  async function test(c) { await openOverlay(({ close }) => html`<${TestDialog} conn=${c} close=${close} />`); conns.reload(); }
  async function del(c) { if (await confirmDialog({ title: `Delete ${c.name}?`, danger: true, confirmLabel: 'Delete connection', body: 'Lakehouses that use it stop working until they are pointed at another connection.' }) && (await act(() => api.del(`/connections/${enc(c.id)}`), { ok: 'Connection deleted' })) !== undefined) conns.reload(); }
  if (conns.loading && !conns.data) return html`<${Loading} />`;
  if (conns.error && !conns.data) return html`<${ErrorBox} error=${conns.error} onRetry=${conns.reloadLoud} />`;
  const items = conns.data;
  return html`<div data-testid="connections"><div class="toolbar"><span class="muted small">${items.length} connection${items.length === 1 ? '' : 's'}. A connection tells the platform how to reach a storage account or bucket.</span><span class="grow"></span>${canManage ? html`<${Btn} kind="primary" icon="plus" onClick=${() => edit(null)} data-testid="new-connection">New connection<//>` : null}</div>
    <${Table} caption="Connections" rows=${items} empty=${html`<${Empty} icon="plug" title="No connections yet" action=${canManage ? html`<${Btn} kind="primary" icon="plus" onClick=${() => edit(null)}>New connection<//>` : null}>Add S3, Azure Data Lake or OneLake storage so lakehouses can live there. Local lakehouses need no connection.<//>`} columns=${[
      { key: 'name', label: 'Connection', render: (c) => html`<b>${c.name}</b><div class="small muted">${c.description || ''}</div>`, sort: (c) => c.name },
      { key: 'type', label: 'Type', render: (c) => html`<${Badge} mono>${c.type}<//>` },
      { key: 'scope', label: 'Available to', render: (c) => (c.workspace_id ? wsName[c.workspace_id] || shortId(c.workspace_id) : 'Organization') },
      { key: 'target', label: 'Target', render: (c) => html`<span class="mono small">${c.config.bucket || (c.config.account_name ? `${c.config.account_name}/${c.config.filesystem || ''}` : '') || c.config.workspace || c.config.root || '–'}</span>`, sortable: false },
      { key: 'secret', label: 'Credentials', render: (c) => (c.secret_id ? html`<span class="row"><${Icon} name="lock" size="sm" /> <span class="small">${sn[c.secret_id] ? sn[c.secret_id].name.slice(0, 22) : shortId(c.secret_id)}</span></span>` : html`<span class="muted small">none</span>`), sortable: false },
      { key: 'test', label: 'Last test', render: (c) => (c.last_test ? html`<${Badge} kind=${c.last_test.ok ? 'ok' : 'bad'}>${c.last_test.ok ? 'Passed' : 'Failed'}<//> <span class="small muted"><${Ago} ts=${c.last_test.at} /></span>` : html`<span class="muted small">never</span>`), sortable: false },
      { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (c) => html`<div class="row"><${Btn} size="sm" onClick=${() => test(c)} data-testid=${'test-' + c.name}>Test<//>${canManage ? html`<${MenuButton} label=${`Actions for ${c.name}`} items=${[{ label: 'Edit', icon: 'edit', onClick: () => edit(c) }, { sep: true }, { label: 'Delete', icon: 'trash', danger: true, onClick: () => del(c) }]} />` : null}</div>` },
    ]} /></div>`;
}

// ---------------------------------------------------------------------------------------------------------------------------------------- secrets
function SecretDialog({ secret, workspaces, close }) {
  const rot = !!secret;
  const [name, setName] = useState('');
  const [scope, setScope] = useState('');
  const [pairs, setPairs] = useState([{ k: '', v: '' }]);
  const [single, setSingle] = useState('');
  const [mode, setMode] = useState('text');
  const [busy, setBusy] = useState(false);
  const valid = mode === 'text' ? !!single : pairs.some((p) => p.k.trim() && p.v);
  function value() { if (mode === 'text') return single; const o = {}; pairs.forEach((p) => { if (p.k.trim() && p.v) o[p.k.trim()] = p.v; }); return o; }
  async function save() {
    setBusy(true);
    const r = await act(() => (rot ? api.post(`/secrets/${enc(secret.id)}/rotate`, { value: value() }) : api.post('/secrets', { name: name.trim(), value: value(), scope_type: scope ? 'workspace' : 'org', scope_id: scope || '', meta: {} })), { ok: rot ? 'Secret rotated' : 'Secret saved' });
    setBusy(false);
    if (r) close(r);
  }
  return html`<${Dialog} title=${rot ? `Rotate ${secret.name}` : 'New secret'} onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!valid || (!rot && !name.trim())} onClick=${save} data-testid="secret-save">${rot ? 'Rotate secret' : 'Save secret'}<//>`}>
    <div class="col gap-3"><${Banner} kind="info">The value is encrypted when saved and can never be read back. ${rot ? 'Everything that uses this secret picks up the new value.' : ''}<//>
      ${rot ? null : html`<${Input} label="Name" value=${name} onInput=${setName} required hint="Letters, digits, dots, dashes and underscores." /><${Select} label="Available to" value=${scope} onChange=${setScope} placeholder="Organization" options=${workspaces.map((w) => ({ value: w.id, label: `Workspace: ${w.display_name || w.name}` }))} />`}
      <div class="chips" role="group" aria-label="Secret format"><button type="button" class="chip" aria-pressed=${mode === 'text'} onClick=${() => setMode('text')}>Single value</button><button type="button" class="chip" aria-pressed=${mode === 'pairs'} onClick=${() => setMode('pairs')}>Several fields</button></div>
      ${mode === 'text' ? html`<${Input} label="Value" type="password" autocomplete="new-password" value=${single} onInput=${setSingle} data-testid="secret-value" />` : html`<div class="col gap-2">${pairs.map((p, i) => html`<div key=${i} class="grid c2"><${Input} label=${i === 0 ? 'Field' : undefined} aria-label="Field name" value=${p.k} onInput=${(v) => setPairs(pairs.map((x, j) => (j === i ? { ...x, k: v } : x)))} /><${Input} label=${i === 0 ? 'Value' : undefined} aria-label="Field value" type="password" autocomplete="new-password" value=${p.v} onInput=${(v) => setPairs(pairs.map((x, j) => (j === i ? { ...x, v } : x)))} /></div>`)}<div><${Btn} size="sm" icon="plus" onClick=${() => setPairs([...pairs, { k: '', v: '' }])}>Add a field<//></div></div>`}
    </div></${Dialog}>`;
}
function SecretsTab() {
  const secrets = usePoll(() => api.get('/secrets').then((r) => r.items), 20000, []);
  const conns = useAsync(() => api.get('/connections', { limit: 200 }).then((r) => r.items).catch(() => []), []);
  const { items: workspaces } = useWorkspaces();
  const canManage = can('secret.manage') || workspaces.some((w) => can('secret.manage', w.id));
  const wsName = Object.fromEntries(workspaces.map((w) => [w.id, w.display_name || w.name]));
  const usedBy = useMemo(() => { const m = {}; (conns.data || []).forEach((c) => { if (c.secret_id) (m[c.secret_id] ||= []).push(c.name); }); return m; }, [conns.data]);
  async function create() { if (await openOverlay(({ close }) => html`<${SecretDialog} workspaces=${workspaces} close=${close} />`)) secrets.reload(); }
  async function rotate(s) { if (await openOverlay(({ close }) => html`<${SecretDialog} secret=${s} workspaces=${workspaces} close=${close} />`)) secrets.reload(); }
  async function del(s) { if (await confirmDialog({ title: `Delete ${s.name}?`, danger: true, confirmLabel: 'Delete secret', body: 'This cannot be undone. A secret that a connection still uses cannot be deleted.' }) && (await act(() => api.del(`/secrets/${enc(s.id)}`), { ok: 'Secret deleted' })) !== undefined) secrets.reload(); }
  if (secrets.loading && !secrets.data) return html`<${Loading} />`;
  if (secrets.error && !secrets.data) return html`<${ErrorBox} error=${secrets.error} onRetry=${secrets.reloadLoud} />`;
  return html`<div data-testid="secrets"><div class="toolbar"><span class="muted small">Values are never shown, not even to administrators. You can replace a value by rotating it.</span><span class="grow"></span>${canManage ? html`<${Btn} kind="primary" icon="plus" onClick=${create} data-testid="new-secret">New secret<//>` : null}</div>
    <${Table} caption="Secrets" rows=${secrets.data} empty=${html`<${Empty} icon="lock" title="No secrets stored" action=${canManage ? html`<${Btn} kind="primary" icon="plus" onClick=${create}>New secret<//>` : null}>Store passwords, tokens and keys here and refer to them from connections and pipelines.<//>`} columns=${[
      { key: 'name', label: 'Secret', render: (s) => html`<b>${s.name}</b><div class="small muted mono">${shortId(s.id, 10)}</div>`, sort: (s) => s.name },
      { key: 'scope', label: 'Scope', render: (s) => (s.scope_type === 'workspace' ? `Workspace: ${wsName[s.scope_id] || shortId(s.scope_id)}` : 'Organization') },
      { key: 'used', label: 'Used by', render: (s) => ((usedBy[s.id] || []).length ? usedBy[s.id].join(', ') : html`<span class="muted small">nothing</span>`), sortable: false },
      { key: 'v', label: 'Version', num: true, render: (s) => `v${s.version}` },
      { key: 'rot', label: 'Last changed', render: (s) => html`<${Ago} ts=${s.rotated_at || s.created_at} />`, sort: (s) => s.rotated_at || s.created_at },
      { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (s) => (canManage ? html`<${MenuButton} label=${`Actions for ${s.name}`} items=${[{ label: 'Rotate value', icon: 'refresh', onClick: () => rotate(s) }, { sep: true }, { label: 'Delete', icon: 'trash', danger: true, onClick: () => del(s) }]} />` : null) },
    ]} /></div>`;
}

export function ConnectionsPage() {
  const [tab, setTab] = useQueryState('tab', 'connections');
  const prov = useProviders();
  return html`<div class="page wide"><${PageHead} title="Connections and secrets" icon="plug" sub="Where your data lives and the credentials used to reach it. Credentials are stored encrypted and are never displayed." />
    <${Tabs} value=${tab} onChange=${setTab} tabs=${[{ id: 'connections', label: 'Connections' }, { id: 'secrets', label: 'Secrets' }]} />
    <div class="mt-4"><${TabPanel} id="connections" value=${tab}>${prov.loading ? html`<${Loading} />` : prov.error ? html`<${ErrorBox} error=${prov.error} onRetry=${prov.reloadLoud} />` : html`<${ConnectionsTab} providers=${prov.data} />`}<//>
      <${TabPanel} id="secrets" value=${tab}><${SecretsTab} /><//></div></div>`;
}
