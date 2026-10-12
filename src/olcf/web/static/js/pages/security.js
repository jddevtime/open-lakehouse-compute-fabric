// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Security: people and service accounts, roles and bindings, API tokens, identity providers, certificates and the tamper-evident audit log.
import { html, useState, useEffect, useMemo, useAsync, useDebounced } from '../core/h.js';
import { api, can, describeError } from '../api/client.js';
import { Btn, Badge, Banner, Empty, ErrorBox, Loading, Tabs, TabPanel, PageHead, Select, Input, Textarea, Check, Switch, Field, Code, Kv, Card, Steps, Progress } from '../ui/basic.js';
import { Table, JsonView, LoadMore } from '../ui/data.js';
import { Icon } from '../ui/icons.js';
import { Dialog, Drawer, MenuButton, openOverlay, confirmDialog, act, toast } from '../ui/overlay.js';
import { enc, fmtTime, fmtBytes, titleCase, shortId, jsonPretty, downloadBlob, uniq, groupBy } from '../core/util.js';
import { useQueryState, Ago, useWorkspaces } from './common.js';

const ROLE_INFO = {
  PlatformAdmin: 'Everything, including platform administration.',
  SecurityAdmin: 'Users, roles, identity providers, secrets and the audit log.',
  RuntimeAdmin: 'Compute fleet, runtimes, diagnostics and updates.',
  WorkspaceAdmin: 'Full control of a workspace: items, data, jobs, connections and members.',
  Contributor: 'Create and edit items, run notebooks, jobs and queries in a workspace.',
  Operator: 'Run and watch jobs and pipelines without editing them.',
  Viewer: 'Read-only access to items and data.',
  ServicePrincipal: 'Automation identity that runs jobs and calls the API.',
};
const SCOPES = [
  ['read', 'Read only', 'Read what the owner can read and run SQL queries.'],
  ['write', 'Read and write', 'Everything the owner can do except platform administration.'],
  ['mcp', 'AI agent, read only', 'The MCP endpoint with read-only tools.'],
  ['mcp:write', 'AI agent, can run jobs', 'The MCP endpoint, including tools that submit and cancel jobs.'],
  ['fabric', 'Fabric submission', 'Submit and watch jobs from Microsoft Fabric notebooks.'],
  ['*', 'Full access', 'Exactly the owner\'s permissions. Treat it like a password.'],
];
const KINDS = [['user', 'People'], ['service', 'Services'], ['worker', 'Nodes'], ['', 'All']];

function genPassword() {
  const a = new Uint32Array(18); crypto.getRandomValues(a);
  const al = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789';
  return [...a].map((x) => al[x % al.length]).join('');
}
const wsName = (items, id) => { const w = items.find((x) => x.id === id); return w ? w.display_name || w.name : shortId(id); };
const scopeText = (b, items) => (b.scope_type === 'org' || !b.scope_id ? 'Whole platform' : `Workspace ${wsName(items, b.scope_id)}`);

/** Scope select: whole platform or one workspace. value is "org" or a workspace id. */
function ScopeSelect({ value, onChange, items, label = 'Applies to' }) {
  return html`<${Select} label=${label} value=${value} onChange=${onChange} options=${[{ value: 'org', label: 'Whole platform' }, ...items.map((w) => ({ value: w.id, label: `Workspace: ${w.display_name || w.name}` }))]} />`;
}
const scopeParts = (v) => (v === 'org' ? { scope_type: 'org', scope_id: '' } : { scope_type: 'workspace', scope_id: v });

function RevealDialog({ title, banner, value, label, close }) {
  return html`<${Dialog} title=${title} onClose=${() => close()} footer=${html`<${Btn} kind="primary" onClick=${() => close()}>I have saved it<//>`}>
    <${Banner} kind="warn" title="Copy it now">${banner}<//>
    <div class="mt-3"><div class="small muted mb-1">${label}</div><${Code} text=${value} /></div></${Dialog}>`;
}

// ----------------------------------------------------------------------------------------------------------------------------------- users
function UserDialog({ user, wsItems, roles, close }) {
  const [name, setName] = useState(user ? user.name : '');
  const [display, setDisplay] = useState(user ? user.display_name || '' : '');
  const [email, setEmail] = useState(user ? user.email || '' : '');
  const [kind, setKind] = useState('user');
  const [pw, setPw] = useState('');
  const [mustChange, setMustChange] = useState(true);
  const [role, setRole] = useState('Contributor');
  const [scope, setScope] = useState('org');
  const [busy, setBusy] = useState(false);
  const nameOk = /^[A-Za-z0-9._@+-]{1,128}$/.test(name);
  async function save() {
    setBusy(true);
    let r;
    if (user) r = await act(() => api.patch(`/identity/users/${enc(user.id)}`, { display_name: display, email: email || null }), { ok: 'User saved' });
    else {
      const body = { name: name.trim(), kind, display_name: display || undefined, email: email || undefined, roles: role ? [{ role, ...scopeParts(scope) }] : [] };
      if (kind === 'user') { body.password = pw; body.must_change_password = mustChange; }
      r = await act(() => api.post('/identity/users', body), { ok: 'Account created' });
    }
    setBusy(false);
    if (r) close(r);
  }
  const ok = user || (nameOk && (kind === 'service' || pw.length >= 8));
  return html`<${Dialog} title=${user ? `Edit ${user.name}` : 'New account'} onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!ok} onClick=${save} data-testid="user-save">${user ? 'Save changes' : 'Create account'}<//>`}>
    <div class="col gap-3">
      ${user ? null : html`<${Select} label="Account type" value=${kind} onChange=${setKind} options=${[{ value: 'user', label: 'Person (signs in with a password)' }, { value: 'service', label: 'Service account (uses API tokens)' }]} />`}
      <${Input} label="User name" value=${name} onInput=${setName} disabled=${!!user} required autocomplete="off" data-testid="user-name" hint="Letters, digits and . _ @ + - only." error=${name && !nameOk ? 'Use only letters, digits and . _ @ + -' : undefined} />
      <${Input} label="Display name" value=${display} onInput=${setDisplay} />
      <${Input} label="Email" type="email" value=${email} onInput=${setEmail} />
      ${!user && kind === 'user' ? html`<div class="row" style="align-items:flex-end"><div class="grow"><${Input} label="Initial password" type="text" mono autocomplete="off" value=${pw} onInput=${setPw} required hint="At least 8 characters. Share it securely; the person can change it under Security, My account." data-testid="user-password" /></div><${Btn} onClick=${() => setPw(genPassword())}>Generate<//></div>
        <${Check} label="Require a new password at first sign-in" checked=${mustChange} onChange=${setMustChange} data-testid="user-must-change" />` : null}
      ${user ? null : html`<div class="grid-2"><${Select} label="First role" value=${role} onChange=${setRole} placeholder="No role yet" options=${roles.map((r) => ({ value: r.name, label: r.name }))} />
        <${ScopeSelect} value=${scope} onChange=${setScope} items=${wsItems} /></div>`}
    </div></${Dialog}>`;
}

function AccessDrawer({ user, roles, wsItems, close }) {
  const b = useAsync(() => api.get('/identity/bindings', { principal_id: user.id }).then((r) => r.items), [user.id]);
  const [role, setRole] = useState('Contributor');
  const [scope, setScope] = useState('org');
  const canManage = can('identity.manage');
  async function grant() { if (await act(() => api.post('/identity/bindings', { principal_id: user.id, role, ...scopeParts(scope) }), { ok: 'Role granted' })) b.reload(); }
  async function revoke(x) {
    if (!(await confirmDialog({ title: `Remove ${x.role}?`, danger: true, confirmLabel: 'Remove role', body: `${user.name} loses ${x.role} on ${scopeText(x, wsItems).toLowerCase()}.` }))) return;
    if ((await act(() => api.del(`/identity/bindings/${enc(x.id)}`), { ok: 'Role removed' })) !== undefined) b.reload();
  }
  return html`<${Drawer} title=${`Access for ${user.display_name || user.name}`} onClose=${() => close()}>
    ${b.loading && !b.data ? html`<${Loading} />` : b.error && !b.data ? html`<${ErrorBox} error=${b.error} onRetry=${b.reloadLoud} />` : html`
    <${Table} caption="Role bindings" dense rows=${b.data} empty=${html`<${Empty} icon="shield" title="No roles">This account cannot do anything until a role is granted.<//>`} columns=${[
      { key: 'role', label: 'Role', render: (x) => html`<b>${x.role}</b>` },
      { key: 'scope', label: 'Applies to', render: (x) => scopeText(x, wsItems) },
      { key: 'source', label: 'Granted by', render: (x) => (x.source === 'manual' ? 'An administrator' : x.source.startsWith('oidc:') ? `Identity provider ${x.source.slice(5)}` : titleCase(x.source)) },
      { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (x) => (canManage && x.source !== 'system' ? html`<${Btn} size="sm" kind="ghost" icon="trash" iconOnly label=${`Remove ${x.role}`} onClick=${() => revoke(x)} />` : null) },
    ]} />`}
    ${canManage ? html`<div class="card pad mt-4" style="padding:var(--s-4)"><h3 class="h4">Grant a role</h3><div class="grid-2 mt-2"><${Select} label="Role" value=${role} onChange=${setRole} options=${roles.map((r) => ({ value: r.name, label: r.name }))} /><${ScopeSelect} value=${scope} onChange=${setScope} items=${wsItems} /></div>
      <div class="muted small mt-2">${ROLE_INFO[role] || ''}</div><div class="mt-3"><${Btn} kind="primary" icon="plus" onClick=${grant} data-testid="grant-role">Grant role<//></div></div>` : null}
  </${Drawer}>`;
}

function UsersTab() {
  const [q, setQ] = useState('');
  const dq = useDebounced(q, 250);
  const [kind, setKind] = useQueryState('ukind', 'user');
  const users = useAsync(() => api.get('/identity/users', { q: dq || undefined, kind: kind || undefined, limit: 200 }), [dq, kind]);
  const binds = useAsync(() => api.get('/identity/bindings').then((r) => r.items), []);
  const roles = useAsync(() => api.get('/roles').then((r) => r.roles), []);
  const ws = useWorkspaces();
  const canManage = can('identity.manage');
  const byUser = useMemo(() => groupBy(binds.data || [], (b) => b.principal_id), [binds.data]);
  const items = (users.data && users.data.items) || [];
  const refresh = () => { users.reload(); binds.reload(); };
  async function create() { const r = await openOverlay(({ close }) => html`<${UserDialog} wsItems=${ws.items} roles=${roles.data || []} close=${close} />`); if (r) refresh(); }
  async function edit(u) { if (await openOverlay(({ close }) => html`<${UserDialog} user=${u} wsItems=${ws.items} roles=${roles.data || []} close=${close} />`)) refresh(); }
  async function access(u) { await openOverlay(({ close }) => html`<${AccessDrawer} user=${u} wsItems=${ws.items} roles=${roles.data || []} close=${close} />`); binds.reload(); }
  async function reset(u) {
    if (!(await confirmDialog({ title: `Reset the password of ${u.name}?`, confirmLabel: 'Reset password', body: 'A temporary password is generated and shown once. The person must choose a new one at the next sign-in, and existing sessions end.' }))) return;
    const r = await act(() => api.post(`/identity/users/${enc(u.id)}/reset-password`));
    if (r) { await openOverlay(({ close }) => html`<${RevealDialog} title=${`Temporary password for ${u.name}`} banner="It is shown once. Share it through a channel you trust." label="Temporary password" value=${r.temporary_password} close=${close} />`); users.reload(); }
  }
  async function toggle(u) {
    if (u.is_active && !(await confirmDialog({ title: `Disable ${u.name}?`, danger: true, confirmLabel: 'Disable account', body: 'The account can no longer sign in or use tokens. Its history and ownership are kept.' }))) return;
    if (await act(() => api.patch(`/identity/users/${enc(u.id)}`, { is_active: !u.is_active }), { ok: u.is_active ? 'Account disabled' : 'Account enabled' })) users.reload();
  }
  return html`<div data-testid="users"><div class="toolbar">
    <div class="chips" role="group" aria-label="Account type">${KINDS.map(([v, l]) => html`<button key=${v} class="chip" aria-pressed=${kind === v} onClick=${() => setKind(v)}>${l}</button>`)}</div>
    <${Input} label="Search" size="sm" value=${q} onInput=${setQ} placeholder="Name or email" type="search" />
    <span class="grow"></span>${canManage ? html`<${Btn} kind="primary" icon="plus" onClick=${create} data-testid="new-user">New account<//>` : null}</div>
    ${users.loading && !users.data ? html`<${Loading} />` : users.error && !users.data ? html`<${ErrorBox} error=${users.error} onRetry=${users.reloadLoud} />` : html`
    <${Table} caption="Accounts" rows=${items} empty=${html`<${Empty} icon="users" title="No accounts match">Try another filter or search.<//>`} columns=${[
      { key: 'name', label: 'Account', sort: (u) => u.name, render: (u) => html`<div><b>${u.display_name || u.name}</b><div class="small muted mono">${u.name}${u.email ? ` · ${u.email}` : ''}</div></div>` },
      { key: 'kind', label: 'Type', render: (u) => titleCase(u.kind) },
      { key: 'source', label: 'Source', render: (u) => (u.source === 'local' || u.source === 'bootstrap' ? titleCase(u.source) : u.source) },
      { key: 'roles', label: 'Roles', sortable: false, render: (u) => { const bs = byUser[u.id] || []; return bs.length ? html`<span class="chips-static">${uniq(bs.map((x) => x.role)).slice(0, 3).map((r) => html`<span key=${r} class="tag">${r}</span>`)}${uniq(bs.map((x) => x.role)).length > 3 ? html`<span class="muted small">+${uniq(bs.map((x) => x.role)).length - 3}</span>` : null}</span>` : html`<span class="muted small">none</span>`; } },
      { key: 'status', label: 'Status', sort: (u) => (u.is_active ? 1 : 0), render: (u) => (!u.is_active ? html`<${Badge}>Disabled<//>` : u.must_change_password ? html`<${Badge} kind="warn">Must change password<//>` : html`<${Badge} kind="ok">Active<//>`) },
      { key: 'last', label: 'Last sign-in', sort: (u) => u.last_login_at || '', render: (u) => (u.last_login_at ? html`<${Ago} ts=${u.last_login_at} />` : html`<span class="muted">never</span>`) },
      { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (u) => html`<${MenuButton} label=${`Actions for ${u.name}`} items=${[
        { label: 'Roles and access', icon: 'shield', onClick: () => access(u) },
        canManage && { label: 'Edit details', icon: 'edit', onClick: () => edit(u) },
        canManage && u.kind === 'user' && { label: 'Reset password', icon: 'key', onClick: () => reset(u) },
        canManage && u.kind !== 'worker' && { sep: true },
        canManage && u.kind !== 'worker' && { label: u.is_active ? 'Disable account' : 'Enable account', icon: u.is_active ? 'stop' : 'check', onClick: () => toggle(u) },
      ].filter(Boolean)} />` },
    ]} />`}</div>`;
}

// ----------------------------------------------------------------------------------------------------------------------------------- roles and bindings
function RoleCard({ role }) {
  const [open, setOpen] = useState(false);
  const perms = role.permissions;
  const groups = useMemo(() => groupBy(perms, (p) => p.split('.')[0]), [perms]);
  return html`<div class="card pad role-card"><div class="row"><h3 class="h4 grow">${role.name}</h3><${Badge}>${perms[0] === '*' ? 'All permissions' : `${perms.length} permissions`}<//></div>
    <p class="muted small mt-1">${ROLE_INFO[role.name] || ''}</p>
    ${perms[0] === '*' ? null : html`<button class="linkbtn small mt-2" aria-expanded=${open} onClick=${() => setOpen(!open)}><${Icon} name=${open ? 'chevron-down' : 'chevron-right'} size="sm" /> ${open ? 'Hide' : 'Show'} permissions</button>`}
    ${open ? html`<div class="perm-groups mt-2">${Object.entries(groups).map(([g, ps]) => html`<div key=${g}><div class="small muted">${titleCase(g)}</div><div class="chips-static">${ps.map((p) => html`<span key=${p} class="tag mono">${p.slice(g.length + 1) || p}</span>`)}</div></div>`)}</div>` : null}</div>`;
}
function AccessTab() {
  const roles = useAsync(() => api.get('/roles').then((r) => r.roles), []);
  const binds = useAsync(() => api.get('/identity/bindings').then((r) => r.items), []);
  const ws = useWorkspaces();
  const users = useAsync(() => api.get('/identity/users', { limit: 200 }).then((r) => r.items), []);
  const canManage = can('identity.manage');
  async function grant() {
    const r = await openOverlay(({ close }) => html`<${GrantDialog} users=${(users.data || []).filter((u) => u.kind !== 'worker')} roles=${roles.data || []} wsItems=${ws.items} close=${close} />`);
    if (r) binds.reload();
  }
  async function revoke(x) {
    if (!(await confirmDialog({ title: `Remove ${x.role} from ${x.principal_name}?`, danger: true, confirmLabel: 'Remove role', body: `Scope: ${scopeText(x, ws.items)}.` }))) return;
    if ((await act(() => api.del(`/identity/bindings/${enc(x.id)}`), { ok: 'Role removed' })) !== undefined) binds.reload();
  }
  return html`<div data-testid="access" class="col gap-4">
    <section aria-labelledby="roles-h"><h2 id="roles-h" class="h3">Roles</h2>
      ${roles.loading && !roles.data ? html`<${Loading} />` : roles.error ? html`<${ErrorBox} error=${roles.error} />` : html`<div class="role-grid mt-2">${roles.data.map((r) => html`<${RoleCard} key=${r.name} role=${r} />`)}</div>`}</section>
    <section aria-labelledby="bind-h"><div class="row"><h2 id="bind-h" class="h3 grow">Who has which role</h2>${canManage ? html`<${Btn} kind="primary" icon="plus" onClick=${grant} data-testid="new-binding">Grant a role<//>` : null}</div>
      ${binds.loading && !binds.data ? html`<${Loading} />` : binds.error && !binds.data ? html`<${ErrorBox} error=${binds.error} onRetry=${binds.reloadLoud} />` : html`<div class="mt-2"><${Table} caption="Role bindings" rows=${binds.data} columns=${[
      { key: 'who', label: 'Account', sort: (b) => b.principal_name, render: (b) => html`<b>${b.principal_name}</b>` },
      { key: 'role', label: 'Role' },
      { key: 'scope', label: 'Applies to', sort: (b) => scopeText(b, ws.items), render: (b) => scopeText(b, ws.items) },
      { key: 'source', label: 'Granted by', render: (b) => (b.source === 'manual' ? 'An administrator' : b.source === 'system' ? 'The platform' : b.source.startsWith('oidc:') ? `Identity provider ${b.source.slice(5)}` : titleCase(b.source)) },
      { key: 'at', label: 'Granted', sort: (b) => b.created_at, render: (b) => html`<${Ago} ts=${b.created_at} />` },
      { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (b) => (canManage && b.source !== 'system' ? html`<${Btn} size="sm" kind="ghost" icon="trash" iconOnly label=${`Remove ${b.role} from ${b.principal_name}`} onClick=${() => revoke(b)} />` : null) },
    ]} /></div>`}</section></div>`;
}
function GrantDialog({ users, roles, wsItems, close }) {
  const [who, setWho] = useState('');
  const [role, setRole] = useState('Contributor');
  const [scope, setScope] = useState('org');
  const [busy, setBusy] = useState(false);
  async function save() { setBusy(true); const r = await act(() => api.post('/identity/bindings', { principal_id: who, role, ...scopeParts(scope) }), { ok: 'Role granted' }); setBusy(false); if (r) close(r); }
  return html`<${Dialog} title="Grant a role" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!who} onClick=${save}>Grant role<//>`}>
    <div class="col gap-3"><${Select} label="Account" value=${who} onChange=${setWho} placeholder="Choose an account" options=${users.map((u) => ({ value: u.id, label: `${u.display_name || u.name} (${u.name})` }))} />
      <${Select} label="Role" value=${role} onChange=${setRole} options=${roles.map((r) => ({ value: r.name, label: r.name }))} /><div class="muted small">${ROLE_INFO[role] || ''}</div>
      <${ScopeSelect} value=${scope} onChange=${setScope} items=${wsItems} /></div></${Dialog}>`;
}

// ----------------------------------------------------------------------------------------------------------------------------------- tokens
function TokenDialog({ users, close }) {
  const me = users.find((u) => u.self);
  const [name, setName] = useState('');
  const [scopes, setScopes] = useState(new Set(['read']));
  const [days, setDays] = useState('90');
  const [owner, setOwner] = useState('');
  const [busy, setBusy] = useState(false);
  const tog = (s) => setScopes((x) => { const n = new Set(x); if (n.has(s)) n.delete(s); else n.add(s); return n; });
  async function save() {
    setBusy(true);
    const body = { name: name.trim(), scopes: [...scopes], ttl_days: Number(days) || 90, ...(owner ? { principal_id: owner } : {}) };
    const r = await act(() => api.post('/identity/tokens', body), { ok: 'Token created' });
    setBusy(false);
    if (r) close(r);
  }
  return html`<${Dialog} title="New API token" onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!name.trim() || !scopes.size} onClick=${save} data-testid="token-save">Create token<//>`}>
    <div class="col gap-3"><${Input} label="Name" value=${name} onInput=${setName} required placeholder="ci-pipeline" data-testid="token-name" />
      <fieldset class="fieldset"><legend>What the token may do</legend><div class="col gap-2">${SCOPES.map(([v, l, d]) => html`<label key=${v} class="check block"><input type="checkbox" checked=${scopes.has(v)} onChange=${() => tog(v)} /> <span><b>${l}</b><span class="muted small block">${d}</span></span></label>`)}</div></fieldset>
      <div class="grid-2"><${Input} label="Valid for (days)" type="number" min="1" max="3650" value=${days} onInput=${setDays} />
        ${users.length ? html`<${Select} label="Acts as" value=${owner} onChange=${setOwner} options=${[{ value: '', label: 'Me' }, ...users.map((u) => ({ value: u.id, label: u.display_name || u.name }))]} />` : null}</div>
      <div class="muted small">A token never has more permissions than the account it acts as.</div></div></${Dialog}>`;
}
function TokensTab() {
  const [all, setAll] = useQueryState('tall', '');
  const tokens = useAsync(() => api.get('/identity/tokens', { all: all ? true : undefined }).then((r) => r.items), [all]);
  const canAdmin = can('identity.manage');
  const users = useAsync(() => (canAdmin ? api.get('/identity/users', { limit: 200 }).then((r) => r.items.filter((u) => u.kind !== 'worker')) : Promise.resolve([])), [canAdmin]);
  const nameOf = (id) => { const u = (users.data || []).find((x) => x.id === id); return u ? u.display_name || u.name : null; };
  async function create() {
    const r = await openOverlay(({ close }) => html`<${TokenDialog} users=${canAdmin ? users.data || [] : []} close=${close} />`);
    if (r) { tokens.reload(); await openOverlay(({ close }) => html`<${RevealDialog} title=${`Token ${r.id ? '' : ''}created`} banner="The token is shown once and cannot be recovered. Store it in a secret manager." label="API token" value=${r.token} close=${close} />`); }
  }
  async function revoke(t) {
    if (!(await confirmDialog({ title: `Revoke ${t.name}?`, danger: true, confirmLabel: 'Revoke token', body: 'Anything using this token stops working immediately.' }))) return;
    if ((await act(() => api.del(`/identity/tokens/${enc(t.id)}`), { ok: 'Token revoked' })) !== undefined) tokens.reload();
  }
  const exp = (t) => (t.expires_at ? Date.parse(t.expires_at) : null);
  return html`<div data-testid="tokens"><div class="toolbar"><span class="muted small">Tokens carry the permissions of the account they act as, limited to the chosen scopes.</span><span class="grow"></span>
    ${canAdmin ? html`<${Switch} label="Everyone's tokens" checked=${!!all} onChange=${(v) => setAll(v ? '1' : '')} />` : null}<${Btn} kind="primary" icon="plus" onClick=${create} data-testid="new-token">New token<//></div>
    ${tokens.loading && !tokens.data ? html`<${Loading} />` : tokens.error && !tokens.data ? html`<${ErrorBox} error=${tokens.error} onRetry=${tokens.reloadLoud} />` : html`
    <${Table} caption="API tokens" rows=${tokens.data} empty=${html`<${Empty} icon="key" title="No tokens">Create a token for scripts, CI and AI agents.<//>`} columns=${[
      { key: 'name', label: 'Token', render: (t) => html`<div><b>${t.name}</b><div class="small muted mono">${t.prefix}…</div></div>` },
      { key: 'who', label: 'Acts as', render: (t) => nameOf(t.principal_id) || (all ? shortId(t.principal_id) : 'Me') },
      { key: 'scopes', label: 'Scopes', sortable: false, render: (t) => html`<span class="chips-static">${t.scopes.map((s) => html`<span key=${s} class="tag">${s}</span>`)}</span>` },
      { key: 'used', label: 'Last used', sort: (t) => t.last_used_at || '', render: (t) => (t.last_used_at ? html`<${Ago} ts=${t.last_used_at} />` : html`<span class="muted">never</span>`) },
      { key: 'exp', label: 'Expires', sort: (t) => t.expires_at || '', render: (t) => (exp(t) ? (exp(t) < Date.now() ? html`<${Badge} kind="bad">Expired<//>` : html`<time datetime=${t.expires_at} title=${t.expires_at}>${fmtTime(t.expires_at)}</time>`) : html`<span class="muted">never</span>`) },
      { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (t) => html`<${Btn} size="sm" kind="ghost" icon="trash" iconOnly label=${`Revoke ${t.name}`} onClick=${() => revoke(t)} />` },
    ]} />`}</div>`;
}

// ----------------------------------------------------------------------------------------------------------------------------------- identity providers
const TYPE_LABEL = { oidc: 'OpenID Connect', entra: 'Microsoft Entra ID', ldap: 'LDAP / Active Directory' };
function ruleToRow(r) {
  return { claim: r.claim || 'groups', mode: r.pattern != null ? 'pattern' : 'value', match: r.pattern != null ? r.pattern : r.value || '', role: r.role, scope: r.scope_type === 'workspace' ? (r.scope_id || `name:${r.workspace}`) : 'org' };
}
function rowToRule(x) {
  const out = { claim: x.claim.trim(), role: x.role, [x.mode]: x.match };
  if (x.scope === 'org') out.scope_type = 'org';
  else if (x.scope.startsWith('name:')) { out.scope_type = 'workspace'; out.workspace = x.scope.slice(5); } else { out.scope_type = 'workspace'; out.scope_id = x.scope; }
  return out;
}
function ProviderDialog({ prov, roles, wsItems, close }) {
  const cfg0 = prov ? prov.config || {} : {};
  const { role_mappings: rm0, ...rest0 } = cfg0;
  const [type, setType] = useState(prov ? prov.type : 'oidc');
  const [name, setName] = useState(prov ? prov.name : '');
  const [display, setDisplay] = useState(prov ? prov.display_name || '' : '');
  const [enabled, setEnabled] = useState(prov ? prov.enabled : false);
  const [issuer, setIssuer] = useState(prov ? prov.issuer || '' : '');
  const [tenant, setTenant] = useState(prov ? cfg0.tenant || '' : '');
  const [clientId, setClientId] = useState(prov ? prov.client_id || '' : '');
  const [secret, setSecret] = useState('');
  const [rows, setRows] = useState(() => (rm0 || []).map(ruleToRow));
  const [extra, setExtra] = useState(() => { const { tenant: _t, ...o } = rest0; return Object.keys(o).length ? jsonPretty(o) : ''; });
  const [err, setErr] = useState(null);
  const [busy, setBusy] = useState(false);
  const setRow = (i, patch) => setRows((rs) => rs.map((r, j) => (j === i ? { ...r, ...patch } : r)));
  const names = rows.map((r) => r.scope).filter((s) => s.startsWith('name:'));
  async function save() {
    let extraObj = {};
    if (extra.trim()) { try { extraObj = JSON.parse(extra); if (!extraObj || typeof extraObj !== 'object' || Array.isArray(extraObj)) throw new Error('must be an object'); } catch (e) { setErr(`Other settings are not valid JSON: ${e.message}`); return; } }
    const config = { ...extraObj, ...(type === 'ldap' ? {} : {}), role_mappings: rows.filter((r) => r.claim.trim() && r.match).map(rowToRule) };
    if (type === 'entra' && tenant) config.tenant = tenant.trim();
    setBusy(true); setErr(null);
    const common = { display_name: display || null, enabled, client_id: clientId || null, config, ...(secret ? { client_secret: secret } : {}) };
    try {
      const r = prov
        ? await api.patch(`/identity/providers/${enc(prov.id)}`, { ...common, issuer: type === 'oidc' ? issuer.trim() || null : undefined, tenant: type === 'entra' ? tenant.trim() || null : undefined }, { ifMatch: prov.etag })
        : await api.post('/identity/providers', { name: name.trim(), type, ...common, issuer: type === 'oidc' ? issuer.trim() : undefined, tenant: type === 'entra' ? tenant.trim() : undefined });
      toast.ok(prov ? 'Provider saved' : 'Provider created'); close(r);
    } catch (e) { setErr(describeError(e)); } finally { setBusy(false); }
  }
  const ok = (prov || /^[a-z0-9][a-z0-9-]{0,62}$/.test(name)) && (type === 'ldap' || clientId) && (type !== 'oidc' || issuer) && (type !== 'entra' || tenant);
  return html`<${Dialog} size="wide" title=${prov ? `Edit ${prov.name}` : 'New identity provider'} onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!ok} onClick=${save} data-testid="idp-save">${prov ? 'Save changes' : 'Create provider'}<//>`}>
    <div class="col gap-3">${err ? html`<${Banner} kind="bad" title="The provider was not saved">${err}<//>` : null}
      <div class="grid-2"><${Select} label="Protocol" value=${type} onChange=${setType} disabled=${!!prov} options=${Object.entries(TYPE_LABEL).map(([value, label]) => ({ value, label }))} />
        <${Input} label="Name" value=${name} onInput=${setName} disabled=${!!prov} required hint="Lower-case letters, digits and dashes. Used in sign-in URLs." data-testid="idp-name" /></div>
      <div class="grid-2"><${Input} label="Button label" value=${display} onInput=${setDisplay} hint="Shown on the sign-in page." /><div class="pt-5"><${Switch} label="Enabled" checked=${enabled} onChange=${setEnabled} /></div></div>
      ${type === 'oidc' ? html`<${Input} label="Issuer URL" mono value=${issuer} onInput=${setIssuer} required placeholder="https://idp.example.com/realms/main" data-testid="idp-issuer" />` : null}
      ${type === 'entra' ? html`<${Input} label="Tenant" mono value=${tenant} onInput=${setTenant} required hint="Tenant id or domain. Multi-tenant authorities need allowed_tenants in the other settings." />` : null}
      ${type !== 'ldap' ? html`<div class="grid-2"><${Input} label="Client ID" mono value=${clientId} onInput=${setClientId} required /><${Input} label=${prov && prov.has_client_secret ? 'Client secret (stored; type to replace)' : 'Client secret'} type="password" autocomplete="new-password" value=${secret} onInput=${setSecret} hint="Write-only. It is never shown again." /></div>` : html`<div class="grid-2"><${Input} label="Bind DN (optional)" mono value=${clientId} onInput=${setClientId} /><${Input} label="Bind password" type="password" autocomplete="new-password" value=${secret} onInput=${setSecret} hint="Write-only." /></div>`}
      <section aria-labelledby="map-h"><div class="row"><h3 id="map-h" class="h4 grow">Role mappings</h3><${Btn} size="sm" icon="plus" onClick=${() => setRows([...rows, { claim: 'groups', mode: 'value', match: '', role: 'Contributor', scope: 'org' }])} data-testid="add-mapping">Add mapping<//></div>
        <p class="muted small">When a sign-in claim matches, the person receives the role. Roles from this provider always follow what the provider says now; manual roles are never touched.</p>
        ${rows.length ? html`<div class="map-rows mt-2">${rows.map((r, i) => html`<div class="map-row" key=${i}>
          <${Input} label="Claim" mono size="sm" value=${r.claim} onInput=${(v) => setRow(i, { claim: v })} />
          <${Select} label="Match" size="sm" value=${r.mode} onChange=${(v) => setRow(i, { mode: v })} options=${[{ value: 'value', label: 'equals' }, { value: 'pattern', label: 'regex' }]} />
          <${Input} label="Value" mono size="sm" value=${r.match} onInput=${(v) => setRow(i, { match: v })} />
          <${Select} label="Role" size="sm" value=${r.role} onChange=${(v) => setRow(i, { role: v })} options=${roles.map((x) => ({ value: x.name, label: x.name }))} />
          <${Select} label="Applies to" size="sm" value=${r.scope} onChange=${(v) => setRow(i, { scope: v })} options=${[{ value: 'org', label: 'Whole platform' }, ...wsItems.map((w) => ({ value: w.id, label: w.display_name || w.name })), ...names.filter((x, j) => names.indexOf(x) === j).map((n) => ({ value: n, label: `Workspace ${n.slice(5)}` }))]} />
          <${Btn} size="sm" kind="ghost" icon="trash" iconOnly label=${`Remove mapping ${i + 1}`} onClick=${() => setRows(rows.filter((_, j) => j !== i))} /></div>`)}</div>` : html`<p class="muted small mt-2">No mappings: people who sign in through this provider get no roles${''}.</p>`}</section>
      <${Textarea} label="Other settings (JSON)" mono rows=${6} value=${extra} onInput=${setExtra} placeholder='{"scopes": ["openid", "profile", "email"], "groups_claim": "groups"}' hint=${type === 'ldap' ? 'LDAP needs an "ldap" object with server_uri and user_base_dn.' : 'Optional: scopes, groups_claim, allowed_audiences, auto_provision and more.'} />
    </div></${Dialog}>`;
}
function PreviewDialog({ prov, close }) {
  const [text, setText] = useState(jsonPretty({ sub: 'user-1', email: 'ada@example.com', groups: ['data-eng'] }));
  const [res, setRes] = useState(null);
  const [err, setErr] = useState(null);
  const [busy, setBusy] = useState(false);
  async function run() {
    let claims; try { claims = JSON.parse(text); } catch (e) { setErr(`Claims are not valid JSON: ${e.message}`); return; }
    setBusy(true); setErr(null);
    try { setRes(await api.post(`/identity/providers/${enc(prov.id)}/preview-mapping`, { claims })); } catch (e) { setErr(describeError(e)); } finally { setBusy(false); }
  }
  return html`<${Dialog} size="wide" title=${`Preview role mapping: ${prov.name}`} onClose=${() => close()} footer=${html`<${Btn} onClick=${() => close()}>Close<//><${Btn} kind="primary" busy=${busy} onClick=${run} data-testid="preview-run">Preview<//>`}>
    <div class="col gap-3"><p class="muted small">Paste the claims of a sign-in and see which roles the current mappings would grant. Nothing is saved.</p>
      <${Textarea} label="Claims (JSON)" mono rows=${8} value=${text} onInput=${setText} />
      ${err ? html`<${Banner} kind="bad" title="Could not preview">${err}<//>` : null}
      ${res ? html`<div data-testid="preview-result"><${Banner} kind=${res.would_login ? 'ok' : 'warn'} title=${res.would_login ? 'This sign-in would be allowed' : 'This sign-in would be refused'}>${res.bindings.length ? `${res.bindings.length} role${res.bindings.length === 1 ? '' : 's'} granted.` : 'No roles would be granted.'}<//>
        ${res.bindings.length ? html`<${Table} dense caption="Granted roles" rows=${res.bindings} rowKey=${(b) => b.role + b.scope_id} columns=${[{ key: 'role', label: 'Role' }, { key: 'scope', label: 'Applies to', render: (b) => (b.scope_type === 'org' ? 'Whole platform' : `Workspace ${shortId(b.scope_id)}`) }]} />` : null}
        ${res.skipped && res.skipped.length ? html`<div class="mt-2"><${Banner} kind="warn" title="Skipped rules"><pre class="pre small">${jsonPretty(res.skipped)}</pre><//></div>` : null}</div>` : null}</div></${Dialog}>`;
}
function TestProviderDialog({ prov, close }) {
  const r = useAsync(() => api.post(`/identity/providers/${enc(prov.id)}/test`, {}), [prov.id]);
  const d = r.data;
  return html`<${Dialog} size="wide" title=${`Test ${prov.name}`} onClose=${() => close()} footer=${html`<${Btn} onClick=${() => r.reloadLoud()}>Run again<//><${Btn} kind="primary" onClick=${() => close()}>Close<//>`}>
    ${r.loading ? html`<div class="loading"><span class="spinner lg"></span><span>Contacting the provider…</span></div>` : r.error ? html`<${ErrorBox} error=${r.error} />` : html`
    <${Banner} kind=${d.ok ? 'ok' : 'bad'} title=${d.ok ? 'The provider is reachable and correctly configured' : 'The provider test failed'}>Checked ${fmtTime(d.at)}<//>
    <div class="mt-3"><${Steps} steps=${(d.steps || []).map((s) => ({ name: s.name, ok: s.ok, detail: s.detail, ms: s.ms, remediation: s.remediation }))} /></div>`}</${Dialog}>`;
}
function ProvidersTab() {
  const list = useAsync(() => api.get('/identity/providers'), []);
  const local = useAsync(() => api.get('/identity/local-login'), []);
  const roles = useAsync(() => api.get('/roles').then((r) => r.roles), []);
  const ws = useWorkspaces();
  const canManage = can('identity.federation.manage');
  const items = (list.data && list.data.items) || [];
  const refresh = () => { list.reload(); local.reload(); };
  async function edit(p) { if (await openOverlay(({ close }) => html`<${ProviderDialog} prov=${p} roles=${roles.data || []} wsItems=${ws.items} close=${close} />`)) refresh(); }
  async function toggle(p) { if (await act(() => api.patch(`/identity/providers/${enc(p.id)}`, { enabled: !p.enabled }, { ifMatch: p.etag }), { ok: p.enabled ? 'Provider disabled' : 'Provider enabled' })) refresh(); }
  async function del(p) { if (await confirmDialog({ title: `Delete ${p.name}?`, danger: true, confirmLabel: 'Delete provider', body: 'People who sign in only through it can no longer do so. Roles it granted are removed.' }) && (await act(() => api.del(`/identity/providers/${enc(p.id)}`), { ok: 'Provider deleted' })) !== undefined) refresh(); }
  async function setLocal(v) { if (await act(() => api.put('/identity/local-login', { enabled: v }), { ok: v ? 'Password sign-in enabled' : 'Password sign-in disabled' })) local.reload(); }
  const ll = local.data;
  return html`<div data-testid="providers" class="col gap-4">
    ${ll ? html`<${Card} title="Password sign-in" sub="Local accounts with a user name and password">
      <div class="row"><${Switch} label=${ll.enabled ? 'Enabled' : 'Disabled'} checked=${ll.enabled} disabled=${!canManage || (ll.enabled && !ll.can_disable)} onChange=${setLocal} data-testid="local-login" />
      <span class="muted small grow">${ll.enabled ? (ll.can_disable ? 'You can turn this off because a federated administrator exists.' : 'Cannot be turned off yet.') : `Re-enable it from the server with ${ll.break_glass_env || 'the break-glass setting'} if you are locked out.`}</span></div>
      ${ll.enabled && !ll.can_disable && ll.blockers && ll.blockers.length ? html`<ul class="small muted mt-2">${ll.blockers.map((b) => html`<li key=${b}>${b[0].toUpperCase() + b.slice(1)}.</li>`)}</ul>` : null}</${Card}>` : null}
    <div class="toolbar"><h2 class="h3 grow">Identity providers</h2>${canManage ? html`<${Btn} kind="primary" icon="plus" onClick=${() => edit(null)} data-testid="new-idp">New provider<//>` : null}</div>
    ${list.loading && !list.data ? html`<${Loading} />` : list.error && !list.data ? html`<${ErrorBox} error=${list.error} onRetry=${list.reloadLoud} />` : html`
    <${Table} caption="Identity providers" rows=${items} empty=${html`<${Empty} icon="plug" title="No identity providers" action=${canManage ? html`<${Btn} kind="primary" icon="plus" onClick=${() => edit(null)}>New provider<//>` : null}>Connect OpenID Connect, Microsoft Entra ID or LDAP so people sign in with their organisation account.<//>`} columns=${[
      { key: 'name', label: 'Provider', render: (p) => html`<div><b>${p.display_name || p.name}</b><div class="small muted mono">${p.name}</div></div>` },
      { key: 'type', label: 'Protocol', render: (p) => TYPE_LABEL[p.type] || p.type },
      { key: 'issuer', label: 'Issuer', render: (p) => html`<span class="mono small">${p.issuer || (p.config && p.config.ldap ? p.config.ldap.server_uri : '–')}</span>` },
      { key: 'map', label: 'Mappings', sort: (p) => ((p.config && p.config.role_mappings) || []).length, render: (p) => ((p.config && p.config.role_mappings) || []).length },
      { key: 'on', label: 'State', sort: (p) => (p.enabled ? 1 : 0), render: (p) => (p.enabled ? html`<${Badge} kind="ok">Enabled<//>` : html`<${Badge}>Disabled<//>`) },
      { key: 'test', label: 'Last test', sort: (p) => (p.last_test ? p.last_test.at : ''), render: (p) => (p.last_test ? html`<span class="row gap-1">${p.last_test.ok ? html`<${Badge} kind="ok">Passed<//>` : html`<${Badge} kind="bad">Failed<//>`}<${Ago} ts=${p.last_test.at} /></span>` : html`<span class="muted">never</span>`) },
      { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (p) => html`<${MenuButton} label=${`Actions for ${p.name}`} items=${[
        { label: 'Test connection', icon: 'activity', onClick: () => openOverlay(({ close }) => html`<${TestProviderDialog} prov=${p} close=${close} />`).then(() => list.reload()) },
        { label: 'Preview role mapping', icon: 'eye', onClick: () => openOverlay(({ close }) => html`<${PreviewDialog} prov=${p} close=${close} />`) },
        canManage && { sep: true }, canManage && { label: 'Edit', icon: 'edit', onClick: () => edit(p) },
        canManage && { label: p.enabled ? 'Disable' : 'Enable', icon: p.enabled ? 'stop' : 'check', onClick: () => toggle(p) },
        canManage && { label: 'Delete', icon: 'trash', onClick: () => del(p) },
      ].filter(Boolean)} />` },
    ]} />`}</div>`;
}

// ----------------------------------------------------------------------------------------------------------------------------------- certificates
function CertCard({ c }) {
  const d = c.data || {};
  const days = d.days_left;
  const kind = c.status === 'ok' ? 'ok' : c.status === 'warn' ? 'warn' : c.status === 'skipped' ? '' : 'bad';
  return html`<${Card} title=${c.name === 'cert:ca' ? 'Platform certificate authority' : c.name === 'cert:server' ? 'Server certificate' : c.name} actions=${html`<${Badge} kind=${kind}>${titleCase(c.status)}<//>`}>
    <p class="small">${c.detail}</p>
    ${days != null ? html`<div class="mt-2"><${Progress} value=${Math.min(100, (days / 3650) * 100)} kind=${days < 30 ? 'bad' : days < 90 ? 'warn' : 'ok'} label="Validity remaining" /><div class="small muted mt-1">${days} days left</div></div>` : null}
    ${d.serial ? html`<${Kv} items=${[['Serial', html`<span class="mono small">${d.serial}</span>`]]} />` : null}</${Card}>`;
}
function CertsTab() {
  const info = useAsync(() => api.get('/admin/info'), []);
  const canDiag = can('admin.diagnostics');
  const diag = useAsync(() => (canDiag ? api.post('/admin/diagnostics', { only: ['certificates'], deep_health: false }) : Promise.resolve(null)), [canDiag]);
  const nodes = useAsync(() => (can('compute.read') ? api.get('/workers').then((r) => r.items) : Promise.resolve([])), []);
  const checks = (diag.data && diag.data.checks) || [];
  const i = info.data;
  return html`<div data-testid="certs" class="col gap-4">
    <div class="grid-2">
      <${Card} title="Transport security" sub="How clients and nodes reach the control plane">
        ${i ? html`<${Kv} items=${[['Address', html`<span class="mono">${i.base_url}</span>`], ['HTTPS', i.tls ? html`<${Badge} kind="ok">On<//>` : html`<${Badge} kind="warn">Off<//>`]]} />
          ${i.tls ? null : html`<div class="mt-2"><${Banner} kind="warn" title="Traffic is not encrypted">Serve the API over HTTPS (tls.enabled) before connecting anything beyond this machine. Node channels are mutually authenticated with certificates issued by the platform CA.<//></div>`}` : html`<${Loading} />`}</${Card}>
      <${Card} title="Node identities" sub="Each node holds a certificate from the platform CA">
        ${nodes.data ? html`<p class="small">${nodes.data.filter((n) => n.state !== 'revoked').length} active node${nodes.data.filter((n) => n.state !== 'revoked').length === 1 ? '' : 's'}, ${nodes.data.filter((n) => n.state === 'revoked').length} revoked.</p>
          <p class="muted small mt-1">Revoking a node adds its certificate to the revocation list and disconnects it immediately.</p><div class="mt-2"><a class="btn" href="#/compute">Manage nodes</a></div>` : html`<${Loading} />`}</${Card}></div>
    <section aria-labelledby="cert-h"><div class="row"><h2 id="cert-h" class="h3 grow">Certificates</h2>${canDiag ? html`<${Btn} icon="refresh" busy=${diag.loading} onClick=${() => diag.reloadLoud()} data-testid="certs-check">Check now<//>` : null}</div>
      ${!canDiag ? html`<${Banner} kind="info" title="Certificate details need diagnostics access">Ask a platform or runtime administrator.<//>` : diag.loading && !diag.data ? html`<${Loading} />` : diag.error ? html`<${ErrorBox} error=${diag.error} onRetry=${diag.reloadLoud} />` : html`<div class="grid-2 mt-2">${checks.map((c) => html`<${CertCard} key=${c.name} c=${c} />`)}</div>`}</section></div>`;
}

// ----------------------------------------------------------------------------------------------------------------------------------- audit
function AuditDrawer({ rec, close }) {
  return html`<${Drawer} title=${rec.action} onClose=${() => close()}>
    <${Kv} items=${[['Time', fmtTime(rec.ts)], ['Actor', `${rec.actor_name || 'system'}${rec.actor_kind ? ` (${rec.actor_kind})` : ''}`], ['Outcome', rec.outcome], ['Resource', rec.resource_type ? `${rec.resource_type} ${rec.resource_id || ''}` : null], ['Workspace', rec.workspace_id], ['From address', rec.ip], ['Correlation id', rec.correlation_id && html`<span class="mono">${rec.correlation_id}</span>`], ['Sequence', rec.seq]]} />
    <h3 class="h4 mt-4">Details</h3><div class="mt-2"><${JsonView} value=${rec.details} depth=${3} /></div>
    <h3 class="h4 mt-4">Hash chain</h3><p class="muted small">Each record hashes the previous one, so any later change breaks the chain.</p>
    <${Kv} items=${[['Previous', html`<span class="mono small break">${rec.prev_hash}</span>`], ['This record', html`<span class="mono small break">${rec.hash}</span>`]]} /></${Drawer}>`;
}
function AuditTab() {
  const [f, setF] = useState({ action: '', actor: '', outcome: '', resource_id: '', since: '', until: '' });
  const df = useDebounced(f, 300);
  const [rows, setRows] = useState([]);
  const [next, setNext] = useState(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState(null);
  const [ver, setVer] = useState(null);
  const [verBusy, setVerBusy] = useState(false);
  const q = (before) => ({ limit: 100, before_seq: before || undefined, ...Object.fromEntries(Object.entries(df).filter(([, v]) => v).map(([k, v]) => [k, k === 'since' || k === 'until' ? new Date(v).toISOString() : v])) });
  async function load(more) {
    setLoading(true); setErr(null);
    try { const r = await api.get('/audit', q(more ? next : null)); setRows((x) => (more ? [...x, ...r.items] : r.items)); setNext(r.next_before_seq || null); } catch (e) { setErr(e); } finally { setLoading(false); }
  }
  useEffect(() => { load(false); }, [df]);
  async function verify() {
    setVerBusy(true);
    try { setVer(await api.get('/audit/verify')); } catch (e) { setVer({ error: describeError(e) }); } finally { setVerBusy(false); }
  }
  async function exp(format) {
    const r = await act(async () => api.blob('/audit/export', { format }), {});
    if (r) { downloadBlob(r, `olcf-audit.${format === 'cef' ? 'cef' : 'jsonl'}`); toast.ok('Audit export downloaded', 'The file contains the full log, oldest first.'); }
  }
  const set = (k) => (v) => setF((x) => ({ ...x, [k]: v }));
  return html`<div data-testid="audit">
    <div class="toolbar wrap">
      <${Input} label="Action" size="sm" value=${f.action} onInput=${set('action')} placeholder="auth.login" data-testid="audit-action" />
      <${Input} label="Actor" size="sm" value=${f.actor} onInput=${set('actor')} placeholder="admin" />
      <${Select} label="Outcome" size="sm" value=${f.outcome} onChange=${set('outcome')} placeholder="Any outcome" options=${[{ value: 'success', label: 'Success' }, { value: 'failure', label: 'Failure' }, { value: 'denied', label: 'Denied' }]} />
      <${Input} label="Resource id" size="sm" value=${f.resource_id} onInput=${set('resource_id')} placeholder="Resource id" />
      <${Input} label="From" title="From" type="datetime-local" size="sm" value=${f.since} onInput=${set('since')} />
      <${Input} label="To" title="To" type="datetime-local" size="sm" value=${f.until} onInput=${set('until')} />
      <span class="grow"></span>
      <${Btn} icon="shield-check" busy=${verBusy} onClick=${verify} data-testid="audit-verify">Verify chain<//>
      <${MenuButton} label="Export audit log" icon="download" iconOnly=${false} kind="" size="" items=${[{ label: 'JSON lines', icon: 'file-code', onClick: () => exp('jsonl') }, { label: 'CEF for a SIEM', icon: 'file-text', onClick: () => exp('cef') }]}>Export<//></div>
    ${ver ? (ver.error ? html`<${Banner} kind="bad" title="Could not verify the chain">${ver.error}<//>` : html`<div data-testid="audit-verdict"><${Banner} kind=${ver.ok ? 'ok' : 'bad'} title=${ver.ok ? 'The audit log is intact' : 'The audit chain is broken'}>${ver.ok ? `${ver.checked} records checked${ver.first_seq > 1 ? ` from sequence ${ver.first_seq}` : ''}. Latest hash ` : `The record at sequence ${ver.broken_at_seq} does not match its hash (checked ${ver.checked}). `}${ver.head ? html`<span class="mono small break">${ver.head.slice(0, 20)}…</span>` : null}${ver.anchors ? ` · ${ver.anchors} anchored archive${ver.anchors === 1 ? '' : 's'}` : ''}<//></div>`) : null}
    ${err && !rows.length ? html`<${ErrorBox} error=${err} onRetry=${() => load(false)} />` : html`
    <${Table} caption="Audit log" rows=${rows} rowKey=${(r) => r.id} onRowClick=${(r) => openOverlay(({ close }) => html`<${AuditDrawer} rec=${r} close=${close} />`)} empty=${loading ? html`<${Loading} />` : html`<${Empty} icon="history" title="No audit records match">Loosen the filters.<//>`} columns=${[
      { key: 'seq', label: '#', sort: (r) => r.seq, render: (r) => html`<span class="mono small muted">${r.seq}</span>` },
      { key: 'ts', label: 'Time', sort: (r) => r.ts, render: (r) => html`<time datetime=${r.ts} title=${r.ts}>${fmtTime(r.ts)}</time>` },
      { key: 'actor', label: 'Actor', sort: (r) => r.actor_name || '', render: (r) => r.actor_name || html`<span class="muted">system</span>` },
      { key: 'action', label: 'Action', render: (r) => html`<span class="mono small">${r.action}</span>` },
      { key: 'res', label: 'Resource', sort: (r) => r.resource_id || '', render: (r) => (r.resource_type ? html`<span class="small">${r.resource_type} <span class="mono muted">${shortId(r.resource_id)}</span></span>` : '') },
      { key: 'out', label: 'Outcome', render: (r) => (r.outcome === 'success' ? html`<${Badge} kind="ok">Success<//>` : html`<${Badge} kind="bad">${titleCase(r.outcome)}<//>`) },
      { key: 'ip', label: 'From', render: (r) => html`<span class="mono small muted">${r.ip || ''}</span>` },
    ]} />`}
    <${LoadMore} cursor=${next} loading=${loading} onMore=${() => load(true)} /></div>`;
}

// ----------------------------------------------------------------------------------------------------------------------------------- my account
function AccountTab() {
  const me = useAsync(() => api.get('/identity/me'), []);
  const [cur, setCur] = useState('');
  const [a, setA] = useState('');
  const [b, setB] = useState('');
  const [busy, setBusy] = useState(false);
  const d = me.data;
  const federated = d && d.principal.source && d.principal.source !== 'local' && d.principal.source !== 'bootstrap';
  const mismatch = b && a !== b;
  async function save() {
    setBusy(true);
    const r = await act(() => api.post('/identity/password', { current_password: cur, new_password: a }), { ok: 'Password changed' });
    setBusy(false);
    if (r !== undefined) { setCur(''); setA(''); setB(''); }
  }
  if (me.loading && !d) return html`<${Loading} />`;
  if (me.error && !d) return html`<${ErrorBox} error=${me.error} onRetry=${me.reloadLoud} />`;
  return html`<div data-testid="account" class="grid-2">
    <${Card} title="You">
      <${Kv} items=${[['User name', html`<span class="mono">${d.principal.name}</span>`], ['Display name', d.principal.display_name], ['Email', d.principal.email], ['Sign-in source', titleCase(d.principal.source || 'local')], ['Last sign-in', d.principal.last_login_at ? fmtTime(d.principal.last_login_at) : null],
        ['Roles', html`<span class="chips-static">${d.bindings.map((x, i) => html`<span key=${i} class="tag">${x.role}${x.scope_type === 'workspace' ? ' (workspace)' : ''}</span>`)}</span>`]]} /></${Card}>
    <${Card} title="Change password">${federated ? html`<p class="muted small">Your password is managed by your identity provider.</p>` : html`<div class="col gap-3" role="form" aria-label="Change password">
      <${Input} label="Current password" type="password" autocomplete="current-password" value=${cur} onInput=${setCur} />
      <${Input} label="New password" type="password" autocomplete="new-password" value=${a} onInput=${setA} hint="At least 12 characters." />
      <${Input} label="Repeat new password" type="password" autocomplete="new-password" value=${b} onInput=${setB} error=${mismatch ? 'The two passwords do not match.' : undefined} />
      <div><${Btn} kind="primary" busy=${busy} disabled=${!cur || a.length < 12 || a !== b} onClick=${save} data-testid="password-save">Change password<//></div></div>`}</${Card}></div>`;
}

// ----------------------------------------------------------------------------------------------------------------------------------- page
export function SecurityPage() {
  const [tab, setTab] = useQueryState('tab', can('identity.read') ? 'users' : 'account');
  const tabs = [
    { id: 'account', label: 'My account' },
    can('identity.read') && { id: 'users', label: 'Accounts' },
    can('identity.read') && { id: 'access', label: 'Roles and access' },
    { id: 'tokens', label: 'API tokens' },
    can('identity.federation.manage') && { id: 'providers', label: 'Identity providers' },
    (can('admin.health') || can('compute.read')) && { id: 'certs', label: 'Certificates' },
    can('audit.read') && { id: 'audit', label: 'Audit log' },
  ].filter(Boolean);
  const cur = tabs.find((t) => t.id === tab) ? tab : tabs[0].id;
  return html`<div class="page" data-testid="security"><${PageHead} icon="shield" title="Security" sub="Who can sign in, what they may do, and a tamper-evident record of what they did." />
    <${Tabs} tabs=${tabs} value=${cur} onChange=${setTab} label="Security sections" />
    <${TabPanel} id="account" value=${cur}><${AccountTab} /><//><${TabPanel} id="users" value=${cur}><${UsersTab} /><//><${TabPanel} id="access" value=${cur}><${AccessTab} /><//><${TabPanel} id="tokens" value=${cur}><${TokensTab} /><//>
    <${TabPanel} id="providers" value=${cur}><${ProvidersTab} /><//><${TabPanel} id="certs" value=${cur}><${CertsTab} /><//><${TabPanel} id="audit" value=${cur}><${AuditTab} /><//></div>`;
}
