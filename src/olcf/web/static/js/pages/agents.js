// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// AI agents (MCP): endpoint, scoped token creation, copy-paste client configuration, tools, a live try-it console, the call audit log and token management.
import { html, useState, useEffect, useRef, useMemo, useAsync, usePoll } from '../core/h.js';
import { api, can, getAccessToken } from '../api/client.js';
import { McpClient, McpError } from '../api/mcp.js';
import { Btn, Badge, Banner, Empty, ErrorBox, Loading, Tabs, TabPanel, PageHead, Select, Input, Textarea, Check, Field, Code, CopyBtn, Kv, Card, Seg } from '../ui/basic.js';
import { Table, JsonView } from '../ui/data.js';
import { Icon } from '../ui/icons.js';
import { act, confirmDialog, toast } from '../ui/overlay.js';
import { enc, fmtTime, fmtMs, titleCase, shortId, jsonPretty, fmtBytes } from '../core/util.js';
import { useQueryState, Ago, openHealthTest } from './common.js';

const PLACEHOLDER = 'olcf_pat_...';

function configs(info, token) {
  const url = info.endpoint;
  const tk = token || PLACEHOLDER;
  const desktop = { mcpServers: { olcf: { command: 'python', args: ['-m', 'olcf.mcp.stdio', '--url', url], env: { OLCF_MCP_TOKEN: tk } } } };
  const http = { type: 'http', url, headers: { Authorization: `Bearer ${tk}` } };
  return [
    { id: 'code', label: 'Claude Code', lang: 'shell', note: 'Run once in a terminal. Claude Code speaks Streamable HTTP, so no bridge is needed.', text: `claude mcp add --transport http olcf ${url} \\\n  --header "Authorization: Bearer ${tk}"` },
    { id: 'desktop', label: 'Claude Desktop', lang: 'json', note: 'Add to claude_desktop_config.json (macOS: ~/Library/Application Support/Claude, Windows: %APPDATA%\\Claude), then restart. Needs the olcf package in the Python that "command" points to; it provides the small stdio bridge.', text: JSON.stringify(desktop, null, 2) },
    { id: 'http', label: 'Other HTTP clients', lang: 'json', note: 'Any client with native Streamable HTTP support.', text: JSON.stringify(http, null, 2) },
    { id: 'python', label: 'Python SDK', lang: 'python', note: 'With the official mcp package.', text: `import asyncio\nfrom mcp.client.client import Client\nfrom mcp.client.streamable_http import streamable_http_client\nfrom mcp.shared._httpx_utils import create_mcp_http_client\n\n\nasync def main() -> None:\n    async with create_mcp_http_client(headers={"Authorization": "Bearer ${tk}"}) as http:\n        async with Client(streamable_http_client("${url}", http_client=http)) as client:\n            tools = await client.list_tools()\n            print([t.name for t in tools.tools])\n            result = await client.call_tool("list_workspaces", {})\n            print(result.content[0].text)\n\n\nasyncio.run(main())` },
    { id: 'curl', label: 'curl', lang: 'shell', note: 'The handshake by hand: the first answer carries the Mcp-Session-Id header to send on later calls.', text: `curl -si -X POST ${url} \\\n  -H "Authorization: Bearer ${tk}" \\\n  -H "Content-Type: application/json" \\\n  -H "Accept: application/json, text/event-stream" \\\n  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"1"}}}'` },
  ];
}

// ---------------------------------------------------------------------------------------------------------------------------------------- connect
function CreateToken({ onCreated }) {
  const [name, setName] = useState('my-agent');
  const [write, setWrite] = useState(false);
  const [ttl, setTtl] = useState('30');
  const [busy, setBusy] = useState(false);
  const allowed = can('mcp.use');
  async function create() {
    setBusy(true);
    const r = await act(() => api.post('/identity/tokens', { name: name.trim(), scopes: write ? ['mcp', 'mcp:write'] : ['mcp'], ttl_days: Number(ttl) || null }), { ok: 'Token created' });
    setBusy(false);
    if (r) onCreated(r);
  }
  return html`<${Card} title="1. Create a token for the agent" sub="A token carries your access, limited to MCP. The agent sees only what you can see.">
    ${allowed ? null : html`<${Banner} kind="warn" title="Your role cannot use MCP">Ask an administrator for a role with the mcp.use permission, such as Contributor. Viewers cannot use MCP.<//>`}
    <div class="col gap-3 mt-2"><div class="grid c2"><${Input} label="Token name" value=${name} onInput=${setName} data-testid="mcp-token-name" /><${Select} label="Expires after" value=${ttl} onChange=${setTtl} options=${[['1', '1 day'], ['7', '7 days'], ['30', '30 days'], ['90', '90 days']].map(([value, label]) => ({ value, label }))} /></div>
      <${Check} label=${html`Allow the agent to start and cancel notebook jobs <span class="muted small">(adds the mcp:write scope)</span>`} checked=${write} onChange=${setWrite} />
      <div><${Btn} kind="primary" icon="key" busy=${busy} disabled=${!name.trim() || !allowed} onClick=${create} data-testid="mcp-token-create">Create token<//></div></div><//>`;
}

function ConnectPanel({ info }) {
  const [created, setCreated] = useState(null);
  const [tab, setTab] = useState('code');
  const list = useMemo(() => configs(info, created && created.token), [info, created]);
  const cur = list.find((c) => c.id === tab) || list[0];
  return html`<div class="grid c2 agents-connect">
    <div class="col gap-4"><${CreateToken} onCreated=${(r) => { setCreated(r); window.dispatchEvent(new CustomEvent('olcf-mcp-token', { detail: r })); }} />
      ${created ? html`<${Banner} kind="ok" title=${`Token "${created.name || 'created'}" is ready`}><div class="small">Copy it now: it is shown once and cannot be recovered. It expires ${fmtTime(created.expires_at)}.</div><div class="mt-2"><${Code} text=${created.token} /></div>${created.note ? html`<div class="small muted mt-2">${created.note}</div>` : null}<//>` : null}</div>
    <${Card} title="2. Add it to your agent" sub=${created ? 'The token is already filled in.' : 'Create a token and it is filled in for you.'}>
      <${Tabs} value=${cur.id} onChange=${setTab} tabs=${list.map((c) => ({ id: c.id, label: c.label }))} label="Client" />
      <p class="small muted mt-3">${cur.note}</p><div class="mt-2" data-testid="mcp-config"><${Code} text=${cur.text} lang=${cur.lang} /></div><//></div>`;
}

// ---------------------------------------------------------------------------------------------------------------------------------------- tools
function ToolsPanel({ info, live }) {
  const schema = useMemo(() => Object.fromEntries((live || []).map((t) => [t.name, t])), [live]);
  return html`<${Table} caption="MCP tools" rows=${info.tools.map((t) => ({ ...t, id: t.name }))} columns=${[
    { key: 'name', label: 'Tool', render: (t) => html`<b class="mono">${t.name}</b><div class="small muted">${t.title}</div>` },
    { key: 'desc', label: 'What it does', render: (t) => html`<span class="small">${t.description}</span>`, sortable: false },
    { key: 'args', label: 'Arguments', render: (t) => { const s = schema[t.name]; if (!s) return html`<span class="muted small">connect the console to see</span>`; const p = Object.keys((s.inputSchema || {}).properties || {}); const req = (s.inputSchema || {}).required || []; return p.length ? html`<span class="small mono">${p.map((k) => (req.includes(k) ? k + '*' : k)).join(', ')}</span>` : html`<span class="muted small">none</span>`; }, sortable: false },
    { key: 'scope', label: 'Scope', render: (t) => (t.write ? html`<${Badge} kind="warn">mcp:write<//>` : html`<${Badge}>mcp<//>`), sort: (t) => (t.write ? 1 : 0) },
  ]} />`;
}

// ---------------------------------------------------------------------------------------------------------------------------------------- try-it console
function SchemaForm({ schema, value, onChange }) {
  const props = (schema && schema.properties) || {};
  const req = (schema && schema.required) || [];
  const keys = Object.keys(props);
  if (!keys.length) return html`<p class="small muted">This tool takes no arguments.</p>`;
  const set = (k, v) => { const n = { ...value }; if (v === undefined || v === '') delete n[k]; else n[k] = v; onChange(n); };
  return html`<div class="col gap-3">${keys.map((k) => { const p = props[k]; const label = html`${k}${req.includes(k) ? ' *' : ''}`; const hint = p.description;
    const t = Array.isArray(p.type) ? p.type.find((x) => x !== 'null') : p.type;
    if (p.enum) return html`<${Select} key=${k} label=${label} value=${value[k] ?? ''} onChange=${(v) => set(k, v)} placeholder="" options=${p.enum} hint=${hint} />`;
    if (t === 'boolean') return html`<${Check} key=${k} label=${html`${label} <span class="muted small">${hint || ''}</span>`} checked=${!!value[k]} onChange=${(v) => set(k, v)} />`;
    if (t === 'integer' || t === 'number') return html`<${Input} key=${k} label=${label} type="number" step=${t === 'number' ? 'any' : '1'} value=${value[k] ?? ''} onInput=${(v) => set(k, v === '' ? undefined : Number(v))} hint=${hint} />`;
    if (t === 'array' || t === 'object') return html`<${JsonArg} key=${k} label=${label} hint=${hint} value=${value[k]} onChange=${(v) => set(k, v)} />`;
    const long = /sql|query|code|text/i.test(k);
    return long ? html`<${Textarea} key=${k} label=${label} mono rows=${3} value=${value[k] ?? ''} onInput=${(v) => set(k, v)} hint=${hint} />` : html`<${Input} key=${k} label=${label} value=${value[k] ?? ''} onInput=${(v) => set(k, v)} hint=${hint} />`; })}</div>`;
}
function JsonArg({ label, hint, value, onChange }) {
  const [t, setT] = useState(value === undefined ? '' : JSON.stringify(value));
  const [err, setErr] = useState('');
  return html`<${Textarea} label=${label} mono rows=${2} value=${t} hint=${hint} error=${err} onInput=${(x) => { setT(x); if (!x.trim()) { setErr(''); onChange(undefined); return; } try { onChange(JSON.parse(x)); setErr(''); } catch { setErr('Not valid JSON'); } }} />`;
}

function resultView(res) {
  const sc = res.structuredContent;
  const text = (res.content || []).filter((c) => c.type === 'text').map((c) => c.text).join('\n');
  let parsed = sc;
  if (!parsed && text) { try { parsed = JSON.parse(text); } catch { parsed = null; } }
  return { parsed, text };
}

function Console({ info, onTools, tokenFromCreate }) {
  const [mode, setMode] = useState('session');
  const [token, setToken] = useState('');
  const [status, setStatus] = useState('idle');
  const [error, setError] = useState(null);
  const [tools, setTools] = useState([]);
  const [sel, setSel] = useState('');
  const [args, setArgs] = useState({});
  const [raw, setRaw] = useState(false);
  const [rawText, setRawText] = useState('{}');
  const [res, setRes] = useState(null);
  const [calling, setCalling] = useState(false);
  const [log, setLog] = useState([]);
  const client = useRef(null);
  const sample = useAsync(() => api.get('/sample/status').catch(() => null), []);
  useEffect(() => { if (tokenFromCreate) { setMode('token'); setToken(tokenFromCreate); } }, [tokenFromCreate]);
  useEffect(() => () => { if (client.current) client.current.close(); }, []);
  async function connect() {
    if (client.current) await client.current.close();
    setStatus('connecting'); setError(null); setRes(null); setLog([]);
    const c = new McpClient({ token: mode === 'session' ? getAccessToken() : token.trim(), onLog: (e) => setLog((l) => [...l, e]) });
    client.current = c;
    try { await c.connect(); setTools(c.tools); onTools(c.tools); setStatus('ready'); if (!sel && c.tools[0]) pick(c.tools[0].name, c.tools); } catch (e) { setStatus('error'); setError(e); }
  }
  async function disconnect() { if (client.current) await client.current.close(); client.current = null; setStatus('idle'); setTools([]); }
  function pick(name, list = tools) { setSel(name); setArgs({}); setRawText('{}'); setRes(null); if (!list.find((t) => t.name === name)) return; }
  const tool = tools.find((t) => t.name === sel);
  let rawArgs = null; let rawErr = '';
  if (raw) { try { rawArgs = JSON.parse(rawText || '{}'); } catch { rawErr = 'Not valid JSON'; } }
  async function call() {
    setCalling(true); setRes(null);
    try { const r = await client.current.call(sel, raw ? rawArgs : args); setRes({ ok: !r.isError, result: r }); } catch (e) { setRes({ ok: false, error: e }); }
    setCalling(false);
  }
  const examples = [
    { label: 'List workspaces', tool: 'list_workspaces', args: {} },
    sample.data && sample.data.installed && { label: 'Count orders', tool: 'run_sql', args: { sql: 'SELECT count(*) AS orders, round(sum(net_amount), 2) AS revenue FROM retail_lakehouse.fact_sales', workspace: sample.data.workspace } },
    sample.data && sample.data.installed && { label: 'Describe fact_sales', tool: 'describe_table', args: { table: 'retail_lakehouse.fact_sales', workspace: sample.data.workspace } },
    sample.data && sample.data.installed && { label: 'Run the sales dashboard', tool: 'list_reports', args: { workspace: sample.data.workspace } },
  ].filter(Boolean);
  function example(e) { if (!tools.find((t) => t.name === e.tool)) return; setSel(e.tool); setArgs(e.args); setRawText(JSON.stringify(e.args, null, 2)); setRes(null); }
  const v = res && res.result ? resultView(res.result) : null;
  return html`<div class="console" data-testid="mcp-console">
    <div class="console-bar"><${Seg} label="Authenticate with" value=${mode} onChange=${setMode} options=${[{ value: 'session', label: 'My console session' }, { value: 'token', label: 'A token' }]} />
      ${mode === 'token' ? html`<input class="input mono grow" type="password" autocomplete="off" aria-label="MCP token" placeholder="olcf_pat_..." value=${token} onInput=${(e) => setToken(e.target.value)} data-testid="mcp-token-input" />` : html`<span class="small muted grow">Calls run as you, with all your permissions. Use a token to test what a scoped agent would see.</span>`}
      ${status === 'ready' ? html`<${Btn} onClick=${disconnect}>Disconnect<//>` : null}
      <${Btn} kind="primary" icon="plug" busy=${status === 'connecting'} disabled=${mode === 'token' && !token.trim()} onClick=${connect} data-testid="mcp-connect">${status === 'ready' ? 'Reconnect' : 'Connect and handshake'}<//></div>
    ${status === 'error' ? html`<${Banner} kind="bad" title="Could not connect">${error && error.message}${error && error.status === 401 ? html`<div class="small mt-1">The token is missing, expired or revoked.</div>` : error && error.status === 403 ? html`<div class="small mt-1">The token needs the mcp scope and its owner the mcp.use permission.</div>` : null}<//>` : null}
    ${status === 'ready' ? html`<div class="row-wrap small muted mb-3" data-testid="mcp-ready"><${Icon} name="check-circle" size="sm" /> Connected to <b>${client.current.info.serverInfo.name}</b> ${client.current.info.serverInfo.version} · protocol ${client.current.info.protocolVersion} · ${tools.length} tools · session <span class="mono">${shortId(client.current.session, 8)}</span></div>
      <div class="console-main">
        <aside class="console-tools" aria-label="Tools"><ul>${tools.map((t) => html`<li key=${t.name}><button type="button" class=${sel === t.name ? 'on' : ''} aria-current=${sel === t.name} onClick=${() => pick(t.name)} data-testid=${'tool-' + t.name}><span class="mono">${t.name}</span>${(t.annotations || {}).readOnlyHint === false ? html`<${Badge} kind="warn">write<//>` : null}</button></li>`)}</ul></aside>
        <section class="console-call">${tool ? html`<h3 class="mono">${tool.name}</h3><p class="small muted">${tool.description}</p>
          ${examples.length ? html`<div class="chips mb-3" aria-label="Examples">${examples.map((e) => html`<button key=${e.label} type="button" class="chip" onClick=${() => example(e)}>${e.label}</button>`)}</div>` : null}
          <div class="row mb-2"><${Check} label="Edit the arguments as JSON" checked=${raw} onChange=${(x) => { setRaw(x); if (x) setRawText(JSON.stringify(args, null, 2)); else if (rawArgs) setArgs(rawArgs); }} /></div>
          ${raw ? html`<${Textarea} label="Arguments" mono rows=${6} value=${rawText} onInput=${setRawText} error=${rawErr} data-testid="mcp-args-json" />` : html`<${SchemaForm} key=${tool.name} schema=${tool.inputSchema} value=${args} onChange=${setArgs} />`}
          <div class="mt-3"><${Btn} kind="primary" icon="play" busy=${calling} disabled=${!!rawErr} onClick=${call} data-testid="mcp-call">Call ${tool.name}<//></div>` : html`<${Empty} icon="bot" title="Pick a tool" />`}
          ${res ? html`<div class="mt-4" data-testid="mcp-result">${res.error ? html`<${Banner} kind="bad" title="The call failed">${res.error.message}${res.error.code != null ? html` <span class="mono small">(${res.error.code})</span>` : ''}<//>`
            : html`${res.result.isError ? html`<${Banner} kind="bad" title="The tool reported an error">${v.text}<//>` : html`<div class="small muted mb-2">${log.length ? `${log[log.length - 1].ms} ms` : ''} · ${fmtBytes(new Blob([v.text || '']).size)}</div>`}
              ${!res.result.isError ? (v.parsed ? html`<${JsonView} value=${v.parsed} depth=${3} />` : html`<pre class="mono small wrap p-3">${v.text}</pre>`) : null}`}</div>` : null}
        </section></div>
      <details class="mt-4"><summary class="small">Protocol log (${log.length} message${log.length === 1 ? '' : 's'})</summary><div class="col gap-2 mt-2">${log.map((e, i) => html`<div key=${i} class="mcp-log"><div class="row"><b class="mono small">${e.method}</b><${Badge} kind=${e.status < 300 ? 'ok' : 'bad'}>${e.status}<//><span class="small muted">${e.ms} ms</span></div><${JsonView} value=${{ request: e.request, response: e.response }} depth=${1} /></div>`)}</div></details>` : status === 'idle' ? html`<${Empty} icon="bot" title="Try the endpoint from here">Connect to run the real MCP handshake against this server, list its tools and call them, exactly as an agent would.<//>` : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------------------------------- calls and tokens
function CallsPanel() {
  const [tool, setTool] = useQueryState('tool', '');
  const [status, setStatus] = useQueryState('cstatus', '');
  const [mine, setMine] = useState(true);
  const calls = usePoll(() => api.get('/mcp/calls', { tool: tool || undefined, status: status || undefined, mine: mine || undefined, limit: 100 }), 6000, [tool, status, mine]);
  const items = ((calls.data && calls.data.items) || []).filter((c) => c.method === 'tools/call' || tool === '');
  return html`<div data-testid="mcp-calls"><div class="toolbar"><input class="input" style="max-width:220px" placeholder="Tool name" aria-label="Filter by tool" value=${tool} onInput=${(e) => setTool(e.target.value)} />
    <${Select} label="Outcome" size="sm" value=${status} onChange=${setStatus} placeholder="Any outcome" options=${['ok', 'error', 'denied'].map((s) => ({ value: s, label: titleCase(s) }))} />
    ${can('mcp.admin') || can('audit.read') ? html`<${Check} label="Only mine" checked=${mine} onChange=${setMine} />` : null}</div>
    ${calls.loading && !calls.data ? html`<${Loading} />` : calls.error && !calls.data ? html`<${ErrorBox} error=${calls.error} onRetry=${calls.reloadLoud} />` : html`
    <${Table} caption="MCP calls" rows=${items} empty=${html`<${Empty} icon="history" title="No calls yet">Calls from agents and the console below are recorded here, with secrets removed.<//>`} initialSort=${{ key: 'when', dir: -1 }} columns=${[
      { key: 'when', label: 'When', render: (c) => html`<${Ago} ts=${c.created_at} />`, sort: (c) => c.created_at },
      { key: 'tool', label: 'Call', render: (c) => html`<b class="mono">${c.tool || c.method}</b><div class="small muted">${c.client_name || ''}</div>`, sort: (c) => c.tool || c.method },
      { key: 'who', label: 'Caller', render: (c) => c.principal_name || shortId(c.principal_id) },
      { key: 'args', label: 'Arguments', render: (c) => (c.arguments ? html`<code class="small mono">${JSON.stringify(c.arguments).slice(0, 90)}</code>` : html`<span class="muted">–</span>`), sortable: false },
      { key: 'st', label: 'Outcome', render: (c) => html`<${Badge} kind=${c.status === 'ok' ? 'ok' : 'bad'}>${titleCase(c.status)}<//>${c.error_message ? html`<div class="small muted">${c.error_message}</div>` : ''}`, sort: (c) => c.status },
      { key: 'ms', label: 'Time', num: true, render: (c) => (c.elapsed_ms != null ? fmtMs(c.elapsed_ms) : '–'), sort: (c) => c.elapsed_ms || 0 },
      { key: 'size', label: 'Result', num: true, render: (c) => (c.result_bytes != null ? fmtBytes(c.result_bytes) + (c.truncated ? ' (cut)' : '') : '–'), sort: (c) => c.result_bytes || 0 },
    ]} />`}</div>`;
}
function TokensPanel() {
  const t = usePoll(() => api.get('/identity/tokens').then((r) => r.items), 15000, []);
  async function revoke(x) { if (await confirmDialog({ title: `Revoke ${x.name}?`, danger: true, confirmLabel: 'Revoke token', body: 'Agents using it stop working immediately and their sessions end.' }) && (await act(() => api.del(`/identity/tokens/${enc(x.id)}`), { ok: 'Token revoked' })) !== undefined) t.reload(); }
  if (t.loading && !t.data) return html`<${Loading} />`;
  if (t.error && !t.data) return html`<${ErrorBox} error=${t.error} onRetry=${t.reloadLoud} />`;
  const rows = t.data.filter((x) => (x.scopes || []).some((s) => s.startsWith('mcp')));
  return html`<div data-testid="mcp-tokens"><${Table} caption="Agent tokens" rows=${rows} empty=${html`<${Empty} icon="key" title="No agent tokens">Create one under Connect.<//>`} columns=${[
    { key: 'name', label: 'Token', render: (x) => html`<b>${x.name}</b><div class="small muted mono">${x.prefix}…</div>` },
    { key: 'scopes', label: 'Scopes', render: (x) => html`<div class="row-wrap">${x.scopes.map((s) => html`<${Badge} key=${s} kind=${s === 'mcp:write' ? 'warn' : ''}>${s}<//>`)}</div>`, sortable: false },
    { key: 'exp', label: 'Expires', render: (x) => (x.expires_at ? fmtTime(x.expires_at) : 'Never'), sort: (x) => x.expires_at || '' },
    { key: 'used', label: 'Last used', render: (x) => (x.last_used_at ? html`<${Ago} ts=${x.last_used_at} />` : html`<span class="muted">never</span>`), sort: (x) => x.last_used_at || '' },
    { key: 'act', label: html`<span class="sr-only">Actions</span>`, sortable: false, render: (x) => html`<${Btn} size="sm" kind="danger" onClick=${() => revoke(x)}>Revoke<//>` },
  ]} /></div>`;
}

// ---------------------------------------------------------------------------------------------------------------------------------------- page
export function AgentsPage() {
  const [tab, setTab] = useQueryState('tab', 'connect');
  const info = useAsync(() => api.get('/mcp/info'), []);
  const [live, setLive] = useState(null);
  const [fresh, setFresh] = useState('');
  useEffect(() => { const h = (e) => setFresh(e.detail.token); window.addEventListener('olcf-mcp-token', h); return () => window.removeEventListener('olcf-mcp-token', h); }, []);
  if (info.loading && !info.data) return html`<div class="page"><${Loading} /></div>`;
  if (info.error) return html`<div class="page"><${PageHead} title="AI agents" icon="bot" /><${ErrorBox} error=${info.error} onRetry=${info.reloadLoud} /></div>`;
  const d = info.data;
  return html`<div class="page wide"><${PageHead} title="AI agents" icon="bot" sub="Let Claude and other MCP clients explore your lakehouse and run read-only SQL, with exactly your permissions and a record of every call."
    badge=${d.enabled ? html`<${Badge} kind="ok">Endpoint on<//>` : html`<${Badge} kind="bad">Endpoint off<//>`} actions=${html`<${Btn} icon="activity" onClick=${() => openHealthTest('mcp')} data-testid="mcp-health">Test the endpoint<//>`} />
    ${d.enabled ? null : html`<${Banner} kind="warn" title="MCP is switched off">An administrator can turn it on with the setting mcp.enabled.<//>`}
    <${Card} class="mb-4"><div class="endpoint"><div class="grow"><div class="small muted">Endpoint</div><div class="endpoint-url mono" data-testid="mcp-endpoint">${d.endpoint}</div></div><${CopyBtn} text=${d.endpoint} label="Copy URL" /></div>
      <div class="row-wrap mt-3 small muted"><span>Transport: ${d.transport}</span><span>Protocol ${d.protocol_versions.join(', ')}</span><span>${d.limits.rate_limit_per_minute} requests/min</span><span>${d.limits.max_rows} rows/query</span><span>${fmtBytes(d.limits.max_result_bytes)} per result</span><span>${d.limits.sql_timeout_seconds}s SQL timeout</span><span>${d.your_active_sessions} active session${d.your_active_sessions === 1 ? '' : 's'}</span></div><//>
    <${Tabs} value=${tab} onChange=${setTab} tabs=${[{ id: 'connect', label: 'Connect' }, { id: 'tools', label: 'Tools', count: d.tools.length }, { id: 'try', label: 'Try it' }, { id: 'calls', label: 'Calls' }, { id: 'tokens', label: 'Tokens' }]} />
    <div class="mt-4"><${TabPanel} id="connect" value=${tab}><${ConnectPanel} info=${d} /><//>
      <${TabPanel} id="tools" value=${tab}><${ToolsPanel} info=${d} live=${live} /><//>
      <${TabPanel} id="try" value=${tab}><${Console} info=${d} onTools=${setLive} tokenFromCreate=${fresh} /><//>
      <${TabPanel} id="calls" value=${tab}><${CallsPanel} /><//>
      <${TabPanel} id="tokens" value=${tab}><${TokensPanel} /><//></div></div>`;
}
