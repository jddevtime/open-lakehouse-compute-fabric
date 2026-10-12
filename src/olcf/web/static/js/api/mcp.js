// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// A small Model Context Protocol client (Streamable HTTP) for the in-browser "try it" console: initialize handshake, tools/list, tools/call, session close.
import { hex } from '../core/util.js';

const PROTOCOL = '2025-06-18';

export class McpError extends Error {
  constructor(message, { status, code, data } = {}) { super(message); this.name = 'McpError'; this.status = status; this.code = code; this.data = data; }
}

/** Pull the JSON-RPC message for `id` out of a JSON or text/event-stream body. */
function parseBody(text, type, id) {
  if (!text) return null;
  if (type.includes('text/event-stream')) {
    let found = null;
    for (const block of text.split(/\n\n+/)) {
      const data = block.split('\n').filter((l) => l.startsWith('data:')).map((l) => l.slice(5).trim()).join('\n');
      if (!data) continue;
      try { const m = JSON.parse(data); if (m.id === id || found === null) found = m; } catch { /* ignore keep-alive comments */ }
    }
    return found;
  }
  try { return JSON.parse(text); } catch { return null; }
}

export class McpClient {
  /** @param {{url?: string, token: string, onLog?: (entry: object) => void}} o */
  constructor({ url = '/mcp', token, onLog }) {
    this.url = url; this.token = token; this.onLog = onLog || (() => {}); this.session = null; this.seq = 0; this.info = null; this.tools = [];
  }

  headers() {
    const h = { 'Content-Type': 'application/json', Accept: 'application/json, text/event-stream', Authorization: `Bearer ${this.token}`, 'X-Correlation-Id': hex(16) };
    if (this.session) { h['Mcp-Session-Id'] = this.session; h['MCP-Protocol-Version'] = PROTOCOL; }
    return h;
  }

  async rpc(method, params, { notify = false } = {}) {
    const id = notify ? undefined : ++this.seq;
    const body = { jsonrpc: '2.0', ...(notify ? {} : { id }), method, ...(params !== undefined ? { params } : {}) };
    const t0 = performance.now();
    const res = await fetch(this.url, { method: 'POST', headers: this.headers(), body: JSON.stringify(body), credentials: 'omit' });
    const sid = res.headers.get('mcp-session-id');
    if (sid && !this.session) this.session = sid;
    const text = await res.text();
    const msg = notify ? null : parseBody(text, res.headers.get('content-type') || '', id);
    const ms = Math.round(performance.now() - t0);
    this.onLog({ method, request: body, status: res.status, response: msg || (text ? text.slice(0, 2000) : null), ms });
    if (!res.ok && !(msg && msg.error)) {
      let m = `The server answered ${res.status}`;
      try { const j = JSON.parse(text); m = (j.error && (j.error.message || j.error)) || j.detail || m; } catch { /* not json */ }
      throw new McpError(String(m), { status: res.status });
    }
    if (notify) return null;
    if (!msg) throw new McpError('The server sent no JSON-RPC answer', { status: res.status });
    if (msg.error) throw new McpError(msg.error.message || 'MCP error', { status: res.status, code: msg.error.code, data: msg.error.data });
    return msg.result;
  }

  async connect() {
    this.info = await this.rpc('initialize', { protocolVersion: PROTOCOL, capabilities: {}, clientInfo: { name: 'olcf-console', version: '1' } });
    await this.rpc('notifications/initialized', undefined, { notify: true });
    const r = await this.rpc('tools/list', {});
    this.tools = r.tools || [];
    return this.info;
  }

  call(name, args) { return this.rpc('tools/call', { name, arguments: args || {} }); }

  async close() {
    if (!this.session) return;
    try { await fetch(this.url, { method: 'DELETE', headers: this.headers(), credentials: 'omit' }); } catch { /* best effort */ }
    this.session = null;
  }
}
