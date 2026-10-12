// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Kernel session controller: finds or starts the notebook's session, speaks the session WebSocket protocol (CONTRACTS 4.4 / C6) and reconnects.
import { api, openSocket } from '../api/client.js';
import { enc } from '../core/util.js';

/** states: none | connecting | starting | idle | busy | dead | reconnecting */
export class Kernel {
  constructor(handlers) {
    this.h = handlers; // {onState(state, info), onOutput(cellId, output, replay), onReply(cellId, msg), onAccepted(cellId), onSpark(info), onReplayDone()}
    this.state = 'none';
    this.session = null;
    this.sock = null;
    this.spark = null;
    this.reqs = new Map();
    this.seq = 0;
    this.closed = false;
  }

  set(state, detail) { this.state = state; this.detail = detail; this.h.onState && this.h.onState(state, { session: this.session, detail, spark: this.spark }); }

  /** Attach to the newest live session of this notebook, if any. */
  async attach(notebookId, workspaceId) {
    let items = [];
    try {
      const r = await api.get('/sessions', { mine: true, workspace_id: workspaceId, limit: 100 });
      items = (r.items || []).filter((s) => s.notebook_id === notebookId && ['starting', 'idle', 'busy'].includes(s.state));
    } catch { /* listing is best effort */ }
    if (!items.length) { this.set('none'); return null; }
    items.sort((a, b) => String(b.created_at || '').localeCompare(String(a.created_at || '')));
    this.session = items[0];
    this.connect();
    return this.session;
  }

  async start(notebookId, body) {
    this.set('connecting', 'Requesting a session');
    try {
      this.session = await api.post(`/notebooks/${enc(notebookId)}/session`, body || {});
    } catch (e) { this.set('none'); throw e; }
    this.connect();
    return this.session;
  }

  connect() {
    if (this.sock) this.sock.close();
    this.set(this.session.state === 'idle' || this.session.state === 'busy' ? 'connecting' : 'starting', 'Connecting');
    const path = this.session.ws_path ? this.session.ws_path.replace(/^\/v1/, '').replace(/\?.*$/, '') : `/sessions/${enc(this.session.id)}/ws`;
    this.replayed = new Set();
    this.sock = openSocket(path, {
      onMessage: (m) => this.onMessage(m),
      onStatus: (s, code) => {
        if (s === 'open') { this.replayed = new Set(); this.opened = true; }
        else if (s === 'closed') {
          if (this.closed) return;
          if ([4404, 4410].includes(code)) { this.session = null; this.set('dead', code === 4410 ? 'The session ended.' : 'The session no longer exists.'); } else if (this.state !== 'dead') this.set('reconnecting', 'Connection lost. Reconnecting.');
        }
      },
    });
  }

  onMessage(m) {
    switch (m.type) {
      case 'status': {
        if (m.state === 'dead') { this.set('dead', m.detail || 'The session ended.'); this.h.onDead && this.h.onDead(); } else this.set(m.state, m.detail);
        break;
      }
      case 'output': {
        const replay = !!m.replay;
        let first = false;
        if (replay && !this.replayed.has(m.cell_id)) { this.replayed.add(m.cell_id); first = true; }
        this.h.onOutput && this.h.onOutput(m.cell_id, m.output, { replay, firstReplay: first });
        break;
      }
      case 'accepted': this.h.onAccepted && this.h.onAccepted(m.cell_id); break;
      case 'reply': this.h.onReply && this.h.onReply(m.cell_id, m); break;
      case 'spark': this.spark = { application_id: m.application_id, ui_url: m.ui_url }; this.h.onSpark && this.h.onSpark(this.spark); this.set(this.state, this.detail); break;
      case 'replay_done': this.h.onReplayDone && this.h.onReplayDone(m); break;
      case 'complete': { const r = this.reqs.get(m.req); if (r) { this.reqs.delete(m.req); r(m); } break; }
      default: break;
    }
  }

  get ready() { return this.state === 'idle' || this.state === 'busy'; }
  send(obj) { if (this.sock) this.sock.send(obj); }
  execute(cellId, code) { this.send({ op: 'execute', cell_id: cellId, code }); }
  async interrupt() { if (this.session) await api.post(`/sessions/${enc(this.session.id)}/interrupt`); }
  async restart() { if (this.session) { this.set('starting', 'Restarting'); await api.post(`/sessions/${enc(this.session.id)}/restart`); } }
  async stop() {
    const s = this.session;
    this.session = null;
    if (this.sock) { this.closed = true; this.sock.close(); this.sock = null; this.closed = false; }
    this.set('none');
    if (s) { try { await api.del(`/sessions/${enc(s.id)}`); } catch { /* already gone */ } }
  }
  complete(code, cursor) {
    return new Promise((resolve) => {
      const req = 'r' + (++this.seq);
      const t = setTimeout(() => { this.reqs.delete(req); resolve(null); }, 4000);
      this.reqs.set(req, (m) => { clearTimeout(t); resolve(m); });
      this.send({ op: 'complete', code, cursor, req });
    });
  }
  dispose() { this.closed = true; if (this.sock) this.sock.close(); this.sock = null; }
}
