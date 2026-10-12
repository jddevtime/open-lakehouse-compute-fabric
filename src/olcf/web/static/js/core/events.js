// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// One shared Server-Sent Events subscription (/v1/events/stream) that fans out to pages through the bus and keeps a short activity buffer.
import { createStore } from './store.js';
import { bus } from './util.js';
import { sse } from '../api/client.js';

export const activity = createStore({ items: [], status: 'closed' });
let conn = null;

export function startEvents() {
  if (conn) return;
  conn = sse('/events/stream', {
    onStatus: (s) => activity.set((st) => ({ ...st, status: s })),
    onEvent: (type, ev) => {
      if (!ev || typeof ev !== 'object') return;
      const rec = { ...ev, type: ev.type || type };
      activity.set((st) => ({ ...st, items: [rec, ...st.items].slice(0, 80) }));
      bus.emit('event', rec);
      bus.emit('event:' + rec.type.split('.')[0], rec);
    },
  });
}
export function stopEvents() { if (conn) { conn.close(); conn = null; } activity.set({ items: [], status: 'closed' }); }

/** One-line human summary of a platform event for the activity feed. */
export function describeEvent(e) {
  const p = e.payload || {};
  switch (e.type) {
    case 'job.state_changed': return { icon: 'play-all', text: `Job ${p.job_id ? p.job_id.slice(0, 12) : ''} ${p.from ? p.from + ' to ' : 'now '}${p.to}`, link: p.job_id ? `#/jobs/${p.job_id}` : null, kind: p.to };
    case 'alert.firing': return { icon: 'bell', text: p.summary || `Alert firing: ${p.rule}`, link: '#/alerts', kind: 'failed' };
    case 'alert.resolved': return { icon: 'bell', text: `Resolved: ${p.rule || ''} ${p.instance || ''}`, link: '#/alerts', kind: 'succeeded' };
    case 'worker.connected': return { icon: 'server', text: `Node ${p.name || p.worker_id || ''} connected`, link: '#/compute', kind: 'succeeded' };
    case 'worker.disconnected': return { icon: 'server', text: `Node ${p.name || p.worker_id || ''} disconnected`, link: '#/compute', kind: 'warn' };
    case 'fabric.submission.received': return { icon: 'cloud', text: `Fabric notebook ${p.notebook || ''} submitted`, link: '#/fabric?tab=submissions', kind: 'running' };
    case 'fabric.submission.completed': return { icon: 'cloud', text: `Fabric run ${p.state || 'finished'}${p.failure_category ? ' (' + p.failure_category + ')' : ''}`, link: '#/fabric?tab=submissions', kind: p.state === 'succeeded' ? 'succeeded' : 'failed' };
    case 'fabric.submission.rejected': return { icon: 'cloud', text: `Fabric submission rejected: ${p.reason || p.code || ''}`.slice(0, 120), link: '#/fabric?tab=submissions', kind: 'warn' };
    case 'fabric.handshake': return { icon: 'cloud', text: `Fabric notebook ${p.ok ? 'connected' : 'handshake failed'}`, link: '#/fabric', kind: p.ok ? 'succeeded' : 'warn' };
    case 'security.login': return { icon: 'shield', text: `${p.actor || 'Someone'} signed in${p.outcome && p.outcome !== 'success' ? ' (' + p.outcome + ')' : ''}`, link: '#/security?tab=audit', kind: p.outcome && p.outcome !== 'success' ? 'warn' : 'info' };
    default: {
      const who = typeof p.actor === 'string' && p.actor ? `${p.actor}: ` : '';
      const what = typeof p.action === 'string' && p.action ? p.action.replace(/[._]/g, ' ') : e.type.replace(/[._]/g, ' ');
      const target = typeof p.name === 'string' && p.name ? ` · ${p.name.slice(0, 40)}` : e.subject && !/^(usr|wrk)_/.test(String(e.subject)) ? ` · ${String(e.subject).slice(0, 18)}` : '';
      return { icon: 'activity', text: who + what + target, link: null, kind: 'info' };
    }
  }
}
