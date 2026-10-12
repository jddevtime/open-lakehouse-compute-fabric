// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// API client: bearer auth with refresh, correlation ids, ETag/If-Match, error envelope, SSE and WebSocket helpers.
// Tokens live in memory and sessionStorage (localStorage only when the user ticks "keep me signed in").
import { hex, qs, bus } from '../core/util.js';
import { session, sess, prefs } from '../core/store.js';

export class ApiError extends Error {
  constructor(status, body, correlationId) {
    const e = (body && body.error) || {};
    super(e.message || (typeof body === 'string' && body) || `Request failed (${status})`);
    this.name = 'ApiError';
    this.status = status;
    this.code = e.code || (status === 0 ? 'network' : 'http_' + status);
    this.details = e.details;
    this.remediation = e.remediation;
    this.correlationId = e.correlation_id || correlationId;
  }
}

const auth = { access: null, refresh: null, exp: 0, remember: false };
let refreshing = null;
let refreshTimer = null;

function persist() {
  // `remember` is part of the session blob: a page reload restores the blob, and without the flag the first token refresh after the reload
  // would (wrongly) treat the session as not remembered and delete the persistent copy.
  const blob = { access: auth.access, refresh: auth.refresh, exp: auth.exp, remember: !!auth.remember };
  if (auth.remember) prefs.set('auth', { refresh: auth.refresh });
  else prefs.del('auth');
  sess.set('auth', blob);
}
export function loadStoredAuth() {
  const s = sess.get('auth');
  const p = prefs.get('auth');
  const remembered = !!(p && p.refresh);
  if (s && (s.access || s.refresh)) {
    Object.assign(auth, { access: s.access || null, refresh: s.refresh || null, exp: s.exp || 0 });
    auth.remember = s.remember === true || remembered; // either store may be the one that survived
    return true;
  }
  if (remembered) { auth.refresh = p.refresh; auth.remember = true; return true; }
  return false;
}
export function setTokens({ access_token: access, refresh_token: refresh, expires_in: ttl }, remember) {
  auth.access = access;
  if (refresh) auth.refresh = refresh;
  auth.exp = Date.now() + (ttl || 1800) * 1000;
  if (remember != null) auth.remember = !!remember;
  persist();
  scheduleRefresh();
}
export function clearTokens() {
  auth.access = auth.refresh = null; auth.exp = 0; auth.remember = false;
  clearTimeout(refreshTimer);
  sess.del('auth'); prefs.del('auth');
}
export const getAccessToken = () => auth.access;
export const hasRefresh = () => !!auth.refresh;
function scheduleRefresh() {
  clearTimeout(refreshTimer);
  if (!auth.exp) return;
  const wait = Math.max(5000, auth.exp - Date.now() - 90000);
  refreshTimer = setTimeout(() => { doRefresh().catch(() => {}); }, wait);
}

async function doRefresh() {
  if (!auth.refresh) throw new ApiError(401, { error: { code: 'unauthenticated', message: 'No refresh token' } });
  if (refreshing) return refreshing;
  refreshing = (async () => {
    try {
      const r = await fetch('/v1/identity/refresh', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ refresh_token: auth.refresh }) });
      if (!r.ok) throw new ApiError(r.status, await safeJson(r));
      setTokens(await r.json());
    } finally { refreshing = null; }
  })();
  return refreshing;
}

async function safeJson(r) { try { return await r.json(); } catch { return null; } }

function expire() {
  clearTokens();
  session.set({ user: null, bindings: [], permissions: [], expired: true });
  bus.emit('session:expired');
}

/** Low level request. Returns {data, etag, res}. Throws ApiError. */
export async function request(method, path, { query, body, headers = {}, ifMatch, signal, raw = false, form, noAuth = false, retry = true } = {}) {
  const url = '/v1' + path + qs(query);
  const cid = hex(16);
  const h = { 'X-Correlation-Id': cid, Accept: raw ? '*/*' : 'application/json', ...headers };
  if (auth.access && !noAuth) h.Authorization = 'Bearer ' + auth.access;
  if (ifMatch) h['If-Match'] = ifMatch.startsWith('"') || ifMatch.startsWith('W/') ? ifMatch : `"${ifMatch}"`;
  let payload;
  if (form) payload = form;
  else if (body !== undefined) { h['Content-Type'] = h['Content-Type'] || 'application/json'; payload = typeof body === 'string' || body instanceof Blob || body instanceof ArrayBuffer ? body : JSON.stringify(body); }
  let res;
  try {
    res = await fetch(url, { method, headers: h, body: payload, signal });
  } catch (e) {
    if (e.name === 'AbortError') throw e;
    throw new ApiError(0, { error: { code: 'network', message: 'The control plane could not be reached.', remediation: 'Check that the server is running and reachable from this browser.' } }, cid);
  }
  if (res.status === 401 && !noAuth && retry && auth.refresh) {
    try { await doRefresh(); } catch { expire(); throw new ApiError(401, { error: { code: 'session_expired', message: 'Your session expired. Sign in again to continue.' } }, cid); }
    return request(method, path, { query, body, headers, ifMatch, signal, raw, form, noAuth, retry: false });
  }
  if (res.status === 401 && !noAuth && !path.startsWith('/identity/login')) {
    const b = await safeJson(res);
    if (auth.access) expire();
    throw new ApiError(401, b, cid);
  }
  if (res.status === 403 && !noAuth && !path.startsWith('/identity/password')) {
    // an administrator required a new password while this session was open: the server refuses everything but the change, so start over at the sign-in screen (which asks for it)
    const b = await safeJson(res.clone());
    if (b && b.error && b.error.code === 'password_change_required') { expire(); throw new ApiError(403, b, cid); }
  }
  if (!res.ok) throw new ApiError(res.status, await safeJson(res) ?? (await res.text().catch(() => '')), cid);
  const etag = res.headers.get('ETag');
  if (raw) return { data: res, etag, res };
  if (res.status === 204) return { data: null, etag, res };
  const ct = res.headers.get('content-type') || '';
  const data = ct.includes('json') ? await res.json() : await res.text();
  return { data, etag, res };
}

const core = (m) => (path, opts) => request(m, path, opts).then((r) => r.data);
export const api = {
  get: (path, query, opts) => request('GET', path, { ...opts, query }).then((r) => r.data),
  post: (path, body, opts) => request('POST', path, { ...opts, body: body ?? {} }).then((r) => r.data),
  put: (path, body, opts) => request('PUT', path, { ...opts, body }).then((r) => r.data),
  patch: (path, body, opts) => request('PATCH', path, { ...opts, body }).then((r) => r.data),
  del: (path, query, opts) => request('DELETE', path, { ...opts, query }).then((r) => r.data),
  full: request,
  core,
  /** GET with the ETag, for optimistic concurrency. */
  withEtag: (path, query) => request('GET', path, { query }),
  /** Fetch a binary/text body as Blob (downloads, previews). */
  blob: async (path, query) => (await request('GET', path, { query, raw: true })).data.blob(),
  text: async (path, query) => (await request('GET', path, { query, raw: true })).data.text(),
  /** Upload with progress using XHR (fetch has no upload progress). */
  upload(path, file, { query, onProgress, signal } = {}) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', '/v1' + path + qs(query));
      if (auth.access) xhr.setRequestHeader('Authorization', 'Bearer ' + auth.access);
      xhr.setRequestHeader('X-Correlation-Id', hex(16));
      xhr.upload.onprogress = (e) => { if (e.lengthComputable && onProgress) onProgress(e.loaded / e.total); };
      xhr.onerror = () => reject(new ApiError(0, { error: { code: 'network', message: 'Upload failed: network error.' } }));
      xhr.onabort = () => reject(new DOMException('aborted', 'AbortError'));
      xhr.onload = () => {
        let b = null; try { b = JSON.parse(xhr.responseText); } catch { /* not JSON */ }
        if (xhr.status >= 200 && xhr.status < 300) resolve(b); else reject(new ApiError(xhr.status, b ?? xhr.responseText));
      };
      if (signal) signal.addEventListener('abort', () => xhr.abort());
      const fd = new FormData();
      fd.append('file', file, file.name);
      xhr.send(fd);
    });
  },
};

// ---- login / logout ----
export async function login(username, password, remember) {
  const { data } = await request('POST', '/identity/login', { body: { username, password }, noAuth: true });
  setTokens(data, remember);
  return data;
}
export async function logout() {
  try { if (auth.access) await request('POST', '/identity/logout', { body: { refresh_token: auth.refresh }, retry: false }); } catch { /* best effort */ }
  clearTokens();
  session.set({ user: null, bindings: [], permissions: [], expired: false, ready: true });
}
export async function loadMe() {
  const me = await api.get('/identity/me');
  let rolePerms = session.get().rolePerms || {};
  try { const r = await api.get('/roles'); rolePerms = Object.fromEntries((r.roles || []).map((x) => [x.name, x.permissions || []])); } catch { /* roles are optional: org permissions still apply */ }
  session.set({ user: me.principal, bindings: me.bindings || [], permissions: me.org_permissions || [], scopes: me.token_scopes || [], rolePerms, ready: true, expired: false });
  return me;
}
export async function bootstrapSession() {
  if (!auth.access && auth.refresh) { try { await doRefresh(); } catch { clearTokens(); } }
  if (!auth.access) { session.set({ ready: true }); return null; }
  try { return await loadMe(); } catch (e) { if (e.status === 401) clearTokens(); session.set({ ready: true }); return null; }
}
/** Does the signed-in user hold `perm`, organisation wide or (with `workspaceId`) through a binding in that workspace? Advisory only: the server enforces. */
export function can(perm, workspaceId) {
  const s = session.get();
  if (s.permissions.includes(perm) || s.permissions.includes('platform.admin')) return true;
  if (!workspaceId) return false;
  const rp = s.rolePerms || {};
  return (s.bindings || []).some((b) => (b.scope_type === 'workspace' && b.scope_id === workspaceId) && ((rp[b.role] || []).includes(perm) || (rp[b.role] || []).includes('*')));
}

// ---- OpenAPI feature detection ----
let specP = null;
export function spec() { return (specP ||= fetch('/v1/openapi.json').then((r) => r.json()).catch(() => ({ paths: {} }))); }
export async function hasRoute(path, method) {
  const s = await spec();
  const p = s.paths && s.paths[path];
  return !!p && (!method || !!p[method.toLowerCase()]);
}
export async function routesMatching(re) { const s = await spec(); return Object.keys(s.paths || {}).filter((p) => re.test(p)); }

// ---- Server-Sent Events over fetch (header auth, no token in URLs) ----
export function sse(path, { query, onEvent, onStatus, lastEventId } = {}) {
  let closed = false;
  let ctrl = null;
  let last = lastEventId;
  let delay = 1000;
  async function loop() {
    while (!closed) {
      ctrl = new AbortController();
      try {
        const h = { Accept: 'text/event-stream' };
        if (auth.access) h.Authorization = 'Bearer ' + auth.access;
        if (last != null) h['Last-Event-ID'] = String(last);
        const res = await fetch('/v1' + path + qs(query), { headers: h, signal: ctrl.signal });
        if (res.status === 401 && auth.refresh) { await doRefresh(); continue; }
        if (!res.ok || !res.body) throw new Error('stream http ' + res.status);
        onStatus && onStatus('open');
        delay = 1000;
        const reader = res.body.getReader();
        const dec = new TextDecoder();
        let buf = '';
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buf += dec.decode(value, { stream: true });
          let i;
          while ((i = buf.indexOf('\n\n')) >= 0) {
            const chunk = buf.slice(0, i); buf = buf.slice(i + 2);
            let ev = 'message'; let data = ''; let id = null;
            for (const line of chunk.split('\n')) {
              if (line.startsWith(':')) continue;
              if (line.startsWith('event:')) ev = line.slice(6).trim();
              else if (line.startsWith('data:')) data += line.slice(5).trimStart();
              else if (line.startsWith('id:')) id = line.slice(3).trim();
            }
            if (id != null) last = id;
            if (data) { let parsed = data; try { parsed = JSON.parse(data); } catch { /* keep text */ } onEvent && onEvent(ev, parsed, id); }
          }
        }
      } catch (e) {
        if (closed || e.name === 'AbortError') break;
      }
      onStatus && onStatus('reconnecting');
      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(delay * 2, 15000);
    }
    onStatus && onStatus('closed');
  }
  loop();
  return { close() { closed = true; if (ctrl) ctrl.abort(); } };
}

// ---- WebSocket with reconnect ----
export function openSocket(path, { query = {}, onMessage, onStatus, reconnect = true } = {}) {
  let ws = null; let closed = false; let delay = 800; let timer = null;
  const queue = [];
  function connect() {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const q = { ...query, access_token: auth.access };
    ws = new WebSocket(`${proto}//${location.host}/v1${path}${qs(q)}`);
    ws.onopen = () => { delay = 800; onStatus && onStatus('open'); while (queue.length) ws.send(queue.shift()); };
    ws.onmessage = (e) => { let m = e.data; try { m = JSON.parse(e.data); } catch { /* text frame */ } onMessage && onMessage(m); };
    ws.onclose = (e) => {
      onStatus && onStatus('closed', e.code);
      if (closed || !reconnect || [4401, 4403, 4404, 4410].includes(e.code)) return;
      timer = setTimeout(async () => { try { if (auth.exp - Date.now() < 30000 && auth.refresh) await doRefresh(); } catch { /* ignore */ } connect(); }, delay);
      delay = Math.min(delay * 1.8, 10000);
    };
    ws.onerror = () => { /* onclose follows */ };
  }
  connect();
  return {
    send(obj) { const s = JSON.stringify(obj); if (ws && ws.readyState === 1) ws.send(s); else queue.push(s); },
    close() { closed = true; clearTimeout(timer); if (ws) ws.close(); },
    get state() { return ws ? ws.readyState : 3; },
  };
}

/** Human readable message for any thrown value, with the server's remediation text when present. */
export function describeError(e) {
  if (!e) return { title: 'Unknown error', body: '' };
  if (e instanceof ApiError) {
    const title = e.status === 403 ? 'Not permitted' : e.status === 0 ? 'Network error' : e.message;
    const body = e.status === 403 ? 'Your account does not have permission for this action.' + (e.message ? ' ' + e.message : '') : '';
    return { title, body, remediation: e.remediation, code: e.code, cid: e.correlationId, details: e.details };
  }
  return { title: e.message || String(e), body: '' };
}
