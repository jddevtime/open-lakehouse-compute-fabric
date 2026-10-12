// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Unit test for the browser API client's token storage ("Keep me signed in on this device", review finding F14).
// Runs under plain Node with fake storages / fetch / timers; tests/web/test_remember_auth.py stages the sources and starts it.
import test from 'node:test';
import assert from 'node:assert/strict';

class MemoryStorage {
  constructor() { this.m = new Map(); this.broken = false; }
  getItem(k) { if (this.broken) throw new Error('blocked'); return this.m.has(k) ? this.m.get(k) : null; }
  setItem(k, v) { if (this.broken) throw new Error('blocked'); this.m.set(k, String(v)); }
  removeItem(k) { if (this.broken) throw new Error('blocked'); this.m.delete(k); }
  get length() { return this.m.size; }
}

const local = new MemoryStorage();
const session = new MemoryStorage();
globalThis.window = { localStorage: local, sessionStorage: session };
const timers = [];
globalThis.setTimeout = (fn, ms) => { timers.push({ fn, ms }); return timers.length; }; // never fire: the proactive refresh is not under test
globalThis.clearTimeout = () => {};

let calls;
let refreshSeq;
let accessExpired; // the server no longer accepts the current access token (as after a reload of a long-open tab)
globalThis.fetch = async (url, init = {}) => {
  const path = String(url);
  const body = init.body ? JSON.parse(init.body) : {};
  calls.push({ path, auth: (init.headers || {}).Authorization, body });
  const json = (status, data) => new Response(JSON.stringify(data), { status, headers: { 'content-type': 'application/json' } });
  if (path === '/v1/identity/login') return json(200, { access_token: 'acc-1', refresh_token: 'ref-1', expires_in: 1800 });
  if (path === '/v1/identity/refresh') {
    if (body.refresh_token !== `ref-${refreshSeq}`) return json(401, { error: { code: 'unauthenticated', message: 'stale refresh token' } });
    refreshSeq += 1;
    accessExpired = false;
    return json(200, { access_token: `acc-${refreshSeq}`, refresh_token: `ref-${refreshSeq}`, expires_in: 1800 });
  }
  if (path === '/v1/identity/logout') return json(200, {});
  // any other call: the access token must be the newest one, otherwise answer 401 so the client refreshes
  const want = `Bearer acc-${refreshSeq}`;
  return !accessExpired && init.headers && init.headers.Authorization === want ? json(200, { ok: true }) : json(401, { error: { code: 'unauthenticated', message: 'expired' } });
};

let n = 0;
/** A "page load": a fresh module instance (in-memory token state is empty) over the same browser storages. */
async function pageLoad() {
  n += 1;
  return import(`./js/api/client.js?load=${n}`);
}
function reset() { local.m.clear(); session.m.clear(); local.broken = session.broken = false; calls = []; refreshSeq = 1; accessExpired = false; }
const stored = (st, key) => { const v = st.getItem('olcf.' + key); return v == null ? null : JSON.parse(v); };

test('a remembered session is still remembered after a reload and a token refresh', async () => {
  reset();
  let c = await pageLoad();
  await c.login('admin', 'pw', true);
  assert.deepEqual(stored(local, 'auth'), { refresh: 'ref-1' });
  assert.equal(stored(session, 'auth').remember, true, 'the session blob must carry the flag');

  c = await pageLoad(); // F5 / navigation: sessionStorage survives, memory does not
  assert.equal(c.loadStoredAuth(), true);
  accessExpired = true;
  await c.api.get('/anything'); // 401 -> refresh (rotates ref-1 -> ref-2) -> retry
  assert.equal(refreshSeq, 2);
  assert.deepEqual(stored(local, 'auth'), { refresh: 'ref-2' }, 'the persistent copy must follow the rotated refresh token, not be deleted');
  assert.equal(stored(session, 'auth').remember, true);

  c = await pageLoad(); // and again: still remembered
  c.loadStoredAuth();
  accessExpired = true;
  await c.api.get('/anything');
  assert.deepEqual(stored(local, 'auth'), { refresh: 'ref-3' });
});

test('after the browser is closed (sessionStorage gone) the remembered refresh token signs the user in again', async () => {
  reset();
  let c = await pageLoad();
  await c.login('admin', 'pw', true);
  session.m.clear();
  c = await pageLoad();
  assert.equal(c.loadStoredAuth(), true);
  assert.equal(c.hasRefresh(), true);
  await c.bootstrapSession().catch(() => null); // refresh succeeds; /identity/me answers 401 in this fake, which bootstrapSession tolerates
  assert.deepEqual(stored(local, 'auth'), { refresh: 'ref-2' });
  assert.equal(stored(session, 'auth').remember, true);
});

test('a session that was not remembered never reaches localStorage, also after reload and refresh', async () => {
  reset();
  let c = await pageLoad();
  await c.login('admin', 'pw', false);
  assert.equal(local.getItem('olcf.auth'), null);
  c = await pageLoad();
  c.loadStoredAuth();
  accessExpired = true;
  await c.api.get('/anything');
  assert.equal(refreshSeq, 2);
  assert.equal(local.getItem('olcf.auth'), null);
  assert.equal(stored(session, 'auth').remember, false);
});

test('signing in again without the box ticked forgets an earlier remembered refresh token', async () => {
  reset();
  let c = await pageLoad();
  await c.login('admin', 'pw', true);
  c = await pageLoad();
  await c.login('admin', 'pw', false);
  assert.equal(local.getItem('olcf.auth'), null);
});

test('logging out clears both stores', async () => {
  reset();
  const c = await pageLoad();
  await c.login('admin', 'pw', true);
  await c.logout();
  assert.equal(local.getItem('olcf.auth'), null);
  assert.equal(session.getItem('olcf.auth'), null);
});

test('only the remembered store survives when the other was cleared; blocked storage does not throw', async () => {
  reset();
  let c = await pageLoad();
  await c.login('admin', 'pw', true);
  local.m.clear(); // site data for localStorage wiped, the tab still has its session blob with remember=true
  c = await pageLoad();
  assert.equal(c.loadStoredAuth(), true);
  accessExpired = true;
  await c.api.get('/anything');
  assert.deepEqual(stored(local, 'auth'), { refresh: 'ref-2' }, 'the flag in the session blob re-creates the persistent copy');

  reset();
  local.broken = session.broken = true;
  c = await pageLoad();
  assert.equal(c.loadStoredAuth(), false);
  await c.login('admin', 'pw', true); // in-memory only, no exception
  assert.equal(c.getAccessToken(), 'acc-1');
});
