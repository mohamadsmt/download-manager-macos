import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

// Load the actual ESM adapter, substituting only its official SDK import.
// An absent adapter is a behavior failure, not an import/parse failure.
let moduleNumber = 0;
async function loadAdapter(sdk) {
  let source;
  try { source = await readFile(new URL('./plugin.js', import.meta.url), 'utf8'); }
  catch (error) {
    if (error.code === 'ENOENT') return { absent: true, register() {} };
    throw error;
  }
  const key = `__hermesDownloadsTest${++moduleNumber}`;
  globalThis[key] = sdk;
  const substitute = `const sdk=globalThis[${JSON.stringify(key)}]; export const host=sdk.host; export const queryClient=sdk.queryClient;`;
  const sdkUrl = `data:text/javascript;base64,${Buffer.from(substitute).toString('base64')}`;
  const loaded = source.replace(/(['"])@hermes\/plugin-sdk\1/g, JSON.stringify(sdkUrl));
  try {
    return (await import(`data:text/javascript;base64,${Buffer.from(loaded).toString('base64')}`)).default;
  } finally { delete globalThis[key]; }
}

function atom(value) {
  const listeners = new Set();
  return {
    get: () => value,
    listen(fn) { listeners.add(fn); return () => listeners.delete(fn); },
    set(next) { value = next; for (const fn of [...listeners]) fn(next); },
    listeners,
  };
}
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
async function flush() { for (let i = 0; i < 25; i++) await Promise.resolve(); }
const uuid = '12345678-1234-4234-8234-123456789abc';
const token = 'a'.repeat(64);
function event(overrides = {}) {
  return {
    event_id: `${uuid}:1`, type: 'completion', job_id: 'job-1', collection_id: null,
    generation: 1, created_at_ms: 100, count: 1, reason_code: null,
    reveal_path: null, presentation_state: 'pending', ...overrides,
  };
}
function pending(e = event(), overrides = {}) {
  return { schema_version: 1, profile: 'default', source: 'local', events: e ? [e] : [], next_cursor: null, has_more: false, ...overrides };
}
function claims(e = event(), overrides = {}) {
  return { schema_version: 1, profile: 'default', source: 'local', claims: e ? [{ event: e, claim_token: token, lease_expires_at_ms: 10000 }] : [], server_now_ms: 100, ...overrides };
}
async function harness(options = {}) {
  const calls = [], notifications = [], reveals = [], contributions = [], intervals = [], sockets = [], disposers = [];
  const state = {
    profile: atom(options.profile ?? 'default'),
    connectionId: atom(options.connectionId ?? null),
    gateway: atom(options.gateway ?? 'open'),
  };
  const queryCalls = [], invalidations = [], cancellations = [], removals = [], cache = new Map();
  const queryClient = {
    async fetchQuery(opts) {
      queryCalls.push(opts);
      const data = options.fetchQuery ? await options.fetchQuery(opts) : await opts.queryFn();
      cache.set(JSON.stringify(opts.queryKey), data);
      return data;
    },
    async invalidateQueries(opts) { invalidations.push(opts); },
    async cancelQueries(opts) { cancellations.push(opts); },
    removeQueries(opts) { removals.push(opts); cache.delete(JSON.stringify(opts.queryKey)); },
  };
  let handler = options.rest;
  const host = { state, notify(input) {
    notifications.push(input);
    return options.notify ? options.notify(input) : input.id;
  } };
  const ctx = {
    register(c) { contributions.push(c); return () => {}; },
    registerMany(c) { contributions.push(...c); return () => {}; },
    onDispose(fn) { disposers.push(fn); },
    setInterval(fn, ms) { const t = { fn, ms, cancelled: false }; intervals.push(t); return () => { t.cancelled = true; }; },
    setTimeout() { assert.fail('adapter must not create catchup timers'); },
    socket(path, fn) { const s = { path, fn, cancelled: false }; sockets.push(s); return () => { s.cancelled = true; }; },
    async rest(path, opts = {}) {
      calls.push({ path, opts, profile: state.profile.get(), connectionId: state.connectionId.get(), gateway: state.gateway.get() });
      if (handler) return handler(path, opts);
      if (path.startsWith('/events?')) return pending();
      if (path === '/events/claim') return claims();
      if (path === '/events/presented') return { status: 'presented' };
      if (path === '/events/release') return { status: 'released' };
      assert.fail(`unexpected route ${path}`);
    },
    os: { async revealPath(path) { reveals.push(path); return options.reveal ? options.reveal(path) : true; } },
  };
  const plugin = await loadAdapter({ host, queryClient });
  assert.equal(plugin.absent, undefined, 'renderer adapter absent: required functionality is missing');
  plugin.register(ctx);
  return {
    plugin, ctx, state, calls, notifications, reveals, contributions, intervals, sockets,
    queryCalls, invalidations, cancellations, removals, cache,
    setRest(fn) { handler = fn; },
    wake() { sockets.at(-1)?.fn({ ignored: 'socket carries no claims' }); },
    poll() { for (const t of intervals) if (!t.cancelled) t.fn(); },
    dispose() { for (const fn of [...disposers].reverse()) fn(); },
  };
}
const of = (h, route) => h.calls.filter(c => c.path === route);

test('plain default-off notification plugin contributes no UI', async () => {
  const h = await harness(); await flush();
  assert.equal(h.plugin.id, 'hermes-downloads');
  assert.equal(h.plugin.defaultEnabled, false);
  assert.equal(typeof h.plugin.name, 'string');
  assert.equal(typeof h.plugin.description, 'string');
  assert.deepEqual(h.contributions, []);
  h.dispose();
});
for (const scope of [
  { profile: 'work' }, { profile: '' }, { connectionId: 'remote-1' },
  { connectionId: '' }, { gateway: 'closed' }, { gateway: 'connecting' },
]) test(`inactive scope makes no calls ${JSON.stringify(scope)}`, async () => {
  const h = await harness(scope); h.poll(); h.wake(); await flush();
  assert.equal(h.calls.length, 0); assert.equal(h.notifications.length, 0);
  assert.equal(h.sockets.length, 0); assert.equal(h.intervals.length, 0);
  h.dispose();
});
test('local/default/open uses pure bounded query then one claim and receipt ack', async () => {
  const h = await harness({ connectionId: 'local' }); await flush();
  assert.deepEqual(h.calls.map(c => c.path), ['/events?view=pending&limit=1', '/events/claim', '/events/presented']);
  assert.equal(h.queryCalls.length, 1);
  const q = h.queryCalls[0];
  assert.equal(q.retry, false); assert.equal(q.staleTime, 0);
  assert.deepEqual(q.queryKey.slice(0, 3), ['hermes-downloads', 'local', 'default']);
  assert.match(q.queryKey[3], /^[0-9a-f-]{36}$/); assert.equal(typeof q.queryKey[4], 'number');
  assert.deepEqual(of(h, '/events/claim')[0].opts.body.limit, 1);
  assert.match(of(h, '/events/claim')[0].opts.body.consumer_id, /^[0-9a-f-]{36}$/);
  assert.equal(h.notifications.length, 1);
  assert.equal(h.notifications[0].id, `hermes-downloads:${uuid}:1`);
  assert.equal(h.notifications[0].kind, 'success');
  assert.equal(h.notifications[0].durationMs, 5000);
  assert.equal(of(h, '/events/presented')[0].opts.body.notification_id, h.notifications[0].id);
  for (const c of h.calls) assert.ok(c.opts.timeoutMs > 0 && c.opts.timeoutMs <= 5000);
  assert.equal(h.intervals[0].ms, 15000);
  assert.equal(h.sockets[0].path, '/events');
  h.dispose();
});
test('empty read never claims; has_more never drains without another wake', async () => {
  const h = await harness({ rest: path => path.startsWith('/events?') ? pending(null, { has_more: true, next_cursor: 'cursor-1' }) : assert.fail('no mutation from empty read') });
  await flush(); assert.equal(h.calls.length, 1); h.dispose();
});
test('has_more with a completion still produces only one notification', async () => {
  const h = await harness({ rest: path => path.startsWith('/events?') ? pending(event(), { has_more: true, next_cursor: 'cursor-1' }) : path === '/events/claim' ? claims() : { status: 'presented' } });
  await flush(); assert.equal(h.notifications.length, 1); assert.equal(h.queryCalls.length, 1); h.dispose();
});
for (const type of ['needs_link', 'needs_login', 'needs_space', 'persistent_error']) test(`fixed Persian persistent notification: ${type}`, async () => {
  const e = event({ type, count: 7 });
  const h = await harness({ rest: path => path.startsWith('/events?') ? pending(e) : path === '/events/claim' ? claims(e) : { status: 'presented' } });
  await flush(); const n = h.notifications[0];
  assert.match(n.title, /[\u0600-\u06ff]/); assert.match(n.message, /[\u0600-\u06ff]/);
  assert.equal(n.durationMs, 0); assert.equal(n.kind, type === 'persistent_error' ? 'error' : 'warning');
  assert.equal(n.action, undefined); h.dispose();
});

const badEvents = [
  ['unknown DTO field', { url: 'https://secret.invalid/token' }],
  ['invalid UUID', { event_id: 'not-uuid:1' }], ['zero sequence', { event_id: `${uuid}:0` }],
  ['newline event id', { event_id: `${uuid}:1\n` }], ['newline job id', { job_id: 'job-1\n' }],
  ['unsafe sequence', { event_id: `${uuid}:9007199254740992` }],
  ['unknown type', { type: 'traceback SECRET' }], ['invalid job', { job_id: 'https://secret.invalid' }],
  ['long collection', { collection_id: 'a'.repeat(129) }], ['negative generation', { generation: -1 }],
  ['unsafe created time', { created_at_ms: Number.MAX_SAFE_INTEGER + 1 }],
  ['zero count', { count: 0 }], ['overlimit count', { count: 501 }], ['fractional count', { count: 1.5 }],
  ['arbitrary reason', { reason_code: 'engine traceback SECRET' }],
  ['unknown state', { presentation_state: 'completed' }],
  ['noncompletion reveal', { type: 'needs_link', reveal_path: '/Users/test/Downloads/Hermes/Videos/movie.mp4' }],
];
for (const [name, patch] of badEvents) test(`reject ${name} in pure read before mutation`, async () => {
  const h = await harness({ rest: path => path.startsWith('/events?') ? pending(event(patch)) : assert.fail('malformed read must not mutate') });
  await flush(); assert.equal(h.notifications.length, 0); assert.equal(h.calls.length, 1); h.dispose();
});
for (const [name, patch] of [
  ['unknown envelope', { message: 'SECRET' }], ['wrong profile', { profile: 'work' }],
  ['wrong source', { source: 'remote' }], ['schema', { schema_version: 2 }],
  ['overlimit events', { events: [event(), event()] }], ['bad cursor', { next_cursor: 'https://secret.invalid' }],
  ['empty cursor', { next_cursor: '' }], ['bad boolean', { has_more: 1 }],
  ['newline cursor', { next_cursor: 'cursor-1\n' }],
]) test(`reject pending ${name}`, async () => {
  const h = await harness({ rest: () => pending(event(), patch) });
  await flush(); assert.equal(h.calls.length, 1); assert.equal(h.notifications.length, 0); h.dispose();
});
for (const [name, patch] of [
  ['wrong profile', { profile: 'work' }], ['wrong source', { source: 'remote' }],
  ['unknown field', { text: 'SECRET' }], ['negative server time', { server_now_ms: -1 }],
  ['unsafe server time', { server_now_ms: Number.MAX_SAFE_INTEGER + 1 }],
  ['overlimit claims', { claims: [claims().claims[0], claims().claims[0]] }],
  ['bad token', { claims: [{ ...claims().claims[0], claim_token: 'A'.repeat(64) }] }],
  ['newline token', { claims: [{ ...claims().claims[0], claim_token: `${token}\n` }] }],
  ['expired lease', { claims: [{ ...claims().claims[0], lease_expires_at_ms: 100 }] }],
  ['extra claim field', { claims: [{ ...claims().claims[0], url: 'SECRET' }] }],
  ['malformed claim DTO', { claims: [{ ...claims().claims[0], event: event({ reason_code: 'SECRET' }) }] }],
]) test(`reject claim ${name} before notification or ack/release`, async () => {
  const h = await harness({ rest: path => path.startsWith('/events?') ? pending() : claims(event(), patch) });
  await flush(); assert.equal(h.calls.length, 2); assert.equal(h.notifications.length, 0); h.dispose();
});
for (const state of ['presented', 'resolved']) test(`already ${state} data never notifies or claims`, async () => {
  const h = await harness({ rest: () => pending(event({ presentation_state: state })) });
  await flush(); assert.equal(h.notifications.length, 0); assert.equal(of(h, '/events/claim').length, 0); h.dispose();
});
for (const receipt of ['', '   ', null, 42, 'x'.repeat(257), 'bad\nreceipt']) test(`invalid notify receipt ${JSON.stringify(receipt)} releases without ack`, async () => {
  const h = await harness({ notify: () => receipt }); await flush();
  assert.equal(of(h, '/events/release').length, 1); assert.equal(of(h, '/events/presented').length, 0); h.dispose();
});
test('notify throw releases without ack', async () => {
  const h = await harness({ notify: () => { throw new Error('SECRET'); } }); await flush();
  assert.equal(of(h, '/events/release').length, 1); assert.equal(of(h, '/events/presented').length, 0); h.dispose();
});
test('ack transport loss retries identical receipt without another read, claim or notify', async () => {
  let acks = 0;
  const h = await harness({ notify: () => 'store-receipt', rest: path => {
    if (path.startsWith('/events?')) return pending();
    if (path === '/events/claim') return claims();
    if (++acks === 1) throw new Error('transport loss');
    return { status: 'already_presented' };
  } });
  await flush(); h.poll(); await flush();
  assert.equal(h.notifications.length, 1); assert.equal(h.queryCalls.length, 1); assert.equal(of(h, '/events/claim').length, 1);
  assert.deepEqual(of(h, '/events/presented')[0].opts.body, of(h, '/events/presented')[1].opts.body);
  assert.equal(of(h, '/events/presented')[1].opts.body.notification_id, 'store-receipt'); h.dispose();
});
for (const response of [{ status: 'unknown' }, { status: 'presented', extra: 'SECRET' }, null]) test(`malformed ack retains uncertainty ${JSON.stringify(response)}`, async () => {
  let acks = 0;
  const h = await harness({ rest: path => path.startsWith('/events?') ? pending() : path === '/events/claim' ? claims() : (++acks === 1 ? response : { status: 'presented' }) });
  await flush(); h.poll(); await flush();
  assert.equal(h.notifications.length, 1); assert.equal(h.queryCalls.length, 1); assert.equal(acks, 2); h.dispose();
});
test('failed release retains one authority and never freshly notifies from uncertainty', async () => {
  let releases = 0;
  const h = await harness({ notify: () => '', rest: path => {
    if (path.startsWith('/events?')) return pending(); if (path === '/events/claim') return claims();
    releases++; throw new Error('lost release');
  } });
  await flush(); h.poll(); await flush();
  assert.equal(h.notifications.length, 1); assert.equal(h.queryCalls.length, 1); assert.equal(releases, 2); h.dispose();
});
test('stale claim clears authority; future replay uses same toast id with fresh token', async () => {
  let count = 0;
  const h = await harness({ rest: path => {
    if (path.startsWith('/events?')) return pending();
    if (path === '/events/claim') { const c = claims(); c.claims[0].claim_token = (++count === 1 ? 'a' : 'b').repeat(64); return c; }
    return { status: count === 1 ? 'stale_claim' : 'presented' };
  } });
  await flush(); h.poll(); await flush();
  assert.equal(h.notifications.length, 2); assert.equal(h.notifications[0].id, h.notifications[1].id);
  assert.notEqual(of(h, '/events/presented')[0].opts.body.claim_token, of(h, '/events/presented')[1].opts.body.claim_token); h.dispose();
});

for (const field of ['profile', 'connectionId', 'gateway']) for (const stage of ['query', 'claim', 'ack', 'release']) test(`scope ${field} switch during ${stage} await fences stale effects`, async () => {
  const d = deferred(); let blocked = false;
  const h = await harness({ notify: stage === 'release' ? () => '' : undefined, rest: path => {
    if ((stage === 'query' && path.startsWith('/events?')) || path === `/events/${stage === 'query' ? 'unused' : stage === 'ack' ? 'presented' : stage}`) { blocked = true; return d.promise; }
    return path.startsWith('/events?') ? pending() : path === '/events/claim' ? claims() : { status: 'presented' };
  } });
  await flush(); assert.equal(blocked, true);
  const before = h.calls.length;
  h.state[field].set(field === 'profile' ? 'work' : field === 'connectionId' ? 'remote' : 'closed');
  d.resolve(stage === 'query' ? pending() : stage === 'claim' ? claims() : { status: stage === 'ack' ? 'presented' : 'released' });
  await flush(); h.poll(); h.wake(); await flush();
  assert.equal(h.calls.length, before);
  assert.equal(h.notifications.length, stage === 'query' || stage === 'claim' ? 0 : 1);
  assert.ok(h.sockets.every(s => s.cancelled)); assert.ok(h.intervals.every(t => t.cancelled)); h.dispose();
});
test('scope change within synchronous notify prevents stale ack and release', async () => {
  let h;
  const claim = deferred();
  h = await harness({ notify: () => { h.state.profile.set('work'); return ''; }, rest: path => path.startsWith('/events?') ? pending() : claim.promise });
  await flush(); claim.resolve(claims()); await flush();
  assert.equal(h.calls.length, 2); assert.equal(h.notifications.length, 1); h.dispose();
});
for (const stage of ['query', 'claim', 'ack', 'release']) test(`dispose during ${stage} await removes lifetime and stops effects`, async () => {
  const d = deferred();
  const h = await harness({ notify: stage === 'release' ? () => '' : undefined, rest: path => {
    if ((stage === 'query' && path.startsWith('/events?')) || path === `/events/${stage === 'ack' ? 'presented' : stage}`) return d.promise;
    return path.startsWith('/events?') ? pending() : claims();
  } });
  await flush(); const before = h.calls.length; h.dispose();
  d.resolve(stage === 'query' ? pending() : stage === 'claim' ? claims() : { status: 'presented' });
  await flush(); h.poll(); h.wake(); await flush();
  assert.equal(h.calls.length, before); assert.ok(h.sockets.every(s => s.cancelled)); assert.ok(h.intervals.every(t => t.cancelled));
  for (const a of Object.values(h.state)) assert.equal(a.listeners.size, 0);
  assert.equal(h.cache.size, 0); assert.ok(h.removals.every(r => r.exact === true));
});
test('returning scope replaces socket, query key and consumer; old wake is inert', async () => {
  const h = await harness(); await flush();
  const old = h.sockets[0]; const consumer = of(h, '/events/claim')[0].opts.body.consumer_id;
  h.state.profile.set('work'); old.fn({}); await flush();
  h.state.profile.set('default'); await flush();
  assert.equal(h.sockets.length, 2); assert.equal(old.cancelled, true);
  assert.notEqual(of(h, '/events/claim')[1].opts.body.consumer_id, consumer);
  assert.notDeepEqual(h.queryCalls[0].queryKey, h.queryCalls[1].queryKey);
  assert.equal(h.cache.size, 1); h.dispose(); await flush(); assert.equal(h.cache.size, 0);
});
test('concurrent socket/poll wakes serialize one claim and at most one queued reconciliation', async () => {
  const d = deferred(); let reads = 0, activeClaims = 0, maxClaims = 0;
  const h = await harness({ rest: async path => {
    if (path.startsWith('/events?')) return ++reads === 1 ? d.promise : pending(null);
    if (path === '/events/claim') { activeClaims++; maxClaims = Math.max(maxClaims, activeClaims); await Promise.resolve(); activeClaims--; return claims(); }
    return { status: 'presented' };
  } });
  for (let i = 0; i < 50; i++) { h.wake(); h.poll(); }
  await flush(); assert.equal(reads, 1); d.resolve(pending()); await flush();
  assert.equal(reads, 2); assert.equal(maxClaims, 1); assert.equal(h.notifications.length, 1); assert.equal(of(h, '/events/claim').length, 1);
  h.dispose();
});
test('poll fallback works without socket messages and exact query cleanup stays bounded', async () => {
  const h = await harness({ rest: () => pending(null) }); await flush();
  for (let i = 0; i < 30; i++) { h.poll(); await flush(); }
  assert.equal(h.queryCalls.length, 31); assert.equal(h.cache.size, 1); assert.equal(h.sockets.length, 1); assert.equal(h.intervals.length, 1);
  h.dispose(); await flush(); assert.equal(h.cache.size, 0); assert.equal(h.cancellations[0].exact, true);
});

const goodPath = '/Users/test/Downloads/Hermes/Videos/فیلم.mp4';
test('validated reveal is explicit only and captured action is inert after activation changes', async () => {
  const e = event({ reveal_path: goodPath });
  const h = await harness({ rest: path => path.startsWith('/events?') ? pending(e) : path === '/events/claim' ? claims(e) : { status: 'presented' } });
  await flush(); assert.equal(h.reveals.length, 0);
  const action = h.notifications[0].action; assert.match(action.label, /[\u0600-\u06ff]/);
  await action.onClick(); assert.deepEqual(h.reveals, [goodPath]);
  h.state.profile.set('work'); h.state.profile.set('default'); await action.onClick();
  assert.equal(h.reveals.length, 1); h.dispose(); await action.onClick(); assert.equal(h.reveals.length, 1);
});
for (const path of [
  '/tmp/movie.mp4', '/Users/test/Downloads/Hermes/Videos/../secret',
  '/Users/test/Downloads/Hermes//Videos/movie.mp4', '/Users/test/Downloads/Hermes/Videos/',
  '/Users/test/Downloads/Hermes/Videos/./movie.mp4', '/Users/test/Downloads/Hermes/Videos/movie\u0000.mp4',
  '/Users/test/Downloads/Hermes/Videos/movie\n.mp4', '/Users/test/Downloads/Hermes/Secrets/movie.mp4',
  '/Users/test/Downloads/Hermes/Videos/a\\b.mp4', 'Users/test/Downloads/Hermes/Videos/movie.mp4',
]) test(`unsafe reveal path ${JSON.stringify(path)} never contributes an action or notification`, async () => {
  const e = event({ reveal_path: path });
  const h = await harness({ rest: () => pending(e) }); await flush();
  assert.equal(h.notifications.length, 0); assert.equal(h.reveals.length, 0); assert.equal(h.calls.length, 1); h.dispose();
});

test('valid bounded identifiers and reason codes never leak into fixed toast copy', async () => {
  const e = event({ job_id: 'PrivateJobName', collection_id: 'SecretCollection', reason_code: 'download_failed', count: 500 });
  const h = await harness({ rest: path => path.startsWith('/events?') ? pending(e) : path === '/events/claim' ? claims(e) : { status: 'presented' } });
  await flush();
  assert.equal(h.notifications.length, 1);
  const n = h.notifications[0];
  assert.equal(n.message, '۵۰۰ دانلود آماده است.');
  assert.equal(n.detail, undefined); assert.equal(n.meta, undefined);
  assert.doesNotMatch(JSON.stringify(n), /PrivateJobName|SecretCollection|download_failed/); h.dispose();
});
for (const stage of ['query', 'claim']) test(`transport failure at ${stage} is silent and a later poll can recover`, async () => {
  let failures = 0;
  const h = await harness({ rest: path => {
    if (((stage === 'query' && path.startsWith('/events?')) || (stage === 'claim' && path === '/events/claim')) && failures++ === 0) throw new Error('https://private.invalid/SECRET traceback');
    return path.startsWith('/events?') ? pending() : path === '/events/claim' ? claims() : { status: 'presented' };
  } });
  await flush(); assert.equal(h.notifications.length, 0); h.poll(); await flush();
  assert.equal(h.notifications.length, 1); assert.doesNotMatch(JSON.stringify(h.notifications), /private|SECRET|traceback/); h.dispose();
});
for (const stage of ['query', 'claim', 'ack', 'release']) test(`leave and return during ${stage} await never restores stale authority`, async () => {
  const d = deferred(); let first = true;
  const h = await harness({ notify: stage === 'release' ? () => '' : undefined, rest: path => {
    const target = stage === 'query' ? path.startsWith('/events?') : path === `/events/${stage === 'ack' ? 'presented' : stage}`;
    if (target && first) { first = false; return d.promise; }
    return path.startsWith('/events?') ? pending() : path === '/events/claim' ? claims() : { status: stage === 'release' ? 'released' : 'presented' };
  } });
  await flush(); const oldCalls = h.calls.length; const oldNotifications = h.notifications.length;
  const oldSocket = h.sockets[0];
  h.state.connectionId.set('remote'); h.state.connectionId.set('local');
  await flush(); assert.equal(h.calls.length, oldCalls, 'new activation waits for the old serialized lane');
  d.resolve(stage === 'query' ? pending() : stage === 'claim' ? claims() : { status: stage === 'release' ? 'released' : 'presented' });
  await flush();
  assert.equal(h.notifications.length, oldNotifications + 1);
  assert.equal(oldSocket.cancelled, true); assert.equal(h.cache.size, 1);
  const claimCalls = of(h, '/events/claim');
  if (claimCalls.length > 1) assert.notEqual(claimCalls[0].opts.body.consumer_id, claimCalls[1].opts.body.consumer_id);
  const before = h.calls.length; oldSocket.fn({}); await flush(); assert.equal(h.calls.length, before);
  h.dispose(); await flush(); assert.equal(h.cache.size, 0);
});
test('disabled lifetime ignores delayed socket, poll and captured reveal callbacks', async () => {
  const e = event({ reveal_path: goodPath });
  const h = await harness({ rest: path => path.startsWith('/events?') ? pending(e) : path === '/events/claim' ? claims(e) : { status: 'presented' } });
  await flush(); const action = h.notifications[0].action; const before = h.calls.length;
  h.dispose(); for (const s of h.sockets) s.fn({}); for (const t of h.intervals) t.fn();
  await action.onClick(); await flush();
  assert.equal(h.calls.length, before); assert.equal(h.reveals.length, 0); assert.equal(h.notifications.length, 1);
});

test('malformed release response preserves uncertainty without duplicate notify', async () => {
  let releases = 0;
  const h = await harness({ notify: () => '', rest: path => {
    if (path.startsWith('/events?')) return pending(); if (path === '/events/claim') return claims();
    return ++releases === 1 ? { status: 'released', unexpected: 'SECRET' } : { status: 'already_released' };
  } });
  await flush(); h.poll(); await flush();
  assert.equal(h.notifications.length, 1); assert.equal(releases, 2); assert.equal(h.queryCalls.length, 1);
  assert.deepEqual(of(h, '/events/release')[0].opts.body, of(h, '/events/release')[1].opts.body); h.dispose();
});
test('empty claim produces no notification or mutation', async () => {
  const h = await harness({ rest: path => path.startsWith('/events?') ? pending() : claims(null) });
  await flush(); assert.equal(h.calls.length, 2); assert.equal(h.notifications.length, 0); h.dispose();
});
test('malformed pure read is never stored in the shared query cache', async () => {
  const h = await harness({ rest: () => pending(event({ url: 'SECRET' })) }); await flush();
  assert.equal(h.cache.size, 0); assert.equal(h.calls.length, 1); h.dispose();
});
test('scope change while shared fetch is pending prevents claim even after the read completed', async () => {
  const d = deferred();
  const h = await harness({ fetchQuery: async opts => { const result = await opts.queryFn(); await d.promise; return result; } });
  await flush(); assert.equal(h.calls.length, 1);
  h.state.gateway.set('closed'); d.resolve(); await flush();
  assert.equal(h.calls.length, 1); assert.equal(h.notifications.length, 0); assert.equal(h.cache.size, 0); h.dispose();
});
for (const state of ['presented', 'resolved']) test(`claim with ${state} DTO grants no presentation authority`, async () => {
  const h = await harness({ rest: path => path.startsWith('/events?') ? pending() : claims(event({ presentation_state: state })) });
  await flush(); assert.equal(h.notifications.length, 0); assert.equal(h.calls.length, 2); h.dispose();
});
