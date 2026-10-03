import { host, queryClient } from '@hermes/plugin-sdk';

const ID = 'hermes-downloads';
const UUID = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';
const EVENT_ID = new RegExp(`^${UUID}:([1-9][0-9]{0,15})$`);
const IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const TYPES = new Set(['completion', 'needs_link', 'needs_login', 'needs_space', 'persistent_error']);
const REASONS = new Set(['link_expired', 'login_required', 'insufficient_space', 'retry_exhausted', 'download_failed']);
const STATES = new Set(['pending', 'claimed', 'presented', 'resolved']);
const EVENT_FIELDS = ['event_id', 'type', 'job_id', 'collection_id', 'generation', 'created_at_ms', 'count', 'reason_code', 'reveal_path', 'presentation_state'];

function exact(value, fields) {
  return value !== null && typeof value === 'object' && !Array.isArray(value) &&
    Object.getPrototypeOf(value) === Object.prototype &&
    Reflect.ownKeys(value).length === fields.length && fields.every(key => Object.hasOwn(value, key));
}
const counter = value => Number.isSafeInteger(value) && value >= 0;
const identifier = value => value === null || (typeof value === 'string' && IDENTIFIER.test(value));
const deliverable = value => value.presentation_state === 'pending' || value.presentation_state === 'claimed';
function requireValid(ok) { if (!ok) throw new Error('Invalid download notification bridge response'); }

function validPath(path) {
  if (typeof path !== 'string' || path.length > 4096 || !path.startsWith('/') ||
      /[\u0000-\u001f\u007f-\u009f\\\ud800-\udfff]/u.test(path)) return false;
  const parts = path.slice(1).split('/');
  if (parts.some(part => !part.trim() || part === '.' || part === '..' || part.length > 255)) return false;
  // No home-directory API is exposed by the SDK. This is lexical validation;
  // the future backend must certify the actual user's canonical owned path.
  return parts.length >= 6 && (parts[0] === 'Users' || parts[0] === 'home') &&
    parts[2] === 'Downloads' && parts[3] === 'Hermes';
}
function dto(value) {
  requireValid(exact(value, EVENT_FIELDS));
  const match = typeof value.event_id === 'string' && EVENT_ID.exec(value.event_id);
  requireValid(match && Number.isSafeInteger(Number(match[1])) && TYPES.has(value.type) &&
    identifier(value.job_id) && identifier(value.collection_id) && counter(value.generation) &&
    counter(value.created_at_ms) && Number.isSafeInteger(value.count) && value.count >= 1 && value.count <= 500 &&
    (value.reason_code === null || REASONS.has(value.reason_code)) && STATES.has(value.presentation_state) &&
    (value.reveal_path === null || (value.type === 'completion' && validPath(value.reveal_path))));
  // Keep only bounded, validated fields; no arbitrary server text reaches UI.
  return Object.fromEntries(EVENT_FIELDS.map(key => [key, value[key]]));
}
function envelope(value, fields) {
  requireValid(exact(value, fields) && value.schema_version === 1 && value.profile === 'default' && value.source === 'local');
}
function pending(value) {
  envelope(value, ['schema_version', 'profile', 'source', 'events', 'next_cursor', 'has_more']);
  requireValid(Array.isArray(value.events) && value.events.length <= 1 && typeof value.has_more === 'boolean' &&
    (value.next_cursor === null || (typeof value.next_cursor === 'string' && /^[A-Za-z0-9._:-]{1,256}$/.test(value.next_cursor))));
  return value.events.map(dto);
}
function claimed(value) {
  envelope(value, ['schema_version', 'profile', 'source', 'claims', 'server_now_ms']);
  requireValid(counter(value.server_now_ms) && Array.isArray(value.claims) && value.claims.length <= 1);
  if (value.claims.length === 0) return null;
  const claim = value.claims[0];
  requireValid(exact(claim, ['event', 'claim_token', 'lease_expires_at_ms']) &&
    typeof claim.claim_token === 'string' && /^[0-9a-f]{64}$/.test(claim.claim_token) &&
    counter(claim.lease_expires_at_ms) && claim.lease_expires_at_ms > value.server_now_ms);
  const event = dto(claim.event);
  requireValid(deliverable(event));
  return { event, claim_token: claim.claim_token };
}
function status(value, allowed) {
  requireValid(exact(value, ['status']) && allowed.includes(value.status));
  return value.status;
}
function receipt(value) {
  return typeof value === 'string' && value.trim().length > 0 && value.length <= 256 &&
    !/[\u0000-\u001f\u007f-\u009f]/u.test(value);
}
function notification(event, action) {
  const count = event.count.toLocaleString('fa-IR');
  const copy = {
    completion: ['دانلود کامل شد', `${count} دانلود آماده است.`, 'success'],
    needs_link: ['لینک تازه لازم است', `${count} دانلود به لینک تازه نیاز دارد.`, 'warning'],
    needs_login: ['ورود لازم است', `${count} دانلود به ورود نیاز دارد.`, 'warning'],
    needs_space: ['فضای کافی نیست', `${count} دانلود به فضای آزاد نیاز دارد.`, 'warning'],
    persistent_error: ['دانلود به بررسی نیاز دارد', `${count} دانلود با خطای پایدار روبه‌رو شده است.`, 'error'],
  };
  const [title, message, kind] = copy[event.type];
  return { id: `${ID}:${event.event_id}`, title, message, kind, durationMs: kind === 'success' ? 5000 : 0, ...(action ? { action } : {}) };
}

export default {
  id: ID,
  name: 'اعلان دانلودهای هرمس',
  description: 'اعلان‌های دانلود محلی؛ نیازمند پل رویداد آینده.',
  defaultEnabled: false,
  register(ctx) {
    const instance = crypto.randomUUID();
    let disposed = false, generation = 0, active = null, running = false, requested = false;
    const inScope = () => host.state.profile.get() === 'default' &&
      (host.state.connectionId.get() === null || host.state.connectionId.get() === 'local') &&
      host.state.gateway.get() === 'open';
    const guard = scope => !disposed && active === scope && scope.generation === generation && inScope();
    const cleanQuery = scope => {
      const filters = { queryKey: scope.key, exact: true };
      if (typeof queryClient.cancelQueries === 'function') {
        try { void Promise.resolve(queryClient.cancelQueries(filters)).catch(() => {}); } catch { /* optional cleanup */ }
      }
      if (typeof queryClient.removeQueries === 'function') {
        try { queryClient.removeQueries(filters); } catch { /* optional cleanup */ }
      }
    };
    function retire() {
      const previous = active;
      active = null;
      generation++;
      requested = false;
      if (previous) {
        for (const cancel of previous.cancels) cancel();
        previous.retained = null;
        cleanQuery(previous);
      }
      // ctx.rest follows the current route: never release an old-scope claim.
      // Its durable lease is the recovery mechanism when scope is lost.
    }
    async function settle(scope) {
      const retained = scope.retained;
      if (!retained || !guard(scope)) return;
      const presented = retained.mode === 'presented';
      const body = {
        consumer_id: scope.consumer,
        event_id: retained.event.event_id,
        claim_token: retained.claim_token,
        ...(presented ? { notification_id: retained.receipt } : {}),
      };
      try {
        if (!guard(scope)) return;
        const response = await ctx.rest(presented ? '/events/presented' : '/events/release', { method: 'POST', body, timeoutMs: 5000 });
        if (!guard(scope)) return;
        status(response, presented ? ['presented', 'already_presented', 'stale_claim'] : ['released', 'already_released', 'stale_claim']);
        scope.retained = null;
      } catch {
        // One bounded authority remains. Retry the same mutation on a later
        // wake, never another notify while its outcome is uncertain.
      }
    }
    async function reconcile(scope) {
      if (!guard(scope)) return;
      if (scope.retained) { await settle(scope); return; }
      try {
        const events = await queryClient.fetchQuery({
          queryKey: scope.key, staleTime: 0, gcTime: 15000, retry: false,
          queryFn: async () => {
            requireValid(guard(scope));
            const response = await ctx.rest('/events?view=pending&limit=1', { method: 'GET', timeoutMs: 5000 });
            requireValid(guard(scope));
            return pending(response);
          },
        });
        if (!guard(scope)) return;
        if (!events.length || !deliverable(events[0])) return;
        const response = await ctx.rest('/events/claim', { method: 'POST', body: { consumer_id: scope.consumer, limit: 1 }, timeoutMs: 5000 });
        if (!guard(scope)) return;
        const claim = claimed(response);
        if (!claim) return;
        const path = claim.event.reveal_path;
        const action = path === null ? undefined : {
          label: 'نمایش در پوشه',
          onClick: async () => {
            if (!guard(scope)) return;
            try {
              await ctx.os.revealPath(path);
              if (!guard(scope)) return;
            } catch { /* no fallback or arbitrary server text */ }
          },
        };
        if (!guard(scope)) return;
        // Store insertion receipt is synchronous in the actual host SDK.
        // It does not prove visual paint or that the user read the toast.
        let inserted;
        try { inserted = host.notify(notification(claim.event, action)); } catch { inserted = null; }
        if (!guard(scope)) return;
        const accepted = receipt(inserted);
        scope.retained = { ...claim, mode: accepted ? 'presented' : 'release', receipt: accepted ? inserted : null };
        await settle(scope);
      } catch {
        // Malformed/absent future bridge and transport failures stay silent.
      } finally {
        if (!guard(scope)) cleanQuery(scope);
      }
    }
    async function lane() {
      if (running) return;
      running = true;
      try {
        while (requested && active && !disposed) {
          requested = false;
          await reconcile(active);
        }
      } finally { running = false; }
    }
    function wake(scope) {
      if (!guard(scope)) return;
      // Socket contents are only a wake; reads use the shared pure query.
      try {
        void Promise.resolve(queryClient.invalidateQueries({ queryKey: scope.key, exact: true, refetchType: 'none' })).catch(() => {});
      } catch { /* poll still requests the bounded read */ }
      requested = true;
      void lane();
    }
    function activate() {
      retire();
      if (disposed || !inScope()) return;
      const scope = {
        generation, consumer: crypto.randomUUID(),
        key: [ID, 'local', 'default', instance, generation, 'pending'],
        cancels: [], retained: null,
      };
      active = scope;
      scope.cancels.push(ctx.setInterval(() => wake(scope), 15000));
      scope.cancels.push(ctx.socket('/events', () => wake(scope)));
      wake(scope);
    }
    for (const atom of [host.state.profile, host.state.connectionId, host.state.gateway]) {
      ctx.onDispose(atom.listen(activate));
    }
    ctx.onDispose(() => { disposed = true; retire(); });
    activate();
  },
};
