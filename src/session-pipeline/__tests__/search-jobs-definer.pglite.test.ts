/**
 * Least privilege for the search-index consumer (migration 20261105062300, docs/handoffs/incremental-search-indexing.md
 * §3.2, §3.3, §3.3b): the runtime boundary is exactly sessions.claim_search_jobs_v1 and sessions.settle_search_job_v1,
 * SECURITY DEFINER, and settlement authenticates the claim-time snapshot from the server's receipt
 * (sessions.search_job_claims), never from the caller.
 *
 * Every behavioural case runs as an ACTUAL unprivileged LOGIN (SET SESSION AUTHORIZATION, so session_user and
 * current_user are the login) with NO role membership, NO table privilege and only EXECUTE on claim and settle. The
 * hosted-equivalent cases add exactly the runtime role's pipeline-table privileges (RUNTIME_APPLICATION_TABLE_PRIVILEGES)
 * and an all-row policy, so the 42501s there come from the stage's own guards, not from missing grants.
 *
 * PGlite cannot switch back from SET SESSION AUTHORIZATION, so each case is its own clone, and state is seeded and
 * observed through owner-made SECURITY DEFINER fixture functions (public.fx_*) created beforehand, as in
 * search-wake-truncate-guard.pglite.test.ts. They are test fixtures, never production code. fx_wake is the §3.1
 * max-epoch merge done by the owner (the writer path), the only way a newer wake reaches a leased row.
 * Synthetic, secret-free data only.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { deadLetterId, type StageMessage } from "@rhc/lib/pipeline/dlq.ts";
import { RUNTIME_APPLICATION_TABLE_PRIVILEGES } from "../../hoopdb/migrate-guards.ts";
import {
  archiveSearchStage, catchUpTick, claimSearchJobs, enqueueSearchWake, parkSearchJobs, replaySearchDeadLetter, searchLetterId,
  searchWorkKey, settleSearchJob, type ClaimedSearchJob, type SearchWakeMessage,
} from "../search-jobs.ts";
import { clonedReplayDatabase, closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanup.splice(0).reverse()) await c(); });
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

const LOGIN = "search_runtime_login";
const HOSTED = "search_runtime_hosted";
const CLAIM = "sessions.claim_search_jobs_v1(integer)";
const SETTLE = "sessions.settle_search_job_v1(bigint,uuid,text)";

type Scope = { orgId: string; workspaceId: string; harness: string; sessionId: string };
const scope: Scope = { orgId: "org:def enc", workspaceId: "ws def", harness: "claude", sessionId: "s_def" };
const other: Scope = { ...scope, sessionId: "s_other" };
const wake = (s: Scope, epoch: number): SearchWakeMessage => ({
  v: 1, stage: "search-index", ctx: { traceId: crypto.randomUUID(), ...s }, workKey: searchWorkKey(s), revision: 1,
  payload: { epoch, clock: { source: "pointer", at: "2026-11-01T10:00:00.000Z" } },
});

async function codeOf(p: Promise<unknown> | (() => Promise<unknown>)): Promise<string> {
  try { await (typeof p === "function" ? p() : p); return "ok"; } catch (e) { return String((e as { code?: string }).code ?? e); }
}

/** Owner-made fixture functions: seed and observe across the authorization switch. */
const FIXTURES = `
CREATE FUNCTION public.fx_state() RETURNS jsonb LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
  SELECT jsonb_build_object(
    'jobs', coalesce((SELECT jsonb_agg(jsonb_build_object('id', j.id::text, 'stage', j.stage, 'epoch', j.message->'payload'->>'epoch',
              'lease', j.lease_id::text, 'attempts', j.attempts, 'dlq', j.dlq, 'visible', j.visible_at <= now(), 'trace', j.trace_id::text,
              'in', round(extract(epoch FROM j.visible_at - now()))::int, 'message', j.message) ORDER BY j.id)
             FROM sessions.pipeline_jobs j), '[]'::jsonb),
    'receipts', coalesce((SELECT jsonb_agg(jsonb_build_object('id', c.job_id::text, 'lease', c.lease_id::text, 'epoch', c.claimed_epoch,
              'attempts', c.claimed_attempts, 'message', c.claimed_message, 'ttl', round(extract(epoch FROM c.lease_until - now()))::int,
              'live_at_tx_start', c.lease_until > now(), 'live_on_clock', c.lease_until > clock_timestamp(), 'reclaims', c.reclaim_count) ORDER BY c.job_id)
             FROM sessions.search_job_claims c), '[]'::jsonb),
    'contention', coalesce((SELECT jsonb_agg(jsonb_build_object('id', k.job_id::text, 'epoch', k.epoch, 'count', k.contention_count,
              'escalated', k.last_escalated_at IS NOT NULL, 'held', k.held_at IS NOT NULL,
              'probe_in', round(extract(epoch FROM k.next_probe_at - clock_timestamp()))::int) ORDER BY k.job_id)
             FROM sessions.search_job_contention k), '[]'::jsonb),
    'letters', coalesce((SELECT jsonb_agg(jsonb_build_object('id', d.id, 'state', d.state, 'dead_count', d.dead_count,
              'resolution', d.resolution, 'message', d.message, 'provenance', jsonb_build_object('trace', d.trace_id::text,
              'org', d.org_id, 'ws', d.workspace_id, 'session', d.session_id, 'machine', d.machine_id)) ORDER BY d.id) FROM sessions.pipeline_dead_letters d), '[]'::jsonb),
    'events', coalesce((SELECT jsonb_agg(jsonb_build_object('status', e.status, 'attempt', e.attempt, 'trace', e.trace_id::text,
              'key', e.idempotency_key, 'attrs', e.attrs, 'code', e.error->>'code') ORDER BY e.attempt, e.at, e.status)
             FROM sessions.pipeline_events e WHERE e.stage = 'search-index'), '[]'::jsonb))
$$;
-- §3.1's max-epoch merge, by the owner (the writer): a newer epoch replaces the message and resets the budget; a
-- leased row keeps its lease.
CREATE FUNCTION public.fx_wake(m jsonb) RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
  INSERT INTO sessions.pipeline_jobs AS j (stage, work_key, trace_id, org_id, workspace_id, session_id, message, visible_at)
  VALUES ('search-index', m->>'workKey', (m->'ctx'->>'traceId')::uuid, m->'ctx'->>'orgId', m->'ctx'->>'workspaceId', m->'ctx'->>'sessionId', m, now())
  ON CONFLICT (work_key) WHERE stage = 'search-index' DO UPDATE SET message = EXCLUDED.message, trace_id = EXCLUDED.trace_id,
    attempts = 0, dlq = false, visible_at = CASE WHEN j.lease_id IS NULL THEN now() ELSE j.visible_at END
  WHERE (EXCLUDED.message->'payload'->>'epoch')::bigint > (j.message->'payload'->>'epoch')::bigint
$$;
-- Time passes: every search-index lease and backoff, and every receipt, is over.
CREATE FUNCTION public.fx_expire() RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
  UPDATE sessions.pipeline_jobs SET visible_at = now() - interval '1 second' WHERE stage = 'search-index';
  UPDATE sessions.search_job_claims SET lease_until = now() - interval '1 second';
$$;
-- The lease (row and receipt) ends p_ms after the wall clock now: inside a caller's transaction, after its start.
CREATE FUNCTION public.fx_lease_ends_in(p_ms integer) RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
  UPDATE sessions.pipeline_jobs SET visible_at = clock_timestamp() + make_interval(secs => p_ms / 1000.0) WHERE stage = 'search-index' AND lease_id IS NOT NULL;
  UPDATE sessions.search_job_claims SET lease_until = clock_timestamp() + make_interval(secs => p_ms / 1000.0);
$$;
-- Contention began p_hours ago; the last escalation (if p_escalated_hours is given) p_escalated_hours ago.
CREATE FUNCTION public.fx_contended_since(p_hours numeric, p_escalated_hours numeric) RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
  UPDATE sessions.search_job_contention SET first_contended_at = clock_timestamp() - make_interval(secs => p_hours * 3600),
    last_escalated_at = CASE WHEN p_escalated_hours IS NULL THEN last_escalated_at ELSE clock_timestamp() - make_interval(secs => p_escalated_hours * 3600) END
$$;
-- Every search-index job held by the 24 h cap at its current epoch; its probe due now (p_due) or in an hour.
CREATE FUNCTION public.fx_hold_all(p_due boolean) RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
  INSERT INTO sessions.search_job_contention (job_id, epoch, first_contended_at, last_contended_at, contention_count, last_escalated_at, held_at, next_probe_at)
  SELECT j.id, (j.message->'payload'->>'epoch')::bigint, clock_timestamp() - interval '25 hours', clock_timestamp(), 100,
         clock_timestamp() - interval '10 minutes', clock_timestamp() - interval '1 hour',
         CASE WHEN p_due THEN clock_timestamp() - interval '1 second' ELSE clock_timestamp() + interval '1 hour' END
    FROM sessions.pipeline_jobs j WHERE j.stage = 'search-index'
  ON CONFLICT (job_id) DO UPDATE SET held_at = EXCLUDED.held_at, next_probe_at = EXCLUDED.next_probe_at, epoch = EXCLUDED.epoch
$$;
-- An hour passes for every hold: each held job's probe is due.
CREATE FUNCTION public.fx_probe_due() RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
  UPDATE sessions.search_job_contention SET next_probe_at = clock_timestamp() - interval '1 second' WHERE held_at IS NOT NULL
$$;
-- The fleet probe bucket holds p_tokens, last refilled p_age_s ago.
CREATE FUNCTION public.fx_budget(p_tokens numeric, p_age_s numeric) RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
  UPDATE sessions.search_probe_budget SET tokens = p_tokens, refilled_at = clock_timestamp() - make_interval(secs => p_age_s)
$$;
CREATE FUNCTION public.fx_attempts(p_id bigint, n integer) RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
  UPDATE sessions.pipeline_jobs SET attempts = n WHERE id = p_id
$$;
-- A job of ANOTHER stage: leased (with a known lease, invisible) or, with a NULL lease, open and visible now.
CREATE FUNCTION public.fx_other_stage(p_lease uuid, p_key text) RETURNS bigint LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
  INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, session_id, message, visible_at, lease_id, attempts)
  VALUES ('session-indexed', p_key, gen_random_uuid(), 'org_x', 'ws_x', 's_x',
          jsonb_build_object('v', 1, 'stage', 'session-indexed', 'payload', jsonb_build_object('epoch', 1)),
          CASE WHEN p_lease IS NULL THEN now() - interval '1 second' ELSE now() + interval '5 minutes' END, p_lease, 1)
  RETURNING id
$$;
-- A settlement failing midway (after its row writes): the events insert raises while fx.fail is on.
CREATE FUNCTION public.fx_fail_events() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF current_setting('fx.fail', true) = 'on' THEN RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'fixture: settlement interrupted'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER fx_fail_events BEFORE INSERT ON sessions.pipeline_events FOR EACH ROW EXECUTE FUNCTION public.fx_fail_events();
GRANT EXECUTE ON FUNCTION public.fx_state(), public.fx_wake(jsonb), public.fx_expire(), public.fx_lease_ends_in(integer), public.fx_contended_since(numeric, numeric), public.fx_hold_all(boolean), public.fx_probe_due(), public.fx_budget(numeric, numeric), public.fx_attempts(bigint, integer),
  public.fx_other_stage(uuid, text) TO PUBLIC;
`;

type State = {
  jobs: Array<{ id: string; stage: string; epoch: string; trace: string; lease: string | null; attempts: number; dlq: boolean; visible: boolean; in: number; message: StageMessage }>;
  receipts: Array<{ id: string; lease: string; epoch: number; attempts: number; message: StageMessage; ttl: number; live_at_tx_start: boolean; live_on_clock: boolean; reclaims: number }>;
  contention: Array<{ id: string; epoch: number; count: number; escalated: boolean; held: boolean; probe_in: number | null }>;
  letters: Array<{ id: string; state: string; dead_count: number; resolution: string | null; message: StageMessage;
    provenance: { trace: string; org: string; ws: string; session: string | null; machine: string | null } }>;
  events: Array<{ status: string; attempt: number; trace: string; key: string; attrs: Record<string, unknown>; code: string | null }>;
};

/**
 * A clone, seeded by the owner, then switched to a fresh LOGIN: `bare` holds only USAGE on sessions and EXECUTE on
 * claim and settle; `hosted` adds the runtime role's exact pipeline-table privileges and an all-row policy.
 */
async function asLogin(kind: "bare" | "hosted", seed: (pg: PGlite) => Promise<void> = async () => {}, opts: { restart?: boolean } = {}) {
  let pg = (await clonedReplayDatabase()).pg;
  cleanup.push(() => pg.close());
  await pg.exec(FIXTURES);
  await seed(pg);
  if (opts.restart) {
    // A fresh client on the same data (a new process's view): only persisted state carries over.
    const fresh = await pg.clone() as PGlite;
    await pg.close();
    pg = fresh;
  }
  const role = kind === "bare" ? LOGIN : HOSTED;
  await pg.exec(`CREATE ROLE ${role} LOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION;
    GRANT USAGE ON SCHEMA sessions TO ${role};
    GRANT EXECUTE ON FUNCTION ${CLAIM}, ${SETTLE} TO ${role};`);
  if (kind === "hosted") {
    for (const t of ["sessions.pipeline_jobs", "sessions.pipeline_events", "sessions.pipeline_dead_letters"] as const) {
      const privileges = (RUNTIME_APPLICATION_TABLE_PRIVILEGES as Record<string, readonly string[]>)[t]!;
      await pg.exec(`GRANT ${privileges.join(", ")} ON ${t} TO ${role};
        CREATE POLICY fx_trusted_backend ON ${t} TO ${role} USING (true) WITH CHECK (true);`);
    }
    await pg.exec(`GRANT USAGE ON SEQUENCE sessions.pipeline_jobs_id_seq TO ${role}`);
  }
  // No membership in either direction.
  expect((await pg.query("SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid IN (m.member, m.roleid) WHERE r.rolname = $1", [role])).rows).toEqual([]);
  await pg.exec(`SET SESSION AUTHORIZATION ${role}`);
  expect((await pg.query<{ s: string; c: string; su: boolean }>(
    "SELECT session_user AS s, current_user AS c, (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) AS su")).rows)
    .toEqual([{ s: role, c: role, su: false }]);
  const state = async () => (await pg.query<{ s: State }>("SELECT public.fx_state() AS s")).rows[0]!.s;
  return { pg, state, wakeNow: (m: SearchWakeMessage) => pg.query("SELECT public.fx_wake($1::jsonb)", [JSON.stringify(m)]),
    expire: () => pg.query("SELECT public.fx_expire()"),
    attempts: (id: string, n: number) => pg.query("SELECT public.fx_attempts($1::bigint, $2)", [id, n]),
    contendedSince: (hours: number, escalatedHours: number | null = null) =>
      pg.query("SELECT public.fx_contended_since($1::numeric, $2::numeric)", [hours, escalatedHours]),
    holdAll: (due: boolean) => pg.query("SELECT public.fx_hold_all($1)", [due]),
    probeDue: () => pg.query("SELECT public.fx_probe_due()"),
    budget: (tokens: number, ageSeconds = 0) => pg.query("SELECT public.fx_budget($1::numeric, $2::numeric)", [tokens, ageSeconds]) };
}
const seeded = (...wakes: SearchWakeMessage[]) => async (pg: PGlite) => { for (const w of wakes) await enqueueSearchWake(pg, w); };
async function claimOne(pg: PGlite): Promise<ClaimedSearchJob> {
  const c = await claimSearchJobs(pg, 1);
  expect(c).toHaveLength(1);
  return c[0]!;
}

// ---------------------------------------------------------------------------------------------------------
describe("claim and settle as the unprivileged login (EXECUTE on the two RPCs only)", () => {
  test("claim, then done: the stored snapshot is returned; the row, the receipt go; one ok event, from the receipt", async () => {
    const w7 = wake(scope, 7);
    const { pg, state } = await asLogin("bare", seeded(w7));
    const c = await claimOne(pg);
    expect({ epoch: c.claimedEpoch, attempts: c.attempts, message: c.claimedMessage }).toEqual({ epoch: 7, attempts: 1, message: w7 });
    const s = await state();
    // The receipt is the claim: same lease, epoch, attempt and message; the server's 300 s lease.
    expect(s.receipts.map((r) => ({ id: r.id, lease: r.lease, epoch: r.epoch, attempts: r.attempts, message: r.message })))
      .toEqual([{ id: c.id, lease: c.lease, epoch: 7, attempts: 1, message: w7 }]);
    expect(s.receipts[0]!.ttl).toBeGreaterThanOrEqual(298);
    expect(s.receipts[0]!.ttl).toBeLessThanOrEqual(300);
    expect(s.jobs.map((j) => ({ lease: j.lease, attempts: j.attempts }))).toEqual([{ lease: c.lease, attempts: 1 }]);
    expect(await settleSearchJob(pg, c, "done")).toEqual({ applied: true, action: "deleted" });
    const after = await state();
    expect({ jobs: after.jobs, receipts: after.receipts, letters: after.letters }).toEqual({ jobs: [], receipts: [], letters: [] });
    expect(after.events.map((e) => ({ status: e.status, attempt: e.attempt, trace: e.trace, epoch: e.attrs.epoch })))
      .toEqual([{ status: "ok", attempt: 1, trace: w7.ctx.traceId, epoch: 7 }]);
    expect(after.events[0]!.key).toBe(`${w7.ctx.traceId}:search-index:ok:1:${w7.workKey}@7`);
  }, 120_000);

  test("claim, then retry: the server's backoff (30 s, then 60 s), whatever delay the caller names; the budget is kept", async () => {
    const { pg, state, expire } = await asLogin("bare", seeded(wake(scope, 7)));
    const c1 = await claimOne(pg);
    expect(await settleSearchJob(pg, c1, { retry: 0 })).toEqual({ applied: true, action: "retried" });
    let s = await state();
    expect(s.jobs.map((j) => ({ lease: j.lease, attempts: j.attempts, visible: j.visible }))).toEqual([{ lease: null, attempts: 1, visible: false }]);
    expect(s.jobs[0]!.in).toBeGreaterThanOrEqual(29);
    expect(s.jobs[0]!.in).toBeLessThanOrEqual(30);
    expect(s.receipts).toEqual([]);
    expect(await claimSearchJobs(pg, 5)).toEqual([]);
    await expire();
    const c2 = await claimOne(pg);
    expect(c2.attempts).toBe(2);
    expect(await settleSearchJob(pg, c2, { retry: 1 })).toEqual({ applied: true, action: "retried" });
    s = await state();
    expect(s.jobs[0]!.in).toBeGreaterThanOrEqual(59);
    expect(s.jobs[0]!.in).toBeLessThanOrEqual(60);
    expect(s.events.map((e) => [e.status, e.attempt, e.attrs.delay_seconds])).toEqual([["retrying", 1, 30], ["retrying", 2, 60]]);
  }, 120_000);

  test("exhaustion at the server max (12): the letter is the 12th claim's snapshot; 'exhausted' asked early is only a retry", async () => {
    const w = wake(scope, 7);
    const { pg, state, expire } = await asLogin("bare", seeded(w));
    let last: ClaimedSearchJob | undefined;
    const delays: number[] = [];
    for (let n = 1; n <= 12; n++) {
      await expire();
      const c = await claimOne(pg);
      expect(c.attempts).toBe(n);
      // The caller asks for exhaustion on the first attempt: the server backs off instead.
      const r = await settleSearchJob(pg, c, n === 1 ? "exhausted" : { retry: 0 });
      if (n < 12) {
        expect(r).toEqual({ applied: true, action: "retried" });
        delays.push((await state()).jobs[0]!.in);
      } else { expect(r).toEqual({ applied: true, action: "parked" }); last = c; }
    }
    expect(delays).toEqual([30, 60, 120, 240, 480, 900, 900, 900, 900, 900, 900]);
    const s = await state();
    expect(s.jobs).toEqual([]);
    expect(s.receipts).toEqual([]);
    expect(s.letters.map((l) => ({ id: l.id, state: l.state, dead_count: l.dead_count }))).toEqual([{ id: searchLetterId(w), state: "dead", dead_count: 1 }]);
    expect(s.letters[0]!.message).toEqual(last!.claimedMessage);
    expect(s.letters[0]!.message).toEqual(w);
    expect(s.events.at(-1)).toMatchObject({ status: "dead_lettered", attempt: 12, code: "search_index_held_exhausted" });
  }, 120_000);

  test("the attempt-12 race: N exhausts after N+1 (another trace) merged; the letter is wholly N's receipt, the row N+1's, reopened", async () => {
    const wN = wake(scope, 7), wN1 = wake(scope, 8);
    expect(wN1.ctx.traceId).not.toBe(wN.ctx.traceId);
    const { pg, state, wakeNow } = await asLogin("bare", async (p) => {
      await seeded(wN)(p);
      // Fixture: the row has already used 11 attempts, so the next claim is the 12th.
      await p.query("UPDATE sessions.pipeline_jobs SET attempts = 11 WHERE stage = 'search-index'");
    });
    const c = await claimOne(pg);
    expect({ attempts: c.attempts, epoch: c.claimedEpoch, message: c.claimedMessage }).toEqual({ attempts: 12, epoch: 7, message: wN });
    await wakeNow(wN1);
    // Fixture soundness: the live row now carries N+1's message and trace, a reset budget, still under N's lease.
    const merged = (await state()).jobs;
    expect(merged.map((j) => ({ lease: j.lease, attempts: j.attempts, message: j.message }))).toEqual([{ lease: c.lease, attempts: 0, message: wN1 }]);
    expect(await settleSearchJob(pg, c, { retry: 0 })).toEqual({ applied: true, action: "parked_reopened" });
    const s = await state();
    expect(s.letters.map((l) => ({ id: l.id, state: l.state, message: l.message }))).toEqual([{ id: searchLetterId(wN), state: "dead", message: wN }]);
    expect(s.letters[0]!.provenance).toEqual({ trace: wN.ctx.traceId, org: scope.orgId, ws: scope.workspaceId, session: scope.sessionId, machine: null });
    expect(s.jobs.map((j) => ({ lease: j.lease, attempts: j.attempts, visible: j.visible, trace: j.trace, message: j.message })))
      .toEqual([{ lease: null, attempts: 0, visible: true, trace: wN1.ctx.traceId, message: wN1 }]);
    expect(s.receipts).toEqual([]);
    expect(s.events.map((e) => ({ status: e.status, trace: e.trace, attempt: e.attempt }))).toEqual([{ status: "dead_lettered", trace: wN.ctx.traceId, attempt: 12 }]);
  }, 120_000);

  test("lease freshness is the wall clock after the locks: a lease that ends inside the settling transaction applies nothing", async () => {
    const { pg, state } = await asLogin("bare", seeded(wake(scope, 7)));
    const c = await claimOne(pg);
    // One transaction: it starts while the lease is live (its now() is before the lease end), the lease then ends,
    // and only then is the settlement made. Judged on the transaction start it would apply; it must not.
    await pg.exec("BEGIN");
    await pg.query("SELECT public.fx_lease_ends_in(300)");
    await pg.query("SELECT pg_sleep(0.6)");
    // Fixture soundness: on the transaction-start clock the lease is still live; on the wall clock it has ended.
    expect((await state()).receipts.map((r) => ({ tx: r.live_at_tx_start, clock: r.live_on_clock }))).toEqual([{ tx: true, clock: false }]);
    expect(await settleSearchJob(pg, c, "done")).toEqual({ applied: false });
    expect(await settleSearchJob(pg, c, { retry: 0 })).toEqual({ applied: false });
    await pg.exec("COMMIT");
    const s = await state();
    expect(s.jobs.map((j) => ({ lease: j.lease, attempts: j.attempts }))).toEqual([{ lease: c.lease, attempts: 1 }]);
    expect(s.receipts.map((r) => r.lease)).toEqual([c.lease]);
  }, 120_000);

  test("a re-death refreshes the scope letter wholly from the new snapshot: park N (trace T1), then N+2 (trace T3)", async () => {
    const t1 = wake(scope, 7), t3 = wake(scope, 9);
    const { pg, state, wakeNow, attempts } = await asLogin("bare", seeded(t1));
    const c1 = await claimOne(pg);
    await attempts(c1.id, 12);
    expect(await settleSearchJob(pg, c1, { retry: 0 })).toEqual({ applied: true, action: "parked" });
    let s = await state();
    expect(s.letters.map((l) => ({ message: l.message, trace: l.provenance.trace, dead_count: l.dead_count }))).toEqual([{ message: t1, trace: t1.ctx.traceId, dead_count: 1 }]);
    await wakeNow(t3);
    const c3 = await claimOne(pg);
    expect(c3.claimedMessage).toEqual(t3);
    await attempts(c3.id, 12);
    expect(await settleSearchJob(pg, c3, { retry: 0 })).toEqual({ applied: true, action: "parked" });
    s = await state();
    expect(t3.ctx.traceId).not.toBe(t1.ctx.traceId);
    expect(s.letters.map((l) => ({ id: l.id, message: l.message, provenance: l.provenance, dead_count: l.dead_count, state: l.state }))).toEqual([{
      id: searchLetterId(t3), message: t3, dead_count: 2, state: "dead",
      provenance: { trace: t3.ctx.traceId, org: scope.orgId, ws: scope.workspaceId, session: scope.sessionId, machine: null },
    }]);
  }, 120_000);

  test("an expired lease settles nothing: {applied:false}, row and receipt unchanged", async () => {
    const { pg, state, expire } = await asLogin("bare", seeded(wake(scope, 7)));
    const c = await claimOne(pg);
    await expire();
    const before = await state();
    for (const outcome of ["done", { retry: 30 }, "exhausted"] as const)
      expect(await settleSearchJob(pg, c, outcome)).toEqual({ applied: false });
    expect(await state()).toEqual(before);
  }, 120_000);

  test("a replaced lease (reclaimed after expiry) cannot settle: the old lease gets {applied:false}; the receipt is the new claim's", async () => {
    const { pg, state, expire, wakeNow } = await asLogin("bare", seeded(wake(scope, 7)));
    const old = await claimOne(pg);
    await expire();
    // A newer wake lands before the reclaim: the reclaim's snapshot replaces the old one.
    const w8 = wake(scope, 8);
    await wakeNow(w8);
    const fresh = await claimOne(pg);
    expect(fresh.id).toBe(old.id);
    expect(fresh.lease).not.toBe(old.lease);
    expect({ epoch: fresh.claimedEpoch, message: fresh.claimedMessage }).toEqual({ epoch: 8, message: w8 });
    const s = await state();
    expect(s.receipts.map((r) => ({ lease: r.lease, epoch: r.epoch, message: r.message }))).toEqual([{ lease: fresh.lease, epoch: 8, message: w8 }]);
    expect(await settleSearchJob(pg, old, "done")).toEqual({ applied: false });
    expect(await state()).toEqual(s);
    expect(await settleSearchJob(pg, fresh, "done")).toEqual({ applied: true, action: "deleted" });
  }, 120_000);

  test("tampering: a forged epoch or message has no effect; the receipt decides (wake N+1 during the lease: done reopens)", async () => {
    const w7 = wake(scope, 7);
    const { pg, state, wakeNow, attempts } = await asLogin("bare", seeded(w7));
    const c = await claimOne(pg);
    const w8 = wake(scope, 8);
    await wakeNow(w8);
    // The lease holder claims it ran N+1 (and names a forged, other-scope message): the server ignores both.
    const forged = { ...c, claimedEpoch: 8, claimedMessage: { ...wake(other, 99), ctx: { ...wake(other, 99).ctx, orgId: "org_forged" } } };
    expect(await settleSearchJob(pg, forged, "done")).toEqual({ applied: true, action: "reopened" });
    let s = await state();
    // A newer wake survives the old settlement: N+1 is kept, reopened now on a fresh budget, not deleted.
    expect(s.jobs.map((j) => ({ epoch: j.epoch, lease: j.lease, visible: j.visible, attempts: j.attempts }))).toEqual([{ epoch: "8", lease: null, visible: true, attempts: 0 }]);
    expect(s.jobs[0]!.message).toEqual(w8);
    expect(s.events.map((e) => ({ trace: e.trace, epoch: e.attrs.epoch, reason: e.attrs.reason }))).toEqual([{ trace: w7.ctx.traceId, epoch: 7, reason: "newer_epoch_reopened" }]);
    // At the last attempt, a forged message is never parked: the letter is the receipt's (N+1), under its own scope.
    const c8 = await claimOne(pg);
    await attempts(c8.id, 12);
    expect(await settleSearchJob(pg, { ...c8, claimedEpoch: 0, claimedMessage: forged.claimedMessage }, { retry: 0 })).toEqual({ applied: true, action: "parked" });
    s = await state();
    expect(s.letters.map((l) => ({ id: l.id, message: l.message }))).toEqual([{ id: searchLetterId(w8), message: w8 }]);
    expect(s.letters.some((l) => l.id.includes("org_forged"))).toBe(false);
    // The RPC itself takes no epoch, message, attempt or delay.
    expect((await pg.query<{ args: string }>("SELECT pg_get_function_arguments($1::regprocedure) AS args", [SETTLE])).rows[0]!.args)
      .toBe("p_job_id bigint, p_lease uuid, p_outcome text");
  }, 120_000);

  test("an other-stage job id is never settled, and a claim never takes another stage's row", async () => {
    const lease = crypto.randomUUID();
    let otherId = "";
    const { pg, state } = await asLogin("bare", async (p) => {
      otherId = (await p.query<{ id: string }>("SELECT public.fx_other_stage($1::uuid, 'definer-leased')::text AS id", [lease])).rows[0]!.id;
      await p.query("SELECT public.fx_other_stage(NULL, 'definer-open')");
    });
    const before = await state();
    expect(before.jobs.map((j) => ({ stage: j.stage, visible: j.visible, leased: j.lease !== null })))
      .toEqual([{ stage: "session-indexed", visible: false, leased: true }, { stage: "session-indexed", visible: true, leased: false }]);
    expect(await claimSearchJobs(pg, 10)).toEqual([]);
    for (const outcome of ["done", { retry: 0 }] as const)
      expect(await settleSearchJob(pg, { id: otherId, lease }, outcome)).toEqual({ applied: false });
    expect(await state()).toEqual(before);
  }, 120_000);

  test("a duplicate settlement is refused: the second gives {applied:false} and changes nothing", async () => {
    const { pg, state, expire } = await asLogin("bare", seeded(wake(scope, 7), wake(other, 3)));
    const [a, b] = await claimSearchJobs(pg, 2);
    expect(await settleSearchJob(pg, a!, "done")).toEqual({ applied: true, action: "deleted" });
    expect(await settleSearchJob(pg, b!, { retry: 0 })).toEqual({ applied: true, action: "retried" });
    const s = await state();
    expect(await settleSearchJob(pg, a!, "done")).toEqual({ applied: false });
    expect(await settleSearchJob(pg, b!, { retry: 0 })).toEqual({ applied: false });
    expect(await settleSearchJob(pg, b!, "done")).toEqual({ applied: false });
    expect(await state()).toEqual(s);
    await expire();
    expect(await settleSearchJob(pg, b!, "done")).toEqual({ applied: false });
  }, 120_000);

  test("rollback: an error midway through a settlement leaves the row, the receipt and the letters unchanged", async () => {
    const { pg, state, attempts } = await asLogin("bare", seeded(wake(scope, 7)));
    const c = await claimOne(pg);
    await attempts(c.id, 12);
    const before = await state();
    await pg.exec("SET fx.fail = 'on'");
    expect(await codeOf(settleSearchJob(pg, c, "done"))).toBe("P0001");
    expect(await codeOf(settleSearchJob(pg, c, { retry: 0 }))).toBe("P0001");
    expect(await state()).toEqual(before);
    await pg.exec("SET fx.fail = 'off'");
    expect(await settleSearchJob(pg, c, { retry: 0 })).toEqual({ applied: true, action: "parked" });
  }, 120_000);

  test("dlq rows are never claimed; the claim is clamped to 10 per call", async () => {
    const many = Array.from({ length: 12 }, (_, n) => wake({ ...scope, sessionId: `s_many_${String(n).padStart(2, "0")}` }, 1));
    const { pg, state } = await asLogin("bare", async (p) => {
      await seeded(wake(scope, 7), ...many)(p);
      await p.query("UPDATE sessions.pipeline_jobs SET dlq = true WHERE work_key = $1", [searchWorkKey(scope)]);
    });
    const claimed = await claimSearchJobs(pg, 50);
    expect(claimed).toHaveLength(10);
    expect(claimed.some((c) => c.claimedMessage.workKey === searchWorkKey(scope))).toBe(false);
    expect(await claimSearchJobs(pg, 50)).toHaveLength(2);
    expect(await claimSearchJobs(pg, 50)).toEqual([]);
    const dlq = (await state()).jobs.filter((j) => j.dlq);
    expect(dlq.map((j) => ({ lease: j.lease, attempts: j.attempts }))).toEqual([{ lease: null, attempts: 0 }]);
  }, 120_000);

  test("a forged outcome string is refused (22023) and changes nothing", async () => {
    const { pg, state } = await asLogin("bare", seeded(wake(scope, 7)));
    const c = await claimOne(pg);
    const before = await state();
    for (const outcome of ["exhausted", "parked", "DONE", "", "done ", "delete"])
      expect(await codeOf(pg.query(`SELECT ${SETTLE.replace(/\(.*$/, "")}($1::bigint, $2::uuid, $3)`, [c.id, c.lease, outcome]))).toBe("22023");
    expect(await codeOf(pg.query("SELECT sessions.settle_search_job_v1($1::bigint, $2::uuid, NULL)", [c.id, c.lease]))).toBe("22023");
    expect(await state()).toEqual(before);
  }, 120_000);

  test("denied: direct search-job and receipt DML, TRUNCATE, the operator paths and every other stage function (42501)", async () => {
    const { pg, state } = await asLogin("bare", seeded(wake(scope, 7)));
    const c = await claimOne(pg);
    const before = await state();
    const denied = {
      insertJob: await codeOf(pg.query(`INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, message)
        VALUES ('search-index', 'x', gen_random_uuid(), 'o', 'w', '{}'::jsonb)`)),
      updateJob: await codeOf(pg.query("UPDATE sessions.pipeline_jobs SET attempts = 0 WHERE stage = 'search-index'")),
      deleteJob: await codeOf(pg.query("DELETE FROM sessions.pipeline_jobs WHERE stage = 'search-index'")),
      truncateJobs: await codeOf(pg.query("TRUNCATE sessions.pipeline_jobs")),
      selectReceipt: await codeOf(pg.query("SELECT * FROM sessions.search_job_claims")),
      insertReceipt: await codeOf(pg.query("INSERT INTO sessions.search_job_claims VALUES (1, gen_random_uuid(), 9, '{}'::jsonb, 1, now(), now())")),
      updateReceipt: await codeOf(pg.query("UPDATE sessions.search_job_claims SET claimed_epoch = 99")),
      deleteReceipt: await codeOf(pg.query("DELETE FROM sessions.search_job_claims")),
      truncateReceipt: await codeOf(pg.query("TRUNCATE sessions.search_job_claims")),
      selectContention: await codeOf(pg.query("SELECT * FROM sessions.search_job_contention")),
      insertContention: await codeOf(pg.query("INSERT INTO sessions.search_job_contention VALUES (1, 1, now(), now(), 0, NULL, NULL)")),
      updateContention: await codeOf(pg.query("UPDATE sessions.search_job_contention SET contention_count = 0")),
      deleteContention: await codeOf(pg.query("DELETE FROM sessions.search_job_contention")),
      truncateContention: await codeOf(pg.query("TRUNCATE sessions.search_job_contention")),
      selectBudget: await codeOf(pg.query("SELECT * FROM sessions.search_probe_budget")),
      updateBudget: await codeOf(pg.query("UPDATE sessions.search_probe_budget SET tokens = 6")),
      resume: await codeOf(pg.query("SELECT sessions.resume_search_contention_v1(1, 'definer-test')")),
      replay: await codeOf(() => replaySearchDeadLetter(pg, searchLetterId(wake(scope, 7)))),
      catchUp: await codeOf(() => catchUpTick({ sql: pg, pageSize: 10 })),
      park: await codeOf(() => parkSearchJobs(pg, { operator: "definer-test" })),
      archive: await codeOf(() => archiveSearchStage(pg)),
      enqueue: await codeOf(() => enqueueSearchWake(pg, wake(scope, 9))),
      managedWake: await codeOf(pg.query("SELECT sessions.managed_search_wake_v1('{}'::jsonb, gen_random_uuid())")),
      letterId: await codeOf(pg.query("SELECT sessions.search_letter_id_v1('o', 'w', 'k')")),
    };
    expect(denied).toEqual(Object.fromEntries(Object.keys(denied).map((k) => [k, "42501"])));
    expect(await state()).toEqual(before);
    // The lease still settles through the RPC.
    expect(await settleSearchJob(pg, c, "done")).toEqual({ applied: true, action: "deleted" });
  }, 120_000);
});

// ---------------------------------------------------------------------------------------------------------
describe("hosted-equivalent runtime role (the runtime's pipeline-table privileges, an all-row policy, EXECUTE on the two RPCs)", () => {
  test("claim and settle work; direct search-index mutation, the receipts and the operator paths are refused (42501); other stages are untouched by the guard", async () => {
    const third: Scope = { ...scope, sessionId: "s_third" };
    const dead = wake(third, 2);
    const { pg, state } = await asLogin("hosted", async (p) => {
      await seeded(wake(scope, 7), wake(other, 3))(p);
      // A dead scope letter (so the replay reaches its guarded enqueue) and an open job of another stage.
      await p.query(`INSERT INTO sessions.pipeline_dead_letters (id, stage, trace_id, org_id, workspace_id, session_id, message, first_dead_at, last_dead_at)
        VALUES ($1, 'search-index', $2::uuid, $3, $4, $5, $6::jsonb, now(), now())`,
        [searchLetterId(dead), dead.ctx.traceId, third.orgId, third.workspaceId, third.sessionId, JSON.stringify(dead)]);
      await p.query("SELECT public.fx_other_stage(NULL, 'definer-open')");
    });
    // Fixture soundness: the role really holds the runtime's DML on the queue (so a 42501 below is the stage's guard).
    expect((await pg.query<{ ok: boolean }>(
      "SELECT has_table_privilege('sessions.pipeline_jobs', 'INSERT, UPDATE, DELETE') AND NOT has_table_privilege('sessions.pipeline_jobs', 'TRUNCATE') AS ok")).rows)
      .toEqual([{ ok: true }]);
    const c = await claimOne(pg);
    const before = await state();
    const denied = {
      insertJob: await codeOf(pg.query(`INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, message)
        VALUES ('search-index', 'x', gen_random_uuid(), 'o', 'w', '{"v":1}'::jsonb)`)),
      updateJob: await codeOf(pg.query("UPDATE sessions.pipeline_jobs SET message = message || '{\"forged\":true}'::jsonb WHERE stage = 'search-index'")),
      moveIntoStage: await codeOf(pg.query("UPDATE sessions.pipeline_jobs SET stage = 'search-index' WHERE stage <> 'search-index'")),
      deleteJob: await codeOf(pg.query("DELETE FROM sessions.pipeline_jobs WHERE stage = 'search-index'")),
      truncateJobs: await codeOf(pg.query("TRUNCATE sessions.pipeline_jobs")),
      selectReceipt: await codeOf(pg.query("SELECT * FROM sessions.search_job_claims")),
      updateReceipt: await codeOf(pg.query("UPDATE sessions.search_job_claims SET claimed_epoch = 99")),
      replay: await codeOf(() => replaySearchDeadLetter(pg, searchLetterId(dead))),
      catchUp: await codeOf(() => catchUpTick({ sql: pg, pageSize: 10 })),
      park: await codeOf(() => parkSearchJobs(pg, { operator: "definer-test" })),
      archive: await codeOf(() => archiveSearchStage(pg)),
      enqueue: await codeOf(() => enqueueSearchWake(pg, wake(scope, 9))),
    };
    expect(denied).toEqual(Object.fromEntries(Object.keys(denied).map((k) => [k, "42501"])));
    // Nothing moved: the letter is still dead (the replay rolled back whole), the other stage's job is untouched.
    expect(await state()).toEqual(before);
    expect(before.letters.map((l) => l.state)).toEqual(["dead"]);
    expect(before.jobs.filter((j) => j.stage === "session-indexed")).toHaveLength(1);
    expect(await settleSearchJob(pg, c, "done")).toEqual({ applied: true, action: "deleted" });
    const [d] = await claimSearchJobs(pg, 1);
    expect(await settleSearchJob(pg, d!, { retry: 0 })).toEqual({ applied: true, action: "retried" });
    // Control: the guard is stage-scoped: the runtime's ordinary queue DML on another stage still works.
    expect(await codeOf(pg.query(`INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, message)
      VALUES ('session-indexed', 'definer-control', gen_random_uuid(), 'o', 'w', '{"v":1}'::jsonb)`))).toBe("ok");
  }, 120_000);
});

// ---------------------------------------------------------------------------------------------------------
describe("ACL receipt (catalog, as the owner)", () => {
  test("claim and settle: owner-owned SECURITY DEFINER, search_path pinned, no PUBLIC; the login executes exactly these two; the receipt table is owner-only", async () => {
    const { pg } = await clonedReplayDatabase();
    cleanup.push(() => pg.close());
    await pg.exec(`CREATE ROLE ${LOGIN} LOGIN NOSUPERUSER NOINHERIT; GRANT USAGE ON SCHEMA sessions TO ${LOGIN};
      GRANT EXECUTE ON FUNCTION ${CLAIM}, ${SETTLE} TO ${LOGIN};`);
    const owner = (await pg.query<{ o: string }>("SELECT pg_get_userbyid(relowner) AS o FROM pg_class WHERE oid = 'sessions.pipeline_jobs'::regclass")).rows[0]!.o;
    const fns = (await pg.query<{ fn: string; owner: string; secdef: boolean; config: string[] | null; public_exec: boolean; grantees: string[] }>(
      `SELECT p.oid::regprocedure::text AS fn, pg_get_userbyid(p.proowner) AS owner, p.prosecdef AS secdef, p.proconfig AS config,
              EXISTS (SELECT 1 FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE') AS public_exec,
              coalesce((SELECT array_agg(DISTINCT pg_get_userbyid(a.grantee) ORDER BY pg_get_userbyid(a.grantee)) FROM aclexplode(p.proacl) a
                         WHERE a.privilege_type = 'EXECUTE' AND a.grantee <> 0 AND a.grantee <> p.proowner), '{}') AS grantees
         FROM pg_proc p WHERE p.oid IN ($1::regprocedure, $2::regprocedure) ORDER BY 1`, [CLAIM, SETTLE])).rows;
    expect(fns).toEqual([
      { fn: CLAIM, owner, secdef: true, config: ["search_path=pg_catalog, pg_temp"], public_exec: false, grantees: [LOGIN, "service_role"] },
      { fn: SETTLE, owner, secdef: true, config: ["search_path=pg_catalog, pg_temp"], public_exec: false, grantees: [LOGIN, "service_role"] },
    ]);
    // Of every sessions function, the login (no membership) can execute exactly claim and settle.
    const executable = (await pg.query<{ fn: string }>(
      `SELECT p.oid::regprocedure::text AS fn FROM pg_proc p WHERE p.pronamespace = 'sessions'::regnamespace
          AND has_function_privilege($1, p.oid, 'EXECUTE') ORDER BY 1`, [LOGIN])).rows.map((r) => r.fn);
    expect(executable).toEqual([CLAIM, SETTLE]);
    // The stage's other functions: no PUBLIC, never the runtime; the archive is service_role's only.
    const stage = (await pg.query<{ fn: string; grantees: string[]; public_exec: boolean; secdef: boolean; config: string[] | null }>(
      `SELECT p.oid::regprocedure::text AS fn, p.prosecdef AS secdef, p.proconfig AS config,
              EXISTS (SELECT 1 FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE') AS public_exec,
              coalesce((SELECT array_agg(DISTINCT pg_get_userbyid(a.grantee) ORDER BY pg_get_userbyid(a.grantee)) FROM aclexplode(p.proacl) a
                         WHERE a.privilege_type = 'EXECUTE' AND a.grantee <> 0 AND a.grantee <> p.proowner), '{}') AS grantees
         FROM pg_proc p WHERE p.oid IN ('sessions.archive_search_stage_v1()'::regprocedure, 'sessions.search_letter_id_v1(text,text,text)'::regprocedure,
                                        'sessions.search_job_claim_attempts_v1()'::regprocedure, 'sessions.enqueue_managed_search_wake_v1(jsonb,uuid)'::regprocedure,
                                        'sessions.managed_search_wake_v1(jsonb,uuid)'::regprocedure,
                                        'sessions.resume_search_contention_v1(bigint,text)'::regprocedure) ORDER BY 1`)).rows;
    expect(stage.map((f) => ({ fn: f.fn, grantees: f.grantees, public_exec: f.public_exec }))).toEqual([
      { fn: "sessions.archive_search_stage_v1()", grantees: ["service_role"], public_exec: false },
      { fn: "sessions.enqueue_managed_search_wake_v1(jsonb,uuid)", grantees: [], public_exec: false },
      { fn: "sessions.managed_search_wake_v1(jsonb,uuid)", grantees: [], public_exec: false },
      { fn: "sessions.resume_search_contention_v1(bigint,text)", grantees: ["service_role"], public_exec: false },
      { fn: "sessions.search_job_claim_attempts_v1()", grantees: [], public_exec: false },
      { fn: "sessions.search_letter_id_v1(text,text,text)", grantees: [], public_exec: false },
    ]);
    expect(stage.every((f) => JSON.stringify(f.config) === JSON.stringify(["search_path=pg_catalog, pg_temp"]))).toBe(true);
    for (const table of ["sessions.search_job_contention", "sessions.search_probe_budget"]) {
      const t = (await pg.query<{ owner: string; acl: string[] | null; rls: boolean }>(
        "SELECT pg_get_userbyid(relowner) AS owner, relacl::text[] AS acl, relrowsecurity AS rls FROM pg_class WHERE oid = $1::regclass", [table])).rows[0]!;
      expect({ table, owner: t.owner, rls: t.rls, foreign: (t.acl ?? []).filter((e) => !e.startsWith(`${owner}=`)) }).toEqual({ table, owner, rls: true, foreign: [] });
    }
    const receipts = (await pg.query<{ owner: string; acl: string[] | null; rls: boolean }>(
      "SELECT pg_get_userbyid(relowner) AS owner, relacl::text[] AS acl, relrowsecurity AS rls FROM pg_class WHERE oid = 'sessions.search_job_claims'::regclass")).rows[0]!;
    expect(receipts.owner).toBe(owner);
    expect(receipts.rls).toBe(true);
    // Only the owner's own entry, if any: no PUBLIC, no service_role, no runtime grant.
    expect((receipts.acl ?? []).filter((e) => !e.startsWith(`${owner}=`))).toEqual([]);
    for (const role of [LOGIN, "service_role", "anon", "authenticated"])
      expect((await pg.query<{ any: boolean }>(
        "SELECT has_table_privilege($1, 'sessions.search_job_claims', 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') AS any", [role])).rows)
        .toEqual([{ any: false }]);
  }, 120_000);
});

// ---------------------------------------------------------------------------------------------------------
describe("the SQL letter id is dlq.ts deadLetterId, byte for byte", () => {
  test("parity over reserved, unreserved, percent, multi-byte and astral characters", async () => {
    const { pg } = await clonedReplayDatabase();
    cleanup.push(() => pg.close());
    const samples = ["org_plain", "org:r8 enc", "ws r8:x", "A-Z_a.z!~*'()", "%2F/?#[]@$&+,;=", "\"<>\\^`{|}", "ü", "日本語", "😀x", "é", "tab\tnl\n", ""];
    for (const org of samples) for (const ws of ["ws", samples[(samples.indexOf(org) + 3) % samples.length]!]) {
      const workKey = searchWorkKey({ orgId: org, workspaceId: ws, harness: "claude", sessionId: "s:1 ü" });
      const sql = (await pg.query<{ id: string }>("SELECT sessions.search_letter_id_v1($1, $2, $3) AS id", [org, ws, workKey])).rows[0]!.id;
      expect(sql).toBe(deadLetterId("search-index", { orgId: org, workspaceId: ws }, workKey));
    }
  }, 120_000);
});

// ---------------------------------------------------------------------------------------------------------
describe("no legacy consumer role path", () => {
  test("the retired role name is absent from the migration, the rollback and all non-test production code", () => {
    const root = join(import.meta.dir, "../../..");
    const NAME = ["bulls96", "search", "wake", "consumer"].join("_");
    const listed = Bun.spawnSync(["git", "ls-files", "src", "packages", "supabase", "scripts"], { cwd: root });
    expect(listed.exitCode).toBe(0);
    const files = new TextDecoder().decode(listed.stdout).split("\n").filter(Boolean)
      .filter((f) => !/(^|\/)(__tests__|tests?)\//.test(f));
    // Fixture soundness: the scan covers the slot's migration and rollback and the consumer's code.
    for (const must of ["supabase/migrations/20261105062300_search_index_stage.sql", "supabase/rollbacks/20261105062300_search_index_stage.down.sql",
      "src/session-pipeline/search-jobs.ts", "src/session-pipeline/lambda.ts"]) expect(files).toContain(must);
    const hits = files.filter((f) => { try { return readFileSync(join(root, f), "utf8").includes(NAME); } catch { return false; } });
    expect(hits).toEqual([]);
  });
});

// ---------------------------------------------------------------------------------------------------------
describe("bounded busy (scope lock contention) and the expired-lease reclaim cap, as the unprivileged login", () => {
  const eventCodes = (s: State) => s.events.map((e) => e.code);

  test("22 busy settlements never park and never spend the budget: attempts stay at the pre-claim value", async () => {
    const { pg, state, expire } = await asLogin("bare", seeded(wake(scope, 7)));
    const leases = new Set<string>();
    for (let n = 1; n <= 22; n++) {
      await expire();
      const c = await claimOne(pg);
      expect(c.attempts).toBe(1);
      leases.add(c.lease);
      expect(await settleSearchJob(pg, c, "busy")).toMatchObject({ applied: true, action: "busy", escalated: false, held: false, contention_count: n, delay_seconds: 30 });
      const s = await state();
      expect(s.jobs.map((j) => ({ lease: j.lease, attempts: j.attempts, visible: j.visible }))).toEqual([{ lease: null, attempts: 0, visible: false }]);
      expect(s.jobs[0]!.in).toBeGreaterThanOrEqual(29);
      expect(s.jobs[0]!.in).toBeLessThanOrEqual(30);
      expect(s.receipts).toEqual([]);
    }
    const s = await state();
    expect(s.letters).toEqual([]);
    expect(s.contention.map((k) => ({ epoch: k.epoch, count: k.count, held: k.held }))).toEqual([{ epoch: 7, count: 22, held: false }]);
    // One durable, retryable scope_busy event per lease (the key carries the lease id).
    expect(s.events).toHaveLength(22);
    expect(s.events.every((e) => e.status === "retrying" && e.code === "scope_busy")).toBe(true);
    expect(new Set(s.events.map((e) => e.key)).size).toBe(22);
    expect(leases.size).toBe(22);
    for (const l of leases) expect(s.events.some((e) => e.key.includes(l))).toBe(true);
  }, 120_000);

  test("a busy cycle, then a real retry: it starts at attempt 1 (30 s), and the contention episode ends", async () => {
    const { pg, state, expire } = await asLogin("bare", seeded(wake(scope, 7)));
    for (let n = 0; n < 3; n++) { await expire(); expect(await settleSearchJob(pg, await claimOne(pg), "busy")).toMatchObject({ action: "busy" }); }
    expect((await state()).contention).toHaveLength(1);
    await expire();
    const c = await claimOne(pg);
    expect(c.attempts).toBe(1);
    expect(await settleSearchJob(pg, c, { retry: 0 })).toEqual({ applied: true, action: "retried" });
    const s = await state();
    expect(s.jobs.map((j) => ({ attempts: j.attempts, in: j.in }))).toEqual([{ attempts: 1, in: 30 }]);
    expect(s.contention).toEqual([]);
  }, 120_000);

  test("busy with N+1 merged during the lease: an immediate reopen at attempts 0, contention reset", async () => {
    const w8 = wake(scope, 8);
    const { pg, state, expire, wakeNow } = await asLogin("bare", seeded(wake(scope, 7)));
    expect(await settleSearchJob(pg, await claimOne(pg), "busy")).toMatchObject({ action: "busy" });
    expect((await state()).contention).toHaveLength(1);
    await expire();
    const c = await claimOne(pg);
    await wakeNow(w8);
    expect(await settleSearchJob(pg, c, "busy")).toEqual({ applied: true, action: "busy_reopened" });
    const s = await state();
    expect(s.jobs.map((j) => ({ lease: j.lease, attempts: j.attempts, visible: j.visible, message: j.message }))).toEqual([{ lease: null, attempts: 0, visible: true, message: w8 }]);
    expect(s.contention).toEqual([]);
    expect(s.receipts).toEqual([]);
    expect(s.letters).toEqual([]);
  }, 120_000);

  test("fairness: the hot scope settled busy is hidden for 30 s; the next claim takes and completes another visible scope", async () => {
    const hot = wake(scope, 7), cold = wake(other, 3);
    const { pg, state } = await asLogin("bare", seeded(hot, cold));
    const first = await claimOne(pg);
    expect(first.claimedMessage).toEqual(hot);
    expect(await settleSearchJob(pg, first, "busy")).toMatchObject({ action: "busy", delay_seconds: 30 });
    const next = await claimOne(pg);
    expect(next.claimedMessage).toEqual(cold);
    expect(await settleSearchJob(pg, next, "done")).toEqual({ applied: true, action: "deleted" });
    expect(await claimSearchJobs(pg, 10)).toEqual([]);
    const s = await state();
    expect(s.jobs.map((j) => ({ message: j.message, visible: j.visible, in: j.in }))).toEqual([{ message: hot, visible: false, in: 30 }]);
  }, 120_000);

  test("escalation starts at 2 h and is rate-limited to one per hour", async () => {
    const { pg, state, expire, contendedSince } = await asLogin("bare", seeded(wake(scope, 7)));
    expect(await settleSearchJob(pg, await claimOne(pg), "busy")).toMatchObject({ escalated: false });
    await contendedSince(1.9);
    await expire();
    expect(await settleSearchJob(pg, await claimOne(pg), "busy")).toMatchObject({ escalated: false });
    await contendedSince(3);
    await expire();
    expect(await settleSearchJob(pg, await claimOne(pg), "busy")).toMatchObject({ escalated: true, held: false, delay_seconds: 30 });
    await expire();
    expect(await settleSearchJob(pg, await claimOne(pg), "busy")).toMatchObject({ escalated: false });
    expect(eventCodes(await state()).filter((c) => c === "scope_contention_escalated")).toHaveLength(1);
    await contendedSince(3, 1.01);
    await expire();
    expect(await settleSearchJob(pg, await claimOne(pg), "busy")).toMatchObject({ escalated: true });
    expect(eventCodes(await state()).filter((c) => c === "scope_contention_escalated")).toHaveLength(2);
  }, 120_000);

  test("the 24 h cap: the job is held (not claimable on the 30 s cadence); an hourly probe is admitted when due; never parked", async () => {
    const { pg, state, expire, contendedSince, probeDue } = await asLogin("bare", seeded(wake(scope, 7)));
    expect(await settleSearchJob(pg, await claimOne(pg), "busy")).toMatchObject({ action: "busy", held: false });
    await contendedSince(25);
    await expire();
    const r = await settleSearchJob(pg, await claimOne(pg), "busy");
    expect(r).toMatchObject({ applied: true, action: "busy", held: true, held_now: true, probe: false, escalated: true, delay_seconds: 30 });
    let s = await state();
    expect(s.contention.map((k) => ({ held: k.held, probe_in: k.probe_in }))).toEqual([{ held: true, probe_in: 3600 }]);
    expect(eventCodes(s).filter((c) => c !== "scope_busy").sort()).toEqual(["scope_contention_escalated", "scope_contention_held"]);
    // The 30 s backoff passes (and passes again): the held job is not claimable.
    for (let n = 0; n < 3; n++) { await expire(); expect(await claimSearchJobs(pg, 10)).toEqual([]); }
    // An hour on, the probe is due: ONE claim is admitted; busy again moves the next probe an hour on.
    await probeDue();
    const p = await claimOne(pg);
    expect(await claimSearchJobs(pg, 10)).toEqual([]);
    expect(await settleSearchJob(pg, p, "busy")).toMatchObject({ action: "busy", held: true, held_now: false, probe: true });
    s = await state();
    expect(s.contention.map((k) => k.probe_in)).toEqual([3600]);
    expect(eventCodes(s).filter((c) => c === "scope_contention_probe")).toHaveLength(1);
    expect(eventCodes(s).filter((c) => c === "scope_contention_held")).toHaveLength(1);
    expect(s.events.find((e) => e.code === "scope_contention_probe")!.key).toContain(p.lease);
    expect(s.letters).toEqual([]);
    await expire();
    expect(await claimSearchJobs(pg, 10)).toEqual([]);
  }, 120_000);

  test("a static scope held past 24 h recovers by itself: hourly probes until contention clears, then it completes (no replay)", async () => {
    const { pg, state, expire, contendedSince, probeDue } = await asLogin("bare", seeded(wake(scope, 7)));
    expect(await settleSearchJob(pg, await claimOne(pg), "busy")).toMatchObject({ held: false });
    await contendedSince(30);
    await expire();
    expect(await settleSearchJob(pg, await claimOne(pg), "busy")).toMatchObject({ held_now: true });
    for (let hour = 1; hour <= 3; hour++) {
      await expire();
      expect(await claimSearchJobs(pg, 10)).toEqual([]);
      await probeDue();
      expect(await settleSearchJob(pg, await claimOne(pg), "busy")).toMatchObject({ probe: true });
    }
    // The lock clears; the next hourly probe (an hour on: the 30 s backoff long past) completes the pass.
    await expire();
    await probeDue();
    expect(await settleSearchJob(pg, await claimOne(pg), "done")).toEqual({ applied: true, action: "deleted" });
    const s = await state();
    expect({ jobs: s.jobs, contention: s.contention, receipts: s.receipts, letters: s.letters }).toEqual({ jobs: [], contention: [], receipts: [], letters: [] });
    expect(s.events.filter((e) => e.status === "replayed")).toEqual([]);
  }, 120_000);

  test("a newer epoch releases a hold: claimable now at its own epoch, contention reset", async () => {
    const w8 = wake(scope, 8);
    const { pg, state, holdAll, wakeNow } = await asLogin("bare", seeded(wake(scope, 7)));
    await holdAll(false);
    expect(await claimSearchJobs(pg, 10)).toEqual([]);
    await wakeNow(w8);
    const c = await claimOne(pg);
    expect({ epoch: c.claimedEpoch, attempts: c.attempts, message: c.claimedMessage }).toEqual({ epoch: 8, attempts: 1, message: w8 });
    expect((await state()).contention).toEqual([]);
    expect(await settleSearchJob(pg, c, "done")).toEqual({ applied: true, action: "deleted" });
  }, 120_000);

  test("the fleet probe budget: with 10 held-due scopes and C = 6, at most 6 probes are admitted; a refill admits more", async () => {
    const held = Array.from({ length: 10 }, (_, n) => wake({ ...scope, sessionId: `s_held_${n}` }, 1));
    const { pg, state, holdAll, budget } = await asLogin("bare", async (p) => { await seeded(...held)(p); await p.query("SELECT public.fx_hold_all(true)"); });
    void holdAll;
    await budget(6, 0);
    const admitted: string[] = [];
    for (let n = 0; n < 10; n++) {
      const got = await claimSearchJobs(pg, 1);
      // At most one probe per call, even when more slots are asked for.
      if (got.length) { admitted.push(got[0]!.id); expect(await settleSearchJob(pg, got[0]!, "busy")).toMatchObject({ probe: true }); }
    }
    expect(admitted).toHaveLength(6);
    expect(await claimSearchJobs(pg, 10)).toEqual([]);
    // 30 s of refill at R = 6/min: three more.
    await budget(0, 30);
    const more = [];
    for (let n = 0; n < 5; n++) { const got = await claimSearchJobs(pg, 10); expect(got.length).toBeLessThanOrEqual(1); more.push(...got); }
    expect(more.length).toBe(3);
    // Oldest-due first: the probes taken are distinct scopes, none twice.
    expect(new Set([...admitted, ...more.map((m) => m.id)]).size).toBe(9);
    expect((await state()).letters).toEqual([]);
  }, 120_000);

  test("sequential mixed load: ordinary work is always ready; 22 held-due scopes get probes only from the bucket and recover", async () => {
    let next = 0;
    const ordinary = () => wake({ ...scope, sessionId: `s_ord_${next++}` }, 1);
    const held = Array.from({ length: 22 }, (_, n) => wake({ ...scope, sessionId: `s_hot_${String(n).padStart(2, "0")}` }, 1));
    const heldKeys = new Set(held.map((w) => w.workKey));
    const { pg, state, budget, wakeNow } = await asLogin("bare", async (p) => {
      await seeded(...held)(p);
      await p.query("SELECT public.fx_hold_all(true)");
    });
    for (let n = 0; n < 4; n++) await wakeNow(ordinary());
    await budget(6, 0);
    let probes = 0, done = 0;
    const started = Date.now();
    for (let n = 0; n < 60; n++) {
      await wakeNow(ordinary());
      const c = await claimOne(pg);
      if (heldKeys.has(c.claimedMessage.workKey)) {
        probes += 1;
        // The probe finds the lock free: the held scope recovers.
        expect(await settleSearchJob(pg, c, "done")).toEqual({ applied: true, action: "deleted" });
      } else {
        done += 1;
        expect(await settleSearchJob(pg, c, "done")).toEqual({ applied: true, action: "deleted" });
      }
    }
    const minutes = (Date.now() - started) / 60_000;
    expect(probes).toBeGreaterThanOrEqual(1);
    expect(probes).toBeLessThanOrEqual(6 + Math.ceil(6 * minutes));
    expect(done).toBe(60 - probes);
    const s = await state();
    // Recovered held scopes are gone with their contention; the rest stay held, unparked.
    expect(s.contention).toHaveLength(22 - probes);
    expect(s.letters).toEqual([]);
    // The ordinary queue never emptied.
    expect(s.jobs.filter((j) => !heldKeys.has(j.message.workKey)).length).toBeGreaterThanOrEqual(4);
    console.log(JSON.stringify({ sequentialMixedLoad: { claims: 60, ordinaryCompleted: done, probesAdmitted: probes, heldRemaining: 22 - probes } }));
  }, 120_000);

  test("low cadence, limit 1: calls spaced wider than the refill interval, both queues ready: probes take at most every other call", async () => {
    let next = 0;
    const ordinary = () => wake({ ...scope, sessionId: `s_low_${next++}` }, 1);
    const held = Array.from({ length: 12 }, (_, n) => wake({ ...scope, sessionId: `s_lowheld_${String(n).padStart(2, "0")}` }, 1));
    const heldKeys = new Set(held.map((w) => w.workKey));
    const { pg, state, budget, wakeNow } = await asLogin("bare", async (p) => { await seeded(...held)(p); await p.query("SELECT public.fx_hold_all(true)"); });
    for (let n = 0; n < 3; n++) await wakeNow(ordinary());
    const kinds: Array<"probe" | "ordinary"> = [];
    for (let n = 0; n < 24; n++) {
      // Each call arrives after a full refill interval: the bucket is never the limit.
      await budget(6, 60);
      await wakeNow(ordinary());
      const c = await claimOne(pg);
      kinds.push(heldKeys.has(c.claimedMessage.workKey) ? "probe" : "ordinary");
      expect(await settleSearchJob(pg, c, "done")).toEqual({ applied: true, action: "deleted" });
    }
    const probes = kinds.filter((k) => k === "probe").length;
    // Never two probes in a row while ordinary work is ready; so ordinary waits at most one call.
    expect(kinds.some((k, i) => k === "probe" && kinds[i - 1] === "probe")).toBe(false);
    expect(probes).toBe(12);
    expect(kinds.length - probes).toBeGreaterThanOrEqual(kinds.length / 2);
    expect((await state()).contention).toEqual([]);
    console.log(JSON.stringify({ lowCadence: { calls: kinds.length, ordinary: kinds.length - probes, probes, sequence: kinds.map((k) => k[0]).join("") } }));
  }, 120_000);

  test("expired-lease reclaims: 5 consecutive expiries do not park, the 6th parks atomically (persisted across a fresh client)", async () => {
    const w = wake(scope, 7);
    let last: ClaimedSearchJob | undefined;
    // Phase one as the owner, then a FRESH client on the same data: the count is persisted, not process state.
    const { pg, state, expire } = await asLogin("bare", async (p) => {
      await seeded(w)(p);
      await claimSearchJobs(p, 1);
      for (let n = 1; n <= 3; n++) { await p.query("SELECT public.fx_expire()"); await claimSearchJobs(p, 1); }
    }, { restart: true });
    expect((await state()).receipts.map((r) => r.reclaims)).toEqual([3]);
    for (let n = 4; n <= 5; n++) { await expire(); last = await claimOne(pg); expect((await state()).receipts.map((r) => r.reclaims)).toEqual([n]); }
    // The 6th: an atomic park. A failure midway changes nothing.
    await expire();
    await pg.exec("SET fx.fail = 'on'");
    const before = await state();
    expect(await codeOf(claimSearchJobs(pg, 10))).toBe("P0001");
    expect(await state()).toEqual(before);
    await pg.exec("SET fx.fail = 'off'");
    expect(await claimSearchJobs(pg, 10)).toEqual([]);
    const s = await state();
    expect({ jobs: s.jobs, receipts: s.receipts, contention: s.contention }).toEqual({ jobs: [], receipts: [], contention: [] });
    expect(s.letters.map((l) => ({ id: l.id, message: l.message, trace: l.provenance.trace }))).toEqual([{ id: searchLetterId(w), message: last!.claimedMessage, trace: w.ctx.traceId }]);
    expect(s.events.filter((e) => e.status === "dead_lettered").map((e) => ({ code: e.code, expired: e.attrs.expired_leases }))).toEqual([{ code: "expired_lease_reclaims", expired: 6 }]);
  }, 120_000);

  test("the reclaim count resets on a settled pass (busy) and on a newer epoch", async () => {
    const { pg, state, expire, wakeNow } = await asLogin("bare", seeded(wake(scope, 7)));
    await claimOne(pg);
    for (let n = 1; n <= 4; n++) { await expire(); await claimOne(pg); }
    expect((await state()).receipts.map((r) => r.reclaims)).toEqual([4]);
    // A newer epoch merged during the lease: the next expiry's reclaim counts from 0.
    await wakeNow(wake(scope, 8));
    await expire();
    const c = await claimOne(pg);
    expect((await state()).receipts.map((r) => r.reclaims)).toEqual([0]);
    for (let n = 1; n <= 3; n++) { await expire(); await claimOne(pg); }
    expect((await state()).receipts.map((r) => r.reclaims)).toEqual([3]);
    void c;
    // Any settled pass (busy here) deletes the receipt: the count is gone.
    const [cur] = (await state()).receipts;
    expect(await settleSearchJob(pg, { id: cur!.id, lease: cur!.lease }, "busy")).toMatchObject({ action: "busy" });
    await expire();
    await claimOne(pg);
    expect((await state()).receipts.map((r) => r.reclaims)).toEqual([0]);
  }, 120_000);
});

// ---------------------------------------------------------------------------------------------------------
describe("resume_search_contention_v1 (operator, service_role only)", () => {
  test("service_role resumes a held job with an audit event (operator text, session_user, current_user, clock); refused while a lease is live", async () => {
    const { pg } = await clonedReplayDatabase();
    cleanup.push(() => pg.close());
    await pg.exec(FIXTURES);
    await enqueueSearchWake(pg, wake(scope, 7));
    await pg.query("SELECT public.fx_hold_all(true)");
    const id = (await pg.query<{ id: string }>("SELECT id::text FROM sessions.pipeline_jobs WHERE stage = 'search-index'")).rows[0]!.id;
    const resume = (operator: string) => pg.query<{ r: { applied: boolean; reason?: string } }>("SELECT sessions.resume_search_contention_v1($1::bigint, $2) AS r", [id, operator]);
    // A live lease (a probe in flight): refused, nothing changed.
    const [probe] = await claimSearchJobs(pg, 1);
    expect(probe).toBeDefined();
    await pg.exec("SET ROLE service_role");
    expect((await resume("ops-oncall")).rows[0]!.r).toEqual({ applied: false, reason: "lease_live" });
    expect(await codeOf(resume(""))).toBe("22023");
    await pg.exec("RESET ROLE");
    expect(await settleSearchJob(pg, probe!, "busy")).toMatchObject({ probe: true });
    await pg.exec("SET ROLE service_role");
    expect((await resume("ops-oncall")).rows[0]!.r).toEqual({ applied: true, epoch: 7 });
    expect((await resume("ops-oncall")).rows[0]!.r).toEqual({ applied: false, reason: "not_held" });
    await pg.exec("RESET ROLE");
    const audit = (await pg.query<{ attrs: Record<string, unknown>; code: string }>(
      "SELECT attrs, error->>'code' AS code FROM sessions.pipeline_events WHERE error->>'code' = 'scope_contention_resumed'")).rows;
    expect(audit).toHaveLength(1);
    expect(audit[0]!.attrs).toMatchObject({ operator: "ops-oncall", session_user: "postgres", epoch: 7 });
    expect(typeof audit[0]!.attrs.current_user).toBe("string");
    expect(Number.isFinite(Date.parse(String(audit[0]!.attrs.resumed_at)))).toBe(true);
    expect((await pg.query("SELECT held_at, next_probe_at FROM sessions.search_job_contention")).rows).toEqual([{ held_at: null, next_probe_at: null }]);
    // Released: claimable on the ordinary cadence again (once its 30 s backoff passes).
    await pg.query("SELECT public.fx_expire()");
    expect(await claimSearchJobs(pg, 1)).toHaveLength(1);
  }, 120_000);

  test("the unprivileged LOGIN cannot resume (42501)", async () => {
    const { pg } = await asLogin("bare", async (p) => { await enqueueSearchWake(p, wake(scope, 7)); await p.query("SELECT public.fx_hold_all(true)"); });
    expect(await codeOf(pg.query("SELECT sessions.resume_search_contention_v1(1, 'login')"))).toBe("42501");
    expect(await codeOf(pg.query("SELECT * FROM sessions.search_probe_budget"))).toBe("42501");
    expect(await codeOf(pg.query("SELECT * FROM sessions.search_job_contention"))).toBe("42501");
  }, 120_000);
});
