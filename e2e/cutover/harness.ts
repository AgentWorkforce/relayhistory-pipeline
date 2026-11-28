/**
 * Synthetic qualification of decision 0005's per-org cutover, end to end on one synthetic org, in the corrected
 * order (layered-sessions-pipeline; FULL after install per the 062340 contract, install < FULL start <= seal):
 *
 *   seed            buffered backlog through the real SQL admission, erasures before the hold
 *   rearm.list      the org's re-arm markers and a dry sweeper pass (rearm-sweeper.ts)
 *   hold.install    062320 _install (adapter: wave-tooling hold-install-execute)
 *   full            backfill-execute over the org (upload-fence-backfill.ts), started after the install
 *   seal.bind       062320 _seal + 062340 _bind (adapter: hold-seal-execute + _bind)
 *   catchup         backfill-catchup-execute, revisions since FULL start minus the 15 min margin
 *   reshard.verify  revision-reshard verify over the org's admission prefix
 *   marker          _marker_attempt + the per-org fence-authority marker (adapter: marker-execute)
 *   marker.verify   the hold is marker_attempted at the epoch, the marker reads back, SQL admission and publication
 *                   are still refused, and S3 admission of the org is still closed
 *   drain           export-execute, then drain-execute, within 3 h of the catch-up's created_at
 *   release         the erasure set replayed, then _release bridge_complete (adapters: the generation proof, the
 *                   062360 manifest, bridge-execute)
 *   admission       resumed upload admission (the uploads refused during the hold, retried, plus new ones)
 *   publication     resumed search publication (the 062320 publication gate opens)
 *   reads           old (drained backlog) and new (post-release) sessions readable from the canonical layout
 *
 * Live uploads arrive at every step boundary; erasures (operator user erase, the user's own session deletion,
 * session erasure fences) land before and during the hold. Each step is timed; the admission downtime is
 * install -> resumed admission.
 *
 * FAIL CLOSED. A step whose head is pending stops the run there (BLOCKED, with the head's code), every later step is
 * SKIP, and the verdict is NOT_QUALIFIED. QUALIFIED needs every step PASS and every adapter `landed`. A
 * --simulate-pending run replaces the pending heads with their S3 effect, for timings only; it is never QUALIFIED.
 */
import { createHash, randomUUID } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { objectKey, readAll, isObjectStoreError, type ObjectStore } from "../../src/shared/vendor.ts";
import { liveMachineId } from "../../src/session-ingest/writer.ts";
import { readManifestRecords } from "../../src/session-read-api/canonical-projection.ts";
import { backfillUploadFences, type BackfillSummary } from "../../src/session-ingest/upload-fence-backfill.ts";
import { exportAcceptedStages } from "../../src/session-ingest/accepted-stage-export.ts";
import { drainBufferedBatches } from "../../src/session-ingest/buffered-drain.ts";
import { reshardRevisions } from "../../src/session-ingest/revision-reshard.ts";
import { sweepRearmMarkers, rearmScopes } from "../../src/session-ingest/rearm-sweeper.ts";
import { orgFenceAuthorityKey, s3LedgerKeys, validOrgFenceAuthority } from "../../src/session-ingest/ledger-s3.ts";
import { HeadPending, allLanded, defaultHeads, integrationSummary, type CutoverContext, type CutoverHeads, type ReplayItem, type ReplayReceipt } from "./heads.ts";
import type { Backend } from "./backend.ts";
import { KEY_ROOT, MIN_USERS, ORG, SIZE_CI, SOURCE, WORKSPACE, originOfSession, originsOf, openFixture, sessionOf, tenantOf, text, userId, type FixtureSize, type Fx } from "./fixture.ts";

export const CATCHUP_MARGIN_MS = 15 * 60_000;
/** QUALIFIED needs this backend (layered: PG17 and MinIO), never PGlite. */
export const QUALIFICATION_BACKEND = "pg17+minio";
/** BACKLOG_OPS_FRESH_BACKFILL_MS: the drain binds a catch-up under 3 h old. */
export const DRAIN_WINDOW_MS = 3 * 3600_000;

export type StepStatus = "PASS" | "FAIL" | "BLOCKED" | "SKIP";
export interface StepReport { id: string; status: StepStatus; ms: number; startedMs: number | null; observed: string[]; code?: string; integration?: string }
export interface UploadEvent {
  step: string; path: "sql" | "s3"; session: string; outcome: string; queued: boolean;
  /** Client clock around the request: started before it was sent, ended when its answer arrived. */
  startedMs: number; endedMs: number;
  /** S3 refusals only: the durable writes found for this upload afterwards (batch record, owner stub, canonical). */
  effects?: string[];
}
/** The upload's synthetic content, kept so a read can compare exactly (never reported). */
interface UploadSpec { u: number; origin: string; batchId: string; session: string; records: number; revision: number; payloadBytes: number; raw: unknown }
export interface CutoverReport {
  v: 1;
  kind: "synthetic-cutover-qualification";
  verdict: "QUALIFIED" | "NOT_QUALIFIED";
  reasons: string[];
  simulated: boolean;
  backend: string;
  scenario: "injected" | "empty";
  size: FixtureSize & { batches: number; records: number; sessions: number; staged_bytes: number; buffered_stages: number };
  steps: StepReport[];
  /**
   * The boundaries, each recorded on its own (layered): the install's and the release's COMMIT times are the
   * database's (hold installed_at / released_at); the first successful S3 admission after the install is the client's
   * request start and answer (an upload accepted then was admitted within that interval). Null when it never happened.
   */
  timeline: { install_committed_ms: number | null; first_s3_admit_started_ms: number | null; first_s3_admit_accepted_ms: number | null;
    release_committed_ms: number | null; first_admit_after_release_started_ms: number | null };
  /**
   * observed_bound_ms: install commit -> the first S3 admission's answer: the org's admission was closed for at most
   * this long (the observed downtime's bound). through_release_ms: install commit -> release commit (the stated
   * order's window; null when the release never committed, e.g. blocked).
   */
  downtime: { observed_bound_ms: number | null; through_release_ms: number | null };
  uploads: UploadEvent[];
  erasures: { before_hold: Record<string, number>; during_hold: Record<string, number>; history_since_install: Record<string, number> | null };
  integration: Record<string, { head: string; integration: string; todo: string | null }>;
  wall_ms: number;
}

/** A failed assertion: the step FAILs and the run stops (what follows would build on a wrong state). */
class StepFailure extends Error { constructor(readonly code: string, msg?: string, readonly soft = false) { super(msg ?? code); this.name = "StepFailure"; } }
/** A failed check that leaves the state usable: the step FAILs, the run goes on so later steps are still measured. */
function must(cond: unknown, code: string, msg?: string): asserts cond { if (!cond) throw new StepFailure(code, msg); }
const sha = (b: string | Uint8Array) => createHash("sha256").update(b).digest("hex");
const enc = new TextEncoder();

async function listAll(store: ObjectStore, prefix: string): Promise<string[]> {
  const keys: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix), cursor);
    keys.push(...page.items.map((i) => String(i.key)));
    cursor = page.nextCursor;
  } while (cursor && keys.length < 1_000_000);
  return keys;
}

export interface RunOptions {
  size?: FixtureSize;
  heads?: CutoverHeads;
  simulatePending?: boolean;
  /** The backend (default: PGlite + fs store). */
  backend?: () => Promise<Backend>;
  /**
   * injected (default): erasures land during the hold (a held user deletion; a sealed operator user erase and session
   * fence), so bridge_complete needs their replay. empty: none during the hold, the empty-window release.
   */
  erasures?: "injected" | "empty";
  /**
   * Test only: plant one more session erasure fence during the hold with NO replay item, to prove the release refuses
   * an erasure the replay never saw (erasure_unaccounted).
   */
  unreplayedErasure?: boolean;
  /** Progress lines (counts and step ids only). */
  log?: (line: string) => void;
}

/** The erasure plan (user indexes; MIN_USERS covers them). Users >= 6 are never erased; 6 and 7 upload live. */
const PLAN = {
  preUserErase: 0, preSelection: [1, 0] as const, preSessionFence: [2, 0] as const,
  heldSelection: [3, 0] as const, sealedUserErase: 4, sealedSessionFence: [5, 0] as const,
  live: [6, 7] as const,
};

export async function runCutoverQualification(opts: RunOptions = {}): Promise<CutoverReport> {
  const size = opts.size ?? SIZE_CI;
  if (size.users < MIN_USERS) throw new Error(`cutover fixture needs at least ${MIN_USERS} users`);
  const heads = opts.heads ?? defaultHeads({ simulatePending: opts.simulatePending });
  const log = opts.log ?? (() => {});
  const wall0 = Date.now();
  const steps: StepReport[] = [];
  const uploads: UploadEvent[] = [];
  const queued: UploadSpec[] = [];
  const before: Record<string, number> = {}, during: Record<string, number> = {};
  const replay: ReplayItem[] = [];
  let historySinceInstall: Record<string, number> | null = null;
  let installMs: number | null = null, releaseCommittedMs: number | null = null;
  let firstS3: { startedMs: number; endedMs: number } | null = null, firstAfterRelease: number | null = null;
  const specs = new Map<string, UploadSpec>();
  /** Each planted in-hold erasure's own history rows (attribution: source|op|at|xid), by replay item index. */
  const attributed = new Map<number, Set<string>>();
  let blocked = false;

  const scenario = opts.erasures ?? "injected";
  const fx: Fx = await openFixture(opts.backend ? await opts.backend() : undefined);
  const ctx: CutoverContext = { sql: { query: (t, p) => fx.db.query(t, p as never) as never }, store: fx.store, keyRoot: KEY_ROOT, org: ORG };
  const dbNowMs = async () => Number((await fx.one<{ t: string }>("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint::text AS t")).t);
  const bump = (m: Record<string, number>, k: string) => { m[k] = (m[k] ?? 0) + 1; };
  /** "ok", or the SQLSTATE an owner statement raised. */
  const sqlCode = (q: string, p: unknown[]) => fx.db.query(q, p as never).then(() => "ok", (e: { code?: unknown }) => String(e?.code ?? "error"));
  let liveSeq = 0;

  /** One upload on a path; refused ones are queued for the retry after release. */
  async function upload(stepId: string, path: "sql" | "s3", spec: Omit<UploadSpec, "raw"> & { raw?: unknown }): Promise<UploadEvent> {
    const raw = spec.raw ?? await fx.rawBatch(spec.u, spec.session, spec.origin, spec.batchId, spec.revision, spec.records, spec.payloadBytes);
    const full: UploadSpec = { ...spec, raw };
    specs.set(spec.session, full);
    const startedMs = Date.now();
    const r = await fx.attempt(() => (path === "sql" ? fx.deliverSql(spec.u, raw) : fx.deliverS3(spec.u, raw)));
    const endedMs = Date.now();
    const outcome = r.ok ? String(r.result.outcome) : r.code;
    if (!r.ok) queued.push(full);
    if (r.ok && path === "s3" && installMs !== null && firstS3 === null) firstS3 = { startedMs, endedMs };
    if (r.ok && releaseCommittedMs !== null && firstAfterRelease === null) firstAfterRelease = startedMs;
    const ev: UploadEvent = { step: stepId, path, session: spec.session, outcome, queued: !r.ok, startedMs, endedMs };
    uploads.push(ev);
    return ev;
  }
  /** One live upload per live user at this boundary, on the path a client of this org uses now. */
  async function liveUploads(stepId: string, path: "sql" | "s3"): Promise<UploadEvent[]> {
    const out: UploadEvent[] = [];
    for (const u of PLAN.live) {
      const n = liveSeq++;
      out.push(await upload(stepId, path, { u, origin: originsOf(u)[0]!, batchId: `live-b-${n}`, session: `live-${String(u).padStart(3, "0")}-${String(n).padStart(4, "0")}`, records: 3, revision: 2, payloadBytes: 128 }));
    }
    return out;
  }
  const exists = async (key: string) => {
    try { await readAll(await fx.store.get(objectKey(key))); return true; }
    catch (e) { if (isObjectStoreError(e) && e.code === "not_found") return false; throw e; }
  };
  /** The session's exact canonical prefix: org, workspace, the live machine of (origin, user), harness. */
  const machinePrefixOf = (u: number, origin: string) => `${KEY_ROOT}/${ORG}/v1/ws=${WORKSPACE}/m=${liveMachineId(WORKSPACE, origin, userId(u))}/h=${SOURCE}`;
  const esc = (x: string) => x.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  /** The one manifest key of exactly this session under its exact prefix (any start date), or null; >1 throws. */
  async function manifestOf(u: number, origin: string, session: string): Promise<string | null> {
    const pre = machinePrefixOf(u, origin);
    const re = new RegExp(`^${esc(pre)}/d=\\d{4}/\\d{2}/\\d{2}/s=${esc(session)}/manifest\\.json$`);
    const hits = (await listAll(fx.store, pre)).filter((k) => re.test(k));
    if (hits.length > 1) throw new StepFailure("manifest_ambiguous");
    return hits[0] ?? null;
  }
  /** Durable writes of one upload: its S3 batch record, its owner stub, any canonical object of its session. */
  async function s3Effects(spec: UploadSpec): Promise<string[]> {
    const keys = s3LedgerKeys(KEY_ROOT, tenantOf(spec.u));
    const out: string[] = [];
    if (await exists(keys.batch(spec.origin, spec.batchId))) out.push("batch_record");
    if (await exists(keys.owner(spec.origin, SOURCE, spec.session))) out.push("owner_stub");
    if ((await listAll(fx.store, machinePrefixOf(spec.u, spec.origin))).some((k) => k.includes(`/s=${spec.session}/`))) out.push("canonical");
    return out;
  }
  /**
   * Between install and COMMITTED bridge_complete the org's S3 admission must be closed: every S3 upload refused with a
   * code the S3-side hold names (s3HoldRefusal), and no durable write. Records each failure on the step.
   */
  async function checkS3Closed(evs: readonly UploadEvent[], fail: (code: string, msg: string) => void, obs: string[]) {
    const codes = await heads.s3HoldRefusal.run();
    for (const ev of evs) {
      if (ev.path !== "s3") continue;
      if (!ev.queued) { fail("s3_admits_before_bridge_complete", `${ev.step}: an S3 upload was accepted before bridge_complete committed`); continue; }
      ev.effects = await s3Effects(specs.get(ev.session)!);
      if (!codes.has(ev.outcome)) fail("s3_refusal_unspecified", `${ev.step}: S3 refusal ${ev.outcome} is not a named hold refusal`);
      if (ev.effects.length) fail("s3_refusal_with_effects", `${ev.step}: refused, but wrote ${ev.effects.join(",")}`);
    }
    obs.push(`S3 probes: ${evs.filter((e) => e.path === "s3").map((e) => e.outcome + (e.effects?.length ? `(+${e.effects.join("+")})` : "")).join(",")}`);
  }
  // A multiset: identical rows (same source, op, at and xid inside one erase transaction) stay distinct by occurrence.
  const historyKeys = async (sinceIso?: string) => new Set((await fx.db.query<{ k: string }>(
    `SELECT source || '|' || op || '|' || at::text || '|' || xid::text || '|' || row_number() OVER (PARTITION BY source, op, at, xid) AS k
       FROM sessions.ingest_org_erasure_history WHERE erased_org = $1${sinceIso ? " AND at >= $2::timestamptz" : ""}`,
    sinceIso ? [ORG, sinceIso] : [ORG])).rows.map((r) => r.k));
  /** Plant one in-hold erasure and attribute exactly the history rows it produced to its replay item. */
  async function planted(item: ReplayItem | null, write: () => Promise<unknown>) {
    const before = await historyKeys();
    await write();
    const rows = [...(await historyKeys())].filter((k) => !before.has(k));
    if (item) { replay.push(item); attributed.set(replay.length - 1, new Set(rows)); }
  }

  async function step(id: string, fn: (obs: string[], fail: (code: string, msg: string) => void) => Promise<void>, integration?: string): Promise<void> {
    if (blocked) { steps.push({ id, status: "SKIP", ms: 0, startedMs: null, observed: ["not run: an earlier step blocked or failed"], ...(integration ? { integration } : {}) }); return; }
    const obs: string[] = [];
    const startedMs = Date.now(), t0 = performance.now();
    log(`step ${id} ...`);
    // Soft failures: checks that leave the state usable. The step FAILs with the first code (all are observed) and
    // the run goes on, so later steps are still measured.
    const soft: { code: string; msg: string }[] = [];
    try {
      await fn(obs, (code, msg) => soft.push({ code, msg }));
      if (soft.length) {
        const codes = [...new Set(soft.map((f) => f.code))];
        steps.push({ id, status: "FAIL", ms: Math.round(performance.now() - t0), startedMs, observed: [...obs, ...soft.map((f) => `${f.code}: ${f.msg}`)], code: codes.join("+").slice(0, 120), ...(integration ? { integration } : {}) });
      } else steps.push({ id, status: "PASS", ms: Math.round(performance.now() - t0), startedMs, observed: obs, ...(integration ? { integration } : {}) });
    } catch (e) {
      const ms = Math.round(performance.now() - t0);
      if (e instanceof HeadPending) {
        blocked = true;
        steps.push({ id, status: "BLOCKED", ms, startedMs, observed: [...obs, ...soft.map((f) => `${f.code}: ${f.msg}`), `head pending: ${e.head}`], code: e.code, ...(integration ? { integration } : {}) });
      } else if (e instanceof StepFailure) {
        if (!e.soft) blocked = true;
        steps.push({ id, status: "FAIL", ms, startedMs, observed: [...obs, ...soft.map((f) => `${f.code}: ${f.msg}`), e.message], code: e.code, ...(integration ? { integration } : {}) });
      } else {
        // An unexpected error stops the run: what follows would build on an unknown state.
        blocked = true;
        const code = String((e as { code?: unknown })?.code ?? (e as Error)?.name ?? "error");
        steps.push({ id, status: "FAIL", ms, startedMs, observed: [...obs, `unexpected: ${code}`], code: `unexpected_${code}`.slice(0, 64), ...(integration ? { integration } : {}) });
      }
    }
    log(`step ${id} ${steps.at(-1)!.status} ${steps.at(-1)!.ms} ms`);
  }

  let epoch = "", fullStartedMs = 0, fullReceipt = "", catchupCreatedMs = 0, catchupReceipt = "";
  let full: BackfillSummary | null = null;
  let drainAlreadyStored = 0, drainWritten = 0;
  const sizeOut = { ...size, batches: 0, records: 0, sessions: 0, staged_bytes: 0, buffered_stages: 0 };

  try {
    // ---- seed: the buffered backlog and the pre-hold erasures ------------------------------------------------------
    await step("seed", async (obs) => {
      for (let b = 0; b < size.batchesPerSession; b++)
        for (let u = 0; u < size.users; u++)
          for (let s = 0; s < size.sessionsPerUser; s++) {
            const raw = await fx.rawBatch(u, sessionOf(u, s), originOfSession(u, s), `b-${u}-${s}-${b}`, b + 2, size.recordsPerBatch, size.payloadBytes);
            const r = await fx.deliverSql(u, raw);
            must(r.outcome === "accepted_buffered", "seed_not_buffered", `seed batch outcome ${String(r.outcome)}`);
            sizeOut.batches++; sizeOut.records += size.recordsPerBatch;
          }
      sizeOut.sessions = size.users * size.sessionsPerUser;
      const st = await fx.one<{ n: number; bytes: string }>("SELECT count(*)::int n, coalesce(sum(stage_bytes),0)::bigint::text bytes FROM sessions.ingest_stage_intents WHERE org_id=$1", [ORG]);
      sizeOut.buffered_stages = st.n; sizeOut.staged_bytes = Number(st.bytes);
      await fx.proveOwnerParity(size.users);
      // Erasures before the hold: covered by FULL (it copies SQL deletions and resets into the fences), not by the bridge.
      await fx.operatorUserErase(PLAN.preUserErase, randomUUID()); bump(before, "operator_user_erase");
      await fx.selectionDeletion(...PLAN.preSelection); bump(before, "selection_deletion");
      await fx.sessionErasureFence(...PLAN.preSessionFence); bump(before, "session_erasure_fence");
      obs.push(`${sizeOut.batches} buffered batches, ${sizeOut.records} records, ${sizeOut.sessions} sessions, ${sizeOut.staged_bytes} staged bytes in ${sizeOut.buffered_stages} stages`);
      obs.push(`pre-hold erasures: ${JSON.stringify(before)}`);
      await liveUploads("seed", "sql");
    });

    // ---- rearm list ------------------------------------------------------------------------------------------------
    await step("rearm.list", async (obs) => {
      const scopes = rearmScopes(KEY_ROOT, [ORG]);
      let markers = 0;
      for (const p of scopes) markers += (await listAll(fx.store, p)).filter((k) => k.includes("/ledger/rearm/") && k.endsWith(".json") && !k.includes(".done/") && !k.includes(".corrupt/")).length;
      const refuse = async (): Promise<never> => { throw new StepFailure("rearm_dry_run_erased"); };
      const pass = await sweepRearmMarkers({ store: fx.store, eraser: { eraseStep: refuse, verifyErased: refuse } as never }, { keyRoot: KEY_ROOT, bucket: "synthetic-bucket", orgs: [ORG], dryRun: true });
      must(pass.complete && pass.counts.failed === 0, "rearm_pass_incomplete", `dry pass complete=${pass.complete} failed=${pass.counts.failed}`);
      obs.push(`${scopes.length} scope(s), ${markers} pending re-arm marker(s); dry pass listed ${pass.counts.listed}, would erase ${pass.counts.wouldErase}`);
      // TODO(sessions-wave-tooling): bind freshSweeperPass / verifySweeperPass for the org (rearmSweeper "live" attestation).
    });

    // ---- hold install ----------------------------------------------------------------------------------------------
    await step("hold.install", async (obs) => {
      const r = await heads.holdInstall.run(ctx, randomUUID());
      epoch = r.epoch;
      const st = (await fx.one<{ s: any }>("SELECT sessions.ingest_org_admission_hold_status($1) s", [ORG])).s;
      must(st.hold?.state === "held" && st.hold?.epoch === epoch, "hold_not_held");
      installMs = Date.parse(st.hold.installed_at); // the install's commit, on the database clock
      await liveUploads("hold.install", "sql");
      const refused = uploads.filter((x) => x.step === "hold.install");
      must(refused.every((x) => x.outcome === "RH042"), "admission_not_refused", `live uploads under the hold: ${refused.map((x) => x.outcome).join(",")}`);
      obs.push(`hold held; ${refused.length} live SQL upload(s) refused RH042`);
      // Held (not sealed): the user's own controls still proceed, and they are erasures the bridge must account for.
      if (scenario === "injected") {
        await planted({ kind: "selection_deletion", tenant: tenantOf(PLAN.heldSelection[0]), originId: originOfSession(...PLAN.heldSelection), source: SOURCE, sessionId: sessionOf(...PLAN.heldSelection) },
          () => fx.selectionDeletion(...PLAN.heldSelection));
        bump(during, "selection_deletion");
        obs.push("a user session deletion while held proceeded (mirrored S3-first)");
      }
    }, heads.holdInstall.integration);

    // ---- FULL ------------------------------------------------------------------------------------------------------
    await step("full", async (obs) => {
      await fx.db.query("SELECT pg_sleep(0.005)");
      fullStartedMs = await dbNowMs(); // the FULL's created_at: strictly after the install committed
      const st = (await fx.one<{ s: any }>("SELECT sessions.ingest_org_admission_hold_status($1) s", [ORG])).s;
      must(fullStartedMs > Date.parse(st.hold.installed_at), "full_before_install");
      full = await backfillUploadFences({ sql: { query: (q, p) => fx.db.query(q, p as never) as never }, store: fx.store, keyRoot: KEY_ROOT, dryRun: false, orgs: [ORG] });
      must(Object.values(full.sections).every((s) => s === "complete") && full.fences.failed + full.ownership.failed + full.revisions.failed === 0, "full_incomplete", JSON.stringify(full.sections));
      fullReceipt = JSON.stringify({ kind: "backfill-execute", run: "synthetic-full-1", created_ms: fullStartedMs, scope: [ORG], summary: full });
      obs.push(`fences ${full.fences.created + full.fences.updated}, ownership ${full.ownership.registered}, revision rows ${full.revisions.rows}, owned lazy slots ${full.revisions.slot_rows}, merged ${full.revisions.merged}`);
      await liveUploads("full", "sql");
    });

    // ---- seal + bind -----------------------------------------------------------------------------------------------
    await step("seal.bind", async (obs) => {
      const r = await heads.holdSealBind.run(ctx, epoch, { run: "synthetic-full-1", startedMs: fullStartedMs, receiptSha256: sha(enc.encode(fullReceipt)) });
      const st = (await fx.one<{ s: any }>("SELECT sessions.ingest_org_admission_hold_status($1) s", [ORG])).s;
      must(st.hold.state === "sealed", "hold_not_sealed");
      must(Number(st.selected_full?.started_ms) === fullStartedMs && st.selected_full?.run === "synthetic-full-1", "bind_mismatch");
      must(Date.parse(st.hold.installed_at) < fullStartedMs && fullStartedMs <= Date.parse(st.hold.sealed_at), "full_order_invalid");
      must(r.fenceAtMs === Date.parse(st.hold.installed_at), "fence_not_install");
      obs.push(`sealed; bound FULL start in (install, seal]; history fence = install; covered=${st.erasure_history?.covered} recording=${st.erasure_history?.recording}`);
      must((await sqlCode("SELECT sessions.ingest_org_admission_hold_check($1,'publication')", [ORG])) === "RH042", "publication_not_refused_while_sealed");
      obs.push("publication refused while sealed (RH042)");
      // Erasures during the sealed hold: erasure class, never refused.
      if (scenario === "injected") {
        const req = randomUUID();
        await planted({ kind: "user_erase", requestId: req, tenant: tenantOf(PLAN.sealedUserErase) }, () => fx.operatorUserErase(PLAN.sealedUserErase, req));
        bump(during, "operator_user_erase");
        await planted({ kind: "session_erase", tenant: tenantOf(PLAN.sealedSessionFence[0]), originIds: originsOf(PLAN.sealedSessionFence[0]), source: SOURCE, sessionId: sessionOf(...PLAN.sealedSessionFence) },
          () => fx.sessionErasureFence(...PLAN.sealedSessionFence));
        bump(during, "session_erasure_fence");
        obs.push(`erasures while sealed proceeded: ${JSON.stringify(during)} (the session fence is a table insert: recorder coverage only, not the 062360 producer)`);
      }
      if (opts.unreplayedErasure) { await planted(null, () => fx.sessionErasureFence(PLAN.sealedSessionFence[0], 1)); bump(during, "session_erasure_fence_unreplayed"); }
      await liveUploads("seal.bind", "sql");
    }, heads.holdSealBind.integration);

    // ---- catch-up --------------------------------------------------------------------------------------------------
    await step("catchup", async (obs) => {
      catchupCreatedMs = await dbNowMs();
      const since = fullStartedMs - CATCHUP_MARGIN_MS;
      const c = await backfillUploadFences({ sql: { query: (q, p) => fx.db.query(q, p as never) as never }, store: fx.store, keyRoot: KEY_ROOT, dryRun: false, orgs: [ORG], revisionsSinceMs: since });
      must(Object.values(c.sections).every((s) => s === "complete") && c.fences.failed + c.ownership.failed + c.revisions.failed === 0, "catchup_incomplete", JSON.stringify(c.sections));
      must(c.revisions_since_ms === since, "catchup_since_mismatch");
      catchupReceipt = JSON.stringify({ kind: "backfill-catchup-execute", run: "synthetic-catchup-1", created_ms: catchupCreatedMs, full_run: "synthetic-full-1", scope: [ORG], summary: c });
      obs.push(`since FULL start - 15 min; revision rows ${c.revisions.rows}, owned lazy slots ${c.revisions.slot_rows} (in full), merged ${c.revisions.merged}; fences ${c.fences.created + c.fences.updated + c.fences.unchanged}`);
      await liveUploads("catchup", "sql");
    });

    // ---- reshard verify --------------------------------------------------------------------------------------------
    await step("reshard.verify", async (obs) => {
      const r = await reshardRevisions(fx.store, KEY_ROOT, { prefix: `${KEY_ROOT}/.admission-inbox/${ORG}/`, execute: false, verify: true });
      must(r.missing === 0 && r.invalid === 0 && r.failed === 0, "reshard_verify_failed", JSON.stringify(r));
      obs.push(`listed ${r.listed}, legacy shards ${r.legacy_shards}, verified ${r.verified}, missing 0`);
    });

    // ---- marker ----------------------------------------------------------------------------------------------------
    await step("marker", async (obs) => {
      await heads.markerExecute.run(ctx, epoch, { catchupReceiptSha256: sha(enc.encode(catchupReceipt)), writtenAtMs: Date.now() });
      obs.push("marker_attempted committed before the PUT; marker written create-only and read back");
    }, heads.markerExecute.integration);

    // ---- post-marker verify ----------------------------------------------------------------------------------------
    await step("marker.verify", async (obs, fail) => {
      const st = (await fx.one<{ s: any }>("SELECT sessions.ingest_org_admission_hold_status($1) s", [ORG])).s;
      must(st.hold.state === "marker_attempted" && st.hold.epoch === epoch, "hold_not_attempted");
      const m = JSON.parse(new TextDecoder().decode(await readAll(await fx.store.get(objectKey(orgFenceAuthorityKey(KEY_ROOT, ORG))))));
      must(validOrgFenceAuthority(m) && m.orgId === ORG, "marker_invalid");
      must((await sqlCode("SELECT sessions.ingest_org_admission_hold_release($1,$2::uuid,'verified_rollback',$3,NULL,NULL)", [ORG, epoch, "f".repeat(64)])) === "55000", "rollback_after_attempt");
      obs.push("hold marker_attempted at the epoch; marker valid; verified_rollback refused (55000)");
      await liveUploads("marker.verify", "sql");
      const sqlNow = uploads.filter((x) => x.step === "marker.verify" && x.path === "sql");
      must(sqlNow.every((x) => x.outcome === "RH042"), "sql_admission_open_after_marker");
      // The stated order resumes admission AFTER release: from here to COMMITTED bridge_complete the S3 path must stay
      // closed (named hold refusal, no durable write). On main the S3 consumer serves an org once its marker exists and
      // never reads the SQL hold.
      const probes = await liveUploads("marker.verify", "s3");
      // The generation question: an upload of the user erased during the hold, after the marker, before its replay.
      if (scenario === "injected")
        probes.push(await upload("marker.verify", "s3", { u: PLAN.sealedUserErase, origin: originsOf(PLAN.sealedUserErase)[1]!, batchId: "late-b-0", session: `late-${String(PLAN.sealedUserErase).padStart(3, "0")}`, records: 2, revision: 2, payloadBytes: 64 }));
      await checkS3Closed(probes, fail, obs);
    });

    // ---- drain (export, then drain, within 3 h of the catch-up) -----------------------------------------------------
    await step("drain", async (obs, fail) => {
      const age = (await dbNowMs()) - catchupCreatedMs;
      must(age < DRAIN_WINDOW_MS, "drain_window_expired", `catch-up age ${age} ms`);
      const ex = await exportAcceptedStages({ sql: { query: (q, p) => fx.db.query(q, p as never) as never }, store: fx.store, keyRoot: KEY_ROOT, dryRun: false, orgs: [ORG] });
      must(ex.failed === 0 && ex.orgs.incomplete === 0, "export_incomplete", JSON.stringify({ failed: ex.failed_codes, orgs: ex.orgs }));
      const dr = await drainBufferedBatches({ store: fx.store, keyRoot: KEY_ROOT, orgs: [ORG], dryRun: false, decompress: (b) => zstdDecompressSync(b) });
      // The operator-erased users' staged objects lost their acknowledged stage in SQL (the erase), so the export gave
      // them no sidecar and the drain refuses them (stage_unacknowledged, never written). Exactly those, no other.
      const erasedStages = (scenario === "injected" ? 2 : 1) * size.sessionsPerUser * size.batchesPerSession;
      must(dr.objects.stage_unacknowledged === erasedStages, "drain_unacknowledged_unexpected", `stage_unacknowledged ${dr.objects.stage_unacknowledged}, expected ${erasedStages} (the operator-erased users)`);
      const bad = Object.entries(dr.objects).filter(([k, v]) => v > 0 && !["drained", "already_drained", "stage_not_live", "stage_superseded", "pre_reset", "redacted", "vanished", "stage_unacknowledged"].includes(k));
      must(dr.stopped === "complete" && bad.length === 0, "drain_incomplete", JSON.stringify(Object.fromEntries(bad)));
      must(dr.objects.drained + dr.objects.already_drained === ex.live, "drain_count_mismatch", `drained ${dr.objects.drained} + already ${dr.objects.already_drained} vs export live ${ex.live}`);
      drainAlreadyStored = dr.lines.already_stored; drainWritten = dr.lines.written;
      obs.push(`catch-up age at drain ${age} ms (< 3 h); export rows ${ex.rows} live ${ex.live} written ${ex.written}; drain listed ${dr.listed} drained ${dr.objects.drained} unacknowledged (erased users) ${dr.objects.stage_unacknowledged} not-live ${dr.objects.stage_not_live} lines written ${dr.lines.written} already stored ${dr.lines.already_stored}`);
      // Materialization (layered): an index revision or digest is not proof of canonical bytes. Every backlog session
      // that is not erased must have exactly its canonical manifest after the drain (its records are compared exactly
      // in `reads`), and no erased session may have one.
      let unmaterialized = 0, erasedMaterialized = 0;
      const erasedSessions = new Set([sessionOf(...PLAN.preSelection), sessionOf(...PLAN.preSessionFence),
        ...(scenario === "injected" ? [sessionOf(...PLAN.heldSelection), sessionOf(...PLAN.sealedSessionFence)] : [])]);
      for (let u = 0; u < size.users; u++) {
        if (u === PLAN.preUserErase || (scenario === "injected" && u === PLAN.sealedUserErase)) continue;
        for (let s = 0; s < size.sessionsPerUser; s++) {
          const has = (await manifestOf(u, originOfSession(u, s), sessionOf(u, s))) !== null;
          if (erasedSessions.has(sessionOf(u, s))) { if (has) erasedMaterialized++; } else if (!has) unmaterialized++;
        }
      }
      obs.push(`backlog sessions without their canonical manifest after the drain: ${unmaterialized}; erased sessions with one: ${erasedMaterialized}`);
      if (unmaterialized > 0) fail("drain_not_materialized", `lines written ${dr.lines.written}, already_stored ${dr.lines.already_stored}: the index entries the FULL seeded from lazy slots are not canonical bytes`);
      if (erasedMaterialized > 0) fail("drain_materialized_erased", `${erasedMaterialized} erased session(s) have a canonical manifest`);
      await checkS3Closed(await liveUploads("drain", "s3"), fail, obs);
    });

    // ---- release (bridge_complete) ---------------------------------------------------------------------------------
    await step("release", async (obs, fail) => {
      const st = (await fx.one<{ s: any }>("SELECT sessions.ingest_org_admission_hold_status($1) s", [ORG])).s;
      historySinceInstall = st.erasure_history?.by_source ?? null;
      obs.push(`erasure history since install: ${JSON.stringify(historySinceInstall)} (${st.erasures} row(s))`);
      if (scenario === "injected") must(Number(st.erasures) > 0, "erasure_window_empty_unexpected", "the fixture plants erasures during the hold");
      else must(Number(st.erasures) === 0, "erasure_window_nonempty", "the empty scenario plants no erasure during the hold");
      // Coverage by request and effect, not by kind: every history row since the fence must be one a planted erasure
      // produced (attributed when it was written), and that erasure must be replayed. The stand-in release passes the
      // count it reads, so _release's own count check cannot catch an erasure the replay never saw.
      const window = await historyKeys(st.erasure_history?.fence_at);
      const covered = new Set([...attributed.values()].flatMap((rows) => [...rows]));
      const unaccounted = [...window].filter((k) => !covered.has(k));
      must(window.size === Number(st.erasures), "erasure_history_count_mismatch", `rows ${window.size} vs status ${st.erasures}`);
      must(unaccounted.length === 0, "erasure_unaccounted", `${unaccounted.length} history row(s) in the window no replayed erasure produced (sources: ${[...new Set(unaccounted.map((k) => k.split("|")[0]))].join(",")})`);
      obs.push(`every one of ${window.size} history row(s) attributed to one of ${replay.length} planted erasure(s)`);
      // Still closed before the replays.
      await checkS3Closed(await liveUploads("release.pre", "s3"), fail, obs);
      const receipts: ReplayReceipt[] = [];
      for (const it of replay) {
        if (it.kind === "user_erase") receipts.push(await heads.userEraseReplay.run(ctx, epoch, it));
        else if (it.kind === "session_erase") receipts.push(await heads.sessionEraseReplay.run(ctx, epoch, it));
        else receipts.push(await heads.selectionReplay.run(ctx, epoch, it));
      }
      must(receipts.length === replay.length, "erasure_unreceipted");
      const again = (await fx.one<{ s: any }>("SELECT sessions.ingest_org_admission_hold_status($1) s", [ORG])).s;
      // A direct upload already in flight when the release commits: it started before the commit, so it must be refused.
      const n = liveSeq++, u = PLAN.live[0], session = `live-${String(u).padStart(3, "0")}-${String(n).padStart(4, "0")}`;
      const raw = await fx.rawBatch(u, session, originsOf(u)[0]!, `live-b-${n}`, 2, 3, 128); // prepared first: sent before the release starts
      const inflight = upload("release.inflight", "s3", { u, origin: originsOf(u)[0]!, batchId: `live-b-${n}`, session, records: 3, revision: 2, payloadBytes: 128, raw });
      try {
        await heads.bridgeRelease.run(ctx, epoch, { erasures: Number(again.erasures), receipts, evidenceSha256: sha(JSON.stringify(receipts.map((r) => r.hash))),
          detail: scenario === "empty" ? "erasure_window_empty" : null });
      } finally {
        const ev = await inflight;
        const after = (await fx.one<{ s: any }>("SELECT sessions.ingest_org_admission_hold_status($1) s", [ORG])).s;
        if (after.hold?.state === "released") releaseCommittedMs = Date.parse(after.hold.released_at);
        obs.push(`in-flight upload: started ${releaseCommittedMs === null ? "?" : ev.startedMs - releaseCommittedMs} ms vs release commit, ${ev.outcome}`);
        if (releaseCommittedMs === null || ev.startedMs < releaseCommittedMs) await checkS3Closed([ev], fail, obs);
      }
      const after = (await fx.one<{ s: any }>("SELECT sessions.ingest_org_admission_hold_status($1) s", [ORG])).s;
      must(after.hold.state === "released" && after.hold.release_reason === "bridge_complete", "not_released");
      obs.push(`${receipts.length} erasure(s) receipted (${receipts.map((r) => r.outcome).join(",") || "none"}); released bridge_complete`);
    }, `${heads.userEraseReplay.integration}/${heads.sessionEraseReplay.integration}/${heads.bridgeRelease.integration}`);

    // ---- resumed upload admission ----------------------------------------------------------------------------------
    await step("admission", async (obs) => {
      // Every upload refused during the hold, retried on the S3 ledger (the org's path after the cutover), then new ones.
      const retry = queued.splice(0);
      const results: UploadEvent[] = [];
      for (const q of retry) results.push(await upload("admission.retry", "s3", { u: q.u, origin: q.origin, batchId: q.batchId, session: q.session, records: q.records, revision: q.revision, payloadBytes: q.payloadBytes }));
      const fresh = await liveUploads("admission", "s3");
      const ok = results.filter((r) => r.outcome === "accepted").length;
      must(ok === retry.length && fresh.every((x) => x.outcome === "accepted"), "admission_not_resumed", `retried ${ok}/${retry.length}; new ${fresh.map((x) => x.outcome).join(",")}`);
      obs.push(`${ok}/${retry.length} upload(s) refused during the hold admitted on retry; ${fresh.length} new admitted (S3 ledger)`);
    });

    // ---- resumed search publication --------------------------------------------------------------------------------
    await step("publication", async (obs, fail) => {
      must((await sqlCode("SELECT sessions.ingest_org_admission_hold_check($1,'publication')", [ORG])) === "ok", "publication_gate_closed");
      obs.push("062320 publication gate open for the org after release");
      // The gate is what the hold changes; publication itself (claim, publish, search rows) is the head's to prove.
      try {
        const r = await heads.publication.run(ctx, { sessions: [] });
        obs.push(`published ${r.published}`);
      } catch (e) {
        if (!(e instanceof HeadPending)) throw e;
        fail(e.code, `head pending: ${e.head}`);
      }
    }, heads.publication.integration);

    // ---- reads of old and new events -------------------------------------------------------------------------------
    await step("reads", async (obs, fail) => {
      const decompress = (b: Uint8Array, max: number) => zstdDecompressSync(b, { maxOutputLength: max });
      /**
       * The session's records through the read API's canonical reader (readManifestRecords: every chunk fetched and its
       * digest verified), from exactly its manifest, compared exactly with what was uploaded: the same record ids, each
       * at the expected revision with the expected payload text. "missing" when there is no manifest.
       */
      const exact = async (u: number, origin: string, session: string, records: number, revision: number, payloadBytes: number): Promise<"exact" | "missing" | "mismatch"> => {
        const mk = await manifestOf(u, origin, session);
        if (!mk) return "missing";
        const manifest = JSON.parse(new TextDecoder().decode(await readAll(await fx.store.get(objectKey(mk)))));
        const rows = await readManifestRecords(manifest, fx.store, mk.slice(0, mk.indexOf("/v1/")), decompress);
        const got = new Map(rows.map((r: any) => [String(r.record_id), r]));
        if (got.size !== records || rows.length !== records) return "mismatch";
        for (let k = 0; k < records; k++) {
          const r: any = got.get(`${session}-r${String(k).padStart(4, "0")}`);
          if (!r || String(r.revision) !== String(revision) || r.payload?.text !== text(payloadBytes, `${session}/${k}/${revision}`)) return "mismatch";
        }
        return "exact";
      };
      const touched = new Set([PLAN.preUserErase, PLAN.preSelection[0], PLAN.preSessionFence[0], PLAN.heldSelection[0], PLAN.sealedUserErase, PLAN.sealedSessionFence[0]]);
      const old = { exact: 0, missing: 0, mismatch: 0 };
      for (let u = 0; u < size.users; u++) {
        if (touched.has(u)) continue;
        for (let s = 0; s < size.sessionsPerUser; s++) old[await exact(u, originOfSession(u, s), sessionOf(u, s), size.recordsPerBatch, size.batchesPerSession + 1, size.payloadBytes)]++;
      }
      const admitted = [...new Set(uploads.filter((x) => x.path === "s3" && x.outcome === "accepted" && !x.session.startsWith("late-")).map((x) => x.session))];
      const fresh = { exact: 0, missing: 0, mismatch: 0 };
      for (const sess of admitted) { const sp = specs.get(sess)!; fresh[await exact(sp.u, sp.origin, sess, sp.records, sp.revision, sp.payloadBytes)]++; }
      obs.push(`old (backlog) sessions: ${JSON.stringify(old)}; new (S3-admitted) sessions: ${JSON.stringify(fresh)}`);
      if (old.missing + old.mismatch > 0) obs.push(`the drain wrote no canonical line for backlog whose owned lazy slots the FULL put in the S3 index with their digest (drain lines already_stored ${drainAlreadyStored}, written ${drainWritten})`);
      const late = specs.get(`late-${String(PLAN.sealedUserErase).padStart(3, "0")}`);
      if (late) obs.push(`the erased user's upload accepted before its replay, after the release: ${await exact(late.u, late.origin, late.session, late.records, late.revision, late.payloadBytes)} (generation boundary: the head decides)`);
      must(admitted.length > 0 && fresh.exact === admitted.length, "new_events_not_exact");
      if (old.exact === 0 || old.missing + old.mismatch > 0) fail("old_events_not_canonical", `old sessions not readable exactly: ${old.missing} missing, ${old.mismatch} mismatched`);
      // TODO(read API): the same reads over HTTP (/v1/sessions/:id/events) on the compose stack.
    });
  } finally {
    await fx.close();
  }

  const reasons: string[] = [];
  for (const s of steps) if (s.status !== "PASS") reasons.push(`${s.id}: ${s.status}${s.code ? ` ${s.code}` : ""}`);
  if (!allLanded(heads)) reasons.push("integration: " + Object.entries(heads).filter(([, a]) => a.integration !== "landed").map(([k, a]) => `${k}=${a.integration}`).join(", "));
  if (opts.simulatePending) reasons.push("simulated: pending heads replaced by their S3 effect (timings only)");
  if (fx.kind !== QUALIFICATION_BACKEND) reasons.push(`backend: ${fx.kind} is not the qualification backend (${QUALIFICATION_BACKEND})`);
  const fs3 = firstS3 as { startedMs: number; endedMs: number } | null;
  return {
    v: 1, kind: "synthetic-cutover-qualification", verdict: reasons.length === 0 ? "QUALIFIED" : "NOT_QUALIFIED", reasons,
    simulated: opts.simulatePending === true, backend: fx.kind, scenario, size: sizeOut, steps,
    timeline: { install_committed_ms: installMs, first_s3_admit_started_ms: fs3?.startedMs ?? null, first_s3_admit_accepted_ms: fs3?.endedMs ?? null,
      release_committed_ms: releaseCommittedMs, first_admit_after_release_started_ms: firstAfterRelease },
    downtime: {
      observed_bound_ms: installMs !== null && fs3 ? fs3.endedMs - installMs : null,
      through_release_ms: installMs !== null && releaseCommittedMs !== null ? releaseCommittedMs - installMs : null,
    },
    uploads, erasures: { before_hold: before, during_hold: during, history_since_install: historySinceInstall },
    integration: integrationSummary(heads), wall_ms: Date.now() - wall0,
  };
}

/** The report as markdown: verdict, size, the per-step table and the downtime. */
export function reportMarkdown(r: CutoverReport): string {
  const lines = [
    `# Synthetic per-org cutover qualification`, ``,
    `**Verdict: ${r.verdict}**${r.simulated ? " (SIMULATED pending heads: timings only)" : ""}`, ``,
    ...r.reasons.map((x) => `- ${x}`), ``,
    `Backend: ${r.backend}. Erasure scenario: ${r.scenario}. Fixture: 1 org, ${r.size.users} users, ${r.size.sessions} sessions, ${r.size.batches} buffered batches, ${r.size.records} records, ${r.size.staged_bytes} staged bytes (${r.size.buffered_stages} stages).`, ``,
    `| Step | Status | ms | Integration | Observed |`, `| --- | --- | ---: | --- | --- |`,
    ...r.steps.map((s) => `| ${s.id} | ${s.status}${s.code ? ` \`${s.code}\`` : ""} | ${s.ms} | ${s.integration ?? "main"} | ${s.observed.join("; ").replace(/\|/g, "\\|")} |`), ``,
    `Timeline (ms after the install commit): first S3 admission ${r.timeline.first_s3_admit_accepted_ms === null ? "none" : r.timeline.first_s3_admit_accepted_ms - r.timeline.install_committed_ms!}` +
      `; release committed ${r.timeline.release_committed_ms === null ? "never" : r.timeline.release_committed_ms - r.timeline.install_committed_ms!}` +
      `; first admission after release ${r.timeline.first_admit_after_release_started_ms === null ? "none" : r.timeline.first_admit_after_release_started_ms - r.timeline.install_committed_ms!}.`,
    `Observed downtime bound (install commit -> first S3 admission): ${r.downtime.observed_bound_ms ?? "n/a"} ms. Install -> release commit: ${r.downtime.through_release_ms ?? "n/a (not released)"} ms.`,
    `Wall clock: ${r.wall_ms} ms.`, ``,
    `Uploads: ${r.uploads.length} (${r.uploads.filter((u) => u.queued).length} refused and queued).`,
    `Erasures before the hold ${JSON.stringify(r.erasures.before_hold)}, during ${JSON.stringify(r.erasures.during_hold)}, history since install ${JSON.stringify(r.erasures.history_since_install)}.`, ``,
    `## Integration points`, ``,
    ...Object.entries(r.integration).map(([k, v]) => `- \`${k}\` (${v.head}): ${v.integration}${v.todo ? ` — TODO: ${v.todo}` : ""}`),
  ];
  return lines.join("\n") + "\n";
}
