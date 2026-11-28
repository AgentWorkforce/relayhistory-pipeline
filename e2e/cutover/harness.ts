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
import { objectKey, readAll, chunkKeys, type ObjectStore } from "../../src/shared/vendor.ts";
import { backfillUploadFences, type BackfillSummary } from "../../src/session-ingest/upload-fence-backfill.ts";
import { exportAcceptedStages } from "../../src/session-ingest/accepted-stage-export.ts";
import { drainBufferedBatches } from "../../src/session-ingest/buffered-drain.ts";
import { reshardRevisions } from "../../src/session-ingest/revision-reshard.ts";
import { sweepRearmMarkers, rearmScopes } from "../../src/session-ingest/rearm-sweeper.ts";
import { orgFenceAuthorityKey, validOrgFenceAuthority } from "../../src/session-ingest/ledger-s3.ts";
import { HeadPending, allLanded, defaultHeads, integrationSummary, type CutoverContext, type CutoverHeads, type ReplayItem, type ReplayReceipt } from "./heads.ts";
import { KEY_ROOT, MIN_USERS, ORG, SIZE_CI, originOfSession, originsOf, openFixture, sessionOf, tenantOf, type FixtureSize, type Fx } from "./fixture.ts";

export const CATCHUP_MARGIN_MS = 15 * 60_000;
/** BACKLOG_OPS_FRESH_BACKFILL_MS: the drain binds a catch-up under 3 h old. */
export const DRAIN_WINDOW_MS = 3 * 3600_000;

export type StepStatus = "PASS" | "FAIL" | "BLOCKED" | "SKIP";
export interface StepReport { id: string; status: StepStatus; ms: number; startedMs: number | null; observed: string[]; code?: string; integration?: string }
export interface UploadEvent { step: string; path: "sql" | "s3"; session: string; outcome: string; queued: boolean }
export interface CutoverReport {
  v: 1;
  kind: "synthetic-cutover-qualification";
  verdict: "QUALIFIED" | "NOT_QUALIFIED";
  reasons: string[];
  simulated: boolean;
  backend: "pglite+fs";
  size: FixtureSize & { batches: number; records: number; sessions: number; staged_bytes: number; buffered_stages: number };
  steps: StepReport[];
  downtime: { install_ms: number | null; resumed_ms: number | null; downtime_ms: number | null; first_s3_admit_after_install_ms: number | null };
  uploads: UploadEvent[];
  erasures: { before_hold: Record<string, number>; during_hold: Record<string, number>; history_since_install: Record<string, number> | null };
  integration: Record<string, { head: string; integration: string; todo: string | null }>;
  wall_ms: number;
}

/** A failed assertion: the step FAILs and the run stops (what follows would build on a wrong state). */
class StepFailure extends Error { constructor(readonly code: string, msg?: string, readonly soft = false) { super(msg ?? code); this.name = "StepFailure"; } }
/** A failed check that leaves the state usable: the step FAILs, the run goes on so later steps are still measured. */
function softMust(cond: unknown, code: string, msg?: string): asserts cond { if (!cond) throw new StepFailure(code, msg, true); }
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
  const queued: { u: number; raw: unknown; session: string }[] = [];
  const before: Record<string, number> = {}, during: Record<string, number> = {};
  const replay: ReplayItem[] = [];
  let historySinceInstall: Record<string, number> | null = null;
  let installMs: number | null = null, resumedMs: number | null = null, firstS3AdmitMs: number | null = null;
  let blocked = false;

  const fx: Fx = await openFixture();
  const ctx: CutoverContext = { sql: { query: (t, p) => fx.db.query(t, p as never) as never }, store: fx.store, keyRoot: KEY_ROOT, org: ORG };
  const dbNowMs = async () => Number((await fx.one<{ t: string }>("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint::text AS t")).t);
  const bump = (m: Record<string, number>, k: string) => { m[k] = (m[k] ?? 0) + 1; };
  /** "ok", or the SQLSTATE an owner statement raised. */
  const sqlCode = (q: string, p: unknown[]) => fx.db.query(q, p as never).then(() => "ok", (e: { code?: unknown }) => String(e?.code ?? "error"));
  let liveSeq = 0;

  /** One live upload per live user at this boundary, on the path a client of this org uses now. */
  async function liveUploads(step: string, path: "sql" | "s3") {
    for (const u of PLAN.live) {
      const n = liveSeq++, session = `live-${String(u).padStart(3, "0")}-${String(n).padStart(4, "0")}`;
      const raw = await fx.rawBatch(u, session, originsOf(u)[0]!, `live-b-${n}`, 2, 3, 128);
      const r = await fx.attempt(() => (path === "sql" ? fx.deliverSql(u, raw) : fx.deliverS3(u, raw)));
      const outcome = r.ok ? String(r.result.outcome) : r.code;
      if (!r.ok) queued.push({ u, raw, session });
      if (r.ok && path === "s3" && installMs !== null && firstS3AdmitMs === null) firstS3AdmitMs = Date.now();
      uploads.push({ step, path, session, outcome, queued: !r.ok });
    }
  }

  async function step(id: string, fn: (obs: string[]) => Promise<void>, integration?: string): Promise<void> {
    if (blocked) { steps.push({ id, status: "SKIP", ms: 0, startedMs: null, observed: ["not run: an earlier step blocked or failed"], ...(integration ? { integration } : {}) }); return; }
    const obs: string[] = [];
    const startedMs = Date.now(), t0 = performance.now();
    log(`step ${id} ...`);
    try {
      await fn(obs);
      steps.push({ id, status: "PASS", ms: Math.round(performance.now() - t0), startedMs, observed: obs, ...(integration ? { integration } : {}) });
    } catch (e) {
      const ms = Math.round(performance.now() - t0);
      if (e instanceof HeadPending) {
        blocked = true;
        steps.push({ id, status: "BLOCKED", ms, startedMs, observed: [...obs, `head pending: ${e.head}`], code: e.code, ...(integration ? { integration } : {}) });
      } else if (e instanceof StepFailure) {
        if (!e.soft) blocked = true;
        steps.push({ id, status: "FAIL", ms, startedMs, observed: [...obs, e.message], code: e.code, ...(integration ? { integration } : {}) });
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
  let lateErasedUpload: { session: string; outcome: string } | null = null;
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
      installMs = Date.now();
      const st = (await fx.one<{ s: any }>("SELECT sessions.ingest_org_admission_hold_status($1) s", [ORG])).s;
      must(st.hold?.state === "held" && st.hold?.epoch === epoch, "hold_not_held");
      await liveUploads("hold.install", "sql");
      const refused = uploads.filter((x) => x.step === "hold.install");
      must(refused.every((x) => x.outcome === "RH042"), "admission_not_refused", `live uploads under the hold: ${refused.map((x) => x.outcome).join(",")}`);
      obs.push(`hold held; ${refused.length} live SQL upload(s) refused RH042`);
      // Held (not sealed): the user's own controls still proceed, and they are erasures the bridge must account for.
      await fx.selectionDeletion(...PLAN.heldSelection); bump(during, "selection_deletion");
      replay.push({ kind: "selection_deletion", tenant: tenantOf(PLAN.heldSelection[0]), originId: originOfSession(...PLAN.heldSelection), source: "claude", sessionId: sessionOf(...PLAN.heldSelection) });
      obs.push("a user session deletion while held proceeded (mirrored S3-first)");
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
      const req = randomUUID();
      await fx.operatorUserErase(PLAN.sealedUserErase, req); bump(during, "operator_user_erase");
      replay.push({ kind: "user_erase", requestId: req, tenant: tenantOf(PLAN.sealedUserErase) });
      await fx.sessionErasureFence(...PLAN.sealedSessionFence); bump(during, "session_erasure_fence");
      replay.push({ kind: "session_erase", tenant: tenantOf(PLAN.sealedSessionFence[0]), originIds: originsOf(PLAN.sealedSessionFence[0]), source: "claude", sessionId: sessionOf(...PLAN.sealedSessionFence) });
      obs.push(`erasures while sealed proceeded: ${JSON.stringify(during)}`);
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
    await step("marker.verify", async (obs) => {
      const st = (await fx.one<{ s: any }>("SELECT sessions.ingest_org_admission_hold_status($1) s", [ORG])).s;
      must(st.hold.state === "marker_attempted" && st.hold.epoch === epoch, "hold_not_attempted");
      const m = JSON.parse(new TextDecoder().decode(await readAll(await fx.store.get(objectKey(orgFenceAuthorityKey(KEY_ROOT, ORG))))));
      must(validOrgFenceAuthority(m) && m.orgId === ORG, "marker_invalid");
      must((await sqlCode("SELECT sessions.ingest_org_admission_hold_release($1,$2::uuid,'verified_rollback',$3,NULL,NULL)", [ORG, epoch, "f".repeat(64)])) === "55000", "rollback_after_attempt");
      obs.push("hold marker_attempted at the epoch; marker valid; verified_rollback refused (55000)");
      await liveUploads("marker.verify", "sql");
      const sqlNow = uploads.filter((x) => x.step === "marker.verify" && x.path === "sql");
      must(sqlNow.every((x) => x.outcome === "RH042"), "sql_admission_open_after_marker");
      // The stated order resumes admission AFTER release. The S3 consumer serves an org once its marker exists and
      // never reads the SQL hold, so an S3 admission here means admission resumed before bridge_complete.
      await liveUploads("marker.verify", "s3");
      // The generation question, observed (not asserted; its semantics are the generation-proof head's): an upload of
      // the user erased during the hold, after the marker and before the bridge replays that erase.
      {
        const u = PLAN.sealedUserErase, session = `late-${String(u).padStart(3, "0")}`;
        const raw = await fx.rawBatch(u, session, originsOf(u)[1]!, "late-b-0", 2, 2, 64);
        const r = await fx.attempt(() => fx.deliverS3(u, raw));
        lateErasedUpload = { session, outcome: r.ok ? String(r.result.outcome) : r.code };
        obs.push(`S3 upload of the user erased during the hold, before its replay: ${lateErasedUpload.outcome}`);
      }
      const s3Now = uploads.filter((x) => x.step === "marker.verify" && x.path === "s3");
      obs.push(`S3 path after the marker, before release: ${s3Now.map((x) => x.outcome).join(",")}`);
      softMust(s3Now.every((x) => x.queued), "s3_admits_before_bridge_complete",
        "S3 admission reopened at the marker PUT, before drain and bridge_complete (the SQL hold does not bind the S3 consumer)");
    });

    // ---- drain (export, then drain, within 3 h of the catch-up) -----------------------------------------------------
    await step("drain", async (obs) => {
      const age = (await dbNowMs()) - catchupCreatedMs;
      must(age < DRAIN_WINDOW_MS, "drain_window_expired", `catch-up age ${age} ms`);
      const ex = await exportAcceptedStages({ sql: { query: (q, p) => fx.db.query(q, p as never) as never }, store: fx.store, keyRoot: KEY_ROOT, dryRun: false, orgs: [ORG] });
      must(ex.failed === 0 && ex.orgs.incomplete === 0, "export_incomplete", JSON.stringify({ failed: ex.failed_codes, orgs: ex.orgs }));
      const dr = await drainBufferedBatches({ store: fx.store, keyRoot: KEY_ROOT, orgs: [ORG], dryRun: false, decompress: (b) => zstdDecompressSync(b) });
      // The operator-erased users' staged objects lost their acknowledged stage in SQL (the erase), so the export gave
      // them no sidecar and the drain refuses them (stage_unacknowledged, never written). Exactly those, no other.
      const erasedStages = 2 * size.sessionsPerUser * size.batchesPerSession;
      must(dr.objects.stage_unacknowledged === erasedStages, "drain_unacknowledged_unexpected", `stage_unacknowledged ${dr.objects.stage_unacknowledged}, expected ${erasedStages} (the two erased users)`);
      const bad = Object.entries(dr.objects).filter(([k, v]) => v > 0 && !["drained", "already_drained", "stage_not_live", "stage_superseded", "pre_reset", "redacted", "vanished", "stage_unacknowledged"].includes(k));
      must(dr.stopped === "complete" && bad.length === 0, "drain_incomplete", JSON.stringify(Object.fromEntries(bad)));
      must(dr.objects.drained + dr.objects.already_drained === ex.live, "drain_count_mismatch", `drained ${dr.objects.drained} + already ${dr.objects.already_drained} vs export live ${ex.live}`);
      drainAlreadyStored = dr.lines.already_stored; drainWritten = dr.lines.written;
      obs.push(`catch-up age at drain ${age} ms (< 3 h); export rows ${ex.rows} live ${ex.live} written ${ex.written}; drain listed ${dr.listed} drained ${dr.objects.drained} unacknowledged (erased users) ${dr.objects.stage_unacknowledged} not-live ${dr.objects.stage_not_live} lines written ${dr.lines.written} already stored ${dr.lines.already_stored}`);
      await liveUploads("drain", "s3");
    });

    // ---- release (bridge_complete) ---------------------------------------------------------------------------------
    await step("release", async (obs) => {
      const st = (await fx.one<{ s: any }>("SELECT sessions.ingest_org_admission_hold_status($1) s", [ORG])).s;
      historySinceInstall = st.erasure_history?.by_source ?? null;
      obs.push(`erasure history since install: ${JSON.stringify(historySinceInstall)} (${st.erasures} row(s))`);
      must(Number(st.erasures) > 0, "erasure_window_empty_unexpected", "the fixture plants erasures during the hold");
      // Fail closed on coverage: every history source in the window must map to a replayed kind. The stand-in release
      // passes the count it reads, so _release's own count check cannot catch an erasure the replay never saw.
      const covers: Record<string, ReplayItem["kind"]> = { operator_user_erasures: "user_erase", session_erasure_fences: "session_erase", upload_deletions: "selection_deletion" };
      const kinds = new Set(replay.map((x) => x.kind));
      const unaccounted = Object.keys(historySinceInstall ?? {}).filter((src) => !covers[src] || !kinds.has(covers[src]!));
      must(unaccounted.length === 0, "erasure_unaccounted", `history sources with no replay: ${unaccounted.join(",")}`);
      const receipts: ReplayReceipt[] = [];
      for (const it of replay) {
        if (it.kind === "user_erase") receipts.push(await heads.userEraseReplay.run(ctx, epoch, it));
        else if (it.kind === "session_erase") receipts.push(await heads.sessionEraseReplay.run(ctx, epoch, it));
        else receipts.push(await heads.selectionReplay.run(ctx, epoch, it));
      }
      const again = (await fx.one<{ s: any }>("SELECT sessions.ingest_org_admission_hold_status($1) s", [ORG])).s;
      await heads.bridgeRelease.run(ctx, epoch, { erasures: Number(again.erasures), receipts, evidenceSha256: sha(JSON.stringify(receipts.map((r) => r.hash))) });
      const after = (await fx.one<{ s: any }>("SELECT sessions.ingest_org_admission_hold_status($1) s", [ORG])).s;
      must(after.hold.state === "released" && after.hold.release_reason === "bridge_complete", "not_released");
      obs.push(`${receipts.length} erasure(s) receipted (${receipts.map((r) => r.outcome).join(",")}); released bridge_complete`);
    }, `${heads.userEraseReplay.integration}/${heads.sessionEraseReplay.integration}/${heads.bridgeRelease.integration}`);

    // ---- resumed upload admission ----------------------------------------------------------------------------------
    await step("admission", async (obs) => {
      const retry = queued.splice(0);
      let ok = 0;
      for (const q of retry) {
        const r = await fx.attempt(() => fx.deliverS3(q.u, q.raw));
        if (r.ok && r.result.outcome === "accepted") ok++;
        uploads.push({ step: "admission.retry", path: "s3", session: q.session, outcome: r.ok ? String(r.result.outcome) : r.code, queued: !r.ok });
      }
      resumedMs = Date.now();
      await liveUploads("admission", "s3");
      const fresh = uploads.filter((x) => x.step === "admission");
      must(ok === retry.length && fresh.every((x) => x.outcome === "accepted"), "admission_not_resumed", `retried ${ok}/${retry.length}; new ${fresh.map((x) => x.outcome).join(",")}`);
      obs.push(`${ok}/${retry.length} upload(s) refused during the hold admitted on retry; ${fresh.length} new admitted (S3 ledger)`);
    });

    // ---- resumed search publication --------------------------------------------------------------------------------
    await step("publication", async (obs) => {
      await fx.db.query("SELECT sessions.ingest_org_admission_hold_check($1,'publication')", [ORG]);
      obs.push("062320 publication gate open for the org after release");
      // TODO(release-path heads): drive one real publication (ingest_publication_claim / the buffered search feed) for
      // a drained session and assert its search_index rows; the gate is what the hold changes, the claim is not.
    });

    // ---- reads of old and new events -------------------------------------------------------------------------------
    await step("reads", async (obs) => {
      const keys = await listAll(fx.store, `${KEY_ROOT}/${ORG}`);
      const manifests = keys.filter((k) => k.endsWith("/manifest.json"));
      const readable = async (session: string) => {
        const mk = manifests.find((k) => k.includes(session));
        if (!mk) return false;
        const m = JSON.parse(new TextDecoder().decode(await readAll(await fx.store.get(objectKey(mk)))));
        const prefix = mk.slice(0, mk.indexOf("/v1/"));
        const named = chunkKeys(m as never, { prefix }).map((k) => String(k.key));
        return named.length > 0 && named.every((k) => keys.includes(k));
      };
      const untouched = new Set([PLAN.preUserErase, PLAN.preSelection[0], PLAN.preSessionFence[0], PLAN.heldSelection[0], PLAN.sealedUserErase, PLAN.sealedSessionFence[0]]);
      let old = 0, oldMissing = 0;
      for (let u = 0; u < size.users; u++) {
        if (untouched.has(u)) continue;
        for (let s = 0; s < size.sessionsPerUser; s++) (await readable(sessionOf(u, s))) ? old++ : oldMissing++;
      }
      const admittedS3 = [...new Set(uploads.filter((x) => x.path === "s3" && x.outcome === "accepted").map((x) => x.session))];
      let newOk = 0;
      for (const sess of admittedS3) if (await readable(sess)) newOk++;
      obs.push(`canonical manifests under the org: ${manifests.length}`);
      obs.push(`old (backlog) sessions readable: ${old}/${old + oldMissing}; new (S3-admitted) sessions readable: ${newOk}/${admittedS3.length}`);
      if (oldMissing > 0) obs.push(`the drain wrote no canonical line for backlog whose owned lazy slots the FULL put in the S3 index with their digest (drain lines already_stored ${drainAlreadyStored}, written ${drainWritten})`);
      // Observations, not assertions: bytes of erased sessions are the eraser's job (sweeper / bytes worker), not the cutover's.
      const erased = [sessionOf(PLAN.preSessionFence[0], 0), sessionOf(PLAN.sealedSessionFence[0], 0), sessionOf(...PLAN.preSelection), sessionOf(...PLAN.heldSelection)];
      obs.push(`erased sessions still holding a canonical manifest (eraser's job): ${erased.filter((s) => manifests.some((k) => k.includes(s))).length}/${erased.length}`);
      if (lateErasedUpload) obs.push(`the erased user's pre-replay S3 upload after the release: ${(await readable(lateErasedUpload.session)) ? "still readable" : "not readable"} (generation boundary: the head decides)`);
      must(admittedS3.length > 0 && newOk === admittedS3.length, "new_events_unreadable");
      must(old > 0 && oldMissing === 0, "old_events_not_canonical");
      // TODO(read API): the same reads through session-read-api (/v1/sessions/:id/events) on the compose stack.
    });
  } finally {
    await fx.close();
  }

  const reasons: string[] = [];
  for (const s of steps) if (s.status !== "PASS") reasons.push(`${s.id}: ${s.status}${s.code ? ` ${s.code}` : ""}`);
  if (!allLanded(heads)) reasons.push("integration: " + Object.entries(heads).filter(([, a]) => a.integration !== "landed").map(([k, a]) => `${k}=${a.integration}`).join(", "));
  if (opts.simulatePending) reasons.push("simulated: pending heads replaced by their S3 effect (timings only)");
  return {
    v: 1, kind: "synthetic-cutover-qualification", verdict: reasons.length === 0 ? "QUALIFIED" : "NOT_QUALIFIED", reasons,
    simulated: opts.simulatePending === true, backend: "pglite+fs", size: sizeOut, steps,
    downtime: {
      install_ms: installMs, resumed_ms: resumedMs,
      downtime_ms: installMs !== null && resumedMs !== null ? resumedMs - installMs : null,
      first_s3_admit_after_install_ms: installMs !== null && firstS3AdmitMs !== null ? firstS3AdmitMs - installMs : null,
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
    `Backend: ${r.backend}. Fixture: 1 org, ${r.size.users} users, ${r.size.sessions} sessions, ${r.size.batches} buffered batches, ${r.size.records} records, ${r.size.staged_bytes} staged bytes (${r.size.buffered_stages} stages).`, ``,
    `| Step | Status | ms | Integration | Observed |`, `| --- | --- | ---: | --- | --- |`,
    ...r.steps.map((s) => `| ${s.id} | ${s.status}${s.code ? ` \`${s.code}\`` : ""} | ${s.ms} | ${s.integration ?? "main"} | ${s.observed.join("; ").replace(/\|/g, "\\|")} |`), ``,
    `Admission downtime (install -> resumed admission): ${r.downtime.downtime_ms === null ? "not reached (fail closed)" : `${r.downtime.downtime_ms} ms`}.`,
    `First S3 admission after install: ${r.downtime.first_s3_admit_after_install_ms === null ? "none" : `${r.downtime.first_s3_admit_after_install_ms} ms`}.`,
    `Wall clock: ${r.wall_ms} ms.`, ``,
    `Uploads: ${r.uploads.length} (${r.uploads.filter((u) => u.queued).length} refused and queued).`,
    `Erasures before the hold ${JSON.stringify(r.erasures.before_hold)}, during ${JSON.stringify(r.erasures.during_hold)}, history since install ${JSON.stringify(r.erasures.history_since_install)}.`, ``,
    `## Integration points`, ``,
    ...Object.entries(r.integration).map(([k, v]) => `- \`${k}\` (${v.head}): ${v.integration}${v.todo ? ` — TODO: ${v.todo}` : ""}`),
  ];
  return lines.join("\n") + "\n";
}
