/**
 * The 062320 admission hold's lifecycle as backlog-ops modes (docs/backlog-ops-workflow.md "Hold lifecycle"; the
 * wave-1 plan docs/design/wave-1-two-org-plan.md). CODE ONLY: every execute is HELD in code
 * (backlog-ops.ts BACKLOG_OPS_HOLD_EXECUTE_ENABLED = false), and installing a hold on a real database needs Khaliq's
 * explicit OK. Run by scripts/backlog-hold.ts; per org, at most a few orgs at once, each with its own verdict.
 *
 *   hold-dry               READ ONLY. Per org: the hold's state and epoch hash, pending batches, open canonical claims
 *                          and whether an install would be accepted now.
 *   hold-install-execute   per DRY-ready org, in ONE READ COMMITTED transaction: `_install(org, request)` (it takes the
 *                          org key EXCLUSIVE and refuses a pending batch itself, RH042), then, in the same transaction
 *                          and under that key, the open-canonical-claim precondition (CANONICAL_COMMITTED_SQL). Any open
 *                          claim rolls the install back (`hold_canonical_committed`). The request id is derived from the
 *                          commit, the DRY run and the org (installRequestId), so a retry gets the same epoch.
 *   hold-seal-execute      per approved org, in ONE transaction: the live hold at the approved epoch, `held` (or
 *                          already `sealed` by this FULL: a retry), installed_at < FULL start <= now; then `_seal(org,
 *                          epoch, digest)` and 062340's `_bind(org, epoch, full_run, full_started_ms, digest)`.
 *                          `digest` is the sha256 of the bound FULL receipt's exact bytes, the same value for both.
 *                          Seal and bind commit together or not at all, so an interruption is a plain rollback and the
 *                          retry starts from `held`.
 *   hold-rollback-execute  per approved org, in ONE transaction: the live hold at the approved epoch, `held` or `sealed`
 *                          with NO marker attempt, then `_release(org, epoch, 'verified_rollback', evidence, ...)`. The
 *                          SQL refuses a rollback after any attempt in that same transaction; the tool checks first.
 *
 * Ordering (062340): installed_at < FULL start <= sealed_at. So the FULL backfill must be DISPATCHED AFTER the install
 * committed, and the org's admission is refused (RH042, retryable) from the install through the release. Publication
 * is refused from the seal until the release. See the plan doc for the outage window and the alarms.
 *
 * Receipts are counts, codes and hashes only (org and epoch hashed). Errors carry fixed codes; SQL errors are mapped
 * from their SQLSTATE and the slot's fixed messages, never echoed.
 */
import { createHash } from "node:crypto";
import { evidenceHash } from "./authority-evidence.ts";
import { epochHash } from "./erasure-window.ts";
import { HOLD_FUNCTIONS, holdSql } from "./hold-bridge.ts";
import { parseLiveHold, type LiveHold } from "./marker-publication.ts";
import { runWave, waveOrgList } from "./org-wave.ts";
import type { ReceiptSchema } from "./backlog-authority-schema.ts";
import { conforms } from "./backlog-authority-schema.ts";

const ORG_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const HEX64 = /^[0-9a-f]{64}$/u;
const HASH = /^h_[0-9a-f]{32}$/u;
const RUN_ID = /^[1-9][0-9]{0,19}$/u;
const SHA40 = /^[0-9a-f]{40}$/u;
/** Orgs worked on at once (each holds one database session; every call takes the org key EXCLUSIVE). */
export const HOLD_LIFECYCLE_CONCURRENCY = 2;

export class HoldLifecycleRefusal extends Error { constructor(readonly code: string) { super(code); this.name = "HoldLifecycleRefusal"; } }
const refuse = (code: string): never => { throw new HoldLifecycleRefusal(code); };

// ---- SQL ------------------------------------------------------------------------------------------------------

/**
 * DIAGNOSTIC ONLY: the org's committed canonical batches, EVER (the 062320 owner's pinned predicate for gap B,
 * 7f8caf3f2170). A staged canonical batch is indistinguishable in SQL from an inline one, so this is a superset of the
 * batches whose fan-out could still be queued; zero would mean no canonical fan-out can exist for the org anywhere.
 * Pipeline has NOT accepted it as a fan-out proof (0a862bdc0160 / d39e24a34e0d: keep fanout unproven, install held),
 * so it is reported, never used to admit an install. ingest_claim_attempts is history (rows leave only through user
 * erase or the batch cascade) and is not used.
 */
export const CANONICAL_COMMITTED_SQL = `SELECT count(*)::int AS n FROM sessions.ingest_batches
  WHERE org_id = $1::text AND publication_state = 'canonical' AND state = 'committed'`;
export const PENDING_BATCHES_SQL = `SELECT count(*)::int AS n FROM sessions.ingest_batches WHERE org_id = $1::text AND state = 'pending'`;
/**
 * Unexpired seed captures of the org (20261104040000 ingest_seed_scope_capture; ingest_seed_proven_records refuses an
 * expired one, so expiry is schema-terminal). Read in the install transaction after _install took the org key, it
 * catches a COMMITTED capture only: capture issuance is classified unaffected and takes no org key, so a racing
 * capture is not excluded (gap C; slot 062390, sessions-authority-evidence). The seeder (scripts/session-ingest-seed.ts)
 * is operator-run only: a run refused during the hold writes nothing, and a later operator-run pass would re-capture,
 * but nothing schedules one.
 */
export const SEED_CAPTURES_ACTIVE_SQL = `SELECT count(*)::int AS n FROM sessions.ingest_seed_captures WHERE org_id = $1::text AND expires_at > clock_timestamp()`;
export const HOLD_BIND_FUNCTION = "sessions.ingest_org_erasure_history_bind";
export const holdBindSql = (org: string, epoch: string, fullRun: string, fullStartedMs: number, evidence: string) => {
  if (!ORG_UUID.test(org) || !ORG_UUID.test(epoch) || !RUN_ID.test(fullRun) || !Number.isSafeInteger(fullStartedMs) || fullStartedMs < 1 || !HEX64.test(evidence))
    refuse("hold_input");
  return { text: `SELECT ${HOLD_BIND_FUNCTION}($1::text, $2::uuid, $3::text, $4::bigint, $5::text)::text AS fence_at`,
    params: [org, epoch, fullRun, String(fullStartedMs), evidence] };
};
export { HOLD_FUNCTIONS };

type Rows = readonly Record<string, unknown>[];
type Q = (text: string, params: readonly unknown[]) => Promise<Rows>;
/**
 * One database session for one org at a time. `read`: one statement alone in a READ COMMITTED READ ONLY transaction.
 * `tx`: a READ COMMITTED READ WRITE transaction around `fn` (every statement of `fn` in it), committed when `fn`
 * returns and rolled back when it throws; a failed COMMIT throws.
 */
export interface HoldSqlSession {
  read: Q;
  tx<T>(fn: (q: Q) => Promise<T>): Promise<T>;
}

/** The install's request id: a v4-shaped uuid from sha256 over the commit, the DRY run and the org. */
export function installRequestId(sha: string, dryRun: string, org: string): string {
  if (!SHA40.test(sha) || !RUN_ID.test(dryRun) || !ORG_UUID.test(org)) refuse("hold_input");
  const h = createHash("sha256").update(`hold-install/v1\n${sha}\n${dryRun}\n${org}`).digest("hex");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${"89ab"[parseInt(h[16]!, 16) & 3]}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

/** A SQL error's fixed code: the SQLSTATE and the slot's fixed message decide; nothing of the error is echoed. */
export function holdSqlCode(error: unknown, stage: "install" | "seal" | "bind" | "release" | "read"): string {
  const e = error as { code?: unknown; message?: unknown } | null;
  const state = typeof e?.code === "string" ? e.code : "", msg = typeof e?.message === "string" ? e.message : "";
  if (error instanceof HoldLifecycleRefusal) return error.code;
  if (state === "RH042") return stage === "install" ? "hold_pending_batch" : "hold_refused";
  if (state === "40001") return "hold_isolation";
  if (state === "55P03" || state === "57014") return "hold_lock_timeout";
  if (state === "55000") {
    if (msg.includes("slot not applied")) return "hold_slot_not_applied";
    if (msg.includes("already live")) return "hold_already_live";
    if (msg.includes("request reused")) return "hold_request_reused";
    if (msg.includes("epoch stale")) return "hold_epoch_stale";
    if (msg.includes("transition refused")) return "hold_transition_refused";
    if (msg.includes("already bound")) return "hold_already_bound";
    if (msg.includes("not between install and seal")) return "hold_full_order";
    if (msg.includes("is not the sealed FULL")) return "hold_bind_evidence";
    if (msg.includes("needs the sealed epoch")) return "hold_bind_not_sealed";
    if (msg.includes("does not cover")) return "hold_history_not_covering";
    if (msg.includes("not armed")) return "hold_history_not_recording";
    if (msg.includes("already released")) return "hold_already_released";
    return "hold_refused";
  }
  if (state === "22023") return "hold_input";
  return `hold_${stage}_failed`;
}

// ---- per-org work ---------------------------------------------------------------------------------------------

export type HoldState = "absent" | "released" | "held" | "sealed" | "marker_attempted";
const stateOf = (r: { hold: LiveHold | null; released: boolean }): HoldState => r.hold ? r.hold.state : r.released ? "released" : "absent";
const statusRows = (q: Q, org: string) => q(holdSql.status(org).text, holdSql.status(org).params);
const countOf = async (q: Q, text: string, org: string) => {
  const n = (await q(text, [org]))[0]?.n;
  if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 0) refuse("hold_unreadable");
  return n as number;
};

export interface HoldOrgResult {
  org_hash: string;
  epoch_hash: string | null;
  hold: HoldState | "not_read";
  pending_batches: number | null;
  canonical_committed: number | null;
  /** Unexpired seed captures (null when not read). */
  seed_captures: number | null;
  /**
   * Canonical fan-out quiescence for the org: `unproven` until a reconciliation or drain barrier exists (gap B,
   * Pipeline a518c23033a8). An install is refused while unproven.
   */
  fanout: "unproven" | "proven" | "not_read";
  verdict: "ready" | "installed" | "sealed" | "rolled_back" | "refused" | "not_in_wave";
  refused: string | null;
}
export type HoldStep = "dry" | "install" | "seal" | "rollback";

export interface HoldRunDeps {
  step: HoldStep;
  orgs: readonly string[];
  sha: string;
  /** install, seal, rollback: the DRY run (the request id's seed, and the approval). */
  dryRun?: string;
  /**
   * install: the DRY-ready orgs' hashes (epochHash null). seal and rollback: the approved orgs with the epoch hash the
   * DRY recorded (any other scope org: not_in_wave).
   */
  approved?: readonly { orgHash: string; epochHash: string | null }[];
  /** seal: the bound FULL backfill-execute (its run id, its GitHub created_at in ms, sha256 of its receipt's exact bytes). */
  full?: { run: string; startedMs: number; evidence: string };
  /** rollback: sha256 of the DRY receipt's exact bytes (the release's evidence). */
  evidence?: string;
  withSession<T>(fn: (s: HoldSqlSession) => Promise<T>): Promise<T>;
  now(): number;
  concurrency?: number;
  /**
   * A SEAM for a future proof that the org's canonical fan-out is quiescent (gap B). Production has no provider and no
   * input can supply one, so every install is refused `hold_fanout_unproven`. Synthetic fixtures only.
   */
  fanoutProof?: (org: string, q: Q) => Promise<boolean>;
}

/** One step over the wave; every org gets its own verdict. Throws (a fixed code) only on wave-level input errors. */
export async function holdRun(d: HoldRunDeps) {
  const orgs = waveOrgList([...d.orgs]);
  if (!SHA40.test(d.sha)) refuse("hold_input");
  const execute = d.step !== "dry";
  if (execute !== (d.dryRun !== undefined && d.approved !== undefined)) refuse("hold_input");
  if (execute && (!RUN_ID.test(d.dryRun!) || d.approved!.length === 0 || d.approved!.length > orgs.length ||
      new Set(d.approved!.map((a) => a.orgHash)).size !== d.approved!.length || !d.approved!.every((a) => HASH.test(a.orgHash) &&
        (d.step === "install" ? a.epochHash === null : typeof a.epochHash === "string" && HASH.test(a.epochHash)))))
    refuse("hold_input");
  if ((d.step === "seal") !== (d.full !== undefined) || (d.full && (!RUN_ID.test(d.full.run) || !Number.isSafeInteger(d.full.startedMs) || d.full.startedMs < 1 ||
      !HEX64.test(d.full.evidence)))) refuse("hold_input");
  if ((d.step === "rollback") !== (d.evidence !== undefined) || (d.evidence !== undefined && !HEX64.test(d.evidence))) refuse("hold_input");
  const hashOf = new Map(orgs.map((o) => [o, evidenceHash("org", o)]));
  if (execute && !d.approved!.every((a) => [...hashOf.values()].includes(a.orgHash))) refuse("hold_input");
  const approved = new Map((d.approved ?? []).map((a) => [a.orgHash, a.epochHash]));

  const settled = await runWave(orgs, d.concurrency ?? HOLD_LIFECYCLE_CONCURRENCY, async (org): Promise<HoldOrgResult> => {
    const r: HoldOrgResult = { org_hash: hashOf.get(org)!, epoch_hash: null, hold: "not_read", pending_batches: null, canonical_committed: null, seed_captures: null,
      fanout: "not_read", verdict: "refused", refused: null };
    const fanoutState = async (q: Q) => (d.fanoutProof !== undefined && (await d.fanoutProof(org, q)) === true ? "proven" : "unproven") as HoldOrgResult["fanout"];
    const stop = (code: string) => { r.refused = code; r.verdict = "refused"; return r; };
    if (execute && !approved.has(r.org_hash)) { r.verdict = "not_in_wave"; r.refused = "not_in_wave"; return r; }
    return d.withSession(async (s) => {
      if (d.step === "dry") {
        try {
          const live = parseLiveHold(await statusRows(s.read, org));
          r.hold = stateOf(live);
          r.epoch_hash = live.hold ? epochHash(live.hold.epoch) : null;
          r.pending_batches = await countOf(s.read, PENDING_BATCHES_SQL, org);
          r.canonical_committed = await countOf(s.read, CANONICAL_COMMITTED_SQL, org);
          r.seed_captures = await countOf(s.read, SEED_CAPTURES_ACTIVE_SQL, org);
          r.fanout = await fanoutState(s.read);
        } catch (error) { return stop(error instanceof HoldLifecycleRefusal ? error.code : holdSqlCode(error, "read")); }
        // Ready means: ready for the NEXT step from this state (install from none/released; seal from held; rollback
        // from held/sealed without an attempt). The execute re-checks everything in its own transaction.
        if (r.hold === "marker_attempted") return stop("hold_marker_attempted");
        if (r.hold === "absent" || r.hold === "released") {
          if (r.pending_batches! > 0) return stop("hold_pending_batch");
          if (r.canonical_committed! > 0) return stop("hold_canonical_history");
          if (r.seed_captures! > 0) return stop("hold_seed_capture_active");
          if (r.fanout !== "proven") return stop("hold_fanout_unproven");
        }
        r.verdict = "ready";
        return r;
      }
      try {
        return await s.tx(async (q) => {
          const live = parseLiveHold(await statusRows(q, org));
          r.hold = stateOf(live);
          if (d.step === "install") {
            // Fan-out quiescence first: unproven refuses before _install (nothing is written).
            r.fanout = await fanoutState(q);
            if (r.fanout !== "proven") refuse("hold_fanout_unproven");
            const req = installRequestId(d.sha, d.dryRun!, org);
            const epoch = (await q(holdSql.install(org, req).text, holdSql.install(org, req).params))[0]?.epoch;
            if (typeof epoch !== "string" || !ORG_UUID.test(epoch)) refuse("hold_install_failed");
            // Under the key _install took: the live hold must be exactly the one this request installed, `held` (a retry
            // of an old request returns the same epoch whatever state it has advanced to: never report that as held).
            const after = parseLiveHold(await statusRows(q, org));
            r.hold = stateOf(after);
            if (after.hold) r.epoch_hash = epochHash(after.hold.epoch);
            if (!after.hold || after.hold.epoch !== epoch || after.hold.state !== "held") refuse("hold_epoch_advanced");
            // Canonical history refuses (rolls back) under the key: its fan-out cannot be shown complete. The count only ever
            // refuses; it never admits (the fan-out check above is what must pass).
            r.canonical_committed = await countOf(q, CANONICAL_COMMITTED_SQL, org);
            if (r.canonical_committed > 0) refuse("hold_canonical_history");
            // And no committed, unexpired seed capture (see SEED_CAPTURES_ACTIVE_SQL for what this does not exclude).
            r.seed_captures = await countOf(q, SEED_CAPTURES_ACTIVE_SQL, org);
            if (r.seed_captures > 0) refuse("hold_seed_capture_active");
            r.verdict = "installed";
            return r;
          }
          if (!live.hold) refuse(live.released ? "hold_released" : "hold_absent");
          const epoch = live.hold!.epoch;
          r.epoch_hash = epochHash(epoch);
          if (approved.get(r.org_hash) !== r.epoch_hash) refuse("hold_epoch_changed");
          if (d.step === "seal") {
            if (live.hold!.state !== "held" && live.hold!.state !== "sealed") refuse("hold_transition_refused");
            // 062340: installed_at < FULL start <= sealed_at (now); refused early, the SQL checks again.
            const installed = Date.parse(String((await statusRows(q, org)).map((x) => (x.status as { hold: { installed_at: string } }).hold.installed_at)[0]));
            if (!(Number.isFinite(installed) && installed < d.full!.startedMs && d.full!.startedMs <= d.now())) refuse("hold_full_order");
            await q(holdSql.seal(org, epoch, d.full!.evidence).text, holdSql.seal(org, epoch, d.full!.evidence).params);
            const b = holdBindSql(org, epoch, d.full!.run, d.full!.startedMs, d.full!.evidence);
            await q(b.text, b.params);
            r.hold = "sealed";
            r.verdict = "sealed";
            return r;
          }
          // rollback: before any marker attempt only (the SQL refuses it too, in this same transaction).
          if (live.hold!.state === "marker_attempted" || live.hold!.attemptedMs !== null) refuse("hold_marker_attempted");
          const rel = holdSql.release(org, epoch, "verified_rollback", d.evidence!, "wave_rollback", 0);
          await q(rel.text, rel.params);
          r.hold = "released";
          r.verdict = "rolled_back";
          return r;
        });
      } catch (error) {
        r.verdict = "refused";
        // An install that rolled back left no hold of its own; an advanced one keeps what it found.
        if (d.step === "install" && holdSqlCode(error, "install") !== "hold_epoch_advanced") { r.epoch_hash = null; r.hold = "not_read"; }
        return stop(holdSqlCode(error, d.step === "install" ? "install" : d.step === "seal" ? "seal" : "release"));
      }
    });
  }, { fallback: "hold_org_failed" });

  const per: HoldOrgResult[] = settled.map((x) => x.ok ? x.value : { org_hash: hashOf.get(x.org)!, epoch_hash: null, hold: "not_read", pending_batches: null,
    canonical_committed: null, seed_captures: null, fanout: "not_read", verdict: "refused", refused: x.code });
  const done = d.step === "dry" ? "ready" : d.step === "install" ? "installed" : d.step === "seal" ? "sealed" : "rolled_back";
  const n = per.filter((p) => p.verdict === done).length, inWave = execute ? d.approved!.length : orgs.length;
  return { mode: execute ? "execute" as const : "dry_run" as const, tool: "hold_lifecycle" as const, step: d.step, org_count: orgs.length,
    wave_org_count: inWave, done_count: n, complete: n === inWave, per_org: per };
}
export type HoldSummary = Awaited<ReturnType<typeof holdRun>>;

// ---- receipt schema ------------------------------------------------------------------------------------------

export const HOLD_ORG_REFUSALS = ["hold_input", "hold_unreadable", "hold_marker_attempted", "hold_pending_batch", "hold_absent",
  "hold_released", "hold_epoch_changed", "hold_transition_refused", "hold_full_order", "hold_refused", "hold_isolation", "hold_lock_timeout",
  "hold_slot_not_applied", "hold_already_live", "hold_request_reused", "hold_epoch_stale", "hold_already_bound", "hold_bind_evidence", "hold_bind_not_sealed",
  "hold_history_not_covering", "hold_history_not_recording", "hold_already_released", "hold_install_failed", "hold_seal_failed", "hold_release_failed",
  "hold_read_failed", "hold_org_failed", "not_in_wave", "hold_state_invalid", "hold_fanout_unproven", "hold_seed_capture_active", "hold_epoch_advanced",
  "hold_canonical_history"] as const;
const codes = (...c: readonly string[]): ReceiptSchema => ({ code: new Set(c) });
const nul = (s: ReceiptSchema): ReceiptSchema => ({ nullable: s });
export const HOLD_SUMMARY_SCHEMA: ReceiptSchema = { obj: {
  mode: codes("dry_run", "execute"), tool: codes("hold_lifecycle"), step: codes("dry", "install", "seal", "rollback"),
  org_count: "count", wave_org_count: "count", done_count: "count", complete: "bool",
  per_org: { list: { obj: {
    org_hash: "hash", epoch_hash: nul("hash"), hold: codes("absent", "released", "held", "sealed", "marker_attempted", "not_read"),
    pending_batches: nul("count"), canonical_committed: nul("count"), seed_captures: nul("count"), fanout: codes("unproven", "proven", "not_read"),
    verdict: codes("ready", "installed", "sealed", "rolled_back", "refused", "not_in_wave"), refused: nul(codes(...HOLD_ORG_REFUSALS)),
  } }, max: 64 },
} };
export const HOLD_TOOL_REFUSALS = ["usage", "orgs", "hold_execute_held", "stdin_too_large", "stdin_invalid", "db_url_required", "hold_input",
  "wave_orgs_empty", "wave_orgs_oversized", "wave_org_invalid", "wave_org_duplicate", "wave_concurrency_invalid", "internal_error"] as const;

/** The summary a hold receipt may carry: exact, the scope's org hashes each once, counts derived, step = the mode's. */
export function holdReceiptSummary(summary: unknown, ctx: { step: HoldStep; orgs: readonly string[] | null;
  approved?: readonly { orgHash: string; epochHash: string | null }[] }): { summary: unknown; ok: boolean } {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const o = summary as any;
  if (o && typeof o === "object" && !Array.isArray(o) && Object.keys(o).length === 1 && Object.hasOwn(o, "refused"))
    return { summary: { refused: (HOLD_TOOL_REFUSALS as readonly string[]).includes(o.refused) ? o.refused : "internal_error" }, ok: false };
  const bad = { summary: { refused: "backlog_ops_receipt_schema" }, ok: false };
  if (!conforms(summary, HOLD_SUMMARY_SCHEMA) || o.step !== ctx.step || o.mode !== (ctx.step === "dry" ? "dry_run" : "execute") || !ctx.orgs) return bad;
  const want = ctx.orgs.map((x) => evidenceHash("org", x)).sort(), got = (o.per_org as HoldOrgResult[]).map((p) => p.org_hash).sort();
  if (got.length !== want.length || !got.every((h, i) => h === want[i]) || o.org_count !== want.length) return bad;
  const per = o.per_org as HoldOrgResult[];
  const done = ctx.step === "dry" ? "ready" : ctx.step === "install" ? "installed" : ctx.step === "seal" ? "sealed" : "rolled_back";
  // The in-wave partition is DERIVED, never taken from the receipt: a dry's wave is the scope; an execute's is every row
  // that is not not_in_wave. Its count, the done count and `complete` must equal what the rows say.
  const inWave = ctx.step === "dry" ? per : per.filter((p) => p.verdict !== "not_in_wave");
  const n = inWave.filter((p) => p.verdict === done).length;
  if (ctx.step === "dry" && per.some((p) => p.verdict === "not_in_wave")) return bad;
  if (o.wave_org_count !== inWave.length || o.done_count !== n || o.complete !== (inWave.length > 0 && n === inWave.length)) return bad;
  // An execute's in-wave rows are EXACTLY the bound hold-dry's approved org hashes (set and count), each once.
  if (ctx.step !== "dry") {
    if (!ctx.approved || ctx.approved.length === 0) return bad;
    const want = ctx.approved.map((a) => a.orgHash).sort(), have = inWave.map((p) => p.org_hash).sort();
    if (new Set(want).size !== want.length || have.length !== want.length || !have.every((h, i) => h === want[i])) return bad;
  }
  if (per.some((p) => (p.verdict === done) !== (p.refused === null) && p.verdict !== "not_in_wave")) return bad;
  if (per.some((p) => p.verdict !== done && p.verdict !== "refused" && p.verdict !== "not_in_wave")) return bad;
  if (per.some((p) => p.verdict === "not_in_wave" && p.refused !== "not_in_wave")) return bad;
  // Every SUCCESS row must agree with its own fields: the verdict is never trusted alone (Pipeline 97fcda3af1ad).
  if (!per.filter((p) => p.verdict === done).every((p) => successRowConsistent(ctx.step, p, ctx.approved))) return bad;
  // A dry's refused row carries a code its own fields support (d5 cac350e4353c).
  if (ctx.step === "dry" && !per.filter((p) => p.verdict === "refused").every(dryRefusalConsistent)) return bad;
  // A dry succeeds with one ready org; an execute only when EVERY in-wave org completed (a refused in-wave row fails it).
  return { summary, ok: ctx.step === "dry" ? n > 0 : o.complete === true && inWave.every((p) => p.verdict === done) };
}

/**
 * A success row is consistent with the fields it reports:
 *   dry `ready`, no live hold (absent, released): fan-out proven, pending, canonical and seed counts all present and 0,
 *     no epoch (the dry's own readiness rule);
 *   dry `ready`, a live hold (held, sealed): an epoch hash and every count present; never marker_attempted or unread;
 *   `installed`: held, with an epoch hash, approved without one (an install has none to approve);
 *   `sealed`: sealed, `rolled_back`: released, each at exactly the epoch hash the dry approved for that org.
 */
export function successRowConsistent(step: HoldStep, p: HoldOrgResult, approved?: readonly { orgHash: string; epochHash: string | null }[]): boolean {
  const counted = [p.pending_batches, p.canonical_committed, p.seed_captures].every((c) => typeof c === "number");
  const a = approved?.find((x) => x.orgHash === p.org_hash);
  if (step === "dry") {
    if (!counted || (p.fanout !== "proven" && p.fanout !== "unproven")) return false;
    if (p.hold === "absent" || p.hold === "released")
      return p.fanout === "proven" && p.pending_batches === 0 && p.canonical_committed === 0 && p.seed_captures === 0 && p.epoch_hash === null;
    return (p.hold === "held" || p.hold === "sealed") && typeof p.epoch_hash === "string";
  }
  if (!a) return false;
  if (step === "install") return p.hold === "held" && typeof p.epoch_hash === "string" && a.epochHash === null;
  if (step === "seal") return p.hold === "sealed" && typeof a.epochHash === "string" && p.epoch_hash === a.epochHash;
  return p.hold === "released" && typeof a.epochHash === "string" && p.epoch_hash === a.epochHash;
}

/** A dry refusal whose code is about a field must report that field accordingly; other codes (a read error) stand alone. */
export function dryRefusalConsistent(p: HoldOrgResult): boolean {
  switch (p.refused) {
    case "hold_pending_batch": return typeof p.pending_batches === "number" && p.pending_batches > 0;
    case "hold_canonical_history": return typeof p.canonical_committed === "number" && p.canonical_committed > 0;
    case "hold_seed_capture_active": return typeof p.seed_captures === "number" && p.seed_captures > 0;
    case "hold_fanout_unproven": return p.fanout === "unproven";
    case "hold_marker_attempted": return p.hold === "marker_attempted";
    default: return p.refused !== null;
  }
}

/**
 * A hold-dry receipt bound by an execute: the orgs that step may act on. install: the READY orgs with no live hold
 * (absent/released). seal: READY orgs whose hold is `held` or `sealed`, with their epoch hashes. rollback: READY orgs
 * `held` or `sealed`, with their epoch hashes.
 */
export function checkHoldDryReceipt(raw: string, orgs: readonly string[], step: Exclude<HoldStep, "dry">) {
  let r: Record<string, any> | null;
  try { r = JSON.parse(raw); } catch { r = null; }
  if (!r || !holdReceiptSummary(r.summary, { step: "dry", orgs }).ok) refuse("backlog_ops_bound_hold_dry");
  const per = (r!.summary.per_org as HoldOrgResult[]).filter((p) => p.verdict === "ready");
  const pick = step === "install" ? per.filter((p) => p.hold === "absent" || p.hold === "released").map((p) => ({ orgHash: p.org_hash, epochHash: null }))
    : per.filter((p) => (p.hold === "held" || p.hold === "sealed") && p.epoch_hash !== null).map((p) => ({ orgHash: p.org_hash, epochHash: p.epoch_hash as string }));
  if (pick.length === 0) refuse("backlog_ops_bound_hold_none");
  return pick;
}
