/**
 * Test harness for the staged output facts (migration 20261105062140): a PGlite database with every
 * migration, a staged run driven through the 062110 writers exactly as staged-reconcile.ts drives
 * one (claim, want batches with their cursors, the want -> have advance with the seal, have, write,
 * the final transaction), with the facts hooks called where the wiring calls them, and the whole-set
 * path (postgresOutputFactsStore) as indexSearchScope's Postgres branch runs it.
 */
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { runMigrations, type MigrationClient, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { outputFactsFromScope, type OutputFactsScope } from "../output-facts.ts";
import {
  finishStagedOutputFacts,
  postgresOutputFactsStore,
  sealOutputFactsWant,
  stageOutputFactsChunk,
  type StagedFactsChunk,
  type StagedOutputFactsResult,
  type StagedOutputFactsScope,
} from "../output-facts-store.ts";
import type { StagedFactsEvents, StagedRunRef } from "../staged-reconcile.ts";
import type { RelayhistoryStreamRecord } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";

export const RUNTIME = "search_consumer_runtime";
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };
export const AT = new Date("2026-11-09T12:00:00Z");

type Row = { table: string; row: Record<string, unknown> };
export type Read = { projectionRows: Row[]; records: RelayhistoryStreamRecord[]; complete: boolean };

function client(db: PGlite): MigrationClient {
  return {
    async query<R>(text: string, params?: unknown[]) {
      if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
      const results = await db.exec(text);
      return { rows: (results.at(-1)?.rows ?? []) as R[] };
    },
  };
}

export async function stagedFactsDatabase(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await runMigrations(client(db), { profile: LOCAL });
  await db.exec(`GRANT USAGE ON SCHEMA sessions TO ${RUNTIME}`); // as session_consumer_role.sql does
  return db;
}

export const code = async (p: Promise<unknown>) => ((await p.then(() => null, (e: unknown) => e)) as { code?: string } | null)?.code ?? null;

/** One transaction as `role` (the consumer runtime by default). */
export const as = <T,>(db: PGlite, work: (tx: SqlExecutor) => Promise<T>, role = RUNTIME) =>
  db.transaction(async (tx) => {
    await tx.exec(`SET LOCAL ROLE ${role}`);
    return work({ query: async (text, params) => (await tx.query(text, params)) as never });
  });

/** factsScope (search-index.ts): the scope's session id, the canonical id, and every projected one. */
export function wholeScope(scope: StagedOutputFactsScope, read: Read): OutputFactsScope {
  const seen = read.projectionRows.filter((r) => r.table === "convergence_events" && typeof r.row.session_id === "string").map((r) => r.row.session_id as string);
  return { orgId: scope.orgId, workspaceId: scope.workspaceId, harness: scope.harness, sessionIds: [...new Set([scope.sessionId, scope.canonicalId, ...seen])], canonicalId: scope.canonicalId };
}

/** indexSearchScope's Postgres facts branch (the whole-set path), on one consumer transaction. */
export async function wholePass(db: PGlite, scope: StagedOutputFactsScope, read: Read, sourceVersion: number) {
  return as(db, async (tx) => {
    const store = postgresOutputFactsStore(tx);
    const fScope = wholeScope(scope, read);
    const prev = await store.previousState(fScope);
    const facts = outputFactsFromScope(fScope, read, sourceVersion);
    const range = facts.range ?? null;
    const r = await store.reconcile(fScope, { eventFacts: facts.events, outputFacts: facts.facts, complete: facts.complete, reasons: facts.reasons }, sourceVersion);
    if (r.applied) {
      const prior = prev.range;
      const span = prior && range ? { min: prior.min < range.min ? prior.min : range.min, max: prior.max > range.max ? prior.max : range.max } : (range ?? prior);
      await store.recordState(fScope, { complete: r.complete, at: AT, range: r.complete ? range : span, sourceVersion });
    }
    return r;
  });
}

/**
 * The facts pass of a read as the wiring delivers it: the whole read's facts input in chunks of `size`
 * projection rows, in source order, the records with the last chunk (or the first, `recordsFirst`).
 * A read with no input is one empty chunk.
 */
export function chunksOf(read: Read, size: number, recordsFirst = false): StagedFactsEvents[] {
  const n = Math.max(1, Math.ceil(read.projectionRows.length / size));
  const at = recordsFirst ? 0 : n - 1;
  return Array.from({ length: n }, (_, i) => ({ projectionRows: read.projectionRows.slice(i * size, (i + 1) * size), records: i === at ? read.records : [] }));
}

/** A want key in the base64 alphabet of streamIndexKey, ordered by `i`. */
export const keyOf = (i: number) => `K${String(i).padStart(6, "0")}`;
/** Want keys per run in these tests. */
export const WANT_KEYS = 3;

export interface Claimed extends StagedRunRef { phase: string; cursor: string | null; restarted: boolean }

export async function claim(db: PGlite, scope: StagedOutputFactsScope, manifest = "mf-1", epoch = 1): Promise<Claimed | null> {
  return as(db, async (tx) => {
    const { rows } = await tx.query<{ run_id: string; lease_nonce: string; phase: string; chunk_cursor: string | null; restarted: boolean }>(
      "SELECT run_id::text, lease_nonce::text, phase, chunk_cursor, restarted FROM sessions.search_reconcile_claim_v1($1,$2,$3,$4,$5,$6,$7,$8)",
      [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId, scope.canonicalId, epoch, manifest, 60_000]);
    const r = rows[0];
    return r ? { runId: r.run_id, nonce: r.lease_nonce, phase: r.phase, cursor: r.chunk_cursor, restarted: r.restarted } : null;
  });
}

/** One want batch as the module commits it (the 062110 stage, moving the cursor); facts take no part. */
export async function wantBatch(db: PGlite, run: StagedRunRef, scope: StagedOutputFactsScope, i: number) {
  return as(db, (tx) => tx.query("SELECT sessions.search_reconcile_stage_batch_v1($1::bigint,$2::uuid,'want',0::smallint,$3::jsonb,$4)",
    [run.runId, run.nonce, JSON.stringify([{ k: keyOf(i), o: JSON.stringify([scope.workspaceId, scope.sessionId]), t: 1000 + i, f: `fp-${i}` }]), keyOf(i)]));
}

/** One chunk of the facts pass, in its own transaction (onBatch phase "facts"). */
export async function factsChunk(db: PGlite, run: StagedRunRef, scope: StagedOutputFactsScope, events: StagedFactsEvents, chunk: StagedFactsChunk, sourceVersion: number) {
  return as(db, (tx) => stageOutputFactsChunk(tx, run, scope, events, chunk, sourceVersion));
}

/** The whole facts pass (chunks `from` onwards; `skip` leaves chunk numbers out, a gap). */
export async function factsPass(db: PGlite, run: StagedRunRef, scope: StagedOutputFactsScope, chunks: StagedFactsEvents[], sourceVersion: number, opts: { from?: number; skip?: number[] } = {}) {
  for (let i = opts.from ?? 0; i < chunks.length; i++)
    if (!opts.skip?.includes(i)) await factsChunk(db, run, scope, chunks[i]!, { seq: i, last: i === chunks.length - 1 }, sourceVersion);
}

/** The want -> have advance, with the seal (onWantDone) when facts are on; then have -> write -> final. */
export async function advanceToFinal(db: PGlite, run: StagedRunRef, facts: boolean, opts: { stopAt?: "have" | "write" } = {}) {
  const advance = (tx: SqlExecutor, phase: string) =>
    tx.query("SELECT sessions.search_reconcile_advance_v1($1::bigint,$2::uuid,$3,NULL,$4::text[],0,0,false)", [run.runId, run.nonce, phase, phase === "have" ? ["history"] : null]);
  await as(db, async (tx) => {
    await advance(tx, "have");
    if (facts) await sealOutputFactsWant(tx, run);
  });
  if (opts.stopAt === "have") return;
  await as(db, (tx) => advance(tx, "write"));
  if (opts.stopAt === "write") return;
  await as(db, (tx) => advance(tx, "final"));
}

/** The final transaction: the facts step (before finish_v1, which drops the stage), then finish_v1. */
export async function finalStep(db: PGlite, run: StagedRunRef, scope: StagedOutputFactsScope, pass: { sourceComplete: boolean; sourceVersion: number }): Promise<StagedOutputFactsResult> {
  return as(db, async (tx) => {
    const r = await finishStagedOutputFacts(tx, run, scope, { ...pass, at: AT });
    await tx.query("SELECT sessions.search_reconcile_finish_v1($1::bigint,$2::uuid)", [run.runId, run.nonce]);
    return r;
  });
}

/** The want batches, then the facts pass, of a fresh or resumed run (from its committed cursor). */
export async function wantPhase(db: PGlite, run: Claimed, scope: StagedOutputFactsScope, chunks: StagedFactsEvents[] | null, sourceVersion: number) {
  const from = run.cursor === null ? 0 : Number(run.cursor.slice(1)) + 1;
  for (let i = from; i < WANT_KEYS; i++) await wantBatch(db, run, scope, i);
  if (chunks) await factsPass(db, run, scope, chunks, sourceVersion);
}

/** A whole staged run from a fresh claim, facts on throughout. */
export async function stagedPass(db: PGlite, scope: StagedOutputFactsScope, read: Read, sourceVersion: number, size = 3, recordsFirst = false) {
  const run = await claim(db, scope, `mf-${sourceVersion}`, sourceVersion);
  if (!run) throw new Error("claim busy");
  await wantPhase(db, run, scope, chunksOf(read, size, recordsFirst), sourceVersion);
  await advanceToFinal(db, run, true);
  return finalStep(db, run, scope, { sourceComplete: read.complete, sourceVersion });
}

/** Every facts and state row, as comparable text. */
export async function factsSnapshot(db: PGlite) {
  const rows = async (table: string, order: string) =>
    (await db.query<{ j: string }>(`SELECT row_to_json(t)::text j FROM sessions.${table} t ORDER BY ${order}`)).rows.map((r) => r.j);
  return {
    events: await rows("session_event_facts", "org_id, workspace_id, row_id"),
    facts: await rows("session_output_facts", "org_id, workspace_id, row_id"),
    state: await rows("output_facts_session_state", "org_id, workspace_id, source, session_id"),
  };
}

export async function clearFacts(db: PGlite) {
  await db.exec(`DELETE FROM sessions.session_event_facts; DELETE FROM sessions.session_output_facts; DELETE FROM sessions.output_facts_session_state;
    DELETE FROM sessions.search_reconcile_runs_v1;`);
}

export const stagedRows = async (db: PGlite) => (await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.staged_facts_v1")).rows[0]!.n;
/** The runs' facts chains (sessions.staged_facts_runs_v1). */
export const chain = async (db: PGlite) =>
  (await db.query<{ whole: boolean; seq: number | null; done: boolean; sealed: boolean }>(
    "SELECT whole, seq, done, sealed FROM sessions.staged_facts_runs_v1 ORDER BY run_id")).rows.map((r) => ({ ...r }));
