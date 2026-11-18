/**
 * 20261105062140 staged output facts: objects and grants, the caller / lease / phase answers, the
 * stage going with the run on every 062110 path (finish, abandon, the claim's restart on a changed
 * source, erase_scope) and coming back after a restart, the wholeness chain (a late or gapped stage
 * is never reconciled), the run's own scope, and the checksum-pinned down. Staged-equals-whole is
 * output-facts-parity.test.ts.
 */
import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { plan, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { CONSUMER_RUNTIME_FUNCTIONS, RUNTIME_APPLICATION_FUNCTIONS } from "../../hoopdb/migrate-guards.ts";
import { MIGRATION_SENTINELS } from "../../hoopdb/migration-sentinels.ts";
import { finishStagedOutputFacts, sealOutputFactsWant, StagedOutputFactsIncompleteError } from "../output-facts-store.ts";
import { StagedRunRestart } from "../staged-reconcile.ts";
import { outputFixture, OUTPUT_FIXTURE_SCOPE } from "./output-facts-fixture.ts";
import {
  advanceToFinal, as, chain, chunksOf, claim, clearFacts, code, factsChunk, factsPass, factsSnapshot, finalStep, keyOf, RUNTIME,
  stagedFactsDatabase, stagedPass, stagedRows, wantPhase, WANT_KEYS, wholePass, type Claimed,
} from "./staged-facts-harness.ts";

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const FILE = "20261105062140_staged_output_facts.sql";
const STEP = `upstream:${FILE}`;
const SQL = read(`supabase/migrations/${FILE}`);
const DOWN = read("supabase/rollbacks/20261105062140_staged_output_facts.down.sql");
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };

const WRITERS = [
  "sessions.staged_facts_stage_v1(bigint,uuid,jsonb,integer,boolean)",
  "sessions.staged_facts_seal_v1(bigint,uuid)",
  "sessions.staged_facts_read_v1(bigint,uuid,text,text,text[],bigint,integer)",
  "sessions.staged_facts_status_v1(bigint,uuid)",
  "sessions.staged_facts_reconcile_v1(bigint,uuid,text,text,text,text[],text,boolean,bigint)",
];
const HELPERS = ["sessions.staged_facts_run_v1(bigint,uuid,text[])", "sessions.staged_facts_whole_v1(bigint)", "sessions.staged_facts_reset_v1()"];
const OTHER_ROLES = ["anon", "authenticated", "service_role", "bulls96_read_api", "bulls96_session_ingest"];
const SCOPE = { orgId: OUTPUT_FIXTURE_SCOPE.orgId, workspaceId: OUTPUT_FIXTURE_SCOPE.workspaceId, harness: OUTPUT_FIXTURE_SCOPE.harness, sessionId: "s1", canonicalId: OUTPUT_FIXTURE_SCOPE.canonicalId };
const CHUNKS = chunksOf(outputFixture(), 4);
const LAST = CHUNKS.length - 1;

describe("20261105062140 staged output facts", () => {
  let db: PGlite;
  beforeAll(async () => {
    db = await stagedFactsDatabase();
    await db.exec("CREATE ROLE staged_facts_probe NOLOGIN; GRANT USAGE ON SCHEMA sessions TO staged_facts_probe");
  });
  afterAll(async () => {
    await db?.close();
  });
  const expire = () => db.exec("UPDATE sessions.search_reconcile_runs_v1 SET lease_expires_at = clock_timestamp() - interval '1 second'");
  /** A run with its want batches and the whole facts pass staged (the want phase not yet ended). */
  const stagedRun = async (manifest = "mf-1", version = 1): Promise<Claimed> => {
    const run = (await claim(db, SCOPE, manifest, version))!;
    await wantPhase(db, run, SCOPE, CHUNKS, version);
    return run;
  };
  const status = (run: Claimed) =>
    as(db, async (tx) => (await tx.query<{ whole: boolean }>("SELECT whole FROM sessions.staged_facts_status_v1($1::bigint,$2::uuid)", [run.runId, run.nonce])).rows[0]!.whole);

  test("objects: the sentinel, SECURITY DEFINER writers with a pinned search_path, owner-only helpers, RLS on with no policy and no table grant", async () => {
    expect(MIGRATION_SENTINELS[STEP]?.evidence).toContain("staged_facts_v1");
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(true);
    expect(SQL.split("\n").filter((l) => !l.trimStart().startsWith("--")).join("\n")).not.toMatch(/CONCURRENTLY|CREATE OR REPLACE/i);
    expect([...CONSUMER_RUNTIME_FUNCTIONS].filter((f) => f.startsWith("sessions.staged_facts_")).sort()).toEqual([...WRITERS].sort());
    expect(RUNTIME_APPLICATION_FUNCTIONS.filter((f) => f.startsWith("sessions.staged_facts_"))).toEqual([]);
    const fns = await db.query<{ sig: string; secdef: boolean; cfg: string[] | null; public_exec: boolean }>(
      `SELECT p.oid::regprocedure::text sig, p.prosecdef secdef, p.proconfig cfg,
              EXISTS (SELECT 1 FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a WHERE a.grantee = 0) public_exec
         FROM pg_proc p WHERE p.pronamespace = 'sessions'::regnamespace AND p.proname LIKE 'staged\\_facts\\_%'`);
    const norm = (s: string) => s.replace(/, /g, ",");
    expect(fns.rows.map((r) => norm(r.sig)).sort()).toEqual([...WRITERS, ...HELPERS].map(norm).sort());
    for (const r of fns.rows) {
      expect([r.sig, r.secdef, r.public_exec]).toEqual([r.sig, WRITERS.includes(norm(r.sig)), false]);
      expect(r.cfg?.some((c) => c.startsWith("search_path=pg_catalog"))).toBe(true);
    }
    for (const fn of WRITERS) {
      expect((await db.query<{ ok: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') ok", [RUNTIME, fn])).rows[0]!.ok).toBe(true);
      for (const role of OTHER_ROLES) expect([fn, role, (await db.query<{ ok: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') ok", [role, fn])).rows[0]!.ok]).toEqual([fn, role, false]);
    }
    for (const fn of HELPERS)
      for (const role of [RUNTIME, ...OTHER_ROLES]) expect((await db.query<{ ok: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') ok", [role, fn])).rows[0]!.ok).toBe(false);
    for (const table of ["sessions.staged_facts_v1", "sessions.staged_facts_runs_v1"]) {
      const rls = await db.query<{ rls: boolean; policies: number }>(
        "SELECT c.relrowsecurity rls, (SELECT count(*)::int FROM pg_policy p WHERE p.polrelid = c.oid) policies FROM pg_class c WHERE c.oid = $1::regclass", [table]);
      expect(rls.rows[0]).toEqual({ rls: true, policies: 0 });
      for (const role of [RUNTIME, ...OTHER_ROLES])
        for (const priv of ["SELECT", "INSERT", "UPDATE", "DELETE"])
          expect((await db.query<{ ok: boolean }>("SELECT has_table_privilege($1, $2, $3) ok", [role, table, priv])).rows[0]!.ok).toBe(false);
    }
    // On the 062110 tables: one reset trigger on the run table; no key references the stage table (its bulk deletes stay as they were).
    const triggers = async (table: string) => (await db.query<{ t: string }>(
      "SELECT tgname t FROM pg_trigger WHERE tgrelid = $1::regclass ORDER BY 1", [table])).rows.map((r) => r.t);
    expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_constraint WHERE confrelid = 'sessions.search_reconcile_stage_v1'::regclass")).rows[0]!.n).toBe(0);
    expect((await triggers("sessions.search_reconcile_runs_v1")).filter((t) => !t.startsWith("RI_ConstraintTrigger"))).toEqual(["staged_facts_reset"]);
    expect(await code(as(db, (tx) => tx.query("SELECT 1 FROM sessions.staged_facts_v1")))).toBe("42501");
  });

  test("the caller (42501), a lost lease (RH062), the phase (55000) and invalid input (22023)", async () => {
    await clearFacts(db);
    const run = (await claim(db, SCOPE))!;
    const stage = (tx: Parameters<Parameters<typeof as>[1]>[0], r: { runId: string; nonce: string }, entries: unknown, seq: number | null, last: boolean | null) =>
      tx.query("SELECT sessions.staged_facts_stage_v1($1::bigint,$2::uuid,$3::jsonb,$4::integer,$5::boolean)", [r.runId, r.nonce, JSON.stringify(entries), seq, last]);
    // Another role: refused before anything (as the 062110 writers refuse it).
    await db.exec(`GRANT EXECUTE ON FUNCTION ${WRITERS[0]} TO staged_facts_probe`);
    expect(await code(as(db, (tx) => stage(tx, run, [], 0, false), "staged_facts_probe"))).toBe("42501");
    await db.exec(`REVOKE EXECUTE ON FUNCTION ${WRITERS[0]} FROM staged_facts_probe`);
    // Another nonce, or no such run: RH062.
    expect(await code(as(db, (tx) => stage(tx, { ...run, nonce: crypto.randomUUID() }, [], 0, false)))).toBe("RH062");
    expect(await code(as(db, (tx) => stage(tx, { ...run, runId: "999999" }, [], 0, false)))).toBe("RH062");
    // Want phase: a chunk number and its last flag, both given (a facts chunk is one call), never negative; entries well formed.
    expect(await code(as(db, (tx) => stage(tx, run, [], null, null)))).toBe("22023");
    expect(await code(as(db, (tx) => stage(tx, run, [], 0, null)))).toBe("22023");
    expect(await code(as(db, (tx) => stage(tx, run, [], null, true)))).toBe("22023");
    expect(await code(as(db, (tx) => stage(tx, run, [], -1, false)))).toBe("22023");
    expect(await code(as(db, (tx) => stage(tx, run, [{ k: "bogus", i: "x", b: "{}" }], 0, false)))).toBe("22023");
    expect(await code(as(db, (tx) => stage(tx, run, [{ k: "note", b: "{}" }], 0, false)))).toBe("22023");
    expect(await code(as(db, (tx) => stage(tx, run, [{ k: "event", b: JSON.stringify({ ts: "2026-11-05T10:00:00Z" }) }], 0, false)))).toBe("22023");
    expect(await code(as(db, (tx) => stage(tx, run, [{ k: "note", i: "x", b: "[]" }], 0, false)))).toBe("22023");
    // Read, status and reconcile only in the final phase; seal only in want or have.
    for (const sql of [
      "SELECT * FROM sessions.staged_facts_read_v1($1::bigint,$2::uuid,'note',NULL,NULL,NULL,10)",
      "SELECT * FROM sessions.staged_facts_status_v1($1::bigint,$2::uuid)",
      "SELECT * FROM sessions.staged_facts_reconcile_v1($1::bigint,$2::uuid,'org-f4','ws-f4','codex',ARRAY['s1','s1-canonical'],'s1-canonical',true,1)",
    ]) expect(await code(as(db, (tx) => tx.query(sql, [run.runId, run.nonce])))).toBe("55000");
    await advanceToFinal(db, run, false);
    expect(await code(as(db, (tx) => sealOutputFactsWant(tx, run)))).toBe("55000");
    // Final phase: no chunk number; the read's bounds; the run's own scope only.
    expect(await code(as(db, (tx) => stage(tx, run, [], 0, false)))).toBe("22023");
    expect(await code(as(db, (tx) => tx.query("SELECT * FROM sessions.staged_facts_read_v1($1::bigint,$2::uuid,'note',NULL,NULL,NULL,0)", [run.runId, run.nonce])))).toBe("22023");
    expect(await code(as(db, (tx) => tx.query("SELECT * FROM sessions.staged_facts_read_v1($1::bigint,$2::uuid,'call',NULL,ARRAY['x'],NULL,10)", [run.runId, run.nonce])))).toBe("22023");
    for (const [org, ws, harness, ids, canonical] of [
      ["org-x", "ws-f4", "codex", ["s1", "s1-canonical"], "s1-canonical"],
      ["org-f4", "ws-x", "codex", ["s1", "s1-canonical"], "s1-canonical"],
      ["org-f4", "ws-f4", "claude", ["s1", "s1-canonical"], "s1-canonical"],
      ["org-f4", "ws-f4", "codex", ["s1", "other"], "other"],
      ["org-f4", "ws-f4", "codex", ["s1-canonical"], "s1-canonical"],
    ] as const)
      expect(await code(as(db, (tx) => tx.query("SELECT * FROM sessions.staged_facts_reconcile_v1($1::bigint,$2::uuid,$3,$4,$5,$6::text[],$7,true,1)",
        [run.runId, run.nonce, org, ws, harness, ids, canonical])))).toBe("22023");
    await as(db, (tx) => tx.query("SELECT sessions.search_reconcile_finish_v1($1::bigint,$2::uuid)", [run.runId, run.nonce]));
  });

  test("the stage goes with the run on every 062110 path: finish, abandon, a restart on a changed source, erase_scope", async () => {
    await clearFacts(db);
    const chained = async () => (await chain(db)).length;
    // finish (after the facts step)
    let run = await stagedRun();
    expect([await stagedRows(db) > 0, await chained()]).toEqual([true, 1]);
    await advanceToFinal(db, run, true);
    await finalStep(db, run, SCOPE, { sourceComplete: true, sourceVersion: 1 });
    expect([await stagedRows(db), await chained()]).toEqual([0, 0]);
    // abandon (the run row stays, back at want)
    run = await stagedRun();
    await as(db, (tx) => tx.query("SELECT sessions.search_reconcile_abandon_v1($1::bigint,$2::uuid,'test')", [run.runId, run.nonce]));
    expect([await stagedRows(db), await chained()]).toEqual([0, 0]);
    expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.search_reconcile_runs_v1")).rows[0]!.n).toBe(1);
    // a claim restart: the same run row, another source stamp
    await clearFacts(db);
    run = await stagedRun("mf-1");
    await expire();
    const restarted = (await claim(db, SCOPE, "mf-2"))!;
    expect([restarted.runId, restarted.restarted, restarted.phase, restarted.cursor]).toEqual([run.runId, true, "want", null]);
    expect([await stagedRows(db), await chained()]).toEqual([0, 0]);
    // ... and the restarted attempt's first chunk starts a new chain, whole from there.
    await wantPhase(db, restarted, SCOPE, null, 1);
    await factsChunk(db, restarted, SCOPE, CHUNKS[0]!, { seq: 0, last: false }, 1);
    expect(await chain(db)).toEqual([{ whole: true, seq: 0, done: false, sealed: false }]);
    expect(await stagedRows(db)).toBeGreaterThan(0);
    // erase_scope (by the canonical id), leased or not
    await as(db, (tx) => tx.query("SELECT sessions.search_reconcile_erase_scope_v1($1,$2,$3,$4::text[])", [SCOPE.orgId, SCOPE.workspaceId, SCOPE.harness, [SCOPE.canonicalId]]));
    expect([await stagedRows(db), await chained()]).toEqual([0, 0]);
    expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.search_reconcile_runs_v1")).rows[0]!.n).toBe(0);
  });

  test("the facts stage never touches the 062110 stage table or its diff pages", async () => {
    await clearFacts(db);
    const run = await stagedRun();
    await advanceToFinal(db, run, true);
    const page = (mode: string) => as(db, async (tx) => (await tx.query<{ key_hash: string }>(
      "SELECT key_hash FROM sessions.search_reconcile_diff_page_v1($1::bigint,$2::uuid,$3,NULL,1000,'bf')", [run.runId, run.nonce, mode])).rows.map((r) => r.key_hash));
    expect(await page("insert")).toEqual(Array.from({ length: WANT_KEYS }, (_v, i) => keyOf(i)));
    expect(await page("delete")).toEqual([]);
    await finalStep(db, run, SCOPE, { sourceComplete: true, sourceVersion: 1 });
  });

  describe("wholeness: only a stage holding the attempt's whole facts pass is reconciled", () => {
    const finishFails = async (run: Claimed) => {
      const before = await factsSnapshot(db);
      const err = await as(db, (tx) => finishStagedOutputFacts(tx, run, SCOPE, { sourceComplete: true, sourceVersion: 2, at: new Date() })).then(() => null, (e: unknown) => e);
      expect(err).toBeInstanceOf(StagedOutputFactsIncompleteError);
      expect(err).toBeInstanceOf(StagedRunRestart);
      expect((err as StagedRunRestart).reason).toBe("output_facts_stage_incomplete");
      // Even called directly, the reconcile writes nothing for this stage.
      const r = await as(db, async (tx) => (await tx.query<{ applied: boolean; inserted: number; deleted: number }>(
        "SELECT applied, inserted::int, deleted::int FROM sessions.staged_facts_reconcile_v1($1::bigint,$2::uuid,'org-f4','ws-f4','codex',ARRAY['s1','s1-canonical'],'s1-canonical',true,2)",
        [run.runId, run.nonce])).rows[0]);
      expect(r).toEqual({ applied: false, inserted: 0, deleted: 0 });
      expect(await factsSnapshot(db)).toEqual(before);
      await as(db, (tx) => tx.query("SELECT sessions.search_reconcile_abandon_v1($1::bigint,$2::uuid,'output_facts_stage_incomplete')", [run.runId, run.nonce]));
      await db.exec("DELETE FROM sessions.search_reconcile_runs_v1");
    };
    /** Facts already recorded by an earlier pass; then a version-2 run with its want batches. */
    const seeded = async () => {
      await clearFacts(db);
      await wholePass(db, SCOPE, outputFixture(), 1);
      expect((await factsSnapshot(db)).facts.length).toBeGreaterThan(0);
      const run = (await claim(db, SCOPE, "mf-2", 2))!;
      await wantPhase(db, run, SCOPE, null, 2);
      return run;
    };

    test("whole: chunks 0..last in order, then the seal", async () => {
      await clearFacts(db);
      const run = await stagedRun();
      await advanceToFinal(db, run, true);
      expect(await chain(db)).toEqual([{ whole: true, seq: LAST, done: true, sealed: true }]);
      expect(await status(run)).toBe(true);
      await finalStep(db, run, SCOPE, { sourceComplete: true, sourceVersion: 1 });
    });

    test("facts turned on mid-pass (no chunk 0): never reconciled, nothing deleted", async () => {
      const run = await seeded();
      await factsPass(db, run, SCOPE, CHUNKS, 2, { from: 1 });
      await advanceToFinal(db, run, true);
      expect(await chain(db)).toEqual([{ whole: false, seq: LAST, done: true, sealed: true }]);
      await finishFails(run);
    });

    test("a gap in the chunk numbers: never reconciled", async () => {
      const run = await seeded();
      await factsPass(db, run, SCOPE, CHUNKS, 2, { skip: [1] });
      await advanceToFinal(db, run, true);
      expect((await chain(db))[0]!.whole).toBe(false);
      await finishFails(run);
    });

    test("the last chunk never came (sealed before it): never reconciled", async () => {
      const run = await seeded();
      await factsPass(db, run, SCOPE, CHUNKS, 2, { skip: [LAST] });
      await advanceToFinal(db, run, true);
      expect(await chain(db)).toEqual([{ whole: false, seq: LAST - 1, done: false, sealed: true }]);
      await finishFails(run);
    });

    test("a budget or deadline stop inside the facts pass (chunks 0..n, no last chunk, no seal): never reconciled, facts untouched", async () => {
      const run = await seeded();
      await factsPass(db, run, SCOPE, CHUNKS, 2, { skip: [LAST] });
      await advanceToFinal(db, run, false);
      expect(await chain(db)).toEqual([{ whole: true, seq: LAST - 1, done: false, sealed: false }]);
      await finishFails(run);
    });

    test("the seal is lease-checked: a lost lease is RH062 and seals nothing", async () => {
      const run = await seeded();
      await factsPass(db, run, SCOPE, CHUNKS, 2);
      await expire();
      const taken = (await claim(db, SCOPE, "mf-2", 2))!;
      expect(taken.runId).toBe(run.runId);
      expect(await code(as(db, (tx) => sealOutputFactsWant(tx, run)))).toBe("RH062");
      expect((await chain(db))[0]!.sealed).toBe(false);
      await db.exec("DELETE FROM sessions.search_reconcile_runs_v1");
    });

    test("a chunk after the last one, or after the seal: never reconciled", async () => {
      let run = await seeded();
      await factsPass(db, run, SCOPE, CHUNKS, 2);
      await factsChunk(db, run, SCOPE, { projectionRows: [], records: [] }, { seq: LAST + 1, last: true }, 2);
      await advanceToFinal(db, run, true);
      expect((await chain(db))[0]!.whole).toBe(false);
      await finishFails(run);
      run = await seeded();
      await factsPass(db, run, SCOPE, CHUNKS, 2);
      await advanceToFinal(db, run, true, { stopAt: "have" });
      expect(await code(factsChunk(db, run, SCOPE, CHUNKS[0]!, { seq: 0, last: true }, 2))).toBe("55000"); // only in the want phase
      await as(db, (tx) => tx.query("SELECT sessions.search_reconcile_advance_v1($1::bigint,$2::uuid,'write',NULL,NULL,0,0,false)", [run.runId, run.nonce]));
      await as(db, (tx) => tx.query("SELECT sessions.search_reconcile_advance_v1($1::bigint,$2::uuid,'final',NULL,NULL,0,0,false)", [run.runId, run.nonce]));
      expect(await status(run)).toBe(true);
      await finalStep(db, run, SCOPE, { sourceComplete: true, sourceVersion: 2 });
    });

    test("facts on only after the want phase (no chain), or a seal with no chunk staged: never reconciled", async () => {
      let run = await seeded();
      await advanceToFinal(db, run, false);
      expect(await chain(db)).toEqual([]);
      await finishFails(run);
      run = await seeded();
      await advanceToFinal(db, run, true);
      expect(await chain(db)).toEqual([{ whole: false, seq: null, done: false, sealed: true }]);
      await finishFails(run);
    });

    test("not sealed (the want phase ended without onWantDone): never reconciled", async () => {
      const run = await seeded();
      await factsPass(db, run, SCOPE, CHUNKS, 2);
      await advanceToFinal(db, run, false);
      expect(await chain(db)).toEqual([{ whole: true, seq: LAST, done: true, sealed: false }]);
      await finishFails(run);
    });

    test("a kill mid facts pass: the resumed attempt's chunk 0 drops the partial stage, and the run reconciles to the whole path's rows", async () => {
      await clearFacts(db);
      const w = await wholePass(db, SCOPE, outputFixture(), 1);
      const ws = await factsSnapshot(db);
      await clearFacts(db);
      const run = (await claim(db, SCOPE, "mf-1", 1))!;
      await wantPhase(db, run, SCOPE, null, 1);
      await factsChunk(db, run, SCOPE, CHUNKS[0]!, { seq: 0, last: false }, 1);
      // A stray item the resumed pass will not stage again: chunk 0 must drop it.
      await factsChunk(db, run, SCOPE, { projectionRows: [{ table: "convergence_events", row: { ...CHUNKS[0]!.projectionRows[0]!.row, event_id: "stray" } }], records: [] }, { seq: 1, last: false }, 1);
      await expire();
      const taken = (await claim(db, SCOPE, "mf-1", 1))!;
      expect([taken.runId, taken.phase, taken.cursor]).toEqual([run.runId, "want", keyOf(WANT_KEYS - 1)]);
      await wantPhase(db, taken, SCOPE, CHUNKS, 1);
      await advanceToFinal(db, taken, true);
      const r = await finalStep(db, taken, SCOPE, { sourceComplete: true, sourceVersion: 1 });
      expect([r.applied, r.inserted, r.deleted, r.complete]).toEqual([true, w.inserted, w.deleted, true]);
      expect(await factsSnapshot(db)).toEqual(ws);
    });

    test("a kill after the seal: a run resumed in a later phase keeps its whole stage and reconciles", async () => {
      await clearFacts(db);
      const run = await stagedRun();
      await advanceToFinal(db, run, true, { stopAt: "write" });
      await expire();
      const taken = (await claim(db, SCOPE, "mf-1", 1))!;
      expect([taken.runId, taken.phase]).toEqual([run.runId, "write"]);
      await as(db, (tx) => tx.query("SELECT sessions.search_reconcile_advance_v1($1::bigint,$2::uuid,'final',NULL,NULL,0,0,false)", [taken.runId, taken.nonce]));
      const r = await finalStep(db, taken, SCOPE, { sourceComplete: true, sourceVersion: 1 });
      expect([r.applied, r.complete]).toEqual([true, true]);
    });

    test("a scope with no facts input (one empty chunk, last) is whole: the reconcile deletes what the scope no longer has", async () => {
      const run = await seeded();
      await factsChunk(db, run, SCOPE, { projectionRows: [], records: [] }, { seq: 0, last: true }, 2);
      await advanceToFinal(db, run, true);
      expect(await chain(db)).toEqual([{ whole: true, seq: 0, done: true, sealed: true }]);
      const r = await finalStep(db, run, SCOPE, { sourceComplete: true, sourceVersion: 2 });
      expect(r.applied).toBe(true);
      const snap = await factsSnapshot(db);
      expect([snap.events.length, snap.facts.length]).toEqual([0, 0]);
    });
  });

  test("a chunk seen again under its own number after the last one breaks the chain; items already staged are not staged twice", async () => {
    await clearFacts(db);
    const run = (await claim(db, SCOPE))!;
    await wantPhase(db, run, SCOPE, CHUNKS, 1);
    const n = await stagedRows(db);
    expect(await chain(db)).toEqual([{ whole: true, seq: LAST, done: true, sealed: false }]);
    const again = await factsChunk(db, run, SCOPE, CHUNKS[LAST]!, { seq: LAST, last: true }, 1);
    expect([again, await stagedRows(db)]).toEqual([0, n]);
    expect((await chain(db))[0]!.whole).toBe(false);
    await clearFacts(db);
    await stagedPass(db, SCOPE, outputFixture(), 1, 4);
  });
});

describe("20261105062140 rollback", () => {
  test("the checksum-pinned down refuses under a later step or any live staged lease, removes the trigger, the tables and the functions and its ledger row; a rerun refuses; the up applies again", async () => {
    expect(DOWN).toContain(createHash("sha256").update(SQL).digest("hex"));
    expect(DOWN).toContain("SET LOCAL lock_timeout='5s'");
    const db = await stagedFactsDatabase();
    try {
      const later = (await plan(LOCAL)).filter((s) => s.name.startsWith("upstream:") && s.name > STEP).reverse();
      if (later.length) {
        expect(await code(db.exec(DOWN))).toBe("55000");
        await db.exec("ROLLBACK");
      }
      for (const step of later) await db.exec(read(`supabase/rollbacks/${step.name.slice("upstream:".length).replace(/\.sql$/, ".down.sql")}`));
      // Any staged run under a live lease refuses the down, before its first facts chunk as after it; once expired, the down proceeds.
      const run = (await claim(db, SCOPE))!;
      await wantPhase(db, run, SCOPE, null, 1);
      expect(await code(db.exec(DOWN))).toBe("55000");
      await db.exec("ROLLBACK");
      await factsChunk(db, run, SCOPE, CHUNKS[0]!, { seq: 0, last: false }, 1);
      expect(await code(db.exec(DOWN))).toBe("55000");
      await db.exec("ROLLBACK");
      await db.exec("UPDATE sessions.search_reconcile_runs_v1 SET lease_expires_at = clock_timestamp() - interval '1 second'");
      await db.exec(DOWN);
      expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.staged_facts_v1')::text r")).rows[0]!.r).toBeNull();
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_proc WHERE pronamespace='sessions'::regnamespace AND proname LIKE 'staged\\_facts\\_%'")).rows[0]!.n).toBe(0);
      // No object of this step is left behind; the run and its 062110 stage rows are untouched.
      expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.staged_facts_runs_v1')::text r")).rows[0]!.r).toBeNull();
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_trigger WHERE tgname = 'staged_facts_reset'")).rows[0]!.n).toBe(0);
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.search_reconcile_runs_v1")).rows[0]!.n).toBe(1);
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.search_reconcile_stage_v1 WHERE side = 'want'")).rows[0]!.n).toBe(WANT_KEYS);
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [STEP])).rows[0]!.n).toBe(0);
      expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(false);
      expect(await code(db.exec(DOWN))).toBe("55000");
      await db.exec("ROLLBACK");
      // The up applies again cleanly after the down.
      await db.exec(SQL);
      expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.staged_facts_v1')::text r")).rows[0]!.r).toBe("sessions.staged_facts_v1");
    } finally {
      await db.close();
    }
  });
});
