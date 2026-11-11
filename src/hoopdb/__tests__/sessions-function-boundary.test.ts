/**
 * The unified sessions function-boundary generator (scripts/sync-sessions-function-boundary.ts):
 * deterministic output, --check catches drift, manifests must match their SQL, and no function
 * may be owned by two lanes.
 */
import { describe, expect, test } from "bun:test";
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { generate, ACL_PATH, GUARDS_PATH, type Reader } from "../../../scripts/sessions-function-boundary/generate.ts";
import { LANES } from "../../../scripts/sessions-function-boundary/lanes/index.ts";
import { ingestHistory } from "../../../scripts/sessions-function-boundary/lanes/ingest-history.ts";
import { bufferedGcSingleCandidate } from "../../../scripts/sessions-function-boundary/lanes/buffered-gc-single-candidate.ts";
import { lazySourceTables, originalBodies } from "../../../scripts/sessions-function-boundary/lanes/lazy-source-tables.ts";
import { lazyClaimClockNarrowing, originalBodies as originalClaimClockBodies } from "../../../scripts/sessions-function-boundary/lanes/lazy-claim-clock-narrowing.ts";
import { lazyAckClockNoKeyUpdate, originalBodies as originalAckClockBodies } from "../../../scripts/sessions-function-boundary/lanes/lazy-ack-clock-no-key-update.ts";
import { privacyAuthority, originalLazyBodies, originalMirrorBodies } from "../../../scripts/sessions-function-boundary/lanes/privacy-authority.ts";
import { bufferedSearchGateRetiredWitness, originalGateBody } from "../../../scripts/sessions-function-boundary/lanes/buffered-search-gate-retired-witness.ts";
import { createHash } from "node:crypto";
import { ingestGenerationOverlap, originalBodies as originalOverlapBodies } from "../../../scripts/sessions-function-boundary/lanes/ingest-generation-overlap.ts";
import { ingestClaimSessions } from "../../../scripts/sessions-function-boundary/lanes/ingest-claim-sessions.ts";
import { ingestClaimSessionsReader } from "../../../scripts/sessions-function-boundary/lanes/ingest-claim-sessions-reader.ts";
import type { LaneManifest } from "../../../scripts/sessions-function-boundary/types.ts";
import { VERIFIERS } from "../../../scripts/sessions-function-boundary/verifiers.ts";
import { run } from "../../../scripts/sync-sessions-function-boundary.ts";
import { BUFFERED_SEARCH_FEED_RUNTIME_ROLE } from "../../session-pipeline/buffered-feed-role.ts";
import { CONSUMER_RUNTIME_FUNCTIONS, CONSUMER_RUNTIME_FUNCTIONS_ROLE, RUNTIME_APPLICATION_FUNCTIONS, pendingExecuteGrants } from "../migrate-guards.ts";

const root = join(import.meta.dir, "../../..");
const reader = (base: string): Reader => async (p) => { try { return readFileSync(join(base, p), "utf8"); } catch { return undefined; } };
const repo = reader(root);
const OWNED = Object.keys(ingestHistory.functions);
const SAMPLE = OWNED.find((k) => ingestHistory.functions[k]!.grants.length)!;

/** A scratch checkout with exactly the files the generator reads and writes. */
function scratch(): string {
  const dir = mkdtempSync(join(tmpdir(), "sessions-boundary-"));
  for (const p of ["supabase/migrations", "supabase/rollbacks", "supabase/bootstrap", "src/hoopdb/sessions-function-acl.json", "src/hoopdb/migrate-guards.ts"]) {
    mkdirSync(dirname(join(dir, p)), { recursive: true });
    cpSync(join(root, p), join(dir, p), { recursive: true });
  }
  return dir;
}
const cli = (args: string[]) => Bun.spawnSync(["bun", ...args], { cwd: root, stdout: "pipe", stderr: "pipe" });

describe("unified sessions function-boundary generator", () => {
  test("the committed artifacts are exactly the generator's output (no drift)", async () => {
    expect((await run({ root, check: true })).drift).toEqual([]);
  });

  test("deterministic: two runs, and any lane order, give byte-identical outputs", async () => {
    const a = await generate(repo, LANES, VERIFIERS);
    const b = await generate(repo, LANES, VERIFIERS);
    const c = await generate(repo, [...LANES].reverse(), [...VERIFIERS].reverse());
    expect(b.outputs).toEqual(a.outputs);
    expect(c.outputs).toEqual(a.outputs);
    expect(Object.keys(a.outputs).sort()).toEqual([
      "src/hoopdb/migrate-guards.ts", "src/hoopdb/sessions-function-acl.json",
      "supabase/bootstrap/acquisition_installer_verify.sql",
      "supabase/bootstrap/babysitter_retention_verify.sql",
      "supabase/bootstrap/buffered_search_feed_runtime_verify.sql",
      "supabase/bootstrap/canonical_project_events_verify.sql",
      "supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql",
      "supabase/bootstrap/privacy_eraser_verify.sql",
      "supabase/bootstrap/session_directory_search_worker_verify.sql", "supabase/bootstrap/session_ingest_verify.sql",
      "supabase/bootstrap/session_read_api_role_verify.sql",
      "supabase/rollbacks/20261105026000_canonical_history_lineage.down.sql",
      "supabase/rollbacks/20261105033000_canonical_project_events.down.sql",
      "supabase/rollbacks/20261105039000_buffered_gc_single_candidate.down.sql",
      "supabase/rollbacks/20261105040000_ingest_lazy_source_tables.down.sql",
      "supabase/rollbacks/20261105041000_ingest_privacy_authority.down.sql",
      "supabase/rollbacks/20261105042000_babysitter_evidence_retention.down.sql",
      "supabase/rollbacks/20261105043500_lazy_claim_clock_narrowing.down.sql",
      "supabase/rollbacks/20261105043800_lazy_ack_clock_no_key_update.down.sql",
      "supabase/rollbacks/20261105043850_ingest_acquisition_owner.down.sql",
      "supabase/rollbacks/20261105043880_ingest_bookkeeping_cleanup.down.sql",
      "supabase/rollbacks/20261105043890_buffered_search_gate_retired_witness.down.sql",
      "supabase/rollbacks/20261105043910_selfhost_session_selection_erase.down.sql",
      "supabase/rollbacks/20261105044010_selfhost_erase_lock_fences.down.sql",
      "supabase/rollbacks/20261105044030_ingest_generation_overlap.down.sql",
      "supabase/rollbacks/20261105044040_buffered_stages_session_first.down.sql",
      "supabase/rollbacks/20261105044050_selfhost_erase_short_summary_provenance.down.sql",
      "supabase/rollbacks/20261105044070_selfhost_erase_search_text_rebuild.down.sql",
      "supabase/rollbacks/20261105044080_ingest_claim_sessions.down.sql",
      "supabase/rollbacks/20261105044090_claim_sessions_reader.down.sql",
      "supabase/rollbacks/20261105045010_buffered_search_feed_same_second.down.sql",
      "supabase/rollbacks/20261105045030_session_consumer_project_text_grant.down.sql",
      "supabase/rollbacks/20261105047010_buffered_exact_session_stages.down.sql",
      "supabase/rollbacks/20261105050000_operator_session_erase.down.sql",
      "supabase/rollbacks/20261105050010_operator_session_erase_fence_check.down.sql",
      "supabase/rollbacks/20261105050020_operator_session_erase_runtime.down.sql",
    ]);
  });

  test("the 044080 claim sessions lane adds two owner-only trigger functions, grants no runtime role anything, and derives its guarded rollback", async () => {
    const withLane = await generate(repo, LANES, VERIFIERS);
    const without = await generate(repo, LANES.filter((l) => l !== ingestClaimSessions), VERIFIERS);
    expect(withLane.summary.lanes["ingest-claim-sessions"]).toEqual({ functions: 2, replaces: 0, grants: 0 });
    for (const v of VERIFIERS) expect(withLane.outputs[v.path]).toBe(without.outputs[v.path]);
    const acl = JSON.parse(withLane.outputs["src/hoopdb/sessions-function-acl.json"]!).functions as Record<string, string[]>;
    const mine = Object.keys(acl).filter((k) => k.startsWith("sessions.ingest_claim_sessions_")).sort();
    expect(mine).toEqual(["sessions.ingest_claim_sessions_policy_guard()", "sessions.ingest_claim_sessions_sync()"]);
    for (const k of mine) expect(acl[k]).toEqual(["<owner>:EXECUTE"]);
    const down = withLane.outputs["supabase/rollbacks/20261105044080_ingest_claim_sessions.down.sql"]!;
    expect(down).toContain(createHash("sha256").update(readFileSync(join(root, ingestClaimSessions.migration!), "utf8")).digest("hex"));
    expect(down.indexOf("own applied checksum required")).toBeLessThan(down.indexOf("DROP TRIGGER ingest_claim_sessions_sync ON sessions.ingest_claim_attempts;"));
    expect(down).toContain("a later migration is applied");
    expect(down.trim().endsWith("COMMIT;")).toBe(true);
  });

  test("the 044090 claim sessions reader lane adds one owner-only trigger function, keeps the replaced ACLs, grants nothing, and derives its guarded rollback", async () => {
    const withLane = await generate(repo, LANES, VERIFIERS);
    const without = await generate(repo, LANES.filter((l) => l !== ingestClaimSessionsReader), VERIFIERS);
    expect(withLane.summary.lanes["ingest-claim-sessions-reader"]).toEqual({ functions: 1, replaces: 3, grants: 0 });
    for (const v of VERIFIERS) expect(withLane.outputs[v.path]).toBe(without.outputs[v.path]);
    const acl = JSON.parse(withLane.outputs["src/hoopdb/sessions-function-acl.json"]!).functions as Record<string, string[]>;
    const before = JSON.parse(without.outputs["src/hoopdb/sessions-function-acl.json"]!).functions as Record<string, string[]>;
    const mine = Object.keys(acl).filter((k) => k.startsWith("sessions.ingest_batch_claim_sessions_")).sort();
    expect(mine).toEqual(["sessions.ingest_batch_claim_sessions_refresh()"]);
    expect(acl["sessions.ingest_batch_claim_sessions_refresh()"]).toEqual(["<owner>:EXECUTE"]);
    // The replaced readers and guard keep exactly their prior ACLs (CREATE OR REPLACE, no GRANT).
    for (const k of Object.keys(ingestClaimSessionsReader.replaces)) expect([k, acl[k]]).toEqual([k, before[k]]);
    const down = withLane.outputs["supabase/rollbacks/20261105044090_claim_sessions_reader.down.sql"]!;
    expect(down).toContain(createHash("sha256").update(readFileSync(join(root, ingestClaimSessionsReader.migration!), "utf8")).digest("hex"));
    expect(down.indexOf("own applied checksum required")).toBeLessThan(down.indexOf("CREATE OR REPLACE FUNCTION sessions.buffered_sessions_for("));
    expect(down.indexOf("CREATE OR REPLACE FUNCTION sessions.ingest_claim_sessions_policy_guard()")).toBeLessThan(down.indexOf("DROP COLUMN read_enabled"));
    expect(down).toContain("DROP TRIGGER ingest_batch_claim_sessions_refresh ON sessions.ingest_batches;");
    expect(down).toContain("a later migration is applied");
    expect(down).not.toContain("read_enabled:=false");
    expect(down.trim().endsWith("COMMIT;")).toBe(true);
  });

  test("the 044030 generation overlap adds six owner-only functions, grants no runtime role anything, and derives its exact guarded rollback", async () => {
    const withLane = await generate(repo, LANES, VERIFIERS);
    const without = await generate(repo, LANES.filter((l) => l !== ingestGenerationOverlap), VERIFIERS);
    const downPath = "supabase/rollbacks/20261105044030_ingest_generation_overlap.down.sql";
    expect(withLane.summary.lanes["ingest-generation-overlap"]).toEqual({ functions: 6, replaces: 2, grants: 0 });
    // Every role verifier is byte-identical with and without the lane: no runtime capability changes.
    for (const v of VERIFIERS) expect(withLane.outputs[v.path]).toBe(without.outputs[v.path]);
    const acl = JSON.parse(withLane.outputs["src/hoopdb/sessions-function-acl.json"]!).functions as Record<string, string[]>;
    const mine = Object.keys(acl).filter((k) => k.startsWith("sessions.ingest_generation_overlap_")).sort();
    expect(mine).toEqual(["sessions.ingest_generation_overlap_admits(text,text,text)", "sessions.ingest_generation_overlap_close(jsonb)",
      "sessions.ingest_generation_overlap_immutable()", "sessions.ingest_generation_overlap_open(jsonb)",
      "sessions.ingest_generation_overlap_policy_retire()", "sessions.ingest_generation_overlap_retired_immutable()"]);
    for (const k of [...mine, "sessions.ingest_require_generation(text)", "sessions.ingest_lazy_guard(text)"]) expect(acl[k]).toEqual(["<owner>:EXECUTE"]);
    const down = withLane.outputs[downPath]!;
    expect(down).toContain(createHash("sha256").update(readFileSync(join(root, ingestGenerationOverlap.migration!), "utf8")).digest("hex"));
    const [generationBody, guardBody] = originalOverlapBodies(
      readFileSync(join(root, "supabase/migrations/20261104050000_buffered_upload_receipts.sql"), "utf8"),
      readFileSync(join(root, "supabase/migrations/20261104055000_lazy_current_custody.sql"), "utf8"));
    expect(generationBody).toContain("AND generation=p_expected AND status='pass' FOR SHARE;");
    expect(guardBody).toContain("IF v_gen IS NULL OR pol.generation<>v_gen");
    expect(down).toContain(generationBody!);
    expect(down).toContain(guardBody!);
    expect(down.indexOf("status='pending'")).toBeLessThan(down.indexOf(generationBody!));
    expect(down.indexOf(guardBody!)).toBeLessThan(down.indexOf("DROP TABLE sessions.ingest_generation_overlap_windows;"));
    expect(down).toContain("a later migration is applied");
    expect(down).toContain("own applied checksum required");
    expect(down.trim().endsWith("COMMIT;")).toBe(true);
  });

  test("the GC replacement changes no ACL/boundary/verifier capability and derives its exact guarded rollback", async () => {
    const withGc = await generate(repo, LANES, VERIFIERS);
    const withoutGc = await generate(repo, LANES.filter((l) => l !== bufferedGcSingleCandidate), VERIFIERS);
    const downPath = "supabase/rollbacks/20261105039000_buffered_gc_single_candidate.down.sql";
    for (const [path, value] of Object.entries(withoutGc.outputs)) expect(withGc.outputs[path]).toBe(value);
    expect(withGc.summary.lanes["buffered-gc-single-candidate"]).toEqual({ functions: 0, replaces: 1, grants: 0 });
    const down = withGc.outputs[downPath]!;
    const source = readFileSync(join(root, bufferedGcSingleCandidate.migration!), "utf8");
    expect(down).toContain(createHash("sha256").update(source).digest("hex"));
    const originalPath = bufferedGcSingleCandidate.replaces["sessions.ingest_buffer_gc_claim(jsonb)"]!.original;
    const original = readFileSync(join(root, originalPath), "utf8").match(/CREATE OR REPLACE FUNCTION sessions\.ingest_buffer_gc_claim\(p jsonb\)[\s\S]*?END \$claim\$;/)![0];
    expect(down).toContain(original);
    expect(down.indexOf("status='pending'")).toBeLessThan(down.indexOf(original));
    expect(down).toContain("pg_advisory_xact_lock(961996,1)");
    expect(down).toContain("a later migration is applied");
    expect(down).toContain("own applied checksum required");
    expect(down.trim().endsWith("COMMIT;")).toBe(true);
  });

  test("the 040000 lazy source-table replacement changes no ACL/boundary/verifier capability and derives its exact guarded rollback", async () => {
    const withLane = await generate(repo, LANES, VERIFIERS);
    const without = await generate(repo, LANES.filter((l) => l !== lazySourceTables), VERIFIERS);
    const downPath = "supabase/rollbacks/20261105040000_ingest_lazy_source_tables.down.sql";
    for (const [path, value] of Object.entries(without.outputs)) expect(withLane.outputs[path]).toBe(value);
    expect(withLane.summary.lanes["lazy-source-tables"]).toEqual({ functions: 0, replaces: 2, grants: 0 });
    const down = withLane.outputs[downPath]!;
    expect(down).toContain(createHash("sha256").update(readFileSync(join(root, lazySourceTables.migration!), "utf8")).digest("hex"));
    for (const body of originalBodies(readFileSync(join(root, "supabase/migrations/20261104055000_lazy_current_custody.sql"), "utf8"))) {
      expect(down).toContain(body);
      expect(down.indexOf("status='pending'")).toBeLessThan(down.indexOf(body));
    }
    expect(down).toContain("a later migration is applied");
    expect(down).toContain("own applied checksum required");
    expect(down.trim().endsWith("COMMIT;")).toBe(true);
  });

  test("the 043500 lazy claim clock narrowing changes no ACL/boundary/verifier capability and derives its exact guarded rollback", async () => {
    const withLane = await generate(repo, LANES, VERIFIERS);
    const without = await generate(repo, LANES.filter((l) => l !== lazyClaimClockNarrowing), VERIFIERS);
    const downPath = "supabase/rollbacks/20261105043500_lazy_claim_clock_narrowing.down.sql";
    for (const [path, value] of Object.entries(without.outputs)) expect(withLane.outputs[path]).toBe(value);
    expect(withLane.summary.lanes["lazy-claim-clock-narrowing"]).toEqual({ functions: 0, replaces: 2, grants: 0 });
    const down = withLane.outputs[downPath]!;
    expect(down).toContain(createHash("sha256").update(readFileSync(join(root, lazyClaimClockNarrowing.migration!), "utf8")).digest("hex"));
    const bodies = originalClaimClockBodies(readFileSync(join(root, "supabase/migrations/20261104055000_lazy_current_custody.sql"), "utf8"));
    expect(bodies).toHaveLength(2);
    for (const body of bodies) {
      expect(down).toContain(body);
      expect(down.indexOf("status='pending'")).toBeLessThan(down.indexOf(body));
    }
    expect(down).not.toContain("lazy-native-origin-v1");
    expect(down).toContain("pg_advisory_xact_lock(961996,1)");
    expect(down).toContain("a later migration is applied");
    expect(down).toContain("own applied checksum required");
    expect(down.trim().endsWith("COMMIT;")).toBe(true);
  });

  test("the 043800 staged-ACK clock NO KEY UPDATE changes no ACL/boundary/verifier capability and derives its exact guarded rollback", async () => {
    const withLane = await generate(repo, LANES, VERIFIERS);
    const without = await generate(repo, LANES.filter((l) => l !== lazyAckClockNoKeyUpdate), VERIFIERS);
    const downPath = "supabase/rollbacks/20261105043800_lazy_ack_clock_no_key_update.down.sql";
    for (const [path, value] of Object.entries(without.outputs)) expect(withLane.outputs[path]).toBe(value);
    expect(Object.keys(withLane.outputs).filter((p) => !(p in without.outputs))).toEqual([downPath]);
    expect(withLane.summary.boundarySHA).toBe(without.summary.boundarySHA);
    expect(withLane.summary.lanes["lazy-ack-clock-no-key-update"]).toEqual({ functions: 0, replaces: 1, grants: 0 });
    const down = withLane.outputs[downPath]!;
    expect(down).toContain(createHash("sha256").update(readFileSync(join(root, lazyAckClockNoKeyUpdate.migration!), "utf8")).digest("hex"));
    const bodies = originalAckClockBodies(readFileSync(join(root, "supabase/migrations/20261105043500_lazy_claim_clock_narrowing.sql"), "utf8"));
    expect(bodies).toHaveLength(1);
    expect(down).toContain(bodies[0]!);
    expect(down.indexOf("status='pending'")).toBeLessThan(down.indexOf(bodies[0]!));
    expect(down).toContain("WHERE singleton FOR UPDATE;v_last:=c.last_seat;");
    expect(down).not.toContain("FOR NO KEY UPDATE");
    expect(down).not.toContain("FUNCTION sessions.ingest_lazy_records"); // only the ACK is restored (its comment names the claim fn)
    expect(down).toContain("pg_advisory_xact_lock(961996,1)");
    expect(down).toContain("a later migration is applied");
    expect(down).toContain("own applied checksum required");
    expect(down.trim().endsWith("COMMIT;")).toBe(true);
  });

  test("041000 privacy authority: exactly two ingest-callable SECDEF functions; the rollback restores the 010000 mirror and the 040000 lazy bodies verbatim behind its guards", async () => {
    const g = await generate(repo, LANES, VERIFIERS);
    expect(g.summary.lanes["privacy-authority"]).toEqual({ functions: 17, replaces: 4, grants: 6 });
    const acl = JSON.parse(g.outputs[ACL_PATH]!).functions as Record<string, string[]>;
    const granted = Object.entries(acl).filter(([k, v]) => k.startsWith("sessions.ingest_privacy_authority_") && v.some((x) => x.startsWith("bulls96_session_ingest"))).map(([k]) => k).sort();
    expect(granted).toEqual(["sessions.ingest_privacy_authority_begin(jsonb)", "sessions.ingest_privacy_authority_complete(jsonb)"]);
    const down = g.outputs["supabase/rollbacks/20261105041000_ingest_privacy_authority.down.sql"]!;
    expect(down).toContain(createHash("sha256").update(readFileSync(join(root, privacyAuthority.migration!), "utf8")).digest("hex"));
    for (const body of originalMirrorBodies(readFileSync(join(root, "supabase/migrations/20261105010000_frozen_source_custody.sql"), "utf8"))) {
      expect(down).toContain(body);
      expect(down.indexOf(body)).toBeLessThan(down.indexOf("DROP FUNCTION sessions.ingest_privacy_authority_lock(boolean);"));
    }
    // The 040000 lazy-current bodies (prefix exclusion) are restored EXACTLY, not 055000's.
    const lazy = originalLazyBodies(readFileSync(join(root, "supabase/migrations/20261105040000_ingest_lazy_source_tables.sql"), "utf8"));
    expect(lazy.length).toBe(2);
    for (const body of lazy) {
      expect(body).toContain("NOT LIKE 'ingest\\_frozen\\_source\\_%'");
      expect(down).toContain(body);
    }
    expect(down).not.toContain("NOT IN('ingest_frozen_source_records'");
    const up = readFileSync(join(root, privacyAuthority.migration!), "utf8");
    expect(up.match(/NOT IN\('ingest_frozen_source_records'/g)?.length).toBe(2);
    expect(up).not.toContain("NOT LIKE 'ingest\\_frozen");
    expect(down.indexOf("release to neon first")).toBeLessThan(down.indexOf("DROP TRIGGER"));
    expect(down).toContain("a later migration is applied");
    expect(down.trim().endsWith("COMMIT;")).toBe(true);
  });

  test("--check catches drift in every generated artifact and writes nothing; a write run repairs it", async () => {
    const dir = scratch();
    try {
      const acl = JSON.parse(readFileSync(join(dir, ACL_PATH), "utf8"));
      acl.functions[SAMPLE] = ["<owner>:EXECUTE", "anon:EXECUTE"];                         // a widened grant
      acl.functions["sessions.ingest_history_dropped_fn(jsonb)"] = ["<owner>:EXECUTE"];     // a stale lane entry
      writeFileSync(join(dir, ACL_PATH), JSON.stringify(acl, null, 2) + "\n");
      const down = "supabase/rollbacks/20261105026000_canonical_history_lineage.down.sql";
      writeFileSync(join(dir, down), readFileSync(join(dir, down), "utf8") + "-- hand edit\n");
      const wrapper = "supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql";
      writeFileSync(join(dir, wrapper), readFileSync(join(dir, wrapper), "utf8").replace("'sessions.ingest_history_mask_register(jsonb)',\n", ""));   // a SECDEF entry dropped by hand
      const tampered = readFileSync(join(dir, ACL_PATH), "utf8");
      const checked = await run({ root: dir, check: true });
      // Lane-owned ACL entries are regenerated from the manifest (the wrapper/guards were never touched).
      expect(checked.drift).toEqual([ACL_PATH, wrapper, down].sort());
      expect(readFileSync(join(dir, ACL_PATH), "utf8")).toBe(tampered);              // --check wrote nothing
      const proc = cli(["scripts/sync-sessions-function-boundary.ts", "--check", "--root=" + dir]);
      expect(proc.exitCode).toBe(1);
      expect(proc.stderr.toString()).toContain("sessions function boundary drift: " + ACL_PATH);
      const old = cli(["scripts/sync-ingest-history-boundary.ts", "--check"]);          // the thin wrapper, on the clean repo
      expect(old.exitCode).toBe(0);
      expect(JSON.parse(old.stdout.toString())).toMatchObject({ functions: 63, publicationRPCs: 28, check: true });
      expect((await run({ root: dir })).drift.length).toBeGreaterThan(0);              // write mode repairs
      expect((await run({ root: dir, check: true })).drift).toEqual([]);
      const fixed = JSON.parse(readFileSync(join(dir, ACL_PATH), "utf8")).functions;
      expect(fixed[SAMPLE]).toEqual(["<owner>:EXECUTE", ...ingestHistory.functions[SAMPLE]!.grants.map((r) => r + ":EXECUTE")]);
      expect(fixed["sessions.ingest_history_dropped_fn(jsonb)"]).toBeUndefined();
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });

  test("two manifests claiming the same function fail (exact signature and prefix overlap); so does a non-empty empty slot", async () => {
    const rival: LaneManifest = { lane: "rival", owner: "test", migration: ingestHistory.migration, prefixes: ["sessions.rival_"],
      grantSources: [], functions: { [SAMPLE]: ingestHistory.functions[SAMPLE]! }, replaces: {} };
    await expect(generate(repo, [ingestHistory, rival], VERIFIERS)).rejects.toThrow(`function ownership conflict: ${SAMPLE} is claimed by lanes ingest-history and rival`);
    const prefixed: LaneManifest = { ...rival, functions: {}, prefixes: ["sessions.ingest_history_scope_"] };
    await expect(generate(repo, [ingestHistory, prefixed], VERIFIERS)).rejects.toThrow("function ownership conflict: prefixes");
    const slot: LaneManifest = { lane: "slot", owner: "test", migration: null, prefixes: [], grantSources: [], functions: { "sessions.x()": { secdef: false, grants: [] } }, replaces: {} };
    await expect(generate(repo, [ingestHistory, slot], VERIFIERS)).rejects.toThrow("empty slot slot must not claim");
    const cliDup = cli(["-e", `import {run} from "./scripts/sync-sessions-function-boundary.ts";import {LANES} from "./scripts/sessions-function-boundary/lanes/index.ts";
      const l=LANES[0]!;await run({check:true,lanes:[...LANES,{...l,lane:"dup",prefixes:[],extraOutputs:undefined}]});`]);
    expect(cliDup.exitCode).not.toBe(0);
    expect(cliDup.stderr.toString()).toContain("function ownership conflict");
  });

  test("a manifest that disagrees with its SQL fails: unlisted function, wrong SECURITY DEFINER, missing grant", async () => {
    const { [SAMPLE]: dropped, ...rest } = ingestHistory.functions;
    await expect(generate(repo, [{ ...ingestHistory, functions: rest }], VERIFIERS)).rejects.toThrow(`manifest drift (ingest-history): ${ingestHistory.migration} defines ${SAMPLE}`);
    await expect(generate(repo, [{ ...ingestHistory, functions: { ...ingestHistory.functions, [SAMPLE]: { ...dropped!, secdef: !dropped!.secdef } } }], VERIFIERS)).rejects.toThrow("SECURITY DEFINER");
    await expect(generate(repo, [{ ...ingestHistory, functions: { ...ingestHistory.functions, [SAMPLE]: { ...dropped!, grants: [] } } }], VERIFIERS)).rejects.toThrow(`manifest drift (ingest-history): ${SAMPLE} grants`);
    await expect(generate(repo, [{ ...ingestHistory, functions: { ...ingestHistory.functions, "sessions.ingest_history_ghost()": { secdef: false, grants: [] } } }], VERIFIERS)).rejects.toThrow("which supabase/migrations/20261105026000_canonical_history_lineage.sql does not define");
  });

  test("an undeclared grantee in a lane's SQL fails (no role is silently dropped from the manifest)", async () => {
    const narrowed: LaneManifest = { ...ingestHistory, grantSources: ingestHistory.grantSources.map((g) => g.path === ingestHistory.migration ? { ...g, roles: g.roles.filter((r) => r !== "bulls96_canonical_feature_producer") } : g) };
    await expect(generate(repo, [narrowed], VERIFIERS)).rejects.toThrow("grants EXECUTE to bulls96_canonical_feature_producer, which the lane's grant sources do not declare");
  });

  test("a templated verifier (a new role cloned from another verifier) is derived deterministically; a duplicate verifier path fails", async () => {
    const base = VERIFIERS[0]!;
    const clone = { ...base, role: "session_directory_search_worker", path: "supabase/bootstrap/zz_clone_verify.sql", template: base.path, transform: (t: string) => t.replaceAll("bulls96_session_ingest", "bulls96_clone") };
    const out = (await generate(repo, LANES, [...VERIFIERS, clone])).outputs[clone.path]!;
    expect(out).toContain("bulls96_clone");
    expect(out).not.toContain("bulls96_session_ingest");
    const workerFns = OWNED.filter((k) => ingestHistory.functions[k]!.grants.includes("session_directory_search_worker"));
    expect(workerFns.length).toBeGreaterThan(0);
    for (const sig of workerFns) expect(out).toContain("'" + sig + "'");
    await expect(generate(repo, LANES, [...VERIFIERS, VERIFIERS[0]!])).rejects.toThrow("is configured twice");
  });

  test("required lanes are present and filled, including project events 033000 and read API 034000", async () => {
    const slots = LANES.filter((l) => l.migration === null);
    expect(slots.map((l) => l.lane).sort()).toEqual([]);
    expect(LANES.map((l) => l.lane)).toEqual(expect.arrayContaining(["ingest-history", "read-api", "canonical-project-events", "canonical-feature-consumer"]));
    for (const l of slots) {
      expect([Object.keys(l.functions).length, Object.keys(l.replaces).length, l.prefixes.length, l.grantSources.length]).toEqual([0, 0, 0, 0]);
      expect(l.extraOutputs).toBeUndefined();
    }
    const summary = (await generate(repo, LANES, VERIFIERS)).summary.lanes;
    for (const l of slots) expect(summary[l.lane]).toEqual({ functions: 0, replaces: 0, grants: 0 });
    expect(LANES.find((l) => l.lane === "read-api")?.migration).toBe("supabase/migrations/20261105034000_session_directory_read_api.sql");
    expect(summary["canonical-project-events"]).toEqual({ functions: 10, replaces: 0, grants: 6 });
    expect(LANES.find((l) => l.lane === "canonical-project-events")?.migration).toBe("supabase/migrations/20261105033000_canonical_project_events.sql");
    expect(summary["read-api"]).toEqual({ functions: 3, replaces: 0, grants: 2 });
    expect(summary["canonical-feature-consumer"]).toEqual({ functions: 2, replaces: 0, grants: 1 });
  });

  test("043700 owner buffered read: two read-api-callable SECDEF readers, owner-only scope and parity helpers, ACL +4", async () => {
    const g = await generate(repo, LANES, VERIFIERS);
    expect(g.summary.lanes["owner-buffered-read"]).toEqual({ functions: 4, replaces: 0, grants: 2 });
    expect(LANES.find((l) => l.lane === "owner-buffered-read")?.migration).toBe("supabase/migrations/20261105043700_owner_buffered_read.sql");
    const acl = JSON.parse(g.outputs[ACL_PATH]!).functions as Record<string, string[]>;
    const mine = Object.entries(acl).filter(([k]) => /^sessions\.(owner_buffered_read_scope|owner_buffered_parity_expected|buffered_sessions_for|buffered_session_stages_for)\(/.test(k));
    expect(Object.fromEntries(mine)).toEqual({
      "sessions.owner_buffered_read_scope(text,text,text,text,text,text,boolean,text)": ["<owner>:EXECUTE"],
      "sessions.owner_buffered_parity_expected(text,text,text,text,bigint,text,text)": ["<owner>:EXECUTE"],
      "sessions.buffered_sessions_for(text,timestamp with time zone,text,text,integer,text,text,text,text,text,boolean,text)": ["<owner>:EXECUTE", "bulls96_read_api:EXECUTE"],
      "sessions.buffered_session_stages_for(text,text,text,text,text,text,text,text,boolean,text)": ["<owner>:EXECUTE", "bulls96_read_api:EXECUTE"],
    });
    // Hand-written down path (no generated rollback), same guard shape as 034000/043000.
    const down = readFileSync(join(root, "supabase/rollbacks/20261105043700_owner_buffered_read.down.sql"), "utf8");
    expect(down).toContain("a later migration is applied");
    expect(down.match(/DROP FUNCTION IF EXISTS/g)?.length).toBe(4);
    expect(down).toContain("DROP TABLE IF EXISTS sessions.owner_buffered_parity;");
    expect(down).toContain("DELETE FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105043700_owner_buffered_read.sql'");
  });

  test("043860 buffered search gate: one read-api-callable SECDEF reader, ACL +1, no table grant", async () => {
    const g = await generate(repo, LANES, VERIFIERS);
    expect(g.summary.lanes["buffered-search-gate"]).toEqual({ functions: 1, replaces: 0, grants: 1 });
    expect(LANES.find((l) => l.lane === "buffered-search-gate")?.migration).toBe("supabase/migrations/20261105043860_buffered_search_gate.sql");
    const acl = JSON.parse(g.outputs[ACL_PATH]!).functions as Record<string, string[]>;
    const mine = Object.entries(acl).filter(([k]) => k.startsWith("sessions.buffered_search_gate_for("));
    expect(Object.fromEntries(mine)).toEqual({
      "sessions.buffered_search_gate_for(text,text,text,text,text,text,text,text,boolean,text,jsonb)": ["<owner>:EXECUTE", "bulls96_read_api:EXECUTE"],
    });
    const migration = readFileSync(join(root, "supabase/migrations/20261105043860_buffered_search_gate.sql"), "utf8");
    // EXECUTE only: no table, sequence or schema privilege for any role.
    expect(migration.match(/^GRANT .*$/gmu)).toEqual(["GRANT EXECUTE ON FUNCTION sessions.buffered_search_gate_for(text, text, text, text, text, text, text, text, boolean, text, jsonb)"]);
    expect(migration).toContain("FROM PUBLIC, anon, authenticated, service_role, bulls96_session_ingest, bulls96_session_ingest_seeder;");
    // The parity tuple is CALLED from 043700, not re-derived.
    expect(migration).toContain("FROM sessions.owner_buffered_parity_expected(v_org, v_ws, p_owner_user_id, p_cutover_epoch, v_version,");
    const down = readFileSync(join(root, "supabase/rollbacks/20261105043860_buffered_search_gate.down.sql"), "utf8");
    expect(down).toContain("a later migration is applied");
    expect(down.match(/DROP FUNCTION IF EXISTS/g)?.length).toBe(1);
    expect(down).toContain("DROP FUNCTION IF EXISTS sessions.buffered_search_gate_for(text, text, text, text, text, text, text, text, boolean, text, jsonb);");
    expect(down).toContain("DELETE FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105043860_buffered_search_gate.sql'");
  });

  test("045030 grant-only lane: appends search_consumer_runtime to project_text's ACL, is credited by the ACL preview while pending, and refuses SQL/manifest disagreement", async () => {
    const g = await generate(repo, LANES, VERIFIERS);
    expect(g.summary.lanes["consumer-project-text-grant"]).toEqual({ functions: 0, replaces: 0, grants: 1 });
    const acl = JSON.parse(g.outputs[ACL_PATH]!).functions as Record<string, string[]>;
    expect(acl["sessions.session_directory_project_text(text,text,text)"]).toEqual(["<owner>:EXECUTE", "service_role:EXECUTE", "search_consumer_runtime:EXECUTE"]);
    // The ACL preview credits a pending step's literal EXECUTE grants (pendingExecuteGrants):
    // for every grant-only lane that is exactly its grantsOn, in manifest key spelling.
    for (const l of LANES.filter((x) => x.grantsOn)) {
      const declared = Object.entries(l.grantsOn!).flatMap(([sig, roles]) => roles.map((r) => `${sig} ${r}:EXECUTE`));
      expect([...pendingExecuteGrants([readFileSync(join(root, l.migration!), "utf8")])].sort()).toEqual(declared.sort());
    }
    const lane = LANES.find((l) => l.lane === "consumer-project-text-grant")!;
    const SIG = "sessions.session_directory_project_text(text,text,text)";
    // grantsOn naming another role than the SQL grants: drift.
    await expect(generate(repo, [...LANES.filter((l) => l !== lane), { ...lane, grantsOn: { [SIG]: ["service_role"] } }], VERIFIERS))
      .rejects.toThrow("grants EXECUTE to search_consumer_runtime, which the lane's grant sources do not declare");
    // No grantsOn at all: the SQL grant is an undeclared grantee.
    await expect(generate(repo, [...LANES.filter((l) => l !== lane), { ...lane, grantsOn: {} }], VERIFIERS))
      .rejects.toThrow("grants EXECUTE to search_consumer_runtime, which the lane's grant sources do not declare");
    // A grantsOn entry the SQL does not grant: drift.
    await expect(generate(repo, [...LANES.filter((l) => l !== lane), { ...lane, grantsOn: { [SIG]: ["search_consumer_runtime"], "sessions.canonical_remote(text)": ["search_consumer_runtime"] } }], VERIFIERS))
      .rejects.toThrow("manifest drift (consumer-project-text-grant): sessions.canonical_remote(text) grant-only grants are [] in SQL");
    // An empty slot may not carry grantsOn.
    await expect(generate(repo, [...LANES.filter((l) => l !== lane), { ...lane, migration: null, extraOutputs: undefined }], VERIFIERS))
      .rejects.toThrow("empty slot consumer-project-text-grant must not claim");
    const down = g.outputs["supabase/rollbacks/20261105045030_session_consumer_project_text_grant.down.sql"]!;
    expect(down).toContain("REVOKE EXECUTE ON FUNCTION sessions.session_directory_project_text(text,text,text) FROM search_consumer_runtime;");
    expect(down).toContain("a later migration is applied");
    expect(down).toContain("DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105045030_session_consumer_project_text_grant.sql'");
  });

  test("043870 buffered search feed: three SECDEF RPCs for the dedicated runtime login, ACL +3, not runtime functions, no table grant", async () => {
    const g = await generate(repo, LANES, VERIFIERS);
    expect(g.summary.lanes["buffered-search-feed"]).toEqual({ functions: 3, replaces: 0, grants: 3 });
    expect(LANES.find((l) => l.lane === "buffered-search-feed")?.migration).toBe("supabase/migrations/20261105043870_buffered_search_feed_cursor.sql");
    expect(BUFFERED_SEARCH_FEED_RUNTIME_ROLE).toBe("buffered_search_feed_runtime");
    const acl = JSON.parse(g.outputs[ACL_PATH]!).functions as Record<string, string[]>;
    const mine = Object.entries(acl).filter(([k]) => k.startsWith("sessions.buffered_search_feed_"));
    expect(Object.fromEntries(mine)).toEqual({
      "sessions.buffered_search_feed_advance(jsonb)": ["<owner>:EXECUTE", "buffered_search_feed_runtime:EXECUTE"],
      "sessions.buffered_search_feed_page(jsonb)": ["<owner>:EXECUTE", "buffered_search_feed_runtime:EXECUTE"],
      "sessions.buffered_search_feed_stage(jsonb)": ["<owner>:EXECUTE", "buffered_search_feed_runtime:EXECUTE"],
    });
    // No service_role grant any more, so they are not runtime application functions in the generated guards.
    for (const sig of Object.keys(Object.fromEntries(mine))) expect(g.outputs[GUARDS_PATH]).not.toContain(JSON.stringify(sig));
    // The generated phase 2 verifier for the role.
    const verify = g.outputs["supabase/bootstrap/buffered_search_feed_runtime_verify.sql"]!;
    expect(verify).toContain("buffered_search_feed_runtime");
    expect(verify).not.toContain("bulls96_session_ingest");
    const migration = readFileSync(join(root, "supabase/migrations/20261105043870_buffered_search_feed_cursor.sql"), "utf8");
    // EXECUTE on the three RPCs plus schema USAGE, for the runtime role only: no table or sequence privilege.
    expect(migration.match(/^GRANT .*$/gmu)).toEqual([
      ...["page", "stage", "advance"].map((f) => `GRANT EXECUTE ON FUNCTION sessions.buffered_search_feed_${f}(jsonb) TO buffered_search_feed_runtime;`),
      "GRANT USAGE ON SCHEMA sessions TO buffered_search_feed_runtime;",
    ]);
    expect(migration.match(/FROM PUBLIC, anon, authenticated, service_role, bulls96_session_ingest, bulls96_session_ingest_seeder;/g)?.length).toBe(3);
    expect(migration.match(/SET search_path = pg_catalog, sessions, pg_temp/g)?.length).toBe(3);
    // Every RPC admits only the genuine login (the 043860 guard): session_user, no SET ROLE.
    expect(migration.match(/IF session_user::text IS DISTINCT FROM 'buffered_search_feed_runtime' OR coalesce\(current_setting\('role', true\), 'none'\) <> 'none' THEN/g)?.length).toBe(3);
    // The role must pre-exist and be safe (043850's shape): 42704 absent, 55000 elevated/member/unexpected member.
    expect(migration).toContain("buffered search feed: role buffered_search_feed_runtime absent; run the role bootstrap first");
    expect(migration).toContain("buffered search feed: role buffered_search_feed_runtime exists elevated or is a member");
    expect(migration).toContain("buffered search feed: role buffered_search_feed_runtime has an unexpected member");
    // Retention is CALLED from the GC authority, not re-derived.
    expect(migration).toContain("v_retained:=sessions.ingest_buffer_gc_ordinals(act.stage_token);");
    const down = readFileSync(join(root, "supabase/rollbacks/20261105043870_buffered_search_feed_cursor.down.sql"), "utf8");
    expect(down).toContain("a later migration is applied");
    expect(down.match(/DROP FUNCTION IF EXISTS/g)?.length).toBe(3);
    expect(down).toContain("DROP TABLE IF EXISTS sessions.buffered_search_feed_cursors;");
    expect(down).toContain("REVOKE USAGE ON SCHEMA sessions FROM buffered_search_feed_runtime;");
    expect(down).not.toMatch(/DROP ROLE/);
    expect(down).toContain("DELETE FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105043870_buffered_search_feed_cursor.sql'");
  });

  test("every function the manifest grants search_consumer_runtime is exempt from the live gate for it; the consumer writer list matches its verifier and stays out of the runtime list", () => {
    const acl = JSON.parse(readFileSync(join(root, "src/hoopdb/sessions-function-acl.json"), "utf8")).functions as Record<string, string[]>;
    const runtime = new Set<string>(RUNTIME_APPLICATION_FUNCTIONS);
    const consumer = new Set<string>(CONSUMER_RUNTIME_FUNCTIONS);
    const granted = Object.entries(acl).filter(([, entries]) => entries.includes(`${CONSUMER_RUNTIME_FUNCTIONS_ROLE}:EXECUTE`)).map(([fn]) => fn);
    expect(granted.length).toBeGreaterThan(0);
    expect(granted.filter((fn) => !runtime.has(fn) && !consumer.has(fn))).toEqual([]);
    // Each consumer-only writer is granted to the consumer (and nobody but the owner), never to the general runtime role.
    for (const fn of CONSUMER_RUNTIME_FUNCTIONS) {
      expect(acl[fn], fn).toEqual(["<owner>:EXECUTE", `${CONSUMER_RUNTIME_FUNCTIONS_ROLE}:EXECUTE`]);
      expect(runtime.has(fn), fn).toBe(false);
    }
    // The consumer role verifier allowlists exactly the same writers (both of its lists).
    const verify = readFileSync(join(root, "supabase/bootstrap/session_consumer_role_verify.sql"), "utf8");
    const listed = [...verify.matchAll(/'(sessions\.(?:output_facts_[a-z_]+|ingest_operator_runtime_upsert)\([^']*\))'/g)].map((m) => m[1]!);
    expect([...new Set(listed)].sort()).toEqual([...CONSUMER_RUNTIME_FUNCTIONS].sort());
    for (const fn of CONSUMER_RUNTIME_FUNCTIONS) expect(listed.filter((x) => x === fn).length, fn).toBe(2);
  });

  test("043890 retired witness: replaces 043860's gate only (ACL/boundary/verifiers unchanged) and derives the exact 043860 rollback", async () => {
    const withLane = await generate(repo, LANES, VERIFIERS);
    const withoutLane = await generate(repo, LANES.filter((l) => l !== bufferedSearchGateRetiredWitness), VERIFIERS);
    const downPath = "supabase/rollbacks/20261105043890_buffered_search_gate_retired_witness.down.sql";
    expect(withLane.summary.lanes["buffered-search-gate-retired-witness"]).toEqual({ functions: 0, replaces: 1, grants: 0 });
    for (const [path, value] of Object.entries(withoutLane.outputs)) expect(withLane.outputs[path]).toBe(value);
    expect(Object.keys(withLane.outputs).filter((p) => !(p in withoutLane.outputs))).toEqual([downPath]);
    const migration = readFileSync(join(root, "supabase/migrations/20261105043890_buffered_search_gate_retired_witness.sql"), "utf8");
    const original = readFileSync(join(root, "supabase/migrations/20261105043860_buffered_search_gate.sql"), "utf8");
    // No GRANT at all (the read API grant stays 043860's); the 043860 REVOKE and sweep are re-issued.
    expect(migration).not.toMatch(/^GRANT /mu);
    expect(migration).toContain("FROM PUBLIC, anon, authenticated, service_role, bulls96_session_ingest, bulls96_session_ingest_seeder;");
    // The body is 043860's plus exactly the retired column and the retired CASE branch.
    const body = migration.match(/CREATE OR REPLACE FUNCTION sessions\.buffered_search_gate_for\([\s\S]*?\n\$gate\$;/)![0];
    const added = [
      "           -- 20261105043890: the 043880 cleanup (Phase A) tombstoned this claim and deleted its lazy ACK\n",
      "           -- decisions, so version_consistent above no longer witnesses anything for it.\n",
      "           EXISTS (SELECT 1 FROM sessions.ingest_bookkeeping_retired_claims AS r\n",
      "                    WHERE r.claim_token = a.claim_token) AS retired,\n",
      "           WHEN j.retired IS NOT FALSE THEN 'retired'\n",
    ];
    let stripped = body;
    for (const line of added) { expect(stripped).toContain(line); stripped = stripped.replace(line, ""); }
    expect(stripped).toBe(originalGateBody(original));
    expect(body.indexOf("'admission_epoch_mismatch'")).toBeLessThan(body.indexOf("'retired'"));
    expect(body.indexOf("'retired'")).toBeLessThan(body.indexOf("'admission_version_mismatch'"));
    expect(body).toContain("LANGUAGE plpgsql STABLE SECURITY DEFINER");
    const down = withLane.outputs[downPath]!;
    expect(readFileSync(join(root, downPath), "utf8")).toBe(down);
    expect(down).toContain(originalGateBody(original));
    expect(down).not.toContain("ingest_bookkeeping_retired_claims AS r");
    expect(down).toContain("a later migration is applied");
    expect(down).toContain("claims were retired; turn the gate off instead of rolling back");
    expect(down).not.toMatch(/DROP |GRANT /);
    expect(down).toContain("DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105043890_buffered_search_gate_retired_witness.sql'");
  });

  test("a stale SECDEF signature under a lane's prefix (an old arity) is dropped from the wrapper, not carried forward", async () => {
    const dir = scratch();
    try {
      const wrapper = "supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql";
      const stale = "sessions.buffered_sessions_for(text,timestamp with time zone,text,text,integer,text,text,text,text,boolean)";
      const clean = readFileSync(join(dir, wrapper), "utf8");
      const current = "'sessions.buffered_sessions_for(text,timestamp with time zone,text,text,integer,text,text,text,text,text,boolean,text)'";
      expect(clean).toContain(current);
      expect(clean).not.toContain("'" + stale + "'");
      writeFileSync(join(dir, wrapper), clean.replace(current, "'" + stale + "',\n    " + current));
      expect(readFileSync(join(dir, wrapper), "utf8")).toContain("'" + stale + "'");
      const out = (await generate(reader(dir), LANES, VERIFIERS)).outputs[wrapper]!;
      expect(out).not.toContain("'" + stale + "'");
      expect(out).toContain(current);
      expect(out).toBe(clean);
      expect((await run({ root: dir, check: true })).drift).toEqual([wrapper]);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });
});
