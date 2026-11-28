/**
 * The empty-erasure-window proof (src/session-ingest/erasure-window.ts) and its backlog-ops mode (bridge-dry): every
 * erasure-lane source classified from the schema, the one-statement read against the real schema in PGlite (against the
 * real 062320/062340 lifecycle, and with the status function replaced by a stub for synthetic status shapes), the closed
 * proof, the release gate on it, and the dispatch, argv and receipt schema of the mode.
 */
import { afterAll, expect, setDefaultTimeout, test } from "bun:test";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { bridgeWindow } from "../../../scripts/backlog-authority.ts";
import { backlogOpsRun } from "../../../scripts/backlog-ops.ts";
import { SCHEMA_RELEASE as P } from "../../hoopdb/schema-release-contract.ts";
import { BACKLOG_OPS_RECEIPT_CONTRACT, backlogOpsHomeId } from "../backlog-ops.ts";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
import { authorityReceiptSummary } from "../backlog-authority-schema.ts";
import {
  BACKLOG_OPS_BRIDGE_EXECUTE_ENABLED, backlogOpsBindings, backlogOpsConfirmation, backlogOpsDispatch, backlogOpsRevisionsSince, backlogOpsScope, backlogOpsTarget,
  backlogOpsToolArgs, type BacklogOpsMode,
} from "../backlog-ops.ts";
import {
  ERASURE_HISTORY_TABLES, ERASURE_PROOF_MAX_AGE_MS, ERASURE_SOURCES, ERASURE_SOURCE_EXCLUSIONS, ERASURE_TABLE_NAME, ERASURE_WINDOW_SQL, epochHash,
  EMPTY_RELEASE_REFUSALS, EMPTY_RELEASE_WAVE_ORG_HASHES, emptyReleaseEvidence, erasureWindowProof, orgHash, releaseEmptyWindow, releaseOnEmptyWindow, validErasureWindowProof,
} from "../erasure-window.ts";
import { applySupabaseSchema } from "./pglite-schema.ts";

setDefaultTimeout(300_000);
/** Every proof in these tests binds the FULL run FULL (the dry dispatch's full_backfill_run). */
/** The bound FULL receipt's sha256, and the FULL's start as the gate knows it (created_at = cutoff + the 15-min margin). */
const EV = "e".repeat(64);
const startedFor = (cut: number) => cut + 15 * 60_000;
const proof = (org: string, epoch: string | null, cut: number, rows: readonly Record<string, unknown>[]) =>
  erasureWindowProof(org, epoch, cut, rows, FULL, EV, startedFor(cut));
/** 062340's final shape (adc8f8f5): the history fence, and the selected FULL bound to the epoch. */
const selected = (cut: number, over: Record<string, unknown> = {}) => ({ run: FULL, started_at: new Date(startedFor(cut)).toISOString(), started_ms: startedFor(cut),
  evidence_sha256: EV, ...over });
const ORG = "11111111-2222-4333-8444-555555555555", ORG2 = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const E = "0f0e0d0c-0b0a-4908-8706-050403020100", E2 = "1f0e0d0c-0b0a-4908-8706-050403020100";
const NOW = Date.parse("2026-11-27T12:00:00Z"), CUT = NOW - 6 * 3600_000, SEALED = NOW - 3600_000;
const codeSync = (fn: () => unknown) => { try { fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); } };
const zeros = () => Object.fromEntries(Object.keys(ERASURE_SOURCES).map((k) => [k, "0"]));
const status = (hold: Record<string, unknown> | null, erasures = 3) => ({ hold, erasures });
const sealedHold = (over: Record<string, unknown> = {}) => ({ epoch: E, state: "sealed", installed_at: new Date(NOW - 9e6).toISOString(), sealed_at: new Date(SEALED).toISOString(),
  marker_attempted_at: null, ...over });
const row = (over: Record<string, unknown> = {}) => ({ ...zeros(), status: status(sealedHold()), read_at_ms: String(NOW), ...over });

// ---- classification ----

test("every erasure-like per-org table in the schema is a source (or a documented singleton exclusion, or 062340's own history)", () => {
  const dir = fileURLToPath(new URL("../../../supabase/migrations/", import.meta.url));
  const tables = new Map<string, string>();
  for (const f of readdirSync(dir).filter((x) => x.endsWith(".sql"))) {
    const sql = readFileSync(join(dir, f), "utf8");
    for (const m of sql.matchAll(/CREATE TABLE (?:IF NOT EXISTS )?sessions\.([a-z_]+)\s*\(([\s\S]*?)\n\);/gu)) tables.set(m[1]!, m[2]!);
  }
  const erasureLike = [...tables.keys()].filter((t) => ERASURE_TABLE_NAME.test(t)).sort();
  expect(erasureLike.length).toBeGreaterThan(10);
  for (const t of erasureLike) {
    const classified = Object.hasOwn(ERASURE_SOURCES, t) || Object.hasOwn(ERASURE_SOURCE_EXCLUSIONS, t) || Object.hasOwn(ERASURE_HISTORY_TABLES, t);
    expect([t, classified]).toEqual([t, true]);
    // A source has an org_id; an exclusion has none.
    expect([t, /\borg_id\b/u.test(tables.get(t)!)]).toEqual([t, Object.hasOwn(ERASURE_SOURCES, t)]);
  }
  for (const t of [...Object.keys(ERASURE_SOURCES), ...Object.keys(ERASURE_SOURCE_EXCLUSIONS), ...Object.keys(ERASURE_HISTORY_TABLES)])
    expect([t, tables.has(t)]).toEqual([t, true]);
  // The three classes are disjoint.
  for (const t of Object.keys(ERASURE_HISTORY_TABLES)) expect([t, Object.hasOwn(ERASURE_SOURCES, t) || Object.hasOwn(ERASURE_SOURCE_EXCLUSIONS, t)]).toEqual([t, false]);
  // The ones the reviewer named.
  for (const t of ["operator_user_erasures", "session_erasure_scopes", "session_erasure_runs", "session_erasure_receipts", "upload_deletions", "upload_erasure_rearm"])
    expect(Object.hasOwn(ERASURE_SOURCES, t)).toBe(true);
});

// ---- the read, against the real schema ----

let pg: PGlite | null = null, realPg: PGlite | null = null;
afterAll(async () => { await pg?.close(); await realPg?.close(); });
/** The real schema with 062320's status function replaced by a stub over a table, for synthetic status shapes. */
async function db() {
  if (pg) return pg;
  pg = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await applySupabaseSchema(pg, { until: null });
  await pg.exec(`CREATE TABLE sessions._stub_hold (org text PRIMARY KEY, status jsonb NOT NULL);
    CREATE OR REPLACE FUNCTION sessions.ingest_org_admission_hold_status(p_org text) RETURNS jsonb LANGUAGE sql STABLE AS
      $$ SELECT coalesce((SELECT status FROM sessions._stub_hold WHERE org = p_org), '{"hold":null,"erasures":0}'::jsonb) $$;`);
  return pg;
}
/** The real schema, untouched: 062320's lifecycle and 062340's history, status and bind. */
async function realDb() {
  if (realPg) return realPg;
  realPg = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await applySupabaseSchema(realPg, { until: null });
  await realPg.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text)");
  for (const n of ["upstream:20261105062320_ingest_org_admission_fences.sql", "upstream:20261105062340_ingest_org_erasure_history.sql"])
    await realPg.query("INSERT INTO sessions.__96bulls_migrations(name) VALUES($1) ON CONFLICT DO NOTHING", [n]);
  return realPg;
}
const ts = (ms: number) => `to_timestamp(${ms} / 1000.0)`;
const read = async (org = ORG) => (await (await db()).query<Record<string, unknown>>(ERASURE_WINDOW_SQL, [org, String(CUT)])).rows;

test("the read: one statement over every source plus the status; an org with nothing in the window is empty", async () => {
  const d = await db();
  await d.query("INSERT INTO sessions._stub_hold VALUES ($1, $2::jsonb)", [ORG, JSON.stringify(status(sealedHold(), 0))]);
  const rows = await read();
  expect(rows.length).toBe(1);
  for (const t of Object.keys(ERASURE_SOURCES)) expect(Number(rows[0]![t])).toBe(0);
  const p = proof(ORG, null, CUT, rows);
  expect([p.status, p.reason, p.epoch_hash, p.hold_state, p.watermark_erasures]).toEqual(["erasure_window_empty", "ok", epochHash(E), "sealed", 0]);
  expect(JSON.stringify(p)).not.toContain(E); // the epoch is a typed hash, never the raw UUID
  expect(validErasureWindowProof(p)).toBe(true);
  expect(JSON.stringify(p)).not.toContain(ORG);
});

test("the read: an upload deletion, an in-flight old user erase, a fence in the window each make it non-empty; old completed ones do not", async () => {
  const d = await db();
  await d.query("INSERT INTO sessions._stub_hold VALUES ($1, $2::jsonb) ON CONFLICT (org) DO UPDATE SET status = EXCLUDED.status", [ORG2, JSON.stringify(status(sealedHold(), 2))]);
  // Out of the window and finished: not counted.
  await d.exec(`INSERT INTO sessions.session_erasure_fences (org_id, workspace_id, source, session_id, created_at) VALUES ('${ORG2}', 'w', 'claude', 's-old', ${ts(CUT - 1000)});
    INSERT INTO sessions.operator_user_erasures (request_id, org_id, workspace_id, user_id, state, origins, created_at, updated_at, metadata_purged_at, bytes_verified_at)
      VALUES ('done', '${ORG2}', 'w', 'u-done', 'bytes_verified', ARRAY[]::text[], ${ts(CUT - 9000)}, ${ts(CUT - 8000)}, ${ts(CUT - 8000)}, ${ts(CUT - 8000)});`);
  expect(proof(ORG2, null, CUT, await read(ORG2)).status).toBe("erasure_window_empty");
  // A user deletion (upload_deletions) in the window: not empty.
  await d.exec(`INSERT INTO sessions.upload_deletions (org_id, workspace_id, user_id, origin_id, source, session_id, deleted_at)
    VALUES ('${ORG2}', 'w', 'u', 'o', 'claude', 's-del', ${ts(CUT + 1000)});`);
  let p = proof(ORG2, null, CUT, await read(ORG2));
  expect([p.status, p.reason, p.sources?.upload_deletions]).toEqual(["erasure_window_nonempty", "erasures_in_window", 1]);
  await d.exec(`DELETE FROM sessions.upload_deletions WHERE org_id = '${ORG2}';`);
  // An OLD user erase still purging (created before the cutoff, not bytes_verified): in flight, so not empty.
  await d.exec(`INSERT INTO sessions.operator_user_erasures (request_id, org_id, workspace_id, user_id, state, origins, created_at, updated_at)
    VALUES ('open', '${ORG2}', 'w', 'u-open', 'purging', ARRAY['o'], ${ts(CUT - 9000)}, ${ts(CUT - 9000)});`);
  p = proof(ORG2, null, CUT, await read(ORG2));
  expect([p.status, p.sources?.operator_user_erasures]).toEqual(["erasure_window_nonempty", 1]);
  // Another org's rows never count for ORG.
  expect(proof(ORG, null, CUT, await read(ORG)).status).toBe("erasure_window_empty");
});

test("final 062340 (synthetic status over the real read): fence_at = the hold's installed_at; the FULL only against selected_full (run, started_ms, evidence); install < start <= seal", async () => {
  const d = await db();
  const cut = Date.parse("2026-11-27T12:00:00.000Z"); // the gate's FULL cutoff; the FULL started at cut + 15 min
  const ORG3 = "33333333-2222-4333-8444-555555555555";
  const set = async (st: Record<string, unknown>) =>
    d.query("INSERT INTO sessions._stub_hold VALUES ($1, $2::jsonb) ON CONFLICT (org) DO UPDATE SET status = EXCLUDED.status", [ORG3, JSON.stringify(st)]);
  const proofAt = async () => {
    let rows: readonly Record<string, unknown>[] = [];
    try { rows = (await d.query<Record<string, unknown>>(ERASURE_WINDOW_SQL, [ORG3, String(cut)])).rows; } catch { rows = []; }
    const p = erasureWindowProof(ORG3, null, cut, rows, FULL, EV, startedFor(cut));
    expect(authorityReceiptSummary("bridge", JSON.parse(JSON.stringify({ mode: "dry_run", tool: "bridge_window", org_count: 1, all_empty: p.status === "erasure_window_empty",
      per_org: [p] })), { orgs: [ORG3], cutoffMs: cut }).ok).toBe(true);
    return [p.status, p.reason];
  };
  const installed = "2026-11-27T11:00:00.123456Z";
  const hold = sealedHold({ installed_at: installed, sealed_at: "2026-11-27T13:00:00Z" });
  const st = (eh: Record<string, unknown> = {}, sf: Record<string, unknown> | null = {}) => ({ hold, erasures: 0,
    selected_full: sf === null ? null : selected(cut, sf),
    erasure_history: { fence_at: installed, covered: true, recording: true, by_source: {}, coverage_started_at: "2026-11-20T00:00:00Z", ...eh } });
  await set(st());
  expect(await proofAt()).toEqual(["erasure_window_empty", "ok"]);
  // The fence: exactly installed_at, at microseconds.
  for (const f of ["2026-11-27T11:00:00.123457Z", "2026-11-27T11:00:00.123455Z", "2026-11-27T11:00:00.123Z"]) {
    await set(st({ fence_at: f }));
    expect(await proofAt()).toEqual(["erasure_window_unproven", "history_cutoff_mismatch"]);
  }
  // The selected FULL: exactly this run's run id, start and evidence.
  for (const sf of [{ run: "999" }, { started_ms: startedFor(cut) + 1 }, { evidence_sha256: "0".repeat(64) }]) {
    await set(st({}, sf));
    expect(await proofAt()).toEqual(["erasure_window_unproven", "history_full_mismatch"]);
  }
  // install < start <= seal, from started_ms (the SQL builds it exactly). Matching start values are needed so the
  // ordering, not the identity, is what refuses: change the hold instead.
  const setHold = async (h: Record<string, unknown>) => set({ ...st(), hold: sealedHold({ installed_at: installed, sealed_at: "2026-11-27T13:00:00Z", ...h }) });
  await setHold({ installed_at: new Date(startedFor(cut)).toISOString() }); // install == start: not strictly after
  expect(await proofAt()).toEqual(["erasure_window_unproven", "history_cutoff_mismatch"]); // (fence no longer = installed_at)
  await set({ ...st({ fence_at: new Date(startedFor(cut)).toISOString() }), hold: sealedHold({ installed_at: new Date(startedFor(cut)).toISOString(), sealed_at: "2026-11-27T13:00:00Z" }) });
  expect(await proofAt()).toEqual(["erasure_window_unproven", "history_full_order_invalid"]);
  await setHold({ sealed_at: new Date(startedFor(cut) - 1).toISOString() }); // seal before start
  expect(await proofAt()).toEqual(["erasure_window_unproven", "history_full_order_invalid"]);
  await setHold({ sealed_at: new Date(startedFor(cut)).toISOString() }); // start == seal: allowed
  expect((await proofAt())[1]).not.toBe("history_full_order_invalid");
  // Unbound, malformed, not recording; 062320 alone stays snapshot-only.
  await set({ ...st({ fence_at: null, covered: null, by_source: null }, null), erasures: null });
  expect(await proofAt()).toEqual(["erasure_window_unproven", "history_unbound"]);
  await set(st({ fence_at: "bad" }));
  expect(await proofAt()).toEqual(["erasure_window_unproven", "read_invalid"]);
  await set(st({ recording: false }));
  expect(await proofAt()).toEqual(["erasure_window_unproven", "history_not_recording"]);
  await set({ hold, erasures: 0 });
  expect(await proofAt()).toEqual(["erasure_window_empty", "ok"]);
});

test("real 062320/062340 lifecycle: install, seal, bind; the read is unbound until _bind, empty after, non-empty on a write after the fence", async () => {
  const d = await realDb();
  const ORG4 = "44444444-2222-4333-8444-555555555555";
  const one = async (sql: string, p: unknown[] = []) => (await d.query<Record<string, unknown>>(sql, p)).rows[0]!;
  const e = (await one("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text e", [ORG4])).e as string;
  await d.query("SELECT pg_sleep(0.003)"); // a FULL start strictly between install and seal
  await d.query("SELECT sessions.ingest_org_admission_hold_seal($1, $2::uuid, $3)", [ORG4, e, EV]);
  // The bridge's FULL started 1 ms after the install (integer ms); the gate's cutoff is that start minus the margin.
  const started = Number((await one("SELECT floor(extract(epoch FROM installed_at) * 1000)::bigint + 1 AS ms FROM sessions.ingest_org_admission_holds WHERE epoch = $1::uuid", [e])).ms);
  const cut = started - 15 * 60_000;
  const proofNow = async (fullRun = FULL, fullStarted = started) => {
    await d.query("SELECT pg_sleep(0.003)"); // the read is after the seal
    const rows = (await d.query<Record<string, unknown>>(ERASURE_WINDOW_SQL, [ORG4, String(cut)])).rows;
    const p = erasureWindowProof(ORG4, e, cut, rows, fullRun, EV, fullStarted);
    expect(validErasureWindowProof(p)).toBe(true);
    expect(authorityReceiptSummary("bridge", JSON.parse(JSON.stringify({ mode: "dry_run", tool: "bridge_window", org_count: 1, all_empty: p.status === "erasure_window_empty",
      per_org: [p] })), { orgs: [ORG4], cutoffMs: cut }).ok).toBe(true);
    return p;
  };
  // Sealed but not bound: the history is not claimed.
  let p = await proofNow();
  expect([p.status, p.reason, p.watermark_kind, p.hold_state]).toEqual(["erasure_window_unproven", "history_unbound", "erasure_history_all_sources", "sealed"]);
  await d.query("SELECT sessions.ingest_org_erasure_history_bind($1, $2::uuid, $3, $4::bigint, $5)", [ORG4, e, FULL, String(started), EV]);
  // Bound to exactly this FULL, covered, recording, nothing since the fence: empty.
  p = await proofNow();
  expect([p.status, p.reason, p.watermark_erasures, p.epoch_hash]).toEqual(["erasure_window_empty", "ok", 0, epochHash(e)]);
  // Another run, or another start, is not the bound FULL.
  expect((await proofNow("445")).reason).toBe("history_full_mismatch");
  expect((await proofNow(FULL, started + 1)).reason).toBe("history_full_mismatch");
  // A write to a source after the fence is in the history (and in the snapshot): not empty.
  await d.query("INSERT INTO sessions.upload_deletions (org_id, workspace_id, user_id, origin_id, source, session_id) VALUES ($1, 'w', 'u', 'o', 'claude', 's-real')", [ORG4]);
  p = await proofNow();
  expect([p.status, p.reason, p.watermark_erasures, p.sources?.upload_deletions]).toEqual(["erasure_window_nonempty", "erasures_in_history", 1, 1]);
  // Removing the row does not hide it: the history keeps the INSERT and the DELETE.
  await d.query("DELETE FROM sessions.upload_deletions WHERE org_id = $1", [ORG4]);
  p = await proofNow();
  expect([p.status, p.reason, p.watermark_erasures, p.sources?.upload_deletions]).toEqual(["erasure_window_nonempty", "erasures_in_history", 2, 0]);
});

test("the source list is pinned to 062340's ORG_ERASURE_HISTORY_SOURCES", async () => {
  const path = fileURLToPath(new URL("../../hoopdb/org-erasure-history-sources.ts", import.meta.url));
  const { ORG_ERASURE_HISTORY_SOURCES } = await import(path);
  expect([...ORG_ERASURE_HISTORY_SOURCES].sort()).toEqual(Object.keys(ERASURE_SOURCES).sort());
});

// ---- the proof ----

test("the proof is empty only with every count 0, a live sealed or attempted hold, and a read after the seal", () => {
  expect(proof(ORG, null, CUT, [row()]).status).toBe("erasure_window_empty");
  expect(proof(ORG, E, CUT, [row()]).status).toBe("erasure_window_empty");
  const reason = (r: Record<string, unknown>[], epoch: string | null = null) => proof(ORG, epoch, CUT, r).reason;
  expect(reason([row({ session_erasure_runs: "1" })])).toBe("erasures_in_window");
  expect(reason([row({ status: status(null) })])).toBe("hold_missing");
  expect(reason([row({ status: status({ ...sealedHold(), state: "released" }) })])).toBe("hold_missing");
  expect(reason([row()], E2)).toBe("hold_epoch_mismatch");
  expect(reason([row({ status: status({ ...sealedHold(), state: "held", sealed_at: null }) })])).toBe("hold_not_sealed");
  expect(reason([row({ read_at_ms: String(SEALED - 1) })])).toBe("read_before_seal");
  // 062340's erasure history: bound at exactly this cutoff (SQL decides history_cutoff_matches), covered, recording,
  // self-consistent; otherwise held with a closed reason. Without it (062320 alone) the proof is snapshot-only.
  const eh = (h: Record<string, unknown>, erasures: unknown = 0, matches: unknown = true) =>
    row({ status: { hold: sealedHold(), erasures, selected_full: selected(CUT), erasure_history: { fence_at: "2026-11-27T06:00:00Z", covered: true, recording: true, by_source: {},
      coverage_started_at: "2026-11-20T00:00:00Z", ...h } }, history_cutoff_matches: matches, history_full_ordered: true });
  expect(proof(ORG, null, CUT, [eh({})]).watermark_kind).toBe("erasure_history_all_sources");
  expect(reason([eh({})])).toBe("ok");
  expect(reason([eh({ fence_at: null }, null, null)])).toBe("history_unbound");
  expect(reason([eh({}, 0, false)])).toBe("history_cutoff_mismatch");
  // The selected FULL is compared exactly (run, started_ms, evidence) and never to the fence; unbound when absent.
  const withFull = (sf: unknown) => row({ status: { hold: sealedHold(), erasures: 0, selected_full: sf, erasure_history: { fence_at: "2026-11-27T06:00:00Z", covered: true,
    recording: true, by_source: {}, coverage_started_at: "2026-11-20T00:00:00Z" } }, history_cutoff_matches: true, history_full_ordered: true });
  expect(reason([withFull(selected(CUT))])).toBe("ok");
  expect(reason([withFull(null)])).toBe("history_unbound");
  for (const bad of [{ run: "999" }, { started_ms: startedFor(CUT) + 1 }, { started_ms: startedFor(CUT) - 1 }, { evidence_sha256: "f".repeat(64) }, { run: 444 }])
    expect(reason([withFull(selected(CUT, bad))])).toBe("history_full_mismatch");
  expect(erasureWindowProof(ORG, null, CUT, [withFull(selected(CUT))], FULL, null, startedFor(CUT)).reason).toBe("history_full_mismatch"); // no evidence bound
  expect(erasureWindowProof(ORG, null, CUT, [withFull(selected(CUT))], FULL, EV, null).reason).toBe("history_full_mismatch"); // no start bound
  expect(reason([eh({ covered: false })])).toBe("history_not_covering_cutoff");
  expect(reason([eh({ recording: false })])).toBe("history_not_recording");
  expect(reason([eh({ by_source: { upload_deletions: 2 } }, 2)])).toBe("erasures_in_history");
  expect(proof(ORG, null, CUT, [eh({ by_source: { upload_deletions: 2 } }, 2)]).status).toBe("erasure_window_nonempty");
  // Inconsistent or malformed history never proves anything.
  for (const bad of [eh({ by_source: { upload_deletions: 1 } }, 2), eh({ by_source: { not_a_source: 1 } }, 1), eh({ by_source: null }), eh({ covered: "yes" }),
    eh({ recording: null }), eh({}, 0, "yes"), eh({}, -1)])
    expect(proof(ORG, null, CUT, [bad]).status).toBe("erasure_window_unproven");
  // 062320 alone (no erasure_history): snapshot-only, and a stray cutoff comparison is inconsistent.
  expect(proof(ORG, null, CUT, [row()]).watermark_kind).toBe("surviving_two_tables");
  expect(proof(ORG, null, CUT, [row({ history_cutoff_matches: true })]).status).toBe("erasure_window_unproven");
  for (const bad of [[], [row(), row()], [row({ upload_resets: "-1" })], [row({ upload_resets: undefined })], [row({ status: "{" })], [row({ status: { hold: null } })],
    [row({ read_at_ms: "x" })]])
    expect(proof(ORG, null, CUT, bad).status).toBe("erasure_window_unproven");
  expect(codeSync(() => proof("org-a", null, CUT, [row()]))).toBe("erasure_window_input");
  // A marker_attempted hold also proves (the release happens from there).
  expect(proof(ORG, null, CUT, [row({ status: status(sealedHold({ state: "marker_attempted", marker_attempted_at: new Date(SEALED + 1).toISOString() })) })]).status)
    .toBe("erasure_window_empty");
  // An unbound epoch (an unreadable hold) never validates as empty.
  const unproven = proof(ORG, null, CUT, []);
  expect(unproven.epoch_hash).toBeNull();
  expect(validErasureWindowProof({ ...unproven, status: "erasure_window_empty", reason: "ok" })).toBe(false);
});

test("release on an empty window: only a valid, empty, all-source-history proof at this org, epoch and cutoff, fresh, with the hold marker_attempted", () => {
  const attemptedHold = sealedHold({ state: "marker_attempted", marker_attempted_at: new Date(SEALED + 1).toISOString() });
  const hist = (over: Record<string, unknown> = {}) => ({ fence_at: "2026-11-27T06:00:00Z", covered: true, recording: true, by_source: {},
    coverage_started_at: "2026-11-20T00:00:00Z", ...over });
  const at = (hold: Record<string, unknown>, eh: Record<string, unknown> = {}, erasures = 0, extra: Record<string, unknown> = {}) =>
    erasureWindowProof(ORG, E, CUT, [row({ status: { hold, erasures, selected_full: selected(CUT), erasure_history: hist(eh) }, history_cutoff_matches: true,
      history_full_ordered: true, ...extra })], FULL, EV, startedFor(CUT));
  const good = at(attemptedHold);
  expect([good.status, good.reason, good.watermark_kind, good.hold_state]).toEqual(["erasure_window_empty", "ok", "erasure_history_all_sources", "marker_attempted"]);
  expect(releaseOnEmptyWindow(good, ORG, E, CUT, NOW + 1)).toEqual({ ok: true, evidence: emptyReleaseEvidence(good) });
  expect(emptyReleaseEvidence(good)).toMatch(/^[0-9a-f]{64}$/u);
  const code = (p: unknown, org = ORG, epoch = E, cut = CUT, now = NOW + 1) => {
    const r = releaseOnEmptyWindow(p, org, epoch, cut, now);
    return r.ok ? "ok" : r.code;
  };
  // Not a proof, or a tampered one.
  for (const p of [null, {}, "x", { ...good, watermark_erasures: 1 }, { ...good, status: "erasure_window_empty", reason: "erasures_in_history" }])
    expect(code(p)).toBe("release_proof_invalid");
  // 062320 alone (a snapshot of surviving rows) never releases, even empty.
  const snapshot = proof(ORG, E, CUT, [row({ status: status(attemptedHold, 0) })]);
  expect([snapshot.status, snapshot.watermark_kind]).toEqual(["erasure_window_empty", "surviving_two_tables"]);
  expect(code(snapshot)).toBe("release_watermark_partial");
  // Bound to exactly this org, epoch and FULL cutoff.
  expect(code(good, ORG2)).toBe("release_binding_mismatch");
  expect(code(good, ORG, E2)).toBe("release_binding_mismatch");
  expect(code(good, ORG, E, CUT + 1)).toBe("release_binding_mismatch");
  expect(code(good, "not-a-uuid")).toBe("release_binding_mismatch");
  // Fresh: read no longer ago than ERASURE_PROOF_MAX_AGE_MS (and not from the future).
  expect(code(good, ORG, E, CUT, NOW + ERASURE_PROOF_MAX_AGE_MS)).toBe("ok");
  expect(code(good, ORG, E, CUT, NOW + ERASURE_PROOF_MAX_AGE_MS + 1)).toBe("release_proof_stale");
  expect(code(good, ORG, E, CUT, NOW - 61_000)).toBe("release_proof_stale");
  // Anything in the window, in the history or in the snapshot: not empty.
  expect(code(at(attemptedHold, { by_source: { upload_deletions: 1 } }, 1))).toBe("release_window_nonempty");
  expect(code(at(attemptedHold, {}, 0, { upload_deletions: "1" }))).toBe("release_window_nonempty");
  // Unproven history (not recording, not covering, another FULL) never releases.
  expect(code(at(attemptedHold, { recording: false }))).toBe("release_window_unproven");
  expect(code(at(attemptedHold, { covered: false }))).toBe("release_window_unproven");
  expect(code(erasureWindowProof(ORG, E, CUT, [row({ status: { hold: attemptedHold, erasures: 0, selected_full: selected(CUT, { run: "999" }), erasure_history: hist() },
    history_cutoff_matches: true, history_full_ordered: true })], FULL, EV, startedFor(CUT)))).toBe("release_window_unproven");
  // Sealed but no marker attempted: 062340 _release refuses bridge_complete from it, so this does first.
  expect(code(at(sealedHold()))).toBe("release_hold_not_attempted");
  // No argument exists to assert history or a watermark.
  expect(releaseOnEmptyWindow.length).toBe(5);
  expect([...EMPTY_RELEASE_REFUSALS].sort()).toEqual(["release_binding_mismatch", "release_hold_not_attempted", "release_org_not_in_wave", "release_proof_invalid", "release_proof_stale",
    "release_sql_refused", "release_watermark_partial", "release_window_nonempty", "release_window_unproven"]);
});

test("real 062320/062340: the empty-window release takes the org key, re-reads the proof, and releases only an attempted, empty, bound epoch", async () => {
  const d = await realDb();
  const ORG5 = "55555555-2222-4333-8444-555555555555";
  const one = async (sql: string, p: unknown[] = []) => (await d.query<Record<string, unknown>>(sql, p)).rows[0]!;
  const e = (await one("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text e", [ORG5])).e as string;
  await d.query("SELECT pg_sleep(0.003)");
  await d.query("SELECT sessions.ingest_org_admission_hold_seal($1, $2::uuid, $3)", [ORG5, e, EV]);
  const started = Number((await one("SELECT floor(extract(epoch FROM installed_at) * 1000)::bigint + 1 AS ms FROM sessions.ingest_org_admission_holds WHERE epoch = $1::uuid", [e])).ms);
  const input = { org: ORG5, epoch: e, cutoffMs: started - 15 * 60_000, fullRun: FULL, fullEvidence: EV, fullStartedMs: started };
  const state = async () => (await one("SELECT state FROM sessions.ingest_org_admission_holds WHERE epoch = $1::uuid", [e])).state;
  const go = async (over: Partial<typeof input> = {}) => { await d.query("SELECT pg_sleep(0.003)"); return releaseEmptyWindow(d, { ...input, ...over }, Date.now, [orgHash(ORG5)]); };
  const codeOf = (r: Awaited<ReturnType<typeof go>>) => (r.status === "released" ? "released" : r.code);
  // On this head no org is in the wave: refused before the lock or any read, whatever the window.
  expect(EMPTY_RELEASE_WAVE_ORG_HASHES).toEqual([]);
  expect(codeOf(await releaseEmptyWindow(d, input))).toBe("release_org_not_in_wave");
  // Sealed, unbound: the proof is unproven.
  expect(codeOf(await go())).toBe("release_window_unproven");
  await d.query("SELECT sessions.ingest_org_erasure_history_bind($1, $2::uuid, $3, $4::bigint, $5)", [ORG5, e, FULL, String(started), EV]);
  // Bound but only sealed: 062340 releases bridge_complete only from marker_attempted.
  expect(codeOf(await go())).toBe("release_hold_not_attempted");
  await d.query(`SELECT sessions.ingest_org_admission_hold_marker_attempt($1, $2::uuid, '${"c".repeat(64)}')`, [ORG5, e]);
  // Another FULL, another epoch: refused, and nothing written.
  expect(codeOf(await go({ fullRun: "445" }))).toBe("release_window_unproven");
  expect(codeOf(await go({ epoch: E2 }))).toBe("release_window_unproven");
  expect(await state()).toBe("marker_attempted");
  // An erasure after the fence: not empty, the org stays held.
  await d.query("INSERT INTO sessions.upload_deletions (org_id, workspace_id, user_id, origin_id, source, session_id) VALUES ($1, 'w', 'u', 'o', 'claude', 's-rel')", [ORG5]);
  expect(codeOf(await go())).toBe("release_window_nonempty");
  expect(await state()).toBe("marker_attempted");
});

test("real 062320/062340: an empty window on an attempted, bound epoch releases with bridge_complete / erasure_window_empty and the proof's evidence", async () => {
  const d = await realDb();
  const ORG6 = "66666666-2222-4333-8444-555555555555";
  const one = async (sql: string, p: unknown[] = []) => (await d.query<Record<string, unknown>>(sql, p)).rows[0]!;
  const e = (await one("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text e", [ORG6])).e as string;
  await d.query("SELECT pg_sleep(0.003)");
  await d.query("SELECT sessions.ingest_org_admission_hold_seal($1, $2::uuid, $3)", [ORG6, e, EV]);
  const started = Number((await one("SELECT floor(extract(epoch FROM installed_at) * 1000)::bigint + 1 AS ms FROM sessions.ingest_org_admission_holds WHERE epoch = $1::uuid", [e])).ms);
  await d.query("SELECT sessions.ingest_org_erasure_history_bind($1, $2::uuid, $3, $4::bigint, $5)", [ORG6, e, FULL, String(started), EV]);
  await d.query(`SELECT sessions.ingest_org_admission_hold_marker_attempt($1, $2::uuid, '${"c".repeat(64)}')`, [ORG6, e]);
  await d.query("SELECT pg_sleep(0.003)");
  const r = await releaseEmptyWindow(d, { org: ORG6, epoch: e, cutoffMs: started - 15 * 60_000, fullRun: FULL, fullEvidence: EV, fullStartedMs: started }, Date.now, [orgHash(ORG6)]);
  expect(r.status).toBe("released");
  if (r.status !== "released") return;
  expect(r.evidence).toBe(emptyReleaseEvidence(r.proof));
  const h = await one("SELECT state, release_reason, release_evidence_sha256 FROM sessions.ingest_org_admission_holds WHERE epoch = $1::uuid", [e]);
  expect([h.state, h.release_reason, h.release_evidence_sha256]).toEqual(["released", "bridge_complete", r.evidence]);
  expect((await one("SELECT detail FROM sessions.ingest_org_admission_hold_events WHERE epoch = $1::uuid AND kind = 'released'", [e])).detail).toBe("erasure_window_empty");
  // A second release of the released epoch: the hold is released, so the proof reads hold_missing; nothing changes.
  const again = await releaseEmptyWindow(d, { org: ORG6, epoch: e, cutoffMs: started - 15 * 60_000, fullRun: FULL, fullEvidence: EV, fullStartedMs: started }, Date.now, [orgHash(ORG6)]);
  expect(again.status).toBe("refused");
});

// ---- the mode ----

const sha = "1".repeat(40), FULL = "444", DRY = "111";
const SCOPE = { BACKLOG_OPS_ORGS: `${ORG2},${ORG}` };
const target = {
  BACKLOG_OPS_BUCKET: "96bulls-aws-sessions-prod-transcripts-000000000000", BACKLOG_OPS_REGION: "us-east-1",
  BACKLOG_OPS_KMS_KEY_ARN: "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-4000-8000-000000000000",
  BACKLOG_OPS_DRY_ROLE_ARN: "arn:aws:iam::000000000000:role/96bulls-aws-sessions-prod-backlog-ops-read",
  BACKLOG_OPS_EXECUTE_ROLE_ARN: "arn:aws:iam::000000000000:role/96bulls-aws-sessions-prod-backlog-ops-write",
};
const env = (mode: BacklogOpsMode, extra: Record<string, string> = {}) => ({ BACKLOG_OPS_MODE: mode, BACKLOG_OPS_SHA: sha, BACKLOG_OPS_FULL_BACKFILL_RUN: FULL,
  ...(mode === "bridge-execute" ? { BACKLOG_OPS_DRY_RUN: DRY, BACKLOG_OPS_CONFIRMATION: `BRIDGE-EXECUTE ${sha} DRY ${DRY} FULL ${FULL}` } : {}), ...extra });

test("bridge-dry: binds FULL (its t is the cutoff), scoped, the tool reads the proof itself; bridge-execute held", () => {
  const d = backlogOpsDispatch(env("bridge-dry"));
  expect(backlogOpsBindings(d).map((b) => [b.kind, b.mode])).toEqual([["full", "backfill-execute"]]);
  expect(codeSync(() => backlogOpsDispatch(env("bridge-dry", { BACKLOG_OPS_FULL_BACKFILL_RUN: "" })))).toBe("backlog_ops_full_backfill_run");
  expect(backlogOpsRevisionsSince({ BACKLOG_OPS_REVISIONS_SINCE_MS: String(CUT) }, d, NOW)).toBe(CUT);
  const t = backlogOpsTarget(target);
  expect(backlogOpsToolArgs(d, t, backlogOpsScope(SCOPE), CUT)).toEqual({ script: "scripts/backlog-authority.ts", needsDb: true,
    args: ["bridge-window", "--cutoff-ms", String(CUT), "--full-run", FULL, "--org", ORG, "--org", ORG2] });
  expect(codeSync(() => backlogOpsToolArgs(d, t, backlogOpsScope({}), CUT))).toBe("backlog_ops_scope_required");
  expect(codeSync(() => backlogOpsToolArgs(d, t, backlogOpsScope(SCOPE), null))).toBe("backlog_ops_revisions_since");
  expect(BACKLOG_OPS_BRIDGE_EXECUTE_ENABLED).toBe(false);
  expect(codeSync(() => backlogOpsDispatch(env("bridge-execute"), true, true))).toBe("backlog_ops_bridge_held");
  const x = backlogOpsDispatch(env("bridge-execute"), true, true, true);
  expect(x.confirmation).toBe(backlogOpsConfirmation(x));
  expect(backlogOpsBindings(x).map((b) => [b.kind, b.mode])).toEqual([["dry", "bridge-dry"], ["full", "backfill-execute"]]);
});

test("bridge-dry's receipt: the tool's own read, an exact schema; a caller cannot supply the set", async () => {
  const calls: unknown[][] = [];
  const sql = async (text: string, params: readonly (string | null)[]) => {
    calls.push([text === ERASURE_WINDOW_SQL, params]);
    return params[0] === ORG ? [row()] : [row({ upload_deletions: "1" })];
  };
  const s = await bridgeWindow([ORG, ORG2], CUT, sql);
  expect(calls).toEqual([[true, [ORG, String(CUT)]], [true, [ORG2, String(CUT)]]]);
  expect(s.all_empty).toBe(false);
  expect(s.per_org.map((p) => p.status)).toEqual(["erasure_window_empty", "erasure_window_nonempty"]);
  const r = authorityReceiptSummary("bridge", JSON.parse(JSON.stringify(s)), { orgs: [ORG, ORG2], cutoffMs: CUT });
  expect(r.ok).toBe(true);
  expect(JSON.stringify(r.summary)).not.toContain(ORG);
  // An off-schema field, or a forged proof shape, refuses the receipt.
  const forged = JSON.parse(JSON.stringify(s));
  forged.per_org[1].status = "erasure_window_empty_please";
  expect(authorityReceiptSummary("bridge", forged, { orgs: [ORG, ORG2], cutoffMs: CUT }).summary).toEqual({ refused: "backlog_ops_receipt_schema" });
  // A read that fails is unproven for that org, never empty.
  const failed = await bridgeWindow([ORG], CUT, async () => { throw new Error("boom"); });
  expect([failed.all_empty, failed.per_org[0]!.status]).toEqual([false, "erasure_window_unproven"]);
  expect(orgHash(ORG)).toBe(s.per_org[0]!.org_hash);
});

test("bridge-dry through the normal runner (scripts/backlog-ops.ts): the logged line and the persisted file carry the proof; full_run recorded", async () => {
  const temp = mkdtempSync(join(tmpdir(), "bo-bridge-"));
  try {
    const home = backlogOpsHomeId({ bucket: target.BACKLOG_OPS_BUCKET, region: target.BACKLOG_OPS_REGION }, null);
    const scopeId = backlogOpsScope(SCOPE).id;
    const fullSummary = { mode: "execute", users: 0, fences: Object.fromEntries(["deletion_rows", "reset_rows", "created", "updated", "unchanged", "would_create", "would_update",
      "pairs_added", "resets_added", "deletions_superseded", "deletions_unproven", "cold_resets", "needs_review", "unreadable", "contended", "tenant_invalid", "input_invalid",
      "failed", "resets_in_flight"].map((k) => [k, 0])), ownership: { rows: 0, registered: 0, would_register: 0, unchanged: 0, skipped_deleted: 0, skipped_no_fence: 0, invalid: 0,
      failed: 0, failed_codes: {} }, revisions: { rows: 0, seed_rows: 0, slot_rows: 0, slots_empty: 0, merged: 0, would_merge: 0, skipped_deleted: 0, skipped_no_fence: 0,
      invalid: 0, shards: 0, failed: 0, failed_codes: {} }, sections: { fences: "complete", ownership: "complete", revisions: "complete" },
      fingerprint: Object.fromEntries(["revisions_count", "revisions_max_updated_ms", "revisions_hash", "ownership_count", "ownership_hash", "slots_owned_count",
        "slots_revision_sum", "slots_hash"].map((k) => [k, 0])) };
    mkdirSync(join(temp, "backlog-ops-bound", "full"), { recursive: true });
    writeFileSync(join(temp, "backlog-ops-bound", "full", "backlog-ops-receipt.json"),
      JSON.stringify({ contract: BACKLOG_OPS_RECEIPT_CONTRACT, mode: "backfill-execute", sha, run: FULL, scope: scopeId, ledger: "none", home, exitCode: 0, summary: fullSummary }));
    const s = await bridgeWindow([ORG, ORG2], CUT, async () => [row()]);
    const xml = `<AssumeRoleWithWebIdentityResponse><Credentials><AccessKeyId>ASIAAAAAAAAAAAAAAAAA</AccessKeyId><SecretAccessKey>s</SecretAccessKey>
      <SessionToken>t</SessionToken><Expiration>${new Date(Date.now() + 6 * 3600_000).toISOString()}</Expiration></Credentials></AssumeRoleWithWebIdentityResponse>`;
    const fetcher = (async (url: string) => url.startsWith("https://token.example/") ? new Response(JSON.stringify({ value: "a.b.c" })) : new Response(xml)) as unknown as typeof fetch;
    const lines: string[] = [];
    const log = console.log;
    console.log = (...a: unknown[]) => { lines.push(a.join(" ")); };
    let exit: number;
    try {
      exit = await backlogOpsRun({ ...target, ...SCOPE, ...env("bridge-dry"), GITHUB_SHA: sha, GITHUB_RUN_ID: "900", RUNNER_TEMP: temp,
        BACKLOG_OPS_REVISIONS_SINCE_MS: String(CUT), ACTIONS_ID_TOKEN_REQUEST_URL: "https://token.example/x", ACTIONS_ID_TOKEN_REQUEST_TOKEN: "bearer",
        SUPABASE_MIGRATION_OWNER_URL: `postgresql://${P.owner}.${P.project}:p%40ss@${P.host}:${P.port}/${P.database}?sslmode=verify-full` },
        { fetcher, runChild: async (child) => {
          expect(child.args.slice(1, 6)).toEqual(["bridge-window", "--cutoff-ms", String(CUT), "--full-run", FULL]);
          // The bound FULL receipt's sha256 over its exact downloaded bytes, on stdin only (the seal's evidence to match).
          const raw = readFileSync(join(temp, "backlog-ops-bound", "full", "backlog-ops-receipt.json")); // exact bytes, no decoding
          const input = JSON.parse(child.input!);
          expect(Object.keys(input).sort()).toEqual(["dbUrl", "fullEvidence"]);
          expect(input.fullEvidence).toBe(createHash("sha256").update(raw).digest("hex"));
          expect(child.args.join(" ")).not.toContain(input.fullEvidence);
          return { code: 0, stdout: JSON.stringify(s) }; } });
    } finally { console.log = log; }
    const logged = JSON.parse(lines.find((l) => l.startsWith("backlog_ops_receipt "))!.slice("backlog_ops_receipt ".length));
    const saved = JSON.parse(readFileSync(join(temp, "backlog-ops-out", "backlog-ops-receipt.json"), "utf8"));
    expect(exit).toBe(0);
    expect(saved).toEqual(logged);
    expect(logged.summary.all_empty).toBe(true);
    expect(logged.summary.per_org.map((p: { status: string }) => p.status)).toEqual(["erasure_window_empty", "erasure_window_empty"]);
    expect(logged.full_run).toBe(FULL);
    expect(logged.home).toBe(home);
    const text = JSON.stringify(logged);
    for (const id of [ORG, ORG2, E]) expect(text).not.toContain(id);
  } finally { rmSync(temp, { recursive: true, force: true }); }
});

test("contradictory bridge evidence is refused at validation and at persistence (normal runner)", async () => {
  // The reviewer's negative: a real positive proof (history counted 2) relabelled empty/ok.
  const positive = proof(ORG, null, CUT, [row({ status: { hold: sealedHold(), erasures: 2, selected_full: selected(CUT), erasure_history: { fence_at: "2026-11-27T06:00:00Z", covered: true,
    recording: true, by_source: { upload_deletions: 2 }, coverage_started_at: "2026-11-20T00:00:00Z" } }, history_cutoff_matches: true, history_full_ordered: true })]);
  expect([positive.status, positive.reason, positive.watermark_erasures]).toEqual(["erasure_window_nonempty", "erasures_in_history", 2]);
  const relabelled = { ...positive, status: "erasure_window_empty", reason: "ok" };
  expect(validErasureWindowProof(relabelled)).toBe(false);
  expect(validErasureWindowProof({ ...positive, reason: "ok" })).toBe(false); // non-empty never claims ok
  const summary = (per: unknown[], over: Record<string, unknown> = {}) => ({ mode: "dry_run", tool: "bridge_window", org_count: per.length,
    all_empty: per.every((p) => (p as { status: string }).status === "erasure_window_empty"), per_org: per, ...over });
  const empty = proof(ORG2, null, CUT, [row()]);
  expect(authorityReceiptSummary("bridge", JSON.parse(JSON.stringify(summary([empty]))), { orgs: [ORG2], cutoffMs: CUT }).ok).toBe(true);
  for (const bad of [summary([relabelled], { all_empty: true }), summary([empty, positive], { all_empty: true }), summary([empty], { org_count: 2 }),
    summary([], { all_empty: true })])
    expect(authorityReceiptSummary("bridge", JSON.parse(JSON.stringify(bad)), { orgs: [ORG, ORG2], cutoffMs: CUT }).summary).toEqual({ refused: "backlog_ops_receipt_schema" });
  // The proofs must be for exactly the requested orgs, each once: a duplicate never substitutes for another org.
  const a = proof(ORG, null, CUT, [row()]), b = proof(ORG2, null, CUT, [row()]);
  expect(authorityReceiptSummary("bridge", JSON.parse(JSON.stringify(summary([a, b]))), { orgs: [ORG, ORG2], cutoffMs: CUT }).ok).toBe(true);
  for (const [per, orgs] of [[[a, a], [ORG, ORG2]], [[a], [ORG, ORG2]], [[a, b], [ORG]], [[a, b], [ORG, ORG]], [[a, b], []]] as const)
    expect(authorityReceiptSummary("bridge", JSON.parse(JSON.stringify(summary([...per]))), { orgs: [...orgs], cutoffMs: CUT }).summary).toEqual({ refused: "backlog_ops_receipt_schema" });
  expect(authorityReceiptSummary("bridge", JSON.parse(JSON.stringify(summary([a, b])))).summary).toEqual({ refused: "backlog_ops_receipt_schema" }); // no scope given
  // Every proof at exactly the run's cutoff: a later or an earlier one, or no cutoff given, refuses.
  for (const off of [1, -1]) {
    const shifted = { ...a, cutoff_ms: CUT + off, read_at_ms: CUT + 2 };
    expect(authorityReceiptSummary("bridge", JSON.parse(JSON.stringify(summary([shifted, b]))), { orgs: [ORG, ORG2], cutoffMs: CUT }).summary)
      .toEqual({ refused: "backlog_ops_receipt_schema" });
  }
  expect(authorityReceiptSummary("bridge", JSON.parse(JSON.stringify(summary([a, b]))), { orgs: [ORG, ORG2] }).summary).toEqual({ refused: "backlog_ops_receipt_schema" });
  // An empty proof must have been read: no read time, or a cutoff after it, is invalid.
  expect(validErasureWindowProof({ ...a, read_at_ms: null })).toBe(false);
  expect(validErasureWindowProof({ ...a, read_at_ms: 0 })).toBe(false);
  expect(validErasureWindowProof({ ...a, cutoff_ms: (a.read_at_ms as number) + 1 })).toBe(false);
  expect(authorityReceiptSummary("bridge", JSON.parse(JSON.stringify(summary([{ ...a, read_at_ms: null }, b]))), { orgs: [ORG, ORG2], cutoffMs: CUT }).summary)
    .toEqual({ refused: "backlog_ops_receipt_schema" });
  // A legacy two-table snapshot may carry a nonzero historical total and still be empty.
  const legacy = proof(ORG, null, CUT, [row({ status: status(sealedHold(), 5) })]);
  expect([legacy.status, legacy.watermark_kind, legacy.watermark_erasures, validErasureWindowProof(legacy)]).toEqual(["erasure_window_empty", "surviving_two_tables", 5, true]);
  // Through the normal runner: the contradictory receipt is refused in both the logged line and the saved file.
  const temp = mkdtempSync(join(tmpdir(), "bo-bridge-bad-"));
  try {
    const home = backlogOpsHomeId({ bucket: target.BACKLOG_OPS_BUCKET, region: target.BACKLOG_OPS_REGION }, null);
    mkdirSync(join(temp, "backlog-ops-bound", "full"), { recursive: true });
    const fullSummary = { mode: "execute", users: 0, fences: Object.fromEntries(["deletion_rows", "reset_rows", "created", "updated", "unchanged", "would_create", "would_update",
      "pairs_added", "resets_added", "deletions_superseded", "deletions_unproven", "cold_resets", "needs_review", "unreadable", "contended", "tenant_invalid", "input_invalid",
      "failed", "resets_in_flight"].map((k) => [k, 0])), ownership: { rows: 0, registered: 0, would_register: 0, unchanged: 0, skipped_deleted: 0, skipped_no_fence: 0, invalid: 0,
      failed: 0, failed_codes: {} }, revisions: { rows: 0, seed_rows: 0, slot_rows: 0, slots_empty: 0, merged: 0, would_merge: 0, skipped_deleted: 0, skipped_no_fence: 0,
      invalid: 0, shards: 0, failed: 0, failed_codes: {} }, sections: { fences: "complete", ownership: "complete", revisions: "complete" },
      fingerprint: Object.fromEntries(["revisions_count", "revisions_max_updated_ms", "revisions_hash", "ownership_count", "ownership_hash", "slots_owned_count",
        "slots_revision_sum", "slots_hash"].map((k) => [k, 0])) };
    writeFileSync(join(temp, "backlog-ops-bound", "full", "backlog-ops-receipt.json"),
      JSON.stringify({ contract: BACKLOG_OPS_RECEIPT_CONTRACT, mode: "backfill-execute", sha, run: FULL, scope: backlogOpsScope(SCOPE).id, ledger: "none", home, exitCode: 0, summary: fullSummary }));
    const xml = `<AssumeRoleWithWebIdentityResponse><Credentials><AccessKeyId>ASIAAAAAAAAAAAAAAAAA</AccessKeyId><SecretAccessKey>s</SecretAccessKey>
      <SessionToken>t</SessionToken><Expiration>${new Date(Date.now() + 6 * 3600_000).toISOString()}</Expiration></Credentials></AssumeRoleWithWebIdentityResponse>`;
    const fetcher = (async (url: string) => url.startsWith("https://token.example/") ? new Response(JSON.stringify({ value: "a.b.c" })) : new Response(xml)) as unknown as typeof fetch;
    const lines: string[] = [];
    const log = console.log;
    console.log = (...a: unknown[]) => { lines.push(a.join(" ")); };
    const runForged = async (forged: unknown, runId: string) => {
      lines.length = 0;
      let code: number;
      const tempOut = join(temp, "backlog-ops-out");
      rmSync(tempOut, { recursive: true, force: true });
      console.log = (...a: unknown[]) => { lines.push(a.join(" ")); };
      try {
        code = await backlogOpsRun({ ...target, ...SCOPE, ...env("bridge-dry"), GITHUB_SHA: sha, GITHUB_RUN_ID: runId, RUNNER_TEMP: temp,
          BACKLOG_OPS_REVISIONS_SINCE_MS: String(CUT), ACTIONS_ID_TOKEN_REQUEST_URL: "https://token.example/x", ACTIONS_ID_TOKEN_REQUEST_TOKEN: "bearer",
          SUPABASE_MIGRATION_OWNER_URL: `postgresql://${P.owner}.${P.project}:p%40ss@${P.host}:${P.port}/${P.database}?sslmode=verify-full` },
          { fetcher, runChild: async () => ({ code: 0, stdout: JSON.stringify(forged) }) });
      } finally { console.log = log; }
      const lg = JSON.parse(lines.find((l) => l.startsWith("backlog_ops_receipt "))!.slice("backlog_ops_receipt ".length));
      const sv = JSON.parse(readFileSync(join(tempOut, "backlog-ops-receipt.json"), "utf8"));
      return { code, lg, sv };
    };
    // The reviewer's two: a duplicate proof standing in for the second org, and an empty proof with no read time.
    const e1 = proof(ORG, null, CUT, [row()]), e2 = proof(ORG2, null, CUT, [row()]);
    // ... and the cutoff case: A read at CUT+1 (later) or CUT-1 (earlier) beside a valid B.
    for (const [i, forgedOne] of [summary([e1, e1]), summary([{ ...e1, read_at_ms: null }, e2]),
      summary([{ ...e1, cutoff_ms: CUT + 1, read_at_ms: CUT + 2 }, e2]), summary([{ ...e1, cutoff_ms: CUT - 1 }, e2])].entries()) {
      const r1 = await runForged(forgedOne, String(910 + i));
      expect(r1.code).toBe(1);
      expect(r1.lg.summary).toEqual({ refused: "backlog_ops_receipt_schema" });
      expect(r1.sv).toEqual(r1.lg);
    }
    // And the legitimate pair is accepted in both sinks.
    const ok = await runForged(summary([e1, e2]), "920");
    expect([ok.code, ok.lg.summary.all_empty, ok.sv.summary.per_org.length]).toEqual([0, true, 2]);
    rmSync(join(temp, "backlog-ops-out"), { recursive: true, force: true });
    lines.length = 0;
    let exit: number;
    const forged = summary([relabelled, empty], { all_empty: true });
    console.log = (...a: unknown[]) => { lines.push(a.join(" ")); };
    try {
      exit = await backlogOpsRun({ ...target, ...SCOPE, ...env("bridge-dry"), GITHUB_SHA: sha, GITHUB_RUN_ID: "901", RUNNER_TEMP: temp,
        BACKLOG_OPS_REVISIONS_SINCE_MS: String(CUT), ACTIONS_ID_TOKEN_REQUEST_URL: "https://token.example/x", ACTIONS_ID_TOKEN_REQUEST_TOKEN: "bearer",
        SUPABASE_MIGRATION_OWNER_URL: `postgresql://${P.owner}.${P.project}:p%40ss@${P.host}:${P.port}/${P.database}?sslmode=verify-full` },
        { fetcher, runChild: async () => ({ code: 0, stdout: JSON.stringify(forged) }) });
    } finally { console.log = log; }
    const logged = JSON.parse(lines.find((l) => l.startsWith("backlog_ops_receipt "))!.slice("backlog_ops_receipt ".length));
    const saved = JSON.parse(readFileSync(join(temp, "backlog-ops-out", "backlog-ops-receipt.json"), "utf8"));
    expect(exit).toBe(1);
    expect(logged.summary).toEqual({ refused: "backlog_ops_receipt_schema" });
    expect(saved).toEqual(logged);
  } finally { rmSync(temp, { recursive: true, force: true }); }
});

test("the bound FULL receipt is hashed over its exact bytes; bytes that do not round-trip the checked text refuse", async () => {
  const { checkBoundReceipts } = await import("../../../scripts/backlog-ops.ts");
  const { backlogOpsDispatch, BACKLOG_OPS_RECEIPT_CONTRACT: C } = await import("../backlog-ops.ts");
  const home = backlogOpsHomeId({ bucket: target.BACKLOG_OPS_BUCKET, region: target.BACKLOG_OPS_REGION }, null);
  const full = JSON.stringify({ contract: C, mode: "backfill-execute", sha, run: FULL, scope: backlogOpsScope(SCOPE).id, ledger: "none", home, exitCode: 0,
    summary: { mode: "execute", sections: { fences: "complete", ownership: "complete", revisions: "complete" } } });
  const e = { ...target, ...SCOPE, ...env("bridge-dry"), RUNNER_TEMP: "/tmp/rt-bh" };
  const d = backlogOpsDispatch(e);
  const read = () => full;
  const exact = new TextEncoder().encode(full);
  expect(checkBoundReceipts(e, d, read, CUT, () => exact).fullEvidence).toBe(createHash("sha256").update(exact).digest("hex"));
  // An invalid UTF-8 byte decodes to U+FFFD in the text, so the raw bytes no longer equal the checked text: refused.
  const bad = new Uint8Array([...exact.slice(0, -1), 0xff, 0x7d]);
  expect(() => checkBoundReceipts(e, d, read, CUT, () => bad)).toThrow("backlog_ops_bound_full_receipt");
});
