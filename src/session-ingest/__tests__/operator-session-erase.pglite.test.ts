/**
 * 20261105050000_operator_session_erase (M1) and 20261105050010_operator_session_erase_fence_check (M2)
 * in PGlite, every migration plus the ingest role, called AS bulls96_session_ingest (SET ROLE).
 * Single connection: lock ORDER is pinned by the owner enumeration and the function body, not by a
 * second connection (PGlite has one). Nothing here touches a real database.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import type { ClaimInput, LedgerRecord } from "../ledger.ts";
import { sqlLedger, type SqlRun } from "../ledger-sql.ts";
import { TURN_ORIGIN_ID } from "../turns.ts";
import { REPO_ROOT, applySupabaseSchema } from "./pglite-schema.ts";

setDefaultTimeout(300_000);
const read = (p: string) => readFileSync(join(REPO_ROOT, p), "utf8");
function sql(p: string): string {
  return read(p).split("\n").map((l) => (l.startsWith("\\ir ") ? sql(join(dirname(p), l.slice(4).trim())) : l.startsWith("\\") ? "" : l)).join("\n");
}
const open: PGlite[] = [];
afterEach(async () => { await Promise.all(open.splice(0).map((d) => d.close())); });

const ORG = "org-a", WS = "ws-a", SRC = "claude", SID = "s-1";
const KEY = [ORG, WS, SRC, SID] as const;
const MARKER = "9007199254740991";
const hex = (c: string) => c.repeat(64);

type Db = { db: PGlite; run: SqlRun; as: (role: string) => SqlRun };
async function database(): Promise<Db> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await applySupabaseSchema(db, { until: null });
  await db.exec(sql("supabase/bootstrap/session_ingest_role.sql"));
  let chain: Promise<unknown> = Promise.resolve();
  const as = (role: string): SqlRun => (text, params) => {
    const next = chain.then(async () => {
      await db.exec(`SET ROLE ${role}`);
      try { return (await db.query(text, params as never)) as { rows: Record<string, unknown>[] }; }
      finally { await db.exec("RESET ROLE"); }
    });
    chain = next.catch(() => {});
    return next;
  };
  return { db, run: as("bulls96_session_ingest"), as };
}

/** A real direct-mode claim + commit through the ledger: ownership and stored revisions for `user`. */
async function land(d: Db, user: string, origin: string, sessions: [string, string][], batchId: string) {
  const tokens = new Map<string, string>();
  const ledger = sqlLedger(async (text, params) => {
    const out = await d.run(text, params);
    const token = (out.rows[0]?.result as { claimToken?: string } | undefined)?.claimToken;
    if (text.includes("ingest_claim_batch") && token) tokens.set(batchId, token);
    return out;
  });
  const records: LedgerRecord[] = sessions.map(([source, sessionId], i) => ({
    source, sessionId, recordId: `${batchId}-r${i}`, revisionId: `${batchId}-r${i}:1`, revision: 1, digest: hex(String(i % 10)),
  }));
  const c: ClaimInput = { tenant: { orgId: ORG, workspaceId: WS, userId: user }, originId: origin, batchId, batchDigest: hex("a"),
    receiptId: `rhr_${hex("e")}`, sessions, records };
  const r = await ledger.claim(c);
  if (r.kind !== "claimed" && r.kind !== "resume") throw new Error(`expected a claim, got ${r.kind}`);
  await ledger.commit({ tenant: c.tenant, originId: origin, batchId, batchDigest: c.batchDigest, claimToken: tokens.get(batchId)!,
    sessions, records, machineId: `srv_${"ab".repeat(16)}` });
}
async function pointer(d: Db, user: string, session: string, machine: string, canonical: string | null = null, ws = WS) {
  await d.db.query(
    `INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,
       storage_provider,bucket,manifest_key,content_sha256,raw_format,trace_id,revision,canonical_session_id)
     VALUES($1,$2,$3,$4,$5,$6,now(),now(),'s3','bucket','k/'||$4||$5,decode(repeat('ab',32),'hex'),'jsonl','019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18',2,$7)`,
    [ORG, ws, SRC, session, machine, user, canonical]);
}
async function directory(d: Db, session: string, user?: string) {
  await d.db.query(`INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at,short_summary,
      short_summary_scrub_ruleset,short_summary_scrub_version,short_summary_source,short_summary_generated_at)
    VALUES($1,$2,$3,$4,now(),now(),'a summary','relayhistory-scrub',1,'test/v1',now())`, [ORG, WS, SRC, session]);
  if (user) await d.db.query(`INSERT INTO sessions.session_directory_users(org_id,workspace_id,harness,session_id,user_id,last_activity_at)
    VALUES($1,$2,$3,$4,$5,now())`, [ORG, WS, SRC, session, user]);
}
type Receipt = { status: string; requestId: string; unreadable: boolean; bytesErased: boolean; uploadsFenced: boolean; attempts: number;
  artifacts: Record<string, { count: number; state: string }>; failure: { step: string; code: string; retryable: boolean } | null;
  legacyScope: string; actor: { kind: string; id: string }; completedAt: string | null };
const erase = async (d: Db, request = "req-1", key: readonly string[] = KEY, actor: [string, string] = ["user", "operator-1"]) =>
  (await d.run("SELECT sessions.ingest_operator_erase_session($1,$2,$3,$4,$5,$6,$7) AS r", [...key, request, ...actor])).rows[0]!.r as Receipt;
const receipt = async (d: Db, key: readonly string[] = KEY) =>
  (await d.run("SELECT sessions.ingest_operator_erase_receipt($1,$2,$3,$4) AS r", [...key])).rows[0]!.r as Receipt | null;
const step = async (d: Db, s: string, state: string, count: number | null, code: string | null = null, key: readonly string[] = KEY) =>
  (await d.run("SELECT sessions.ingest_operator_erase_record_step($1,$2,$3,$4,$5,$6,$7,$8) AS r", [...key, s, state, count, code])).rows[0]!.r as Receipt;
const BYTE_KEYS = ["transcriptObjects", "stagedObjects", "bufferedBatches", "bufferedRows", "catalogRows", "largeReadJobs", "searchDocuments",
  "briefDocuments", "summaries", "briefs", "replayPublications", "analysisPublications", "historyDerivedObjects"];
const rows = async <T>(d: Db, q: string, p: unknown[] = []) => (await d.db.query<T>(q, p)).rows;
const events = async (d: Db) => (await rows<{ kind: string }>(d, "SELECT kind FROM sessions.session_erasure_events ORDER BY event_id")).map((e) => e.kind);
const code = (p: Promise<unknown>) => p.then(() => "ok", (e: { code?: string }) => e.code);

describe("050000 operator per-session erase", () => {
  test("fences every owner (ownership, pointer and directory users) in user id order, tombstones every copy, keeps the rest", async () => {
    const d = await database();
    await land(d, "u-b", "origin-1", [[SRC, SID], [SRC, "s-other"]], "b1");
    await land(d, "U-a", "origin-2", [[SRC, SID]], "b2");
    await pointer(d, "u-b", SID, "m-1");
    await pointer(d, "U-a", "s-alias", "m-2", SID);       // an alias naming the key as its canonical session
    await pointer(d, "u-c", SID, "m-3");                  // pointer-only owner
    await pointer(d, "u-b", "s-other", "m-1");            // another session: untouched
    await pointer(d, "u-b", SID, "m-1", null, "ws-b");     // another workspace: untouched
    await directory(d, SID, "u-d");                       // directory-only owner
    await directory(d, "s-other");

    // Owner enumeration is the lock order: COLLATE "C" (upper case first), every source of owners.
    const owners = (await rows<{ o: string[] }>(d, "SELECT sessions.ingest_operator_erase_owners($1,$2,$3,ARRAY[$4,'s-alias']) o", [...KEY]))[0]!.o;
    expect(owners).toEqual(["U-a", "u-b", "u-c", "u-d"]);
    const body = (await rows<{ s: string }>(d, "SELECT prosrc s FROM pg_proc WHERE oid='sessions.ingest_operator_erase_fence(text,text,text,text,text[])'::regprocedure"))[0]!.s;
    expect(body.indexOf("ingest_lock_fences")).toBeLessThan(body.indexOf("ingest_set_session_uploads"));

    const r = await erase(d);
    expect(r).toMatchObject({ status: "in_progress", requestId: "req-1", uploadsFenced: true, unreadable: true, bytesErased: false,
      legacyScope: "excluded", actor: { kind: "user", id: "operator-1" }, failure: null, attempts: 1 });
    expect(r.artifacts.pointerRows).toEqual({ count: 3, state: "done" });
    expect(r.artifacts.directoryRows).toEqual({ count: 1, state: "done" });
    expect(r.artifacts.ledgerRows.state).toBe("done");
    expect(r.artifacts.ledgerRows.count).toBe(2);         // one stored revision each for u-b and U-a
    for (const k of BYTE_KEYS) expect(r.artifacts[k]).toEqual({ count: 0, state: "pending" });

    // Fences: the key and the alias.
    expect(await rows(d, "SELECT session_id FROM sessions.session_erasure_fences ORDER BY 1")).toEqual([{ session_id: SID }, { session_id: "s-alias" }]);
    // Every owner is fence-locked and fenced from its upload origins plus the turns origin.
    expect((await rows<{ user_id: string }>(d, "SELECT user_id FROM sessions.ingest_fence_owners ORDER BY user_id COLLATE \"C\"")).map((x) => x.user_id))
      .toEqual(["U-a", "u-b", "u-c", "u-d"]);
    const fv = await rows<{ user_id: string; origin_id: string; session_id: string }>(d,
      `SELECT user_id,origin_id,session_id FROM sessions.ingest_fence_versions WHERE session_id=$1 ORDER BY user_id COLLATE "C",origin_id COLLATE "C"`, [SID]);
    expect(fv).toEqual([
      { user_id: "U-a", origin_id: "origin-2", session_id: SID }, { user_id: "U-a", origin_id: TURN_ORIGIN_ID, session_id: SID },
      { user_id: "u-b", origin_id: "origin-1", session_id: SID }, { user_id: "u-b", origin_id: TURN_ORIGIN_ID, session_id: SID },
      { user_id: "u-c", origin_id: TURN_ORIGIN_ID, session_id: SID }, { user_id: "u-d", origin_id: TURN_ORIGIN_ID, session_id: SID }]);
    expect(await rows(d, "SELECT 1 FROM sessions.ingest_session_ownership WHERE session_id=$1", [SID])).toEqual([]);
    expect(await rows(d, "SELECT session_id FROM sessions.ingest_session_ownership")).toEqual([{ session_id: "s-other" }]);
    // Pointer rows: tombstoned with the marker revision, every machine and the alias; others live.
    expect(await rows(d, `SELECT session_id,machine_id,workspace_id,deleted_at IS NOT NULL dead,revision::text rev FROM sessions.session_index ORDER BY 3,1,2`)).toEqual([
      { session_id: SID, machine_id: "m-1", workspace_id: WS, dead: true, rev: MARKER },
      { session_id: SID, machine_id: "m-3", workspace_id: WS, dead: true, rev: MARKER },
      { session_id: "s-alias", machine_id: "m-2", workspace_id: WS, dead: true, rev: MARKER },
      { session_id: "s-other", machine_id: "m-1", workspace_id: WS, dead: false, rev: "2" },
      { session_id: SID, machine_id: "m-1", workspace_id: "ws-b", dead: false, rev: "2" }]);
    expect(await rows(d, "SELECT session_id,deleted_at IS NOT NULL dead,short_summary FROM sessions.session_directory ORDER BY 1")).toEqual([
      { session_id: SID, dead: true, short_summary: null }, { session_id: "s-other", dead: false, short_summary: "a summary" }]);
    expect(await events(d)).toEqual(["requested", "fenced"]);
  });

  test("a repeat with the same or another request id returns the existing receipt and erases nothing again", async () => {
    const d = await database();
    await pointer(d, "u-1", SID, "m-1");
    const first = await erase(d, "req-1");
    const epochs = await rows(d, "SELECT user_id,origin_id,epoch FROM sessions.ingest_fence_versions ORDER BY 1,2");
    await pointer(d, "u-2", SID, "m-9").catch(() => {}); // whatever happens to a late pointer, a repeat does not erase again
    const same = await erase(d, "req-1"), other = await erase(d, "req-2", KEY, ["bootstrap", "boot"]);
    expect(same).toEqual(first);
    expect(other).toEqual(first);                         // requestId and actor stay the first call's
    expect(await rows(d, "SELECT user_id,origin_id,epoch FROM sessions.ingest_fence_versions ORDER BY 1,2")).toEqual(epochs);
    expect(await events(d)).toEqual(["requested", "fenced", "repeated", "repeated"]);
    expect(await rows(d, "SELECT count(*)::int n FROM sessions.session_erasure_receipts")).toEqual([{ n: 1 }]);
    // A request id already used for another key is refused.
    expect(await code(erase(d, "req-1", [ORG, WS, SRC, "s-2"]))).toBe("22023");
  });

  test("an unknown session is still fenced and gets a zero-count receipt", async () => {
    const d = await database();
    expect(await receipt(d)).toBeNull();
    const r = await erase(d);
    expect(r).toMatchObject({ status: "in_progress", uploadsFenced: true, unreadable: true, bytesErased: false });
    for (const k of ["pointerRows", "directoryRows", "ledgerRows"]) expect(r.artifacts[k]).toEqual({ count: 0, state: "done" });
    expect(await rows(d, "SELECT org_id,workspace_id,source,session_id FROM sessions.session_erasure_fences"))
      .toEqual([{ org_id: ORG, workspace_id: WS, source: SRC, session_id: SID }]);
    expect(await receipt(d)).toEqual(r);
  });

  test("an empty workspace id is a valid key", async () => {
    const d = await database();
    expect((await erase(d, "req-1", [ORG, "", SRC, SID])).uploadsFenced).toBe(true);
  });

  test("record_step: completed only when no artifact is pending; completed is final", async () => {
    const d = await database();
    await erase(d);
    let r: Receipt | undefined;
    for (const [i, k] of BYTE_KEYS.entries()) {
      expect(r?.status ?? "in_progress").toBe("in_progress");
      r = await step(d, k, i % 2 ? "not_applicable" : "done", i);
    }
    expect(r).toMatchObject({ status: "completed", unreadable: true, bytesErased: true });
    expect(r!.completedAt).not.toBeNull();
    // An identical repeat is a no-op; any other step is refused.
    expect(await step(d, "transcriptObjects", "done", 0)).toEqual(r!);
    expect(await code(step(d, "transcriptObjects", "done", 5))).toBe("55000");
    expect(await code(step(d, "transcriptObjects", "pending", 0))).toBe("55000");
    expect((await events(d)).filter((e) => e === "completed")).toEqual(["completed"]);
  });

  test("record_step: one pending artifact keeps the receipt open; a pending pointer makes it readable again", async () => {
    const d = await database();
    await erase(d);
    expect(await step(d, "pointerRows", "pending", 1)).toMatchObject({ status: "in_progress", unreadable: false, bytesErased: false });
    for (const k of BYTE_KEYS.slice(1)) await step(d, k, "done", 1);
    expect((await receipt(d))!).toMatchObject({ status: "in_progress", unreadable: false, bytesErased: false });
    const pending = await step(d, BYTE_KEYS[0]!, "done", 1);
    expect(pending).toMatchObject({ status: "in_progress", unreadable: false, bytesErased: true });
    expect((await step(d, "pointerRows", "done", 1)).status).toBe("completed");
  });

  test("record_step input checks, and no receipt is P0002", async () => {
    const d = await database();
    expect(await code(step(d, "transcriptObjects", "done", 1))).toBe("P0002");
    await erase(d);
    expect(await code(step(d, "nope", "done", 1))).toBe("22023");
    expect(await code(step(d, "transcriptObjects", "gone", 1))).toBe("22023");
    expect(await code(step(d, "transcriptObjects", "done", -1))).toBe("22023");
    expect(await code(step(d, "transcriptObjects", "done", 1, "X1"))).toBe("22023");
    expect(await code(step(d, "transcriptObjects", "failed", null))).toBe("22023");
    expect(await code(step(d, "transcriptObjects", "failed", null, "bad code!"))).toBe("22023");
  });

  test("failed then retried on the same receipt", async () => {
    const d = await database();
    await erase(d, "req-1");
    const failed = await step(d, "transcriptObjects", "failed_retryable", null, "S3_503");
    expect(failed).toMatchObject({ status: "failed", failure: { step: "transcriptObjects", code: "S3_503", retryable: true } });
    expect(failed.artifacts.transcriptObjects.state).toBe("pending");
    expect(await code(step(d, "stagedObjects", "done", 0))).toBe("55000");   // retry the erase first
    const retried = await erase(d, "req-2");
    expect(retried).toMatchObject({ status: "in_progress", failure: null, attempts: 2, requestId: "req-1", uploadsFenced: true });
    expect(retried.artifacts.pointerRows).toEqual(failed.artifacts.pointerRows); // fencing is not re-run once done
    expect(await rows(d, "SELECT count(*)::int n FROM sessions.session_erasure_receipts")).toEqual([{ n: 1 }]);
    for (const k of BYTE_KEYS) await step(d, k, "done", 0);
    expect((await receipt(d))!.status).toBe("completed");
    expect(await events(d)).toContain("retried");
    const nonRetryable = await database();
    await erase(nonRetryable);
    expect((await step(nonRetryable, "briefs", "failed", null, "E1")).failure).toEqual({ step: "briefs", code: "E1", retryable: false });
  });

  test("owner cap (RH010): the session fence stays, the receipt is failed and retryable; under the cap a retry fences", async () => {
    const d = await database();
    for (let i = 0; i < 65; i++) await pointer(d, `u-${String(i).padStart(2, "0")}`, SID, `m-${i}`);
    const r = await erase(d);
    expect(r).toMatchObject({ status: "failed", uploadsFenced: false, unreadable: false, failure: { step: "fence", code: "RH010", retryable: true } });
    expect(await rows(d, "SELECT session_id FROM sessions.session_erasure_fences")).toEqual([{ session_id: SID }]);
    expect(await rows(d, "SELECT count(*)::int n FROM sessions.session_index WHERE deleted_at IS NOT NULL")).toEqual([{ n: 0 }]);
    expect(await rows(d, "SELECT count(*)::int n FROM sessions.ingest_fence_versions")).toEqual([{ n: 0 }]);
    await d.db.query("DELETE FROM sessions.session_index WHERE machine_id='m-64'");
    const retried = await erase(d, "req-2");
    expect(retried).toMatchObject({ status: "in_progress", uploadsFenced: true, attempts: 2, artifacts: { pointerRows: { count: 64, state: "done" } } });
  });

  test("a pointer write the publication guard skips stays live: pointerRows is pending and the session is not unreadable", async () => {
    const d = await database();
    await pointer(d, "u-1", SID, "m-1");
    await pointer(d, "u-1", SID, "m-keep");
    // Stand-in for ingest_publication_pointer_guard skipping a live managed publication head.
    await d.db.exec(`CREATE FUNCTION pg_temp.skip() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF OLD.machine_id='m-keep' THEN RETURN NULL; END IF; RETURN NEW; END$$;
      CREATE TRIGGER aaa_test_skip BEFORE UPDATE ON sessions.session_index FOR EACH ROW EXECUTE FUNCTION pg_temp.skip();`);
    const r = await erase(d);
    expect(r.artifacts.pointerRows).toEqual({ count: 1, state: "pending" });
    expect(r).toMatchObject({ status: "in_progress", uploadsFenced: true, unreadable: false });
  });

  test("lazy, buffered or ordered org: RH011 before any write", async () => {
    const d = await database();
    await d.db.query(`INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch)
      VALUES($1,$2,$2,$2,now()+interval '1 day',$2)`, [ORG, hex("a")]);
    expect(await code(erase(d))).toBe("RH011");
    expect(await rows(d, "SELECT 1 FROM sessions.session_erasure_receipts")).toEqual([]);
    expect(await rows(d, "SELECT 1 FROM sessions.session_erasure_fences")).toEqual([]);
    // Another (direct-mode) org is not affected.
    expect((await erase(d, "req-b", ["org-b", WS, SRC, SID])).uploadsFenced).toBe(true);
  });

  test("receipts and fences are never deleted; events are append-only", async () => {
    const d = await database();
    await erase(d);
    for (const q of ["DELETE FROM sessions.session_erasure_receipts", "DELETE FROM sessions.session_erasure_fences", "UPDATE sessions.session_erasure_events SET kind='step'",
      "DELETE FROM sessions.session_erasure_events", "TRUNCATE sessions.session_erasure_fences", "TRUNCATE sessions.session_erasure_receipts CASCADE"])
      expect([q, await code(d.db.query(q))]).toEqual([q, "55000"]);
  });

  test("grants: the ingest role executes the three entry points; PUBLIC, service_role and others cannot; tables are closed", async () => {
    const d = await database();
    const sigs = ["sessions.ingest_operator_erase_session(text,text,text,text,text,text,text)", "sessions.ingest_operator_erase_receipt(text,text,text,text)",
      "sessions.ingest_operator_erase_record_step(text,text,text,text,text,text,bigint,text)"];
    for (const s of sigs) {
      const g = (await rows<{ i: boolean; p: boolean; sr: boolean; a: boolean; d: boolean; sp: string[] }>(d, `SELECT has_function_privilege('bulls96_session_ingest',$1,'EXECUTE') i,
        has_function_privilege('anon',$1,'EXECUTE') p,has_function_privilege('service_role',$1,'EXECUTE') sr,has_function_privilege('authenticated',$1,'EXECUTE') a,
        prosecdef d,proconfig sp FROM pg_proc WHERE oid=$1::regprocedure`, [s]))[0]!;
      expect([s, g]).toEqual([s, { i: true, p: false, sr: false, a: false, d: true, sp: ["search_path=pg_catalog, pg_temp"] }]);
      expect(await rows(d, "SELECT 1 FROM pg_proc p, aclexplode(p.proacl) a WHERE p.oid=$1::regprocedure AND a.grantee=0", [s])).toEqual([]);
    }
    const helpers = await rows<{ f: string }>(d, `SELECT p.oid::regprocedure::text f FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
      WHERE n.nspname='sessions' AND p.proname LIKE 'ingest\\_operator\\_erase\\_%' AND has_function_privilege('bulls96_session_ingest',p.oid,'EXECUTE') ORDER BY 1`);
    expect(helpers.map((h) => h.f).sort()).toEqual(sigs.map((s) => s.replace(/\(.*/, "")).sort().map((n) => helpers.find((h) => h.f.startsWith(n + "("))!.f));
    expect(helpers).toHaveLength(3);
    for (const t of ["session_erasure_receipts", "session_erasure_events", "session_erasure_fences"])
      for (const role of ["bulls96_session_ingest", "anon", "authenticated", "service_role", "search_consumer_runtime"])
        expect([t, role, (await rows<{ x: boolean }>(d, `SELECT has_table_privilege($1,$2,'SELECT,INSERT,UPDATE,DELETE') x`, [role, `sessions.${t}`]))[0]!.x]).toEqual([t, role, false]);
    // Called as anon (no EXECUTE) is 42501; the ingest role's direct table read is 42501 too.
    expect(await code(d.as("anon")("SELECT sessions.ingest_operator_erase_receipt($1,$2,$3,$4)", [...KEY]))).toBe("42501");
    expect(await code(d.run("SELECT 1 FROM sessions.session_erasure_receipts", []))).toBe("42501");
    // The role verifier accepts the new grants.
    await d.db.exec(sql("supabase/bootstrap/session_ingest_verify.sql"));
  });
});
