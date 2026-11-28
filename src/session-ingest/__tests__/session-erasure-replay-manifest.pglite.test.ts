/**
 * 20261105062360_session_erasure_replay_manifest in PGlite: the in-transaction capture of a session erase's target set,
 * its seal, the immutability guards and the owner reader (design docs/design/operator-erase-replay-target-manifest.md).
 * Single connection; the native interleavings are in session-erasure-replay-manifest.postgres.test.ts. Nothing here
 * touches a real database.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import type { ClaimInput, LedgerRecord } from "../ledger.ts";
import { sqlLedger, type SqlRun } from "../ledger-sql.ts";
import { TURN_ORIGIN_ID } from "../turns.ts";
import { REPO_ROOT, applySupabaseSchema, migrationFiles } from "./pglite-schema.ts";
import { compareTargets, replayManifestDigest, ReplayDigest, type ReplayTarget } from "../../hoopdb/session-erasure-replay-digest.ts";

setDefaultTimeout(300_000);
const read = (p: string) => readFileSync(join(REPO_ROOT, p), "utf8");
function sql(p: string): string {
  return read(p).split("\n").map((l) => (l.startsWith("\\ir ") ? sql(join(dirname(p), l.slice(4).trim())) : l.startsWith("\\") ? "" : l)).join("\n");
}
const open: PGlite[] = [];
afterEach(async () => { await Promise.all(open.splice(0).map((d) => d.close())); });

const MIGRATION = "20261105062360_session_erasure_replay_manifest.sql";
const ORG = "org-a", WS = "ws-a", SRC = "claude", SID = "s-1";
const KEY = [ORG, WS, SRC, SID] as const;
const hex = (c: string) => c.repeat(64);

type Db = { db: PGlite; run: SqlRun };
/** Every migration through `until` (default: all, i.e. 062360 applied). */
async function database(until: string | null = null): Promise<Db> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await applySupabaseSchema(db, { until });
  await db.exec(sql("supabase/bootstrap/session_ingest_role.sql"));
  let chain: Promise<unknown> = Promise.resolve();
  const run: SqlRun = (text, params) => {
    const next = chain.then(async () => {
      await db.exec("SET ROLE bulls96_session_ingest");
      try { return (await db.query(text, params as never)) as { rows: Record<string, unknown>[] }; }
      finally { await db.exec("RESET ROLE"); }
    });
    chain = next.catch(() => {});
    return next;
  };
  return { db, run };
}
const before = () => { const all = migrationFiles(REPO_ROOT, null); return all[all.indexOf(MIGRATION) - 1]!; };

async function land(d: Db, user: string, origin: string, sessions: [string, string][], batchId: string) {
  let token: string | undefined;
  const ledger = sqlLedger(async (text, params) => {
    const out = await d.run(text, params);
    const t = (out.rows[0]?.result as { claimToken?: string } | undefined)?.claimToken;
    if (text.includes("ingest_claim_batch") && t) token = t;
    return out;
  });
  const records: LedgerRecord[] = sessions.map(([source, sessionId], i) => ({
    source, sessionId, recordId: `${batchId}-r${i}`, revisionId: `${batchId}-r${i}:1`, revision: 1, digest: hex(String(i % 10)),
  }));
  const c: ClaimInput = { tenant: { orgId: ORG, workspaceId: WS, userId: user }, originId: origin, batchId, batchDigest: hex("a"),
    receiptId: `rhr_${hex("e")}`, sessions, records };
  const r = await ledger.claim(c);
  if (r.kind !== "claimed" && r.kind !== "resume") throw new Error(`expected a claim, got ${r.kind}`);
  await ledger.commit({ tenant: c.tenant, originId: origin, batchId, batchDigest: c.batchDigest, claimToken: token!, sessions, records,
    machineId: `srv_${"ab".repeat(16)}` });
}
async function pointer(d: Db, user: string, session: string, machine: string, canonical: string | null = null) {
  await d.db.query(
    `INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,
       storage_provider,bucket,manifest_key,content_sha256,raw_format,trace_id,revision,canonical_session_id)
     VALUES($1,$2,$3,$4,$5,$6,now(),now(),'s3','bucket','k/'||$4||$5,decode(repeat('ab',32),'hex'),'jsonl','019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18',2,$7)`,
    [ORG, WS, SRC, session, machine, user, canonical]);
}
async function directoryUser(d: Db, session: string, user: string) {
  await d.db.query(`INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at)
    VALUES($1,$2,$3,$4,now(),now()) ON CONFLICT DO NOTHING`, [ORG, WS, SRC, session]);
  await d.db.query(`INSERT INTO sessions.session_directory_users(org_id,workspace_id,harness,session_id,user_id,last_activity_at)
    VALUES($1,$2,$3,$4,$5,now())`, [ORG, WS, SRC, session, user]);
}
const erase = async (d: Db, request = "req-1", key: readonly string[] = KEY) =>
  (await d.db.query("SELECT sessions.ingest_operator_erase_session($1,$2,$3,$4,$5,'user','operator-1') AS r", [...key, request])).rows[0]!.r as
    { status: string; failure: { code: string } | null; uploadsFenced: boolean };
const rows = async <T>(d: Db, q: string, p: unknown[] = []) => (await d.db.query<T>(q, p)).rows;
const code = (p: Promise<unknown>) => p.then(() => "ok", (e: { code?: string }) => e.code);

type Page = { v: 1; complete: boolean; reason: string; manifest_id: string | null; request_id: string | null;
  scope: { org_id: string; workspace_id: string; source: string; requested_session_id: string } | null; capture_version: 1 | null;
  alias_ids: string[]; target_count: string | null; manifest_sha256: string | null; targets: ReplayTarget[]; next_cursor: unknown };
/** The reader, in a READ ONLY transaction (as the bridge's owner connection). */
async function page(d: Db, fence: string, cursor: unknown = null, size = 512, readOnly = true): Promise<Page> {
  await d.db.exec(readOnly ? "BEGIN READ ONLY" : "BEGIN");
  try {
    return (await d.db.query<{ p: Page }>("SELECT sessions.session_erasure_replay_read($1,$2,$3,$4,$5::jsonb,$6) p",
      [ORG, WS, SRC, fence, cursor === null ? null : JSON.stringify(cursor), size])).rows[0]!.p;
  } finally { await d.db.exec("COMMIT"); }
}
/** Drain every page, validating as the bridge's preparation pass must (identity, order, count, streamed digest). */
async function drain(d: Db, fence: string, size: number): Promise<{ first: Page; targets: ReplayTarget[]; pages: number }> {
  const first = await page(d, fence, null, size);
  if (!first.complete) throw new Error(first.reason);
  const digest = new ReplayDigest({ capture_version: 1, manifest_id: first.manifest_id!, request_id: first.request_id!, ...first.scope! },
    first.alias_ids, Number(first.target_count));
  const targets: ReplayTarget[] = [];
  let p = first, pages = 0;
  for (;;) {
    pages++;
    expect({ ...p, targets: null, next_cursor: null }).toEqual({ ...first, targets: null, next_cursor: null });
    expect(p.targets.length).toBeGreaterThanOrEqual(1);
    expect(p.targets.length).toBeLessThanOrEqual(size);
    for (const t of p.targets) { digest.push(t); targets.push(t); }
    if (p.next_cursor === null) break;
    expect(p.next_cursor).toEqual({ v: 1, manifest_id: first.manifest_id, manifest_sha256: first.manifest_sha256, after: p.targets.at(-1) });
    p = await page(d, fence, p.next_cursor, size);
  }
  expect(digest.hex()).toBe(first.manifest_sha256!);
  return { first, targets, pages };
}
const effectSet = async (d: Db) => (await rows<ReplayTarget>(d,
  `SELECT user_id,origin_id,source,session_id FROM sessions.upload_deletions WHERE org_id=$1 AND workspace_id=$2`, [ORG, WS]))
  .sort(compareTargets);

/**
 * The erase never seals (F1 ruling, option (c): no authoritative alias-coverage proof yet). To exercise the seal, its
 * guards and the reader, the TEST OWNER seals every captured, unsealed manifest from its own rows: a stand-in for a
 * future proof-gated seal, never a production path. The reader still recomputes and verifies everything per page.
 */
const ownerSeal = async (d: Db) => {
  // The seal trigger refuses EVERY seal in this slot (no alias-coverage proof), so the test owner disables it for this
  // one statement and re-enables it: test-only, never a production path.
  await d.db.exec('ALTER TABLE sessions.session_erasure_replay_completions DISABLE TRIGGER session_erasure_replay_seal');
  try { await d.db.query(`INSERT INTO sessions.session_erasure_replay_completions(manifest_id,org_id,workspace_id,source,derivation_version,alias_count,owner_count,pair_count,target_count,manifest_sha256)
  SELECT r.manifest_id,r.org_id,r.workspace_id,r.source,1,(SELECT count(*) FROM sessions.session_erasure_replay_aliases a WHERE a.manifest_id=r.manifest_id),
    count(DISTINCT t.user_id),count(DISTINCT (t.user_id,t.origin_id)),count(*),sessions.session_erasure_replay_digest(r.manifest_id)
  FROM sessions.session_erasure_replay_requests r JOIN sessions.session_erasure_replay_targets t ON t.manifest_id=r.manifest_id
  WHERE NOT EXISTS(SELECT 1 FROM sessions.session_erasure_replay_completions c WHERE c.manifest_id=r.manifest_id)
  GROUP BY r.manifest_id,r.org_id,r.workspace_id,r.source`); } finally { await d.db.exec('ALTER TABLE sessions.session_erasure_replay_completions ENABLE TRIGGER session_erasure_replay_seal'); }
};
/** The scenario every equivalence test uses: owners from all three sources, owner-specific origins, two aliases. */
async function scenario(d: Db) {
  await land(d, "u-b", "origin-1", [[SRC, SID], [SRC, "s-other"]], "b1");
  await land(d, "U-a", "origin-2", [[SRC, SID]], "b2");
  await pointer(d, "u-b", SID, "m-1");
  await pointer(d, "U-a", "s-alias", "m-2", SID);   // physical alias naming the key as canonical
  await pointer(d, "u-c", SID, "m-3");              // index-only owner
  await directoryUser(d, SID, "u-d");               // directory-only owner
}

describe("062360 session erasure replay manifest", () => {
  test("a first erase captures T in its own transaction (never sealed), and T equals the SQL selection effects exactly", async () => {
    const d = await database();
    await scenario(d);
    const r = await erase(d);
    expect(r).toMatchObject({ status: "in_progress", uploadsFenced: true, failure: null });
    // F1 ruling (c): the erase records T but never seals it; every alias reads unsealed, with no targets.
    for (const a of [SID, "s-alias"]) expect(await page(d, a)).toMatchObject({ complete: false, reason: "manifest_unsealed", targets: [] });
    expect(await rows(d, "SELECT count(*)::int n FROM sessions.session_erasure_replay_completions")).toEqual([{ n: 0 }]);
    await ownerSeal(d);
    const aliases = [SID, "s-alias"];
    const expected: ReplayTarget[] = [];
    for (const [u, origins] of [["U-a", ["origin-2", TURN_ORIGIN_ID]], ["u-b", ["origin-1", TURN_ORIGIN_ID]], ["u-c", [TURN_ORIGIN_ID]], ["u-d", [TURN_ORIGIN_ID]]] as const)
      for (const o of origins) for (const a of aliases) expected.push({ user_id: u, origin_id: o, source: SRC, session_id: a });
    expected.sort(compareTargets);
    // Owner-specific origins: origin-1 only for u-b, origin-2 only for U-a; index/directory-only owners get the turns origin.
    const { first, targets, pages } = await drain(d, "s-alias", 3);
    expect(targets).toEqual(expected);
    expect(pages).toBe(Math.ceil(expected.length / 3));
    expect(first).toMatchObject({ complete: true, reason: "complete", request_id: "req-1", capture_version: 1, alias_ids: aliases,
      target_count: String(expected.length), scope: { org_id: ORG, workspace_id: WS, source: SRC, requested_session_id: SID } });
    expect(first.manifest_sha256).toBe(replayManifestDigest({ capture_version: 1, manifest_id: first.manifest_id!, request_id: "req-1",
      org_id: ORG, workspace_id: WS, source: SRC, requested_session_id: SID }, aliases, expected));
    // The production effect path: every (owner, origin, alias) selection the erase applied is exactly T.
    expect(await effectSet(d)).toEqual(expected);
    // Both fence aliases resolve to the same manifest.
    expect((await page(d, SID)).manifest_id).toBe(first.manifest_id);
    expect(await rows(d, "SELECT alias_count,owner_count,pair_count::int,target_count::int FROM sessions.session_erasure_replay_completions"))
      .toEqual([{ alias_count: 2, owner_count: 4, pair_count: 6, target_count: 12 }]);
  });

  test("the captured path's effects are identical to the pre-062360 erase on the same data", async () => {
    const legacy = await database(before());
    await scenario(legacy);
    await erase(legacy);
    const captured = await database();
    await scenario(captured);
    await erase(captured);
    const snapshot = async (d: Db) => ({
      effects: await effectSet(d),
      fences: await rows(d, `SELECT user_id,origin_id,source,session_id FROM sessions.ingest_fence_versions ORDER BY 1,2,3,4`),
      ownership: await rows(d, "SELECT user_id,origin_id,session_id FROM sessions.ingest_session_ownership ORDER BY 1,2,3"),
      index: await rows(d, "SELECT session_id,machine_id,deleted_at IS NOT NULL dead,revision::text rev FROM sessions.session_index ORDER BY 1,2"),
      receipt: await rows(d, "SELECT status,artifacts,uploads_fenced,unreadable FROM sessions.session_erasure_receipts"),
      events: await rows(d, "SELECT kind,count FROM sessions.session_erasure_events ORDER BY event_id"),
      // 062340's erasure history: the manifest writes are not erasure sources, so the release count is unchanged.
      history: await rows(d, "SELECT source,op,count(*)::int n FROM sessions.ingest_org_erasure_history GROUP BY 1,2 ORDER BY 1,2"),
    });
    expect(await snapshot(captured)).toEqual(await snapshot(legacy));
  });

  test("a (test-owner) sealed manifest is reused unchanged by repeats after cleanup removed the metadata; another request id does not rebind it", async () => {
    const d = await database();
    await scenario(d);
    await erase(d, "req-1");
    await ownerSeal(d);
    const sealed = await drain(d, SID, 512);
    expect(await rows(d, "SELECT 1 FROM sessions.ingest_session_ownership WHERE session_id=$1", [SID])).toEqual([]);
    await erase(d, "req-2");
    await erase(d, "req-1", [ORG, WS, SRC, "s-alias"]).catch(() => {}); // alias input: answered from its own key or refused
    const again = await drain(d, SID, 512);
    expect(again).toEqual(sealed);
    expect(await rows(d, "SELECT count(*)::int n FROM sessions.session_erasure_replay_requests")).toEqual([{ n: 1 }]);
  });

  test("a caught RH010 keeps the fences and the intent but no targets or seal; retries never seal, with any alias closure", async () => {
    for (const change of ["same", "larger", "smaller"] as const) {
      const d = await database();
      await pointer(d, "u-0", SID, "m-0");
      if (change === "smaller") await pointer(d, "u-0", "s-gone", "m-9", SID);
      // 65 owners: over the 64-owner cap, a retryable RH010 inside the fence block.
      for (let i = 1; i <= 65; i++) await directoryUser(d, SID, `u-${String(i).padStart(2, "0")}`);
      const failed = await erase(d);
      expect(failed).toMatchObject({ status: "failed", failure: { code: "RH010" }, uploadsFenced: false });
      expect(await rows(d, "SELECT count(*)::int n FROM sessions.session_erasure_replay_requests")).toEqual([{ n: 1 }]);
      expect(await rows(d, "SELECT count(*)::int n FROM sessions.session_erasure_replay_targets")).toEqual([{ n: 0 }]);
      expect(await rows(d, "SELECT count(*)::int n FROM sessions.session_erasure_replay_completions")).toEqual([{ n: 0 }]);
      expect((await page(d, SID)).reason).toBe("manifest_unsealed");
      // Bring the owners under the cap and change (or keep) the alias closure, then retry.
      await d.db.query("DELETE FROM sessions.session_directory_users WHERE user_id LIKE 'u-%' AND user_id<>'u-01'");
      if (change === "larger") await pointer(d, "u-0", "s-new", "m-8", SID);
      if (change === "smaller") await d.db.query("DELETE FROM sessions.session_index WHERE session_id='s-gone'");
      const retried = await erase(d);
      // The existing erase behaviour: the retry fences and proceeds with its own derivation.
      expect(retried).toMatchObject({ status: "in_progress", uploadsFenced: true, failure: null });
      expect((await effectSet(d)).length).toBeGreaterThan(0);
      expect((await page(d, SID)).reason).toBe("manifest_unsealed");
      expect(await rows(d, "SELECT count(*)::int n FROM sessions.session_erasure_replay_completions")).toEqual([{ n: 0 }]);
      if (change === "larger") expect((await page(d, "s-new")).reason).toBe("manifest_missing");
    }
  });

  test("F1 (missing index link): a physical session committed under an origin whose index link is gone stays out of A, so the erase never claims completeness", async () => {
    const d = await database();
    // The layered reproduction: a physical session under an origin, its pointer linked to the logical id, a directory
    // owner for the logical id, then only the index row removed before the real erase.
    await land(d, "u-p", "origin-p", [[SRC, "s-phys"]], "bp");
    await pointer(d, "u-p", "s-phys", "m-p", SID);
    await directoryUser(d, SID, "u-d");
    await d.db.exec("ALTER TABLE sessions.session_index DISABLE TRIGGER USER");
    await d.db.query("DELETE FROM sessions.session_index WHERE session_id='s-phys'");
    await d.db.exec("ALTER TABLE sessions.session_index ENABLE TRIGGER USER");
    const r = await erase(d);
    expect(r).toMatchObject({ uploadsFenced: true, failure: null });
    // The closure is the logical id only (session_index has no link left), exactly as the legacy erase derives it.
    expect(await rows(d, "SELECT alias_session_id a FROM sessions.session_erasure_replay_aliases")).toEqual([{ a: SID }]);
    // The physical session's ownership survives the erase, as it does in the legacy erase: no inferred alias.
    expect(await rows(d, "SELECT user_id,origin_id FROM sessions.ingest_session_ownership WHERE session_id='s-phys'"))
      .toEqual([{ user_id: "u-p", origin_id: "origin-p" }]);
    // Neither id reads complete: the logical one is unsealed, the physical one has no manifest.
    expect(await page(d, SID)).toMatchObject({ complete: false, reason: "manifest_unsealed", targets: [] });
    expect(await page(d, "s-phys")).toMatchObject({ complete: false, reason: "manifest_missing", targets: [] });
    expect(await rows(d, "SELECT count(*)::int n FROM sessions.session_erasure_replay_completions")).toEqual([{ n: 0 }]);
  });

  test("F1, the kernel's three closure cases: none can read complete (an ownership veto, empty ownership with a surviving directory row, a removed link)", async () => {
    const cases: Record<string, (d: Db) => Promise<void>> = {
      // An unmatched ownership row: the physical session's ownership survives without any index row.
      "ownership-without-index": async (d) => { await land(d, "u-p", "origin-p", [[SRC, "s-phys"]], "bp"); await directoryUser(d, SID, "u-d"); },
      // Ownership empty, a directory-only row for the physical id survives: an ownership scan would pass, no proof.
      "directory-only-physical": async (d) => {
        await directoryUser(d, SID, "u-d");
        await d.db.query(`INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at)
          VALUES($1,$2,$3,'s-phys',now(),now())`, [ORG, WS, SRC]);
        await d.db.query(`INSERT INTO sessions.session_directory_users(org_id,workspace_id,harness,session_id,user_id,last_activity_at)
          VALUES($1,$2,$3,'s-phys','u-q',now())`, [ORG, WS, SRC]);
      },
      // The physical/canonical edge was removed: the current rows look like a session that never had the alias.
      "removed-link": async (d) => {
        await pointer(d, "u-p", "s-phys", "m-p", SID);
        await pointer(d, "u-b", SID, "m-1");
        await d.db.exec("ALTER TABLE sessions.session_index DISABLE TRIGGER USER");
        await d.db.query("DELETE FROM sessions.session_index WHERE session_id='s-phys'");
        await d.db.exec("ALTER TABLE sessions.session_index ENABLE TRIGGER USER");
      },
    };
    for (const [name, setup] of Object.entries(cases)) {
      const legacy = await database(before());
      await setup(legacy);
      await erase(legacy);
      const d = await database();
      await setup(d);
      await erase(d);
      for (const id of [SID, "s-phys"]) expect([name, id, (await page(d, id)).complete]).toEqual([name, id, false]);
      expect([name, (await rows<{ n: number }>(d, "SELECT count(*)::int n FROM sessions.session_erasure_replay_completions"))[0]!.n]).toEqual([name, 0]);
      // The legacy erase's effects are preserved exactly: nothing is inferred into the closure.
      expect([name, await effectSet(d)]).toEqual([name, await effectSet(legacy)]);
    }
  });

  test("RH011 (a lazy, buffered or ordered org) refuses the erase before any write: no receipt, fence, intent or seal", async () => {
    const d = await database();
    await scenario(d);
    await d.db.query(`INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch)
      VALUES($1,$2,$2,$2,now()+interval '1 day',$2)`, [ORG, hex("c")]);
    expect(await code(erase(d))).toBe("RH011");
    for (const t of ["session_erasure_receipts", "session_erasure_fences", "session_erasure_replay_requests", "session_erasure_replay_aliases",
      "session_erasure_replay_targets", "session_erasure_replay_completions"])
      expect([t, (await rows<{ n: number }>(d, `SELECT count(*)::int n FROM sessions.${t}`))[0]!.n]).toEqual([t, 0]);
    // A refusal is never proof: the reader has nothing to resolve.
    expect((await page(d, SID)).reason).toBe("manifest_missing");
  });

  test("the size bound: over an injected small cap no target is captured and the erase (effects, receipt, events, history) is exactly the legacy one", async () => {
    const legacy = await database(before());
    await scenario(legacy);
    await erase(legacy);
    const d = await database();
    await scenario(d);
    const at = (await rows<{ n: string }>(d, "SELECT sessions.session_erasure_replay_digest_cap()::text n"))[0]!.n;
    expect(at).toBe("67108864");
    // The test stands in for an oversized manifest by lowering the cap below this scenario's framed size.
    await d.db.exec("CREATE OR REPLACE FUNCTION sessions.session_erasure_replay_digest_cap() RETURNS bigint LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $c$ SELECT 200::bigint $c$");
    const r = await erase(d);
    expect(r).toMatchObject({ status: "in_progress", uploadsFenced: true, failure: null });
    expect((await page(d, SID)).reason).toBe("manifest_unsealed");
    expect(await rows(d, "SELECT count(*)::int n FROM sessions.session_erasure_replay_completions")).toEqual([{ n: 0 }]);
    expect(await effectSet(d)).toEqual(await effectSet(legacy));
    // The preflight ran before any target row: nothing over the cap is written or aggregated.
    expect(await rows(d, "SELECT count(*)::int n FROM sessions.session_erasure_replay_targets")).toEqual([{ n: 0 }]);
    // The whole legacy record is unchanged: receipt, events and 062340 history, not only the selection effects.
    const record = async (x: Db) => ({
      receipt: await rows(x, "SELECT status,artifacts,uploads_fenced,unreadable,attempts FROM sessions.session_erasure_receipts"),
      events: await rows(x, "SELECT kind,count FROM sessions.session_erasure_events ORDER BY event_id"),
      history: await rows(x, "SELECT source,op,count(*)::int n FROM sessions.ingest_org_erasure_history GROUP BY 1,2 ORDER BY 1,2"),
    });
    expect(await record(d)).toEqual(await record(legacy));
  });

  test("completion is fail-closed: even a seal that exactly matches its rows is refused (no alias-coverage proof)", async () => {
    const d = await database();
    await scenario(d);
    await erase(d);
    let refused: { code?: string; message?: string } | undefined;
    try {
      await d.db.query(`INSERT INTO sessions.session_erasure_replay_completions(manifest_id,org_id,workspace_id,source,derivation_version,alias_count,owner_count,pair_count,target_count,manifest_sha256)
  SELECT r.manifest_id,r.org_id,r.workspace_id,r.source,1,(SELECT count(*) FROM sessions.session_erasure_replay_aliases a WHERE a.manifest_id=r.manifest_id),
    count(DISTINCT t.user_id),count(DISTINCT (t.user_id,t.origin_id)),count(*),sessions.session_erasure_replay_digest(r.manifest_id)
  FROM sessions.session_erasure_replay_requests r JOIN sessions.session_erasure_replay_targets t ON t.manifest_id=r.manifest_id
  WHERE NOT EXISTS(SELECT 1 FROM sessions.session_erasure_replay_completions c WHERE c.manifest_id=r.manifest_id)
  GROUP BY r.manifest_id,r.org_id,r.workspace_id,r.source`);
    } catch (e) { refused = e as { code?: string; message?: string }; }
    expect(refused).toMatchObject({ code: "55000", message: "session erasure replay manifest: sealing requires an authoritative alias-coverage proof" });
    expect(await rows(d, "SELECT count(*)::int n FROM sessions.session_erasure_replay_completions")).toEqual([{ n: 0 }]);
    expect((await page(d, SID)).reason).toBe("manifest_unsealed");
  });

  test("the size bound at the boundary: at the cap the targets are captured, one octet under it none are; effects stay legacy", async () => {
    const legacy = await database(before());
    await scenario(legacy);
    await erase(legacy);
    const measure = await database();
    await scenario(measure);
    await erase(measure);
    const m = (await rows<{ m: string }>(measure, "SELECT manifest_id::text m FROM sessions.session_erasure_replay_requests"))[0]!.m;
    const size = Number((await rows<{ n: string }>(measure, "SELECT sessions.session_erasure_replay_framed_bytes($1)::text n", [m]))[0]!.n);
    expect(size).toBeGreaterThan(100);
    const cost: Record<string, number> = {};
    const capped = async (cap: number) => {
      const d = await database();
      await scenario(d);
      await d.db.exec(`CREATE OR REPLACE FUNCTION sessions.session_erasure_replay_digest_cap() RETURNS bigint LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $c$ SELECT ${cap}::bigint $c$`);
      const t0 = performance.now();
      await erase(d);
      cost[`cap${cap - size >= 0 ? "+" : ""}${cap - size}`] = Math.round(performance.now() - t0);
      return { d, targets: (await rows<{ n: number }>(d, "SELECT count(*)::int n FROM sessions.session_erasure_replay_targets"))[0]!.n };
    };
    const lt0 = performance.now();
    { const l2 = await database(before()); await scenario(l2); const t0 = performance.now(); await erase(l2); cost.legacy = Math.round(performance.now() - t0); }
    void lt0;
    for (const [cap, captured] of [[size + 1, true], [size, true], [size - 1, false]] as const) {
      const { d, targets } = await capped(cap);
      expect([cap - size, targets > 0]).toEqual([cap - size, captured]);
      expect(await effectSet(d)).toEqual(await effectSet(legacy));
      // The full legacy record at every cap: receipt, events and 062340 history, not only the selection effects.
      const record = async (x: Db) => ({
        receipt: await rows(x, "SELECT status,artifacts,uploads_fenced,unreadable,attempts FROM sessions.session_erasure_receipts"),
        events: await rows(x, "SELECT kind,count FROM sessions.session_erasure_events ORDER BY event_id"),
        history: await rows(x, "SELECT source,op,count(*)::int n FROM sessions.ingest_org_erasure_history GROUP BY 1,2 ORDER BY 1,2"),
        fences: await rows(x, "SELECT user_id,origin_id,source,session_id FROM sessions.ingest_fence_versions ORDER BY 1,2,3,4"),
      });
      expect([cap - size, await record(d)]).toEqual([cap - size, await record(legacy)]);
      expect((await page(d, SID)).reason).toBe("manifest_unsealed");
      // The digest never builds an over-cap message: NULL at size - 1 for the measured rows' shape.
      if (captured) {
        const dm = (await rows<{ m: string }>(d, "SELECT manifest_id::text m FROM sessions.session_erasure_replay_requests"))[0]!.m;
        expect((await rows<{ h: string | null }>(d, "SELECT sessions.session_erasure_replay_digest($1) h", [dm]))[0]!.h).toMatch(/^[0-9a-f]{64}$/u);
        await d.db.exec("CREATE OR REPLACE FUNCTION sessions.session_erasure_replay_digest_cap() RETURNS bigint LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $c$ SELECT " + (size - 1) + "::bigint $c$");
        expect((await rows<{ h: string | null }>(d, "SELECT sessions.session_erasure_replay_digest($1) h", [dm]))[0]!.h).toBeNull();
        // The seal trigger checks the bound in its own statement, before any digest: an over-cap seal is refused as such.
        let why: string | undefined;
        try {
          await d.db.query(`INSERT INTO sessions.session_erasure_replay_completions SELECT r.manifest_id,r.org_id,r.workspace_id,r.source,1,
            (SELECT count(*) FROM sessions.session_erasure_replay_aliases a WHERE a.manifest_id=r.manifest_id),count(DISTINCT t.user_id),
            count(DISTINCT (t.user_id,t.origin_id)),count(*),repeat('0',64) FROM sessions.session_erasure_replay_requests r
            JOIN sessions.session_erasure_replay_targets t USING(manifest_id) GROUP BY r.manifest_id,r.org_id,r.workspace_id,r.source`);
        } catch (e) { why = (e as { message?: string }).message; }
        expect(why).toBe("session erasure replay manifest: over the size bound");
      }
    }
    // Capture cost at the boundary (informational, not asserted: single-connection PGlite timing): erase ms per cap.
    console.log(`062360 capture cost at the size boundary (framed ${size} octets): ${JSON.stringify(cost)}`);
  });

  test("an erase with no owners is never sealed (empty coverage is unproven)", async () => {
    const d = await database();
    const r = await erase(d);
    expect(r.uploadsFenced).toBe(true);
    const p = await page(d, SID);
    expect(p.complete).toBe(false);
    expect(p.reason).toBe("manifest_unsealed");
    expect(p).toMatchObject({ manifest_id: null, request_id: null, scope: null, targets: [], next_cursor: null, alias_ids: [] });
  });

  test("a fence written before 062360 has no intent and stays manifest_missing; so does a later overlapping closure", async () => {
    const d = await database(before());
    await scenario(d);
    await erase(d);
    await d.db.exec(read(`supabase/migrations/${MIGRATION}`));
    expect((await page(d, SID)).reason).toBe("manifest_missing");
    expect((await page(d, "s-alias")).reason).toBe("manifest_missing");
    // A fresh receipt for an alias fenced before 062360 (its closure is all pre-fenced) writes no intent: the debt is
    // not laundered through the owners that survive. The erase itself behaves as before.
    const r = await erase(d, "req-alias", [ORG, WS, SRC, "s-alias"]);
    expect(r.uploadsFenced).toBe(true);
    expect(await rows(d, "SELECT count(*)::int n FROM sessions.session_erasure_replay_requests")).toEqual([{ n: 0 }]);
    expect((await page(d, "s-alias")).reason).toBe("manifest_missing");
    // A new erase of another key whose closure reaches the sealed aliases writes no intent.
    const e = await database();
    await scenario(e);
    await erase(e);
    // The 050010 guard refuses a new pointer onto a fenced id; an owner bypass stands in for any path that makes two
    // closures overlap.
    await e.db.exec("ALTER TABLE sessions.session_index DISABLE TRIGGER ingest_erase_fence_index_guard");
    await pointer(e, "u-z", "s-2", "m-z", SID);
    await e.db.exec("ALTER TABLE sessions.session_index ENABLE TRIGGER ingest_erase_fence_index_guard");
    await erase(e, "req-2", [ORG, WS, SRC, "s-2"]);
    expect(await rows(e, "SELECT requested_session_id FROM sessions.session_erasure_replay_requests")).toEqual([{ requested_session_id: SID }]);
    expect((await page(e, "s-2")).reason).toBe("manifest_missing");
  });

  test("records are immutable: no UPDATE, DELETE, TRUNCATE, post-seal INSERT or forged seal", async () => {
    const d = await database();
    await scenario(d);
    await erase(d);
    await ownerSeal(d);
    const m = (await rows<{ m: string }>(d, "SELECT manifest_id::text m FROM sessions.session_erasure_replay_requests"))[0]!.m;
    for (const t of ["requests", "aliases", "targets", "completions"]) {
      expect(await code(d.db.query(`UPDATE sessions.session_erasure_replay_${t} SET manifest_id=manifest_id`))).toBe("55000");
      expect(await code(d.db.query(`DELETE FROM sessions.session_erasure_replay_${t}`))).toBe("55000");
      expect(await code(d.db.query(`TRUNCATE sessions.session_erasure_replay_${t} CASCADE`))).toBe("55000");
    }
    await d.db.query("INSERT INTO sessions.session_erasure_fences(org_id,workspace_id,source,session_id) VALUES($1,$2,$3,'s-extra')", [ORG, WS, SRC]);
    expect(await code(d.db.query("INSERT INTO sessions.session_erasure_replay_aliases VALUES($1,$2,$3,$4,'s-extra')", [m, ORG, WS, SRC]))).toBe("55000");
    expect(await code(d.db.query("INSERT INTO sessions.session_erasure_replay_targets VALUES($1,$2,$3,'u-new','o',$4,$5)", [m, ORG, WS, SRC, SID]))).toBe("55000");
    // A seal that does not match its rows is refused, even by the owner.
    await d.db.query("INSERT INTO sessions.session_erasure_receipts(org_id,workspace_id,source,session_id,request_id,artifacts,actor_kind,actor_id) SELECT org_id,workspace_id,source,'s-extra','req-x',artifacts,actor_kind,actor_id FROM sessions.session_erasure_receipts");
    const m2 = (await rows<{ m: string }>(d, "INSERT INTO sessions.session_erasure_replay_requests(request_id,org_id,workspace_id,source,requested_session_id,capture_version) VALUES('req-x',$1,$2,$3,'s-extra',1) RETURNING manifest_id::text m", [ORG, WS, SRC]))[0]!.m;
    await d.db.query("INSERT INTO sessions.session_erasure_replay_aliases VALUES($1,$2,$3,$4,'s-extra')", [m2, ORG, WS, SRC]).catch(() => {});
    expect(await code(d.db.query("INSERT INTO sessions.session_erasure_replay_completions VALUES($1,$2,$3,$4,1,1,1,1,1,$5)", [m2, ORG, WS, SRC, hex("0")]))).toBe("55000");
    // A request bound to another receipt's request id is refused by the compound key.
    expect(await code(d.db.query("INSERT INTO sessions.session_erasure_replay_requests(request_id,org_id,workspace_id,source,requested_session_id,capture_version) VALUES('req-forged',$1,$2,$3,'s-none',1)", [ORG, WS, SRC]))).toBe("23503");
    expect(await code(d.db.query("INSERT INTO sessions.session_erasure_replay_requests(request_id,org_id,workspace_id,source,requested_session_id,capture_version) VALUES('req-forged',$1,$2,$3,'s-extra',1)", [ORG, WS, SRC]))).toBe("23505");
  });

  test("the reader: READ ONLY or read-write, bounded page size, scope binding, forged and stale cursors", async () => {
    const d = await database();
    await scenario(d);
    await erase(d);
    await ownerSeal(d);
    // A plain STABLE SELECT: the same page in a READ ONLY and a read-write transaction.
    expect(await page(d, SID, null, 512, false)).toEqual(await page(d, SID, null, 512, true));
    for (const size of [0, 513]) expect((await page(d, SID, null, size)).reason).toBe("reader_input_invalid");
    const first = await page(d, SID, null, 5);
    const cursor = first.next_cursor as { v: 1; manifest_id: string; manifest_sha256: string; after: ReplayTarget };
    expect((await page(d, SID, cursor, 5)).complete).toBe(true);
    const forged = [
      { ...cursor, v: 2 }, { ...cursor, manifest_sha256: hex("0") }, { ...cursor, manifest_id: "00000000-0000-4000-8000-000000000000" },
      { ...cursor, extra: 1 }, { ...cursor, after: { ...cursor.after, user_id: "nobody" } }, { ...cursor, after: { ...cursor.after, extra: "x" } },
      { ...cursor, after: { user_id: "u-d", origin_id: TURN_ORIGIN_ID, source: SRC, session_id: "s-alias" } }, // the last target
    ];
    for (const c of forged) expect((await page(d, SID, c, 5)).reason).toBe("manifest_cursor_invalid");
    // Another workspace never sees this manifest.
    await d.db.exec("BEGIN READ ONLY");
    const other = (await d.db.query<{ p: Page }>("SELECT sessions.session_erasure_replay_read($1,'ws-b',$2,$3) p", [ORG, SRC, SID])).rows[0]!.p;
    await d.db.exec("COMMIT");
    expect(other.reason).toBe("manifest_missing");
    // The live latest runtime_begin still carries 062320's marked org check as its first statement.
    const begin = (await rows<{ s: string }>(d, "SELECT prosrc s FROM pg_proc WHERE oid='sessions.ingest_operator_runtime_begin(text,text,text,text,text,text,text,text,text,text)'::regprocedure"))[0]!.s;
    const lines = begin.split("\n").map((l) => l.trim());
    expect(lines[lines.indexOf("BEGIN") + 1]).toBe("PERFORM sessions.ingest_org_admission_hold_check(p_org,'erasure'); -- 062320 org admission hold");
  });

  test("identifiers as the erase accepts them: an empty workspace and non-ASCII ids are captured and read", async () => {
    const d = await database();
    await d.db.query(`INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at)
      VALUES($1,'',$2,'sé ss',now(),now())`, [ORG, SRC]);
    await d.db.query(`INSERT INTO sessions.session_directory_users(org_id,workspace_id,harness,session_id,user_id,last_activity_at)
      VALUES($1,'',$2,'sé ss','ü-1',now())`, [ORG, SRC]);
    await erase(d, "req-é", [ORG, "", SRC, "sé ss"]);
    await ownerSeal(d);
    await d.db.exec("BEGIN READ ONLY");
    const p = (await d.db.query<{ p: Page }>("SELECT sessions.session_erasure_replay_read($1,'',$2,'sé ss') p", [ORG, SRC])).rows[0]!.p;
    const bad = (await d.db.query<{ p: Page }>("SELECT sessions.session_erasure_replay_read($1,$2,$3,E'a\\x01') p", [ORG, WS, SRC])).rows[0]!.p;
    const empty = (await d.db.query<{ p: Page }>("SELECT sessions.session_erasure_replay_read('',$1,$2,'x') p", [WS, SRC])).rows[0]!.p;
    await d.db.exec("COMMIT");
    expect(p).toMatchObject({ complete: true, scope: { workspace_id: "", requested_session_id: "sé ss" }, target_count: "1",
      targets: [{ user_id: "ü-1", origin_id: TURN_ORIGIN_ID, source: SRC, session_id: "sé ss" }] });
    expect(p.manifest_sha256).toBe(replayManifestDigest({ capture_version: 1, manifest_id: p.manifest_id!, request_id: "req-é", org_id: ORG,
      workspace_id: "", source: SRC, requested_session_id: "sé ss" }, ["sé ss"], p.targets));
    expect([bad.reason, empty.reason]).toEqual(["reader_input_invalid", "reader_input_invalid"]);
  });

  test("owner-only: no runtime role reads the tables or calls the reader", async () => {
    const d = await database();
    for (const role of ["bulls96_session_ingest", "authenticated", "anon", "service_role"]) {
      await d.db.exec(`SET ROLE ${role}`);
      try {
        expect(await code(d.db.query("SELECT 1 FROM sessions.session_erasure_replay_targets"))).toBe("42501");
        expect(await code(d.db.query("SELECT sessions.session_erasure_replay_read('o','w','s','x')"))).toBe("42501");
      } finally { await d.db.exec("RESET ROLE"); }
    }
  });
});

describe("062360 digest vectors", () => {
  const fixture = JSON.parse(read("src/hoopdb/__tests__/fixtures/session-erasure-replay-digest-vectors.json")) as {
    vectors: { name: string; binding: { capture_version: 1; manifest_id: string; request_id: string; org_id: string; workspace_id: string; source: string;
      requested_session_id: string }; aliases: string[]; targets: ReplayTarget[]; sha256: string }[] };

  test("the TS encoder reproduces every pinned vector", () => {
    for (const v of fixture.vectors) expect([v.name, replayManifestDigest(v.binding, v.aliases, v.targets)]).toEqual([v.name, v.sha256]);
  });

  test("the smallest vector, built byte by byte without the encoder", () => {
    const v = fixture.vectors.find((x) => x.aliases.length === 1)!;
    const f = (x: string) => { const b = Buffer.from(x, "utf8"); const n = Buffer.alloc(4); n.writeUInt32BE(b.length); return Buffer.concat([n, b]); };
    const bytes = Buffer.concat([Buffer.from("rh-erase-replay-manifest/v1", "ascii"), f("1"), f(v.binding.manifest_id), f(v.binding.request_id), f(v.binding.org_id),
      f(v.binding.workspace_id), f(v.binding.source), f(v.binding.requested_session_id), f("1"), f("s"), f("1"), f("u"), f("o"), f("claude"), f("s")]);
    expect(bytes.toString("hex")).toBe((fixture as unknown as { minimal_message_hex: string }).minimal_message_hex);
    expect(createHash("sha256").update(bytes).digest("hex")).toBe(v.sha256);
  });

  test("sessions.session_erasure_replay_digest computes the same bytes from the stored rows", async () => {
    const d = await database();
    for (const v of fixture.vectors) {
      const b = v.binding;
      await d.db.query(`INSERT INTO sessions.session_erasure_receipts(org_id,workspace_id,source,session_id,request_id,artifacts,actor_kind,actor_id)
        SELECT $1,$2,$3,$4,$5,jsonb_object_agg(k,jsonb_build_object('count',0,'state','pending')),'user','vector'
        FROM unnest(sessions.ingest_operator_erase_artifact_keys()) AS k`, [b.org_id, b.workspace_id, b.source, b.requested_session_id, b.request_id]);
      await d.db.query(`INSERT INTO sessions.session_erasure_replay_requests(manifest_id,request_id,org_id,workspace_id,source,requested_session_id,capture_version)
        VALUES($1,$2,$3,$4,$5,$6,1)`, [b.manifest_id, b.request_id, b.org_id, b.workspace_id, b.source, b.requested_session_id]);
      for (const a of v.aliases) {
        await d.db.query("INSERT INTO sessions.session_erasure_fences(org_id,workspace_id,source,session_id) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",
          [b.org_id, b.workspace_id, b.source, a]);
        await d.db.query("INSERT INTO sessions.session_erasure_replay_aliases VALUES($1,$2,$3,$4,$5)", [b.manifest_id, b.org_id, b.workspace_id, b.source, a]);
      }
      for (const t of v.targets) await d.db.query("INSERT INTO sessions.session_erasure_replay_targets VALUES($1,$2,$3,$4,$5,$6,$7)",
        [b.manifest_id, b.org_id, b.workspace_id, t.user_id, t.origin_id, t.source, t.session_id]);
      expect([v.name, (await rows<{ h: string }>(d, "SELECT sessions.session_erasure_replay_digest($1) h", [b.manifest_id]))[0]!.h]).toEqual([v.name, v.sha256]);
      // framed_bytes is exactly the message's octet length, computed without building it.
      const f = (x: string) => 4 + Buffer.byteLength(x, "utf8");
      const len = Buffer.byteLength("rh-erase-replay-manifest/v1", "ascii")
        + ["1", b.manifest_id, b.request_id, b.org_id, b.workspace_id, b.source, b.requested_session_id].reduce((n, x) => n + f(x), 0)
        + f(String(v.aliases.length)) + v.aliases.reduce((n, x) => n + f(x), 0)
        + f(String(v.targets.length)) + v.targets.reduce((n, t) => n + f(t.user_id) + f(t.origin_id) + f(t.source) + f(t.session_id), 0);
      expect([v.name, Number((await rows<{ n: string }>(d, "SELECT sessions.session_erasure_replay_framed_bytes($1)::text n", [b.manifest_id]))[0]!.n)]).toEqual([v.name, len]);
    }
  });
});

describe("062360 down and reapply", () => {
  const DOWN = "supabase/rollbacks/20261105062360_session_erasure_replay_manifest.down.sql";
  async function ledgered(d: Db) {
    await d.db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text)");
    for (const f of migrationFiles(REPO_ROOT, null))
      await d.db.query("INSERT INTO sessions.__96bulls_migrations(name,sha256) VALUES($1,$2) ON CONFLICT DO NOTHING",
        [`upstream:${f}`, createHash("sha256").update(read(`supabase/migrations/${f}`)).digest("hex")]);
  }
  const down = async (d: Db) => {
    try { await d.db.exec(read(DOWN)); } catch (e) { await d.db.exec("ROLLBACK"); throw e; }
  };
  const reapply = async (d: Db) => {
    await d.db.exec(read(`supabase/migrations/${MIGRATION}`));
    await d.db.query("INSERT INTO sessions.__96bulls_migrations(name,sha256) VALUES($1,$2)",
      [`upstream:${MIGRATION}`, createHash("sha256").update(read(`supabase/migrations/${MIGRATION}`)).digest("hex")]);
  };
  const body = async (d: Db, sig: string) => (await rows<{ s: string; d: boolean }>(d, "SELECT prosrc s,prosecdef d FROM pg_proc WHERE oid=$1::regprocedure", [sig]))[0]!;
  const original = (name: string, tag: string) => {
    const src = read("supabase/migrations/20261105050000_operator_session_erase.sql");
    const at = src.indexOf(`CREATE FUNCTION sessions.${name}(`);
    const open = src.indexOf(tag, at) + tag.length;
    return src.slice(open, src.indexOf(tag, open));
  };
  const SESSION = "sessions.ingest_operator_erase_session(text,text,text,text,text,text,text)";
  const FENCE = "sessions.ingest_operator_erase_fence(text,text,text,text,text[])";
  const replayRefs = async (d: Db) => (await rows<{ n: number }>(d, `SELECT count(*)::int n FROM pg_constraint
    WHERE confrelid IN('sessions.session_erasure_receipts'::regclass,'sessions.session_erasure_fences'::regclass)
      AND conrelid::regclass::text LIKE 'sessions.session_erasure_replay%'`))[0]!.n;

  test("the live _session carries 062320's marked erasure check first; the restored 050000 bodies carry none", async () => {
    const d = await database();
    const lines = (await body(d, SESSION)).s.split("\n").map((l) => l.trim());
    expect(lines[lines.indexOf("BEGIN") + 1]).toBe("PERFORM sessions.ingest_org_admission_hold_check(p_org,'erasure'); -- 062320 org admission hold");
    await ledgered(d);
    await down(d);
    expect(await body(d, SESSION)).toEqual({ s: original("ingest_operator_erase_session", "$operator_erase$"), d: true });
    expect(await body(d, FENCE)).toEqual({ s: original("ingest_operator_erase_fence", "$fence$"), d: false });
  });

  test("EMPTY: the down drops the whole manifest and the receipts binding, so nothing references the 050000 tables; reapply and down again", async () => {
    const d = await database();
    await ledgered(d);
    expect(await replayRefs(d)).toBe(2);
    await down(d);
    expect(await replayRefs(d)).toBe(0);
    for (const t of ["requests", "aliases", "targets", "completions"])
      expect((await rows<{ r: string | null }>(d, "SELECT to_regclass($1)::text r", [`sessions.session_erasure_replay_${t}`]))[0]!.r).toBeNull();
    expect(await rows(d, "SELECT 1 FROM pg_constraint WHERE conname='session_erasure_receipts_request_binding'")).toEqual([]);
    expect(await rows(d, "SELECT proname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions' AND proname LIKE 'session\\_erasure\\_replay\\_%'")).toEqual([]);
    expect(await rows(d, "SELECT 1 FROM sessions.__96bulls_migrations WHERE name=$1", [`upstream:${MIGRATION}`])).toEqual([]);
    // The erase is the pre-062360 erase again; the 050000 tables could be dropped (nothing depends on them).
    await scenario(d);
    expect((await erase(d)).uploadsFenced).toBe(true);
    // A rerun of the down refuses: its own ledger row is gone.
    expect(await code(down(d))).toBe("55000");
    // Reapply over the (populated) 050000 tables: a fence written while 062360 was down has no intent.
    await reapply(d);
    expect((await page(d, SID)).reason).toBe("manifest_missing");
    await down(d);
  });

  test("POPULATED: the down keeps the sealed witness with its guards and reader; a reapply verifies the shape and reuses it", async () => {
    const d = await database();
    await scenario(d);
    await erase(d);
    await ownerSeal(d);
    const sealed = await drain(d, SID, 4);
    await ledgered(d);
    await down(d);
    expect(to(await body(d, SESSION))).toBe(original("ingest_operator_erase_session", "$operator_erase$"));
    expect(await replayRefs(d)).toBe(2);
    // The witness is still immutable and readable after the down.
    expect(await code(d.db.query("DELETE FROM sessions.session_erasure_replay_targets"))).toBe("55000");
    expect(await drain(d, SID, 4)).toEqual(sealed);
    // While down, a new erase writes no intent (pre-062360 body).
    await pointer(d, "u-x", "s-9", "m-x");
    await erase(d, "req-9", [ORG, WS, SRC, "s-9"]);
    expect((await page(d, "s-9")).reason).toBe("manifest_missing");
    await reapply(d);
    expect(await drain(d, SID, 4)).toEqual(sealed);
    // A drifted retained table refuses the reapply (55000) and changes nothing.
    await down(d);
    await d.db.exec("ALTER TABLE sessions.session_erasure_replay_targets ADD COLUMN extra text");
    expect(await code(d.db.exec(read(`supabase/migrations/${MIGRATION}`)))).toBe("55000");
    expect(to(await body(d, SESSION))).toBe(original("ingest_operator_erase_session", "$operator_erase$"));
  });

  test("marker attribution: one 062320-marked line in 062360, inside _session; 062320's classes count only 062320; its down refuses while 062360 is applied", async () => {
    const own = read(`supabase/migrations/${MIGRATION}`);
    const marked = own.split("\n").map((l, i) => ({ l, i })).filter((x) => x.l.includes("-- 062320 org admission hold"));
    expect(marked.map((x) => x.l.trim())).toEqual(["PERFORM sessions.ingest_org_admission_hold_check(p_org,'erasure'); -- 062320 org admission hold"]);
    const opened = own.lastIndexOf("CREATE OR REPLACE FUNCTION sessions.ingest_operator_erase_session(", own.split("\n").slice(0, marked[0]!.i).join("\n").length);
    expect(opened).toBeGreaterThanOrEqual(0);
    expect(own.indexOf("$operator_erase$;", opened)).toBeGreaterThan(own.split("\n").slice(0, marked[0]!.i).join("\n").length);
    // The 062320 classes test reads only 062320's own file, so this line is never counted as one of 062320's entries.
    const classes = read("src/hoopdb/__tests__/org-admission-hold-classes.test.ts");
    expect(classes).toContain("20261105062320_ingest_org_admission_fences.sql");
    expect(classes).not.toContain("20261105062360");
    // 062320's down refuses (55000) while 062360 is applied, and changes nothing: _session keeps the marked check.
    const d = await database();
    await ledgered(d);
    await d.db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY, status text, updated_at timestamptz)");
    let refused: { code?: string; message?: string } | undefined;
    try { await d.db.exec(read("supabase/rollbacks/20261105062320_ingest_org_admission_fences.down.sql")); }
    catch (e) { refused = e as { code?: string; message?: string }; await d.db.exec("ROLLBACK"); }
    expect(refused).toMatchObject({ code: "55000", message: "org admission hold: a later migration is applied; roll it back first" });
    expect((await body(d, SESSION)).s).toContain("PERFORM sessions.ingest_org_admission_hold_check(p_org,'erasure'); -- 062320 org admission hold");
  });

  test("the down refuses while a later upstream slot is applied, and with a foreign checksum", async () => {
    const d = await database();
    await ledgered(d);
    await d.db.query("INSERT INTO sessions.__96bulls_migrations(name,sha256) VALUES('upstream:20261105099000_probe.sql','x')");
    expect(await code(down(d))).toBe("55000");
    await d.db.query("DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105099000_probe.sql'");
    await d.db.query("UPDATE sessions.__96bulls_migrations SET sha256='x' WHERE name=$1", [`upstream:${MIGRATION}`]);
    expect(await code(down(d))).toBe("55000");
    expect((await body(d, SESSION)).s).toContain("-- 062360 session erasure replay manifest");
  });
});
const to = (b: { s: string }) => b.s;
