/**
 * ISOLATED SYNTHETIC PROOF (docs/design/org-bridge-gate.md, W2 (1) and (2); lander: design and isolated proof only).
 * Nothing here is product code or a migration: the stand-in schema `cutover_proof` exists only in this test
 * database, over every real migration (062320 hold, 062340 erasure-history capture triggers).
 *
 * What it pins:
 *   - The serialization point. A publish takes the org hold key EXCLUSIVE, then stores pg_current_snapshot() in a
 *     later statement. Every erasure-history row is either visible in that snapshot (committed before: the publish
 *     refuses when it is after the bound export snapshot) or not (committed after: the effect is revoked).
 *     The history `xid` is assigned before the trigger's lock, so ordering is by snapshot visibility, never by xid.
 *   - Revocation is the erase's own SQL commit, through a real source-table write (the capture trigger), with no
 *     replay and no S3: a pre-IO verdict that said visible and a post-IO verdict after the erase disagree, and the
 *     reader must refuse.
 *   - Scope is org-wide with 062340 as it is (option (i), fail closed): an erase of an unrelated session of the
 *     same org revokes too, until reconciliation or B's scope columns.
 *   - The installer race (A): a withdrawal that consumes the request id in SQL (_install then
 *     _release(verified_rollback)) leaves a paused installer unable to commit that request.
 *   - Publish refuses once the hold is released (late drain or retry after the switch).
 * Two-connection orderings (a publish waiting for an in-flight erase that holds the key SHARED) need real PostgreSQL:
 * they belong in a .postgres.test.ts on the reserved PG17 lane (claim-ci), not here.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { authorityFixture } from "./privacy-authority-support.ts";
import { STAND_IN } from "./cutover-visibility-standin.ts";

setDefaultTimeout(180_000);
const open: Array<() => unknown> = [];
afterEach(async () => { for (const c of open.splice(0).reverse()) await c(); });

const SLOT = "upstream:20261105062320_ingest_org_admission_fences.sql";
const HISTORY_SLOT = "upstream:20261105062340_ingest_org_erasure_history.sql";
const ORG = "org-proof-a";
const OTHER = "org-proof-b";
const EVIDENCE = "e".repeat(64);

async function fixture() {
  const f = await authorityFixture();
  open.push(f.close);
  await f.db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text)");
  for (const s of [SLOT, HISTORY_SLOT]) await f.db.query("INSERT INTO sessions.__96bulls_migrations(name) VALUES($1) ON CONFLICT DO NOTHING", [s]);
  await f.db.exec(STAND_IN);
  const one = async <T = Record<string, unknown>>(sql: string, p: unknown[] = []) => (await f.db.query<T>(sql, p as never)).rows[0]!;
  const install = async (org: string, request = crypto.randomUUID()) =>
    (await one<{ e: string }>("SELECT sessions.ingest_org_admission_hold_install($1, $2::uuid)::text AS e", [org, request])).e;
  /** The export's bound: the snapshot its SQL verdicts were read under. */
  const snapshot = async () => (await one<{ s: string }>("SELECT pg_current_snapshot()::text AS s")).s;
  const publish = async (org: string, digest: string, session: string, bound: string) =>
    (await one<{ r: string }>("SELECT cutover_proof.publish($1, 'drain', $2, $3, $4::pg_snapshot) AS r", [org, digest, session, bound])).r;
  const verdict = async (org: string, digest: string, session: string) =>
    (await one<{ r: string }>("SELECT cutover_proof.verdict($1, 'drain', $2, $3) AS r", [org, digest, session])).r;
  /** A SQL-only erasure through a real source table (its 062340 trigger records the history row under the key). */
  const erase = (org: string, session: string) => f.db.query(
    "INSERT INTO sessions.upload_deletions (org_id, workspace_id, user_id, origin_id, source, session_id) VALUES ($1, 'ws-proof', 'user-proof', 'origin-proof', 'claude', $2)", [org, session]);
  return { f, one, install, snapshot, publish, verdict, erase };
}

describe("W2 isolated proof: visibility under the 062340 history lock (stand-in schema only)", () => {
  test("the capture trigger records a real source write, with an xid and no session scope", async () => {
    const x = await fixture();
    await x.install(ORG);
    await x.erase(ORG, "sess-1");
    const h = await x.one<{ n: number; cols: string }>(`SELECT count(*)::int n,
      (SELECT string_agg(attname, ',' ORDER BY attnum) FROM pg_attribute WHERE attrelid = 'sessions.ingest_org_erasure_history'::regclass AND attnum > 0 AND NOT attisdropped) cols
      FROM sessions.ingest_org_erasure_history WHERE erased_org = $1 AND source = 'upload_deletions'`, [ORG]);
    expect(h.n).toBe(1);
    expect(h.cols).toBe("erased_org,source,op,at,xid"); // no workspace/user/session: revocation is org-wide (option i)
  });

  test("erase first (after the export's bound): the publish refuses, nothing becomes visible", async () => {
    const x = await fixture();
    await x.install(ORG);
    const bound = await x.snapshot();
    await x.erase(ORG, "sess-2");
    expect(await x.publish(ORG, "stage-a", "sess-2", bound)).toBe("refused_erased");
    expect(await x.verdict(ORG, "stage-a", "sess-2")).toBe("absent");
  });

  test("an erase committed before the export's bound does not refuse (the export already saw it)", async () => {
    const x = await fixture();
    await x.install(ORG);
    await x.erase(ORG, "sess-0");
    const bound = await x.snapshot();
    expect(await x.publish(ORG, "stage-a", "sess-1", bound)).toBe("visible");
    expect(await x.verdict(ORG, "stage-a", "sess-1")).toBe("visible");
  });

  test("publish first, then a SQL-only erase: revoked at the erase's own commit; a read whose post-IO verdict follows it refuses", async () => {
    const x = await fixture();
    await x.install(ORG);
    const bound = await x.snapshot();
    expect(await x.publish(ORG, "stage-a", "sess-1", bound)).toBe("visible");
    const preIo = await x.verdict(ORG, "stage-a", "sess-1");
    await x.erase(ORG, "sess-1"); // commits while the read's IO is in flight
    const postIo = await x.verdict(ORG, "stage-a", "sess-1");
    expect([preIo, postIo]).toEqual(["visible", "revoked"]);
    // No replay, no S3: the revocation is already in force, and it is stable.
    expect(await x.verdict(ORG, "stage-a", "sess-1")).toBe("revoked");
  });

  test("scope is org-wide with 062340 as is: another session of the same org revokes too (fail closed); another org does not", async () => {
    const x = await fixture();
    await x.install(ORG);
    await x.install(OTHER);
    const bound = await x.snapshot();
    expect(await x.publish(ORG, "stage-a", "sess-1", bound)).toBe("visible");
    expect(await x.publish(OTHER, "stage-b", "sess-1", bound)).toBe("visible");
    await x.erase(ORG, "sess-unrelated");
    expect(await x.verdict(ORG, "stage-a", "sess-1")).toBe("revoked");
    expect(await x.verdict(OTHER, "stage-b", "sess-1")).toBe("visible");
  });

  test("ordering is by snapshot: a transaction not committed at publish time is not visible in the stored snapshot, so its later history row revokes (the xid-before-commit interleaving itself needs two connections: PG17 lane)", async () => {
    const x = await fixture();
    await x.install(ORG);
    const bound = await x.snapshot();
    expect(await x.publish(ORG, "stage-a", "sess-1", bound)).toBe("visible");
    // The history row of a later commit is not visible in the stored snapshot, whatever its xid.
    const v = await x.one<{ visible: boolean }>(`SELECT pg_visible_in_snapshot(pg_current_xact_id(), (SELECT published_snapshot FROM cutover_proof.effects LIMIT 1)) AS visible`);
    expect(v.visible).toBe(false);
    await x.erase(ORG, "sess-9");
    expect(await x.verdict(ORG, "stage-a", "sess-1")).toBe("revoked");
  });

  test("a late drain or a retry after the switch: publish refuses once the hold is released", async () => {
    const x = await fixture();
    const e = await x.install(ORG);
    const bound = await x.snapshot();
    await x.f.db.query("SELECT sessions.ingest_org_admission_hold_release($1, $2::uuid, 'verified_rollback', $3, NULL, NULL)", [ORG, e, EVIDENCE]);
    expect(await x.publish(ORG, "stage-late", "sess-1", bound)).toBe("refused_released");
    expect(await x.verdict(ORG, "stage-late", "sess-1")).toBe("absent");
  });

  test("installer race (A): the withdrawal consumes the request in SQL first, so a paused installer can never commit it", async () => {
    const x = await fixture();
    const request = crypto.randomUUID();
    // The withdrawal: _install(org, request) (creates the hold the installer never committed), then verified_rollback.
    const e = await x.install(ORG, request);
    await x.f.db.query("SELECT sessions.ingest_org_admission_hold_release($1, $2::uuid, 'verified_rollback', $3, NULL, NULL)", [ORG, e, EVIDENCE]);
    // The paused installer resumes with the same request id: refused, no hold.
    await expect(x.install(ORG, request)).rejects.toMatchObject({ code: "55000" });
    const live = await x.one<{ n: number }>("SELECT count(*)::int n FROM sessions.ingest_org_admission_holds WHERE held_org = $1 AND state <> 'released'", [ORG]);
    expect(live.n).toBe(0);
  });
});
