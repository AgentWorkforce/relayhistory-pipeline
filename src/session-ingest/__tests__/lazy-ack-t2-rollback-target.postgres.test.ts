/**
 * Opt-in: the 062210 rollback to its target with 062210 ABSENT, under the non-superuser owner (owner-role-harness.ts).
 * Needs SESSION_INGEST_TEST_PG_URL (loopback fixture superuser, disposable cluster) AND
 * LAZY_ACK_T2_ROLLBACK_TARGET_TREE: a checkout of the rollback target commit (the tree without 062210, e.g. a
 * `git worktree add --detach <dir> <target sha>` with its own `bun install --frozen-lockfile`). Skipped otherwise.
 *
 * Schedule: apply this tree as the owner; roll back every slot later than 062210 (discovered in reverse filename
 * order, each from its real down file as the owner; a later slot without a down fails the test); stage a delivery (claim and stage committed, the commit not yet run); the
 * down as the owner; the commit is refused while readiness is pending (55000 'generation not ready'); then the
 * operator's readiness restoration, all as the owner, with the TARGET tree's own code: (1) its migrator (nothing
 * applied: 062210 is not in that tree; phase 2 marks PASS at its generation), (2) its generation rebind of the lazy
 * policy and admission rows (src/hoopdb/ingest-generation-rebind.ts), (3) the client re-sends the same batch to the
 * target tree's delivery code. Asserted: the 043800 ACK body, no gate table, no 062210 ledger row, PASS at the
 * target generation, the old generation-bound commit payload still refused, exactly one committed receipt for the
 * batch and its decisions.
 */
import { describe, expect, test } from "bun:test";
import pg from "pg";
import { existsSync, readFileSync } from "node:fs";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";
import { laterSlotDowns, migrateAsOwner, ownedLoopbackUrl, ownerDatabase, ownerProfile, rollBackLaterSlots, roleReceipt, OWNER_ROLE } from "./owner-role-harness.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL, tree = process.env.LAZY_ACK_T2_ROLLBACK_TARGET_TREE;
ownedLoopbackUrl(url);
if (tree && !existsSync(tree + "/src/hoopdb/migrate.ts")) throw Error("LAZY_ACK_T2_ROLLBACK_TARGET_TREE is not a checkout");
if (tree && existsSync(tree + "/supabase/migrations/20261105062210_lazy_ack_t2.sql")) throw Error("the rollback target tree must not contain 062210");
const epoch = "3".repeat(64), identity = "c".repeat(64);
const COMMIT = "SELECT sessions.ingest_origin_commit_buffered_batch($1::jsonb) AS result";
const DOWN = readFileSync("supabase/rollbacks/20261105062210_lazy_ack_t2.down.sql", "utf8");
const MAIN_BODY = readFileSync("supabase/migrations/20261105043800_lazy_ack_clock_no_key_update.sql", "utf8")
  .match(/CREATE OR REPLACE FUNCTION sessions\.ingest_lazy_ack_batch\(p jsonb\)[\s\S]*?AS \$lazy_ack_batch\$([\s\S]*?)\$lazy_ack_batch\$;/)![1]!;

describe.skipIf(!url || !tree)("062210 rollback to the target tree (062210 absent), readiness restored by the operator", () => {
  test("down -> refused while pending -> target migrator PASS -> rebind -> the re-sent batch commits once on 043800", async () => {
    const target = {
      migrate: (await import(tree + "/src/hoopdb/migrate.ts")).runMigrations,
      derived: (await import(tree + "/src/session-pipeline/derived-schema.ts")).derivedSchemaAssets,
      rebind: (await import(tree + "/src/hoopdb/ingest-generation-rebind.ts")).rebindIngestGeneration,
      sqlLedger: (await import(tree + "/src/session-ingest/ledger-sql.ts")).sqlLedger,
      deliverBatch: (await import(tree + "/src/session-ingest/deliver.ts")).deliverBatch,
    };
    const db = await ownerDatabase(url!, "t2rbt");
    const fx = new pg.Pool({ connectionString: db.fixtureUrl, max: 6 }), owner = new pg.Pool({ connectionString: db.ownerUrl, max: 3 });
    const scratch = scratchStore();
    const asOwner = async <T>(f: (c: pg.Client) => Promise<T>) => { const c = new pg.Client({ connectionString: db.ownerUrl }); await c.connect();
      try { expect(await roleReceipt(c)).toMatchObject({ current_user: OWNER_ROLE, rolsuper: false, rolbypassrls: false }); return await f(c); } finally { await c.end(); } };
    const runtime = async (sql: string, params: unknown[]) => { const c = await fx.connect();
      try { await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); const r = await c.query(sql, params as never[]); await c.query("COMMIT"); return { ok: true as const, r: r.rows[0] }; }
      catch (e: any) { await c.query("ROLLBACK").catch(() => {}); return { ok: false as const, code: String(e.code), message: String(e.message) }; } finally { c.release(); } };
    const state = async () => ({
      table: (await fx.query("SELECT to_regclass('sessions.lazy_ack_t2_mode')::text r")).rows[0].r as string | null,
      ack043800: (await fx.query("SELECT prosrc FROM pg_proc WHERE oid='sessions.ingest_lazy_ack_batch(jsonb)'::regprocedure")).rows[0].prosrc === MAIN_BODY,
      ledger062210: (await fx.query("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062210_lazy_ack_t2.sql'")).rows[0].n as number,
      marker: (await fx.query("SELECT generation,status FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker'")).rows[0] as { generation: string; status: string },
    });
    const log: Record<string, unknown> = {};
    try {
      await migrateAsOwner(db.ownerUrl);
      // 062210's down refuses while a later slot is applied: those come off first, newest first, as the owner.
      const later = laterSlotDowns("20261105062210_lazy_ack_t2").map((s) => s.name);
      expect(later.length).toBeGreaterThan(0);
      log.laterRolledBack = await rollBackLaterSlots(db.ownerUrl, "20261105062210_lazy_ack_t2");
      expect(log.laterRolledBack).toEqual(later);
      expect(Number((await fx.query("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE \"C\" > 'upstream:20261105062210_lazy_ack_t2.sql'")).rows[0].n)).toBe(0);
      const G = (await derivedSchemaAssets()).expected.generation as string, H = (await target.derived()).expected.generation as string;
      expect(G).not.toBe(H);
      await owner.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation: G, sourceIdentitySha256: identity, controlEpoch: epoch,
        privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
      await owner.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
        capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
        allocatedHighWater: "9007199254740993", generation: G, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
      await owner.query("UPDATE sessions.ingest_history_switch SET live=false WHERE singleton");
      const ledgerFor = (gen: string, mk: typeof sqlLedger, intercept?: (sql: string, p: unknown[]) => void) => mk(async (sql, params) => {
        intercept?.(sql, params as unknown[]); const c = await fx.connect();
        try { await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); const r = await c.query(sql, params as never); await c.query("COMMIT"); return r as never; }
        catch (e) { await c.query("ROLLBACK").catch(() => {}); throw e; } finally { c.release(); }
      }, { acceptanceMode: "buffered", expectedGeneration: gen, originClaimFence: true });
      const t = { ...TENANT, userId: "rbt-u1" }, origin = "rbt-o1";
      const custody = async (raw: any) => {
        const p = await prepareDelivery(t, raw);
        const witness = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant: t,
          originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
          originalRecordCount: raw.records.length, candidateCount: p.records.length,
          records: p.records.map((r: any, i: number) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
          privacy: { complete: true, deletions: [], resets: [] } };
        return { witness, sourceDigest: (await fx.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).rows[0].d as string };
      };
      const mk = async (id: string, ids: string[]) => batch(ids.map((rid) => record({ record_id: rid, revision: 2, session_id: "s-" + rid, origin_id: origin })), { batch_id: id, account_id: await deliveryAccount(t), origin_id: origin });
      const warm = await mk("rbt-warm", ["rbt-warm-r"]);
      expect((await deliverBatch({ ledger: ledgerFor(G, sqlLedger), acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(warm) } as never, t, warm)).outcome).toBe("accepted_buffered");
      const raw = await mk("rbt-b1", ["rbt-r1", "rbt-r2"]); const cust = await custody(raw);
      let payload: string | undefined;
      await deliverBatch({ ledger: ledgerFor(G, sqlLedger, (sql, p) => { if (sql.includes("commit_buffered_batch")) { payload = p[0] as string; throw Object.assign(Error("captured"), { code: "XXCAP" }); } }),
        acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: cust } as never, t, raw).catch(() => {});
      expect(typeof payload).toBe("string");
      const receipts = async () => Number((await fx.query("SELECT count(*)::int n FROM sessions.ingest_batches WHERE batch_id='rbt-b1' AND state='committed'")).rows[0].n);
      expect(await asOwner((c) => c.query(DOWN).then(() => "ok"))).toBe("ok");
      expect(await state()).toEqual({ table: null, ack043800: true, ledger062210: 0, marker: { generation: G, status: "pending" } });
      expect(await runtime(COMMIT, [payload])).toMatchObject({ ok: false, code: "55000", message: expect.stringContaining("generation not ready") });
      // (1) the target tree's migrator, as the owner.
      log.targetMigrate = await asOwner((c) => target.migrate(c as never, { profile: ownerProfile as never }));
      expect(log.targetMigrate).toMatchObject({ applied: 0, held: [] });
      expect(await state()).toEqual({ table: null, ack043800: true, ledger062210: 0, marker: { generation: H, status: "pass" } });
      // (2) the rebind G -> H (dry run, then committed), as the owner.
      log.rebindDry = await asOwner((c) => target.rebind(c as never, { from: G, to: H, dryRun: true }));
      log.rebind = await asOwner((c) => target.rebind(c as never, { from: G, to: H }));
      expect(log.rebind).toMatchObject({ from: G, to: H, policy: "rebound", dryRun: false });
      // The old payload stays bound to G: refused, never committed.
      expect(await runtime(COMMIT, [payload])).toMatchObject({ ok: false, code: "55000" });
      expect(await receipts()).toBe(0);
      // (3) the client re-sends the same batch; the target tree's delivery code serves it.
      log.resend = (await target.deliverBatch({ ledger: ledgerFor(H, target.sqlLedger), acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: cust } as never, t, raw)).outcome;
      expect(log.resend).toBe("accepted_buffered");
      expect(await receipts()).toBe(1);
      expect(Number((await fx.query("SELECT count(*)::int n FROM sessions.ingest_lazy_ack_decisions d JOIN sessions.ingest_claim_attempts a USING(claim_token) WHERE a.batch_id='rbt-b1'")).rows[0].n)).toBe(2);
      expect(await state()).toEqual({ table: null, ack043800: true, ledger062210: 0, marker: { generation: H, status: "pass" } });
      console.log(JSON.stringify({ case: "ROLLBACK-TARGET", server: (await fx.query("SHOW server_version")).rows[0].server_version, G, H, ...log }));
    } finally { await fx.end(); await owner.end(); scratch.done(); await db.drop(); }
  }, 300000);
});
