// The seal fence against the real landed schema (PGlite as Supabase, every migration applied) and a scripted Neon.
// Causal: each refusal is produced by changing exactly one input, and every refusal leaves NO seal row.
import { test, expect, afterEach, setDefaultTimeout } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { migrationFiles } from "../../session-ingest/__tests__/pglite-schema.ts";
import {
  AUTHORITY_LOCK, FENCE_GUARD_CHECK, FENCED_TABLES, FrozenCopyUnknownCommit, MANIFEST_VERSION, NEON_CONTROL_HEADS, NEON_DELETIONS, NEON_RESETS,
  addDigest, emptyDigest, jobBudget, preRead, sealFence, type OrgVerification, type Queryable,
} from "../frozen-source-copy-seal.ts";
import { jobReceipt, reconcileInDoubt } from "../frozen-source-copy-progress.ts";
import { testEnvelope } from "./fixtures/frozen-copy-envelope.ts";

setDefaultTimeout(60000);
const root = join(import.meta.dir, "../../..");
const generation = "a".repeat(64), epoch = "3".repeat(64), identity = "c".repeat(64), baseline = "a".repeat(64), D = "d".repeat(64);
const open: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const close of open.splice(0).reverse()) await close();
});

type Row = Record<string, unknown>;
/** Scripted Neon: answers exactly the sealer's read-only queries from mutable state; `onRead` fires on each privacy read. */
function fakeNeon(state: { status: Row; deletions: Row[]; resets: Row[]; heads: Row[] }) {
  const log: string[] = [];
  let reads = 0;
  const hooks: Array<(n: number) => void> = [];
  const neon: Queryable & { log: string[]; onSnapshot(f: (n: number) => void): void } = {
    log,
    onSnapshot: (f) => void hooks.push(f),
    async query(sql: string) {
      log.push(sql);
      if (/^(BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY|ROLLBACK)$/.test(sql)) return { rows: [] };
      if (sql === "SELECT sessions.delivery_cutover_status() AS s") {
        reads++;
        for (const h of hooks) h(reads);
        return { rows: [{ s: { ...state.status } }] };
      }
      if (sql === NEON_DELETIONS) return { rows: state.deletions };
      if (sql === NEON_RESETS) return { rows: state.resets };
      if (sql === NEON_CONTROL_HEADS) return { rows: [...state.heads].sort((a, b) => String(a.user_id).localeCompare(String(b.user_id))) };
      throw new Error("fake neon: unexpected SQL " + sql);
    },
  };
  return neon;
}

async function fixture() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(() => db.close());
  const read = (p: string) => readFileSync(join(root, p), "utf8");
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const f of migrationFiles(root)) await db.exec(read("supabase/migrations/" + f));
  await db.exec(`CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','${generation}','pass')`);
  await db.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [
    JSON.stringify({ baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: baseline, retentionContractSha256: "b".repeat(64), sourceProofSha256: D, validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" }),
  ]);
  // The copy (what --copy left behind): one deletion, two resets, one settled control head.
  await db.exec(`INSERT INTO sessions.ingest_frozen_source_deletions VALUES('org-a','ws','u1','origin','claude','s1');
    INSERT INTO sessions.ingest_frozen_source_resets VALUES('org-a','ws','u1','req-1',3),('org-b','ws','u2','req-2',0);
    INSERT INTO sessions.ingest_source_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending)
      VALUES('org-a','ws','u1','${epoch}',1,'${D}','reset','{}',false);`);
  const neonState = {
    status: { phase: "active", epoch, privacyBaselineSha256: baseline },
    deletions: [{ org_id: "org-a", workspace_id: "ws", user_id: "u1", origin_id: "origin", source: "claude", session_id: "s1" }],
    resets: [
      { org_id: "org-a", workspace_id: "ws", user_id: "u1", request_id: "req-1", deleted: "3" },
      { org_id: "org-b", workspace_id: "ws", user_id: "u2", request_id: "req-2", deleted: "0" },
    ],
    heads: [{ org_id: "org-a", workspace_id: "ws", user_id: "u1", v: "1", digest: D, pending: false }],
  };
  const neon = fakeNeon(neonState);
  // Supabase = PGlite, with a hook before each statement and an optional COMMIT override (fault injection).
  const log: string[] = [];
  const before: Array<(sql: string) => Promise<void> | void> = [];
  let commit: ((run: () => Promise<unknown>) => Promise<unknown>) | null = null;
  const supa: Queryable = {
    async query(sql: string, params?: unknown[]) {
      log.push(sql);
      for (const h of before) await h(sql);
      if (sql === "COMMIT" && commit) return (await commit(() => db.query(sql))) as never;
      return (await db.query(sql, params as never)) as never;
    },
  };
  const digestOf = (...t: string[]) => {
    const d = emptyDigest();
    for (const x of t) addDigest(d, x);
    return d;
  };
  const orgs: OrgVerification[] = [
    { org: "org-a", records: digestOf("rec-1", "rec-2"), receipts: digestOf("rcpt-1") },
    { org: "org-b", records: digestOf("rec-3"), receipts: emptyDigest() },
  ];
  const seals = async () => ({
    completeness: (await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.ingest_frozen_source_completeness")).rows[0]!.n,
    orgSeals: (await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.ingest_frozen_source_org_seals")).rows[0]!.n,
  });
  const noOwnerCheck = { requireOwner: async () => {} }; // PGlite's user is a superuser; identity is tested in the pure suite
  return { db, neon, neonState, supa, log, before, setCommit: (f: typeof commit) => void (commit = f), orgs, seals, noOwnerCheck };
}
const refusal = async (p: Promise<unknown>) => {
  try {
    await p;
    return "no-refusal";
  } catch (e) {
    return (e as { code?: string }).code ?? String(e);
  }
};

test("the landed guard covers exactly the fenced tables with the sealer's key", async () => {
  const f = await fixture();
  expect((await f.db.query<{ n: number }>(FENCE_GUARD_CHECK)).rows[0]!.n).toBe(FENCED_TABLES.length);
});

test("success: one transaction writes org seals (privacy = deletions + resets) and a completeness row whose manifest lists resets", async () => {
  const f = await fixture();
  const pre = await preRead(f.neon, f.supa);
  const r = await sealFence(f.neon, f.supa, { orgs: f.orgs, pre }, f.noOwnerCheck);
  expect(r).toMatchObject({ inserted: true, orgs: 2, records: "3", deletions: "1", resets: "2", controlHeads: "1" });
  const c = (await f.db.query<any>("SELECT * FROM sessions.ingest_frozen_source_completeness")).rows[0];
  expect(c).toMatchObject({ source_identity_sha256: identity, control_epoch: epoch, privacy_baseline_sha256: baseline, manifest_sha256: r.manifestSha256 });
  expect(String(c.org_count)).toBe("2");
  expect(c.neon_snapshot).toMatchObject({ manifestVersion: MANIFEST_VERSION, verified: ["records", "receipts", "deletions", "resets", "control_heads"], resetRows: "2", deletionRows: "1", privacyRows: "3" });
  expect(c.neon_snapshot.privacyBaseline).toContain("not recomputed");
  const os = (await f.db.query<any>("SELECT org_id,record_rows::text r,privacy_rows::text p FROM sessions.ingest_frozen_source_org_seals ORDER BY 1")).rows;
  expect(os).toEqual([{ org_id: "org-a", r: "2", p: "2" }, { org_id: "org-b", r: "1", p: "1" }]);
  // Exclusive key taken before the serializable transaction, released after; Neon read-only snapshot opened and rolled back.
  const lockAt = f.log.indexOf("SELECT pg_advisory_lock($1,$2)"), beginAt = f.log.indexOf("BEGIN ISOLATION LEVEL SERIALIZABLE");
  expect(lockAt).toBeGreaterThanOrEqual(0);
  expect(beginAt).toBeGreaterThan(lockAt);
  expect(f.log.at(-1)).toBe("SELECT pg_advisory_unlock($1,$2)");
  expect(f.neon.log.filter((s) => s.startsWith("BEGIN")).length).toBe(f.neon.log.filter((s) => s === "ROLLBACK").length);
  expect((await f.db.query<{ n: number }>("SELECT count(*)::int n FROM pg_locks WHERE locktype='advisory'")).rows[0]!.n).toBe(0);
  // Rerun with the same manifest is idempotent; the completeness row is not rewritten.
  const again = await sealFence(f.neon, f.supa, { orgs: f.orgs, pre: await preRead(f.neon, f.supa) }, f.noOwnerCheck);
  expect(again).toMatchObject({ inserted: false, manifestSha256: r.manifestSha256 });
});

test("the exclusive authority key is held for the whole fence (a guarded writer elsewhere would wait)", async () => {
  const f = await fixture();
  let heldAtCommit = -1;
  f.before.push(async (sql) => {
    if (sql === "COMMIT") heldAtCommit = (await f.db.query<{ n: number }>("SELECT count(*)::int n FROM pg_locks WHERE locktype='advisory' AND mode='ExclusiveLock' AND classid=$1 AND objid=$2", [AUTHORITY_LOCK[0], AUTHORITY_LOCK[1]])).rows[0]!.n;
  });
  await sealFence(f.neon, f.supa, { orgs: f.orgs, pre: await preRead(f.neon, f.supa) }, f.noOwnerCheck);
  expect(heldAtCommit).toBe(1);
});

for (const [name, mutate, codeWanted] of [
  ["a changed reset row (same key, different deleted)", "UPDATE sessions.ingest_frozen_source_resets SET deleted=4 WHERE request_id='req-1'", "frozen_copy_reset_changed"],
  ["a missing reset row", "DELETE FROM sessions.ingest_frozen_source_resets WHERE request_id='req-2'", "frozen_copy_reset_missing"],
  ["an extra reset row", "INSERT INTO sessions.ingest_frozen_source_resets VALUES('org-b','ws','u2','req-9',1)", "frozen_copy_reset_extra"],
  ["a missing deletion", "DELETE FROM sessions.ingest_frozen_source_deletions", "frozen_copy_deletion_mismatch"],
] as const)
  test(`reset/deletion parity at seal: ${name} refuses before any seal row`, async () => {
    const f = await fixture();
    await f.db.exec(mutate);
    expect(await refusal(preRead(f.neon, f.supa))).toBe(codeWanted);
    expect(await f.seals()).toEqual({ completeness: 0, orgSeals: 0 });
  });

test("a reset row changed AFTER the pre-read is caught inside the fence (rollback, no seal row)", async () => {
  const f = await fixture();
  const pre = await preRead(f.neon, f.supa);
  await f.db.exec("UPDATE sessions.ingest_frozen_source_resets SET deleted=5 WHERE request_id='req-2'");
  expect(await refusal(sealFence(f.neon, f.supa, { orgs: f.orgs, pre }, f.noOwnerCheck))).toBe("frozen_copy_reset_changed");
  expect(await f.seals()).toEqual({ completeness: 0, orgSeals: 0 });
});

test("mirror advance between the pre-read and the in-transaction read: frozen_copy_mirror_advanced, ROLLBACK, no seal", async () => {
  const f = await fixture();
  const pre = await preRead(f.neon, f.supa);
  // A mirror writer commits a new control head just before the fence takes the key.
  f.before.push(async (sql) => {
    if (sql === "SELECT pg_advisory_lock($1,$2)")
      await f.db.exec(`INSERT INTO sessions.ingest_source_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending)
        VALUES('org-b','ws','u2','${epoch}',1,'${D}','selection','{}',true)`);
  });
  expect(await refusal(sealFence(f.neon, f.supa, { orgs: f.orgs, pre }, f.noOwnerCheck))).toBe("frozen_copy_mirror_advanced");
  expect(f.log).toContain("ROLLBACK");
  expect(f.log).not.toContain("COMMIT");
  expect(await f.seals()).toEqual({ completeness: 0, orgSeals: 0 });
});

test("a mirror head version bump (same user) is also an advance", async () => {
  const f = await fixture();
  const pre = await preRead(f.neon, f.supa);
  await f.db.exec("UPDATE sessions.ingest_source_control_heads SET version=2");
  expect(await refusal(sealFence(f.neon, f.supa, { orgs: f.orgs, pre }, f.noOwnerCheck))).toBe("frozen_copy_mirror_advanced");
  expect(await f.seals()).toEqual({ completeness: 0, orgSeals: 0 });
});

test("Neon advancing after the pre-read (heads move on both sides in lockstep) still refuses", async () => {
  const f = await fixture();
  const pre = await preRead(f.neon, f.supa);
  f.neonState.heads = [{ ...f.neonState.heads[0]!, v: "2" }];
  await f.db.exec("UPDATE sessions.ingest_source_control_heads SET version=2");
  expect(await refusal(sealFence(f.neon, f.supa, { orgs: f.orgs, pre }, f.noOwnerCheck))).toBe("frozen_copy_mirror_advanced");
  expect(await f.seals()).toEqual({ completeness: 0, orgSeals: 0 });
});

test("Neon privacy advancing after the pre-read (both sides agree, but not with the pre-read) refuses", async () => {
  const f = await fixture();
  const pre = await preRead(f.neon, f.supa);
  f.neonState.resets = [...f.neonState.resets, { org_id: "org-b", workspace_id: "ws", user_id: "u2", request_id: "req-3", deleted: "1" }];
  await f.db.exec("INSERT INTO sessions.ingest_frozen_source_resets VALUES('org-b','ws','u2','req-3',1)");
  expect(await refusal(sealFence(f.neon, f.supa, { orgs: f.orgs, pre }, f.noOwnerCheck))).toBe("frozen_copy_privacy_advanced");
  expect(await f.seals()).toEqual({ completeness: 0, orgSeals: 0 });
});

const rebind = (f: Awaited<ReturnType<typeof fixture>>, patch: Record<string, string>) =>
  f.db.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [
    JSON.stringify({ baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: baseline, retentionContractSha256: "b".repeat(64), sourceProofSha256: D, validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud", ...patch }),
  ]);

test("the landed policy setter itself refuses a source-identity rebind", async () => {
  const f = await fixture();
  await expect(rebind(f, { sourceIdentitySha256: "5".repeat(64) })).rejects.toThrow("immutable");
});

for (const [name, change, codeWanted] of [
  ["stale epoch (Neon re-epoched after the pre-read)", (f: Awaited<ReturnType<typeof fixture>>) => void (f.neonState.status = { ...f.neonState.status, epoch: "4".repeat(64) }), "frozen_copy_stale_epoch"],
  ["stale privacy baseline (descriptor moved)", (f: Awaited<ReturnType<typeof fixture>>) => void (f.neonState.status = { ...f.neonState.status, privacyBaselineSha256: "9".repeat(64) }), "frozen_copy_stale_privacy_baseline"],
  // ingest_lazy_policy_set refuses an identity rebind ("immutable"); the sealer re-checks anyway (defence in depth), so
  // the row is changed underneath it directly.
  ["stale source identity (policy row changed underneath)", (f: Awaited<ReturnType<typeof fixture>>) => f.db.exec("ALTER TABLE sessions.ingest_lazy_policy DISABLE TRIGGER USER; UPDATE sessions.ingest_lazy_policy SET source_identity_sha256=repeat('5',64); ALTER TABLE sessions.ingest_lazy_policy ENABLE TRIGGER USER"), "frozen_copy_stale_source_identity"],
  ["Neon left the frozen phase", (f: Awaited<ReturnType<typeof fixture>>) => void (f.neonState.status = { ...f.neonState.status, phase: "rollback" }), "frozen_copy_neon_not_frozen"],
] as const)
  test(`epoch/baseline/identity are checked in the same transaction: ${name}`, async () => {
    const f = await fixture();
    const pre = await preRead(f.neon, f.supa);
    await change(f);
    expect(await refusal(sealFence(f.neon, f.supa, { orgs: f.orgs, pre }, f.noOwnerCheck))).toBe(codeWanted);
    expect(await f.seals()).toEqual({ completeness: 0, orgSeals: 0 });
  });

test("authority already active refuses before any read", async () => {
  const f = await fixture();
  const pre = await preRead(f.neon, f.supa);
  await f.db.query("INSERT INTO sessions.ingest_privacy_authority(state,control_epoch,neon_fingerprint_sha256,activated_at) VALUES('active',$1,$2,now())", [epoch, D]);
  expect(await refusal(sealFence(f.neon, f.supa, { orgs: f.orgs, pre }, f.noOwnerCheck))).toBe("frozen_copy_privacy_authority_active");
  expect(f.neon.log.filter((s) => s.startsWith("BEGIN")).length).toBe(1); // only the pre-read's snapshot
  expect(await f.seals()).toEqual({ completeness: 0, orgSeals: 0 });
});

test("serialization failure at COMMIT: refusal, ROLLBACK, no retry, no seal row, key released", async () => {
  const f = await fixture();
  const pre = await preRead(f.neon, f.supa);
  let commits = 0;
  f.setCommit(async () => {
    commits++;
    throw Object.assign(new Error("could not serialize access due to read/write dependencies among transactions"), { code: "40001" });
  });
  expect(await refusal(sealFence(f.neon, f.supa, { orgs: f.orgs, pre }, f.noOwnerCheck))).toBe("frozen_copy_serialization_failure");
  expect(commits).toBe(1);
  expect(f.log.slice(f.log.indexOf("COMMIT"))).toEqual(["COMMIT", "ROLLBACK", "SELECT pg_advisory_unlock($1,$2)"]);
  expect(await f.seals()).toEqual({ completeness: 0, orgSeals: 0 });
  expect((await f.db.query<{ n: number }>("SELECT count(*)::int n FROM pg_locks WHERE locktype='advisory'")).rows[0]!.n).toBe(0);
});

test("budget exhausted at the last check before COMMIT: refused receipt, ROLLBACK, no seal row", async () => {
  const f = await fixture();
  const pre = await preRead(f.neon, f.supa);
  let t = 0;
  const budget = jobBudget(100, () => t);
  f.before.push((sql) => {
    if (sql.includes("ingest_frozen_source_completeness(")) t = 100; // the clock runs out while writing the seal
  });
  let error: unknown;
  try {
    await sealFence(f.neon, f.supa, { orgs: f.orgs, pre }, { ...f.noOwnerCheck, budget: budget.check });
  } catch (e) {
    error = e;
  }
  expect((error as { code?: string }).code).toBe("frozen_copy_budget_exhausted");
  expect(f.log).not.toContain("COMMIT");
  expect(await f.seals()).toEqual({ completeness: 0, orgSeals: 0 });
  const receipt = jobReceipt("seal", error, testEnvelope({ startedAt: budget.startedAt, finishedAt: t }));
  expect(receipt).toMatchObject({ outcome: "refused", status: "REFUSED", code: "frozen_copy_budget_exhausted", seal: null, budget: { exceeded: true, elapsedWallMs: 100 } });
  expect(receipt.next).toContain("no seal row");
  // No completeness row: an in-doubt seal would read back NOT_COMMITTED.
  expect(await reconcileInDoubt(f.supa, { phase: "seal", dataset: null, checkpointSeq: null, manifestSha256: "a".repeat(64), detectedAt: 0 })).toBe("NOT_COMMITTED");
});

test("unknown commit: COMMIT reached the server but the answer was lost; the receipt says unknown-commit and the seal exists", async () => {
  const f = await fixture();
  const pre = await preRead(f.neon, f.supa);
  f.setCommit(async (run) => {
    await run();
    throw new Error("Connection terminated unexpectedly");
  });
  let error: unknown;
  try {
    await sealFence(f.neon, f.supa, { orgs: f.orgs, pre }, f.noOwnerCheck);
  } catch (e) {
    error = e;
  }
  expect(error).toBeInstanceOf(FrozenCopyUnknownCommit);
  expect(f.log.slice(f.log.indexOf("COMMIT") + 1)).toEqual(["SELECT pg_advisory_unlock($1,$2)"]); // no ROLLBACK after an in-doubt COMMIT
  expect(await f.seals()).toEqual({ completeness: 1, orgSeals: 2 });
  // Readback from the target: the completeness row carries exactly this run's manifest -> COMMITTED (never a blind retry).
  const ctx = (error as FrozenCopyUnknownCommit).context;
  expect(ctx).toMatchObject({ phase: "seal", dataset: null, checkpointSeq: null });
  expect(ctx.manifestSha256).toMatch(/^[0-9a-f]{64}$/);
  const result = await reconcileInDoubt(f.supa, ctx);
  expect(result).toBe("COMMITTED");
  expect(await reconcileInDoubt(f.supa, { ...ctx, manifestSha256: "e".repeat(64) })).toBe("REFUSED");
  const receipt = jobReceipt("seal", error, testEnvelope({ reconciliation: { result, readbackAt: 5 } }));
  expect(receipt).toMatchObject({ outcome: "unknown-commit", status: "UNKNOWN_COMMIT", code: "frozen_copy_unknown_commit",
    unknownCommit: { detected: true, phase: "seal", reconciliation: "COMMITTED", noBlindRetry: true } });
  expect(receipt.unknownCommit.resolvedAt).toBe(new Date(5).toISOString());
  expect(receipt.next).toContain("ingest_frozen_source_completeness");
});

test("an existing completeness row for a different manifest refuses (evidence is immutable)", async () => {
  const f = await fixture();
  await f.db.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')", [identity, epoch, baseline, "e".repeat(64)]);
  expect(await refusal(sealFence(f.neon, f.supa, { orgs: f.orgs, pre: await preRead(f.neon, f.supa) }, f.noOwnerCheck))).toBe("frozen_copy_completeness_conflict");
  expect((await f.seals()).orgSeals).toBe(0);
});
