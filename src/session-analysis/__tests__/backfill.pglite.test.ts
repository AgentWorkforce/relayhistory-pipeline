/**
 * Backfill + parity (F1.7/F1.8) on the real migration: a Neon v1 payload (the shape
 * read_session_analysis_v1 serves, brief-era md5 ids and microsecond timestamps
 * included) goes in, SP's GET comes out byte-equal under canonical JSON.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { objectKey, type ObjectStore } from "../../../packages/object-store/src/index.ts";
import { sweepAnalysisTickets } from "../store.ts";
import { storeEraser } from "../../migration/neon-s3-erasure/eraser.ts";
import { FakeS3Versions } from "../../migration/__tests__/fake-s3-versions.ts";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { database, hoopDb, pointer, read, KEY } from "../../session-brief/__tests__/harness.ts";
import {
  applyBackfill,
  backfillScope,
  BackfillError,
  expectedAfterBackfill,
  MIGRATION_INTERRUPTED,
  parityEqual,
  planBackfill,
  type NeonV1Payload,
} from "../backfill.ts";
import { analysisResultPrefix, readAnalysisResult } from "../results.ts";
import { assembleAnalyses, type ReadRpc } from "../read.ts";

setDefaultTimeout(60_000);
const SCOPE = { orgId: KEY.orgId, workspaceId: KEY.workspaceId, sessionId: KEY.sessionId };
const V1 = "6f0c3a52-8d2a-5b3e-a4e1-9f1e0d2c3b4a";
const NEON: NeonV1Payload = {
  outcome: "ok",
  sessionId: KEY.sessionId,
  requests: [
    { id: "22222222-2222-4222-8222-222222222222", sessionId: KEY.sessionId, status: "completed", analysisId: V1, error: null,
      createdAt: "2026-10-01T00:00:00.123456+00:00", updatedAt: "2026-10-01T00:00:03.5+00:00" },
    { id: "33333333-3333-4333-8333-333333333333", sessionId: KEY.sessionId, status: "failed", analysisId: null,
      error: "session_evidence_incomplete", createdAt: "2026-09-30T00:00:00+00:00", updatedAt: "2026-09-30T00:00:01+00:00" },
  ],
  analyses: [
    { id: V1, sessionId: KEY.sessionId, sourceProvider: "claude", sourceFingerprint: "abc", status: "completed", attempts: 1,
      analysisVersion: "session-analysis-v2.4:relayhistory-session-v2", schemaVersion: 2,
      summaryVersion: "session-summary-v3:workspace-output-v2", modelVersion: "gpt-5.4", summaryModelVersion: "gpt-5.6-luna",
      result: { schemaVersion: 1, analysisVersion: "session-analysis-v2.4:relayhistory-session-v2", sourceFingerprint: "abc",
                analysis: { title: "Fix the queue" }, durationMs: 12, brief: { title: "Fix the queue", state: "completed" } },
      error: null, createdAt: "2026-10-01T00:00:00.2+00:00", updatedAt: "2026-10-01T00:00:03.5+00:00",
      completedAt: "2026-10-01T00:00:03.5+00:00" },
  ],
};

let pg: PGlite | undefined;
afterEach(async () => {
  await pg?.close();
  pg = undefined;
});
const fresh = async () => {
  const db = (pg = await database());
  await db.exec(read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  await pointer(db, { manifestKey: "org-a/ws-a/m-1/claude/2026-10-29/s-1/manifest.json" });
  return db;
};
const tempStore = () => createFsStore({ root: mkdtempSync(join(tmpdir(), "analysis-bf-")) });
/** The shared paged eraser over the unversioned fs store (src/migration/neon-s3-erasure). */
const fsEraser = (store: ObjectStore) => ({ eraser: storeEraser(store) });
async function readSp(db: PGlite, store: ObjectStore) {
  await db.exec("BEGIN");
  await db.query("SELECT set_config('bulls96_read.org_id', $1, true), set_config('bulls96_read.workspace_id', $2, true)", [KEY.orgId, KEY.workspaceId]);
  await db.exec("SET LOCAL ROLE bulls96_read_api");
  const rpc = (await db.query<{ r: ReadRpc }>("SELECT sessions.analysis_read_v1($1, $2) AS r", [KEY.workspaceId, KEY.sessionId])).rows[0]!.r;
  await db.exec("ROLLBACK");
  if (rpc.outcome !== "ok") return null;
  return assembleAnalyses(rpc, { result: (ref) => readAnalysisResult(store, ref) });
}
const KEY_PREFIX = analysisResultPrefix("org-a/ws-a/m-1/claude/2026-10-29/s-1/manifest.json");

test("backfilled history is served byte-equal to Neon, idempotently", async () => {
  const db = await fresh();
  const store = tempStore();
  const plan = planBackfill(SCOPE, NEON);
  expect(await applyBackfill(hoopDb(db), store, SCOPE, plan, fsEraser(store))).toEqual({ jobs: 1, requests: 2, objects: 1 });
  await applyBackfill(hoopDb(db), store, SCOPE, plan, fsEraser(store));
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_requests_v2")).rows).toHaveLength(2);
  // The result key derives from the live anchor, never a caller prefix; no ticket is left.
  expect((await db.query<{ k: string }>("SELECT result_s3_key k FROM sessions.session_analysis_jobs_v2")).rows[0]!.k.startsWith(KEY_PREFIX)).toBe(true);
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_result_tickets")).rows).toHaveLength(0);
  const sp = (await readSp(db, store))!;
  expect(parityEqual(NEON, sp)).toEqual({ equal: true, requests: 2, analyses: 1 });
  // A changed byte is caught, and the verdict carries no content.
  const drift = structuredClone(sp);
  (drift.analyses[0] as any).attempts = 2;
  expect(parityEqual(NEON, drift)).toEqual({ equal: false, requests: 2, analyses: 1 });
});

test("P1-3: in-flight Neon work is terminalized, parity compares the terminalized payload", async () => {
  const db = await fresh();
  const store = tempStore();
  const neon = structuredClone(NEON) as any;
  neon.analyses[0] = { ...neon.analyses[0], status: "running", result: null, completedAt: null };
  neon.requests[0] = { ...neon.requests[0], status: "running" };
  const plan = planBackfill(SCOPE, neon);
  expect(plan.terminalized).toBe(2);
  const out = await backfillScope({ db: hoopDb(db), store, readSp: () => readSp(db, store), ...fsEraser(store) }, SCOPE, neon);
  expect(out.parity.equal).toBe(true);
  const rows = (await db.query<Record<string, unknown>>("SELECT status, last_error FROM sessions.session_analysis_jobs_v2")).rows;
  expect(rows).toEqual([{ status: "failed", last_error: MIGRATION_INTERRUPTED }]);
  expect(expectedAfterBackfill(neon)).not.toEqual(neon);
});

test("P0-2: an erased scope is refused before any write; an erase mid-backfill leaves a ticket the sweeper clears", async () => {
  const db = await fresh();
  const store = tempStore();
  await db.query("UPDATE sessions.session_index SET deleted_at = now()");
  await expect(applyBackfill(hoopDb(db), store, SCOPE, planBackfill(SCOPE, NEON), fsEraser(store))).rejects.toThrow("backfill_session_not_live");
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_result_tickets")).rows).toHaveLength(0);

  await db.query("UPDATE sessions.session_index SET deleted_at = NULL");
  let written = "";
  const racing: ObjectStore = {
    ...store,
    head: (k) => store.head(k), get: (k, r) => store.get(k, r), getObject: (k, r) => store.getObject(k, r), list: (p, c) => store.list(p, c),
    presignGet: (k, t) => store.presignGet(k, t), delete: (k) => store.delete(k), backend: store.backend,
    async put(key, body, opts) {
      written = key;
      const ack = await store.put(key, body, opts);
      await db.query("UPDATE sessions.session_index SET deleted_at = now()");
      return ack;
    },
  };
  // The post-write ownership fence sees the erase. Our ticket becomes a durable lease-free
  // 'forgotten' one (an unversioned store gives no version id to erase exactly), and the
  // sweeper's every-version erase clears the object.
  await expect(applyBackfill(hoopDb(db), racing, SCOPE, planBackfill(SCOPE, NEON), fsEraser(store))).rejects.toThrow("backfill_ticket_lease_lost");
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_jobs_v2")).rows).toHaveLength(0);
  expect((await db.query<{ k: string; reason: string; lease: unknown }>("SELECT result_key k, reason, lease_expires_at lease FROM sessions.session_analysis_result_tickets")).rows)
    .toEqual([{ k: written, reason: "forgotten", lease: null }]);
  expect(await sweepAnalysisTickets(store, { graceSeconds: 0, transact: (w) => w(hoopDb(db) as never), ...fsEraser(store) })).toBe(1);
  expect(await store.head(objectKey(written))).toBeUndefined();
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_result_tickets")).rows).toHaveLength(0);
});

test("P2: an existing row that drifted is refused; the parity gate refuses a drifted SP answer", async () => {
  const db = await fresh();
  const store = tempStore();
  const plan = planBackfill(SCOPE, NEON);
  await applyBackfill(hoopDb(db), store, SCOPE, plan, fsEraser(store));
  await db.query("UPDATE sessions.session_analysis_jobs_v2 SET attempts = 7");
  await expect(applyBackfill(hoopDb(db), store, SCOPE, plan, fsEraser(store))).rejects.toThrow("backfill_existing_row_drift");
  await db.query("UPDATE sessions.session_analysis_jobs_v2 SET attempts = 1");
  await expect(
    backfillScope({ db: hoopDb(db), store, readSp: async () => ({ sessionId: KEY.sessionId, requests: [], analyses: [] }), ...fsEraser(store) }, SCOPE, NEON),
  ).rejects.toThrow("backfill_parity_failed");
});

test("the planner refuses what it cannot keep exactly", () => {
  const bad = (mut: (p: any) => void) => {
    const p = structuredClone(NEON) as any;
    mut(p);
    return () => planBackfill(SCOPE, p);
  };
  expect(bad((p) => (p.sessionId = "other"))).toThrow(BackfillError);
  expect(bad((p) => (p.analyses[0].result = null))).toThrow("backfill_completed_without_result");
  expect(bad((p) => (p.requests[0].analysisId = "44444444-4444-4444-8444-444444444444"))).toThrow("backfill_request_job_unknown");
  expect(planBackfill(SCOPE, { outcome: "not_found" })).toEqual({ jobs: [], requests: [], terminalized: 0 });
  expect(parityEqual({ outcome: "not_found" }, null).equal).toBe(true);
});

const spyStore = (store: ObjectStore, onPut?: (key: string) => Promise<void>) => {
  const puts: string[] = [];
  const wrapped: ObjectStore = {
    backend: store.backend,
    head: (k) => store.head(k), get: (k, r) => store.get(k, r), getObject: (k, r) => store.getObject(k, r), list: (p, c) => store.list(p, c),
    presignGet: (k, t) => store.presignGet(k, t), delete: (k) => store.delete(k),
    async put(key, body, opts) {
      puts.push(key);
      const ack = await store.put(key, body, opts);
      if (onPut) await onPut(key);
      return ack;
    },
  };
  return { wrapped, puts };
};

test("P1-b: a drifted existing row is refused before any object is written (result_bytes compared)", async () => {
  const db = await fresh();
  const store = tempStore();
  const plan = planBackfill(SCOPE, NEON);
  await applyBackfill(hoopDb(db), store, SCOPE, plan, fsEraser(store));
  await db.query("UPDATE sessions.session_analysis_jobs_v2 SET result_bytes = result_bytes + 1");
  const spy = spyStore(store);
  await expect(applyBackfill(hoopDb(db), spy.wrapped, SCOPE, plan, fsEraser(store))).rejects.toThrow("backfill_existing_row_drift");
  expect(spy.puts).toEqual([]);
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_result_tickets")).rows).toHaveLength(0);
});

test("P1-b: losing the insert race to a drifted row keeps the ticket, so the sweeper deletes the orphan", async () => {
  const db = await fresh();
  const store = tempStore();
  const plan = planBackfill(SCOPE, NEON);
  const job = plan.jobs[0]!;
  // Between the existence check and the insert, another writer lands a different row.
  const spy = spyStore(store, async () => {
    await db.query(
      `INSERT INTO sessions.session_analysis_jobs_v2 (id, org_id, workspace_id, session_id, status, analysis_version, schema_version, model_version, origin, attempts)
       VALUES ($1::uuid, $2, $3, $4, 'failed', 'x:y', 2, 'gpt-5.4', 'neon_backfill', 9)`,
      [job.id, KEY.orgId, KEY.workspaceId, KEY.sessionId],
    );
  });
  await expect(applyBackfill(hoopDb(db), spy.wrapped, SCOPE, plan, fsEraser(store))).rejects.toThrow("backfill_existing_row_drift");
  expect(spy.puts).toHaveLength(1);
  const tickets = (await db.query<{ k: string }>("SELECT result_key k FROM sessions.session_analysis_result_tickets")).rows;
  expect(tickets.map((t) => t.k)).toEqual(spy.puts);
  expect(await sweepAnalysisTickets(store, { graceSeconds: 0, transact: (w) => w(hoopDb(db) as never), ...fsEraser(store) })).toBe(1);
  expect(await store.head(objectKey(spy.puts[0]!))).toBeUndefined();
});

/** hoopDb with a hook that runs just before a statement carrying `marker`. */
const hooked = (db: PGlite, marker: string, before: () => Promise<void>) => {
  const base = hoopDb(db);
  return {
    ...base,
    async query(text: string, params?: unknown[]) {
      if (text.includes(marker)) await before();
      return base.query(text, params as never);
    },
  } as unknown as ReturnType<typeof hoopDb>;
};

test("P1-b: a tombstone interposed after verify never lets the lost-race retirement delete the forgotten ticket", async () => {
  const db = await fresh();
  const store = tempStore();
  const plan = planBackfill(SCOPE, NEON);
  const job = plan.jobs[0]!;
  const bytes = new TextEncoder().encode(JSON.stringify(job.result));
  const { createHash } = await import("node:crypto");
  const sha = createHash("sha256").update(bytes).digest("hex");
  const key = `${KEY_PREFIX}${job.id}/${sha}.json`;
  // Another writer lands the IDENTICAL row between the existence check and our insert.
  const spy = spyStore(store, async () => {
    await db.query(
      `INSERT INTO sessions.session_analysis_jobs_v2 (id, org_id, workspace_id, session_id, harness, status, attempts, analysis_version, schema_version,
         model_version, summary_version, summary_model_version, source_fingerprint, result_s3_key, result_sha256, result_bytes, last_error, origin,
         created_at, updated_at, completed_at)
       VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, 'neon_backfill', $18::timestamptz, $19::timestamptz, $20::timestamptz)`,
      [job.id, KEY.orgId, KEY.workspaceId, KEY.sessionId, job.harness, job.status, job.attempts, job.analysis_version, job.schema_version,
       job.model_version, job.summary_version, job.summary_model_version, job.source_fingerprint, key, sha, bytes.length, job.last_error,
       job.created_at, job.updated_at, job.completed_at],
    );
  });
  // ...and the session is erased right after our verify, before the retirement.
  const tombstone = async () => {
    await db.query("UPDATE sessions.session_index SET deleted_at = now()");
  };
  await expect(applyBackfill(hooked(db, "backfill:retire-lost-race", tombstone), spy.wrapped, SCOPE, plan, fsEraser(store))).rejects.toThrow();
  const tickets = (await db.query<{ k: string; reason: string }>("SELECT result_key k, reason FROM sessions.session_analysis_result_tickets")).rows;
  // The PUT's outcome is known to us, but the erase trigger cannot know it: 'unknown' (never retired on a timer).
  expect(tickets).toEqual([{ k: key, reason: "unknown" }]);
});

test("P1-c: a lapsed backfill lease (fake clock) refuses the Put after a sweep: no orphan can land", async () => {
  const db = await fresh();
  const store = tempStore();
  const spy = spyStore(store);
  // The clock jumps past the lease between the ticket and the Put, and a sweep runs.
  const lapse = async () => {
    await db.query("UPDATE sessions.session_analysis_result_tickets SET lease_expires_at = now() - interval '1 second', created_at = now() - interval '1 day'");
    // A pending write of unknown outcome: swept (nothing there) but kept, never retired on a timer.
    expect(await sweepAnalysisTickets(store, { graceSeconds: 0, transact: (w) => w(hoopDb(db) as never), ...fsEraser(store) })).toBe(0);
  };
  await expect(applyBackfill(hooked(db, "backfill:heartbeat", lapse), spy.wrapped, SCOPE, planBackfill(SCOPE, NEON), fsEraser(store))).rejects.toThrow("backfill_ticket_lease_lost");
  expect(spy.puts).toEqual([]);
  // A live lease survives the sweeper.
  const db2 = await fresh();
  const plan = planBackfill(SCOPE, NEON);
  await applyBackfill(hooked(db2, "backfill:heartbeat", async () => {
    expect(await sweepAnalysisTickets(store, { graceSeconds: 0, transact: (w) => w(hoopDb(db2) as never), ...fsEraser(store) })).toBe(0);
  }), store, SCOPE, plan, fsEraser(store));
  expect((await db2.query("SELECT 1 FROM sessions.session_analysis_result_tickets")).rows).toHaveLength(0);
});

/**
 * A version-retaining S3 over the SHARED fake (src/migration/__tests__/fake-s3-versions.ts):
 * every put creates a version id (no conditional-create shortcut unless `createOnly`); a plain
 * delete only adds a delete marker; `eraser` is the shared strict paged eraser over it. Bytes
 * live in the fs base (removed once no object version of the key is left).
 */
function versionedStore(base: ObjectStore, opts: { createOnly?: boolean } = {}) {
  const s3 = new FakeS3Versions();
  const list = (k: string) => s3.keys.get(k) ?? [];
  const live = (k: string) => list(k).length > 0 && !list(k)[0]!.marker;
  const raw = s3.fetch;
  s3.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const res = await raw(input, init);
    if (init?.method === "DELETE") {
      const key = decodeURIComponent(new URL(String(input)).pathname.replace(/^\/b\//, ""));
      if (!list(key).some((v) => !v.marker)) await base.delete(objectKey(key));
    }
    return res;
  }) as typeof fetch;
  const store: ObjectStore = {
    backend: base.backend,
    head: (k) => base.head(k), get: (k, r) => base.get(k, r), getObject: (k, r) => base.getObject(k, r), list: (p, c) => base.list(p, c),
    presignGet: (k, t) => base.presignGet(k, t),
    async put(key, body, o) {
      const k = String(key);
      if (opts.createOnly && live(k)) {
        // S3 If-None-Match: identical bytes already current -> no new version.
        const ack = await base.put(key, body, o);
        return { ...ack, created: false, version: {} };
      }
      const id = s3.put(k);
      if (await base.head(key)) await base.delete(key); // let the fs base accept an overwrite
      const ack = await base.put(key, body, { ...o, ifNoneMatch: false });
      return { ...ack, created: true, version: { ...ack.version, version: id } };
    },
    async delete(key) {
      const k = String(key);
      s3.keys.set(k, [{ id: `m${Date.now()}${Math.random()}`, at: Date.now(), marker: true }, ...list(k)]);
      await base.delete(key);
    },
  };
  return {
    store, s3, eraser: s3.eraser(),
    ids: (k: string) => list(k).filter((v) => !v.marker).map((v) => v.id),
    /** C3's whole-prefix erase: every version of every key under the prefix. */
    erasePrefix: async (prefix: string) => {
      for (const k of [...s3.keys.keys()].filter((x) => x.startsWith(prefix))) {
        s3.keys.delete(k);
        await base.delete(objectKey(k));
      }
    },
  };
}

test("P1-b (verification): a Put stalled past its lease, then sweep + tombstone, then released: its own version is erased exactly, zero remain", async () => {
  const db = await fresh();
  const v = versionedStore(tempStore());
  let written = "";
  const paused: ObjectStore = {
    ...v.store,
    async put(key, body, opts) {
      // Paused right after the heartbeat: the lease lapses, a sweep runs, the session is erased,
      // and C3 retires the whole session prefix.
      await db.query("UPDATE sessions.session_analysis_result_tickets SET lease_expires_at = now() - interval '1 second', created_at = now() - interval '1 day'");
      expect(await sweepAnalysisTickets(v.store, { graceSeconds: 0, transact: (w) => w(hoopDb(db) as never), eraser: v.eraser })).toBe(0); // kept: clean, nothing accounted for
      await db.query("UPDATE sessions.session_index SET deleted_at = now()");
      await v.erasePrefix("org-a/ws-a/m-1/claude/2026-10-29/s-1/");
      // ...then the stalled Put is released and lands a version.
      written = String(key);
      return v.store.put(key, body, opts);
    },
  };
  await expect(applyBackfill(hoopDb(db), paused, SCOPE, planBackfill(SCOPE, NEON), { eraser: v.eraser })).rejects.toThrow("backfill_ticket_lease_lost");
  expect(written).not.toBe("");
  expect(v.ids(written)).toEqual([]);
  expect(await v.store.head(objectKey(written))).toBeUndefined();
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_jobs_v2")).rows).toHaveLength(0);
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_result_tickets")).rows).toHaveLength(0);
});

test("P1 (final): if the exact-version erase throws once after the fence, a durable ticket survives and a later sweep clears every version", async () => {
  const db = await fresh();
  const v = versionedStore(tempStore());
  let written = "";
  const paused: ObjectStore = {
    ...v.store,
    async put(key, body, opts) {
      await db.query("UPDATE sessions.session_analysis_result_tickets SET lease_expires_at = now() - interval '1 second', created_at = now() - interval '1 day'");
      await sweepAnalysisTickets(v.store, { graceSeconds: 0, transact: (w) => w(hoopDb(db) as never), eraser: v.eraser });
      await db.query("UPDATE sessions.session_index SET deleted_at = now()");
      written = String(key);
      return v.store.put(key, body, opts);
    },
  };
  let lose = true;
  v.s3.onDelete = () => (lose ? ((lose = false), "lose") : undefined); // the exact delete's ack is lost
  await expect(applyBackfill(hoopDb(db), paused, SCOPE, planBackfill(SCOPE, NEON), { eraser: v.eraser })).rejects.toThrow();
  expect(v.ids(written)).toEqual([]); // deleted, but the ack was lost: the erase is unproven
  const tickets = (await db.query<{ k: string; reason: string }>("SELECT result_key k, reason FROM sessions.session_analysis_result_tickets")).rows;
  expect(tickets).toEqual([{ k: written, reason: "forgotten" }]);
  await db.query("UPDATE sessions.session_analysis_result_tickets SET created_at = now() - interval '1 second'");
  expect(await sweepAnalysisTickets(v.store, { graceSeconds: 0, transact: (w) => w(hoopDb(db) as never), eraser: v.eraser })).toBe(1);
  expect(v.ids(written)).toEqual([]);
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_result_tickets")).rows).toHaveLength(0);
});

test("P1 (final): the backfill refuses to run without the every-version eraser", async () => {
  const db = await fresh();
  await expect(applyBackfill(hoopDb(db), tempStore(), SCOPE, planBackfill(SCOPE, NEON), {} as never)).rejects.toThrow("backfill_eraser_required");
  await expect(applyBackfill(hoopDb(db), tempStore(), SCOPE, planBackfill(SCOPE, NEON), { eraseObject: async () => 0 } as never)).rejects.toThrow("backfill_eraser_required");
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_result_tickets")).rows).toHaveLength(0);
});

test("item 2: a PUT that outlives its deadline is aborted and refused inside the lease; the ticket is kept durable for the sweeper", async () => {
  const db = await fresh();
  const store = tempStore();
  let aborted = false;
  const stalling: ObjectStore = {
    ...store,
    put: (_k, _b, opts) =>
      new Promise<never>((_, reject) => opts?.signal?.addEventListener("abort", () => { aborted = true; reject(new Error("aborted")); })),
  };
  await expect(applyBackfill(hoopDb(db), stalling, SCOPE, planBackfill(SCOPE, NEON), { ...fsEraser(store), putTimeoutMs: 50 })).rejects.toThrow("analysis_put_timeout");
  expect(aborted).toBe(true);
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_jobs_v2")).rows).toHaveLength(0);
  expect((await db.query<{ reason: string; lease: unknown }>("SELECT reason, lease_expires_at lease FROM sessions.session_analysis_result_tickets")).rows)
    .toEqual([{ reason: "unknown", lease: null }]);
});

test("item 4: a source-generation change during the PUT (a new manifest etag) refuses publication; the version is erased exactly", async () => {
  const db = await fresh();
  const v = versionedStore(tempStore());
  let written = "";
  const moving: ObjectStore = {
    ...v.store,
    async put(key, body, opts) {
      written = String(key);
      const ack = await v.store.put(key, body, opts);
      await db.query("UPDATE sessions.session_index SET manifest_etag = 'etag-moved'");
      return ack;
    },
  };
  await expect(applyBackfill(hoopDb(db), moving, SCOPE, planBackfill(SCOPE, NEON), { eraser: v.eraser })).rejects.toThrow("backfill_ticket_lease_lost");
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_jobs_v2")).rows).toHaveLength(0);
  expect(v.ids(written)).toEqual([]);
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_result_tickets")).rows).toHaveLength(0);
});

test("successor-safe: writer A's slow PUT lands AFTER writer B (the new lease) published the same key; A removes only its own version", async () => {
  const db = await fresh();
  const v = versionedStore(tempStore());
  const plan = planBackfill(SCOPE, NEON);
  let written = "";
  const slowA: ObjectStore = {
    ...v.store,
    async put(key, body, opts) {
      // A's lease lapses while its PUT is in flight; B takes the ticket over and publishes.
      await db.query("UPDATE sessions.session_analysis_result_tickets SET lease_expires_at = now() - interval '1 second'");
      await applyBackfill(hoopDb(db), v.store, SCOPE, plan, { eraser: v.eraser });
      written = String(key);
      // ...then A's PUT lands a second version over B's.
      return v.store.put(key, body, opts);
    },
  };
  await expect(applyBackfill(hoopDb(db), slowA, SCOPE, plan, { eraser: v.eraser })).rejects.toThrow("backfill_ticket_lease_lost");
  // B's version stays (and stays published); A's is gone.
  expect(v.ids(written)).toEqual(["v00000001"]);
  expect((await db.query<{ k: string }>("SELECT result_s3_key k FROM sessions.session_analysis_jobs_v2")).rows).toEqual([{ k: written }]);
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_result_tickets")).rows).toHaveLength(0);
  expect(parityEqual(NEON, (await readSp(db, v.store))!)).toEqual({ equal: true, requests: 2, analyses: 1 });
});

test("successor-safe: with create-only S3 semantics A's late PUT creates nothing and deletes nothing; B's sole version is never touched", async () => {
  const db = await fresh();
  const v = versionedStore(tempStore(), { createOnly: true });
  const plan = planBackfill(SCOPE, NEON);
  let written = "";
  const slowA: ObjectStore = {
    ...v.store,
    async put(key, body, opts) {
      await db.query("UPDATE sessions.session_analysis_result_tickets SET lease_expires_at = now() - interval '1 second'");
      await applyBackfill(hoopDb(db), v.store, SCOPE, plan, { eraser: v.eraser });
      written = String(key);
      return v.store.put(key, body, opts);
    },
  };
  await expect(applyBackfill(hoopDb(db), slowA, SCOPE, plan, { eraser: v.eraser })).rejects.toThrow("backfill_ticket_lease_lost");
  expect(v.ids(written)).toEqual(["v00000001"]);
  expect(parityEqual(NEON, (await readSp(db, v.store))!)).toEqual({ equal: true, requests: 2, analyses: 1 });
});

test("F2 parity: backfilled title-only legacy briefs (NULL description and state, or absent) are served byte-equal", async () => {
  const db = await fresh();
  const store = tempStore();
  const neon = structuredClone(NEON) as any;
  neon.analyses[0].result.brief = { title: "Fix the queue", description: null, state: null };
  const second = structuredClone(neon.analyses[0]);
  second.id = "7a1d4b63-9e3b-5c4f-b5f2-0a2f1e3d4c5b";
  second.createdAt = "2026-09-29T00:00:00+00:00";
  second.sourceFingerprint = second.result.sourceFingerprint = "def";
  second.result.brief = { title: "Older" }; // title-only: no description/state keys at all
  neon.analyses.push(second);
  neon.requests.push({ ...neon.requests[0], id: "44444444-4444-4444-8444-444444444444", analysisId: second.id,
    createdAt: "2026-09-29T00:00:00+00:00", updatedAt: "2026-09-29T00:00:02+00:00" });
  const out = await backfillScope({ db: hoopDb(db), store, readSp: () => readSp(db, store), ...fsEraser(store) }, SCOPE, neon);
  expect(out.parity.equal).toBe(true);
  const sp = (await readSp(db, store))!;
  const briefs = sp.analyses.map((a: any) => a.result.brief).sort((a: any, b: any) => a.title.localeCompare(b.title));
  expect(briefs).toEqual([{ title: "Fix the queue", description: null, state: null }, { title: "Older" }]);
});

test("final review P1-a: a C3 purge committed after the generation capture inserts NO request history (bound to the generation and the purge fence)", async () => {
  const db = await fresh();
  const store = tempStore();
  await db.exec(`CREATE TABLE IF NOT EXISTS sessions.ingest_frozen_source_owned_sessions(
    org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,
    source text NOT NULL,session_id text NOT NULL,PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id))`);
  await db.query("INSERT INTO sessions.ingest_frozen_source_owned_sessions VALUES ($1, $2, 'u-owner', 'o', 'claude', $3)", [KEY.orgId, KEY.workspaceId, KEY.sessionId]);
  // An ok payload with no analyses and one failed request (analysisId null): no job, no object.
  const neon: NeonV1Payload = { outcome: "ok", sessionId: KEY.sessionId, analyses: [], requests: [(NEON as any).requests[1]] };
  let purged = false;
  const purgeFirst = async () => {
    if (purged) return;
    purged = true;
    await db.query("SELECT sessions.purge_session_analysis_for_owned($1, $2, 'u-owner')", [KEY.orgId, KEY.workspaceId]);
  };
  await expect(applyBackfill(hooked(db, "backfill:request", purgeFirst), store, SCOPE, planBackfill(SCOPE, neon), fsEraser(store))).rejects.toThrow("backfill_generation_changed");
  expect(purged).toBe(true);
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_requests_v2")).rows).toHaveLength(0);
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_jobs_v2")).rows).toHaveLength(0);
});

test("unknown-PUT: a backfill PUT that loses its response leaves an 'unknown' ticket; the sweeper lists versions and erases the committed stray", async () => {
  const db = await fresh();
  const v = versionedStore(tempStore());
  let written = "";
  const lossy: ObjectStore = {
    ...v.store,
    async put(key, body, opts) {
      written = String(key);
      await v.store.put(key, body, opts); // committed...
      throw new Error("socket hang up"); // ...but the response (and its version id) is lost
    },
  };
  await expect(applyBackfill(hoopDb(db), lossy, SCOPE, planBackfill(SCOPE, NEON), { eraser: v.eraser })).rejects.toThrow("socket hang up");
  expect((await db.query<{ reason: string }>("SELECT reason FROM sessions.session_analysis_result_tickets")).rows).toEqual([{ reason: "unknown" }]);
  expect(v.ids(written)).toHaveLength(1);
  expect(await sweepAnalysisTickets(v.store, { graceSeconds: 0, transact: (w) => w(hoopDb(db) as never), eraser: v.eraser })).toBe(1);
  expect(v.ids(written)).toEqual([]);
  expect((await db.query("SELECT 1 FROM sessions.session_analysis_result_tickets")).rows).toHaveLength(0);
});

test("unknown-PUT: a published backfill job records its exact version; an unknown stray at the same key is erased, the published version kept", async () => {
  const db = await fresh();
  const v = versionedStore(tempStore());
  await applyBackfill(hoopDb(db), v.store, SCOPE, planBackfill(SCOPE, NEON), { eraser: v.eraser });
  const job = (await db.query<{ k: string; v: string }>("SELECT result_s3_key k, result_version_id v FROM sessions.session_analysis_jobs_v2")).rows[0]!;
  expect(job.v).toBe("v00000001");
  // A stale writer's unknown PUT at the same key commits later.
  await v.store.put(objectKey(job.k), new TextEncoder().encode("{}"), {});
  await db.query(`INSERT INTO sessions.session_analysis_result_tickets (result_key, job_id, org_id, workspace_id, session_id, reason, created_at)
    VALUES ($1, gen_random_uuid(), $2, $3, $4, 'unknown', now() - interval '1 day')`, [job.k, KEY.orgId, KEY.workspaceId, KEY.sessionId]);
  expect(await sweepAnalysisTickets(v.store, { graceSeconds: 0, transact: (w) => w(hoopDb(db) as never), eraser: v.eraser })).toBe(1);
  expect(v.ids(job.k)).toEqual(["v00000001"]);
});
