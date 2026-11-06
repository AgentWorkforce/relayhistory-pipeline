/**
 * SESSIONS_NEON_EXIT on the session ingest (src/shared/neon-exit.ts,
 * docs/neon-exit-edge-map.md): every Neon edge refuses at settings time, and a
 * valid profile configuration serves the real handlers (the worker's own app
 * builder, the Supabase ledger in PGlite, a scratch object store) with the
 * poisoned replica queue and globalThis.fetch never called.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import worker, { ingestApp, ingestSettings, type IngestWorkerEnv } from "../worker.ts";
import type { NeonReplicaMessage } from "../replica.ts";
import { CURRENT_CUSTODY_ACK_HEADER } from "../custody-protocol.ts";
import { SIGNING_KEY, TENANT, batch, record, scratchStore, signedHeaders } from "./support.ts";

setDefaultTimeout(30000);
const root = join(import.meta.dir, "../../..");
const generation = "a".repeat(64), epoch = "3".repeat(64), identity = "c".repeat(64), baseline = "a".repeat(64);
const open: Array<() => Promise<unknown> | void> = [];
const realFetch = globalThis.fetch;
afterEach(async () => {
  globalThis.fetch = realFetch;
  for (const close of open.splice(0).reverse()) await close();
});

/** A NEON_REPLICA binding that records every call and fails it. */
function poisonedQueue() {
  const calls: string[] = [];
  const queue = {
    send: async (_body: NeonReplicaMessage) => { calls.push("send"); throw new Error("poisoned NEON_REPLICA: send"); },
    sendBatch: async (_messages: { body: NeonReplicaMessage }[]) => { calls.push("sendBatch"); throw new Error("poisoned NEON_REPLICA: sendBatch"); },
  };
  return { queue, calls };
}

/** globalThis.fetch replaced, for the test, by a recorder that throws. */
function poisonFetch() {
  const calls: string[] = [];
  globalThis.fetch = Object.assign(async (input: unknown) => { calls.push(String(input instanceof Request ? input.url : input)); throw new Error("poisoned fetch"); }, { preconnect: () => {} }) as typeof fetch;
  return calls;
}

const fanoutCalls: string[] = [];
const base = {
  // Never connected: the profile tests drive the app with a PGlite run instead.
  HYPERDRIVE: { connectionString: "postgres://poison:poison@hyperdrive.invalid:5432/poison" },
  INGEST_FANOUT: { send: async () => { fanoutCalls.push("send"); }, sendBatch: async () => { fanoutCalls.push("sendBatch"); } } as never,
  S3_ACCESS_KEY_ID: "k",
  S3_SECRET_ACCESS_KEY: "s",
  INGEST_SIGNING_KEY: SIGNING_KEY,
  INGEST_S3_BUCKET: "b",
  INGEST_KMS_KEY_ARN: "arn",
} satisfies IngestWorkerEnv;
const profile = { ...base, SESSIONS_NEON_EXIT: "true", INGEST_ACCEPTANCE_MODE: "buffered", INGEST_EXPECT_GENERATION: generation, INGEST_INLINE_MAX_SESSIONS: "0", INGEST_REPLICA_MODE: "off", INGEST_CUSTODY_SOURCE: "frozen-supabase" } satisfies IngestWorkerEnv;

const refusal = async (env: IngestWorkerEnv) => {
  try { await ingestSettings(env); return null; } catch (error) { return error as Error & { code?: string }; }
};

describe("SESSIONS_NEON_EXIT=true refuses every ingest Neon edge at settings time", () => {
  test("INGEST_REPLICA_MODE=queue with a poisoned NEON_REPLICA: refused, the queue never called, the Worker answers 503", async () => {
    const { queue, calls } = poisonedQueue();
    const fetchCalls = poisonFetch();
    const env: IngestWorkerEnv = { ...base, SESSIONS_NEON_EXIT: "true", INGEST_REPLICA_MODE: "queue", NEON_REPLICA: queue };
    const error = await refusal(env);
    expect(error?.message).toBe("96bulls: SESSIONS_NEON_EXIT=true refuses the session ingest: INGEST_REPLICA_MODE is not off");
    expect(error?.code).toBe("neon_exit_refused");
    const body = new TextEncoder().encode(JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "q1" })]) }));
    const res = await worker.fetch(new Request("https://x/v1/delivery/batches", { method: "POST", body, headers: signedHeaders({ ...TENANT, scopes: ["rth:sync"] }, "POST", "/v1/delivery/batches", body) }), env, { waitUntil: () => {} });
    expect([res.status, ((await res.json()) as { error: { code: string } }).error.code]).toEqual([503, "not_configured"]);
    // The fan-out consumer refuses the same way: retried later, never processed.
    const retried: number[] = [];
    const errorLines: string[] = [];
    const realWrite = process.stderr.write;
    process.stderr.write = ((chunk: string | Uint8Array) => { errorLines.push(String(chunk)); return true; }) as typeof process.stderr.write;
    try {
      await worker.queue({ messages: [{ body: { kind: "ingest-fanout" }, ack: () => { throw new Error("must not ack"); }, retry: (o) => retried.push(o?.delaySeconds ?? 0) }] }, env);
    } finally {
      process.stderr.write = realWrite;
    }
    expect(retried).toEqual([300]);
    const refusedLog = errorLines.map((line) => JSON.parse(line) as { message: string; error_code?: string }).find((line) => line.message === "session ingest settings invalid");
    expect(refusedLog?.error_code).toBe("neon_exit_refused");
    expect(calls).toEqual([]);
    expect(fetchCalls).toEqual([]);
  });

  test("each other edge names its own setting; nothing secret-looking is echoed", async () => {
    const { queue, calls } = poisonedQueue();
    const cases: [IngestWorkerEnv, string][] = [
      [{ ...profile, INGEST_REPLICA_MODE: "best_effort" }, "INGEST_REPLICA_MODE is not off"],
      [{ ...profile, NEON_REPLICA: queue }, "a NEON_REPLICA queue is bound"],
      [{ ...profile, INGEST_CUSTODY_SOURCE: undefined }, "INGEST_CUSTODY_SOURCE is not frozen-supabase"],
      [{ ...profile, INGEST_CUSTODY_SOURCE: "relayhistory" }, "INGEST_CUSTODY_SOURCE is not frozen-supabase"],
      [{ ...profile, INGEST_CUSTODY_SOURCE: "shadow" }, "INGEST_CUSTODY_SOURCE is not frozen-supabase"],
      [{ ...profile, INGEST_CUSTODY_SOURCE: "postgres://poison:poison@neon.invalid:5432/poison" }, "INGEST_CUSTODY_SOURCE is not frozen-supabase"],
      [{ ...profile, INGEST_ACCEPTANCE_MODE: undefined }, "INGEST_ACCEPTANCE_MODE is not buffered (frozen-supabase custody needs it)"],
      [{ ...profile, INGEST_ACCEPTANCE_MODE: "canonical" }, "INGEST_ACCEPTANCE_MODE is not buffered (frozen-supabase custody needs it)"],
      // Refused even before the Worker is configured: a loud refusal, never a quiet not_configured.
      [{ SESSIONS_NEON_EXIT: "true", NEON_REPLICA: queue }, "a NEON_REPLICA queue is bound"],
    ];
    for (const [env, setting] of cases) {
      const error = await refusal(env);
      expect(error?.message).toBe(`96bulls: SESSIONS_NEON_EXIT=true refuses the session ingest: ${setting}`);
      expect(error?.message).not.toContain("poison");
    }
    for (const value of ["1", "TRUE", "yes", ""]) expect((await refusal({ ...profile, SESSIONS_NEON_EXIT: value }))?.message).toContain("SESSIONS_NEON_EXIT must be");
    expect(calls).toEqual([]);
  });
});

async function frozenSupabase() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(() => db.close());
  const read = (p: string) => readFileSync(join(root, p), "utf8");
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles"]) await db.exec(read("supabase/bootstrap/" + f + ".sql"));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read("supabase/migrations/" + f));
  await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','" + generation + "','pass')");
  const owner = async (name: string, p: unknown) => (await db.query("SELECT sessions." + name + "($1::jsonb) AS r", [JSON.stringify(p)])).rows[0];
  await owner("ingest_lazy_policy_set", { baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: baseline, retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" });
  await owner("ingest_receipt_seat_initialize", { protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch, capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })), allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) });
  // As production logs in: every statement AS bulls96_session_ingest.
  const run = async (s: string, p: unknown[] = []) => {
    await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
    try { const r = await db.query(s, p as never); await db.exec("COMMIT"); return r as { rows: Record<string, unknown>[] }; } catch (e) { await db.exec("ROLLBACK"); throw e; }
  };
  const seal = () => db.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')", [identity, epoch, baseline, "e".repeat(64)]);
  return { run, seal };
}

describe("SESSIONS_NEON_EXIT=true with a valid configuration serves the real handlers without Neon", () => {
  test("success, error, retry and a disabled legacy route: no replica queue, no fetch", async () => {
    const fetchCalls = poisonFetch();
    const db = await frozenSupabase();
    const fs = scratchStore();
    open.push(fs.done);
    const settings = await ingestSettings(profile);
    expect(settings).toMatchObject({ acceptanceMode: "buffered", replicaMode: "off", inlineMaxSessions: 0, expectedGeneration: generation });
    // The gate refuses a bound NEON_REPLICA, so the valid profile has none; the poisoned one proves the refusal above.
    const { queue, calls } = poisonedQueue();
    expect((await refusal({ ...profile, NEON_REPLICA: queue }))?.message).toContain("NEON_REPLICA");
    const app = ingestApp(profile, settings!, db.run, { store: fs.store, keyRoot: settings!.keyRoot });
    const who = { ...TENANT, scopes: ["rth:sync"] };
    const post = (path: string, payload: unknown) => {
      const body = new TextEncoder().encode(JSON.stringify(payload));
      return app.fetch(new Request("https://x" + path, { method: "POST", body, headers: { "content-type": "application/json", ...signedHeaders(who, "POST", path, body) } }));
    };
    const raw = await batch([record({ record_id: "nx-1" }), record({ record_id: "nx-2" })], { batch_id: "neon-exit" });

    // Error: the frozen copy is not sealed yet, so frozen-supabase custody refuses (retryable 503), nothing guessed.
    const unsealed = await post("/v1/delivery/batches", { protocolVersion: 1, batch: raw });
    expect(unsealed.status).toBe(503);
    expect(unsealed.headers.get(CURRENT_CUSTODY_ACK_HEADER)).toBeNull();
    // Error: a malformed body is relayhistory's 400.
    const invalid = await post("/v1/delivery/batches", { protocolVersion: 1, batch: "x" });
    expect([invalid.status, ((await invalid.json()) as { error: { code: string } }).error.code]).toEqual([400, "invalid_delivery"]);

    // Success: once sealed, the witness is built from the Supabase copy.
    await db.seal();
    const ok = await post("/v1/delivery/batches", { protocolVersion: 1, batch: raw });
    expect(ok.status).toBe(200);
    const receipt = (await ok.json()) as { receiptId: string };
    expect(receipt.receiptId).toMatch(/^rhr_/);
    expect(JSON.parse(ok.headers.get(CURRENT_CUSTODY_ACK_HEADER)!)).toMatchObject({ version: 1 });

    // Retry: the same batch re-POSTed answers the same receipt.
    const again = await post("/v1/delivery/batches", { protocolVersion: 1, batch: raw });
    expect(again.status).toBe(200);
    expect(((await again.json()) as { receiptId: string }).receiptId).toBe(receipt.receiptId);

    // A disabled legacy route stays 501 unsupported_by_backend.
    const legacy = await app.fetch(new Request("https://x/v1/sessions", { headers: signedHeaders(who, "GET", "/v1/sessions", new Uint8Array()) }));
    expect([legacy.status, ((await legacy.json()) as { error: { code: string } }).error.code]).toEqual([501, "unsupported_by_backend"]);

    // The Worker entry with the same env: configured (not 503), so the profile itself is served.
    const health = await worker.fetch(new Request("https://x/healthz"), profile, { waitUntil: () => {} });
    expect(health.status).toBe(200);

    expect(calls).toEqual([]);
    expect(fetchCalls).toEqual([]);
    expect(fanoutCalls).toEqual([]);
  });
});

describe("the default (SESSIONS_NEON_EXIT unset or false) is unchanged", () => {
  test("the replica queue and relayhistory custody are still accepted and wired", async () => {
    for (const flag of [undefined, "false"]) {
      const sent: NeonReplicaMessage[] = [];
      const queue = { send: async (m: NeonReplicaMessage) => { sent.push(m); }, sendBatch: async () => {} };
      const env: IngestWorkerEnv = { ...base, SESSIONS_NEON_EXIT: flag, INGEST_REPLICA_MODE: "queue", NEON_REPLICA: queue };
      expect(await ingestSettings({ ...base, SESSIONS_NEON_EXIT: flag })).toMatchObject({ replicaMode: "off", acceptanceMode: "canonical", inlineMaxSessions: 40 });
      expect(await ingestSettings({ ...base, SESSIONS_NEON_EXIT: flag, INGEST_REPLICA_MODE: "queue" })).toBeNull();
      const settings = await ingestSettings(env);
      expect(settings?.replicaMode).toBe("required");
      // The Worker builds and serves the app (default relayhistory custody) with the replica wired.
      const res = await worker.fetch(new Request("https://x/healthz"), env, { waitUntil: () => {} });
      expect(res.status).toBe(200);
    }
  });
});
