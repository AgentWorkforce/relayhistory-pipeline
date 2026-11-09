/**
 * The session-ingest Worker, configured the way the combined deploy configures it, on a G4-only
 * database (pglite-schema.ts: migrations through G4_LAST_MIGRATION, the undefined-object guard
 * installed before anything runs).
 *
 * Nothing here hand-builds createIngestApp: the Cloudflare stack renders the Worker env
 * (stacks/cloudflare/session-ingest.ts sessionIngestWorkerProps: buffered acceptance, custodySource
 * frozen-supabase, asyncAdmission drain, syncAdmissionFrozenReceipt on), worker.ts ingestSettings
 * parses it, and worker.ts ingestApp builds the app over the fixture's ingest-role `run` (the
 * Hyperdrive client's stand-in). Requests are HMAC-signed with the rendered signing-key binding and
 * carry the queued-v1 opt-in, which in drain mode takes the synchronous path, so
 * INGEST_SYNC_ADMISSION_FROZEN_RECEIPT decides it (docs/design/s3-first-async-ingest.md).
 *
 * Cases: (i) a stored legacy receipt, and a batch committed here and then replayed; (ii) no stored
 * receipt: frozen witness, claim, stage, commit; (iii) a conflicting legacy receipt: 409 with the flag
 * on, and (CAUSAL mutation) 200 with only the flag removed from the deploy inputs. Then the drain
 * consumer (worker.ts admitQueueBatch) on messages queued while async admission was on.
 * The Queue bindings (INGEST_FANOUT, INGEST_ADMISSION) are recording objects with the binding's
 * send / sendBatch shape: there is no Cloudflare queue runtime here; the consumer is driven directly.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { encodeIdentity, IDENTITY_HEADER, SIGNATURE_HEADER, signRequest, SYNC_SCOPE } from "../app.ts";
import { ADMISSION_ACCEPT_VALUE, ADMISSION_HEADER, ADMISSION_LEGACY_REPLAY, ADMISSION_QUEUED, admissionStatus, type AdmissionMessage } from "../async-admission.ts";
import { CURRENT_CUSTODY_ACK_HEADER } from "../custody-protocol.ts";
import { admitQueueBatch, ingestApp, ingestSettings, type IngestWorkerEnv } from "../worker.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import type { HistoryExportBatch } from "../vendor/delivery/delivery-contracts.ts";
import { sessionIngestWorkerProps, type SessionIngestInputs } from "../../../stacks/cloudflare/session-ingest.ts";
import { FROZEN_GENERATION, digestOf, frozenSupabaseFixture } from "./frozen-supabase-support.ts";
import { G4_LAST_MIGRATION, POST_G4_MIGRATIONS, REPO_ROOT, migrationFiles, type UndefinedObjectError } from "./pglite-schema.ts";
import { SIGNING_KEY, TENANT, batch, record } from "./support.ts";

setDefaultTimeout(120000);
const open: Array<() => Promise<unknown> | void> = [];
afterEach(async () => { for (const close of open.splice(0).reverse()) await close(); });

/** The combined deploy's stack inputs (ids are placeholders; only the switches matter here). */
const DEPLOY: SessionIngestInputs = {
  accountId: "0".repeat(32),
  hyperdriveId: "1".repeat(32),
  secretsStoreId: "2".repeat(32),
  kmsKeyArn: "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000",
  acceptanceMode: "buffered",
  custodySource: "frozen-supabase",
  expectedGeneration: FROZEN_GENERATION,
  inlineMaxSessions: 0,
  replicaMode: "off",
  asyncAdmission: "drain",
  syncAdmissionFrozenReceipt: "on",
};

/** A recording stand-in for a Cloudflare Queue producer binding. */
function recordingQueue<T>() {
  const sent: T[] = [];
  return { sent, send: async (body: T) => { sent.push(body); }, sendBatch: async (messages: { body: T }[]) => { sent.push(...messages.map((m) => m.body)); } };
}

/** The env the stack deploys (vars + bindings), with Secrets Store bindings that resolve like the runtime's. */
function workerEnv(inputs: SessionIngestInputs, fanout: unknown, admission: unknown): IngestWorkerEnv {
  const props = sessionIngestWorkerProps(inputs, { HYPERDRIVE: { connectionString: "postgres://pglite.invalid/ingest" }, INGEST_FANOUT: fanout, ...(inputs.asyncAdmission ? { INGEST_ADMISSION: admission } : {}) });
  const secret = (value: string) => ({ get: async () => value });
  return { ...props.env, S3_ACCESS_KEY_ID: secret("test-access-key"), S3_SECRET_ACCESS_KEY: secret("test-secret-key"), INGEST_SIGNING_KEY: secret(SIGNING_KEY) } as unknown as IngestWorkerEnv;
}

/** A G4-only frozen-supabase database, and the deployed Worker app over it. */
async function deployed(inputs: SessionIngestInputs = DEPLOY) {
  const rpcs: string[] = [];
  const f = await frozenSupabaseFixture((close) => open.push(close), {
    until: G4_LAST_MIGRATION,
    guard: true,
    onStatement: (sql) => { for (const m of sql.matchAll(/sessions\.(\w+)\(/g)) rpcs.push(m[1]!); },
  });
  const fanout = recordingQueue<unknown>(), admission = recordingQueue<AdmissionMessage>();
  const env = workerEnv(inputs, fanout, admission);
  const settings = await ingestSettings(env);
  if (!settings) throw new Error("the deploy env did not configure the Worker");
  const target = { store: f.store, keyRoot: settings.keyRoot };
  const app = ingestApp(env, settings, f.run, target);
  const post = async (b: HistoryExportBatch) => {
    const identity = encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE], admission: ADMISSION_ACCEPT_VALUE });
    const body = new TextEncoder().encode(JSON.stringify({ protocolVersion: 1, batch: b }));
    return app.request("/v1/delivery/batches", { method: "POST", body, headers: { "content-type": "application/json", [IDENTITY_HEADER]: identity,
      [SIGNATURE_HEADER]: signRequest(SIGNING_KEY, Math.floor(Date.now() / 1000), "POST", "/v1/delivery/batches", identity, body) } });
  };
  const outbox = async (batchId: string) => (await f.db.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.ingest_buffer_outbox WHERE batch_id=$1", [batchId])).rows[0]!.n;
  /** The RPCs run since the last call (the fixture's own setup is not traced: it bypasses `run`). */
  const takeRpcs = () => rpcs.splice(0);
  return { ...f, env, settings, target, app, post, outbox, takeRpcs, fanout, admission };
}

const expectClean = (errors: UndefinedObjectError[]) => expect(errors).toEqual([]);
const BUFFERED_PATH = ["ingest_frozen_source_receipt", "ingest_frozen_source_witness", "ingest_claim_buffered_batch", "ingest_prepare_buffer_stage", "ingest_commit_buffered_batch"];
/** `want` appears in `seen` in this order (other RPCs, e.g. slots, may interleave). */
const inOrder = (seen: string[], want: string[]) => want.every(((i) => (x: string) => (i = seen.indexOf(x, i) + 1) > 0)(0));

/** Three distinct conflicting-legacy shapes: one record, two records of one session, two sessions. */
async function conflictingBatches() {
  return [
    await batch([record({ record_id: "x1" })], { batch_id: "conflict-1" }),
    await batch([record({ record_id: "x2" }), record({ record_id: "x3" })], { batch_id: "conflict-2" }),
    await batch([record({ record_id: "x4", session_id: "sess-a" }), record({ record_id: "x5", session_id: "sess-b" })], { batch_id: "conflict-3" }),
  ];
}

describe("G4 + the combined deploy settings (stack vars -> ingestSettings -> ingestApp)", () => {
  test("the deploy renders and parses as buffered / frozen-supabase / drain / sync frozen receipt on; the database is G4-only", async () => {
    const t = await deployed();
    expect(t.env.INGEST_ACCEPTANCE_MODE).toBe("buffered");
    expect(t.env.INGEST_CUSTODY_SOURCE).toBe("frozen-supabase");
    expect(t.env.INGEST_ASYNC_ADMISSION).toBe("drain");
    expect(t.env.INGEST_SYNC_ADMISSION_FROZEN_RECEIPT).toBe("on");
    expect(t.env.INGEST_INLINE_MAX_SESSIONS).toBe("0");
    expect(t.env.INGEST_ADMISSION).toBeDefined();
    expect(t.settings).toMatchObject({ acceptanceMode: "buffered", expectedGeneration: FROZEN_GENERATION, inlineMaxSessions: 0, replicaMode: "off", asyncAdmission: "drain", syncAdmissionFrozenReceipt: true });
    // Absence (fails on the full migration set, by design): exactly the G4 files, no post-G4 object.
    expect(t.applied).toEqual(migrationFiles(REPO_ROOT, G4_LAST_MIGRATION));
    for (const f of POST_G4_MIGRATIONS) expect(t.applied).not.toContain(f);
    const absent = await t.db.query<{ a: string | null; b: string | null; c: number }>("SELECT to_regclass('sessions.ingest_acquisition_hooks_switch')::text AS a, to_regclass('sessions.ingest_bookkeeping_cleanup_settings')::text AS b, (SELECT count(*)::int FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions' AND p.proname IN ('ingest_acquisition_admit','buffered_search_gate_for')) AS c");
    expect(absent.rows[0]).toEqual({ a: null, b: null, c: 0 });
    expectClean(t.undefinedObjects);
  });

  test("(i) stored legacy receipt, same digest: 200 legacy-replay from the frozen receipt alone; then a batch committed here replays 200 with its receipt", async () => {
    const t = await deployed();
    const legacyBatch = await batch([record({ record_id: "l1" })], { batch_id: "legacy-same" });
    await t.legacy(legacyBatch, await digestOf(legacyBatch));
    t.takeRpcs();
    const res = await t.post(legacyBatch);
    expect(res.status).toBe(200);
    expect(res.headers.get(ADMISSION_HEADER)).toBe(ADMISSION_LEGACY_REPLAY);
    expect(res.headers.get(CURRENT_CUSTODY_ACK_HEADER)).toBeNull();
    expect(((await res.json()) as { receiptId: string }).receiptId).toBe("rhr_legacy");
    expect(t.takeRpcs()).toEqual(["ingest_frozen_source_receipt"]);
    expect(await t.outbox("legacy-same")).toBe(0);

    // A batch committed through this Worker (no legacy receipt), then sent again: the claim's replay.
    const b = await batch([record({ record_id: "l2" })], { batch_id: "committed-then-replayed" });
    const first = await t.post(b);
    expect(first.status).toBe(200);
    const receipt = (await first.json()) as { receiptId: string };
    t.takeRpcs();
    const again = await t.post(b);
    expect(again.status).toBe(200);
    expect(((await again.json()) as { receiptId: string }).receiptId).toBe(receipt.receiptId);
    expect(again.headers.get(ADMISSION_HEADER)).toBeNull();
    const replayRpcs = t.takeRpcs();
    expect(replayRpcs[0]).toBe("ingest_frozen_source_receipt");
    expect(replayRpcs).toContain("ingest_claim_buffered_batch");
    expect(await t.outbox("committed-then-replayed")).toBe(1);
    expect(t.admission.sent).toHaveLength(0);
    expectClean(t.undefinedObjects);
  });

  test("(ii) no stored receipt: frozen receipt read, frozen witness, claim, stage, commit; custody ACK header, no admission header", async () => {
    const t = await deployed();
    const b = await batch([record({ record_id: "a1" }), record({ record_id: "a2" })], { batch_id: "receipt-absent" });
    t.takeRpcs();
    const res = await t.post(b);
    expect(res.status).toBe(200);
    const rpcs = t.takeRpcs();
    expect(rpcs[0]).toBe("ingest_frozen_source_receipt");
    expect(inOrder(rpcs, BUFFERED_PATH)).toBe(true);
    expect(JSON.parse(res.headers.get(CURRENT_CUSTODY_ACK_HEADER)!)).toMatchObject({ version: 1 });
    expect(res.headers.get(ADMISSION_HEADER)).toBeNull();
    const { limits: _limits, ...body } = (await res.json()) as Record<string, unknown>;
    expect(body).toEqual((await prepareDelivery(TENANT, b)).receipt as never);
    expect(await t.outbox("receipt-absent")).toBe(1);
    // drain: the opted-in request never takes the async ACK.
    expect(t.admission.sent).toHaveLength(0);
    expectClean(t.undefinedObjects);
  });

  // CAUSAL: identical inputs; only syncAdmissionFrozenReceipt differs in the deploy inputs.
  test.each([["on", 409], ["off", 200]] as const)("(iii) conflicting legacy receipt x3, INGEST_SYNC_ADMISSION_FROZEN_RECEIPT %s: %i", async (flag, status) => {
    const inputs: SessionIngestInputs = flag === "on" ? DEPLOY : { ...DEPLOY, syncAdmissionFrozenReceipt: undefined };
    const t = await deployed(inputs);
    expect(t.env.INGEST_SYNC_ADMISSION_FROZEN_RECEIPT).toBe(flag === "on" ? "on" : undefined);
    expect(t.settings.syncAdmissionFrozenReceipt).toBe(flag === "on" ? true : undefined);
    const batches = await conflictingBatches();
    for (const b of batches) await t.legacy(b, "9".repeat(64));
    const statuses: number[] = [];
    for (const b of batches) {
      t.takeRpcs();
      const res = await t.post(b);
      statuses.push(res.status);
      const rpcs = t.takeRpcs();
      if (flag === "on") {
        const body = (await res.json()) as { error: { code: string; conflict: Record<string, unknown> } };
        expect(body.error.code).toBe("delivery_conflict");
        expect(body.error.conflict).toEqual({ type: "batch_id", originId: b.origin_id, batchId: b.batch_id, submittedDigest: await digestOf(b), currentDigest: "9".repeat(64) });
        expect(rpcs).toEqual(["ingest_frozen_source_receipt"]);
        expect(res.headers.get(CURRENT_CUSTODY_ACK_HEADER)).toBeNull();
        expect(await t.outbox(b.batch_id)).toBe(0);
      } else {
        // Today's path without the flag: the conflicting pre-cutover batch is claimed and delivered.
        expect(rpcs).not.toContain("ingest_frozen_source_receipt");
        expect(inOrder(rpcs, BUFFERED_PATH.slice(1))).toBe(true);
        expect(res.headers.get(CURRENT_CUSTODY_ACK_HEADER)).not.toBeNull();
        expect(await t.outbox(b.batch_id)).toBe(1);
      }
      expect(res.headers.get(ADMISSION_HEADER)).toBeNull();
    }
    expect(statuses).toEqual([status, status, status]);
    expect(t.admission.sent).toHaveLength(0);
    expectClean(t.undefinedObjects);
  });

  test("async admission: messages queued while on are admitted (or refused) by the drain consumer (admitQueueBatch) on G4", async () => {
    // While on: the opted-in request is queued (the queued ACK), nothing touches the ledger.
    const on = await deployed({ ...DEPLOY, asyncAdmission: "on" });
    expect(on.settings.asyncAdmission).toBe("on");
    const fresh = await batch([record({ record_id: "q1" })], { batch_id: "queued-fresh" });
    const conflicting = await batch([record({ record_id: "q2" })], { batch_id: "queued-conflict" });
    await on.legacy(conflicting, "9".repeat(64));
    on.takeRpcs();
    for (const b of [fresh, conflicting]) {
      const res = await on.post(b);
      expect(res.status).toBe(200);
      expect(res.headers.get(ADMISSION_HEADER)).toBe(ADMISSION_QUEUED);
    }
    expect(on.takeRpcs()).toEqual([]);
    expect(on.admission.sent).toHaveLength(2);

    // Then drain (the deploy): the same database, store and messages; the Worker's queue() consumer path.
    const env = workerEnv(DEPLOY, recordingQueue<unknown>(), recordingQueue<AdmissionMessage>());
    const settings = (await ingestSettings(env))!;
    expect(settings.asyncAdmission).toBe("drain");
    const acked: number[] = [], retried: unknown[] = [];
    const messages = on.admission.sent.map((body, i) => ({ body, attempts: 1, ack: () => { acked.push(i); }, retry: (o?: unknown) => { retried.push(o); } }));
    await admitQueueBatch(messages, env, settings, () => ({ run: on.run, end: async () => {} }), on.target);
    expect(retried).toEqual([]);
    expect(acked).toEqual([0, 1]);
    const rpcs = on.takeRpcs();
    expect(inOrder(rpcs, BUFFERED_PATH)).toBe(true);
    expect(await on.outbox("queued-fresh")).toBe(1);
    expect(await on.outbox("queued-conflict")).toBe(0);
    const receiptId = async (b: HistoryExportBatch) => (await prepareDelivery(TENANT, b)).receipt.receiptId;
    expect(await admissionStatus(on.store, on.target.keyRoot, TENANT, fresh.origin_id, await receiptId(fresh))).toMatchObject({ state: "admitted" });
    expect(await admissionStatus(on.store, on.target.keyRoot, TENANT, conflicting.origin_id, await receiptId(conflicting))).toMatchObject({ state: "refused", code: "delivery_conflict" });
    expectClean(on.undefinedObjects);
  });
});
