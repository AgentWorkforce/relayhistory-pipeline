/**
 * Cloudflare Worker entry for the session ingest (no Lambda on the request
 * path). stacks/cloudflare/session-ingest.ts, docs/session-ingest-design.md.
 *
 *   relayhistory Worker (history.agentrelay.com, per-org DELIVERY_FORWARD)
 *     --service binding DELIVERY_INGEST--> fetch(): POST /v1/delivery/batches
 *         -> Supabase (Hyperdrive, role bulls96_session_ingest): claim / slot / commit
 *         -> S3 (srv-writer key, SigV4, SSE-KMS): chunks, manifests, pointers or a staged batch
 *         -> NEON_REPLICA queue (optional, transition only)
 *   INGEST_FANOUT queue --> queue(): staged batches written session by session
 *
 * Bindings and settings (the stack declares them; secrets are set out of band):
 *   HYPERDRIVE                 Supabase transaction pooler as bulls96_session_ingest
 *   INGEST_FANOUT              producer + consumer (this Worker)
 *   NEON_REPLICA               optional producer: relayhistory's replica queue
 *   S3_ACCESS_KEY_ID / S3_SECRET_ACCESS_KEY   Secrets Store bindings (`await .get()`): the transcript
 *                              srv-writer user, set out of band from 1Password
 *   INGEST_SIGNING_KEY         Secrets Store: the HMAC key relayhistory signs each request's
 *                              identity with (shared with relayhistory's DELIVERY_INGEST_SIGNING_KEY)
 *   INGEST_S3_BUCKET, INGEST_S3_REGION, INGEST_KEY_ROOT, INGEST_KMS_KEY_ARN
 *   INGEST_MAX_PER_USER / INGEST_MAX_PER_ORG   (defaults 4 / 32)
 *   INGEST_INLINE_MAX_SESSIONS (default 40)
 *   INGEST_REPLICA_MODE        off | queue (= required) | best_effort; required needs NEON_REPLICA
 *   INGEST_BABYSITTER_RETENTION_ROUTING  unset/off (default) | on (any case): Babysitter upserts go to
 *                              retention buckets (babysitter-retention.ts); only with the replica
 *                              off, and only with INGEST_SUBREQUEST_CAP set (below)
 *   INGEST_SUBREQUEST_CAP      required for routing: this Worker's PROVEN effective subrequest cap
 *   INGEST_SUBREQUEST_REDIRECTS  worst-case redirects per S3 fetch (0..20, default 20)
 *   INGEST_STAGE_LATENCY_LOG   unset/off (default) | on (any case): one `ingest.stage_latency` line
 *                              per deliver stage { stage, outcome, duration_ms, sqlstate } (deliver.ts)
 *   INGEST_CLAIM_LOCK_BUSY     unset/off (default) | on (any case): a claim that loses its lock wait
 *                              (55P03/40P01) is a 429 delivery_busy with Retry-After 5-10 s instead of a
 *                              503 (deliver.ts ClaimLockBusy); 57014 and every other cause stay 503
 *   INGEST_ASYNC_ADMISSION     unset/off (default) | on | drain: S3-first async admission
 *                              (async-admission.ts, docs/design/s3-first-async-ingest.md); on/drain need
 *                              buffered acceptance and the INGEST_ADMISSION queue (producer + consumer)
 *   INGEST_SYNC_ADMISSION_FROZEN_RECEIPT  unset/off (default) | on: under frozen-supabase custody, a queued-v1
 *                              caller on the synchronous path reads the frozen legacy receipt before any claim
 *                              (app.ts acceptBatch, as the admission consumer does)
 *   SESSIONS_NEON_EXIT         unset/false (default) | true: refuse every Neon edge at startup
 *                              (replica, NEON_REPLICA binding, a custody source other than
 *                              frozen-supabase in buffered mode); docs/neon-exit-edge-map.md
 *
 * Until the S3 key, the KMS key ARN and Hyperdrive are present, every request
 * is 503 `not_configured` and nothing is written.
 */
import pg from "pg";
import type { IngestAcceptanceMode } from "./buffered-protocol.ts";
import { zstdDecompressSync } from "node:zlib";
import { createS3Store, objectKey, type ObjectStore } from "../shared/vendor.ts";
import { log } from "../shared/log.ts";
import { dbClientErrorFields } from "./db-error-class.ts";
import { assertNoNeonEdge, neonExitProfile, NeonExitRefusal } from "../shared/neon-exit.ts";
import { createIngestApp } from "./app.ts";
import { sqlLedger } from "./ledger-sql.ts";
import type { ReplicaMode, ReplicaSink, NeonReplicaMessage } from "./replica.ts";
import { fanout, type FanoutMessage } from "./staging.ts";
import type { StageLatencyFields } from "./deliver.ts";
import type { WriteTarget } from "./writer.ts";
import { routingFlag } from "./babysitter-retention.ts";
import { asyncAdmissionMode, syncAdmissionFrozenReceiptMode, type AdmissionMessage, type AsyncAdmissionMode } from "./async-admission.ts";
import { admitMessage, type AdmissionAction } from "./admission-consumer.ts";
import { lockAlertFields } from "./app.ts";
import type { DeliverDeps } from "./deliver.ts";

interface QueueLike<T> {
  send(body: T): Promise<void>;
  sendBatch(messages: { body: T }[]): Promise<void>;
}

export interface IngestWorkerEnv {
  HYPERDRIVE?: { connectionString: string };
  INGEST_FANOUT?: QueueLike<FanoutMessage>;
  /** S3-first async admission queue (producer + consumer: this Worker). Required with INGEST_ASYNC_ADMISSION on/drain. */
  INGEST_ADMISSION?: QueueLike<AdmissionMessage>;
  /** off (default) | on | drain (async-admission.ts). */
  INGEST_ASYNC_ADMISSION?: string;
  /** off (default) | on (async-admission.ts syncAdmissionFrozenReceiptMode). */
  INGEST_SYNC_ADMISSION_FROZEN_RECEIPT?: string;
  NEON_REPLICA?: QueueLike<NeonReplicaMessage>;
  /** Secrets Store bindings (a plain string in tests). */
  S3_ACCESS_KEY_ID?: SecretBinding | string;
  S3_SECRET_ACCESS_KEY?: SecretBinding | string;
  /** Secrets Store: the HMAC key relayhistory signs the identity with (app.ts signRequest). */
  INGEST_SIGNING_KEY?: SecretBinding | string;
  INGEST_S3_BUCKET?: string;
  INGEST_S3_REGION?: string;
  INGEST_KEY_ROOT?: string;
  INGEST_KMS_KEY_ARN?: string;
  INGEST_MAX_PER_USER?: string;
  INGEST_MAX_PER_ORG?: string;
  INGEST_INLINE_MAX_SESSIONS?: string;
  INGEST_REPLICA_MODE?: string;
  INGEST_ACCEPTANCE_MODE?: string;
  INGEST_EXPECT_GENERATION?:string;
  /** P1: relayhistory (default) | shadow | frozen-supabase (sessions.ingest_frozen_source_witness). */
  INGEST_CUSTODY_SOURCE?: string;
  /** C3: "supabase" makes the Supabase frozen custody heads the privacy authority (requires INGEST_CUSTODY_SOURCE=frozen-supabase). */
  INGEST_PRIVACY_AUTHORITY?: string;
  /** Neon-exit release profile: unset/"false" (default) | "true" (src/shared/neon-exit.ts). */
  SESSIONS_NEON_EXIT?: string;
  /** Babysitter retention buckets (babysitter-retention.ts): only "on" (any case) enables; default off. */
  INGEST_BABYSITTER_RETENTION_ROUTING?: string;
  /** The Worker's proven effective subrequest cap; routing stays inactive without it (never assumed). */
  INGEST_SUBREQUEST_CAP?: string;
  /** Worst-case redirects one S3 fetch may follow (0..20; default 20, the fetch maximum). */
  INGEST_SUBREQUEST_REDIRECTS?: string;
  /** "on" (any case): per-stage latency lines (deliver.ts StageLatencyFields). Default off. */
  INGEST_STAGE_LATENCY_LOG?: string;
  INGEST_CLAIM_LOCK_BUSY?: string;
}

interface SecretBinding {
  get(): Promise<string>;
}

async function secret(value: SecretBinding | string | undefined): Promise<string | undefined> {
  if (value === undefined || typeof value === "string") return value || undefined;
  try {
    return (await value.get()) || undefined;
  } catch {
    // An unset Secrets Store secret: not configured yet.
    return undefined;
  }
}

interface Ctx {
  waitUntil(work: Promise<unknown>): void;
}

/** A required integer setting within [lo, hi]; never echoes the value. */
function bounded(raw: string, lo: number, hi: number, name: string): number {
  const n = Number(raw.trim());
  if (!Number.isSafeInteger(n) || n < lo || n > hi) throw new Error(`session ingest: ${name} must be an integer from ${lo} to ${hi}`);
  return n;
}

function limit(raw: string | undefined, fallback: number, name: string): number {
  if (raw === undefined || raw === "") return fallback;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 1 || n > 1000) throw new Error(`session ingest: ${name} must be an integer from 1 to 1000`);
  return n;
}

export interface IngestSettings {
  bucket: string;
  region: string;
  keyRoot: string;
  kmsKeyArn: string;
  accessKeyId: string;
  secretAccessKey: string;
  signingKey: string;
  maxPerUser: number;
  maxPerOrg: number;
  inlineMaxSessions: number;
  replicaMode: ReplicaMode;
  acceptanceMode: IngestAcceptanceMode;
  expectedGeneration?:string;
  /** Babysitter retention: present (true) only when INGEST_BABYSITTER_RETENTION_ROUTING=on. */
  babysitterRetention?: true;
  /** Babysitter retention budget (babysitter-retention.ts worstCaseSubrequests). */
  subrequestCap?: number;
  subrequestRedirects?: number;
  /** INGEST_EXPECT_GENERATION, when set: every Babysitter evidence write is fenced on it. */
  retentionGeneration?: string;
  /** INGEST_STAGE_LATENCY_LOG=on: per-stage latency lines. Absent (the default): none. */
  stageLatencyLog?: true;
  /** INGEST_CLAIM_LOCK_BUSY=on: a claim lock-wait failure is a 429 with Retry-After. Absent (the default): 503. */
  claimLockBusy?: true;
  /** INGEST_ASYNC_ADMISSION: absent = off (the default). */
  asyncAdmission?: Exclude<AsyncAdmissionMode, "off">;
  /** INGEST_SYNC_ADMISSION_FROZEN_RECEIPT=on. Absent (the default): off. */
  syncAdmissionFrozenReceipt?: true;
}

/** The settings, or null while any required one is missing (every request is then 503 not_configured). */
export async function ingestSettings(env: IngestWorkerEnv): Promise<IngestSettings | null> {
  // Before the not-configured checks, so a Neon edge under the profile is a loud refusal, never a quiet 503.
  assertNoNeonEdge(neonExitProfile(env as Record<string, unknown>), "the session ingest", {
    "INGEST_REPLICA_MODE is not off": (env.INGEST_REPLICA_MODE ?? "off") !== "off",
    "a NEON_REPLICA queue is bound": env.NEON_REPLICA !== undefined,
    "INGEST_CUSTODY_SOURCE is not frozen-supabase": custodySourceOf(env) !== "frozen-supabase",
    "INGEST_ACCEPTANCE_MODE is not buffered (frozen-supabase custody needs it)": (env.INGEST_ACCEPTANCE_MODE ?? "canonical") !== "buffered",
  });
  const { INGEST_S3_BUCKET: bucket, INGEST_KMS_KEY_ARN: kmsKeyArn } = env;
  if (!bucket || !kmsKeyArn || !env.HYPERDRIVE || !env.INGEST_FANOUT) return null;
  const [accessKeyId, secretAccessKey, signingKey] = await Promise.all([
    secret(env.S3_ACCESS_KEY_ID),
    secret(env.S3_SECRET_ACCESS_KEY),
    secret(env.INGEST_SIGNING_KEY),
  ]);
  if (!accessKeyId || !secretAccessKey || !signingKey) return null;
  const acceptanceMode = env.INGEST_ACCEPTANCE_MODE ?? "canonical";
  if (acceptanceMode !== "canonical" && acceptanceMode !== "buffered") throw new Error("session ingest: acceptance mode invalid");
  if(acceptanceMode==="buffered"&&(!/^[a-f0-9]{64}$/.test(env.INGEST_EXPECT_GENERATION??"")||env.INGEST_KEY_ROOT&&env.INGEST_KEY_ROOT!=="96bulls"))throw new Error("session ingest: buffered generation or prefix invalid");
  const raw = env.INGEST_REPLICA_MODE ?? "off";
  if (!["off", "queue", "required", "best_effort"].includes(raw)) throw new Error("session ingest: INGEST_REPLICA_MODE must be off, queue, required or best_effort");
  const mode: ReplicaMode = raw === "queue" ? "required" : (raw as ReplicaMode);
  if (acceptanceMode === "buffered" && (mode !== "off" || env.INGEST_INLINE_MAX_SESSIONS !== "0")) throw new Error("session ingest: buffered requires inline0 and replicaoff");
  // A replica that must be sent but has no queue would refuse every batch: not configured.
  if (mode !== "off" && !env.NEON_REPLICA) return null;
  const asyncMode = asyncAdmissionMode(env.INGEST_ASYNC_ADMISSION);
  if (asyncMode !== "off" && acceptanceMode !== "buffered") throw new Error("session ingest: INGEST_ASYNC_ADMISSION needs buffered acceptance");
  // Async admission without its queue would refuse every opted-in batch: not configured.
  if (asyncMode !== "off" && !env.INGEST_ADMISSION) return null;
  const syncFrozenReceipt = syncAdmissionFrozenReceiptMode(env.INGEST_SYNC_ADMISSION_FROZEN_RECEIPT);
  return {
    bucket,
    region: env.INGEST_S3_REGION ?? "us-east-1",
    keyRoot: env.INGEST_KEY_ROOT ?? "96bulls",
    kmsKeyArn,
    accessKeyId,
    secretAccessKey,
    signingKey,
    maxPerUser: limit(env.INGEST_MAX_PER_USER, 4, "INGEST_MAX_PER_USER"),
    maxPerOrg: limit(env.INGEST_MAX_PER_ORG, 32, "INGEST_MAX_PER_ORG"),
    inlineMaxSessions: acceptanceMode === "buffered" ? 0 : limit(env.INGEST_INLINE_MAX_SESSIONS, 40, "INGEST_INLINE_MAX_SESSIONS"),
    acceptanceMode,
    ...(acceptanceMode==="buffered"?{expectedGeneration:env.INGEST_EXPECT_GENERATION!}:{}),
    replicaMode: mode,
    // Babysitter retention (additive; babysitter-retention.ts).
    ...(routingFlag(env.INGEST_BABYSITTER_RETENTION_ROUTING) ? { babysitterRetention: true as const } : {}),
    ...(env.INGEST_SUBREQUEST_CAP ? { subrequestCap: bounded(env.INGEST_SUBREQUEST_CAP, 1, 10_000_000, "INGEST_SUBREQUEST_CAP") } : {}),
    ...(env.INGEST_SUBREQUEST_REDIRECTS ? { subrequestRedirects: bounded(env.INGEST_SUBREQUEST_REDIRECTS, 0, 20, "INGEST_SUBREQUEST_REDIRECTS") } : {}),
    ...(/^[0-9a-f]{64}$/.test(env.INGEST_EXPECT_GENERATION ?? "") ? { retentionGeneration: env.INGEST_EXPECT_GENERATION! } : {}),
    ...(routingFlag(env.INGEST_STAGE_LATENCY_LOG) ? { stageLatencyLog: true as const } : {}),
    ...(routingFlag(env.INGEST_CLAIM_LOCK_BUSY) ? { claimLockBusy: true as const } : {}),
    ...(asyncMode !== "off" ? { asyncAdmission: asyncMode } : {}),
    ...(syncFrozenReceipt === "on" ? { syncAdmissionFrozenReceipt: true as const } : {}),
  };
}

function custodySourceOf(env: IngestWorkerEnv): string {
  return env.INGEST_CUSTODY_SOURCE?.trim() || "relayhistory";
}

function store(s: IngestSettings): ObjectStore {
  return createS3Store({
    bucket: s.bucket,
    region: s.region,
    credentials: { accessKeyId: s.accessKeyId, secretAccessKey: s.secretAccessKey },
    serverSideEncryption: "aws:kms",
    sseKmsKeyId: s.kmsKeyArn,
  });
}

/**
 * The request app for one invocation, from validated settings: a Hyperdrive `run`
 * and the S3 target in production, a test database and a scratch store in tests.
 */
type Run = (text: string, params: unknown[]) => Promise<{ rows: Record<string, unknown>[] }>;

/** The custody / privacy-authority selection, validated (shared by the request app and the admission consumer). */
function custodyConfig(env: IngestWorkerEnv) {
  const custodySource = custodySourceOf(env);
  if (!["relayhistory", "shadow", "frozen-supabase"].includes(custodySource) || (custodySource !== "relayhistory" && (env.INGEST_ACCEPTANCE_MODE ?? "canonical") !== "buffered"))
    throw new Error("session ingest: INGEST_CUSTODY_SOURCE must be relayhistory, shadow or frozen-supabase (the latter two only in buffered mode)");
  const privacyAuthority = env.INGEST_PRIVACY_AUTHORITY?.trim() || "neon";
  if (!["neon", "supabase"].includes(privacyAuthority) || (privacyAuthority === "supabase" && custodySource !== "frozen-supabase"))
    throw new Error("session ingest: INGEST_PRIVACY_AUTHORITY must be neon, or supabase with INGEST_CUSTODY_SOURCE=frozen-supabase");
  return { custodySource: custodySource as "relayhistory" | "shadow" | "frozen-supabase", privacyAuthority: privacyAuthority as "neon" | "supabase" };
}

/** The deliver deps for one invocation (request or admission consumer): the same ledger, slots and target. */
export function deliverDepsFor(settings: IngestSettings, run: Run, target: WriteTarget, replica?: ReplicaSink): DeliverDeps {
  return {
    ledger: sqlLedger(run, {acceptanceMode:settings.acceptanceMode,expectedGeneration:settings.expectedGeneration}),
    acceptanceMode:settings.acceptanceMode,
    target,
    replica,
    // Slot TTL: past the Worker's wall-clock budget, so a lost release frees itself.
    slots: { maxPerUser: settings.maxPerUser, maxPerOrg: settings.maxPerOrg, ttlSeconds: 90 },
    inlineMaxSessions: settings.inlineMaxSessions,
    // Babysitter retention (additive; babysitter-retention.ts). Inactive with a Neon replica on.
    babysitterRetention: settings.babysitterRetention === true && replica === undefined,
    ...(settings.subrequestCap !== undefined ? { subrequestCap: settings.subrequestCap } : {}),
    ...(settings.subrequestRedirects !== undefined ? { subrequestRedirects: settings.subrequestRedirects } : {}),
    ...(settings.retentionGeneration !== undefined ? { retentionGeneration: settings.retentionGeneration } : {}),
    // Default off: per-stage latency lines through the same allowlisted logger as the route line.
    ...(settings.stageLatencyLog ? { stageLatency: (fields: StageLatencyFields) => log.info("session ingest", { ...fields }) } : {}),
    ...(settings.claimLockBusy ? { claimLockBusy: true as const } : {}),
  };
}

export function ingestApp(env: IngestWorkerEnv, settings: IngestSettings, run: Run, target: WriteTarget) {
  const replica: ReplicaSink | undefined = settings.replicaMode !== "off" && env.NEON_REPLICA
    ? {
        mode: settings.replicaMode,
        send: (message) => env.NEON_REPLICA!.send(message),
        putDescriptor: async (key, body) => {
          await target.store.put(objectKey(key), body, { ifNoneMatch: true, contentType: "application/json" }).catch((error: { code?: string }) => {
            // Deterministic body: an existing descriptor is this batch's own earlier attempt.
            if (error?.code !== "already_exists" && error?.code !== "precondition_failed") throw error;
          });
        },
      }
    : undefined;
  const { custodySource, privacyAuthority } = custodyConfig(env);
  // Babysitter retention (additive): on but inactive with a Neon replica; loud, never a 503.
  if (settings.babysitterRetention && replica) log.warn("babysitter retention routing inactive", { error_code: "replica_on" });
  if (settings.babysitterRetention && settings.subrequestCap === undefined) log.warn("babysitter retention routing inactive", { error_code: "subrequest_cap_unset" });
  if (settings.babysitterRetention && settings.retentionGeneration === undefined) log.warn("babysitter retention routing inactive", { error_code: "generation_unset" });
  return createIngestApp({
    custodySource,
    privacyAuthority,
    signingKey: settings.signingKey,
    deliver: {
      ...deliverDepsFor(settings, run, target, replica),
      fanout: (messages) => env.INGEST_FANOUT!.sendBatch(messages.map((body) => ({ body }))),
    },
    onboarding: { run },
    ...(settings.asyncAdmission && env.INGEST_ADMISSION
      ? { asyncAdmission: { mode: settings.asyncAdmission, enqueue: (message: AdmissionMessage) => env.INGEST_ADMISSION!.send(message) } }
      : {}),
    ...(settings.syncAdmissionFrozenReceipt ? { syncAdmissionFrozenReceipt: "on" as const } : {}),
  });
}

interface QueueMessage { body: unknown; attempts?: number; ack(): void; retry(o?: { delaySeconds?: number }): void }

/**
 * INGEST_ADMISSION: one message, one acknowledged inbox object (admission-consumer.ts). Its own
 * pg client per batch; every transient failure retries with backoff, then the DLQ.
 */
export async function admitQueueBatch(messages: QueueMessage[], env: IngestWorkerEnv, settings: IngestSettings | null, connect: (() => { run: Run; end: () => Promise<void> }) | null, target: WriteTarget | null): Promise<void> {
  if (!settings || !connect || !target) {
    for (const message of messages) message.retry({ delaySeconds: 300 });
    log.error("session ingest admission refused", { reason: "not_configured" });
    return;
  }
  let custody: ReturnType<typeof custodyConfig>;
  try {
    custody = custodyConfig(env);
  } catch (error) {
    logSettingsInvalid(error);
    for (const message of messages) message.retry({ delaySeconds: 300 });
    return;
  }
  const db = connect();
  try {
    const deliver = deliverDepsFor(settings, db.run, target);
    for (const message of messages) {
      const started = Date.now();
      const r = await admitMessage({ mode: settings.asyncAdmission ?? "off", deliver, custodySource: custody.custodySource, decompress: zstd }, message.body, message.attempts ?? 1)
        .catch((error: unknown): AdmissionAction => ({ action: "retry", delaySeconds: 300, code: "admission_failed", error }));
      if (r.action === "ack") {
        log.info("session ingest admission", { route: "ingest.admission", outcome: r.outcome, error_code: r.code, duration_ms: Date.now() - started, ...(r.lagMs !== undefined ? { offset: r.lagMs } : {}), attempt: message.attempts ?? 1 });
        message.ack();
      } else {
        const fields: Record<string, unknown> = { route: "ingest.admission", outcome: "retry", error_code: r.code, ...(r.stage ? { stage: r.stage } : {}), duration_ms: Date.now() - started, attempt: message.attempts ?? 1 };
        const db = (r.error as { db?: { db_error_class: string; db_error_code: string; db_error_basis: string } } | undefined)?.db;
        if (db) Object.assign(fields, { db_error_class: db.db_error_class, db_error_code: db.db_error_code, db_error_basis: db.db_error_basis });
        log.warn("session ingest admission", fields);
        // The same lock-wait alert stages as the request path, labelled as the background consumer's.
        const lock = lockAlertFields(r.error, settings.acceptanceMode);
        if (lock) log.warn("session ingest admission", { ...lock, route: "ingest.admission_lock_alert" });
        message.retry({ delaySeconds: r.delaySeconds });
      }
    }
  } finally {
    await db.end().catch(() => {});
  }
}

/** A malformed setting or a Neon-exit refusal: a code, never the setting's value. */
function logSettingsInvalid(error: unknown): void {
  log.error("session ingest settings invalid", { error_code: error instanceof NeonExitRefusal ? error.code : "settings_invalid" });
}

const NOT_CONFIGURED = () =>
  Response.json({ error: { code: "not_configured", message: "the session ingest is not configured yet" }, correlationId: "" }, { status: 503, headers: { "retry-after": "60" } });

const zstd = (bytes: Uint8Array) => new Uint8Array(zstdDecompressSync(bytes));

export default {
  async fetch(request: Request, env: IngestWorkerEnv, ctx: Ctx): Promise<Response> {
    let settings: IngestSettings | null;
    try {
      settings = await ingestSettings(env);
    } catch (error) {
      // A malformed setting: relayhistory's envelope, retryable, never a bare 500.
      logSettingsInvalid(error);
      return NOT_CONFIGURED();
    }
    if (!settings) return NOT_CONFIGURED();
    // One Hyperdrive connection per request; Hyperdrive pools the real ones.
    const client = new pg.Client({ connectionString: env.HYPERDRIVE!.connectionString });
    // A dropped socket surfaces on the next query as a coded 503, never as an uncaught error.
    client.on("error", (err: Error) => log.error("session ingest db error", dbClientErrorFields(err)));
    let connected: Promise<void> | undefined;
    const run = async (text: string, params: unknown[]) => {
      connected ??= client.connect();
      await connected;
      return (await client.query(text, params as unknown[])) as unknown as { rows: Record<string, unknown>[] };
    };
    const app = ingestApp(env, settings, run, { store: store(settings), keyRoot: settings.keyRoot });
    try {
      return await app.fetch(request);
    } finally {
      if (connected) ctx.waitUntil(client.end().catch(() => {}));
    }
  },

  /**
   * INGEST_FANOUT: one message, one slice of a staged batch. A failure retries it
   * (then the DLQ: runbooks/session-ingest-rollout.md "Fan-out DLQ redrive"). While
   * the Worker is not configured (a missing or rotated secret) a message waits five
   * minutes per attempt instead of walking to the DLQ in seconds.
   */
  async queue(batch: { queue?: string; messages: QueueMessage[] }, env: IngestWorkerEnv): Promise<void> {
    const settings = await ingestSettings(env).catch((error: unknown) => { logSettingsInvalid(error); return null; });
    // Admission messages (INGEST_ADMISSION) are not canonical fanout: buffered mode admits them.
    const admission = batch.messages.filter((m) => (m.body as { kind?: unknown } | null)?.kind === "ingest-admission");
    if (admission.length > 0) {
      const connect = settings && env.HYPERDRIVE ? () => {
        const client = new pg.Client({ connectionString: env.HYPERDRIVE!.connectionString });
        client.on("error", (err: Error) => log.error("session ingest db error", dbClientErrorFields(err)));
        let connected: Promise<void> | undefined;
        return {
          run: async (text: string, params: unknown[]) => {
            connected ??= client.connect();
            await connected;
            return (await client.query(text, params as unknown[])) as unknown as { rows: Record<string, unknown>[] };
          },
          end: async () => { if (connected) await client.end(); },
        };
      } : null;
      await admitQueueBatch(admission, env, settings, connect, settings ? { store: store(settings), keyRoot: settings.keyRoot } : null);
      batch = { ...batch, messages: batch.messages.filter((m) => !admission.includes(m)) };
      if (batch.messages.length === 0) return;
    }
    if (settings?.acceptanceMode === "buffered") {
      for (const message of batch.messages) message.retry({delaySeconds:300});
      log.info("session ingest fanout refused", {reason:"buffered_publication_refused"});
      return;
    }
    const client = settings && env.HYPERDRIVE ? new pg.Client({ connectionString: env.HYPERDRIVE.connectionString }) : null;
    client?.on("error", (err: Error) => log.error("session ingest db error", dbClientErrorFields(err)));
    let connected: Promise<void> | undefined;
    const ledger = client
      ? sqlLedger(async (text, params) => {
          connected ??= client.connect();
          await connected;
          return (await client.query(text, params as unknown[])) as unknown as { rows: Record<string, unknown>[] };
        })
      : null;
    try {
      for (const message of batch.messages) {
        const body = message.body as FanoutMessage;
        if (!settings || !ledger || body?.kind !== "ingest-fanout") {
          log.error("session ingest fanout refused", { reason: settings ? "unknown_message" : "not_configured" });
          message.retry({ delaySeconds: 300 });
          continue;
        }
        try {
          const written = await fanout({ store: store(settings), keyRoot: settings.keyRoot }, body, zstd, ledger);
          log.info("session ingest fanout", {
            outcome: "ok",
            manifests: written.manifests.filter((m) => m.written).length,
            skipped: written.skipped,
            inserted: written.revisions.committed,
            count: body.to - body.from,
          });
          message.ack();
        } catch (error) {
          log.error("session ingest fanout failed", { error_code: (error as { code?: string }).code ?? "fanout_failed" });
          message.retry({ delaySeconds: 30 });
        }
      }
    } finally {
      if (connected) await client!.end().catch(() => {});
    }
  },
};
