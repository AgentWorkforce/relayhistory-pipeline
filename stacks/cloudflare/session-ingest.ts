/**
 * The session ingest's front door: one Cloudflare Worker, `96bulls-session-ingest`,
 * that takes relayhistory's `POST /v1/delivery/batches` (same request, receipt and
 * 409/429/503 semantics) and writes scrubbed chunks and manifests straight into
 * the sessions-prod transcripts bucket, with idempotency, revision checks and
 * deletion fences in Supabase. Neon is not on the request path, and no Lambda is
 * (the sessions-prod account's Lambda concurrency limit is 10, and cold starts).
 *
 *   Relay Desktop --HTTPS--> history.agentrelay.com (relayhistory-api Worker, auth + per-org flag)
 *       --service binding DELIVERY_INGEST--> 96bulls-session-ingest (this Worker; no route, no URL)
 *            --> S3 transcripts (transcript srv-writer IAM user: m=srv_*, live-sessions/, live-batches/)
 *            --> Supabase via HYPERDRIVE (bulls96_session_ingest role, transaction pooler)
 *            --> INGEST_FANOUT queue (this stack) --> this Worker's queue() handler (DLQ after 10)
 *            --> NEON_REPLICA (relayhistory's existing queue, producer only; transition only)
 *
 * Security invariant (assertPrivateWorker, tested and mutation-checked): the only
 * way in is relayhistory's service binding. No route, no custom domain, no
 * workers.dev URL and no preview URLs, so nothing on the internet reaches this
 * script. A service binding requires both Workers in the same Cloudflare
 * account: `accountId` is the relayhistory account, and the entry refuses any
 * other.
 *
 * Credentials. Alchemy never holds one:
 *   - Supabase: a Hyperdrive config the lead creates out of band (its origin
 *     password lives only in Cloudflare); this stack only looks it up by id.
 *   - S3: the transcript srv-writer user's access key, in the account's Secrets
 *     Store (created out of band from 1Password), bound by store id and secret
 *     name. Not `wrangler secret put`: this Alchemy version uploads every Worker
 *     version without `keep_bindings`, so a secret set out of band on the script
 *     would be dropped by the next deploy (WorkerProvider: `keepBindings:
 *     undefined`). A Secrets Store binding is part of the declared bindings and
 *     carries no value. At runtime it is `await env.S3_ACCESS_KEY_ID.get()`.
 *
 * External bindings (`externalQueue`, `secretsStoreSecret`) are binding
 * descriptors, not resources: no state row, so plan, deploy and destroy can never
 * create, change or delete relayhistory's queue or the secrets. They are shaped
 * as the resources Alchemy's Worker classifies (`Type`), and a test pins that
 * classification against the installed @alchemy.run/cloudflare.
 */
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";

export const SESSION_INGEST_WORKER = "96bulls-session-ingest";
export const SESSION_INGEST_STAGE = "cf-sessions-prod";
/** Pinned: a later date changes runtime behaviour, so it moves only in a reviewed update. */
export const COMPATIBILITY_DATE = "2026-10-01";
export const COMPATIBILITY_FLAGS = ["nodejs_compat"] as const;
export const TRANSCRIPT_BUCKET = "96bulls-aws-sessions-prod-transcripts-941433935742";
export const TRANSCRIPT_REGION = "us-east-1";
export const KEY_ROOT = "96bulls";
export const INBOX_AUTHORITY_MODE = "authority-v1" as const;
export const INBOX_CANDIDATE_KEY_ROOT = "96bulls/upload-inbox/v1";
export const FANOUT_QUEUE = "96bulls-session-ingest-fanout";
export const FANOUT_DLQ = "96bulls-session-ingest-fanout-dlq";
/** One batch's fan-out per invocation; retried with backoff, then dead-lettered. */
export const FANOUT_BATCH_SIZE = 1;
export const FANOUT_MAX_RETRIES = 10;
/** Secrets Store secret names (values created out of band from 1Password). */
export const S3_ACCESS_KEY_SECRET = "96bulls-session-ingest-s3-access-key-id";
export const S3_SECRET_KEY_SECRET = "96bulls-session-ingest-s3-secret-access-key";
/** The HMAC key relayhistory signs the forwarded identity with (src/session-ingest/app.ts). */
export const SIGNING_KEY_SECRET = "96bulls-session-ingest-signing-key";

/** Plan resources, in declaration order. The Hyperdrive lookup and the external bindings have no state row. */
export const SESSION_INGEST_RESOURCES = [
  "session-ingest-fanout-dlq",
  "session-ingest-fanout",
  "session-ingest",
  "session-ingest-fanout-consumer",
] as const;

export type ReplicaMode = "off" | "queue";

export interface InboxAuthorityDeploymentInputs {
  mode: typeof INBOX_AUTHORITY_MODE;
  /** Immutable authority head/snapshot root. Candidate bodies use INBOX_CANDIDATE_KEY_ROOT instead. */
  keyRoot: string;
  expectedGeneration: string;
  expectedControlEpoch: string;
  expectedAuthEpoch: string;
  expectedSourceIdentitySha256: string;
}

export interface SessionIngestInputs {
  /** The relayhistory Cloudflare account (the service binding needs both Workers in one account). */
  accountId: string;
  /** Buffered acceptance stores S3/outbox only; canonical publication is separately refused. */
  acceptanceMode?: "canonical" | "buffered";
  /** P1 custody source; omit for the relayhistory (Neon) witness. shadow compares, frozen-supabase serves. */
  custodySource?: "shadow" | "frozen-supabase";
  /** C3 privacy authority; omit for Neon. "supabase" requires custodySource frozen-supabase (and the owner-activated Supabase authority). */
  privacyAuthority?: "supabase";
  /** Exact final composed migrator generation(), required only for buffered acceptance. */
  expectedGeneration?: string;
  /** The Hyperdrive config the lead created (Supabase transaction pooler, bulls96_session_ingest). */
  hyperdriveId: string;
  /** The account Secrets Store holding the srv-writer access key. */
  secretsStoreId: string;
  /** The transcripts CMK; every PUT names it (the bucket policy pins the exact key). */
  kmsKeyArn: string;
  /** In-flight batches per user and per org before 429 (the per-user lock's replacement). Defaults 4 and 32. */
  maxPerUser?: number;
  maxPerOrg?: number;
  /** Sessions a batch may write inline before the rest go through INGEST_FANOUT. Default 40. */
  inlineMaxSessions?: number;
  /** `queue`: send the Neon replica message to `neonReplicaQueue` (transition only). Default `off`. */
  replicaMode?: ReplicaMode;
  /** relayhistory's existing Neon replica queue (producer binding only). Required with replicaMode `queue`. */
  neonReplicaQueue?: string;
  /** Default-off S3 inbox authority. Presence is an all-or-nothing managed selection. */
  authority?: InboxAuthorityDeploymentInputs;
}

const HEX32 = /^[0-9a-f]{32}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const KEY_ARN = /^arn:aws:kms:us-east-1:941433935742:key\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const REPLICA_QUEUE = /^relayhistory-neon-replica(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?$/;
const AUTHORITY_KEYS = ["mode", "keyRoot", "expectedGeneration", "expectedControlEpoch", "expectedAuthEpoch", "expectedSourceIdentitySha256"] as const;
const AUTHORITY_ENV_KEYS = [
  "INGEST_AUTHORITY_MODE",
  "INGEST_AUTHORITY_KEY_ROOT",
  "INGEST_AUTHORITY_EXPECT_GENERATION",
  "INGEST_AUTHORITY_EXPECT_CONTROL_EPOCH",
  "INGEST_AUTHORITY_EXPECT_AUTH_EPOCH",
  "INGEST_AUTHORITY_EXPECT_SOURCE_IDENTITY",
] as const;
const KEYS = ["custodySource", "privacyAuthority", "expectedGeneration", "acceptanceMode", "accountId", "hyperdriveId", "secretsStoreId", "kmsKeyArn", "maxPerUser", "maxPerOrg", "inlineMaxSessions", "replicaMode", "neonReplicaQueue", "authority"];

function validAuthorityKeyRoot(value: unknown): value is string {
  if (typeof value !== "string" || !/^96bulls\/[A-Za-z0-9][A-Za-z0-9_/-]{0,151}$/.test(value) || value.endsWith("/")) return false;
  const parts = value.split("/");
  if (parts.some((part) => !part || part === "." || part === "..")) return false;
  return !(
    value === INBOX_CANDIDATE_KEY_ROOT ||
    value.startsWith(`${INBOX_CANDIDATE_KEY_ROOT}/`) ||
    INBOX_CANDIDATE_KEY_ROOT.startsWith(`${value}/`)
  );
}

function inboxAuthority(
  value: unknown,
  refuse: (why: string) => never,
): InboxAuthorityDeploymentInputs | undefined {
  if (value === undefined) return undefined;
  if (value === null || typeof value !== "object" || Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype)
    refuse("sessionIngest.authority must be a plain object");
  const authority = value as Record<string, unknown>;
  for (const key of Object.keys(authority))
    if (!(AUTHORITY_KEYS as readonly string[]).includes(key)) refuse(`sessionIngest.authority.${key} is not a known setting`);
  for (const key of AUTHORITY_KEYS)
    if (!(key in authority)) refuse(`sessionIngest.authority.${key} is required when authority mode is selected`);
  if (authority.mode !== INBOX_AUTHORITY_MODE)
    refuse(`sessionIngest.authority.mode must be exactly ${INBOX_AUTHORITY_MODE}`);
  if (!validAuthorityKeyRoot(authority.keyRoot))
    refuse(`sessionIngest.authority.keyRoot must be an isolated 96bulls/ root distinct from ${INBOX_CANDIDATE_KEY_ROOT}`);
  for (const key of ["expectedGeneration", "expectedControlEpoch", "expectedAuthEpoch", "expectedSourceIdentitySha256"] as const)
    if (typeof authority[key] !== "string" || !HEX64.test(authority[key] as string))
      refuse(`sessionIngest.authority.${key} must be a 64-character lowercase hex digest`);
  return authority as unknown as InboxAuthorityDeploymentInputs;
}

function inboxAuthorityVars(authority: unknown, refuse: (why: string) => never): Record<string, string> {
  const selected = inboxAuthority(authority, refuse);
  if (!selected) return {};
  return {
    INGEST_AUTHORITY_MODE: selected.mode,
    INGEST_AUTHORITY_KEY_ROOT: selected.keyRoot,
    INGEST_AUTHORITY_EXPECT_GENERATION: selected.expectedGeneration,
    INGEST_AUTHORITY_EXPECT_CONTROL_EPOCH: selected.expectedControlEpoch,
    INGEST_AUTHORITY_EXPECT_AUTH_EPOCH: selected.expectedAuthEpoch,
    INGEST_AUTHORITY_EXPECT_SOURCE_IDENTITY: selected.expectedSourceIdentitySha256,
  };
}

/** Same validator for source admission and direct declaration/render callers. */
function bufferedGeneration(p: { acceptanceMode?: unknown; expectedGeneration?: unknown }, refuse: (why: string) => never): string | undefined {
  if (p.acceptanceMode === "buffered") {
    if (typeof p.expectedGeneration !== "string" || !/^[0-9a-f]{64}$/.test(p.expectedGeneration))
      refuse("sessionIngest.expectedGeneration must be the final composed 64-character lowercase hex generation in buffered mode");
    return p.expectedGeneration as string;
  }
  if (p.expectedGeneration !== undefined)
    refuse("sessionIngest.expectedGeneration is only bound in buffered mode");
  return undefined;
}

export function assertSessionIngestInputs(p: unknown, refuse: (why: string) => never): asserts p is SessionIngestInputs {
  if (p === null || typeof p !== "object" || Array.isArray(p) || Object.getPrototypeOf(p) !== Object.prototype) refuse("sessionIngest inputs must be a plain object");
  const v = p as Record<string, unknown>;
  for (const k of Object.keys(v)) if (!KEYS.includes(k)) refuse(`sessionIngest.${k} is not a known setting`);
  for (const k of ["accountId", "hyperdriveId", "secretsStoreId"] as const)
    if (typeof v[k] !== "string" || !HEX32.test(v[k] as string)) refuse(`sessionIngest.${k} must be a 32-character lowercase hex id`);
  if (typeof v.kmsKeyArn !== "string" || !KEY_ARN.test(v.kmsKeyArn)) refuse("sessionIngest.kmsKeyArn must be the transcripts key's ARN (arn:aws:kms:us-east-1:941433935742:key/<uuid>)");
  const int = (k: string, lo: number, hi: number) => {
    if (v[k] !== undefined && !(Number.isInteger(v[k]) && (v[k] as number) >= lo && (v[k] as number) <= hi)) refuse(`sessionIngest.${k} must be an integer from ${lo} to ${hi}`);
  };
  int("maxPerUser", 1, 64);
  int("maxPerOrg", 1, 1024);
  // 48 sessions is the Worker's subrequest budget per batch (relayhistory MAX_LIVE_SESSIONS).
  if (v.acceptanceMode !== undefined && v.acceptanceMode !== "canonical" && v.acceptanceMode !== "buffered") refuse("sessionIngest.acceptanceMode must be canonical or buffered");
  if (v.custodySource !== undefined && (!(v.custodySource === "shadow" || v.custodySource === "frozen-supabase") || v.acceptanceMode !== "buffered")) refuse("sessionIngest.custodySource must be shadow or frozen-supabase, in buffered mode only (omit for relayhistory)");
  if (v.privacyAuthority !== undefined && (v.privacyAuthority !== "supabase" || v.custodySource !== "frozen-supabase")) refuse("sessionIngest.privacyAuthority must be supabase, with custodySource frozen-supabase only (omit for neon)");
  bufferedGeneration(v, refuse);
  int("inlineMaxSessions", v.acceptanceMode === "buffered" ? 0 : 1, 48);
  if (v.acceptanceMode === "buffered" && (v.inlineMaxSessions !== 0 || (v.replicaMode ?? "off") !== "off")) refuse("buffered acceptance requires explicit inlineMaxSessions 0 and replicaMode off");
  if (v.maxPerUser !== undefined && v.maxPerOrg !== undefined && (v.maxPerUser as number) > (v.maxPerOrg as number)) refuse("sessionIngest.maxPerUser cannot exceed maxPerOrg");
  if (v.replicaMode !== undefined && v.replicaMode !== "off" && v.replicaMode !== "queue") refuse('sessionIngest.replicaMode must be "off" or "queue"');
  if (v.neonReplicaQueue !== undefined && (typeof v.neonReplicaQueue !== "string" || !REPLICA_QUEUE.test(v.neonReplicaQueue)))
    refuse("sessionIngest.neonReplicaQueue must be relayhistory's Neon replica queue (relayhistory-neon-replica[-<suffix>])");
  if (v.replicaMode === "queue" && v.neonReplicaQueue === undefined) refuse('sessionIngest.replicaMode "queue" needs neonReplicaQueue');
  if (v.replicaMode !== "queue" && v.neonReplicaQueue !== undefined) refuse('sessionIngest.neonReplicaQueue is bound only with replicaMode "queue"');
  inboxAuthority(v.authority, refuse);
}

/** A producer binding to a queue this stack does not own (no state row: never created, changed or deleted here). */
export function externalQueue(queueName: string) {
  return { Type: "Cloudflare.Queues.Queue" as const, LogicalId: `external:${queueName}`, queueName, queueId: undefined };
}

/** A Secrets Store secret this stack does not own: bound by store and name, never a value. */
export function secretsStoreSecret(storeId: string, secretName: string) {
  return { Type: "Cloudflare.SecretsStore.Secret" as const, LogicalId: `external:${secretName}`, storeId, secretName };
}

/** Plain vars: ids and switches only. */
export function sessionIngestVars(p: SessionIngestInputs) {
  const expectedGeneration = bufferedGeneration(p, (why) => { throw new Error(why); });
  return {
    INGEST_S3_BUCKET: TRANSCRIPT_BUCKET,
    INGEST_S3_REGION: TRANSCRIPT_REGION,
    INGEST_KEY_ROOT: KEY_ROOT,
    INGEST_KMS_KEY_ARN: p.kmsKeyArn,
    INGEST_MAX_PER_USER: String(p.maxPerUser ?? 4),
    INGEST_MAX_PER_ORG: String(p.maxPerOrg ?? 32),
    INGEST_INLINE_MAX_SESSIONS: String(p.inlineMaxSessions ?? 40),
    INGEST_REPLICA_MODE: p.replicaMode ?? "off",
    ...(p.acceptanceMode === "buffered" ? { INGEST_ACCEPTANCE_MODE: "buffered", INGEST_EXPECT_GENERATION: expectedGeneration! } : {}),
    ...(p.custodySource ? { INGEST_CUSTODY_SOURCE: p.custodySource } : {}),
    ...(p.privacyAuthority ? { INGEST_PRIVACY_AUTHORITY: p.privacyAuthority } : {}),
    ...inboxAuthorityVars(p.authority, (why) => { throw new Error(why); }),
  };
}

/**
 * The Worker's props. `bindings` are supplied by the stage (the fan-out queue
 * resource, the Hyperdrive lookup), so the pure invariants can be checked on
 * exactly what is deployed.
 */
export function sessionIngestWorkerProps(p: SessionIngestInputs, bindings: { HYPERDRIVE: unknown; INGEST_FANOUT: unknown }) {
  return {
    name: SESSION_INGEST_WORKER,
    main: "src/session-ingest/worker.ts",
    // worker.ts is a plain Cloudflare Fetch/Queue module, not an Alchemy Effect entrypoint.
    isExternal: true as const,
    compatibility: { date: COMPATIBILITY_DATE, flags: [...COMPATIBILITY_FLAGS] },
    // Smart placement runs the Worker near what it calls most (S3 and Supabase,
    // both us-east-1), not near relayhistory's edge PoP: one service-binding hop
    // in, then many short round trips out.
    placement: { mode: "smart" as const },
    // The only way in is relayhistory's service binding (assertPrivateWorker).
    workersDev: false as const,
    routes: [] as never[],
    // null (not omitted): Alchemy then detaches any custom domain attached out of band.
    domain: null,
    env: {
      ...sessionIngestVars(p),
      HYPERDRIVE: bindings.HYPERDRIVE,
      INGEST_FANOUT: bindings.INGEST_FANOUT,
      S3_ACCESS_KEY_ID: secretsStoreSecret(p.secretsStoreId, S3_ACCESS_KEY_SECRET),
      S3_SECRET_ACCESS_KEY: secretsStoreSecret(p.secretsStoreId, S3_SECRET_KEY_SECRET),
      INGEST_SIGNING_KEY: secretsStoreSecret(p.secretsStoreId, SIGNING_KEY_SECRET),
      ...(p.replicaMode === "queue" && p.neonReplicaQueue ? { NEON_REPLICA: externalQueue(p.neonReplicaQueue) } : {}),
    },
  };
}

export type SessionIngestWorkerProps = ReturnType<typeof sessionIngestWorkerProps>;

/**
 * The Worker is reachable only through a service binding: no workers.dev URL or
 * preview URLs, no zone route, no custom domain, no preview or version parent,
 * no Access app (which would imply a hostname). Plus the runtime it needs.
 */
export function assertPrivateWorker(props: Record<string, unknown>, refuse: (why: string) => never): void {
  if (props.name !== SESSION_INGEST_WORKER) refuse(`the ingest Worker must be named ${SESSION_INGEST_WORKER} (relayhistory binds it by name)`);
  if (props.isExternal !== true) refuse("the ingest Worker must be packaged as the plain external Fetch/Queue module (isExternal: true)");
  if (props.workersDev !== false) refuse("workers.dev must be disabled (workersDev: false): the only way in is relayhistory's service binding");
  if (!Array.isArray(props.routes) || props.routes.length !== 0) refuse("the ingest Worker must have no routes");
  if (props.domain !== null) refuse("the ingest Worker must have no custom domain (domain: null detaches any)");
  for (const k of ["url", "preview", "version", "access", "namespace", "assets", "crons"])
    if (props[k] !== undefined) refuse(`the ingest Worker must not set ${k}`);
  const compat = props.compatibility as { date?: unknown; flags?: unknown } | undefined;
  if (compat?.date !== COMPATIBILITY_DATE) refuse(`the compatibility date must be pinned to ${COMPATIBILITY_DATE}`);
  if (!Array.isArray(compat?.flags) || !compat.flags.includes("nodejs_compat") || compat.flags.includes("no_nodejs_compat"))
    refuse("the ingest Worker needs nodejs_compat (node:zlib zstd, node:crypto)");
  if ((props.placement as { mode?: unknown } | undefined)?.mode !== "smart") refuse("the ingest Worker must use smart placement");
}

/** Names whose values would be credentials: they must be bindings, never literals Alchemy state would hold. */
const SECRET_NAME = /SECRET|PASSWORD|TOKEN|ACCESS_KEY|PRIVATE|CREDENTIAL|DATABASE_URL|CONNECTION/i;

/**
 * No credential is a literal in the Worker's env (a literal would be uploaded
 * from, and recorded in, Alchemy state). Credential-named entries must be
 * Secrets Store descriptors; every other plain value is a string var with no
 * URL userinfo.
 */
export function assertNoSecretLiterals(env: Record<string, unknown>, refuse: (why: string) => never): void {
  for (const [k, v] of Object.entries(env)) {
    const isSecretBinding = typeof v === "object" && v !== null && (v as { Type?: unknown }).Type === "Cloudflare.SecretsStore.Secret";
    if (SECRET_NAME.test(k) && !isSecretBinding) refuse(`env ${k} must be a Secrets Store binding, never a value`);
    if (typeof v === "string" && /:\/\/[^/\s]*@/.test(v)) refuse(`env ${k} carries URL credentials`);
    if (typeof v === "object" && v !== null && "value" in v) refuse(`env ${k} carries a value`);
  }
  for (const k of ["S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY", "INGEST_SIGNING_KEY"]) if (!(k in env)) refuse(`env ${k} is missing (Secrets Store binding)`);
  const authorityNames = Object.keys(env).filter((key) => key.startsWith("INGEST_AUTHORITY_"));
  for (const key of authorityNames)
    if (!(AUTHORITY_ENV_KEYS as readonly string[]).includes(key)) refuse(`env ${key} is not a known inbox authority binding`);
  if (authorityNames.length > 0) {
    for (const key of AUTHORITY_ENV_KEYS) if (!(key in env)) refuse(`env ${key} is missing from the selected inbox authority bindings`);
    inboxAuthority({
      mode: env.INGEST_AUTHORITY_MODE,
      keyRoot: env.INGEST_AUTHORITY_KEY_ROOT,
      expectedGeneration: env.INGEST_AUTHORITY_EXPECT_GENERATION,
      expectedControlEpoch: env.INGEST_AUTHORITY_EXPECT_CONTROL_EPOCH,
      expectedAuthEpoch: env.INGEST_AUTHORITY_EXPECT_AUTH_EPOCH,
      expectedSourceIdentitySha256: env.INGEST_AUTHORITY_EXPECT_SOURCE_IDENTITY,
    }, refuse);
  }
}

export function fanoutConsumerSettings() {
  return { batchSize: FANOUT_BATCH_SIZE, maxRetries: FANOUT_MAX_RETRIES };
}

/** The consumer must exist, be this Worker, single-message batches, bounded retries, dead-lettered. */
export function assertFanoutConsumer(c: { scriptName: unknown; deadLetterQueue: unknown; settings?: { batchSize?: number; maxRetries?: number } }, refuse: (why: string) => never) {
  if (c.scriptName !== SESSION_INGEST_WORKER) refuse("the fan-out queue's consumer must be the ingest Worker");
  if (c.deadLetterQueue !== FANOUT_DLQ) refuse(`the fan-out consumer must dead-letter to ${FANOUT_DLQ}`);
  if (c.settings?.batchSize !== FANOUT_BATCH_SIZE) refuse(`the fan-out consumer's batch size must be ${FANOUT_BATCH_SIZE}`);
  if (c.settings?.maxRetries !== FANOUT_MAX_RETRIES) refuse(`the fan-out consumer's max retries must be ${FANOUT_MAX_RETRIES}`);
}

export function fanoutConsumerProps(queueId: unknown) {
  return { queueId, scriptName: SESSION_INGEST_WORKER, deadLetterQueue: FANOUT_DLQ, settings: fanoutConsumerSettings() };
}

export type SessionIngestPlanPhase = "session ingest rollout" | "session ingest update" | "post-apply";

/**
 * The deploy plan contract (same `Plan:` summary and `[resource] action` line
 * format as the AWS stages): exactly the ingest's resources, each listed once;
 * rollout creates (all, or the rest after a partial apply), update changes in
 * place, post-apply changes nothing. Never a replacement or deletion, never
 * creates and updates together. The detailed plan must list the same actions
 * and must not show a credential.
 */
export function assertSessionIngestPlanContract(planText: string, detailedPlanText: string): SessionIngestPlanPhase {
  const refuse = (why: string): never => {
    throw new Error(`96bulls: ${SESSION_INGEST_STAGE} plan contract refused: ${why}. Nothing was changed.`);
  };
  const parse = (text: string, label: string) => {
    const summaries = text.split(/\r?\n/).filter((l) => l.startsWith("Plan:"));
    if (summaries.length !== 1) refuse(`${label} plan: expected exactly one Plan: summary, found ${summaries.length}`);
    const ops = new Map<string, string>();
    for (const line of text.split(/\r?\n/)) {
      const m = /^\s*\[([^\]]+)\]\s+(\S+)\s*$/.exec(line);
      if (!m) continue;
      if (ops.has(m[1]!)) refuse(`${label} plan: resource ${m[1]} appears more than once`);
      ops.set(m[1]!, m[2]!);
    }
    for (const r of ops.keys()) if (!(SESSION_INGEST_RESOURCES as readonly string[]).includes(r)) refuse(`${label} plan: unexpected resource ${r}`);
    for (const r of SESSION_INGEST_RESOURCES) if (!ops.has(r)) refuse(`${label} plan: missing resource ${r}`);
    for (const [r, a] of ops) if (a !== "create" && a !== "update" && a !== "noop") refuse(`${label} plan: ${r} has forbidden action ${a}`);
    const counts = new Map<string, number>();
    for (const a of ops.values()) if (a !== "noop") counts.set(a, (counts.get(a) ?? 0) + 1);
    const summary = summaries[0]!;
    const clauses = [...summary.matchAll(/(\d+) to ([a-z-]+)/g)].map((m) => [m[2]!, Number(m[1])] as const);
    if (clauses.length === 0) {
      if (!/^Plan:\s*(?:0 changes|no changes)\s*$/i.test(summary) || counts.size) refuse(`${label} plan: summary does not match the resource lines`);
    } else {
      const said = new Map(clauses);
      if (said.size !== clauses.length || said.size !== counts.size) refuse(`${label} plan: summary does not match the resource lines`);
      for (const [a, n] of counts) if (said.get(a) !== n) refuse(`${label} plan: summary says ${said.get(a) ?? 0} ${a} but resource lines contain ${n}`);
    }
    return { summary, ops };
  };
  const compact = parse(planText, "compact");
  const detailed = parse(detailedPlanText, "detailed");
  if (compact.summary !== detailed.summary) refuse("compact and detailed plan summaries differ");
  for (const r of SESSION_INGEST_RESOURCES) if (compact.ops.get(r) !== detailed.ops.get(r)) refuse(`compact and detailed plans disagree on ${r}`);
  // A credential in the planned props would be one Alchemy state now holds.
  if (/secret_text|AKIA[0-9A-Z]{16}|-----BEGIN [A-Z ]*PRIVATE KEY|postgres(?:ql)?:\/\/[^\s"]*:[^\s"@]*@/.test(detailedPlanText))
    refuse("the detailed plan shows a credential value or a secret_text binding");
  const actions = [...compact.ops.values()];
  if (actions.every((a) => a === "noop")) return "post-apply";
  if (actions.every((a) => a === "create" || a === "noop")) return "session ingest rollout";
  if (actions.every((a) => a === "update" || a === "noop")) return "session ingest update";
  return refuse(`action set is neither a rollout (creates only) nor an update (in-place only): ${[...compact.ops].filter(([, a]) => a !== "noop").map(([r, a]) => `${r}:${a}`).join(", ")}`);
}

/** Declares the ingest: fan-out queue + DLQ, the Worker, the queue consumer. */
export const sessionIngestStage = (p: SessionIngestInputs) =>
  Effect.gen(function* () {
    const refuse = (why: string): never => {
      throw new Error(`96bulls: ${SESSION_INGEST_STAGE}: ${why}. Nothing was created.`);
    };
    const dlq = yield* Cloudflare.Queues.Queue("session-ingest-fanout-dlq", { name: FANOUT_DLQ, ...(p.acceptanceMode === "buffered" ? { deliveryPaused: true } : {}) } as never);
    const fanout = yield* Cloudflare.Queues.Queue("session-ingest-fanout", { name: FANOUT_QUEUE, ...(p.acceptanceMode === "buffered" ? { deliveryPaused: true } : {}) } as never);
    const props = sessionIngestWorkerProps(p, {
      HYPERDRIVE: Cloudflare.Hyperdrive.lookup("HYPERDRIVE", { hyperdriveId: p.hyperdriveId }),
      INGEST_FANOUT: fanout,
    });
    assertPrivateWorker(props, refuse);
    assertNoSecretLiterals(props.env, refuse);
    const worker = yield* Cloudflare.Worker("session-ingest", props as never);
    const consumer = fanoutConsumerProps(fanout.queueId);
    assertFanoutConsumer(consumer, refuse);
    // The outputs order it after the Worker and the DLQ: Cloudflare refuses a consumer for a
    // script or dead-letter queue that does not exist yet.
    yield* Cloudflare.Queues.Consumer("session-ingest-fanout-consumer", {
      ...consumer,
      scriptName: (worker as unknown as { workerName: string }).workerName,
      deadLetterQueue: dlq.queueName,
    } as never);
    return { workerName: SESSION_INGEST_WORKER, fanoutQueue: FANOUT_QUEUE, fanoutDlq: FANOUT_DLQ };
  });
