/**
 * Configuration of the self-host session ingest edge (SERVICE=session-ingest,
 * selfhost-server.ts), from the environment only. The hosted Cloudflare Worker
 * (worker.ts) never reads this file; nothing here changes the hosted path.
 *
 *   RELAYHISTORY_SELFHOST_INGEST     unset | "" | "false" (default: dormant) | exactly "true"; anything
 *                                    else is a startup error, so a typo never silently means either
 *   RELAYHISTORY_INGEST_PORT         listen port, default 8081 (parsed even when dormant)
 *   RELAYHISTORY_SELFHOST_SESSION_SELECTION
 *                                    unset | "" | "false" (default: off) | exactly "true"; anything else is a
 *                                    startup error. Serves POST /v1/delivery/sessions/selection with enabled:false
 *                                    (per-session erase) and needs migration 043910 applied.
 *   RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT
 *                                    unset | "" | "false" (default: off, the route answers 501) | exactly "true";
 *                                    anything else is a startup error. Serves POST /v1/onboarding/heartbeat as an
 *                                    authenticated (rth:sync) 204 that discards the body: nothing is stored or logged.
 *   RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS
 *                                    unset | "" | "0" (default: off, every upload request is introspected) | a
 *                                    plain decimal integer 1..60000 (no sign, spaces, exponent or leading zero);
 *                                    anything else, a value over 60000 included, is a startup error (never
 *                                    clamped). How long an admitted token is reused per process before it is
 *                                    introspected again, also capped at the token's exp: the revocation lag.
 *                                    Recommended 30000-60000 when Control load matters (selfhost-auth.ts).
 *   RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS
 *                                    unset | "" | "report" (default) | exactly "enforce"; anything else is a startup
 *                                    error. The capture policy upload check (capture-policy.ts): report logs an upload
 *                                    made under a missing or older policy revision (or a session older than a
 *                                    `sharing_mode: new` policy) and accepts it; enforce refuses it.
 *   SESSION_ERASE_API_ENABLED        operator per-session erase route (session-erase.ts): exactly "1" enables;
 *                                    unset, "" or "0" is off; anything else is a startup error. With it,
 *                                    SESSION_ERASE_API_SECRET (+ _PREVIOUS during a rotation; Secret-injected, min
 *                                    32 chars). Flag off or secret unset: /internal/v1/session-erase/* answers 501.
 *
 * Enabled only (each missing or malformed value refuses startup, naming the variable, never the value):
 *   RELAYHISTORY_INGEST_ORIGIN       this edge's public origin, sent to relaycontrol as `aud` (must be the
 *                                    origin relaycontrol binds the Desktop token to)
 *   RELAYCONTROL_INTROSPECT_URL      relaycontrol's introspection base URL (destination policy:
 *                                    https, loopback, or a k8s Service name with ALLOW_IN_CLUSTER_HTTP=true)
 *   RELAYCONTROL_INTROSPECT_SECRET   the introspection service secret (Secret-injected, never on disk)
 *   RELAYHISTORY_INGEST_DATABASE_URL the bulls96_session_ingest login (Secret-injected). Only host, port,
 *                                    user, password and database are used; the query string never reaches pg
 *   SESSION_POSTGRES_TLS_PROFILE     required explicitly: supabase | custom-ca (with SESSION_POSTGRES_CA_FILE /
 *                                    SESSION_POSTGRES_CA_SHA256); no implicit default
 *   LOCKER_BUCKET (+ LOCKER_ENDPOINT, LOCKER_REGION, LOCKER_ACCESS_KEY_ID / LOCKER_SECRET_ACCESS_KEY or
 *     EKS Pod Identity, or LOCKER_PROVIDER=gcs)   the object store, exactly as every other self-host
 *                                    service builds it (src/shared/locker.ts)
 *   INGEST_KEY_ROOT                  object key root, default "96bulls" (same as the Worker)
 *   INGEST_MAX_PER_USER / INGEST_MAX_PER_ORG   in-flight batch slots, defaults 4 / 32 (same as the Worker). With
 *                                    INGEST_LEDGER=s3 they bound nothing unless INGEST_S3_TENANT_LIMIT=on
 *   RELAYHISTORY_INGEST_PG_POOL_MAX  pool size, 1..32, default 4
 *   INGEST_CLAIM_LOCK_BUSY           unset/off (default) | on (any case, trimmed; the Worker's routingFlag, so any
 *                                    other value is off, as on the Worker): a claim that loses its lock wait
 *                                    (55P03/40P01) is a 429 delivery_busy with Retry-After 5-10 s instead of a 503
 *                                    (deliver.ts ClaimLockBusy); 57014 and every other cause stay 503. HTTP only:
 *                                    the async/drain admission consumer keeps its normal backoff (admission-consumer.ts)
 *
 *   SELFHOST_INGEST_ADMISSION        unset | "" | "sync" (default: today's synchronous path, unchanged) |
 *                                    "async" | "drain" (selfhost-admission.ts). Config only: no request
 *                                    header can select it. async: the upload is ACKed after one create-only
 *                                    inbox PUT, with no SQL on the request path, and an in-process consumer
 *                                    admits it. drain: the synchronous path again, while the consumer keeps
 *                                    admitting what is already in the inbox (the rollback step)
 *   With async or drain only:
 *   SELFHOST_ADMISSION_CONCURRENCY   admissions in flight in this process, 1..32, default 4
 *   SELFHOST_ADMISSION_SCAN_MS       the inbox ListBucket scan interval, 1000..600000, default 15000
 *   SELFHOST_ADMISSION_MAX_ATTEMPTS  attempts before an inbox object is dead-lettered, 1..100, default 20
 *   SELFHOST_ADMISSION_AWAIT_AUTHORITY  on | off (default off). on: an org with no fence-authority marker yet (or the
 *                                    deployment-wide stop) is a deferral, never dead-lettered (selfhost-admission.ts
 *                                    AWAIT_AUTHORITY_CODES); off: an ordinary retry, unchanged
 *
 *   HISTORY_UPLOAD_DIRECT            unset | "" | "off" (default) | "allowlist" | "on" (upload-admission.ts,
 *                                    relay-upload-inbox-v1): the in-process scan of <keyRoot>/.upload-inbox/
 *                                    promotes desktop-written objects into the admission inbox, and
 *                                    GET /v1/delivery/admission?uploadKey= answers. Needs async or drain
 *                                    admission and an INGEST_KEY_ROOT of the contract's form ([a-z0-9][a-z0-9-]{0,62})
 *   With allowlist only:
 *   HISTORY_UPLOAD_DIRECT_ALLOWLIST  comma-separated org:<orgId> / ws:<workspaceId>, at least one
 *   With allowlist or on only:
 *   UPLOAD_WITNESS_KEY               the witness MAC key (32 bytes, unpadded base64url; Secret-injected). Never
 *                                    defaulted; missing or malformed leaves every upload pending (retried), never refused
 *   SELFHOST_UPLOAD_SCAN_BATCH       upload objects promoted per scan, 1..2000, default 256 (the scan reuses
 *                                    SELFHOST_ADMISSION_SCAN_MS), chosen fairly across uploading origins, oldest first
 *   SELFHOST_UPLOAD_PROMOTE_CONCURRENCY  promotions in flight at once within a scan, 1..64, default 8
 *   HISTORY_UPLOAD_INBOX_SSE         unset | "" | "none" (default: no requirement, e.g. MinIO without KMS) | "aws:kms":
 *                                    refuse (upload_unencrypted, terminal, before the witness check) an upload object
 *                                    whose own HEAD/GET lacks x-amz-server-side-encryption: aws:kms. Any other value
 *                                    refuses startup, even with HISTORY_UPLOAD_DIRECT off
 *
 *   ANALYTICS_SINK                   unset | "" | "noop" (default: no analytics) | "collector"; "posthog" and any
 *                                    ANALYTICS_POSTHOG_* value refuse startup (src/shared/analytics-config.ts).
 *                                    collector needs ANALYTICS_DEPLOYMENT_ID, ANALYTICS_COLLECTOR_URL (the full
 *                                    in-cluster URL ending in /v1/events) and ANALYTICS_COLLECTOR_TOKEN (Secret-injected)
 *
 * Refused when enabled (they describe hosted-only edges this process does not have):
 * INGEST_ACCEPTANCE_MODE other than canonical, INGEST_INLINE_MAX_SESSIONS (no fan-out queue: every
 * batch is written inline, see selfhost-server.ts), INGEST_REPLICA_MODE other than off,
 * INGEST_CUSTODY_SOURCE other than relayhistory/unset, INGEST_PRIVACY_AUTHORITY other than neon/unset,
 * INGEST_SIGNING_KEY (the relayhistory HMAC is never accepted here).
 *
 *   INGEST_LEDGER                    unset | "" | "sql" (default: ledger-sql.ts, unchanged) | "s3" (decision 0005:
 *                                    every admission, the consumer and the selection / reset fences through
 *                                    ledger-s3.ts, no SQL for upload accounting). s3 refuses the per-session erase
 *                                    route, the operator erase runtime (except as INGEST_S3_ERASE_FENCE=on allows)
 *                                    and INGEST_UPLOAD_CONSENT other than off.
 *                                    Until the read side moves, s3 controls also write SQL upload_deletions (S3
 *                                    first, reconciled to the fence; ledger-s3.ts withSqlControlMirror). No
 *                                    admission on ANY path (sync, async, direct upload) until the fence-authority
 *                                    marker exists (FenceAuthorityDoc).
 *   INGEST_S3_TENANT_LIMIT           unset | "" | "off" (default: no tenant limit on the S3 ledger, unchanged) |
 *                                    exactly "on" (anything else refuses startup). Needs INGEST_LEDGER=s3. An
 *                                    in-memory limit PER REPLICA (tenant-limiter.ts, no SQL): the synchronous path
 *                                    takes INGEST_MAX_PER_USER / _ORG slots (429 delivery_busy at the cap); the
 *                                    async consumer has its own pool, INGEST_MAX_PER_USER per user and
 *                                    INGEST_ASYNC_MAX_PER_ORG per org, and defers (never dead-letters) at the cap.
 *   INGEST_S3_ERASE_FENCE            unset | "" | "off" (default: no tombstone guard or write, unchanged) | exactly "on"
 *                                    (anything else refuses startup). Needs INGEST_LEDGER=s3; refuses
 *                                    INGEST_LEDGER_LOG_COMMIT=on. Lifts ONLY the operator per-session erase refusal
 *                                    (SESSION_ERASE_API_ENABLED + SESSION_ERASE_FENCE_ENFORCED): the erase route writes
 *                                    create-only S3 tombstones after the SQL fence and before it answers (503 if it
 *                                    cannot), and every admission path refuses a tombstoned session 409
 *                                    operator_session_erased (s3-erase-fence.ts). Set it on the async processes too.
 *                                    Still refused on s3: SESSION_ERASE_BYTES_ENABLED, selection, consent, whole-user
 *                                    erase. Tombstones are never deleted. Residual: a claim in another process past
 *                                    its tombstone HEAD before the write can still land (as the SQL cross-process one).
 *   INGEST_LEDGER_BUCKET             unset | "" (default: the ledger lives in LOCKER_BUCKET, unchanged) | a bucket name other
 *                                    than LOCKER_BUCKET: the S3 ledger's own objects (ledger-store.ts: each user's ledger/
 *                                    and the ledger-home marker) live there; everything else, the fences included, stays
 *                                    in LOCKER_BUCKET. Same provider, endpoint and credentials as the Locker
 *                                    store. Needs INGEST_LEDGER=s3. Claims then also need the ledger-home marker in that
 *                                    bucket naming it (docs/ledger-relocation.md). Proven for conditional writes at startup.
 *   INGEST_LEDGER_REGION             the ledger bucket's signing region; default LOCKER_REGION. Only with INGEST_LEDGER_BUCKET.
 *   INGEST_LEDGER_ENDPOINT           the ledger bucket's endpoint origin; default LOCKER_ENDPOINT, but only when the regions
 *                                    match. The cross-region refusal applies only when LOCKER_ENDPOINT is set: then a
 *                                    different INGEST_LEDGER_REGION without INGEST_LEDGER_ENDPOINT refuses startup (it would
 *                                    send the ledger bucket's requests to the Locker region's endpoint); with neither set the
 *                                    store uses AWS S3 in INGEST_LEDGER_REGION. Accepts any http(s) bare origin, http:// to
 *                                    any host included, exactly as LOCKER_ENDPOINT does (locker.ts). Set without
 *                                    INGEST_LEDGER_BUCKET: refused.
 *   INGEST_LEDGER_HOME_GUARD         unset | "" | "off" (default: no marker read, byte-identical) | exactly "on": every
 *                                    claim's fence-authority read also reads the ledger-home marker
 *                                    (<keyRoot>/.ledger/fence-authority/.ledger-home.json) in LOCKER_BUCKET and refuses
 *                                    when it names another bucket (`ledger_home_moved`). Needs INGEST_LEDGER=s3. Implied
 *                                    by INGEST_LEDGER_BUCKET (then the marker must name that bucket). docs/ledger-relocation.md
 *                                    requires it on before a relocation starts.
 *   INGEST_ASYNC_MAX_PER_ORG         the async consumer's per-org cap with INGEST_S3_TENANT_LIMIT=on and
 *                                    SELFHOST_INGEST_ADMISSION=async|drain: 1..SELFHOST_ADMISSION_CONCURRENCY,
 *                                    default max(1, floor(concurrency / 2)), so one org never holds more than half
 *                                    the consumer. Set without the flag (or without async admission): refused.
 */
import { UPLOAD_CONSENT_VAR, uploadConsentEnforcement, type UploadConsentEnforcement } from "./upload-consent.ts";
import { INGEST_LEDGER_VAR, ingestLedgerMode } from "./ledger-s3.ts";
import { LEDGER_HOME_GUARD_VAR, ledgerHomeGuard } from "./ledger-store.ts";
import { INGEST_LEDGER_LOG_COMMIT_VAR, ledgerLogCommitMode } from "./ledger-log.ts";
import { S3_ERASE_FENCE_VAR, s3EraseFenceMode } from "./s3-erase-fence.ts";
import type { CapturePolicyUploadsMode } from "./capture-policy.ts";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import { normalizedAudience } from "@rhc/auth/relaycontrol.ts";
import { postgresConnectionString, postgresTlsOptions } from "../shared/postgres-tls.ts";
import type pg from "pg";
import { sessionEraseConfig, type SessionEraseConfig } from "./session-erase.ts";
import { analyticsConfig, type AnalyticsConfig } from "../shared/analytics-config.ts";
// Already in the self-host graph through deliver.ts; the same parser as the Worker's INGEST_CLAIM_LOCK_BUSY.
import { routingFlag } from "./babysitter-retention.ts";
import { eraseRuntimeSettings } from "../session-erase/flags.ts";
import { UPLOAD_DIRECT_FLAG, UPLOAD_WITNESS_KEY_VAR, uploadDirectConfig, type UploadDirectConfig } from "./upload-admission.ts";

export const SELFHOST_INGEST_FLAG = "RELAYHISTORY_SELFHOST_INGEST";
export const SELFHOST_INGEST_PORT_VAR = "RELAYHISTORY_INGEST_PORT";
export const SELFHOST_SESSION_SELECTION_FLAG = "RELAYHISTORY_SELFHOST_SESSION_SELECTION";
export const SELFHOST_HEARTBEAT_ACCEPT_FLAG = "RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT";
export const SELFHOST_INTROSPECT_POSITIVE_TTL_VAR = "RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS";
/** The largest positive introspection TTL accepted; relaycontrol.ts also caps at this. */
export const SELFHOST_INTROSPECT_POSITIVE_TTL_MAX_MS = 60_000;
export const SELFHOST_INGEST_ORIGIN_VAR = "RELAYHISTORY_INGEST_ORIGIN";
export const SELFHOST_INGEST_DATABASE_URL_VAR = "RELAYHISTORY_INGEST_DATABASE_URL";
export const SELFHOST_INGEST_POOL_MAX_VAR = "RELAYHISTORY_INGEST_PG_POOL_MAX";
export const SELFHOST_INGEST_DEFAULT_PORT = 8081;
/** The only login the ledger's SECURITY DEFINER functions admit (supabase/bootstrap/session_ingest_role.sql). */
export const SELFHOST_INGEST_ROLE = "bulls96_session_ingest";

export const SELFHOST_INGEST_ADMISSION_VAR = "SELFHOST_INGEST_ADMISSION";
export const SELFHOST_CLAIM_LOCK_BUSY_VAR = "INGEST_CLAIM_LOCK_BUSY";

/** SELFHOST_INGEST_ADMISSION=async|drain; absent for sync (the default). */
export interface SelfhostAdmissionSettings {
  mode: "async" | "drain";
  concurrency: number;
  scanIntervalMs: number;
  maxAttempts: number;
  /** SELFHOST_ADMISSION_AWAIT_AUTHORITY=on only; absent when off (byte-identical to before). */
  awaitAuthority?: true;
}

export interface SelfhostIngestSettings {
  port: number;
  /** Normalized origin (scheme + host + non-default port). */
  origin: string;
  introspectUrl: string;
  introspectSecret: string;
  /** Discrete pg fields; no URL query string ever reaches pg. */
  pool: pg.PoolConfig;
  keyRoot: string;
  maxPerUser: number;
  maxPerOrg: number;
  /** RELAYHISTORY_SELFHOST_SESSION_SELECTION: serve the per-session erase route. Default off. */
  sessionSelection: boolean;
  /** RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT: answer the Desktop's onboarding heartbeat 204 (discarded). Default off. */
  heartbeatAccept: boolean;
  /** RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS: the capture policy upload check (capture-policy.ts). Default report. */
  capturePolicyUploads: CapturePolicyUploadsMode;
  /** INGEST_UPLOAD_CONSENT (upload-consent.ts): off (default) | report | hold-only | enforce. */
  uploadConsent: UploadConsentEnforcement;
  /** RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS: positive introspection cache, ms. 0 (default) = none. */
  introspectPositiveTtlMs: number;
  /** SESSION_ERASE_API_ENABLED (session-erase.ts). Default: disabled, the route answers 501. */
  sessionErase: SessionEraseConfig;
  eraseFenceEnforced?: true;
  eraseBytesEnabled?: true;
  /** Present only for SELFHOST_INGEST_ADMISSION=async|drain. */
  admission?: SelfhostAdmissionSettings;
  /** INGEST_LEDGER=s3 (decision 0005): every admission and fence through ledger-s3.ts. Absent: the SQL ledger. */
  ledger?: "s3";
  /** INGEST_LEDGER_LOG_COMMIT, parsed once here (selfhostLedgerLogCommit; on only with ledger s3). */
  ledgerLogCommit?: "off" | "on";
  /** HISTORY_UPLOAD_DIRECT allowlist|on (upload-admission.ts). Absent: off, nothing reads the upload prefix. */
  uploadDirect?: SelfhostUploadDirectSettings;
  /** ANALYTICS_* (src/shared/analytics-config.ts, self-host profile: noop or collector). Absent = noop. */
  analytics?: AnalyticsConfig;
  /** INGEST_CLAIM_LOCK_BUSY=on: a claim lock-wait failure is a 429 with Retry-After. Absent (the default): 503. */
  claimLockBusy?: true;
  /** INGEST_S3_TENANT_LIMIT=on (INGEST_LEDGER=s3 only). Absent (the default): no tenant limit on the S3 ledger. */
  s3TenantLimit?: SelfhostS3TenantLimitSettings;
  /** INGEST_LEDGER_BUCKET (+ INGEST_LEDGER_REGION), INGEST_LEDGER=s3 only. Absent (the default): the ledger is in LOCKER_BUCKET. */
  ledgerStore?: SelfhostLedgerStoreSettings;
  /** INGEST_LEDGER_HOME_GUARD=on (INGEST_LEDGER=s3 only). Absent (the default): no ledger-home read without ledgerStore. */
  ledgerHomeGuard?: true;
  /** INGEST_S3_ERASE_FENCE=on (INGEST_LEDGER=s3 only, s3-erase-fence.ts). Absent (the default): no tombstone guard or write. */
  s3EraseFence?: true;
}

/** The S3 ledger's own bucket (ledger-store.ts). `region`: INGEST_LEDGER_REGION, else LOCKER_REGION, else us-east-1. */
export interface SelfhostLedgerStoreSettings { bucket: string; region: string; endpoint?: string }
export const INGEST_LEDGER_BUCKET_VAR = "INGEST_LEDGER_BUCKET";
export const INGEST_LEDGER_REGION_VAR = "INGEST_LEDGER_REGION";

/** INGEST_LEDGER_BUCKET / INGEST_LEDGER_REGION. Errors name the variable, never the value. */
export function selfhostLedgerStore(env: NodeJS.ProcessEnv, ledger: "s3" | undefined): SelfhostLedgerStoreSettings | undefined {
  const bucket = env[INGEST_LEDGER_BUCKET_VAR]?.trim() ?? "";
  const regionRaw = env[INGEST_LEDGER_REGION_VAR]?.trim() ?? "";
  if (!bucket) {
    if (regionRaw) throw new Error(`96bulls: ${INGEST_LEDGER_REGION_VAR} needs ${INGEST_LEDGER_BUCKET_VAR}`);
    if (env.INGEST_LEDGER_ENDPOINT?.trim()) throw new Error(`96bulls: INGEST_LEDGER_ENDPOINT needs ${INGEST_LEDGER_BUCKET_VAR}`);
    return undefined;
  }
  if (ledger !== "s3") throw new Error(`96bulls: ${INGEST_LEDGER_BUCKET_VAR} needs ${INGEST_LEDGER_VAR}=s3`);
  if (!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket)) throw new Error(`96bulls: ${INGEST_LEDGER_BUCKET_VAR} must be a bucket name`);
  if (bucket === env.LOCKER_BUCKET?.trim()) throw new Error(`96bulls: ${INGEST_LEDGER_BUCKET_VAR} must differ from LOCKER_BUCKET (unset it to keep the ledger there)`);
  const lockerRegion = env.LOCKER_REGION?.trim() || "us-east-1";
  const region = regionRaw || lockerRegion;
  if (!/^[a-z0-9-]{1,63}$/.test(region)) throw new Error(`96bulls: ${INGEST_LEDGER_REGION_VAR} must be a region name`);
  const own = env.INGEST_LEDGER_ENDPOINT?.trim();
  const locker = env.LOCKER_ENDPOINT?.trim();
  if (!own && locker && region !== lockerRegion)
    throw new Error(`96bulls: LOCKER_ENDPOINT is set and ${INGEST_LEDGER_REGION_VAR} differs from LOCKER_REGION: set INGEST_LEDGER_ENDPOINT`);
  const endpoint = own || locker;
  if (endpoint && !/^https?:\/\/[^/?#\s]+$/.test(endpoint)) throw new Error("96bulls: INGEST_LEDGER_ENDPOINT must be a bare origin");
  return { bucket, region, ...(endpoint ? { endpoint } : {}) };
}

/** INGEST_LEDGER_HOME_GUARD (ledger-store.ts): on only with INGEST_LEDGER=s3. */
export function selfhostLedgerHomeGuard(env: NodeJS.ProcessEnv, ledger: "s3" | undefined): boolean {
  let on: boolean;
  try { on = ledgerHomeGuard(env); } catch { throw new Error(`96bulls: ${LEDGER_HOME_GUARD_VAR} must be on or off`); }
  if (on && ledger !== "s3") throw new Error(`96bulls: ${LEDGER_HOME_GUARD_VAR}=on needs ${INGEST_LEDGER_VAR}=s3`);
  return on;
}

/** INGEST_S3_TENANT_LIMIT=on. `asyncMaxPerOrg` (INGEST_ASYNC_MAX_PER_ORG): present only with async/drain admission. */
export interface SelfhostS3TenantLimitSettings {
  asyncMaxPerOrg?: number;
}

export const S3_TENANT_LIMIT_VAR = "INGEST_S3_TENANT_LIMIT";
export const ASYNC_MAX_PER_ORG_VAR = "INGEST_ASYNC_MAX_PER_ORG";

/** The async consumer's default per-org cap: half the consumer, at least one. */
export const defaultAsyncMaxPerOrg = (concurrency: number) => Math.max(1, Math.floor(concurrency / 2));

/**
 * INGEST_S3_TENANT_LIMIT: unset, "" or "off" is off (and INGEST_ASYNC_MAX_PER_ORG must be unset); exactly "on" is on;
 * anything else refuses startup. On needs INGEST_LEDGER=s3. Errors name the variable, never the value.
 */
export function selfhostS3TenantLimit(env: NodeJS.ProcessEnv, ledger: "s3" | undefined, admission: SelfhostAdmissionSettings | undefined): SelfhostS3TenantLimitSettings | undefined {
  const raw = env[S3_TENANT_LIMIT_VAR];
  const asyncSet = (env[ASYNC_MAX_PER_ORG_VAR]?.trim() ?? "") !== "";
  if (raw === undefined || raw === "" || raw === "off") {
    if (asyncSet) throw new Error(`96bulls: ${ASYNC_MAX_PER_ORG_VAR} needs ${S3_TENANT_LIMIT_VAR}=on`);
    return undefined;
  }
  if (raw !== "on") throw new Error(`96bulls: ${S3_TENANT_LIMIT_VAR} must be off or on`);
  if (ledger !== "s3") throw new Error(`96bulls: ${S3_TENANT_LIMIT_VAR}=on needs ${INGEST_LEDGER_VAR}=s3`);
  if (!admission) {
    if (asyncSet) throw new Error(`96bulls: ${ASYNC_MAX_PER_ORG_VAR} needs ${SELFHOST_INGEST_ADMISSION_VAR}=async or drain`);
    return {};
  }
  return { asyncMaxPerOrg: bounded(env, ASYNC_MAX_PER_ORG_VAR, defaultAsyncMaxPerOrg(admission.concurrency), 1, admission.concurrency) };
}

/** HISTORY_UPLOAD_DIRECT=allowlist|on. `witnessKey` is passed through raw: promoteUpload treats missing or malformed as retry. */
export interface SelfhostUploadDirectSettings {
  config: UploadDirectConfig;
  witnessKey: string | undefined;
  scanBatch: number;
  promoteConcurrency: number;
}

/** HISTORY_UPLOAD_DIRECT: absent when off. On needs async/drain admission and a contract-form key root. */
export function selfhostUploadDirectSettings(env: NodeJS.ProcessEnv, keyRoot: string, admission: SelfhostAdmissionSettings | undefined): SelfhostUploadDirectSettings | undefined {
  const config = uploadDirectConfig(env);
  if (config.mode === "off") return undefined;
  if (!admission) throw new Error(`96bulls: ${UPLOAD_DIRECT_FLAG} needs ${SELFHOST_INGEST_ADMISSION_VAR}=async or drain`);
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/u.test(keyRoot)) throw new Error(`96bulls: ${UPLOAD_DIRECT_FLAG} needs INGEST_KEY_ROOT of the form [a-z0-9][a-z0-9-]{0,62}`);
  return { config, witnessKey: env[UPLOAD_WITNESS_KEY_VAR], scanBatch: bounded(env, "SELFHOST_UPLOAD_SCAN_BATCH", 256, 1, 2000),
    promoteConcurrency: bounded(env, "SELFHOST_UPLOAD_PROMOTE_CONCURRENCY", 8, 1, 64) };
}

export type SelfhostIngestConfig = { enabled: false; port: number } | ({ enabled: true } & SelfhostIngestSettings);

/** Exactly "true" enables; unset, empty or "false" is off; anything else throws. */
export function selfhostIngestEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env[SELFHOST_INGEST_FLAG];
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw === "true") return true;
  throw new Error(`96bulls: ${SELFHOST_INGEST_FLAG} must be true or false`);
}

/** Exactly "true" enables; unset, empty or "false" is off; anything else throws (a typo never silently means either). */
export function selfhostSessionSelectionEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env[SELFHOST_SESSION_SELECTION_FLAG];
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw === "true") return true;
  throw new Error(`96bulls: ${SELFHOST_SESSION_SELECTION_FLAG} must be true or false`);
}

export const SELFHOST_CAPTURE_POLICY_UPLOADS_VAR = "RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS";

/** Unset, empty or "report" is report (the default); "enforce" enforces; anything else throws. */
export function selfhostCapturePolicyUploads(env: NodeJS.ProcessEnv = process.env): CapturePolicyUploadsMode {
  const raw = env[SELFHOST_CAPTURE_POLICY_UPLOADS_VAR];
  if (raw === undefined || raw === "" || raw === "report") return "report";
  if (raw === "enforce") return "enforce";
  throw new Error(`96bulls: ${SELFHOST_CAPTURE_POLICY_UPLOADS_VAR} must be report or enforce`);
}

/** Exactly "true" enables; unset, empty or "false" is off; anything else throws (a typo never silently means either). */
export function selfhostHeartbeatAcceptEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env[SELFHOST_HEARTBEAT_ACCEPT_FLAG];
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw === "true") return true;
  throw new Error(`96bulls: ${SELFHOST_HEARTBEAT_ACCEPT_FLAG} must be true or false`);
}

/**
 * RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS: unset, "" or "0" is 0 (no positive cache, today's
 * behaviour); otherwise exactly a decimal integer up to 60000 (the read API's rule: no sign, spaces,
 * exponent or leading zero). Anything else throws, a too-large value included (never clamped). Never echoes the value.
 */
export function selfhostIntrospectPositiveTtlMs(env: NodeJS.ProcessEnv = process.env): number {
  const raw = env[SELFHOST_INTROSPECT_POSITIVE_TTL_VAR];
  if (raw === undefined || raw === "") return 0;
  const value = /^(0|[1-9][0-9]{0,4})$/.test(raw) ? Number(raw) : NaN;
  if (!Number.isSafeInteger(value) || value < 0 || value > SELFHOST_INTROSPECT_POSITIVE_TTL_MAX_MS)
    throw new Error(`96bulls: ${SELFHOST_INTROSPECT_POSITIVE_TTL_VAR} must be unset or an integer from 0 to ${SELFHOST_INTROSPECT_POSITIVE_TTL_MAX_MS}`);
  return value;
}

function port(env: NodeJS.ProcessEnv): number {
  const raw = env[SELFHOST_INGEST_PORT_VAR]?.trim();
  if (!raw) return SELFHOST_INGEST_DEFAULT_PORT;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1 || value > 65_535) throw new Error(`96bulls: ${SELFHOST_INGEST_PORT_VAR} must be a TCP port`);
  return value;
}

function bounded(env: NodeJS.ProcessEnv, name: string, fallback: number, lo: number, hi: number): number {
  const raw = env[name]?.trim();
  if (!raw) return fallback;
  const n = Number(raw);
  if (!Number.isSafeInteger(n) || n < lo || n > hi) throw new Error(`96bulls: ${name} must be an integer from ${lo} to ${hi}`);
  return n;
}

function need(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name]?.trim();
  if (!value) throw new Error(`96bulls: ${SELFHOST_INGEST_FLAG}=true needs ${name}`);
  return value;
}

/** SELFHOST_INGEST_ADMISSION: exactly sync (or unset / empty), async or drain; anything else refuses startup. */
export function selfhostAdmissionSettings(env: NodeJS.ProcessEnv): SelfhostAdmissionSettings | undefined {
  const raw = env[SELFHOST_INGEST_ADMISSION_VAR];
  if (raw === undefined || raw === "" || raw === "sync") return undefined;
  if (raw !== "async" && raw !== "drain") throw new Error(`96bulls: ${SELFHOST_INGEST_ADMISSION_VAR} must be sync, async or drain`);
  return {
    mode: raw,
    concurrency: bounded(env, "SELFHOST_ADMISSION_CONCURRENCY", 4, 1, 32),
    scanIntervalMs: bounded(env, "SELFHOST_ADMISSION_SCAN_MS", 15_000, 1_000, 600_000),
    maxAttempts: bounded(env, "SELFHOST_ADMISSION_MAX_ATTEMPTS", 20, 1, 100),
    ...(awaitAuthorityFlag(env) ? { awaitAuthority: true as const } : {}),
  };
}

/** SELFHOST_ADMISSION_AWAIT_AUTHORITY: on or off (unset / empty = off); anything else refuses startup. */
function awaitAuthorityFlag(env: NodeJS.ProcessEnv): boolean {
  const raw = env.SELFHOST_ADMISSION_AWAIT_AUTHORITY;
  if (raw === undefined || raw === "" || raw === "off") return false;
  if (raw === "on") return true;
  throw new Error("96bulls: SELFHOST_ADMISSION_AWAIT_AUTHORITY must be on or off");
}

/** Discrete fields from the sanitized URL (the read API service's rule): user, password and database required. */
export function ingestPoolTarget(url: string): pg.PoolConfig {
  let target: URL;
  try {
    target = new URL(postgresConnectionString(url));
  } catch {
    throw new Error(`96bulls: ${SELFHOST_INGEST_DATABASE_URL_VAR} is invalid`);
  }
  let user: string, password: string, database: string;
  try {
    user = decodeURIComponent(target.username);
    password = decodeURIComponent(target.password);
    database = decodeURIComponent(target.pathname.replace(/^\//u, ""));
  } catch {
    throw new Error(`96bulls: ${SELFHOST_INGEST_DATABASE_URL_VAR} is invalid`);
  }
  if (!user || !password || !database || database.includes("/") || target.hash)
    throw new Error(`96bulls: ${SELFHOST_INGEST_DATABASE_URL_VAR} needs a user, password and database`);
  return { host: target.hostname.replace(/^\[(.*)\]$/u, "$1"), port: target.port ? Number(target.port) : 5432, user, password, database };
}

/** Hosted-only settings that must not silently do nothing here. */
function refuseHostedOnly(env: NodeJS.ProcessEnv): void {
  const set = (name: string) => (env[name]?.trim() ?? "") !== "";
  const refuse = (name: string, why: string) => {
    throw new Error(`96bulls: the self-host session ingest refuses ${name} (${why})`);
  };
  if (set("INGEST_ACCEPTANCE_MODE") && env.INGEST_ACCEPTANCE_MODE!.trim() !== "canonical") refuse("INGEST_ACCEPTANCE_MODE", "only canonical");
  if (set("INGEST_INLINE_MAX_SESSIONS")) refuse("INGEST_INLINE_MAX_SESSIONS", "every batch is written inline; there is no fan-out queue");
  if (set("INGEST_REPLICA_MODE") && env.INGEST_REPLICA_MODE!.trim() !== "off") refuse("INGEST_REPLICA_MODE", "no Neon replica");
  if (set("INGEST_CUSTODY_SOURCE") && env.INGEST_CUSTODY_SOURCE!.trim() !== "relayhistory") refuse("INGEST_CUSTODY_SOURCE", "no custody source");
  if (set("INGEST_PRIVACY_AUTHORITY") && env.INGEST_PRIVACY_AUTHORITY!.trim() !== "neon") refuse("INGEST_PRIVACY_AUTHORITY", "no custody source");
  if (set("INGEST_SIGNING_KEY")) refuse("INGEST_SIGNING_KEY", "the relayhistory HMAC identity is never accepted here");
}

/**
 * INGEST_LEDGER_LOG_COMMIT (ledger-log.ts, docs/design/s3-ledger-log-commit.md): off (unset, the default) is today.
 * `on` (INGEST_LEDGER=s3 only): the legacy claim and commit refuse a stream whose log mode marker exists
 * (ledger-s3.ts logModeGuard), and the async consumer admits such a stream through its log (ledger-log-admit.ts).
 * A stream enters log mode ONLY through its durable marker, which nothing here writes, so `on` alone changes no
 * stream's admission; it adds one parallel HEAD per legacy claim and commit. Until the materializer ships, a
 * log-mode stream's batches are not visible to readers (docs/design/s3-ledger-log-commit.md).
 */
export function selfhostLedgerLogCommit(env: NodeJS.ProcessEnv): "off" | "on" {
  try { return ledgerLogCommitMode(env[INGEST_LEDGER_LOG_COMMIT_VAR]); } catch { throw new Error(`96bulls: ${INGEST_LEDGER_LOG_COMMIT_VAR} must be off or on`); }
}

/**
 * INGEST_LEDGER (decision 0005): sql (unset, the default: unchanged) or s3. With s3 this process's ONE ledger is
 * ledger-s3.ts: the synchronous path, the async consumer, upload-inbox promotion and the selection / reset routes
 * all admit and fence through S3, and the database is never called for upload accounting. Refused with s3: the
 * features whose state is SQL-only (the per-session erase ledger, the operator erase runtime, upload consent).
 */
function selfhostLedger(env: NodeJS.ProcessEnv, s: { sessionSelection: boolean; sessionErase: { enabled: boolean }; uploadConsent: UploadConsentEnforcement; eraseRuntime: { eraseFenceEnforced?: true; eraseBytesEnabled?: true }; eraseFence: boolean }): "s3" | undefined {
  let mode;
  try { mode = ingestLedgerMode(env[INGEST_LEDGER_VAR]?.trim() === "" ? undefined : env[INGEST_LEDGER_VAR]?.trim()); } catch { throw new Error(`96bulls: ${INGEST_LEDGER_VAR} must be sql or s3`); }
  const logCommit = selfhostLedgerLogCommit(env);
  if (mode === "sql") {
    if (logCommit === "on") throw new Error(`96bulls: ${INGEST_LEDGER_LOG_COMMIT_VAR}=on needs ${INGEST_LEDGER_VAR}=s3`);
    if (s.eraseFence) throw new Error(`96bulls: ${S3_ERASE_FENCE_VAR}=on needs ${INGEST_LEDGER_VAR}=s3`);
    return undefined;
  }
  // A log append admits without deliver's session guard (ledger-log-admit.ts), so the fence would not hold there.
  if (s.eraseFence && logCommit === "on") throw new Error(`96bulls: ${S3_ERASE_FENCE_VAR}=on refuses ${INGEST_LEDGER_LOG_COMMIT_VAR}=on`);
  const refuse = (what: string) => { throw new Error(`96bulls: ${INGEST_LEDGER_VAR}=s3 refuses ${what} (its state is SQL-only)`); };
  if (s.sessionSelection) refuse("RELAYHISTORY_SELFHOST_SESSION_SELECTION");
  // INGEST_S3_ERASE_FENCE=on lifts the per-session erase route and its fence only (s3-erase-fence.ts); the bytes
  // worker stays refused (its staged-objects proof lists the admission inbox, which holds the S3 ledger).
  if (s.eraseRuntime.eraseBytesEnabled || !s.eraseFence && (s.sessionErase.enabled || s.eraseRuntime.eraseFenceEnforced)) refuse("the operator session erase");
  if (s.uploadConsent !== "off") refuse(UPLOAD_CONSENT_VAR);
  return "s3";
}

/** Parse everything once at startup. Errors name the variable, never its value. */
export function selfhostIngestConfig(env: NodeJS.ProcessEnv = process.env): SelfhostIngestConfig {
  if (!selfhostIngestEnabled(env)) return { enabled: false, port: port(env) };
  const sessionSelection = selfhostSessionSelectionEnabled(env);
  const heartbeatAccept = selfhostHeartbeatAcceptEnabled(env);
  const capturePolicyUploads = selfhostCapturePolicyUploads(env);
  const uploadConsent = uploadConsentEnforcement(env[UPLOAD_CONSENT_VAR]);
  const introspectPositiveTtlMs = selfhostIntrospectPositiveTtlMs(env);
  const sessionErase = sessionEraseConfig(env);
  const policy = destinationPolicy(env);
  refuseHostedOnly(env);
  const originRaw = need(env, SELFHOST_INGEST_ORIGIN_VAR);
  assertDestinationUrl(SELFHOST_INGEST_ORIGIN_VAR, originRaw, policy, { allowPath: false, allowAnyHttpHost: true });
  const origin = normalizedAudience(originRaw);
  if (!origin) throw new Error(`96bulls: ${SELFHOST_INGEST_ORIGIN_VAR} must be an absolute http(s) origin`);
  const introspectUrl = need(env, "RELAYCONTROL_INTROSPECT_URL");
  assertDestinationUrl("RELAYCONTROL_INTROSPECT_URL", introspectUrl, policy);
  const introspectSecret = need(env, "RELAYCONTROL_INTROSPECT_SECRET");
  // The self-host service never inherits the hosted Supabase pin by omission.
  if (!env.SESSION_POSTGRES_TLS_PROFILE) throw new Error(`96bulls: ${SELFHOST_INGEST_FLAG}=true needs SESSION_POSTGRES_TLS_PROFILE set explicitly`);
  const ssl = postgresTlsOptions(env);
  const target = ingestPoolTarget(need(env, SELFHOST_INGEST_DATABASE_URL_VAR));
  need(env, "LOCKER_BUCKET");
  const keyRoot = env.INGEST_KEY_ROOT?.trim() || "96bulls";
  if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/u.test(keyRoot)) throw new Error("96bulls: INGEST_KEY_ROOT must be a single key segment");
  const admission = selfhostAdmissionSettings(env);
  const eraseRuntime = eraseRuntimeSettings(env, sessionErase, admission);
  const uploadDirect = selfhostUploadDirectSettings(env, keyRoot, admission);
  let eraseFence: boolean;
  try { eraseFence = s3EraseFenceMode(env[S3_ERASE_FENCE_VAR]) === "on"; } catch { throw new Error(`96bulls: ${S3_ERASE_FENCE_VAR} must be off or on`); }
  const ledger = selfhostLedger(env, { sessionSelection, sessionErase, uploadConsent, eraseRuntime, eraseFence });
  const ledgerLogCommit = selfhostLedgerLogCommit(env);
  const analytics = analyticsConfig(env, "selfhost");
  const claimLockBusy = routingFlag(env[SELFHOST_CLAIM_LOCK_BUSY_VAR]);
  const s3TenantLimit = selfhostS3TenantLimit(env, ledger, admission);
  const ledgerStore = selfhostLedgerStore(env, ledger);
  const homeGuard = selfhostLedgerHomeGuard(env, ledger);
  return {
    enabled: true,
    port: port(env),
    origin,
    introspectUrl,
    introspectSecret,
    pool: { ...target, ssl, max: bounded(env, SELFHOST_INGEST_POOL_MAX_VAR, 4, 1, 32) },
    keyRoot,
    maxPerUser: bounded(env, "INGEST_MAX_PER_USER", 4, 1, 1000),
    maxPerOrg: bounded(env, "INGEST_MAX_PER_ORG", 32, 1, 1000),
    sessionSelection,
    heartbeatAccept,
    capturePolicyUploads,
    uploadConsent,
    introspectPositiveTtlMs,
    sessionErase,
    ...eraseRuntime,
    ...(admission ? { admission } : {}),
    ...(uploadDirect ? { uploadDirect } : {}),
    ...(analytics.sink !== "noop" ? { analytics } : {}),
    ...(claimLockBusy ? { claimLockBusy: true as const } : {}),
    ...(ledger ? { ledger, ledgerLogCommit } : {}),
    ...(eraseFence ? { s3EraseFence: true as const } : {}),
    ...(s3TenantLimit ? { s3TenantLimit } : {}),
    ...(ledgerStore ? { ledgerStore } : {}),
    ...(homeGuard ? { ledgerHomeGuard: true as const } : {}),
  };
}
