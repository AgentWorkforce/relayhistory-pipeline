/**
 * RelayAuth migration assertions (`POST /v1/tokens/migration-assertion`,
 * relayauth#110) at the ingest. RelayAuth records neither `jti` nor `nonce`,
 * so this service makes each assertion single-use: chunk requests may reuse it
 * (bounded, see below) until it is consumed, and the manifest commit consumes
 * it exactly once.
 *
 * Authorization comes from the signed assertion itself, never from echoed
 * request or manifest fields, and never from a membership lookup:
 *  - the signed `wks` equals `meta.workspaceId` and the signed `sponsorId`
 *    equals `meta.sourceUserId` (else 403 `migration_workspace_forbidden`);
 *  - the signed `meta.upload` (an `UploadIdentity` the issuer derives from
 *    the grant's stored plan; contract and vectors in 96BullSessions
 *    docs/contracts/relayhistory-export-plan-entry-v1.md) must name the
 *    signed grant, plan digest, org, workspace and user (canonical ids, no
 *    `relay_` alias), and the ingest RECOMPUTES the identity from the actual
 *    request: the key, machine, harness, session and date of the chunk,
 *    status or commit request; at the commit also the session, the chunk
 *    list, the content hash chain and the entry digest of the manifest. Any
 *    difference is 403 `migration_upload_forbidden`. A fresh key, another
 *    machine or session, or other content cannot match;
 *  - the grant and its plan must be one this ingest accepts
 *    (`RELAYHISTORY_MIGRATION_GRANTS`, else 403 `migration_grant_forbidden`).
 *
 * Chunks: at most one chunk per `(jti, seq)` (an identical repeat is
 * idempotent, any other chunk at that seq is 409 `migration_chunk_conflict`)
 * and at most `MAX_CHUNKS_PER_ASSERTION` chunks per assertion (403
 * `migration_chunk_limit`): 256 chunks of at most 4 MiB is 1 GiB, more than a
 * courier can send within an assertion's <= 65 second validity; the courier
 * re-mints for the rest of a session.
 *
 * What is atomic: the ledger row and the manifest commit's durable write, the
 * `object-landed` queue row, are written in one SQL transaction, so a crash
 * leaves both or neither. A Cloudflare Queue send cannot join a Postgres
 * transaction, so a deployment that enqueues to Cloudflare has no ledger and
 * refuses migration assertions (503 `migration_ledger_unavailable`).
 *
 * Retention, no cron: every ledger insert first deletes at most
 * `RETENTION_BATCH` rows whose `expires_at` (the token's `exp` plus the 5
 * second skew) passed more than `RETENTION_MARGIN_SECONDS` ago. The verifier
 * refuses an expired token before the ledger is ever consulted, so a purged
 * `jti` or nonce can never be accepted again.
 *
 * A retry after an ambiguous failure (the commit landed, the response was
 * lost) uses a fresh assertion and enqueues the same work key again; every
 * stage is idempotent by work key, so it is never applied twice.
 */
import { sha256Hex } from "../../../../object-store/src/crypto.js";
import { computeContentSha256 } from "../../../../object-store/src/manifest.js";
import type { AuthContext } from "../../env.js";
import type { StageMessage } from "./dlq.js";
import { manifestKey, type ManifestV1 } from "./object-layout.js";
import { postgresPipelineQueue, type SqlExecutor } from "./queue.js";
import type { PipelineStage } from "./trace.js";

/** What a verified migration assertion carries (`auth.migration`). */
export interface MigrationAssertionClaims {
  jti: string;
  nonce: string;
  grantId: string;
  planDigest: string;
  /** `meta.upload`: the signed identity of the one upload this assertion is for. */
  upload: UploadIdentity;
  /** `meta.sourceUserId`: the grant pair's user. */
  sourceUserId: string;
  /** `meta.workspaceId`: the grant pair's workspace. */
  workspaceId: string;
  /** Unix seconds after which no verifier accepts the token (`exp` plus skew). */
  expiresAt: number;
}

/** One consumption: the assertion plus the tenancy it was spent on. */
export interface MigrationAssertionUse {
  jti: string;
  nonce: string;
  grantId: string;
  planDigest: string;
  /** `meta.upload.entry_sha256`, stored as `plan_entry_digest`. */
  planEntryDigest: string;
  orgId: string;
  workspaceId: string;
  userId: string;
  /** Unix seconds; stored as `expires_at`, the earliest the row may be purged. */
  expiresAt: number;
}

/** One chunk write under an assertion (`sessions.migration_assertion_chunks`). */
export interface MigrationChunkUse {
  jti: string;
  seq: number;
  startOffset: number;
  /** SHA-256 of the chunk's source bytes (the chunk entry's `sha256`). */
  sha256: string;
  expiresAt: number;
}

/**
 * Whether a migration assertion may act on this target: the signed pair
 * (`wks`, `sponsorId`) equals the grant pair (`meta.workspaceId`,
 * `meta.sourceUserId`), and the target workspace and user (from the upload
 * key, the manifest and the resolved auth context) are that pair. RelayAuth
 * signs only pairs stored in the grant, so this is the whole authorization;
 * there is no membership lookup.
 */
export function migrationPairAuthorized(
  auth: Pick<AuthContext, "claims" | "userId" | "workspaceId" | "migration">,
  target: { workspaceIds?: readonly (string | undefined)[] } = {},
): boolean {
  const migration = auth.migration;
  if (!migration) return false;
  const wks = auth.claims.wks;
  const sponsorId = auth.claims.sponsorId;
  if (
    typeof wks !== "string" ||
    typeof sponsorId !== "string" ||
    normalizeRelayId(wks) !== normalizeRelayId(migration.workspaceId) ||
    normalizeRelayId(sponsorId) !== normalizeRelayId(migration.sourceUserId) ||
    !auth.workspaceId ||
    normalizeRelayId(auth.workspaceId) !== normalizeRelayId(wks) ||
    normalizeRelayId(auth.userId) !== normalizeRelayId(sponsorId)
  )
    return false;
  return (target.workspaceIds ?? []).every(
    (workspaceId) =>
      workspaceId !== undefined &&
      normalizeRelayId(workspaceId) === normalizeRelayId(wks),
  );
}

const GRANT_ID_SHAPE = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/;
const DIGEST_SHAPE = /^[0-9a-f]{64}$/;

/**
 * The grants this ingest accepts (`RELAYHISTORY_MIGRATION_GRANTS`:
 * comma-separated `grantId=planDigest`, copied from the grant records). A
 * malformed entry is ignored, so it can only refuse, never admit.
 */
export function migrationGrantsFromEnv(env: {
  RELAYHISTORY_MIGRATION_GRANTS?: string;
}): ReadonlyMap<string, string> {
  const grants = new Map<string, string>();
  for (const entry of (env.RELAYHISTORY_MIGRATION_GRANTS ?? "").split(",")) {
    const [grantId, digest, ...rest] = entry.trim().split("=");
    if (
      rest.length === 0 &&
      grantId &&
      digest &&
      GRANT_ID_SHAPE.test(grantId) &&
      DIGEST_SHAPE.test(digest)
    )
      grants.set(grantId, digest);
  }
  return grants;
}

/**
 * Whether the assertion's signed `meta.grantId` is a grant this ingest
 * accepts and its signed `meta.planDigest` is that grant's plan.
 */
export function migrationGrantAllowed(
  env: { RELAYHISTORY_MIGRATION_GRANTS?: string },
  migration: Pick<MigrationAssertionClaims, "grantId" | "planDigest">,
): boolean {
  return (
    migrationGrantsFromEnv(env).get(migration.grantId) === migration.planDigest
  );
}

// --- The upload identity and plan entry (contract v1) -------------------
//
// docs/contracts/relayhistory-export-plan-entry-v1.md in 96BullSessions, with
// cross-repo vectors (test/fixtures/relayhistory-export-plan-entry-v1.vectors.json).
// The shapes are those of the #40 courier (derivePlanContract).

/** The session identity in an entry and in the signed upload identity. */
export interface UploadSession {
  machine_id: string;
  harness: string;
  session_id: string;
  started_at: string;
}

/** One chunk as a plan entry lists it (all known before upload). */
export interface PlanEntryChunk {
  seq: number;
  epoch: number;
  start_offset: number;
  bytes: number;
  /** SHA-256 of the chunk's source bytes (the chunk entry's `sha256`). */
  sha256: string;
  records: number;
}

/** A canonical, grant-bound plan entry: one manifest upload. */
export interface PlanEntry {
  grant_id: string;
  /** The v1 manifest key without any deployment prefix. */
  key: string;
  org_id: string;
  workspace_id: string;
  source_user_id: string;
  session: UploadSession;
  state: "open" | "closed";
  chunks: PlanEntryChunk[];
  /** The manifest v1 `content_sha256` hash chain over `chunks`. */
  content_sha256: string;
  /** Sum of the chunks' `bytes`. */
  raw_bytes: number;
}

/**
 * The per-upload identity the issuer derives from the stored plan and signs
 * (`meta.upload`). Ids are in canonical form (no `relay_` prefix).
 */
export interface UploadIdentity {
  grant_id: string;
  /** The grant's plan digest (`exportPlanDigest`); equals the signed `meta.planDigest`. */
  plan_digest: string;
  /** The manifest key (the work key's object); the unprefixed v1 key. */
  key: string;
  org_id: string;
  workspace_id: string;
  source_user_id: string;
  session: UploadSession;
  content_sha256: string;
  /** `sha256(canonical(entry))`, lowercase hex. */
  entry_sha256: string;
}

/**
 * One id normalization everywhere: a leading `relay_` is not part of the id
 * (as for every RelayAuth token's user, org and workspace).
 */
export function normalizeRelayId(value: string): string {
  return value.startsWith("relay_") ? value.slice("relay_".length) : value;
}

/**
 * Canonical JSON: object keys sorted (UTF-16 code unit order, as
 * `Array.prototype.sort`), undefined members dropped, no whitespace, every
 * other value as `JSON.stringify` writes it.
 */
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .filter((key) => record[key] !== undefined)
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

/** `sha256(canonical(entry))`, lowercase hex. */
export function planEntrySha256(entry: PlanEntry): Promise<string> {
  return sha256Hex(canonicalJson(entry));
}

/**
 * A grant's plan digest (`meta.planDigest`, `RELAYHISTORY_MIGRATION_GRANTS`):
 * `sha256(canonical({v: 1, grant_id, exported_at, manifests}))` with the
 * entries sorted by key.
 */
export function exportPlanDigest(input: {
  grantId: string;
  exportedAt: string;
  entries: readonly PlanEntry[];
}): Promise<string> {
  const manifests = [...input.entries].sort((a, b) =>
    a.key < b.key ? -1 : a.key > b.key ? 1 : 0,
  );
  return sha256Hex(
    canonicalJson({
      v: 1,
      grant_id: input.grantId,
      exported_at: input.exportedAt,
      manifests,
    }),
  );
}

/** The upload identity of an entry under its grant's plan digest. */
export async function uploadIdentityOf(
  entry: PlanEntry,
  planDigest: string,
): Promise<UploadIdentity> {
  return {
    grant_id: entry.grant_id,
    plan_digest: planDigest,
    key: entry.key,
    org_id: entry.org_id,
    workspace_id: entry.workspace_id,
    source_user_id: entry.source_user_id,
    session: entry.session,
    content_sha256: entry.content_sha256,
    entry_sha256: await planEntrySha256(entry),
  };
}

/** The unprefixed v1 manifest key of a session location. */
export function sessionManifestKey(location: {
  workspaceId: string;
  machineId: string;
  harness: string;
  sessionId: string;
  startDate: string;
}): string {
  return manifestKey({
    workspaceId: location.workspaceId,
    machineId: location.machineId,
    harness: location.harness,
    sessionId: location.sessionId,
    startDate: location.startDate,
  });
}

/** The tenancy of an upload, from the signed claims only, normalized. */
function signedTenancy(
  auth: Pick<AuthContext, "claims" | "migration">,
):
  | Pick<
      UploadIdentity,
      "grant_id" | "org_id" | "workspace_id" | "source_user_id"
    >
  | undefined {
  const { org, wks, sponsorId } = auth.claims;
  if (
    !auth.migration ||
    typeof org !== "string" ||
    typeof wks !== "string" ||
    typeof sponsorId !== "string"
  )
    return undefined;
  return {
    grant_id: auth.migration.grantId,
    org_id: normalizeRelayId(org),
    workspace_id: normalizeRelayId(wks),
    source_user_id: normalizeRelayId(sponsorId),
  };
}

/**
 * The plan entry of an actual upload, DERIVED by the ingest: tenancy from the
 * signed claims, key from the upload's key, session and chunk list from the
 * validated manifest, content recomputed over the chunk list. Nothing echoed.
 * `undefined` when the manifest's declared summary disagrees with its chunks.
 */
export async function derivePlanEntry(
  auth: Pick<AuthContext, "claims" | "migration">,
  location: Parameters<typeof sessionManifestKey>[0],
  manifest: Pick<ManifestV1, "session" | "raw">,
): Promise<PlanEntry | undefined> {
  const tenancy = signedTenancy(auth);
  if (!tenancy) return undefined;
  const raw = manifest.raw;
  const content = await computeContentSha256(raw.chunks);
  if (content !== raw.content_sha256 || raw.chunks.length !== raw.chunk_count)
    return undefined;
  return {
    ...tenancy,
    key: sessionManifestKey(location),
    session: {
      machine_id: manifest.session.machine_id,
      harness: manifest.session.harness,
      session_id: manifest.session.session_id,
      started_at: manifest.session.started_at,
    },
    state: raw.state,
    chunks: raw.chunks.map((chunk) => ({
      seq: chunk.seq,
      epoch: chunk.epoch,
      start_offset: chunk.start_offset,
      bytes: chunk.bytes,
      sha256: chunk.sha256,
      records: chunk.records ?? 0,
    })),
    content_sha256: content,
    raw_bytes: raw.chunks.reduce((sum, chunk) => sum + chunk.bytes, 0),
  };
}

/** The signed identity names exactly the signed tenancy and plan (canonical ids). */
function sameTenancy(
  auth: Pick<AuthContext, "migration">,
  upload: UploadIdentity,
  tenancy: Pick<
    UploadIdentity,
    "grant_id" | "org_id" | "workspace_id" | "source_user_id"
  >,
): boolean {
  return (
    upload.grant_id === tenancy.grant_id &&
    upload.plan_digest === auth.migration?.planDigest &&
    upload.org_id === tenancy.org_id &&
    upload.workspace_id === tenancy.workspace_id &&
    upload.source_user_id === tenancy.source_user_id
  );
}

/**
 * Chunk and status requests: the request's session (key, machine, harness,
 * session id, date) and the signed tenancy must be the signed upload's.
 * This binds every key the token may write, fresh ones included, before the
 * first write.
 */
export function migrationSessionMatches(
  auth: Pick<AuthContext, "claims" | "migration">,
  location: Parameters<typeof sessionManifestKey>[0],
): boolean {
  const upload = auth.migration?.upload;
  const tenancy = signedTenancy(auth);
  if (!upload || !tenancy || !sameTenancy(auth, upload, tenancy)) return false;
  return (
    upload.key === sessionManifestKey(location) &&
    upload.session.machine_id === location.machineId &&
    upload.session.harness === location.harness &&
    upload.session.session_id === location.sessionId &&
    upload.session.started_at.slice(0, 10) === location.startDate
  );
}

/**
 * The manifest commit (and a status read of a stored manifest): the entry
 * derived from the actual upload must be the signed one, field by field and
 * by `entry_sha256`.
 */
export async function migrationUploadMatches(
  auth: Pick<AuthContext, "claims" | "migration">,
  location: Parameters<typeof sessionManifestKey>[0],
  manifest: Pick<ManifestV1, "session" | "raw">,
): Promise<boolean> {
  const upload = auth.migration?.upload;
  if (!upload || !migrationSessionMatches(auth, location)) return false;
  const entry = await derivePlanEntry(auth, location, manifest);
  if (!entry) return false;
  const derived = await uploadIdentityOf(entry, upload.plan_digest);
  return (
    canonicalJson(derived.session) === canonicalJson(upload.session) &&
    derived.content_sha256 === upload.content_sha256 &&
    derived.entry_sha256 === upload.entry_sha256
  );
}

/**
 * Parses a signed `meta.upload`; `undefined` when it is not exactly that
 * shape, or when an id is not in canonical form (a `relay_` alias).
 */
export function parseUploadIdentity(
  value: unknown,
): UploadIdentity | undefined {
  const record = (v: unknown) =>
    v && typeof v === "object" && !Array.isArray(v)
      ? (v as Record<string, unknown>)
      : undefined;
  const u = record(value);
  const session = record(u?.session);
  const text = (v: unknown, max = 512) =>
    typeof v === "string" && v.length > 0 && v.length <= max;
  if (
    !u ||
    !session ||
    !["grant_id", "key", "org_id", "workspace_id", "source_user_id"].every(
      (k) => text(u[k]),
    ) ||
    ["org_id", "workspace_id", "source_user_id"].some(
      (k) => normalizeRelayId(u[k] as string) !== u[k],
    ) ||
    typeof u.plan_digest !== "string" ||
    !DIGEST_SHAPE.test(u.plan_digest) ||
    !["machine_id", "harness", "session_id", "started_at"].every((k) =>
      text(session[k], 256),
    ) ||
    typeof u.content_sha256 !== "string" ||
    !DIGEST_SHAPE.test(u.content_sha256) ||
    typeof u.entry_sha256 !== "string" ||
    !DIGEST_SHAPE.test(u.entry_sha256)
  )
    return undefined;
  return {
    grant_id: u.grant_id as string,
    plan_digest: u.plan_digest,
    key: u.key as string,
    org_id: u.org_id as string,
    workspace_id: u.workspace_id as string,
    source_user_id: u.source_user_id as string,
    session: {
      machine_id: session.machine_id as string,
      harness: session.harness as string,
      session_id: session.session_id as string,
      started_at: session.started_at as string,
    },
    content_sha256: u.content_sha256,
    entry_sha256: u.entry_sha256,
  };
}

/** An executor that can run work in one transaction (a pg Pool client, PGlite). */
export interface TransactionalSqlExecutor extends SqlExecutor {
  transaction<T>(work: (tx: SqlExecutor) => Promise<T>): Promise<T>;
}

/**
 * Routes a migration assertion may call: the courier protocol's chunk hop,
 * manifest commit and manifest status read. Everything else refuses it
 * (including `POST /v1/pipeline/manifests/status`).
 */
const MIGRATION_ROUTES: readonly (readonly [string, string])[] = [
  ["POST", "/v1/pipeline/chunks"],
  ["POST", "/v1/pipeline/manifests"],
  ["GET", "/v1/pipeline/manifests/status"],
];

export function migrationRouteAllowed(method: string, path: string): boolean {
  const normalized = path.length > 1 ? path.replace(/\/+$/, "") : path;
  return MIGRATION_ROUTES.some(
    ([allowedMethod, allowedPath]) =>
      method.toUpperCase() === allowedMethod && normalized === allowedPath,
  );
}

// --- The single-use ledger ----------------------------------------------

/** Rows whose `expires_at` passed longer ago than this may be deleted. */
export const RETENTION_MARGIN_SECONDS = 300;
/** At most this many expired rows are deleted per ledger insert. */
export const RETENTION_BATCH = 100;

/**
 * `grant_mismatch`: the grant's earlier uses name another plan or org, so this
 * use is not of the grant it claims (nothing written).
 */
export type MigrationConsumeResult = "consumed" | "replayed" | "grant_mismatch";

/** At most this many chunks per assertion (see the module comment). */
export const MAX_CHUNKS_PER_ASSERTION = 256;

/**
 * `repeat`: this exact chunk was reserved before; `conflict`: another chunk
 * holds the seq; `limit`: the assertion already holds MAX_CHUNKS_PER_ASSERTION.
 */
export type MigrationChunkResult = "reserved" | "repeat" | "conflict" | "limit";

export interface MigrationAssertionLedger {
  /** Whether this `jti` or this `nonce` was already consumed. */
  consumed(
    claims: Pick<MigrationAssertionClaims, "jti" | "nonce">,
  ): Promise<boolean>;
  /**
   * Records one chunk write under an assertion: at most one chunk per
   * `(jti, seq)` and at most MAX_CHUNKS_PER_ASSERTION per `jti`.
   */
  reserveChunk(use: MigrationChunkUse): Promise<MigrationChunkResult>;
  /**
   * Consumes the assertion and enqueues the commit's message as one unit:
   * both are durable or neither is. `replayed` (nothing enqueued) when the
   * `jti` or the `nonce` was consumed before. Throws when the unit failed, in
   * which case nothing was consumed or enqueued.
   */
  consume(
    use: MigrationAssertionUse,
    stage: PipelineStage,
    message: StageMessage,
  ): Promise<MigrationConsumeResult>;
}

/**
 * The ledger over SQL: `sessions.migration_assertion_uses` (jti primary key,
 * nonce unique) and the queue row in one transaction, plus
 * `sessions.migration_assertion_chunks`. `enqueue` defaults to the Postgres
 * pipeline queue on the same transaction; a deployment that enriches
 * messages first passes its own, and must write through `tx`.
 */
export function sqlMigrationAssertionLedger(
  sql: TransactionalSqlExecutor,
  options: {
    schema?: string;
    enqueue?: (
      tx: SqlExecutor,
      stage: PipelineStage,
      message: StageMessage,
    ) => Promise<void>;
  } = {},
): MigrationAssertionLedger {
  const schema = options.schema ?? "sessions";
  if (!/^[a-z_][a-z0-9_]*$/.test(schema))
    throw new Error(`bad migration ledger schema ${schema}`);
  const uses = `${schema}.migration_assertion_uses`;
  const chunks = `${schema}.migration_assertion_chunks`;
  const enqueue =
    options.enqueue ??
    ((tx: SqlExecutor, stage: PipelineStage, message: StageMessage) =>
      postgresPipelineQueue(tx, { schema }).send({ ...message, stage }));
  // Opportunistic, bounded retention on every insert (no cron).
  const purge = (tx: SqlExecutor, table: string, key: string) =>
    tx.query(
      `DELETE FROM ${table} WHERE (${key}) IN (
         SELECT ${key} FROM ${table}
          WHERE expires_at < now() - make_interval(secs => $1)
          ORDER BY expires_at
          LIMIT $2
          FOR UPDATE SKIP LOCKED)`,
      [RETENTION_MARGIN_SECONDS, RETENTION_BATCH],
    );
  return {
    async consumed({ jti, nonce }) {
      const { rows } = await sql.query(
        `SELECT 1 FROM ${uses} WHERE jti = $1 OR nonce = $2 LIMIT 1`,
        [jti, nonce],
      );
      return rows.length > 0;
    },
    reserveChunk(use) {
      return sql.transaction(async (tx) => {
        await purge(tx, chunks, "jti, seq");
        // Serializes this assertion's reservations, so the cap is strict.
        await tx.query("SELECT pg_advisory_xact_lock(hashtext($1))", [use.jti]);
        const held = await tx.query<{
          start_offset: string | number;
          sha256: string;
        }>(
          `SELECT start_offset, sha256 FROM ${chunks} WHERE jti = $1 AND seq = $2`,
          [use.jti, use.seq],
        );
        const row = held.rows[0];
        if (row)
          return Number(row.start_offset) === use.startOffset &&
            row.sha256 === use.sha256
            ? ("repeat" as const)
            : ("conflict" as const);
        const counted = await tx.query<{ n: number }>(
          `SELECT count(*)::int AS n FROM ${chunks} WHERE jti = $1`,
          [use.jti],
        );
        if (Number(counted.rows[0]?.n ?? 0) >= MAX_CHUNKS_PER_ASSERTION)
          return "limit" as const;
        const { rows } = await tx.query(
          `INSERT INTO ${chunks} (jti, seq, start_offset, sha256, expires_at)
           VALUES ($1, $2, $3, $4, to_timestamp($5))
           ON CONFLICT DO NOTHING
           RETURNING jti`,
          [use.jti, use.seq, use.startOffset, use.sha256, use.expiresAt],
        );
        return rows.length ? ("reserved" as const) : ("conflict" as const);
      });
    },
    consume(use, stage, message) {
      return sql.transaction(async (tx) => {
        await purge(tx, uses, "jti");
        // One grant, one org and one plan: a use that disagrees with the
        // grant's earlier uses is refused, and nothing is written.
        const conflict = await tx.query(
          `SELECT 1 FROM ${uses}
            WHERE grant_id = $1 AND (plan_digest <> $2 OR org_id <> $3)
            LIMIT 1`,
          [use.grantId, use.planDigest, use.orgId],
        );
        if (conflict.rows.length) return "grant_mismatch" as const;
        // Insert-if-absent on both keys. A concurrent consumer of the same jti
        // or nonce waits on the unique index, then sees the winner's row.
        const { rows } = await tx.query(
          `INSERT INTO ${uses} (jti, nonce, grant_id, org_id, workspace_id,
                                user_id, plan_digest, plan_entry_digest,
                                expires_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, to_timestamp($9))
           ON CONFLICT DO NOTHING
           RETURNING jti`,
          [
            use.jti,
            use.nonce,
            use.grantId,
            use.orgId,
            use.workspaceId,
            use.userId,
            use.planDigest,
            use.planEntryDigest,
            use.expiresAt,
          ],
        );
        if (!rows.length) return "replayed" as const;
        // Throwing here rolls the consumption back with the queue row.
        await enqueue(tx, stage, message);
        return "consumed" as const;
      });
    },
  };
}

/**
 * An in-process ledger (tests, single-process stacks). The reservation and
 * the send are one unit: a send that throws releases the reservation. The
 * same opportunistic, bounded retention as the SQL ledger.
 */
export class InMemoryMigrationAssertionLedger implements MigrationAssertionLedger {
  readonly uses = new Map<string, MigrationAssertionUse>();
  readonly chunks = new Map<string, MigrationChunkUse>();
  readonly #nonces = new Map<string, string>();

  constructor(
    private readonly send: (
      stage: PipelineStage,
      message: StageMessage,
    ) => Promise<void>,
    private readonly now: () => number = () => Math.floor(Date.now() / 1000),
  ) {}

  #purge() {
    const cutoff = this.now() - RETENTION_MARGIN_SECONDS;
    let budget = RETENTION_BATCH;
    for (const [jti, use] of this.uses) {
      if (budget <= 0) break;
      if (use.expiresAt < cutoff) {
        this.uses.delete(jti);
        this.#nonces.delete(use.nonce);
        budget--;
      }
    }
    budget = RETENTION_BATCH;
    for (const [key, chunk] of this.chunks) {
      if (budget <= 0) break;
      if (chunk.expiresAt < cutoff) {
        this.chunks.delete(key);
        budget--;
      }
    }
  }

  async consumed({
    jti,
    nonce,
  }: Pick<MigrationAssertionClaims, "jti" | "nonce">) {
    return this.uses.has(jti) || this.#nonces.has(nonce);
  }

  async reserveChunk(use: MigrationChunkUse): Promise<MigrationChunkResult> {
    this.#purge();
    const key = `${use.jti}\u0000${use.seq}`;
    const held = this.chunks.get(key);
    if (!held) {
      let count = 0;
      for (const chunk of this.chunks.values())
        if (chunk.jti === use.jti) count++;
      if (count >= MAX_CHUNKS_PER_ASSERTION) return "limit";
      this.chunks.set(key, use);
      return "reserved";
    }
    return held.startOffset === use.startOffset && held.sha256 === use.sha256
      ? "repeat"
      : "conflict";
  }

  async consume(
    use: MigrationAssertionUse,
    stage: PipelineStage,
    message: StageMessage,
  ): Promise<MigrationConsumeResult> {
    this.#purge();
    if (this.uses.has(use.jti) || this.#nonces.has(use.nonce))
      return "replayed";
    for (const earlier of this.uses.values())
      if (
        earlier.grantId === use.grantId &&
        (earlier.planDigest !== use.planDigest || earlier.orgId !== use.orgId)
      )
        return "grant_mismatch";
    // Reserved synchronously, before the first await, so a concurrent consume
    // of the same jti or nonce sees it.
    this.uses.set(use.jti, use);
    this.#nonces.set(use.nonce, use.jti);
    try {
      await this.send(stage, message);
    } catch (cause) {
      this.uses.delete(use.jti);
      this.#nonces.delete(use.nonce);
      throw cause;
    }
    return "consumed";
  }
}
