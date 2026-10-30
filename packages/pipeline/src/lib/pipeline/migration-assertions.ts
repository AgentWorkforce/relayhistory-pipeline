/**
 * Single use for RelayAuth migration assertions (`POST
 * /v1/tokens/migration-assertion`, relayauth#110). RelayAuth records neither
 * `jti` nor `nonce`, so this service makes each assertion single-use: chunk
 * requests may reuse it until it is consumed, and the manifest commit consumes
 * it exactly once.
 *
 * What is atomic: the ledger row and the manifest commit's own durable write,
 * the `object-landed` queue row, are written by one ledger call. The SQL ledger
 * inserts both in one transaction, so a crash leaves either both (the upload
 * is committed and the assertion spent) or neither (nothing happened, and the
 * same or a fresh assertion may retry). A Cloudflare Queue send cannot join a
 * Postgres transaction, so a deployment that enqueues to Cloudflare has no
 * ledger and refuses migration assertions (503 `migration_ledger_unavailable`).
 *
 * A retry after an ambiguous failure (the commit landed, the response was
 * lost) uses a fresh assertion and enqueues the same work key again; every
 * stage is idempotent by work key, so it is never applied twice.
 *
 * Authorization comes from the signed assertion itself, not a membership
 * lookup: RelayAuth mints only for a `(sourceUserId, workspaceId)` pair stored
 * in the grant. The ingest accepts the token only when the signed `wks` equals
 * `meta.workspaceId`, the signed `sponsorId` equals `meta.sourceUserId`, and
 * the upload's target workspace and user are that pair (else 403
 * `migration_workspace_forbidden`). The manifest commit must also carry the
 * export plan digest the assertion signs (`meta.planDigest`) in
 * `extensions["client.relayhistory_export"].plan_digest` (else 403
 * `migration_plan_mismatch`), name the signed grant in `grant_id` (else 403
 * `migration_grant_forbidden`) and the source user in `source_user_id` (else
 * 403 `migration_workspace_forbidden`). The grant itself must be one this
 * ingest accepts (`RELAYHISTORY_MIGRATION_GRANTS`, `grantId=planDigest`),
 * and the ledger refuses a use whose plan or org disagrees with the grant's
 * earlier uses.
 *
 * Retention: see `supabase/migrations/20261031000000_migration_assertion_ledger.sql`.
 * Rows are kept until the grant is revoked or its export is complete, and
 * never removed before `expires_at` (the token's `exp` plus the 5 second
 * skew), so a purge can never make a `jti` or nonce acceptable again: any
 * token whose row was purged is already refused as expired.
 */
import type { AuthContext } from "../../env.js";
import type { StageMessage } from "./dlq.js";
import { postgresPipelineQueue, type SqlExecutor } from "./queue.js";
import type { PipelineStage } from "./trace.js";

/** What a verified migration assertion carries (`auth.migration`). */
export interface MigrationAssertionClaims {
  jti: string;
  nonce: string;
  grantId: string;
  planDigest: string;
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
  orgId: string;
  workspaceId: string;
  userId: string;
  /** Unix seconds; stored as `expires_at`, the earliest the row may be purged. */
  expiresAt: number;
}

/** The manifest extension that marks an export and commits to its plan. */
export const EXPORT_MARKER_EXTENSION = "client.relayhistory_export";

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
    wks !== migration.workspaceId ||
    sponsorId !== migration.sourceUserId ||
    !auth.workspaceId ||
    auth.workspaceId !== stripRelayPrefix(migration.workspaceId) ||
    auth.userId !== stripRelayPrefix(migration.sourceUserId)
  )
    return false;
  return (target.workspaceIds ?? []).every(
    (workspaceId) => workspaceId === auth.workspaceId,
  );
}

/**
 * The export plan digest a manifest commits to:
 * `extensions["client.relayhistory_export"].plan_digest`, or `undefined`.
 */
export function manifestPlanDigest(manifest: {
  extensions?: Record<string, unknown> | undefined;
}): string | undefined {
  const marker = manifest.extensions?.[EXPORT_MARKER_EXTENSION];
  if (!marker || typeof marker !== "object" || Array.isArray(marker))
    return undefined;
  const digest = (marker as Record<string, unknown>).plan_digest;
  return typeof digest === "string" && /^[0-9a-f]{64}$/.test(digest)
    ? digest
    : undefined;
}

/**
 * Whether the manifest names the grant the assertion was minted from:
 * `extensions["client.relayhistory_export"].grant_id` equals the signed
 * `meta.grantId`.
 */
export function migrationGrantMarked(
  migration: Pick<MigrationAssertionClaims, "grantId">,
  manifest: { extensions?: Record<string, unknown> | undefined },
): boolean {
  const marker = manifest.extensions?.[EXPORT_MARKER_EXTENSION];
  return (
    !!marker &&
    typeof marker === "object" &&
    !Array.isArray(marker) &&
    (marker as Record<string, unknown>).grant_id === migration.grantId
  );
}

/** Whether the manifest commits to exactly the plan the assertion signs. */
export function migrationPlanMatches(
  migration: Pick<MigrationAssertionClaims, "planDigest">,
  manifest: { extensions?: Record<string, unknown> | undefined },
): boolean {
  return manifestPlanDigest(manifest) === migration.planDigest;
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

/**
 * The source user a manifest names: the export marker's `source_user_id`
 * and the 96Bulls extension's `user_id` (either may be absent).
 */
export function manifestSourceUsers(manifest: {
  extensions?: Record<string, unknown> | undefined;
}): { marker?: string; session?: string } {
  const field = (ext: unknown, name: string) => {
    if (!ext || typeof ext !== "object" || Array.isArray(ext)) return undefined;
    const value = (ext as Record<string, unknown>)[name];
    return typeof value === "string" ? value : undefined;
  };
  const marker = field(
    manifest.extensions?.[EXPORT_MARKER_EXTENSION],
    "source_user_id",
  );
  const session = field(manifest.extensions?.["96bulls"], "user_id");
  return {
    ...(marker !== undefined ? { marker } : {}),
    ...(session !== undefined ? { session } : {}),
  };
}

/**
 * The manifest a migration assertion commits must name the asserted source
 * user: the export marker's `source_user_id` is required and equal to
 * `meta.sourceUserId`, and a 96Bulls `user_id`, when present, agrees.
 */
export function migrationSourceMatches(
  migration: Pick<MigrationAssertionClaims, "sourceUserId">,
  manifest: { extensions?: Record<string, unknown> | undefined },
): boolean {
  const { marker, session } = manifestSourceUsers(manifest);
  return (
    marker === migration.sourceUserId &&
    (session === undefined || session === migration.sourceUserId)
  );
}

/**
 * Whether a manifest already stored at the upload's key names a different
 * user: a token for user A must not write under B's session key (and so
 * B's machine). A stored manifest that names no user is not refused here;
 * the key's workspace must still be the grant pair's.
 */
export function storedManifestOwnedByOther(
  migration: Pick<MigrationAssertionClaims, "sourceUserId">,
  stored: { extensions?: Record<string, unknown> | undefined },
): boolean {
  const { marker, session } = manifestSourceUsers(stored);
  return [marker, session].some(
    (user) => user !== undefined && user !== migration.sourceUserId,
  );
}

function stripRelayPrefix(value: string): string {
  return value.startsWith("relay_") ? value.slice("relay_".length) : value;
}

/**
 * `grant_mismatch`: the grant's earlier uses name another plan or org, so this
 * use is not of the grant it claims (nothing written).
 */
export type MigrationConsumeResult = "consumed" | "replayed" | "grant_mismatch";

export interface MigrationAssertionLedger {
  /** Whether this `jti` or this `nonce` was already consumed. */
  consumed(
    claims: Pick<MigrationAssertionClaims, "jti" | "nonce">,
  ): Promise<boolean>;
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

/**
 * The ledger over SQL: `sessions.migration_assertion_uses` (jti primary key,
 * nonce unique) and the queue row in one transaction. `enqueue` defaults to
 * the Postgres pipeline queue on the same transaction; a deployment that
 * enriches messages first passes its own, and must write through `tx`.
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
  const enqueue =
    options.enqueue ??
    ((tx: SqlExecutor, stage: PipelineStage, message: StageMessage) =>
      postgresPipelineQueue(tx, { schema }).send({ ...message, stage }));
  return {
    async consumed({ jti, nonce }) {
      const { rows } = await sql.query(
        `SELECT 1 FROM ${uses} WHERE jti = $1 OR nonce = $2 LIMIT 1`,
        [jti, nonce],
      );
      return rows.length > 0;
    },
    consume(use, stage, message) {
      return sql.transaction(async (tx) => {
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
                                user_id, plan_digest, expires_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, to_timestamp($8))
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
 * the send are one unit: a send that throws releases the reservation.
 */
export class InMemoryMigrationAssertionLedger implements MigrationAssertionLedger {
  readonly uses = new Map<string, MigrationAssertionUse>();
  readonly #nonces = new Set<string>();

  constructor(
    private readonly send: (
      stage: PipelineStage,
      message: StageMessage,
    ) => Promise<void>,
  ) {}

  async consumed({
    jti,
    nonce,
  }: Pick<MigrationAssertionClaims, "jti" | "nonce">) {
    return this.uses.has(jti) || this.#nonces.has(nonce);
  }

  async consume(
    use: MigrationAssertionUse,
    stage: PipelineStage,
    message: StageMessage,
  ): Promise<MigrationConsumeResult> {
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
    this.#nonces.add(use.nonce);
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
