import { Hono, type Context } from "hono";
import { createMiddleware } from "hono/factory";
import {
  migrationGrantAllowed,
  migrationPairAuthorized,
  migrationSessionMatches,
  migrationUploadMatches,
  sqlMigrationAssertionLedger,
  type TransactionalSqlExecutor,
  type MigrationAssertionClaims,
  type MigrationAssertionLedger,
  type MigrationConsumeResult,
} from "@rhc/lib/pipeline/migration-assertions.ts";
import {
  buildObjectLandedMessage,
  isObjectStoreError,
  objectKey,
  OBJECT_LANDED_STAGE,
  orgResolverFromEnv,
  parseManifest,
  parseSessionKey,
  postgresPipelineQueue,
  prefixAllowed,
  rawChunkKey,
  readCapped,
  telemetryIdHash,
  telemetryIdKey,
  type AppEnv,
  type Bindings,
  type ManifestReader,
  type ManifestV1,
  type ObjectVersion,
  type PipelineStage,
  type RawStore,
  type StageMessage,
} from "../shared/vendor.ts";
import { enrichMessage } from "../shared/enrich.ts";

/**
 * The ingest's migration ledger over HoopDb: `sessions.migration_assertion_uses`
 * insert, enrichMessage, and the `sessions.pipeline_jobs` insert, all on the
 * same transaction `tx`. `around` wraps the enqueue (the ingest's span).
 */
export function courierMigrationLedger(
  sql: TransactionalSqlExecutor,
  reader: ManifestReader,
  around: (stage: PipelineStage, message: StageMessage, work: () => Promise<void>) => Promise<void> = (_stage, _message, work) => work(),
): MigrationAssertionLedger {
  return sqlMigrationAssertionLedger(sql, {
    enqueue: (tx, stage, message) =>
      around(stage, message, async () => {
        await postgresPipelineQueue(tx).send(await enrichMessage({ ...message, stage }, reader));
      }),
  });
}

const MAX_MANIFEST_BYTES = 8 * 1024 * 1024;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

interface ManifestRoutesDeps {
  manifests: ManifestReader;
  raw: RawStore;
  send(env: Bindings, stage: typeof OBJECT_LANDED_STAGE, message: StageMessage): Promise<void>;
  /**
   * Single-use ledger for RelayAuth migration assertions (relayauth#110). For
   * a migration commit it replaces `send`: the ledger row, the enriched
   * message (enrichMessage) and the `pipeline_jobs` insert run in ONE SQL
   * transaction (ingest.ts). Absent: migration assertions get 503
   * `migration_ledger_unavailable`.
   */
  migrationLedger?: MigrationAssertionLedger;
}

interface PostedManifest {
  key?: unknown;
  manifest?: unknown;
  previousVersion?: unknown;
}

type Auth = {
  userId: string;
  orgId: string;
  workspaceId?: string;
  historyOrigin?: string;
  scopes: string[];
  claims: Record<string, unknown>;
  migration?: MigrationAssertionClaims;
};

const responseError = (c: Context<AppEnv>, status: 400 | 403 | 404 | 409 | 413 | 503, code: string, message: string) =>
  c.json({ error: { code, message }, correlationId: c.get("correlationId") ?? "" }, status);

function version(value: unknown): ObjectVersion | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const v = value as Record<string, unknown>;
  const eTag = typeof v.eTag === "string" && v.eTag ? v.eTag : undefined;
  const generation = typeof v.version === "string" && v.version ? v.version : undefined;
  return eTag || generation ? { ...(eTag ? { eTag } : {}), ...(generation ? { version: generation } : {}) } : undefined;
}

function sameVersion(expected: ObjectVersion, actual: ObjectVersion | undefined): boolean {
  if (!actual) return false;
  if (expected.eTag && expected.eTag !== actual.eTag) return false;
  if (expected.version && expected.version !== actual.version) return false;
  return Boolean(expected.eTag || expected.version);
}

function sessionUuid(manifest: ManifestV1): string | undefined {
  const ext = (manifest.extensions as Record<string, unknown> | undefined)?.["96bulls"];
  if (!ext || typeof ext !== "object") return undefined;
  const value = (ext as Record<string, unknown>).session_uuid;
  return typeof value === "string" && UUID.test(value) ? value.toLowerCase() : undefined;
}

function manifestMetadata(manifest: ManifestV1, env: Bindings): Record<string, string> {
  const key = telemetryIdKey(env.PIPELINE_TELEMETRY_ID_KEY);
  const uuid = sessionUuid(manifest);
  return {
    "trace-id": manifest.trace_id,
    revision: String(manifest.revision),
    ...(uuid ? { "session-uuid": uuid } : {}),
    ...(key
      ? {
          "machine-token": telemetryIdHash(key, "machine", manifest.session.machine_id),
          "session-token": telemetryIdHash(key, "session", manifest.session.session_id),
        }
      : {}),
  };
}

async function chunksAreServerSealed(raw: RawStore, manifest: ManifestV1): Promise<boolean> {
  const scrub = manifest.raw.scrub;
  if (!scrub || scrub.ruleset !== "relayhistory-scrub" || scrub.version < 1) return false;
  const location = {
    workspaceId: manifest.session.workspace_id,
    machineId: manifest.session.machine_id,
    harness: manifest.session.harness,
    sessionId: manifest.session.session_id,
    startDate: manifest.session.started_at.slice(0, 10),
  };
  for (let start = 0; start < manifest.raw.chunks.length; start += 32) {
    const batch = manifest.raw.chunks.slice(start, start + 32);
    const valid = await Promise.all(batch.map(async (chunk) => {
      const key = rawChunkKey(location, chunk.seq, chunk.start_offset, chunk.sha256, manifest.raw.format);
      const meta = await raw.store.head(key);
      if (!meta) return false;
      return meta.size === chunk.stored_bytes &&
        meta.metadata["content-sha256"] === chunk.stored_sha256 &&
        meta.metadata["raw-sha256"] === chunk.raw_sha256 &&
        Number(meta.metadata["raw-bytes"]) === chunk.stored_raw_bytes &&
        meta.metadata["scrub-ruleset"] === scrub.ruleset &&
        Number(meta.metadata["scrub-version"]) === scrub.version &&
        meta.metadata["trace-id"] === manifest.trace_id;
    }));
    if (valid.includes(false)) return false;
  }
  return true;
}

async function storedManifest(raw: RawStore, key: string): Promise<{ manifest: ManifestV1; version: ObjectVersion; text: string } | undefined> {
  try {
    const object = await raw.store.getObject(objectKey(key));
    const bytes = await readCapped(object.body, MAX_MANIFEST_BYTES);
    if (!bytes) throw new Error("manifest too large");
    const text = new TextDecoder().decode(bytes);
    return { manifest: await parseManifest(text, key), version: object.meta.version, text };
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return undefined;
    throw error;
  }
}

export function createCourierManifestRoutes(deps: ManifestRoutesDeps) {
  const routes = new Hono<AppEnv>();

  const ledgerUnavailable = (c: Context<AppEnv>) =>
    responseError(c, 503, "migration_ledger_unavailable", "The migration assertion ledger is not configured or not available");
  const workspaceForbidden = (c: Context<AppEnv>) =>
    responseError(c, 403, "migration_workspace_forbidden", "The upload's workspace and user are not the pair this migration assertion was signed for");
  const grantForbidden = (c: Context<AppEnv>) =>
    responseError(c, 403, "migration_grant_forbidden", "The migration assertion's grant is not accepted here, or its plan is not that grant's plan");
  const uploadForbidden = (c: Context<AppEnv>) =>
    responseError(c, 403, "migration_upload_forbidden", "The upload is not the one this migration assertion's signed upload identity names");
  const replayed = (c: Context<AppEnv>) =>
    responseError(c, 409, "migration_assertion_replayed", "This migration assertion was already used; retry with a fresh assertion");

  /**
   * Migration-assertion admission, the same as relayhistory-cloud's
   * (routes/pipeline.ts) and with the same codes, ahead of any storage call
   * on the manifest commit and the status read (any method; requireAuth
   * already refuses every route but GET status and POST manifests): a ledger
   * (503), the signed pair (403 migration_workspace_forbidden), an accepted
   * grant and plan (403 migration_grant_forbidden), the requested key being
   * the signed upload identity's (status; 403 migration_upload_forbidden),
   * and not yet consumed (409 migration_assertion_replayed; a ledger error is
   * 503). Ordinary tokens pass through.
   */
  const migrationAdmission = createMiddleware<AppEnv>(async (c, next) => {
    const auth = c.get("auth") as Auth;
    if (!auth?.migration) return next();
    const ledger = deps.migrationLedger;
    if (!ledger) return ledgerUnavailable(c);
    if (!migrationPairAuthorized(auth)) return workspaceForbidden(c);
    if (!migrationGrantAllowed(c.env ?? {}, auth.migration)) return grantForbidden(c);
    if (c.req.path.endsWith("/pipeline/manifests/status")) {
      const location = parseSessionKey(c.req.query("key") ?? "");
      if (!location || location.rest !== "manifest.json" || !migrationSessionMatches(auth, location))
        return uploadForbidden(c);
    }
    let consumed: boolean;
    try {
      consumed = await ledger.consumed(auth.migration);
    } catch {
      return ledgerUnavailable(c);
    }
    if (consumed) return replayed(c);
    await next();
  });
  routes.use("/pipeline/manifests", migrationAdmission);
  routes.use("/pipeline/manifests/status", migrationAdmission);

  /**
   * A migration assertion's manifest: the target workspace is the signed pair's
   * (403 migration_workspace_forbidden), and the upload identity recomputed
   * from this key and manifest (session, state, chunk list, content hash
   * chain, entry digest) is the signed one (403 migration_upload_forbidden).
   */
  const migrationManifestIssue = async (c: Context<AppEnv>, auth: Auth, location: NonNullable<ReturnType<typeof parseSessionKey>>, manifest: ManifestV1) => {
    if (!migrationPairAuthorized(auth, { workspaceIds: [location.workspaceId, manifest.session.workspace_id] }))
      return workspaceForbidden(c);
    if (!(await migrationUploadMatches(auth, location, manifest))) return uploadForbidden(c);
    return undefined;
  };

  const owns = async (c: Context<AppEnv>, location: NonNullable<ReturnType<typeof parseSessionKey>>) => {
    const auth = c.get("auth") as Auth;
    const env = c.env ?? {};
    if (!auth.scopes.includes("rth:sync"))
      return responseError(c, 403, "forbidden", "rth:sync scope is required");
    if (auth.migration && !migrationPairAuthorized(auth, { workspaceIds: [location.workspaceId] }))
      return workspaceForbidden(c);
    const workspaceId = auth.workspaceId || "default";
    if (location.workspaceId !== workspaceId || !prefixAllowed(env, location.prefix, auth.orgId))
      return responseError(c, 403, "forbidden", "key is outside the caller's workspace");
    const owners = orgResolverFromEnv(env);
    if (!owners) return responseError(c, 503, "not_configured", "Object ownership is not configured");
    if ((await owners.orgFor(location)) !== auth.orgId)
      return responseError(c, 403, "forbidden", "key is outside the caller's workspace");
    if (env.RELAYHISTORY_ORIGIN && !auth.historyOrigin)
      return responseError(c, 403, "forbidden", "origin-bound courier assertion required");
    return true as const;
  };

  /**
   * The host courier's manifest write boundary. Ownership is established
   * before any storage call; every referenced chunk must carry AlleyOop's
   * scrub metadata. Revision 1 is create-only, an identical retry is safe,
   * and later revisions require the exact prior object version plus the same
   * delivery writer. The authoritative stored copy is then enqueued.
   */
  routes.post("/pipeline/manifests", async (c) => {
    let body: PostedManifest;
    try {
      body = await c.req.json();
    } catch {
      return responseError(c, 400, "bad_request", "Request body must be valid JSON");
    }
    const key = typeof body.key === "string" ? body.key : "";
    const location = parseSessionKey(key);
    if (!location || location.rest !== "manifest.json")
      return responseError(c, 400, "bad_request", "key must be a v1 session manifest key");
    const owned = await owns(c, location);
    if (owned !== true) return owned;
    if (body.manifest === undefined)
      return responseError(c, 400, "bad_request", "manifest is required");
    let manifest: ManifestV1;
    try {
      manifest = await parseManifest(body.manifest, key);
    } catch {
      return responseError(c, 400, "bad_request", "manifest failed validation");
    }
    if (c.req.header("X-Session-Id") !== manifest.session.session_id || c.req.header("X-Machine-Id") !== manifest.session.machine_id)
      return responseError(c, 400, "bad_request", "session headers do not match the manifest");
    const auth = c.get("auth") as Auth;
    const migration = auth.migration;
    if (migration) {
      const issue = await migrationManifestIssue(c, auth, location, manifest);
      if (issue) return issue;
    }
    const encoded = new TextEncoder().encode(JSON.stringify(manifest));
    if (encoded.byteLength > MAX_MANIFEST_BYTES)
      return responseError(c, 413, "payload_too_large", "manifest is too large");
    try {
      if (!(await chunksAreServerSealed(deps.raw, manifest)))
        return responseError(c, 409, "chunks_unverified", "manifest chunks are not server-sealed");
      const existing = await storedManifest(deps.raw, key);
      let storedVersion: ObjectVersion;
      if (!existing) {
        if (manifest.revision !== 1)
          return responseError(c, 409, "revision_conflict", "the first manifest revision must be 1");
        const ack = await deps.raw.store.put(objectKey(key), encoded, {
          ifNoneMatch: true,
          contentType: "application/json",
          metadata: manifestMetadata(manifest, c.env ?? {}),
        });
        storedVersion = ack.version;
      } else if (existing.text === new TextDecoder().decode(encoded)) {
        storedVersion = existing.version;
      } else {
        const previous = version(body.previousVersion);
        if (!sameVersion(existing.version, previous) ||
          existing.manifest.session.machine_id !== manifest.session.machine_id ||
          existing.manifest.writer.delivery_instance_id !== manifest.writer.delivery_instance_id ||
          manifest.revision !== existing.manifest.revision + 1)
          return responseError(c, 409, "revision_conflict", "manifest version or writer does not match");
        const ack = await deps.raw.store.put(objectKey(key), encoded, {
          ifMatch: existing.version,
          contentType: "application/json",
          metadata: manifestMetadata(manifest, c.env ?? {}),
        });
        storedVersion = ack.version;
      }
      const read = await deps.manifests.read(key);
      if (!read) throw new Error("manifest disappeared after write");
      const built = buildObjectLandedMessage({
        landed: { key, provider: deps.manifests.provider, ...(deps.manifests.bucket ? { bucket: deps.manifests.bucket } : {}) },
        orgId: (c.get("auth") as Auth).orgId,
        manifest: read.manifest,
        read,
      });
      if (built.status !== "enqueue") return responseError(c, 400, "bad_request", "key is not a manifest");
      if (migration) {
        // The ledger row and the enriched pipeline_jobs row commit in one SQL
        // transaction, or neither does (fail closed: 503, nothing consumed).
        // The object write above is idempotent: a retry of the identical
        // manifest takes the "identical" branch and enqueues then.
        const ledger = deps.migrationLedger;
        if (!ledger) return ledgerUnavailable(c);
        let outcome: MigrationConsumeResult;
        try {
          outcome = await ledger.consume({
            jti: migration.jti,
            nonce: migration.nonce,
            grantId: migration.grantId,
            planDigest: migration.planDigest,
            planEntryDigest: migration.upload.entry_sha256,
            expiresAt: migration.expiresAt,
            orgId: auth.orgId,
            workspaceId: location.workspaceId,
            userId: auth.userId,
          }, OBJECT_LANDED_STAGE, built.message);
        } catch {
          return ledgerUnavailable(c);
        }
        if (outcome === "replayed") return replayed(c);
        if (outcome === "grant_mismatch") return grantForbidden(c);
      } else {
        await deps.send(c.env ?? {}, OBJECT_LANDED_STAGE, built.message);
      }
      return c.json({
        workKey: built.message.workKey,
        traceId: built.message.ctx.traceId,
        revision: built.message.revision,
        scrubbed: built.message.payload.scrubbed,
        version: storedVersion,
      }, 202);
    } catch (error) {
      if (isObjectStoreError(error, "already_exists") || isObjectStoreError(error, "precondition_failed"))
        return responseError(c, 409, "revision_conflict", "manifest changed concurrently");
      return responseError(c, 503, "storage_unavailable", "Object storage is temporarily unavailable");
    }
  });

  /** Startup reconciliation: version only, scoped to the stored machine/writer. */
  routes.get("/pipeline/manifests/status", async (c) => {
    const key = c.req.query("key") ?? "";
    const location = parseSessionKey(key);
    if (!location || location.rest !== "manifest.json") return c.body(null, 400);
    const owned = await owns(c, location);
    if (owned !== true)
      return (c.get("auth") as Auth).migration ? owned : c.body(null, owned.status as 403 | 503);
    try {
      const stored = await storedManifest(deps.raw, key);
      if (!stored) return c.body(null, 404);
      // A migration assertion reads only the status of a manifest of its own
      // grant, workspace, source user and plan.
      const auth = c.get("auth") as Auth;
      if (auth.migration) {
        const issue = await migrationManifestIssue(c, auth, location, stored.manifest);
        if (issue) return issue;
      }
      if (c.req.query("machine") !== stored.manifest.session.machine_id ||
        c.req.query("writer") !== stored.manifest.writer.delivery_instance_id)
        return c.body(null, 403);
      return c.json({ version: stored.version }, 200);
    } catch {
      return c.body(null, 503);
    }
  });

  return routes;
}
