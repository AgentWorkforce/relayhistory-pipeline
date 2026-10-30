import { Hono, type Context } from "hono";
import {
  backfillWriteIssues,
  adoptionWriteIssues,
  buildObjectLandedMessage,
  isObjectStoreError,
  MANIFEST_EPOCHS_CAPABILITY,
  writerUnderstandsBackfill,
  objectKey,
  OBJECT_LANDED_STAGE,
  orgResolverFromEnv,
  parseManifest,
  parseSessionKey,
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
  type RawStore,
  type StageMessage,
} from "../shared/vendor.ts";

const MAX_MANIFEST_BYTES = 8 * 1024 * 1024;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

interface ManifestRoutesDeps {
  manifests: ManifestReader;
  raw: RawStore;
  send(env: Bindings, stage: typeof OBJECT_LANDED_STAGE, message: StageMessage): Promise<void>;
}

interface PostedManifest {
  key?: unknown;
  manifest?: unknown;
  previousVersion?: unknown;
}

type Auth = {
  orgId: string;
  workspaceId?: string;
  historyOrigin?: string;
  tokenSubject: string;
  scopes: string[];
  claims: Record<string, unknown>;
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

const MIGRATION_SUBJECT = "agent_relayhistory_migration";
const MIGRATION_TOKEN_CLASS = "migration_assertion";
const MIGRATION_ID = /^[A-Za-z0-9._:-]{1,255}$/;
const PLAN_DIGEST = /^[0-9a-f]{64}$/;

/**
 * The manifest identity of a verified RelayAuth migration assertion. The
 * assertion verifier has already checked the signature, audience and expiry;
 * this boundary additionally requires the grant and plan bindings E4/O4 uses
 * to fence a backfill epoch.
 */
function migrationWriter(auth: Auth): string | undefined {
  if (auth.tokenSubject !== MIGRATION_SUBJECT) return undefined;
  const meta = auth.claims.meta;
  if (!meta || typeof meta !== "object" || Array.isArray(meta)) return undefined;
  const values = meta as Record<string, unknown>;
  const grantId = values.grantId;
  const planDigest = values.planDigest;
  if (
    values.tokenClass !== MIGRATION_TOKEN_CLASS ||
    typeof grantId !== "string" ||
    !MIGRATION_ID.test(grantId) ||
    typeof planDigest !== "string" ||
    !PLAN_DIGEST.test(planDigest)
  )
    return undefined;
  return `migration:${grantId}`;
}

/** Every reader that must understand the new epochs before one is written. */
export const MANIFEST_READERS = ["transform", "session-index", "summaries", "hoopdb", "fetch"] as const;

/**
 * The activation gate. `MANIFEST_READER_CAPABILITIES` is what each deployed
 * reader advertises, as `reader=capability` pairs. Unset or partial refuses,
 * so a backfill can never reach a reader nobody confirmed.
 */
function activationIssue(existing: ManifestV1, incoming: ManifestV1, env: Bindings): string | undefined {
  const adds = incoming.raw.epochs.slice(existing.raw.epochs.length);
  const backfill = adds.some((epoch) => epoch.reason === "backfill");
  const adoption = adds.some((epoch) => epoch.reason === "adopted");
  if (!backfill && !adoption) return undefined;
  if (!writerUnderstandsBackfill(adoption ? incoming : existing)) return "the session's writer lacks the capability";
  const missing = readersMissing(env);
  return missing.length ? `readers lack the capability: ${missing.join(", ")}` : undefined;
}

function readersMissing(env: Bindings): string[] {
  const advertised = new Set(
    String((env as Record<string, unknown>).MANIFEST_READER_CAPABILITIES ?? "").split(",").map((pair) => pair.trim()),
  );
  return MANIFEST_READERS.filter((reader) => !advertised.has(`${reader}=${MANIFEST_EPOCHS_CAPABILITY}`));
}

const parkedKey = (key: string, machineId: string) => key.replace(/manifest\.json$/, `parked/${machineId}/manifest.json`);

/**
 * A live client that meets a cold export but cannot take it over yet: it lacks
 * the capability, or the gate is closed. It must not stall on 409s or touch
 * the export, so its manifest is held beside it. Readers never see a parked
 * manifest: nothing is enqueued, and the session stays the export until a
 * capable client takes over and re-reads its source.
 */
function mustPark(existing: ManifestV1, incoming: ManifestV1, auth: Auth, env: Bindings): boolean {
  return (
    existing.raw.owner.startsWith("migration:") &&
    auth.tokenSubject !== MIGRATION_SUBJECT &&
    incoming.raw.owner === incoming.writer.delivery_instance_id &&
    incoming.writer.delivery_instance_id === incoming.session.machine_id &&
    (!writerUnderstandsBackfill(incoming) || readersMissing(env).length > 0)
  );
}

function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([a], [b]) => compareText(a, b))
        .map(([key, child]) => [key, canonical(child)]),
    );
  return value;
}

/** Locale-independent Unicode code-point order for canonical JSON keys. */
function compareText(left: string, right: string): number {
  const leftPoints = left[Symbol.iterator]();
  const rightPoints = right[Symbol.iterator]();
  while (true) {
    const leftPoint = leftPoints.next();
    const rightPoint = rightPoints.next();
    if (leftPoint.done || rightPoint.done)
      return leftPoint.done === rightPoint.done ? 0 : leftPoint.done ? -1 : 1;
    const leftValue = leftPoint.value.codePointAt(0)!;
    const rightValue = rightPoint.value.codePointAt(0)!;
    if (leftValue !== rightValue) return leftValue < rightValue ? -1 : 1;
  }
}

function sameJson(left: unknown, right: unknown): boolean {
  return JSON.stringify(canonical(left)) === JSON.stringify(canonical(right));
}

function hasPrefix<T>(before: readonly T[], after: readonly T[]): boolean {
  return (
    after.length >= before.length &&
    before.every((value, index) => sameJson(value, after[index]))
  );
}

function exportMarker(manifest: ManifestV1): boolean {
  const marker = manifest.extensions?.["client.relayhistory_export"];
  return (
    !!marker &&
    typeof marker === "object" &&
    !Array.isArray(marker) &&
    (marker as Record<string, unknown>).version === 1 &&
    (marker as Record<string, unknown>).source === "neon.delivery_records"
  );
}

/**
 * Enforce the raw-owner contract across manifest revisions. Normal live
 * writers may append only as the existing owner. A migration assertion may
 * create a cold export, or append exactly one `backfill` epoch while leaving
 * the live writer's owner token in place. The object-store version remains
 * the serialization fence for a concurrent live append.
 */
function manifestOwnerIssue(
  existing: ManifestV1 | undefined,
  incoming: ManifestV1,
  auth: Auth,
): string | undefined {
  const migration = migrationWriter(auth);
  if (auth.tokenSubject === MIGRATION_SUBJECT && !migration)
    return "migration assertion is not bound to a grant and plan";
  const raw = incoming.raw;
  if (!existing) {
    if (raw.owner !== incoming.writer.delivery_instance_id)
      return "raw owner and delivery writer differ";
    if (migration) {
      if (raw.owner !== migration) return "cold export is not owned by its migration grant";
      if (!exportMarker(incoming)) return "cold export marker is missing";
    }
    return undefined;
  }

  if (existing.raw.owner.startsWith("migration:") && raw.owner !== existing.raw.owner)
    return migration ? "an exporter does not take over" : adoptionWriteIssues(existing, incoming)[0];

  if (
    incoming.trace_id !== existing.trace_id ||
    incoming.created_at !== existing.created_at ||
    !sameJson(incoming.server, existing.server) ||
    raw.owner !== existing.raw.owner ||
    incoming.writer.delivery_instance_id !== existing.writer.delivery_instance_id ||
    raw.format !== existing.raw.format ||
    raw.compression !== existing.raw.compression ||
    !sameJson(raw.scrub, existing.raw.scrub) ||
    (exportMarker(existing) && !sameJson(
      incoming.extensions?.["client.relayhistory_export"],
      existing.extensions?.["client.relayhistory_export"],
    )) ||
    !hasPrefix(existing.raw.epochs, raw.epochs) ||
    !hasPrefix(existing.raw.chunks, raw.chunks)
  )
    return "immutable manifest state changed";

  if (!migration) {
    if (raw.owner !== incoming.writer.delivery_instance_id)
      return "live writer does not own the raw stream";
    if (
      raw.epochs
        .slice(existing.raw.epochs.length)
        .some((epoch) => epoch.reason === "backfill")
    )
      return "live writer attempted to open a backfill epoch";
    return backfillWriteIssues(existing, incoming, "live")[0];
  }

  const addedEpochs = raw.epochs.slice(existing.raw.epochs.length);
  const addedChunks = raw.chunks.slice(existing.raw.chunks.length);
  const backfill = addedEpochs[0];
  if (
    !sameJson(incoming.session, existing.session) ||
    !sameJson(incoming.writer, existing.writer) ||
    raw.state !== existing.raw.state ||
    existing.raw.epochs.some((epoch) => epoch.reason === "backfill") ||
    addedEpochs.length !== 1 ||
    !backfill ||
    backfill.reason !== "backfill" ||
    backfill.opened_by !== migration ||
    backfill.first_seq !== existing.raw.chunk_count ||
    addedChunks.length === 0 ||
    addedChunks.some((chunk) => chunk.epoch !== backfill.epoch) ||
    !exportMarker(incoming)
  )
    return "migration is not an append-only backfill epoch";

  for (const [key, value] of Object.entries(existing.extensions ?? {})) {
    if (key === "client.relayhistory_export") continue;
    if (!sameJson(value, incoming.extensions?.[key]))
      return "migration changed another extension owner";
  }
  for (const key of Object.keys(incoming.extensions ?? {})) {
    if (key !== "client.relayhistory_export" && !(key in (existing.extensions ?? {})))
      return "migration added another extension owner's key";
  }
  return undefined;
}

/** Exact replay is idempotent, but it does not bypass migration identity. */
function migrationReplayIssue(manifest: ManifestV1, auth: Auth): boolean {
  const lastEpoch = manifest.raw.epochs.at(-1);
  const expected = manifest.raw.owner.startsWith("migration:")
    ? manifest.raw.owner
    : lastEpoch?.reason === "backfill"
      ? lastEpoch.opened_by
      : undefined;
  return expected !== undefined && migrationWriter(auth) !== expected;
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

async function storedManifest(raw: RawStore, key: string, target = key): Promise<{ manifest: ManifestV1; version: ObjectVersion; text: string } | undefined> {
  try {
    const object = await raw.store.getObject(objectKey(target));
    const bytes = await readCapped(object.body, MAX_MANIFEST_BYTES);
    if (!bytes) throw new Error("manifest too large");
    const text = new TextDecoder().decode(bytes);
    return { manifest: await parseManifest(text, key), version: object.meta.version, text };
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return undefined;
    throw error;
  }
}

/** Reconcile a lost create/CAS race when the winner stored this exact retry. */
async function putManifestOrReplay(
  raw: RawStore,
  key: string,
  text: string,
  auth: Auth,
  put: () => Promise<{ version: ObjectVersion }>,
): Promise<ObjectVersion> {
  try {
    return (await put()).version;
  } catch (error) {
    if (
      !isObjectStoreError(error, "already_exists") &&
      !isObjectStoreError(error, "precondition_failed")
    )
      throw error;
    const winner = await storedManifest(raw, key);
    if (
      !winner ||
      winner.text !== text ||
      migrationReplayIssue(winner.manifest, auth)
    )
      throw error;
    return winner.version;
  }
}

export function createCourierManifestRoutes(deps: ManifestRoutesDeps) {
  const routes = new Hono<AppEnv>();

  const owns = async (c: Context<AppEnv>, location: NonNullable<ReturnType<typeof parseSessionKey>>) => {
    const auth = c.get("auth") as Auth;
    const env = c.env ?? {};
    if (!auth.scopes.includes("rth:sync"))
      return responseError(c, 403, "forbidden", "rth:sync scope is required");
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
    const encoded = new TextEncoder().encode(JSON.stringify(manifest));
    const encodedText = new TextDecoder().decode(encoded);
    if (encoded.byteLength > MAX_MANIFEST_BYTES)
      return responseError(c, 413, "payload_too_large", "manifest is too large");
    try {
      if (!(await chunksAreServerSealed(deps.raw, manifest)))
        return responseError(c, 409, "chunks_unverified", "manifest chunks are not server-sealed");
      const existing = await storedManifest(deps.raw, key);
      if (existing && mustPark(existing.manifest, manifest, c.get("auth") as Auth, c.env ?? {})) {
        const target = parkedKey(key, manifest.session.machine_id);
        const held = await storedManifest(deps.raw, key, target);
        if (held && held.text !== encodedText &&
          (!sameVersion(held.version, version(body.previousVersion)) || manifest.revision !== held.manifest.revision + 1))
          return responseError(c, 409, "revision_conflict", "parked manifest version does not match");
        const parked = held?.text === encodedText
          ? held.version
          : (await deps.raw.store.put(objectKey(target), encoded, {
              ...(held ? { ifMatch: held.version } : { ifNoneMatch: true }),
              contentType: "application/json",
              metadata: manifestMetadata(manifest, c.env ?? {}),
            })).version;
        return c.json({ parked: true, revision: manifest.revision, scrubbed: true, version: parked }, 202);
      }
      let storedVersion: ObjectVersion;
      if (!existing) {
        if (manifest.revision !== 1)
          return responseError(c, 409, "revision_conflict", "the first manifest revision must be 1");
        if (manifestOwnerIssue(undefined, manifest, c.get("auth") as Auth))
          return responseError(c, 409, "owner_conflict", "manifest owner or append fence does not match");
        storedVersion = await putManifestOrReplay(
          deps.raw,
          key,
          encodedText,
          c.get("auth") as Auth,
          () =>
            deps.raw.store.put(objectKey(key), encoded, {
              ifNoneMatch: true,
              contentType: "application/json",
              metadata: manifestMetadata(manifest, c.env ?? {}),
            }),
        );
      } else if (existing.text === encodedText) {
        if (migrationReplayIssue(existing.manifest, c.get("auth") as Auth))
          return responseError(c, 409, "owner_conflict", "manifest owner or append fence does not match");
        storedVersion = existing.version;
      } else {
        const previous = version(body.previousVersion);
        if (!sameVersion(existing.version, previous) ||
          manifest.revision !== existing.manifest.revision + 1)
          return responseError(c, 409, "revision_conflict", "manifest version or writer does not match");
        if (manifestOwnerIssue(existing.manifest, manifest, c.get("auth") as Auth))
          return responseError(c, 409, "owner_conflict", "manifest owner or append fence does not match");
        const inactive = activationIssue(existing.manifest, manifest, c.env ?? {});
        if (inactive) return responseError(c, 409, "activation_required", inactive);
        storedVersion = await putManifestOrReplay(
          deps.raw,
          key,
          encodedText,
          c.get("auth") as Auth,
          () =>
            deps.raw.store.put(objectKey(key), encoded, {
              ifMatch: existing.version,
              contentType: "application/json",
              metadata: manifestMetadata(manifest, c.env ?? {}),
            }),
        );
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
      await deps.send(c.env ?? {}, OBJECT_LANDED_STAGE, built.message);
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

  /**
   * Startup reconciliation: version, scoped to the stored machine/writer. With
   * `manifest=1` also the stored copy from the same read, for a compare-and-swap
   * retry: a writer that lost a race edits this, never its own local rebuild.
   * That form is scoped to the machine only, so a client can see that another
   * writer owns the stream, and fence itself on the stored owner.
   */
  routes.get("/pipeline/manifests/status", async (c) => {
    const key = c.req.query("key") ?? "";
    const location = parseSessionKey(key);
    if (!location || location.rest !== "manifest.json") return c.body(null, 400);
    const owned = await owns(c, location);
    if (owned !== true) return c.body(null, owned.status as 403 | 503);
    try {
      const stored = await storedManifest(deps.raw, key);
      if (!stored) return c.body(null, 404);
      const withManifest = c.req.query("manifest") === "1";
      const machine = c.req.query("machine") ?? "";
      if (!withManifest && machine === stored.manifest.session.machine_id && stored.manifest.raw.owner.startsWith("migration:")) {
        const held = await storedManifest(deps.raw, key, parkedKey(key, machine));
        if (held) return c.json({ version: held.version }, 200);
      }
      if (c.req.query("machine") !== stored.manifest.session.machine_id ||
        (!withManifest && c.req.query("writer") !== stored.manifest.writer.delivery_instance_id))
        return c.body(null, 403);
      return c.json({
        version: stored.version,
        ...(withManifest ? { manifest: stored.manifest } : {}),
      }, 200);
    } catch {
      return c.body(null, 503);
    }
  });

  return routes;
}
