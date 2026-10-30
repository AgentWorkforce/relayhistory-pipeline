// Vendored from AgentWorkforce/relayauth PR #111 (feat/migration-grant-pair-binding)
// packages/server/src/lib/migration-plan.ts at 2adf414a7767d8cc3e268c6a4bb790d27447e9e4.
// The grant-plan admission oracle for export sharding: do not edit; re-vendor
// from relayauth instead. Byte-equality with the contract-v1 vectors is tested
// in src/migration/__tests__/relayhistory-export-shards.test.ts.
import crypto from "node:crypto";

const RELAY_PREFIX = "relay_";
const ID_SHAPE = /^[A-Za-z0-9._:-]{1,255}$/u;
const KEY_COMPONENT = "[A-Za-z0-9._-]{1,128}";
const MANIFEST_KEY_SHAPE = new RegExp(
  `^v1/ws=(${KEY_COMPONENT})/m=(${KEY_COMPONENT})/h=(${KEY_COMPONENT})/d=(\\d{4})/(\\d{2})/(\\d{2})/s=(${KEY_COMPONENT})/manifest\\.json$`,
  "u",
);
const HEX_64 = /^[0-9a-f]{64}$/u;
const GRANT_ID_SHAPE = /^mgr_[0-9a-f]{32}$/u;

export const MAX_MIGRATION_GRANT_ENTRIES = 1000;
export const MAX_MIGRATION_PLAN_CHUNKS_PER_ENTRY = 4096;
export const MAX_MIGRATION_PLAN_TOTAL_CHUNKS = 50_000;
/**
 * D1 caps one string and one table row at 2,000,000 bytes. Keep the two new
 * JSON columns together below 1,000,000 bytes so the legacy user JSON, scalar
 * columns and SQLite row encoding retain ample headroom. The exact contract-v1
 * fixture shared with 96BullSessions #43 is 1,695 bytes combined.
 */
export const MAX_MIGRATION_GRANT_SERIALIZED_BYTES = 1_000_000;
/**
 * Cloud resolves both the canonical and one-prefix alias through an identity
 * Durable Object to detect ambiguity. This cap bounds that work at 128 identity
 * subrequests, leaving headroom under the 1,000 internal-service floor.
 */
export const MAX_MIGRATION_GRANT_SOURCE_USERS = 64;

export type MigrationGrantPair = {
  sourceUserId: string;
  workspaceId: string;
};

export type MigrationPlanChunk = {
  seq: number;
  epoch: number;
  start_offset: number;
  bytes: number;
  sha256: string;
  records: number;
};

export type MigrationPlanSession = {
  machine_id: string;
  harness: string;
  session_id: string;
  started_at: string;
};

export type MigrationPlanEntry = {
  key: string;
  org_id: string;
  workspace_id: string;
  source_user_id: string;
  session: MigrationPlanSession;
  state: "open" | "closed";
  chunks: MigrationPlanChunk[];
  grant_id: string;
  content_sha256: string;
  raw_bytes: number;
};

export type MigrationGrantPlan = {
  v: 1;
  grant_id: string;
  exported_at: string;
  manifests: MigrationPlanEntry[];
};

export type MigrationUploadIdentity = {
  grant_id: string;
  plan_digest: string;
  key: string;
  org_id: string;
  workspace_id: string;
  source_user_id: string;
  session: MigrationPlanSession;
  content_sha256: string;
  entry_sha256: string;
};

export class MigrationPlanError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "MigrationPlanError";
  }
}

/** Contract-v1 id form: one optional RelayAuth prefix is stripped. */
export function canonicalMigrationId(value: unknown, name: string): string {
  if (typeof value !== "string") {
    throw new MigrationPlanError("invalid_plan", `${name} is malformed`);
  }
  const bare = value.startsWith(RELAY_PREFIX)
    ? value.slice(RELAY_PREFIX.length)
    : value;
  if (!ID_SHAPE.test(bare) || bare.startsWith(RELAY_PREFIX)) {
    throw new MigrationPlanError("invalid_plan", `${name} is malformed`);
  }
  return bare;
}

/** JSON with recursively sorted object keys, byte-for-byte with contract v1. */
export function canonicalMigrationJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(canonicalMigrationJson).join(",")}]`;
  }
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .filter((key) => record[key] !== undefined)
      .map(
        (key) =>
          `${JSON.stringify(key)}:${canonicalMigrationJson(record[key])}`,
      )
      .join(",")}}`;
  }
  const encoded = JSON.stringify(value);
  if (encoded === undefined) {
    throw new MigrationPlanError("invalid_plan", "plan contains undefined");
  }
  return encoded;
}

export function migrationSha256(value: Uint8Array | string): string {
  return crypto.createHash("sha256").update(value).digest("hex");
}

/** Manifest-v1 content hash chain over the source chunk identity. */
export function migrationContentSha256(
  chunks: readonly Pick<
    MigrationPlanChunk,
    "epoch" | "start_offset" | "bytes" | "sha256"
  >[],
): string {
  let hash = Buffer.alloc(32);
  for (const chunk of chunks) {
    const buffer = Buffer.alloc(32 + 4 + 8 + 8 + 32);
    hash.copy(buffer, 0);
    buffer.writeUInt32BE(chunk.epoch, 32);
    buffer.writeBigUInt64BE(BigInt(chunk.start_offset), 36);
    buffer.writeBigUInt64BE(BigInt(chunk.bytes), 44);
    Buffer.from(chunk.sha256, "hex").copy(buffer, 52);
    hash = crypto.createHash("sha256").update(buffer).digest();
  }
  return hash.toString("hex");
}

export function deriveMigrationGrantPlan(input: {
  grantId: string;
  orgId: string;
  plan: unknown;
}): {
  plan: MigrationGrantPlan;
  planDigest: string;
  allowedPairs: MigrationGrantPair[];
  allowedPairHash: string;
} {
  if (!GRANT_ID_SHAPE.test(input.grantId)) {
    throw new MigrationPlanError("invalid_plan", "grant id is malformed");
  }
  const orgId = canonicalMigrationId(input.orgId, "organization id");
  const rawPlan = objectWithExactKeys(
    input.plan,
    ["exported_at", "manifests"],
    "plan",
  );
  const exportedAt = canonicalTimestamp(rawPlan.exported_at, "exported_at");
  if (
    !Array.isArray(rawPlan.manifests) ||
    rawPlan.manifests.length === 0 ||
    rawPlan.manifests.length > MAX_MIGRATION_GRANT_ENTRIES
  ) {
    throw new MigrationPlanError(
      "invalid_plan",
      `plan.manifests must contain 1 to ${MAX_MIGRATION_GRANT_ENTRIES} entries`,
    );
  }

  let totalChunks = 0;
  for (const rawEntry of rawPlan.manifests) {
    if (rawEntry && typeof rawEntry === "object" && !Array.isArray(rawEntry)) {
      const rawChunks = (rawEntry as Record<string, unknown>).chunks;
      if (Array.isArray(rawChunks)) {
        totalChunks += rawChunks.length;
      }
    }
    if (totalChunks > MAX_MIGRATION_PLAN_TOTAL_CHUNKS) {
      throw new MigrationPlanError(
        "invalid_plan",
        `plan contains more than ${MAX_MIGRATION_PLAN_TOTAL_CHUNKS} chunks`,
      );
    }
  }

  const manifests = rawPlan.manifests.map((value, index) =>
    normalizePlanEntry(value, index, input.grantId, orgId),
  );
  manifests.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  if (new Set(manifests.map((entry) => entry.key)).size !== manifests.length) {
    throw new MigrationPlanError(
      "invalid_plan",
      "plan lists a manifest key more than once",
    );
  }

  const plan: MigrationGrantPlan = {
    v: 1,
    grant_id: input.grantId,
    exported_at: exportedAt,
    manifests,
  };
  const planDigest = migrationSha256(canonicalMigrationJson(plan));
  const allowedPairs = normalizeMigrationGrantPairs(
    manifests.map((entry) => ({
      sourceUserId: entry.source_user_id,
      workspaceId: entry.workspace_id,
    })),
  );
  const sourceUserCount = new Set(
    allowedPairs.map((pair) => pair.sourceUserId),
  ).size;
  if (sourceUserCount > MAX_MIGRATION_GRANT_SOURCE_USERS) {
    throw new MigrationPlanError(
      "migration_plan_source_user_limit",
      `plan contains more than ${MAX_MIGRATION_GRANT_SOURCE_USERS} distinct source users`,
    );
  }
  if (
    migrationGrantSerializedBytes(plan, allowedPairs) >
    MAX_MIGRATION_GRANT_SERIALIZED_BYTES
  ) {
    throw new MigrationPlanError(
      "migration_plan_too_large",
      `canonical plan and allowed pairs exceed ${MAX_MIGRATION_GRANT_SERIALIZED_BYTES} bytes`,
    );
  }
  return {
    plan,
    planDigest,
    allowedPairs,
    allowedPairHash: migrationAllowedPairHash(allowedPairs),
  };
}

/** Recompute every derived field before trusting a persisted plan. */
export function verifyStoredMigrationGrantPlan(input: {
  grantId: string;
  orgId: string;
  planDigest: string;
  allowedPairs: readonly MigrationGrantPair[];
  plan: MigrationGrantPlan;
}): void {
  const stored = input.plan;
  if (
    !stored ||
    stored.v !== 1 ||
    stored.grant_id !== input.grantId ||
    !Array.isArray(stored.manifests)
  ) {
    throw new MigrationPlanError("invalid_plan", "stored plan is malformed");
  }
  const derived = deriveMigrationGrantPlan({
    grantId: input.grantId,
    orgId: input.orgId,
    plan: {
      exported_at: stored.exported_at,
      manifests: stored.manifests.map((entry) => ({
        key: entry.key,
        org_id: entry.org_id,
        workspace_id: entry.workspace_id,
        source_user_id: entry.source_user_id,
        session: entry.session,
        state: entry.state,
        chunks: entry.chunks,
      })),
    },
  });
  if (
    derived.planDigest !== input.planDigest ||
    canonicalMigrationJson(derived.plan) !== canonicalMigrationJson(stored) ||
    canonicalMigrationJson(derived.allowedPairs) !==
      canonicalMigrationJson(normalizeMigrationGrantPairs(input.allowedPairs))
  ) {
    throw new MigrationPlanError(
      "invalid_plan",
      "stored plan, digest and allowed pairs disagree",
    );
  }
}

export function migrationAllowedPairHash(
  pairs: readonly MigrationGrantPair[],
): string {
  const canonicalPairs = normalizeMigrationGrantPairs(pairs).map((pair) => ({
    source_user_id: pair.sourceUserId,
    workspace_id: pair.workspaceId,
  }));
  return migrationSha256(canonicalMigrationJson(canonicalPairs));
}

export function migrationUploadIdentity(
  entry: MigrationPlanEntry,
  planDigest: string,
): MigrationUploadIdentity {
  return {
    grant_id: entry.grant_id,
    plan_digest: planDigest,
    key: entry.key,
    org_id: entry.org_id,
    workspace_id: entry.workspace_id,
    source_user_id: entry.source_user_id,
    session: { ...entry.session },
    content_sha256: entry.content_sha256,
    entry_sha256: migrationSha256(canonicalMigrationJson(entry)),
  };
}

function normalizePlanEntry(
  value: unknown,
  index: number,
  grantId: string,
  orgId: string,
): MigrationPlanEntry {
  const name = `plan.manifests[${index}]`;
  const raw = objectWithExactKeys(
    value,
    [
      "key",
      "org_id",
      "workspace_id",
      "source_user_id",
      "session",
      "state",
      "chunks",
    ],
    name,
  );
  const entryOrg = canonicalMigrationId(raw.org_id, `${name}.org_id`);
  if (entryOrg !== orgId) {
    throw new MigrationPlanError(
      "plan_org_mismatch",
      `${name}.org_id is outside the caller's organization`,
    );
  }
  const workspaceId = canonicalMigrationId(
    raw.workspace_id,
    `${name}.workspace_id`,
  );
  const sourceUserId = canonicalMigrationId(
    raw.source_user_id,
    `${name}.source_user_id`,
  );
  if (raw.state !== "open" && raw.state !== "closed") {
    throw new MigrationPlanError("invalid_plan", `${name}.state is malformed`);
  }
  const sessionRaw = objectWithExactKeys(
    raw.session,
    ["machine_id", "harness", "session_id", "started_at"],
    `${name}.session`,
  );
  const session: MigrationPlanSession = {
    machine_id: keyComponent(sessionRaw.machine_id, `${name}.session.machine_id`),
    harness: keyComponent(sessionRaw.harness, `${name}.session.harness`),
    session_id: keyComponent(sessionRaw.session_id, `${name}.session.session_id`),
    started_at: canonicalTimestamp(
      sessionRaw.started_at,
      `${name}.session.started_at`,
    ),
  };
  if (typeof raw.key !== "string") {
    throw new MigrationPlanError("invalid_plan", `${name}.key is malformed`);
  }
  const key = raw.key;
  const keyMatch = MANIFEST_KEY_SHAPE.exec(key);
  if (!keyMatch) {
    throw new MigrationPlanError(
      "invalid_plan",
      `${name}.key is not a canonical v1 manifest key`,
    );
  }
  const [, keyWorkspace, machine, harness, year, month, day, sessionId] =
    keyMatch;
  const startedDate = new Date(session.started_at).toISOString().slice(0, 10);
  if (
    keyWorkspace !== workspaceId ||
    machine !== session.machine_id ||
    harness !== session.harness ||
    sessionId !== session.session_id ||
    `${year}-${month}-${day}` !== startedDate
  ) {
    throw new MigrationPlanError(
      "invalid_plan",
      `${name}.key does not match its workspace and session`,
    );
  }
  if (
    !Array.isArray(raw.chunks) ||
    raw.chunks.length === 0 ||
    raw.chunks.length > MAX_MIGRATION_PLAN_CHUNKS_PER_ENTRY
  ) {
    throw new MigrationPlanError(
      "invalid_plan",
      `${name}.chunks must contain 1 to ${MAX_MIGRATION_PLAN_CHUNKS_PER_ENTRY} chunks`,
    );
  }
  const chunks = raw.chunks.map((chunk, chunkIndex) =>
    normalizeChunk(chunk, name, chunkIndex),
  );
  return {
    key,
    org_id: entryOrg,
    workspace_id: workspaceId,
    source_user_id: sourceUserId,
    session,
    state: raw.state,
    chunks,
    grant_id: grantId,
    content_sha256: migrationContentSha256(chunks),
    raw_bytes: chunks.reduce((sum, chunk) => {
      const next = sum + chunk.bytes;
      if (!Number.isSafeInteger(next)) {
        throw new MigrationPlanError("invalid_plan", `${name}.raw_bytes overflows`);
      }
      return next;
    }, 0),
  };
}

function normalizeChunk(
  value: unknown,
  entryName: string,
  index: number,
): MigrationPlanChunk {
  const name = `${entryName}.chunks[${index}]`;
  const raw = objectWithAllowedKeys(
    value,
    ["seq", "epoch", "start_offset", "bytes", "sha256", "records"],
    ["seq", "epoch", "start_offset", "bytes", "sha256"],
    name,
  );
  const seq = uint(raw.seq, `${name}.seq`);
  if (seq !== index) {
    throw new MigrationPlanError("invalid_plan", `${name}.seq must be ${index}`);
  }
  const epoch = uint(raw.epoch, `${name}.epoch`, 0xffff_ffff);
  const startOffset = uint(raw.start_offset, `${name}.start_offset`);
  const bytes = uint(raw.bytes, `${name}.bytes`);
  if (bytes === 0) {
    throw new MigrationPlanError("invalid_plan", `${name}.bytes must be positive`);
  }
  if (typeof raw.sha256 !== "string" || !HEX_64.test(raw.sha256)) {
    throw new MigrationPlanError("invalid_plan", `${name}.sha256 is malformed`);
  }
  const records =
    raw.records === undefined ? 0 : uint(raw.records, `${name}.records`);
  return {
    seq,
    epoch,
    start_offset: startOffset,
    bytes,
    sha256: raw.sha256,
    records,
  };
}

/**
 * Normalize the storage boundary shared by Node SQLite and external adapters.
 * Objects must contain exactly the two contract fields; identities are
 * canonicalized, then pairs are sorted and de-duplicated deterministically.
 */
export function normalizeMigrationGrantPairs(
  value: unknown,
): MigrationGrantPair[] {
  if (
    !Array.isArray(value) ||
    value.length === 0 ||
    value.length > MAX_MIGRATION_GRANT_ENTRIES
  ) {
    throw new MigrationPlanError(
      "invalid_plan",
      `allowed pairs must contain 1 to ${MAX_MIGRATION_GRANT_ENTRIES} entries`,
    );
  }
  return sortedUniquePairs(
    value.map((pair) => {
      const record = objectWithExactKeys(
        pair,
        ["sourceUserId", "workspaceId"],
        "allowed pair",
      );
      return {
        sourceUserId: canonicalMigrationId(
          record.sourceUserId,
          "source user id",
        ),
        workspaceId: canonicalMigrationId(
          record.workspaceId,
          "workspace id",
        ),
      };
    }),
  );
}

/** Exact UTF-8 bytes persisted across allowed_pairs_json and plan_json. */
export function migrationGrantSerializedBytes(
  plan: MigrationGrantPlan,
  allowedPairs: readonly MigrationGrantPair[],
): number {
  return (
    Buffer.byteLength(canonicalMigrationJson(plan), "utf8") +
    Buffer.byteLength(
      canonicalMigrationJson(normalizeMigrationGrantPairs(allowedPairs)),
      "utf8",
    )
  );
}

function sortedUniquePairs(
  pairs: readonly MigrationGrantPair[],
): MigrationGrantPair[] {
  const normalized = pairs.map((pair) => ({
    sourceUserId: canonicalMigrationId(pair.sourceUserId, "source user id"),
    workspaceId: canonicalMigrationId(pair.workspaceId, "workspace id"),
  }));
  normalized.sort((a, b) => {
    const ak = `${a.sourceUserId}\u0000${a.workspaceId}`;
    const bk = `${b.sourceUserId}\u0000${b.workspaceId}`;
    return ak < bk ? -1 : ak > bk ? 1 : 0;
  });
  return normalized.filter(
    (pair, index) =>
      index === 0 ||
      pair.sourceUserId !== normalized[index - 1]!.sourceUserId ||
      pair.workspaceId !== normalized[index - 1]!.workspaceId,
  );
}

function objectWithExactKeys(
  value: unknown,
  keys: readonly string[],
  name: string,
): Record<string, unknown> {
  return objectWithAllowedKeys(value, keys, keys, name);
}

function objectWithAllowedKeys(
  value: unknown,
  allowed: readonly string[],
  required: readonly string[],
  name: string,
): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new MigrationPlanError("invalid_plan", `${name} must be an object`);
  }
  const record = value as Record<string, unknown>;
  const present = Object.keys(record).sort();
  if (
    present.some((key) => !allowed.includes(key)) ||
    required.some((key) => !Object.hasOwn(record, key))
  ) {
    throw new MigrationPlanError(
      "invalid_plan",
      `${name} has missing or unsupported fields`,
    );
  }
  return record;
}

function canonicalTimestamp(value: unknown, name: string): string {
  if (typeof value !== "string") {
    throw new MigrationPlanError("invalid_plan", `${name} is malformed`);
  }
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds) || new Date(milliseconds).toISOString() !== value) {
    throw new MigrationPlanError(
      "invalid_plan",
      `${name} must be canonical ISO 8601 UTC`,
    );
  }
  return value;
}

function keyComponent(value: unknown, name: string): string {
  if (
    typeof value !== "string" ||
    !new RegExp(`^${KEY_COMPONENT}$`, "u").test(value) ||
    value.startsWith(".")
  ) {
    throw new MigrationPlanError("invalid_plan", `${name} is malformed`);
  }
  return value;
}

function uint(
  value: unknown,
  name: string,
  maximum = Number.MAX_SAFE_INTEGER,
): number {
  if (
    typeof value !== "number" ||
    !Number.isSafeInteger(value) ||
    value < 0 ||
    value > maximum
  ) {
    throw new MigrationPlanError("invalid_plan", `${name} is malformed`);
  }
  return value;
}
