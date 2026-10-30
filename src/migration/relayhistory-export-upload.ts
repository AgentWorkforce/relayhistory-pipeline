/**
 * Upload courier for the Neon-to-S3 export (docs/relayhistory-export-design.md,
 * "Delivery path" and O2). It delivers an export plan through the ingest API
 * exactly as the live courier does (src/alleyoop/courier.ts): every raw range
 * goes through the scrubbing chunk hop (`POST /v1/pipeline/chunks`), then the
 * v1 manifest is committed (`POST /v1/pipeline/manifests`). The ingest's
 * migration-assertion contract (#43, relayhistory-cloud #228) shapes it:
 *
 * - **One fresh assertion per manifest attempt.** Minted through
 *   `MigrationAssertionClient` (relayhistory-export-auth.ts), never cached
 *   across manifests; chunk requests reuse it until the manifest commit
 *   consumes it. One about to expire is replaced by another fresh mint.
 * - **Plan contract.** One pure function (`derivePlanContract`, swappable via
 *   `ExportUploadConfig.contract` for #43's shared fixture) turns the plan into
 *   canonical, grant-bound entries (key, canonical user, session, exact chunk
 *   set), the plan digest (`RELAYHISTORY_MIGRATION_GRANTS` registers
 *   `grantId=planDigest` today) and one per-upload identity per manifest. The
 *   issuer derives the signed identity from its stored plan; a mint names
 *   only the manifest key. The courier refuses, before any byte is sent, an
 *   assertion whose signed plan or identity differs from its own derivation,
 *   and never uses it for another upload. Before each commit it re-runs the
 *   same function over the exact manifest about to be sent.
 * - **User ids.** One canonical form (`canonicalUserId`: no `relay_` prefix)
 *   in the plan, the mint, the manifest and the identity.
 * - **Errors.** 409 `migration_assertion_replayed`: one fresh re-mint, same
 *   content (sealed chunks are not resent); a second replay fails that
 *   manifest loudly. 401 `migration_assertion_expired` (exactly): one fresh
 *   re-mint and retry; a second expiry, or any other 401, stops the run. 503 (and other 5xx, and lost responses): bounded backoff
 *   with a fresh assertion each time, then the run stops (fail closed). 401 or
 *   403: the run stops at once, no retry. A lost response after the server
 *   committed is safe to retry: the identical manifest takes the ingest's
 *   "identical" branch and re-enqueues the same work key.
 * - **Checkpoint.** Per manifest, bound to the plan digest, grant and ingest
 *   origin: committed manifests are never re-uploaded after a crash, and
 *   sealed chunks are not resent. It holds ids, digests and work keys only,
 *   never a token or key.
 *
 * Fail closed: `runExportUpload` refuses unless `execute: true`, and the CLI
 * (`relayhistory-export.ts main`) still refuses `--execute` until the O2 grant
 * and P9 keys exist, so by default nothing leaves the machine.
 */
import { createHash } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import {
  computeContentSha256,
  manifestKey,
  validateManifest,
  type ChunkEntry,
  type ManifestV1,
  type SessionLocation,
} from "@rhc-object-store";
import { log } from "../shared/log.ts";
import {
  buildSessionStream,
  chunkRanges,
  type DeliveryRecord,
} from "./relayhistory-export.ts";
import type { MigrationAssertion } from "./relayhistory-export-auth.ts";

export const EXPORT_PLUGIN_VERSION = "96bulls-relayhistory-export/0.1.0";
export const EXPORT_WRITER_ID = "relayhistory-export";
export const EXPORT_MARKER = "client.relayhistory_export";
const SESSION_EXT = "96bulls";
const RAW_FORMAT = "ndjson" as const;

// ---------------------------------------------------------------- the plan

export interface PlannedChunk {
  seq: number;
  epoch: number;
  start_offset: number;
  /** The exact source bytes sent to the chunk hop. */
  data: Uint8Array;
  records: number;
}

export interface PlannedManifest {
  /** `startDate` as `yyyy-mm-dd` (the chunk hop's `date`). */
  location: SessionLocation & { startDate: string };
  orgId: string;
  sourceUserId: string;
  traceId: string;
  sessionUuid: string;
  startedAt: string;
  endedAt?: string;
  state: "open" | "closed";
  catalog?: Record<string, unknown>;
  chunks: PlannedChunk[];
}

export interface ExportPlan {
  /** One timestamp for the whole plan, so every manifest is deterministic. */
  exportedAt: string;
  manifests: PlannedManifest[];
}

/**
 * One upload as the plan contract sees it: everything about a manifest that
 * is known before upload (server-sealed fields such as stored digests are
 * not). Built from a planned manifest (`uploadFromPlanned`) and, before each
 * commit, from the exact manifest about to be sent (`uploadFromManifest`).
 */
export interface UploadInput {
  key: string;
  org_id: string;
  workspace_id: string;
  source_user_id: string;
  session: { machine_id: string; harness: string; session_id: string; started_at: string };
  state: "open" | "closed";
  chunks: { seq: number; epoch: number; start_offset: number; bytes: number; sha256: string; records: number }[];
}

/** A canonical, grant-bound plan entry. */
export interface PlanEntry extends UploadInput {
  grant_id: string;
  content_sha256: string;
  raw_bytes: number;
}

/**
 * The per-upload identity an assertion signs. The ISSUER (RelayAuth) derives
 * it from the canonical stored plan; the courier derives the same value
 * locally and refuses an assertion whose signed identity differs from what it
 * is about to send. It binds the grant and plan, the tenancy (org, workspace,
 * canonical source user), the machine/session key, the manifest key, and the
 * content and source-chunk digests. The work key is not bound in full: the
 * ingest's work key is `<manifest key>@<object version>`, and the version is
 * assigned by storage at write time, so the courier checks that the returned
 * work key is of this manifest key instead.
 */
export interface UploadIdentity {
  grant_id: string;
  plan_digest: string;
  org_id: string;
  workspace_id: string;
  source_user_id: string;
  session: UploadInput["session"];
  manifest_key: string;
  content_sha256: string;
  /** sha256 of the canonical source-chunk list (seq, epoch, offset, bytes, sha256). */
  source_chunks_sha256: string;
}

/** What a mint asks the issuer for: the planned upload, by manifest key. */
export interface UploadSelector {
  manifest_key: string;
}

const RELAY_PREFIX = "relay_";
const USER_ID = /^[A-Za-z0-9._:-]{1,255}$/;

/**
 * The one canonical user-id form: without RelayAuth's `relay_` prefix (the
 * form the ingest's auth context carries). `relay_u1` and `u1` are the same
 * user; anything malformed is refused. Pending #43's shared fixture.
 */
export function canonicalUserId(id: string): string {
  const bare = id.startsWith(RELAY_PREFIX) ? id.slice(RELAY_PREFIX.length) : id;
  if (!USER_ID.test(bare) || bare.startsWith(RELAY_PREFIX))
    throw Object.assign(new Error("export upload: source user id is malformed"), { code: "user_id_malformed" });
  return bare;
}

export interface PlanContract {
  entries: PlanEntry[];
  /** Digest of all entries: what `RELAYHISTORY_MIGRATION_GRANTS` registers today. */
  planDigest: string;
  /** One per entry, same order. */
  uploads: UploadIdentity[];
}

/**
 * The single pure function that turns a plan into its canonical entries, plan
 * digest and per-upload identities. Swappable (`ExportUploadConfig.contract`)
 * for the shared fixture from #43, so courier and ingest derive identically.
 */
export type PlanContractFn = (input: { grantId: string; exportedAt: string; uploads: readonly UploadInput[] }) => PlanContract;

const sha256Hex = (bytes: Uint8Array | string) => createHash("sha256").update(bytes).digest("hex");

/** JSON with sorted object keys: one byte string per value. */
export function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object")
    return `{${Object.keys(value as Record<string, unknown>)
      .sort()
      .filter((k) => (value as Record<string, unknown>)[k] !== undefined)
      .map((k) => `${JSON.stringify(k)}:${canonical((value as Record<string, unknown>)[k])}`)
      .join(",")}}`;
  return JSON.stringify(value);
}

/** `raw.content_sha256`, synchronously (the same chain as object-store's computeContentSha256). */
export function contentSha256(chunks: readonly { epoch: number; start_offset: number; bytes: number; sha256: string }[]): string {
  let h: Buffer = Buffer.alloc(32);
  for (const c of chunks) {
    const buf = Buffer.alloc(32 + 4 + 8 + 8 + 32);
    h.copy(buf, 0);
    buf.writeUInt32BE(c.epoch, 32);
    buf.writeBigUInt64BE(BigInt(c.start_offset), 36);
    buf.writeBigUInt64BE(BigInt(c.bytes), 44);
    Buffer.from(c.sha256, "hex").copy(buf, 52);
    h = createHash("sha256").update(buf).digest();
  }
  return h.toString("hex");
}

export const derivePlanContract: PlanContractFn = ({ grantId, exportedAt, uploads }) => {
  const entries: PlanEntry[] = uploads.map((u) => ({
    ...u,
    source_user_id: canonicalUserId(u.source_user_id),
    grant_id: grantId,
    content_sha256: contentSha256(u.chunks),
    raw_bytes: u.chunks.reduce((n, c) => n + c.bytes, 0),
  }));
  const sorted = [...entries].sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  const planDigest = sha256Hex(canonical({ v: 1, grant_id: grantId, exported_at: exportedAt, manifests: sorted }));
  return {
    entries,
    planDigest,
    uploads: entries.map((e) => ({
      grant_id: e.grant_id,
      plan_digest: planDigest,
      org_id: e.org_id,
      workspace_id: e.workspace_id,
      source_user_id: e.source_user_id,
      session: e.session,
      manifest_key: e.key,
      content_sha256: e.content_sha256,
      source_chunks_sha256: sha256Hex(canonical(e.chunks.map(({ seq, epoch, start_offset, bytes, sha256 }) => ({ seq, epoch, start_offset, bytes, sha256 })))),
    })),
  };
};

/** The contract input of a planned manifest, from its exact chunk bytes. */
export function uploadFromPlanned(m: PlannedManifest): UploadInput {
  return {
    key: manifestKey(m.location),
    org_id: m.orgId,
    workspace_id: m.location.workspaceId,
    source_user_id: m.sourceUserId,
    session: { machine_id: m.location.machineId, harness: m.location.harness, session_id: m.location.sessionId, started_at: m.startedAt },
    state: m.state,
    chunks: m.chunks.map((c) => ({
      seq: c.seq,
      epoch: c.epoch,
      start_offset: c.start_offset,
      bytes: c.data.byteLength,
      sha256: sha256Hex(c.data),
      records: c.records,
    })),
  };
}

/** The contract input of the exact manifest about to be sent. */
export function uploadFromManifest(key: string, manifest: ManifestV1): UploadInput {
  const ext = (manifest.extensions ?? {}) as Record<string, Record<string, unknown> | undefined>;
  return {
    key,
    org_id: String(ext[SESSION_EXT]?.tenant_id ?? ""),
    workspace_id: manifest.session.workspace_id,
    source_user_id: String(ext[EXPORT_MARKER]?.source_user_id ?? ""),
    session: {
      machine_id: manifest.session.machine_id,
      harness: manifest.session.harness,
      session_id: manifest.session.session_id,
      started_at: manifest.session.started_at,
    },
    state: manifest.raw.state,
    chunks: manifest.raw.chunks.map((c) => ({
      seq: c.seq,
      epoch: c.epoch,
      start_offset: c.start_offset,
      bytes: c.bytes,
      sha256: c.sha256,
      records: c.records ?? 0,
    })),
  };
}

/** The plan's contract under `grantId` (default derivation). */
export function planContract(plan: ExportPlan, grantId: string, contract: PlanContractFn = derivePlanContract): PlanContract {
  return contract({ grantId, exportedAt: plan.exportedAt, uploads: plan.manifests.map(uploadFromPlanned) });
}

/** A UUIDv7 derived from a seed (timestamp bits from `ms`): stable across re-exports. */
export function derivedUuidV7(seed: string, ms: number): string {
  const h = sha256Hex(seed);
  const ts = Math.max(0, Math.floor(ms)).toString(16).padStart(12, "0").slice(-12);
  const variant = ((parseInt(h[16]!, 16) & 0x3) | 0x8).toString(16);
  return `${ts.slice(0, 8)}-${ts.slice(8, 12)}-7${h.slice(1, 4)}-${variant}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

const component = (s: string) => s.replace(/[^A-Za-z0-9._-]/g, "_").slice(0, 128).replace(/^\./, "_");

/**
 * One machine stream of a Neon session as a planned manifest: the stream's
 * exact chunks (record boundaries, at most 4 MiB), deterministic ids. Null
 * when the stream has no live records.
 */
export function plannedManifestFromRecords(input: {
  orgId: string;
  workspaceId: string;
  source: string;
  sessionId: string;
  machine: string;
  sourceUserId: string;
  records: readonly DeliveryRecord[];
  state?: "open" | "closed";
  catalog?: Record<string, unknown>;
  maxChunkBytes?: number;
}): PlannedManifest | null {
  const stream = buildSessionStream(input.records);
  if (stream.records === 0 || stream.firstMs === null) return null;
  const location: PlannedManifest["location"] = {
    workspaceId: input.workspaceId,
    machineId: component(input.machine),
    harness: component(input.source),
    sessionId: component(input.sessionId),
    startDate: new Date(stream.firstMs).toISOString().slice(0, 10),
  };
  const seed = `${input.orgId}\n${input.workspaceId}\n${location.machineId}\n${location.harness}\n${location.sessionId}`;
  return {
    location,
    orgId: input.orgId,
    sourceUserId: input.sourceUserId,
    traceId: derivedUuidV7(`trace\n${seed}`, stream.firstMs),
    sessionUuid: derivedUuidV7(`session\n${seed}`, stream.firstMs),
    startedAt: new Date(stream.firstMs).toISOString(),
    ...(stream.lastMs !== null ? { endedAt: new Date(stream.lastMs).toISOString() } : {}),
    state: input.state ?? "closed",
    ...(input.catalog ? { catalog: input.catalog } : {}),
    chunks: chunkRanges(stream, input.maxChunkBytes).map((range, seq) => ({
      seq,
      epoch: 0,
      start_offset: range.start,
      data: stream.bytes.subarray(range.start, range.end),
      records: range.records,
    })),
  };
}

/**
 * The v1 manifest for a planned session and its server-sealed chunk entries.
 * Deterministic: a retry sends the identical bytes, so the ingest treats it
 * as the same commit.
 */
export async function buildExportManifest(
  plan: ExportPlan,
  m: PlannedManifest,
  sealed: readonly ChunkEntry[],
  scrub: { ruleset: string; version: number } | undefined,
  commit: { grantId: string; planDigest: string },
): Promise<ManifestV1> {
  const last = sealed.at(-1)!;
  return {
    schema_v: 1,
    layout: "v1",
    trace_id: m.traceId,
    revision: 1,
    created_at: plan.exportedAt,
    updated_at: plan.exportedAt,
    session: {
      workspace_id: m.location.workspaceId,
      machine_id: m.location.machineId,
      harness: m.location.harness,
      session_id: m.location.sessionId,
      started_at: m.startedAt,
      ...(m.state === "closed" && m.endedAt ? { ended_at: m.endedAt } : {}),
    },
    writer: { delivery_instance_id: EXPORT_WRITER_ID, plugin_version: EXPORT_PLUGIN_VERSION },
    raw: {
      owner: m.location.machineId,
      state: m.state,
      updated_at: plan.exportedAt,
      format: RAW_FORMAT,
      compression: "zstd",
      chunk_count: sealed.length,
      raw_bytes: sealed.reduce((n, c) => n + c.bytes, 0),
      stored_bytes: sealed.reduce((n, c) => n + c.stored_bytes, 0),
      content_sha256: await computeContentSha256(sealed),
      epoch: 0,
      epochs: [{ epoch: 0, opened_at: m.startedAt, reason: "initial", first_seq: 0, opened_by: EXPORT_WRITER_ID }],
      next_offset: last.start_offset + last.bytes,
      chunks: [...sealed],
      ...(scrub ? { scrub } : {}),
    },
    extensions: {
      [SESSION_EXT]: { session_uuid: m.sessionUuid, session_uuid_source: "minted", tenant_id: m.orgId, user_id: canonicalUserId(m.sourceUserId) },
      [EXPORT_MARKER]: {
        version: 1,
        source: "neon.delivery_records",
        exported_at: plan.exportedAt,
        grant_id: commit.grantId,
        plan_digest: commit.planDigest,
        source_user_id: canonicalUserId(m.sourceUserId),
        ...(m.catalog ? { catalog: m.catalog } : {}),
      },
    },
  } as ManifestV1;
}

// ---------------------------------------------------------------- checkpoint

export type ManifestCheckpoint =
  | { status: "sealing"; chunks: ChunkEntry[]; scrub?: { ruleset: string; version: number } }
  | { status: "committed"; work_key: string; chunks: number };

export interface UploadCheckpoint {
  version: 1;
  plan_digest: string;
  grant_id: string;
  ingest_origin: string;
  manifests: Record<string, ManifestCheckpoint>;
}

async function loadCheckpoint(path: string, binding: Omit<UploadCheckpoint, "version" | "manifests">): Promise<UploadCheckpoint> {
  let text: string;
  try {
    text = await readFile(path, "utf8");
  } catch (error) {
    if ((error as { code?: string }).code === "ENOENT") return { version: 1, ...binding, manifests: {} };
    throw error;
  }
  const cp = JSON.parse(text) as UploadCheckpoint;
  if (cp.version !== 1 || cp.plan_digest !== binding.plan_digest || cp.grant_id !== binding.grant_id || cp.ingest_origin !== binding.ingest_origin)
    throw Object.assign(new Error("export upload: checkpoint belongs to another plan, grant or ingest"), { code: "checkpoint_mismatch" });
  return cp;
}

async function saveCheckpoint(path: string, cp: UploadCheckpoint): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const tmp = `${path}.tmp`;
  await writeFile(tmp, JSON.stringify(cp, null, 2), { mode: 0o600 });
  await rename(tmp, path);
}

// ---------------------------------------------------------------- courier

export interface AssertionSource {
  /** A fresh assertion that signs `upload`, the one upload it may be used for. */
  mint(sourceUserId: string, workspaceId: string, upload: UploadSelector): Promise<MigrationAssertion>;
}

export interface ExportUploadConfig {
  /** The write guard: anything but `true` refuses before any request. */
  execute: boolean;
  /** Ingest API base URL (https; plain http only on loopback). */
  ingestUrl: string;
  grantId: string;
  /** The digest the grant was created with; the plan must hash to it. */
  expectedPlanDigest: string;
  /** Fresh single-use assertions (MigrationAssertionClient). */
  assertions: AssertionSource;
  checkpointPath: string;
  /** The plan contract (default derivePlanContract); swap in #43's shared fixture. */
  contract?: PlanContractFn;
  fetcher?: typeof fetch;
  requestTimeoutMs?: number;
  /** Backoff before each retry of a 5xx or lost response; its length is the retry budget. */
  backoffMs?: readonly number[];
  /** A token closer than this to expiry is replaced by a fresh mint before the next request. */
  refreshMarginMs?: number;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
}

export interface ExportUploadResult {
  committed: { key: string; workKey: string }[];
  skipped: string[];
  failed: { key: string; code: string }[];
  /** Set when the run stopped (401/403, exhausted 5xx budget, plan mismatch). */
  stopped?: { key: string; code: string; status?: number };
}

type Outcome =
  | { kind: "replayed"; code: string; status: number }
  | { kind: "expired"; code: string; status: number }
  | { kind: "transient"; code: string; status?: number }
  | { kind: "stop"; code: string; status?: number }
  | { kind: "failed"; code: string; status?: number };

class OutcomeError extends Error {
  constructor(readonly outcome: Outcome) {
    super(`export upload: ${outcome.code}`);
  }
}

const LOOPBACK = new Set(["localhost", "127.0.0.1", "[::1]"]);
const DEFAULT_BACKOFF_MS = [500, 2_000, 8_000] as const;

function classify(status: number, code: string): Outcome {
  if (status === 409 && code === "migration_assertion_replayed") return { kind: "replayed", code, status };
  // Only this exact code is retried; any other 401 (invalid signature, claims) is terminal.
  if (status === 401 && code === "migration_assertion_expired") return { kind: "expired", code, status };
  if (status === 401 || status === 403) return { kind: "stop", code, status };
  if (status === 429 || status >= 500) return { kind: "transient", code, status };
  return { kind: "failed", code, status };
}

async function errorCode(response: Response): Promise<string> {
  const body = (await response.json().catch(() => ({}))) as { error?: { code?: unknown } };
  return typeof body.error?.code === "string" ? body.error.code : `http_${response.status}`;
}

/**
 * What a minted assertion signs, read without verifying (the ingest
 * verifies): `meta.planDigest` and the per-upload identity `meta.upload`.
 * Used only to refuse, before any byte is sent, an assertion for another
 * plan or another upload.
 */
export function assertionCommitments(token: string): { planDigest?: string; upload?: unknown } {
  const payload = token.split(".")[1];
  if (!payload) return {};
  try {
    const claims = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")) as { meta?: { planDigest?: unknown; upload?: unknown } };
    return {
      ...(typeof claims.meta?.planDigest === "string" ? { planDigest: claims.meta.planDigest } : {}),
      ...(claims.meta?.upload !== undefined ? { upload: claims.meta.upload } : {}),
    };
  } catch {
    return {};
  }
}

const keyHash = (key: string) => sha256Hex(key).slice(0, 16);

/**
 * Deliver an export plan, manifest by manifest. Returns what committed, what
 * the checkpoint already had, what failed (loudly logged) and, when the run
 * stopped, why. Never throws for an ingest refusal; throws for a refused
 * guard, a plan that does not hash to the grant's digest, or a checkpoint of
 * another plan.
 */
export async function runExportUpload(plan: ExportPlan, cfg: ExportUploadConfig): Promise<ExportUploadResult> {
  if (cfg.execute !== true)
    throw Object.assign(new Error("export upload: refused without --execute (dry run writes nothing)"), { code: "execute_required" });
  const base = new URL(cfg.ingestUrl);
  if (base.username || base.password || base.search || base.hash)
    throw new Error("export upload: ingest URL must not carry credentials, query or fragment");
  if (base.protocol !== "https:" && !(base.protocol === "http:" && LOOPBACK.has(base.hostname)))
    throw new Error("export upload: ingest URL must use https");
  const ingest = cfg.ingestUrl.replace(/\/$/, "");
  const fetcher = cfg.fetcher ?? fetch;
  const timeoutMs = cfg.requestTimeoutMs ?? 30_000;
  const backoff = cfg.backoffMs ?? DEFAULT_BACKOFF_MS;
  const sleep = cfg.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const now = cfg.now ?? Date.now;
  const refreshMarginMs = cfg.refreshMarginMs ?? 15_000;

  const contractFn = cfg.contract ?? derivePlanContract;
  const contract = planContract(plan, cfg.grantId, contractFn);
  const { entries, uploads } = contract;
  const digest = contract.planDigest;
  if (entries.length !== plan.manifests.length || uploads.length !== plan.manifests.length)
    throw new Error("export upload: the plan contract must give one entry and one identity per manifest");
  if (digest !== cfg.expectedPlanDigest)
    throw Object.assign(new Error("export upload: the plan does not hash to the grant's plan digest"), { code: "plan_digest_mismatch" });
  const keys = entries.map((e) => e.key);
  if (new Set(keys).size !== keys.length) throw new Error("export upload: the plan lists a manifest key twice");

  const cp = await loadCheckpoint(cfg.checkpointPath, { plan_digest: digest, grant_id: cfg.grantId, ingest_origin: base.origin });
  const result: ExportUploadResult = { committed: [], skipped: [], failed: [] };

  const request = async (token: string, path: string, init: RequestInit): Promise<Response> => {
    const headers = new Headers(init.headers);
    headers.set("authorization", `Bearer ${token}`);
    try {
      return await fetcher(`${ingest}${path}`, { ...init, headers, redirect: "error", signal: AbortSignal.timeout(timeoutMs) });
    } catch {
      // Timeout or connection loss: the server may have committed. Retrying
      // with a fresh assertion is idempotent on the ingest side.
      throw new OutcomeError({ kind: "transient", code: "response_lost" });
    }
  };

  for (const [index, m] of plan.manifests.entries()) {
    const entry = entries[index]!;
    const identity = uploads[index]!;
    const key = entry.key;
    const prior = cp.manifests[key];
    if (prior?.status === "committed") {
      result.skipped.push(key);
      continue;
    }
    const state: Extract<ManifestCheckpoint, { status: "sealing" }> =
      prior?.status === "sealing" ? prior : { status: "sealing", chunks: [] };
    cp.manifests[key] = state;
    let replays = 0;
    let expiries = 0;
    let transient = 0;
    let outcome: "committed" | Outcome | undefined;
    while (!outcome) {
      let assertion: MigrationAssertion | undefined;
      const token = async (): Promise<string> => {
        if (assertion && assertion.expiresAt.getTime() - now() > refreshMarginMs) return assertion.token;
        try {
          assertion = await cfg.assertions.mint(identity.source_user_id, identity.workspace_id, { manifest_key: identity.manifest_key });
        } catch (error) {
          const e = error as { code?: unknown; retryable?: unknown };
          const code = typeof e.code === "string" ? e.code : "mint_failed";
          throw new OutcomeError(e.retryable === true ? { kind: "transient", code } : { kind: "stop", code });
        }
        // The assertion must sign this plan and exactly this upload; it is
        // never used for any other upload.
        const signed = assertionCommitments(assertion.token);
        if (signed.planDigest !== digest) throw new OutcomeError({ kind: "stop", code: "assertion_plan_mismatch" });
        if (canonical(signed.upload) !== canonical(identity))
          throw new OutcomeError({ kind: "stop", code: "assertion_upload_mismatch" });
        return assertion.token;
      };
      try {
        await token();
        for (const chunk of m.chunks) {
          if (state.chunks.some((c) => c.seq === chunk.seq)) continue; // sealed before a retry or crash
          const q = new URLSearchParams({
            format: RAW_FORMAT,
            seq: String(chunk.seq),
            epoch: String(chunk.epoch),
            offset: String(chunk.start_offset),
            date: m.location.startDate,
            machine: m.location.machineId,
            harness: m.location.harness,
            session: m.location.sessionId,
          });
          const res = await request(await token(), `/v1/pipeline/chunks?${q}`, {
            method: "POST",
            headers: {
              "content-type": "application/octet-stream",
              "x-relay-trace-id": m.traceId,
              "x-machine-id": m.location.machineId,
              "x-session-id": m.location.sessionId,
            },
            body: chunk.data as Uint8Array<ArrayBuffer>,
          });
          if (res.status !== 200 && res.status !== 201) throw new OutcomeError(classify(res.status, await errorCode(res)));
          const sealed = (await res.json().catch(() => ({}))) as { entry?: ChunkEntry; scrub?: { ruleset: string; version: number } };
          const planned = entry.chunks.find((c) => c.seq === chunk.seq)!;
          const got = sealed.entry;
          if (!got || got.seq !== planned.seq || got.epoch !== planned.epoch || got.start_offset !== planned.start_offset ||
            got.bytes !== planned.bytes || got.sha256 !== planned.sha256 || !sealed.scrub)
            throw new OutcomeError({ kind: "failed", code: "sealed_chunk_mismatch" });
          state.chunks.push({ ...got, records: planned.records });
          state.chunks.sort((a, b) => a.seq - b.seq);
          state.scrub = state.scrub && state.scrub.version <= sealed.scrub.version ? state.scrub : sealed.scrub;
          await saveCheckpoint(cfg.checkpointPath, cp);
        }
        const manifest = await buildExportManifest(plan, m, state.chunks, state.scrub, { grantId: cfg.grantId, planDigest: digest });
        // The same contract over the exact manifest about to be sent must give
        // the plan's digest and this upload's signed identity.
        const inputs = plan.manifests.map(uploadFromPlanned);
        inputs[index] = uploadFromManifest(key, manifest);
        const sent = contractFn({ grantId: cfg.grantId, exportedAt: plan.exportedAt, uploads: inputs });
        if (sent.planDigest !== digest || canonical(sent.uploads[index]) !== canonical(identity))
          throw new OutcomeError({ kind: "failed", code: "manifest_plan_mismatch" });
        const issues = await validateManifest(manifest, key);
        if (issues.length) throw new OutcomeError({ kind: "failed", code: "manifest_invalid" });
        const res = await request(await token(), "/v1/pipeline/manifests", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-relay-trace-id": m.traceId,
            "x-machine-id": m.location.machineId,
            "x-session-id": m.location.sessionId,
          },
          body: JSON.stringify({ key, manifest }),
        });
        if (res.status !== 202) throw new OutcomeError(classify(res.status, await errorCode(res)));
        const body = (await res.json().catch(() => ({}))) as { workKey?: unknown };
        if (typeof body.workKey !== "string" || !body.workKey)
          throw new OutcomeError({ kind: "transient", code: "work_key_missing" });
        if (!body.workKey.startsWith(`${key}@`)) throw new OutcomeError({ kind: "failed", code: "work_key_mismatch" });
        cp.manifests[key] = { status: "committed", work_key: body.workKey, chunks: state.chunks.length };
        await saveCheckpoint(cfg.checkpointPath, cp);
        result.committed.push({ key, workKey: body.workKey });
        log.info("export manifest committed", { manifest_key_sha256: keyHash(key), work_key: body.workKey, chunk_count: state.chunks.length, attempt: replays + expiries + transient + 1 });
        outcome = "committed";
      } catch (error) {
        if (!(error instanceof OutcomeError)) throw error;
        const o = error.outcome;
        if (o.kind === "replayed") {
          // A replay means this assertion was spent; one fresh mint, same content.
          if (replays >= 1) outcome = { ...o, kind: "failed" };
          replays += 1;
        } else if (o.kind === "expired") {
          // Expired before use: one fresh re-mint and retry; a second expiry stops the run.
          if (expiries >= 1) outcome = { ...o, kind: "stop" };
          expiries += 1;
        } else if (o.kind === "transient") {
          if (transient >= backoff.length) outcome = { kind: "stop", code: o.code, ...(o.status ? { status: o.status } : {}) };
          else await sleep(backoff[transient]!);
          transient += 1;
        } else {
          outcome = o;
        }
        if (!outcome) log.warn("export manifest retrying with a fresh assertion", { manifest_key_sha256: keyHash(key), error_code: o.code, status: o.status, attempt: replays + expiries + transient });
      }
    }
    await saveCheckpoint(cfg.checkpointPath, cp);
    if (outcome === "committed") continue;
    if (outcome.kind === "failed") {
      result.failed.push({ key, code: outcome.code });
      log.error("export manifest FAILED; needs a human", { manifest_key_sha256: keyHash(key), error_code: outcome.code, status: outcome.status });
      continue;
    }
    result.stopped = { key, code: outcome.code, ...(outcome.status ? { status: outcome.status } : {}) };
    log.error("export upload STOPPED; no further manifests attempted", { manifest_key_sha256: keyHash(key), error_code: outcome.code, status: outcome.status });
    break;
  }
  return result;
}
