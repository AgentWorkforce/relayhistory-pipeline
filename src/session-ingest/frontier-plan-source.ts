/**
 * Read-only source adapters for publication-frontier evidence.
 *
 * This module accepts identities and bounds, never conclusions. Completeness
 * comes from the seed RPC's second scope capture, object immutability comes
 * from the version returned with the exact manifest GET, and bucket
 * versioning comes from a signed S3 status read.
 */
import type { ManifestRead, ManifestReader } from "../../packages/pipeline/src/lib/pipeline/adapter.ts";
import type { AwsCredentials } from "../../packages/object-store/src/sigv4.ts";
import { signRequest } from "../../packages/object-store/src/sigv4.ts";
import { sha256Hex } from "../../packages/object-store/src/crypto.ts";
import type { FrontierExportBinding, FrontierExportRoot, FrontierTenant } from "./frontier-plan.ts";
import { frontierExportRecordsSha256, frontierExportRootSha256, frontierSha256 } from "./frontier-plan.ts";
import { SeedProofIndex, type ProofDeps, type SeedEvidence, type SeedPointer, type SeedProofBudget } from "./seed-proof.ts";
import type { SeedSql } from "./seed.ts";

const HEX40 = /^[a-f0-9]{40}$/;
const HEX64 = /^[a-f0-9]{64}$/;
const CONTROL = /[\u0000-\u001f\u007f]/u;

export class FrontierSourceEvidenceError extends Error {
  constructor(readonly code: string) { super(code); }
}
function fail(code: string): never { throw new FrontierSourceEvidenceError(code); }

interface SeedScopeCapture {
  protocol: 1;
  capture_id: string | null;
  expires_at: string | null;
  fingerprint: string;
  source_epoch: string;
  pointers: SeedPointer[];
}

export interface FrontierBucketVersioningReader {
  /** Reads the current bucket setting from its authoritative API. */
  read(bucket: string): Promise<"Enabled" | "Disabled" | "Unknown">;
}

export interface S3FrontierVersioningConfig {
  region: string;
  credentials: AwsCredentials;
  /** Path-style endpoint override, for example MinIO. */
  endpoint?: string;
  /** Test-only path-style endpoint; must resolve to literal loopback. */
  allowFixtureEndpoint?: boolean;
  fetch?: typeof fetch;
  maxBytes?: number;
  /** Synthetic credentials only; production HTTP endpoints are forbidden. */
  allowInsecureLoopbackFixture?: boolean;
}

/** Signed, bounded, read-only GetBucketVersioning adapter. */
export function createS3FrontierVersioningReader(config: S3FrontierVersioningConfig): FrontierBucketVersioningReader {
  const doFetch = config.fetch ?? ((...args: Parameters<typeof fetch>) => fetch(...args));
  const maxBytes = config.maxBytes ?? 64 * 1024;
  if (!/^[a-z0-9-]{1,63}$/.test(config.region)) fail("frontier_versioning_region");
  const endpoint = config.endpoint === undefined ? undefined : new URL(config.endpoint);
  const loopback = endpoint && ["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname);
  const fixtureEndpoint = config.allowFixtureEndpoint === true || config.allowInsecureLoopbackFixture === true;
  if (endpoint && (!fixtureEndpoint || !["http:", "https:"].includes(endpoint.protocol) || !loopback || endpoint.username || endpoint.password || endpoint.search || endpoint.hash))
    fail("frontier_versioning_endpoint");
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > 1024 * 1024) fail("frontier_versioning_bound");
  return {
    async read(bucket) {
      if (!/^[a-z0-9][a-z0-9.-]{1,253}[a-z0-9]$/.test(bucket)) fail("frontier_bucket_identity");
      const url = endpoint
        ? new URL(`${endpoint.toString().replace(/\/+$/, "")}/${encodeURIComponent(bucket)}`)
        : new URL(`https://${bucket}.s3.${config.region}.amazonaws.com/`);
      url.searchParams.set("versioning", "");
      const headers = new Headers();
      await signRequest({ method: "GET", url, headers, payloadSha256: await sha256Hex(new Uint8Array()), region: config.region, credentials: config.credentials });
      const response = await doFetch(url, { method: "GET", headers, redirect: "error", signal: AbortSignal.timeout(10_000) });
      if (response.redirected || (response.url && response.url !== url.toString())) fail("frontier_versioning_identity");
      if (!response.ok) fail(`frontier_versioning_http_${response.status}`);
      const body = response.body;
      if (body === null) fail("frontier_versioning_body");
      const reader = body.getReader();
      const chunks: Uint8Array[] = [];
      let size = 0;
      try {
        for (;;) {
          const part = await reader.read();
          if (part.done) break;
          size += part.value.byteLength;
          if (size > maxBytes) fail("frontier_versioning_bound");
          chunks.push(part.value);
        }
      } finally {
        await reader.cancel().catch(() => {});
        reader.releaseLock();
      }
      const bytes = new Uint8Array(size);
      let offset = 0;
      for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
      let xml: string;
      try { xml = new TextDecoder("utf-8", { fatal: true }).decode(bytes); }
      catch { fail("frontier_versioning_encoding"); }
      const document = /^\s*(?:<\?xml[^?]*\?>\s*)?<VersioningConfiguration(?:\s+xmlns=(?:"[^"]*"|'[^']*'))?\s*>([\s\S]*)<\/VersioningConfiguration>\s*$/.exec(xml!);
      if (document === null) fail("frontier_versioning_document");
      const contents = document[1]!;
      if (/<\/?(?!Status\b)[A-Za-z_][\w:.-]*/.test(contents)) fail("frontier_versioning_document");
      const statuses = [...contents.matchAll(/<Status\s*>\s*([^<]*)\s*<\/Status\s*>/g)].map(match => match[1]!.trim());
      if (statuses.length !== 1 || contents.replace(/<Status\s*>[\s\S]*?<\/Status\s*>/, "").trim()) fail("frontier_versioning_document");
      return statuses[0] === "Enabled" ? "Enabled" : statuses[0] === "Suspended" ? "Disabled" : "Unknown";
    },
  };
}

export interface CaptureSeedScopeExportInput {
  tenant: FrontierTenant;
  generation: string;
  sourceHead: string;
  source: string;
  sessionId: string;
  keyRoot: string;
  /** Caller-owned local SQLite path. No source or destination write occurs. */
  cachePath: string;
  maxRecords: number;
  maxEvidenceBytes: number;
  /** Preferred wall-clock bound. `maxDurationMs` is retained for the capture CLI. */
  deadlineMs?: number;
  maxDurationMs?: number;
  kind?: FrontierExportRoot["kind"];
}

export interface SeedScopeExportEvidence {
  roots: FrontierExportRoot[];
  bindings: FrontierExportBinding[];
  records: Array<{ evidence: SeedEvidence; exportRootSha256: string }>;
  sourceEpoch: string;
  scopeFingerprint: string;
  snapshotSha256: string;
  pointerCount: number;
  recordCount: number;
  versioningOpeningSha256: string;
  versioningClosingSha256: string;
}

function validIdentity(value: string, empty = false): boolean {
  return typeof value === "string" && (empty || value.length > 0) && Buffer.byteLength(value) <= 512 && !CONTROL.test(value);
}

function validateCapture(capture: SeedScopeCapture, input: CaptureSeedScopeExportInput): void {
  if (capture?.protocol !== 1 || capture.capture_id !== null || capture.expires_at !== null ||
      !HEX64.test(capture?.fingerprint ?? "") || !/^(0|[1-9][0-9]{0,18})$/.test(capture?.source_epoch ?? "") ||
      !Array.isArray(capture?.pointers) || capture.pointers.length > 1000) fail("frontier_seed_capture_shape");
  const ids = new Set<string>();
  for (const pointer of capture.pointers) {
    const pointerId = String(pointer.capture?.pointer_id ?? "");
    if (pointer.org_id !== input.tenant.orgId || pointer.workspace_id !== input.tenant.workspaceId ||
        pointer.harness !== input.source || pointer.session_id !== input.sessionId ||
        !HEX64.test(pointerId) || ids.has(pointerId)) fail("frontier_seed_capture_namespace");
    ids.add(pointerId);
  }
}

/**
 * Captures all immutable roots that back the target origin's current seed
 * maxima. Every conclusion is derived from the source RPC, verified bytes,
 * same-response manifest version, or bucket API; none is passed as a boolean.
 */
export async function captureSeedScopeExportEvidence(
  sql: SeedSql,
  deps: ProofDeps,
  versioning: FrontierBucketVersioningReader,
  input: CaptureSeedScopeExportInput,
): Promise<SeedScopeExportEvidence> {
  const { tenant } = input;
  if (![tenant.orgId, tenant.workspaceId, tenant.userId, tenant.originId, input.source, input.sessionId].every(value => validIdentity(value, value === tenant.workspaceId)) ||
      !HEX64.test(input.generation) || !HEX40.test(input.sourceHead) || !validIdentity(input.keyRoot) ||
      input.keyRoot.split("/").some(part => part === "." || part === "..") || !validIdentity(input.cachePath) ||
      (input.kind !== undefined && input.kind !== "legacy-export" && input.kind !== "buffered-stage")) fail("frontier_seed_identity");
  const maxRecords = input.maxRecords;
  const maxEvidenceBytes = input.maxEvidenceBytes;
  const durations = [input.deadlineMs, input.maxDurationMs].filter((value): value is number => value !== undefined);
  const maxDurationMs = durations.length ? Math.min(...durations) : undefined;
  if (!Number.isSafeInteger(maxRecords) || maxRecords < 0 || maxRecords > 10_000_000 || !Number.isSafeInteger(maxEvidenceBytes) || maxEvidenceBytes < 1 || maxEvidenceBytes > 256 * 1024 * 1024 || !Number.isSafeInteger(maxDurationMs) || maxDurationMs! < 1 || maxDurationMs! > 60 * 60 * 1000) fail("frontier_seed_bound");
  if (deps.provider !== "s3" || deps.reader.provider !== "s3" || deps.reader.bucket !== deps.bucket) fail("frontier_seed_store_identity");
  const budget: SeedProofBudget = { records: 0, evidenceBytes: 0, maxRecords, maxEvidenceBytes, deadlineAtMs: Date.now() + maxDurationMs! };
  const checkDeadline = (): void => { if (Date.now() >= budget.deadlineAtMs) fail("frontier_seed_deadline"); };
  const rpc = async (): Promise<SeedScopeCapture> => {
    checkDeadline();
    const params = { org_id: tenant.orgId, workspace_id: tenant.workspaceId, source: input.source, session_id: input.sessionId, generation: input.generation, mode: "preview" };
    const result = await sql.query<{ r: SeedScopeCapture }>("SELECT sessions.ingest_seed_scope_capture($1::jsonb) AS r", [JSON.stringify(params)]);
    const capture = result.rows[0]?.r;
    if (!capture || result.rows.length !== 1) fail("frontier_seed_rpc_shape");
    checkDeadline(); validateCapture(capture, input);
    return capture;
  };

  checkDeadline();
  const openingVersioning = await versioning.read(deps.bucket);
  checkDeadline();
  if (openingVersioning !== "Enabled") fail("frontier_seed_bucket_unversioned");
  const opening = await rpc();
  checkDeadline();
  const versions = new Map<string, string>();
  const capturingReader: ManifestReader = {
    provider: deps.reader.provider,
    ...(deps.reader.bucket ? { bucket: deps.reader.bucket } : {}),
    async read(key): Promise<ManifestRead | null> {
      const value = await deps.reader.read(key);
      if (!value) return null;
      if (!value.version || value.version === "null" || Buffer.byteLength(value.version) > 1024 || CONTROL.test(value.version)) fail("frontier_seed_manifest_version");
      const version = value.version;
      const prior = versions.get(key);
      if (prior !== undefined && prior !== version) fail("frontier_seed_object_race");
      versions.set(key, version);
      return value;
    },
  };
  const index = new SeedProofIndex(input.cachePath, tenant.orgId, input.sourceHead);
  try {
    index.restartVerification();
    for (const pointer of opening.pointers) await index.addPointer(pointer, { ...deps, reader: capturingReader }, budget);
    const closing = await rpc();
    checkDeadline();
    if (closing.fingerprint !== opening.fingerprint || closing.source_epoch !== opening.source_epoch) fail("frontier_seed_source_changed");
    const sealed = index.seal(closing.pointers);
    if (sealed.non_delivery_pointers !== 0 || sealed.unseeded_records !== 0) fail("frontier_seed_unknown_records");
    checkDeadline();
    const closingVersioning = await versioning.read(deps.bucket);
    checkDeadline();
    if (closingVersioning !== "Enabled") fail("frontier_seed_bucket_changed");

    const pointers = new Map(opening.pointers.map(pointer => [String(pointer.capture.pointer_id), pointer]));
    const byPointer = new Map(opening.pointers.map(pointer => [String(pointer.capture.pointer_id), [] as SeedEvidence[]]));
    let enumeratedRecords = 0;
    let enumeratedBytes = 0;
    for (const batch of index.evidenceBatches()) for (const record of batch.records) {
      checkDeadline();
      enumeratedRecords++;
      enumeratedBytes += Buffer.byteLength(JSON.stringify(record));
      if (enumeratedRecords > maxRecords || enumeratedBytes > maxEvidenceBytes) fail("frontier_seed_bound");
      if (record.org_id !== tenant.orgId || record.workspace_id !== tenant.workspaceId || record.user_id !== tenant.userId || record.origin_id !== tenant.originId) continue;
      const records = byPointer.get(record.pointer_id) ?? [];
      records.push(record);
      byPointer.set(record.pointer_id, records);
    }
    if (enumeratedRecords !== sealed.records) fail("frontier_seed_terminal_coverage");
    const roots: FrontierExportRoot[] = [];
    const bindings: FrontierExportBinding[] = [];
    const evidenceRecords: Array<{ evidence: SeedEvidence; exportRootSha256: string }> = [];
    let outputEvidenceBytes = 0;
    for (const pointerId of [...byPointer.keys()].sort()) {
      checkDeadline();
      const pointer = pointers.get(pointerId);
      const records = byPointer.get(pointerId)!;
      if (pointer === undefined) fail("frontier_seed_root_identity");
      const versionId = versions.get(pointer.manifest_key);
      if (pointer.user_id !== tenant.userId || versionId === undefined ||
          !(pointer.manifest_key === input.keyRoot || pointer.manifest_key.startsWith(`${input.keyRoot}/`))) fail("frontier_seed_root_identity");
      const root: FrontierExportRoot = {
        tenant: { ...tenant }, kind: input.kind ?? "legacy-export", provider: "s3", bucket: pointer.bucket,
        keyRoot: input.keyRoot, manifestKey: pointer.manifest_key, source: pointer.harness, sessionId: pointer.session_id,
        machineId: pointer.machine_id, versionId, manifestRevision: pointer.revision, contentSha256: pointer.content_sha256,
        recordCount: records.length, inventoryComplete: true, versioning: "Enabled",
      };
      const rootSha256 = frontierExportRootSha256(root);
      roots.push(root);
      bindings.push({
        exportRootSha256: rootSha256,
        recordsSha256: frontierExportRecordsSha256(records.map(evidence => ({
          tenant: { orgId: evidence.org_id, workspaceId: evidence.workspace_id, userId: evidence.user_id, originId: evidence.origin_id },
          recordId: evidence.record_id, revision: evidence.revision, source: evidence.source,
          sessionId: evidence.session_id, operation: evidence.operation,
        }))),
      });
      for (const evidence of records) {
        const item = { evidence, exportRootSha256: rootSha256 };
        outputEvidenceBytes += Buffer.byteLength(JSON.stringify(item));
        if (evidenceRecords.length >= maxRecords || outputEvidenceBytes > maxEvidenceBytes || Date.now() > budget.deadlineAtMs) fail("frontier_seed_bound");
        evidenceRecords.push(item);
      }
    }
    roots.sort((a, b) => a.manifestKey < b.manifestKey ? -1 : a.manifestKey > b.manifestKey ? 1 : (a.versionId! < b.versionId! ? -1 : a.versionId! > b.versionId! ? 1 : 0));
    evidenceRecords.sort((a, b) => a.evidence.record_id < b.evidence.record_id ? -1 : a.evidence.record_id > b.evidence.record_id ? 1 : 0);
    if (enumeratedRecords !== sealed.records || roots.length !== sealed.pointers) fail("frontier_seed_record_closure");
    const versioningOpeningSha256 = frontierSha256({ bucket: deps.bucket, status: openingVersioning });
    const versioningClosingSha256 = frontierSha256({ bucket: deps.bucket, status: closingVersioning });
    const result: SeedScopeExportEvidence = {
      roots, bindings, records: evidenceRecords, sourceEpoch: opening.source_epoch, scopeFingerprint: opening.fingerprint,
      snapshotSha256: frontierSha256({ protocol: 1, seedSnapshotSha256: sealed.snapshot_sha256, scopeFingerprint: opening.fingerprint, sourceEpoch: opening.source_epoch, versions: [...versions].sort() }),
      pointerCount: sealed.pointers, recordCount: evidenceRecords.length,
      versioningOpeningSha256, versioningClosingSha256,
    };
    checkDeadline();
    if (Buffer.byteLength(JSON.stringify(result)) > maxEvidenceBytes) fail("frontier_seed_evidence_bound");
    checkDeadline();
    return result;
  } finally {
    index.close();
  }
}
