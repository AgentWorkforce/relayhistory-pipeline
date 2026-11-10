import {
  isManifestRest,
  contentSha256Chain,
  validateManifest,
  type ManifestV1,
} from "../../packages/object-store/src/index.ts";
import { nodeReadBudget, type NodeReadProfile } from "./node-read-profile.ts";
/** Canonical S3 session projections, shared with the live S3 -> CH writer.
 * The successful claim-bound pointer RPC is the authentication boundary. No
 * object is read from an unverified JWT claim or caller-supplied object key.
 */
import { ARTIFACT_MAX_SOURCE_BYTES, ARTIFACT_MAX_SOURCE_RECORDS, ARTIFACT_MAX_SOURCE_POINTERS } from "./canonical-artifact.ts";
import { canonicalLegacy } from "./canonical-legacy.ts";
import { canonicalLiveTurns, mergeTurnRows } from "./canonical-turns.ts";
import { createHash } from "node:crypto";
import { readCanonicalScope, manifestUserId } from "./canonical-projection.ts";
import { canonicalManifestIdentity } from "../shared/manifest-identity.ts";
import { neonDerivedManifest } from "../shared/never-in-neon.ts";
import {
  objectStoreManifestReader,
  prefixAllowed,
} from "../../packages/pipeline/src/lib/pipeline/adapter.ts";
import type { ObjectStore } from "../../packages/object-store/src/index.ts";

export interface CanonicalIdentity {
  orgId: string;
  workspaceId: string;
  source: string;
  sessionId: string;
}
export interface CanonicalPointer {
  org_id: string;
  workspace_id: string;
  harness: string;
  session_id: string;
  machine_id: string;
  user_id: string;
  storage_provider: string;
  bucket: string;
  manifest_key: string;
  content_sha256: string;
  raw_format: string;
  /** Available to the Node catalog capture; legacy compact pointer RPC omits it. */
  manifest_etag?: string | null;
  revision: number | string;
  deleted_at: null;
}
export class CanonicalReadError extends Error {
  constructor(
    readonly reason: string,
    readonly status: 400 | 401 | 403 | 501 | 503 = 503,
    /** Projection reason codes behind `canonical_projection_incomplete` (codes only). */
    readonly details: readonly string[] = [],
  ) {
    super("Canonical session read refused");
    this.name = "CanonicalReadError";
  }
}
/** A reason code token: snake_case only, so no id, key, path or free text can pass. */
const REASON_CODE = /^[a-z][a-z0-9_]{0,63}$/u;
/**
 * Log fields for the codes behind a refusal (CanonicalReadError.details), e.g.
 * `{ reason_codes: "manifest_missing,scope_too_large" }`: deduplicated, in order,
 * non-code values dropped, `{}` when none survive. One string, so the shared log
 * sanitizer (src/shared/log.ts, allowlisted `reason_codes`) scrubs and truncates it.
 */
export function canonicalReasonCodeFields(codes: readonly unknown[] | undefined): { reason_codes?: string } {
  const kept = [...new Set((codes ?? []).filter((c): c is string => typeof c === "string" && REASON_CODE.test(c)))];
  return kept.length > 0 ? { reason_codes: kept.join(",").slice(0, 256) } : {};
}
export interface CanonicalDeps {
  pointers(identity: CanonicalIdentity): Promise<CanonicalPointer[]>;
  store: ObjectStore;
  provider: string;
  bucket: string;
  prefix: { PIPELINE_ORG_FROM_PREFIX?: string; PIPELINE_KEY_PREFIX?: string };
  decompress(bytes: Uint8Array, maxBytes: number): Uint8Array;
  /**
   * Self-host only (serviceCanonicalDeps, READ_API_CANONICAL_PIN_LAGGING_POINTER):
   * a manifest that advanced past its pointer row (the reconcile has not
   * re-indexed it yet) is read pinned to the pointer's generation, see
   * pinnedToPointer. Unset: any difference is `manifest_mismatch`, as before.
   */
  pinLaggingPointer?: boolean;
}
export const CANONICAL_SCOPE_BYTES = 16 * 1024 * 1024;
const MAX_POINTERS = 64; // RPC has LIMIT 64 without a sentinel: refuse the boundary.
const digest = (value: string) => value.replace(/^\\x/u, "").toLowerCase();
/**
 * `m` cut back to the generation a lagging pointer indexed, or null (the caller
 * then refuses with `manifest_mismatch`, as without pinning). Chunks are
 * append-only, so the generation at pointer revision `revision` is a chunk
 * prefix of `m`; it is pinned only when `m.revision > revision >= 1` and the
 * hash chain over some prefix chunks[0..k], k >= 1, equals `want` (the pointer's
 * content_sha256, lowercase hex). The pinned body carries the pointer revision
 * and the prefix's chunk_count, raw_bytes, stored_bytes, next_offset, epochs
 * (those opened before chunk k: first_seq is a chunk index) and epoch, and must
 * pass validateManifest. Everything else (session, user, extensions) is `m`'s
 * and is still checked against the pointer by the caller.
 */
export async function pinnedToPointer(
  m: ManifestV1,
  want: string,
  revision: number,
): Promise<ManifestV1 | null> {
  if (!Number.isSafeInteger(revision) || revision < 1 || !(m.revision > revision))
    return null;
  if (!/^[0-9a-f]{64}$/u.test(want)) return null;
  let k = 0;
  for await (const step of contentSha256Chain(m.raw.chunks)) {
    k++;
    if (step !== want) continue;
    const chunks = m.raw.chunks.slice(0, k);
    const last = chunks[k - 1]!;
    const pinned: ManifestV1 = {
      ...m,
      revision,
      raw: {
        ...m.raw,
        chunks,
        chunk_count: k,
        content_sha256: want,
        raw_bytes: chunks.reduce((n, c) => n + c.bytes, 0),
        stored_bytes: chunks.reduce((n, c) => n + c.stored_bytes, 0),
        next_offset: last.start_offset + last.bytes,
        epochs: m.raw.epochs.filter((e) => e.first_seq < k),
        epoch: last.epoch,
      },
    };
    return (await validateManifest(pinned)).length === 0 ? pinned : null;
  }
  return null;
}
export function checkedCanonicalPointers(
  pointers: CanonicalPointer[],
  identity: CanonicalIdentity,
  deps: CanonicalDeps,
  maxPointersExclusive = MAX_POINTERS,
  format: "ndjson" | "jsonl" = "ndjson",
): CanonicalPointer[] {
  if (!Array.isArray(pointers) || pointers.length >= maxPointersExclusive)
    throw new CanonicalReadError("pointer_bound");
  const seen = new Set<string>();
  for (const p of pointers) {
    if (
      p.org_id !== identity.orgId ||
      p.workspace_id !== identity.workspaceId ||
      p.harness !== identity.source ||
      p.session_id !== identity.sessionId
    )
      throw new CanonicalReadError("tenant_mismatch");
    if (
      p.deleted_at !== null ||
      p.storage_provider !== deps.provider ||
      p.bucket !== deps.bucket ||
      typeof p.manifest_key !== "string" ||
      seen.has(p.manifest_key) ||
      typeof p.content_sha256 !== "string" ||
      !/^([0-9a-f]{64})$/u.test(digest(p.content_sha256)) ||
      !Number.isSafeInteger(Number(p.revision)) ||
      Number(p.revision) < 1 ||
      p.raw_format !== format
    )
      throw new CanonicalReadError("pointer_invalid");
    seen.add(p.manifest_key);
  }
  return pointers;
}
export function canonicalSourceGeneration(
  pointers: CanonicalPointer[],
  identity: CanonicalIdentity,
): string {
  return createHash("sha256")
    .update(
      JSON.stringify([
        identity,
        ...pointers
          .map((p) => [
            p.manifest_key,
            String(p.revision),
            digest(p.content_sha256),
            p.machine_id,
            p.user_id,
            p.bucket,
            p.storage_provider,
            p.raw_format,
          ])
          .sort((a, b) => String(a[0]).localeCompare(String(b[0]))),
      ]),
    )
    .digest("hex");
}
/**
 * Live raw read (/canonical raw, timing, turns). Display values are projected only when the
 * caller asks for them (`directoryValues: true`, i.e. table=session_directory_event_values);
 * every other read does no display work and returns exactly the pre-#184 snapshot. When
 * projected, a display-value refusal omits those rows (`displayOmitted`) instead of failing.
 */
export const CANONICAL_SCOPE_EVENTS = 100000;
export function canonicalSnapshot(
  deps: CanonicalDeps,
  identity: CanonicalIdentity,
  /**
   * scopeBytes / scopeEvents: self-host only (serviceCanonicalDeps,
   * READ_API_CANONICAL_SCOPE_BYTES / _EVENTS, validated there at startup).
   * Absent: CANONICAL_SCOPE_BYTES / CANONICAL_SCOPE_EVENTS, as before.
   */
  options: { directoryValues?: boolean; scopeBytes?: number; scopeEvents?: number } = {},
) {
  return snapshotWithLimits(
    deps, identity,
    options.scopeBytes ?? CANONICAL_SCOPE_BYTES, options.scopeEvents ?? CANONICAL_SCOPE_EVENTS,
    MAX_POINTERS, 500000, options.directoryValues === true ? "optional" : undefined,
  );
}
/** Trusted Node producer only. Never called by the raw Worker request path. */
export function canonicalProducerSnapshot(deps: CanonicalDeps, identity: CanonicalIdentity, profile: NodeReadProfile = "standard") {
  const budget=nodeReadBudget(profile);
  return snapshotWithLimits(deps, identity, budget.sourceBytes, budget.sourceRecords, ARTIFACT_MAX_SOURCE_POINTERS + 1,budget.projectedRows,"required");
}
async function snapshotWithLimits(
  deps: CanonicalDeps, identity: CanonicalIdentity,
  maxScopeBytes: number, maxScopeEvents: number, maxPointersExclusive: number, maxRows: number,
  directoryValues: "required" | "optional" | undefined,
) {
  const pointers = checkedCanonicalPointers(await deps.pointers(identity), identity, deps, maxPointersExclusive),
    version = canonicalSourceGeneration(pointers, identity);
  if (!pointers.length)
    return {
      generation: version,
      exists: false,
      legacyCoverage: null,
      neonDerived: false,
      rows: [] as Array<{ table: string; row: Record<string, unknown> }>,
    };
  const byKey = new Map(pointers.map((p) => [p.manifest_key, p]));
  // Whether any manifest of the scope was written by the Neon export/sync (never-in-Neon proof).
  let neonDerived = false;
  const reader = objectStoreManifestReader(deps.store, {
    provider: deps.provider as "s3",
    bucket: deps.bucket,
    maxBytes: 1024 * 1024,
  });
  const result = await readCanonicalScope(
    {
      orgId: identity.orgId,
      workspaceId: identity.workspaceId,
      harness: identity.source,
      sessionId: identity.sessionId,
    },
    [...byKey.keys()],
    {
      store: deps.store,
      prefixFor: () =>
        deps.prefix.PIPELINE_ORG_FROM_PREFIX === "true"
          ? [deps.prefix.PIPELINE_KEY_PREFIX, identity.orgId]
              .filter(Boolean)
              .join("/")
          : (deps.prefix.PIPELINE_KEY_PREFIX ?? ""),
      decompress: deps.decompress,
      reader: {
        ...reader,
        async read(key) {
          const p = byKey.get(key);
          if (!p) throw new CanonicalReadError("pointer_invalid");
          const read = await reader.read(key);
          if (!read) throw new CanonicalReadError("object_missing");
          let m = read.manifest;
          if (deps.pinLaggingPointer && m.raw.content_sha256 !== digest(p.content_sha256))
            m = (await pinnedToPointer(m, digest(p.content_sha256), Number(p.revision))) ?? m;
          const canonical = canonicalManifestIdentity(m);
          if (neonDerivedManifest(m)) neonDerived = true;
          // content_sha256 is the RAW CHUNK HASH CHAIN, not SHA256(manifest bytes).
          if (
            m.raw.content_sha256 !== digest(p.content_sha256) ||
            (p.manifest_etag != null && read.etag !== p.manifest_etag) ||
            manifestUserId(m) !== p.user_id ||
            String(m.revision) !== String(p.revision) ||
            canonical.sessionId !== p.session_id ||
            canonical.machineId !== p.machine_id ||
            m.session.workspace_id !== identity.workspaceId ||
            m.session.harness !== identity.source
          )
            throw new CanonicalReadError("manifest_mismatch");
          const { parseSessionKey } =
            await import("../../packages/object-store/src/index.ts");
          const at = parseSessionKey(key);
          if (
            !at ||
            !isManifestRest(at.rest) ||
            !prefixAllowed(deps.prefix, at.prefix, identity.orgId)
          )
            throw new CanonicalReadError("prefix_mismatch");
          return m === read.manifest ? read : { ...read, manifest: m };
        },
      },
    },
    {
      maxScopeBytes,
      maxScopeEvents,
      maxRows,
      insertBatch: 5000,
      ...(directoryValues ? { directoryValues } : {}),
    },
    0,
    identity.sessionId,
  );
  if (!result.complete || result.directorySessionId !== identity.sessionId)
    throw new CanonicalReadError("canonical_projection_incomplete", 503, result.reasons);
  const fresh = checkedCanonicalPointers(await deps.pointers(identity), identity, deps, maxPointersExclusive);
  if (canonicalSourceGeneration(fresh, identity) !== version)
    throw new CanonicalReadError("source_generation_changed");
  const legacy = canonicalLegacy(result.records, identity);
  // Live relay session turns (canonical-turns.ts) replace the legacy row of the same key.
  const live = canonicalLiveTurns(result.records, identity);
  return {
    generation: version,
    exists: true,
    legacyCoverage: legacy.coverage,
    rows: [...result.projectionRows, ...mergeTurnRows(legacy.rows, live)],
    // Always present: the never-in-Neon objects half fails closed on absence (evidence.ts).
    neonDerived,
    /** Bounded directory_display_* code when display rows were omitted (raw read only). */
    ...(result.displayOmitted ? { displayOmitted: result.displayOmitted } : {}),
  };
}
