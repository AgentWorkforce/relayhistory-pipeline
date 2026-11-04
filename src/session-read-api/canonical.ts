/** Canonical S3 session projections, shared with the live S3 -> CH writer.
 * The successful claim-bound pointer RPC is the authentication boundary. No
 * object is read from an unverified JWT claim or caller-supplied object key.
 */
import { createHash } from "node:crypto";
import { readCanonicalScope, manifestUserId } from "./canonical-projection.ts";
import { canonicalManifestIdentity } from "../shared/manifest-identity.ts";
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
  revision: number | string;
  deleted_at: null;
}
export class CanonicalReadError extends Error {
  constructor(
    readonly reason: string,
    readonly status: 400 | 401 | 403 | 503 = 503,
  ) {
    super("Canonical session read refused");
    this.name = "CanonicalReadError";
  }
}
export interface CanonicalDeps {
  pointers(identity: CanonicalIdentity): Promise<CanonicalPointer[]>;
  store: ObjectStore;
  provider: string;
  bucket: string;
  prefix: { PIPELINE_ORG_FROM_PREFIX?: string; PIPELINE_KEY_PREFIX?: string };
  decompress(bytes: Uint8Array, maxBytes: number): Uint8Array;
}
export const CANONICAL_SCOPE_BYTES = 16 * 1024 * 1024;
const MAX_POINTERS = 64; // RPC has LIMIT 64 without a sentinel: refuse the boundary.
const digest = (value: string) => value.replace(/^\\x/u, "").toLowerCase();
function checked(
  pointers: CanonicalPointer[],
  identity: CanonicalIdentity,
  deps: CanonicalDeps,
): CanonicalPointer[] {
  if (!Array.isArray(pointers) || pointers.length >= MAX_POINTERS)
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
      p.raw_format !== "ndjson"
    )
      throw new CanonicalReadError("pointer_invalid");
    seen.add(p.manifest_key);
  }
  return pointers;
}
function generation(
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
/** Stable fingerprint of a pointer vector; callers must validate pointer authority first. */
export function canonicalPointerGeneration(identity: CanonicalIdentity, pointers: CanonicalPointer[]): string {
  return generation(pointers, identity);
}
/** Recheck the validated pointer vector without re-reading transcript objects. */
export async function canonicalGeneration(deps: CanonicalDeps, identity: CanonicalIdentity): Promise<string> {
  return generation(checked(await deps.pointers(identity), identity, deps), identity);
}

export async function canonicalSnapshot(
  deps: CanonicalDeps,
  identity: CanonicalIdentity,
) {
  const pointers = checked(await deps.pointers(identity), identity, deps),
    version = generation(pointers, identity);
  if (!pointers.length)
    return {
      generation: version,
      rows: [] as Array<{ table: string; row: Record<string, unknown> }>,
    };
  const byKey = new Map(pointers.map((p) => [p.manifest_key, p]));
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
          const m = read.manifest,
            canonical = canonicalManifestIdentity(m);
          // content_sha256 is the RAW CHUNK HASH CHAIN, not SHA256(manifest bytes).
          if (
            m.raw.content_sha256 !== digest(p.content_sha256) ||
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
            at.rest !== "manifest.json" ||
            !prefixAllowed(deps.prefix, at.prefix, identity.orgId)
          )
            throw new CanonicalReadError("prefix_mismatch");
          return read;
        },
      },
    },
    {
      maxScopeBytes: CANONICAL_SCOPE_BYTES,
      maxScopeEvents: 100000,
      maxRows: 500000,
      insertBatch: 5000,
    },
    0,
    identity.sessionId,
  );
  if (!result.complete || result.directorySessionId !== identity.sessionId)
    throw new CanonicalReadError("canonical_projection_incomplete");
  const fresh = checked(await deps.pointers(identity), identity, deps);
  if (generation(fresh, identity) !== version)
    throw new CanonicalReadError("source_generation_changed");
  return { generation: version, rows: result.projectionRows };
}
