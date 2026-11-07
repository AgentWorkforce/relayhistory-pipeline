/**
 * The PR-evidence worker tick: claim due wakeups (armed by every canonical
 * publish while enabled), read each scope's exact canonical source through the
 * shared reader/projection (`readCanonicalScope`, the same code as search and
 * replay), extract the Neon-equivalent PR evidence, and install it bound to the
 * replay-source fingerprint it was read from.
 *
 * An unreadable, oversize or inconsistent source is installed as `incomplete`
 * with a code: Cloud then refuses to treat it as "no PRs". A source that moved
 * while reading is refused by the install fence and re-read on the next tick.
 * Logs carry counts and codes only.
 */
import { zstdDecompressSync } from "node:zlib";
import { PipelineError, type ManifestReader, type ObjectStore } from "../shared/vendor.ts";
import { readCanonicalScope } from "../session-read-api/canonical-projection.ts";
import { ARTIFACT_MAX_SOURCE_BYTES, ARTIFACT_MAX_SOURCE_RECORDS } from "../session-read-api/canonical-artifact.ts";
import { log } from "../shared/log.ts";
import { extractPullRequestEvidence, PR_EVIDENCE_EXTRACTOR_VERSION, type PullRequestEvidenceRow } from "./extract.ts";
import type { PrEvidenceClaim, PrEvidenceInstall, PrEvidenceStore } from "./store.ts";

export interface PrEvidenceDeps {
  store: PrEvidenceStore;
  reader: ManifestReader;
  objects: ObjectStore;
  prefixFor: (orgId: string) => string;
  decompress?: (stored: Uint8Array, maxOutputLength: number) => Uint8Array;
  limits?: { maxScopeBytes: number; maxScopeEvents: number };
  batch?: number;
}
export interface PrEvidenceTickResult { claimed: number; ready: number; incomplete: number; absent: number; refused: number; failed: number }

const defaultDecompress = (stored: Uint8Array, maxOutputLength: number) =>
  new Uint8Array(zstdDecompressSync(stored, { maxOutputLength }));
const OWNER = /^[A-Za-z0-9-]{1,255}$/, REPO = /^[A-Za-z0-9_.-]{1,255}$/;
const bytes = (s: string | null) => (s === null ? 0 : new TextEncoder().encode(s).byteLength);

/** The rows the 044000 table accepts; anything else is an incomplete build, never silently dropped. */
function storable(rows: PullRequestEvidenceRow[]): boolean {
  return rows.length <= 10_000 && rows.every((r) =>
    OWNER.test(r.owner) && REPO.test(r.repo) && bytes(r.head_ref) <= 4096 &&
    (r.tool_use_id === null || (bytes(r.tool_use_id) >= 1 && bytes(r.tool_use_id) <= 1024)) && bytes(r.tool_name) <= 1024);
}

/** Read, extract and classify one claimed scope. Retryable read failures throw. */
export async function buildPrEvidence(claim: PrEvidenceClaim, deps: PrEvidenceDeps): Promise<PrEvidenceInstall> {
  if (!claim.exists || claim.blocked) return { status: "absent", reason: claim.blocked ? "session_blocked" : "session_absent" };
  const scope = { orgId: claim.scope.orgId, workspaceId: claim.scope.workspaceId, harness: claim.scope.source, sessionId: claim.scope.sessionId };
  const limits = deps.limits ?? { maxScopeBytes: ARTIFACT_MAX_SOURCE_BYTES, maxScopeEvents: ARTIFACT_MAX_SOURCE_RECORDS };
  const read = await readCanonicalScope(scope, claim.manifestKeys,
    { reader: deps.reader, store: deps.objects, prefixFor: deps.prefixFor, decompress: deps.decompress ?? defaultDecompress },
    { ...limits, maxRows: limits.maxScopeEvents, insertBatch: 1000 }, 0, claim.scope.sessionId);
  // A live pointer whose manifest is still landing is transient: retry, never record it incomplete.
  if (read.reasons.includes("manifest_missing")) throw new PipelineError("manifest_missing", "a live pointer's manifest is missing", true);
  if (read.partial) return { status: "incomplete", reason: "scope_too_large" };
  if (!read.complete) return { status: "incomplete", reason: (read.reasons[0] ?? "source_incomplete").replace(/[^a-z0-9_]/g, "_").slice(0, 64) };
  const rows = extractPullRequestEvidence(read.projectionRows);
  if (!storable(rows)) return { status: "incomplete", reason: "evidence_not_storable" };
  return { status: "ready", rows };
}

export async function prEvidenceTick(deps: PrEvidenceDeps): Promise<PrEvidenceTickResult> {
  const result: PrEvidenceTickResult = { claimed: 0, ready: 0, incomplete: 0, absent: 0, refused: 0, failed: 0 };
  const claims = await deps.store.claim(deps.batch ?? 5);
  result.claimed = claims.length;
  for (const claim of claims) {
    let built: PrEvidenceInstall;
    try {
      built = await buildPrEvidence(claim, deps);
    } catch (error) {
      result.failed++;
      const code = error instanceof PipelineError && /^[a-z0-9_]{1,64}$/.test(error.code) ? error.code : "read_failed";
      await deps.store.release(claim, code).catch(() => false);
      continue;
    }
    if (built.status !== "absent" && claim.source.epoch === null) {
      result.failed++;
      await deps.store.release(claim, "source_epoch_missing").catch(() => false);
      continue;
    }
    if (built.status === "absent" && claim.source.epoch === null) {
      // Nothing was ever published for this scope; let the wakeup back off.
      result.absent++;
      await deps.store.release(claim, "session_absent").catch(() => false);
      continue;
    }
    const installed = await deps.store.install(claim, PR_EVIDENCE_EXTRACTOR_VERSION, built);
    if (!installed.ok) result.refused++;
    else result[built.status]++;
  }
  if (result.claimed) log.info("pr evidence tick", { stage: "session-pr-evidence", ...result });
  return result;
}
