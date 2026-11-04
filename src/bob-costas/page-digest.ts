/** Bounded consumer port for canonicalArtifactPage's authoritative replay view.
 * The composed caller must bind readPage to the trusted publication/identity
 * reader. This module neither decodes/folds raw data nor grants SQL authority.
 * It is not activated until the canonical foundation and Native fences compose.
 */
import { createHash } from "node:crypto";
import { PipelineError } from "../shared/vendor.ts";
import { addProjectedDigestRecord, newDigest, type SessionDigest } from "./summarizer.ts";

export interface DigestArtifactIdentity {
  orgId: string; workspaceId: string; source: string; sessionId: string;
}
export interface DigestArtifactQuery {
  table: "convergence_events"; view: "replay"; order: "asc";
  limit: 1000; maxContent: 2000; generation: string; cursor?: string;
}
export interface DigestArtifactPage {
  generation: string; exists: boolean;
  rows: { table: string; row: Record<string, unknown> }[];
  nextCursor: string | null;
}
const MAX_PAGES = 10_000;
const hash = (v: string) => createHash("sha256").update(v).digest("hex");
const invalid = (): never => { throw new PipelineError("summary_artifact_invalid", "the canonical summary page contract was refused", false); };
const stale = (): never => { throw new PipelineError("summary_artifact_stale", "the canonical summary publication changed", true); };
const obj = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);

/** Return a complete digest or throw; never publish a partially read session.
 * generation is the artifact SHA-256, distinct from schema/upload generations.
 * The authoritative page reader rechecks publication currentness on every page;
 * the final Bob pointer still needs Native's independent short SQL write fence.
 */
export async function pageDigest(args: {
  identity: DigestArtifactIdentity;
  generation: string;
  readPage(query: DigestArtifactQuery, signal: AbortSignal): Promise<DigestArtifactPage>;
  signal: AbortSignal;
  /** A smaller host budget may refuse earlier; it may not raise the hard cap. */
  maxPages?: number;
}): Promise<SessionDigest> {
  const identity = { orgId: args.identity?.orgId, workspaceId: args.identity?.workspaceId,
    source: args.identity?.source, sessionId: args.identity?.sessionId }, generation = args.generation;
  if (!Object.values(identity).every(v => typeof v === "string" && v.length > 0) ||
      typeof generation !== "string" || !/^[0-9a-f]{64}$/.test(generation)) invalid();
  const maxPages = args.maxPages ?? MAX_PAGES;
  if (!Number.isSafeInteger(maxPages) || maxPages < 1 || maxPages > MAX_PAGES) invalid();
  const digest = newDigest(identity.source), seen = new Set<string>();
  let cursor: string | undefined;
  for (let pageNo = 0; pageNo < maxPages; pageNo++) {
    args.signal.throwIfAborted();
    const page = await args.readPage({
      table: "convergence_events", view: "replay", order: "asc",
      limit: 1000, maxContent: 2000, generation, ...(cursor ? { cursor } : {}),
    }, args.signal);
    args.signal.throwIfAborted();
    if (!obj(page)) invalid();
    if (page.exists !== true || page.generation !== generation) stale();
    if (!Array.isArray(page.rows) || page.rows.length > 1000 ||
        !(page.nextCursor === null || (typeof page.nextCursor === "string" && page.nextCursor.length > 0 && page.nextCursor.length <= 4096))) invalid();
    for (const item of page.rows) {
      if (!obj(item) || item.table !== "convergence_events" || !obj(item.row)) invalid();
      const row = item.row;
      // Official projections can omit org/workspace; the trusted reader binds
      // those in its publication envelope. Never accept conflicting copies.
      if (row.source !== identity.source || row.session_id !== identity.sessionId ||
          (row.org_id !== undefined && row.org_id !== identity.orgId) ||
          (row.workspace_id !== undefined && row.workspace_id !== identity.workspaceId) ||
          (row.ts != null && (typeof row.ts !== "string" || row.ts.length > 64)) ||
          (row.content != null && (typeof row.content !== "string" || row.content.length > 2001))) invalid();
      addProjectedDigestRecord(digest, row);
    }
    if (page.nextCursor === null) return digest;
    if (page.rows.length === 0) invalid();
    const next = hash(page.nextCursor);
    if (seen.has(next)) invalid();
    seen.add(next);
    cursor = page.nextCursor;
  }
  throw new PipelineError("summary_artifact_budget", "the canonical summary exceeded its verified page budget", false);
}
