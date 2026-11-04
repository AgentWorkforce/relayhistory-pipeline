/**
 * Canonical feature copy cycle: mirrors the legacy event-cycle checkpoint
 * structure, but reads the durable canonical journal (publication + retirement)
 * instead of the legacy Neon feature journal. One journal entry maps to one
 * bounded image page projection + one CH insert; the journal revision is the
 * CAS fence — entries at or below the checkpoint are stale and refused.
 */
import type { ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import {
  boundPage,
  projectBriefRow,
  projectBriefTombstone,
  projectEventRow,
  projectTombstone,
  type CanonicalProjectResult,
} from "./canonical-image.ts";
import {
  produceBriefFeatures,
  produceEventFeatures,
  type CanonicalBriefRow,
  type CanonicalBriefTombstone,
  type CanonicalEventRow,
  type CanonicalTombstone,
  type CanonicalImageEvidence,
  type CanonicalProduceResult,
} from "./canonical-producer.ts";
import type {
  CanonicalJournalEntry,
  CanonicalJournalPort,
} from "./canonical-version.ts";

/** Bounded port that returns one page of opaque canonical image rows. */
export interface CanonicalImagePort {
  /** event-pool projected rows for this image page; null = end of image. */
  events(imageKey: string, page: number): Promise<unknown[] | null>;
  /** brief-pool projected rows for this image page; null = end of image. */
  briefs(imageKey: string, page: number): Promise<unknown[] | null>;
  /** prior keyset for retire entries (exact tombstone targets). */
  priorEvents(imageKey: string): Promise<unknown[]>;
  priorBriefs(imageKey: string): Promise<unknown[]>;
}

export interface CanonicalCycleState {
  checkpoint: string;
}
export type CanonicalCycleResult =
  | { kind: "copied"; checkpoint: string; rows: number }
  | { kind: "refused"; reason: string }
  | { kind: "idle"; checkpoint: string };

const projectAll = <T>(
  page: unknown[],
  fn: (v: unknown) => { kind: "row"; row: T } | { kind: "refused"; reason: string },
): CanonicalProjectResult<T> => {
  const bounded = boundPage(page);
  if (bounded.kind !== "rows") return bounded;
  const out: T[] = [];
  for (const v of bounded.rows) {
    const r = fn(v);
    if (r.kind !== "row") return { kind: "refused", reason: r.reason };
    out.push(r.row);
  }
  return { kind: "rows", rows: out };
};

/**
 * Copy one journal entry into the event+brief pools. Stale entries (revision
 * at/below the checkpoint) refuse — a delayed writer can never resurrect.
 */
export async function copyCanonicalEntry(
  journal: CanonicalJournalPort,
  images: CanonicalImagePort,
  ch: ClickHouseClient,
  state: CanonicalCycleState,
  entry: CanonicalJournalEntry,
): Promise<CanonicalCycleResult> {
  if (BigInt(entry.revision) <= BigInt(state.checkpoint))
    return { kind: "refused", reason: "stale_revision" };
  const ev: CanonicalImageEvidence =
    entry.kind === "publish"
      ? {
          schemaGeneration: entry.schemaGeneration, imageKey: entry.imageKey,
          revision: entry.revision, sourceHash: entry.sourceHash,
        }
      : {
          schemaGeneration: entry.schemaGeneration, imageKey: entry.imageKey,
          revision: entry.revision, sourceHash: entry.priorKeysetHash,
        };
  const inserts: Record<string, unknown>[] = [];
  if (entry.kind === "publish") {
    const epage = await images.events(entry.imageKey, 0);
    if (epage === null) return { kind: "refused", reason: "image_unavailable" };
    const bpage = await images.briefs(entry.imageKey, 0);
    if (bpage === null) return { kind: "refused", reason: "image_unavailable" };
    const events = projectAll<CanonicalEventRow>(epage, projectEventRow);
    if (events.kind !== "rows") return events;
    const briefs = projectAll<CanonicalBriefRow>(bpage, projectBriefRow);
    if (briefs.kind !== "rows") return briefs;
    const e = produceEventFeatures(events.rows, [], ev);
    if (e.kind !== "rows") return e;
    const b = produceBriefFeatures(briefs.rows, [], ev);
    if (b.kind !== "rows") return b;
    inserts.push(...e.rows, ...b.rows);
  } else {
    const events = projectAll<CanonicalTombstone>(
      await images.priorEvents(entry.imageKey), projectTombstone);
    if (events.kind !== "rows") return events;
    const briefs = projectAll<CanonicalBriefTombstone>(
      await images.priorBriefs(entry.imageKey), projectBriefTombstone);
    if (briefs.kind !== "rows") return briefs;
    const e = produceEventFeatures(
      [], events.rows.map((r) => ({
        orgId: r.orgId, workspaceId: r.workspaceId, source: r.source,
        sessionId: r.sessionId, machineId: r.machineId, kind: r.kind,
        eventId: r.eventId, ts: r.ts,
      })), ev);
    if (e.kind !== "rows") return e;
    const b = produceBriefFeatures([], briefs.rows.map((r) => ({
      orgId: r.orgId, workspaceId: r.workspaceId, source: r.source,
      sessionId: r.sessionId,
    })), ev);
    if (b.kind !== "rows") return b;
    inserts.push(...e.rows, ...b.rows);
  }
  await ch.insert("session_event_features", inserts);
  return { kind: "copied", checkpoint: entry.revision, rows: inserts.length };
}
export { type CanonicalProduceResult };
