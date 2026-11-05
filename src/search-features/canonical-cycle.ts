/**
 * Canonical feature copy cycle: journal-driven, mirrors event-cycle's
 * checkpoint structure, reads the durable canonical journal. Native's
 * paged/terminal/authority contract: bounded imagePages/priorKeyPages with
 * explicit terminal cursors + pageSha256, per-page ackPage, and finish() CAS
 * requiring both pool terminal descriptors + all page ACKs + still-current
 * source authority + unchanged bindings. A stale revision at/below the
 * checkpoint is fenced before any write.
 */
import type { ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import { createHash } from "node:crypto";
import { EVENT_FEATURES_TABLE } from "./event-schema.ts";
import { BRIEF_FEATURES_TABLE } from "./brief-schema.ts";
import {
  boundPage,
  projectBriefRow,
  projectBriefTombstone,
  projectEventRow,
  projectTombstone,
} from "./canonical-image.ts";
import {
  produceBriefFeatures,
  produceEventFeatures,
  type CanonicalBriefRow,
  type CanonicalBriefTombstone,
  type CanonicalEventRow,
  type CanonicalImageEvidence,
  type CanonicalTombstone,
  type CanonicalProduceResult,
} from "./canonical-producer.ts";
import type {
  CanonicalJournalEntry,
  CanonicalPageAckPort,
  CanonicalSourceAuthority,
} from "./canonical-version.ts";

/** One bounded image page: rows, explicit terminal cursor, CAS hashes. */
export interface CanonicalImagePage {
  rows: unknown[];
  cursor: string | null;      // null = terminal
  source: string;
  pointerSetHash: string;
  pageSha256: string;
}
/** The paged image/prior-keyset port — bounded, never a whole image at once. */
export interface CanonicalImagePort {
  imagePages(
    entry: CanonicalJournalEntry, kind: "event" | "brief", cursor: string | null,
  ): Promise<CanonicalImagePage>;
  priorKeyPages(
    entry: CanonicalJournalEntry, kind: "event" | "brief", cursor: string | null,
  ): Promise<CanonicalImagePage>;
  /** A verified complete-image descriptor — a null first page is a valid
   *  empty pool ONLY when this exists. */
  imageDescriptor(entry: CanonicalJournalEntry): Promise<
    { imageKey: string; sourceHash: string; pointerSetHash: string; complete: boolean } | null>;
}

export interface CanonicalCycleState { checkpoint: string; }
export type CanonicalCycleResult =
  | { kind: "copied"; checkpoint: string; rows: number }
  | { kind: "refused"; reason: string }
  | { kind: "idle"; checkpoint: string };

const projectAll = <T>(
  page: unknown[],
  fn: (v: unknown) => { kind: "row"; row: T } | { kind: "refused"; reason: string },
) => {
  const bounded = boundPage(page);
  if (bounded.kind !== "rows") return bounded;
  const out: T[] = [];
  for (const v of bounded.rows) {
    const r = fn(v);
    if (r.kind !== "row") return { kind: "refused" as const, reason: r.reason };
    out.push(r.row);
  }
  return { kind: "rows" as const, rows: out };
};

const hashRows = (rows: unknown[]) =>
  createHash("sha256").update(JSON.stringify(rows)).digest("hex");
const hash = (v: unknown): v is string =>
  typeof v === "string" && /^[a-f0-9]{64}$/.test(v);

/**
 * Copy one journal entry into the event+brief pools, page-by-page with
 * per-page ack + terminal finish CAS. Stale entries refuse before any write.
 */
export async function copyCanonicalEntry(
  images: CanonicalImagePort,
  acks: CanonicalPageAckPort,
  ch: ClickHouseClient,
  state: CanonicalCycleState,
  entry: CanonicalJournalEntry,
): Promise<CanonicalCycleResult> {
  if (BigInt(entry.revision) <= BigInt(state.checkpoint))
    return { kind: "refused", reason: "stale_revision" };
  const descriptor = await images.imageDescriptor(entry);
  if (!descriptor?.complete || descriptor.imageKey !== entry.imageKey ||
      descriptor.pointerSetHash !== entry.pointerSet.hash ||
      !hash(descriptor.sourceHash) ||
      (entry.kind === "publish" && descriptor.sourceHash !== entry.sourceHash))
    return { kind: "refused", reason: "image_descriptor_changed" };
  const ev: CanonicalImageEvidence = {
    schemaGeneration: entry.schemaGeneration,
    imageKey: entry.imageKey,
    revision: entry.revision,
    sourceHash:
      entry.kind === "publish" ? entry.sourceHash : entry.priorKeysetHash,
  };
  const sourceAuthority: CanonicalSourceAuthority =
    entry.kind === "publish" ? entry.sourceAuthority : entry.priorSourceAuthority;
  const insertOnePage = async (
    kind: "event" | "brief", page: CanonicalImagePage,
  ) => {
    const rows =
      kind === "event"
        ? projectAll<CanonicalEventRow | CanonicalTombstone>(page.rows,
            entry.kind === "publish" ? projectEventRow : projectTombstone)
        : projectAll<CanonicalBriefRow | CanonicalBriefTombstone>(page.rows,
            entry.kind === "publish" ? projectBriefRow : projectBriefTombstone);
    if (rows.kind !== "rows") return rows;
    const produced: CanonicalProduceResult =
      kind === "event"
        ? produceEventFeatures(
            entry.kind === "publish"
              ? (rows.rows as CanonicalEventRow[]) : [],
            entry.kind === "retire"
              ? (rows.rows as CanonicalTombstone[]) : [], ev)
        : produceBriefFeatures(
            entry.kind === "publish"
              ? (rows.rows as CanonicalBriefRow[]) : [],
            entry.kind === "retire"
              ? (rows.rows as CanonicalBriefTombstone[]) : [], ev);
    if (produced.kind !== "rows") return produced;
    await ch.insert(
      kind === "event" ? EVENT_FEATURES_TABLE : BRIEF_FEATURES_TABLE,
      produced.rows);
    await acks.ackPage({
      entry, kind, cursor: page.cursor ?? "terminal",
      pageSha256: page.pageSha256,
    });
    return produced;
  };
  let total = 0, terminalEvent = "", terminalBrief = "";
  for (const kind of ["event", "brief"] as const) {
    let cursor: string | null = null;
    const seen = new Set<string>();
    for (;;) {
      const page: CanonicalImagePage = entry.kind === "retire"
        ? await images.priorKeyPages(entry, kind, cursor)
        : await images.imagePages(entry, kind, cursor);
      if (!Array.isArray(page.rows) || page.source !== entry.imageKey ||
          page.pointerSetHash !== entry.pointerSet.hash ||
          !hash(page.pageSha256) || page.pageSha256 !== hashRows(page.rows) ||
          !(page.cursor === null ||
            (typeof page.cursor === "string" && page.cursor.length > 0 &&
             page.cursor.length <= 4096)) ||
          (page.cursor !== null &&
            (page.cursor === cursor || seen.has(page.cursor))))
        return { kind: "refused", reason: "image_page_changed" };
      const r = await insertOnePage(kind, page);
      if (r.kind === "refused") return r;
      total += r.rows.length;
      if (page.cursor === null) {
        if (kind === "event") terminalEvent = page.pageSha256;
        else terminalBrief = page.pageSha256;
        break;
      }
      seen.add(page.cursor);
      cursor = page.cursor;
    }
  }
  // finish CAS: terminal descriptors + all page ACKs + still-current authority
  await acks.finish({
    entry, terminalEvent, terminalBrief,
    sourceAuthority,
    profileBinding: "builtin-fold",
  });
  return { kind: "copied", checkpoint: entry.revision, rows: total };
}
export { type CanonicalProduceResult };
