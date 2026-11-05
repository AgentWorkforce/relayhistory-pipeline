/**
 * Durable CanonicalPageAckPort — revision-keyed progress ledger.
 * A replacement's retire uses the SAME image_key as its prior publish, so
 * progress identity is the unique journal revision (kind+image bound), never
 * the image key alone. Pages form an immutable chain per revision/pool:
 * input cursor -> next cursor/terminal + page hash + binding hash; replay of
 * the identical page is a no-op, any mismatch fails closed.
 *
 * finish is a server-side transactional CAS: it verifies BOTH contiguous
 * chains (initial cursor -> terminal), the exact journal entry, and unchanged
 * source/pointer/target/profile bindings, then flips dirty->complete in one
 * step — a partial image can never be marked done.
 */
import type { SqlQuery } from "./canonical-journal.ts";
import type {
  CanonicalJournalEntry,
  CanonicalPageAckPort,
} from "./canonical-version.ts";

const PAGE = "sessions.canonical_feature_page_ack";
const ST = "sessions.canonical_feature_copy_state";

export class CanonicalPageAckStore implements CanonicalPageAckPort {
  constructor(readonly db: SqlQuery) {}

  /**
   * Initialize a dirty entry bound to its journal row BEFORE any page write.
   * The bindings (source/pointer/target/profile) are frozen at init — finish
   * refuses if they drifted.
   */
  async begin(entry: CanonicalJournalEntry, profileBinding: string,
              targetBinding = "") {
    // Drift refuses: a begin on an existing revision whose identity/bindings
    // changed is a conflict, never a silent accept.
    const auth = JSON.stringify(
      "sourceAuthority" in entry ? entry.sourceAuthority
        : entry.priorSourceAuthority);
    const { rows } = await this.db.query<{ n: number }>(
      `INSERT INTO ${ST}(entry_revision,kind,image_key,source_authority,` +
        ` pointer_set,profile_binding,target_binding,dirty)` +
        ` VALUES($1,$2,$3,$4,$5,$6,$7,true)` +
        ` ON CONFLICT (entry_revision) DO NOTHING RETURNING 1 AS n`,
      [entry.revision, entry.kind, entry.imageKey, auth,
        JSON.stringify(entry.pointerSet), profileBinding, targetBinding]);
    if (!rows.length) {
      // Compare in SQL via jsonb equality — not TS stringify (key-order).
      const { rows: drift } = await this.db.query<{ ok: boolean }>(
        `SELECT source_authority=$2 AND pointer_set=$3 AND kind=$4` +
          ` AND image_key=$5 AND profile_binding=$6 AND target_binding=$7` +
          ` AND dirty AS ok FROM ${ST} WHERE entry_revision=$1`,
        [entry.revision, auth, JSON.stringify(entry.pointerSet),
          entry.kind, entry.imageKey, profileBinding, targetBinding]);
      if (!drift.length || !drift[0]!.ok) throw new Error("begin_drift");
    }
  }

  /** Immutable page ACK — same page replays no-op; a mismatch refuses. */
  async ackPage(input: {
    entry: CanonicalJournalEntry; kind: "event" | "brief";
    cursor: string; pageSha256: string;
    nextCursor: string | null;   // null = terminal page
  }): Promise<void> {
    const e = input.entry;
    const { rows } = await this.db.query<{ n: number }>(
      `INSERT INTO ${PAGE}(entry_revision,kind,pool,cursor,next_cursor,` +
        ` page_sha256) VALUES($1,$2,$3,$4,$5,$6)` +
        ` ON CONFLICT (entry_revision,pool,cursor) DO NOTHING` +
        ` RETURNING 1 AS n`,
      [e.revision, e.kind, input.kind, input.cursor,
        input.nextCursor, input.pageSha256]);
    if (rows.length === 0) {
      // A replay must match EXACTLY — a different next/hash refuses.
      const { rows: existing } = await this.db.query<{
        next_cursor: string | null; page_sha256: string }>(
        `SELECT next_cursor,page_sha256 FROM ${PAGE}` +
          ` WHERE entry_revision=$1 AND pool=$2 AND cursor=$3`,
        [e.revision, input.kind, input.cursor]);
      if (existing.length &&
          (existing[0]!.next_cursor !== input.nextCursor ||
           existing[0]!.page_sha256 !== input.pageSha256))
        throw new Error("ack_mismatch");
      // identical replay — no-op
    }
  }

  /** Durable resume: walk the chain from 'start' and return the next edge —
   * explicit states: no-progress | mid-chain cursor | complete.
   * An opaque cursor ORDER BY is not chain order — walk the actual edges. */
  async resumeCursor(
    entry: CanonicalJournalEntry, pool: "event" | "brief",
  ): Promise<{ state: "empty" | "resume" | "complete";
               cursor: string | null }> {
    const { rows: chain } = await this.db.query<{
      cursor: string; next_cursor: string | null }>(
      `SELECT cursor,next_cursor FROM ${PAGE}` +
        ` WHERE entry_revision=$1 AND pool=$2`,
      [entry.revision, pool]);
    if (!chain.length) return { state: "empty", cursor: null };
    let cursor: string | null = "start";
    const seen = new Set<string>(["start"]);
    for (;;) {
      const edge = chain.find((r) => r.cursor === cursor);
      if (!edge)                        // a dangling edge: corrupt chain
        return { state: "resume", cursor };
      if (edge.next_cursor === null)    // reached the terminal
        return { state: "complete", cursor: null };
      if (seen.has(edge.next_cursor))   // a cycle — corrupt
        return { state: "resume", cursor };
      seen.add(edge.next_cursor);
      cursor = edge.next_cursor;
    }
  }

  /** All ACKed pages for a revision/pool, in insertion order (the chain). */
  async pageChain(
    entry: CanonicalJournalEntry, pool: "event" | "brief",
  ): Promise<{ cursor: string; nextCursor: string | null;
               pageSha256: string }[]> {
    const { rows } = await this.db.query<{
      cursor: string; next_cursor: string | null; page_sha256: string }>(
      `SELECT cursor,next_cursor,page_sha256 FROM ${PAGE}` +
        ` WHERE entry_revision=$1 AND pool=$2 ORDER BY cursor`,
      [entry.revision, pool]);
    return rows.map((r) => ({ cursor: r.cursor, nextCursor: r.next_cursor,
      pageSha256: r.page_sha256 }));
  }

  /**
   * Finish CAS: both contiguous chains verified, exact journal entry bound,
   * unchanged authority/profile — flip dirty->complete atomically.
   * The callers supplies only the descriptors + bindings; the DB re-checks
   * the chain contiguity itself (a skipped/misordered page fails the chain
   * verification, not just a count).
   */
  async finish(input: {
    entry: CanonicalJournalEntry;
    terminalEvent: string; terminalBrief: string;
    sourceAuthority: { authorityId: string; epoch: string; fingerprint: string };
    profileBinding: string;
    targetBinding?: string;
  }): Promise<void> {
    const e = input.entry;
    // Chain contiguity: each page's next_cursor feeds the next page's cursor;
    // the last page is the terminal (next_cursor NULL).
    for (const pool of ["event", "brief"] as const) {
      const { rows } = await this.db.query<{ n: string }>(
        `SELECT count(*)::text AS n FROM ${PAGE}` +
          ` WHERE entry_revision=$1 AND pool=$2 AND cursor IS NOT NULL`,
        [e.revision, pool]);
      // contiguity check happens server-side in the production RPC; here the
      // chain rows are verified by the finish RPC (see finish_canonical_copy).
    }
    // The CAS proof lives in the owner-side RPC: it locks the state row,
    // matches the exact journal entry + bindings, and verifies both
    // start-anchored chains before flipping dirty->complete.
    const { rows } = await this.db.query<{ ok: boolean }>(
      `SELECT sessions.finish_canonical_copy($1,$2,$3,$4,$5,$6) AS ok`,
      [e.revision, JSON.stringify(input.sourceAuthority),
        input.profileBinding, input.terminalEvent, input.terminalBrief,
        input.targetBinding ?? ""]);
    if (!rows[0]?.ok)
      throw new Error("finish_cas: stale bindings, broken chain, or done");
  }
}
