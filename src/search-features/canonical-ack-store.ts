/**
 * Durable CanonicalPageAckPort: page ACKs and the finish CAS persist to a
 * producer-side state table so a restart resumes at the last ACKed page
 * instead of restarting the image. Separate from the journal — the journal is
 * _install's; this is the consumer's own progress ledger.
 *
 * Table (proposal — 040610 companion, same draft bundle):
 *   sessions.canonical_feature_page_ack(
 *     image_key text NOT NULL, pool text NOT NULL, cursor text NOT NULL,
 *     page_sha256 text NOT NULL, acked_at timestamptz DEFAULT now(),
 *     PRIMARY KEY (image_key, pool, cursor));
 *   sessions.canonical_feature_copy_state(
 *     image_key text PRIMARY KEY, entry_revision text NOT NULL,
 *     terminal_event text, terminal_brief text,
 *     source_authority jsonb, profile_binding text,
 *     finished boolean NOT NULL DEFAULT false);
 */
import type { SqlQuery } from "./canonical-journal.ts";
import type {
  CanonicalJournalEntry,
  CanonicalPageAckPort,
} from "./canonical-version.ts";

const ACK = "sessions.canonical_feature_page_ack";
const STATE = "sessions.canonical_feature_copy_state";
const key = (e: CanonicalJournalEntry) => e.imageKey;

export class CanonicalPageAckStore implements CanonicalPageAckPort {
  constructor(readonly db: SqlQuery) {}

  /** Idempotent page ACK — restart-safe, a re-ACK of the same page is a no-op. */
  async ackPage(input: {
    entry: CanonicalJournalEntry; kind: "event" | "brief";
    cursor: string; pageSha256: string;
  }): Promise<void> {
    await this.db.query(
      `INSERT INTO ${ACK}(image_key,pool,cursor,page_sha256)` +
        ` VALUES($1,$2,$3,$4) ON CONFLICT (image_key,pool,cursor)` +
        ` DO UPDATE SET page_sha256=EXCLUDED.page_sha256`,
      [key(input.entry), input.kind, input.cursor, input.pageSha256]);
  }

  /** All ACKed cursors for an image+pool (restart resumes after the last). */
  async ackedCursors(
    entry: CanonicalJournalEntry, pool: "event" | "brief",
  ): Promise<string[]> {
    const { rows } = await this.db.query<{ cursor: string }>(
      `SELECT cursor FROM ${ACK} WHERE image_key=$1 AND pool=$2` +
        ` ORDER BY cursor`,
      [key(entry), pool]);
    return rows.map((r) => r.cursor);
  }

  /**
   * Finish CAS: terminal descriptors + all page ACKs + current source
   * authority + unchanged profile binding — all-or-nothing via the state's
   * optimistic row. Refuses on stale authority/profile, no writes on fail.
   */
  async finish(input: {
    entry: CanonicalJournalEntry;
    terminalEvent: string; terminalBrief: string;
    sourceAuthority: { authorityId: string; epoch: string; fingerprint: string };
    profileBinding: string;
  }): Promise<void> {
    const imageKey = key(input.entry);
    const { rows } = await this.db.query<{ finished: boolean }>(
      `SELECT finished FROM ${STATE} WHERE image_key=$1`, [imageKey]);
    if (rows[0]?.finished === true)
      throw new Error("already_finished");
    // The CAS binds the captured authority + profile — a late writer racing
    // the same image_key with different bindings fails the insert's WHERE.
    const { rows: done } = await this.db.query<{ n: number }>(
      `INSERT INTO ${STATE}(image_key,entry_revision,terminal_event,` +
        ` terminal_brief,source_authority,profile_binding,finished)` +
        ` VALUES($1,$2,$3,$4,$5,$6,true)` +
        ` ON CONFLICT (image_key) DO UPDATE SET finished=true` +
        ` WHERE ${STATE}.finished=false` +
        ` AND ${STATE}.source_authority=$5 AND ${STATE}.profile_binding=$6` +
        ` RETURNING 1 AS n`,
      [imageKey, input.entry.revision, input.terminalEvent,
        input.terminalBrief, JSON.stringify(input.sourceAuthority),
        input.profileBinding]);
    if (!done.length) throw new Error("finish_cas_conflict");
  }

  /** Resume position for a restart: the highest ACKed cursor per pool. */
  async resume(entry: CanonicalJournalEntry): Promise<{
    event: string | null; brief: string | null;
  }> {
    const cursor = async (pool: "event" | "brief") => {
      const { rows } = await this.db.query<{ cursor: string }>(
        `SELECT cursor FROM ${ACK} WHERE image_key=$1 AND pool=$2` +
          ` ORDER BY acked_at DESC LIMIT 1`,
        [key(entry), pool]);
      return rows[0]?.cursor ?? null;
    };
    return { event: await cursor("event"), brief: await cursor("brief") };
  }
}
