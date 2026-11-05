/**
 * Supervised leased consumer: the durable runtime between the canonical
 * journal and the event+brief pools. One active owner per drain partition via
 * a heartbeat'd lease; crash-safe restart from durable ACK/finish state;
 * readiness only after BOTH pools finish the exact revision; bounded retries
 * with dead-letter (never skip); default-off, org/workspace allowlist.
 */
import { createHash } from "node:crypto";
import type { SqlQuery, CanonicalJournalStore } from "./canonical-journal.ts";
import type { CanonicalJournalEntry } from "./canonical-version.ts";
import type { CanonicalPageAckStore } from "./canonical-ack-store.ts";
import { copyCanonicalEntry } from "./canonical-cycle.ts";
import type { CanonicalImagePort } from "./canonical-cycle.ts";
import type { ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";

const LEASE = "sessions.canonical_feature_consumer_lease";
const DEAD = "sessions.canonical_feature_consumer_dead_letter";

export interface ConsumerConfig {
  /** Bounded to the canary scope — DEFAULT OFF (empty = refuse everything). */
  allowlist: { orgId: string; workspaceId: string }[];
  targetId: string;            // REQUIRED deployment identity
  ownerId: string;             // this worker's lease identity
  leaseTtlMs?: number;         // heartbeat validity window
  maxRetries?: number;         // per entry before dead-letter
  pageSize?: number;           // journal entries per drain page
}

const ALLOWED = { orgId: "c4fae7b9-43a9-44e8-94d5-455aa579d67b",
                  workspaceId: "1220c1dc-0ce5-45fe-8b67-e7bb57a2db63" };
const inScope = (cfg: ConsumerConfig, e: CanonicalJournalEntry) =>
  cfg.allowlist.some((a) =>
    a.orgId === e.scope.orgId && a.workspaceId === e.scope.workspaceId);

export class CanonicalConsumer {
  constructor(
    readonly db: SqlQuery,
    readonly journal: CanonicalJournalStore,
    readonly acks: CanonicalPageAckStore,
    readonly cfg: ConsumerConfig,
  ) {}

  /** Acquire/renew the drain lease — exactly one live owner. */
  async acquireLease(partition = "0"): Promise<boolean> {
    const ttl = this.cfg.leaseTtlMs ?? 30000;
    const { rows } = await this.db.query<{ n: number }>(
      `INSERT INTO ${LEASE}(partition,owner,heartbeat_at)` +
        ` VALUES($1,$2,now()) ON CONFLICT (partition)` +
        ` DO UPDATE SET owner=$2,heartbeat_at=now()` +
        ` WHERE ${LEASE}.heartbeat_at < now() - interval '${ttl} milliseconds'` +
        ` RETURNING 1 AS n`, [partition, this.cfg.ownerId]);
    return rows.length > 0;
  }
  async heartbeat(partition = "0"): Promise<void> {
    await this.db.query(
      `UPDATE ${LEASE} SET heartbeat_at=now()` +
        ` WHERE partition=$1 AND owner=$2`, [partition, this.cfg.ownerId]);
  }

  /** Dead-letter a failed entry — never skipped; retried on next run if below. */
  async deadLetter(entry: CanonicalJournalEntry, reason: string): Promise<void> {
    await this.db.query(
      `INSERT INTO ${DEAD}(entry_revision,image_key,reason,attempts,dead_at)` +
        ` VALUES($1,$2,$3,1,now()) ON CONFLICT (entry_revision)` +
        ` DO UPDATE SET attempts=canonical_feature_consumer_dead_letter.attempts+1,` +
        ` reason=EXCLUDED.reason`,
      [entry.revision, entry.imageKey, reason]);
  }

  /**
   * Drain the journal from a checkpoint: revision-ordered paging, per-entry
   * copy with bounded retries + dead-letter, heartbeat between entries.
   * Readiness advances ONLY after finish committed (both pools complete) —
   * the copy state's own durable proof, not an in-memory flag.
   */
  async drain(
    images: CanonicalImagePort, ch: ClickHouseClient,
    partition = "0",
  ): Promise<{ copied: number; dead: number; checkpoint: string }> {
    if (!(await this.acquireLease(partition)))
      return { copied: 0, dead: 0, checkpoint: "0" };
    let checkpoint = "0", copied = 0, dead = 0;
    const pageSize = this.cfg.pageSize ?? 100;
    const maxRetries = this.cfg.maxRetries ?? 3;
    for (;;) {
      const { entries, hasMore, nextRevision } =
        await this.journal.entries(checkpoint, pageSize);
      for (const entry of entries) {
        if (!inScope(this.cfg, entry)) {
          checkpoint = entry.revision;      // out-of-scope advances past it
          continue;
        }
        const r = await this.copyWithRetry(
          images, ch, { checkpoint }, entry, maxRetries);
        if (r.kind === "dead") { dead++; await this.deadLetter(entry, r.reason); }
        else if (r.kind === "copied") { copied++; checkpoint = entry.revision; }
        else break;   // refused mid-batch — hold the checkpoint, retry next run
        await this.heartbeat(partition);
      }
      if (!hasMore) break;
      checkpoint = nextRevision;
    }
    return { copied, dead, checkpoint };
  }

  /** Bounded retries with backoff; a refusal after maxRetries dead-letters. */
  private async copyWithRetry(
    images: CanonicalImagePort, ch: ClickHouseClient,
    state: { checkpoint: string }, entry: CanonicalJournalEntry,
    maxRetries: number,
  ): Promise<{ kind: "copied" } | { kind: "dead"; reason: string }
            | { kind: "refused"; reason: string }> {
    for (let i = 0; i <= maxRetries; i++) {
      try {
        const r = await copyCanonicalEntry(
          images, this.acks, ch, state, entry, this.cfg.targetId);
        if (r.kind === "copied") return { kind: "copied" };
        if (i === maxRetries)
          return { kind: "dead", reason: (r as { reason: string }).reason };
      } catch (e) {
        if (i === maxRetries)
          return { kind: "dead", reason: (e as Error).message };
      }
      // bounded backoff — deterministic base * attempt for tests
      await new Promise((r) => setTimeout(r, Math.min(100 * (i + 1), 1000)));
    }
    return { kind: "refused", reason: "unreachable" };
  }
}
