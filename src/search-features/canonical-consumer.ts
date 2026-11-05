/**
 * Supervised leased consumer: durable runtime between the canonical journal
 * and the event+brief pools. One live owner per partition via a heartbeat'd
 * lease (owner + TTL, not owner alone); crash-safe restart from durable
 * ACK/finish state; readiness only after BOTH pools finish the exact
 * revision; bounded retries; dead-letter only for stable poison codes
 * (never lease/infra failures, which are retryable holds); default-off,
 * org/workspace allowlist validated before any DB access.
 */
import type { SqlQuery, CanonicalJournalStore } from "./canonical-journal.ts";
import type { CanonicalJournalEntry } from "./canonical-version.ts";
import type { CanonicalPageAckStore } from "./canonical-ack-store.ts";
import { copyCanonicalEntry } from "./canonical-cycle.ts";
import type { CanonicalImagePort } from "./canonical-cycle.ts";
import type { ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";

const LEASE = "sessions.canonical_feature_consumer_lease";
const DEAD = "sessions.canonical_feature_consumer_dead_letter";
/** Stable bounded poison codes — never a raw exception message. */
const POISON = new Set([
  "row_invalid", "image_page_changed", "image_descriptor_changed",
  "row_bound",
]);
/** Retryable holds — lease/infra failures pause, never poison. */
const HOLD = new Set(["lease_lost", "heartbeat_failed", "db_timeout"]);

export interface ConsumerConfig {
  /** The EXACT canary scope; validated before any DB access. */
  allowlist: { orgId: string; workspaceId: string }[];
  targetId: string;
  ownerId: string;
  partition: string;
  leaseTtlMs: number;           // bounded
  maxRetries: number;
  pageSize: number;
}
/** Fail-closed config: exact canary scope + bounded numeric knobs. */
export function validateConfig(cfg: ConsumerConfig): void {
  const ok = cfg.allowlist.length === 1 &&
    cfg.allowlist[0]!.orgId === "c4fae7b9-43a9-44e8-94d5-455aa579d67b" &&
    cfg.allowlist[0]!.workspaceId === "1220c1dc-0ce5-45fe-8b67-e7bb57a2db63";
  if (!ok) throw new Error("config_not_canary_scope");
  if (typeof cfg.targetId !== "string" || !cfg.targetId.length ||
      cfg.targetId.length > 256) throw new Error("config_bad_target");
  if (typeof cfg.ownerId !== "string" || !cfg.ownerId.length ||
      cfg.ownerId.length > 128) throw new Error("config_bad_owner");
  if (typeof cfg.partition !== "string" || !cfg.partition.length ||
      cfg.partition.length > 64) throw new Error("config_bad_partition");
  for (const [k, min, max] of [
    ["leaseTtlMs", 1, 300000], ["maxRetries", 0, 10], ["pageSize", 1, 1000],
  ] as const) {
    const v = cfg[k];
    if (!Number.isInteger(v) || v < min || v > max)
      throw new Error(`config_bad_${k}`);
  }
}

export class CanonicalConsumer {
  constructor(
    readonly db: SqlQuery,
    readonly journal: CanonicalJournalStore,
    readonly acks: CanonicalPageAckStore,
    readonly cfg: ConsumerConfig,
  ) { validateConfig(cfg); }

  /**
   * Acquire-or-renew the drain lease in ONE atomic statement: inserts a new
   * owner, or takes over an expired one, or renews the SAME owner's fresh
   * lease. A live different owner cannot take it. ttl stays an integer param.
   */
  async acquireLease(): Promise<boolean> {
    const { rows } = await this.db.query<{ n: number }>(
      `INSERT INTO ${LEASE}(partition,owner,heartbeat_at)` +
        ` VALUES($1,$2,now()) ON CONFLICT (partition)` +
        ` DO UPDATE SET owner=$2,heartbeat_at=now()` +
        ` WHERE ${LEASE}.owner=$2` +
        `    OR ${LEASE}.heartbeat_at < now() - make_interval(secs => $3)` +
        ` RETURNING 1 AS n`,
      [this.cfg.partition, this.cfg.ownerId, this.cfg.leaseTtlMs / 1000]);
    return rows.length > 0;
  }
  /** Heartbeat — fails if this owner no longer holds the lease (0 rows). */
  async heartbeat(): Promise<boolean> {
    const { rows } = await this.db.query<{ n: number }>(
      `UPDATE ${LEASE} SET heartbeat_at=now()` +
        ` WHERE partition=$1 AND owner=$2 RETURNING 1 AS n`,
      [this.cfg.partition, this.cfg.ownerId]);
    return rows.length > 0;
  }
  /** The lease guard — held only while THIS owner is within TTL. */
  private leaseHeld() {
    return this.db.query<{ n: number }>(
      `SELECT 1 AS n FROM ${LEASE} WHERE partition=$1 AND owner=$2` +
        ` AND heartbeat_at >= now() - make_interval(secs => $3)`,
      [this.cfg.partition, this.cfg.ownerId, this.cfg.leaseTtlMs / 1000])
      .then((r) => r.rows.length > 0);
  }

  /** Dead-letter only for STABLE poison codes — bounded, never raw messages. */
  async deadLetter(entry: CanonicalJournalEntry, code: string): Promise<void> {
    if (!POISON.has(code)) return;           // not poison: never record
    await this.db.query(
      `INSERT INTO ${DEAD}(entry_revision,image_key,reason,attempts,dead_at)` +
        ` VALUES($1,$2,$3,1,now()) ON CONFLICT (entry_revision)` +
        ` DO UPDATE SET attempts=` +
        ` canonical_feature_consumer_dead_letter.attempts+1`,
      [entry.revision, entry.imageKey, code]);
  }

  /**
   * Drain the journal from a checkpoint: revision-ordered paging, per-entry
   * copy with bounded retries. Ordering is fail-atomic: the FIRST non-copied
   * entry stops the drain WITHOUT advancing the checkpoint (a dead letter is
   * evidence, not a pass; a hold pauses the whole run).
   */
  async drain(
    images: CanonicalImagePort, ch: ClickHouseClient,
  ): Promise<{ copied: number; dead: number; checkpoint: string }> {
    if (!(await this.acquireLease()))
      return { copied: 0, dead: 0, checkpoint: "0" };
    let checkpoint = "0", copied = 0, dead = 0;
    let lastRev = "0";
    for (;;) {
      const { entries, hasMore } =
        await this.journal.entries(checkpoint, this.cfg.pageSize);
      for (const entry of entries) {
        // Strict revision order — an out-of-order or repeated page refuses.
        if (BigInt(entry.revision) <= BigInt(lastRev))
          return { copied, dead, checkpoint };
        lastRev = entry.revision;
        const r = await this.copyOnce(images, ch, checkpoint, entry);
        if (r === "copied") { copied++; checkpoint = entry.revision; }
        else {
          // A hold or dead entry stops the whole drain — never advance past
          // it and never let later revisions in the page copy.
          if (r === "dead") { dead++; await this.deadLetter(entry, "row_invalid"); }
          return { copied, dead, checkpoint };
        }
        if (!(await this.heartbeat()))
          return { copied, dead, checkpoint };   // lost mid-drain: hold
      }
      if (!hasMore) break;
      checkpoint = lastRev;
    }
    return { copied, dead, checkpoint };
  }

  /** One bounded attempt set; lease/infra -> hold, poison -> dead. */
  private async copyOnce(
    images: CanonicalImagePort, ch: ClickHouseClient,
    checkpoint: string, entry: CanonicalJournalEntry,
  ): Promise<"copied" | "dead" | "hold"> {
    for (let i = 0; i <= this.cfg.maxRetries; i++) {
      try {
        const r = await copyCanonicalEntry(
          images, this.acks, ch, { checkpoint }, entry, this.cfg.targetId,
          { beforeWrite: () => this.leaseHeld(),
            beforeFinish: () => this.leaseHeld() });
        if (r.kind === "copied") return "copied";
        const reason = (r as { reason: string }).reason;
        if (HOLD.has(reason)) return "hold";
        if (i === this.cfg.maxRetries) return "dead";
      } catch {
        if (i === this.cfg.maxRetries) return "hold"; // infra: retryable
      }
      await new Promise((r) => setTimeout(r, Math.min(50 * (i + 1), 500)));
    }
    return "dead";
  }
}
