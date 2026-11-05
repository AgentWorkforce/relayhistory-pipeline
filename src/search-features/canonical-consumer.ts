/**
 * Supervised leased consumer: durable runtime between the canonical journal
 * and the event+brief pools. Scoped to the exact canary tenant (the journal
 * read is fenced — an out-of-scope entry never reaches a descriptor or a CH
 * write); ONE bound partition per canary+target; heartbeat'd lease with a
 * still-fresh TTL (a stale owner cannot resurrect); durable partition
 * checkpoint CAS'd only after a copied or scope-skipped revision; dead-letter
 * persists only stable poison codes (lease/infra/unknown refuse -> hold).
 */
import type { SqlQuery, CanonicalJournalStore } from "./canonical-journal.ts";
import type { CanonicalJournalEntry } from "./canonical-version.ts";
import type { CanonicalPageAckStore } from "./canonical-ack-store.ts";
import { copyCanonicalEntry } from "./canonical-cycle.ts";
import type { CanonicalImagePort } from "./canonical-cycle.ts";
import type { ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";

const LEASE = "sessions.canonical_feature_consumer_lease";
const DEAD = "sessions.canonical_feature_consumer_dead_letter";
const CKPT = "sessions.canonical_feature_consumer_checkpoint";
/** The exact canary scope AND its single bound partition name. */
const CANARY = { orgId: "c4fae7b9-43a9-44e8-94d5-455aa579d67b",
  workspaceId: "1220c1dc-0ce5-45fe-8b67-e7bb57a2db63" };
const PARTITION = "c4fae7b9-1220c1dc-0";
const POISON: Record<string, true> = {
  row_invalid: true, image_page_changed: true,
  image_descriptor_changed: true, row_bound: true,
};
const HOLD = new Set(["lease_lost", "heartbeat_failed", "db_timeout"]);

export interface ConsumerConfig {
  targetId: string;
  ownerId: string;
  leaseTtlMs: number;
  maxRetries: number;
  pageSize: number;
}
/** Fail-closed: exact canary scope, the bound partition, bounded knobs. */
export function validateConfig(cfg: ConsumerConfig): void {
  if (typeof cfg.targetId !== "string" || !cfg.targetId.length ||
      cfg.targetId.length > 256) throw new Error("config_bad_target");
  if (typeof cfg.ownerId !== "string" || !cfg.ownerId.length ||
      cfg.ownerId.length > 128) throw new Error("config_bad_owner");
  for (const [k, min, max] of [
    ["leaseTtlMs", 1, 300000], ["maxRetries", 0, 10], ["pageSize", 1, 1000],
  ] as const) {
    const v = cfg[k];
    if (!Number.isInteger(v) || v < min || v > max)
      throw new Error(`config_bad_${k}`);
  }
}
const inScope = (e: CanonicalJournalEntry) =>
  e.scope.orgId === CANARY.orgId && e.scope.workspaceId === CANARY.workspaceId;

export class CanonicalConsumer {
  constructor(
    readonly db: SqlQuery,
    readonly journal: CanonicalJournalStore,
    readonly acks: CanonicalPageAckStore,
    readonly cfg: ConsumerConfig,
  ) { validateConfig(cfg); }

  /** One atomic acquire/renew/takeover — the bound partition only. */
  async acquireLease(): Promise<boolean> {
    const { rows } = await this.db.query<{ n: number }>(
      `INSERT INTO ${LEASE}(partition,owner,heartbeat_at)` +
        ` VALUES($1,$2,now()) ON CONFLICT (partition)` +
        ` DO UPDATE SET owner=$2,heartbeat_at=now()` +
        ` WHERE ${LEASE}.owner=$2` +
        `    OR ${LEASE}.heartbeat_at < now() - make_interval(secs => $3)` +
        ` RETURNING 1 AS n`,
      [PARTITION, this.cfg.ownerId, this.cfg.leaseTtlMs / 1000]);
    return rows.length > 0;
  }
  /** Heartbeat — requires a still-fresh held lease; a stale owner cannot. */
  async heartbeat(): Promise<boolean> {
    const { rows } = await this.db.query<{ n: number }>(
      `UPDATE ${LEASE} SET heartbeat_at=now()` +
        ` WHERE partition=$1 AND owner=$2` +
        ` AND heartbeat_at >= now() - make_interval(secs => $3)` +
        ` RETURNING 1 AS n`,
      [PARTITION, this.cfg.ownerId, this.cfg.leaseTtlMs / 1000]);
    return rows.length > 0;
  }
  private leaseHeld() {
    return this.db.query<{ n: number }>(
      `SELECT 1 AS n FROM ${LEASE} WHERE partition=$1 AND owner=$2` +
        ` AND heartbeat_at >= now() - make_interval(secs => $3)`,
      [PARTITION, this.cfg.ownerId, this.cfg.leaseTtlMs / 1000])
      .then((r) => r.rows.length > 0);
  }
  /** Durable partition checkpoint — CAS'd only after a real advance. */
  private async readCheckpoint(): Promise<string> {
    const { rows } = await this.db.query<{ rev: string | number }>(
      `SELECT rev FROM ${CKPT} WHERE partition=$1`, [PARTITION]);
    return rows.length ? String(rows[0]!.rev) : "0";
  }
  private async advanceCheckpoint(rev: string): Promise<void> {
    await this.db.query(
      `INSERT INTO ${CKPT}(partition,rev) VALUES($1,$2::bigint)` +
        ` ON CONFLICT (partition) DO UPDATE SET rev=$2::bigint` +
        ` WHERE ${CKPT}.rev < $2::bigint`,
      [PARTITION, rev]);
  }
  /** Dead-letter: stable poison code only; immutable (no update/delete). */
  async deadLetter(entry: CanonicalJournalEntry, code: string): Promise<void> {
    if (!POISON[code]) return;
    await this.db.query(
      `INSERT INTO ${DEAD}(entry_revision,image_key,reason,attempts,dead_at)` +
        ` VALUES($1,$2,$3,1,now()) ON CONFLICT (entry_revision) DO NOTHING`,
      [entry.revision, entry.imageKey, code]);
  }

  /**
   * Drain: revision-ordered, scope-fenced, fail-atomic. Out-of-scope entries
   * are scope-skipped (checkpoint advances, zero image/CH access); a copied
   * entry advances the durable checkpoint; a hold or poison stops the drain.
   */
  async drain(
    images: CanonicalImagePort, ch: ClickHouseClient,
  ): Promise<{ copied: number; dead: number; skipped: number;
              checkpoint: string }> {
    if (!(await this.acquireLease()))
      return { copied: 0, dead: 0, skipped: 0,
        checkpoint: await this.readCheckpoint() };
    let copied = 0, dead = 0, skipped = 0;
    let checkpoint = await this.readCheckpoint();
    for (;;) {
      const { entries, hasMore } =
        await this.journal.entries(checkpoint, this.cfg.pageSize);
      for (const entry of entries) {
        if (BigInt(entry.revision) <= BigInt(checkpoint))
          continue;                       // already consumed
        if (!inScope(entry)) {            // scope-skipped: never touches CH
          checkpoint = entry.revision;
          await this.advanceCheckpoint(checkpoint);
          skipped++;
          continue;
        }
        const r = await this.copyOnce(images, ch, checkpoint, entry);
        if (r === "copied") {
          copied++;
          checkpoint = entry.revision;
          await this.advanceCheckpoint(checkpoint);
        } else {
          if (r === "dead") { dead++; await this.deadLetter(entry, "row_invalid"); }
          return { copied, dead, skipped, checkpoint };
        }
        if (!(await this.heartbeat()))
          return { copied, dead, skipped, checkpoint };
      }
      if (!hasMore) break;
      checkpoint = entries[entries.length - 1]!.revision;
    }
    return { copied, dead, skipped, checkpoint };
  }

  /** One bounded attempt set; only a stable poison code -> dead. */
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
        if (POISON[reason] && i === this.cfg.maxRetries) return "dead";
        if (!POISON[reason]) return "hold";   // unknown -> hold, never poison
      } catch {
        return "hold";                          // infra: retryable hold
      }
      await new Promise((r) => setTimeout(r, Math.min(50 * (i + 1), 500)));
    }
    return "hold";
  }
}
