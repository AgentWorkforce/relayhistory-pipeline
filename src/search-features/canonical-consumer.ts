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
import { createHash } from "node:crypto";
/** The exact canary scope; the partition is DERIVED from it + the target. */
const CANARY = { orgId: "c4fae7b9-43a9-44e8-94d5-455aa579d67b",
  workspaceId: "1220c1dc-0ce5-45fe-8b67-e7bb57a2db63" };
/** Stable poison codes — the producer's deterministic refusals plus the
 *  cycle's data-poison refusals. source_changed + anything else -> hold. */
const POISON: Record<string, true> = {
  reference_invalid: true, tombstone_invalid: true, payload_invalid: true,
  vector_invalid: true, payload_bound: true, batch_invalid: true,
  duplicate: true,
  row_invalid: true, row_bound: true, image_page_changed: true,
  image_descriptor_changed: true, corrupt_chain: true,
  page_bytes_bound: true, page_rows_bound: true,
};
const HOLD = new Set(["lease_lost", "heartbeat_failed", "db_timeout",
  "stale_revision", "target_id_required", "source_changed"]);

export interface ConsumerConfig {
  /** REQUIRED explicit gate — false/unset means do not run anything. */
  enabled: boolean;
  targetId: string;
  ownerId: string;
  leaseTtlMs: number;
  maxRetries: number;
  pageSize: number;
}
/** Fail-closed: exact canary scope, the bound partition, bounded knobs. */
export function validateConfig(cfg: ConsumerConfig): void {
  if (cfg.enabled !== true) return;      // off is a valid config (no access)
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
  ) {
    validateConfig(cfg);
    // The partition identity binds canary + target — a new deployment target
    // gets a disjoint lease + checkpoint, never reusing the old one.
    // Derived lazily: a disabled consumer never hashes or touches the DB.
    this.partition = null as unknown as string;
  }
  private partition: string;
  private ensurePartition(): string {
    this.partition ??= `c4fae7b9-1220c1dc-` +
      createHash("sha256").update(this.cfg.targetId).digest("hex")
        .slice(0, 32);
    return this.partition;
  }

  private get on(): boolean { return this.cfg.enabled === true; }
  /** One atomic acquire/renew/takeover — the bound partition only. */
  async acquireLease(): Promise<boolean> {
    if (!this.on) return false;
    const { rows } = await this.db.query<{ n: number }>(
      `INSERT INTO ${LEASE}(partition,owner,heartbeat_at)` +
        ` VALUES($1,$2,now()) ON CONFLICT (partition)` +
        ` DO UPDATE SET owner=$2,heartbeat_at=now()` +
        ` WHERE ${LEASE}.owner=$2` +
        `    OR ${LEASE}.heartbeat_at < now() - make_interval(secs => $3)` +
        ` RETURNING 1 AS n`,
      [this.ensurePartition(), this.cfg.ownerId, this.cfg.leaseTtlMs / 1000]);
    return rows.length > 0;
  }
  /** Atomic renew-and-held: one statement checks THIS owner is still fresh
   *  AND extends the lease — used as the page/finish guard so a long copy
   *  stays owned and a lost lease stops the worker. */
  async heartbeat(): Promise<boolean> {
    if (!this.on) return false;
    const { rows } = await this.db.query<{ n: number }>(
      `UPDATE ${LEASE} SET heartbeat_at=now()` +
        ` WHERE partition=$1 AND owner=$2` +
        ` AND heartbeat_at >= now() - make_interval(secs => $3)` +
        ` RETURNING 1 AS n`,
      [this.ensurePartition(), this.cfg.ownerId, this.cfg.leaseTtlMs / 1000]);
    return rows.length > 0;
  }
  private leaseHeld() {
    return this.db.query<{ n: number }>(
      `SELECT 1 AS n FROM ${LEASE} WHERE partition=$1 AND owner=$2` +
        ` AND heartbeat_at >= now() - make_interval(secs => $3)`,
      [this.ensurePartition(), this.cfg.ownerId, this.cfg.leaseTtlMs / 1000])
      .then((r) => r.rows.length > 0);
  }
  /** Durable partition checkpoint — CAS'd only after a real advance. */
  private async readCheckpoint(): Promise<string> {
    const { rows } = await this.db.query<{ rev: string | number }>(
      `SELECT rev FROM ${CKPT} WHERE partition=$1`, [this.ensurePartition()]);
    return rows.length ? String(rows[0]!.rev) : "0";
  }
  /** Advance only while this owner still holds a fresh lease — a stale
   *  worker cannot move the shared checkpoint after takeover. */
  private async advanceCheckpoint(rev: string): Promise<boolean> {
    const { rows } = await this.db.query<{ n: number }>(
      `UPDATE ${CKPT} SET rev=$2::bigint WHERE partition=$1` +
        ` AND rev < $2::bigint` +
        ` AND EXISTS (SELECT 1 FROM ${LEASE}` +
        `   WHERE partition=$1 AND owner=$3` +
        `   AND heartbeat_at >= now() - make_interval(secs => $4))` +
        ` RETURNING 1 AS n`,
      [this.ensurePartition(), rev, this.cfg.ownerId,
        this.cfg.leaseTtlMs / 1000]);
    return rows.length > 0;
  }
  private async seedCheckpoint(): Promise<void> {
    await this.db.query(
      `INSERT INTO ${CKPT}(partition,rev) VALUES($1,0)` +
        ` ON CONFLICT (partition) DO NOTHING`, [this.ensurePartition()]);
  }
  /** Dead-letter: stable poison code only; immutable (no update/delete). */
  async deadLetter(entry: CanonicalJournalEntry, code: string): Promise<void> {
    if (!this.on || !POISON[code]) return;
    await this.db.query(
      `INSERT INTO ${DEAD}(entry_revision,partition,image_key,reason,` +
        `attempts,dead_at)` +
        ` VALUES($1,$2,$3,$4,1,now())` +
        ` ON CONFLICT (entry_revision,partition) DO NOTHING`,
      [entry.revision, this.ensurePartition(), entry.imageKey, code]);
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
    // The explicit gate: disabled returns a constant — no partition
    // derivation, no lease, no journal, no CH, zero DB queries.
    if (this.cfg.enabled !== true)
      return { copied: 0, dead: 0, skipped: 0, checkpoint: "0" };
    if (!(await this.acquireLease()))
      return { copied: 0, dead: 0, skipped: 0,
        checkpoint: await this.readCheckpoint() };
    await this.seedCheckpoint();
    let copied = 0, dead = 0, skipped = 0;
    let checkpoint = await this.readCheckpoint();
    let lastSeen = checkpoint;
    for (;;) {
      const { entries, hasMore } =
        await this.journal.entries(checkpoint, this.cfg.pageSize);
      for (const entry of entries) {
        // Strict revision order — a non-increasing page is a violation.
        if (BigInt(entry.revision) <= BigInt(lastSeen))
          return { copied, dead, skipped, checkpoint };
        lastSeen = entry.revision;
        if (!inScope(entry)) {
          if (!(await this.advanceCheckpoint(entry.revision)))
            return { copied, dead, skipped, checkpoint };
          checkpoint = entry.revision;
          skipped++;
          continue;
        }
        const r = await this.copyOnce(images, ch, checkpoint, entry);
        if (r === "copied") {
          // Metrics count a copy only after the fenced CAS accepted it.
          if (!(await this.advanceCheckpoint(entry.revision)))
            return { copied, dead, skipped, checkpoint };
          copied++;
          checkpoint = entry.revision;
        } else {
          if (r !== "hold") {
            dead++;
            await this.deadLetter(entry, r);
          }
          return { copied, dead, skipped, checkpoint };
        }
        if (!(await this.heartbeat()))
          return { copied, dead, skipped, checkpoint };
      }
      if (!hasMore) break;
      checkpoint = lastSeen;
    }
    return { copied, dead, skipped, checkpoint };
  }

  /** One bounded attempt set; only a stable poison code -> dead. */
  private async copyOnce(
    images: CanonicalImagePort, ch: ClickHouseClient,
    checkpoint: string, entry: CanonicalJournalEntry,
  ): Promise<"copied" | "hold" | string> {
    for (let i = 0; i <= this.cfg.maxRetries; i++) {
      try {
        const r = await copyCanonicalEntry(
          images, this.acks, ch, { checkpoint }, entry, this.cfg.targetId,
          { beforeWrite: () => this.heartbeat(),
            beforeFinish: () => this.heartbeat() });
        if (r.kind === "copied") return "copied";
        const reason = (r as { reason: string }).reason;
        if (HOLD.has(reason)) return "hold";
        if (POISON[reason] && i === this.cfg.maxRetries)
          return reason;                        // exact stable poison code
        if (!POISON[reason]) return "hold";   // unknown -> hold, never poison
      } catch {
        return "hold";                          // infra: retryable hold
      }
      await new Promise((r) => setTimeout(r, Math.min(50 * (i + 1), 500)));
    }
    return "hold";
  }
}
