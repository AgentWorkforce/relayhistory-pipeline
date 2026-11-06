/**
 * Supervised leased consumer: durable runtime between the canonical journal
 * and the event+brief pools. Scoped to an explicitly configured tenant (the journal
 * read is fenced — an out-of-scope entry never reaches a descriptor or a CH
 * write); ONE bound partition per tenant+target; heartbeat'd lease with a
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
  /** Required when enabled; never inferred from journal contents. */
  scope?: Readonly<{ orgId: string; workspaceId: string }>;
  /** Explicit so existing checkpoints never silently change identity. */
  partitionScheme?: "scope-v1" | "legacy-prefix-v1";
  targetId: string;
  ownerId: string;
  leaseTtlMs: number;
  maxRetries: number;
  pageSize: number;
}
/** Fail-closed: explicit tenant scope, bound partition and bounded knobs. */
export function validateConfig(cfg: ConsumerConfig): void {
  if (cfg.enabled !== true) return;      // off is a valid config (no access)
  const validId = (v: unknown): v is string => typeof v === "string" &&
    /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/.test(v);
  if (!cfg.scope || !validId(cfg.scope.orgId) ||
      !validId(cfg.scope.workspaceId)) throw new Error("config_bad_scope");
  if (cfg.partitionScheme !== "scope-v1" &&
      cfg.partitionScheme !== "legacy-prefix-v1")
    throw new Error("config_bad_partitionScheme");
  // The legacy representation uses UUID prefixes; refuse truncation of
  // arbitrary identifiers into a different existing checkpoint namespace.
  const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
  if (cfg.partitionScheme === "legacy-prefix-v1" &&
      (!uuid.test(cfg.scope.orgId) || !uuid.test(cfg.scope.workspaceId)))
    throw new Error("config_bad_legacy_scope");
  if (typeof cfg.targetId !== "string" || !cfg.targetId.length ||
      cfg.targetId.length > 256) throw new Error("config_bad_target");
  if (typeof cfg.ownerId !== "string" || !cfg.ownerId.length ||
      cfg.ownerId.length > 128) throw new Error("config_bad_owner");
  for (const [k, min, max] of [
    ["leaseTtlMs", 60000, 300000], ["maxRetries", 0, 10],
    ["pageSize", 1, 1000],
  ] as const) {
    const v = cfg[k];
    if (!Number.isInteger(v) || v < min || v > max)
      throw new Error(`config_bad_${k}`);
  }
}

export class CanonicalConsumer {
  readonly cfg: ConsumerConfig;
  constructor(
    readonly db: SqlQuery,
    readonly journal: CanonicalJournalStore,
    readonly acks: CanonicalPageAckStore,
    cfg: ConsumerConfig,
  ) {
    validateConfig(cfg);
    // Snapshot the tenant and target together: mutating the caller's config
    // after construction cannot move a cached checkpoint to another tenant.
    this.cfg = cfg.enabled === true
      ? Object.freeze({ ...cfg, scope: Object.freeze({ ...cfg.scope! }) })
      : Object.freeze({ ...cfg });
    // The partition identity binds tenant + target — a new deployment target
    // gets a disjoint lease + checkpoint, never reusing the old one.
    // Derived lazily: a disabled consumer never hashes or touches the DB.
  }
  private partition: string | null = null;
  private ensurePartition(): string {
    if (this.partition === null) {
      const scope = this.cfg.scope!;
      this.partition = this.cfg.partitionScheme === "legacy-prefix-v1"
        ? `${scope.orgId.slice(0, 8)}-${scope.workspaceId.slice(0, 8)}-` +
          createHash("sha256").update(this.cfg.targetId).digest("hex").slice(0, 32)
        : "v1-" + createHash("sha256").update(JSON.stringify([
          "canonical-consumer-scope-v1", scope.orgId, scope.workspaceId,
          this.cfg.targetId,
        ])).digest("hex").slice(0, 60);
    }
    return this.partition;
  }

  private inScope(e: CanonicalJournalEntry): boolean {
    return e.scope.orgId === this.cfg.scope!.orgId &&
      e.scope.workspaceId === this.cfg.scope!.workspaceId;
  }

  private get on(): boolean { return this.cfg.enabled === true; }
  /** One atomic acquire/renew/takeover — the bound partition only. */
  async acquireLease(): Promise<boolean> {
    if (!this.on) return false;
    const { rows } = await this.db.query<{ n: number }>(
      `INSERT INTO ${LEASE}(partition,owner,heartbeat_at,ttl_ms)` +
        ` VALUES($1,$2,now(),$3) ON CONFLICT (partition)` +
        ` DO UPDATE SET owner=$2,heartbeat_at=now(),ttl_ms=$3` +
        ` WHERE ${LEASE}.owner=$2` +
        `    OR ${LEASE}.heartbeat_at <` +
        `       now() - (${LEASE}.ttl_ms || ' milliseconds')::interval` +
        ` RETURNING 1 AS n`,
      [this.ensurePartition(), this.cfg.ownerId, this.cfg.leaseTtlMs]);
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
        ` AND heartbeat_at >=` +
        `  now() - (${LEASE}.ttl_ms || ' milliseconds')::interval` +
        ` RETURNING 1 AS n`,
      [this.ensurePartition(), this.cfg.ownerId]);
    return rows.length > 0;
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
        `   AND heartbeat_at >=` +
        `    now() - (${LEASE}.ttl_ms || ' milliseconds')::interval)` +
        ` RETURNING 1 AS n`,
      [this.ensurePartition(), rev, this.cfg.ownerId]);
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
  /** Bounded drain outcome — the runner distinguishes hold vs progress. */
  async drain(
    images: CanonicalImagePort, ch: ClickHouseClient,
    signal?: AbortSignal,
  ): Promise<{ copied: number; dead: number; skipped: number;
              checkpoint: string;
              status: "idle" | "no_lease" | "held" | "advanced" |
                      "dead" | "aborted" }> {
    const none = (status: "idle" | "no_lease" | "held" | "dead" |
                          "aborted", checkpoint = "0") =>
      ({ copied: 0, dead: 0, skipped: 0, checkpoint, status });
    // The explicit gate: disabled returns a constant — no partition
    // derivation, no lease, no journal, no CH, zero DB queries.
    if (this.cfg.enabled !== true) return none("idle");
    if (!(await this.acquireLease()))
      return { ...none("no_lease"),
        checkpoint: await this.readCheckpoint() };
    await this.seedCheckpoint();
    let copied = 0, dead = 0, skipped = 0;
    let checkpoint = await this.readCheckpoint();
    let lastSeen = checkpoint;
    for (;;) {
      if (signal?.aborted) return { copied, dead, skipped, checkpoint,
        status: "aborted" };
      const { entries, hasMore } =
        await this.journal.entries(checkpoint, this.cfg.pageSize);
      for (const entry of entries) {
        if (signal?.aborted)
          return { copied, dead, skipped, checkpoint, status: "aborted" };
        // Strict revision order — a non-increasing page is a violation.
        if (BigInt(entry.revision) <= BigInt(lastSeen))
          return { copied, dead, skipped, checkpoint, status: "held" };
        lastSeen = entry.revision;
        if (!this.inScope(entry)) {
          if (!(await this.advanceCheckpoint(entry.revision)))
            return { copied, dead, skipped, checkpoint, status: "held" };
          checkpoint = entry.revision;
          skipped++;
          continue;
        }
        const r = await this.copyOnce(images, ch, checkpoint, entry, signal);
        if (r === "aborted")
          return { copied, dead, skipped, checkpoint, status: "aborted" };
        if (r === "copied") {
          // Metrics count a copy only after the fenced CAS accepted it.
          if (!(await this.advanceCheckpoint(entry.revision)))
            return { copied, dead, skipped, checkpoint, status: "held" };
          copied++;
          checkpoint = entry.revision;
        } else {
          if (r !== "hold") {
            dead++;
            await this.deadLetter(entry, r);
            return { copied, dead, skipped, checkpoint, status: "dead" };
          }
          return { copied, dead, skipped, checkpoint, status: "held" };
        }
        if (!(await this.heartbeat()))
          return { copied, dead, skipped, checkpoint, status: "held" };
      }
      if (!hasMore) break;
      checkpoint = lastSeen;
    }
    return { copied, dead, skipped, checkpoint,
      status: copied > 0 || skipped > 0 ? "advanced" : "idle" };
  }

  /** One bounded attempt set; only a stable poison code -> dead. */
  private async copyOnce(
    images: CanonicalImagePort, ch: ClickHouseClient,
    checkpoint: string, entry: CanonicalJournalEntry, signal?: AbortSignal,
  ): Promise<"copied" | "hold" | "aborted" | string> {
    // A write longer than the lease window is aborted: the page/finish guards
    // renew the lease, but a hung op must not outlive the TTL and let a
    // competing owner take over mid-write.
    const writeBudget = Math.min(this.cfg.leaseTtlMs / 2, 5000);
    for (let i = 0; i <= this.cfg.maxRetries; i++) {
      if (signal?.aborted) return "aborted";
      // One controller: timeout OR external abort cancels the op in-band —
      // the cycle checks the same signal at every side-effecting boundary,
      // so a timed-out op performs zero later CH/ACK/finish effects.
      const ctl = new AbortController();
      const onAbort = () => ctl.abort();
      signal?.addEventListener("abort", onAbort, { once: true });
      const timer = setTimeout(() => ctl.abort(), writeBudget);
      try {
        const r = await copyCanonicalEntry(
          images, this.acks, ch, { checkpoint }, entry, this.cfg.targetId,
          { beforeWrite: () => this.heartbeat(),
            beforeFinish: () => this.heartbeat() }, ctl.signal);
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        if (ctl.signal.aborted) return "aborted";
        if (r.kind === "copied") return "copied";
        const reason = (r as { reason: string }).reason;
        if (HOLD.has(reason)) return "hold";
        if (POISON[reason] && i === this.cfg.maxRetries)
          return reason;                        // exact stable poison code
        if (!POISON[reason]) return "hold";   // unknown -> hold, never poison
      } catch (e) {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        if (ctl.signal.aborted || signal?.aborted) return "aborted";
        return "hold";                          // infra: retryable hold
      }
      await new Promise((r) => setTimeout(r, Math.min(50 * (i + 1), 500)));
    }
    return "hold";
  }
}
