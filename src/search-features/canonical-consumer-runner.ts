/**
 * Supervised runtime for the canonical consumer: a bounded poll loop over
 * drain() with abort, health/metrics export, and a default-off gate. Uses
 * the existing PG/CH bindings — no new secrets or deployment surface.
 * A run returns when aborted or when the deadline is hit; an unhealthy run
 * surfaces via health() rather than silently looping.
 */
import { CanonicalConsumer, type ConsumerConfig, validateConfig }
  from "./canonical-consumer.ts";
import type { SqlQuery, CanonicalJournalStore }
  from "./canonical-journal.ts";
import type { CanonicalPageAckStore } from "./canonical-ack-store.ts";
import type { CanonicalImagePort } from "./canonical-cycle.ts";
import type { ClickHouseClient }
  from "../session-pipeline/vendor/search-index/clickhouse.ts";

export interface RuntimeConfig extends ConsumerConfig {
  /** Bounded poll cadence + a hard deadline so the loop always terminates. */
  pollIntervalMs: number;
  deadlineMs: number;
}
export function validateRuntimeConfig(cfg: RuntimeConfig): void {
  validateConfig(cfg);
  if (cfg.enabled !== true) return;
  if (!Number.isInteger(cfg.pollIntervalMs) ||
      cfg.pollIntervalMs < 50 || cfg.pollIntervalMs > 600000)
    throw new Error("config_bad_pollIntervalMs");
  if (!Number.isInteger(cfg.deadlineMs) ||
      cfg.deadlineMs < cfg.pollIntervalMs || cfg.deadlineMs > 86400000)
    throw new Error("config_bad_deadlineMs");
}
export interface RuntimeHealth {
  readonly enabled: boolean;
  readonly runs: number;
  readonly copied: number;
  readonly dead: number;
  readonly skipped: number;
  readonly checkpoint: string;
  readonly healthy: boolean;      // last drain completed without hold
  readonly lastAt: string | null;
}

export class CanonicalConsumerRunner {
  private stats = { runs: 0, copied: 0, dead: 0, skipped: 0,
    checkpoint: "0", healthy: true, lastAt: null as string | null };
  private stopped = false;
  constructor(
    private readonly consumer: CanonicalConsumer,
    private readonly images: CanonicalImagePort,
    private readonly ch: ClickHouseClient,
    private readonly cfg: RuntimeConfig,
  ) { validateRuntimeConfig(cfg); }

  static make(
    db: SqlQuery, journal: CanonicalJournalStore,
    acks: CanonicalPageAckStore, images: CanonicalImagePort,
    ch: ClickHouseClient, cfg: RuntimeConfig,
  ): CanonicalConsumerRunner {
    return new CanonicalConsumerRunner(
      new CanonicalConsumer(db, journal, acks, cfg), images, ch, cfg);
  }

  health(): RuntimeHealth {
    return { enabled: this.cfg.enabled, ...this.stats };
  }
  stop(): void { this.stopped = true; }

  /** Bounded poll loop — returns when aborted/stopped or at the deadline. */
  async run(signal?: AbortSignal): Promise<RuntimeHealth> {
    if (this.cfg.enabled !== true) return this.health();
    const deadline = Date.now() + this.cfg.deadlineMs;
    while (!this.stopped && !signal?.aborted && Date.now() < deadline) {
      try {
        const r = await this.consumer.drain(this.images, this.ch);
        this.stats = { runs: this.stats.runs + 1,
          copied: this.stats.copied + r.copied,
          dead: this.stats.dead + r.dead,
          skipped: this.stats.skipped + r.skipped,
          checkpoint: r.checkpoint, healthy: true,
          lastAt: new Date().toISOString() };
      } catch {
        this.stats.healthy = false;
      }
      await new Promise((r) => setTimeout(r, this.cfg.pollIntervalMs));
    }
    return this.health();
  }
}
