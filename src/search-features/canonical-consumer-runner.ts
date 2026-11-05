/**
 * Supervised runtime for the canonical consumer: a bounded poll loop over
 * drain() with single-flight, abortable sleep, and honest health. Default-off;
 * existing PG/CH bindings only. The fencing identity is ownerId + a per-run
 * nonce so two concurrent runs can never share lease ownership.
 */
import { CanonicalConsumer, type ConsumerConfig, validateConfig }
  from "./canonical-consumer.ts";
import type { SqlQuery, CanonicalJournalStore }
  from "./canonical-journal.ts";
import type { CanonicalPageAckStore } from "./canonical-ack-store.ts";
import type { CanonicalImagePort } from "./canonical-cycle.ts";
import type { ClickHouseClient }
  from "../session-pipeline/vendor/search-index/clickhouse.ts";
import { randomUUID } from "node:crypto";

export interface RuntimeConfig extends ConsumerConfig {
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
  /** idle | no_lease | held | advanced | dead | aborted — last drain's. */
  readonly status: string;
  readonly holds: number;
  readonly deads: number;
  readonly noLease: number;
  readonly healthy: boolean;   // last drain advanced OR idle, never a hold
  readonly lastAt: string | null;
}

// Bounded collision-resistant fencing identity: a fresh nonce per RUN,
// capped under the 128-char owner bound regardless of the base length.
const fencedOwner = (ownerId: string) =>
  `${ownerId.slice(0, 48)}:${randomUUID().slice(0, 8)}`;

const abortableSleep = (ms: number, sig?: AbortSignal) =>
  new Promise<void>((res) => {
    if (sig?.aborted) return res();
    const t = setTimeout(res, ms);
    sig?.addEventListener("abort", () => { clearTimeout(t); res(); },
      { once: true });
  });

export class CanonicalConsumerRunner {
  private stats = { runs: 0, copied: 0, dead: 0, skipped: 0,
    checkpoint: "0", status: "idle", holds: 0, deads: 0, noLease: 0,
    healthy: true, lastAt: null as string | null };
  private running = false;
  private stopped = false;
  private readonly makeConsumer: () => CanonicalConsumer;
  constructor(
    consumer: CanonicalConsumer | (() => CanonicalConsumer),
    private readonly images: CanonicalImagePort,
    private readonly ch: ClickHouseClient,
    private readonly cfg: RuntimeConfig,
  ) {
    validateRuntimeConfig(cfg);
    this.makeConsumer =
      typeof consumer === "function" ? consumer : () => consumer;
  }

  static make(
    db: SqlQuery, journal: CanonicalJournalStore,
    acks: CanonicalPageAckStore, images: CanonicalImagePort,
    ch: ClickHouseClient, cfg: RuntimeConfig,
  ): CanonicalConsumerRunner {
    // Per-RUN fencing: each run() builds a consumer with a fresh nonce —
    // a redeployed instance can never share the prior run's lease identity.
    return new CanonicalConsumerRunner(
      () => new CanonicalConsumer(db, journal, acks,
        { ...cfg, ownerId: fencedOwner(cfg.ownerId) }),
      images, ch, cfg);
  }

  health(): RuntimeHealth {
    return { enabled: this.cfg.enabled, ...this.stats };
  }
  stop(): void { this.stopped = true; }

  /**
   * Bounded single-flight poll loop: refuses a concurrent run; abortable
   * sleep; an abort signal is propagated into drain; the deadline bounds the
   * whole run (each drain/copy op carries its own per-write timeout below
   * the lease TTL, so a hung op cannot outlive ownership).
   */
  async run(signal?: AbortSignal): Promise<RuntimeHealth> {
    if (this.cfg.enabled !== true) return this.health();
    if (this.running) return { ...this.health(), status: "already_running" };
    this.running = true;
    try {
      const deadline = Date.now() + this.cfg.deadlineMs;
      while (!this.stopped && !signal?.aborted && Date.now() < deadline) {
        try {
          // Remaining budget bounds THIS drain: external abort OR the run
          // deadline cancels the operation in-band via AbortSignal.any.
          const remaining = Math.max(1, deadline - Date.now());
          const bounded = AbortSignal.any
            ? AbortSignal.any(
                [signal ?? new AbortController().signal,
                 AbortSignal.timeout(remaining)])
            : signal;
          const r = await this.makeConsumer().drain(
            this.images, this.ch, bounded);
          this.stats = { ...this.stats, runs: this.stats.runs + 1,
            copied: this.stats.copied + r.copied,
            dead: this.stats.dead + r.dead,
            skipped: this.stats.skipped + r.skipped,
            checkpoint: r.checkpoint, status: r.status,
            holds: this.stats.holds + (r.status === "held" ? 1 : 0),
            deads: this.stats.deads + (r.status === "dead" ? 1 : 0),
            noLease: this.stats.noLease + (r.status === "no_lease" ? 1 : 0),
            // only progress or clean idle is healthy — never a hold/dead
            healthy: r.status === "advanced" || r.status === "idle",
            lastAt: new Date().toISOString() };
        } catch {
          this.stats = { ...this.stats, healthy: false, status: "error",
            lastAt: new Date().toISOString() };
        }
        await abortableSleep(this.cfg.pollIntervalMs, signal);
      }
      if (signal?.aborted || this.stopped ||
          Date.now() >= deadline)
        this.stats.status = signal?.aborted ? "aborted" : "deadline";
      return this.health();
    } finally {
      this.running = false;
    }
  }
}
