/**
 * Read-only lock observer for the 062020 turns-forward-gates rehearsal.
 *
 * It never applies SQL. Run it beside the guarded migrations Job and give the
 * database URL only through OBSERVER_DATABASE_URL. Evidence contains relation
 * names, backend PIDs, lock modes and durations; it never contains the URL,
 * usernames, query text or database row contents.
 */
import { existsSync } from "node:fs";
import pg from "pg";

export const TURNS_MIGRATION =
  "upstream:20261105062020_turns_forward_gates.sql";
export const TURNS_MIGRATION_SHA256 =
  "63ca3c75bd1b09ec32369e761446da6a592e57fd90aca6640e9fb6a5dbd8ff5a";
export const DEFAULT_TABLES = [
  "sessions.upload_deletions",
  "sessions.operator_user_erasures",
  "sessions.session_directory",
] as const;

export interface ObserverConfig {
  databaseUrl: string;
  tables: string[];
  intervalMs: number;
  maxSeconds: number;
  maxWriterWaitMs: number;
  maxHoldMs: number;
  untilFile?: string;
}

export interface LockSample {
  pid: number;
  relation: string;
  mode: string;
  granted: boolean;
  blockingPids: number[];
}

export interface ObserverSample {
  atMs: number;
  migrationPids: number[];
  locks: LockSample[];
  ledgerSha256: string | null;
}

export interface ObserverSummary {
  type: "summary";
  ok: boolean;
  migration_seen: boolean;
  ledger_seen: boolean;
  ledger_sha256: string | null;
  max_hold_ms: Record<string, number>;
  max_wait_ms: number;
  max_migration_wait_ms: number;
  max_waiters: number;
  blocked_writer_count: number;
  abort_evidence: string[];
}

const integer = (
  name: string,
  raw: string,
  min: number,
  max: number,
): number => {
  if (!/^[0-9]+$/u.test(raw))
    throw new Error(`turns lock observer: ${name} must be decimal digits`);
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < min || value > max)
    throw new Error(
      `turns lock observer: ${name} must be between ${min} and ${max}`,
    );
  return value;
};

const argument = (args: string[], at: number, name: string): string => {
  const value = args[at + 1];
  if (value === undefined || value.startsWith("--"))
    throw new Error(`turns lock observer: ${name} needs a value`);
  return value;
};

export function observerConfig(
  args: string[],
  env: Record<string, string | undefined>,
): ObserverConfig {
  const databaseUrl = env.OBSERVER_DATABASE_URL;
  if (!databaseUrl)
    throw new Error(
      "turns lock observer: OBSERVER_DATABASE_URL is required",
    );
  try {
    const parsed = new URL(databaseUrl);
    const loopback = ["127.0.0.1", "::1", "localhost"].includes(
      parsed.hostname,
    );
    if (
      !["postgres:", "postgresql:"].includes(parsed.protocol) ||
      !parsed.hostname ||
      !parsed.username ||
      (!loopback && !parsed.password) ||
      parsed.pathname.length < 2
    )
      throw new Error("shape");
  } catch {
    throw new Error(
      "turns lock observer: OBSERVER_DATABASE_URL is malformed",
    );
  }

  let tables = [...DEFAULT_TABLES] as string[];
  let intervalMs = 100;
  let maxSeconds = 180;
  let maxWriterWaitMs = 5_000;
  let maxHoldMs = 120_000;
  let untilFile: string | undefined;
  for (let i = 0; i < args.length; i += 1) {
    const flag = args[i]!;
    if (flag === "--tables") {
      tables = argument(args, i, flag).split(",");
      i += 1;
    } else if (flag === "--interval-ms") {
      intervalMs = integer(flag, argument(args, i, flag), 25, 5_000);
      i += 1;
    } else if (flag === "--max-seconds") {
      maxSeconds = integer(flag, argument(args, i, flag), 1, 900);
      i += 1;
    } else if (flag === "--max-writer-wait-ms") {
      maxWriterWaitMs = integer(flag, argument(args, i, flag), 1, 120_000);
      i += 1;
    } else if (flag === "--max-hold-ms") {
      maxHoldMs = integer(flag, argument(args, i, flag), 1, 300_000);
      i += 1;
    } else if (flag === "--until-file") {
      untilFile = argument(args, i, flag);
      i += 1;
    } else {
      throw new Error(`turns lock observer: unknown argument ${flag}`);
    }
  }
  if (
    tables.length === 0 ||
    tables.length > 16 ||
    new Set(tables).size !== tables.length ||
    tables.some(
      (table) => !/^sessions\.[a-z_][a-z0-9_]*$/u.test(table),
    )
  )
    throw new Error("turns lock observer: tables are malformed");
  if (untilFile !== undefined && untilFile.length === 0)
    throw new Error("turns lock observer: until-file is empty");
  return {
    databaseUrl,
    tables,
    intervalMs,
    maxSeconds,
    maxWriterWaitMs,
    maxHoldMs,
    ...(untilFile === undefined ? {} : { untilFile }),
  };
}

interface SeenInterval {
  first: number;
  last: number;
}

const lockKey = (lock: LockSample) =>
  `${lock.pid}\u001f${lock.relation}\u001f${lock.mode}`;
const WRITER_LOCK_MODES = new Set([
  "RowExclusiveLock",
  "ShareUpdateExclusiveLock",
  "ShareRowExclusiveLock",
  "ExclusiveLock",
  "AccessExclusiveLock",
]);

/** Pure evidence accumulator; exported so its fail-closed classifications are unit tested. */
export class LockHoldEvidence {
  private readonly knownMigrationPids = new Set<number>();
  private readonly activeHolds = new Map<string, SeenInterval>();
  private readonly activeWaits = new Map<string, SeenInterval>();
  private readonly blockedWriters = new Set<number>();
  private readonly abort = new Set<string>();
  private readonly maxHoldByTable: Record<string, number>;
  private maxWaitMs = 0;
  private maxMigrationWaitMs = 0;
  private maxWaiters = 0;
  private ledgerSha256: string | null = null;
  private migrationSeen = false;

  constructor(
    private readonly config: Pick<
      ObserverConfig,
      "tables" | "intervalMs" | "maxWriterWaitMs" | "maxHoldMs"
    >,
  ) {
    this.maxHoldByTable = Object.fromEntries(
      config.tables.map((table) => [table, 0]),
    );
  }

  addAbort(code: string): void {
    this.abort.add(code);
  }

  observe(sample: ObserverSample): void {
    for (const pid of sample.migrationPids) {
      this.knownMigrationPids.add(pid);
      this.migrationSeen = true;
    }
    if (sample.ledgerSha256 !== null) {
      this.ledgerSha256 = sample.ledgerSha256;
      if (sample.ledgerSha256 !== TURNS_MIGRATION_SHA256)
        this.abort.add("ledger_checksum_mismatch");
    }

    const presentHolds = new Set<string>();
    const presentWaits = new Set<string>();
    const waitingPids = new Set<number>();
    for (const lock of sample.locks) {
      const migration = this.knownMigrationPids.has(lock.pid);
      const blockedByMigration = lock.blockingPids.some((pid) =>
        this.knownMigrationPids.has(pid),
      );
      const key = lockKey(lock);
      if (lock.granted && migration) {
        presentHolds.add(key);
        const seen = this.activeHolds.get(key) ?? {
          first: sample.atMs,
          last: sample.atMs,
        };
        seen.last = sample.atMs;
        this.activeHolds.set(key, seen);
        const duration = sample.atMs - seen.first + this.config.intervalMs;
        this.maxHoldByTable[lock.relation] = Math.max(
          this.maxHoldByTable[lock.relation] ?? 0,
          duration,
        );
        if (duration > this.config.maxHoldMs)
          this.abort.add("lock_hold_ceiling_exceeded");
      }
      if (!lock.granted && (migration || blockedByMigration)) {
        presentWaits.add(key);
        waitingPids.add(lock.pid);
        const seen = this.activeWaits.get(key) ?? {
          first: sample.atMs,
          last: sample.atMs,
        };
        seen.last = sample.atMs;
        this.activeWaits.set(key, seen);
        const duration = sample.atMs - seen.first + this.config.intervalMs;
        if (migration) {
          this.maxMigrationWaitMs = Math.max(
            this.maxMigrationWaitMs,
            duration,
          );
          if (duration > this.config.maxWriterWaitMs)
            this.abort.add("migration_lock_wait_exceeded");
        } else {
          if (WRITER_LOCK_MODES.has(lock.mode)) {
            this.blockedWriters.add(lock.pid);
            this.maxWaitMs = Math.max(this.maxWaitMs, duration);
            if (duration > this.config.maxWriterWaitMs)
              this.abort.add("blocked_writer_wait_exceeded");
          }
        }
      }
    }
    this.maxWaiters = Math.max(this.maxWaiters, waitingPids.size);
    for (const key of this.activeHolds.keys())
      if (!presentHolds.has(key)) this.activeHolds.delete(key);
    for (const key of this.activeWaits.keys())
      if (!presentWaits.has(key)) this.activeWaits.delete(key);
  }

  summary(
    stopReason: "ledger" | "until_file" | "max_seconds" | "error",
  ): ObserverSummary {
    const ledgerSeen = this.ledgerSha256 !== null;
    if (!this.migrationSeen) this.abort.add("migration_not_observed");
    if (!ledgerSeen) {
      this.abort.add("ledger_not_observed");
      if (this.migrationSeen) this.abort.add("migration_disappeared_before_ledger");
    }
    if (stopReason === "until_file" && !ledgerSeen)
      this.abort.add("completion_signal_before_ledger");
    if (stopReason === "max_seconds" && !ledgerSeen)
      this.abort.add("observer_deadline_exceeded");
    if (stopReason === "error") this.abort.add("observer_query_failed");
    const abortEvidence = [...this.abort].sort();
    return {
      type: "summary",
      ok: abortEvidence.length === 0,
      migration_seen: this.migrationSeen,
      ledger_seen: ledgerSeen,
      ledger_sha256: this.ledgerSha256,
      max_hold_ms: { ...this.maxHoldByTable },
      max_wait_ms: this.maxWaitMs,
      max_migration_wait_ms: this.maxMigrationWaitMs,
      max_waiters: this.maxWaiters,
      blocked_writer_count: this.blockedWriters.size,
      abort_evidence: abortEvidence,
    };
  }
}

interface ActivityRow {
  pid: number;
}
interface RelationRow {
  relation: string;
  oid: number | string | null;
}
interface DatabaseLockRow {
  pid: number;
  relation: string;
  mode: string;
  granted: boolean;
  blocking_pids: number[] | null;
}

const delay = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));
const emit = (value: unknown) =>
  process.stdout.write(`${JSON.stringify(value)}\n`);

async function relationOids(
  client: pg.Client,
  tables: string[],
): Promise<{ oids: number[]; missing: string[] }> {
  const relations = await client.query<RelationRow>(
    "SELECT name AS relation,to_regclass(name)::oid AS oid FROM unnest($1::text[]) AS name ORDER BY name",
    [tables],
  );
  return {
    oids: relations.rows
      .filter((row) => row.oid !== null)
      .map((row) => Number(row.oid)),
    missing: relations.rows
      .filter((row) => row.oid === null)
      .map((row) => row.relation),
  };
}

async function collect(
  client: pg.Client,
  relationOids: number[],
): Promise<Omit<ObserverSample, "atMs">> {
  const activities = await client.query<ActivityRow>(`
    SELECT pid
    FROM pg_catalog.pg_stat_activity
    WHERE pid <> pg_catalog.pg_backend_pid()
      AND application_name <> 'turns-062020-lock-observer'
      AND (
        position('SLOT 062020' in query) > 0
        OR position('20261105062020_turns_forward_gates.sql' in query) > 0
        OR application_name LIKE 'hoopdb-migrate-062020%'
      )
    ORDER BY pid`);
  const locks = await client.query<DatabaseLockRow>(`
    SELECT l.pid,
      pg_catalog.format('%I.%I', n.nspname, c.relname) AS relation,
      l.mode,l.granted,pg_catalog.pg_blocking_pids(l.pid) AS blocking_pids
    FROM pg_catalog.pg_locks l
    JOIN pg_catalog.pg_class c ON c.oid=l.relation
    JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
    WHERE l.locktype='relation' AND l.relation=ANY($1::oid[])
    ORDER BY l.pid,relation,l.mode`, [relationOids]);
  let ledgerSha256: string | null = null;
  try {
    const ledger = await client.query<{ sha256: string }>(
      "SELECT sha256 FROM sessions.__96bulls_migrations WHERE name=$1",
      [TURNS_MIGRATION],
    );
    ledgerSha256 = ledger.rows[0]?.sha256 ?? null;
  } catch (error) {
    if ((error as { code?: string }).code !== "42P01") throw error;
  }
  return {
    migrationPids: activities.rows.map((row) => Number(row.pid)),
    locks: locks.rows.map((row) => ({
      pid: Number(row.pid),
      relation: row.relation,
      mode: row.mode,
      granted: row.granted,
      blockingPids: (row.blocking_pids ?? []).map(Number),
    })),
    ledgerSha256,
  };
}

export async function runObserver(
  config: ObserverConfig,
): Promise<ObserverSummary> {
  const client = new pg.Client({
    connectionString: config.databaseUrl,
    application_name: "turns-062020-lock-observer",
    options:
      "-c default_transaction_read_only=on -c statement_timeout=1000 -c lock_timeout=1000 -c idle_in_transaction_session_timeout=5000",
    connectionTimeoutMillis: 10_000,
  });
  const evidence = new LockHoldEvidence(config);
  let stopReason: "ledger" | "until_file" | "max_seconds" | "error" =
    "max_seconds";
  try {
    await client.connect();
    const settings = await client.query<{
      read_only: string;
      statement_timeout: string;
    }>(
      "SELECT current_setting('default_transaction_read_only') AS read_only,current_setting('statement_timeout') AS statement_timeout",
    );
    if (
      settings.rows[0]?.read_only !== "on" ||
      settings.rows[0]?.statement_timeout !== "1s"
    )
      throw new Error("observer_read_only_settings");
    emit({ type: "connected", tables: config.tables });
    const started = Date.now();
    let resolved: Awaited<ReturnType<typeof relationOids>>;
    for (;;) {
      resolved = await relationOids(client, config.tables);
      if (resolved.missing.length === 0) break;
      if (config.untilFile && existsSync(config.untilFile)) {
        evidence.addAbort("target_relation_missing");
        stopReason = "until_file";
        return evidence.summary(stopReason);
      }
      if (Date.now() - started >= config.maxSeconds * 1_000) {
        evidence.addAbort("target_relation_missing");
        stopReason = "max_seconds";
        return evidence.summary(stopReason);
      }
      await delay(config.intervalMs);
    }
    emit({
      type: "ready",
      migration: TURNS_MIGRATION,
      migration_sha256: TURNS_MIGRATION_SHA256,
      tables: config.tables,
      interval_ms: config.intervalMs,
      max_seconds: config.maxSeconds,
      max_writer_wait_ms: config.maxWriterWaitMs,
      max_hold_ms: config.maxHoldMs,
    });
    for (;;) {
      const atMs = Date.now();
      if (config.untilFile && existsSync(config.untilFile)) {
        stopReason = "until_file";
        break;
      }
      const sample = { atMs, ...(await collect(client, resolved.oids)) };
      evidence.observe(sample);
      if (
        sample.migrationPids.length > 0 ||
        sample.locks.length > 0 ||
        sample.ledgerSha256 !== null
      )
        emit({
          type: "sample",
          at: new Date(atMs).toISOString(),
          migration_pids: sample.migrationPids,
          ledger_sha256: sample.ledgerSha256,
          locks: sample.locks.map((lock) => ({
            pid: lock.pid,
            relation: lock.relation,
            mode: lock.mode,
            granted: lock.granted,
            blocking_pids: lock.blockingPids,
          })),
        });
      if (sample.ledgerSha256 !== null) {
        stopReason = "ledger";
        break;
      }
      if (Date.now() - started >= config.maxSeconds * 1_000) {
        stopReason = "max_seconds";
        break;
      }
      await delay(config.intervalMs);
    }
  } catch {
    stopReason = "error";
  } finally {
    await client.end().catch(() => {});
  }
  return evidence.summary(stopReason);
}

async function main(): Promise<void> {
  try {
    const config = observerConfig(process.argv.slice(2), process.env);
    const summary = await runObserver(config);
    emit(summary);
    process.exitCode = summary.ok ? 0 : 1;
  } catch (error) {
    emit({
      type: "summary",
      ok: false,
      abort_evidence: [
        error instanceof Error ? error.message : "turns lock observer: failed",
      ],
    });
    process.exitCode = 1;
  }
}

if (import.meta.main) await main();
