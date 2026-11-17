import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  DEFAULT_TABLES,
  LockHoldEvidence,
  observerConfig,
  TURNS_MIGRATION_SHA256,
  type ObserverSample,
} from "../../../scripts/pg-proof/lock-hold-observer.ts";

const ROOT = join(import.meta.dir, "../../..");
const DATABASE_URL = "postgres://observer@127.0.0.1:5432/postgres";
const config = () => ({
  tables: [...DEFAULT_TABLES],
  intervalMs: 100,
  maxWriterWaitMs: 5_000,
  maxHoldMs: 120_000,
});
const sample = (
  atMs: number,
  values: Partial<Omit<ObserverSample, "atMs">> = {},
): ObserverSample => ({
  atMs,
  migrationPids: [],
  locks: [],
  ledgerSha256: null,
  ...values,
});

describe("turns 062020 lock observer inputs", () => {
  test("accepts passwordless loopback only and keeps credentials out of errors", () => {
    expect(observerConfig([], { OBSERVER_DATABASE_URL: DATABASE_URL })).toEqual({
      databaseUrl: DATABASE_URL,
      tables: [...DEFAULT_TABLES],
      intervalMs: 100,
      maxSeconds: 180,
      maxWriterWaitMs: 5_000,
      maxHoldMs: 120_000,
    });
    expect(() =>
      observerConfig([], {
        OBSERVER_DATABASE_URL: "postgres://observer@db.example.invalid/postgres",
      }),
    ).toThrow("OBSERVER_DATABASE_URL is malformed");
    expect(() =>
      observerConfig([], {
        OBSERVER_DATABASE_URL:
          "postgres://observer:do-not-echo@db.example.invalid",
      }),
    ).toThrow("OBSERVER_DATABASE_URL is malformed");
    try {
      observerConfig([], {
        OBSERVER_DATABASE_URL:
          "postgres://observer:do-not-echo@db.example.invalid",
      });
    } catch (error) {
      expect(String(error)).not.toContain("do-not-echo");
    }
    expect(
      observerConfig([], {
        OBSERVER_DATABASE_URL:
          "postgres://observer:secret@db.example.invalid/postgres?sslmode=verify-full",
      }).databaseUrl,
    ).toContain("sslmode=verify-full");
  });

  test("parses bounded evidence controls and refuses malformed tables or flags", () => {
    expect(
      observerConfig(
        [
          "--tables",
          "sessions.upload_deletions,sessions.session_directory",
          "--interval-ms",
          "25",
          "--max-seconds",
          "900",
          "--max-writer-wait-ms",
          "5001",
          "--max-hold-ms",
          "120001",
          "--until-file",
          "/tmp/observer.done",
        ],
        { OBSERVER_DATABASE_URL: DATABASE_URL },
      ),
    ).toMatchObject({
      tables: ["sessions.upload_deletions", "sessions.session_directory"],
      intervalMs: 25,
      maxSeconds: 900,
      maxWriterWaitMs: 5_001,
      maxHoldMs: 120_001,
      untilFile: "/tmp/observer.done",
    });
    for (const args of [
      ["--tables", "public.upload_deletions"],
      ["--tables", "sessions.upload_deletions,sessions.upload_deletions"],
      ["--interval-ms", "24"],
      ["--max-seconds", "901"],
      ["--unknown", "1"],
    ])
      expect(() =>
        observerConfig(args, { OBSERVER_DATABASE_URL: DATABASE_URL }),
      ).toThrow();
  });
});

describe("turns 062020 lock evidence", () => {
  test("records migration holds and blocked writers, then binds success to the ledger", () => {
    const evidence = new LockHoldEvidence(config());
    evidence.observe(
      sample(0, {
        migrationPids: [10],
        locks: [
          {
            pid: 10,
            relation: "sessions.upload_deletions",
            mode: "ShareRowExclusiveLock",
            granted: true,
            blockingPids: [],
          },
          {
            pid: 20,
            relation: "sessions.upload_deletions",
            mode: "RowExclusiveLock",
            granted: false,
            blockingPids: [10],
          },
        ],
      }),
    );
    evidence.observe(
      sample(1_000, {
        locks: [
          {
            pid: 10,
            relation: "sessions.upload_deletions",
            mode: "ShareRowExclusiveLock",
            granted: true,
            blockingPids: [],
          },
          {
            pid: 20,
            relation: "sessions.upload_deletions",
            mode: "RowExclusiveLock",
            granted: false,
            blockingPids: [10],
          },
        ],
      }),
    );
    evidence.observe(
      sample(1_100, { ledgerSha256: TURNS_MIGRATION_SHA256 }),
    );
    expect(evidence.summary("ledger")).toEqual({
      type: "summary",
      ok: true,
      migration_seen: true,
      ledger_seen: true,
      ledger_sha256: TURNS_MIGRATION_SHA256,
      max_hold_ms: {
        "sessions.upload_deletions": 1_100,
        "sessions.operator_user_erasures": 0,
        "sessions.session_directory": 0,
      },
      max_wait_ms: 1_100,
      max_migration_wait_ms: 0,
      max_waiters: 1,
      blocked_writer_count: 1,
      target_lock_observed: true,
      representative: true,
      error_code: null,
      abort_evidence: [],
    });
  });

  test("flags writer and migration waits only after the five-second ceiling", () => {
    const evidence = new LockHoldEvidence(config());
    const waits = (atMs: number) =>
      evidence.observe(
        sample(atMs, {
          migrationPids: [10],
          locks: [
            {
              pid: 10,
              relation: "sessions.operator_user_erasures",
              mode: "ShareRowExclusiveLock",
              granted: false,
              blockingPids: [30],
            },
            {
              pid: 20,
              relation: "sessions.session_directory",
              mode: "RowExclusiveLock",
              granted: false,
              blockingPids: [10],
            },
          ],
        }),
      );
    waits(0);
    waits(5_000);
    evidence.observe(sample(5_100, { ledgerSha256: TURNS_MIGRATION_SHA256 }));
    expect(evidence.summary("ledger")).toMatchObject({
      ok: false,
      max_wait_ms: 5_100,
      max_migration_wait_ms: 5_100,
      abort_evidence: [
        "blocked_writer_wait_exceeded",
        "migration_lock_wait_exceeded",
      ],
    });
  });

  test("counts relation readers as waiters but not blocked writers", () => {
    const evidence = new LockHoldEvidence(config());
    evidence.observe(
      sample(0, {
        migrationPids: [10],
        locks: [
          {
            pid: 20,
            relation: "sessions.session_directory",
            mode: "AccessShareLock",
            granted: false,
            blockingPids: [10],
          },
        ],
      }),
    );
    evidence.observe(
      sample(5_000, {
        locks: [
          {
            pid: 20,
            relation: "sessions.session_directory",
            mode: "AccessShareLock",
            granted: false,
            blockingPids: [10],
          },
        ],
        ledgerSha256: TURNS_MIGRATION_SHA256,
      }),
    );
    expect(evidence.summary("ledger")).toMatchObject({
      ok: true,
      max_waiters: 1,
      blocked_writer_count: 0,
      max_wait_ms: 0,
      abort_evidence: [],
    });
  });

  test("fails closed on checksum drift, deadlines, and external completion", () => {
    const mismatch = new LockHoldEvidence(config());
    mismatch.observe(sample(0, { migrationPids: [10] }));
    mismatch.observe(
      sample(100, { ledgerSha256: "0".repeat(64) }),
    );
    expect(mismatch.summary("ledger")).toMatchObject({
      ok: false,
      abort_evidence: ["ledger_checksum_mismatch"],
    });

    const deadline = new LockHoldEvidence(config());
    deadline.observe(sample(0, { migrationPids: [10] }));
    expect(deadline.summary("max_seconds").abort_evidence).toEqual([
      "ledger_not_observed",
      "migration_disappeared_before_ledger",
      "observer_deadline_exceeded",
    ]);

    const signalled = new LockHoldEvidence(config());
    expect(signalled.summary("until_file").abort_evidence).toEqual([
      "completion_signal_before_ledger",
      "ledger_not_observed",
      "migration_not_observed",
    ]);
  });

  test("refuses a matching ledger that predates attachment or appears with the first migration sample", () => {
    const preexisting = new LockHoldEvidence(config());
    preexisting.observe(
      sample(0, {
        migrationPids: [10],
        ledgerSha256: TURNS_MIGRATION_SHA256,
      }),
    );
    expect(preexisting.summary("ledger").abort_evidence).toEqual([
      "ledger_absence_not_observed",
      "migration_not_observed_before_ledger",
    ]);

    const sameSample = new LockHoldEvidence(config());
    sameSample.observe(sample(0));
    sameSample.observe(
      sample(100, {
        migrationPids: [10],
        ledgerSha256: TURNS_MIGRATION_SHA256,
      }),
    );
    expect(sameSample.summary("ledger").abort_evidence).toEqual([
      "migration_not_observed_before_ledger",
    ]);
  });

  test("a zero-hold ordered run passes but is explicitly non-representative", () => {
    const evidence = new LockHoldEvidence(config());
    evidence.observe(sample(0, { migrationPids: [10] }));
    evidence.observe(
      sample(100, { ledgerSha256: TURNS_MIGRATION_SHA256 }),
    );
    expect(evidence.summary("ledger")).toMatchObject({
      ok: true,
      target_lock_observed: false,
      representative: false,
    });
  });

  test("query failures retain only a SQLSTATE-shaped code", () => {
    const evidence = new LockHoldEvidence(config());
    evidence.addError({ code: "42501", message: "do-not-emit-server-message" });
    const summary = evidence.summary("error");
    expect(summary.error_code).toBe("42501");
    expect(summary.abort_evidence).toContain("observer_query_failed");
    expect(JSON.stringify(summary)).not.toContain("do-not-emit-server-message");
  });
});

test("a positional secret is never echoed in the JSONL refusal", () => {
  const secret = "postgres://observer:never-print@db.example.invalid/postgres";
  const result = spawnSync(
    process.execPath,
    [join(ROOT, "scripts/pg-proof/lock-hold-observer.ts"), secret],
    {
      encoding: "utf8",
      env: { ...process.env, OBSERVER_DATABASE_URL: DATABASE_URL },
    },
  );
  expect(result.status).toBe(1);
  expect(result.stdout).toContain("argument 1 is unknown");
  expect(result.stdout).not.toContain(secret);
  expect(result.stdout).not.toContain("never-print");
});

test("the disposable rehearsal wires the observer without putting a URL in argv", () => {
  const workflow = readFileSync(
    join(ROOT, ".github/workflows/rehearse-session-schema.yml"),
    "utf8",
  );
  expect(workflow).toContain("observe_turns_lock_hold:");
  expect(workflow).toContain("OBSERVER_DATABASE_URL=");
  expect(workflow).not.toMatch(/^\s+OBSERVER_DATABASE_URL:/m);
  expect(workflow).toContain("scripts/pg-proof/lock-hold-observer.ts");
  expect(workflow).toContain("turns-lock-hold.jsonl");
  expect(workflow).toContain("--until-file");
  expect(workflow).toContain("--interval-ms 25");
  expect(workflow.match(/OBSERVER_DATABASE_URL=/g)).toHaveLength(1);
  expect(workflow).toMatch(
    /OBSERVER_DATABASE_URL=postgres:\/\/supabase_admin@127\.0\.0\.1:5432\/postgres \\\n\s+bun \.rehearsal-tooling\/scripts\/pg-proof\/lock-hold-observer\.ts/,
  );
  expect(workflow).not.toContain(
    "lock-hold-observer.ts --database-url",
  );
});
