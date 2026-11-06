/**
 * Shape checks for the dormant Output/digest ClickHouse DDL, plus (when
 * READ_API_TEST_CLICKHOUSE_URL is set, as in CI) an apply against a real
 * ClickHouse in a throwaway database.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { createClickHouseClient, type ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import { schemaStatements } from "../vendor/search-index/schema.ts";
import { EVENT_FACTS_TABLE, OUTPUT_FACTS_TABLE, outputFactsSchemaStatements } from "../output-facts-schema.ts";

const CH_URL = process.env.READ_API_TEST_CLICKHOUSE_URL;
const CH_USER = process.env.READ_API_TEST_CLICKHOUSE_USER ?? "default";
const CH_PASSWORD = process.env.READ_API_TEST_CLICKHOUSE_PASSWORD ?? "";

describe("output facts DDL shape", () => {
  const statements = outputFactsSchemaStatements();
  test("two idempotent tables, monthly partitions, ReplacingMergeTree(version), tenant-first keys", () => {
    expect(statements).toHaveLength(2);
    for (const [i, table] of [EVENT_FACTS_TABLE, OUTPUT_FACTS_TABLE].entries()) {
      const s = statements[i]!;
      expect(s.startsWith(`CREATE TABLE IF NOT EXISTS ${table}\n`)).toBe(true);
      expect(s).toContain("ENGINE = ReplacingMergeTree(version)");
      expect(s).toContain("PARTITION BY toYYYYMM(ts)");
      expect(s).toMatch(/ORDER BY \(org_id, workspace_id, /);
      expect(s).toContain("DateTime64(6, 'UTC')");
      expect(s).toMatch(/retention_days\s+UInt16 DEFAULT 0/);
    }
  });
  test("no TTL is active: the only TTL text is a comment", () => {
    for (const s of statements)
      for (const line of s.split("\n")) if (/\bTTL\b/.test(line)) expect(line.trimStart().startsWith("--")).toBe(true);
  });
  test("the vendored transcript_events DDL is untouched by this skeleton", () => {
    expect(schemaStatements().join("\n")).not.toContain("cost_usd_micros");
  });
});

const suite = CH_URL ? describe : describe.skip;
suite("output facts DDL on ClickHouse", () => {
  let admin: ClickHouseClient, ch: ClickHouseClient;
  const database = `output_facts_test_${crypto.randomUUID().replaceAll("-", "").slice(0, 16)}`;
  beforeAll(async () => {
    admin = createClickHouseClient({ url: CH_URL!, user: CH_USER, password: CH_PASSWORD, database: "default" });
    await admin.command(`CREATE DATABASE ${database}`);
    ch = createClickHouseClient({ url: CH_URL!, user: CH_USER, password: CH_PASSWORD, database });
    for (const s of outputFactsSchemaStatements()) await ch.command(s);
    // Re-applying is a no-op.
    for (const s of outputFactsSchemaStatements()) await ch.command(s);
  });
  afterAll(async () => {
    await admin?.command(`DROP DATABASE IF EXISTS ${database}`);
  });

  test("microsecond time survives, newer version wins, NULL cost is skipped by sum()", async () => {
    const base = {
      org_id: "org-a", workspace_id: "ws-a", ts: "2026-11-05 23:59:59.999999", session_id: "s1", source: "codex",
      machine_id: "m1", kind: "session_event", event_id: "e1", project_id: null, task_ref_id: null, branch: null,
      model: "gpt-5", provider: null, input_tokens: 10, output_tokens: 5, reasoning_tokens: 0, cache_read_tokens: 0,
      cache_create_tokens: 0, cache_create_5m_tokens: null, cache_create_1h_tokens: null, cost_usd_micros: null,
      files_touched: ["a.ts"], fingerprint: "f1", version: 1,
    };
    await ch.insert(EVENT_FACTS_TABLE, [base]);
    await ch.insert(EVENT_FACTS_TABLE, [{ ...base, input_tokens: 11, fingerprint: "f2", version: 2 }]);
    await ch.insert(EVENT_FACTS_TABLE, [{ ...base, event_id: "e2", cost_usd_micros: 700, version: 1 }]);
    const [r] = await ch.select<{ n: string; input: string; cost: string; max_ts: string }>(
      `SELECT count() AS n, sum(input_tokens) AS input, coalesce(sum(cost_usd_micros), 0) AS cost,
              toString(max(ts)) AS max_ts
         FROM ${EVENT_FACTS_TABLE} FINAL
        WHERE org_id = 'org-a' AND workspace_id = 'ws-a'
          AND ts >= toDateTime64('2026-11-05 00:00:00', 6, 'UTC') AND ts < toDateTime64('2026-11-06 00:00:00', 6, 'UTC')`,
    );
    expect(Number(r!.n)).toBe(2);
    expect(Number(r!.input)).toBe(21);
    expect(Number(r!.cost)).toBe(700);
    expect(r!.max_ts).toBe("2026-11-05 23:59:59.999999");
  });

  test("message facts deduplicate by (source, session, message id) within a window", async () => {
    const fact = (row_key: string, fact_key: string, ts: string) => ({
      org_id: "org-a", workspace_id: "ws-a", fact_kind: "message", ts, source: "claude", session_id: "s1",
      fact_key, origin: "event", row_key, added: null, removed: null, fingerprint: row_key, version: 1,
    });
    await ch.insert(OUTPUT_FACTS_TABLE, [
      fact("e1", "msg-1", "2026-11-05 10:00:00"),
      fact("e2", "msg-1", "2026-11-05 10:00:01"),
      fact("e3", "msg-2", "2026-11-05 10:00:02"),
      fact("e4", "msg-3", "2026-11-07 10:00:00"),
    ]);
    const [r] = await ch.select<{ n: string }>(
      `SELECT uniqExact(source, session_id, fact_key) AS n FROM ${OUTPUT_FACTS_TABLE} FINAL
        WHERE org_id = 'org-a' AND workspace_id = 'ws-a' AND fact_kind = 'message'
          AND ts >= toDateTime64('2026-11-05 00:00:00', 6, 'UTC') AND ts <= toDateTime64('2026-11-06 00:00:00', 6, 'UTC')`,
    );
    expect(Number(r!.n)).toBe(2);
  });
});
