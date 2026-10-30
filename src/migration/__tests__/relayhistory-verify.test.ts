import { describe, expect, test } from "bun:test";
import {
  isPerDatabaseState,
  addFingerprint,
  chunkFingerprintSql,
  chunkUpperBoundSql,
  fingerprintSql,
  quoteIdent,
  selectUsableKey,
} from "../relayhistory-verify.ts";

describe("relayhistory migration verifier", () => {
  test("quotes catalog identifiers and hashes only source columns", () => {
    expect(quoteIdent('odd"name')).toBe('"odd""name"');
    const sql = fingerprintSql("sessions", "machines", ["org_id", "workspace_id"]);
    expect(sql).toContain('SELECT "org_id", "workspace_id" FROM "sessions"."machines"');
    expect(sql).toContain("hashtextextended");
  });

  test("builds deterministic composite-key ranges without an unbounded table scan", () => {
    const upper = chunkUpperBoundSql("sessions", "events", ["org_id", "event_id"], true);
    expect(upper).toContain('("org_id", "event_id") > ($1, $2)');
    expect(upper).toContain('ORDER BY "org_id", "event_id" LIMIT $3');
    expect(upper).toContain('ORDER BY "org_id" DESC, "event_id" DESC LIMIT 1');

    const fingerprint = chunkFingerprintSql(
      "sessions",
      "events",
      ["org_id", "event_id", "content"],
      ["org_id", "event_id"],
      true,
    );
    expect(fingerprint).toContain('("org_id", "event_id") > ($1, $2)');
    expect(fingerprint).toContain('("org_id", "event_id") <= ($3, $4)');
    expect(fingerprint).not.toContain("FROM (SELECT *");
  });

  test("fails closed when source and target lack the same primary key", () => {
    expect(selectUsableKey([], [])).toBeNull();
    expect(selectUsableKey(["id"], [])).toBeNull();
    expect(selectUsableKey(["org_id", "id"], ["id", "org_id"])).toBeNull();
    expect(selectUsableKey(["org_id", "id"], ["org_id", "id"])).toEqual(["org_id", "id"]);
  });

  test("combines chunk fingerprints without losing 64-bit precision", () => {
    expect(addFingerprint(
      { count: "9007199254740992", hash1: "9223372036854775807", hash2: "-5" },
      { count: "2", hash1: "10", hash2: "12" },
    )).toEqual({ count: "9007199254740994", hash1: "9223372036854775817", hash2: "7" });
  });
});

describe("per-database state", () => {
  test("skips the runner ledger and rollout progress, nothing else", () => {
    expect(isPerDatabaseState("__migrations")).toBe(true);
    expect(isPerDatabaseState("session_rollup_rollout")).toBe(true);
    expect(isPerDatabaseState("delivery_rollout")).toBe(true);
    expect(isPerDatabaseState("session_rollups")).toBe(false);
    expect(isPerDatabaseState("auth_sessions")).toBe(false);
  });
});
