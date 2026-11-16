/** Summaries regeneration push A: flag parsing, the backoff ladder, the config hash and the audit log line. */
import { describe, expect, test } from "bun:test";
import { regenActive, regenSettings, REGEN_BOUNDS } from "../settings.ts";
import { clampAttempts, maxDueAttempts, nextRetryAt } from "../backoff.ts";
import { configHash, configHashMaterial, keyFingerprint } from "../config-hash.ts";
import { logRegenQueued, sessionHash, writeRegenAudit, AUDIT_APPEND_SQL, type RegenAuditEntry } from "../audit.ts";
import { bobCostasConfigHash, bobCostasRegenFromEnv, reasonFromWorkKey } from "../../bob-costas/regen.ts";
import { captureLogs } from "../../shared/__tests__/golden.ts";

const KEY = "sk-test-0123456789abcdef0123456789abcdef";

describe("regenSettings", () => {
  test("defaults: every flag off, scope defaults", () => {
    const s = regenSettings({});
    expect(s).toEqual({
      turns: { enabled: false, threshold: 10, idleSeconds: 600 },
      retry: { enabled: false, backoffSeconds: [300, 3600, 21600], configReset: false },
      audit: { enabled: false, retentionDays: 90 },
    });
    expect(regenActive(s)).toBe(false);
    expect(bobCostasRegenFromEnv({})).toBeNull();
  });

  test("a flag is on only for exactly \"true\"", () => {
    for (const v of ["TRUE", "True", "1", "yes", " true", "true ", ""])
      expect(regenSettings({ SUMMARY_REGEN_TURNS_ENABLED: v }).turns.enabled).toBe(false);
    expect(regenSettings({ SUMMARY_REGEN_TURNS_ENABLED: "true" }).turns.enabled).toBe(true);
    expect(regenSettings({ SUMMARY_RETRY_BACKOFF_ENABLED: "true" }).retry.enabled).toBe(true);
    expect(regenSettings({ SUMMARY_RETRY_CONFIG_RESET_ENABLED: "true" }).retry.configReset).toBe(true);
    expect(regenSettings({ SUMMARY_REGEN_AUDIT_ENABLED: "true" }).audit.enabled).toBe(true);
  });

  test("tuning values: in-bounds kept, malformed or out of bounds fall back", () => {
    expect(regenSettings({ SUMMARY_REGEN_TURN_THRESHOLD: "3" }).turns.threshold).toBe(3);
    for (const bad of ["0", "-1", "1.5", "10001", "abc", "1e3"])
      expect(regenSettings({ SUMMARY_REGEN_TURN_THRESHOLD: bad }).turns.threshold).toBe(10);
    expect(regenSettings({ SUMMARY_REGEN_IDLE_SECONDS: "59" }).turns.idleSeconds).toBe(600);
    expect(regenSettings({ SUMMARY_REGEN_IDLE_SECONDS: "60" }).turns.idleSeconds).toBe(60);
    expect(regenSettings({ SUMMARY_RETRY_BACKOFF_SECONDS: "10,20" }).retry.backoffSeconds).toEqual([10, 20]);
    for (const bad of ["", "10,,20", "10,0", "a", "1,2,3,4,5,6,7,8,9,10,11", "604801"])
      expect(regenSettings({ SUMMARY_RETRY_BACKOFF_SECONDS: bad }).retry.backoffSeconds).toEqual([300, 3600, 21600]);
    expect(regenSettings({ SUMMARY_REGEN_AUDIT_RETENTION_DAYS: "3651" }).audit.retentionDays).toBe(90);
    expect(REGEN_BOUNDS.retentionDays.max).toBe(3650);
  });

  test("audit alone is not a coverage flag: no config hash is computed", () => {
    const r = bobCostasRegenFromEnv({ SUMMARY_REGEN_AUDIT_ENABLED: "true", BOB_COSTAS_MODEL: "gpt-test-mini", OPENAI_API_KEY: KEY });
    expect(r?.configHash).toBe("");
    expect(regenActive(r?.settings)).toBe(false);
  });
});

describe("backoff ladder", () => {
  const now = new Date("2026-11-01T12:00:00.000Z");
  const ladder = [300, 3600, 21600];
  test("failure n waits backoff[n-1]; failure L+1 is terminal", () => {
    expect(nextRetryAt(1, ladder, now)?.toISOString()).toBe("2026-11-01T12:05:00.000Z");
    expect(nextRetryAt(2, ladder, now)?.toISOString()).toBe("2026-11-01T13:00:00.000Z");
    expect(nextRetryAt(3, ladder, now)?.toISOString()).toBe("2026-11-01T18:00:00.000Z");
    expect(nextRetryAt(4, ladder, now)).toBeNull();
    expect(nextRetryAt(0, ladder, now)).toBeNull();
    expect(nextRetryAt(1.5, ladder, now)).toBeNull();
    expect(maxDueAttempts(ladder)).toBe(3);
  });
  test("the stored counter is clamped to smallint", () => {
    expect(clampAttempts(40_000)).toBe(32_767);
    expect(clampAttempts(-3)).toBe(0);
  });
});

describe("config hash", () => {
  const base = { model: "gpt-test-mini", baseUrl: "https://api.example.test/v1/responses", reasoningEffort: "low", promptVersion: "p1", key: KEY };
  test("the raw key never enters the hashed material", () => {
    expect(configHashMaterial(base)).not.toContain(KEY);
    expect(configHashMaterial(base)).toContain(keyFingerprint(KEY));
    expect(keyFingerprint(KEY)).toMatch(/^[0-9a-f]{16}$/);
    expect(configHash(base)).toMatch(/^[0-9a-f]{64}$/);
  });
  test("each input changes the hash; no key is \"nokey\"", () => {
    const h = configHash(base);
    for (const over of [{ model: "x" }, { baseUrl: "https://other.test/r" }, { reasoningEffort: undefined }, { promptVersion: "p2" }, { key: `${KEY}x` }])
      expect(configHash({ ...base, ...over })).not.toBe(h);
    expect(keyFingerprint(null)).toBe("nokey");
    expect(keyFingerprint("")).toBe("nokey");
  });
  test("Bob Costas: a -disabled key hashes as no key; the extractive summariser ignores the key", () => {
    const env = { BOB_COSTAS_MODEL: "gpt-test-mini" };
    expect(bobCostasConfigHash({ ...env, OPENAI_API_KEY: "sk-x-disabled" })).toBe(bobCostasConfigHash(env));
    expect(bobCostasConfigHash({ ...env, OPENAI_API_KEY: KEY })).not.toBe(bobCostasConfigHash(env));
    expect(bobCostasConfigHash({ OPENAI_API_KEY: KEY })).toBe(bobCostasConfigHash({}));
    expect(bobCostasConfigHash({ ...env, OPENAI_API_KEY: KEY })).not.toContain(KEY);
  });
});

describe("audit writer", () => {
  const entry: RegenAuditEntry = {
    orgId: "org-a", workspaceId: "ws-a", harness: "claude", sessionId: "secret-session-id",
    system: "bob_costas", kind: "short", reason: "retry_backoff",
    priorStatus: "ready", priorFailureCode: "model_failed", priorPipelineVersion: "bc-1",
  };
  test("the log line carries the session hash, never the session id or any text", async () => {
    const { lines } = await captureLogs(async () => logRegenQueued(entry, "summary-short"));
    expect(lines).toHaveLength(1);
    const l = lines[0]!;
    expect(l.message).toBe("summary regeneration queued");
    expect(l.session_hash).toBe(sessionHash(entry));
    expect(l.reason).toBe("retry_backoff");
    expect(l.prior_failure_code).toBe("model_failed");
    expect(JSON.stringify(l)).not.toContain("secret-session-id");
  });
  test("a prior failure code outside the code shape is dropped, not stored", async () => {
    const calls: { sql: string; params: unknown[] }[] = [];
    const db = { query: async (sql: string, params?: unknown[]) => { calls.push({ sql, params: params ?? [] }); return { rows: [{ n: 2 }] }; } };
    expect(await writeRegenAudit(db as never, [entry, { ...entry, priorFailureCode: "Free Text: boom", priorStatus: "READY" }])).toBe(2);
    expect(calls).toHaveLength(1);
    expect(calls[0]!.sql).toBe(AUDIT_APPEND_SQL);
    const rows = JSON.parse(calls[0]!.params[0] as string) as Record<string, unknown>[];
    expect(rows.map((r) => r.prior_failure_code)).toEqual(["model_failed", null]);
    expect(rows.map((r) => r.prior_status)).toEqual(["ready", null]);
    expect(Object.keys(rows[0]!).sort()).toEqual(["harness", "kind", "org_id", "prior_failure_code", "prior_pipeline_version", "prior_status",
      "reason", "request_id", "requested_by", "scope_hash", "session_id", "system", "workspace_id"]);
    expect(await writeRegenAudit(db as never, [])).toBe(0);
    expect(calls).toHaveLength(1);
  });
});

test("reasonFromWorkKey reads only the regeneration suffixes", () => {
  expect(reasonFromWorkKey("summary-short:a:b:3:regen-retry")).toBe("retry_backoff");
  expect(reasonFromWorkKey("summary-short:a:b:3:regen-config")).toBe("config_changed");
  expect(reasonFromWorkKey("summary-short:a:b:3:regen-turns")).toBe("turns");
  // A coverage stamp is not a regeneration: no reason, so the handler only stamps.
  expect(reasonFromWorkKey("summary-short:a:b:3:regen-stamp")).toBeUndefined();
  expect(reasonFromWorkKey("summary-short:a:b:3")).toBeUndefined();
  expect(reasonFromWorkKey(undefined)).toBeUndefined();
});
