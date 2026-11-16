/**
 * Summaries regeneration push A, brief side, over PGlite with every migration: the turns gate
 * (1), the retry ladder (2) and config reset (2b) through sweepUnbriefed, the audit rows and log
 * lines (4), feature detection with the columns missing, and the race fences with coverage on.
 * Flags off is covered byte for byte by regen-golden.test.ts.
 */
import { afterEach, beforeEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import type { ManifestV1 } from "../../shared/vendor.ts";
import type { HoopDb } from "../../shared/db.ts";
import { captureLogs } from "../../shared/__tests__/golden.ts";
import { resetDetection } from "../../summary-regen/detect.ts";
import { configHashMaterial } from "../../summary-regen/config-hash.ts";
import { writeRegenAudit } from "../../summary-regen/audit.ts";
import { regenSettings } from "../../summary-regen/settings.ts";
import { createAnalysisTransport, SessionAnalysisProviderError } from "../engine/transport.ts";
import { briefConfigHash, briefRegenFromEnv, type BriefRegen } from "../regen.ts";
import { armWakeup, recordBrief } from "../store.ts";
import { SESSION_BRIEF_ENGINE_VERSION, SESSION_BRIEF_PROMPT_VERSION } from "../versions.ts";
import { briefTick } from "../worker.ts";
import { FakeModel, KEY, database, dueNow, hoopDb, mapReader, pointer, tempStore, testConfig, transcript } from "./harness.ts";

setDefaultTimeout(120_000);

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});
beforeEach(() => resetDetection());

/** A conversation with `turns` user turns, each answered. */
const conversation = (turns: number) =>
  Array.from({ length: turns }, (_, i) => [
    { type: "user", timestamp: `2026-10-29T10:${String(i).padStart(2, "0")}:01.000Z`, message: { role: "user", content: `step ${i}: fix the build` } },
    { type: "assistant", timestamp: `2026-10-29T10:${String(i).padStart(2, "0")}:02.000Z`, message: { role: "assistant", model: "claude-x", content: [{ type: "text", text: `did step ${i}` }] } },
  ]).flat();

const regen = (env: Record<string, string>, configHash = "hash-a"): BriefRegen => ({ settings: regenSettings(env), configHash });
const TURNS = { SUMMARY_REGEN_TURNS_ENABLED: "true", SUMMARY_REGEN_TURN_THRESHOLD: "3", SUMMARY_REGEN_IDLE_SECONDS: "600" };
const RETRY = { SUMMARY_RETRY_BACKOFF_ENABLED: "true", SUMMARY_REGEN_AUDIT_ENABLED: "true" };
const authFailed = () => new SessionAnalysisProviderError("model_auth_failed", false);

async function setup() {
  const pg = await database();
  open.push(pg);
  const store = tempStore();
  const manifests = new Map<string, ManifestV1>();
  const model = new FakeModel();
  let revision = 0;
  const upload = async (turns: number) => {
    revision += 1;
    const manifestKey = `manifest/${KEY.sessionId}/m-1/r${revision}`;
    manifests.set(manifestKey, await transcript(store, conversation(turns), { machine: "m-1", revision }));
    await pointer(pg, { machine: "m-1", revision, manifestKey });
    await armWakeup(hoopDb(pg), KEY, { idleSeconds: 600, maxWaitSeconds: 3600 });
  };
  const tick = (r: BriefRegen | null, opts: { sweep?: boolean; now?: Date; via?: (db: HoopDb) => HoopDb } = {}) =>
    briefTick({
      reader: mapReader(manifests),
      store,
      transport: model,
      config: testConfig({ throttleSeconds: 0, sessionDailyCap: 1000, sweepSeconds: opts.sweep ? 1 : 0 }),
      regen: r,
    })((opts.via ?? ((db) => db))(hoopDb(pg)), opts.now ?? new Date());
  const row = async () =>
    (await pg.query<Record<string, unknown>>(
      `SELECT status, error, covered_turns, retry_attempts, config_hash, pipeline_version, next_retry_at,
              extract(epoch FROM next_retry_at - updated_at)::int AS backoff
         FROM sessions.session_briefs`,
    )).rows[0]!;
  const wakeups = async () => (await pg.query<Record<string, unknown>>("SELECT * FROM sessions.session_brief_wakeups")).rows;
  const audit = async () =>
    (await pg.query<Record<string, unknown>>(
      "SELECT system, kind, reason, prior_status, prior_failure_code, prior_pipeline_version, session_id FROM sessions.summary_regeneration_audit ORDER BY created_at, reason",
    )).rows;
  /** Make the stored retry due (as if its backoff elapsed). */
  const elapse = () => pg.exec("UPDATE sessions.session_briefs SET next_retry_at = now() - interval '1 second' WHERE next_retry_at IS NOT NULL");
  return { pg, model, upload, tick, row, wakeups, audit, elapse };
}

const byMessage = (lines: Record<string, unknown>[], message: string) => lines.filter((l) => l.message === message);

describe("(1) turns gate", () => {
  test("defers below N while active, then regenerates on turns_gained, idle_uncovered and ended_uncovered", async () => {
    const s = await setup();
    const r = regen(TURNS);
    await s.upload(2);
    await dueNow(s.pg);
    expect(await s.tick(r)).toEqual({ generated: 1 });
    expect(await s.row()).toMatchObject({
      status: "ready", covered_turns: 2, retry_attempts: 0, config_hash: "hash-a",
      pipeline_version: SESSION_BRIEF_ENGINE_VERSION, next_retry_at: null,
    });

    // Two more turns (< 3) while the session is active: deferred, no model call, no admission.
    await s.upload(4);
    await dueNow(s.pg);
    const deferred = await captureLogs(() => s.tick(r));
    expect(deferred.result).toEqual({ deferred: 1 });
    expect(s.model.requests).toHaveLength(1);
    expect(byMessage(deferred.lines, "session brief deferred")).toEqual([
      expect.objectContaining({ count: 2 }),
    ]);
    const [w] = await s.wakeups();
    expect(w).toMatchObject({ attempts: 0, lease_id: null });
    const left = (await s.pg.query<{ s: number }>("SELECT extract(epoch FROM due_at - now())::int AS s FROM sessions.session_brief_wakeups")).rows[0]!.s;
    expect(left).toBeGreaterThan(590);
    expect(left).toBeLessThanOrEqual(600);
    expect((await s.row()).covered_turns).toBe(2);
    expect((await s.pg.query("SELECT 1 FROM sessions.session_brief_attempts")).rows.length).toBe(1);

    // A third new turn reaches N.
    await s.upload(5);
    await dueNow(s.pg);
    const gained = await captureLogs(() => s.tick(r));
    expect(gained.result).toEqual({ generated: 1 });
    expect(byMessage(gained.lines, "session brief generated")[0]).toMatchObject({ regeneration_reason: "turns_gained" });
    expect((await s.row()).covered_turns).toBe(5);

    // One new turn, but the session has been quiet for longer than the idle window.
    await s.upload(6);
    await s.pg.exec("UPDATE sessions.session_index SET last_activity_at = now() - interval '11 minutes'");
    await dueNow(s.pg);
    const idle = await captureLogs(() => s.tick(r));
    expect(idle.result).toEqual({ generated: 1 });
    expect(byMessage(idle.lines, "session brief generated")[0]).toMatchObject({ regeneration_reason: "idle_uncovered" });

    // One new turn on an ended session.
    await s.upload(7);
    await s.pg.exec("UPDATE sessions.session_index SET state = 'done'");
    await dueNow(s.pg);
    const ended = await captureLogs(() => s.tick(r));
    expect(ended.result).toEqual({ generated: 1 });
    expect(byMessage(ended.lines, "session brief generated")[0]).toMatchObject({ regeneration_reason: "ended_uncovered" });
    expect((await s.row()).covered_turns).toBe(7);
  });

  test("flags off: one new turn regenerates as today and no coverage column is written", async () => {
    const s = await setup();
    await s.upload(2);
    await dueNow(s.pg);
    expect(await s.tick(null)).toEqual({ generated: 1 });
    await s.upload(3);
    await dueNow(s.pg);
    const out = await captureLogs(() => s.tick(null));
    expect(out.result).toEqual({ generated: 1 });
    expect(byMessage(out.lines, "session brief generated")[0]).not.toHaveProperty("regeneration_reason");
    expect(await s.row()).toMatchObject({ covered_turns: null, config_hash: null, pipeline_version: null, retry_attempts: 0 });
  });

  test("a legacy ready row (covered_turns NULL) is regenerated as today, then stamped", async () => {
    const s = await setup();
    await s.upload(2);
    await dueNow(s.pg);
    expect(await s.tick(null)).toEqual({ generated: 1 });
    await s.upload(3);
    await dueNow(s.pg);
    expect(await s.tick(regen(TURNS))).toEqual({ generated: 1 });
    expect((await s.row()).covered_turns).toBe(3);
  });
});

describe("(2) retry ladder", () => {
  test("model_auth_failed: +300, +3600, +21600 by the database clock, then terminal; each enqueue is audited and logged hashed", async () => {
    const s = await setup();
    const r = regen(RETRY);
    await s.upload(2);
    s.model.next.push(authFailed());
    await dueNow(s.pg);
    expect(await s.tick(r)).toEqual({ failed: 1 });
    expect(await s.row()).toMatchObject({ status: "failed", error: "model_auth_failed", retry_attempts: 1, backoff: 300, config_hash: "hash-a" });

    // Not due yet: the sweep arms nothing.
    expect(await s.tick(r, { sweep: true })).toEqual({});
    expect(await s.wakeups()).toEqual([]);

    for (const [attempts, backoff] of [[2, 3600], [3, 21600], [4, null]] as const) {
      await s.elapse();
      s.model.next.push(authFailed());
      const out = await captureLogs(() => s.tick(r, { sweep: true }));
      expect(out.result).toEqual({ failed: 1 });
      const row = await s.row();
      expect(row).toMatchObject({ status: "failed", retry_attempts: attempts, backoff });
      if (backoff === null) expect(row.next_retry_at).toBeNull();
      const queued = byMessage(out.lines, "summary regeneration queued");
      expect(queued).toHaveLength(1);
      expect(queued[0]).toMatchObject({ system: "brief", kind: "brief", reason: "retry_backoff", prior_status: "failed", prior_failure_code: "model_auth_failed" });
      expect(queued[0]!.session_hash).toMatch(/^[0-9a-f]{64}$/);
      // The regeneration line carries the hash only (the existing failure line keeps its session_id).
      expect(queued[0]).not.toHaveProperty("session_id");
      expect(JSON.stringify(queued)).not.toContain(`"${KEY.sessionId}"`);
    }
    const rows = await s.audit();
    expect(rows).toHaveLength(3);
    for (const a of rows) {
      expect(a).toMatchObject({ system: "brief", kind: "brief", reason: "retry_backoff", prior_status: "failed", prior_failure_code: "model_auth_failed", session_id: KEY.sessionId });
    }

    // Terminal: the sweep arms nothing, even after a one-year clock jump.
    expect(await s.tick(r, { sweep: true })).toEqual({});
    await s.pg.exec(`UPDATE sessions.session_briefs SET attempted_at = attempted_at - interval '1 year',
                       updated_at = updated_at - interval '1 year', generated_at = generated_at - interval '1 year'`);
    const jump = new Date(Date.now() + 366 * 86_400_000);
    expect(await s.tick(r, { sweep: true, now: jump })).toEqual({});
    expect(await s.wakeups()).toEqual([]);
    expect(s.model.requests).toHaveLength(4);
    expect(await s.audit()).toHaveLength(3);
  });

  test("the attempt counter saturates and stays terminal", async () => {
    const s = await setup();
    await s.upload(2);
    s.model.next.push(authFailed());
    await dueNow(s.pg);
    expect(await s.tick(regen(RETRY))).toEqual({ failed: 1 });
    await s.pg.exec("UPDATE sessions.session_briefs SET retry_attempts = 32767");
    await s.upload(3);
    s.model.next.push(authFailed());
    await dueNow(s.pg);
    expect(await s.tick(regen(RETRY))).toEqual({ failed: 1 });
    expect(await s.row()).toMatchObject({ retry_attempts: 32767, next_retry_at: null });
  });

  test("a successful retry clears the ladder", async () => {
    const s = await setup();
    const r = regen(RETRY);
    await s.upload(2);
    s.model.next.push(authFailed());
    await dueNow(s.pg);
    expect(await s.tick(r)).toEqual({ failed: 1 });
    await s.elapse();
    expect(await s.tick(r, { sweep: true })).toEqual({ generated: 1 });
    expect(await s.row()).toMatchObject({ status: "ready", retry_attempts: 0, next_retry_at: null, error: null });
  });

  test("flag off: a final failure writes no ladder and the sweep never re-arms it", async () => {
    const s = await setup();
    await s.upload(2);
    s.model.next.push(authFailed());
    await dueNow(s.pg);
    expect(await s.tick(null)).toEqual({ failed: 1 });
    expect(await s.row()).toMatchObject({ retry_attempts: 0, next_retry_at: null, config_hash: null });
    await s.pg.exec("UPDATE sessions.session_briefs SET retry_attempts = 1, next_retry_at = now() - interval '1 hour'");
    expect(await s.tick(null, { sweep: true })).toEqual({});
    expect(await s.wakeups()).toEqual([]);
  });
});

describe("(2b) config reset", () => {
  test("a terminal row is revived once by a config change, attempts restart at 1; canonical_input_refused never", async () => {
    const s = await setup();
    const env = { ...RETRY, SUMMARY_RETRY_CONFIG_RESET_ENABLED: "true" };
    await s.upload(2);
    s.model.next.push(authFailed());
    await dueNow(s.pg);
    expect(await s.tick(regen(env, "hash-a"))).toEqual({ failed: 1 });
    await s.pg.exec("UPDATE sessions.session_briefs SET retry_attempts = 4, next_retry_at = NULL");

    // Same config: terminal stays terminal.
    expect(await s.tick(regen(env, "hash-a"), { sweep: true })).toEqual({});

    // New config: due once, reason config_changed, attempts back to 1 on the next failure.
    s.model.next.push(authFailed());
    const out = await captureLogs(() => s.tick(regen(env, "hash-b"), { sweep: true }));
    expect(out.result).toEqual({ failed: 1 });
    expect(byMessage(out.lines, "summary regeneration queued")[0]).toMatchObject({ reason: "config_changed" });
    expect(await s.row()).toMatchObject({ retry_attempts: 1, backoff: 300, config_hash: "hash-b" });
    expect((await s.audit()).map((a) => a.reason)).toEqual(["config_changed"]);

    // The new hash is now stored: not due again until the backoff elapses.
    expect(await s.tick(regen(env, "hash-b"), { sweep: true })).toEqual({});

    await s.pg.exec("UPDATE sessions.session_briefs SET error = 'canonical_input_refused', retry_attempts = 4, next_retry_at = NULL");
    expect(await s.tick(regen(env, "hash-c"), { sweep: true })).toEqual({});
    expect(await s.wakeups()).toEqual([]);
  });

  test("configReset without the retry flag stamps the hash on failure and revives on change", async () => {
    const s = await setup();
    const env = { SUMMARY_RETRY_CONFIG_RESET_ENABLED: "true" };
    await s.upload(2);
    s.model.next.push(authFailed());
    await dueNow(s.pg);
    expect(await s.tick(regen(env, "hash-a"))).toEqual({ failed: 1 });
    expect(await s.row()).toMatchObject({ config_hash: "hash-a", retry_attempts: 0, next_retry_at: null });
    expect(await s.tick(regen(env, "hash-a"), { sweep: true })).toEqual({});
    expect(await s.tick(regen(env, "hash-b"), { sweep: true })).toEqual({ generated: 1 });
  });
});

describe("feature detection", () => {
  test("flags on with the columns missing: one warning, today's behaviour, no crash", async () => {
    const s = await setup();
    await s.pg.exec("ALTER TABLE sessions.session_briefs DROP COLUMN covered_turns");
    const r = regen({ ...TURNS, ...RETRY, SUMMARY_RETRY_CONFIG_RESET_ENABLED: "true" });
    await s.upload(2);
    s.model.next.push(authFailed());
    await dueNow(s.pg);
    const first = await captureLogs(() => s.tick(r, { sweep: true }));
    expect(first.result).toEqual({ failed: 1 });
    await s.upload(3);
    await dueNow(s.pg);
    const second = await captureLogs(() => s.tick(r, { sweep: true }));
    expect(second.result).toEqual({ generated: 1 });
    const warnings = [...first.lines, ...second.lines].filter((l) => l.message === "summary regeneration schema missing; flags behave as off");
    expect(warnings).toHaveLength(1);
    expect((await s.pg.query<Record<string, unknown>>("SELECT retry_attempts, next_retry_at, config_hash FROM sessions.session_briefs")).rows[0])
      .toEqual({ retry_attempts: 0, next_retry_at: null, config_hash: null });
  });
});

describe("(4) audit retention", () => {
  test("the prune tick purges rows past the retention, at most hourly", async () => {
    const s = await setup();
    const entry = { orgId: KEY.orgId, workspaceId: KEY.workspaceId, harness: KEY.harness, system: "brief" as const, kind: "brief" as const, reason: "retry_backoff" as const };
    // The audit only records sessions with a live pointer (append_v1 skips absent or tombstoned ones).
    for (const sessionId of ["old-one", "new-one"]) await pointer(s.pg, { key: { ...KEY, sessionId } });
    await writeRegenAudit(hoopDb(s.pg), [{ ...entry, sessionId: "old-one" }, { ...entry, sessionId: "new-one" }]);
    await s.pg.transaction(async (tx) => {
      await tx.query("ALTER TABLE sessions.summary_regeneration_audit DISABLE TRIGGER summary_regeneration_audit_append_only");
      await tx.query("UPDATE sessions.summary_regeneration_audit SET created_at = now() - interval '91 days' WHERE session_id = 'old-one'");
      await tx.query("ALTER TABLE sessions.summary_regeneration_audit ENABLE TRIGGER summary_regeneration_audit_append_only");
    });
    // Audit off: nothing purged.
    expect(await s.tick(regen({}))).toBeDefined();
    expect((await s.audit()).length).toBe(2);
    const out = await captureLogs(() => s.tick(regen({ SUMMARY_REGEN_AUDIT_ENABLED: "true" })));
    expect(byMessage(out.lines, "summary regeneration audit purged")).toEqual([expect.objectContaining({ count: 1 })]);
    expect((await s.audit()).map((a) => a.session_id)).toEqual(["new-one"]);
  });

  test("a failing purge is logged and the tick still generates", async () => {
    const s = await setup();
    await s.upload(2);
    await dueNow(s.pg);
    const failingPurge = (db: HoopDb): HoopDb => {
      const query = ((text: string, params?: unknown[]) =>
        text.includes("summary_regeneration_audit_purge_v1") ? Promise.reject(new Error("purge boom")) : db.query(text as never, params as never)) as HoopDb["query"];
      return { ...db, query, pool: { connect: async () => ({ query, release() {} }) } } as unknown as HoopDb;
    };
    const out = await captureLogs(() => s.tick(regen({ SUMMARY_REGEN_AUDIT_ENABLED: "true" }), { via: failingPurge }));
    expect(out.result).toEqual({ generated: 1 });
    expect(byMessage(out.lines, "summary regeneration audit purge failed"))
      .toEqual([expect.objectContaining({ level: "warn", stage: "session-brief", error_code: "Error" })]);
    expect(await s.row()).toMatchObject({ status: "ready" });
  });
});

/** The pointer vanishes between the sweep and the claim (a race): the claimed attempt is "gone" and writes nothing. */
const goneAtClaim = (db: HoopDb): HoopDb => {
  const query = ((text: string, params?: unknown[]) =>
    text.includes("SELECT machine_id, manifest_key, revision::text AS revision FROM sessions.session_index")
      ? Promise.resolve({ rows: [] })
      : db.query(text as never, params as never)) as HoopDb["query"];
  return { ...db, query, pool: { connect: async () => ({ query, release() {} }) } } as unknown as HoopDb;
};

describe("(P3) armed rows that write nothing", () => {
  test("a gone config_changed row is re-armed and re-audited at most once per hold, then due again", async () => {
    const s = await setup();
    const env = { ...RETRY, SUMMARY_RETRY_CONFIG_RESET_ENABLED: "true" };
    await s.upload(2);
    s.model.next.push(authFailed());
    await dueNow(s.pg);
    expect(await s.tick(regen(env, "hash-a"))).toEqual({ failed: 1 });
    await s.pg.exec("UPDATE sessions.session_briefs SET retry_attempts = 4, next_retry_at = NULL");

    const first = await captureLogs(() => s.tick(regen(env, "hash-b"), { sweep: true, via: goneAtClaim }));
    expect(first.result).toEqual({ gone: 1 });
    expect(byMessage(first.lines, "summary regeneration queued")).toEqual([expect.objectContaining({ reason: "config_changed" })]);
    const held = await s.pg.query<{ hold: number }>("SELECT extract(epoch FROM next_retry_at - now())::int AS hold FROM sessions.session_briefs");
    expect(held.rows[0]!.hold).toBeGreaterThan(21_000);
    expect(held.rows[0]!.hold).toBeLessThanOrEqual(21_600);
    expect(await s.row()).toMatchObject({ status: "failed", retry_attempts: 4, config_hash: "hash-a" });

    // Held: later sweeps arm nothing and audit nothing.
    for (let i = 0; i < 3; i++) {
      const again = await captureLogs(() => s.tick(regen(env, "hash-b"), { sweep: true }));
      expect(again.result).toEqual({});
      expect(byMessage(again.lines, "summary regeneration queued")).toEqual([]);
    }
    expect(await s.wakeups()).toEqual([]);
    expect((await s.audit()).map((a) => a.reason)).toEqual(["config_changed"]);
    expect(s.model.requests).toHaveLength(1);

    // After the hold: due once more.
    await s.elapse();
    const later = await captureLogs(() => s.tick(regen(env, "hash-b"), { sweep: true, via: goneAtClaim }));
    expect(later.result).toEqual({ gone: 1 });
    expect((await s.audit()).map((a) => a.reason)).toEqual(["config_changed", "config_changed"]);
  });

  test("a gone retry_backoff row is held the same way; a real attempt replaces the hold with its ladder step", async () => {
    const s = await setup();
    const r = regen(RETRY);
    await s.upload(2);
    s.model.next.push(authFailed());
    await dueNow(s.pg);
    expect(await s.tick(r)).toEqual({ failed: 1 });
    await s.elapse();
    expect(await s.tick(r, { sweep: true, via: goneAtClaim })).toEqual({ gone: 1 });
    expect(await s.tick(r, { sweep: true })).toEqual({});
    expect((await s.audit()).map((a) => a.reason)).toEqual(["retry_backoff"]);
    expect(await s.row()).toMatchObject({ retry_attempts: 1 });

    // After the hold the retry runs and the ladder moves on (+3600).
    await s.elapse();
    s.model.next.push(authFailed());
    expect(await s.tick(r, { sweep: true })).toEqual({ failed: 1 });
    expect(await s.row()).toMatchObject({ retry_attempts: 2, backoff: 3600 });
  });
});

describe("race fences with coverage", () => {
  test("an attempt over older revisions never overwrites the row or its coverage", async () => {
    const s = await setup();
    await s.upload(2);
    await s.upload(3);
    await dueNow(s.pg);
    expect(await s.tick(regen(TURNS))).toEqual({ generated: 1 });
    const before = await s.row();
    expect(before).toMatchObject({ covered_turns: 3 });
    const db = hoopDb(s.pg);
    const ready = await recordBrief(db, KEY, { "m-1": 1 }, {
      status: "ready",
      text: { title: "older", description: "older", state: "completed", fingerprint: "f", promptVersion: SESSION_BRIEF_PROMPT_VERSION,
        engineVersion: SESSION_BRIEF_ENGINE_VERSION, model: "m", messageCount: 1, shortenedMessages: 0, inputTokens: 1, outputTokens: 1,
        durationMs: 1, generatedAt: new Date() },
    } as never, { coverage: { configHash: "other", coveredTurns: 99, pipelineVersion: "other" } });
    expect(ready.written).toBe(false);
    const failed = await recordBrief(db, KEY, { "m-1": 1 }, { status: "failed", error: "model_auth_failed" },
      { coverage: { configHash: "other", retry: { backoffSeconds: [300], configReset: true } } });
    expect(failed.written).toBe(false);
    expect(await s.row()).toEqual(before);
  });
});

describe("transport and config", () => {
  test("a 401 from the model is model_auth_failed and not retryable at the transport", async () => {
    const transport = createAnalysisTransport({ apiKey: () => "sk-test-only", fetch: (async () => new Response("{}", { status: 401 })) as unknown as typeof fetch });
    const error = await transport.complete({ model: "m", instructions: "i", input: "x", schemaName: "s", schema: {}, timeoutMs: 1_000 } as never)
      .then(() => null, (e: unknown) => e);
    expect(error).toBeInstanceOf(SessionAnalysisProviderError);
    expect(error).toMatchObject({ code: "model_auth_failed", retryable: false });
  });

  test("briefRegenFromEnv: only the exact string true turns a flag on; the raw key never enters the hash", () => {
    for (const v of ["TRUE", "1", " true", "yes", ""]) {
      expect(briefRegenFromEnv("m", "k", { SUMMARY_REGEN_TURNS_ENABLED: v, SUMMARY_RETRY_BACKOFF_ENABLED: v, SUMMARY_RETRY_CONFIG_RESET_ENABLED: v, SUMMARY_REGEN_AUDIT_ENABLED: v })).toBeNull();
    }
    expect(briefRegenFromEnv("m", "k", {})).toBeNull();
    const audit = briefRegenFromEnv("m", "k", { SUMMARY_REGEN_AUDIT_ENABLED: "true" })!;
    expect(audit.configHash).toBe("");
    const on = briefRegenFromEnv("m", "sk-raw-secret-value", { SUMMARY_RETRY_BACKOFF_ENABLED: "true" })!;
    expect(on.configHash).toMatch(/^[0-9a-f]{64}$/);
    expect(on.configHash).toBe(briefConfigHash("m", "sk-raw-secret-value", {}));
    expect(briefConfigHash("m", "sk-other", {})).not.toBe(on.configHash);
    expect(briefConfigHash("m2", "sk-raw-secret-value", {})).not.toBe(on.configHash);
    const material = configHashMaterial({ model: "m", baseUrl: "u", reasoningEffort: "none", promptVersion: "p", key: "sk-raw-secret-value" });
    expect(material).not.toContain("sk-raw-secret-value");
  });
});
