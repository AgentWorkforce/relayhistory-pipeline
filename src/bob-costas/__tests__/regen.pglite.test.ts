/**
 * Summaries regeneration push A, Bob Costas with the flags on: turn coverage (1), the retry ladder (2),
 * config reset (2b), the audit rows and log lines (4), the race fences, and a missing schema behaving
 * as flags off.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { zstdCompressSync } from "node:zlib";
import type { PGlite } from "@electric-sql/pglite";
import { rawChunkKey } from "../../shared/vendor.ts";
import { masterKeyring, parseMasterKeys } from "../../shared/keyring.ts";
import { bobCostasHandler } from "../handler.ts";
import { extractiveSummarizer, type Summarizer } from "../summarizer.ts";
import { createModelSummarizer, modelSummarizerConfig } from "../model-summarizer.ts";
import { summarySweep } from "../sweep.ts";
import { BOB_COSTAS_PIPELINE_VERSION } from "../versions.ts";
import type { BobCostasRegen } from "../regen.ts";
import { SessionAnalysisProviderError } from "../../session-brief/engine/transport.ts";
import { database, hoopDb, pointer, tempStore } from "../../session-brief/__tests__/harness.ts";
import { captureLogs } from "../../shared/__tests__/golden.ts";
import { sessionHash } from "../../summary-regen/audit.ts";
import { regenSettings } from "../../summary-regen/settings.ts";
import { resetDetection } from "../../summary-regen/detect.ts";

setDefaultTimeout(120_000);
const open: PGlite[] = [];
afterEach(async () => {
  resetDetection();
  while (open.length) await open.pop()!.close();
});

const TRACE = "01a188e1-ef86-7b16-938a-e91b6b78643b";
const T0 = new Date("2026-11-01T12:00:00.000Z");
const at = (seconds: number) => new Date(T0.getTime() + seconds * 1000);
const HASH_A = "a".repeat(64);
const HASH_B = "b".repeat(64);
const regenOf = (env: Record<string, string>, configHash = HASH_A): BobCostasRegen => ({ settings: regenSettings(env), configHash });
const keyOf = (session: string) => `v1/ws=w/m=m/h=claude/d=2026/10/29/s=${session}/manifest.json`;
const loc = (session: string) => ({ workspaceId: "w", machineId: "m", harness: "claude", sessionId: session, startDate: "2026-10-29" });
const line = (text: string) => JSON.stringify({ type: "user", message: { role: "user", content: text } });

const failing = () => createModelSummarizer({
  transport: { complete: async () => { throw new SessionAnalysisProviderError("model_unavailable", true); } },
  config: { ...modelSummarizerConfig("gpt-test-mini", {}), maxAttempts: 1 },
  sleep: async () => {},
});

async function world() {
  const pg = await database();
  open.push(pg);
  const db = hoopDb(pg);
  const store = tempStore();
  const keyring = await masterKeyring(parseMasterKeys(`k1:${"a".repeat(64)}`));
  const manifests = new Map<string, unknown>();
  const reader = { provider: "fs", read: async (k: string) => (manifests.has(k) ? { manifest: manifests.get(k) } : null) } as never;
  const directory = { write: async () => ({ written: true }), clear: async () => true };

  /** A session with `turns` user turns at `revision`, its manifest and its session_index pointer. */
  async function session(id: string, turns: number, revision = 2, state = "active", lastActivity = T0) {
    await store.put(rawChunkKey(loc(id), 0, 0, "abcdef12", "jsonl"),
      zstdCompressSync(Buffer.from(Array.from({ length: turns }, (_, i) => line(`step ${i} for ${id}`)).join("\n") + "\n")));
    manifests.set(keyOf(id), {
      revision, trace_id: TRACE,
      session: { workspace_id: "w", machine_id: "m", harness: "claude", session_id: id, started_at: "2026-10-29T10:00:00Z" },
      raw: { format: "jsonl", content_sha256: String(revision).repeat(64), chunks: [{ seq: 0, start_offset: 0, sha256_8: "abcdef12" }], scrub: { ruleset: "r", version: 1 } },
    });
    await pointer(pg, { key: { orgId: "o", workspaceId: "w", harness: "claude", sessionId: id }, machine: "m", revision, manifestKey: keyOf(id) });
    await pg.query("UPDATE sessions.session_index SET state=$2, last_activity_at=$3 WHERE session_id=$1", [id, state, lastActivity.toISOString()]);
  }
  async function handle(id: string, opts: { regen: BobCostasRegen | null; now?: Date; summarizer?: Summarizer; kind?: "short" | "long"; workKey?: string }) {
    const handler = bobCostasHandler(opts.kind ?? "long", {
      db, reader, store, directory, keyring, summarizer: opts.summarizer ?? extractiveSummarizer,
      regen: opts.regen, now: () => opts.now ?? T0,
    });
    return captureLogs(() => handler({
      payload: { manifestKey: keyOf(id) }, workKey: opts.workKey,
      ctx: { orgId: "o", session_uuid: "01a188e1-ef86-7892-9389-39149894eeec" },
    } as never, { log: { info: () => {} } } as never));
  }
  const row = async (id: string, kind = "long") => (await pg.query<Record<string, unknown>>(
    `SELECT status, revision, covered_turns, failure_code, degraded, attempts, next_retry_at, config_hash, pipeline_version
       FROM sessions.session_summaries WHERE session_id=$1 AND kind=$2`, [id, kind])).rows[0];
  async function sweep(regen: BobCostasRegen | null, now: Date, via: typeof db = db) {
    await pg.exec("DELETE FROM sessions.pipeline_jobs");
    const { lines } = await captureLogs(() => summarySweep({ keyring, regen, everyMs: 0 })(via, now));
    const jobs = (await pg.query<{ session_id: string; work_key: string }>(
      "SELECT session_id, work_key FROM sessions.pipeline_jobs WHERE stage='summary-long' AND work_key LIKE '%:regen-%' ORDER BY session_id")).rows;
    return { jobs, lines };
  }
  const audit = async () => (await pg.query<Record<string, unknown>>(
    "SELECT session_id, system, kind, reason, prior_status, prior_failure_code, prior_pipeline_version FROM sessions.summary_regeneration_audit ORDER BY session_id, created_at")).rows;
  return { pg, db, session, handle, row, sweep, audit };
}

const summaryLine = (lines: Record<string, unknown>[]) => lines.find((l) => l.message === "bob costas summary");
/** A summarizer that counts its calls (a regeneration is a call; a coverage stamp is not). */
const counting = () => {
  const calls = { n: 0 };
  const summarizer = { ...extractiveSummarizer, summarize: (...args: Parameters<Summarizer["summarize"]>) => {
    calls.n += 1;
    return extractiveSummarizer.summarize(...args);
  } } as Summarizer;
  return { calls, summarizer };
};

describe("turn coverage (SUMMARY_REGEN_TURNS_ENABLED)", () => {
  const TURNS = regenOf({ SUMMARY_REGEN_TURNS_ENABLED: "true", SUMMARY_REGEN_TURN_THRESHOLD: "3", SUMMARY_REGEN_IDLE_SECONDS: "600" });

  test("a legacy row current by revision is stamped, never regenerated: below N, at N, idle, ended, abandoned", async () => {
    const w = await world();
    const cases = [
      { id: "below", turns: 2, state: "active", last: T0 },
      { id: "at-n", turns: 3, state: "active", last: T0 },
      { id: "idle", turns: 2, state: "active", last: at(-601) },
      { id: "not-idle-yet", turns: 2, state: "active", last: at(-599) },
      { id: "ended", turns: 1, state: "done", last: T0 },
      { id: "abandoned", turns: 1, state: "abandoned", last: T0 },
    ];
    for (const c of cases) {
      await w.session(c.id, c.turns, 2, c.state, c.last);
      await w.handle(c.id, { regen: null }); // written before the flag: covered_turns NULL
      expect((await w.row(c.id))!.covered_turns).toBeNull();
    }
    const { calls, summarizer } = counting();
    for (const c of cases) {
      const { result, lines } = await w.handle(c.id, { regen: TURNS, summarizer });
      const r = (await w.row(c.id))!;
      expect(result).toEqual({ status: "skipped", reason: "summary_current" });
      expect(summaryLine(lines)).toBeUndefined();
      expect(r).toMatchObject({ covered_turns: c.turns, pipeline_version: null, config_hash: null, revision: 2 });
    }
    expect(calls.n).toBe(0);
    // A stamped, current row is not read again: the next delivery is a plain skip.
    const again = await w.handle("below", { regen: TURNS, summarizer });
    expect(again.result).toEqual({ status: "skipped", reason: "summary_current" });
    expect(calls.n).toBe(0);
  });

  test("a legacy row stale by revision regenerates and is stamped by the new write", async () => {
    const w = await world();
    await w.session("stale", 2, 2, "done");
    await w.handle("stale", { regen: null });
    await w.session("stale", 4, 3, "done");
    const { calls, summarizer } = counting();
    const { result } = await w.handle("stale", { regen: TURNS, summarizer });
    expect(result ?? null).toBeNull();
    expect(calls.n).toBe(1);
    expect(await w.row("stale")).toMatchObject({ revision: 3, covered_turns: 4, pipeline_version: BOB_COSTAS_PIPELINE_VERSION, config_hash: HASH_A });
  });

  test("the threshold is configurable", async () => {
    const w = await world();
    await w.session("t", 2);
    await w.handle("t", { regen: null });
    const two = regenOf({ SUMMARY_REGEN_TURNS_ENABLED: "true", SUMMARY_REGEN_TURN_THRESHOLD: "2" });
    await w.handle("t", { regen: two }); // stamps covered_turns 2
    expect((await w.row("t"))!.covered_turns).toBe(2);
    await w.session("t", 4, 3);
    const { lines } = await w.handle("t", { regen: two });
    expect(summaryLine(lines)?.regeneration_reason).toBe("turns_gained");
    await w.session("t", 5, 4);
    const three = await w.handle("t", { regen: regenOf({ SUMMARY_REGEN_TURNS_ENABLED: "true", SUMMARY_REGEN_TURN_THRESHOLD: "2", SUMMARY_REGEN_IDLE_SECONDS: "600" }) });
    expect(summaryLine(three.lines)?.regeneration_reason).toBeUndefined();
  });

  test("the sweep queues revision-current legacy rows for a stamp only (no model call, no audit), and stale ended or idle rows once with :regen-turns", async () => {
    const w = await world();
    await w.session("sess-ended-5", 2, 2, "done");
    await w.session("sess-idle-6", 2, 2, "active", at(-3600));
    await w.session("sess-active-7", 2, 2, "active", T0);
    for (const id of ["sess-ended-5", "sess-idle-6", "sess-active-7"]) await w.handle(id, { regen: null });
    const on = regenOf({ SUMMARY_REGEN_TURNS_ENABLED: "true", SUMMARY_REGEN_AUDIT_ENABLED: "true" });

    // Current by revision: queued for the stamp only.
    const first = await w.sweep(on, T0);
    expect(first.jobs.map((j) => j.session_id)).toEqual(["sess-ended-5", "sess-idle-6"]);
    for (const j of first.jobs) expect(j.work_key.endsWith(":regen-stamp")).toBe(true);
    expect(await w.audit()).toEqual([]);
    expect(first.lines.filter((l) => l.message === "summary regeneration queued")).toEqual([]);
    expect(first.lines.filter((l) => l.message === "summary coverage stamp queued" && l.stage === "summary-long"))
      .toEqual([expect.objectContaining({ count: 2 })]);
    expect(JSON.stringify(first.lines)).not.toContain("sess-ended-5");
    const { calls, summarizer } = counting();
    for (const j of first.jobs) {
      const { result, lines } = await w.handle(j.session_id, { regen: on, workKey: j.work_key, summarizer });
      expect(result).toEqual({ status: "skipped", reason: "summary_current" });
      expect(summaryLine(lines)).toBeUndefined();
      expect((await w.row(j.session_id))!.covered_turns).toBe(2);
    }
    expect(calls.n).toBe(0);
    expect((await w.sweep(on, T0)).jobs).toEqual([]);

    // Stale by revision: a real regeneration, audited and logged hashed, once.
    await w.session("sess-ended-5", 3, 3, "done");
    await w.session("sess-idle-6", 3, 3, "active", at(-3600));
    await w.session("sess-active-7", 3, 3, "active", T0);
    const second = await w.sweep(on, T0);
    expect(second.jobs.map((j) => j.session_id)).toEqual(["sess-ended-5", "sess-idle-6"]);
    for (const j of second.jobs) expect(j.work_key.endsWith(":regen-turns")).toBe(true);
    expect(await w.audit()).toEqual([
      { session_id: "sess-ended-5", system: "bob_costas", kind: "long", reason: "ended_uncovered", prior_status: "ready", prior_failure_code: null, prior_pipeline_version: null },
      { session_id: "sess-idle-6", system: "bob_costas", kind: "long", reason: "idle_uncovered", prior_status: "ready", prior_failure_code: null, prior_pipeline_version: null },
    ]);
    const queued = second.lines.filter((l) => l.message === "summary regeneration queued" && l.stage === "summary-long");
    // Log order follows the sweep (newest activity first); compare as a set.
    expect(queued.map((l) => l.session_hash).sort()).toEqual([
      sessionHash({ orgId: "o", workspaceId: "w", harness: "claude", sessionId: "sess-ended-5" }),
      sessionHash({ orgId: "o", workspaceId: "w", harness: "claude", sessionId: "sess-idle-6" }),
    ].sort());
    expect(JSON.stringify(second.lines)).not.toContain("sess-ended-5");
    for (const j of second.jobs) {
      const { lines } = await w.handle(j.session_id, { regen: on, workKey: j.work_key, summarizer });
      expect(summaryLine(lines)?.regeneration_reason).toBe(j.session_id === "sess-ended-5" ? "ended_uncovered" : "idle_uncovered");
    }
    expect(calls.n).toBe(2);
    expect((await w.sweep(on, T0)).jobs).toEqual([]);
    expect(await w.audit()).toHaveLength(2);
  });

  test("first turns-flag-on rollout: a backlog of revision-current legacy rows is stamped with no model call", async () => {
    const w = await world();
    const ids = Array.from({ length: 12 }, (_, i) => `backlog-${String(i).padStart(2, "0")}`);
    for (const [i, id] of ids.entries()) {
      const state = i % 3 === 0 ? "done" : i % 3 === 1 ? "abandoned" : "active";
      await w.session(id, 1 + (i % 5), 2, state, state === "active" ? at(-7200) : T0);
      await w.handle(id, { regen: null });
    }
    const on = regenOf({ SUMMARY_REGEN_TURNS_ENABLED: "true", SUMMARY_REGEN_TURN_THRESHOLD: "1", SUMMARY_REGEN_AUDIT_ENABLED: "true" });
    const { calls, summarizer } = counting();
    const { jobs, lines } = await w.sweep(on, T0);
    expect(jobs).toHaveLength(ids.length);
    for (const j of jobs) {
      expect(j.work_key.endsWith(":regen-stamp")).toBe(true);
      expect((await w.handle(j.session_id, { regen: on, workKey: j.work_key, summarizer })).result).toEqual({ status: "skipped", reason: "summary_current" });
    }
    expect(calls.n).toBe(0);
    expect(lines.filter((l) => l.message === "summary regeneration queued")).toEqual([]);
    expect(await w.audit()).toEqual([]);
    for (const [i, id] of ids.entries()) expect(await w.row(id)).toMatchObject({ covered_turns: 1 + (i % 5), revision: 2, pipeline_version: null });
    expect((await w.sweep(on, T0)).jobs).toEqual([]);
  });

  test("audit off: the enqueue and its log line still happen, no audit row is written", async () => {
    const w = await world();
    await w.session("e", 2, 2, "done");
    await w.handle("e", { regen: null });
    await w.session("e", 3, 3, "done");
    const { jobs, lines } = await w.sweep(regenOf({ SUMMARY_REGEN_TURNS_ENABLED: "true" }), T0);
    expect(jobs).toHaveLength(1);
    expect(lines.some((l) => l.message === "summary regeneration queued")).toBe(true);
    expect(await w.audit()).toEqual([]);
  });

  test("a failing audit purge is logged and does not stop the regeneration pass", async () => {
    const w = await world();
    await w.session("p", 2, 2, "done");
    await w.handle("p", { regen: null });
    await w.session("p", 3, 3, "done");
    const failingPurge = new Proxy(w.db, {
      get(target, prop, receiver) {
        if (prop === "query") {
          return (text: string, params?: unknown[]) =>
            text.includes("summary_regeneration_audit_purge_v1") ? Promise.reject(new Error("purge boom")) : target.query(text as never, params as never);
        }
        const v = Reflect.get(target, prop, receiver);
        return typeof v === "function" ? v.bind(target) : v;
      },
    });
    const { jobs, lines } = await w.sweep(regenOf({ SUMMARY_REGEN_TURNS_ENABLED: "true", SUMMARY_REGEN_AUDIT_ENABLED: "true" }), T0, failingPurge);
    expect(lines.filter((l) => l.message === "summary regeneration audit purge failed"))
      .toEqual([expect.objectContaining({ level: "warn", stage: "summary-sweep", error_code: "Error" })]);
    expect(jobs.map((j) => j.work_key.endsWith(":regen-turns"))).toEqual([true]);
    expect((await w.audit()).map((a) => a.reason)).toEqual(["ended_uncovered"]);
  });
});

describe("retry backoff (SUMMARY_RETRY_BACKOFF_ENABLED)", () => {
  const RETRY = regenOf({ SUMMARY_RETRY_BACKOFF_ENABLED: "true", SUMMARY_REGEN_AUDIT_ENABLED: "true" });

  test("ladder 300/3600/21600: exact next_retry_at, the 4th failure is terminal, a 1-year clock jump selects nothing", async () => {
    const w = await world();
    await w.session("r", 2);
    await w.handle("r", { regen: RETRY, summarizer: failing(), now: T0 });
    let r = (await w.row("r"))!;
    expect(r).toMatchObject({ status: "ready", degraded: true, attempts: 1, failure_code: "model_failed" });
    expect(new Date(r.next_retry_at as string).toISOString()).toBe(at(300).toISOString());
    expect((await w.sweep(RETRY, at(299))).jobs).toEqual([]);

    let clock = 300;
    for (const [attempt, wait] of [[2, 3600], [3, 21600]] as const) {
      const { jobs } = await w.sweep(RETRY, at(clock));
      expect(jobs).toHaveLength(1);
      expect(jobs[0]!.work_key.endsWith(":regen-retry")).toBe(true);
      const { lines } = await w.handle("r", { regen: RETRY, summarizer: failing(), now: at(clock), workKey: jobs[0]!.work_key });
      expect(summaryLine(lines)?.regeneration_reason).toBe("retry_backoff");
      r = (await w.row("r"))!;
      expect(r.attempts).toBe(attempt);
      expect(new Date(r.next_retry_at as string).toISOString()).toBe(at(clock + wait).toISOString());
      clock += wait;
    }
    expect((await w.sweep(RETRY, at(clock))).jobs).toHaveLength(1);
    await w.handle("r", { regen: RETRY, summarizer: failing(), now: at(clock) });
    r = (await w.row("r"))!;
    expect(r).toMatchObject({ degraded: true, attempts: 4, next_retry_at: null, status: "ready" });
    const year = at(clock + 366 * 86_400);
    expect((await w.sweep(RETRY, year)).jobs).toEqual([]);
    expect((await w.handle("r", { regen: RETRY, summarizer: failing(), now: year })).result).toEqual({ status: "skipped", reason: "summary_current" });
    const reasons = (await w.audit()).map((a) => [a.reason, a.prior_failure_code]);
    expect(reasons).toEqual([["retry_backoff", "model_failed"], ["retry_backoff", "model_failed"], ["retry_backoff", "model_failed"]]);
  });

  test("a successful retry clears the retry state", async () => {
    const w = await world();
    await w.session("ok", 2);
    await w.handle("ok", { regen: RETRY, summarizer: failing(), now: T0 });
    await w.handle("ok", { regen: RETRY, summarizer: extractiveSummarizer, now: at(300) });
    expect(await w.row("ok")).toMatchObject({ degraded: false, attempts: 0, next_retry_at: null, failure_code: null });
  });

  test("a non-retryable fallback (no_context is not in the set) is never degraded", async () => {
    const w = await world();
    await w.session("nc", 2);
    const noContext: Summarizer = { id: "x", summarize: async (k, d) => ({ ...(await extractiveSummarizer.summarize(k, d, undefined as never)), fallbackReason: "no_context" }) } as Summarizer;
    await w.handle("nc", { regen: RETRY, summarizer: noContext });
    expect(await w.row("nc")).toMatchObject({ degraded: false, attempts: 0, next_retry_at: null, failure_code: "no_context" });
  });
});

describe("config reset (SUMMARY_RETRY_CONFIG_RESET_ENABLED)", () => {
  test("a degraded row under another config hash is due, and its attempts restart", async () => {
    const w = await world();
    await w.session("c", 2);
    const flags = { SUMMARY_RETRY_BACKOFF_ENABLED: "true", SUMMARY_RETRY_CONFIG_RESET_ENABLED: "true" };
    const oldCfg = regenOf(flags, HASH_A);
    for (const s of [0, 300, 3900, 25500]) await w.handle("c", { regen: oldCfg, summarizer: failing(), now: at(s) });
    expect(await w.row("c")).toMatchObject({ attempts: 4, next_retry_at: null, config_hash: HASH_A });
    expect((await w.sweep(oldCfg, at(90_000))).jobs).toEqual([]);
    const newCfg = regenOf(flags, HASH_B);
    const { jobs } = await w.sweep(newCfg, at(90_000));
    expect(jobs.map((j) => j.work_key.endsWith(":regen-config"))).toEqual([true]);
    const { lines } = await w.handle("c", { regen: newCfg, summarizer: failing(), now: at(90_000), workKey: jobs[0]!.work_key });
    expect(summaryLine(lines)?.regeneration_reason).toBe("config_changed");
    const r = (await w.row("c"))!;
    expect(r).toMatchObject({ attempts: 1, config_hash: HASH_B, degraded: true });
    expect(new Date(r.next_retry_at as string).toISOString()).toBe(at(90_300).toISOString());
    expect((await w.sweep(newCfg, at(90_000))).jobs).toEqual([]);
  });

  test("config reset alone (retry off) still revives a degraded row", async () => {
    const w = await world();
    await w.session("c2", 2);
    const only = { SUMMARY_RETRY_CONFIG_RESET_ENABLED: "true" };
    await w.handle("c2", { regen: regenOf(only, HASH_A), summarizer: failing() });
    expect(await w.row("c2")).toMatchObject({ degraded: true, attempts: 0, next_retry_at: null });
    const { jobs } = await w.sweep(regenOf(only, HASH_B), T0);
    expect(jobs).toHaveLength(1);
    await w.handle("c2", { regen: regenOf(only, HASH_B), summarizer: extractiveSummarizer, workKey: jobs[0]!.work_key });
    expect(await w.row("c2")).toMatchObject({ degraded: false, config_hash: HASH_B });
  });
});

describe("fences and feature detection", () => {
  const ALL = regenOf({
    SUMMARY_REGEN_TURNS_ENABLED: "true", SUMMARY_RETRY_BACKOFF_ENABLED: "true",
    SUMMARY_RETRY_CONFIG_RESET_ENABLED: "true", SUMMARY_REGEN_AUDIT_ENABLED: "true",
  });

  test("an older revision is still refused as stale_revision", async () => {
    const w = await world();
    await w.session("f", 2, 3);
    await w.handle("f", { regen: ALL });
    await w.session("f", 2, 2);
    expect((await w.handle("f", { regen: ALL })).result).toEqual({ status: "skipped", reason: "stale_revision" });
    expect((await w.row("f"))!.revision).toBe(3);
  });

  test("columns missing: flags behave as off, one warning, no crash", async () => {
    const w = await world();
    await w.pg.exec("ALTER TABLE sessions.session_summaries DROP COLUMN degraded");
    await w.session("m1", 2, 2, "done");
    const first = await w.handle("m1", { regen: ALL, summarizer: failing() });
    expect(first.result ?? null).toBeNull();
    const warns = first.lines.filter((l) => l.message === "summary regeneration schema missing; flags behave as off");
    expect(warns).toHaveLength(1);
    expect((await w.pg.query<{ covered_turns: unknown }>("SELECT covered_turns FROM sessions.session_summaries WHERE session_id='m1'")).rows[0]!.covered_turns).toBeNull();
    const second = await w.handle("m1", { regen: ALL, summarizer: failing() });
    expect(second.lines.filter((l) => l.message === warns[0]!.message)).toEqual([]);
    const swept = await w.sweep(ALL, T0);
    expect(swept.jobs).toEqual([]);
  });
});
