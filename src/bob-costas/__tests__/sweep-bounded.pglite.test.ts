/**
 * SUMMARY_SWEEP_BOUNDED=on: strict flag parsing, the window pass, the catch-up walk and its cursor,
 * the live-job and dead-letter guards, and parity of the page queries with the legacy queries.
 * Flags-off byte-identity is regen-golden.test.ts.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { masterKeyring, parseMasterKeys } from "../../shared/keyring.ts";
import { findMissingSummaries, findRegenDue, summarySweep } from "../sweep.ts";
import {
  BOUNDED_SWEEP_BOUNDS,
  boundedSweepFromEnv,
  boundedSweepStartup,
  consumePage,
  findMissingPage,
  findRegenPage,
  type BoundedSweepSettings,
  type PageRow,
  verifySummarySweepIndex,
} from "../sweep-bounded.ts";
import type { BobCostasRegen } from "../regen.ts";
import { regenSettings } from "../../summary-regen/settings.ts";
import { database, hoopDb, pointer, read } from "../../session-brief/__tests__/harness.ts";
import { captureLogs } from "../../shared/__tests__/golden.ts";

setDefaultTimeout(120_000);
const open: PGlite[] = [];
afterEach(async () => {
  while (open.length) await open.pop()!.close();
});

const NOW = new Date("2026-11-01T12:00:00.000Z");
const TRACE = "01a188e1-ef86-7b16-938a-e91b6b78643b";
const keyOf = (s: string) => `v1/ws=w/m=m/h=claude/d=2026/10/29/s=${s}/manifest.json`;
const ago = (seconds: number) => new Date(NOW.getTime() - seconds * 1000).toISOString();

async function world() {
  const pg = await database();
  open.push(pg);
  const db = hoopDb(pg);
  const keyring = await masterKeyring(parseMasterKeys(`k1:${"a".repeat(64)}`));
  const keyId = (await keyring.currentKey({ orgId: "-", workspaceId: "-" })).keyId;
  /** A live session_index row indexed `secondsAgo` before NOW. */
  async function session(id: string, secondsAgo: number, revision = 2) {
    await pointer(pg, { key: { orgId: "o", workspaceId: "w", harness: "claude", sessionId: id }, machine: "m", revision, manifestKey: keyOf(id) });
    await pg.query("UPDATE sessions.session_index SET indexed_at=$2 WHERE session_id=$1", [id, ago(secondsAgo)]);
  }
  /** A summary row that is not "missing" (not ready, so no pointer needed). */
  async function summary(id: string, kind: "short" | "long", status = "disabled") {
    await pg.query(
      `INSERT INTO sessions.session_summaries (org_id, workspace_id, harness, session_id, machine_id, kind, trace_id, revision, content_sha256, status)
       VALUES ('o', 'w', 'claude', $1, 'm', $2, $3, 2, $4, $5)`,
      [id, kind, TRACE, "c".repeat(64), status],
    );
  }
  async function readySummary(id: string, kind: "short" | "long", cols: Record<string, unknown>) {
    const names = Object.keys(cols);
    await pg.query(
      `INSERT INTO sessions.session_summaries (org_id, workspace_id, harness, session_id, machine_id, kind, trace_id, revision, content_sha256,
         status, object_key, object_sha256, key_id${names.map((n) => `, ${n}`).join("")})
       VALUES ('o', 'w', 'claude', $1, 'm', $2, $3, 2, $4, 'ready', 'obj', $4, $5${names.map((_, i) => `, $${i + 6}`).join("")})`,
      [id, kind, TRACE, "d".repeat(64), keyId, ...Object.values(cols)],
    );
  }
  async function liveJob(id: string, stage: string) {
    await pg.query(
      `INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, session_id, machine_id, message)
       VALUES ($2, $1 || '@test', $3, 'o', 'w', $1, 'm', '{}'::jsonb)`,
      [id, stage, TRACE],
    );
  }
  async function deadLetter(id: string, stage: string) {
    await pg.query(
      `INSERT INTO sessions.pipeline_dead_letters (id, stage, trace_id, org_id, workspace_id, session_id, machine_id, message)
       VALUES ($2 || ':' || $1, $2, $3, 'o', 'w', $1, 'm', '{}'::jsonb)`,
      [id, stage, TRACE],
    );
  }
  async function queued(stage: string): Promise<string[]> {
    return (await pg.query<{ session_id: string }>(
      "SELECT session_id FROM sessions.pipeline_jobs WHERE stage=$1 AND work_key LIKE '%@summary-sweep:%' ORDER BY session_id", [stage])).rows.map((r) => r.session_id);
  }
  return { pg, db, keyring, keyId, session, summary, readySummary, liveJob, deadLetter, queued };
}

const settings = (o: Partial<BoundedSweepSettings> = {}): BoundedSweepSettings => ({ windowSeconds: 3_600, pageRows: 1_000, catchupSeconds: 86_400, ...o });

describe("SUMMARY_SWEEP_BOUNDED parsing", () => {
  test("unset, empty and off are off (null); on gives the defaults", () => {
    expect(boundedSweepFromEnv({})).toBeNull();
    expect(boundedSweepFromEnv({ SUMMARY_SWEEP_BOUNDED: "" })).toBeNull();
    expect(boundedSweepFromEnv({ SUMMARY_SWEEP_BOUNDED: "off" })).toBeNull();
    expect(boundedSweepFromEnv({ SUMMARY_SWEEP_BOUNDED: "on" })).toEqual({
      windowSeconds: BOUNDED_SWEEP_BOUNDS.windowSeconds.fallback,
      pageRows: BOUNDED_SWEEP_BOUNDS.pageRows.fallback,
      catchupSeconds: BOUNDED_SWEEP_BOUNDS.catchupSeconds.fallback,
    });
  });

  test("anything else refuses startup", () => {
    for (const v of ["ON", "On", "true", "1", "yes", " on", "on ", "of"])
      expect(() => boundedSweepFromEnv({ SUMMARY_SWEEP_BOUNDED: v })).toThrow("SUMMARY_SWEEP_BOUNDED must be on or off");
  });

  test("tuning is read only when on; in range is taken, malformed or out of range refuses startup", () => {
    expect(boundedSweepFromEnv({ SUMMARY_SWEEP_BOUNDED: "off", SUMMARY_SWEEP_PAGE_ROWS: "nope" })).toBeNull();
    expect(boundedSweepFromEnv({
      SUMMARY_SWEEP_BOUNDED: "on", SUMMARY_SWEEP_WINDOW_SECONDS: "900", SUMMARY_SWEEP_PAGE_ROWS: "250", SUMMARY_SWEEP_CATCHUP_SECONDS: "7200",
    })).toEqual({ windowSeconds: 900, pageRows: 250, catchupSeconds: 7_200 });
    for (const [name, v] of [
      ["SUMMARY_SWEEP_PAGE_ROWS", "9"], ["SUMMARY_SWEEP_PAGE_ROWS", "10001"], ["SUMMARY_SWEEP_PAGE_ROWS", "1e3"], ["SUMMARY_SWEEP_PAGE_ROWS", " 100"],
      ["SUMMARY_SWEEP_WINDOW_SECONDS", "299"], ["SUMMARY_SWEEP_CATCHUP_SECONDS", "-1"], ["SUMMARY_SWEEP_CATCHUP_SECONDS", "3599"],
    ] as const)
      expect(() => boundedSweepFromEnv({ SUMMARY_SWEEP_BOUNDED: "on", [name]: v })).toThrow(name);
  });
});

describe("SUMMARY_SWEEP_WINDOW_SECONDS >= SUMMARY_REGEN_IDLE_SECONDS", () => {
  test("a window shorter than the effective idle setting refuses startup; equal or longer is taken", () => {
    // SUMMARY_REGEN_IDLE_SECONDS unset: its default (600) applies.
    expect(() => boundedSweepFromEnv({ SUMMARY_SWEEP_BOUNDED: "on", SUMMARY_SWEEP_WINDOW_SECONDS: "599" }))
      .toThrow("SUMMARY_SWEEP_WINDOW_SECONDS (599) must be at least SUMMARY_REGEN_IDLE_SECONDS (600)");
    expect(boundedSweepFromEnv({ SUMMARY_SWEEP_BOUNDED: "on", SUMMARY_SWEEP_WINDOW_SECONDS: "600" })?.windowSeconds).toBe(600);
    expect(() => boundedSweepFromEnv({ SUMMARY_SWEEP_BOUNDED: "on", SUMMARY_REGEN_IDLE_SECONDS: "7200" }))
      .toThrow("SUMMARY_SWEEP_WINDOW_SECONDS (3600) must be at least SUMMARY_REGEN_IDLE_SECONDS (7200)");
    expect(boundedSweepFromEnv({ SUMMARY_SWEEP_BOUNDED: "on", SUMMARY_REGEN_IDLE_SECONDS: "7200", SUMMARY_SWEEP_WINDOW_SECONDS: "7200" })?.windowSeconds).toBe(7_200);
    // Flag off: never checked.
    expect(boundedSweepFromEnv({ SUMMARY_SWEEP_BOUNDED: "off", SUMMARY_REGEN_IDLE_SECONDS: "7200", SUMMARY_SWEEP_WINDOW_SECONDS: "300" })).toBeNull();
  });
});

describe("consumePage", () => {
  const row = (rn: number, needs: boolean, page_rows: number): PageRow => ({
    org_id: "o", workspace_id: "w", harness: "claude", session_id: `s${rn}`, machine_id: "m", manifest_key: "k",
    trace_id: TRACE, revision: "1", session_uuid: null, cursor_at: `t${rn}`, rn, page_rows, needs,
  });
  test("a full page advances to its edge; a short one is exhausted", () => {
    expect(consumePage([row(2, true, 5), row(5, false, 5)], 10, 5)).toMatchObject({ cursor: { session_id: "s5" }, exhausted: false });
    expect(consumePage([row(2, true, 3), row(3, true, 3)], 10, 5)).toMatchObject({ cursor: { session_id: "s3" }, exhausted: true });
    expect(consumePage([], 10, 5)).toEqual({ picked: [], cursor: null, exhausted: true });
  });
  test("the limit stops at the last picked row, so the rest is read again", () => {
    const r = consumePage([row(1, true, 5), row(3, true, 5), row(5, true, 5)], 2, 5);
    expect(r.picked.map((p) => p.session_id)).toEqual(["s1", "s3"]);
    expect(r).toMatchObject({ cursor: { session_id: "s3" }, exhausted: false });
  });
});

describe("bounded summary sweep (pglite)", () => {
  test("window pass: queues missing summaries indexed inside the window only, skipping live jobs and dead letters", async () => {
    const w = await world();
    await w.session("recent-missing", 60);
    await w.session("recent-job", 120);
    await w.session("recent-dlq", 180);
    await w.session("recent-done", 240);
    await w.session("old-missing", 7_200);
    await w.liveJob("recent-job", "summary-short");
    await w.deadLetter("recent-dlq", "summary-short");
    for (const k of ["short", "long"] as const) await w.summary("recent-done", k);
    // A walk that already ran: only the window pass reads.
    const sweep = summarySweep({ keyring: w.keyring, regen: null, bounded: settings({ catchupSeconds: 86_400 }) });
    const { lines } = await captureLogs(() => sweep(w.db, NOW));
    // First tick also starts the walk from the top, so the old row is reached by the walk, not the window.
    expect(await w.queued("summary-short")).toEqual(["old-missing", "recent-missing"]);
    // summary-long has neither guard for those two sessions.
    expect(await w.queued("summary-long")).toEqual(["old-missing", "recent-dlq", "recent-job", "recent-missing"]);
    const queuedLines = lines.filter((l) => (l as { message?: string }).message === "summary sweep queued");
    expect(queuedLines.map((l) => (l as Record<string, unknown>).reason)).toEqual(["bounded_window", "bounded_walk", "bounded_window", "bounded_walk"]);
    // Second tick: everything has a live job now, nothing new is queued twice.
    const before = (await w.pg.query("SELECT count(*)::int AS n FROM sessions.pipeline_jobs")).rows;
    await summarySweep({ keyring: w.keyring, regen: null, bounded: settings(), everyMs: 0 })(w.db, NOW);
    expect((await w.pg.query("SELECT count(*)::int AS n FROM sessions.pipeline_jobs")).rows).toEqual(before);
  });

  test("window pass alone never reads rows older than the window", async () => {
    const w = await world();
    await w.session("recent-missing", 60);
    await w.session("old-missing", 7_200);
    const rows = await findMissingPage(w.db, "summary-short", "short", 1_000, w.keyId, { since: new Date(NOW.getTime() - 3_600_000) });
    expect(rows.filter((r) => r.needs).map((r) => r.session_id)).toEqual(["recent-missing"]);
  });

  test("catch-up walk: pages newest-first after its cursor, advances, finishes, then waits for the cadence", async () => {
    const w = await world();
    // Ten sessions all outside the window (so only the walk sees them), s0 newest.
    for (let i = 0; i < 10; i++) await w.session(`s${i}`, 10_000 + i * 60);
    for (const k of ["short", "long"] as const) for (const id of ["s1", "s2", "s5"]) await w.summary(id, k);
    await w.liveJob("s6", "summary-short");
    await w.deadLetter("s7", "summary-short");
    const sweep = summarySweep({ keyring: w.keyring, regen: null, bounded: settings({ pageRows: 3, catchupSeconds: 3_600 }), everyMs: 0, limit: 100 });
    const ticks: string[][] = [];
    let prev: string[] = [];
    for (let t = 0; t < 5; t++) {
      await sweep(w.db, new Date(NOW.getTime() + t * 300_000));
      const now = await w.queued("summary-short");
      ticks.push(now.filter((s) => !prev.includes(s)));
      prev = now;
    }
    // Pages of 3: [s0 s1 s2] [s3 s4 s5] [s6 s7 s8] [s9] (short: done), then idle until the cadence.
    expect(ticks).toEqual([["s0"], ["s3", "s4"], ["s8"], ["s9"], []]);
    // A new walk after catchupSeconds finds a summary that went missing meanwhile.
    await w.pg.query("DELETE FROM sessions.session_summaries WHERE session_id='s2' AND kind='short'");
    await sweep(w.db, new Date(NOW.getTime() + 4 * 300_000 + 60_000));
    expect((await w.queued("summary-short")).includes("s2")).toBe(false);
    await sweep(w.db, new Date(NOW.getTime() + 3_600_000 + 1));
    expect((await w.queued("summary-short")).includes("s2")).toBe(true);
  });

  test("the queue limit stops the walk at the last queued row and resumes after it", async () => {
    const w = await world();
    for (let i = 0; i < 6; i++) await w.session(`s${i}`, 10_000 + i * 60);
    const sweep = summarySweep({ keyring: w.keyring, regen: null, bounded: settings({ pageRows: 1_000 }), everyMs: 0, limit: 2 });
    await sweep(w.db, NOW);
    expect(await w.queued("summary-short")).toEqual(["s0", "s1"]);
    await sweep(w.db, new Date(NOW.getTime() + 300_000));
    expect(await w.queued("summary-short")).toEqual(["s0", "s1", "s2", "s3"]);
    await sweep(w.db, new Date(NOW.getTime() + 600_000));
    expect(await w.queued("summary-short")).toEqual(["s0", "s1", "s2", "s3", "s4", "s5"]);
  });

  test("a full page queues the same sessions as the legacy query (missing and regeneration-due)", async () => {
    const w = await world();
    for (let i = 0; i < 8; i++) await w.session(`s${i}`, i * 900, 3);
    await w.summary("s1", "short");
    await w.summary("s1", "long", "quarantined");
    await w.liveJob("s2", "summary-short");
    await w.deadLetter("s3", "summary-long");
    await w.pg.query("UPDATE sessions.session_index SET deleted_at=now() WHERE session_id='s4'");
    await w.readySummary("s5", "short", { degraded: true, config_hash: "x", attempts: 1 });
    await w.readySummary("s6", "short", { covered_turns: 4 });
    await w.pg.query("UPDATE sessions.session_index SET state='done' WHERE session_id='s6'");
    await w.readySummary("s7", "long", { degraded: true, attempts: 1, next_retry_at: ago(10) });
    const regen: BobCostasRegen = {
      settings: regenSettings({ SUMMARY_REGEN_TURNS_ENABLED: "true", SUMMARY_RETRY_BACKOFF_ENABLED: "true", SUMMARY_RETRY_CONFIG_RESET_ENABLED: "true" }),
      configHash: "a".repeat(64),
    };
    const strip = (rows: Record<string, unknown>[]) =>
      rows.map(({ cursor_at: _c, rn: _r, page_rows: _p, needs: _n, ...rest }) => rest).sort((a, b) => String(a.session_id).localeCompare(String(b.session_id)));
    for (const [stage, kind] of [["summary-short", "short"], ["summary-long", "long"]] as const) {
      const legacy = await findMissingSummaries(w.db, stage, 1_000, w.keyId);
      const page = (await findMissingPage(w.db, stage, kind, 1_000, w.keyId, { after: null })).filter((r) => r.needs);
      expect(strip(page as never)).toEqual(strip(legacy as never));
      expect(legacy.length).toBeGreaterThan(0);
      const legacyRegen = await findRegenDue(w.db, stage, 1_000, regen, NOW);
      const pageRegen = (await findRegenPage(w.db, stage, kind, 1_000, regen, NOW, { after: null })).filter((r) => r.needs);
      expect(strip(pageRegen as never)).toEqual(strip(legacyRegen as never));
      expect(legacyRegen.length).toBeGreaterThan(0);
    }
  });

  test("the pending index builds and serves the page; results are the same with and without it", async () => {
    const w = await world();
    for (let i = 0; i < 5; i++) await w.session(`s${i}`, i * 60);
    await w.summary("s2", "short");
    const bound = { after: null };
    const without = await findMissingPage(w.db, "summary-short", "short", 3, w.keyId, bound);
    await w.pg.exec(read("supabase/pending/summary_sweep_indexed_at.sql"));
    expect((await w.pg.query("SELECT to_regclass('sessions.session_index_summary_sweep_idx') IS NOT NULL AS ok")).rows).toEqual([{ ok: true }]);
    expect(await findMissingPage(w.db, "summary-short", "short", 3, w.keyId, bound)).toEqual(without);
    expect(without.map((r) => [r.session_id, r.needs, r.rn, r.page_rows])).toEqual([["s0", true, 1, 3], ["s1", true, 2, 3], ["s2", false, 3, 3]]);
  });

  test("flag on with regeneration active: regen jobs and audit rows come from the page queries, never the legacy SQL", async () => {
    const w = await world();
    // One due session inside the window (config_changed) and one only the walk reaches (ended, turns behind).
    await w.session("r-config", 60, 3);
    await w.session("r-turns", 7_200, 3);
    await w.readySummary("r-config", "short", { degraded: true, config_hash: "x", attempts: 1, covered_turns: 4 });
    await w.readySummary("r-turns", "short", { covered_turns: 4 });
    await w.pg.query("UPDATE sessions.session_index SET state='done' WHERE session_id='r-turns'");
    const regen: BobCostasRegen = {
      settings: regenSettings({
        SUMMARY_REGEN_TURNS_ENABLED: "true", SUMMARY_RETRY_CONFIG_RESET_ENABLED: "true", SUMMARY_REGEN_AUDIT_ENABLED: "true",
      }),
      configHash: "a".repeat(64),
    };
    const texts: string[] = [];
    const db = { ...w.db, query: async (text: string, params?: unknown[]) => { texts.push(text); return w.db.query(text, params as never); } } as typeof w.db;
    await summarySweep({ keyring: w.keyring, regen, bounded: settings() })(db, NOW);
    const jobs = (await w.pg.query<{ session_id: string; work_key: string }>(
      "SELECT session_id, work_key FROM sessions.pipeline_jobs WHERE stage='summary-short' ORDER BY session_id")).rows;
    expect(jobs.map((j) => [j.session_id, j.work_key.slice(j.work_key.lastIndexOf("@"))])).toEqual([
      ["r-config", "@summary-sweep:3:regen-config"],
      ["r-turns", "@summary-sweep:3:regen-turns"],
    ]);
    const audit = (await w.pg.query("SELECT session_id, system, kind, reason FROM sessions.summary_regeneration_audit ORDER BY session_id")).rows;
    expect(audit).toEqual([
      { session_id: "r-config", system: "bob_costas", kind: "short", reason: "config_changed" },
      { session_id: "r-turns", system: "bob_costas", kind: "short", reason: "ended_uncovered" },
    ]);
    // Every read of session_index went through a keyset page: no legacy findMissingSummaries / findRegenDue text.
    const indexReads = texts.filter((t) => /sessions\.session_index i\b/.test(t));
    expect(indexReads.length).toBe(8);
    expect(indexReads.every((t) => t.startsWith("WITH page AS MATERIALIZED"))).toBe(true);
    expect(texts.some((t) => t.includes("NOT EXISTS (SELECT 1 FROM sessions.session_summaries s"))).toBe(false);
    expect(texts.some((t) => /JOIN sessions\.session_summaries s\s+ON/.test(t))).toBe(false);
  });

  test("keyset ties: ten rows sharing one indexed_at across two workspaces, pages of 3, each queued exactly once", async () => {
    const w = await world();
    const AT = "2026-11-01T09:00:00.123456+00";
    for (const ws of ["w1", "w2"])
      for (let i = 0; i < 5; i++)
        await pointer(w.pg, {
          key: { orgId: "o", workspaceId: ws, harness: "claude", sessionId: `t${i}` }, machine: "m", revision: 2,
          manifestKey: `v1/ws=${ws}/m=m/h=claude/d=2026/10/29/s=t${i}/manifest.json`,
        });
    await w.pg.query("UPDATE sessions.session_index SET indexed_at=$1", [AT]);
    expect((await w.pg.query("SELECT count(DISTINCT indexed_at)::int AS n, count(*)::int AS rows FROM sessions.session_index")).rows)
      .toEqual([{ n: 1, rows: 10 }]);
    const sweep = summarySweep({ keyring: w.keyring, regen: null, bounded: settings({ pageRows: 3 }), everyMs: 0, limit: 100 });
    // ceil(10 / 3) = 4 walk pages, then one more tick to prove nothing is queued twice.
    for (let t = 0; t < 5; t++) await sweep(w.db, new Date(NOW.getTime() + t * 300_000));
    for (const stage of ["summary-short", "summary-long"]) {
      const rows = (await w.pg.query<{ workspace_id: string; session_id: string; n: number }>(
        `SELECT workspace_id, session_id, count(*)::int AS n FROM sessions.pipeline_jobs WHERE stage=$1
          GROUP BY workspace_id, session_id ORDER BY workspace_id, session_id`, [stage])).rows;
      expect(rows.length).toBe(10);
      expect(rows.every((r) => r.n === 1)).toBe(true);
    }
  });

  test("bounded off (null) issues only the legacy queries", async () => {
    const w = await world();
    await w.session("s0", 60);
    const texts: string[] = [];
    const db = { ...w.db, query: async (text: string, params?: unknown[]) => { texts.push(text); return w.db.query(text, params as never); } } as typeof w.db;
    await summarySweep({ keyring: w.keyring, regen: null, bounded: null })(db, NOW);
    expect(texts.some((t) => t.includes("WITH page AS MATERIALIZED"))).toBe(false);
    expect(texts.filter((t) => t.includes("NOT EXISTS (SELECT 1 FROM sessions.session_summaries s")).length).toBe(2);
  });
});

describe("bounded sweep startup index probe (pglite)", () => {
  test("absent refuses startup (index_missing); built and valid passes and logs the settings; invalid refuses (index_invalid)", async () => {
    const w = await world();
    await expect(verifySummarySweepIndex(w.db)).rejects.toThrow("index_missing");
    await expect(boundedSweepStartup(settings())(w.db)).rejects.toThrow("SUMMARY_SWEEP_BOUNDED=on needs a valid sessions.session_index_summary_sweep_idx");
    await w.pg.exec(read("supabase/pending/summary_sweep_indexed_at.sql"));
    await verifySummarySweepIndex(w.db);
    const { lines } = await captureLogs(() => boundedSweepStartup(settings({ pageRows: 250 }))(w.db));
    expect(lines.filter((l) => (l as { message?: string }).message === "bob costas summary sweep")).toMatchObject([
      { stage: "summary", state: "bounded_on", window_seconds: 3_600, page_rows: 250, catchup_seconds: 86_400 },
    ]);
    // A failed or unfinished CONCURRENTLY build leaves the index present but not valid.
    await w.pg.query("UPDATE pg_catalog.pg_index SET indisvalid=false WHERE indexrelid='sessions.session_index_summary_sweep_idx'::regclass");
    await expect(verifySummarySweepIndex(w.db)).rejects.toThrow("index_invalid");
    await w.pg.query("UPDATE pg_catalog.pg_index SET indisvalid=true, indisready=false WHERE indexrelid='sessions.session_index_summary_sweep_idx'::regclass");
    await expect(verifySummarySweepIndex(w.db)).rejects.toThrow("index_invalid");
  });

  test("a probe error refuses startup (index_unverified)", async () => {
    const db = { query: async () => { throw new Error("boom"); } };
    await expect(verifySummarySweepIndex(db as never)).rejects.toThrow("index_unverified");
  });
});
