import { describe, expect, test } from "bun:test";
import {
  AUTHORITY_CLOCK_SKEW_MS,
  AUTHORITY_EVIDENCE as E,
  AUTHORITY_MIN_OBSERVATION_WINDOW_MS,
  AUTHORITY_OBSERVATION_WINDOW_MS,
  AUTHORITY_QUEUE_PAGE,
  AUTHORITY_REFUSALS,
  AUTHORITY_SQL,
  AUTHORITY_STATS_SETTLE_MS,
  AUTHORITY_STATS_SQL,
  AUTHORITY_MARKER_MAX_AGE_MS,
  authorityMarkerAllowed,
  authorityBaseline,
  authorityEvidence,
  evidenceHash,
  resolveTurnsWritePath,
  type AuthorityBaseline,
  type AuthorityEvidenceReport,
  type AuthorityEvidenceSources,
  type AuthorityRefusal,
} from "../authority-evidence.ts";

const ORG = "11111111-2222-4333-8444-555555555555";
const ORG2 = "66666666-7777-4888-9999-aaaaaaaaaaaa";
const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const qid = (n: number) => String(n).repeat(32).slice(0, 32);
const NOW = Date.parse("2026-11-26T12:00:00Z");
const iso = (ms: number) => new Date(ms).toISOString();
const HOUR = 3_600_000;

type Binding = Record<string, unknown>;
const plain = (name: string, text: string): Binding => ({ name, type: "plain_text", text });

const Q = { name: "INGEST_ADMISSION", type: "queue", queue_name: E.admissionQueue };
const ING_CONSUMER = { type: "worker", script_name: E.ingestWorker, dead_letter_queue: E.admissionDlq, queue_name: E.admissionQueue, consumer_id: "c", settings: {} };
const ING_PRODUCER = { type: "worker", script: E.ingestWorker };
const D1 = "h" + "ab".repeat(16), D2 = "h" + "cd".repeat(16);
const STOPPED = (orgs = `${ORG},${ORG2}`) => [plain("INGEST_HTTP_ADMISSION_STOPPED_ORGS", orgs), plain("INGEST_S3_FENCE_MIRROR", "on"), Q];
const at = <T>(arr: T[], i: number): T => arr[Math.min(i, arr.length - 1)]!;

/**
 * A mutable fake of production. Default: every check passes for ORG and ORG2. The stop was established by deployment
 * 102 (five hours ago); 103 (nine hours ago) is the proven boundary (readable, without the stop). The clock advances
 * by exactly what sleep is asked for.
 */
function world() {
  const w = {
    ingestDeployments: [
      { id: uuid(101), created_on: iso(NOW - 1 * HOUR), versions: [{ version_id: uuid(1), percentage: 100 }] },
      { id: uuid(102), created_on: iso(NOW - 5 * HOUR), versions: [{ version_id: uuid(2), percentage: 100 }] },
      { id: uuid(103), created_on: iso(NOW - 9 * HOUR), versions: [{ version_id: uuid(3), percentage: 100 }] },
    ] as Array<Record<string, unknown>>,
    versions: new Map<string, Binding[] | "fail">([
      [uuid(1), [...STOPPED(), { name: "HYPERDRIVE", type: "hyperdrive", id: "x" }]],
      [uuid(2), STOPPED()],
      [uuid(3), [plain("INGEST_S3_FENCE_MIRROR", "on"), Q]],
      [uuid(51), [plain("TURNS_WRITE_PATH", "neon")]],
      [uuid(52), []],
    ]),
    rhDeployments: [{ id: uuid(201), created_on: iso(NOW - 2 * HOUR), versions: [{ version_id: uuid(51), percentage: 100 }] }] as Array<Record<string, unknown>>,
    queues: [
      { queue_id: qid(1), queue_name: E.admissionQueue, consumers: [ING_CONSUMER], consumers_total_count: 1, producers: [ING_PRODUCER], producers_total_count: 1 },
      { queue_id: qid(2), queue_name: E.admissionDlq, consumers: [], consumers_total_count: 0, producers: [], producers_total_count: 0 },
      { queue_id: qid(3), queue_name: E.fanoutQueue, consumers: [{ ...ING_CONSUMER, dead_letter_queue: "fanout-dlq" }], consumers_total_count: 1, producers: [ING_PRODUCER], producers_total_count: 1 },
      { queue_id: qid(4), queue_name: "something-else", consumers: [{ type: "worker", script_name: "other" }], consumers_total_count: 1, producers: [], producers_total_count: 0 },
    ] as Array<Record<string, unknown>>,
    /** Replaces the queue list from the second listing on. */
    queuesAfter: null as Array<Record<string, unknown>> | null,
    mainQueue: {
      consumers: [ING_CONSUMER], consumers_total_count: 1, producers: [ING_PRODUCER], producers_total_count: 1,
    } as Record<string, unknown>,
    dlqTotal: null as unknown,
    backlog: [0] as unknown[],
    dlqBacklog: [3] as unknown[],
    dlqConsumers: [[]] as unknown[][],
    sqlRows: [] as Array<Record<string, unknown> | "fail" | "bad">,
    sqlDefault: {
      visible_tables: "3", tables: "3", batches_claimed_since: "0", batches_committed_since: "0", batches_pending: "0",
      batches_new_since_base: "0", revisions_updated_since: "0", revisions_count: "120", revisions_new_since_base: "0",
      revisions_digest: D1, ownership_count: "40", ownership_new_since_base: "0", ownership_digest: D2,
      snapshot: "1000:1005:1001,1003", isolation: "read committed", read_only: "on", read_at_ms: String(NOW),
    } as Record<string, unknown>,
    /** Deletion-counter reads in order (the last repeats). */
    stats: [{ tables: "1", batches_deleted: "7", stats_reset_ms: "0", started_ms: "1700000000000", track_counts: "on" }] as Array<Record<string, unknown> | "fail">,
    /** The queue list's result_info; null = the honest one for the returned page. */
    listInfo: null as unknown,
    /** The isolation the SQL fake reports for a baseline read (authorityBaseline's own transport). */
    baselineIsolation: "repeatable read",
    ingestAfter: null as Array<Record<string, unknown>> | null,
    rhAfter: null as Array<Record<string, unknown>> | "fail" | null,
    fail: new Set<string>(),
    /** Clock advance per sleep, as a fraction of the requested ms (1 = honest). */
    sleepRate: 1,
    calls: [] as string[],
    sqlCalls: [] as Array<readonly string[]>,
    slept: [] as number[],
    clock: NOW,
  };
  let ingestReads = 0, rhReads = 0, metricsReads = 0, dlqMetricsReads = 0, dlqReads = 0, listReads = 0, sqlIdx = 0, statsIdx = 0;
  const raw = async (path: string): Promise<unknown> => {
    {
      w.calls.push(path);
      if (w.fail.has(path)) throw new Error(`boom with secret ${ORG}`);
      if (path === `/workers/scripts/${E.ingestWorker}/deployments`)
        return { deployments: ingestReads++ % 2 === 0 || !w.ingestAfter ? w.ingestDeployments : w.ingestAfter };
      if (path === `/workers/scripts/${E.relayhistoryWorker}/deployments`) {
        const second = rhReads++ % 2 === 1;
        if (second && w.rhAfter === "fail") throw new Error("x");
        return { deployments: second && w.rhAfter ? w.rhAfter : w.rhDeployments };
      }
      const v = /^\/workers\/scripts\/[^/]+\/versions\/(.+)$/u.exec(path);
      if (v) {
        const b = w.versions.get(v[1]!);
        if (b === undefined || b === "fail") throw new Error("404");
        return { id: v[1], resources: { bindings: b } };
      }
      if (path === `/queues?page=1&per_page=${AUTHORITY_QUEUE_PAGE}`) return listReads++ > 0 && w.queuesAfter ? w.queuesAfter : w.queues;
      const ids = (listReads > 1 && w.queuesAfter ? w.queuesAfter : w.queues).filter((q) => q.queue_name === E.admissionQueue).map((q) => q.queue_id);
      if (path.endsWith("/metrics") && ids.some((id) => path === `/queues/${id}/metrics`))
        return { backlog_count: at(w.backlog, metricsReads++), backlog_bytes: 0 };
      if (path === `/queues/${qid(2)}/metrics`) return { backlog_count: at(w.dlqBacklog, dlqMetricsReads++) };
      if (path === `/queues/${qid(2)}`) {
        const consumers = at(w.dlqConsumers, dlqReads++);
        return { queue_id: qid(2), queue_name: E.admissionDlq, consumers, consumers_total_count: w.dlqTotal ?? (Array.isArray(consumers) ? consumers.length : 0),
          producers: [], producers_total_count: 0 };
      }
      const main = ids.find((id) => path === `/queues/${id}`);
      if (main) return { queue_id: main, queue_name: E.admissionQueue, ...w.mainQueue };
      throw new Error("unexpected " + path);
    }
  };
  const src: AuthorityEvidenceSources = {
    // The whole API body, as the real transport resolves it.
    async cfGet(path) {
      const result = await raw(path);
      if (path.startsWith("/queues?")) {
        const n = Array.isArray(result) ? result.length : 0;
        return { success: true, errors: [], result, result_info: w.listInfo ?? { page: 1, per_page: AUTHORITY_QUEUE_PAGE, count: n, total_count: n, total_pages: 1 } };
      }
      return { success: true, errors: [], result };
    },
    async sql(text, params) {
      if (text === AUTHORITY_STATS_SQL) {
        const st = at(w.stats, statsIdx++);
        if (st === "fail") throw new Error("stats");
        return [st];
      }
      expect(text).toBe(AUTHORITY_SQL);
      w.sqlCalls.push(params);
      const row = w.sqlRows[sqlIdx++] ?? w.sqlDefault;
      if (row === "fail") throw new Error(`pg error for ${params[0]}`);
      if (row === "bad") return [{ ...w.sqlDefault, batches_pending: "-1" }];
      return [{ ...w.sqlDefault, ...row }];
    },
    nowMs: () => w.clock,
    async sleep(ms) { w.slept.push(ms); w.clock += Math.floor(ms * w.sleepRate); },
  };
  return { w, src };
}

const WINDOW = AUTHORITY_MIN_OBSERVATION_WINDOW_MS;
const baseline = (src: AuthorityEvidenceSources, orgs: unknown[] = [ORG]) => authorityEvidence(orgs, src, { observationWindowMs: WINDOW });
type World = ReturnType<typeof world>;
/** authorityBaseline over the world's SQL, reporting the world's baseline isolation (the backfill's snapshot transaction). */
const takeBaseline = (wd: World, orgs: unknown[], unbound: unknown) =>
  authorityBaseline(orgs, unbound, async (text, params) => {
    const rows = await wd.src.sql(text, params);
    return rows.map((r) => ({ ...r, isolation: wd.w.baselineIsolation }));
  });
/**
 * The only path to a pass: an unbound run, a baseline in the backfill's snapshot, then a run bound to it. `between`
 * mutates the world after the baseline; `edit` edits the baseline document.
 */
async function bound(wd: World, orgs: unknown[] = [ORG], between?: (w: World["w"]) => void, edit?: (b: AuthorityBaseline) => unknown) {
  const unbound = await baseline(wd.src, orgs);
  const b = await takeBaseline(wd, orgs, JSON.parse(JSON.stringify(unbound)));
  wd.w.clock += 60_000;
  between?.(wd.w);
  return authorityEvidence(orgs, wd.src, { observationWindowMs: WINDOW, bound: edit ? edit(b) : JSON.parse(JSON.stringify(b)) });
}

function refusedWith(report: AuthorityEvidenceReport, code: AuthorityRefusal, org = 0) {
  expect(report.status).toBe("authority_evidence_refused");
  expect(report.orgs[org]!.ok).toBe(false);
  expect(report.orgs[org]!.refusals).toContain(code);
}

/** No org id, deployment/version/queue id or raw binding text may appear anywhere in the output. */
function assertNoLeak(report: AuthorityEvidenceReport) {
  const text = JSON.stringify(report);
  for (const secret of [ORG, ORG2, uuid(1), uuid(101), uuid(201), qid(1), qid(2), "boom", "pg error", "neon,"])
    expect(text).not.toContain(secret);
  const walk = (v: unknown): void => {
    if (typeof v === "string") expect(v).toMatch(/^[a-z][a-z0-9_]{0,39}$/u);
    else if (Array.isArray(v)) v.forEach(walk);
    else if (v && typeof v === "object") Object.values(v).forEach(walk);
    else if (v !== null) expect(["number", "boolean"]).toContain(typeof v);
  };
  walk(report);
}

describe("authorityEvidence baseline and pass", () => {
  test("an unbound run is never a pass", async () => {
    const { w, src } = world();
    const r = await baseline(src, [ORG, ORG2]);
    expect(r.status).toBe("authority_evidence_unbound");
    for (const v of r.orgs) {
      expect(v.ok).toBe(false);
      expect(v.refusals).toEqual(["sql_unbound"]);
      expect(v.stop).toMatchObject({ ok: true, servingVersions: 1, deploymentsWalked: 3, stopDeployments: 2, effectiveStopMs: NOW - 5 * HOUR });
      expect(v.stop.stopDeploymentHash).toBe(evidenceHash("deployment", uuid(102)));
      expect(v.mirror.ok).toBe(true);
      expect(v.queueDrained).toMatchObject({ ok: true, backlogReads: [0, 0], dlqBacklogReads: [3, 3], dlqConsumers: 0, observedWindowMs: WINDOW });
      expect(v.sqlQuiet).toMatchObject({ ok: false, code: "sql_unbound", reads: 2, revisionsStable: true, ownershipStable: true,
        fingerprint: { revisionsCount: 120, revisionsDigest: D1, ownershipCount: 40, ownershipDigest: D2 } });
      expect(v.turnsNeon).toMatchObject({ ok: true, path: "neon", servingVersions: 1 });
    }
    const anchor = String(NOW - 5 * HOUR - AUTHORITY_CLOCK_SKEW_MS);
    // Round 2 counts rows written after round 1's own snapshot.
    const snap = "1000:1005:1001,1003";
    expect(w.sqlCalls).toEqual([[ORG, anchor, null], [ORG2, anchor, null], [ORG, anchor, snap], [ORG2, anchor, snap]]);
    expect(w.slept).toEqual([WINDOW, AUTHORITY_STATS_SETTLE_MS]);
    assertNoLeak(r);
  });

  test("a run bound to a clean baseline with nothing changed passes", async () => {
    const wd = world();
    const r = await bound(wd, [ORG, ORG2]);
    expect(r.status).toBe("authority_evidence_pass");
    for (const v of r.orgs) { expect(v.ok).toBe(true); expect(v.refusals).toEqual([]); expect(v.sqlQuiet.bound).toBe(true); }
    expect(r.orgs[0]!.orgHash).toBe(evidenceHash("org", ORG));
    expect(r.orgs[0]!.orgHash).not.toBe(r.orgs[1]!.orgHash);
    assertNoLeak(r);
  });

  test("default window is the default", async () => {
    const { w, src } = world();
    await authorityEvidence([ORG], src);
    expect(w.slept).toEqual([AUTHORITY_OBSERVATION_WINDOW_MS, AUTHORITY_STATS_SETTLE_MS]);
  });

  test("gradual deployment: every serving version is checked, a 0% version is not serving", async () => {
    const wd = world();
    wd.w.ingestDeployments[0]!.versions = [{ version_id: uuid(1), percentage: 60 }, { version_id: uuid(2), percentage: 40 }, { version_id: uuid(3), percentage: 0 }];
    const r = await baseline(wd.src);
    expect(r.status).toBe("authority_evidence_unbound");
    expect(r.orgs[0]!.stop.servingVersions).toBe(2);
  });

  test("the boundary may be a partially stopped deployment (one serving version without the org)", async () => {
    const wd = world();
    wd.w.ingestDeployments[2]!.versions = [{ version_id: uuid(2), percentage: 50 }, { version_id: uuid(3), percentage: 50 }];
    expect((await baseline(wd.src)).orgs[0]!.stop).toMatchObject({ ok: true, effectiveStopMs: NOW - 5 * HOUR });
  });

  test("a missing TURNS_WRITE_PATH on a verified version is the documented default (neon)", async () => {
    const { w, src } = world();
    w.rhDeployments[0]!.versions = [{ version_id: uuid(52), percentage: 100 }];
    expect((await baseline(src)).orgs[0]!.turnsNeon).toMatchObject({ ok: true, path: "neon" });
  });

  test("dual for other orgs only resolves neon; a secret TURNS_FORWARD_ORGS is irrelevant when the mode is neon", async () => {
    const a = world();
    a.w.versions.set(uuid(51), [plain("TURNS_WRITE_PATH", "dual"), plain("TURNS_FORWARD_ORGS", ` ${ORG2} ,x`)]);
    const ra = await baseline(a.src, [ORG, ORG2]);
    expect(ra.orgs[0]!.turnsNeon).toMatchObject({ ok: true, path: "neon" });
    expect(ra.orgs[1]!.turnsNeon).toMatchObject({ ok: false, path: "dual", code: "turns_path_not_neon" });
    const b = world();
    b.w.versions.set(uuid(51), [plain("TURNS_WRITE_PATH", "neon"), { name: "TURNS_FORWARD_ORGS", type: "secret_text" }]);
    expect((await baseline(b.src)).orgs[0]!.turnsNeon.ok).toBe(true);
  });

  test("the bound run's SQL counts rows written after the BASELINE snapshot in both rounds", async () => {
    const wd = world();
    wd.w.sqlDefault = { ...wd.w.sqlDefault, snapshot: "2000:2002:" };
    await bound(wd);
    expect(wd.w.sqlCalls.slice(-2).map((p) => p[2])).toEqual(["2000:2002:", "2000:2002:"]);
  });
});

describe("review f84452bc regressions", () => {
  test("1. a serving version producing to a different queue refuses even though the measured queue is empty", async () => {
    const wd = world();
    wd.w.versions.set(uuid(1), [...STOPPED().slice(0, 2), { name: "INGEST_ADMISSION", type: "queue", queue_name: "other-queue" }]);
    refusedWith(await bound(wd), "queue_binding_mismatch");
    // A version inside the stop window (not only the current one) is checked too.
    const w2 = world();
    w2.w.versions.set(uuid(2), STOPPED().slice(0, 2));
    refusedWith(await bound(w2), "queue_binding_mismatch");
  });

  test("1b. routing: consumer, its DLQ, producers, and queue identity across the window", async () => {
    const cons = (w: World["w"], c: unknown[]) => { w.mainQueue.consumers = c; w.mainQueue.consumers_total_count = c.length; };
    const prod = (w: World["w"], p: unknown[]) => { w.mainQueue.producers = p; w.mainQueue.producers_total_count = p.length; };
    for (const [code, mutate] of [
      ["queue_consumer_mismatch", (w) => cons(w, [{ ...ING_CONSUMER, script_name: "other" }])],
      ["queue_consumer_mismatch", (w) => cons(w, [{ ...ING_CONSUMER, dead_letter_queue: "other-dlq" }])],
      ["queue_consumer_mismatch", (w) => cons(w, [{ ...ING_CONSUMER, dead_letter_queue: "" }])],
      // Per the provider schema the consumer's Worker is script_name and its DLQ is top level, nothing else: a Worker
      // consumer without script_name is an unreadable identity.
      ["queue_inventory_incomplete", (w) => cons(w, [{ type: "worker", script: E.ingestWorker, dead_letter_queue: E.admissionDlq }])],
      ["queue_consumer_mismatch", (w) => cons(w, [{ type: "worker", script_name: E.ingestWorker, settings: { dead_letter_queue: E.admissionDlq } }])],
      ["queue_consumer_mismatch", (w) => cons(w, [ING_CONSUMER, { type: "http_pull", dead_letter_queue: "" }])],
      ["queue_producer_mismatch", (w) => prod(w, [ING_PRODUCER, { type: "worker", script: "intruder" }])],
      ["queue_inventory_incomplete", (w) => prod(w, [{ type: "worker", script_name: E.ingestWorker }])],
      ["queue_producer_mismatch", (w) => prod(w, [])],
      ["queue_identity_changed", (w) => { w.queuesAfter = [{ ...w.queues[0]!, queue_id: qid(7) }, ...w.queues.slice(1)]; }],
    ] as Array<[AuthorityRefusal, (w: ReturnType<typeof world>["w"]) => void]>) {
      const wd = world();
      mutate(wd.w);
      refusedWith(await baseline(wd.src), code);
    }
  });

  test("2. an unreadable version before the stop deployment refuses instead of moving the anchor later", async () => {
    const wd = world();
    wd.w.versions.set(uuid(3), "fail"); // the boundary cannot be read
    const r = await bound(wd, [ORG], (w) => { w.sqlRows = []; });
    refusedWith(r, "stop_boundary_unproven");
    expect(r.orgs[0]!.stop.effectiveStopMs).toBeNull();
    // ...so no SQL window was ever substituted (a modeled claim three hours ago could not hide).
    expect(r.orgs[0]!.refusals).toContain("sql_no_stop_anchor");
    const w2 = world();
    w2.w.versions.set(uuid(2), "fail");
    refusedWith(await baseline(w2.src), "stop_boundary_unproven");
    const w3 = world();
    w3.w.versions.set(uuid(3), [plain("INGEST_HTTP_ADMISSION_STOPPED_ORGS", "NOT-A-UUID")]); // malformed boundary
    refusedWith(await baseline(w3.src), "stop_boundary_unproven");
  });

  test("2b. exhausted or bounded history refuses: no boundary seen", async () => {
    const wd = world();
    wd.w.versions.set(uuid(3), STOPPED()); // every retained deployment carries the stop
    refusedWith(await baseline(wd.src), "stop_boundary_unproven");
    const w2 = world();
    w2.w.ingestDeployments = Array.from({ length: 30 }, (_, i) =>
      ({ id: uuid(300 + i), created_on: iso(NOW - (i + 1) * HOUR), versions: [{ version_id: uuid(2), percentage: 100 }] }));
    const r = await baseline(w2.src);
    refusedWith(r, "stop_boundary_unproven");
    expect(r.orgs[0]!.stop.deploymentsWalked).toBe(25);
  });

  test("3. the window is measured: a no-op sleep or a constant clock refuses", async () => {
    const wd = world();
    wd.w.sleepRate = 0;
    const r = await baseline(wd.src);
    refusedWith(r, "observation_window_short");
    expect(r.orgs[0]!.queueDrained).toMatchObject({ ok: false, code: "observation_window_short", observedWindowMs: 0 });
    expect(r.orgs[0]!.sqlQuiet.code).toBe("observation_window_short");
    const w2 = world();
    w2.w.sleepRate = 0.99;
    refusedWith(await baseline(w2.src), "observation_window_short");
  });

  test("4. rows hidden from the reader by RLS or privileges refuse (answered in the same statement); not READ ONLY refuses", async () => {
    const wd = world();
    wd.w.sqlDefault = { ...wd.w.sqlDefault, visible_tables: "2" };
    const r = await bound(wd);
    refusedWith(r, "sql_rows_not_visible");
    expect(r.orgs[0]!.sqlQuiet.reads).toBe(0);
    for (const tables of ["2", "4", null]) {
      const w2 = world();
      w2.w.sqlDefault = { ...w2.w.sqlDefault, tables };
      refusedWith(await baseline(w2.src), "sql_unreadable");
    }
    const w3 = world();
    w3.w.sqlDefault = { ...w3.w.sqlDefault, read_only: "off" };
    refusedWith(await baseline(w3.src), "sql_not_read_only");
  });
});

describe("review 5c067d90 regressions", () => {
  test("1. the last pre-stop deployment produced to a different queue: route change refuses", async () => {
    const wd = world();
    wd.w.versions.set(uuid(3), [plain("INGEST_S3_FENCE_MIRROR", "on"), { name: "INGEST_ADMISSION", type: "queue", queue_name: "former-queue" }]);
    refusedWith(await bound(wd), "queue_route_changed");
    // A boundary with no admission binding at all is an unproven route too.
    const w2 = world();
    w2.w.versions.set(uuid(3), [plain("INGEST_S3_FENCE_MIRROR", "on")]);
    refusedWith(await bound(w2), "queue_route_changed");
    // Another queue still consumed by the ingest Worker is a former route that may hold messages.
    const w3 = world();
    w3.w.queues.push({ queue_id: qid(5), queue_name: "former-queue", consumers: [ING_CONSUMER], consumers_total_count: 1, producers: [], producers_total_count: 0 });
    refusedWith(await bound(w3), "queue_route_changed");
  });

  test("2. an inventory disagreeing with the provider totals refuses", async () => {
    for (const mutate of [
      (w: World["w"]) => { w.dlqTotal = 1; },
      (w: World["w"]) => { w.mainQueue.consumers_total_count = 2; },
      (w: World["w"]) => { w.mainQueue.producers_total_count = 2; },
      (w: World["w"]) => { delete w.mainQueue.producers_total_count; },
      (w: World["w"]) => { w.queues[3]!.consumers_total_count = 2; },
      (w: World["w"]) => { delete w.queues[3]!.consumers; },
    ]) {
      const wd = world();
      mutate(wd.w);
      const r = await bound(wd);
      expect(r.status).toBe("authority_evidence_refused");
      expect(r.orgs[0]!.refusals.some((c) => c === "dlq_unreadable" || c === "queue_inventory_incomplete")).toBe(true);
    }
  });

  test("3. a row rewritten after the baseline snapshot refuses even with every count and digest equal", async () => {
    const r = await bound(world(), [ORG], (w) => { w.sqlDefault = { ...w.sqlDefault, ownership_new_since_base: "1" }; });
    refusedWith(r, "sql_rows_rewritten");
    for (const k of ["revisions_new_since_base", "batches_new_since_base"]) {
      refusedWith(await bound(world(), [ORG], (w) => { w.sqlDefault = { ...w.sqlDefault, [k]: "1" }; }), "sql_rows_rewritten");
    }
    // Inside the window too (unbound): round 2 against round 1's snapshot.
    const wd = world();
    wd.w.sqlRows = [{}, { revisions_new_since_base: "2" }];
    refusedWith(await baseline(wd.src), "sql_rows_rewritten");
  });

  test("3b. the baseline must be read in a snapshot transaction (the backfill's), after the stop, and clean", async () => {
    const wd = world();
    wd.w.baselineIsolation = "read committed";
    const b = await takeBaseline(wd, [ORG], await baseline(wd.src));
    expect(b.status).toBe("authority_baseline_refused");
    expect(b.orgs[0]!.refusals).toEqual(["baseline_not_snapshot"]);
    refusedWith(await authorityEvidence([ORG], wd.src, { observationWindowMs: WINDOW, bound: b }), "bound_not_ready");
    const w2 = world();
    const u2 = await baseline(w2.src);
    w2.w.sqlDefault = { ...w2.w.sqlDefault, batches_pending: "1" };
    expect((await takeBaseline(w2, [ORG], u2)).orgs[0]!.refusals).toEqual(["sql_batches_pending"]);
    const w3 = world();
    const u3 = await baseline(w3.src);
    w3.w.sqlDefault = { ...w3.w.sqlDefault, read_at_ms: String(NOW - 6 * HOUR) };
    expect((await takeBaseline(w3, [ORG], u3)).orgs[0]!.refusals).toEqual(["bound_stop_mismatch"]);
    // It takes only an unbound report whose every live check passed.
    const w4 = world();
    w4.w.backlog = [1];
    expect((await takeBaseline(w4, [ORG], await baseline(w4.src))).orgs[0]!.refusals).toEqual(["bound_invalid"]);
    const w5 = world();
    const u5 = await baseline(w5.src);
    expect((await takeBaseline(w5, [ORG], { ...u5, orgs: [{ ...u5.orgs[0]!, refusals: ["sql_unbound", "mirror_off"] }] })).orgs[0]!.refusals)
      .toEqual(["bound_not_ready"]);
    const b6 = await takeBaseline(w5, [ORG], u5);
    expect(b6.status).toBe("authority_baseline_ready");
    assertNoLeak(b6 as unknown as AuthorityEvidenceReport);
  });

  test("4. a changed digest or count since the baseline refuses", async () => {
    refusedWith(await bound(world(), [ORG], (w) => { w.sqlDefault = { ...w.sqlDefault, ownership_digest: "h" + "9".repeat(32) }; }), "sql_ownership_changed");
    refusedWith(await bound(world(), [ORG], (w) => { w.sqlDefault = { ...w.sqlDefault, ownership_count: "39" }; }), "sql_ownership_changed");
    refusedWith(await bound(world(), [ORG], (w) => { w.sqlDefault = { ...w.sqlDefault, revisions_digest: "h" + "9".repeat(32) }; }), "sql_revisions_changed");
  });
});

describe("review 949a2a62 regressions", () => {
  test("1. an admission committed and deleted inside the interval refuses: the deletion counter moved", async () => {
    // Unbound: between round 1's reference and the settled read.
    const wd = world();
    wd.w.stats = [{ ...wd.w.stats[0] as Record<string, unknown> }, { ...wd.w.stats[0] as Record<string, unknown>, batches_deleted: "8" }];
    refusedWith(await baseline(wd.src), "sql_batches_deleted");
    // Bound: between the baseline's counters and the bound run's settled read, every surviving count unchanged.
    const r = await bound(world(), [ORG], (w) => { w.stats = [{ ...w.stats[0] as Record<string, unknown>, batches_deleted: "9" }]; });
    refusedWith(r, "sql_batches_deleted");
    expect(r.orgs[0]!.sqlQuiet.counts).toMatchObject({ batchesClaimedSince: 0, batchesPending: 0 });
  });

  test("1b. a statistics reset, a restart, a falling counter, track_counts off or an unreadable read refuses", async () => {
    const s0 = { tables: "1", batches_deleted: "7", stats_reset_ms: "0", started_ms: "1700000000000", track_counts: "on" };
    for (const [code, later] of [
      ["sql_stats_reset", { ...s0, stats_reset_ms: String(NOW) }],
      ["sql_stats_reset", { ...s0, started_ms: String(NOW) }],
      ["sql_stats_reset", { ...s0, batches_deleted: "0" }],
      ["sql_stats_unreadable", { ...s0, track_counts: "off" }],
      ["sql_stats_unreadable", { ...s0, tables: "0", batches_deleted: null }],
      ["sql_stats_unreadable", "fail"],
    ] as Array<[AuthorityRefusal, Record<string, unknown> | "fail"]>) {
      const wd = world();
      wd.w.stats = [s0, later];
      refusedWith(await baseline(wd.src), code);
    }
    // The settle wait is measured, like the window: an honest window and a settle sleep that does not advance the clock.
    const w2 = world();
    const sleep = w2.src.sleep;
    w2.src.sleep = async (ms) => (ms === AUTHORITY_STATS_SETTLE_MS ? undefined : sleep(ms));
    const r = await baseline(w2.src);
    refusedWith(r, "observation_window_short");
    expect(r.orgs[0]!.queueDrained.ok).toBe(true); // the window itself was honest
    expect(r.orgs[0]!.sqlQuiet.code).toBe("observation_window_short");
  });

  test("1c. the marker handoff needs a fresh pass for that org", async () => {
    const wd = world();
    const r = await bound(wd, [ORG, ORG2]);
    expect(r.status).toBe("authority_evidence_pass");
    expect(authorityMarkerAllowed(r, ORG, r.finishedAtMs)).toBe(true);
    expect(authorityMarkerAllowed(r, ORG2, r.finishedAtMs + AUTHORITY_MARKER_MAX_AGE_MS)).toBe(true);
    expect(authorityMarkerAllowed(r, ORG, r.finishedAtMs + AUTHORITY_MARKER_MAX_AGE_MS + 1)).toBe(false);
    expect(authorityMarkerAllowed(r, ORG, r.finishedAtMs - 1)).toBe(false);
    expect(authorityMarkerAllowed(r, "99999999-0000-4000-8000-000000000000", r.finishedAtMs)).toBe(false);
    expect(authorityMarkerAllowed({ ...r, status: "authority_evidence_unbound" }, ORG, r.finishedAtMs)).toBe(false);
    expect(authorityMarkerAllowed(await baseline(world().src), ORG, NOW + HOUR)).toBe(false);
  });

  test("3. a consumer or producer whose identity cannot be read refuses, on any queue", async () => {
    for (const mutate of [
      (w: World["w"]) => { w.queues[3]!.consumers = [{ type: "worker" }]; },
      (w: World["w"]) => { w.queues[3]!.consumers = [{ type: "worker", script_name: "" }]; },
      (w: World["w"]) => { w.queues[3]!.consumers = [{ type: "mystery", script_name: E.ingestWorker }]; },
      (w: World["w"]) => { w.queues[3]!.producers = [{ type: "worker" }]; w.queues[3]!.producers_total_count = 1; },
      (w: World["w"]) => { w.queues[3]!.producers = [{ type: "r2_bucket" }]; w.queues[3]!.producers_total_count = 1; },
    ]) {
      const wd = world();
      mutate(wd.w);
      refusedWith(await bound(wd), "queue_inventory_incomplete");
    }
  });

  test("3b. the list is complete only by the provider's pagination metadata", async () => {
    const n = 4;
    for (const info of [undefined, null, {}, { total_count: n + 1, total_pages: 1 }, { total_count: n, total_pages: 2 }, { total_count: n, page: 2 },
      { count: n, total_pages: 1 }]) {
      const wd = world();
      wd.w.listInfo = info === null ? "x" : info ?? { total_count: undefined };
      refusedWith(await baseline(wd.src), "queue_list_truncated");
    }
    const ok = world();
    ok.w.listInfo = { total_count: n };
    expect((await baseline(ok.src)).status).toBe("authority_evidence_unbound");
  });

  test("an unsuccessful API body is never read as a result", async () => {
    const wd = world();
    const inner = wd.src.cfGet;
    wd.src.cfGet = async (path) => {
      const b = await inner(path) as Record<string, unknown>;
      return path.endsWith("/metrics") ? { ...b, success: false } : b;
    };
    refusedWith(await baseline(wd.src), "queue_metrics_unreadable");
  });
});

describe("bound baseline refusals", () => {
  const org0 = (b: AuthorityBaseline) => b.orgs[0]!;
  test.each([
    ["bound_invalid", () => ({})],
    ["bound_invalid", (b: AuthorityBaseline) => ({ ...b, kind: "authority_evidence" })],
    ["bound_invalid", (b: AuthorityBaseline) => ({ ...b, orgs: [] })],
    ["bound_invalid", (b: AuthorityBaseline) => ({ ...b, orgs: [{ ...org0(b), fingerprint: null }] })],
    ["bound_invalid", (b: AuthorityBaseline) => ({ ...b, orgs: [{ ...org0(b), snapshot: { xmin: 5, xmax: 3, xip: [] } }] })],
    ["bound_invalid", (b: AuthorityBaseline) => ({ ...b, orgs: [{ ...org0(b), fingerprint: { ...org0(b).fingerprint!, ownershipDigest: "x" } }] })],
    ["bound_invalid", (b: AuthorityBaseline) => ({ ...b, orgs: [org0(b), org0(b)] })],
    ["bound_invalid", (b: AuthorityBaseline) => ({ ...b, orgs: [{ ...org0(b), stats: null }] })],
    ["bound_not_ready", (b: AuthorityBaseline) => ({ ...b, status: "authority_baseline_refused" })],
    ["bound_not_ready", (b: AuthorityBaseline) => ({ ...b, orgs: [{ ...org0(b), ok: false }] })],
    ["bound_not_ready", (b: AuthorityBaseline) => ({ ...b, orgs: [{ ...org0(b), readAtMs: NOW + 10 * HOUR }] })],
    ["bound_stop_mismatch", (b: AuthorityBaseline) => ({ ...b, orgs: [{ ...org0(b), stopDeploymentHash: "h_" + "0".repeat(32) }] })],
    ["bound_stop_mismatch", (b: AuthorityBaseline) => ({ ...b, orgs: [{ ...org0(b), effectiveStopMs: NOW - 6 * HOUR }] })],
    ["bound_stop_mismatch", (b: AuthorityBaseline) => ({ ...b, orgs: [{ ...org0(b), readAtMs: NOW - 6 * HOUR }] })],
  ] as Array<[AuthorityRefusal, (b: AuthorityBaseline) => unknown]>)("%s %#", async (code, edit) => {
    refusedWith(await bound(world(), [ORG], undefined, edit), code);
  });

  test("an unbound evidence report is not a baseline; a baseline for another org does not bind", async () => {
    const wd = world();
    refusedWith(await authorityEvidence([ORG], wd.src, { observationWindowMs: WINDOW, bound: await baseline(wd.src) }), "bound_invalid");
    const w2 = world();
    const b = await takeBaseline(w2, [ORG2], await baseline(w2.src, [ORG2]));
    refusedWith(await authorityEvidence([ORG], w2.src, { observationWindowMs: WINDOW, bound: b }), "bound_invalid");
  });

  test("a stop re-established after the baseline (new boundary) does not bind", async () => {
    refusedWith(await bound(world(), [ORG], (w) => {
      w.ingestDeployments.unshift({ id: uuid(100), created_on: iso(NOW), versions: [{ version_id: uuid(1), percentage: 100 }] });
      w.ingestDeployments[1] = { ...w.ingestDeployments[1]!, versions: [{ version_id: uuid(3), percentage: 100 }] };
    }), "bound_stop_mismatch");
  });
});

describe("resolveTurnsWritePath mirrors relayhistory-cloud turnsWritePath", () => {
  test.each([
    [undefined, undefined, "neon"], ["", undefined, "neon"], ["neon", undefined, "neon"], ["DUAL", undefined, "neon"],
    ["garbage", ORG, "neon"], [" dual ", undefined, "dual"], ["dual", "", "dual"], ["dual", ` ${ORG} `, "dual"],
    ["forward", ORG2, "neon"], ["forward", `${ORG2},${ORG}`, "forward"], ["forward", ",,", "forward"],
  ] as const)("%p / %p", (mode, orgs, want) => {
    expect(resolveTurnsWritePath(mode, orgs, ORG)).toBe(want);
  });
});

describe("authorityEvidence refusals", () => {
  type W = ReturnType<typeof world>["w"];
  type Case = [AuthorityRefusal, (w: W) => void];
  const cases: Case[] = [
    // stop / mirror
    ["stop_deployments_unreadable", (w) => w.fail.add(`/workers/scripts/${E.ingestWorker}/deployments`)],
    ["stop_deployment_invalid", (w) => { w.ingestDeployments = []; }],
    ["stop_deployment_invalid", (w) => { w.ingestDeployments[0]!.versions = [{ version_id: uuid(1), percentage: 90 }]; }],
    ["stop_deployment_invalid", (w) => { w.ingestDeployments[0]!.id = "not-a-uuid"; }],
    ["stop_deployment_invalid", (w) => { w.ingestDeployments[0]!.created_on = iso(NOW + HOUR); w.ingestDeployments.splice(1); }],
    ["stop_deployment_order", (w) => { w.ingestDeployments[1]!.created_on = iso(NOW - 0.5 * HOUR); }],
    ["stop_version_unreadable", (w) => w.versions.set(uuid(1), "fail")],
    ["stop_version_unreadable", (w) => w.versions.set(uuid(1), [plain("A", "1"), plain("A", "2")])],
    ["stop_cutover_unreadable", (w) => w.versions.set(uuid(1), [plain("INGEST_HTTP_ADMISSION_STOPPED_ORGS", `${ORG},`), plain("INGEST_S3_FENCE_MIRROR", "on"), Q])],
    ["stop_cutover_unreadable", (w) => w.versions.set(uuid(1), [{ name: "INGEST_HTTP_ADMISSION_STOPPED_ORGS", type: "secret_text" }])],
    ["stop_org_not_listed", (w) => w.versions.set(uuid(1), STOPPED(ORG2))],
    ["stop_org_not_listed", (w) => w.versions.set(uuid(1), [plain("INGEST_S3_FENCE_MIRROR", "on"), Q])],
    ["stop_org_not_listed", (w) => { w.ingestDeployments[0]!.versions = [{ version_id: uuid(1), percentage: 99 }, { version_id: uuid(3), percentage: 1 }]; }],
    ["stop_boundary_unproven", (w) => w.versions.set(uuid(3), "fail")],
    ["mirror_off", (w) => w.versions.set(uuid(1), [plain("INGEST_HTTP_ADMISSION_STOPPED_ORGS", ORG), Q])],
    ["mirror_off", (w) => { w.ingestDeployments[0]!.versions = [{ version_id: uuid(1), percentage: 50 }, { version_id: uuid(4), percentage: 50 }];
      w.versions.set(uuid(4), [plain("INGEST_HTTP_ADMISSION_STOPPED_ORGS", ORG), Q]); }],
    ["stop_deployment_changed", (w) => { w.ingestAfter = [{ ...w.ingestDeployments[0]!, id: uuid(109), created_on: iso(NOW) }]; }],
    ["stop_deployment_changed", (w) => { w.ingestAfter = [{ ...w.ingestDeployments[0]!, versions: [{ version_id: uuid(2), percentage: 100 }] }]; }],
    // queue
    ["queue_list_unreadable", (w) => w.fail.add(`/queues?page=1&per_page=${AUTHORITY_QUEUE_PAGE}`)],
    ["queue_list_unreadable", (w) => { w.queues = { queues: [] } as unknown as Array<Record<string, unknown>>; }],
    ["queue_list_unreadable", (w) => { w.queues[0]!.queue_id = "x"; }],
    ["queue_list_unreadable", (w) => w.fail.add(`/queues/${qid(1)}`)],
    ["queue_list_truncated", (w) => { w.queues = [...w.queues, ...Array.from({ length: AUTHORITY_QUEUE_PAGE }, (_, i) => ({ queue_id: qid(6), queue_name: `q${i}`, consumers: [], consumers_total_count: 0, producers: [], producers_total_count: 0 }))]; }],
    ["queue_missing", (w) => { w.queues.splice(0, 1); }],
    ["queue_missing", (w) => { w.queues.splice(1, 1); }],
    ["queue_missing", (w) => { w.queues.push({ ...w.queues[0]!, queue_id: qid(5) }); }],
    ["queue_identity_changed", (w) => { w.queuesAfter = [{ ...w.queues[0]!, queue_id: qid(7) }, ...w.queues.slice(1)]; }],
    ["queue_binding_mismatch", (w) => w.versions.set(uuid(1), STOPPED().slice(0, 2))],
    ["queue_consumer_mismatch", (w) => { w.mainQueue.consumers = []; w.mainQueue.consumers_total_count = 0; }],
    ["queue_route_changed", (w) => w.versions.set(uuid(3), [plain("INGEST_S3_FENCE_MIRROR", "on")])],
    ["queue_inventory_incomplete", (w) => { w.mainQueue.consumers_total_count = 3; }],
    ["queue_producer_mismatch", (w) => { w.mainQueue.producers = [ING_PRODUCER, { type: "r2_bucket", bucket_name: "b" }]; w.mainQueue.producers_total_count = 2; }],
    ["queue_metrics_unreadable", (w) => w.fail.add(`/queues/${qid(1)}/metrics`)],
    ["queue_metrics_unreadable", (w) => { w.backlog = [undefined, 0]; }],
    ["queue_metrics_unreadable", (w) => { w.backlog = [0, -1]; }],
    ["queue_metrics_unreadable", (w) => { w.backlog = [0, 0.5]; }],
    ["queue_backlog_nonzero", (w) => { w.backlog = [1, 0]; }],
    ["queue_backlog_nonzero", (w) => { w.backlog = [0, "2"]; }],
    ["dlq_unreadable", (w) => w.fail.add(`/queues/${qid(2)}`)],
    ["dlq_unreadable", (w) => w.fail.add(`/queues/${qid(2)}/metrics`)],
    ["dlq_unreadable", (w) => { w.dlqConsumers = [undefined as unknown as unknown[], []]; }],
    ["dlq_consumer_attached", (w) => { w.dlqConsumers = [[], [{ type: "http_pull" }]]; }],
    ["dlq_backlog_decreased", (w) => { w.dlqBacklog = [3, 2]; }],
    ["observation_window_short", (w) => { w.sleepRate = 0.5; }],
    // sql
    ["sql_unreadable", (w) => { w.sqlRows = ["fail"]; }],
    ["sql_unreadable", (w) => { w.sqlRows = [{}, "bad"]; }],
    ["sql_unreadable", (w) => { w.sqlRows = [{ revisions_digest: null }]; }],
    ["sql_unreadable", (w) => { w.sqlRows = [{ snapshot: "5:3:" }]; }],
    ["sql_unreadable", (w) => { w.sqlRows = [{ snapshot: "1:9:4,4" }]; }],
    ["sql_rows_not_visible", (w) => { w.sqlRows = [{ visible_tables: "2" }]; }],
    ["sql_not_read_only", (w) => { w.sqlRows = [{}, { read_only: "off" }]; }],
    ["sql_rows_rewritten", (w) => { w.sqlRows = [{}, { ownership_new_since_base: "1" }]; }],
    ["sql_batches_claimed_after_stop", (w) => { w.sqlRows = [{}, { batches_claimed_since: "1" }]; }],
    ["sql_batches_committed_after_stop", (w) => { w.sqlRows = [{ batches_committed_since: 2 }]; }],
    ["sql_batches_pending", (w) => { w.sqlRows = [{ batches_pending: "1" }]; }],
    ["sql_revisions_after_stop", (w) => { w.sqlRows = [{ revisions_updated_since: "4" }]; }],
    ["sql_revisions_changed", (w) => { w.sqlRows = [{}, { revisions_digest: D2 }]; }],
    ["sql_revisions_changed", (w) => { w.sqlRows = [{}, { revisions_count: "121" }]; }],
    ["sql_ownership_changed", (w) => { w.sqlRows = [{}, { ownership_count: "41" }]; }],
    ["sql_ownership_changed", (w) => { w.sqlRows = [{}, { ownership_digest: D1 }]; }],
    ["sql_no_stop_anchor", (w) => w.versions.set(uuid(1), [plain("INGEST_S3_FENCE_MIRROR", "on"), Q])],
    // turns
    ["turns_deployments_unreadable", (w) => w.fail.add(`/workers/scripts/${E.relayhistoryWorker}/deployments`)],
    ["turns_deployment_invalid", (w) => { w.rhDeployments = []; }],
    ["turns_deployment_invalid", (w) => { w.rhDeployments[0]!.versions = [{ version_id: uuid(51), percentage: "100" }]; }],
    ["turns_version_unreadable", (w) => w.versions.set(uuid(51), "fail")],
    ["turns_path_unknown", (w) => w.versions.set(uuid(51), [{ name: "TURNS_WRITE_PATH", type: "secret_text" }])],
    ["turns_path_unknown", (w) => w.versions.set(uuid(51), [plain("TURNS_WRITE_PATH", "forward"), { name: "TURNS_FORWARD_ORGS", type: "secret_text" }])],
    ["turns_path_unknown", (w) => w.versions.set(uuid(51), [{ name: "TURNS_WRITE_PATH", type: "plain_text", text: 1 }])],
    ["turns_path_not_neon", (w) => w.versions.set(uuid(51), [plain("TURNS_WRITE_PATH", "dual")])],
    ["turns_path_not_neon", (w) => w.versions.set(uuid(51), [plain("TURNS_WRITE_PATH", "forward"), plain("TURNS_FORWARD_ORGS", ORG)])],
    ["turns_path_not_neon", (w) => {
      w.rhDeployments[0]!.versions = [{ version_id: uuid(51), percentage: 90 }, { version_id: uuid(53), percentage: 10 }];
      w.versions.set(uuid(53), [plain("TURNS_WRITE_PATH", "forward")]);
    }],
    ["turns_deployment_changed", (w) => { w.rhAfter = [{ ...w.rhDeployments[0]!, id: uuid(209) }]; }],
    ["turns_deployment_changed", (w) => { w.rhAfter = "fail"; }],
  ];

  test.each(cases.map((c, i) => [`${i}:${c[0]}`, c] as const))("%s (bound run)", async (_name, [code, mutate]) => {
    // Mutated before the baseline, so the baseline refuses and the bound run refuses with the case's own code.
    const wd = world();
    mutate(wd.w);
    const r = await authorityEvidence([ORG], wd.src, { observationWindowMs: WINDOW, bound: await (async () => {
      const ok = world();
      return JSON.parse(JSON.stringify(await takeBaseline(ok, [ORG], await baseline(ok.src))));
    })() });
    refusedWith(r, code);
    assertNoLeak(r);
  });

  test("every declared refusal code has a test", () => {
    const tested = new Set<string>(cases.map((c) => c[0]));
    for (const c of ["bound_invalid", "bound_not_ready", "bound_stop_mismatch", "sql_unbound", "baseline_not_snapshot", "sql_stats_unreadable",
      "sql_stats_reset", "sql_batches_deleted"]) tested.add(c);
    expect(AUTHORITY_REFUSALS.filter((c) => !tested.has(c))).toEqual([]);
  });

  test("one org refusing refuses the report; the other org's verdict stays ok", async () => {
    const wd = world();
    const r = await bound(wd, [ORG, ORG2], (w) => w.versions.set(uuid(1), STOPPED(ORG)));
    expect(r.status).toBe("authority_evidence_refused");
    expect(r.orgs[0]!.ok).toBe(true);
    expect(r.orgs[1]!.refusals).toEqual(["stop_org_not_listed", "sql_no_stop_anchor"]);
  });

  test("a baseline with any other refusal is refused, not baseline", async () => {
    const wd = world();
    wd.w.backlog = [1];
    expect((await baseline(wd.src)).status).toBe("authority_evidence_refused");
  });

  test("an unreadable SQL round is not retried into a pass", async () => {
    const { w, src } = world();
    w.sqlRows = ["fail", {}];
    const r = await baseline(src);
    expect(r.orgs[0]!.sqlQuiet).toMatchObject({ ok: false, code: "sql_unreadable", reads: 0 });
    expect(w.sqlCalls).toHaveLength(1);
  });
});

describe("authorityEvidence inputs", () => {
  test.each([
    [[]], [["ABCDEF00-0000-4000-8000-000000000000"]], [[ORG, ORG]], [[`${ORG},${ORG2}`]], [[1]],
    [Array.from({ length: 65 }, (_, i) => uuid(i))],
  ])("orgs %#", async (orgs) => {
    const { src } = world();
    await expect(authorityEvidence(orgs as unknown[], src)).rejects.toMatchObject({ code: "authority_orgs" });
  });
  test.each([[0], [AUTHORITY_MIN_OBSERVATION_WINDOW_MS - 1], [31 * 60_000], [1.5e4 + 0.5]])("window %p", async (ms) => {
    const { src } = world();
    await expect(authorityEvidence([ORG], src, { observationWindowMs: ms })).rejects.toMatchObject({ code: "authority_window" });
  });
});

describe("AUTHORITY_SQL", () => {
  test("is read-only, org-leading and takes no other value", () => {
    expect(AUTHORITY_SQL).not.toMatch(/\b(INSERT|UPDATE|DELETE|TRUNCATE|ALTER|CREATE|DROP|GRANT|COPY|LOCK|nextval|setval)\b/iu);
    expect(new Set(AUTHORITY_SQL.match(/\$\d/gu))).toEqual(new Set(["$1", "$2", "$3"]));
    // Visibility is a CTE of the same statement, never a separate query.
    expect(AUTHORITY_SQL).toContain("v AS (");
    expect(AUTHORITY_SQL).toContain("pg_has_role(current_user, c.relowner, 'USAGE')");
    const scans = [...AUTHORITY_SQL.matchAll(/FROM (sessions\.\w+) x(?: CROSS JOIN s)? WHERE x\.org_id = \$1::text/gu)].map((m) => m[1]);
    expect(scans).toEqual(["sessions.ingest_batches", "sessions.ingest_record_revisions", "sessions.ingest_session_ownership"]);
    expect((AUTHORITY_SQL.match(/FROM sessions\./gu) ?? []).length).toBe(3);
  });
});
