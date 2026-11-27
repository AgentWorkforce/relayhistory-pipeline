import { describe, expect, test } from "bun:test";
import {
  AUTHORITY_CLOCK_SKEW_MS,
  AUTHORITY_EVIDENCE as E,
  AUTHORITY_MIN_OBSERVATION_WINDOW_MS,
  AUTHORITY_OBSERVATION_WINDOW_MS,
  AUTHORITY_QUEUE_PAGE,
  AUTHORITY_REFUSALS,
  AUTHORITY_SQL,
  authorityEvidence,
  evidenceHash,
  resolveTurnsWritePath,
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
      { queue_id: qid(1), queue_name: E.admissionQueue },
      { queue_id: qid(2), queue_name: E.admissionDlq },
      { queue_id: qid(3), queue_name: "something-else" },
    ] as Array<Record<string, unknown>>,
    /** Replaces the queue list from the second listing on. */
    queuesAfter: null as Array<Record<string, unknown>> | null,
    mainQueue: {
      consumers: [{ type: "worker", script: E.ingestWorker, dead_letter_queue: E.admissionDlq }],
      producers: [{ type: "worker", script: E.ingestWorker }],
    } as Record<string, unknown>,
    backlog: [0] as unknown[],
    dlqBacklog: [3] as unknown[],
    dlqConsumers: [[]] as unknown[][],
    visibility: [
      { table_name: "ingest_batches", visible: true },
      { table_name: "ingest_record_revisions", visible: true },
      { table_name: "ingest_session_ownership", visible: true },
    ] as unknown,
    sqlRows: [] as Array<Record<string, unknown> | "fail" | "bad">,
    sqlDefault: {
      batches_claimed_since: "0", batches_committed_since: "0", batches_pending: "0", revisions_updated_since: "0",
      revisions_count: "120", revisions_hash: "987654321", ownership_count: "40", ownership_hash: "123456789",
    } as Record<string, unknown>,
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
  let ingestReads = 0, rhReads = 0, metricsReads = 0, dlqMetricsReads = 0, dlqReads = 0, listReads = 0, sqlIdx = 0;
  const src: AuthorityEvidenceSources = {
    async cfGet(path) {
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
      if (path === `/queues/${qid(2)}`) return { queue_id: qid(2), queue_name: E.admissionDlq, consumers: at(w.dlqConsumers, dlqReads++) };
      const main = ids.find((id) => path === `/queues/${id}`);
      if (main) return { queue_id: main, queue_name: E.admissionQueue, ...w.mainQueue };
      throw new Error("unexpected " + path);
    },
    async sql(text, params) {
      if (text !== AUTHORITY_SQL) return w.visibility as Record<string, unknown>[];
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
/** A baseline, then a run bound to it (the only path to a pass). `between` mutates the world after the baseline. */
async function bound(world: ReturnType<typeof world>, orgs: unknown[] = [ORG], between?: (w: ReturnType<typeof world>["w"]) => void,
  boundOverride?: (b: AuthorityEvidenceReport) => unknown) {
  const b = await baseline(world.src, orgs);
  world.w.clock += 60_000;
  between?.(world.w);
  return authorityEvidence(orgs, world.src, { observationWindowMs: WINDOW, bound: boundOverride ? boundOverride(b) : JSON.parse(JSON.stringify(b)) });
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
  test("an unbound run is a baseline, never a pass", async () => {
    const { w, src } = world();
    const r = await baseline(src, [ORG, ORG2]);
    expect(r.status).toBe("authority_evidence_baseline");
    for (const v of r.orgs) {
      expect(v.ok).toBe(false);
      expect(v.refusals).toEqual(["sql_unbound"]);
      expect(v.stop).toMatchObject({ ok: true, servingVersions: 1, deploymentsWalked: 3, stopDeployments: 2, effectiveStopMs: NOW - 5 * HOUR });
      expect(v.stop.stopDeploymentHash).toBe(evidenceHash("deployment", uuid(102)));
      expect(v.mirror.ok).toBe(true);
      expect(v.queueDrained).toMatchObject({ ok: true, backlogReads: [0, 0], dlqBacklogReads: [3, 3], dlqConsumers: 0, observedWindowMs: WINDOW });
      expect(v.sqlQuiet).toMatchObject({ ok: false, code: "sql_unbound", reads: 2, revisionsStable: true, ownershipStable: true,
        fingerprint: { revisionsCount: 120, revisionsHash: 987654321, ownershipCount: 40, ownershipHash: 123456789 } });
      expect(v.turnsNeon).toMatchObject({ ok: true, path: "neon", servingVersions: 1 });
    }
    const anchor = String(NOW - 5 * HOUR - AUTHORITY_CLOCK_SKEW_MS);
    expect(w.sqlCalls).toEqual([[ORG, anchor], [ORG2, anchor], [ORG, anchor], [ORG2, anchor]]);
    expect(w.slept).toEqual([WINDOW]);
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
    expect(w.slept).toEqual([AUTHORITY_OBSERVATION_WINDOW_MS]);
  });

  test("gradual deployment: every serving version is checked, a 0% version is not serving", async () => {
    const wd = world();
    wd.w.ingestDeployments[0]!.versions = [{ version_id: uuid(1), percentage: 60 }, { version_id: uuid(2), percentage: 40 }, { version_id: uuid(3), percentage: 0 }];
    const r = await baseline(wd.src);
    expect(r.status).toBe("authority_evidence_baseline");
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

  test("consumer and producer script may be named by script_name; the DLQ by settings.dead_letter_queue", async () => {
    const wd = world();
    wd.w.mainQueue = { consumers: [{ type: "worker", script_name: E.ingestWorker, settings: { dead_letter_queue: E.admissionDlq } }],
      producers: [{ type: "worker", script_name: E.ingestWorker }] };
    expect((await baseline(wd.src)).status).toBe("authority_evidence_baseline");
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
    for (const [code, mutate] of [
      ["queue_consumer_mismatch", (w) => { w.mainQueue.consumers = [{ type: "worker", script: "other", dead_letter_queue: E.admissionDlq }]; }],
      ["queue_consumer_mismatch", (w) => { w.mainQueue.consumers = [{ type: "worker", script: E.ingestWorker, dead_letter_queue: "other-dlq" }]; }],
      ["queue_consumer_mismatch", (w) => { w.mainQueue.consumers = [{ type: "worker", script: E.ingestWorker }]; }],
      ["queue_consumer_mismatch", (w) => { w.mainQueue.consumers = [...(w.mainQueue.consumers as unknown[]), { type: "http_pull" }]; }],
      ["queue_consumer_mismatch", (w) => { w.mainQueue.consumers = [{ type: "worker", script: E.ingestWorker, script_name: "other", dead_letter_queue: E.admissionDlq }]; }],
      ["queue_producer_mismatch", (w) => { w.mainQueue.producers = [{ type: "worker", script: E.ingestWorker }, { type: "worker", script: "intruder" }]; }],
      ["queue_producer_mismatch", (w) => { w.mainQueue.producers = []; }],
      ["queue_identity_changed", (w) => { w.queuesAfter = [{ queue_id: qid(7), queue_name: E.admissionQueue }, w.queues[1]!]; }],
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

  test("4. rows hidden from the reader by RLS or privileges refuse; a malformed visibility answer refuses", async () => {
    const wd = world();
    wd.w.visibility = [{ table_name: "ingest_batches", visible: false }, { table_name: "ingest_record_revisions", visible: true },
      { table_name: "ingest_session_ownership", visible: true }];
    const r = await bound(wd);
    refusedWith(r, "sql_rows_not_visible");
    expect(r.orgs[0]!.sqlQuiet.reads).toBe(0);
    for (const vis of [[], [{ table_name: "ingest_batches", visible: true }], null,
      [{ table_name: "ingest_batches", visible: true }, { table_name: "ingest_batches", visible: true }, { table_name: "ingest_session_ownership", visible: true }]]) {
      const w2 = world();
      w2.w.visibility = vis;
      refusedWith(await baseline(w2.src), "sql_unreadable");
    }
  });

  test("4b. ownership continuity needs a bound baseline; a change since it refuses", async () => {
    refusedWith(await bound(world(), [ORG], (w) => { w.sqlDefault = { ...w.sqlDefault, ownership_hash: "5" }; }), "sql_ownership_changed");
    refusedWith(await bound(world(), [ORG], (w) => { w.sqlDefault = { ...w.sqlDefault, ownership_count: "39" }; }), "sql_ownership_changed");
    refusedWith(await bound(world(), [ORG], (w) => { w.sqlDefault = { ...w.sqlDefault, revisions_hash: "5" }; }), "sql_revisions_changed");
  });
});

describe("bound baseline refusals", () => {
  test.each([
    ["bound_invalid", () => ({})],
    ["bound_invalid", (b: AuthorityEvidenceReport) => ({ ...b, status: "authority_evidence_refused" })],
    ["bound_invalid", (b: AuthorityEvidenceReport) => ({ ...b, orgs: [] })],
    ["bound_invalid", (b: AuthorityEvidenceReport) => ({ ...b, orgs: [{ ...b.orgs[0]!, sqlQuiet: { ...b.orgs[0]!.sqlQuiet, fingerprint: null } }] })],
    ["bound_invalid", (b: AuthorityEvidenceReport) => ({ ...b, orgs: [b.orgs[0], b.orgs[0]] })],
    ["bound_not_ready", (b: AuthorityEvidenceReport) => ({ ...b, orgs: [{ ...b.orgs[0]!, refusals: ["sql_unbound", "mirror_off"] }] })],
    ["bound_not_ready", (b: AuthorityEvidenceReport) => ({ ...b, finishedAtMs: NOW + 10 * HOUR })],
    ["bound_stop_mismatch", (b: AuthorityEvidenceReport) => ({ ...b, orgs: [{ ...b.orgs[0]!, stop: { ...b.orgs[0]!.stop, stopDeploymentHash: "h_" + "0".repeat(32) } }] })],
    ["bound_stop_mismatch", (b: AuthorityEvidenceReport) => ({ ...b, orgs: [{ ...b.orgs[0]!, stop: { ...b.orgs[0]!.stop, effectiveStopMs: NOW - 6 * HOUR } }] })],
    ["bound_stop_mismatch", (b: AuthorityEvidenceReport) => ({ ...b, startedAtMs: NOW - 6 * HOUR })],
  ] as Array<[AuthorityRefusal, (b: AuthorityEvidenceReport) => unknown]>)("%s %#", async (code, edit) => {
    refusedWith(await bound(world(), [ORG], undefined, edit), code);
  });

  test("a baseline for another org does not bind", async () => {
    const wd = world();
    const b = await baseline(wd.src, [ORG2]);
    refusedWith(await authorityEvidence([ORG], wd.src, { observationWindowMs: WINDOW, bound: b }), "bound_invalid");
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
    ["queue_list_truncated", (w) => { w.queues = [...w.queues, ...Array.from({ length: AUTHORITY_QUEUE_PAGE }, (_, i) => ({ queue_id: qid(4), queue_name: `q${i}` }))]; }],
    ["queue_missing", (w) => { w.queues.splice(0, 1); }],
    ["queue_missing", (w) => { w.queues.splice(1, 1); }],
    ["queue_missing", (w) => { w.queues.push({ queue_id: qid(5), queue_name: E.admissionQueue }); }],
    ["queue_identity_changed", (w) => { w.queuesAfter = [{ queue_id: qid(7), queue_name: E.admissionQueue }, w.queues[1]!]; }],
    ["queue_binding_mismatch", (w) => w.versions.set(uuid(1), STOPPED().slice(0, 2))],
    ["queue_consumer_mismatch", (w) => { w.mainQueue.consumers = []; }],
    ["queue_producer_mismatch", (w) => { delete w.mainQueue.producers; }],
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
    ["sql_unreadable", (w) => { w.sqlRows = [{ revisions_hash: null }]; }],
    ["sql_rows_not_visible", (w) => { w.visibility = (w.visibility as Array<Record<string, unknown>>).map((r) => ({ ...r, visible: r.table_name !== "ingest_session_ownership" })); }],
    ["sql_batches_claimed_after_stop", (w) => { w.sqlRows = [{}, { batches_claimed_since: "1" }]; }],
    ["sql_batches_committed_after_stop", (w) => { w.sqlRows = [{ batches_committed_since: 2 }]; }],
    ["sql_batches_pending", (w) => { w.sqlRows = [{ batches_pending: "1" }]; }],
    ["sql_revisions_after_stop", (w) => { w.sqlRows = [{ revisions_updated_since: "4" }]; }],
    ["sql_revisions_changed", (w) => { w.sqlRows = [{}, { revisions_hash: "1" }]; }],
    ["sql_revisions_changed", (w) => { w.sqlRows = [{}, { revisions_count: "121" }]; }],
    ["sql_ownership_changed", (w) => { w.sqlRows = [{}, { ownership_count: "41" }]; }],
    ["sql_ownership_changed", (w) => { w.sqlRows = [{}, { ownership_hash: "5" }]; }],
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
      return JSON.parse(JSON.stringify(await baseline(ok.src)));
    })() });
    refusedWith(r, code);
    assertNoLeak(r);
  });

  test("every declared refusal code has a test", () => {
    const tested = new Set<string>(cases.map((c) => c[0]));
    for (const c of ["bound_invalid", "bound_not_ready", "bound_stop_mismatch", "sql_unbound", "queue_consumer_mismatch", "queue_producer_mismatch"]) tested.add(c);
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
    expect(AUTHORITY_SQL.match(/\$\d/gu)!.every((p) => p === "$1" || p === "$2")).toBe(true);
    const scans = [...AUTHORITY_SQL.matchAll(/FROM (sessions\.\w+) x(?: CROSS JOIN s)? WHERE x\.org_id = \$1::text/gu)].map((m) => m[1]);
    expect(scans).toEqual(["sessions.ingest_batches", "sessions.ingest_record_revisions", "sessions.ingest_session_ownership"]);
    expect((AUTHORITY_SQL.match(/FROM sessions\./gu) ?? []).length).toBe(3);
  });
});
