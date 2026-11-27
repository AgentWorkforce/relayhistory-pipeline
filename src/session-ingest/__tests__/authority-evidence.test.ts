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

/** A mutable fake of production. Default: every check passes for ORG and ORG2. */
function world() {
  const w = {
    // ingest deployments newest first; each has a list of [versionId, percentage].
    ingestDeployments: [
      { id: uuid(101), created_on: iso(NOW - 1 * HOUR), versions: [{ version_id: uuid(1), percentage: 100 }] },
      { id: uuid(102), created_on: iso(NOW - 5 * HOUR), versions: [{ version_id: uuid(2), percentage: 100 }] },
      { id: uuid(103), created_on: iso(NOW - 9 * HOUR), versions: [{ version_id: uuid(3), percentage: 100 }] },
    ] as Array<Record<string, unknown>>,
    versions: new Map<string, Binding[] | "fail">([
      [uuid(1), [plain("INGEST_HTTP_ADMISSION_STOPPED_ORGS", `${ORG},${ORG2}`), plain("INGEST_S3_FENCE_MIRROR", "on"), { name: "HYPERDRIVE", type: "hyperdrive", id: "x" }]],
      [uuid(2), [plain("INGEST_HTTP_ADMISSION_STOPPED_ORGS", `${ORG},${ORG2}`), plain("INGEST_S3_FENCE_MIRROR", "on")]],
      [uuid(3), [plain("INGEST_S3_FENCE_MIRROR", "on")]],
      [uuid(51), [plain("TURNS_WRITE_PATH", "neon")]],
      [uuid(52), []],
    ]),
    rhDeployments: [{ id: uuid(201), created_on: iso(NOW - 2 * HOUR), versions: [{ version_id: uuid(51), percentage: 100 }] }] as Array<Record<string, unknown>>,
    queues: [
      { queue_id: qid(1), queue_name: E.admissionQueue },
      { queue_id: qid(2), queue_name: E.admissionDlq },
      { queue_id: qid(3), queue_name: "something-else" },
    ] as unknown[],
    backlog: [0, 0] as unknown[],
    dlqBacklog: [3, 3] as unknown[],
    dlqConsumers: [[], []] as unknown[][],
    sqlRows: [] as Array<Record<string, unknown> | "fail" | "bad">,
    sqlDefault: {
      batches_claimed_since: "0", batches_committed_since: "0", batches_pending: "0", revisions_updated_since: "0",
      revisions_count: "120", revisions_hash: "987654321", ownership_count: "40", ownership_hash: "123456789",
    } as Record<string, unknown>,
    /** Second ingest deployments read replaced (deployment during the window). */
    ingestAfter: null as Array<Record<string, unknown>> | null,
    rhAfter: null as Array<Record<string, unknown>> | null,
    fail: new Set<string>(),
    calls: [] as string[],
    sqlCalls: [] as Array<readonly string[]>,
    slept: [] as number[],
  };
  let ingestReads = 0, rhReads = 0, metricsReads = 0, dlqMetricsReads = 0, dlqReads = 0, sqlIdx = 0;
  const src: AuthorityEvidenceSources = {
    async cfGet(path) {
      w.calls.push(path);
      if (w.fail.has(path)) throw new Error(`boom with secret ${ORG}`);
      if (path === `/workers/scripts/${E.ingestWorker}/deployments`)
        return { deployments: ingestReads++ === 0 || !w.ingestAfter ? w.ingestDeployments : w.ingestAfter };
      if (path === `/workers/scripts/${E.relayhistoryWorker}/deployments`)
        return { deployments: rhReads++ === 0 || !w.rhAfter ? w.rhDeployments : w.rhAfter };
      const v = /^\/workers\/scripts\/[^/]+\/versions\/(.+)$/u.exec(path);
      if (v) {
        const b = w.versions.get(v[1]!);
        if (b === undefined || b === "fail") throw new Error("404");
        return { id: v[1], resources: { bindings: b } };
      }
      if (path === `/queues?page=1&per_page=${AUTHORITY_QUEUE_PAGE}`) return w.queues;
      if (path === `/queues/${qid(1)}/metrics`) return { backlog_count: w.backlog[metricsReads++], backlog_bytes: 0 };
      if (path === `/queues/${qid(2)}/metrics`) return { backlog_count: w.dlqBacklog[dlqMetricsReads++] };
      if (path === `/queues/${qid(2)}`) return { queue_id: qid(2), queue_name: E.admissionDlq, consumers: w.dlqConsumers[dlqReads++] };
      throw new Error("unexpected " + path);
    },
    async sql(text, params) {
      w.sqlCalls.push(params);
      expect(text).toBe(AUTHORITY_SQL);
      const row = w.sqlRows[sqlIdx++] ?? w.sqlDefault;
      if (row === "fail") throw new Error(`pg error for ${params[0]}`);
      if (row === "bad") return [{ ...w.sqlDefault, batches_pending: "-1" }];
      return [{ ...w.sqlDefault, ...row }];
    },
    nowMs: () => NOW,
    async sleep(ms) { w.slept.push(ms); },
  };
  return { w, src };
}

const run = (src: AuthorityEvidenceSources, orgs: unknown[] = [ORG]) =>
  authorityEvidence(orgs, src, { observationWindowMs: AUTHORITY_MIN_OBSERVATION_WINDOW_MS });

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

describe("authorityEvidence pass", () => {
  test("every check passes; anchor is the deployment that established the stop", async () => {
    const { w, src } = world();
    const r = await run(src, [ORG, ORG2]);
    expect(r.status).toBe("authority_evidence_pass");
    expect(r.orgCount).toBe(2);
    for (const v of r.orgs) {
      expect(v.ok).toBe(true);
      expect(v.refusals).toEqual([]);
      expect(v.stop).toMatchObject({ ok: true, servingVersions: 1, deploymentsWalked: 3, effectiveStopMs: NOW - 5 * HOUR });
      expect(v.stop.stopDeploymentHash).toBe(evidenceHash("deployment", uuid(102)));
      expect(v.mirror.ok).toBe(true);
      expect(v.queueDrained).toMatchObject({ ok: true, backlogReads: [0, 0], dlqBacklogReads: [3, 3], dlqConsumers: 0 });
      expect(v.sqlQuiet).toMatchObject({ ok: true, reads: 2, revisionsStable: true, ownershipStable: true });
      expect(v.turnsNeon).toMatchObject({ ok: true, path: "neon", servingVersions: 1 });
    }
    expect(r.orgs[0]!.orgHash).toBe(evidenceHash("org", ORG));
    expect(r.orgs[0]!.orgHash).not.toBe(r.orgs[1]!.orgHash);
    // The SQL anchor comes from the live deployment, never an input.
    const at = String(NOW - 5 * HOUR - AUTHORITY_CLOCK_SKEW_MS);
    expect(w.sqlCalls).toEqual([[ORG, at], [ORG2, at], [ORG, at], [ORG2, at]]);
    expect(w.slept).toEqual([AUTHORITY_MIN_OBSERVATION_WINDOW_MS]);
    assertNoLeak(r);
  });

  test("default window; every retained deployment carrying the stop anchors on the oldest", async () => {
    const { w, src } = world();
    w.versions.set(uuid(3), w.versions.get(uuid(2))!);
    const r = await authorityEvidence([ORG], src);
    expect(w.slept).toEqual([AUTHORITY_OBSERVATION_WINDOW_MS]);
    expect(r.orgs[0]!.stop).toMatchObject({ ok: true, deploymentsWalked: 3, effectiveStopMs: NOW - 9 * HOUR });
  });

  test("an unreadable older version ends the walk (later anchor), it does not refuse", async () => {
    const { w, src } = world();
    w.versions.set(uuid(2), "fail");
    const r = await run(src);
    expect(r.status).toBe("authority_evidence_pass");
    expect(r.orgs[0]!.stop.effectiveStopMs).toBe(NOW - 1 * HOUR);
    const m = world();
    m.w.versions.set(uuid(2), [plain("INGEST_HTTP_ADMISSION_STOPPED_ORGS", "NOT-A-UUID")]);
    expect((await run(m.src)).orgs[0]!.stop.effectiveStopMs).toBe(NOW - 1 * HOUR);
  });

  test("gradual deployment: every serving version is checked, a 0% version is not serving", async () => {
    const { w, src } = world();
    w.ingestDeployments[0]!.versions = [{ version_id: uuid(1), percentage: 60 }, { version_id: uuid(2), percentage: 40 }, { version_id: uuid(3), percentage: 0 }];
    const r = await run(src);
    expect(r.status).toBe("authority_evidence_pass");
    expect(r.orgs[0]!.stop.servingVersions).toBe(2);
    expect(w.calls.filter((c) => c.endsWith(`/versions/${uuid(3)}`))).toHaveLength(1); // only by the walk (deployment 103)
  });

  test("a missing TURNS_WRITE_PATH on a verified version is the documented default (neon)", async () => {
    const { w, src } = world();
    w.rhDeployments[0]!.versions = [{ version_id: uuid(52), percentage: 100 }];
    expect((await run(src)).orgs[0]!.turnsNeon).toMatchObject({ ok: true, path: "neon" });
  });

  test("dual for other orgs only resolves neon; a secret TURNS_FORWARD_ORGS is irrelevant when the mode is neon", async () => {
    const a = world();
    a.w.versions.set(uuid(51), [plain("TURNS_WRITE_PATH", "dual"), plain("TURNS_FORWARD_ORGS", ` ${ORG2} ,x`)]);
    const ra = await run(a.src, [ORG, ORG2]);
    expect(ra.orgs[0]!.turnsNeon).toMatchObject({ ok: true, path: "neon" });
    expect(ra.orgs[1]!.turnsNeon).toMatchObject({ ok: false, path: "dual", code: "turns_path_not_neon" });
    const b = world();
    b.w.versions.set(uuid(51), [plain("TURNS_WRITE_PATH", "neon"), { name: "TURNS_FORWARD_ORGS", type: "secret_text" }]);
    expect((await run(b.src)).orgs[0]!.turnsNeon.ok).toBe(true);
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
  type Case = [AuthorityRefusal, (w: ReturnType<typeof world>["w"]) => void, unknown[]?];
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
    ["stop_cutover_unreadable", (w) => w.versions.set(uuid(1), [plain("INGEST_HTTP_ADMISSION_STOPPED_ORGS", `${ORG},`), plain("INGEST_S3_FENCE_MIRROR", "on")])],
    ["stop_cutover_unreadable", (w) => w.versions.set(uuid(1), [{ name: "INGEST_HTTP_ADMISSION_STOPPED_ORGS", type: "secret_text" }])],
    ["stop_org_not_listed", (w) => w.versions.set(uuid(1), [plain("INGEST_HTTP_ADMISSION_STOPPED_ORGS", ORG2), plain("INGEST_S3_FENCE_MIRROR", "on")])],
    ["stop_org_not_listed", (w) => w.versions.set(uuid(1), [plain("INGEST_S3_FENCE_MIRROR", "on")])],
    ["stop_org_not_listed", (w) => { // one of two serving versions lacks the stop
      w.ingestDeployments[0]!.versions = [{ version_id: uuid(1), percentage: 99 }, { version_id: uuid(3), percentage: 1 }];
    }],
    ["mirror_off", (w) => w.versions.set(uuid(1), [plain("INGEST_HTTP_ADMISSION_STOPPED_ORGS", ORG)])],
    ["mirror_off", (w) => { w.ingestDeployments[0]!.versions = [{ version_id: uuid(1), percentage: 50 }, { version_id: uuid(4), percentage: 50 }];
      w.versions.set(uuid(4), [plain("INGEST_HTTP_ADMISSION_STOPPED_ORGS", ORG)]); }],
    ["stop_deployment_changed", (w) => { w.ingestAfter = [{ ...w.ingestDeployments[0]!, id: uuid(109), created_on: iso(NOW) }]; }],
    ["stop_deployment_changed", (w) => { w.ingestAfter = [{ ...w.ingestDeployments[0]!, versions: [{ version_id: uuid(2), percentage: 100 }] }]; }],
    // queue
    ["queue_list_unreadable", (w) => w.fail.add(`/queues?page=1&per_page=${AUTHORITY_QUEUE_PAGE}`)],
    ["queue_list_unreadable", (w) => { w.queues = { queues: [] } as unknown as unknown[]; }],
    ["queue_list_unreadable", (w) => { (w.queues[0] as Record<string, unknown>).queue_id = "x"; }],
    ["queue_list_truncated", (w) => { w.queues = [...w.queues, ...Array.from({ length: AUTHORITY_QUEUE_PAGE }, (_, i) => ({ queue_id: qid(4), queue_name: `q${i}` }))]; }],
    ["queue_missing", (w) => { w.queues.splice(0, 1); }],
    ["queue_missing", (w) => { w.queues.splice(1, 1); }],
    ["queue_missing", (w) => { w.queues.push({ queue_id: qid(5), queue_name: E.admissionQueue }); }],
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
    // sql
    ["sql_unreadable", (w) => { w.sqlRows = ["fail"]; }],
    ["sql_unreadable", (w) => { w.sqlRows = [{}, "bad"]; }],
    ["sql_unreadable", (w) => { w.sqlRows = [{ revisions_hash: null }]; }],
    ["sql_batches_claimed_after_stop", (w) => { w.sqlRows = [{}, { batches_claimed_since: "1" }]; }],
    ["sql_batches_committed_after_stop", (w) => { w.sqlRows = [{ batches_committed_since: 2 }]; }],
    ["sql_batches_pending", (w) => { w.sqlRows = [{ batches_pending: "1" }]; }],
    ["sql_revisions_after_stop", (w) => { w.sqlRows = [{ revisions_updated_since: "4" }]; }],
    ["sql_revisions_changed", (w) => { w.sqlRows = [{}, { revisions_hash: "1" }]; }],
    ["sql_revisions_changed", (w) => { w.sqlRows = [{}, { revisions_count: "121" }]; }],
    ["sql_ownership_changed", (w) => { w.sqlRows = [{}, { ownership_count: "41" }]; }],
    ["sql_ownership_changed", (w) => { w.sqlRows = [{}, { ownership_hash: "5" }]; }],
    ["sql_no_stop_anchor", (w) => w.versions.set(uuid(1), [plain("INGEST_S3_FENCE_MIRROR", "on")])],
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
    ["turns_path_not_neon", (w) => { // one of two serving versions is on forward
      w.rhDeployments[0]!.versions = [{ version_id: uuid(51), percentage: 90 }, { version_id: uuid(53), percentage: 10 }];
      w.versions.set(uuid(53), [plain("TURNS_WRITE_PATH", "forward")]);
    }],
    ["turns_deployment_changed", (w) => { w.rhAfter = [{ ...w.rhDeployments[0]!, id: uuid(209) }]; }],
    ["turns_deployment_changed", (w) => w.fail.add("never") /* replaced below */],
  ];
  // The last case: the re-read itself fails.
  cases[cases.length - 1] = ["turns_deployment_changed", (w) => { w.rhAfter = "fail" as unknown as Array<Record<string, unknown>>; }];

  test.each(cases.map((c, i) => [`${i}:${c[0]}`, c] as const))("%s", async (_name, [code, mutate, orgs]) => {
    const { w, src } = world();
    mutate(w);
    // rhAfter = "fail" means: the second relayhistory deployments read throws.
    if ((w.rhAfter as unknown) === "fail") {
      w.rhAfter = null;
      const inner = src.cfGet;
      let n = 0;
      src.cfGet = async (p) => {
        if (p === `/workers/scripts/${E.relayhistoryWorker}/deployments` && n++ > 0) throw new Error("x");
        return inner(p);
      };
    }
    const r = await run(src, orgs ?? [ORG]);
    refusedWith(r, code);
    assertNoLeak(r);
  });

  test("every declared refusal code has a test", () => {
    const tested = new Set(cases.map((c) => c[0]));
    expect(AUTHORITY_REFUSALS.filter((c) => !tested.has(c))).toEqual([]);
  });

  test("one org refusing refuses the report; the other org's verdict stays ok", async () => {
    const { w, src } = world();
    w.versions.set(uuid(1), [plain("INGEST_HTTP_ADMISSION_STOPPED_ORGS", ORG), plain("INGEST_S3_FENCE_MIRROR", "on")]);
    const r = await run(src, [ORG, ORG2]);
    expect(r.status).toBe("authority_evidence_refused");
    expect(r.orgs[0]!.ok).toBe(true);
    expect(r.orgs[1]!.refusals).toEqual(["stop_org_not_listed", "sql_no_stop_anchor"]);
  });

  test("a failed fetch is a code, never a guess: an empty queue list does not count as an empty queue", async () => {
    const { w, src } = world();
    w.queues = [];
    const r = await run(src);
    expect(r.orgs[0]!.queueDrained).toMatchObject({ ok: false, code: "queue_missing", backlogReads: [] });
  });

  test("an unreadable SQL round is not retried into a pass", async () => {
    const { w, src } = world();
    w.sqlRows = ["fail", {}];
    const r = await run(src);
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
    // Exactly one scan per table, each filtered by the org.
    const scans = [...AUTHORITY_SQL.matchAll(/FROM (sessions\.\w+) x(?: CROSS JOIN s)? WHERE x\.org_id = \$1::text/gu)].map((m) => m[1]);
    expect(scans).toEqual(["sessions.ingest_batches", "sessions.ingest_record_revisions", "sessions.ingest_session_ownership"]);
    expect((AUTHORITY_SQL.match(/FROM sessions\./gu) ?? []).length).toBe(3);
  });
});
