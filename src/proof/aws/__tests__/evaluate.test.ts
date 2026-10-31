import { describe, expect, test } from "bun:test";
import { expectedShortSummary, FAKE_AWS_KEY_ID, plantCorpus } from "../corpus.ts";
import { evaluatePlantedTrace, evaluateStaleSummary, evaluateV1Scope, type SessionObservation, type StaleObservation } from "../evaluate.ts";

const tenants = [
  { orgId: "org_proof_a", workspaceId: "default" },
  { orgId: "org_proof_b", workspaceId: "default" },
];
const corpus = plantCorpus({ runId: "aws-proof-evaluate-0001", sessions: 4, tenants, now: 1_800_000_000_000 });
const forbidden = (p: (typeof corpus)[number]) => [FAKE_AWS_KEY_ID, p.transcriptCanary];

async function healthy(): Promise<Map<string, SessionObservation>> {
  const m = new Map<string, SessionObservation>();
  for (const [i, p] of corpus.entries())
    m.set(p.sessionId, {
      sessionId: p.sessionId, s3Present: true, indexRow: true,
      times: { objectLanded: 1000, indexed: 1000 + 100 * (i + 1), summary: 1000 + 200 * (i + 1) },
      shortSummary: await expectedShortSummary(p, 1), searchFound: true, searchLatencyMs: 5 + i,
    });
  return m;
}

describe("planted-session trace", () => {
  test("all four stages present: no failures, per-stage percentiles computed", async () => {
    const expected = new Map(await Promise.all(corpus.map(async (p) => [p.sessionId, await expectedShortSummary(p, 1)] as const)));
    const v = evaluatePlantedTrace(corpus, await healthy(), { expectedSummaries: expected, forbiddenInSummary: forbidden, searchChecked: true });
    expect(v.failures).toEqual([]);
    expect(v.metrics.stageCounts).toEqual({ s3: 4, index: 4, summary: 4, search: 4 });
    expect(v.metrics.landedToIndexedMs).toMatchObject({ n: 4, p50: 200, p95: 400, errors: 0 });
    expect(v.metrics.landedToSummaryMs.p99).toBe(800);
    expect(v.metrics.searchQueryMs).toMatchObject({ n: 4, errorRate: 0 });
  });
  test("each missing stage is named, with the session ids", async () => {
    const o = await healthy();
    const [a, b, c, d] = corpus.map((p) => p.sessionId);
    o.set(a!, { ...o.get(a!)!, s3Present: false });
    o.set(b!, { ...o.get(b!)!, indexRow: false, times: { objectLanded: 1000 } });
    o.set(c!, { ...o.get(c!)!, shortSummary: null });
    o.set(d!, { ...o.get(d!)!, searchFound: false, searchError: "search_failed", searchLatencyMs: undefined });
    const v = evaluatePlantedTrace(corpus, o, { forbiddenInSummary: forbidden, searchChecked: true });
    expect(v.failures.map((f) => f.split(":")[0])).toEqual(["S3 manifest object missing", "HoopDb index row missing", "short_summary not populated", "V1 search did not return the session to its own tenant"]);
    expect(v.failures[0]).toContain(a!);
    expect(v.metrics.landedToIndexedMs.errors).toBe(1);
    expect(v.metrics.searchQueryMs.errors).toBe(1);
  });
  test("an unscrubbed short_summary (fake key or transcript-only text) fails", async () => {
    const o = await healthy();
    const p = corpus[0]!;
    o.set(p.sessionId, { ...o.get(p.sessionId)!, shortSummary: `claude: AWS_ACCESS_KEY_ID=${FAKE_AWS_KEY_ID}` });
    const v = evaluatePlantedTrace(corpus, o, { forbiddenInSummary: forbidden, searchChecked: true });
    expect(v.failures.some((f) => f.startsWith("short_summary carries planted secret"))).toBe(true);
  });
  test("the expected extractive summary has the fake key scrubbed and no transcript-only text", async () => {
    const p = corpus[0]!;
    const s = await expectedShortSummary(p, 1);
    expect(s).toContain(p.summaryTerm);
    expect(s).toContain(p.tenantCanary);
    expect(s).not.toContain(FAKE_AWS_KEY_ID);
    expect(s).not.toContain(p.transcriptCanary);
    expect(await expectedShortSummary(p, 2)).not.toBe(s);
  });
});

describe("V1 search scope", () => {
  test("no transcript hits: UNSUPPORTED, stated as such", () => {
    expect(evaluateV1Scope([{ hits: 0 }, { hits: 0 }])).toEqual({ status: "unsupported", failures: [] });
  });
  test("transcript text reaching V1 search is a failure, never 'support'", () => {
    expect(evaluateV1Scope([{ hits: 0 }, { hits: 2 }]).status).toBe("fail");
  });
  test("no probes ran: blocked", () => {
    expect(evaluateV1Scope([]).status).toBe("blocked");
  });
});

/**
 * #38 HOLD fixes as test cases (99e96d1, 134afa8): a re-ingested session whose
 * summary is invalidated must never keep serving the old revision's short_summary.
 */
describe("#38 HOLD: stale short_summary after re-ingest", () => {
  const base: StaleObservation = {
    sessionId: "s1",
    rev1Summary: 'claude: "Proof session zq…" — 2 user turns, 1 assistant turns, tools: none.',
    rev1GeneratedAtMs: 1000,
    rev2Expected: 'claude: "Proof session zq…" — 3 user turns, 2 assistant turns, tools: none.',
    searchSummaries: [],
    deadlineReached: false,
  };
  test("revision 2's summary served: ok", () => {
    expect(evaluateStaleSummary({ ...base, current: base.rev2Expected, currentGeneratedAtMs: 2000, searchSummaries: [base.rev2Expected!] }).state).toBe("ok");
  });
  test("summary cleared after the object could not be authenticated: ok (no stale text searchable)", () => {
    expect(evaluateStaleSummary({ ...base, current: null, searchSummaries: [null] })).toEqual({ state: "ok", reason: "summary cleared" });
  });
  test("99e96d1: revision 1's text left behind by a failed directory write is stale once the deadline passes", () => {
    const o = { ...base, current: base.rev1Summary, currentGeneratedAtMs: 1000, searchSummaries: [base.rev1Summary] };
    expect(evaluateStaleSummary(o).state).toBe("pending");
    expect(evaluateStaleSummary({ ...o, deadlineReached: true }).state).toBe("stale");
  });
  test("search serving revision 1 while the row moved on is stale immediately", () => {
    expect(evaluateStaleSummary({ ...base, current: base.rev2Expected, searchSummaries: [base.rev1Summary] }).state).toBe("stale");
  });
  test("a summary that is neither revision is stale", () => {
    expect(evaluateStaleSummary({ ...base, current: "something else", searchSummaries: [] }).state).toBe("stale");
  });
  test("generated_at going backwards (an older object replacing a newer one) is stale", () => {
    expect(evaluateStaleSummary({ ...base, rev2Expected: undefined, current: "newer text", currentGeneratedAtMs: 500 }).state).toBe("stale");
  });
  test("without a predictable revision-2 text, anything but revision 1 with a fresh generated_at is ok", () => {
    expect(evaluateStaleSummary({ ...base, rev2Expected: undefined, current: "model text", currentGeneratedAtMs: 3000 }).state).toBe("ok");
  });
});
