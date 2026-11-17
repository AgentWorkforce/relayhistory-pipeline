/** Relayfacts extractor: pure mapping from session-analysis decisions to decision records, with fakes. */
import { describe, expect, test } from "bun:test";
import { scrubText } from "../../shared/vendor.ts";
import type { AnalysisClaim, SessionAnalysisSnapshot } from "../engine/session-analysis-result.ts";
import type { AnalysisJob, AnalysisJobOutput } from "../processor.ts";
import {
  decisionRecordsEnabled,
  decisionRecordsHook,
  decisionTitleKey,
  decisionTopic,
  extractDecisionRecords,
  postgresDecisionRecordsSink,
  recordDecisions,
  type DecisionRecordInput,
} from "../decision-records.ts";

const ref = (id: string, sessionId = "sess-root", source = "claude") => ({
  id, source, kind: "assistant-message" as const, sessionId, at: null,
  summary: "TRANSCRIPT SUMMARY MUST NOT BE READ", detail: "TRANSCRIPT DETAIL MUST NOT BE READ",
  truncated: false, isError: null, toolCallId: null, locator: { eventUid: null, toolUseId: null, fileEditId: null },
});
const claim = (text: string, evidenceIds: string[], confidence: AnalysisClaim["confidence"] = "high"): AnalysisClaim =>
  ({ text, evidenceIds, confidence, basis: "observed" });

function snapshot(decisions: AnalysisClaim[], over: { files?: { path: string; editCount: number }[]; refs?: ReturnType<typeof ref>[] } = {}) {
  return {
    source: "claude",
    sessionId: "sess-root",
    evidence: { references: over.refs ?? [ref("e0"), ref("e1"), ref("c0", "sess-child"), ref("e2"), ref("c1", "sess-child"), ref("e3")] },
    metrics: { filesChanged: over.files ?? [{ path: "src/session-read-api/app.ts", editCount: 4, linesAdded: null, linesRemoved: null, userModified: null }] },
    understanding: { decisions },
  } as unknown as SessionAnalysisSnapshot;
}

describe("decisionRecordsEnabled", () => {
  test("exactly the string true", () => {
    expect(decisionRecordsEnabled({})).toBe(false);
    for (const v of ["1", "TRUE", "True", " true", "yes", ""]) expect(decisionRecordsEnabled({ SUMMARY_DECISION_RECORDS_ENABLED: v })).toBe(false);
    expect(decisionRecordsEnabled({ SUMMARY_DECISION_RECORDS_ENABLED: "true" })).toBe(true);
  });
});

describe("decisionTopic", () => {
  const t = (path: string, editCount = 1) => decisionTopic([{ path, editCount }]);
  test("first two directories of the most-edited file, relative to the repository", () => {
    expect(t("src/session-read-api/app.ts")).toBe("src-session-read-api");
    expect(t("./packages/session-read-worker/src/index.ts")).toBe("packages-session-read-worker");
    expect(t("README.md")).toBe("general");
    expect(t("docs/relayfacts.md")).toBe("docs");
    expect(decisionTopic([{ path: "docs/a.md", editCount: 1 }, { path: "src/hoopdb/migrate.ts", editCount: 3 }])).toBe("src-hoopdb");
    expect(decisionTopic([])).toBe("general");
  });
  test("an absolute path never yields a home directory or user name", () => {
    expect(t("/home/alice/work/repo/src/session-analysis/worker.ts")).toBe("src-session-analysis");
    expect(t("/Users/bob/project/notes.txt")).toBe("general");
    expect(t("~/repo/x/y.ts")).toBe("general");
    expect(t("C:\\Users\\carol\\repo\\packages\\kit\\a.ts")).toBe("packages-kit");
    expect(t("../outside/dir/file.ts")).toBe("general");
  });
  test("slugged and bounded to the table's topic pattern", () => {
    expect(t("Src/Session Read_API/app.ts")).toBe("src-session-read-api");
    const long = t(`${"a".repeat(50)}/${"b".repeat(50)}/f.ts`);
    expect(long.length).toBeLessThanOrEqual(64);
    expect(long).toMatch(/^[a-z0-9][a-z0-9-]{0,63}$/);
    expect(t("___/---/f.ts")).toBe("general");
  });
});

describe("extractDecisionRecords", () => {
  test("no decisions: nothing to write, and the sink is never called", async () => {
    expect(extractDecisionRecords(snapshot([]))).toEqual([]);
    let calls = 0;
    const sink = async () => { calls += 1; return null; };
    expect(await recordDecisions(sink, { orgId: "org-a", workspaceId: "ws-a" }, snapshot([]))).toBe(0);
    // Only low confidence, or no resolvable evidence: still nothing.
    const none = snapshot([claim("Low confidence choice.", ["e0"], "low"), claim("Uncited choice.", []), claim("Unknown ids.", ["zz"])]);
    expect(extractDecisionRecords(none)).toEqual([]);
    expect(await recordDecisions(sink, { orgId: "org-a", workspaceId: "ws-a" }, none)).toBe(0);
    expect(calls).toBe(0);
  });

  test("maps a decision to a record with one evidence row per cited session and its turn range", () => {
    const records = extractDecisionRecords(snapshot([
      claim("Use keyset paging for the topic page. It keeps pages stable, see https://github.com/acme/widgets/pull/42 for the change.", ["e3", "e1", "c1", "c0"], "medium"),
    ]));
    expect(records).toEqual([{
      topic: "src-session-read-api",
      title: "Use keyset paging for the topic page.",
      statement: "Use keyset paging for the topic page. It keeps pages stable, see https://github.com/acme/widgets/pull/42 for the change.",
      status: "decided",
      evidence: [
        // e1 is position 1 and e3 position 3 among sess-root's references; c0/c1 are 0 and 1 in sess-child.
        { source: "claude", session_id: "sess-root", turn_start: 1, turn_end: 3, claim: "Use keyset paging for the topic page. It keeps pages stable, see https://github.com/acme/widgets/pull/42 for the change.", ref: "https://github.com/acme/widgets/pull/42" },
        { source: "claude", session_id: "sess-child", turn_start: 0, turn_end: 1, claim: "Use keyset paging for the topic page. It keeps pages stable, see https://github.com/acme/widgets/pull/42 for the change.", ref: "https://github.com/acme/widgets/pull/42" },
      ],
    }]);
    // The references' transcript fields never reach a record.
    expect(JSON.stringify(records)).not.toContain("MUST NOT BE READ");
  });

  test("bounds: title 200, statement 1000, claim 300; line breaks collapse; surrogate pairs are never split", () => {
    const long = `${"word ".repeat(400)}end`;
    const [r] = extractDecisionRecords(snapshot([claim(long, ["e0"])]));
    expect(r!.title.length).toBeLessThanOrEqual(200);
    expect(r!.statement.length).toBeLessThanOrEqual(1000);
    expect(r!.evidence[0]!.claim.length).toBeLessThanOrEqual(300);
    const [multi] = extractDecisionRecords(snapshot([claim("First line\nsecond\r\nthird.\tDone", ["e0"])]));
    expect(multi!.statement).toBe("First line second third. Done");
    expect(multi!.title).toBe("First line second third.");
    for (const s of [multi!.title, multi!.statement, multi!.evidence[0]!.claim]) expect(s).not.toMatch(/[\r\n]/);
    const emoji = `${"a".repeat(299)}\u{1F600} tail`;
    const [e] = extractDecisionRecords(snapshot([claim(emoji, ["e0"])]));
    expect(e!.evidence[0]!.claim).toBe("a".repeat(299));
  });

  test("a claim is at most 300 characters and never holds a secret-like line: such a decision is not recorded", () => {
    const secrets = [
      `Rotate the deploy key ghp_${"a".repeat(36)} after the move.`,
      `Use AKIA${"ABCDEFGHIJKLMNOP"} for the bucket.`,
      "Set password=hunter2abc in the config.",
      `Call it with Bearer ${"x".repeat(40)}.`,
      // A secret beyond the claim's 300 characters still drops the whole decision.
      `${"Plain words here. ".repeat(20)}token ghp_${"b".repeat(36)}`,
    ];
    for (const s of secrets) expect(scrubText(s)).not.toBe(s);
    const records = extractDecisionRecords(snapshot([...secrets.map((s) => claim(s, ["e0"])), claim("Keep the plain decision.", ["e1"])]));
    expect(records.map((r) => r.title)).toEqual(["Keep the plain decision."]);
    for (const r of records)
      for (const e of r.evidence) {
        expect(e.claim.length).toBeLessThanOrEqual(300);
        expect(scrubText(e.claim)).toBe(e.claim);
        expect(e.claim).not.toContain("REDACTED");
      }
  });

  test("dedupe within one snapshot by topic and normalized title; evidence is merged, not repeated", () => {
    const records = extractDecisionRecords(snapshot([
      claim("Use keyset paging.", ["e0"]),
      claim("  use KEYSET   paging!! ", ["e0", "c0"]),
      claim("Use offset paging.", ["e2"]),
    ]));
    expect(records.map((r) => r.title)).toEqual(["Use keyset paging.", "Use offset paging."]);
    expect(records[0]!.evidence.map((e) => [e.session_id, e.turn_start, e.turn_end])).toEqual([["sess-root", 0, 0], ["sess-child", 0, 0]]);
    expect(decisionTitleKey("  use KEYSET   paging!! ")).toBe(decisionTitleKey("Use keyset paging."));
  });

  test("only GitHub commit, pull or issue URLs become refs; malformed sources and session ids are skipped", () => {
    const refOf = (text: string) => extractDecisionRecords(snapshot([claim(text, ["e0"])]))[0]!.evidence[0]!.ref;
    expect(refOf("Landed in https://github.com/o/r/commit/0123abcd.")).toBe("https://github.com/o/r/commit/0123abcd");
    expect(refOf("Tracked at https://github.com/o/r/issues/7#note")).toBe("https://github.com/o/r/issues/7");
    expect(refOf("See https://example.com/o/r/pull/1 instead.")).toBeNull();
    expect(refOf("See https://github.com/o/r/pull/1abc please.")).toBeNull();
    const odd = extractDecisionRecords(snapshot([claim("Pick one.", ["x0", "x1", "x2"])], {
      refs: [ref("x0", "sess\nbad"), ref("x1", "s", "Not A Source"), ref("x2", "x".repeat(513))],
    }));
    expect(odd).toEqual([]);
  });

  test("at most one evidence row per session per record: a repeated decision widens its session's span", () => {
    // e0, e1, e2, e3 are sess-root positions 0-3; c0, c1 are sess-child positions 0-1.
    const [r, ...rest] = extractDecisionRecords(snapshot([
      claim("Use keyset paging.", ["e1"]),
      claim("Use keyset paging.", ["e3", "c1"]),
      claim("use keyset paging", ["e0", "c0"]),
    ]));
    expect(rest).toEqual([]);
    expect(r!.evidence.map((e) => [e.session_id, e.turn_start, e.turn_end])).toEqual([["sess-root", 0, 3], ["sess-child", 0, 1]]);
    // The cap still holds: a full record widens existing sessions and drops new ones.
    const refs = Array.from({ length: 11 }, (_, i) => ref(`r${i}`, `sess-${i}`)).concat([ref("r0b", "sess-0")]);
    const [full] = extractDecisionRecords(snapshot([
      claim("Spread decision.", refs.slice(0, 10).map((x) => x.id)),
      claim("Spread decision.", ["r0b", "r10"]),
    ], { refs }));
    expect(full!.evidence.length).toBe(10);
    expect(full!.evidence.filter((e) => e.session_id === "sess-0").map((e) => [e.turn_start, e.turn_end])).toEqual([[0, 1]]);
    expect(full!.evidence.some((e) => e.session_id === "sess-10")).toBe(false);
  });

  test("at most 10 evidence rows per record and 100 records per snapshot", () => {
    const refs = Array.from({ length: 12 }, (_, i) => ref(`r${i}`, `sess-${i}`));
    const [r] = extractDecisionRecords(snapshot([claim("Spread decision.", refs.map((x) => x.id))], { refs }));
    expect(r!.evidence.length).toBe(10);
    const many = extractDecisionRecords(snapshot(Array.from({ length: 130 }, (_, i) => claim(`Decision number ${i}.`, ["e0"]))));
    expect(many.length).toBe(100);
  });
});

describe("postgresDecisionRecordsSink", () => {
  test("one call to relayfacts_insert_v1 with the job's workspace; none for an empty list", async () => {
    const calls: Array<[string, unknown[] | undefined]> = [];
    const db = { async query<R>(text: string, params?: unknown[]) {
      calls.push([text, params]);
      return { rows: [{ inserted: 1, merged: 0, evidence_added: 1 }] as R[] };
    } };
    const sink = postgresDecisionRecordsSink(db);
    expect(await sink({ orgId: "org-a", workspaceId: "ws-a" }, [])).toBeNull();
    expect(calls).toEqual([]);
    const records: DecisionRecordInput[] = [{ topic: "general", title: "T.", statement: "T.", status: "decided",
      evidence: [{ source: "claude", session_id: "s", turn_start: 0, turn_end: 0, claim: "T.", ref: null }] }];
    expect(await sink({ orgId: "org-a", workspaceId: "ws-a" }, records)).toEqual({ inserted: 1, merged: 0, evidence_added: 1 });
    expect(calls.length).toBe(1);
    expect(calls[0]![0]).toContain("sessions.relayfacts_insert_v1($1, $2, $3::jsonb)");
    expect(calls[0]![1]).toEqual(["org-a", "ws-a", JSON.stringify(records)]);
  });
});

describe("decisionRecordsHook", () => {
  const job = { orgId: "org-b", workspaceId: "ws-b" } as AnalysisJob;
  const output = (decisions: AnalysisClaim[]) => ({ result: { analysis: snapshot(decisions) } }) as unknown as AnalysisJobOutput;

  test("writes in the job's own workspace and counts the records sent", async () => {
    const seen: Array<[{ orgId: string; workspaceId: string }, number]> = [];
    const counts: Record<string, number> = { completed: 0 };
    const hook = decisionRecordsHook(async (scope, records) => { seen.push([scope, records.length]); return null; }, counts);
    await hook(job, output([claim("Use keyset paging.", ["e0"]), claim("Use a flag.", ["e1"])]));
    expect(seen).toEqual([[{ orgId: "org-b", workspaceId: "ws-b" }, 2]]);
    expect(counts).toEqual({ completed: 0, decision_records: 2 });
  });

  test("no decisions: no sink call; a failing sink is counted, never thrown", async () => {
    let calls = 0;
    const counts: Record<string, number> = {};
    await decisionRecordsHook(async () => { calls += 1; return null; }, counts)(job, output([]));
    expect(calls).toBe(0);
    expect(counts).toEqual({ decision_records: 0 });
    const failing: Record<string, number> = {};
    await decisionRecordsHook(async () => { throw new Error("db down"); }, failing)(job, output([claim("Use a flag.", ["e0"])]));
    expect(failing).toEqual({ decision_records_failed: 1 });
  });
});
