import { describe, expect, test } from "bun:test";
import { buildReport, canaryTag, CHECK_IDS, evidence, REPORT_SCHEMA, sanitize, validateReport, verdictOf, type CheckResult } from "../report.ts";

const T = "2026-10-30T12:00:00.000Z";
const ran = [{ id: "s", command: "kubectl get pods", mutating: false, touches: [], approval: "operator" as const, status: "ran" as const }];
const check = (over: Partial<CheckResult> = {}): CheckResult => ({ check: "fresh-install", status: "pass", startedAt: T, finishedAt: T, summary: "ok", metrics: {}, failures: [], evidence: [], steps: ran, ...over });
const CH = "ClickHouse Cloud abc.us-west-2.aws.clickhouse.cloud (proof.transcripts; feed: harness-synthetic)";
const report = (checks: CheckResult[], transcript = CH) =>
  buildReport({ runId: "aws-proof-test", generatedAt: T, mode: "dry-run", backends: { metadata: "HoopDb Postgres (CNPG 96bulls/hoopdb)", transcript }, target: { stage: "aws-proof", clusterName: "96bulls-aws-proof", kubeContext: "ctx" }, checks });

describe("report schema", () => {
  test("a built report validates, states the V1 scope, and names each search gate's backend", () => {
    const r = report([check()]);
    expect(r.schema).toBe(REPORT_SCHEMA);
    expect(r.scope.v1FullTranscriptSearch).toBe("UNSUPPORTED");
    expect(r.scope.reason).toMatch(/UNSUPPORTED on V1.*INCOMPLETE, never PASS/);
    expect(r.searchGates.metadata.backend).toMatch(/^HoopDb Postgres/);
    expect(r.searchGates.transcript.backend).toBe(CH);
    expect(validateReport(r)).toEqual([]);
  });
  test("a transcript-search pass with no backend is invalid", () => {
    const r = report([check({ check: "transcript-search" })], "none");
    expect(validateReport(r)).toContain("transcript-search cannot pass with no transcript backend");
  });
  test("structural problems are reported", () => {
    const r = report([check()]) as unknown as Record<string, unknown>;
    expect(validateReport({ ...r, schema: "x" })).toContain(`schema must be ${REPORT_SCHEMA}`);
    expect(validateReport({ ...r, scope: { v1FullTranscriptSearch: "SUPPORTED", reason: "" } })).toContain("scope must state V1 full-transcript search is UNSUPPORTED");
    expect(validateReport({ ...r, searchGates: { metadata: { backend: "" }, transcript: {} } })[0]).toMatch(/searchGates must name the backend/);
    expect(validateReport(report([check({ status: "pass", failures: ["x"] })])).some((i) => i.includes("passes with failures"))).toBe(true);
    expect(validateReport(report([check({ status: "fail" })])).some((i) => i.includes("without a stated failure"))).toBe(true);
    expect(validateReport(report([check({ status: "unsupported" })])).some((i) => i.includes("only v1-search-scope"))).toBe(true);
    expect(validateReport(report([check(), check()])).some((i) => i.includes("duplicated"))).toBe(true);
    expect(validateReport(report([check({ check: "nope" as never })])).some((i) => i.includes("unknown"))).toBe(true);
    expect(validateReport({ ...r, verdict: "pass" }).some((i) => i.startsWith("verdict must be"))).toBe(true);
  });
  test("the report self-check catches secrets and canaries that slipped through", () => {
    const jwt = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4eHh4eHh4In0.c2lnbmF0dXJlLXZhbHVl";
    const leaky = report([check({ summary: `token ${jwt}` })]);
    expect(validateReport(leaky).some((i) => i.startsWith("secret-shaped value"))).toBe(true);
    const canary = "zqrun1t0s1transcriptonly";
    expect(validateReport(report([check({ summary: canary })]), { canaries: [canary] }).some((i) => i.includes("planted canary"))).toBe(true);
    expect(validateReport(report([check({ summary: "minted-run-token-123" })]), { secrets: ["minted-run-token-123"] })).toContain("a run secret appears in the report");
  });
});

describe("verdict", () => {
  const all = (status: CheckResult["status"]) => CHECK_IDS.map((id) => check({ check: id, status: id === "v1-search-scope" ? "unsupported" : status }));
  test("pass needs every check to pass (v1-search-scope may be unsupported) and a configured transcript backend", () => {
    expect(verdictOf(all("pass"), CH)).toBe("pass");
  });
  test("unconfigured transcript backend: overall INCOMPLETE even when every other check passes", () => {
    expect(verdictOf(all("pass"), "none")).toBe("incomplete");
    const blocked = all("pass").map((c) => (c.check === "transcript-search" ? { ...c, status: "blocked" as const } : c));
    expect(report(blocked, "none").verdict).toBe("incomplete");
  });
  test("a PASS backed only by planned steps never counts", () => {
    const planned = all("pass").map((c) => (c.check === "teardown" ? { ...c, steps: [{ ...ran[0]!, status: "planned" as const }] } : c));
    expect(verdictOf(planned, CH)).toBe("incomplete");
    expect(verdictOf(all("pass").map((c) => (c.check === "teardown" ? { ...c, steps: [] } : c)), CH)).toBe("incomplete");
  });
  test("V1's UNSUPPORTED label alone never yields PASS: transcript-search must itself pass", () => {
    const noTranscript = all("pass").filter((c) => c.check !== "transcript-search");
    expect(verdictOf(noTranscript, CH)).toBe("incomplete");
  });
  test("any fail fails; anything blocked/planned/skipped is incomplete", () => {
    const a = all("pass");
    a[2] = { ...a[2]!, status: "fail", failures: ["x"] };
    expect(verdictOf(a, CH)).toBe("fail");
    for (const s of ["blocked", "planned", "skipped"] as const) {
      const b = all("pass");
      b[3] = { ...b[3]!, status: s };
      expect(verdictOf(b, CH)).toBe("incomplete");
    }
    expect(verdictOf(all("pass").slice(1), CH)).toBe("incomplete");
  });
});

describe("sanitize", () => {
  test("redacts every secret shape the stack uses", () => {
    const raw = [
      "Authorization: Bearer abcdefgh12345678",
      "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4eHh4eHh4In0.c2lnbmF0dXJl",
      "x-api-key rak_0123456789abcdef0123456789abcdef",
      "AKIAIOSFODNN7EXAMPLE",
      "postgres://hoop:hunter22@hoopdb-rw:5432/hoopdb",
      `SUMMARY_MASTER_KEYS=sd1:${"a".repeat(64)}`,
      "-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----",
      '"password": "correct-horse"',
    ].join("\n");
    const out = sanitize(raw);
    for (const s of ["abcdefgh12345678", "eyJhbGci", "rak_0123", "AKIAIOSFODNN7EXAMPLE", "hunter22", "a".repeat(64), "MIIE", "correct-horse"]) expect(out).not.toContain(s);
  });
  test("keeps image digests, which are evidence", () => {
    const d = `ghcr.io/x@sha256:${"b".repeat(64)}`;
    expect(sanitize(d)).toBe(d);
  });
  test("replaces known run secrets and canaries with stable tags", () => {
    expect(sanitize("tok=abcdef-run zq-canary-1", { secrets: ["abcdef-run"], canaries: ["zq-canary-1"] })).toBe(`tok=[redacted:run-secret] ${canaryTag("zq-canary-1")}`);
  });
  test("evidence hashes the raw bytes but stores only the sanitized excerpt", () => {
    const e = evidence("x", "Bearer abcdefgh12345678");
    expect(e.sha256).toMatch(/^[0-9a-f]{64}$/);
    expect(e.bytes).toBe(23);
    expect(e.excerpt).toBe("Bearer [redacted]");
  });
});
