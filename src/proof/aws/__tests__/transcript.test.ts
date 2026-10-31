import { describe, expect, test } from "bun:test";
import { newCtx, transcriptSearch } from "../checks.ts";
import { defaultConfig, validateConfig, type ProofConfig } from "../config.ts";
import { Runner } from "../exec.ts";
import { PROOF_FENCE } from "../fence.ts";

// Tests only: the proof fence plus the fake transcript host. Production code never widens the fence.
const TEST_FENCE = { ...PROOF_FENCE, hosts: new Set([...PROOF_FENCE.hosts, "abc.us-west-2.aws.clickhouse.cloud"]) };
import { PROOF_ACCOUNT_ID, PROOF_CLUSTER } from "../guard.ts";
import { tenantKey, type Tenant } from "../leak-scan.ts";
import { buildReport } from "../report.ts";
import { backendLabel, clickhouseBackend, credentialsFromEnv, validateTranscriptConfig, type TranscriptBackend } from "../transcript.ts";

const ARN = `arn:aws:eks:us-west-2:${PROOF_ACCOUNT_ID}:cluster/${PROOF_CLUSTER}`;
const identity = { confirmLive: true, accountId: PROOF_ACCOUNT_ID, clusterName: PROOF_CLUSTER, kubeContext: ARN, kubeContextCluster: ARN };

const configured = (): ProofConfig => ({
  ...defaultConfig(),
  sessions: 6,
  pollIntervalMs: 1,
  stageDeadlineMs: 50,
  transcriptSearch: { backend: "clickhouse", feed: "harness-synthetic", endpoint: "https://abc.us-west-2.aws.clickhouse.cloud:8443", database: "proof", table: "transcripts", credentials: { tenants: "T_CREDS", writer: "T_WRITER" } },
});

/** An in-memory backend with a switchable row policy. */
function fakeBackend(enforceTenancy: boolean): TranscriptBackend & { rows: Array<{ org_id: string; session_id: string; content: string }> } {
  const rows: Array<{ org_id: string; session_id: string; content: string }> = [];
  return {
    label: "ClickHouse Cloud abc.us-west-2.aws.clickhouse.cloud (proof.transcripts; feed: harness-synthetic)",
    rows,
    async insert(rs) {
      rows.push(...rs);
    },
    async search(t: Tenant, q: string) {
      return { rows: rows.filter((r) => r.content.includes(q) && (!enforceTenancy || r.org_id === t.orgId)).map(({ org_id, session_id }) => ({ org_id, session_id })), clientMs: 3, serverMs: 1 };
    },
  };
}

async function run(cfg: ProofConfig, backend?: TranscriptBackend, mode: "live" | "dry-run" = "live") {
  const ctx = newCtx(cfg, new Runner({ mode, identity, print: () => {}, fence: TEST_FENCE }), "aws-proof-test-run-0001", {});
  ctx.print = () => {};
  if (backend) ctx.transcriptBackend = backend;
  return { ctx, result: await transcriptSearch(ctx) };
}

describe("transcript-search gate", () => {
  test("unconfigured backend: the check is blocked and the overall verdict is INCOMPLETE", async () => {
    const { result } = await run({ ...defaultConfig(), sessions: 4 });
    expect(result.status).toBe("blocked");
    expect(result.metrics.backend).toBe("none");
    const r = buildReport({ runId: "x", mode: "live", backends: { metadata: "HoopDb Postgres", transcript: "none" }, target: { stage: "aws-proof", clusterName: PROOF_CLUSTER, kubeContext: ARN }, checks: [result] });
    expect(r.verdict).toBe("incomplete");
    expect(r.searchGates.transcript.backend).toBe("none");
  });

  test("backend that enforces tenancy: every transcript found by its own tenant, no cross-tenant hits, latency recorded", async () => {
    const { result } = await run(configured(), fakeBackend(true));
    expect(result.status).toBe("pass");
    expect(result.metrics).toMatchObject({ planted: 6, searchable: 6, crossTenantHits: 0, feed: "harness-synthetic" });
    expect((result.metrics.queryMs as { n: number; p95: number; errorRate: number }).errorRate).toBe(0);
    expect((result.metrics.landedToSearchableMs as { n: number }).n).toBe(6);
  });

  test("cross-tenant transcript hit: FAIL", async () => {
    const { result } = await run(configured(), fakeBackend(false));
    expect(result.status).toBe("fail");
    expect(result.failures.some((f) => f.startsWith("cross-tenant transcript hit"))).toBe(true);
    expect(result.metrics.crossTenantHits).toBeGreaterThan(0);
  });

  test("transcripts that never become searchable fail with an error count, not a pass", async () => {
    const b = fakeBackend(true);
    b.insert = async () => {};
    const { result } = await run(configured(), b);
    expect(result.status).toBe("fail");
    expect(result.failures[0]).toMatch(/never became searchable/);
  });

  test("dry run plans the synthetic insert and the searches without calling the backend", async () => {
    const b = fakeBackend(true);
    const { result } = await run(configured(), b, "dry-run");
    expect(result.status).toBe("planned");
    expect(b.rows).toHaveLength(0);
    expect(result.steps.find((s) => s.id === "transcript.feed.synthetic-insert")).toMatchObject({ mutating: true, approval: "khaliq", status: "planned" });
  });
});

describe("transcript backend config and labels", () => {
  test("labels name the kind and host exactly", () => {
    const base = configured().transcriptSearch;
    expect(backendLabel(base)).toBe("ClickHouse Cloud abc.us-west-2.aws.clickhouse.cloud (proof.transcripts; feed: harness-synthetic)");
    expect(backendLabel({ ...base, backend: "obsessiondb", endpoint: "https://db.obsession.example" })).toBe("ObsessionDB db.obsession.example (proof.transcripts; feed: harness-synthetic)");
    expect(backendLabel({ ...base, endpoint: "https://ch.internal.example" })).toMatch(/^ClickHouse ch\.internal\.example/);
    expect(backendLabel({ ...base, backend: "none" })).toBe("none");
  });
  test("credentials are referenced by environment variable name, and validated", () => {
    expect(validateTranscriptConfig(configured().transcriptSearch)).toEqual([]);
    expect(validateTranscriptConfig({ ...configured().transcriptSearch, endpoint: "http://plain" })).toContain("transcriptSearch.endpoint must be https");
    expect(validateTranscriptConfig({ ...configured().transcriptSearch, table: "t; DROP" })).toContain("transcriptSearch database/table must be plain identifiers");
    expect(validateTranscriptConfig({ ...configured().transcriptSearch, credentials: { tenants: "user:pw" } }).length).toBeGreaterThan(0);
    // The fake host is not in the hardcoded allowlist, so the real config validator refuses it.
    expect(validateConfig(configured()).some((i) => i.includes("not an allowlisted proof transcript host"))).toBe(true);
  });
  test("credentialsFromEnv reads only the named variables and reports them as run secrets", () => {
    const a = { orgId: "org_proof_a", workspaceId: "default" };
    const c = credentialsFromEnv(configured().transcriptSearch, { T_CREDS: JSON.stringify({ [tenantKey(a)]: "reader_a:pw-a-123456" }), T_WRITER: "writer:pw-w-123456" });
    expect(c.tenants.get(tenantKey(a))).toEqual({ user: "reader_a", password: "pw-a-123456" });
    expect(c.writer?.user).toBe("writer");
    expect(c.secrets).toContain("pw-a-123456");
    expect(c.secrets).toContain("pw-w-123456");
  });
  test("the ClickHouse query carries no tenant predicate and passes the term as a bound parameter", async () => {
    const seen: Array<{ url: string; body: string; user: string | null }> = [];
    const tenant = { orgId: "org_proof_a", workspaceId: "default" };
    const b = clickhouseBackend({
      url: "https://abc.clickhouse.cloud:8443", database: "proof", table: "transcripts", label: "x",
      tenants: new Map([[tenantKey(tenant), { user: "reader_a", password: "pw" }]]),
      fetch: async (url, init) => {
        seen.push({ url, body: String(init?.body), user: new Headers(init?.headers).get("x-clickhouse-user") });
        return new Response(JSON.stringify({ data: [{ session_id: "s1", org_id: "org_proof_a" }], statistics: { elapsed: 0.004 } }));
      },
    });
    const res = await b.search(tenant, "zq'; DROP TABLE x");
    expect(res.rows).toHaveLength(1);
    expect(res.serverMs).toBe(4);
    expect(seen[0]!.body).not.toMatch(/org_id\s*=/);
    expect(seen[0]!.body).not.toContain("DROP");
    expect(new URL(seen[0]!.url).searchParams.get("param_q")).toBe("zq'; DROP TABLE x");
    expect(new URL(seen[0]!.url).searchParams.get("readonly")).toBe("1");
    expect(seen[0]!.user).toBe("reader_a");
    await expect(b.search({ orgId: "org_x", workspaceId: "default" }, "abc")).rejects.toThrow();
  });
});

describe("transcript-search: probe errors never count as isolation", () => {
  test("a backend error on the cross-tenant probe is FAIL, not a clean empty observation", async () => {
    const b = fakeBackend(true);
    const own = b.search.bind(b);
    // Own-tenant searches work; every query for another tenant's canary errors.
    b.search = async (t, q) => {
      if (!b.rows.some((r) => r.content.includes(q) && r.org_id === t.orgId)) throw Object.assign(new Error("boom"), { code: "http_500" });
      return own(t, q);
    };
    const { result } = await run(configured(), b);
    expect(result.status).toBe("fail");
    expect(result.failures.some((f) => /cross-tenant transcript probe\(s\) errored \(http_500\); isolation unproven/.test(f))).toBe(true);
    expect(result.metrics.crossTenantHits).toBe(0);
    expect(result.metrics.crossTenantProbeErrors).toBeGreaterThan(0);
  });
});
