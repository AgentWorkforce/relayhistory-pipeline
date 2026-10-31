import { describe, expect, test } from "bun:test";
import { crossTenantHits, refusedAsExpected, scanForCanaries, summaryLeaks, tenantKey, type ProbeObservation } from "../leak-scan.ts";
import { canaryTag } from "../report.ts";

const A = { orgId: "org_a", workspaceId: "default" };
const B = { orgId: "org_b", workspaceId: "default" };
const owner = new Map([
  ["sa1", A],
  ["sb1", B],
]);
const canaries = new Map([
  [tenantKey(A), ["zqaaaa-tenantcanary"]],
  [tenantKey(B), ["zqbbbb-tenantcanary"]],
]);

describe("cross-tenant detection", () => {
  test("own rows are not hits", () => {
    const probes: ProbeObservation[] = [{ surface: "search", caller: A, status: 200, sessionIds: ["sa1"], body: '[{"s":"zqaaaa-tenantcanary"}]' }];
    expect(crossTenantHits(probes, owner, canaries)).toEqual({ hits: [], unknownIds: [] });
  });
  test("another tenant's session id is a hit on every surface", () => {
    for (const surface of ["search", "by-id", "list", "manifest-status"]) {
      const { hits } = crossTenantHits([{ surface, caller: A, status: 200, sessionIds: ["sb1"] }], owner, canaries);
      expect(hits).toEqual([{ surface, caller: "org_a/default", owner: "org_b/default", subject: "sb1" }]);
    }
  });
  test("another tenant's canary in a body is a hit even when no id leaked", () => {
    const { hits } = crossTenantHits([{ surface: "search", caller: A, status: 200, sessionIds: [], body: "…zqbbbb-tenantcanary…" }], owner, canaries);
    expect(hits).toHaveLength(1);
    expect(hits[0]!.subject).toBe(canaryTag("zqbbbb-tenantcanary"));
  });
  test("ids the run never planted are reported, not silently accepted", () => {
    expect(crossTenantHits([{ surface: "list", caller: A, status: 200, sessionIds: ["stranger"] }], owner, canaries).unknownIds).toEqual(["stranger"]);
  });
  test("refusals: 401/403/404 and SQLSTATE 42501", () => {
    for (const s of [401, 403, 404, "42501"]) expect(refusedAsExpected(s)).toBe(true);
    for (const s of [200, 204, 400, 500]) expect(refusedAsExpected(s)).toBe(false);
  });
});

describe("canary scan over logs and traces", () => {
  const c = 'zqrun1t0s1transcriptonly';
  test("finds raw and JSON-escaped forms, counts and locates without echoing", () => {
    const quoted = 'say "hi" zq-quote';
    const hits = scanForCanaries(new Map([
      ["logs.ingest", `{"msg":"x"}\n{"msg":"${c}"}\n${c}`],
      ["traces", JSON.stringify({ v: quoted })],
      ["clean", "nothing here"],
    ]), [c, quoted]);
    expect(hits).toEqual([
      { source: "logs.ingest", canary: canaryTag(c), offset: 20, count: 2 },
      { source: "traces", canary: canaryTag(quoted), offset: 6, count: 1 },
    ]);
    expect(JSON.stringify(hits)).not.toContain(c);
  });
  test("clean sources produce no hits", () => {
    expect(scanForCanaries(new Map([["a", "zqrun1t0s1transcriptonlX"]]), [c])).toEqual([]);
  });
  test("short canaries are refused (they would match by accident)", () => {
    expect(() => scanForCanaries(new Map([["a", "x"]]), ["abc"])).toThrow();
  });
  test("short_summary leaks are tagged, never echoed", () => {
    expect(summaryLeaks("claude: Proof AKIAIOSFODNN7EXAMPLE", ["AKIAIOSFODNN7EXAMPLE", c])).toEqual([canaryTag("AKIAIOSFODNN7EXAMPLE")]);
    expect(summaryLeaks(null, [c])).toEqual([]);
  });
});
