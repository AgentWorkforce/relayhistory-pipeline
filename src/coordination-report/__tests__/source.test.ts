import { describe, expect, test } from "bun:test";
import {
  buildCanonicalReportExport,
  type ReportSourceDeps,
  type ReportSourceRequest,
} from "../source.ts";
const request: ReportSourceRequest = {
  scope: { organizationId: "org", workspaceId: "ws" },
  window: {
    since: "2026-11-01T00:00:00.000Z",
    until: "2026-11-02T00:00:00.000Z",
  },
  maxSessions: 10,
  maxEvents: 20,
};
function deps(): ReportSourceDeps {
  return {
    trustedScope: request.scope,
    listSessions: async () => ({
      generation: "v1",
      complete: true,
      sessions: [],
    }),
    now: () => "2026-11-03T00:00:00.000Z",
    canonical: { store: { backend: "fs" } } as ReportSourceDeps["canonical"],
  };
}
describe("canonical coordination source boundary", () => {
  test("exports a verified empty window without accessing raw storage", async () => {
    const result = await buildCanonicalReportExport(deps(), request);
    expect(result.export.sessions).toEqual([]);
    expect(result.export.coverage.status).toBe("complete");
    expect(result.sourceSnapshot.directoryGeneration).toBe("v1");
  });
  test("denies request scope before inventory access", async () => {
    const d = deps();
    d.listSessions = async () => {
      throw new Error("unexpected inventory access");
    };
    await expect(
      buildCanonicalReportExport(d, {
        ...request,
        scope: { ...request.scope, organizationId: "other" },
      }),
    ).rejects.toThrow("scope_denied");
  });
  test("refuses incomplete or over-budget inventories", async () => {
    const d = deps();
    d.listSessions = async () => ({
      generation: "v1",
      complete: false,
      sessions: [],
    });
    await expect(buildCanonicalReportExport(d, request)).rejects.toThrow(
      "inventory_incomplete",
    );
  });
  test("refuses changed directory generation including erasures", async () => {
    const d = deps();
    let calls = 0;
    d.listSessions = async () => ({
      generation: String(++calls),
      complete: true,
      sessions: [],
    });
    await expect(buildCanonicalReportExport(d, request)).rejects.toThrow(
      "directory_changed",
    );
  });
  test("rejects tenant-mismatched inventory rows before canonical access", async () => {
    const d = deps();
    d.listSessions = async () => ({
      generation: "v1",
      complete: true,
      sessions: [
        { orgId: "other", workspaceId: "ws", source: "codex", sessionId: "s1" },
      ],
    });
    await expect(buildCanonicalReportExport(d, request)).rejects.toThrow(
      "tenant_mismatch",
    );
  });
  test("rejects unbounded jobs and future windows", async () => {
    await expect(
      buildCanonicalReportExport(deps(), { ...request, maxEvents: 10001 }),
    ).rejects.toThrow("budget_invalid");
    const d = deps();
    d.now = () => request.window.since;
    await expect(buildCanonicalReportExport(d, request)).rejects.toThrow(
      "future_window",
    );
  });
});

test("deadline rejects a stalled inventory without dispatching canonical reads", async () => {
  const d = deps(),
    controller = new AbortController();
  d.signal = controller.signal;
  d.listSessions = async () => new Promise(() => {});
  const pending = buildCanonicalReportExport(d, request);
  controller.abort(new Error("deadline"));
  await expect(pending).rejects.toThrow("deadline");
});
