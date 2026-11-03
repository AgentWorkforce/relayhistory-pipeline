import { expect, it } from "vitest";
import {
  canonicalAnalysisHandle,
  type AnalysisHandleDeps,
} from "../src/analysis-cache.ts";
const identity = {
    orgId: "org-a",
    workspaceId: "ws-a",
    source: "codex",
    sessionId: "one",
  },
  source = { epoch: null, fingerprint: "a".repeat(64) };
const token =
  "header." +
  btoa(
    JSON.stringify({
      role: "session_directory_search_worker",
      app_metadata: {
        org_id: identity.orgId,
        workspace_ids: [identity.workspaceId],
      },
    }),
  ) +
  ".signature";
const env = { CANONICAL_READ_ENABLED: "true", CANONICAL_READ_MODE: "pages" };
const request = (query = "workspace=ws-a&source=codex", auth = true) =>
  new Request("https://read.test/v1/canonical/analyses/one?" + query, {
    headers: auth ? { authorization: "Bearer " + token } : {},
  });
const forbidden = () => {
  throw new Error("Neon/network forbidden");
};
function fixture() {
  let calls = 0;
  const deps: AnalysisHandleDeps = {
    signal: new AbortController().signal,
    wake: async () => {
      calls++;
      return {
        version: 1,
        accepted: false,
        exists: false,
        identity,
        source,
        nextRefreshAt: null,
      };
    },
    publication: async () => {
      calls++;
      return { version: 1, exists: false, current: false, identity, source };
    },
    read: forbidden,
  };
  return { deps, calls: () => calls };
}
it("authenticated absent source wakes and double-checks metadata with no Neon or object access", async () => {
  const f = fixture();
  const response = await canonicalAnalysisHandle(
    request(),
    env,
    forbidden,
    f.deps,
  );
  expect(response.status).toBe(200);
  expect(((await response.json()) as any).outcome).toBe("not_found");
  expect(f.calls()).toBe(3);
  expect(response.headers.get("X-Relayhistory-Workspace-Id")).toBe(
    identity.workspaceId,
  );
});
it("default mode, missing bearer and invalid query never reach metadata or objects", async () => {
  for (const [req, config, status] of [
    [request(), {}, 503],
    [request(undefined, false), env, 401],
    [request("workspace=ws-a&source=codex&source=claude"), env, 400],
    [request("workspace=ws-a&source=codex&q=secret"), env, 400],
  ] as const) {
    const f = fixture();
    expect(
      (await canonicalAnalysisHandle(req, config, forbidden, f.deps)).status,
    ).toBe(status);
    expect(f.calls()).toBe(0);
  }
});
it("failed JWT-authorized demand RPC stops before pointer and S3 access", async () => {
  const f = fixture();
  f.deps.wake = async () => {
    throw new Error("JWT refused");
  };
  const response = await canonicalAnalysisHandle(
    request(),
    env,
    forbidden,
    f.deps,
  );
  expect(response.status).toBe(503);
  expect(f.calls()).toBe(0);
});
it("foreign demand acknowledgement stops before pointer and S3 access", async () => {
  const f = fixture();
  f.deps.wake = async () => ({
    version: 1,
    accepted: true,
    exists: true,
    identity: { ...identity, orgId: "foreign" },
  });
  const response = await canonicalAnalysisHandle(
    request(),
    env,
    forbidden,
    f.deps,
  );
  expect(response.status).toBe(503);
  expect(response.headers.get("X-Session-Read-Reason")).toBe("analysis_wakeup");
  expect(f.calls()).toBe(0);
});
