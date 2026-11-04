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

async function titleFixture(title: string | null, includeTitle = true) {
  const { analysisGenerationJson, analysisSourceSnapshotJson } =
    await import("../../../src/session-read-api/session-analysis-artifact.ts");
  const sha = async (value: string | Uint8Array) =>
    Array.from(
      new Uint8Array(
        await crypto.subtle.digest(
          "SHA-256",
          typeof value === "string" ? new TextEncoder().encode(value) : value,
        ),
      ),
      (b) => b.toString(16).padStart(2, "0"),
    ).join("");
  const cachedBriefTitle = {
    version: 1 as const,
    complete: true as const,
    title,
  };
  const snapshotAt = new Date().toISOString(),
    validUntil = new Date(Date.parse(snapshotAt) + 120000).toISOString(),
    snapshotId = "title-snapshot",
    prefix = "objects/title/";
  const scopedSource = { epoch: "9", fingerprint: "a".repeat(64) },
    snapshotSha256 = "e".repeat(64);
  const sourceSnapshotSha256 = includeTitle
    ? await sha(analysisSourceSnapshotJson(snapshotSha256, cachedBriefTitle))
    : undefined;
  const generation = await sha(
    analysisGenerationJson(
      identity,
      scopedSource,
      snapshotAt,
      snapshotId,
      snapshotSha256,
      sourceSnapshotSha256,
    ),
  );
  const wrapper = new TextEncoder().encode(
    JSON.stringify({
      version: 1,
      kind: "session-analysis-index",
      codec: "canonical-analysis-graph-v1",
      projection: "read_session_analysis_v1/0031",
      outcome: "not_found",
      identity,
      source: scopedSource,
      generation,
      snapshotAt,
      snapshotId,
      snapshotSha256,
      ...(includeTitle ? { cachedBriefTitle, sourceSnapshotSha256 } : {}),
      graph: { key: prefix + "graph", sha256: "b".repeat(64), bytes: 1 },
      counts: { requests: 0, analyses: 0 },
    }),
  );
  const index = {
    key: prefix + "wrapper",
    sha256: await sha(wrapper),
    bytes: wrapper.length,
  };
  const claim = {
    version: 1,
    exists: true,
    current: true,
    identity,
    source: scopedSource,
    generation,
    prefix,
    index,
    snapshotAt,
    snapshotId,
    snapshotSha256,
    validUntil,
  };
  let reads = 0,
    claims = 0;
  const deps: AnalysisHandleDeps = {
    signal: new AbortController().signal,
    now: () => Date.parse(snapshotAt) + 1000,
    wake: async () => ({
      version: 1,
      accepted: true,
      exists: true,
      identity,
      source: scopedSource,
    }),
    publication: async () => {
      claims++;
      return claim;
    },
    read: async (key) => {
      reads++;
      expect(key).toBe(index.key);
      return new ReadableStream({
        start(c) {
          c.enqueue(wrapper);
          c.close();
        },
      });
    },
  };
  return { deps, reads: () => reads, claims: () => claims };
}
it("actual title view serves a single bounded wrapper even when v1 is not_found, and preserves known-null", async () => {
  for (const title of ["Faithful title", null]) {
    const f = await titleFixture(title);
    const response = await canonicalAnalysisHandle(
      request("workspace=ws-a&source=codex&view=title"),
      env,
      forbidden,
      f.deps,
    );
    expect(
      response.status,
      response.headers.get("X-Session-Read-Reason") ?? "",
    ).toBe(200);
    const body = (await response.json()) as any;
    expect(body.outcome).toBe("title");
    expect(body.cachedBriefTitle).toEqual({
      version: 1,
      complete: true,
      title,
    });
    expect(body.payload).toBeUndefined();
    expect(f.reads()).toBe(1);
    expect(f.claims()).toBe(2);
  }
});
it("old title-less index is explicit unavailable and unknown views never reach metadata", async () => {
  const f = await titleFixture(null, false);
  const response = await canonicalAnalysisHandle(
    request("workspace=ws-a&source=codex&view=title"),
    env,
    forbidden,
    f.deps,
  );
  expect(response.status).toBe(503);
  expect(response.headers.get("X-Session-Read-Reason")).toBe(
    "analysis_title_not_ready",
  );
  const bad = await titleFixture("Title");
  expect(
    (
      await canonicalAnalysisHandle(
        request("workspace=ws-a&source=codex&view=full"),
        env,
        forbidden,
        bad.deps,
      )
    ).status,
  ).toBe(400);
  expect(bad.reads()).toBe(0);
  expect(bad.claims()).toBe(0);
});
