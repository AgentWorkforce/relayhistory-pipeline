import { expect, inject, it } from "vitest";
import { createHash } from "node:crypto";
import {
  canonicalAnalysisHandle,
  type AnalysisHandleDeps,
} from "../src/analysis-cache.ts";
import { readSessionAnalysisCache } from "../../../src/session-read-api/session-analysis-reader.ts";
import { analysisSnapshotJson } from "../../../src/session-read-api/session-analysis-artifact.ts";
declare module "vitest" {
  export interface ProvidedContext {
    artifactFixtureUrl: string;
  }
}
const base = () => inject("artifactFixtureUrl");
async function setup() {
  const claim = (await (await fetch(base() + "/claim")).json()) as any,
    proof = (await (await fetch(base() + "/proof")).json()) as any;
  let wake = 0,
    claims = 0;
  const deps: AnalysisHandleDeps = {
    signal: new AbortController().signal,
    now: () => Date.parse(claim.snapshotAt) + 1000,
    wake: async (identity) => {
      wake++;
      return {
        version: 1,
        accepted: true,
        exists: true,
        identity,
        source: claim.source,
        nextRefreshAt: null,
      };
    },
    publication: async () => {
      claims++;
      return (await fetch(base() + "/claim")).json();
    },
    read: async (key, signal) => {
      const response = await fetch(
        base() + "/object?" + new URLSearchParams({ key }),
        { signal },
      );
      if (!response.ok || !response.body) throw new Error("fixture missing");
      return response.body;
    },
  };
  const token =
    "header." +
    btoa(
      JSON.stringify({
        role: "session_directory_search_worker",
        app_metadata: {
          org_id: claim.identity.orgId,
          workspace_ids: [claim.identity.workspaceId],
        },
      }),
    ) +
    ".signature";
  const request = () =>
    new Request(
      "https://read.test/v1/canonical/analyses/" +
        encodeURIComponent(claim.identity.sessionId) +
        "?" +
        new URLSearchParams({
          workspace: claim.identity.workspaceId,
          source: claim.identity.source,
        }),
      { headers: { authorization: "Bearer " + token } },
    );
  return { claim, proof, deps, request, counts: () => ({ wake, claims }) };
}
it("actual0031 SQL -> leased publication -> HTTP workerd preserves real virtual IDs/running brief and all large data", async () => {
  const f = await setup();
  await fetch(base() + "/reset");
  const response = await canonicalAnalysisHandle(
    f.request(),
    { CANONICAL_READ_ENABLED: "true", CANONICAL_READ_MODE: "pages" },
    () => {
      throw new Error("Neon/raw forbidden");
    },
    f.deps,
  );
  expect(response.status).toBe(200);
  const body = (await response.json()) as any;
  expect(body).toMatchObject({
    orgId: f.claim.identity.orgId,
    workspaceId: f.claim.identity.workspaceId,
    source: f.claim.identity.source,
    sessionId: f.claim.identity.sessionId,
    outcome: "ok",
    generation: f.claim.generation,
    snapshotAt: f.claim.snapshotAt,
  });
  const payload = body.payload;
  expect(payload.requests.map((x: any) => x.id)).toEqual(
    f.proof.expected.requests,
  );
  expect(payload.analyses.map((x: any) => x.id)).toEqual(
    f.proof.expected.analyses,
  );
  const analysis = payload.analyses[0];
  expect(analysis.status).toBe(f.proof.expected.status);
  expect(analysis.result.brief.title).toBe(f.proof.expected.title);
  expect(analysis.result.brief.description.length).toBe(
    f.proof.expected.descriptionChars,
  );
  expect(analysis.result.brief.metadata.tools.length).toBe(
    f.proof.expected.toolCount,
  );
  expect(
    createHash("sha256").update(analysisSnapshotJson(payload)).digest("hex"),
  ).toBe(f.claim.snapshotSha256);
  expect(f.counts()).toEqual({ wake: 1, claims: 2 });
  const stats = (await (await fetch(base() + "/stats")).json()) as any;
  expect(stats.maxBytes).toBeLessThanOrEqual(256 * 1024);
  expect(stats.requests).toBeLessThan(512);
  console.info(
    "analysis_cache_workerd",
    JSON.stringify({
      source: f.proof.head,
      objects: stats.requests,
      bytes: stats.totalBytes,
      maxObjectBytes: stats.maxBytes,
      descriptionChars: analysis.result.brief.description.length,
      toolCount: analysis.result.brief.metadata.tools.length,
    }),
  );
});
it("immutable expired/future cache refuses before object access and never rewrites fixture timestamps", async () => {
  const f = await setup();
  await fetch(base() + "/reset");
  f.deps.now = () => Date.parse(f.claim.snapshotAt) + 121000;
  await expect(
    readSessionAnalysisCache(f.deps, f.claim.identity),
  ).rejects.toMatchObject({ reason: "analysis_cache_expired" });
  expect(
    ((await (await fetch(base() + "/stats")).json()) as any).requests,
  ).toBe(0);
  f.deps.now = () => Date.parse(f.claim.snapshotAt) - 31000;
  await expect(
    readSessionAnalysisCache(f.deps, f.claim.identity),
  ).rejects.toMatchObject({ reason: "analysis_cache_expired" });
});
it("stale/foreign publication and changed final index refuse without raw/Neon fallback", async () => {
  const f = await setup();
  await fetch(base() + "/reset");
  f.deps.publication = async () => ({ ...f.claim, current: false });
  await expect(
    readSessionAnalysisCache(f.deps, f.claim.identity),
  ).rejects.toMatchObject({ reason: "analysis_cache_not_ready" });
  expect(
    ((await (await fetch(base() + "/stats")).json()) as any).requests,
  ).toBe(0);
  f.deps.publication = async () => ({
    ...f.claim,
    identity: { ...f.claim.identity, workspaceId: "foreign" },
  });
  await expect(
    readSessionAnalysisCache(f.deps, f.claim.identity),
  ).rejects.toMatchObject({ reason: "analysis_publication" });
  let calls = 0;
  f.deps.publication = async () =>
    ++calls === 1
      ? f.claim
      : { ...f.claim, index: { ...f.claim.index, sha256: "d".repeat(64) } };
  await expect(
    readSessionAnalysisCache(f.deps, f.claim.identity),
  ).rejects.toMatchObject({ reason: "source_generation_changed" });
});
it("HTTP demand wakeup scope and disabled mode refuse before cache/object operations", async () => {
  const f = await setup();
  f.deps.wake = async () => ({
    version: 1,
    accepted: true,
    exists: true,
    identity: { ...f.claim.identity, workspaceId: "foreign" },
  });
  const refused = await canonicalAnalysisHandle(
    f.request(),
    { CANONICAL_READ_ENABLED: "true", CANONICAL_READ_MODE: "pages" },
    () => {
      throw new Error("network forbidden");
    },
    f.deps,
  );
  expect(refused.status).toBe(503);
  expect(refused.headers.get("X-Session-Read-Reason")).toBe("analysis_wakeup");
  expect(f.counts().claims).toBe(0);
  const disabled = await canonicalAnalysisHandle(
    f.request(),
    {},
    () => {
      throw new Error("network forbidden");
    },
    f.deps,
  );
  expect(disabled.status).toBe(503);
  expect(f.counts().claims).toBe(0);
});
