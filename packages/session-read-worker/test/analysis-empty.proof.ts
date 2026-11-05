import { expect, inject, it } from "vitest";
import { createHash } from "node:crypto";
import { canonicalAnalysisHandle } from "../src/analysis-cache.ts";
import { analysisSnapshotJson } from "../../../src/session-read-api/session-analysis-artifact.ts";
declare module "vitest" {
  export interface ProvidedContext {
    artifactFixtureUrl: string;
  }
}
it("actual Node empty/request-only/not-found publication retains the strict fixed table inventory in workerd", async () => {
  const base = inject("artifactFixtureUrl"),
    claim = (await (await fetch(base + "/claim")).json()) as any,
    proof = (await (await fetch(base + "/proof")).json()) as any;
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
  let wake = 0,
    claims = 0;
  const request = new Request(
    "https://reader.test/v1/canonical/analyses/" +
      encodeURIComponent(claim.identity.sessionId) +
      "?" +
      new URLSearchParams({
        workspace: claim.identity.workspaceId,
        source: claim.identity.source,
      }),
    { headers: { authorization: "Bearer " + token } },
  );
  const response = await canonicalAnalysisHandle(
    request,
    { CANONICAL_READ_ENABLED: "true", CANONICAL_READ_MODE: "pages" },
    () => {
      throw Error("raw/Neon forbidden");
    },
    {
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
        };
      },
      publication: async () => {
        claims++;
        return claim;
      },
      read: async (key, signal) => {
        const r = await fetch(
          base + "/object?" + new URLSearchParams({ key }),
          { signal },
        );
        if (!r.ok || !r.body) throw Error("fixture missing");
        return r.body;
      },
    },
  );
  expect(response.status).toBe(200);
  const body = (await response.json()) as any;
  expect(body.payload).toEqual(proof.expected);
  expect(
    createHash("sha256")
      .update(analysisSnapshotJson(body.payload))
      .digest("hex"),
  ).toBe(proof.expectedSha256);
  expect(wake).toBe(1);
  expect(claims).toBe(2);
  console.info(
    "actual_empty_cache_workerd",
    JSON.stringify({
      label: proof.label,
      head: proof.head,
      outcome: body.payload.outcome,
      requests: body.payload.requests?.length ?? null,
      analyses: body.payload.analyses?.length ?? null,
    }),
  );
});
