import { beforeEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import {
  readSessionAnalysisCache,
  type AnalysisCacheDeps,
} from "../session-analysis-reader.ts";
import {
  analysisSnapshotJson,
  analysisGenerationJson,
} from "../session-analysis-artifact.ts";
const identity = {
    orgId: "org-a",
    workspaceId: "ws-a",
    source: "codex",
    sessionId: "session:one",
  },
  source = { epoch: "9", fingerprint: "a".repeat(64) };
const snapshotAt = "2026-11-03T21:00:00.000000Z",
  validUntil = "2026-11-03T21:02:00.000000Z",
  snapshotId = "snapshot-one",
  prefix = "objects/analysis/";
const bytes = (v: unknown) => new TextEncoder().encode(JSON.stringify(v));
const sha = (v: Uint8Array | string) =>
  createHash("sha256").update(v).digest("hex");
let objects: Map<string, Uint8Array>,
  ack: any,
  deps: AnalysisCacheDeps,
  reads: number,
  claims: number;
async function fixture(change?: {
  row?: Record<string, unknown>;
  count?: number;
  outcome?: string;
  snapshotSha?: string;
}) {
  objects = new Map();
  reads = 0;
  claims = 0;
  const payload = {
    outcome: "ok" as const,
    sessionId: identity.sessionId,
    requests: [
      { id: "request-real", sessionId: identity.sessionId, status: "running" },
    ],
    analyses: [
      {
        id: "virtual-v1-id",
        sessionId: identity.sessionId,
        status: "running",
        result: {
          brief: {
            title: "ready while running",
            description: "large preserved",
            metadata: { tools: [{ id: "t" }] },
          },
        },
      },
    ],
  };
  const snapshotSha256 =
    change?.snapshotSha ?? sha(analysisSnapshotJson(payload));
  const generation = sha(
    analysisGenerationJson(
      identity,
      source,
      snapshotAt,
      snapshotId,
      snapshotSha256,
    ),
  );
  const put = (name: string, v: unknown) => {
    const body = bytes(v),
      key = prefix + name;
    objects.set(key, body);
    return { key, sha256: sha(body), bytes: body.length };
  };
  const tables: Record<string, unknown> = {};
  for (const [table, items] of [
    ["session_analysis_requests", payload.requests],
    ["session_analyses", payload.analyses],
  ] as const) {
    const rows = items.map((item, ordinal) => ({
      table,
      row: {
        org_id: identity.orgId,
        workspace_id: identity.workspaceId,
        source: identity.source,
        session_id: identity.sessionId,
        ordinal,
        payload: item,
        ...(table === "session_analyses" ? change?.row : {}),
      },
    }));
    const page = {
      ...put(table + "-page", {
        version: 1,
        kind: "row-page",
        identity,
        generation,
        table,
        view: "raw",
        start: 0,
        rows,
      }),
      kind: "page",
      start: 0,
      count: rows.length,
    };
    const root = {
      ...put(table + "-root", {
        version: 1,
        kind: "range-node",
        identity,
        generation,
        table,
        view: "raw",
        unit: "rows",
        start: 0,
        count: rows.length,
        children: [page],
      }),
      kind: "node",
      start: 0,
      count: rows.length,
    };
    tables[table] = { raw: { count: items.length, root } };
  }
  const graph = put("graph", {
    version: 1,
    kind: "canonical-read-index",
    codec: "json-utf8-v1",
    identity,
    generation,
    source,
    prefix,
    legacyCoverage: null,
    tables,
  });
  const index = put("wrapper", {
    version: 1,
    kind: "session-analysis-index",
    codec: "canonical-analysis-graph-v1",
    projection: "read_session_analysis_v1/0031",
    outcome: change?.outcome ?? "ok",
    identity,
    source,
    generation,
    snapshotAt,
    snapshotId,
    snapshotSha256,
    graph,
    counts: { requests: 1, analyses: change?.count ?? 1 },
  });
  ack = {
    version: 1,
    exists: true,
    current: true,
    identity,
    source,
    generation,
    snapshotAt,
    snapshotId,
    snapshotSha256,
    validUntil,
    prefix,
    index,
  };
  deps = {
    signal: new AbortController().signal,
    now: () => Date.parse(snapshotAt) + 1000,
    publication: async () => {
      claims++;
      return ack;
    },
    read: async (key) => {
      reads++;
      const body = objects.get(key);
      if (!body) throw new Error("missing object");
      return new ReadableStream({
        start(c) {
          c.enqueue(body);
          c.close();
        },
      });
    },
  };
  return payload;
}
beforeEach(async () => {
  await fixture();
});
test("preserves actual public virtual IDs, array ordering, nested brief and status with final fence", async () => {
  const expected = await fixture();
  const result = await readSessionAnalysisCache(deps, identity);
  expect(result.payload).toEqual(expected);
  expect(claims).toBe(2);
  expect(reads).toBeGreaterThan(0);
});
test("missing/stale/foreign scopes refuse before any S3 bytes", async () => {
  for (const changed of [
    { current: false },
    { identity: { ...identity, workspaceId: "foreign" } },
    { snapshotAt: "2026-11-03T20:00:00Z" },
  ]) {
    await fixture();
    ack = { ...ack, ...changed };
    await expect(
      readSessionAnalysisCache(deps, identity),
    ).rejects.toBeDefined();
    expect(reads).toBe(0);
  }
});
test("absent authority requires unchanged second authenticated absence", async () => {
  const absent = {
    version: 1,
    exists: false,
    current: false,
    identity,
    source: { epoch: null, fingerprint: "f".repeat(64) },
  };
  deps.publication = async () => absent;
  expect((await readSessionAnalysisCache(deps, identity)).outcome).toBe(
    "not_found",
  );
  expect(reads).toBe(0);
  let n = 0;
  deps.publication = async () =>
    ++n === 1
      ? absent
      : { ...absent, source: { epoch: null, fingerprint: "e".repeat(64) } };
  await expect(readSessionAnalysisCache(deps, identity)).rejects.toMatchObject({
    reason: "source_generation_changed",
  });
});
test("private row tenant/source/session and ordinal cannot escape authenticated wrapper", async () => {
  for (const row of [
    { workspace_id: "foreign" },
    { source: "claude" },
    { session_id: "other" },
    { ordinal: 9 },
    { payload: { sessionId: "other" } },
  ]) {
    await fixture({ row });
    await expect(
      readSessionAnalysisCache(deps, identity),
    ).rejects.toMatchObject({
      reason:
        "ordinal" in row || "payload" in row
          ? "analysis_row_identity"
          : "artifact_row_identity",
    });
  }
});
test("count, not-found payload, wrapper digest and snapshot digest are enforced", async () => {
  await fixture({ count: 0 });
  await expect(readSessionAnalysisCache(deps, identity)).rejects.toMatchObject({
    reason: "analysis_history_bound",
  });
  await fixture({ outcome: "not_found" });
  await expect(readSessionAnalysisCache(deps, identity)).rejects.toMatchObject({
    reason: "analysis_index",
  });
  await fixture({ snapshotSha: "d".repeat(64) });
  await expect(readSessionAnalysisCache(deps, identity)).rejects.toMatchObject({
    reason: "analysis_snapshot_digest",
  });
  await fixture();
  objects.set(ack.index.key, bytes({ tampered: true }));
  await expect(readSessionAnalysisCache(deps, identity)).rejects.toBeDefined();
});
test("final current pointer must match every bound snapshot and index field", async () => {
  const original = ack;
  deps.publication = async () =>
    ++claims === 1
      ? original
      : { ...original, index: { ...original.index, sha256: "c".repeat(64) } };
  await expect(readSessionAnalysisCache(deps, identity)).rejects.toMatchObject({
    reason: "source_generation_changed",
  });
});
