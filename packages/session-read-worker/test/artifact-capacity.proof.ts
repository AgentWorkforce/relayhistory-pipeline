import { expect, inject, it } from "vitest";
import { canonicalHandle } from "../src/canonical.ts";
import {
  canonicalArtifactPage,
  type ArtifactReadDeps,
} from "../../../src/session-read-api/canonical-artifact-reader.ts";
import { compareArtifactEventRows } from "../../../src/session-read-api/canonical-artifact.ts";
declare module "vitest" {
  export interface ProvidedContext {
    artifactFixtureUrl: string;
  }
}
const fixture = () => inject("artifactFixtureUrl");
const encodeCursor = (value: unknown) =>
  "artifact-v1:" +
  btoa(
    Array.from(new TextEncoder().encode(JSON.stringify(value)), (b) =>
      String.fromCharCode(b),
    ).join(""),
  );
async function setup() {
  const ack = (await (await fetch(fixture() + "/claim")).json()) as any;
  const deps: ArtifactReadDeps = {
    signal: new AbortController().signal,
    publication: async () => (await fetch(fixture() + "/claim")).json(),
    read: async (key) => {
      const response = await fetch(
        fixture() + "/object?" + new URLSearchParams({ key }),
      );
      if (!response.ok || !response.body) throw new Error("fixture missing");
      return response.body;
    },
  };
  const response = await fetch(
    fixture() + "/object?" + new URLSearchParams({ key: ack.index.key }),
  );
  const index = JSON.parse(
    new TextDecoder().decode(await response.arrayBuffer()),
  );
  const proof = (await (await fetch(fixture() + "/proof")).json()) as any;
  return { ack, index, deps, proof };
}
it("reads actual >=300001-event Node publication in bounded workerd pages", async () => {
  const { ack, index, deps } = await setup();
  const table = index.tables.convergence_events.replay;
  expect(table.count).toBeGreaterThanOrEqual(300001);
  const identity = ack.identity,
    query = {
      table: "convergence_events",
      view: "replay" as const,
      limit: 200,
      order: "asc" as const,
      maxContent: 80,
    };
  await fetch(fixture() + "/reset");
  const first = await canonicalArtifactPage(deps, identity, query);
  expect(first.rows.length).toBeGreaterThan(0);
  expect(first.nextCursor).toBeTruthy();
  expect(first.exists).toBe(true);
  for (let i = 1; i < first.rows.length; i++)
    expect(
      compareArtifactEventRows(first.rows[i - 1]!.row, first.rows[i]!.row),
    ).toBeLessThanOrEqual(0);
  const second = await canonicalArtifactPage(deps, identity, {
    ...query,
    cursor: first.nextCursor,
  });
  expect(
    compareArtifactEventRows(first.rows.at(-1)!.row, second.rows[0]!.row),
  ).toBeLessThanOrEqual(0);
  const offset = table.count - 1,
    cursor = encodeCursor({
      version: 1,
      identity,
      generation: ack.generation,
      indexSha256: ack.index.sha256,
      table: query.table,
      view: query.view,
      order: query.order,
      maxContent: query.maxContent,
      offset,
    });
  const last = await canonicalArtifactPage(deps, identity, {
    ...query,
    cursor,
  });
  expect(last.rows.length).toBe(1);
  expect(last.nextCursor).toBeNull();
  const stats = (await (await fetch(fixture() + "/stats")).json()) as any;
  expect(stats.maxBytes).toBeLessThanOrEqual(256 * 1024);
  expect(stats.requests).toBeLessThan(40);
  expect(stats.totalBytes).toBeLessThan(4 * 1024 * 1024);
});
it("serves full and prefix large replay rows with mandatory native fields through real Worker handler", async () => {
  const { ack, deps, proof } = await setup(),
    identity = ack.identity;
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
  async function read(maxContent?: number) {
    const params = new URLSearchParams({
      workspace: identity.workspaceId,
      source: identity.source,
      view: "replay",
      limit: "1",
      order: "asc",
    });
    if (maxContent) params.set("maxContent", String(maxContent));
    const large = proof.expectedPositions.reduce((a: any, b: any) =>
      a.contentChars > b.contentChars ? a : b,
    );
    params.set(
      "cursor",
      encodeCursor({
        version: 1,
        identity,
        generation: ack.generation,
        indexSha256: ack.index.sha256,
        table: "convergence_events",
        view: "replay",
        order: "asc",
        maxContent: maxContent ?? null,
        offset: large.position,
      }),
    );
    return canonicalHandle(
      new Request(
        "https://read.test/v1/canonical/sessions/" +
          encodeURIComponent(identity.sessionId) +
          "?" +
          params,
        { headers: { authorization: "Bearer " + token } },
      ),
      { CANONICAL_READ_ENABLED: "true", CANONICAL_READ_MODE: "pages" },
      () => {
        throw new Error("raw or Neon forbidden");
      },
      undefined,
      deps,
    );
  }
  const full = await read();
  expect(full.status).toBe(200);
  const body = (await full.json()) as any;
  const row = body.rows[0].row;
  expect(row.content.length).toBeGreaterThan(2 * 1024 * 1024);
  expect(row.record.payload.message_id).toBeTruthy();
  expect(row.tool_calls.length).toBeGreaterThan(0);
  const prefix = await read(80);
  expect(prefix.status).toBe(200);
  const bounded = (await prefix.json()) as any;
  expect(bounded.rows[0].row.content).toBe(row.content.slice(0, 81));
  expect(bounded.rows[0].row.tool_calls).toEqual(row.tool_calls);
  expect(bounded.rows[0].row.record.payload.message_id).toBe(
    row.record.payload.message_id,
  );
});

it("matches producer oracle at physical page boundaries/middle/end and seeks tied timestamp windows", async () => {
  const { ack, deps, proof } = await setup(),
    identity = ack.identity,
    query = {
      table: "convergence_events",
      view: "replay" as const,
      limit: 1,
      order: "asc" as const,
      maxContent: 80,
    };
  for (const expected of proof.expectedPositions) {
    const cursor = encodeCursor({
      version: 1,
      identity,
      generation: ack.generation,
      indexSha256: ack.index.sha256,
      table: query.table,
      view: query.view,
      order: query.order,
      maxContent: query.maxContent,
      offset: expected.position,
    });
    const result = await canonicalArtifactPage(deps, identity, {
      ...query,
      cursor,
    });
    expect(result.rows).toHaveLength(1);
    const row: any = result.rows[0]!.row;
    const { position, ...oracle } = expected;
    expect({
      eventId: row.event_id,
      machineId: row.machine_id,
      ts: row.ts,
      nativeMessageId: row.record?.payload?.message_id ?? null,
      contentChars: typeof row.content === "string" ? row.content.length : 0,
      toolCalls: Array.isArray(row.tool_calls) ? row.tool_calls.length : 0,
      inputTokens: row.input_tokens ?? 0,
    }).toEqual({
      ...oracle,
      contentChars: Math.min(expected.contentChars, 81),
    });
  }
  const timestamp = proof.expectedPositions[0].ts;
  const ordinary = await canonicalArtifactPage(deps, identity, {
    ...query,
    limit: 200,
  });
  await fetch(fixture() + "/reset");
  const bounded = await canonicalArtifactPage(deps, identity, {
    ...query,
    limit: 200,
    since: timestamp,
    until: timestamp,
  });
  expect(bounded.rows.map((x) => x.row.event_id)).toEqual(
    ordinary.rows.map((x) => x.row.event_id),
  );
  expect(bounded.rows.every((x) => x.row.ts === timestamp)).toBe(true);
  const stats = (await (await fetch(fixture() + "/stats")).json()) as any;
  expect(stats.requests).toBeLessThan(60);
  expect(stats.maxBytes).toBeLessThanOrEqual(256 * 1024);
  expect(stats.totalBytes).toBeLessThan(8 * 1024 * 1024);
  console.info(
    "canonical_artifact_capacity",
    JSON.stringify({
      events: proof.events,
      producerHead: proof.head,
      sourceRawBytes: proof.sourceRawBytes,
      windowObjectReads: stats.requests,
      windowBytes: stats.totalBytes,
      maxObjectBytes: stats.maxBytes,
      oraclePositions: proof.expectedPositions.length,
    }),
  );
});
