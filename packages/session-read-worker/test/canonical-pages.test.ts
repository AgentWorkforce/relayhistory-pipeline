import { expect, it } from "vitest";
import {
  canonicalHandle,
  canonicalPublicationReader,
} from "../src/canonical.ts";
import { sha256Hex } from "../../object-store/src/crypto.ts";
import type { ArtifactReadDeps } from "../../../src/session-read-api/canonical-artifact-reader.ts";
const identity = {
  orgId: "org-a",
  workspaceId: "ws-a",
  source: "codex",
  sessionId: "会話:1",
};
const generation = "b".repeat(64),
  source = { epoch: "1", fingerprint: "a".repeat(64) },
  prefix =
    "96bulls/org-a/v1/ws=ws-a/m=m/h=codex/d=2026/11/01/s=sess_1/normalized/read-projection-v1/" +
    "c".repeat(64) +
    "/" +
    generation +
    "/";
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
const request = (suffix = "") =>
  new Request(
    "https://read.test/v1/canonical/sessions/" +
      encodeURIComponent(identity.sessionId) +
      "?workspace=ws-a&source=codex&view=replay&limit=2" +
      suffix,
    { headers: { authorization: "Bearer " + token } },
  );
const forbidden = () => {
  throw new Error("raw/Neon transport forbidden");
};
async function fixture() {
  const objects = new Map<string, Uint8Array>();
  let reads = 0,
    claims = 0;
  async function put(name: string, v: unknown) {
    const bytes = new TextEncoder().encode(JSON.stringify(v)),
      key = prefix + name;
    objects.set(key, bytes);
    return { key, sha256: await sha256Hex(bytes), bytes: bytes.length };
  }
  const rows = [1, 2].map((n) => ({
    table: "convergence_events",
    row: {
      org_id: identity.orgId,
      workspace_id: identity.workspaceId,
      source: identity.source,
      session_id: identity.sessionId,
      event_id: "e" + n,
      machine_id: "machine:1",
      kind: "assistant",
      ts: "2026-11-01T10:00:00.00000" + n + "Z",
      content: "hello" + n,
      task_ref: { id: "task1" },
      input_tokens: 7,
      record: { payload: { message_id: "native" + n } },
    },
  }));
  const page = await put("page.json", {
    version: 1,
    kind: "row-page",
    identity,
    generation,
    table: "convergence_events",
    view: "replay",
    start: 0,
    rows,
  });
  const index = await put("index.json", {
    version: 1,
    kind: "canonical-read-index",
    codec: "json-utf8-v1",
    identity,
    generation,
    source,
    prefix,
    legacyCoverage: null,
    tables: {
      convergence_events: {
        replay: {
          count: 2,
          root: { ...page, start: 0, count: 2, kind: "page" },
        },
      },
    },
  });
  let ack: any = {
    version: 1,
    exists: true,
    current: true,
    identity,
    generation,
    source,
    prefix,
    index,
  };
  const deps: ArtifactReadDeps = {
    signal: new AbortController().signal,
    publication: async () => {
      claims++;
      return ack;
    },
    read: async (key) => {
      reads++;
      const bytes = objects.get(key);
      if (!bytes) throw new Error("not found");
      return new ReadableStream({
        start(c) {
          c.enqueue(bytes);
          c.close();
        },
      });
    },
  };
  return {
    deps,
    counts: () => ({ reads, claims }),
    setAck: (value: unknown) => {
      ack = value;
    },
  };
}
it("serves selected page mode in real workerd without entering the raw provider", async () => {
  const f = await fixture();
  const response = await canonicalHandle(
    request(),
    { CANONICAL_READ_ENABLED: "true", CANONICAL_READ_MODE: "pages" },
    forbidden,
    { pointers: forbidden } as never,
    f.deps,
  );
  expect(response.status).toBe(200);
  const body: any = await response.json();
  expect(body).toMatchObject({
    orgId: identity.orgId,
    workspaceId: identity.workspaceId,
    sessionId: identity.sessionId,
    generation,
    exists: true,
  });
  expect(body.rows.map((x: any) => x.row.event_id)).toEqual(["e1", "e2"]);
  expect(body.rows[0].row.record.payload.message_id).toBe("native1");
  expect(f.counts()).toEqual({ reads: 2, claims: 2 });
});
it("refuses stale/foreign selected publications before object or raw reads", async () => {
  for (const ack of [
    { version: 1, exists: true, current: false, identity, source },
    {
      version: 1,
      exists: true,
      current: true,
      identity: { ...identity, orgId: "foreign" },
      source,
    },
  ]) {
    const f = await fixture();
    f.setAck(ack);
    const response = await canonicalHandle(
      request(),
      { CANONICAL_READ_ENABLED: "true", CANONICAL_READ_MODE: "pages" },
      forbidden,
      { pointers: forbidden } as never,
      f.deps,
    );
    expect(response.status).toBe(503);
    expect(f.counts().reads).toBe(0);
  }
});
it("keeps selected mode dormant and rejects unknown mode before source I/O", async () => {
  for (const env of [
    { CANONICAL_READ_MODE: "pages" },
    { CANONICAL_READ_ENABLED: "true", CANONICAL_READ_MODE: "typo" },
  ]) {
    const f = await fixture();
    const response = await canonicalHandle(
      request(),
      env,
      forbidden,
      { pointers: forbidden } as never,
      f.deps,
    );
    expect(response.status).toBe(503);
    expect(f.counts()).toEqual({ reads: 0, claims: 0 });
  }
});
it("sends the same claim bearer and exact tenant key to the publication RPC", async () => {
  let calls = 0;
  const reader = canonicalPublicationReader(
    {
      CANONICAL_SUPABASE_URL: "https://metadata.test/rest/v1",
      CANONICAL_SUPABASE_API_KEY: "public-gateway",
    },
    token,
    new AbortController().signal,
    (async (input, init) => {
      calls++;
      expect(String(input)).toBe(
        "https://metadata.test/rest/v1/rpc/session_directory_replay_pages",
      );
      expect(new Headers(init?.headers).get("authorization")).toBe(
        "Bearer " + token,
      );
      expect(JSON.parse(String(init?.body))).toEqual({
        p_workspace_id: "ws-a",
        p_harness: "codex",
        p_session_id: identity.sessionId,
      });
      return Response.json({
        version: 1,
        exists: false,
        current: false,
        identity,
        source: { epoch: null, fingerprint: "d".repeat(64) },
      });
    }) as typeof fetch,
  );
  expect(await reader(identity)).toMatchObject({ exists: false });
  expect(calls).toBe(1);
});
