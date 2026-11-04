import { beforeEach, expect, test } from "bun:test";
import { sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import {
  canonicalArtifactPage,
  canonicalArtifactNodePage,
  artifactOrdinalCursor,
  type ArtifactReadDeps,
} from "../canonical-artifact-reader.ts";
import {
  ARTIFACT_TABLES,
  type ArtifactIdentity,
  type ArtifactRangeRef,
  type ArtifactIndex,
  type ArtifactEntry,
} from "../canonical-artifact.ts";
const identity: ArtifactIdentity = {
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
const encode = (v: unknown) => new TextEncoder().encode(JSON.stringify(v));
let objects: Map<string, Uint8Array>,
  ack: any,
  index: ArtifactIndex,
  deps: ArtifactReadDeps,
  readKeys: string[],
  cancelled: number,
  controller: AbortController;
async function put(
  name: string,
  body: Uint8Array,
  start: number,
  count: number,
  kind: ArtifactRangeRef["kind"],
): Promise<ArtifactRangeRef> {
  const key = prefix + name;
  objects.set(key, body);
  return {
    key,
    sha256: await sha256Hex(body),
    bytes: body.byteLength,
    start,
    count,
    kind,
  };
}
async function node(
  name: string,
  unit: "rows" | "bytes" | "utf16",
  children: ArtifactRangeRef[],
  extra: Record<string, unknown> = {},
) {
  const start = children[0]!.start,
    count = children.reduce((n, c) => n + c.count, 0);
  return put(
    name,
    encode({
      version: 1,
      kind: "range-node",
      identity,
      generation,
      unit,
      start,
      count,
      children,
      ...extra,
    }),
    start,
    count,
    "node",
  );
}
async function publish() {
  const ref = await put("index.json", encode(index), 0, 0, "node");
  ack = {
    version: 1,
    exists: true,
    current: true,
    identity,
    generation,
    source,
    prefix,
    index: { key: ref.key, sha256: ref.sha256, bytes: ref.bytes },
  };
}
const original = "prefix😀\ud800tail".replace("\\ud800", "\ud800");
beforeEach(async () => {
  objects = new Map();
  readKeys = [];
  cancelled = 0;
  controller = new AbortController();
  const pieces = [original.slice(0, 7), original.slice(7)],
    strings: ArtifactRangeRef[] = [];
  let at = 0;
  for (let i = 0; i < pieces.length; i++) {
    strings.push(
      await put(
        "string-" + i + ".json",
        encode(pieces[i]),
        at,
        pieces[i]!.length,
        "segment",
      ),
    );
    at += pieces[i]!.length;
  }
  const stringRoot = await node("string-node.json", "utf16", strings);
  const big = {
    source: "codex",
    session_id: identity.sessionId,
    event_id: "e2",
    machine_id: "m",
    kind: "tool_call",
    ts: "2026-11-01T10:00:00.000002Z",
    tool_calls: Array.from({ length: 15000 }, (_, i) => ({
      id: String(i),
      arguments: { kept: true },
    })),
    content: "tools intact",
  };
  const blob = encode(big),
    blobRefs: ArtifactRangeRef[] = [];
  for (let i = 0; i < blob.byteLength; i += 65536)
    blobRefs.push(
      await put(
        "blob-" + i + ".json",
        blob.slice(i, i + 65536),
        i,
        Math.min(65536, blob.byteLength - i),
        "segment",
      ),
    );
  const blobRoot = await node("blob-node.json", "bytes", blobRefs);
  const entries: ArtifactEntry[] = [
    {
      table: "convergence_events",
      row: {
        source: "codex",
        session_id: identity.sessionId,
        event_id: "e1",
        machine_id: "m",
        kind: "assistant",
        ts: "2026-11-01T10:00:00.000001Z",
        content: null,
        task_ref: { id: "task1" },
        input_tokens: 7,
        record: { payload: { message_id: "native1" } },
      },
      content: {
        codec: "json-string-segments-v1",
        chars: original.length,
        jsonSha256: await sha256Hex(encode(original)),
        root: stringRoot,
      },
    },
    {
      table: "convergence_events",
      rowJson: {
        codec: "json-utf8-segments-v1",
        bytes: blob.byteLength,
        sha256: await sha256Hex(blob),
        root: blobRoot,
      },
    },
    {
      table: "convergence_events",
      row: {
        source: "codex",
        session_id: identity.sessionId,
        event_id: "e3",
        machine_id: "m",
        kind: "assistant",
        ts: "2026-11-01T10:00:00.000003Z",
        content: "last",
      },
    },
  ];
  const page1 = await put(
    "page-1.json",
    encode({
      version: 1,
      kind: "row-page",
      identity,
      generation,
      table: "convergence_events",
      view: "replay",
      start: 0,
      rows: entries.slice(0, 2),
    }),
    0,
    2,
    "page",
  );
  const page2 = await put(
    "page-2.json",
    encode({
      version: 1,
      kind: "row-page",
      identity,
      generation,
      table: "convergence_events",
      view: "replay",
      start: 2,
      rows: entries.slice(2),
    }),
    2,
    1,
    "page",
  );
  const root = await node("row-node.json", "rows", [page1, page2], {
    table: "convergence_events",
    view: "replay",
  });
  index = {
    version: 1,
    kind: "canonical-read-index",
    codec: "json-utf8-v1",
    identity,
    generation,
    source,
    prefix,
    legacyCoverage: {
      version: 1,
      tables: ["conversation_turns", "session_links", "session_outcomes"],
    },
    tables: Object.fromEntries(
      ARTIFACT_TABLES.map((table) => [
        table,
        { raw: { count: 0, root: null } },
      ]),
    ),
  };
  index.tables.convergence_events!.replay = { count: 3, root };
  const boundRows = [entries[0]!.row!, big, entries[2]!.row!].map((row) => ({
    table: "convergence_events",
    row: Object.fromEntries(
      ["source", "session_id", "event_id", "machine_id", "kind", "ts"].map(
        (key) => [key, row[key]],
      ),
    ),
  }));
  const boundPage = await put(
    "bounds-page.json",
    encode({
      version: 1,
      kind: "row-page",
      identity,
      generation,
      table: "convergence_events",
      view: "bounds",
      start: 0,
      rows: boundRows,
    }),
    0,
    3,
    "page",
  );
  index.tables.convergence_events!.bounds = { count: 3, root: boundPage };
  await publish();
  deps = {
    signal: controller.signal,
    publication: async () => ack,
    read: async (key) => {
      readKeys.push(key);
      const body = objects.get(key);
      if (!body) throw new Error("unexpected object");
      return new ReadableStream({
        start(c) {
          c.enqueue(body);
          c.close();
        },
        cancel() {
          cancelled++;
        },
      });
    },
  };
});
const query = (extra: Record<string, unknown> = {}) => ({
  table: "convergence_events",
  view: "replay" as const,
  limit: 1,
  ...extra,
});
test("reads ordinal pages with exact native identity and backend/index-bound cursors", async () => {
  const first = await canonicalArtifactPage(deps, identity, query());
  expect(first.rows[0]!.row).toMatchObject({
    event_id: "e1",
    content: original,
    task_ref: { id: "task1" },
    input_tokens: 7,
    record: { payload: { message_id: "native1" } },
  });
  expect(first.nextCursor).toStartWith("artifact-v1:");
  const second = await canonicalArtifactPage(
    deps,
    identity,
    query({ cursor: first.nextCursor }),
  );
  expect(second.rows[0]!.row.event_id).toBe("e2");
  expect((second.rows[0]!.row.tool_calls as unknown[]).length).toBe(15000);
  const third = await canonicalArtifactPage(
    deps,
    identity,
    query({ cursor: second.nextCursor }),
  );
  expect(third.rows[0]!.row.event_id).toBe("e3");
  expect(third.nextCursor).toBeNull();
  const descending = await canonicalArtifactPage(
    deps,
    identity,
    query({ order: "desc" }),
  );
  expect(descending.rows[0]!.row.event_id).toBe("e3");
  await expect(
    canonicalArtifactPage(
      deps,
      identity,
      query({ cursor: first.nextCursor, order: "desc" }),
    ),
  ).rejects.toMatchObject({ status: 400 });
});
test("maxContent prefix skips unused segments and preserves UTF16 split/lone surrogates", async () => {
  const result = await canonicalArtifactPage(
    deps,
    identity,
    query({ maxContent: 6 }),
  );
  expect(result.rows[0]!.row.content).toBe(original.slice(0, 7));
  expect(readKeys.some((k) => k.endsWith("string-0.json"))).toBe(true);
  expect(readKeys.some((k) => k.endsWith("string-1.json"))).toBe(false);
  const complete = await canonicalArtifactPage(deps, identity, query());
  expect(complete.rows[0]!.row.content).toBe(original);
});
test("refuses forged tenant publication before object access", async () => {
  ack.identity = { ...identity, workspaceId: "other" };
  await expect(
    canonicalArtifactPage(deps, identity, query()),
  ).rejects.toMatchObject({ reason: "artifact_publication" });
  expect(readKeys).toEqual([]);
});
test("refuses absent/stale publication without raw fallback", async () => {
  ack = { version: 1, exists: true, current: false, identity, source };
  await expect(
    canonicalArtifactPage(deps, identity, query()),
  ).rejects.toMatchObject({ reason: "artifact_not_ready" });
  expect(readKeys).toEqual([]);
  ack = {
    version: 1,
    exists: false,
    current: false,
    identity,
    source: { epoch: null, fingerprint: "d".repeat(64) },
  };
  expect(await canonicalArtifactPage(deps, identity, query())).toMatchObject({
    exists: false,
    rows: [],
    legacyCoverage: null,
    nextCursor: null,
  });
  expect(readKeys).toEqual([]);
});
test("validates every object digest before parsing or returning content", async () => {
  objects.set(
    prefix + "string-0.json",
    encode(original.slice(0, 7).replace("x", "z")),
  );
  await expect(
    canonicalArtifactPage(deps, identity, query({ maxContent: 6 })),
  ).rejects.toMatchObject({ reason: "artifact_digest" });
});
test("rejects incomplete/gapped ordinal ranges even with a newly acknowledged index hash", async () => {
  const root = index.tables.convergence_events!.replay!.root!;
  const value = JSON.parse(new TextDecoder().decode(objects.get(root.key)!));
  value.children[1].start = 3;
  const replacement = await put("bad-node.json", encode(value), 0, 3, "node");
  index.tables.convergence_events!.replay!.root = replacement;
  await publish();
  await expect(
    canonicalArtifactPage(deps, identity, query()),
  ).rejects.toMatchObject({ reason: "artifact_range" });
});
test("rejects source changes after hydration and changed index hashes on old cursors", async () => {
  let calls = 0;
  deps.publication = async () =>
    ++calls === 1 ? ack : { ...ack, source: { ...source, epoch: "2" } };
  await expect(
    canonicalArtifactPage(deps, identity, query()),
  ).rejects.toMatchObject({ reason: "source_generation_changed" });
  deps.publication = async () => ack;
  const first = await canonicalArtifactPage(deps, identity, query());
  index.legacyCoverage = null;
  await publish();
  await expect(
    canonicalArtifactPage(deps, identity, query({ cursor: first.nextCursor })),
  ).rejects.toMatchObject({ status: 400 });
});
test("rejects cross-prefix object references even when digest is valid", async () => {
  ack.index = { ...ack.index, key: "other-tenant/index.json" };
  await expect(
    canonicalArtifactPage(deps, identity, query()),
  ).rejects.toMatchObject({ reason: "artifact_reference" });
  expect(readKeys).toEqual([]);
});
test("aborts stalled artifact streams without waiting for cancellation to resolve", async () => {
  let seen = false;
  deps.read = async () =>
    new ReadableStream({
      start() {},
      cancel() {
        seen = true;
        return new Promise(() => {});
      },
    });
  const pending = canonicalArtifactPage(deps, identity, query());
  setTimeout(() => controller.abort(), 1);
  await expect(pending).rejects.toMatchObject({ reason: "deadline" });
  expect(seen).toBe(true);
});

test("rechecks authenticated absence and binds expected generation on empty results", async () => {
  const absent = {
    version: 1,
    exists: false,
    current: false,
    identity,
    source: { epoch: null, fingerprint: "d".repeat(64) },
  };
  let calls = 0;
  deps.publication = async () => (++calls === 1 ? absent : ack);
  await expect(
    canonicalArtifactPage(deps, identity, query()),
  ).rejects.toMatchObject({ reason: "source_generation_changed" });
  expect(readKeys).toEqual([]);
  deps.publication = async () => absent;
  await expect(
    canonicalArtifactPage(
      deps,
      identity,
      query({ generation: "e".repeat(64) }),
    ),
  ).rejects.toMatchObject({ reason: "source_generation_changed" });
});
test("rejects acknowledged reversed event rows using the shared byte comparator", async () => {
  const key = prefix + "page-1.json",
    value = JSON.parse(new TextDecoder().decode(objects.get(key)!));
  value.rows.reverse();
  const page = await put("reversed-page.json", encode(value), 0, 2, "page");
  const root = index.tables.convergence_events!.replay!.root!,
    parent = JSON.parse(new TextDecoder().decode(objects.get(root.key)!));
  parent.children[0] = page;
  index.tables.convergence_events!.replay!.root = await put(
    "reversed-node.json",
    encode(parent),
    0,
    3,
    "node",
  );
  await publish();
  await expect(
    canonicalArtifactPage(deps, identity, query({ limit: 2 })),
  ).rejects.toMatchObject({ reason: "artifact_order" });
});

test("seeks inclusive precise timestamp ranges through bounds without scanning content", async () => {
  const since = "2026-11-01T10:00:00.000002Z",
    until = since;
  const middle = await canonicalArtifactPage(
    deps,
    identity,
    query({ since, until, limit: 3 }),
  );
  expect(middle.rows.map((x) => x.row.event_id)).toEqual(["e2"]);
  expect(middle.nextCursor).toBeNull();
  expect(readKeys.some((k) => k.endsWith("string-0.json"))).toBe(false);
  readKeys = [];
  const latest = await canonicalArtifactPage(
    deps,
    identity,
    query({ since, order: "desc" }),
  );
  expect(latest.rows[0]!.row.event_id).toBe("e3");
  const older = await canonicalArtifactPage(
    deps,
    identity,
    query({ since, order: "desc", cursor: latest.nextCursor }),
  );
  expect(older.rows[0]!.row.event_id).toBe("e2");
  await expect(
    canonicalArtifactPage(
      deps,
      identity,
      query({
        since: "2026-11-01T10:00:00.000001Z",
        order: "desc",
        cursor: latest.nextCursor,
      }),
    ),
  ).rejects.toMatchObject({ status: 400 });
  expect(
    (
      await canonicalArtifactPage(
        deps,
        identity,
        query({ since: "2026-11-01T10:00:00.000004Z" }),
      )
    ).rows,
  ).toEqual([]);
});
test("refuses missing/misaligned timestamp bounds instead of an unbounded replay scan", async () => {
  delete index.tables.convergence_events!.bounds;
  await publish();
  await expect(
    canonicalArtifactPage(deps, identity, query({ since: "2026-11-01" })),
  ).rejects.toMatchObject({ reason: "artifact_bounds" });
  expect(readKeys.some((k) => k.endsWith("page-1.json"))).toBe(false);
});

test("reader-owned ordinal seeks bind view, index, generation and byte-exact row position", async () => {
  const cursor = artifactOrdinalCursor(identity, ack, { table: "convergence_events", view: "replay", order: "asc", position: 1 });
  const page = await canonicalArtifactPage(deps, identity, query({ cursor }));
  expect(page.rows[0]!.row.event_id).toBe("e2");
  await expect(canonicalArtifactPage(deps, identity, query({ cursor, order: "desc" }))).rejects.toMatchObject({ status: 400 });
  await expect(canonicalArtifactPage(deps, identity, query({ cursor, view: "raw" }))).rejects.toMatchObject({ status: 400 });
  const changed = artifactOrdinalCursor(identity, { ...ack, index: { ...ack.index, sha256: "f".repeat(64) } }, { table: "convergence_events", view: "replay", position: 1 });
  await expect(canonicalArtifactPage(deps, identity, query({ cursor: changed }))).rejects.toMatchObject({ status: 400 });
  expect(() => artifactOrdinalCursor(identity, ack, { table: "convergence_events", position: -1 })).toThrow();
});

test("trusted Node raw entry preserves multiMiB arbitrary native fields while Worker raw refuses", async () => {
  const body = { source: identity.source, session_id: identity.sessionId, event_id: "large-native", machine_id: "m", kind: "tool_call", ts: "2026-11-01T10:00:00.000001Z", record: { payload: { native: "😀".repeat(600_000), tools: Array.from({length: 10_001}, (_, i) => ({id: i, nested: {keep: true}})) } } };
  const raw = encode(body), parts: ArtifactRangeRef[] = [];
  for (let at = 0; at < raw.length; at += 65536) parts.push(await put("raw-blob-" + at, raw.slice(at, at + 65536), at, Math.min(65536, raw.length - at), "segment"));
  const root = await node("raw-blob-node.json", "bytes", parts);
  const page = await put("raw-row.json", encode({ version: 1, kind: "row-page", identity, generation, table: "convergence_events", view: "raw", start: 0, rows: [{ table: "convergence_events", rowJson: { codec: "json-utf8-segments-v1", bytes: raw.length, sha256: await sha256Hex(raw), root } }] }), 0, 1, "page");
  index.tables.convergence_events!.raw = { count: 1, root: page };
  await publish();
  const cursor = artifactOrdinalCursor(identity, ack, { table: "convergence_events", view: "raw", position: 0 });
  const q = { table: "convergence_events", view: "raw" as const, limit: 1, cursor };
  await expect(canonicalArtifactPage(deps, identity, q)).rejects.toMatchObject({ reason: "row_bound" });
  const got = await canonicalArtifactNodePage(deps, identity, q);
  expect(raw.length).toBeGreaterThan(2 * 1024 ** 2);
  expect(got.rows[0]!.row).toEqual(body);
  expect(got.nextCursor).toBeNull();
  const old = deps.publication; let reads = 0;
  deps.publication = async (...args) => { const value = await old(...args); return ++reads === 1 ? value : {...value as any, generation: "f".repeat(64)} };
  await expect(canonicalArtifactNodePage(deps, identity, q)).rejects.toMatchObject({ reason: "source_generation_changed" });
});
