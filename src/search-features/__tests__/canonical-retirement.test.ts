import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { copyCanonicalEntry, type CanonicalImagePort } from "../canonical-cycle.ts";
import type { CanonicalJournalEntry, CanonicalPageAckPort } from "../canonical-version.ts";
import { featureRetirementCommands } from "../feature-retirement.ts";
import { eventFeatureSchemaStatements, EVENT_FEATURES_TABLE } from "../event-schema.ts";
import { briefFeatureSchemaStatements, BRIEF_FEATURES_TABLE } from "../brief-schema.ts";
import { documentFeatureSchemaStatements, DOCUMENT_FEATURES_TABLE } from "../document-schema.ts";
import { produceEventFeatures, produceBriefFeatures } from "../canonical-producer.ts";
import { produceDocumentFeatures } from "../document-producer.ts";
import { featureVersion } from "../source.ts";
import { publishEventFeatureSnapshots } from "../event-publisher.ts";
import { publishBriefFeatureSnapshots } from "../brief-publisher.ts";
import type { FeatureReader, FeatureSnapshot } from "../source.ts";
import { createClickHouseClient, type ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";

const HASH = "a".repeat(64), POINTER = "c".repeat(64), GEN = "e".repeat(64);
const REV = "9007199254740994";
const stamp = "2026-11-01T10:00:00.123456Z";
const key = { orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s" };
const eventKey = { ...key, machineId: "m", kind: "session_event", eventId: "e", ts: stamp };
const authority = { authorityId: "12345678-1234-1234-1234-123456789012", epoch: "7", fingerprint: HASH };
const entry: CanonicalJournalEntry = {
  kind: "retire", revision: REV, imageKey: "retirement-fixture",
  schemaGeneration: GEN, sourceHash: HASH, priorKeysetHash: POINTER,
  scope: { orgId: key.orgId, workspaceId: key.workspaceId, harness: key.source,
    sessionId: key.sessionId, generation: GEN },
  priorSourceAuthority: authority,
  pointerSet: { authorityId: authority.authorityId, hash: POINTER, count: "1" },
};
const hashRows = (rows: unknown[]) => createHash("sha256")
  .update(JSON.stringify(rows)).digest("hex");
function setup(eventCount = 1) {
  const calls: string[] = [], commands: Parameters<ClickHouseClient["command"]>[] = [];
  const images: CanonicalImagePort = {
    imageDescriptor: async () => ({ imageKey: entry.imageKey, sourceHash: HASH,
      pointerSetHash: POINTER, complete: true }),
    imagePages: async () => { throw Error("unexpected publish page"); },
    priorKeyPages: async (_entry, kind) => {
      const rows = kind === "event"
        ? Array.from({ length: eventCount }, (_, n) => ({ ...eventKey, eventId: "e" + n }))
        : [key];
      return { rows, cursor: null, source: entry.imageKey,
        pointerSetHash: POINTER, pageSha256: hashRows(rows) };
    },
  };
  const acks: CanonicalPageAckPort = {
    begin: async () => "started",
    resumeCursor: async () => ({ state: "empty", cursor: null }),
    ackPage: async ({ kind }) => { calls.push("ack:" + kind); },
    finish: async () => { calls.push("finish"); },
  };
  const ch: ClickHouseClient = {
    database: "retirement_fixture",
    insert: async (table) => { calls.push("insert:" + table); },
    command: async (...args) => {
      calls.push("delete:" + args[0].split(" ")[2]); commands.push(args);
    },
    select: async () => [],
  };
  const run = (guards?: Parameters<typeof copyCanonicalEntry>[6], signal?: AbortSignal,
    documentPool = true) => copyCanonicalEntry(images, acks, ch, { checkpoint: "0" },
      entry, "fixture-target", guards, signal, documentPool);
  return { calls, commands, images, acks, ch, run };
}

test("canonical retirement waits for exact-key deletes in all three pools before ACK/finish", async () => {
  const d = setup();
  assert.equal((await d.run()).kind, "copied");
  assert.deepEqual(d.calls, [
    "insert:session_event_features", "delete:session_event_features", "ack:event",
    "insert:session_brief_features", "delete:session_brief_features", "ack:brief",
    "insert:session_document_features", "delete:session_document_features", "ack:document", "finish",
  ]);
  for (const [sql, options] of d.commands) {
    assert.match(sql, /version<\{v0:UInt64\}/);
    assert.equal(options?.params?.v0, featureVersion(REV));
    assert.equal(options?.settings?.lightweight_deletes_sync, 2);
  }
  for (const field of ["org_id", "workspace_id", "source", "session_id", "machine_id", "kind", "event_id", "ts"])
    assert.ok(d.commands[0]![0].includes(field + "="));
  assert.ok(d.commands[0]![0].includes(",6,'UTC')"));
});

test("two-pool operation does not touch the document table", async () => {
  const d = setup();
  await d.run(undefined, undefined, false);
  assert.equal(d.commands.length, 2);
  assert.ok(d.calls.every((call) => !call.includes("document")));
});

test("live rows and empty terminal pages generate no DELETE", () => {
  assert.deepEqual(featureRetirementCommands(EVENT_FEATURES_TABLE, []), []);
  assert.deepEqual(featureRetirementCommands(BRIEF_FEATURES_TABLE, [{ deleted: 0 }]), []);
});

test("retirement SQL parameterizes routing values and preserves the strict version fence", () => {
  const value = "x' OR 1=1 --";
  const [command] = featureRetirementCommands(DOCUMENT_FEATURES_TABLE, [{
    org_id: value, workspace_id: "", source: "h", session_id: "s",
    deleted: 1, version: "18446744073709551615",
  }]);
  assert.ok(!command!.query.includes(value));
  assert.deepEqual(command!.options.params, { o0: value, w0: "", h0: "h", s0: "s", v0: "18446744073709551615" });
  assert.match(command!.query, /version<\{v0:UInt64\}/);
  assert.throws(() => featureRetirementCommands("foreign_table" as typeof EVENT_FEATURES_TABLE, []));
});

test("failed INSERT performs no delete, ACK or finish", async () => {
  const d = setup();
  d.ch.insert = async () => { throw Error("insert failed"); };
  await assert.rejects(d.run(), /insert failed/);
  assert.deepEqual(d.calls, []);
});

test("failed DELETE cannot ACK or finish; retry repeats the unacknowledged cleanup", async () => {
  const d = setup(), command = d.ch.command;
  d.ch.command = async () => { throw Error("delete failed"); };
  await assert.rejects(d.run(), /delete failed/);
  assert.deepEqual(d.calls, ["insert:session_event_features"]);
  d.calls.length = 0; d.ch.command = command;
  assert.equal((await d.run()).kind, "copied");
  assert.equal(d.calls[1], "delete:session_event_features");
  assert.equal(d.calls[2], "ack:event");
});

test("abort after INSERT prevents DELETE and ACK", async () => {
  const d = setup(), ctl = new AbortController();
  d.ch.insert = async () => { ctl.abort(); };
  assert.deepEqual(await d.run(undefined, ctl.signal), { kind: "refused", reason: "aborted" });
  assert.deepEqual(d.calls, []);
});

test("an in-flight DELETE must finish before its durable ACK", async () => {
  const d = setup();
  let release!: () => void, entered!: () => void;
  const deleting = new Promise<void>((resolve) => { entered = resolve; });
  const blocked = new Promise<void>((resolve) => { release = resolve; });
  const command = d.ch.command; let first = true;
  d.ch.command = async (...args) => {
    if (first) { first = false; entered(); await blocked; }
    await command(...args);
  };
  const running = d.run();
  await deleting;
  assert.deepEqual(d.calls, ["insert:session_event_features"]);
  release();
  assert.equal((await running).kind, "copied");
});

test("resume cleans only unacknowledged pages; historical ACKs require separate reconciliation", async () => {
  const d = setup();
  d.acks.resumeCursor = async (_entry, kind) => kind === "event"
    ? { state: "complete", cursor: null, terminalSha256: HASH }
    : { state: "resume", cursor: "prior-ack" };
  const prior = d.images.priorKeyPages;
  d.images.priorKeyPages = async (e, kind, cursor) => {
    assert.notEqual(kind, "event"); assert.equal(cursor, "prior-ack");
    return prior(e, kind, cursor);
  };
  assert.equal((await d.run()).kind, "copied");
  assert.ok(d.calls.every((call) => !call.includes("event")));
  assert.deepEqual(d.calls.slice(0, 3), ["insert:session_brief_features",
    "delete:session_brief_features", "ack:brief"]);
});

test("already-complete entries do not retroactively repair historical tombstone-only rows", async () => {
  const d = setup(); d.acks.begin = async () => "already_complete";
  d.images.imageDescriptor = async () => { throw Error("unexpected source read"); };
  assert.deepEqual(await d.run(), { kind: "copied", checkpoint: REV, rows: 0 });
  assert.deepEqual(d.calls, []);
});

for (const kind of ["event", "brief"] as const) {
  test(`legacy ${kind} retirement preserves exact cleanup and refuses source CAS on delete failure`, async () => {
    const reference = { kind, key: kind === "event" ? eventKey : key,
      revision: REV, sourceHash: HASH, deleted: true };
    const snapshot: FeatureSnapshot = { reference, row: null };
    const d = setup(); let reads = 0;
    const db: FeatureReader = { async query<T>() {
      reads++;
      return { rows: [{ page: { too_large: false, rows: [{ ordinal: 0,
        kind, key: reference.key, revision: REV, source_hash: HASH, deleted: true,
        payload: null, actual_hash: HASH }] } }] as T[] };
    } };
    const publish = kind === "event" ? publishEventFeatureSnapshots : publishBriefFeatureSnapshots;
    assert.deepEqual(await publish({ db, ch: d.ch }, [snapshot]), [reference]);
    assert.equal(reads, 1); assert.equal(d.commands.length, 1);
    assert.match(d.commands[0]![0], /version<\{v0:UInt64\}/);
    if (kind === "brief") assert.deepEqual(d.commands[0]![1]?.params,
      { o0: "org", w0: "ws", h0: "codex", s0: "s", v0: featureVersion(REV) });
    d.ch.command = async () => { throw Error("delete failed"); };
    await assert.rejects(publish({ db, ch: d.ch }, [snapshot]), { reason: "publication_failed" });
    assert.equal(reads, 1);
  });
}

test("lease loss between INSERT and DELETE prevents cleanup and ACK", async () => {
  const d = setup(); let gates = 0;
  assert.deepEqual(await d.run({ beforeWrite: async () => ++gates === 1 }),
    { kind: "refused", reason: "lease_lost" });
  assert.deepEqual(d.calls, ["insert:session_event_features"]);
});

test("abort during the delete lease gate prevents DELETE and ACK", async () => {
  const d = setup(), ctl = new AbortController(); let gates = 0;
  assert.deepEqual(await d.run({ beforeWrite: async () => {
    if (++gates === 2) ctl.abort(); return true;
  } }, ctl.signal), { kind: "refused", reason: "aborted" });
  assert.deepEqual(d.calls, ["insert:session_event_features"]);
});

test("abort during DELETE prevents ACK and later pools", async () => {
  const d = setup(), ctl = new AbortController();
  d.ch.command = async () => { ctl.abort(); };
  assert.deepEqual(await d.run(undefined, ctl.signal), { kind: "refused", reason: "aborted" });
  assert.deepEqual(d.calls, ["insert:session_event_features"]);
});

test("large canonical pages renew the lease for each bounded deletion batch", async () => {
  const d = setup(1000); let gates = 0;
  await d.run({ beforeWrite: async () => { gates++; return true; } });
  assert.equal(d.commands.length, 7); // five event batches, brief, document
  assert.equal(gates, 10); // three inserts and seven deletes
  for (const [sql] of d.commands) assert.ok(sql.split(" OR ").length <= 200);
  assert.equal(d.calls.indexOf("ack:event"), 6);
});

test("failure in a later deletion batch cannot acknowledge a partially cleaned page", async () => {
  const d = setup(201), command = d.ch.command; let count = 0;
  d.ch.command = async (...args) => {
    if (++count === 2) throw Error("second delete failed");
    await command(...args);
  };
  await assert.rejects(d.run(), /second delete failed/);
  assert.ok(d.calls.every((call) => !call.startsWith("ack:") && call !== "finish"));
});

// Explicit opt-in only; fixed loopback endpoint and uniquely owned synthetic DB.
// This proves ordinary SELECT visibility, not physical purge of inactive parts.
test("real ClickHouse: retirement removes older raw rows, keeps other tenants and newer resurrection", {
  skip: process.env.CANONICAL_RETIREMENT_CH_PROOF !== "1",
}, async () => {
  const config = { url: "http://127.0.0.1:32771", user: "default", password: "",
    database: "default" };
  const admin = createClickHouseClient(config);
  const database = "canonical_retirement_233_" + randomUUID().replaceAll("-", "");
  await admin.command(`CREATE DATABASE ${database}`);
  const ch = createClickHouseClient({ ...config, database });
  try {
    for (const sql of [...eventFeatureSchemaStatements(), ...briefFeatureSchemaStatements(),
      ...documentFeatureSchemaStatements()]) await ch.command(sql);
    const evidence = { schemaGeneration: GEN, imageKey: entry.imageKey,
      revision: "9007199254740993", sourceHash: HASH };
    const event = { ...eventKey, eventId: "e0", ingestedAt: stamp, userId: "u",
      actorName: null, actorRole: null, projectId: null, projectFolded: null,
      taskTitle: "secret", taskTitleFolded: "secret", content: "secret",
      contentFolded: "secret", taskRefText: "{}", taskRefTextFolded: "{}",
      tags: [], embedding: null, embeddingModel: null, contentHash: null };
    const brief = { ...key, title: "secret", description: "secret", titleFolded: "secret",
      descriptionFolded: "secret", briefCompletedAt: stamp,
      embedding: null, embeddingModel: null, contentHash: "secret" };
    const doc = { ...key, title: "secret", summary: "secret", titleFolded: "secret",
      summaryFolded: "secret", projectId: null, lastActivityAt: stamp };
    const pools = [
      [EVENT_FEATURES_TABLE, produceEventFeatures([event], [], evidence)],
      [BRIEF_FEATURES_TABLE, produceBriefFeatures([brief], [], evidence)],
      [DOCUMENT_FEATURES_TABLE, produceDocumentFeatures([doc], [], evidence)],
    ] as const;
    for (const [table, produced] of pools) {
      assert.equal(produced.kind, "rows");
      if (produced.kind !== "rows") throw Error("invalid fixture");
      const old = produced.rows[0]!;
      await ch.insert(table, [old, { ...old, org_id: "other" },
        { ...old, version: featureVersion("9007199254740995"), source_revision: "9007199254740995" }]);
      const before = await ch.select<{ version: string }>(
        `SELECT toString(version) AS version FROM ${table} WHERE org_id='org'`);
      assert.ok(before.some((r) => r.version === featureVersion(evidence.revision)),
        "control must observe the older raw row before retirement");
    }
    const d = setup();
    const result = await copyCanonicalEntry(d.images, d.acks, ch, { checkpoint: "0" },
      entry, "local-proof", undefined, undefined, true);
    assert.equal(result.kind, "copied");
    for (const [table] of pools) {
      // Deliberately no FINAL and no deleted filter.
      const rows = await ch.select<{ org_id: string; version: string }>(
        `SELECT org_id,toString(version) AS version FROM ${table}`);
      assert.equal(rows.some((r) => r.org_id === "org" && r.version === featureVersion(evidence.revision)), false);
      assert.equal(rows.filter((r) => r.org_id === "other").length, 1);
      assert.equal(rows.filter((r) => r.org_id === "org" && r.version === featureVersion("9007199254740995")).length, 1);
    }
  } finally { await admin.command(`DROP DATABASE ${database} SYNC`); }
});
