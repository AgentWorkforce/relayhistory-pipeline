/**
 * T126 ordered (FIFO) consumer with the REAL search stage (indexSearchScope, the legacy ClickHouse binding over
 * search-index-hosted-harness.ts) and SEARCH_INDEX_SKIP_CURRENT_ENABLED on. Pipeline review of 1cb42716: the
 * search stage's alreadyCurrent compares manifest keys and revision numbers only. After manifest A is replaced by
 * B at the same key and revision (B's notification lost; the revision fence keeps the pointer row at A), a delayed
 * A must NOT earn skip-current credit: the pass runs with skip-current off and indexes B before the ACK.
 * Synthetic, secret-free data only.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { computeContentSha256, objectKey, readAll, type ManifestV1 } from "../../shared/vendor.ts";
import { indexSearchScope } from "../search-index.ts";
import { dispatchSqsBatch, type RevisionWiring, type SqsRecord } from "../revision-dispatch.ts";
import { revisionMessage, type RevisionEvent } from "../revision-events.ts";
import { SCOPE, exportStreamScope, generate, type ExportedScope } from "../../session-read-api/__tests__/stream-fixture.ts";
import { closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";
import { hostedRig } from "./search-index-hosted-harness.ts";

const ARN = "arn:aws:sqs:us-east-1:000000000000:synthetic-revisions.fifo";
const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

let x: ExportedScope;
beforeAll(async () => {
  x = await exportStreamScope(generate(31), 1200);
}, 120_000);
afterAll(() => x?.dispose());

const wiring: RevisionWiring = {
  config: { queueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/synthetic-revisions.fifo", mode: "direct", busyRetrySeconds: 5 },
  sender: async () => ({ failed: [] }),
  changeVisibility: async () => {},
  now: () => Date.now(),
};

const etagOf = async (key: string) => (await x.store.head(objectKey(key as never)))!.version.eTag!.replace(/^"|"$/g, "");
const readManifest = async (key: string) =>
  JSON.parse(new TextDecoder().decode(await readAll(await x.store.get(objectKey(key as never))))) as ManifestV1;

let n = 0;
function record(event: RevisionEvent): SqsRecord {
  const m = revisionMessage(event)!;
  n += 1;
  return { messageId: `m${n}`, receiptHandle: `rh${n}`, body: m.MessageBody, eventSourceARN: ARN,
    attributes: { MessageGroupId: m.MessageGroupId, MessageDeduplicationId: m.MessageDeduplicationId, SentTimestamp: String(Date.now()), ApproximateReceiveCount: "1" } };
}

/** Replace the manifest at `key` with B: the SAME revision, its last chunk dropped (different content, new ETag). */
async function replaceSameRevision(key: string): Promise<{ manifest: ManifestV1; dropped: number }> {
  const a = await readManifest(key);
  expect(a.raw.chunks.length).toBeGreaterThan(1);
  const chunks = a.raw.chunks.slice(0, -1);
  const last = chunks[chunks.length - 1]!;
  const b = {
    ...a,
    raw: { ...a.raw, chunks, chunk_count: chunks.length, raw_bytes: chunks.reduce((s, c) => s + c.bytes, 0),
      stored_bytes: chunks.reduce((s, c) => s + c.stored_bytes, 0), content_sha256: await computeContentSha256(chunks),
      next_offset: last.start_offset + last.bytes },
  } as ManifestV1;
  await x.store.put(objectKey(key as never), new TextEncoder().encode(JSON.stringify(b)), { contentType: "application/json" });
  return { manifest: b, dropped: a.raw.chunks.length - chunks.length };
}

describe("ordered consumer, real search stage, skip-current on", () => {
  test("lost B, delayed A: the pass runs with skip-current OFF and indexes B before the ACK (Pipeline review of 1cb42716)", async () => {
    const r = await hostedRig(x, cleanup);
    const key = x.keys[0]!;
    const aEtag = await etagOf(key);
    const aRevision = (await readManifest(key)).revision;
    // A indexed by a complete pass.
    await r.tick({});
    expect((await indexSearchScope(SCOPE, r.search())).complete).toBe(true);
    const stored = (await r.db.pg.query<{ manifest_etag: string | null }>("SELECT manifest_etag FROM sessions.session_index WHERE manifest_key = $1", [key])).rows[0];
    expect(stored?.manifest_etag?.replace(/^"|"$/g, "")).toBe(aEtag);
    const indexedA = r.fake.dump();

    // B replaces A at the same key and revision; B's notification is lost.
    await replaceSameRevision(key);
    expect(await etagOf(key)).not.toBe(aEtag);

    // What B alone indexes, from scratch (the expected result).
    const fresh = await hostedRig(x, cleanup);
    await fresh.tick({});
    expect((await indexSearchScope(SCOPE, fresh.search())).complete).toBe(true);
    const indexedB = fresh.fake.dump();
    expect(indexedB).not.toEqual(indexedA);

    // The delayed A event alone, through the ordered consumer with skip-current ON.
    const event: RevisionEvent = { key, etag: aEtag, revision: aRevision, committedAt: new Date().toISOString(), source: "writer" };
    const result = await dispatchSqsBatch([record(event)], { ...r.consumer({ skipCurrent: true }) }, wiring);
    expect(result).toEqual({ batchItemFailures: [] });
    // Acked, and B (not A) is what the index now holds.
    expect(r.fake.dump()).toEqual(indexedB);
  }, 120_000);

  test("control: physically current (A still live), a delayed A is skipped by skip-current as before", async () => {
    const r = await hostedRig(x, cleanup);
    const key = x.keys[0]!;
    const aEtag = await etagOf(key);
    await r.tick({});
    expect((await indexSearchScope(SCOPE, r.search())).complete).toBe(true);
    const before = r.fake.dump();
    const reads = r.reads.count;
    const event: RevisionEvent = { key, etag: aEtag, revision: (await readManifest(key)).revision, committedAt: new Date().toISOString(), source: "writer" };
    const result = await dispatchSqsBatch([record(event)], r.consumer({ skipCurrent: true }), wiring);
    expect(result).toEqual({ batchItemFailures: [] });
    expect(r.fake.dump()).toEqual(before);
    // The pointer stage read the manifest; the search stage read nothing (skip-current held).
    expect(r.reads.count - reads).toBeLessThanOrEqual(1);
  }, 120_000);
});
