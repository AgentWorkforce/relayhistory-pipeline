/**
 * WriteTarget.onManifestCommitted (writer.ts): the per-session FIFO revision event fires after each manifest
 * CAS commits, with the PUT ack's stamps, and is never sent for a replay; a throwing hook never fails the
 * write (the forwarder carries the event from the write's own S3 notification). Synthetic data only.
 */
import { afterEach, expect, test } from "bun:test";
import { objectKey } from "../../shared/vendor.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { lastChunkProof, writeBatchObjects, type WriteTarget } from "../writer.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

const cleanup: Array<() => void> = [];
afterEach(() => cleanup.splice(0).forEach((f) => f()));

async function write(target: WriteTarget, n: number) {
  const p = await prepareDelivery(TENANT, await batch([record({ record_id: `record${n}`, revision: 1, operation: "upsert", revision_id: `rev-${n}`,
    payload: { text: `synthetic ${n}`, ts_ms: 1761955200000 + n } })], { batch_id: `batch-${n}` }));
  return writeBatchObjects(target, { orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, userId: TENANT.userId, originId: p.originId, batchDigest: p.batchDigest, records: p.records });
}

test("fires once per committed manifest with revision, ack stamps and commit time; never for a replay", async () => {
  const fs = scratchStore();
  cleanup.push(fs.done);
  const seen: unknown[] = [];
  const target: WriteTarget = { store: fs.store, keyRoot: "96bulls", sleep: async () => {}, now: () => new Date("2026-11-01T10:00:00.000Z"),
    onManifestCommitted: async (c) => { seen.push(...c); } };
  const first = await write(target, 1);
  const second = await write(target, 2);
  await write(target, 2); // the same batch again: a replay, nothing new committed
  const key = first.manifests[0]!.key;
  const etag = (await fs.store.head(objectKey(key)))!.version.eTag!.replace(/^"|"$/g, "");
  expect(seen).toEqual([
    { key, revision: 1, committedAt: "2026-11-01T10:00:00.000Z", etag: expect.any(String), chunk: lastChunkProof(first.manifests[0]!.manifest) },
    { key, revision: 2, committedAt: "2026-11-01T10:00:00.000Z", etag, chunk: lastChunkProof(second.manifests[0]!.manifest) },
  ]);
  expect(second.manifests[0]!.manifest.revision).toBe(2);
  // The real writer is cumulative: revision 2 still lists the chunk revision 1 appended, and appends its own. This is
  // the premise of the consumer's 'revision covered' proof.
  const [c1, c2] = seen as { chunk: string }[];
  expect(c1!.chunk).toMatch(/^0:[0-9a-f]{64}$/);
  expect(c2!.chunk).toMatch(/^1:[0-9a-f]{64}$/);
  expect(second.manifests[0]!.manifest.raw.chunks.map((c) => `${c.seq}:${c.sha256}`)).toEqual([c1!.chunk, c2!.chunk]);
});

test("a throwing hook never fails the write", async () => {
  const fs = scratchStore();
  cleanup.push(fs.done);
  const out = await write({ store: fs.store, keyRoot: "96bulls", sleep: async () => {}, onManifestCommitted: async () => { throw new Error("synthetic sqs down"); } }, 1);
  expect(out.manifests[0]!.written).toBe(true);
});
