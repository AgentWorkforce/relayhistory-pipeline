/**
 * One-time reshard of the S3 upload ledger's revision index (ledger-s3.ts): the superseded 64-shard layout
 * (`ledger/revisions/o=<h(origin)>/r=<2 hex>.json`) merged into the 4096-shard one (`r=<3 hex>.json`).
 *
 * Why: a shard holds every record of its origin that hashes to it, so at 64 shards a long-lived origin's shards grew
 * to hundreds of KB and every admission read and rewrote ~half of them. The new layout is read from the moment the
 * new code runs; until this has run for a user, that user's earlier entries are absent from it, so a re-upload of an
 * already stored record is judged new (written again) rather than stale. Nothing is lost, and nothing is wrong for a
 * record re-sent with the same content; #250's equal-revision refusal only holds again once this has run.
 *
 * Each legacy entry goes through `mergeIndex` (s3LedgerObjects) under the user's CURRENT fence: max revision wins, an
 * equal revision keeps the stored entry, and an entry the fence has forgotten (a disable or reset since it was
 * written) is never carried over. So it is idempotent, safe to rerun, and safe while admissions run (their merges and
 * these commute). Legacy shards are never deleted or changed (nothing in the S3 ledger is deleted).
 *
 * Dry run (default) lists and validates only. A legacy shard that is unreadable or invalid is counted and skipped,
 * never guessed at. Codes and counts only in the summary: no keys or ids.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import { LEGACY_SHARD_KEY, admissionFenceView, revisionShard, s3LedgerObjects, validLegacyIndex, type FenceDoc, type IndexEntryInput } from "./ledger-s3.ts";

export interface ReshardSummary {
  /** Objects listed under the prefix. */
  listed: number;
  /** Legacy (two-hex-digit) revision shards found. */
  legacy_shards: number;
  /** Their entries. */
  entries: number;
  /** Entries merged into the new layout (execute), or that would be (dry run). */
  merged: number;
  would_merge: number;
  /** New-layout shards written or confirmed (execute). */
  shards: number;
  /** Legacy shards skipped: unreadable, invalid, or naming another origin than their key. */
  invalid: number;
  /** Entries whose merge failed (a store error or `ledger_revision_contended`); a rerun retries them. */
  failed: number;
}

const decoder = new TextDecoder();

async function mapLimit<T>(items: readonly T[], limit: number, fn: (item: T) => Promise<void>): Promise<void> {
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) await fn(items[next++]!);
  }));
}
const originHash = (originId: string) => createHash("sha256").update(originId).digest("hex").slice(0, 32);

export async function reshardRevisions(store: ObjectStore, keyRoot: string, opts: { prefix: string; execute: boolean; concurrency?: number }): Promise<ReshardSummary> {
  const summary: ReshardSummary = { listed: 0, legacy_shards: 0, entries: 0, merged: 0, would_merge: 0, shards: 0, invalid: 0, failed: 0 };
  const inbox = `${keyRoot}/.admission-inbox/`;
  if (!opts.prefix.startsWith(inbox)) throw new Error("reshard_prefix_refused");
  const concurrency = opts.concurrency ?? 8;
  if (!Number.isSafeInteger(concurrency) || concurrency < 1 || concurrency > 64) throw new Error("reshard_concurrency_refused");
  const io = s3LedgerObjects(store, keyRoot);
  const fences = new Map<string, Promise<FenceDoc>>();
  const fenceOf = (dir: string) => {
    let f = fences.get(dir);
    if (!f) { f = io.readFence(`${dir}/upload-fences.json`).then(admissionFenceView); fences.set(dir, f); }
    return f;
  };

  const legacy: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(opts.prefix.replace(/\/$/, "")), cursor);
    for (const item of page.items) {
      summary.listed++;
      const key = String(item.key);
      if (key.startsWith(inbox) && LEGACY_SHARD_KEY.test(key)) legacy.push(key);
    }
    cursor = page.nextCursor;
  } while (cursor);
  summary.legacy_shards = legacy.length;

  // 1. Read every legacy shard and regroup its entries by their new shard, so each new shard is merged ONCE
  //    (an origin's 64 legacy shards spread over all 4096 new ones).
  const groups = new Map<string, { dir: string; originId: string; entries: IndexEntryInput[] }>();
  await mapLimit(legacy, concurrency, async (key) => {
    const m = LEGACY_SHARD_KEY.exec(key)!;
    const dir = key.slice(0, key.indexOf("/ledger/revisions/"));
    let doc: unknown;
    try {
      doc = JSON.parse(decoder.decode(await readAll((await store.getObject(objectKey(key))).body)));
    } catch (error) {
      if (!isObjectStoreError(error, "not_found")) summary.invalid++;
      return;
    }
    if (!validLegacyIndex(doc) || doc.shard !== m[2] || originHash(doc.originId) !== m[1]) { summary.invalid++; return; }
    for (const [recordId, e] of Object.entries(doc.records)) {
      summary.entries++;
      const target = `${dir}/ledger/revisions/o=${m[1]}/r=${revisionShard(recordId)}.json`;
      const g = groups.get(target) ?? { dir, originId: doc.originId, entries: [] };
      if (g.originId !== doc.originId) { summary.invalid++; continue; } // two origins hashing to one o= (never expected)
      g.entries.push({ recordId, revision: e[0], revisionId: e[1], digest: e[2], sessionKey: e[3], epoch: e[4], resets: e[5] });
      groups.set(target, g);
    }
  });
  if (!opts.execute) {
    for (const g of groups.values()) summary.would_merge += g.entries.length;
    return summary;
  }
  // 2. Merge each new shard once, under its user's current fence (read once per user).
  await mapLimit([...groups], concurrency, async ([target, g]) => {
    try {
      await io.mergeIndex(target, await fenceOf(g.dir), g.originId, g.entries);
      summary.merged += g.entries.length;
      summary.shards++;
    } catch {
      summary.failed += g.entries.length;
    }
  });
  return summary;
}
