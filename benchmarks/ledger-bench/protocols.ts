/**
 * Step 3: the two commit protocols replayed with synthetic objects. These are not the production code
 * paths. They issue the same request sequence with the same object sizes, so what is measured is the
 * protocol's shape (dependent round trips per committed batch) on each backend.
 */
import { isObjectStoreError } from "../../packages/object-store/src/errors.ts";
import type { ObjectStore } from "../../packages/object-store/src/types.ts";
import { DynamoError, type Dynamo } from "./aws.ts";
import { jsonDoc, key, KIB, listAll, randomBytes, readBody, readOptional, seqName } from "./context.ts";
import { errorClass, mapLimit } from "./load.ts";
import { Histogram, Tally, type LatencySummary } from "./stats.ts";

export const BATCH_KIB = 64;
const SHARDS_PER_STREAM = 16;
const SHARDS_PER_BATCH = 2;
const INBOX_POOL = 8;
const CAS_RETRIES = 8;

/** Dependent round trips one P13 batch walks when nothing is contended (see {@link p13Batch}). */
export const P13_ROUND_TRIPS = 17;

export async function seedP13(store: ObjectStore, root: string, stream: number, lanes: number): Promise<void> {
  const s = `${root}/st${stream}`;
  const small = (name: string) => store.put(key(s, name), jsonDoc({ name }, KIB));
  await Promise.all([small("fence.json"), small("owner.json"), small("journal.json")]);
  await mapLimit(Array.from({ length: SHARDS_PER_STREAM }, (_, i) => i), 16, (i) => store.put(key(s, "rev", `${i}.json`), jsonDoc({ shard: i }, 8 * KIB)));
  await mapLimit(Array.from({ length: INBOX_POOL }, (_, i) => i), 8, (i) => store.put(key(s, "inbox", i), randomBytes(BATCH_KIB * KIB)));
  await mapLimit(Array.from({ length: lanes }, (_, i) => i), 8, async (lane) => {
    await store.put(key(s, `sess${lane}`, "pointer.json"), jsonDoc({ lane }, KIB));
    await store.put(key(s, `sess${lane}`, "manifest.json"), jsonDoc({ lane, chunks: 0 }, 4 * KIB));
  });
}

/** GET then `If-Match` PUT, retried on a lost race: one revision-shard merge or manifest append. */
async function casUpdate(store: ObjectStore, k: ReturnType<typeof key>, size: number, retries: Tally, label: string): Promise<void> {
  for (let attempt = 0; ; attempt++) {
    const got = await store.getObject(k);
    await got.body.cancel();
    try {
      await store.put(k, jsonDoc({ attempt, nonce: crypto.randomUUID() }, size), { ifMatch: got.meta.version });
      return;
    } catch (error) {
      if (!isObjectStoreError(error, "precondition_failed") || attempt >= CAS_RETRIES) throw error;
      retries.add(label);
    }
  }
}

/**
 * One batch through today's per-batch chain, in the order of the group-commit design's §1 table for a
 * 1-session batch touching two revision shards. The numbered comments are that table's steps. Each `await`
 * line is one dependent round trip: 17 in all.
 */
export async function p13Batch(store: ObjectStore, root: string, stream: number, lane: number, batch: string, retries: Tally): Promise<void> {
  const s = `${root}/st${stream}`;
  const doc = key(s, "batches", `${batch}.json`);
  const hash = [...batch].reduce((a, c) => (a * 31 + c.charCodeAt(0)) >>> 0, 7);
  const shards = Array.from({ length: SHARDS_PER_BATCH }, (_, i) => key(s, "rev", `${(hash + i * 5) % SHARDS_PER_STREAM}.json`));
  const fence = key(s, "fence.json");
  await readBody(store, key(s, "inbox", hash % INBOX_POOL)); // 1 inbox object
  await Promise.all([readOptional(store, doc), readBody(store, fence)]); // 3 batch doc + fence
  await Promise.all(shards.map((k) => readBody(store, k))); // 4 revision shards
  await readBody(store, key(s, "owner.json")); // 5 ownership stub
  await readBody(store, fence); // 6 fence again
  const pending = await store.put(doc, jsonDoc({ batch, state: "pending" }, KIB), { ifNoneMatch: true }); // 7 claim
  await readBody(store, key(s, "journal.json")); // 8 privacy journal
  await readBody(store, key(s, `sess${lane}`, "pointer.json")); // 10 session pointer
  const manifest = key(s, `sess${lane}`, "manifest.json");
  const before = await store.getObject(manifest); // 10 manifest
  await before.body.cancel();
  await store.put(key(s, `sess${lane}`, "chunks", batch), randomBytes(BATCH_KIB * KIB)); // 10 chunk
  await store.put(manifest, jsonDoc({ batch }, 4 * KIB), { ifMatch: before.meta.version }); // 10 manifest CAS (one writer per session)
  await Promise.all([readBody(store, doc), readBody(store, fence)]); // 11 batch doc + fence
  await Promise.all(shards.map((k) => casUpdate(store, k, 8 * KIB, retries, "shard"))); // 14 revision merges: GET, then CAS PUT
  await store.put(doc, jsonDoc({ batch, state: "committed" }, KIB), { ifMatch: pending.version }); // 16 commit
  await store.put(key(s, "outcomes", `${batch}.json`), jsonDoc({ batch }, KIB)); // 17 outcome marker
  await store.put(key(s, "tombstones", `${batch}.json`), jsonDoc({ batch }, KIB)); // 17 inbox tombstone
}

export interface StreamWriter {
  stream: number;
  /** The last sequence number this writer knows is committed. */
  tail: number;
  /** The payload PUT already in flight for the next group (pipelined behind the current commit). */
  next?: { startedAt: number; done: Promise<string> };
  committed: number;
}

export interface CommitStats {
  /** Payload start to commit acknowledgement, per group. */
  commit: Histogram;
  groups: number;
}

/** How a group's commit step ended: committed, or the sequence number was already taken. */
export type CommitOutcome = "committed" | "lost";

/**
 * One PLOG group: an unconditioned payload PUT of `group` batches to a unique key, then a create-if-absent
 * PUT of log entry tail+1. The next group's payload PUT starts before this group's entry PUT, so the two
 * overlap. `commit` is the atomic step, so PDDB reuses this with a DynamoDB transaction instead.
 */
export async function logGroup(
  store: ObjectStore,
  root: string,
  writer: StreamWriter,
  group: number,
  stats: CommitStats,
  commit: (seq: number, payloadKey: string, batchIds: string[]) => Promise<CommitOutcome>,
): Promise<string> {
  const startPayload = (): NonNullable<StreamWriter["next"]> => {
    const k = key(root, `st${writer.stream}`, "payloads", crypto.randomUUID());
    return { startedAt: performance.now(), done: store.put(k, randomBytes(group * BATCH_KIB * KIB)).then(() => k) };
  };
  const current = writer.next ?? startPayload();
  writer.next = undefined;
  const payloadKey = await current.done;
  // Pipelining: the next group's payload goes out while this group commits.
  writer.next = startPayload();
  writer.next.done.catch(() => {});
  const seq = writer.tail + 1;
  const batchIds = Array.from({ length: group }, (_, i) => `${writer.stream}-${seq}-${i}-${crypto.randomUUID().slice(0, 8)}`);
  const outcome = await commit(seq, payloadKey, batchIds);
  writer.tail = seq;
  if (outcome === "lost") return "lost_sequence";
  writer.committed++;
  stats.groups++;
  stats.commit.record(performance.now() - current.startedAt);
  return "ok";
}

/** The object-store commit: create-if-absent of the numbered entry. An ambiguous failure is settled by reading the entry back. */
export function entryCommit(store: ObjectStore, root: string, stream: number, writerId: string): (seq: number, payloadKey: string, batchIds: string[]) => Promise<CommitOutcome> {
  return async (seq, payloadKey, batchIds) => {
    const k = key(root, `st${stream}`, "log", `${seqName(seq)}.json`);
    const body = jsonDoc({ seq, writer: writerId, payload: payloadKey, batches: batchIds }, KIB);
    try {
      await store.put(k, body, { ifNoneMatch: true });
      return "committed";
    } catch (error) {
      if (isObjectStoreError(error, "already_exists")) return "lost";
      if (!isObjectStoreError(error, "transient")) throw error;
      // Timeout or 5xx: the entry may have landed. Never assume it failed.
      const landed = await readOptional(store, k);
      if (landed === undefined) throw error;
      return new TextDecoder().decode(landed.body).includes(`"writer":"${writerId}"`) && new TextDecoder().decode(landed.body).includes(batchIds[0]!) ? "committed" : "lost";
    }
  };
}

const HEAD_SK = "head";

/**
 * The DynamoDB commit (0008 option E): one TransactWriteItems that advances the stream head only from the
 * expected sequence number, writes the numbered entry, and puts one item per batch id that must not exist.
 */
export function dynamoCommit(dynamo: Dynamo, pk: string, cancelled: Tally): (seq: number, payloadKey: string, batchIds: string[]) => Promise<CommitOutcome> {
  const TableName = dynamo.target.table;
  return async (seq, payloadKey, batchIds) => {
    const items: Record<string, unknown>[] = [
      {
        Update: {
          TableName,
          Key: { pk: { S: pk }, sk: { S: HEAD_SK } },
          UpdateExpression: "SET seq = :next",
          ConditionExpression: seq === 1 ? "attribute_not_exists(seq)" : "seq = :prev",
          ExpressionAttributeValues: { ":next": { N: String(seq) }, ...(seq === 1 ? {} : { ":prev": { N: String(seq - 1) } }) },
        },
      },
      { Put: { TableName, Item: { pk: { S: pk }, sk: { S: `e#${seqName(seq)}` }, payload: { S: payloadKey } }, ConditionExpression: "attribute_not_exists(pk)" } },
      ...batchIds.map((id) => ({ Put: { TableName, Item: { pk: { S: pk }, sk: { S: `b#${id}` }, seq: { N: String(seq) } }, ConditionExpression: "attribute_not_exists(pk)" } })),
    ];
    try {
      await dynamo.transactWrite(items);
      return "committed";
    } catch (error) {
      if (error instanceof DynamoError && error.exception === "TransactionCanceledException") {
        cancelled.add(error.errorClass);
        if (error.reasons.includes("ConditionalCheckFailed")) return "lost";
      }
      throw error;
    }
  };
}

export async function dynamoHeadSeq(dynamo: Dynamo, pk: string): Promise<number> {
  const head = await dynamo.get({ pk: { S: pk }, sk: { S: HEAD_SK } });
  const seq = head?.seq;
  return seq && "N" in seq ? Number(seq.N) : 0;
}

export interface LogCheck {
  entries: number;
  /** Sequence numbers missing between 1 and the highest entry. Must be 0. */
  gaps: number;
  /** Batch ids that appear in more than one entry. Must be 0. */
  duplicate_batches: number;
  by_writer: Record<string, number>;
}

/** Read a stream's whole log back: contiguous from 1, and no batch id committed twice. */
export async function checkLog(store: ObjectStore, root: string, stream: number, readEntries = true): Promise<LogCheck> {
  const prefix = key(root, `st${stream}`, "log");
  const keys = (await listAll(store, prefix)).sort();
  const seqs = keys.map((k) => Number(/(\d+)\.json$/.exec(k)?.[1] ?? Number.NaN));
  let gaps = 0;
  for (let i = 0; i < seqs.length; i++) if (seqs[i] !== i + 1) gaps++;
  const byWriter = new Tally();
  const seen = new Set<string>();
  let duplicates = 0;
  if (readEntries) {
    const docs = await mapLimit(keys, 16, async (k) => JSON.parse(new TextDecoder().decode(await readBody(store, key(k)))) as { writer: string; batches: string[] });
    for (const doc of docs) {
      byWriter.add(doc.writer);
      for (const id of doc.batches) {
        if (seen.has(id)) duplicates++;
        seen.add(id);
      }
    }
  }
  return { entries: keys.length, gaps, duplicate_batches: duplicates, by_writer: byWriter.toJSON() };
}

export interface RacingWritersResult {
  writers: number;
  batches_per_writer: number;
  log: LogCheck;
  /** Commits each writer believes it made; the sum must equal the log's entries. */
  believed: Record<string, number>;
  lost_sequences: number;
  errors: Record<string, number>;
  /** 0 when the log has no gap, no duplicate batch, and exactly the commits the writers believe. */
  violations: number;
}

/**
 * Two (or more) clients write one stream with the same epoch. Each has `batches` groups to commit and takes
 * the next sequence number it knows of; a loser sees the number taken and retries the same group at the next.
 * Exactly one commit per sequence number may land.
 */
export async function racingWriters(store: ObjectStore, root: string, stream: number, writers: number, batches: number): Promise<RacingWritersResult> {
  const believed = new Tally();
  const errors = new Tally();
  let lost = 0;
  let shared = 0; // The writers poll the same tail, as two consumers reading one stream head would.
  await Promise.all(
    Array.from({ length: writers }, async (_, w) => {
      const writerId = `w${w}`;
      const commit = entryCommit(store, root, stream, writerId);
      for (let b = 0; b < batches; b++) {
        const batchId = `${writerId}-b${b}`;
        for (let attempt = 0; attempt < batches * writers + 16; attempt++) {
          const seq = shared + 1;
          let outcome: CommitOutcome;
          try {
            outcome = await commit(seq, `payload-${batchId}`, [batchId]);
          } catch (error) {
            errors.add(errorClass(error));
            continue;
          }
          shared = Math.max(shared, seq);
          if (outcome === "committed") {
            believed.add(writerId);
            break;
          }
          lost++;
        }
      }
    }),
  );
  const log = await checkLog(store, root, stream);
  const believedTotal = believed.total();
  const mismatch = Object.entries(log.by_writer).some(([w, n]) => believed.get(w) !== n) || believedTotal !== log.entries;
  return {
    writers,
    batches_per_writer: batches,
    log,
    believed: believed.toJSON(),
    lost_sequences: lost,
    errors: errors.toJSON(),
    violations: log.gaps + log.duplicate_batches + (mismatch ? 1 : 0) + (log.entries === writers * batches ? 0 : 1),
  };
}

export function commitSummary(stats: CommitStats): { groups: number; commit_latency: LatencySummary; commit_histogram: [number, number][] } {
  return { groups: stats.groups, commit_latency: stats.commit.summary(), commit_histogram: stats.commit.buckets() };
}
