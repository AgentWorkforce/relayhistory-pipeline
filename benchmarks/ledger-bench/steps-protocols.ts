/** Steps 3, 4 and 5 of the plan: the commit protocols, S3 Express specifics and DynamoDB specifics. */
import { DynamoError, expressStore, type CredentialProvider, type Dynamo } from "./aws.ts";
import type { ExpressTarget } from "./aws.ts";
import { GROUP_SIZES, HOT_HEAD_LEVELS, logOpCap, STREAM_COUNTS, TRANSACT_ITEMS, TRANSACT_WRITERS, transactOpCap } from "./caps.ts";
import type { TargetId } from "./config.ts";
import { cell, jsonDoc, key, KIB, need, randomBytes, readBody, type Ctx } from "./context.ts";
import { errorClass } from "./load.ts";
import { checkLog, commitSummary, dynamoCommit, dynamoHeadSeq, entryCommit, logGroup, P13_ROUND_TRIPS, p13Batch, racingWriters, seedP13, SHARDS_PER_BATCH, type CommitStats, type StreamWriter } from "./protocols.ts";
import { Histogram, Tally } from "./stats.ts";

type BucketId = Exclude<TargetId, "R6">;

/** In-flight batches (P13) or stream writers (PLOG, PDDB) per client, as the plan sets. */
const CLIENT_CONCURRENCY = 8;

const summed = (checks: { entries: number; gaps: number; duplicate_batches: number }[]) => ({
  entries: checks.reduce((a, c) => a + c.entries, 0),
  gaps: checks.reduce((a, c) => a + c.gaps, 0),
  duplicate_batches: checks.reduce((a, c) => a + c.duplicate_batches, 0),
});

/** P13 on one target: 8 batches in flight, on 1 stream (a hot user) and spread over 64. */
export async function step3P13(ctx: Ctx, target: BucketId): Promise<void> {
  const store = need(ctx, target);
  for (const streams of STREAM_COUNTS) {
    const root = `${ctx.root}/s3/${target.toLowerCase()}/p13-s${streams}`;
    // One session per in-flight lane: a session's manifest has one writer, as in production.
    for (let s = 0; s < streams; s++) await seedP13(store, root, s, CLIENT_CONCURRENCY);
    const retries = new Tally();
    await cell(
      ctx,
      { step: 3, cell: "p13", target, params: { streams, in_flight: CLIENT_CONCURRENCY, round_trips: P13_ROUND_TRIPS, shards: SHARDS_PER_BATCH } },
      { concurrency: CLIENT_CONCURRENCY, seconds: ctx.profile.protocolSeconds, op: async (w, i) => void (await p13Batch(store, root, streams === 1 ? 0 : (w * 8 + (i % 8)) % streams, w, `w${w}-b${i}`, retries)) },
      () => ({ batches_per_op: 1, cas_retries: retries.toJSON() }),
    );
  }
}

/**
 * PLOG (object-store log commit) or PDDB (DynamoDB commit) on one target, at every group size and stream
 * count. One writer per stream; each of the 8 workers owns its share of the streams.
 */
export async function step3Log(ctx: Ctx, target: BucketId, protocol: "plog" | "pddb"): Promise<void> {
  const store = need(ctx, target);
  const dynamo = ctx.dynamo;
  if (protocol === "pddb" && !dynamo) throw new Error("ledger-bench: PDDB needs target R6");
  for (const streams of STREAM_COUNTS) {
    for (const group of GROUP_SIZES) {
      const root = `${ctx.root}/s3/${target.toLowerCase()}/${protocol}-s${streams}-g${group}`;
      const stats: CommitStats = { commit: new Histogram(), groups: 0 };
      const cancelled = new Tally();
      const writers: StreamWriter[] = Array.from({ length: streams }, (_, stream) => ({ stream, tail: 0, committed: 0 }));
      const commits = writers.map((w) => (protocol === "plog" ? entryCommit(store, root, w.stream, "w0") : dynamoCommit(dynamo!, `${root}/st${w.stream}`, cancelled)));
      const concurrency = Math.min(CLIENT_CONCURRENCY, streams);
      const result = await cell(
        ctx,
        // Every stream keeps one payload PUT pipelined, so up to `streams` payload PUTs are in flight.
        { step: 3, cell: protocol, target, params: { streams, group, writers: concurrency, pipelined_payloads: streams } },
        {
          concurrency,
          seconds: ctx.profile.protocolSeconds,
          maxOps: logOpCap(ctx.profile, group),
          op: (w, i) => {
            // Worker w owns streams w, w+8, w+16, ...: never two workers on one stream.
            const mine = Math.floor((streams - 1 - w) / concurrency) + 1;
            const writer = writers[w + (i % mine) * concurrency]!;
            return logGroup(store, root, writer, group, stats, commits[writer.stream]!);
          },
        },
        () => ({ batches_per_op: group, ...commitSummary(stats), cancelled: cancelled.toJSON() }),
      );
      // Let the pipelined payloads land before the readback.
      await Promise.all(writers.map((w) => w.next?.done.catch(() => {})));
      const committed = writers.reduce((a, w) => a + w.committed, 0);
      const check =
        protocol === "plog"
          ? summed(await Promise.all(writers.slice(0, 8).map((w) => checkLog(store, root, w.stream, group <= 4))))
          : { entries: (await Promise.all(writers.slice(0, 8).map((w) => dynamoHeadSeq(dynamo!, `${root}/st${w.stream}`)))).reduce((a, n) => a + n, 0), gaps: 0, duplicate_batches: 0 };
      const expected = writers.slice(0, 8).reduce((a, w) => a + w.committed, 0);
      ctx.emit({
        step: 3,
        cell: `${protocol}_readback`,
        target,
        params: { streams, group, checked_streams: Math.min(streams, 8), entries_read: protocol === "plog" && group <= 4 },
        result: { ...check, committed_all_streams: committed, committed_checked_streams: expected, violations: check.gaps + check.duplicate_batches + (check.entries === expected ? 0 : 1), batches_per_sec: Math.round(result.ops_per_sec * group * 100) / 100 },
      });
    }
  }
}

/** The racing-writer check on one target: 2 and 8 clients, one stream, same epoch. */
export async function step3Race(ctx: Ctx, target: BucketId): Promise<void> {
  const store = need(ctx, target);
  for (const writers of [2, 8]) {
    const result = await racingWriters(store, `${ctx.root}/s3/${target.toLowerCase()}/race-w${writers}`, 0, writers, Math.max(5, Math.floor(ctx.profile.raceRounds / writers)));
    ctx.emit({ step: 3, cell: "racing_writers", target, params: { writers }, result });
    ctx.log(`step 3 racing_writers ${target} writers ${writers}: violations ${result.violations} entries ${result.log.entries}`);
  }
}

const summaryOf = (values: number[]) => {
  const h = new Histogram();
  for (const v of values) h.record(v);
  return h.summary();
};

/** Step 4: CreateSession cost, behaviour across the 5-minute session expiry, the dual-AZ write and the single-key rule. */
export async function step4(ctx: Ctx, credentials: CredentialProvider, fetcher: typeof fetch, targets: { R4: ExpressTarget; R5: ExpressTarget }, foreignKmsKeyArn?: string): Promise<void> {
  const r4 = need(ctx, "R4");
  const r5 = need(ctx, "R5");
  const root = `${ctx.root}/s4`;
  const body = randomBytes(KIB);

  // CreateSession on its own, 20 times.
  const probe = expressStore(targets.R4, credentials, fetcher);
  for (let i = 0; i < 20 && !ctx.meter.exhausted; i++) await probe.createSession().catch(() => {});
  ctx.emit({ step: 4, cell: "create_session", target: "R4", params: { calls: probe.stats.createSessionCalls }, result: { latency: summaryOf(probe.stats.createSessionMs), failures: probe.stats.createSessionFailures } });

  // Steady load across at least two session refreshes. Any failed request at a boundary is a finding.
  if (ctx.profile.refreshSoakSeconds > 0) {
    const before = ctx.expressStats.R4?.createSessionCalls ?? 0;
    await cell(
      ctx,
      { step: 4, cell: "session_refresh_soak", target: "R4", params: { concurrency: 8, seconds: ctx.profile.refreshSoakSeconds, refresh_before_s: 60 } },
      {
        concurrency: 8,
        seconds: ctx.profile.refreshSoakSeconds,
        maxOps: Number.MAX_SAFE_INTEGER,
        // Paced to about 300 requests a second: the soak has to span two refreshes, not spend the budget.
        op: async (w, i) => {
          if (i % 2 === 0) await r4.put(key(root, "soak", (w * 131 + i) % 512), body);
          else await readBody(r4, key(root, "soak", (w * 131 + i - 1) % 512));
          await new Promise((resolve) => setTimeout(resolve, 20));
        },
      },
      () => ({ create_session_calls: (ctx.expressStats.R4?.createSessionCalls ?? 0) - before, create_session_failures: ctx.expressStats.R4?.createSessionFailures ?? 0 }),
    );
  }

  // The object-store package's own cache refreshes 300 s before expiry, which a 300 s session never clears.
  const eager = expressStore(targets.R4, credentials, fetcher, { refreshBeforeSeconds: 300 });
  await cell(ctx, { step: 4, cell: "package_refresh_window", target: "R4", params: { concurrency: 8, refresh_before_s: 300 } }, { concurrency: 8, maxOps: 4000, op: async (w, i) => void (await eager.store.put(key(root, "eager", (w * 131 + i) % 512), body)) }, () => ({
    create_session_calls: eager.stats.createSessionCalls,
    create_session_latency: summaryOf(eager.stats.createSessionMs),
  }));

  // WarpStream-style dual-AZ write: acknowledge when both directory buckets have the object.
  const r2 = ctx.stores.R2;
  for (const kib of [1, 64]) {
    const payload = randomBytes(kib * KIB);
    await cell(ctx, { step: 4, cell: "single_az_write", target: "R4", params: { concurrency: 8, kib } }, { concurrency: 8, op: async (w, i) => void (await r4.put(key(root, `single-${kib}k`, (w * 131 + i) % 1024), payload)) });
    let copied = 0;
    let copying = 0;
    await cell(
      ctx,
      { step: 4, cell: "dual_az_write", target: "R4+R5", params: { concurrency: 8, kib } },
      {
        concurrency: 8,
        op: async (w, i) => {
          const k = key(root, `dual-${kib}k`, (w * 131 + i) % 1024);
          await Promise.all([r4.put(k, payload), r5.put(k, payload)]);
          // The background copy to Standard, outside the acknowledged path and never more than 8 at once.
          if (r2 && copying < 8) {
            copying++;
            void r2.put(k, payload).then(() => copied++, () => {}).finally(() => copying--);
          }
        },
      },
      () => ({ background_copies_to_r2: copied }),
    );
  }

  // One customer-managed key per directory bucket: a put naming another key must be refused.
  if (foreignKmsKeyArn) {
    const other = expressStore(targets.R4, credentials, fetcher, { sseKmsKeyArn: foreignKmsKeyArn });
    let outcome: string;
    let violations = 0;
    try {
      await other.store.put(key(root, "foreign-key"), jsonDoc({ probe: "foreign-key" }, KIB));
      const stored = await r4.head(key(root, "foreign-key"));
      outcome = stored?.sseKmsKeyId === foreignKmsKeyArn ? "accepted_with_foreign_key" : "accepted_with_bucket_key";
      // Enforced only if the foreign key was not used; a refusal or the bucket's key both count.
      violations = outcome === "accepted_with_foreign_key" ? 1 : 0;
    } catch (error) {
      outcome = `refused_${errorClass(error)}`;
    }
    ctx.emit({ step: 4, cell: "single_kms_key", target: "R4", params: {}, result: { outcome, violations } });
  }
}

const THROTTLES = new Set(["ThrottlingException", "ProvisionedThroughputExceededException", "RequestLimitExceeded", "ThrottledException"]);

/** Step 5: TransactWriteItems sizes and contention, one hot stream head, and idempotency-token replay. */
export async function step5(ctx: Ctx): Promise<void> {
  const dynamo: Dynamo | undefined = ctx.dynamo;
  if (!dynamo) throw new Error("ledger-bench: step 5 needs target R6");
  const TableName = dynamo.target.table;
  const root = `${ctx.root}/s5`;

  for (const items of TRANSACT_ITEMS) {
    for (const writers of TRANSACT_WRITERS) {
      const pk = `${root}/tx-i${items}-w${writers}`;
      const cancelled = new Tally();
      const throttled = new Tally();
      let committed = 0;
      // Every writer commits against the head it last saw; a loser re-reads the head, as a real writer would.
      let seen = 0;
      await cell(
        ctx,
        { step: 5, cell: "transact_write", target: "R6", params: { items, writers } },
        {
          concurrency: writers,
          maxOps: transactOpCap(ctx.profile, items),
          op: async () => {
            const seq = seen + 1;
            const commit = dynamoCommit(dynamo, pk, cancelled);
            try {
              const outcome = await commit(seq, "payload", Array.from({ length: items - 2 }, (_, j) => `${seq}-${j}-${crypto.randomUUID().slice(0, 8)}`));
              if (outcome === "committed") {
                committed++;
                seen = Math.max(seen, seq);
                return "ok";
              }
              seen = Math.max(seen, await dynamoHeadSeq(dynamo, pk));
              return "lost_sequence";
            } catch (error) {
              if (error instanceof DynamoError && THROTTLES.has(error.exception)) throttled.add(error.exception);
              if (error instanceof DynamoError && error.exception === "TransactionCanceledException") {
                seen = Math.max(seen, await dynamoHeadSeq(dynamo, pk));
                return "cancelled";
              }
              throw error;
            }
          },
        },
        () => ({ cancelled: cancelled.toJSON(), throttled: throttled.toJSON() }),
      );
      const head = await dynamoHeadSeq(dynamo, pk);
      ctx.emit({ step: 5, cell: "transact_readback", target: "R6", params: { items, writers }, result: { head_seq: head, committed, violations: head === committed ? 0 : 1 } });
    }
  }

  // One hot item, driven as hard as the client can: where does the partition limit show?
  for (const inFlight of HOT_HEAD_LEVELS) {
    const hot = { pk: { S: `${root}/hot-c${inFlight}` }, sk: { S: "head" } };
    const throttled = new Tally();
    let applied = 0;
    await cell(
      ctx,
      { step: 5, cell: "hot_head", target: "R6", params: { in_flight: inFlight } },
      {
        concurrency: inFlight,
        seconds: Math.max(5, Math.floor(ctx.profile.cellSeconds / 2)),
        op: async () => {
          try {
            await dynamo.call("UpdateItem", { TableName, Key: hot, UpdateExpression: "ADD n :one", ExpressionAttributeValues: { ":one": { N: "1" } } });
            applied++;
          } catch (error) {
            if (error instanceof DynamoError && THROTTLES.has(error.exception)) throttled.add(error.exception);
            throw error;
          }
        },
      },
      () => ({ throttled: throttled.toJSON() }),
    );
    const n = (await dynamo.get(hot))?.n;
    const stored = n && "N" in n ? Number(n.N) : 0;
    ctx.emit({ step: 5, cell: "hot_head_readback", target: "R6", params: { in_flight: inFlight }, result: { stored, acknowledged: applied, violations: stored >= applied ? 0 : 1, unacknowledged_applied: stored - applied } });
  }

  // The same transaction sent twice with one idempotency token must apply once.
  const idem = { pk: { S: `${root}/idempotency` }, sk: { S: "counter" } };
  const token = crypto.randomUUID();
  const tx = [{ Update: { TableName, Key: idem, UpdateExpression: "ADD n :one", ExpressionAttributeValues: { ":one": { N: "1" } } } }];
  let replay = "ok";
  await dynamo.transactWrite(tx, token);
  try {
    await dynamo.transactWrite(tx, token);
  } catch (error) {
    replay = errorClass(error);
  }
  let mismatch = "accepted";
  try {
    await dynamo.transactWrite([{ Update: { TableName, Key: idem, UpdateExpression: "ADD n :two", ExpressionAttributeValues: { ":two": { N: "2" } } } }], token);
  } catch (error) {
    mismatch = errorClass(error);
  }
  const n = (await dynamo.get(idem))?.n;
  const stored = n && "N" in n ? Number(n.N) : 0;
  ctx.emit({ step: 5, cell: "idempotency_replay", target: "R6", params: {}, result: { replay, same_token_other_payload: mismatch, applied: stored, violations: stored === 1 && replay === "ok" ? 0 : 1 } });
}
