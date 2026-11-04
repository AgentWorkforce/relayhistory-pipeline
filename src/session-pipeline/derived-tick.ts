/** One bounded, default-off Lambda tick over the existing Postgres stage queue.
 * No daemon, no transient migration-runner dependency, no new queue backend. */
import {
  pipelineQueueDeps, postgresPipelineQueue, postgresDeadLetterStore, pollPostgresStage,
  type ManifestReader, type ObjectStore,
} from "../shared/vendor.ts";
import { schemaVerdict, inTransaction, type HoopDb } from "../shared/db.ts";
import { derivedSchemaAssets } from "./derived-schema.ts";
import { sqlPipelineEventSink } from "../hoopdb/sink.ts";
import { refereeHandler } from "../referee/handler.ts";
import { bobCostasHandler } from "../bob-costas/handler.ts";
import { summarizerFromEnv } from "../bob-costas/summarizer.ts";
import { summarySweep } from "../bob-costas/sweep.ts";
import type { WorkspaceKeyring } from "../bob-costas/summary-objects.ts";

export async function derivedTick(input: {
  db: HoopDb; store: ObjectStore; reader: ManifestReader; keyring?: WorkspaceKeyring;
  /** Lambda context deadline; leave time for acknowledgment and pool cleanup. */
  remainingMs: () => number;
  sweep?: boolean;
}) {
  const { db, reader, store, keyring } = input;
  const assets = await derivedSchemaAssets();
  const publication = process.env.SESSION_MANAGED_DERIVED_ENABLED === "true"
    ? { schemaGeneration: assets.expected.generation } : {};
  const gate = async () => (await schemaVerdict(assets.expected, work => inTransaction(db, work), { verifierSql: assets.verifiers })).ok;
  const summarizer = summarizerFromEnv();
  const deps = pipelineQueueDeps({ ENVIRONMENT: "production" }, {
    queue: postgresPipelineQueue(db), deadLetters: postgresDeadLetterStore(db), sink: sqlPipelineEventSink(db),
    handlers: {
      "status-classify": refereeHandler({ db, reader, ...publication }),
      ...(keyring ? {
        "summary-short": bobCostasHandler("short", { db, reader, store, summarizer, keyring, ...publication, maxRawBytes: 64 * 1024 * 1024 }),
        "summary-long": bobCostasHandler("long", { db, reader, store, summarizer, keyring, ...publication, maxRawBytes: 64 * 1024 * 1024 }),
      } : {}),
    },
  });
  const result = { claimed: 0, acked: 0, retried: 0, exhausted: 0, gated: false, summaryKeyringReady: !!keyring };
  for (const stage of ["status-classify", ...(keyring ? ["summary-short", "summary-long"] as const : [])] as const) {
    if (input.remainingMs() < 60_000 || !(await gate())) { result.gated = true; return result; }
    const r = await pollPostgresStage({ sql: db, stage, deps, batchSize: 1, leaseSeconds: 300 });
    result.claimed += r.claimed; result.acked += r.acked; result.retried += r.retried; result.exhausted += r.exhausted;
  }
  // Explicit second opt-in: never flood historical jobs on first activation.
  if (keyring && input.sweep && input.remainingMs() >= 60_000 && await gate())
    await summarySweep({ keyring, limit: 10 })(db, new Date());
  return result;
}
