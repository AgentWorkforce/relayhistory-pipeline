/**
 * The `file-normalize` worker: projects RelayHistory exports into normalized
 * objects and sends each count on. Its database login is queue-limited
 * (`file_normalize_worker`, see docs/event-count-writeback.md), so it can't use
 * `runWorker`, which reads the schema ledger and the tables directly. It refuses to
 * start on a broader login, or while normalization is off.
 */
import { assertQueueLimitedRole, pollFileNormalize, RelayhistoryExportTransform } from "../shared/vendor.ts";
import { openHoopDb, waitForHoopDb } from "../shared/db.ts";
import { flag, integer } from "../shared/env.ts";
import { lockerManifestReader, lockerStore } from "../shared/locker.ts";
import { errorFields, log } from "../shared/log.ts";
import { initTelemetry, shutdownTelemetry } from "../shared/telemetry.ts";
import { instrumented, serveHealth, traced, type WorkerHealth } from "../shared/worker.ts";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

if (!flag("PIPELINE_FILE_NORMALIZE_ENABLED")) throw new Error("96bulls: PIPELINE_FILE_NORMALIZE_ENABLED is off");

process.env.OTEL_SERVICE_NAME ||= "file-normalizer";
initTelemetry("file-normalizer");
const health: WorkerHealth = { ready: false, lastTickAt: Date.now(), consecutiveDbFailures: 0 };
serveHealth(health, integer("WORKER_HEALTH_PORT", 8081));
const db = openHoopDb();
await waitForHoopDb(db);
await assertQueueLimitedRole(db);
health.ready = true;

const transform = new RelayhistoryExportTransform({ manifests: lockerManifestReader(), store: lockerStore().store });
const beat = { start: () => void (health.lastTickAt = Date.now()), end: () => void (health.lastTickAt = Date.now()) };
const wrap: Parameters<typeof pollFileNormalize>[0]["wrap"] = (handler) => instrumented(traced("file-normalize", handler), beat);
const idleMs = integer("WORKER_IDLE_MS", 1_000);
const batchSize = integer("WORKER_BATCH_SIZE", 10);
const leaseSeconds = integer("WORKER_LEASE_SECONDS", 300);

const stop = async () => {
  await shutdownTelemetry();
  await db.end();
  process.exit(0);
};
process.on("SIGTERM", stop);
process.on("SIGINT", stop);
log.info("worker started", { stage: "file-normalize" });

for (;;) {
  let claimed = 0;
  try {
    const r = await pollFileNormalize({ sql: db, transform, wrap, batchSize, leaseSeconds });
    claimed = r.claimed;
    health.consecutiveDbFailures = 0;
    if (claimed) log.info("poll", { stage: "file-normalize", claimed, acked: r.acked, retried: r.retried, exhausted: r.exhausted });
  } catch (err) {
    health.consecutiveDbFailures += 1;
    log.error("poll failed", { stage: "file-normalize", ...errorFields(err) });
  }
  health.lastTickAt = Date.now();
  if (claimed === 0) await sleep(idleMs);
}
