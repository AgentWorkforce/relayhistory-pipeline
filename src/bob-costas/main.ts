import { lockerManifestReader, lockerStore } from "../shared/locker.ts";
import { runWorker } from "../shared/worker.ts";
import { bobCostasHandler, sqlLongSummaryDirectory } from "./handler.ts";
import { summarizerFromEnv } from "./model-summarizer.ts";
import { log } from "../shared/log.ts";
import { keyringFromEnv } from "../shared/keyring.ts";
import { summarySweep } from "./sweep.ts";
import { readerMaxRuleset } from "../../packages/pipeline/src/lib/scrub-engine/reader-policy.ts";
import { SESSION_LONG_SUMMARY_PROJECTION_ENABLED, exactTrueFlag } from "../session-read-api/config.ts";

const raw = lockerStore();
// BOB_COSTAS_MODEL / OPENAI_BASE_URL are validated here, so a bad value fails at startup.
const summarizer = summarizerFromEnv();
log.info("bob costas summariser", { stage: "summary", model_id: summarizer.id });
// Summaries are stored encrypted in the Locker; refuse to start without a keyring.
const keyring = await keyringFromEnv();
// Unset keeps legacy reads; a malformed value fails at startup.
const maximum = readerMaxRuleset(process.env);
// Default off: only the exact string "true" projects the scrubbed long summary into HoopDb
// (migration 20261105060000). Unset leaves the handler exactly as before.
const projectLongSummary = exactTrueFlag(process.env[SESSION_LONG_SUMMARY_PROJECTION_ENABLED]);
if (projectLongSummary) log.info("bob costas long summary projection enabled", { stage: "summary" });
await runWorker({
  service: "bob-costas",
  stages: ["summary-short", "summary-long"],
  attests: ["summaries"],
  // Queues summaries for indexed sessions that have none (e.g. after migration 0004).
  tick: summarySweep({
    keyring,
    everyMs: Number(process.env.SUMMARY_SWEEP_SECONDS ?? 300) * 1000,
  }),
  handlers: (db) => {
    const deps = {
      db,
      reader: lockerManifestReader(),
      store: raw.store,
      summarizer,
      keyring,
      readerMaxRuleset: maximum,
      ...(projectLongSummary ? { longSummary: sqlLongSummaryDirectory(db) } : {}),
    };
    return {
      "summary-short": bobCostasHandler("short", deps),
      "summary-long": bobCostasHandler("long", deps),
    };
  },
});
