import { lockerManifestReader, lockerStore } from "../shared/locker.ts";
import { runWorker } from "../shared/worker.ts";
import { bobCostasHandler } from "./handler.ts";
import { summarizerFromEnv } from "./model-summarizer.ts";
import { log } from "../shared/log.ts";
import { keyringFromEnv } from "../shared/keyring.ts";
import { summarySweep } from "./sweep.ts";
import { bobCostasRegenFromEnv } from "./regen.ts";
import { readerMaxRuleset } from "../../packages/pipeline/src/lib/scrub-engine/reader-policy.ts";

const raw = lockerStore();
// BOB_COSTAS_MODEL / OPENAI_BASE_URL are validated here, so a bad value fails at startup.
const summarizer = summarizerFromEnv();
log.info("bob costas summariser", { stage: "summary", model_id: summarizer.id });
// Summaries are stored encrypted in the Locker; refuse to start without a keyring.
const keyring = await keyringFromEnv();
// Unset keeps legacy reads; a malformed value fails at startup.
const maximum = readerMaxRuleset(process.env);
// Summaries regeneration flags (all default off: null keeps today's code path); read once, shared.
const regen = bobCostasRegenFromEnv();
await runWorker({
  service: "bob-costas",
  stages: ["summary-short", "summary-long"],
  attests: ["summaries"],
  // Queues summaries for indexed sessions that have none (e.g. after migration 0004).
  tick: summarySweep({
    keyring,
    regen,
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
      regen,
    };
    return {
      "summary-short": bobCostasHandler("short", deps),
      "summary-long": bobCostasHandler("long", deps),
    };
  },
});
