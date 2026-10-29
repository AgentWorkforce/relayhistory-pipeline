import { manifestReaderFromEnv, rawStoreFromEnv, type Bindings } from "../shared/vendor.ts";
import { vendorBindings } from "../shared/env.ts";
import { runWorker } from "../shared/worker.ts";
import { bobCostasHandler } from "./handler.ts";
import { summarizerFromEnv } from "./summarizer.ts";

const env = vendorBindings() as unknown as Bindings;
const raw = rawStoreFromEnv(env);
if (!raw) throw new Error("96bulls: LOCKER_* env is required for bob-costas");
const summarizer = summarizerFromEnv();
await runWorker({
  service: "bob-costas",
  stages: ["summary-short", "summary-long"],
  handlers: (db) => {
    const deps = { db, reader: manifestReaderFromEnv(env)!, store: raw.store, summarizer };
    return {
      "summary-short": bobCostasHandler("short", deps),
      "summary-long": bobCostasHandler("long", deps),
    };
  },
});
