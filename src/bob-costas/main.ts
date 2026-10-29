import { lockerManifestReader, lockerStore } from "../shared/locker.ts";
import { runWorker } from "../shared/worker.ts";
import { bobCostasHandler } from "./handler.ts";
import { summarizerFromEnv } from "./summarizer.ts";

const raw = lockerStore();
const summarizer = summarizerFromEnv();
await runWorker({
  service: "bob-costas",
  stages: ["summary-short", "summary-long"],
  handlers: (db) => {
    const deps = { db, reader: lockerManifestReader(), store: raw.store, summarizer };
    return {
      "summary-short": bobCostasHandler("short", deps),
      "summary-long": bobCostasHandler("long", deps),
    };
  },
});
