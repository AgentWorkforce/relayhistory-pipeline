import { manifestReaderFromEnv, type Bindings } from "../shared/vendor.ts";
import { vendorBindings } from "../shared/env.ts";
import { runWorker } from "../shared/worker.ts";
import { refereeHandler } from "./handler.ts";

const env = vendorBindings() as unknown as Bindings;
await runWorker({
  service: "referee",
  stages: ["status-classify"],
  handlers: (db) => ({ "status-classify": refereeHandler({ db, reader: manifestReaderFromEnv(env)! }) }),
});
