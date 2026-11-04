import { lockerManifestReader } from "../shared/locker.ts";
import { runWorker } from "../shared/worker.ts";
import { refereeHandler } from "./handler.ts";
import { expectedSchema } from "../hoopdb/migrate.ts";

const publication = process.env.SESSION_MANAGED_DERIVED_ENABLED === "true"
  ? { schemaGeneration: (await expectedSchema()).generation } : {};

await runWorker({
  service: "referee",
  stages: ["status-classify"],
  handlers: (db) => ({ "status-classify": refereeHandler({ db, reader: lockerManifestReader(), ...publication }) }),
});
