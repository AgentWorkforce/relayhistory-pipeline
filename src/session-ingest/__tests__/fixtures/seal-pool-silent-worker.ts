/** Seal-pool test fixture: a worker that never answers, so a seal stays in flight until the pool closes. */
import { parentPort } from "node:worker_threads";
parentPort!.on("message", () => {});
