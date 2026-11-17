/** Seal-pool test fixture: a worker that exits on its first task, so the pool must fall back and respawn. */
import { parentPort } from "node:worker_threads";
parentPort!.on("message", () => process.exit(3));
