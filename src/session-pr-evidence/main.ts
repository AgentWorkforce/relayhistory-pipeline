/**
 * The PR-evidence worker (SERVICE=session-pr-evidence). Off unless
 * SESSION_PR_EVIDENCE_ENABLED=true, and even then it claims nothing until the
 * operator enables sessions.session_pr_evidence_settings (migration 044000):
 * deploying it changes nothing on its own. Reads the scrubbed canonical raw from
 * the Locker (read-only) and writes HoopDb only through the 044000 RPCs.
 */
import { lockerManifestReader, lockerStore } from "../shared/locker.ts";
import { runWorker } from "../shared/worker.ts";
import { log } from "../shared/log.ts";
import { prEvidenceStore } from "./store.ts";
import { prEvidenceTick } from "./worker.ts";

const enabled = process.env.SESSION_PR_EVIDENCE_ENABLED === "true";
const prefix = (process.env.PIPELINE_KEY_PREFIX ?? "").replace(/^\/+|\/+$/g, "");
if (enabled && !prefix) throw new Error("PIPELINE_KEY_PREFIX is required when SESSION_PR_EVIDENCE_ENABLED=true");
log.info("session pr evidence worker", { stage: "session-pr-evidence", ok: enabled });
await runWorker({
  service: "session-pr-evidence",
  stages: [],
  handlers: () => ({}),
  tick: async (db) => {
    if (!enabled) return;
    const raw = lockerStore();
    await prEvidenceTick({ store: prEvidenceStore(db), reader: lockerManifestReader(), objects: raw.store, prefixFor: (org) => `${prefix}/${org}` });
  },
});
