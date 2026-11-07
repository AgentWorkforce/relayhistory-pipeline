/**
 * The PR-evidence worker (SERVICE=session-pr-evidence). Off unless
 * SESSION_PR_EVIDENCE_ENABLED=true, and even then it claims nothing until the
 * operator enables sessions.session_pr_evidence_settings (migration 044000):
 * deploying it changes nothing on its own.
 *
 * The 044000 worker RPCs are pinned to bulls96_session_ingest (never the owner or
 * service_role), so evidence work runs on its own ingest-actor connection
 * (SESSION_PR_EVIDENCE_DATABASE_URL, a 1Password-sourced runtime secret; never on
 * disk) and verifies `current_user` before any RPC. INGEST_EXPECT_GENERATION is the
 * reviewed schema pin used to read history masks. Reads the scrubbed canonical raw
 * from the Locker (read-only).
 */
import pg from "pg";
import { lockerManifestReader, lockerStore } from "../shared/locker.ts";
import { runWorker } from "../shared/worker.ts";
import { log } from "../shared/log.ts";
import { prEvidenceStore } from "./store.ts";
import { prEvidenceTick } from "./worker.ts";

const enabled = process.env.SESSION_PR_EVIDENCE_ENABLED === "true";
const prefix = (process.env.PIPELINE_KEY_PREFIX ?? "").replace(/^\/+|\/+$/g, "");
const generation = process.env.INGEST_EXPECT_GENERATION ?? "";
const url = process.env.SESSION_PR_EVIDENCE_DATABASE_URL ?? "";
if (enabled && (!prefix || !/^[0-9a-f]{64}$/.test(generation) || !url))
  throw new Error("SESSION_PR_EVIDENCE_ENABLED=true requires PIPELINE_KEY_PREFIX, INGEST_EXPECT_GENERATION and SESSION_PR_EVIDENCE_DATABASE_URL");
const pool = enabled ? new pg.Pool({ connectionString: url, max: 2, application_name: "96bulls-session-pr-evidence" }) : null;
pool?.on("error", () => log.error("pr evidence pool error", { stage: "session-pr-evidence" }));
let verified = false;
const actor = {
  async query<R = Record<string, unknown>>(text: string, params?: unknown[]) {
    if (!pool) throw new Error("pr_evidence_disabled");
    if (!verified) {
      const who = await pool.query<{ actor: string }>("SELECT current_user::text AS actor");
      if (who.rows[0]?.actor !== "bulls96_session_ingest") throw new Error("pr_evidence_actor_invalid");
      verified = true;
    }
    return { rows: (await pool.query(text, params as unknown[])).rows as R[] };
  },
};
log.info("session pr evidence worker", { stage: "session-pr-evidence", ok: enabled });
await runWorker({
  service: "session-pr-evidence",
  stages: [],
  handlers: () => ({}),
  tick: async () => {
    if (!enabled) return;
    const raw = lockerStore();
    await prEvidenceTick({ store: prEvidenceStore(actor), reader: lockerManifestReader(), objects: raw.store,
      prefixFor: (org) => `${prefix}/${org}`, generation });
  },
});
