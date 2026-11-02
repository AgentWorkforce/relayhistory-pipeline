/**
 * Seed the session ingest's committed-revision index for one org from Neon
 * (src/session-ingest/seed.ts; runbooks/session-ingest-rollout.md step 4).
 *
 *   NEON_URL_FILE=<(op read …) SUPABASE_SEED_URL_FILE=<(op read …) \
 *   EXPORT_STATE_FILE=~/.agent-tmp/s3x/state.sqlite \
 *     bun scripts/session-ingest-seed.ts <org_id>
 *
 * EXPORT_STATE_FILE: the Neon->S3 exporter's state (read only). Only sessions it
 * fully exported are seeded; the rest stay unstored so a re-send is written.
 *
 * NEON_URL_FILE: a read-only Neon URL (every read runs in READ ONLY transactions).
 * SUPABASE_SEED_URL_FILE: Supabase as a login granted bulls96_session_ingest_seeder
 * (the tool runs `SET ROLE bulls96_session_ingest_seeder` per call).
 * Credentials come only from those files (process substitution from 1Password);
 * nothing is written to disk and output is JSON counts only.
 */
import { readFileSync } from "node:fs";
import pg from "pg";
import { DatabaseSync } from "node:sqlite";
import {
  exportedFromState,
  seedFences,
  seedOrg,
  type SeedSql,
} from "../src/session-ingest/seed.ts";

const org = process.argv[2];
if (!org || !/^[A-Za-z0-9._-]{1,128}$/.test(org)) {
  console.error("usage: bun scripts/session-ingest-seed.ts <org_id>");
  process.exit(2);
}
const url = (name: string) => {
  const file = process.env[name];
  if (!file) throw new Error(`${name} is required`);
  return readFileSync(file, "utf8").trim();
};
const neon = new pg.Client({
  connectionString: url("NEON_URL_FILE"),
  application_name: "96bulls-session-ingest-seed",
});
const supa = new pg.Client({
  connectionString: url("SUPABASE_SEED_URL_FILE"),
  application_name: "96bulls-session-ingest-seed",
});
const statePath = process.env.EXPORT_STATE_FILE;
if (!statePath)
  throw new Error(
    "EXPORT_STATE_FILE is required (the exporter's state.sqlite)",
  );
const state = new DatabaseSync(statePath, { readOnly: true });
const exported = exportedFromState(
  state
    .prepare(
      `SELECT g.session_id, g.revision AS group_revision, e.revision AS exported_revision, e.error, e.failed_revision
         FROM groups g LEFT JOIN exported e ON e.org_id = g.org_id AND e.session_id = g.session_id
        WHERE g.org_id = ?`,
    )
    .all(org) as never,
);
state.close();
await Promise.all([neon.connect(), supa.connect()]);
try {
  const neonRO = {
    async readOnly<T>(work: (sql: SeedSql) => Promise<T>): Promise<T> {
      await neon.query("BEGIN READ ONLY");
      try {
        const out = await work({
          query: (text, params) => neon.query(text, params) as never,
        });
        await neon.query("COMMIT");
        return out;
      } catch (error) {
        await neon.query("ROLLBACK").catch(() => {});
        throw error;
      }
    },
  };
  const seeder: SeedSql = {
    async query(text, params) {
      await supa.query("BEGIN");
      try {
        await supa.query("SET LOCAL ROLE bulls96_session_ingest_seeder");
        const out = await supa.query(text, params);
        await supa.query("COMMIT");
        return out as never;
      } catch (error) {
        await supa.query("ROLLBACK").catch(() => {});
        throw error;
      }
    },
  };
  const result = await seedOrg(neonRO, seeder, org, exported, {
    log: (fields) => console.log(JSON.stringify(fields)),
  });
  // Fences after revisions: a deletion forgets the seeded revisions of its session.
  const fences = await seedFences(neonRO, seeder, org);
  console.log(
    JSON.stringify({ outcome: "seeded", org_id: org, ...result, fences }),
  );
} finally {
  await Promise.allSettled([neon.end(), supa.end()]);
}
