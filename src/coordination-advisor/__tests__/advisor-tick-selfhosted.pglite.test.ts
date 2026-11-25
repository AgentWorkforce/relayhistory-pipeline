/**
 * The advisor tick on a self-hosted database: a full migrate with the SELFHOSTED profile (Supabase shim, bootstrap,
 * verify, dedicated runtime role history_rt, global default-revoke function boundary), set up as
 * src/hoopdb/__tests__/migration-target.pglite.test.ts does, then one advisorTick with every statement run as
 * history_rt: candidates, controls, the postgres facts readers, upsert, DM mark-delivered, recent and the sweep.
 * No statement may fail (in particular no 42501), and the Locker environment is unusable for the whole tick, so a
 * Locker read would throw. advisor-tick.pglite.test.ts covers the scan's behaviour as service_role on the LOCAL profile.
 */
import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { runMigrations, type MigrationClient, type MigrationProfile } from "../../hoopdb/migrate.ts";
import type { SqlQuery } from "../../session-read-api/liveness.ts";
import { InMemoryRelaySender } from "../relay-sender.ts";
import { AdvisorLimits } from "../rubric.ts";
import { advisorTick, scanOnce, sqlAdvisorStore } from "../scan.ts";
import { postgresAdvisorBackend } from "../main.ts";

setDefaultTimeout(300_000);
const SELFHOSTED: MigrationProfile = { mode: "selfhosted", shim: true, bootstrap: true, verify: true, runtimeRole: "history_rt", runtimeLogin: false, functionBoundary: "global-default-revoke" };
const ORG = "fx-org";
const WS = "fx-ws1";

function client(db: PGlite): MigrationClient {
  return { async query<R>(text: string, params?: unknown[]) {
    if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
    const results = await db.exec(text);
    return { rows: (results.at(-1)?.rows ?? []) as R[] };
  } };
}

/** Every worker statement runs as the self-hosted runtime role; every error code it raises is recorded. */
const asRuntime = (db: PGlite, errors: string[]): SqlQuery => ({
  query: async <R,>(text: string, params?: unknown[]) => {
    try {
      return await db.transaction(async (tx) => {
        await tx.exec("SET LOCAL ROLE history_rt");
        return { rows: (await tx.query<R>(text, params ?? [])).rows };
      });
    } catch (err) {
      errors.push(String((err as { code?: unknown }).code ?? "no-code"));
      throw err;
    }
  },
}) as SqlQuery;

/** The Locker environment a vendored or buildLockerStore read would need, made unusable for `work`. */
async function withoutLocker<T>(work: () => Promise<T>): Promise<T> {
  const keys = ["LOCKER_PROVIDER", "LOCKER_BUCKET", "LOCKER_FS_ROOT", "PIPELINE_RAW_PROVIDER", "PIPELINE_RAW_BUCKET"] as const;
  const saved = Object.fromEntries(keys.map((k) => [k, process.env[k]]));
  for (const k of keys) delete process.env[k];
  process.env.LOCKER_PROVIDER = "fx-not-a-provider";
  try {
    return await work();
  } finally {
    for (const [k, v] of Object.entries(saved)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
  }
}

describe("coordination advisor tick on a SELFHOSTED migrate as the runtime role (pglite)", () => {
  let db: PGlite;
  const now = new Date();
  const rel = (min: number) => new Date(now.getTime() + min * 60_000).toISOString();
  const rows = async () => (await db.query<{ session_id: string; urgency: string; status: string }>(
    "SELECT session_id, urgency, status FROM sessions.coordination_advisories ORDER BY session_id")).rows.map((r) => [r.session_id, r.urgency, r.status]);

  beforeAll(async () => {
    db = new PGlite({ extensions: { btree_gin, pg_trgm } });
    await db.exec(`CREATE ROLE history_rt NOLOGIN NOINHERIT; ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
      DO $$ BEGIN EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC',current_database()); END $$;`);
    expect((await runMigrations(client(db), { profile: SELFHOSTED })).applied).toBeGreaterThan(0);
    // Operator rows (deny by default): the global row AND the workspace row, per feature.
    await db.exec(`INSERT INTO sessions.session_intel_controls(scope,feature,enabled,updated_by)
      SELECT 'global', f, true, 'fx-operator' FROM unnest(ARRAY['advisor','dm','conflicts']) AS f;
      INSERT INTO sessions.session_intel_controls(scope,org_id,workspace_id,feature,enabled,updated_by)
      SELECT 'workspace','${ORG}','${WS}',f,true,'fx-operator' FROM unnest(ARRAY['advisor','dm','conflicts']) AS f;`);
    for (const s of ["fx-a", "fx-b"]) await db.query(
      `INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at, project_id)
       VALUES ($1,$2,'claude',$3,$4,$5,'fx-proj')`, [ORG, WS, s, rel(-120), rel(-10)]);
    const fact = (session: string, min: number, row: string) => db.query(
      `INSERT INTO sessions.session_event_facts (org_id,workspace_id,ts,session_id,source,machine_id,kind,event_id,project_id,branch,
         input_tokens,output_tokens,reasoning_tokens,cache_read_tokens,cache_create_tokens,files_touched,row_id,fingerprint,source_version)
       VALUES ($1,$2,$3,$4,'claude','fx-m','tool',$5,'fx-proj','b/' || $4,0,0,0,0,0,ARRAY['src/x/shared.ts'],$6,'f',1)`,
      [ORG, WS, rel(min), session, `${session}-${min}`, row.repeat(64)]);
    await fact("fx-a", -40, "1");
    await fact("fx-b", -20, "2");
  });
  afterAll(async () => { await db?.close(); });

  test("the role is the restricted runtime role: the advisory table itself is not readable, only its functions", async () => {
    const errors: string[] = [];
    await expect(asRuntime(db, errors).query("SELECT count(*) FROM sessions.coordination_advisories")).rejects.toThrow();
    expect(errors).toEqual(["42501"]);
  });

  test("one tick (scan, DM, mark delivered, sweep) and a second scan run as history_rt with no failed statement and the Locker unusable", async () => {
    const errors: string[] = [];
    const q = asRuntime(db, errors);
    const sender = new InMemoryRelaySender();
    const deps = () => ({ workspaces: "*" as const, store: sqlAdvisorStore(q), backend: postgresAdvisorBackend(q),
      limits: new AdvisorLimits({ pairCooldownMs: 0, sessionHourlyCap: 3 }), sender, conflicts: true });
    await withoutLocker(() => advisorTick(() => deps(), { intervalMs: 1 })(q, now));
    expect(errors).toEqual([]);
    expect(await rows()).toEqual([["fx-a", "high", "delivered"], ["fx-b", "high", "delivered"]]);
    expect(sender.sent).toHaveLength(2);

    // The same scan again on fresh rows, for its counters: a swallowed per-workspace error would show as failed,
    // a swallowed controls error as workspaces 0, a swallowed claims error as conflictsSkipped.
    await db.exec("DELETE FROM sessions.coordination_advisories");
    sender.sent.length = 0;
    const stats = await withoutLocker(() => scanOnce(deps(), now));
    expect(errors).toEqual([]);
    expect(stats).toMatchObject({ workspaces: 1, written: 2, dmSent: 2, dmFailed: 0, failed: 0, conflictsSkipped: 0 });
    expect(await rows()).toEqual([["fx-a", "high", "delivered"], ["fx-b", "high", "delivered"]]);
    expect(await sqlAdvisorStore(q).sweep()).toEqual({ expired: 0, deleted: 0 });
    expect(errors).toEqual([]);
  });
});
