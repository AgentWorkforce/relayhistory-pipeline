import { expect, test } from "bun:test";
import pg from "pg";
import { SemanticPostgresExecutor } from "../../search-features/semantic-copy-postgres.ts";
import { publicationFleetScopeKey, runPublicationFleetTurn } from "../publication-fleet.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
const proposal = new URL("../../../supabase/proposals/continuous-publication/", import.meta.url);
const sql = (suffix: string) => Bun.file(new URL("062290-fleet-storage" + suffix + ".sql", proposal)).text();

test.skipIf(!url)("dedicated fleet logins isolate state, retain audit holds and block destructive rollback", async () => {
  const id = crypto.randomUUID().replaceAll("-", "");
  const db = "fleet_" + id;
  const roles = ["fa_", "fb_"].map(prefix => prefix + id);
  const password = crypto.randomUUID();
  const admin = new pg.Client({ connectionString: url });
  const scratch = new URL(url!);
  scratch.pathname = "/" + db;
  const owner = new pg.Client({ connectionString: scratch.href });
  const pools = roles.map(role => {
    const login = new URL(scratch);
    login.username = role;
    login.password = password;
    return new pg.Pool({ connectionString: login.href, max: 1, connectionTimeoutMillis: 1000 });
  });
  const target = { provider: "s3" as const, bucket: "fixture", region: "us-east-1", keyRoot: "fixture", generation: "1".repeat(64) };
  const bindings = roles.map(orgId => ({ tenant: {orgId, workspaceId:"w", userId:"u", originId:"o"}, target,
    admissionId:"00000000-0000-4000-8000-000000000001", planSha256:"2".repeat(64), frontierSha256:"3".repeat(64) }));
  const scope = bindings.map(publicationFleetScopeKey);
  const fleets = ["a".repeat(64), "b".repeat(64)];
  const bind = async (role: string, fleet: string, key: string) => {
    await owner.query("BEGIN");
    try {
      await owner.query("SELECT set_config('publication_scheduler.login',$1,true),set_config('publication_scheduler.fleet',$2,true),set_config('publication_scheduler.scope',$3,true)", [role,fleet,key]);
      await owner.query(await sql("-access"));
      await owner.query("COMMIT");
    } finally { await owner.query("ROLLBACK"); }
  };
  await admin.connect();
  try {
    await admin.query("CREATE DATABASE " + db);
    await owner.connect();
    for (const role of roles) await admin.query("CREATE ROLE " + role + " LOGIN PASSWORD '" + password + "'");
    await owner.query("CREATE SCHEMA sessions; ALTER DEFAULT PRIVILEGES IN SCHEMA sessions GRANT SELECT ON TABLES TO PUBLIC");
    await owner.query(await sql(".up"));
    await owner.query("BEGIN;" + await sql(".down") + "ROLLBACK");
    expect((await owner.query("SELECT has_table_privilege($1,'sessions.publication_receipt_retries','SELECT') ok",[roles[0]])).rows[0].ok).toBe(false);
    for (const [i,role] of roles.entries()) {
      await bind(role,fleets[i],scope[i]);
      const result = await runPublicationFleetTurn(new SemanticPostgresExecutor(pools[i]), {
        enabled:true, target, bindings:[bindings[i]], fleetId:fleets[i], budgetMs:10000, receiptLimit:1,
        sourceFor: bound => ({scope:publicationFleetScopeKey(bound),highest:async()=>null,page:async()=>[],reconcile:async()=>"missing"})
      });
      expect(result).toBe("committed");
      await pools[i].query("INSERT INTO sessions.publication_receipt_retries VALUES($1,'00000000-0000-4000-8000-000000000099',1,'audit',0)",[scope[i]]);
    }
    await expect(bind(roles[0],fleets[0],scope[1])).rejects.toMatchObject({code:"42501"});
    const a = pools[0];
    for (const table of ["publication_scheduler_scopes","publication_retry_checkpoints","publication_receipt_retries"]) {
      expect((await a.query("SELECT scope FROM sessions."+table)).rows).toEqual([{scope:scope[0]}]);
    }
    expect((await a.query("DELETE FROM sessions.publication_receipt_retries")).rowCount).toBe(0);
    await expect(a.query("TRUNCATE sessions.publication_receipt_retries")).rejects.toMatchObject({code:"42501"});
    await expect(a.query("INSERT INTO sessions.publication_receipt_retries VALUES($1,gen_random_uuid(),1,'retry',0)",[scope[1]])).rejects.toMatchObject({code:"42501"});
    await owner.query("SET ROLE "+roles[0]);
    expect((await owner.query("SELECT scope FROM sessions.publication_scheduler_scopes")).rows).toEqual([]);
    await owner.query("RESET ROLE; DELETE FROM sessions.publication_scheduler_scopes; DELETE FROM sessions.publication_scheduler_fleets");
    expect((await a.query("SELECT scope FROM sessions.publication_receipt_retries")).rows).toEqual([]);
    await owner.query("BEGIN");
    await expect(owner.query(await sql(".down"))).rejects.toMatchObject({code:"55000"});
    await owner.query("ROLLBACK");
    expect((await owner.query("SELECT state FROM sessions.publication_receipt_retries")).rows).toEqual([{state:"audit"},{state:"audit"}]);
  } finally {
    await Promise.allSettled(pools.map(pool=>pool.end()));
    await owner.end();
    try {
      await admin.query("DROP DATABASE IF EXISTS "+db);
      for(const role of roles) await admin.query("DROP ROLE IF EXISTS "+role);
    } finally { await admin.end(); }
  }
},30000);
