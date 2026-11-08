/**
 * Native-Postgres proof for the self-host writer contract.
 * Runs against real Postgres only (PGlite cannot express the creator
 * edge or SET ROLE denial faithfully); skipped without a URL.
 *
 *   SESSION_PIPELINE_TEST_PG_URL          owner URL of a database with every
 *                                         Supabase + HoopDb migration applied
 *   SESSION_PIPELINE_TEST_WRITER_PG_URL   direct search_consumer_runtime URL
 *                                         (password set by the operator; the
 *                                         bootstrap never handles it)
 *
 * Owner sections prove: the bootstrap converges twice (idempotent rerun),
 * verify fails closed on drift, the failure rolls back atomically, the lock
 * names hashtextextended, reserved names are refused, and an owner
 * session_user is refused by assertRuntimeIdentity.
 * Writer sections prove: RLS visibility through the writer policy, the full
 * write surface as the role (including the tombstone retire), checkOwner
 * refusal, and the exact committed grant set.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { readFileSync, readdirSync } from "node:fs";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import pg from "pg";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import { composeSearchService } from "../../shared/search-composition.ts";
import { postgresSearchState } from "../postgres-search-state.ts";
import { writerTransactionSql } from "../transaction-sql.ts";
import { sessionsFunctionAclPreview } from "../../hoopdb/migrate-guards.ts";
import {
  assertRuntimeIdentity,
  assertSearchTenantGate,
  collectFreshnessSend,
  makeDedupe,
  reconcileTombstonedScopes,
  runConsumerTick,
  writerComposition,
  writerTenantRunner,
  MAX_PENDING_ROWS,
} from "../consumer-service.ts";
import { processBatch } from "../consumer.ts";
import { droppedFanout, scopeLockKey, type PointerDeps, type ScopeLock, type SessionScope } from "../pointer.ts";
import type { SearchIndexDeps } from "../search-index.ts";
import { reconcileRecent, transaction } from "../lambda.ts";
import { inTransaction, type HoopDb } from "../../shared/db.ts";
import { expectedSchema } from "../../hoopdb/migrate.ts";
import { composeSearchService } from "../../shared/search-composition.ts";
import {
  computeContentSha256,
  manifestKey,
  objectKey,
  objectStoreManifestReader,
  prefixAllowed,
  prefixOrgResolver,
  rawChunkKey,
  type ChunkEntry,
  type ManifestV1,
  type ObjectStore,
  type RelayhistoryStreamRecord,
  type SessionLocation,
} from "../../shared/vendor.ts";
import type { TenantRunner } from "../../session-read-api/liveness.ts";
import {
  bootstrapCheckOwner,
  bootstrapLock,
  ensureServiceTables,
  runBootstrap,
  stripPsqlMeta,
} from "../consumer-bootstrap.ts";

const ownerUrl = process.env.SESSION_PIPELINE_TEST_PG_URL;
const writerUrl = process.env.SESSION_PIPELINE_TEST_WRITER_PG_URL;
const ORG = "pgtest";
const BACKEND = "pgtest-backend";
const ROLE_SQL = fileURLToPath(new URL("../../../supabase/bootstrap/session_consumer_role.sql", import.meta.url));
const VERIFY_SQL = fileURLToPath(new URL("../../../supabase/bootstrap/session_consumer_role_verify.sql", import.meta.url));

function ownerPool(): pg.Pool {
  return new pg.Pool({ connectionString: ownerUrl });
}

/** The repo's SqlExecutor shape over one pg client. */
function asSql(client: pg.PoolClient): {
  query: <R>(text: string, params?: unknown[]) => Promise<{ rows: R[] }>;
} {
  return {
    query: async <R>(text: string, params?: unknown[]): Promise<{ rows: R[] }> => {
      const r = await client.query(text, params as unknown[]);
      return { rows: r.rows as R[] };
    },
  };
}

async function files(): Promise<{ role: string; verify: string }> {
  return { role: stripPsqlMeta(readFileSync(ROLE_SQL, "utf8")), verify: stripPsqlMeta(readFileSync(VERIFY_SQL, "utf8")) };
}

/**
 * Self-provisioning for CI and reruns: the test database gains the migrated
 * schema (shim, bootstrap roles, every Supabase + HoopDb migration) when it
 * is absent, exactly like the PGlite migratedPg fixture but on real
 * Postgres. The migrator's own PUBLIC-EXECUTE revoke runs too, mirroring the
 * hardening migrate performs before this bootstrap ever runs.
 */
const repoRoot = join(import.meta.dir, "../../..");
const readSql = (path: string): string =>
  readFileSync(join(repoRoot, path), "utf8")
    .split("\n")
    .filter((line) => !line.startsWith("\\"))
    .join("\n");

const BASE_ROLE_FILES = [
  "supabase/bootstrap/session_directory_search_worker.sql",
  "supabase/bootstrap/session_token_verifier.sql",
  "supabase/bootstrap/session_ingest_roles.sql",
  "supabase/bootstrap/session_read_api_roles.sql",
  // Mirrors migrate's bootstrap-before-schema order: the consumer grant
  // migration needs the bare role to exist (migrations never create roles).
  "supabase/bootstrap/session_consumer_roles.sql",
];

async function ensureProvisioned(pool: pg.Pool): Promise<void> {
  const { rows } = await pool.query<{ r: string | null }>(
    `SELECT to_regclass('sessions.session_index') AS r`,
  );
  if (rows[0]?.r) return;
  // The shim's service_role carries BYPASSRLS, which only a superuser can
  // create: when the base roles are absent the connected user must be a
  // superuser (CI pre-provisions them in its setup step, so the owner run
  // skips this branch). The owner then installs the trusted extensions and
  // the migration chain itself.
  const base = await pool.query<{ n: string }>(
    `SELECT count(*) AS n FROM pg_roles WHERE rolname IN ('service_role', 'anon', 'authenticated')`,
  );
  if (base.rows[0]?.n !== "3") {
    const [{ rolsuper }] = (await pool.query(`SELECT rolsuper FROM pg_roles WHERE rolname = current_user`)).rows as { rolsuper: boolean }[];
    if (!rolsuper)
      throw new Error("writer proof needs its base roles once (service_role/anon/authenticated, incl BYPASSRLS): apply supabase_shim + the four bootstrap role files as a superuser, then rerun");
    await pool.query(readSql("supabase/tests/supabase_shim.sql"));
    for (const file of BASE_ROLE_FILES) await pool.query(readSql(file));
  }
  await pool.query("CREATE EXTENSION IF NOT EXISTS pg_trgm; CREATE EXTENSION IF NOT EXISTS btree_gin");
  for (const file of readdirSync(join(repoRoot, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort())
    await pool.query(readSql(`supabase/migrations/${file}`));
  for (const file of readdirSync(join(repoRoot, "src/hoopdb/migrations")).filter((f) => f.endsWith(".sql")).sort())
    await pool.query(readSql(`src/hoopdb/migrations/${file}`));
  await pool.query("REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions FROM PUBLIC");
  await pool.query(
    `CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text NOT NULL);
     CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY, generation text, status text, updated_at timestamptz DEFAULT now())`,
  );
}

describe.skipIf(!ownerUrl)("self-host writer bootstrap on real Postgres", () => {
  let pool: pg.Pool;

  beforeAll(async () => {
    pool = ownerPool();
    await ensureProvisioned(pool);
  });

  afterAll(async () => {
    await pool.query(`DELETE FROM sessions.session_index WHERE org_id=$1`, [ORG]);
    // Forced RLS filters even the table owner, so the search-table residue
    // cleanup binds the tenant GUCs first: without them the DELETE silently
    // removes nothing and the next run inherits rows. ORG/BACKEND are
    // file constants, safe to inline (multi-statement cleanup takes no
    // parameters under the simple protocol).
    await pool.query(
      `BEGIN; SELECT set_config('bulls96_read.org_id','${ORG}',true),set_config('bulls96_read.workspace_id','w',true);
       DELETE FROM search_index.transcript_events WHERE org_id='${ORG}';
       DELETE FROM search_index.projection_state WHERE backend_id='${BACKEND}'; COMMIT`,
    ).catch(() => {});
    await pool.query(`DELETE FROM sessions.search_consumer_pending WHERE backend_id=$1`, [BACKEND]).catch(() => {});
    await pool.query(`DELETE FROM sessions.search_consumer_progress WHERE backend_id=$1`, [BACKEND]).catch(() => {});
    await pool.end();
  });

  // Models serve(): one outer transaction around every owner step, so a
  // failed verify rolls back role, grants, policies and tables together.
  async function bootstrapInTx(
    client: pg.PoolClient,
    overrides?: { verifyRole?: (o: ReturnType<typeof asSql>) => Promise<void> },
  ): Promise<void> {
    const db = asSql(client);
    const binding = composeSearchService(
      writerComposition(BACKEND, 0),
      { env: { SEARCH_INDEX_BACKEND: "postgres" }, sql: writerTransactionSql(db).sql, secrets: {} },
      "writer",
    );
    const { role, verify } = await files();
    await client.query("BEGIN");
    try {
      await runBootstrap({
        owner: db,
        lock: () => bootstrapLock(db),
        checkOwner: () => bootstrapCheckOwner(db),
        ensureIndexSchema: () => binding.index.ensureSchema(),
        ensureProjectionState: (o) => postgresSearchState({ backendId: BACKEND, fromMs: 0 }).ensureSchema(o),
        ensureServiceTables,
        ensureRole: (o) => o.query(role).then(() => {}),
        verifyRole: overrides?.verifyRole ?? ((o) => o.query(verify).then(() => {})),
        generation: "native-test",
      });
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    }
  }

  async function bootstrapOnce(): Promise<void> {
    const client = await pool.connect();
    try {
      await bootstrapInTx(client);
    } finally {
      client.release();
    }
  }

  // Drop a leftover writer install (grants and policies first: DROP ROLE
  // refuses while anything depends on the role). Tables go too, so the
  // rollback below can prove the bootstrap recreated nothing.
  async function dropWriterInstall(client: pg.PoolClient): Promise<void> {
    const { rows } = await client.query<{ n: string }>(
      `SELECT count(*) AS n FROM pg_roles WHERE rolname='search_consumer_runtime'`,
    );
    if (rows[0]?.n === "0") {
      await client.query(`DROP TABLE IF EXISTS sessions.search_consumer_pending, sessions.search_consumer_progress`);
      return;
    }
    for (const table of [
      "session_index", "session_index_ext", "session_directory", "session_directory_users",
      "session_directory_machines", "session_briefs", "session_directory_event_pending",
      "session_directory_event_times", "session_directory_agents", "session_directory_relationships",
      "session_directory_resurrection_fences", "pipeline_jobs", "repos", "repo_names",
    ])
      await client.query(`DROP POLICY IF EXISTS search_consumer_trusted ON sessions.${table}`);
    await client.query(`REVOKE ALL ON SCHEMA sessions, search_index FROM search_consumer_runtime`);
    await client.query(`REVOKE ALL ON ALL TABLES IN SCHEMA sessions, search_index FROM search_consumer_runtime`);
    await client.query(`REVOKE ALL ON ALL FUNCTIONS IN SCHEMA sessions, search_index FROM search_consumer_runtime`);
    await client.query(`DROP TABLE IF EXISTS sessions.search_consumer_pending, sessions.search_consumer_progress`);
    await client.query(`DROP ROLE search_consumer_runtime`);
  }

  // Runs first: it starts from no role and no service tables (dropping any
  // leftovers from an earlier run), so the rollback can prove everything the
  // bootstrap created is gone.
  test("a failed verify rolls the full bootstrap back atomically", async () => {
    const client = await pool.connect();
    try {
      await dropWriterInstall(client);
      const { verify } = await files();
      // The whole bootstrap runs inside one transaction (exactly like
      // serve): the role, grants, policies and service tables install, then
      // an injected drift makes the real verify fail, and the rollback must
      // remove everything the bootstrap created.
      await expect(
        bootstrapInTx(client, {
          verifyRole: async (o) => {
            await o.query(`GRANT TRUNCATE ON sessions.search_consumer_pending TO search_consumer_runtime`);
            await o.query(verify);
          },
        }),
      ).rejects.toThrow("forbidden grant sessions.search_consumer_pending TRUNCATE");
      const gone = await client.query(
        `SELECT count(*)::int AS n FROM pg_roles WHERE rolname='search_consumer_runtime'`,
      );
      expect(gone.rows[0]).toMatchObject({ n: 0 });
      const tables = await client.query(
        `SELECT count(*)::int AS n FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname='sessions' AND c.relname IN ('search_consumer_pending', 'search_consumer_progress')`,
      );
      expect(tables.rows[0]).toMatchObject({ n: 0 });
      // And the same bootstrap converges cleanly on retry.
      await bootstrapInTx(client);
      const back = await client.query(
        `SELECT count(*)::int AS n FROM pg_roles WHERE rolname='search_consumer_runtime'`,
      );
      expect(back.rows[0]).toMatchObject({ n: 1 });
    } finally {
      client.release();
    }
  }, 180_000);

  test("bootstrap converges twice: role, grants, policies, verify", async () => {
    await bootstrapOnce();
    await bootstrapOnce();
    const role = await pool.query(`SELECT rolcanlogin, rolsuper FROM pg_roles WHERE rolname='search_consumer_runtime'`);
    expect(role.rows[0]).toMatchObject({ rolcanlogin: true, rolsuper: false });
    const usage = await pool.query(`SELECT has_schema_privilege('search_consumer_runtime','sessions','USAGE') AS u`);
    expect(usage.rows[0]).toMatchObject({ u: true });
    // The creator edge, pinned exactly where the catalog allows. A
    // superuser creator gets no automatic admin-membership edge (so CI must
    // bootstrap as the CREATEROLE owner, like the operator model), and
    // pg_auth_members only carries inherit_option/set_option on PG16+, so
    // older servers get the portable invariant instead of the exact shape.
    const [{ n: versionNum }] = (await pool.query(`SELECT current_setting('server_version_num')::int AS n`)).rows as { n: number }[];
    const [{ rolsuper }] = (await pool.query(`SELECT rolsuper FROM pg_roles WHERE rolname = current_user`)).rows as { rolsuper: boolean }[];
    if (!rolsuper && versionNum >= 160000) {
      const members = await pool.query(
        `SELECT m.rolname AS member, a.admin_option, a.inherit_option, a.set_option
         FROM pg_auth_members a JOIN pg_roles m ON m.oid = a.member
         WHERE a.roleid = (SELECT oid FROM pg_roles WHERE rolname='search_consumer_runtime')`,
      );
      expect(members.rowCount).toBe(1);
      expect(members.rows[0]).toMatchObject({ admin_option: true, inherit_option: false, set_option: false });
    } else {
      const edges = await pool.query(
        `SELECT a.admin_option FROM pg_auth_members a
         WHERE a.roleid = (SELECT oid FROM pg_roles WHERE rolname='search_consumer_runtime')`,
      );
      expect(edges.rowCount).toBeLessThanOrEqual(1);
      for (const edge of edges.rows) expect(edge.admin_option).toBe(true);
    }
  }, 180_000);

  test("the advisory lock names hashtextextended (typo fails 42883)", async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await bootstrapLock(asSql(client));
      await client.query("ROLLBACK");
    } finally {
      client.release();
    }
  }, 120_000);

  test("reserved role names are refused before any role lookup", async () => {
    const { role } = await files();
    for (const name of ["postgres", "pg_read_all_data"]) {
      const swapped = role.replace(
        "v_name text := 'search_consumer_runtime'",
        `v_name text := '${name}'`,
      );
      expect(swapped).not.toBe(role);
      await expect(pool.query(swapped)).rejects.toThrow("reserved role name");
    }
    const missing = await pool.query(`SELECT count(*)::int AS n FROM pg_roles WHERE rolname='search_consumer_runtime'`);
    expect(missing.rows[0]).toMatchObject({ n: 1 }); // the install test's role, untouched by refusals
  }, 120_000);

  test("assertRuntimeIdentity refuses the owner login", async () => {
    const client = await pool.connect();
    try {
      await expect(assertRuntimeIdentity(asSql(client))).rejects.toThrow("refusing to run as");
    } finally {
      client.release();
    }
  }, 120_000);
});

describe.skipIf(!writerUrl)("self-host writer surface as the role on real Postgres", () => {
  let wpool: pg.Pool;
  let admin: pg.Pool;
  let writerRun: TenantRunner;

  /** Writer-side tenant transaction: the search tables are gated by the
   * tenant_only policies, so every writer write binds the GUCs first. */
  function tenantRunner(pool: pg.Pool): TenantRunner {
    return async (orgId, workspaceId, work) => {
      const c = await pool.connect();
      try {
        await c.query("BEGIN");
        await c.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true)", [orgId, workspaceId]);
        try {
          const out = await work(asSql(c));
          await c.query("COMMIT");
          return out;
        } catch (error) {
          await c.query("ROLLBACK");
          throw error;
        }
      } finally {
        c.release();
      }
    };
  }

  beforeAll(async () => {
    wpool = new pg.Pool({ connectionString: writerUrl });
    admin = ownerPool();
    writerRun = tenantRunner(wpool);
    await admin.query(
      `INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,trace_id)
       VALUES($1,'w','h','wlive','m','u',now(),now(),'s3','b','96bulls/pgtest/wlive.json','00000000-0000-0000-0000-000000000000')
       ON CONFLICT DO NOTHING`,
      [ORG],
    );
    await admin.query(
      `INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,trace_id,deleted_at)
       VALUES($1,'w','h','wdead','m','u',now(),now(),'s3','b','96bulls/pgtest/wdead.json','00000000-0000-0000-0000-000000000000',now())
       ON CONFLICT DO NOTHING`,
      [ORG],
    );
    await writerRun(ORG, "w", (tx) =>
      tx.query(
        `INSERT INTO search_index.projection_state(backend_id,org_id,workspace_id,source,session_id,canonical_session_id,source_epoch,coverage_from_ms,revision,row_count,complete,updated_at)
         VALUES($1,$2,'w','h','wdead','wdead',3,0,1,4,true,now()) ON CONFLICT DO NOTHING`,
        [BACKEND, ORG],
      ),
    );
    // Forced tenant_only RLS binds the owner too: the transcript seed goes
    // through the writer with the tenant GUCs, like every production write.
    await writerRun(ORG, "w", (tx) =>
      tx.query(
        `INSERT INTO search_index.transcript_events(org_id,workspace_id,ts_ms,session_id,source,machine_id,kind,event_id,type,user_id,actor_name,actor_role,subagent_id,project_id,task_title,task_ref,task_ref_id,tags,files_touched,tool_name,tool_status,delivery_record_id,content,content_bytes,content_prefix_folded,task_title_folded,fingerprint,version)
         VALUES($1,'w',1,'wdead','h','m','message','e1','user','u','user','r','','','t','ref','ref1','{}','{}','','','', 'x',1,'x','t','fp',1)
         ON CONFLICT DO NOTHING`,
        [ORG],
      ),
    );
  });

  afterAll(async () => {
    await wpool.end();
    await admin.query(`DELETE FROM sessions.session_index WHERE org_id=$1`, [ORG]);
    // Same forced-RLS owner-blindness as the owner block: bind the tenant
    // GUCs or the search-table DELETEs silently remove nothing.
    await admin.query(
      `BEGIN; SELECT set_config('bulls96_read.org_id','${ORG}',true),set_config('bulls96_read.workspace_id','w',true);
       DELETE FROM search_index.transcript_events WHERE org_id='${ORG}';
       DELETE FROM search_index.projection_state WHERE backend_id='${BACKEND}'; COMMIT`,
    ).catch(() => {});
    await admin.query(`DELETE FROM sessions.search_consumer_pending WHERE backend_id=$1`, [BACKEND]).catch(() => {});
    await admin.query(`DELETE FROM sessions.search_consumer_progress WHERE backend_id=$1`, [BACKEND]).catch(() => {});
    await admin.end();
  });

  test("writer sees seeded rows through RLS but checkOwner refuses it", async () => {
    const client = await wpool.connect();
    try {
      const seen = await client.query(`SELECT count(*)::int AS n FROM sessions.session_index WHERE org_id=$1`, [ORG]);
      expect(seen.rows[0].n).toBeGreaterThanOrEqual(2);
      await expect(assertRuntimeIdentity(asSql(client))).resolves.toBeUndefined();
      const refused = await bootstrapCheckOwner(asSql(client)).then(
        () => null,
        (e: unknown) => e as { code?: string; message?: string },
      );
      expect(refused?.code).toBe("42501");
      expect(refused?.message).toContain("does not own");
    } finally {
      client.release();
    }
  }, 120_000);

  test("pointer upsert and tombstone run as the role (trigger INSERT needs no extra grant)", async () => {
    const client = await wpool.connect();
    try {
      // A live pointer upsert fires sessions.session_directory_events_changed
      // as the writer: the trigger INSERTs the dirty marker, so this fails
      // 42501 without the event_pending INSERT grant.
      const payload = (revision: number, deletedAt: string | null) =>
        JSON.stringify([
          {
            org_id: ORG, workspace_id: "w", harness: "h", session_id: "wtrig", machine_id: "m",
            user_id: "u", created_at: new Date().toISOString(), last_activity_at: new Date().toISOString(),
            storage_provider: "s3", bucket: "b", manifest_key: "96bulls/pgtest/wtrig.json",
            trace_id: "00000000-0000-0000-0000-000000000000", revision, deleted_at: deletedAt,
          },
        ]);
      const live = await client.query(`SELECT applied FROM sessions.upsert_session_index($1::jsonb)`, [payload(1, null)]);
      expect(live.rows[0]).toMatchObject({ applied: true });
      const mark = await client.query(
        `SELECT dirty FROM sessions.session_directory_event_pending WHERE org_id=$1 AND source_session_id='wtrig'`,
        [ORG],
      );
      expect(mark.rows[0]).toMatchObject({ dirty: true });
      // Tombstone the same pointer: the trigger path fires again and the
      // pointer row carries deleted_at.
      const tombed = await client.query(`SELECT applied FROM sessions.upsert_session_index($1::jsonb)`, [payload(2, new Date().toISOString())]);
      expect(tombed.rows[0]).toMatchObject({ applied: true });
      const gone = await client.query(
        `SELECT deleted_at IS NOT NULL AS tombed FROM sessions.session_index WHERE org_id=$1 AND session_id='wtrig'`,
        [ORG],
      );
      expect(gone.rows[0]).toMatchObject({ tombed: true });
    } finally {
      client.release();
    }
  }, 120_000);

  test("a genuine writer login passes admission", async () => {
    // The BYPASSRLS negative runs in PGlite (only a BYPASSRLS role may flip
    // the attribute, which the CREATEROLE owner is not): here the genuine
    // writer login must pass with the pinned shape.
    const client = await wpool.connect();
    try {
      await assertRuntimeIdentity(asSql(client));
    } finally {
      client.release();
    }
  }, 120_000);

  test("tombstone retire runs as the role and converges", async () => {
    const client = await wpool.connect();
    try {
      const db = asSql(client);
      // Erase wdead the way indexSearchScope does, then the backstop retires
      // the residue itself: nothing re-sent, state row gone. The search
      // writes bind the tenant GUCs; the backstop's tenant enumeration runs
      // on the plain writer connection (sessions writer policy, no GUCs).
      await writerRun(ORG, "w", (tx) =>
        tx.query(`DELETE FROM search_index.transcript_events WHERE org_id=$1 AND session_id='wdead'`, [ORG]),
      );
      await writerRun(ORG, "w", (tx) =>
        tx.query(
          `INSERT INTO search_index.projection_state(backend_id,org_id,workspace_id,source,session_id,canonical_session_id,source_epoch,coverage_from_ms,revision,row_count,complete,updated_at)
           VALUES($1,$2,'w','h','wdead','wdead',3,0,1,0,true,now())
           ON CONFLICT(backend_id,org_id,workspace_id,source,session_id) DO UPDATE SET row_count=0, complete=true`,
          [BACKEND, ORG],
        ),
      );
      const sent: string[] = [];
      const out = await reconcileTombstonedScopes(db, writerRun, BACKEND, "96bulls/pgtest", 100, async (b) => {
        sent.push(b);
      });
      expect(out).toMatchObject({ tombstoned: 0, overflow: false });
      expect(sent).toHaveLength(0);
      const left = await writerRun(ORG, "w", (tx) =>
        tx.query<{ n: number }>(`SELECT count(*)::int AS n FROM search_index.projection_state WHERE backend_id=$1 AND session_id='wdead'`, [BACKEND]),
      );
      expect(left.rows[0]).toMatchObject({ n: 0 });
    } finally {
      client.release();
    }
  }, 120_000);

  test("writer executes exactly the twelve composed-path functions, nothing else in sessions", async () => {
    // The closed call-graph of the production tick (pointer upsert through
    // the publication-authority retry, directory projection, repo catalog):
    // the e2e proves the writer 42501s without any one of these, and the
    // static closure in the merge notes proves no other sessions function is
    // reachable.
    const fns = await wpool.query<{ fn: string }>(
      `SELECT p.oid::regprocedure::text AS fn FROM pg_proc p
       WHERE p.pronamespace = to_regnamespace('sessions')
         AND has_function_privilege('search_consumer_runtime', p.oid, 'EXECUTE')
       ORDER BY 1`,
    );
    expect(fns.rows.map((r) => r.fn)).toEqual([
      "sessions.applied_repo_evidence(jsonb,jsonb)",
      "sessions.canonical_remote(text)",
      "sessions.clean_root_commit(text)",
      "sessions.legacy_remote(text)",
      "sessions.lock_repo_catalog(text)",
      "sessions.repo_live(text,uuid)",
      "sessions.resolve_repo(text,text,text,timestamp with time zone)",
      "sessions.session_directory_search_text(text,text,text,text,text,text,text,text)",
      "sessions.session_index_batch(jsonb)",
      "sessions.session_publication_is_managed(text,text,text,text,text)",
      "sessions.session_publication_lock(jsonb)",
      "sessions.upsert_session_index(jsonb)",
    ]);
  }, 120_000);

  test("function ACL drift is zero: the twelve consumer grants are manifest-authorized", async () => {
    // B2 receipt: the twelve EXECUTE grants now sit inside the migrator's
    // function-ACL manifest (installed by the grant migration on every
    // migrate), so the boundary preview must report no extras. Any new
    // writer grant outside the manifest fails here. Missing runtime grants
    // are installed by migrate itself, not by this bootstrap, so only
    // extras are pinned.
    const client = await admin.connect();
    try {
      const preview = await sessionsFunctionAclPreview(asSql(client), "service_role", true);
      expect(preview.function_acl_drift_extra).toEqual([]);
    } finally {
      client.release();
    }
  }, 120_000);

  test("exact committed grant set, no TRUNCATE, nothing unlisted", async () => {
    const grants = await wpool.query(
      `SELECT c.oid::regclass::text AS obj, a.privilege_type AS priv
       FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace,
            aclexplode(c.relacl) a JOIN pg_roles r ON r.oid = a.grantee
       WHERE n.nspname IN ('sessions','search_index') AND r.rolname='search_consumer_runtime'
       ORDER BY 1,2`,
    );
    const pairs = grants.rows.map((r: { obj: string; priv: string }) => `${r.obj} ${r.priv}`).sort();
    expect(pairs).not.toContainEqual(expect.stringContaining("TRUNCATE"));
    // The full evidence set is pinned: any added or missing grant fails here.
    expect(pairs).toEqual([
      "search_index.projection_state DELETE",
      "search_index.projection_state INSERT",
      "search_index.projection_state SELECT",
      "search_index.projection_state UPDATE",
      "search_index.transcript_events DELETE",
      "search_index.transcript_events INSERT",
      "search_index.transcript_events SELECT",
      "search_index.transcript_events UPDATE",
      "sessions.__96bulls_migrations SELECT",
      "sessions.__96bulls_role_validation SELECT",
      "sessions.pipeline_jobs INSERT",
      "sessions.repo_names INSERT",
      "sessions.repo_names SELECT",
      "sessions.repo_names UPDATE",
      "sessions.repos INSERT",
      "sessions.repos SELECT",
      "sessions.repos UPDATE",
      "sessions.search_consumer_pending DELETE",
      "sessions.search_consumer_pending INSERT",
      "sessions.search_consumer_pending SELECT",
      "sessions.search_consumer_pending UPDATE",
      "sessions.search_consumer_progress INSERT",
      "sessions.search_consumer_progress SELECT",
      "sessions.search_consumer_progress UPDATE",
      "sessions.session_briefs SELECT",
      "sessions.session_directory DELETE",
      "sessions.session_directory INSERT",
      "sessions.session_directory SELECT",
      "sessions.session_directory UPDATE",
      "sessions.session_directory_agents DELETE",
      "sessions.session_directory_agents INSERT",
      "sessions.session_directory_agents SELECT",
      "sessions.session_directory_agents UPDATE",
      "sessions.session_directory_event_pending INSERT",
      "sessions.session_directory_event_pending SELECT",
      "sessions.session_directory_event_pending UPDATE",
      "sessions.session_directory_event_times DELETE",
      "sessions.session_directory_event_times INSERT",
      "sessions.session_directory_event_times SELECT",
      "sessions.session_directory_machines DELETE",
      "sessions.session_directory_machines INSERT",
      "sessions.session_directory_machines SELECT",
      "sessions.session_directory_machines UPDATE",
      "sessions.session_directory_relationships DELETE",
      "sessions.session_directory_relationships INSERT",
      "sessions.session_directory_relationships SELECT",
      "sessions.session_directory_relationships UPDATE",
      "sessions.session_directory_resurrection_fences SELECT",
      "sessions.session_directory_users DELETE",
      "sessions.session_directory_users INSERT",
      "sessions.session_directory_users SELECT",
      "sessions.session_directory_users UPDATE",
      "sessions.session_index DELETE",
      "sessions.session_index INSERT",
      "sessions.session_index SELECT",
      "sessions.session_index UPDATE",
      "sessions.session_index_ext INSERT",
      "sessions.session_index_ext SELECT",
      "sessions.session_index_ext UPDATE",
    ]);
  }, 120_000);

  test("the serve-time gate activates on the composed bootstrap with no staging", async () => {
    // P1-a activation: the composed ensureSchema provisions exactly
    // tenant_only with forced RLS on both search tables, so the gate the
    // service runs at startup passes here with nothing staged. The writer
    // reads its own gate catalogs through its table grants. (The owner
    // block's bootstrap already ran: this block logs in as its product.)
    const client = await wpool.connect();
    try {
      await assertSearchTenantGate(asSql(client));
    } finally {
      client.release();
    }
  }, 120_000);

  test("composed writer e2e: a reconcile body through processBatch lands the projection", async () => {
    // This test runs LAST in the file (bun runs in file order): the sibling
    // seeds (wlive/wdead) have already served the tests that need them, so
    // their session state is cleared here to give the tick a pristine
    // database. complete:true below is then genuine, not masked by harness
    // dirt. If a later test needs the seeds, move this test after it.
    await admin.query(`DELETE FROM sessions.session_index WHERE org_id=$1 AND session_id IN ('wlive','wdead')`, [ORG]);
    await admin.query(`DELETE FROM search_index.projection_state WHERE backend_id=$1 AND session_id IN ('wlive','wdead')`, [BACKEND]);
    // Serve-identical composition on the writer pool: the ledgered writer
    // binding, the pass-scoped writer SQL, and the writer's own tenant
    // runner (every tenant statement binds the GUCs as the runtime role).
    const wdb: HoopDb = {
      pool: wpool,
      query: async (text, params) => ({ rows: (await wpool.query(text, params as never[])).rows as Record<string, unknown>[] }),
      end: async () => {},
    };
    const writerSql = writerTransactionSql(wpool);
    const tenantRunner = writerTenantRunner(wdb);
    const binding = composeSearchService(
      writerComposition(BACKEND, 0),
      { env: { SEARCH_INDEX_BACKEND: "postgres", SEARCH_INDEX_POSTGRES_BACKEND_ID: BACKEND }, sql: writerSql.sql, secrets: {} },
      "writer",
      tenantRunner,
    );
    const expected = await expectedSchema();
    // The operator step the lock requires: the worker bootstrap marked
    // pass at this generation (runbooks/session-directory-search-worker-bootstrap.md).
    // Without it every pointer lock fails "generation not ready", in
    // production exactly as here.
    await admin.query(
      `INSERT INTO sessions.__96bulls_role_validation(name, generation, status) VALUES ('session_directory_search_worker', $1, 'pass')
       ON CONFLICT (name) DO UPDATE SET generation = EXCLUDED.generation, status = 'pass', updated_at = now()`,
      [expected.generation],
    );
    // The object-store driver is orthogonal (pipeline.test.ts proves the
    // same indexSearchScope driver-independent on a file store); the
    // composed Postgres path is what's under proof here.
    const root = await mkdtemp(join(tmpdir(), "96bulls-consumer-e2e-"));
    try {
      const store: ObjectStore = createFsStore({ root });
      const reader = objectStoreManifestReader(store, { provider: "s3", bucket: "b" });
      const location: SessionLocation = {
        prefix: "96bulls/pgtest", workspaceId: "w", machineId: "m", harness: "h", sessionId: "we2e", startDate: "2026-11-02",
      };
      const at = "2026-11-02T10:00:00.000Z";
      const stream: RelayhistoryStreamRecord[] = [
        {
          kind: "session_event", origin_id: "desktop-origin", record_id: "e1", revision: "1",
          operation: "upsert", received_at: at, payload: { ts_ms: Date.parse("2026-11-02T10:00:01Z"), text: "first", role: "assistant" },
        },
        {
          kind: "session_event", origin_id: "desktop-origin", record_id: "e2", revision: "1",
          operation: "upsert", received_at: at, payload: { ts_ms: Date.parse("2026-11-02T10:00:02Z"), text: "second", role: "assistant" },
        },
      ];
      const source = new TextEncoder().encode(`${stream.map((r) => JSON.stringify(r)).join("\n")}\n`);
      const stored = new Uint8Array(zstdCompressSync(source));
      const sha = await sha256Hex(source);
      const chunk: ChunkEntry = {
        seq: 0, epoch: 0, start_offset: 0, bytes: source.byteLength, stored_bytes: stored.byteLength,
        sha256: sha, sha256_8: sha.slice(0, 8), stored_sha256: await sha256Hex(stored),
        stored_raw_bytes: source.byteLength, raw_sha256: sha, compression: "zstd", records: stream.length,
      };
      await store.put(rawChunkKey(location, chunk.seq, 0, sha, "ndjson"), stored, {});
      const manifest: ManifestV1 = {
        schema_v: 1, layout: "v1", trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18", revision: 1,
        created_at: at, updated_at: at,
        session: { workspace_id: "w", machine_id: "m", harness: "h", session_id: "we2e", started_at: at },
        writer: { delivery_instance_id: "m", plugin_version: "test" },
        raw: {
          owner: "m", state: "open", updated_at: at, format: "ndjson", compression: "zstd",
          scrub: { ruleset: "relayhistory-scrub", version: 1 }, chunk_count: 1,
          raw_bytes: chunk.bytes, stored_bytes: chunk.stored_bytes, content_sha256: await computeContentSha256([chunk]),
          epoch: 0, epochs: [{ epoch: 0, opened_at: at, reason: "initial", first_seq: chunk.seq, opened_by: "m" }],
          next_offset: source.byteLength, chunks: [chunk],
        },
        extensions: { "96bulls": { user_id: "u" } },
      } as ManifestV1;
      const key = manifestKey(location);
      await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(manifest)), { contentType: "application/json" });

      const bindings = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
      const pointer: PointerDeps = {
        db: wdb,
        lock: ((scope: SessionScope, work: () => Promise<never>) =>
          transaction(wdb, async (tx) => {
            await tx.query("SET LOCAL lock_timeout = '30s'");
            await tx.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [scopeLockKey(scope)]);
            return work();
          })) as ScopeLock,
        reader,
        exists: async (k) => (await store.head(objectKey(k))) !== undefined,
        orgs: prefixOrgResolver,
        allowPrefix: (prefix, orgId) => prefixAllowed(bindings, prefix, orgId),
        fanout: droppedFanout,
        publication: {
          schemaGeneration: expected.generation,
          transact: (work) =>
            inTransaction(wdb, async (tx) => {
              await tx.query("SET LOCAL statement_timeout='5s';SET LOCAL lock_timeout='1s'");
              return work(tx);
            }),
        },
      };
      const search: SearchIndexDeps = {
        transact: (work) => transaction(wdb, (tx) => writerSql.run(tx, () => work(tx))),
        reader,
        store,
        adapter: binding,
        prefixFor: (orgId) => `96bulls/${orgId}`,
      };
      const bodies: string[] = [];
      const io = {
        reconcile: async (send: (body: string) => Promise<void>) => {
          const body = JSON.stringify({ reconcile: { key, etag: "e1" } });
          bodies.push(body);
          await send(body);
          return { enqueued: 1, complete: true };
        },
        checkFreshness: async (send: (body: string) => Promise<void>) => {
          const collected = await collectFreshnessSend(
            wdb, BACKEND, "96bulls",
            (tenant, after) => binding.state.partials(tenant, { limit: 1000, ...(after === undefined ? {} : { after }) }),
            tenantRunner, MAX_PENDING_ROWS, new Set<string>(), send,
          );
          return { stale: collected.stale, tombstoned: collected.tombstoned, overflow: collected.overflow };
        },
        dedupe: makeDedupe(wdb, BACKEND),
        consume: (records: { messageId: string; body: string }[]) =>
          processBatch(records, { pointer, search, requeue: async () => {} }),
        pending: [] as string[],
        requeued: [] as string[],
        now: () => new Date(),
      };
      const summary = await runConsumerTick(io, new Date());
      expect(summary.failedKeys).toEqual([]);
      // The harness siblings (wlive/wdead) ride the same freshness send and
      // are ignored (legacy keys); the e2e key must be among the consumed.
      expect(summary.consumedKeys).toContain(key);
      expect(summary).toMatchObject({ failed: 0, requeued: 0, pending: 0, stale: 0, tombstoned: 0, complete: true });
      // The pointer landed, both transcript rows landed in the writer's
      // tenant, and the projection recorded complete.
      const ptr = await admin.query(`SELECT manifest_key FROM sessions.session_index WHERE org_id=$1 AND session_id='we2e'`, [ORG]);
      expect(ptr.rows[0]).toMatchObject({ manifest_key: key });
      const seen = await writerRun(ORG, "w", (tx) =>
        tx.query(`SELECT count(*)::int AS n FROM search_index.transcript_events WHERE session_id='we2e'`),
      );
      expect(seen.rows[0]).toMatchObject({ n: 2 });
      const proj = await writerRun(ORG, "w", (tx) =>
        tx.query(`SELECT complete FROM search_index.projection_state WHERE backend_id=$1 AND session_id='we2e'`, [BACKEND]),
      );
      expect(proj.rows[0]).toMatchObject({ complete: true });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }, 300_000);
});
