/**
 * Native-Postgres proof for the self-host writer contract.
 * Runs against real Postgres only (PGlite cannot express RLS FORCE, the
 * creator edge, or SET ROLE denial faithfully); skipped without a URL.
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
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { composeSearchService } from "../../shared/search-composition.ts";
import { postgresSearchState } from "../postgres-search-state.ts";
import { writerTransactionSql } from "../transaction-sql.ts";
import {
  assertRuntimeIdentity,
  reconcileTombstonedScopes,
  writerComposition,
} from "../consumer-service.ts";
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

describe.skipIf(!ownerUrl)("self-host writer bootstrap on real Postgres", () => {
  let pool: pg.Pool;

  beforeAll(async () => {
    pool = ownerPool();
    await pool.query("CREATE EXTENSION IF NOT EXISTS pg_trgm");
    await pool.query(
      `CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text NOT NULL);
       CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY, generation text, status text, updated_at timestamptz DEFAULT now())`,
    );
  });

  afterAll(async () => {
    await pool.query(`DELETE FROM sessions.session_index WHERE org_id=$1`, [ORG]);
    await pool.query(`DELETE FROM search_index.transcript_events WHERE org_id=$1`, [ORG]).catch(() => {});
    await pool.query(`DELETE FROM search_index.projection_state WHERE backend_id=$1`, [BACKEND]).catch(() => {});
    await pool.query(`DELETE FROM sessions.search_consumer_pending WHERE backend_id=$1`, [BACKEND]).catch(() => {});
    await pool.query(`DELETE FROM sessions.search_consumer_progress WHERE backend_id=$1`, [BACKEND]).catch(() => {});
    await pool.end();
  });

  async function bootstrapOnce(): Promise<void> {
    const client = await pool.connect();
    try {
      const db = asSql(client);
      const binding = composeSearchService(
        writerComposition(BACKEND, 0),
        { env: { SEARCH_INDEX_BACKEND: "postgres" }, sql: writerTransactionSql(db).sql, secrets: {} },
        "writer",
      );
      const { role, verify } = await files();
      await runBootstrap({
        owner: db,
        lock: () => bootstrapLock(db),
        checkOwner: () => bootstrapCheckOwner(db),
        ensureIndexSchema: () => binding.index.ensureSchema(),
        ensureProjectionState: (o) => postgresSearchState({ backendId: BACKEND, fromMs: 0 }).ensureSchema(o),
        ensureServiceTables,
        ensureRole: (o) => o.query(role).then(() => {}),
        verifyRole: (o) => o.query(verify).then(() => {}),
        generation: "native-test",
      });
    } finally {
      client.release();
    }
  }

  test("bootstrap converges twice: role, grants, policies, verify", async () => {
    await bootstrapOnce();
    await bootstrapOnce();
    const role = await pool.query(`SELECT rolcanlogin, rolsuper FROM pg_roles WHERE rolname='search_consumer_runtime'`);
    expect(role.rows[0]).toMatchObject({ rolcanlogin: true, rolsuper: false });
    const usage = await pool.query(`SELECT has_schema_privilege('search_consumer_runtime','sessions','USAGE') AS u`);
    expect(usage.rows[0]).toMatchObject({ u: true });
    // Exactly the creator edge, nothing else.
    const members = await pool.query(
      `SELECT m.rolname AS member, a.admin_option, a.inherit_option, a.set_option
       FROM pg_auth_members a JOIN pg_roles m ON m.oid = a.member
       WHERE a.roleid = (SELECT oid FROM pg_roles WHERE rolname='search_consumer_runtime')`,
    );
    expect(members.rowCount).toBe(1);
    expect(members.rows[0].admin_option).toBe(true);
    expect(members.rows[0].inherit_option).toBe(false);
    expect(members.rows[0].set_option).toBe(false);
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

  test("verify failure rolls back atomically", async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      // A stray extra grant is drift: verify names it, the rollback removes it.
      await client.query(`GRANT TRUNCATE ON sessions.search_consumer_pending TO search_consumer_runtime`);
      const { verify } = await files();
      await expect(client.query(verify)).rejects.toThrow("unlisted grant sessions.search_consumer_pending TRUNCATE");
      await client.query("ROLLBACK");
      const { verify: again } = await files();
      await client.query(again); // passes after the rollback
    } finally {
      client.release();
    }
  }, 120_000);

  test("assertRuntimeIdentity refuses the owner session_user", async () => {
    const client = await pool.connect();
    try {
      await expect(assertRuntimeIdentity(asSql(client))).rejects.toThrow("refusing to run as session_user");
    } finally {
      client.release();
    }
  }, 120_000);
});

describe.skipIf(!writerUrl)("self-host writer surface as the role on real Postgres", () => {
  let wpool: pg.Pool;
  let admin: pg.Pool;

  beforeAll(async () => {
    wpool = new pg.Pool({ connectionString: writerUrl });
    admin = ownerPool();
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
    await wpool.query(
      `INSERT INTO search_index.projection_state(backend_id,org_id,workspace_id,source,session_id,canonical_session_id,source_epoch,coverage_from_ms,revision,row_count,complete,updated_at)
       VALUES($1,$2,'w','h','wdead','wdead',3,0,1,4,true,now()) ON CONFLICT DO NOTHING`,
      [BACKEND, ORG],
    );
    await admin.query(
      `INSERT INTO search_index.transcript_events(org_id,workspace_id,ts_ms,session_id,source,machine_id,kind,event_id,type,user_id,actor_name,actor_role,subagent_id,project_id,task_title,task_ref,task_ref_id,tags,files_touched,tool_name,tool_status,delivery_record_id,content,content_bytes,content_prefix_folded,task_title_folded,fingerprint,version)
       VALUES($1,'w',1,'wdead','h','m','message','e1','user','u','user','r','','','t','ref','ref1','{}','{}','','','', 'x',1,'x','t','fp',1)
       ON CONFLICT DO NOTHING`,
      [ORG],
    );
  });

  afterAll(async () => {
    await wpool.end();
    await admin.query(`DELETE FROM sessions.session_index WHERE org_id=$1`, [ORG]);
    await admin.query(`DELETE FROM search_index.transcript_events WHERE org_id=$1`, [ORG]).catch(() => {});
    await admin.query(`DELETE FROM search_index.projection_state WHERE backend_id=$1`, [BACKEND]).catch(() => {});
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

  test("tombstone retire runs as the role and converges", async () => {
    const client = await wpool.connect();
    try {
      const db = asSql(client);
      // Erase wdead the way indexSearchScope does, then the backstop retires
      // the residue itself: nothing re-sent, state row gone.
      await client.query(`DELETE FROM search_index.transcript_events WHERE org_id=$1 AND session_id='wdead'`, [ORG]);
      await client.query(
        `INSERT INTO search_index.projection_state(backend_id,org_id,workspace_id,source,session_id,canonical_session_id,source_epoch,coverage_from_ms,revision,row_count,complete,updated_at)
         VALUES($1,$2,'w','h','wdead','wdead',3,0,1,0,true,now())
         ON CONFLICT(backend_id,org_id,workspace_id,source,session_id) DO UPDATE SET row_count=0, complete=true`,
        [BACKEND, ORG],
      );
      const sent: string[] = [];
      const out = await reconcileTombstonedScopes(db, BACKEND, "96bulls/pgtest", 100, async (b) => {
        sent.push(b);
      });
      expect(out).toMatchObject({ tombstoned: 0, overflow: false });
      expect(sent).toHaveLength(0);
      const left = await client.query(`SELECT count(*)::int AS n FROM search_index.projection_state WHERE backend_id=$1 AND session_id='wdead'`, [BACKEND]);
      expect(left.rows[0]).toMatchObject({ n: 0 });
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
      "sessions.session_directory_event_pending SELECT",
      "sessions.session_directory_event_pending UPDATE",
      "sessions.session_directory_event_times DELETE",
      "sessions.session_directory_event_times INSERT",
      "sessions.session_directory_event_times SELECT",
      "sessions.session_directory_machines DELETE",
      "sessions.session_directory_machines INSERT",
      "sessions.session_directory_machines SELECT",
      "sessions.session_directory_machines UPDATE",
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
});
