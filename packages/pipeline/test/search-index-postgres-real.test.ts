import { describe, expect, it } from "vitest";
import { searchIndexConformance, syntheticDoc } from "../src/search-index/conformance.js";
import { TENANT_SETTINGS_SQL, createPostgresSearchIndex, type SqlClient, type TenantSqlRunner } from "../src/search-index/index.js";

// Real PostgreSQL run of the shared suite. Skipped unless
// SEARCH_CONFORMANCE_PG_URL names a disposable server whose role can create
// a database (the test creates and drops its own, with pg_trgm).
const url = process.env.SEARCH_CONFORMANCE_PG_URL;

if (!url) {
  describe.skip("SearchIndexAdapter conformance: postgres (real)", () => {
    it("needs SEARCH_CONFORMANCE_PG_URL", () => {});
  });
} else {
  /** One pooled client per tenant call, in a transaction with the settings bound. */
  const poolTenants = (pool: import("pg").Pool): TenantSqlRunner => async (orgId, workspaceId, work) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(TENANT_SETTINGS_SQL, [orgId, workspaceId]);
      const result = await work(client as unknown as SqlClient);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally { client.release(); }
  };

  /** A disposable database of its own with pg_trgm; dispose drops it. */
  const disposable = async () => {
    const { default: pg } = await import("pg");
    // A disposable database of its own: other proofs sharing the server may
    // have locked down the default database (pg-proof runs role proofs first).
    const name = `search_conformance_${process.pid}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
    const admin = new pg.Client({ connectionString: url });
    await admin.connect();
    await admin.query(`CREATE DATABASE "${name}"`);
    const target = new URL(url);
    target.pathname = `/${name}`;
    const pool = new pg.Pool({ connectionString: target.toString(), max: 2 });
    await pool.query("CREATE EXTENSION IF NOT EXISTS pg_trgm");
    return {
      pool,
      dispose: async () => {
        await pool.end();
        await admin.query(`DROP DATABASE IF EXISTS "${name}"`);
        await admin.end();
      },
    };
  };

  searchIndexConformance("postgres (real server)", async () => {
    const { pool, dispose } = await disposable();
    return { adapter: createPostgresSearchIndex({ client: pool as unknown as SqlClient, tenantRunner: poolTenants(pool) }), dispose };
  });

  describe("postgres (real server) bare pool", () => {
    it("erase and delete on a bare pool throw tenant_unbound and leave the row; a tenant runner erases it", async () => {
      const { pool, dispose } = await disposable();
      try {
        const bare = createPostgresSearchIndex({ client: pool as unknown as SqlClient });
        const bound = createPostgresSearchIndex({ client: pool as unknown as SqlClient, tenantRunner: poolTenants(pool) });
        await bare.ensureSchema();
        const doc = syntheticDoc({ orgId: "org-a", sessionId: "sa" });
        const scope = { orgId: "org-a", workspaceId: doc.workspaceId, source: doc.source, sessionIds: ["sa"] };
        await bound.upsert([doc]);
        const rows = async () => (await pool.query("SELECT count(*)::int AS n FROM search_index.transcript_events")).rows[0].n;
        await expect(bare.eraseScope(scope)).rejects.toMatchObject({ code: "unavailable", detail: "tenant_unbound" });
        await expect(bare.deleteEvents(scope, [doc])).rejects.toMatchObject({ code: "unavailable", detail: "tenant_unbound" });
        expect(await rows()).toBe(1);
        await bound.eraseScope(scope);
        expect(await rows()).toBe(0);
      } finally { await dispose(); }
    });
  });
}
