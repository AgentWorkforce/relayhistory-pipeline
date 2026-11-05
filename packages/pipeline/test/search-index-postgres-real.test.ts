import { describe, it } from "vitest";
import { searchIndexConformance } from "../src/search-index/conformance.js";
import { createPostgresSearchIndex, type SqlClient } from "../src/search-index/index.js";

// Real PostgreSQL run of the shared suite. Skipped unless
// SEARCH_CONFORMANCE_PG_URL names a disposable database whose role can
// create a schema; pg_trgm must already exist or be creatable by that role.
const url = process.env.SEARCH_CONFORMANCE_PG_URL;

if (!url) {
  describe.skip("SearchIndexAdapter conformance: postgres (real)", () => {
    it("needs SEARCH_CONFORMANCE_PG_URL", () => {});
  });
} else {
  searchIndexConformance("postgres (real server)", async () => {
    const { default: pg } = await import("pg");
    const pool = new pg.Pool({ connectionString: url, max: 2 });
    await pool.query("CREATE EXTENSION IF NOT EXISTS pg_trgm");
    const schema = `search_conformance_${process.pid}_${Date.now().toString(36)}`;
    return {
      adapter: createPostgresSearchIndex({ client: pool as unknown as SqlClient, schema }),
      dispose: async () => {
        await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
        await pool.end();
      },
    };
  });
}
