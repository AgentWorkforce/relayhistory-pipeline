import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { describe, expect, it } from "vitest";
import { searchIndexConformance, syntheticDoc } from "../src/search-index/conformance.js";
import {
  TENANT_SETTINGS_SQL,
  createPostgresSearchIndex,
  likeLiteral,
  planScopeReconcile,
  positionOf,
  type SqlClient,
  type TenantSqlRunner,
} from "../src/search-index/index.js";

/** One PGlite transaction per tenant call, with the tenant settings bound. */
function pgliteTenants(db: PGlite): TenantSqlRunner {
  return (orgId, workspaceId, work) =>
    db.transaction(async (tx) => {
      await tx.query(TENANT_SETTINGS_SQL, [orgId, workspaceId]);
      return work(tx as unknown as SqlClient);
    });
}

// In-process PGlite with pg_trgm (no database service). CI's pg-proof job
// also runs this file against real PostgreSQL 15/16/17 via
// SEARCH_CONFORMANCE_PG_URL (search-index-postgres-real.test.ts).
searchIndexConformance("postgres (pglite + pg_trgm)", async () => {
  const db = new PGlite({ extensions: { pg_trgm } });
  await db.exec("CREATE EXTENSION IF NOT EXISTS pg_trgm");
  return { adapter: createPostgresSearchIndex({ client: db as unknown as SqlClient, tenantRunner: pgliteTenants(db) }), dispose: () => db.close() };
});

describe("postgres search index specifics", () => {
  it("keeps distinct reconcile identities separate when fields contain separators", () => {
    const desired = syntheticDoc({ orgId: "o", kind: "a\u001fb", eventId: "c" });
    const stored = syntheticDoc({ orgId: "o", kind: "a", eventId: "b\u001fc" });
    expect(planScopeReconcile([desired], [stored], { complete: true })).toEqual({
      upserts: [desired],
      deletes: [{ ...positionOf(stored), workspaceId: stored.workspaceId }],
    });
    expect(planScopeReconcile([desired], [stored], { complete: false })).toEqual({
      upserts: [desired], deletes: [],
    });
  });

  it("escapes LIKE metacharacters", () => {
    expect(likeLiteral("100%_\\x")).toBe("%100\\%\\_\\\\x%");
  });

  it("refuses unsafe identifiers and a missing pg_trgm", async () => {
    const db = new PGlite();
    expect(() => createPostgresSearchIndex({ client: db as unknown as SqlClient, schema: 'x"; drop' })).toThrow();
    const adapter = createPostgresSearchIndex({ client: db as unknown as SqlClient });
    await expect(adapter.ensureSchema()).rejects.toMatchObject({ code: "schema_missing" });
    expect(await adapter.health()).toEqual({ ok: false, code: "schema_missing" });
    await db.close();
  });

  it("the query's q predicates match the trigram index expressions", async () => {
    // Guards the normalized search columns: if the
    // query and index drifted, no plan could use the index. Which index the
    // planner picks for a tenant-filtered query is a cost decision on real stats.
    const db = new PGlite({ extensions: { pg_trgm } });
    await db.exec("CREATE EXTENSION IF NOT EXISTS pg_trgm");
    const adapter = createPostgresSearchIndex({ client: db as unknown as SqlClient, tenantRunner: pgliteTenants(db) });
    await adapter.ensureSchema();
    const docs = Array.from({ length: 2000 }, (_, i) =>
      syntheticDoc({ orgId: "o", workspaceId: "w", eventId: `p${i}`, tsMs: i, content: `row ${i} filler text ${i * 7919}` }),
    );
    for (let i = 0; i < docs.length; i += 1000) await adapter.upsert(docs.slice(i, i + 1000));
    await db.exec("ANALYZE search_index.transcript_events");
    await db.exec("SET enable_seqscan = off");
    const plan = await db.query<{ "QUERY PLAN": string }>(
      `EXPLAIN SELECT 1 FROM search_index.transcript_events
        WHERE content_prefix_folded LIKE '%needle%' ESCAPE '\\' OR task_title_folded LIKE '%needle%' ESCAPE '\\'`,
    );
    const text = plan.rows.map((r) => r["QUERY PLAN"]).join("\n");
    expect(text).toContain("transcript_events_content_trgm");
    expect(text).toContain("transcript_events_title_trgm");
    await db.close();
  });

  it("a client outside a transaction never silently erases, deletes or reads nothing: it throws tenant_unbound", async () => {
    // The tenant settings are transaction-local; in autocommit they lapse
    // before the next statement, and under forced RLS a DELETE would match
    // nothing and report success. Each call must throw instead.
    const db = new PGlite({ extensions: { pg_trgm } });
    await db.exec("CREATE EXTENSION IF NOT EXISTS pg_trgm");
    const bare = createPostgresSearchIndex({ client: db as unknown as SqlClient });
    const bound = createPostgresSearchIndex({ client: db as unknown as SqlClient, tenantRunner: pgliteTenants(db) });
    await bare.ensureSchema();
    const doc = syntheticDoc({ orgId: "org-a", sessionId: "sa", content: "erasable" });
    const tenant = { orgId: "org-a", workspaceId: doc.workspaceId };
    const scope = { ...tenant, source: doc.source, sessionIds: ["sa"] };
    const count = async () => Number((await db.query<{ n: string }>("SELECT count(*)::text AS n FROM search_index.transcript_events")).rows[0]!.n);
    await bound.upsert([doc]);
    const unbound = { code: "unavailable", detail: "tenant_unbound" };
    await expect(bare.eraseScope(scope)).rejects.toMatchObject(unbound);
    await expect(bare.deleteEvents(tenant, [positionOf(doc)])).rejects.toMatchObject(unbound);
    await expect(bare.query({ tenant, order: "asc", limit: 1 })).rejects.toMatchObject(unbound);
    await expect(bare.scanFingerprints(scope, { fromMs: 0, limit: 1 })).rejects.toMatchObject(unbound);
    await expect(bare.upsert([{ ...doc, eventId: "other" }])).rejects.toMatchObject(unbound);
    expect(await count()).toBe(1);
    // Inside one transaction the same client binds the tenant and erases.
    await db.transaction(async (tx) => {
      await createPostgresSearchIndex({ client: tx as unknown as SqlClient }).eraseScope(scope);
    });
    expect(await count()).toBe(0);
    await db.close();
  });

  it("a delete that removed fewer rows than it saw, or an erase that leaves a scope row, throws delete_incomplete", async () => {
    // Inert fixture client: the settings check holds; only the counts differ.
    const answer = (erase: { present: number; deleted: number }, left: number): SqlClient => ({
      async query<T>(text: string) {
        const rows = text.startsWith("WITH present AS") ? [{ bound: true, ...erase }]
          : text.includes("AS bound, count(*)") ? [{ bound: true, n: left }]
          : text.includes("AS bound") ? [{ bound: true }] : [];
        return { rows: rows as T[] };
      },
    });
    const doc = syntheticDoc({ orgId: "org-a", sessionId: "sa" });
    const scope = { orgId: "org-a", workspaceId: doc.workspaceId, source: doc.source, sessionIds: ["sa"] };
    const incomplete = { code: "unavailable", detail: "delete_incomplete", retryable: true };
    await expect(createPostgresSearchIndex({ client: answer({ present: 2, deleted: 1 }, 0) }).eraseScope(scope)).rejects.toMatchObject(incomplete);
    await expect(createPostgresSearchIndex({ client: answer({ present: 1, deleted: 1 }, 1) }).eraseScope(scope)).rejects.toMatchObject(incomplete);
    await expect(createPostgresSearchIndex({ client: answer({ present: 1, deleted: 0 }, 0) }).deleteEvents(scope, [positionOf(doc)])).rejects.toMatchObject(incomplete);
    await expect(createPostgresSearchIndex({ client: answer({ present: 1, deleted: 1 }, 0) }).eraseScope(scope)).resolves.toBeUndefined();
  });
});
