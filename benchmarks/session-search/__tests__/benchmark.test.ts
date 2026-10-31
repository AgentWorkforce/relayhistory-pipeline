/**
 * The harness's own proof, on PGlite with the real migrations: the oracle agrees with
 * the RPC, tenants stay apart, and transcript content stays out of the directory.
 * These tests measure nothing; timings come only from run.ts.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { directorySearch, installSchema, loadBatch, loadNeonBatch, neonBriefSearch, neonEventSearch, type Db } from "../arms.ts";
import { corpusHash, generateCorpus, PLANTED_SECRET, SCALES, TERMS, tenantCanary, type CorpusSession } from "../corpus.ts";
import { assertLocalTarget } from "../guard.ts";
import { identity, listQueries, oracleLaneA, oracleLaneB, SMALL, textQueries, WHALE } from "../queries.ts";
import { agentGate, percentile, type PlanNode } from "../stats.ts";

const params = SCALES.test!;
let pg: PGlite;
let db: Db;
let corpus: CorpusSession[];

beforeAll(async () => {
  pg = new PGlite({ extensions: { btree_gin, pg_trgm } });
  db = { query: (s, a) => pg.query(s, a as never) as never, exec: async (s) => void (await pg.exec(s)) };
  await installSchema(db);
  corpus = [...generateCorpus(params)];
  for (let i = 0; i < corpus.length; i += 500) {
    await loadBatch(db, corpus.slice(i, i + 500), true);
    await loadNeonBatch(db, corpus.slice(i, i + 500));
  }
}, 120_000);

afterAll(() => pg.close());

const laneA = (t: typeof WHALE) => textQueries(t).filter((q) => q.lane === "A");

describe("corpus", () => {
  test("is deterministic", () => {
    expect(corpusHash(params)).toBe(corpusHash(params));
    expect(corpusHash({ ...params, seed: 1 })).not.toBe(corpusHash(params));
  });
  test("plants every term the query set depends on", () => {
    const hits = (term: string) => corpus.filter((s) => `${s.title} ${s.shortSummary} ${s.events.map((e) => e.content).join(" ")}`.includes(term)).length;
    for (const term of Object.values(TERMS)) expect(hits(term)).toBeGreaterThan(0);
    expect(hits(PLANTED_SECRET)).toBeGreaterThan(0);
  });
});

describe("Lane A: the directory RPC against the oracle", () => {
  for (const t of [WHALE, SMALL]) {
    for (const q of laneA(t)) {
      test(`${t.workspaceId} ${q.id}`, async () => {
        const hits = await directorySearch(db, t, q.term);
        const want = oracleLaneA(corpus, t, q.term);
        expect(hits.slice(0, 50).map((h) => identity({ harness: h.harness, sessionId: h.session_id }))).toEqual(want.page);
        expect(hits.length > 50).toBe(want.total > 50);
      });
    }
  }
});

describe("tenant isolation", () => {
  test("another tenant's canary returns nothing, in every arm", async () => {
    const other = tenantCanary(SMALL.orgId, SMALL.workspaceId);
    expect(oracleLaneA(corpus, SMALL, other).total).toBeGreaterThan(0);
    expect(await directorySearch(db, WHALE, other)).toEqual([]);
    expect(await neonBriefSearch(db, WHALE, other)).toEqual([]);
    expect(await neonEventSearch(db, WHALE, other)).toEqual([]);
  });
  test("every row of a term shared by all tenants belongs to the caller", async () => {
    for (const t of [WHALE, SMALL]) {
      const rows = await directorySearch(db, t, TERMS.sharedAcrossTenants);
      expect(rows.length).toBeGreaterThan(0);
      for (const r of rows) expect([r.org_id, r.workspace_id]).toEqual([t.orgId, t.workspaceId]);
    }
  });
  test("a workspace outside the claims is refused", async () => {
    await db.query("SELECT set_config('request.jwt.claims', $1, false)", [JSON.stringify({
      role: "session_directory_search_worker", app_metadata: { org_id: WHALE.orgId, workspace_ids: [WHALE.workspaceId] },
    })]);
    await db.exec("SET ROLE session_directory_search_worker");
    await expect(db.query("SELECT * FROM sessions.search_session_directory($1, $2)", [SMALL.workspaceId, TERMS.common])).rejects.toThrow();
    await db.exec("RESET ROLE");
  });
});

describe("the search boundary", () => {
  test("Lane B terms and the planted secret are in no directory column", async () => {
    for (const term of [TERMS.transcriptOnly, TERMS.oldTranscriptOnly, PLANTED_SECRET]) {
      const { rows } = await db.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM sessions.session_directory AS d WHERE d::text ILIKE $1`, [`%${term}%`]);
      expect(rows[0]!.n).toBe(0);
    }
  });
  test("the V1 directory answers no transcript term; the Neon stand-in does", async () => {
    for (const q of textQueries(WHALE).filter((x) => x.lane === "B")) {
      expect(await directorySearch(db, WHALE, q.term)).toEqual([]);
      const hits = await neonEventSearch(db, WHALE, q.term);
      expect(new Set(hits.map((h) => identity({ harness: h.harness, sessionId: h.session_id }))))
        .toEqual(oracleLaneB(corpus, WHALE, q.term));
    }
  });
  test("a 2-character query is refused", async () => {
    await expect(directorySearch(db, WHALE, "de")).rejects.toThrow();
  });
});

describe("Lane C", () => {
  test("covers every indexed shape in the contract's table", async () => {
    const { REQUIRED_SEARCH_INDEXES } = await import("../../../src/hoopdb/search-index-contract.ts");
    const covered = new Set(listQueries(corpus, WHALE).map((q) => q.index));
    const missing = REQUIRED_SEARCH_INDEXES.map((i) => i.indexName)
      .filter((n) => !covered.has(n) && n !== "session_directory_tenant_search_trgm_idx");
    expect(missing).toEqual([]);
  });
  test("every shape returns rows for both tenants, membership included", async () => {
    for (const t of [WHALE, SMALL]) {
      for (const q of listQueries(corpus, t)) expect([q.id, (await db.query(q.sql, q.args)).rows.length > 0]).toEqual([q.id, true]);
    }
  });
});

describe("agent gate", () => {
  const scan = (node: Partial<PlanNode>): PlanNode => ({ "Node Type": "Index Only Scan", "Index Name": "x", "Actual Rows": 50, "Actual Loops": 1, ...node });
  const limit = (child: PlanNode): PlanNode => ({ "Node Type": "Limit", "Actual Rows": 50, Plans: [child] });
  test("an empty or absent plan is NOT MEASURED, never PASS", () => {
    expect(agentGate("exact", null).verdict).toBe("NOT MEASURED");
    expect(agentGate("exact", { "Node Type": "Limit", "Actual Rows": 0 }).verdict).toBe("NOT MEASURED");
  });
  test("a name applied as a filter fails, however few rows it reads", () => {
    expect(agentGate("selective-prefix", limit(scan({ "Index Cond": "(org_id = 'a')", "Rows Removed by Filter": 10 }))).verdict).toBe("FAIL");
  });
  test("a name in the Index Cond passes", () => {
    expect(agentGate("selective-prefix", limit(scan({ "Index Cond": "((org_id = 'a') AND (agent_name_normalized ~>=~ 'x'))" }))).verdict).toBe("PASS");
  });
  test("a broad prefix may walk recency, within 100x", () => {
    expect(agentGate("broad-prefix", limit(scan({ "Index Cond": "(org_id = 'a')", "Rows Removed by Filter": 100 }))).verdict).toBe("PASS");
    expect(agentGate("broad-prefix", limit(scan({ "Index Cond": "(org_id = 'a')", "Rows Removed by Filter": 6000 }))).verdict).toBe("FAIL");
  });
});

describe("guard and stats", () => {
  test("refuses anything but a local target", () => {
    expect(() => assertLocalTarget("postgres://u:p@db.example.supabase.co:5432/x", {})).toThrow("non-local");
    expect(() => assertLocalTarget("postgres://u:p@localhost/x?host=db.example.com", {})).toThrow("host override");
    expect(() => assertLocalTarget("postgres://u:p@localhost/x", { PGHOSTADDR: "10.0.0.1" })).toThrow("PGHOSTADDR");
    expect(() => assertLocalTarget("postgres://u:p@127.0.0.1:5432/x", {})).not.toThrow();
  });
  test("percentile is nearest-rank", () => {
    const xs = Array.from({ length: 100 }, (_, i) => i + 1);
    expect(percentile(xs, 50)).toBe(50);
    expect(percentile(xs, 99)).toBe(99);
    expect(percentile([], 50)).toBeNull();
  });
});
