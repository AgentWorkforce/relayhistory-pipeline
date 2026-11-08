import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { searchIndexConformance, syntheticDoc } from "../src/search-index/conformance.js";
import { matchesQuery, positionOf, type SearchIndexAdapter } from "../src/search-index/contracts.js";
import { WILDCARD_MAX_LITERAL_BYTES, createOpenSearchSearchIndex, type OpenSearchDialect } from "../src/search-index/opensearch.js";

// Real OpenSearch / Elasticsearch run of the shared suite. Skipped unless
// SEARCH_INDEX_OPENSEARCH_TEST_URL names a disposable, unauthenticated server
// (the opensearch-proof workflow's container, or a local one).
// SEARCH_INDEX_OPENSEARCH_TEST_DIALECT selects opensearch (default) or
// elasticsearch. Each run uses its own index prefix and deletes its index.
const url = process.env.SEARCH_INDEX_OPENSEARCH_TEST_URL;
const dialect = (process.env.SEARCH_INDEX_OPENSEARCH_TEST_DIALECT || "opensearch") as OpenSearchDialect;
const label = `${dialect} (real server)`;

function disposable() {
  const prefix = `conformance-${process.pid}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
  const adapter = createOpenSearchSearchIndex({ url: url!, dialect, indexPrefix: prefix, requestTimeoutMs: 60_000 });
  return {
    adapter,
    dispose: async () => {
      await fetch(`${url!.replace(/\/+$/, "")}/${prefix}-transcript-events`, { method: "DELETE" });
    },
  };
}

if (!url) {
  describe.skip("SearchIndexAdapter conformance: opensearch (real)", () => {
    it("needs SEARCH_INDEX_OPENSEARCH_TEST_URL", () => {});
  });
} else {
  searchIndexConformance(label, async () => disposable());

  describe(`opensearch adapter parity beyond the suite: ${label}`, () => {
    let target: ReturnType<typeof disposable>;
    let a: SearchIndexAdapter;
    beforeAll(async () => {
      target = disposable();
      a = target.adapter;
      expect(await a.health()).toEqual({ ok: false, code: "schema_missing" });
      await a.ensureSchema();
    }, 120_000);
    afterAll(async () => {
      await target?.dispose();
    });

    it("re-writes a deleted key at a lower version, as postgres does once a row is gone", async () => {
      const org = `org-${Date.now().toString(36)}-gc`;
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const doc = syntheticDoc({ orgId: org, eventId: "gc", version: 5, content: "five" });
      expect(await a.upsert([doc])).toEqual({ written: 1 });
      expect(await a.upsert([{ ...doc, version: 4, content: "four" }])).toEqual({ written: 0 });
      expect(await a.deleteEvents(tenant, [positionOf(doc)])).toEqual({ deleted: 1 });
      expect(await a.upsert([{ ...doc, version: 1, content: "one" }])).toEqual({ written: 1 });
      expect((await a.query({ tenant, order: "asc", limit: 5 })).hits.map((h) => h.content)).toEqual(["one"]);
    });

    it("matches a 500-character q (one İ) across a window boundary of a long title", async () => {
      const org = `org-${Date.now().toString(36)}-win`;
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const title = [...Array(9000)].map((_, i) => String.fromCharCode(65 + ((i * 7) % 26))).join("");
      const doc = syntheticDoc({ orgId: org, eventId: "w", content: "", taskTitle: title });
      await a.upsert([doc]);
      for (const start of [0, 3500, 4000, 7999]) {
        const q = title.slice(start, start + 500).replace(/./, "İ");
        const hits = (await a.query({ tenant, q, order: "asc", limit: 5 })).hits.length;
        expect(hits, `start ${start}`).toBe(matchesQuery(doc, q) ? 1 : 0);
        const plain = title.slice(start, start + 500).toLowerCase();
        expect((await a.query({ tenant, q: plain, order: "asc", limit: 5 })).hits).toHaveLength(1);
      }
    });

    // Repetitive and maximally folded q (500 x U+0130 folds to 1000 code points):
    // the wildcard automata Lucene builds for these are the largest q can produce.
    it("matches repetitive and maximally folded 500-character q like the oracle", async () => {
      const org = `org-${Date.now().toString(36)}-rep`;
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const runs = `${"a".repeat(600)} ${"İ".repeat(600)} ${"ab".repeat(300)}`;
      const docs = [
        // Every run in one window.
        syntheticDoc({ orgId: org, eventId: "rep-short", content: "", taskTitle: runs }),
        // The runs past the first window boundary of a long title.
        syntheticDoc({ orgId: org, eventId: "rep-long", content: "", taskTitle: `${"x".repeat(3800)}${runs}` }),
        // One character short of every run: matches none of them.
        syntheticDoc({ orgId: org, eventId: "rep-near", content: `${"a".repeat(499)}b${"İ".repeat(499)}b${"ab".repeat(249)}`, taskTitle: "" }),
      ];
      expect(await a.upsert(docs)).toEqual({ written: docs.length });
      for (const [name, q] of [["a x 500", "a".repeat(500)], ["İ x 500", "İ".repeat(500)], ["ab x 250", "ab".repeat(250)]] as const) {
        const expected = docs.filter((d) => matchesQuery(d, q)).map((d) => d.eventId).sort();
        expect(expected, `${name} oracle`).toEqual(["rep-long", "rep-short"]);
        const hits = (await a.query({ tenant, q, order: "asc", limit: 10 })).hits.map((h) => h.eventId).sort();
        expect(hits, name).toEqual(expected);
      }
    }, 60_000);

    // The wildcard / script switch: a repetitive literal exactly at
    // WILDCARD_MAX_LITERAL_BYTES must still compile as a wildcard on the real
    // engine, and one byte more takes the script path; both match the oracle.
    it("matches repetitive q at and just past the wildcard byte bound like the oracle", async () => {
      const org = `org-${Date.now().toString(36)}-bound`;
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const n = WILDCARD_MAX_LITERAL_BYTES;
      const docs = [
        syntheticDoc({ orgId: org, eventId: "b-a", content: `x${"a".repeat(n + 1)}y`, taskTitle: "" }),
        syntheticDoc({ orgId: org, eventId: "b-emoji", content: "", taskTitle: `${"x".repeat(5000)}${"😀".repeat(n / 4 + 1)}` }),
        syntheticDoc({ orgId: org, eventId: "b-near", content: `${"a".repeat(n - 1)}b${"😀".repeat(n / 4 - 1)}`, taskTitle: "" }),
      ];
      expect(await a.upsert(docs)).toEqual({ written: docs.length });
      for (const q of ["a".repeat(n), "A".repeat(n + 1), "😀".repeat(n / 4), "😀".repeat(n / 4 + 1)]) {
        const expected = docs.filter((d) => matchesQuery(d, q)).map((d) => d.eventId).sort();
        expect(expected.length, `${q.length} oracle`).toBe(1);
        const hits = (await a.query({ tenant, q, order: "asc", limit: 10 })).hits.map((h) => h.eventId).sort();
        expect(hits, `${q.slice(0, 2)} x ${[...q].length}`).toEqual(expected);
      }
    }, 60_000);

    it("scans past index.max_result_window (10000) with search_after", async () => {
      const org = `org-${Date.now().toString(36)}-scan`;
      const docs = Array.from({ length: 10_050 }, (_, i) =>
        syntheticDoc({ orgId: org, tsMs: 1_000 + i, eventId: `s${String(i).padStart(5, "0")}`, content: "x", fingerprint: `f${i}` }),
      );
      for (let i = 0; i < docs.length; i += 5000) await a.upsert(docs.slice(i, i + 5000));
      const scope = { orgId: org, workspaceId: "ws-a", source: "claude", sessionIds: ["sess-1"] };
      const all = await a.scanFingerprints(scope, { fromMs: 0, limit: 20_000 });
      expect(all.truncated).toBe(false);
      expect(all.rows).toHaveLength(10_050);
      expect(all.rows.at(-1)!.eventId).toBe("s10049");
      const cut = await a.scanFingerprints(scope, { fromMs: 0, limit: 10_049 });
      expect(cut.truncated).toBe(true);
      expect(cut.rows).toHaveLength(10_049);
      await a.eraseScope(scope);
      expect((await a.scanFingerprints(scope, { fromMs: 0, limit: 10 })).rows).toEqual([]);
    }, 120_000);
  });
}
