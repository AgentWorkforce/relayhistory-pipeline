/**
 * buildSummarySearchQuery against a real ClickHouse with the brief-documents DDL
 * (session-pipeline/brief-documents-schema.ts). Skipped unless READ_API_TEST_CLICKHOUSE_URL
 * is set (CI sets it; see support.ts).
 *
 * Merges are stopped and every version is its own INSERT, so stale versions coexist with
 * newer ones exactly as they do in production before a merge folds them.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { BRIEF_DOCUMENTS_TABLE, briefDocumentSchemaStatements } from "../../session-pipeline/brief-documents-schema.ts";
import { buildSummarySearchQuery, foldSummaryText, type SummaryCandidateRow } from "../clickhouse/summary-search-query.ts";
import { READ_SETTINGS } from "../queries.ts";
import { CH_URL, freshClickHouse } from "./support.ts";

const suite = CH_URL ? describe : describe.skip;
let ch: ClickHouseClient & { drop(): Promise<void> };

interface Doc { org?: string; ws?: string; source?: string; session: string; epoch: number; deleted?: boolean; title?: string; summary?: string; at?: string }

async function put(d: Doc) {
  const deleted = d.deleted === true;
  const title = deleted ? "" : (d.title ?? "");
  const summary = deleted ? "" : (d.summary ?? "");
  await ch.insert(BRIEF_DOCUMENTS_TABLE, [{
    org_id: d.org ?? "org-a", workspace_id: d.ws ?? "ws-a", source: d.source ?? "codex", session_id: d.session,
    epoch: String(d.epoch), document_hash: "a".repeat(64), deleted: deleted ? 1 : 0, visible: 1,
    title, summary, title_folded: foldSummaryText(title), summary_folded: foldSummaryText(summary), project_id: "",
    last_activity_at: deleted ? "1970-01-01 00:00:00.000000" : (d.at ?? "2026-11-01 10:00:00.000000"),
  }]);
}

async function search(q: string, limit = 10, tenant = { orgId: "org-a", workspaceId: "ws-a" }) {
  const b = buildSummarySearchQuery(ch.database, { ...tenant, q, limit });
  const rows = await ch.select<SummaryCandidateRow>(b.query, { params: b.params, settings: { ...READ_SETTINGS } });
  return rows.map((r) => `${r.source}:${r.session_id}`);
}

suite("summary search on ClickHouse", () => {
  beforeAll(async () => {
    ch = await freshClickHouse();
    for (const statement of briefDocumentSchemaStatements()) await ch.command(statement);
    await ch.command(`SYSTEM STOP MERGES ${BRIEF_DOCUMENTS_TABLE}`);
  });
  afterAll(async () => {
    await ch?.drop();
  });

  test("a cleared or tombstoned summary is never matched through its stale predecessor", async () => {
    await put({ session: "cleared", epoch: 1, summary: "rotated the hunter2 credential" });
    await put({ session: "cleared", epoch: 2, summary: "rotated a credential" });
    await put({ session: "erased", epoch: 3, summary: "hunter2 in the erased session" });
    await put({ session: "erased", epoch: 4, deleted: true });
    const [n] = await ch.select<{ n: number }>(`SELECT count() AS n FROM ${BRIEF_DOCUMENTS_TABLE} WHERE session_id IN ('cleared', 'erased')`);
    expect(Number(n!.n)).toBe(4);
    expect(await search("hunter2")).toEqual([]);
    expect(await search("ROTATED A")).toEqual(["codex:cleared"]);
  });

  test("title or summary, case-folded, newest activity first, tenant-scoped, limit + 1", async () => {
    await put({ session: "old", epoch: 10, summary: "Pipeline erasure fix", at: "2026-10-01 00:00:00.000000" });
    await put({ session: "new", source: "claude", epoch: 11, title: "ERASURE follow-up", at: "2026-11-02 00:00:00.000000" });
    await put({ session: "other-ws", ws: "ws-b", epoch: 12, summary: "erasure in another workspace" });
    await put({ session: "other-org", org: "org-b", epoch: 13, summary: "erasure in another org" });
    expect(await search("Erasure")).toEqual(["claude:new", "codex:old"]);
    expect(await search("erasure", 1)).toEqual(["claude:new", "codex:old"]);
    expect(await search("erasure", 10, { orgId: "org-a", workspaceId: "ws-b" })).toEqual(["codex:other-ws"]);
  });
});
