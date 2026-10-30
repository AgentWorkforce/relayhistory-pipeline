import { describe, expect, test } from "bun:test";
import {
  REQUIRED_SEARCH_INDEXES,
  auditSearchIndexes,
  type SearchIndexCatalogRow,
} from "../search-index-contract.ts";

const READY = { agentLookupRolloutComplete: true } as const;

function catalogRows(): SearchIndexCatalogRow[] {
  return REQUIRED_SEARCH_INDEXES.map((requirement) => ({
    table_name: requirement.tableName,
    index_name: requirement.indexName,
    index_definition: `CREATE INDEX ${requirement.indexName} ${requirement.definitionFragments.join(" ")}`,
    predicate: requirement.expectedPredicate ?? null,
    is_valid: true,
    is_ready: true,
    reloptions: requirement.reloptionFragments?.join(",") ?? null,
  }));
}

describe("production search index contract", () => {
  test("passes only when every consumer index is structurally ready", () => {
    const report = auditSearchIndexes(catalogRows(), READY);
    expect(report.passed).toBe(true);
    expect(report.indexes.every((index) => index.ready)).toBe(true);
  });

  test("fails closed when an index is absent", () => {
    const rows = catalogRows();
    const missing = rows.pop()!;
    const report = auditSearchIndexes(rows, READY);
    expect(report.passed).toBe(false);
    expect(report.indexes.find((index) => index.indexName === missing.index_name)?.reasons).toEqual(["missing"]);
  });

  test("requires the timing and opening-summary indexes used by documented surfaces", () => {
    for (const indexName of [
      "session_markers_timing_lookup_idx",
      "convergence_events_opening_prompt_idx",
    ]) {
      const report = auditSearchIndexes(
        catalogRows().filter((row) => row.index_name !== indexName),
        READY,
      );
      expect(report.passed).toBe(false);
      expect(report.indexes.find((index) => index.indexName === indexName)?.reasons).toEqual(["missing"]);
    }
  });

  test("rejects invalid, unfinished, or structurally different indexes", () => {
    const rows = catalogRows();
    rows[0] = {
      ...rows[0]!,
      is_valid: false,
      is_ready: false,
      index_definition: "CREATE INDEX wrong ON sessions.session_rollups (org_id)",
    };
    const report = auditSearchIndexes(rows, READY);
    expect(report.passed).toBe(false);
    const item = report.indexes[0]!;
    expect(item.reasons).toContain("invalid");
    expect(item.reasons).toContain("not ready");
    expect(item.reasons.some((reason) => reason.startsWith("definition missing:"))).toBe(true);
  });

  test("requires direct-write GIN options for hot transcript indexes", () => {
    const rows = catalogRows();
    const row = rows.find((candidate) => candidate.index_name === "convergence_events_content_prefix_trgm_idx")!;
    row.reloptions = null;
    const report = auditSearchIndexes(rows, READY);
    expect(report.passed).toBe(false);
    expect(report.indexes.find((index) => index.indexName === row.index_name)?.reasons).toContain(
      "option missing: fastupdate=off",
    );
  });

  test("rejects a restrictive predicate on an unrestricted index", () => {
    const rows = catalogRows();
    const row = rows.find((candidate) => candidate.index_name === "session_rollups_workspace_recent_idx")!;
    row.predicate = "false";
    row.index_definition += " WHERE false";
    const report = auditSearchIndexes(rows, READY);
    expect(report.passed).toBe(false);
    expect(report.indexes.find((index) => index.indexName === row.index_name)?.reasons).toContain(
      "unexpected predicate",
    );
  });

  test("rejects an additional restriction on an expected partial index", () => {
    const rows = catalogRows();
    const row = rows.find((candidate) => candidate.index_name === "convergence_events_task_title_direct_trgm_idx")!;
    row.predicate = `(${row.predicate} AND false)`;
    const report = auditSearchIndexes(rows, READY);
    expect(report.passed).toBe(false);
    expect(report.indexes.find((index) => index.indexName === row.index_name)?.reasons).toContain(
      "predicate mismatch",
    );
  });

  test("normalizes superficial predicate formatting", () => {
    const rows = catalogRows();
    const row = rows.find((candidate) => candidate.index_name === "convergence_events_task_title_direct_trgm_idx")!;
    row.predicate = "  (TASK_TITLE   IS NOT NULL)  ";
    expect(auditSearchIndexes(rows, READY).passed).toBe(true);
  });

  test("fails until the agent lookup projection rollout is complete", () => {
    const report = auditSearchIndexes(catalogRows(), { agentLookupRolloutComplete: false });
    expect(report.passed).toBe(false);
    expect(
      report.indexes.find((index) => index.indexName === "session_catalog_agents_lookup_idx")?.reasons,
    ).toContain("agent lookup rollout incomplete");
  });
});
