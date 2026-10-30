import { describe, expect, test } from "bun:test";
import {
  REQUIRED_SEARCH_INDEXES,
  auditSearchIndexes,
  type SearchIndexCatalogRow,
} from "../search-index-contract.ts";

function catalogRows(): SearchIndexCatalogRow[] {
  return REQUIRED_SEARCH_INDEXES.map((requirement) => ({
    table_name: requirement.tableName,
    index_name: requirement.indexName,
    index_definition: `CREATE INDEX ${requirement.indexName} ${requirement.definitionFragments.join(" ")}`,
    is_valid: true,
    is_ready: true,
    reloptions: requirement.reloptionFragments?.join(",") ?? null,
  }));
}

describe("production search index contract", () => {
  test("passes only when every consumer index is structurally ready", () => {
    const report = auditSearchIndexes(catalogRows());
    expect(report.passed).toBe(true);
    expect(report.indexes.every((index) => index.ready)).toBe(true);
  });

  test("fails closed when an index is absent", () => {
    const rows = catalogRows();
    const missing = rows.pop()!;
    const report = auditSearchIndexes(rows);
    expect(report.passed).toBe(false);
    expect(report.indexes.find((index) => index.indexName === missing.index_name)?.reasons).toEqual(["missing"]);
  });

  test("rejects invalid, unfinished, or structurally different indexes", () => {
    const rows = catalogRows();
    rows[0] = {
      ...rows[0]!,
      is_valid: false,
      is_ready: false,
      index_definition: "CREATE INDEX wrong ON sessions.session_rollups (org_id)",
    };
    const report = auditSearchIndexes(rows);
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
    const report = auditSearchIndexes(rows);
    expect(report.passed).toBe(false);
    expect(report.indexes.find((index) => index.indexName === row.index_name)?.reasons).toContain(
      "option missing: fastupdate=off",
    );
  });
});
