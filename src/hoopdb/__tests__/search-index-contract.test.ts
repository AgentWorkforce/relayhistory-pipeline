import { describe, expect, test } from "bun:test";
import {
  MIN_AGENT_PREFIX_CHARS,
  REQUIRED_SEARCH_INDEXES,
  SESSION_DIRECTORY_SEARCH_BOUNDARY,
  agentPrefixPattern,
  agentPrefixRange,
  NonAsciiAgentPrefixError,
  NonAsciiAgentFilterError,
  agentExactValue,
  textPatternUpperBound,
  auditSearchIndexes,
  isUsefulAgentPrefix,
  normalizeAgentName,
  type SearchIndexCatalogRow,
} from "../search-index-contract.ts";

const READY = { directoryProjectionComplete: true } as const;

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

describe("decision 0002 search index contract", () => {
  test("passes only when every logical-directory index is structurally ready", () => {
    const report = auditSearchIndexes(catalogRows(), READY);
    expect(report.passed).toBe(true);
    expect(report.indexes.every((index) => index.ready)).toBe(true);
    expect(
      report.indexes.every((index) =>
        index.tableName.startsWith("session_directory"),
      ),
    ).toBe(true);
  });

  test("fails closed when an index is absent", () => {
    const rows = catalogRows();
    const missing = rows.pop()!;
    const report = auditSearchIndexes(rows, READY);
    expect(report.passed).toBe(false);
    expect(
      report.indexes.find((index) => index.indexName === missing.index_name)
        ?.reasons,
    ).toEqual(["missing"]);
  });

  test("rejects invalid, unfinished, or structurally different indexes", () => {
    const rows = catalogRows();
    rows[0] = {
      ...rows[0]!,
      is_valid: false,
      is_ready: false,
      index_definition:
        "CREATE INDEX wrong ON sessions.session_directory (org_id)",
    };
    const report = auditSearchIndexes(rows, READY);
    expect(report.passed).toBe(false);
    expect(report.indexes[0]!.reasons).toContain("invalid");
    expect(report.indexes[0]!.reasons).toContain("not ready");
    expect(
      report.indexes[0]!.reasons.some((reason) =>
        reason.startsWith("definition missing:"),
      ),
    ).toBe(true);
  });

  test("rejects an unexpected or changed partial predicate", () => {
    const rows = catalogRows();
    const row = rows.find(
      (candidate) =>
        candidate.index_name === "session_directory_agent_recent_idx",
    )!;
    row.predicate = `(${row.predicate} AND false)`;
    expect(
      auditSearchIndexes(rows, READY).indexes.find(
        (index) => index.indexName === row.index_name,
      )?.reasons,
    ).toContain("predicate mismatch");
  });

  test("fails until the logical-session projector and backfill declare completion", () => {
    const report = auditSearchIndexes(catalogRows(), {
      directoryProjectionComplete: false,
    });
    expect(report.passed).toBe(false);
    expect(
      report.indexes.every((index) =>
        index.reasons.includes("directory projection incomplete"),
      ),
    ).toBe(true);
  });

  test("defines an explicit metadata-only search boundary", () => {
    expect(SESSION_DIRECTORY_SEARCH_BOUNDARY.metadataOnly).toBe(true);
    expect(SESSION_DIRECTORY_SEARCH_BOUNDARY.fields).toContain("agent_name");
    expect(SESSION_DIRECTORY_SEARCH_BOUNDARY.excluded).toContain(
      "transcript content",
    );
    expect(
      REQUIRED_SEARCH_INDEXES.some(
        (requirement) => requirement.tableName === "convergence_events",
      ),
    ).toBe(false);
  });
});

describe("agent-name lookup contract", () => {
  test("normalizes exact names and requires a useful prefix", () => {
    expect(normalizeAgentName("  Helm-Chart-Chief ")).toBe("helm-chart-chief");
    for (const value of ["ΟΔΟΣ", "İstanbul", "Ägent", "日本語"]) {
      expect(() => normalizeAgentName(value)).toThrow(NonAsciiAgentFilterError);
      expect(() => agentExactValue(value)).toThrow(/p_agent_match => 'exact'/);
      expect(() => agentPrefixPattern(value)).toThrow(NonAsciiAgentPrefixError);
    }
    expect(MIN_AGENT_PREFIX_CHARS).toBe(3);
    expect(isUsefulAgentPrefix("he")).toBe(false);
    expect(isUsefulAgentPrefix("hel")).toBe(true);
  });

  test("escapes LIKE metacharacters without changing prefix semantics", () => {
    expect(agentPrefixPattern("Helm_100%")).toBe("helm\\_100\\%%");
    expect(() => agentPrefixPattern("ab")).toThrow(RangeError);
  });

  test("builds finite text-pattern bounds for leakproof RLS prefix planning", () => {
    expect(agentPrefixRange("Helm_100%")).toEqual({
      lower: "helm_100%",
      upper: "helm_100&",
      pattern: "helm\\_100\\%%",
    });
    // The carry itself, on the shared byte-order helper.
    expect(textPatternUpperBound(`ab${String.fromCodePoint(0x10ffff)}`)).toBe(
      "ac",
    );
    expect(textPatternUpperBound(`ab${String.fromCodePoint(0xd7ff)}`)).toBe(
      `ab${String.fromCodePoint(0xe000)}`,
    );
    expect(() =>
      textPatternUpperBound(String.fromCodePoint(0x10ffff).repeat(3)),
    ).toThrow(RangeError);
    // The direct bounded path accepts ASCII only and points elsewhere for the rest.
    for (const value of [
      `ab${String.fromCodePoint(0x10ffff)}`,
      "ÄGENT",
      "café",
      "日本語",
    ]) {
      expect(() => agentPrefixRange(value)).toThrow(NonAsciiAgentPrefixError);
    }
    expect(() => agentPrefixRange("ÄGENT")).toThrow(/search_session_directory/);
  });
});
