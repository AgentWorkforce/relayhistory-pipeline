import { describe, expect, it } from "vitest";
import {
  AdapterConfigError,
  AdapterRegistry,
  PIPELINE_RUNTIMES,
  SEARCH_INDEX_BACKENDS,
  validateAdapterEnv,
  type SearchIndexBackendName,
} from "../src/adapters/registry.js";

describe("adapter selection", () => {
  it("defaults to postgres search and the worker runtime when nothing is set", () => {
    expect(validateAdapterEnv({}, { runtime: ["workers"] })).toEqual({ search: "postgres", runtime: "workers" });
    expect(validateAdapterEnv({ SEARCH_INDEX_BACKEND: " ", PIPELINE_RUNTIME: "" }, { runtime: ["workers"] })).toEqual({
      search: "postgres",
      runtime: "workers",
    });
  });

  it("refuses unknown names and names the entry point cannot run", () => {
    expect(() => validateAdapterEnv({ SEARCH_INDEX_BACKEND: "elastic" }, { runtime: ["workers"] })).toThrow(AdapterConfigError);
    expect(() => validateAdapterEnv({ PIPELINE_RUNTIME: "lambda" }, { runtime: ["workers"] })).toThrow(/not allowed/);
    expect(
      validateAdapterEnv({ PIPELINE_RUNTIME: "lambda" }, { runtime: ["lambda"], runtimeDefault: "lambda", search: ["clickhouse"], searchDefault: "clickhouse" }),
    ).toEqual({ search: "clickhouse", runtime: "lambda" });
  });

  it("creates only registered adapters and fails closed otherwise", () => {
    const registry = new AdapterRegistry<SearchIndexBackendName, { tag: string }, string>("SEARCH_INDEX_BACKEND", SEARCH_INDEX_BACKENDS, "postgres");
    registry.register({ name: "postgres", create: (c) => `pg:${c.tag}` });
    expect(registry.create({}, { tag: "x" })).toBe("pg:x");
    expect(() => registry.create({ SEARCH_INDEX_BACKEND: "opensearch" }, { tag: "x" })).toThrow(/not registered/);
    expect(() => registry.register({ name: "postgres", create: () => "again" })).toThrow();
    expect(() => registry.register({ name: "solr" as SearchIndexBackendName, create: () => "" })).toThrow(AdapterConfigError);
    expect(registry.registered()).toEqual(["postgres"]);
  });

  it("knows the documented names", () => {
    expect(SEARCH_INDEX_BACKENDS).toEqual(["postgres", "clickhouse", "opensearch"]);
    expect(PIPELINE_RUNTIMES).toEqual(["workers", "lambda", "cloudrun", "terse"]);
  });
});
