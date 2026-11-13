import { describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  checkPlan,
  discoverTestFiles,
  loadWeights,
  planShards,
  TEST_FILE,
  weightOf,
} from "../../../scripts/ci/unit-test-shards.ts";

const repo = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const weights = loadWeights(join(repo, "scripts/ci/unit-test-weights.json"));
const files = discoverTestFiles(repo);
// The four heaviest files, ordered as planShards orders them; longest-first places each on its own shard.
const HEAVY = [...files]
  .sort((a, b) => weightOf(b, weights) - weightOf(a, weights) || (a < b ? -1 : a > b ? 1 : 0))
  .slice(0, 4);

describe("unit-test shard planner", () => {
  test("discovers the repository's test files, sorted, including this one", () => {
    expect(files.length).toBeGreaterThan(400);
    expect(files).toContain("src/shared/__tests__/unit-test-shards.test.ts");
    expect(files.some((f) => f.endsWith(".pglite.test.ts"))).toBe(true);
    expect(files.some((f) => f.endsWith(".postgres.test.ts"))).toBe(true);
    expect([...files].sort()).toEqual(files);
    for (const file of Object.keys(weights.files)) expect(files).toContain(file);
  });

  test("matches every pattern bun test runs and nothing else", () => {
    for (const name of ["a.test.ts", "a.test.tsx", "a.test.js", "a.test.jsx", "a.test.mjs", "a_test.ts", "a.spec.ts", "a_spec.js", "a.pglite.test.ts", "a.postgres.test.ts"]) {
      expect(TEST_FILE.test(name)).toBe(true);
    }
    for (const name of ["image_copy_test.py", "support.ts", "a.test.ts.snap", "test.ts", "a.tests.ts"]) {
      expect(TEST_FILE.test(name)).toBe(false);
    }
    const root = mkdtempSync(join(tmpdir(), "shards-"));
    try {
      for (const rel of ["src/a/x.test.ts", "src/a/y_spec.js", "src/b/z.spec.tsx", "src/b/helper.ts", "src/node_modules/m/n.test.ts", "src/.hidden/h.test.ts"]) {
        mkdirSync(dirname(join(root, rel)), { recursive: true });
        writeFileSync(join(root, rel), "");
      }
      expect(discoverTestFiles(root)).toEqual(["src/a/x.test.ts", "src/a/y_spec.js", "src/b/z.spec.tsx"]);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  for (const of of [1, 2, 4, 8]) {
    test(`every test file lands in exactly one of ${of} shards`, () => {
      const shards = planShards(files, of, weights);
      expect(shards).toHaveLength(of);
      expect(checkPlan(files, shards)).toEqual([]);
      expect(shards.flatMap((s) => s.files).sort()).toEqual(files);
      for (const shard of shards) expect(shard.files.length).toBeGreaterThan(0);
    });
  }

  test("assignment is stable regardless of input order", () => {
    const reversed = [...files].reverse();
    const shuffled = [...files].sort((a, b) => (a.length - b.length) || (a < b ? 1 : -1));
    const expected = planShards(files, 4, weights);
    expect(planShards(reversed, 4, weights)).toEqual(expected);
    expect(planShards(shuffled, 4, weights)).toEqual(expected);
  });

  test("heavy files are spread across different shards", () => {
    const shards = planShards(files, 4, weights);
    const homes = HEAVY.map((file) => shards.findIndex((s) => s.files.includes(file)));
    expect(new Set(homes).size).toBe(HEAVY.length);
    const seconds = shards.map((s) => s.seconds);
    expect(Math.max(...seconds) - Math.min(...seconds)).toBeLessThan(weightOf(HEAVY[1], weights));
  });

  test("checkPlan reports missing, duplicated and unknown files", () => {
    const shards = [
      { index: 1, seconds: 0, files: ["src/a.test.ts", "src/b.test.ts"] },
      { index: 2, seconds: 0, files: ["src/b.test.ts", "src/x.test.ts"] },
    ];
    expect(checkPlan(["src/a.test.ts", "src/b.test.ts", "src/c.test.ts"], shards)).toEqual([
      "src/b.test.ts: in 2 shards",
      "src/c.test.ts: in 0 shards",
      "src/x.test.ts: planned but not discovered",
    ]);
  });

  test("rejects invalid shard counts and weights", () => {
    expect(() => planShards(files, 0, weights)).toThrow();
    expect(() => planShards(files, 1.5, weights)).toThrow();
    const root = mkdtempSync(join(tmpdir(), "weights-"));
    try {
      writeFileSync(join(root, "w.json"), JSON.stringify({ defaults: { pglite: 60, other: 5 }, files: { "src/a.test.ts": -1 } }));
      expect(() => loadWeights(join(root, "w.json"))).toThrow();
      writeFileSync(join(root, "w.json"), JSON.stringify({ files: {} }));
      expect(() => loadWeights(join(root, "w.json"))).toThrow();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
