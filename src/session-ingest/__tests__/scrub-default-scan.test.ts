/**
 * scripts/scrub-default-scan.ts (docs/scrub-engine.md, "Default flip"): the repository
 * passes, and each rule is mutation-effective: a planted violation in an otherwise
 * clean copy of the tracked sources makes the scan fail with that rule.
 */
import { describe, expect, test } from "bun:test";
import { ENTRY_EXEMPT, isExecutableEntry, scan, seamChain, trackedSources, TS_CAPABILITY_DECLARERS, type Sources } from "../../../scripts/scrub-default-scan.ts";

const repo = trackedSources();
const mutate = (path: string, edit: (src: string) => string): Sources => {
  const files = new Map(repo);
  const before = files.get(path) ?? "";
  const after = edit(before);
  expect(after).not.toBe(before);
  files.set(path, after);
  return files;
};
const rules = (files: Sources) => scan(files).map((v) => `${v.rule}:${v.file}`);

describe("scrub-default-scan", () => {
  test("the repository passes", () => {
    expect(scan(repo)).toStrictEqual([]);
  });

  test("the hosted declarers really declare (the allowlist is not stale)", () => {
    for (const f of TS_CAPABILITY_DECLARERS) expect(repo.get(f) ?? "", f).toMatch(/declareScrubTsCapability\s*\(/);
  });

  test("a planted declaration in a Node self-host file fails", () => {
    const f = "src/migration/neon-s3-sync/main.ts";
    const files = mutate(f, (s) => `import { declareScrubTsCapability } from "../../../packages/pipeline/src/lib/scrub-engine/ts-capability.ts";\ndeclareScrubTsCapability("test");\n${s}`);
    expect(rules(files)).toEqual(expect.arrayContaining([`ts-capability-declarer:${f}`, `ts-capability-importer:${f}`]));
  });

  test("a planted re-export of the grant module from the public seam index fails", () => {
    const f = "packages/pipeline/src/lib/scrub-engine/index.ts";
    const files = mutate(f, (s) => `${s}\nexport { declareScrubTsCapability } from "./ts-capability.ts";\n`);
    expect(rules(files)).toContain(`ts-capability-reexport:${f}`);
  });

  test("a production source importing a test preload fails", () => {
    const f = "src/session-ingest/worker.ts";
    const files = mutate(f, (s) => `import "../../scripts/test-preload/scrub-ts-capability.ts";\n${s}`);
    expect(rules(files)).toContain(`preload-import:${f}`);
    const g = "packages/session-read-worker/src/index.ts";
    expect(rules(mutate(g, (s) => `import "../../pipeline/test/setup/scrub-ts-capability.ts";\n${s}`))).toContain(`preload-import:${g}`);
  });

  test.each([
    "src/migration/neon-s3-sync/main.ts",
    "scripts/opensearch-proof/e2e.ts",
    "scripts/proof-buffer-gc.ts",
    "src/proof/aws/cli.ts",
    "src/session-erase/user-erase-main.ts",
    "src/session-ingest/rearm-sweeper-main.ts",
    "src/session-pipeline/large-read-main.ts",
  ])("removing the initialiser from %s fails", (f) => {
    expect(seamChain(repo, f)).toBeDefined();
    const files = mutate(f, (s) => s.replace(/\binitScrubEngine\s*\(/g, "noScrubEngine("));
    expect(rules(files)).toContain(`entry-without-init:${f}`);
  });

  test("every exemption is an executable entry that reaches the seam, with a reason", () => {
    for (const [f, why] of Object.entries(ENTRY_EXEMPT)) {
      expect(why.length, f).toBeGreaterThan(10);
      expect(isExecutableEntry(f, repo.get(f) ?? ""), f).toBe(true);
      expect(seamChain(repo, f), f).toBeDefined();
    }
  });

  test("an exemption that stops reaching the seam is reported stale", () => {
    const f = "scripts/ops/claim-records-archive.ts";
    const files = mutate(f, (s) => s.replace(/^import[^\n]*claim-records-archive[^\n]*\n/gm, ""));
    expect(rules(files)).toContain(`stale-exemption:${f}`);
  });

  test("entry detection", () => {
    expect(isExecutableEntry("scripts/x.ts", "import a from 'b';\nawait a();\n")).toBe(true);
    expect(isExecutableEntry("scripts/x.ts", "if (import.meta.main) main();\n")).toBe(true);
    expect(isExecutableEntry("scripts/x.ts", "import a from 'b';\ntry {\n  a();\n} finally {}\n")).toBe(true);
    expect(isExecutableEntry("scripts/x.ts", "export function a() {\nreturn 1;\n}\nconst b = `\nawait c\n`;\n")).toBe(false);
    expect(isExecutableEntry("src/x/__tests__/a.ts", "await a();\n")).toBe(false);
  });
});
