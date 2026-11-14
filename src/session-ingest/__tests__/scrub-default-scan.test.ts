/**
 * scripts/scrub-default-scan.ts (docs/scrub-engine.md, "Default flip"): the repository
 * passes, and each rule has a planted-violation test: a violation planted in an otherwise
 * clean copy of the tracked sources makes the scan fail with that rule. Entry-shape coverage
 * (G1 .mjs, G2 Worker default export, G8 Lambda handler export, S11 `const x = await`), the
 * engine-running-name check (S9), require edges (G3) and string-only initialisers (G6) are
 * planted too. Known limits are listed in the scan header.
 */
import { describe, expect, test } from "bun:test";
import { ENTRY_EXEMPT, HOSTED_ENTRIES, HOSTED_GRANT_MODULES, isExecutableEntry, isHandlerEntry, scan, seamChain, trackedSources, TS_CAPABILITY_DECLARERS, type Sources } from "../../../scripts/scrub-default-scan.ts";

const repo = trackedSources();
const mutate = (path: string, edit: (src: string) => string): Sources => {
  const files = new Map(repo);
  const before = files.get(path) ?? "";
  const after = edit(before);
  expect(after).not.toBe(before);
  files.set(path, after);
  return files;
};
const plant = (path: string, src: string, base: Sources = repo): Sources => {
  expect(base.has(path)).toBe(false);
  return new Map(base).set(path, src);
};
const SEAM = "packages/pipeline/src/lib/scrub-engine/index";
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

  // S9: importing only an engine-running name from the seam makes an entry reach it.
  test.each(["scrubText", "scrubTextForLog", "activeScrubEngine"])("an entry importing only %s from the seam needs an initialiser", (name) => {
    const f = "scripts/planted-s9.ts";
    expect(rules(plant(f, `import { ${name} } from "../${SEAM}.ts";\nconsole.log(${name});\n`))).toContain(`entry-without-init:${f}`);
  });

  test("an entry importing only inert seam names does not reach it", () => {
    const f = "scripts/planted-inert.ts";
    expect(rules(plant(f, `import { scrubEngineReady, MAX_SCRUB_CHARS } from "../${SEAM}.ts";\nconsole.log(scrubEngineReady(), MAX_SCRUB_CHARS);\n`))).not.toContain(`entry-without-init:${f}`);
  });

  // S11: a top-level await bound to a name is still an executable entry.
  test.each([
    "const x = await scrubText(\"a\");\n",
    "export const x = await scrubText(\"a\");\n",
    "let y: string = await scrubText(\"a\");\n",
  ])("top-level `%s` is an entry", (body) => {
    expect(isExecutableEntry("scripts/x.ts", `import { scrubText } from "../${SEAM}.ts";\n${body}`)).toBe(true);
    const f = "scripts/planted-s11.ts";
    expect(rules(plant(f, `import { scrubText } from "../${SEAM}.ts";\n${body}`))).toContain(`entry-without-init:${f}`);
  });

  // G1: .mjs sources are scanned (and trackedSources lists them).
  test("a planted .mjs entry reaching the seam fails", () => {
    const f = "scripts/planted-g1.mjs";
    expect(rules(plant(f, `import { scrubText } from "../${SEAM}.js";\nconsole.log(scrubText("a"));\n`))).toContain(`entry-without-init:${f}`);
  });

  // G2: a Cloudflare Worker module is a handler entry.
  test.each(["fetch(req) { return scrubText(String(req)); }", "async queue(batch) { scrubText(String(batch)); }", "scheduled: async () => { scrubText(\"a\"); }"])(
    "a planted Worker default export { %s } reaching the seam fails",
    (member) => {
      const f = "packages/planted-worker/src/index.ts";
      const src = `import { scrubText } from "../../pipeline/src/lib/scrub-engine/index.ts";\nexport default {\n  ${member},\n};\n`;
      expect(isHandlerEntry(f, src)).toBe(true);
      expect(rules(plant(f, src))).toContain(`entry-without-init:${f}`);
    },
  );

  // G8: an AWS Lambda handler export is a handler entry.
  test.each([
    "export const handler = async (e: unknown) => scrubText(String(e));",
    "export async function handler(e: unknown) { return scrubText(String(e)); }",
    "export function handler(e: unknown) { return scrubText(String(e)); }",
  ])("a planted `%s` reaching the seam fails", (body) => {
    const f = "src/planted-g8/lambda.ts";
    const src = `import { scrubText } from "../../${SEAM}.ts";\n${body}\n`;
    expect(isHandlerEntry(f, src)).toBe(true);
    expect(rules(plant(f, src))).toContain(`entry-without-init:${f}`);
  });

  test("handler-shaped text in a comment or string is not a handler entry", () => {
    expect(isHandlerEntry("src/x.ts", "// export const handler = 1\nconst s = `\nexport default { fetch }\n`;\n")).toBe(false);
  });

  // G3: a literal require() edge is followed.
  test("an entry reaching the seam through require() fails", () => {
    const f = "scripts/planted-g3.ts";
    expect(rules(plant(f, `const { scrubText } = require("../${SEAM}.ts");\nconsole.log(scrubText("a"));\n`))).toContain(`entry-without-init:${f}`);
  });

  // G6: an initialiser named only in a comment or string does not count.
  test("an initialiser only in a string or comment does not satisfy the rule", () => {
    const f = "scripts/planted-g6.ts";
    const src = `import { scrubText } from "../${SEAM}.ts";\n// initScrubEngine()\nconsole.log("initScrubEngine()", scrubText("a"));\n`;
    expect(rules(plant(f, src))).toContain(`entry-without-init:${f}`);
    expect(rules(plant(f, src.replace("// initScrubEngine()", `import { initScrubEngine } from "../${SEAM}.ts";\nawait initScrubEngine();`)))).not.toContain(`entry-without-init:${f}`);
  });

  // Module-load grants (F1).
  test("every hosted entry imports its grant module first, as a bare import", () => {
    for (const [f, kind] of Object.entries(HOSTED_ENTRIES)) expect(repo.get(f) ?? "", f).toContain(HOSTED_GRANT_MODULES[kind].split("/").at(-1)!);
  });

  test.each(Object.keys(HOSTED_ENTRIES))("removing the module-load grant import from %s fails", (f) => {
    const files = mutate(f, (s) => s.replace(/^import\s+["'][^"']*hosted-(?:lambda|worker)-grant\.(?:js|ts)["'];[^\n]*\n/m, ""));
    expect(rules(files)).toContain(`hosted-grant-first:${f}`);
  });

  test("a hosted entry whose grant import is not first fails", () => {
    const f = "src/session-ingest/buffered-gc-lambda.ts";
    const files = mutate(f, (s) => {
      const line = s.match(/^import\s+["'][^"']*hosted-lambda-grant\.(?:js|ts)["'];[^\n]*\n/m)![0];
      const rest = s.replace(line, "");
      const at = rest.search(/^import\s[^\n]*from\s/m);
      const end = rest.indexOf("\n", at) + 1;
      return rest.slice(0, end) + line + rest.slice(end);
    });
    expect(rules(files)).toContain(`hosted-grant-first:${f}`);
  });

  test("a hosted entry importing the wrong grant module fails", () => {
    const f = "src/session-ingest/buffered-gc-lambda.ts";
    expect(rules(mutate(f, (s) => s.replace("hosted-lambda-grant", "hosted-worker-grant")))).toContain(`hosted-grant-first:${f}`);
  });

  test("a self-host main importing a hosted entry reaches the grant and fails", () => {
    const f = "src/session-pipeline/large-read-main.ts";
    const files = mutate(f, (s) => `import "../session-ingest/buffered-gc-lambda.ts";\n${s}`);
    expect(rules(files)).toContain(`hosted-grant-reach:${f}`);
  });

  test("a self-host script importing a grant module directly fails", () => {
    const f = "scripts/proof-buffer-gc.ts";
    const files = mutate(f, (s) => `import "../packages/pipeline/src/lib/scrub-engine/hosted-lambda-grant.ts";\n${s}`);
    expect(rules(files)).toEqual(expect.arrayContaining([`hosted-grant-reach:${f}`]));
  });

  test("a wrapper that stops re-exporting its handler fails", () => {
    const f = "src/session-pipeline/lambda-entry.ts";
    expect(rules(mutate(f, (s) => s.replace(/^export \{ handler \} from "\.\/lambda\.ts";\n/m, "export const handler = () => undefined;\n")))).toContain(`hosted-grant-first:${f}`);
  });

  test("a stacks main pointed back at the shared lambda.ts fails", () => {
    const f = "stacks/aws/session-pipeline.ts";
    expect(rules(mutate(f, (s) => s.replace('"src/session-pipeline/lambda-entry.ts"', '"src/session-pipeline/lambda.ts"')))).toContain(`deploy-main:${f}`);
  });

  test("a wrangler main pointed at a non-hosted module fails", () => {
    const f = "packages/session-read-worker/wrangler.jsonc";
    expect(rules(mutate(f, (s) => s.replace(/("main"\s*:\s*")[^"]+"/, '$1src/planted.ts"')))).toContain(`deploy-main:${f}`);
  });
});
