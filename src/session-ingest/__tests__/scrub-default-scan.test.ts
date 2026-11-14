/**
 * scripts/scrub-default-scan.ts (docs/scrub-engine.md, "Default flip"): the repository
 * passes, and each rule has a planted-violation test: a violation planted in an otherwise
 * clean copy of the tracked sources makes the scan fail with that rule. Entry-shape coverage
 * (G1 .mjs, G2 Worker default export, G8 Lambda handler export, S11 `const x = await`), the
 * engine-running-name check (S9), require edges (G3) and string-only initialisers (G6) are
 * planted too, as are directory imports (N14), extensionless .mts/.mjs/.js resolution, plain .js
 * entries, workspace `exports` (an unknown shape is reported) and the exemption-reason checks
 * (M5b bypass, intermediate-only, extensionless paths). Known limits are listed in the scan header.
 */
import { describe, expect, test } from "bun:test";
import { edges, ENTRY_EXEMPT, HOSTED_ENTRIES, HOSTED_GRANT_MODULES, isExecutableEntry, isHandlerEntry, REASON_PATH_RE, resolveSpec, scan, seamChain, trackedSources, TS_CAPABILITY_DECLARERS, type Sources } from "../../../scripts/scrub-default-scan.ts";

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

  // hosted-grant-reach edge forms: each planted import of a grant module must be seen.
  const GRANT_REL = "../packages/pipeline/src/lib/scrub-engine/hosted-lambda-grant.ts";
  test.each([
    ["empty-braces import", `import {} from "${GRANT_REL}";\nexport const x = 1;\n`],
    ["package-specifier bare import", `import "@96bulls/pipeline/src/lib/scrub-engine/hosted-lambda-grant.ts";\nexport const x = 1;\n`],
    ["package-specifier empty-braces import", `import {} from "@96bulls/pipeline/src/lib/scrub-engine/hosted-worker-grant.ts";\nexport const x = 1;\n`],
    ["semicolon-less bare import followed by another import", `import "${GRANT_REL}"\nimport { log } from "../src/shared/log.ts";\nexport const x = log;\n`],
    ["template-literal dynamic import", `export const g = () => import(\`${GRANT_REL}\`);\n`],
    ["createRequire call", `import { createRequire } from "node:module";\ncreateRequire(import.meta.url)("${GRANT_REL}");\n`],
  ])("a planted %s of a grant module is a hosted-grant-reach violation", (_form, src) => {
    const f = "scripts/planted-reach.ts";
    expect(rules(plant(f, src))).toContain(`hosted-grant-reach:${f}`);
  });

  test("a planted file importing a reaching module through a semicolon-less bare import is flagged too", () => {
    const f = "scripts/planted-reach-chain.ts";
    const src = `import "../src/session-ingest/buffered-gc-lambda.ts"\nimport { log } from "../src/shared/log.ts"\nlog;\n`;
    expect(rules(plant(f, src))).toContain(`hosted-grant-reach:${f}`);
  });

  test("type-only imports of a grant module and a `${...}` template are not edges", () => {
    const f = "scripts/planted-reach-types.ts";
    const src = `import type {} from "${GRANT_REL}";\nimport { type X } from "${GRANT_REL}";\nconst k = "lambda";\nexport const g = () => import(\`../packages/pipeline/src/lib/scrub-engine/hosted-\${k}-grant.ts\`);\n`;
    expect(rules(plant(f, src))).not.toContain(`hosted-grant-reach:${f}`);
  });

  // hosted-grant-first: only a static bare import as the first statement counts (M1, M12, M13).
  const GC = "src/session-ingest/buffered-gc-lambda.ts";
  const GC_GRANT = /^import\s+"@rhc\/lib\/scrub-engine\/hosted-lambda-grant\.ts";[^\n]*\n/m;
  test.each([
    ["the grant import inside a template literal (M1)", (line: string) => `const s = \`\n${line}\`;\n`],
    ["`void import(grant)` (M12)", () => `void import("@rhc/lib/scrub-engine/hosted-lambda-grant.ts");\n`],
    ["top-level `await import(grant)` (M13)", () => `await import("@rhc/lib/scrub-engine/hosted-lambda-grant.ts");\n`],
    ["a namespace import of the grant", () => `import * as grant from "@rhc/lib/scrub-engine/hosted-lambda-grant.ts";\n`],
    ["an empty-braces import of the grant", () => `import {} from "@rhc/lib/scrub-engine/hosted-lambda-grant.ts";\n`],
  ])("a hosted entry whose first statement is %s fails hosted-grant-first", (_form, first) => {
    const files = mutate(GC, (s) => {
      const line = s.match(GC_GRANT)![0];
      const i = s.indexOf(line);
      return s.slice(0, i) + first(line) + s.slice(i + line.length);
    });
    expect(rules(files)).toContain(`hosted-grant-first:${GC}`);
  });

  test("a semicolon-less static bare grant import first passes hosted-grant-first", () => {
    const files = mutate(GC, (s) => s.replace(GC_GRANT, (l) => l.replace('.ts";', '.ts"')));
    expect(rules(files)).not.toContain(`hosted-grant-first:${GC}`);
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

  // hosted-grant-reach: forms a line-anchored regex parser misses. Edges come from
  // Bun.Transpiler.scanImports (plus import.meta.require / createRequire), so each must be seen.
  test.each([
    ["compact empty-braces import", `import{}from"${GRANT_REL}";\nexport const x = 1;\n`],
    ["compact namespace import", `import*as g from"${GRANT_REL}";\nexport const x = g;\n`],
    ["compact star re-export", `export*from"${GRANT_REL}";\n`],
    ["compact empty-braces re-export", `export{}from"${GRANT_REL}";\n`],
    ["bare import after a statement on the same line", `const a = 1; import "${GRANT_REL}";\nexport const x = a;\n`],
    ["bare import after a block comment on the same line", `/* x */ import "${GRANT_REL}";\nexport const x = 1;\n`],
    ["dynamic import with a comment before the specifier", `await import(/* c */ "${GRANT_REL}");\n`],
    ["dynamic import with an options argument", `await import("${GRANT_REL}", {});\n`],
    ["dynamic import with a trailing comma", `await import("${GRANT_REL}",);\n`],
    ["import.meta.require", `import.meta.require("${GRANT_REL}");\n`],
    ["unicode escape in the specifier", `import "${GRANT_REL.replace("hosted", "\\u0068osted")}";\nexport const x = 1;\n`],
    ["query suffix on the specifier", `import "${GRANT_REL}?x=1";\nexport const x = 1;\n`],
    ["hash suffix on the specifier", `import "${GRANT_REL}#h";\nexport const x = 1;\n`],
    ["indented bare import", `if (true) {}\n   import "${GRANT_REL}";\n`],
    ["bare import after a shebang and a directive", `#!/usr/bin/env bun\n"use strict";\nimport "${GRANT_REL}";\n`],
  ])("a planted %s of a grant module is a hosted-grant-reach violation", (_form, src) => {
    const f = "scripts/planted-reach.ts";
    expect(rules(plant(f, src))).toContain(`hosted-grant-reach:${f}`);
  });

  test("the unicode-escape plant really contains an escape (not a decoded character)", () => {
    expect(`"${GRANT_REL.replace("hosted", "\\u0068osted")}"`).toContain("\\u0068osted");
  });

  test("resolveSpec strips a query or hash suffix and resolves the tsconfig aliases (N8)", () => {
    expect(resolveSpec(repo, "scripts/x.ts", `${GRANT_REL}?x=1`)).toBe("packages/pipeline/src/lib/scrub-engine/hosted-lambda-grant.ts");
    expect(resolveSpec(repo, "scripts/x.ts", `${GRANT_REL}#h`)).toBe("packages/pipeline/src/lib/scrub-engine/hosted-lambda-grant.ts");
    expect(resolveSpec(repo, "scripts/x.ts", "@rhc-object-store")).toBe("packages/object-store/src/index.ts");
    expect(resolveSpec(repo, "scripts/x.ts", "@rhc/lib/scrub-engine/index.ts")).toBe(`${SEAM}.ts`);
  });

  // N10 and the parser: edges read like the module loader, not like a line regex.
  test.each([
    ["an indented bare import", `   import "./x.ts";\n`, "import"],
    ["a compact bare import", `import"./x.ts";\n`, "import"],
    ["a compact named import", `import{x}from'./x.ts';\nx;\n`, "import"],
    ["a compact named re-export", `export{x}from'./x.ts';\n`, "import"],
    ["a bare import after a shebang", `#!/usr/bin/env bun\nimport "./x.ts";\n`, "import"],
    ["a bare import after \"use strict\"", `"use strict";\nimport "./x.ts";\n`, "import"],
    ["import.meta.require", `import.meta.require("./x.ts");\n`, "require"],
    ["createRequire(...)(x)", `import { createRequire } from "node:module";\ncreateRequire(import.meta.url)("./x.ts");\n`, "require"],
  ])("edges() sees %s", (_form, src, kind) => {
    expect(edges(src).filter((e) => e.spec === "./x.ts").map((e) => e.kind)).toStrictEqual([kind]);
  });

  test("edges() ignores import text in strings, templates and comments, and type-only imports", () => {
    expect(edges(`const s = 'import "./a.ts"';\nconst t = \`\nimport "./b.ts"\n\`;\n// import "./c.ts"\n/* import "./d.ts" */\nimport type { T } from "./e.ts";\nimport { type U } from "./f.ts";\n`)).toStrictEqual([]);
  });

  test("a compact named import of an engine-running seam name in an entry needs an initialiser", () => {
    const f = "scripts/planted-compact.ts";
    expect(rules(plant(f, `import{scrubText}from'../${SEAM}.ts';\nawait scrubText("a");\n`))).toContain(`entry-without-init:${f}`);
    expect(seamChain(plant(f, `import{scrubText}from'../${SEAM}.ts';\nawait scrubText("a");\n`), f)).toStrictEqual([f, `${SEAM}.ts`]);
  });

  test("a file the transpiler cannot scan is reported, not treated as edge-free", () => {
    const f = "scripts/planted-unparseable.ts";
    expect(rules(plant(f, `import { scrubText } from "../${SEAM}.ts";\nconst = ;\nawait scrubText("a");\n`))).toContain(`unparseable:${f}`);
  });

  // ENTRY_EXEMPT reasons that name a repo path are checked against the graph.
  test("every repo path named in an exemption reason is a tracked source every seam route passes through", () => {
    const named = Object.entries(ENTRY_EXEMPT).flatMap(([f, why]) => (why.match(REASON_PATH_RE) ?? []).map((p) => [f, p] as const));
    expect(named.length).toBeGreaterThan(0);
    for (const [f, p] of named) {
      expect(repo.has(p), `${f}: ${p}`).toBe(true);
      expect(seamChain(repo, f)!.slice(1, -1), `${f}: ${p}`).toContain(p);
      expect(seamChain(repo, f, new Set([p])), `${f}: ${p}`).toBeUndefined();
    }
  });

  test.each(["scripts/build-large-read-worker.ts", "stacks/aws/alchemy.aws.run.ts"])("an exempt %s given a seam route that bypasses the paths its reason names is reported stale", (f) => {
    const files = mutate(f, (s) => `import { scrubTextForLog } from "../${f.startsWith("stacks/") ? "../" : ""}${SEAM}.ts";\n${s}`);
    expect(scan(files).filter((v) => v.file === f).map((v) => v.rule)).toContain("stale-exemption");
  });

  // hosted-grant-first is read from the TypeScript AST and the transpiler's first load.
  test.each([
    ["an import hidden from a comment stripper by `// /*` above the grant", (line: string) => `// /*\nimport "./evil.ts";\n// */\n${line}`],
    ["the grant imported with `with { type: \"text\" }`", (line: string) => line.replace(/";/, '" with { type: "text" };')],
    ["the grant imported with `assert { type: \"json\" }`", (line: string) => line.replace(/";/, '" assert { type: "json" };')],
    ["a named import on the same line before the grant", (line: string) => `import { log } from "../shared/log.ts"; ${line}`],
    ["a directive followed by a different import", (line: string) => `"use strict";\nimport "./publication-main.ts";\n${line}`],
  ])("a hosted entry with %s fails hosted-grant-first", (_form, edit) => {
    const files = mutate(GC, (s) => s.replace(GC_GRANT, (line) => edit(line)));
    expect(rules(files)).toContain(`hosted-grant-first:${GC}`);
  });

  test("the grant first after a shebang, a comment or a directive prologue passes hosted-grant-first", () => {
    expect(rules(mutate(GC, (s) => `#!/usr/bin/env node\n${s}`))).not.toContain(`hosted-grant-first:${GC}`);
    for (const before of ["/* header */\n", "\"use strict\";\n"])
      expect(rules(mutate(GC, (s) => s.replace(GC_GRANT, (line) => `${before}${line}`)))).not.toContain(`hosted-grant-first:${GC}`);
  });
  // F3: exemption reasons must cite real files that are intermediate modules every seam route passes.
  const LARGE = "scripts/build-large-read-worker.ts";
  const staleDetails = (files: Sources, f: string) => scan(files).filter((v) => v.file === f && v.rule === "stale-exemption").map((v) => v.detail);
  const withReason = <T>(f: string, reason: string, run: () => T): T => {
    const table = ENTRY_EXEMPT as Record<string, string>;
    const before = table[f]!;
    table[f] = reason;
    try {
      return run();
    } finally {
      table[f] = before;
    }
  };

  test("an exempt entry whose seam route passes the named paths but has a longer bypass is reported stale (M5b)", () => {
    // The first (shortest) chain still runs through the named paths; only the avoid check finds the bypass.
    const hops = ["a", "b", "c", "d", "e"].map((h) => `scripts/planted-bypass-${h}.ts`);
    let files = mutate(LARGE, (s) => `${s}\nimport "./planted-bypass-a.ts";\n`);
    hops.forEach((h, i) => { files = plant(h, i < hops.length - 1 ? `export * from "./planted-bypass-${"abcde"[i + 1]}.ts";\n` : `export { scrubText } from "../${SEAM}.ts";\n`, files); });
    expect(seamChain(files, LARGE)).toStrictEqual(seamChain(repo, LARGE));
    expect(seamChain(files, LARGE, new Set(["src/shared/log.ts"]))).toContain(hops[0]);
    expect(staleDetails(files, LARGE).join("\n")).toContain("not every route from the entry to the seam passes through it");
  });

  test("a reason naming the seam module or the entry itself is reported stale (intermediate only)", () => {
    const chain = seamChain(repo, LARGE)!;
    for (const named of [chain.at(-1)!, "packages/pipeline/src/lib/scrub.ts", LARGE]) {
      const details = withReason(LARGE, `bundler: reaches the seam only through ${named}, never called`, () => staleDetails(repo, LARGE));
      expect(details.join("\n"), named).toContain(`reason names ${named}, but the reason must name an intermediate module`);
    }
  });

  test("a reason naming a tracked file off the seam route is reported stale", () => {
    expect(withReason(LARGE, "bundler: reaches the seam only through src/session-ingest/accept-generations.ts", () => staleDetails(repo, LARGE)).join("\n")).toContain("must name an intermediate module");
  });

  test("a reason path written without an extension (or not tracked) is checked and reported stale", () => {
    expect("through src/shared/log, never".match(REASON_PATH_RE)).toStrictEqual(["src/shared/log"]);
    expect("ends a sentence at src/shared/log.ts.".match(REASON_PATH_RE)).toStrictEqual(["src/shared/log.ts"]);
    for (const named of ["src/shared/log", "stacks/aws/derived-schema-build", "src/shared/no-such-file.ts"]) {
      const details = withReason(LARGE, `bundler: reaches the seam only through ${named}, never called`, () => staleDetails(repo, LARGE));
      expect(details.join("\n"), named).toContain(`reason names ${named}, which is not a tracked source`);
    }
  });

  // N14: a directory import resolves to its index module.
  test("an entry reaching the seam through a directory import fails", () => {
    const f = "scripts/planted-n14.ts";
    const files = plant(f, `import { scrubText } from "./planted-n14-dir";\nconsole.log(scrubText("a"));\n`, plant("scripts/planted-n14-dir/index.ts", `export { scrubText } from "../../${SEAM}.ts";\n`));
    expect(seamChain(files, f)).toStrictEqual([f, "scripts/planted-n14-dir/index.ts", `${SEAM}.ts`]);
    expect(rules(files)).toContain(`entry-without-init:${f}`);
  });

  // F2a: extensionless specifiers follow Bun's order (.mts, .ts, .mjs, .js, then /index.*).
  test("resolveSpec resolves extensionless specifiers to .mts, .mjs and .js and index files in Bun's order", () => {
    const one = (path: string) => plant(path, "export const x = 1;\n");
    for (const ext of ["mts", "ts", "mjs", "js"]) {
      expect(resolveSpec(one(`scripts/planted-r.${ext}`), "scripts/x.ts", "./planted-r")).toBe(`scripts/planted-r.${ext}`);
      expect(resolveSpec(one(`scripts/planted-r/index.${ext}`), "scripts/x.ts", "./planted-r")).toBe(`scripts/planted-r/index.${ext}`);
    }
    const order = ["mts", "ts", "mjs", "js"];
    for (let i = 0; i < order.length; i++) {
      let files: Sources = repo;
      for (const ext of order.slice(i)) files = plant(`scripts/planted-r.${ext}`, "export const x = 1;\n", files);
      expect(resolveSpec(plant("scripts/planted-r/index.mts", "", files), "scripts/x.ts", "./planted-r")).toBe(`scripts/planted-r.${order[i]}`);
    }
  });

  // F2b: plain .js sources are scanned, except the vendored wasm-bindgen glue.
  test("a planted .js entry reaching the seam fails; the vendored glue is not scanned", () => {
    const f = "scripts/planted-js.js";
    expect(rules(plant(f, `import { scrubText } from "../${SEAM}.js";\nconsole.log(scrubText("a"));\n`))).toContain(`entry-without-init:${f}`);
    expect([...repo.keys()].filter((k) => k.endsWith(".js"))).toStrictEqual([]);
  });

  // F2c: a workspace package's "exports" is followed; a shape the scan cannot read is reported.
  test("resolveSpec follows a workspace package's exports", () => {
    expect(resolveSpec(repo, "scripts/x.ts", "@96bulls/coordination-detect")).toBe("packages/coordination-detect/src/index.ts");
    const pkg = (exports: unknown) => plant("packages/planted-pkg/src/sub.ts", "export const y = 1;\n", plant("packages/planted-pkg/src/main.ts", "export const x = 1;\n", plant("packages/planted-pkg/package.json", JSON.stringify({ name: "@planted/pkg", exports }))));
    expect(resolveSpec(pkg("./src/main.ts"), "scripts/x.ts", "@planted/pkg")).toBe("packages/planted-pkg/src/main.ts");
    const map = pkg({ ".": { types: "./x.d.ts", bun: "./src/main.ts" }, "./sub": "./src/sub.ts" });
    expect(resolveSpec(map, "scripts/x.ts", "@planted/pkg")).toBe("packages/planted-pkg/src/main.ts");
    expect(resolveSpec(map, "scripts/x.ts", "@planted/pkg/sub")).toBe("packages/planted-pkg/src/sub.ts");
    expect(resolveSpec(map, "scripts/x.ts", "@planted/pkg/src/sub")).toBe("packages/planted-pkg/src/sub.ts");
    expect(resolveSpec(pkg({ ".": { import: "./src/main.ts", default: "./src/sub.ts" } }), "scripts/x.ts", "@planted/pkg")).toBe("packages/planted-pkg/src/main.ts");
  });

  test.each([
    ["an array", ["./src/index.ts"]],
    ["a pattern subpath", { "./*": "./src/*.ts" }],
    ["a node condition first", { ".": { node: "./src/index.ts", import: "./src/index.ts" } }],
    ["a nested condition", { ".": { import: { bun: "./src/index.ts" } } }],
    ["an untracked target", { ".": "./dist/index.js" }],
  ])("an entry importing a workspace package whose exports is %s is reported, not silently unfollowed", (_shape, exports) => {
    const f = "scripts/planted-exports.ts";
    let files = plant("packages/planted-pkg/package.json", JSON.stringify({ name: "@planted/pkg", exports }));
    files = plant("packages/planted-pkg/src/index.ts", `export { scrubText } from "../../pipeline/src/lib/scrub-engine/index.ts";\n`, files);
    files = plant(f, `import { scrubText } from "@planted/pkg";\nconsole.log(scrubText("a"));\n`, files);
    expect(rules(files)).toContain(`unresolved-workspace-import:${f}`);
  });
});
