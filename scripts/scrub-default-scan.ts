/** CI guard for the scrub-engine default flip (docs/scrub-engine.md, "Default flip").
 *
 * The pinned wasm-bindgen engine is the default scrub path. These properties keep it so:
 *
 *  1. TS capability grants: `declareScrubTsCapability(` appears only in the files listed in
 *     TS_CAPABILITY_DECLARERS (the two module-load grant modules, the hosted handlers, the test
 *     preloads) and test files; the grant module is imported only by those files, the
 *     scrub-engine directory and tests (other files may import only the read-only
 *     scrubTsAllowed / scrubTsCapabilities); and no file re-exports it.
 *  1b. Module-load grants: each HOSTED_ENTRIES file's first statement (comments, a shebang and a
 *     directive prologue skipped, read from the TypeScript AST) is the static bare side-effect import
 *     of its HOSTED_GRANT_MODULES module with no import attributes, and it is also the first module
 *     load scanImports reports ("hosted-grant-first"; a named, namespace or dynamic import, an
 *     attributed `with { type: "text" }` / `assert` import, or the text inside a string, template or
 *     comment, does not count),
 *     and no other non-test file under the scan roots (scripts/pg-proof/ included) reaches a grant
 *     module through the edges below ("hosted-grant-reach"), so no self-host main or script gains
 *     the TS engine.
 *  1c. Deploy mains: every stacks/ `main: "..."` and wrangler*.jsonc "main" is a HOSTED_ENTRIES
 *     file ("deploy-main"); WRAPPED_HANDLERS are reachable only through their hosted wrapper.
 *  2. Preload isolation: no production source (src/, packages/<name>/src) imports a test preload.
 *  3. Entry inventory: every detected executable entry (isExecutableEntry: import.meta.main, top-level
 *     await, a non-declaration top-level statement, a Worker `export default { fetch|queue|scheduled }`
 *     or a Lambda `export const|function handler`) whose static import graph reaches the scrub seam
 *     calls initScrubEngine( or runWorker( in its code (not in a comment or string), is a hosted
 *     entry or wrapped handler, or is listed in ENTRY_EXEMPT with the reason it never scrubs or seals.
 *     A repo path named in an ENTRY_EXEMPT reason must be a tracked source that every route from the
 *     entry to the seam passes through ("stale-exemption" otherwise).
 *  4. Every scanned file can be read by the transpiler ("unparseable" otherwise: fail closed).
 *
 * Scope: tracked .ts/.mts/.mjs under src, packages, scripts, e2e, benchmarks and stacks (plain .js is
 * only the vendored wasm-bindgen glue). Edges are the module loads Bun.Transpiler.scanImports reports
 * (the transpiler's own parse, after the shebang line is blanked): static imports and re-exports in any
 * spacing or position (`import{}from"x"`, `export*from"x"`, mid-line, after a comment, unicode escapes
 * decoded; `import {} from "x"` is a value edge, only `import type` and all-`type` braces are erased),
 * `import("x")` with any comments, options or trailing comma, `import(\`x\`)` with no `${`, and
 * `require("x")`; plus, from the TypeScript AST, `import.meta.require("x")` and `createRequire(...)("x")`
 * with a literal. Specifiers lose a `?query` / `#hash` suffix and resolve when relative, through the
 * tsconfig aliases `@rhc/*` (packages/pipeline/src) and `@rhc-object-store`, or through a workspace
 * package name (packages/<dir>/package.json "name", e.g. `@96bulls/pipeline/...`, linked into
 * node_modules by bun workspaces); other bare specifiers are third-party and not followed.
 *
 * Known limits (heuristics, not a proof; review covers these):
 *  - G4 order: an entry passes rule 3 if it calls the initialiser anywhere, even after a seam use at
 *    module evaluation or before the call; the runtime still fails closed there (wasm-bindgen with no
 *    init answers scrubTextForLog "[scrub-unavailable]" and throws on scrubText).
 *  - Computed and variable loads are invisible: `import(variable)`, `require(variable)`,
 *    `import.meta.require(variable)`, a template with `${...}`, a wrapper such as
 *    `const mod = (p) => import(join(ROOT, p))`, a stored `const req = createRequire(...)` called
 *    later, `require.resolve`, `module.require`, and `new Worker(url)` / `Bun.spawn` / a child
 *    process of another file.
 *    Review covers these (scripts/pg-proof/schema-rehearsal.ts loads candidate-tree modules this way
 *    and takes acceptedGenerations from the grant-free src/session-ingest/accept-generations.ts;
 *    src/hoopdb/__tests__/schema-rehearsal-r6.test.ts pins that choice).
 *  - An attributed import elsewhere (`import x from "y" with { type: "text" }`) is still counted as a
 *    code edge (conservative: it may report a chain that never evaluates "y").
 *  - G9 conditional init: an initialiser only under `if (process.env.X)` (or any branch) still counts
 *    (src/session-ingest/publication-main.ts initialises only for an enabled publish run, by design).
 *  - The ENTRY_EXEMPT path check reads only repo-relative paths with an extension under the scan
 *    roots; prose such as "the logger" is not checked.
 *  - Entry detection is line-anchored and regex-based: entries shaped differently (for example
 *    `export default app`, a handler exported through `export { handler }` of a local binding) are not
 *    detected as entries; their wiring is covered by rule 1c when they are deployed from stacks/wrangler.
 *  - Rule 1c reads wrangler*.jsonc only; packages/amp-transport-worker/wrangler.template.json (plain
 *    .json) is not checked, and its main does not reach the seam.
 *
 *   bun scripts/scrub-default-scan.ts      exit 1 and list each violation
 */
import { execFileSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

export const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
export const GRANT_MODULE = "packages/pipeline/src/lib/scrub-engine/ts-capability.ts";
export const SEAM_MODULES = ["packages/pipeline/src/lib/scrub-engine/index.ts", "packages/pipeline/src/lib/scrub.ts"] as const;
export const PRELOADS = ["scripts/test-preload/scrub-ts-capability.ts", "packages/pipeline/test/setup/scrub-ts-capability.ts"] as const;

/** The module-load grant modules: each hosted entry imports its kind's module FIRST, as a bare
 * side-effect import, so the grant exists before any other module of its graph evaluates. */
export const HOSTED_GRANT_MODULES = {
  "hosted-lambda": "packages/pipeline/src/lib/scrub-engine/hosted-lambda-grant.ts",
  "hosted-worker": "packages/pipeline/src/lib/scrub-engine/hosted-worker-grant.ts",
} as const;

/** The hosted (artifact-incapable) entries and the grant module each imports first. Only these
 * files (and tests) may reach a hosted grant module through static or literal dynamic imports.
 * The pipeline and read API Lambdas deploy thin lambda-entry.ts wrappers (stacks/aws `main`),
 * because the self-host services import their lambda.ts modules for helpers; those lambda.ts
 * files keep only the handler-entry declaration. The two lambda-artifact wrappers are the esbuild
 * entry points of the portable Lambda zip (scripts/build-lambda-artifact.ts); they evaluate other
 * modules before the Lambda module, so they import the grant first too. */
export const HOSTED_ENTRIES: Readonly<Record<string, keyof typeof HOSTED_GRANT_MODULES>> = {
  "src/session-ingest/worker.ts": "hosted-worker",
  "packages/session-read-worker/src/index.ts": "hosted-worker",
  "packages/session-read-worker/src/digest-narrative.ts": "hosted-worker",
  "src/session-pipeline/lambda-entry.ts": "hosted-lambda",
  "src/session-read-api/lambda-entry.ts": "hosted-lambda",
  "src/session-ingest/upload-admission-lambda.ts": "hosted-lambda",
  "src/session-ingest/buffered-gc-lambda.ts": "hosted-lambda",
  "src/session-ingest/rearm-sweeper-lambda.ts": "hosted-lambda",
  "src/lambda-artifact/pipeline-entry.ts": "hosted-lambda",
  "src/lambda-artifact/read-api-entry.ts": "hosted-lambda",
};

/** Handler modules deployed only through a hosted wrapper (the wrapper re-exports their handler), never
 * as a deploy `main` themselves: self-host services import them for helpers, so they carry only the
 * handler-entry declaration. */
export const WRAPPED_HANDLERS: Readonly<Record<string, string>> = {
  "src/session-pipeline/lambda.ts": "src/session-pipeline/lambda-entry.ts",
  "src/session-read-api/lambda.ts": "src/session-read-api/lambda-entry.ts",
};

/** The only files that may call declareScrubTsCapability(: the module-load grant modules, the
 * hosted handlers (an idempotent second declaration at handler entry) and the test preloads. */
export const TS_CAPABILITY_DECLARERS: readonly string[] = [
  ...Object.values(HOSTED_GRANT_MODULES),
  "src/session-ingest/worker.ts",
  "packages/session-read-worker/src/digest-narrative.ts",
  "packages/session-read-worker/src/index.ts",
  "src/session-pipeline/lambda.ts",
  "src/session-read-api/lambda.ts",
  "src/session-ingest/upload-admission-lambda.ts",
  "src/session-ingest/buffered-gc-lambda.ts",
  "src/session-ingest/rearm-sweeper-lambda.ts",
  ...PRELOADS,
  "packages/session-read-worker/test/setup-env.ts",
];

/** Executable entries that reach the seam's module graph but never scrub or seal, so they
 * need no engine. Each reason names what the entry actually uses. */
export const ENTRY_EXEMPT: Readonly<Record<string, string>> = {
  "scripts/index-prebuild-058000-github.ts": "GitHub API gate: imports contracts/constants only, prints with console",
  "scripts/ingest-admission-renewal-github.ts": "GitHub API gate: imports contracts/constants only, prints with console",
  "scripts/ingest-generation-overlap-github.ts": "GitHub API gate: imports contracts/constants only, prints with console",
  "scripts/frozen-source-custody-github.ts": "GitHub API gate: imports contracts/constants only, prints with console",
  "src/hoopdb/rds-activation.ts": "dynamic import of migrate.ts only for generation(), which hashes migration files",
  "scripts/build-canonical-publication.ts": "bundler: builds an artifact, runs none of it",
  "scripts/build-lambda-artifact.ts": "bundler: builds an artifact, runs none of it",
  "scripts/build-buffered-gc.ts": "bundler: builds an artifact, runs none of it",
  "scripts/build-large-read-worker.ts": "bundler: builds an artifact, runs none of it; reaches the seam only through stacks/aws/derived-schema-build.ts (its build plugin) and then src/shared/log.ts (scrubTextForLog, reached via the SQL-path constants of the db and migrate modules), never called",
  "stacks/aws/alchemy.aws.run.ts": "Alchemy IaC program: declares AWS resources and Lambda bundles, never scrubs or seals; reaches the seam only through stacks/aws/derived-schema-build.ts (the derivedSchemaBuildPlugin that stacks/aws/session-pipeline.ts hands the bundler) and then src/shared/log.ts (scrubTextForLog), never called",
  "packages/pipeline/scripts/technical-foul-manifest.ts": "manifest generator: hashes the ruleset and vendored files, imports only version/size constants",
  "scripts/ops/claim-records-archive.ts": "uses only rawZstdFrame (zstd framing) from raw-chunks; never scrubs or seals",
};

/** A repo path named in an ENTRY_EXEMPT reason (checked against the graph by the scan). */
const REASON_PATH_RE = /\b(?:src|packages|scripts|e2e|benchmarks|stacks)\/[\w./-]+\.(?:m?ts|mjs)\b/g;

/** Files the entry heuristic flags that are not executable entries (with why). */
export const NOT_ENTRIES: Readonly<Record<string, string>> = {
  "src/proof/aws/checks.ts": "library for src/proof/aws/cli.ts (which initialises); its top-level await lines are inside a template string shipped to a pod",
};

const SCAN_ROOTS = ["src", "packages", "scripts", "e2e", "benchmarks", "stacks"];
/** Source extensions scanned. Plain .js is not: the only tracked .js under the roots is the vendored
 * wasm-bindgen glue (packages/pipeline/scrub-bindgen), which is data, not an entry. */
const SOURCE_EXTS = ["ts", "mts", "mjs"];
const SOURCE_RE = /\.(m?ts|mjs)$/;
const isTestPath = (f: string) => /(^|\/)(__tests__|test|tests)\//.test(f) || /\.(test|spec)\.(m?ts|mjs)$/.test(f);

export type Sources = Map<string, string>;

/** Tracked and new (not ignored) .ts/.mts/.mjs sources under the scan roots, plus every wrangler*.jsonc
 * (for its `main`), relative to the repo root. */
export function trackedSources(root = ROOT): Sources {
  const out = execFileSync("git", ["ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", ...SCAN_ROOTS.flatMap((r) => SOURCE_EXTS.map((x) => `${r}/*.${x}`)), "wrangler*.jsonc", "*/wrangler*.jsonc"], { cwd: root, encoding: "utf8" });
  const files = new Map<string, string>();
  for (const f of out.split("\0")) if (f && !f.endsWith(".d.ts") && existsSync(resolve(root, f))) files.set(f, readFileSync(resolve(root, f), "utf8"));
  return files;
}

/** Import edges come from Bun's own transpiler (`Bun.Transpiler#scanImports`), the parser that runs
 * these files, so every syntactic form it evaluates is an edge whatever its spacing, comments or
 * escapes: `import{x}from"y"`, `import*as g from"y"`, `export*from"y"`, `export{}from"y"`, an import
 * after another statement on the same line, `import(/* c *\/ "y")`, `import("y", {})`, unicode escapes
 * in the specifier, `import x = require("y")`, literal `require("y")` and `` require(`y`) ``. It
 * omits what TypeScript erases (`import type`, all-`type` braces). The supplemental detector adds the
 * literal loads it does not report: `import.meta.require("y")` and `createRequire(...)("y")`.
 * Import-statement names and re-export flags are read from the TypeScript AST of the same file, only
 * when a rule needs them. A file the transpiler cannot parse is an "unparseable" violation. */
type ScanRecord = { kind: string; path: string };
const transpiler = new (globalThis as unknown as { Bun: { Transpiler: new (o: { loader: "ts" }) => { scanImports(src: string): ScanRecord[] } } }).Bun.Transpiler({ loader: "ts" });
const asts = new Map<string, ts.SourceFile>();
function ast(src: string): ts.SourceFile {
  let sf = asts.get(src);
  if (!sf) asts.set(src, (sf = ts.createSourceFile("scan.ts", src, ts.ScriptTarget.Latest, false, ts.ScriptKind.TS)));
  return sf;
}

/** Bound names and re-export flag of the value import/export-from statements of `spec` (a bare
 * import, default, namespace or `export *` is "*"). Unknown statement shapes fail closed ("*", re-export). */
function staticBindings(src: string, spec: string): { names: string[] | "*"; reexport: boolean } {
  let names: string[] | "*" = [], reexport = false, found = false;
  const add = (n: string[] | "*") => { names = names === "*" || n === "*" ? "*" : [...names, ...n]; };
  for (const st of ast(src).statements) {
    if (!(ts.isImportDeclaration(st) || ts.isExportDeclaration(st)) || !st.moduleSpecifier || !ts.isStringLiteral(st.moduleSpecifier) || st.moduleSpecifier.text !== spec) continue;
    if (ts.isImportDeclaration(st)) {
      const c = st.importClause;
      if (c?.isTypeOnly) continue;
      found = true;
      if (!c || c.name || (c.namedBindings && ts.isNamespaceImport(c.namedBindings))) add("*");
      else if (c.namedBindings) add(c.namedBindings.elements.filter((e) => !e.isTypeOnly).map((e) => (e.propertyName ?? e.name).text));
    } else {
      if (st.isTypeOnly) continue;
      found = true;
      reexport = true;
      if (!st.exportClause || ts.isNamespaceExport(st.exportClause)) add("*");
      else add(st.exportClause.elements.filter((e) => !e.isTypeOnly).map((e) => (e.propertyName ?? e.name).text));
    }
  }
  return found ? { names, reexport } : { names: "*", reexport: true };
}

/** Literal `import.meta.require("x")` and `createRequire(...)("x")` / `module.createRequire(...)("x")`. */
function supplementalLoads(src: string): string[] {
  if (!/import\.meta\.require|createRequire/.test(src)) return [];
  const out: string[] = [];
  const visit = (n: ts.Node): void => {
    if (ts.isCallExpression(n) && n.arguments[0] && (ts.isStringLiteral(n.arguments[0]) || ts.isNoSubstitutionTemplateLiteral(n.arguments[0]))) {
      const c = n.expression;
      const metaRequire = ts.isPropertyAccessExpression(c) && c.name.text === "require" && ts.isMetaProperty(c.expression) && c.expression.keywordToken === ts.SyntaxKind.ImportKeyword;
      const created = ts.isCallExpression(c) && ((ts.isIdentifier(c.expression) && c.expression.text === "createRequire") || (ts.isPropertyAccessExpression(c.expression) && c.expression.name.text === "createRequire"));
      if (metaRequire || created) out.push(n.arguments[0].text);
    }
    ts.forEachChild(n, visit);
  };
  visit(ast(src));
  return out;
}

export type EdgeKind = "import" | "dynamic" | "require";
/** One module load. `names` and `reexport` (read lazily) matter only for import statements; dynamic
 * and require loads are "*" and never a re-export. */
export type Edge = { readonly spec: string; readonly kind: EdgeKind; readonly names: string[] | "*"; readonly reexport: boolean };
function edge(spec: string, kind: EdgeKind, src: string): Edge {
  let b: { names: string[] | "*"; reexport: boolean } | undefined;
  const bindings = () => (b ??= kind === "import" ? staticBindings(src, spec) : { names: "*", reexport: false });
  return { spec, kind, get names() { return bindings().names; }, get reexport() { return bindings().reexport; } };
}

const edgeCache = new Map<string, Edge[]>();
/** The module loads of a source file, in scanImports (source) order, then the supplemental loads.
 * Throws when the transpiler cannot parse the file. */
export function edges(src: string): Edge[] {
  let out = edgeCache.get(src);
  if (out) return out;
  // scanImports rejects a shebang; blank it (length kept).
  const code = src.replace(/^#![^\n]*/, (m) => " ".repeat(m.length));
  out = [];
  for (const r of transpiler.scanImports(code)) {
    if (r.kind === "import-statement") out.push(edge(r.path, "import", src));
    else if (r.kind === "dynamic-import") out.push(edge(r.path, "dynamic", src));
    else if (r.kind === "require-call") out.push(edge(r.path, "require", src));
  }
  for (const spec of supplementalLoads(src)) out.push(edge(spec, "require", src));
  edgeCache.set(src, out);
  return out;
}
/** edges(), or [] for an unparseable file (scan() reports it as "unparseable"). */
const safeEdges = (src: string): Edge[] => { try { return edges(src); } catch { return []; } };

/** One pass over comments, strings and templates, left to right, so a `//` inside a block comment,
 * a `/*` after `//`, or either inside a string is read as what it is. Regex literals are not
 * tokenised (heuristic). */
const TOKENS = /\/\*[\s\S]*?\*\/|\/\/[^\n]*|`(?:\\[\s\S]|[^`\\])*`|"(?:\\.|[^"\\\n])*"|'(?:\\.|[^'\\\n])*'/g;
const dropComment = (m: string) => (m[1] === "*" ? m.replace(/[^\n]/g, "") : "");
const stripComments = (src: string) => src.replace(TOKENS, (m) => (m[0] === "/" ? dropComment(m) : m));
/** Comment- and string-free code (string and template bodies emptied), for name tests that a string must not satisfy. */
const codeOnly = (src: string) => src.replace(TOKENS, (m) => (m[0] === "/" ? dropComment(m) : '""'));

/** Workspace package name -> repo-relative directory (root package.json "workspaces": packages/*). */
export const WORKSPACE_PACKAGES: ReadonlyMap<string, string> = (() => {
  const out = new Map<string, string>();
  for (const d of existsSync(resolve(ROOT, "packages")) ? readdirSync(resolve(ROOT, "packages")) : []) {
    const pj = resolve(ROOT, "packages", d, "package.json");
    if (!existsSync(pj)) continue;
    const name = (JSON.parse(readFileSync(pj, "utf8")) as { name?: unknown }).name;
    if (typeof name === "string" && name) out.set(name, `packages/${d}`);
  }
  return out;
})();

/** Repo-relative tracked file a specifier loads, or undefined (third-party / unknown). A `?query` or
 * `#hash` suffix is dropped (Bun loads the same module). */
export function resolveSpec(files: Sources, from: string, specifier: string): string | undefined {
  const spec = specifier.replace(/(?<=.)[?#][\s\S]*$/, "");
  let base: string | undefined;
  if (spec.startsWith("@rhc/")) base = `packages/pipeline/src/${spec.slice(5)}`;
  else if (spec === "@rhc-object-store") base = "packages/object-store/src/index.ts";
  else if (spec.startsWith(".")) base = resolve("/", dirname(from), spec).slice(1);
  else for (const [name, dir] of WORKSPACE_PACKAGES) if (spec === name || spec.startsWith(`${name}/`)) { base = `${dir}${spec.slice(name.length)}`; break; }
  if (base === undefined) return undefined;
  for (const c of [base, base.replace(/\.js$/, ".ts"), base.replace(/\.mjs$/, ".mts"), `${base}.ts`, `${base}/index.ts`]) if (files.has(c)) return c;
}

/** Names whose import alone never runs the engine (init, readiness, error classes, test reset, the size constant). */
const INERT = /^(initScrubEngine|scrubEngineReady|resetScrubEngineForTests|ScrubEngine\w*Error|ScrubEngineUnavailable|MAX_SCRUB_CHARS)$/;

/** First import chain from `entry` to a seam module that pulls in an engine-running name, or undefined.
 * Modules in `avoid` are not entered (to test whether every route passes through them). */
export function seamChain(files: Sources, entry: string, avoid: ReadonlySet<string> = new Set()): string[] | undefined {
  const parent = new Map<string, string | null>([[entry, null]]);
  const queue = [entry];
  while (queue.length) {
    const f = queue.shift()!;
    for (const e of safeEdges(files.get(f) ?? "")) {
      const r = resolveSpec(files, f, e.spec);
      if (!r || parent.has(r) || avoid.has(r)) continue;
      if ((SEAM_MODULES as readonly string[]).includes(r)) {
        if (e.names !== "*" && e.names.every((n) => INERT.test(n))) continue;
        const chain = [r, f]; let c = parent.get(f);
        while (c) { chain.push(c); c = parent.get(c); }
        return chain.reverse();
      }
      parent.set(r, f); queue.push(r);
    }
  }
}

/** A runtime handler entry: a Cloudflare Worker module (`export default { fetch | queue | scheduled ... }`)
 * or an AWS Lambda module (`export const handler` / `export [async] function handler`). The runtime
 * calls it, so it needs the hosted grant (HOSTED_ENTRIES), an initialiser, or an exemption. */
export function isHandlerEntry(path: string, src: string): boolean {
  if (isTestPath(path) || !SOURCE_RE.test(path) || path in NOT_ENTRIES) return false;
  const code = codeOnly(src);
  return (/^export\s+default\s*\{/m.test(code) && /\b(fetch|queue|scheduled)\s*[(:,}]/.test(code.slice(code.search(/^export\s+default\s*\{/m))))
    || /^export\s+(?:(?:const|let|var)\s+handler\b|(?:async\s+)?function\s*\*?\s*handler\b)/m.test(code);
}

/** An executable entry: `import.meta.main`, top-level await (including `const x = await` and
 * `export const x = await`), any other top-level statement that is not a declaration (a call,
 * if/for/try block, process.exit, ...), or a runtime handler entry (isHandlerEntry). */
export function isExecutableEntry(path: string, src: string): boolean {
  if (isTestPath(path) || !SOURCE_RE.test(path) || path in NOT_ENTRIES) return false;
  if (/import\.meta\.main/.test(src)) return true;
  const code = src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/`(?:\\[\s\S]|[^`\\])*`/g, "``");
  return /^(?:export\s+)?(?:(?:const|let|var)\s[^\n=]*=\s*)?await\s/m.test(code)
    || /^(?!(?:import|export|function|async\s+function|class|abstract\s+class|type|interface|const|let|var|declare|enum|namespace|return|else|case|default|break|continue)\b)[A-Za-z_$(]/m.test(code)
    || isHandlerEntry(path, src);
}

/** Files whose static or literal dynamic import graph reaches a hosted grant module. */
export function hostedGrantReach(files: Sources): Map<string, string[]> {
  const importers = new Map<string, string[]>();
  for (const [f, src] of files) for (const e of SOURCE_RE.test(f) ? safeEdges(src) : []) {
    const r = resolveSpec(files, f, e.spec);
    if (r) importers.set(r, [...(importers.get(r) ?? []), f]);
  }
  const chain = new Map<string, string[]>();
  const queue: string[] = [];
  for (const g of Object.values(HOSTED_GRANT_MODULES)) { chain.set(g, [g]); queue.push(g); }
  while (queue.length) {
    const t = queue.shift()!;
    for (const f of importers.get(t) ?? []) if (!chain.has(f)) { chain.set(f, [f, ...chain.get(t)!]); queue.push(f); }
  }
  return chain;
}

/** Why a hosted entry's grant import is not first, or undefined. Its first statement (comments, a
 * shebang and a directive prologue skipped, per the TypeScript AST) must be the static bare import of
 * `want` with no import attributes (`with { type: "text" }` / `assert` would load it as data and never
 * run the grant), and the first module load scanImports reports must be that same import: a named,
 * namespace or dynamic import, or import text inside a string, template or comment, does not count. */
function hostedGrantFirstProblem(files: Sources, f: string, want: string): string | undefined {
  const src = files.get(f) ?? "";
  const statements = ast(src).statements;
  let i = 0;
  while (i < statements.length && ts.isExpressionStatement(statements[i]!) && ts.isStringLiteral((statements[i] as ts.ExpressionStatement).expression)) i++;
  const st = statements[i];
  const need = `first statement must be the static bare side-effect import of ${want}`;
  if (!st || !ts.isImportDeclaration(st) || st.importClause || !ts.isStringLiteral(st.moduleSpecifier) || resolveSpec(files, f, st.moduleSpecifier.text) !== want) return need;
  if (st.attributes) return `${need}, without import attributes (with / assert)`;
  let first: Edge | undefined;
  try {
    first = edges(src)[0];
  } catch {
    return `${need} (the transpiler cannot scan the file)`;
  }
  if (!first || first.kind !== "import" || resolveSpec(files, f, first.spec) !== want) return `${need} (the first module load is ${first ? first.spec : "none"})`;
}

export type Violation = { rule: "ts-capability-declarer" | "ts-capability-importer" | "ts-capability-reexport" | "preload-import" | "hosted-grant-reach" | "hosted-grant-first" | "deploy-main" | "entry-without-init" | "stale-exemption" | "unparseable"; file: string; detail: string };

export function scan(files: Sources): Violation[] {
  const v: Violation[] = [];
  for (const [f, src] of files) {
    if (!SOURCE_RE.test(f)) continue;
    let fileEdges: Edge[];
    try {
      fileEdges = edges(src);
    } catch (error) {
      // Fail closed: a file whose loads cannot be read is not silently edge-free.
      v.push({ rule: "unparseable", file: f, detail: `the transpiler cannot scan its imports (${String(error).slice(0, 200)})` });
      continue;
    }
    // 1. grants
    if (/declareScrubTsCapability\s*\(/.test(src.replace(/\/\*[\s\S]*?\*\/|\/\/[^\n]*/g, "")) && f !== GRANT_MODULE && !TS_CAPABILITY_DECLARERS.includes(f) && !isTestPath(f))
      v.push({ rule: "ts-capability-declarer", file: f, detail: "declares a TS capability outside the hosted entries, test preloads and tests" });
    for (const e of fileEdges) {
      const r = resolveSpec(files, f, e.spec);
      if (r === GRANT_MODULE) {
        if (e.reexport) v.push({ rule: "ts-capability-reexport", file: f, detail: `re-exports ${GRANT_MODULE}` });
        const allowed = TS_CAPABILITY_DECLARERS.includes(f) || (SEAM_MODULES as readonly string[]).includes(f) || f.startsWith("packages/pipeline/src/lib/scrub-engine/") || isTestPath(f);
        const onlyReads = e.names !== "*" && e.names.every((n) => n === "scrubTsAllowed" || n === "scrubTsCapabilities");
        if (!allowed && !onlyReads) v.push({ rule: "ts-capability-importer", file: f, detail: `imports ${e.names === "*" ? "*" : e.names.join(", ")} from the grant module` });
      }
      // 2. preloads stay out of production sources
      if (r && (PRELOADS as readonly string[]).includes(r) && !isTestPath(f) && (f.startsWith("src/") || /^packages\/[^/]+\/src\//.test(f)))
        v.push({ rule: "preload-import", file: f, detail: `imports test preload ${r}` });
    }
  }
  // 1b. module-load grants: only the hosted entries reach a grant module, and each imports its own first
  for (const [f, chain] of hostedGrantReach(files)) {
    if (f in HOSTED_ENTRIES || (Object.values(HOSTED_GRANT_MODULES) as string[]).includes(f) || isTestPath(f)) continue;
    v.push({ rule: "hosted-grant-reach", file: f, detail: `reaches a hosted module-load TS grant (${chain.join(" -> ")}); only HOSTED_ENTRIES may` });
  }
  for (const [f, kind] of Object.entries(HOSTED_ENTRIES)) {
    const problem = hostedGrantFirstProblem(files, f, HOSTED_GRANT_MODULES[kind]);
    if (problem) v.push({ rule: "hosted-grant-first", file: f, detail: problem });
  }
  for (const [m, w] of Object.entries(WRAPPED_HANDLERS))
    if (!(w in HOSTED_ENTRIES) || !safeEdges(files.get(w) ?? "").some((e) => e.reexport && resolveSpec(files, w, e.spec) === m))
      v.push({ rule: "hosted-grant-first", file: w, detail: `must be a hosted entry that re-exports the handler of ${m}` });
  // 1c. every deploy main (stacks `main: "..."`, wrangler "main") is a hosted entry
  for (const [f, src] of files) {
    const wrangler = /(^|\/)wrangler[^/]*\.jsonc$/.test(f);
    if (!wrangler && !(f.startsWith("stacks/") && !isTestPath(f))) continue;
    for (const m of stripComments(src).matchAll(wrangler ? /"main"\s*:\s*"([^"]+)"/g : /\bmain\s*:\s*["']([^"']+)["']/g)) {
      const target = wrangler ? resolve("/", dirname(f), m[1]!).slice(1) : m[1]!;
      if (!(target in HOSTED_ENTRIES)) v.push({ rule: "deploy-main", file: f, detail: `deploys ${target}, which is not in HOSTED_ENTRIES (no module-load TS grant)` });
    }
  }
  // 3. entries
  for (const [f, src] of files) {
    if (!isExecutableEntry(f, src)) continue;
    const chain = seamChain(files, f);
    const exempt = ENTRY_EXEMPT[f];
    const inits = /\b(initScrubEngine|runWorker)\s*\(/.test(codeOnly(src));
    if (chain && !inits && !exempt && !(f in HOSTED_ENTRIES) && !(f in WRAPPED_HANDLERS)) v.push({ rule: "entry-without-init", file: f, detail: `reaches the seam (${chain.join(" -> ")}) without initScrubEngine(, runWorker( or a hosted module-load grant` });
    if (exempt && !chain) v.push({ rule: "stale-exemption", file: f, detail: "exempt but no longer reaches the seam; drop the exemption" });
    // A repo path named in an exemption reason is a claim about the graph: the entry reaches the seam
    // only through it, so every route from the entry to the seam must pass through it.
    if (exempt && chain) for (const named of exempt.match(REASON_PATH_RE) ?? []) {
      if (!files.has(named)) v.push({ rule: "stale-exemption", file: f, detail: `reason names ${named}, which is not a tracked source` });
      else if (!chain.includes(named) || seamChain(files, f, new Set([named]))) v.push({ rule: "stale-exemption", file: f, detail: `reason names ${named}, but not every route from the entry to the seam passes through it (${(seamChain(files, f, new Set([named])) ?? chain).join(" -> ")})` });
    }
  }
  for (const f of Object.keys(ENTRY_EXEMPT)) if (!files.has(f) || !isExecutableEntry(f, files.get(f)!)) v.push({ rule: "stale-exemption", file: f, detail: "exempt file is not an executable entry" });
  return v;
}

if (import.meta.main) {
  const v = scan(trackedSources());
  for (const x of v) console.error(`${x.rule}: ${x.file}: ${x.detail}`);
  console.log(v.length ? `scrub-default-scan: ${v.length} violation(s)` : "scrub-default-scan: ok");
  process.exit(v.length ? 1 : 0);
}
