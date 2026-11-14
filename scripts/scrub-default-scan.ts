/** CI guard for the scrub-engine default flip (docs/scrub-engine.md, "Default flip").
 *
 * The pinned wasm-bindgen engine is the default scrub path. These properties keep it so:
 *
 *  1. TS capability grants: `declareScrubTsCapability(` appears only in the files listed in
 *     TS_CAPABILITY_DECLARERS (the two module-load grant modules, the hosted handlers, the test
 *     preloads) and test files; the grant module is imported only by those files, the
 *     scrub-engine directory and tests (other files may import only the read-only
 *     scrubTsAllowed / scrubTsCapabilities); and no file re-exports it.
 *  1b. Module-load grants: each HOSTED_ENTRIES file's first statement (comments skipped) is the
 *     static bare side-effect import of its HOSTED_GRANT_MODULES module ("hosted-grant-first"; a
 *     named, namespace or dynamic import, or the text inside a string or template, does not count),
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
 *
 * Scope: tracked .ts/.mts/.mjs under src, packages, scripts, e2e, benchmarks and stacks (plain .js is
 * only the vendored wasm-bindgen glue). Edges: import/export-from (`import {} from "x"` is a value
 * edge: it evaluates "x"; only `import type` and all-`type` braces are erased), bare import (with or
 * without a semicolon), literal `import("x")`, `import(\`x\`)` with no `${`, literal `require("x")`
 * and `createRequire(...)("x")`. Specifiers: relative, the tsconfig aliases `@rhc/*`
 * (packages/pipeline/src) and `@rhc-object-store`, and every workspace package name
 * (packages/<dir>/package.json "name", e.g. `@96bulls/pipeline/...`, linked into node_modules by
 * bun workspaces); other bare specifiers are third-party and not followed.
 *
 * Known limits (heuristics, not a proof; review covers these):
 *  - G4 order: an entry passes rule 3 if it calls the initialiser anywhere, even after a seam use at
 *    module evaluation or before the call; the runtime still fails closed there (wasm-bindgen with no
 *    init answers scrubTextForLog "[scrub-unavailable]" and throws on scrubText).
 *  - Computed and indirect loads are invisible: `import(variable)`, `require(variable)`, a template
 *    with `${...}`, a wrapper such as `const mod = (p) => import(join(ROOT, p))`, a stored
 *    `const req = createRequire(...)` called later, and `new Worker(url)` / `Bun.spawn` of another file.
 *    Review covers these (scripts/pg-proof/schema-rehearsal.ts loads candidate-tree modules this way
 *    and takes acceptedGenerations from the grant-free src/session-ingest/accept-generations.ts).
 *  - G9 conditional init: an initialiser only under `if (process.env.X)` (or any branch) still counts.
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
  "scripts/build-large-read-worker.ts": "bundler: builds an artifact, runs none of it (reaches the seam only through the logger of stacks/aws/derived-schema-build.ts)",
  "packages/pipeline/scripts/technical-foul-manifest.ts": "manifest generator: hashes the ruleset and vendored files, imports only version/size constants",
  "scripts/ops/claim-records-archive.ts": "uses only rawZstdFrame (zstd framing) from raw-chunks; never scrubs or seals",
};

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

/** Import edges. A statement ends at the closing quote of its specifier, not at `;`: the import/export
 * clause may not contain a quote, backtick or `;`, nor run onto a line that starts another
 * import/export, so a semicolon-less bare import is never folded into the next statement's `from`. */
const IMPORT_RE = new RegExp([
  /(?:^|\n)\s*(?<kw>import|export)\s+(?<type>type\s+)?(?<clause>(?:(?!\n\s*(?:import|export)\b)[^;"'`])*?)\s*from\s*["'](?<from>[^"']+)["']/.source,
  /(?:\bimport\s*|(?<![\w.$])require\s*|\bcreateRequire\s*\((?:[^()]|\([^()]*\))*\)\s*)\(\s*(?:["'](?<dyn>[^"']+)["']|`(?<tpl>[^`$]+)`)\s*\)/.source,
  /(?:^|\n)\s*import\s*["'](?<bare>[^"']+)["']/.source,
].join("|"), "g");
const stripComments = (src: string) => src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:"'`\\])\/\/[^\n]*/g, "$1");
/** Comment- and string-free code (string and template bodies emptied), for name tests that a string must not satisfy. */
const codeOnly = (src: string) => stripComments(src).replace(/`(?:\\[\s\S]|[^`\\])*`|"(?:\\.|[^"\\\n])*"|'(?:\\.|[^'\\\n])*'/g, '""');

type Edge = { spec: string; names: string[] | "*"; typeOnly: boolean; reexport: boolean };
function edges(src: string): Edge[] {
  const out: Edge[] = [];
  for (const m of src.matchAll(IMPORT_RE)) {
    const g = m.groups!;
    if (g.from) {
      const clause = g.clause ?? "";
      const braces = clause.match(/\{([^}]*)\}/);
      const items = braces ? braces[1]!.split(",").map((x) => x.trim()).filter(Boolean) : [];
      const names = braces && !/\*|^\s*\w+\s*,/.test(clause.replace(braces[0], ""))
        ? items.filter((x) => !x.startsWith("type ")).map((x) => x.split(/\s+as\s+/)[0]!.trim())
        : "*";
      // Erased at runtime: `import type`, or braces whose every name is `type`-marked. Empty braces
      // (`import {} from "x"`) are NOT erased: the module is still evaluated.
      const erased = !!g.type || (Array.isArray(names) && names.length === 0 && items.length > 0);
      out.push({ spec: g.from, names, typeOnly: erased, reexport: g.kw === "export" });
    } else out.push({ spec: (g.dyn ?? g.tpl ?? g.bare)!, names: "*", typeOnly: false, reexport: false });
  }
  return out;
}

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

function resolveSpec(files: Sources, from: string, spec: string): string | undefined {
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

/** First import chain from `entry` to a seam module that pulls in an engine-running name, or undefined. */
export function seamChain(files: Sources, entry: string): string[] | undefined {
  const parent = new Map<string, string | null>([[entry, null]]);
  const queue = [entry];
  while (queue.length) {
    const f = queue.shift()!;
    for (const e of edges(files.get(f) ?? "")) {
      if (e.typeOnly) continue;
      const r = resolveSpec(files, f, e.spec);
      if (!r || parent.has(r)) continue;
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
  for (const [f, src] of files) for (const e of edges(src)) {
    if (e.typeOnly) continue;
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

export type Violation = { rule: "ts-capability-declarer" | "ts-capability-importer" | "ts-capability-reexport" | "preload-import" | "hosted-grant-reach" | "hosted-grant-first" | "deploy-main" | "entry-without-init" | "stale-exemption"; file: string; detail: string };

export function scan(files: Sources): Violation[] {
  const v: Violation[] = [];
  for (const [f, src] of files) {
    // 1. grants
    if (/declareScrubTsCapability\s*\(/.test(src.replace(/\/\*[\s\S]*?\*\/|\/\/[^\n]*/g, "")) && f !== GRANT_MODULE && !TS_CAPABILITY_DECLARERS.includes(f) && !isTestPath(f))
      v.push({ rule: "ts-capability-declarer", file: f, detail: "declares a TS capability outside the hosted entries, test preloads and tests" });
    for (const e of edges(src)) {
      const r = resolveSpec(files, f, e.spec);
      if (r === GRANT_MODULE && !e.typeOnly) {
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
    // The first statement itself (comments and a shebang skipped) must be the static bare import: an
    // import named in a string or template, a named / namespace import, `void import(...)` or
    // `await import(...)` does not count.
    const first = /^\s*import\s*["']([^"']+)["']/.exec(stripComments(files.get(f) ?? "").replace(/^#![^\n]*/, ""));
    const want = HOSTED_GRANT_MODULES[kind];
    if (!first || resolveSpec(files, f, first[1]!) !== want)
      v.push({ rule: "hosted-grant-first", file: f, detail: `first statement must be the static bare side-effect import of ${want}` });
  }
  for (const [m, w] of Object.entries(WRAPPED_HANDLERS))
    if (!(w in HOSTED_ENTRIES) || !edges(files.get(w) ?? "").some((e) => e.reexport && resolveSpec(files, w, e.spec) === m))
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
