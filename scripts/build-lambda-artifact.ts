#!/usr/bin/env bun
/**
 * Build the session-pipeline Lambda ZIP locally, the way Alchemy's AWS Lambda
 * provider bundles it at deploy time. No AWS, no registry, no credential lookup,
 * no network, no publication.
 *
 *   bun scripts/build-lambda-artifact.ts --source-sha "$(git rev-parse HEAD)" [--out dist/lambda/session-pipeline]
 *
 * Fidelity: this does not re-state Alchemy's rolldown options; it calls the
 * locked Alchemy bundler itself (`makeFunctionBundler` from
 * @alchemy.run/aws/Lambda/FunctionBundle and `Bundle.build` from
 * alchemy/Bundle/Bundle, the same modules `alchemy deploy` runs under Bun) with
 * the props from stacks/aws/session-pipeline.ts. The only piece supplied here is
 * the output directory that `Bundle.build` would otherwise derive from the
 * resource context (`<.alchemy>/bundles/<stack>-<stage>-<fqn>`): a directory
 * three levels below the repository root is used, so the source map's relative
 * `sources` (`../../../src/...`) come out the same. The ZIP is Alchemy's own
 * `zipCode` (fflate, sorted entries, fixed 1980-01-01 mtime, mode 0644).
 *
 * Output (in --out): index.mjs and index.js.map (the ZIP's two entries, as
 * extracted from it), session-pipeline.zip, and build.json (sizes and sha256).
 */
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { mkdir, readFile, rm, writeFile, lstat } from "node:fs/promises";
import { join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import * as BunServices from "@effect/platform-bun/BunServices";
import * as Bundle from "alchemy/Bundle/Bundle";
import { unzipFiles } from "alchemy/Util/zip";
import * as Effect from "effect/Effect";
import { makeFunctionBundler } from "@alchemy.run/aws/Lambda/FunctionBundle";
import { sessionPipelineFunctionProps } from "../stacks/aws/session-pipeline.ts";

const root = fileURLToPath(new URL("..", import.meta.url));
const args = process.argv.slice(2);
const allowed = new Set(["--source-sha", "--out"]);
const seen = new Set<string>();
for (let i = 0; i < args.length; i++) {
  const arg = args[i]!;
  if (!allowed.has(arg) || seen.has(arg) || !args[i + 1] || args[i + 1]!.startsWith("--")) {
    throw new Error("Usage: bun scripts/build-lambda-artifact.ts --source-sha FULL_SHA [--out dist/<a>/<b>]");
  }
  seen.add(arg);
  i++;
}
const option = (name: string) => { const i = args.indexOf(name); return i < 0 ? undefined : args[i + 1]; };
const sourceSha = option("--source-sha");
if (!/^[a-f0-9]{40}$/.test(sourceSha ?? "")) throw new Error("Require --source-sha FULL_40_HEX_SHA");
if (process.cwd() !== resolve(root)) throw new Error("Run from the repository root (Alchemy resolves `main` and `tsconfig` against it)");

const git = (...argv: string[]) => execFileSync("git", argv, { cwd: root, encoding: "utf8" }).trim();
const status = () => git("status", "--porcelain", "--untracked-files=normal");
const head = git("rev-parse", "HEAD");
if (head !== sourceSha) throw new Error(`HEAD ${head} is not --source-sha ${sourceSha}`);
if (status()) throw new Error("Working tree is dirty; build only from a clean checkout");

// Same depth as `.alchemy/bundles/<name>`: the map's relative sources depend on it.
const output = resolve(root, option("--out") ?? "dist/lambda/session-pipeline");
const rel = relative(root, output);
if (rel.startsWith("..") || rel.startsWith("/") || rel.split("/").length !== 3 || rel.split("/")[0] !== "dist") {
  throw new Error("--out must be exactly three levels deep below dist/ (dist/<a>/<b>), like .alchemy/bundles/<name>");
}
let checked = root;
for (const part of rel.split("/")) {
  checked = join(checked, part);
  try { if ((await lstat(checked)).isSymbolicLink()) throw new Error("--out must not traverse a symlink"); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
}
// Rolldown writes into --out itself (cleanDir), as Alchemy writes into its bundles dir.
const bundleDir = output;
await rm(output, { recursive: true, force: true });
await mkdir(output, { recursive: true });

// The deploy-time props. Only the code-shaping fields matter to the bundler
// (main, handler, architecture, build, bundle, uploadSourceMap); the stage
// inputs below are placeholders that reach env/names only, never the code.
const deployProps = sessionPipelineFunctionProps({
  stage: "build", accountId: "000000000000", region: "us-east-1", bucket: "build", p: {},
} as never);
// Alchemy's Platform marks a resource yielded without an Effect implementation
// (`yield* AWS.Lambda.Function(id, props)`) as `isExternal: true`: the module is
// bundled as-is, with no generated bootstrap entry.
const props = {
  main: deployProps.main,
  handler: deployProps.handler,
  runtime: deployProps.runtime,
  architecture: deployProps.architecture,
  build: deployProps.build,
  isExternal: true,
} as const;

const program = Effect.gen(function* () {
  const { resolveBundlePlan, finishBundle } = yield* makeFunctionBundler;
  const plan = yield* resolveBundlePlan(props as never);
  // Bundle.build's resolveOutputOptions: inside a resource context it adds
  // `{ dir: <.alchemy>/bundles/..., cleanDir: true }` and writes; same here.
  const bundleOutput = yield* Bundle.build(
    plan.inputOptions,
    { ...plan.outputOptions, dir: bundleDir, cleanDir: true },
    plan.extra,
  );
  const result = yield* finishBundle(plan, bundleOutput);
  const { archive } = yield* result.buildArchive;
  const files = yield* unzipFiles(archive);
  return { archive, files, emitted: bundleOutput.files.map(f => f.path) };
});
const built = await Effect.runPromise(program.pipe(Effect.provide(BunServices.layer)) as Effect.Effect<{ archive: Uint8Array; files: Record<string, Uint8Array>; emitted: string[] }>);
// Rolldown's own emitted files (index.js, index.js.map) are replaced by the ZIP's entries.
await rm(join(bundleDir, "index.js"), { force: true });

const digest = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const names = Object.keys(built.files).sort();
if (names.join(",") !== "index.js.map,index.mjs") throw new Error(`Unexpected archive entries: ${names.join(",")}`);
const report: Record<string, unknown> = { sourceSha, bun: Bun.version, emitted: built.emitted, files: {} as Record<string, unknown> };
for (const name of names) {
  const bytes = built.files[name]!;
  await writeFile(join(output, name), bytes);
  (report.files as Record<string, unknown>)[name] = { bytes: bytes.length, sha256: digest(bytes) };
}
await writeFile(join(output, "session-pipeline.zip"), built.archive);
(report.files as Record<string, unknown>)["session-pipeline.zip"] = { bytes: built.archive.length, sha256: digest(built.archive) };

if (git("rev-parse", "HEAD") !== head || status()) throw new Error("Source changed during build");
await writeFile(join(output, "build.json"), JSON.stringify(report, null, 2) + "\n");
for (const [name, v] of Object.entries(report.files as Record<string, { bytes: number; sha256: string }>)) {
  console.log(`${v.sha256}  ${String(v.bytes).padStart(9)}  ${name}`);
}
console.log(`output: ${relative(root, output)}`);
