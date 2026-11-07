#!/usr/bin/env bun
/** Prepare a source-bound Lambda ZIP locally. No registry, AWS, credential
 * lookup or publication. Private providers are not discovered or bundled. */
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { mkdir, readFile, writeFile, rm, lstat } from "node:fs/promises";
import { resolve, relative, join } from "node:path";
import { fileURLToPath } from "node:url";
import { packagedDerivedSchemaSource } from "../stacks/aws/derived-schema-build.ts";
import { derivedSchemaAssets } from "../src/session-pipeline/derived-schema.ts";

const root = fileURLToPath(new URL("..", import.meta.url));
const args = process.argv.slice(2);
const allowed = new Set(["--service", "--architecture", "--source-sha", "--out"]);
const seen = new Set<string>();
for (let i = 0; i < args.length; i++) {
  const arg = args[i]!;
  if (seen.has(arg)) throw new Error("Duplicate Lambda build option");
  seen.add(arg);
  if (arg === "--allow-dirty-test") continue;
  if (!allowed.has(arg) || !args[i + 1] || args[i + 1]!.startsWith("--")) throw new Error("Unknown or incomplete Lambda build option");
  i++;
}
function option(name: string): string | undefined { const i = args.indexOf(name); return i < 0 ? undefined : args[i + 1]; }
const service = option("--service");
const architecture = option("--architecture") ?? "arm64";
const sourceSha = option("--source-sha");
const allowDirtyTest = args.includes("--allow-dirty-test");
if (!["pipeline", "read-api"].includes(service ?? "") || !["arm64", "x86_64"].includes(architecture) || !/^[a-f0-9]{40}$/.test(sourceSha ?? "")) throw new Error("Require --service pipeline|read-api --source-sha FULL_SHA [--architecture arm64|x86_64] [--allow-dirty-test]");
if (Bun.version !== "1.4.2") throw new Error("Lambda builder requires Bun 1.4.2");
const git = (...argv: string[]) => execFileSync("git", argv, { cwd: root, encoding: "utf8" }).trim();
const status = () => git("status", "--porcelain", "--untracked-files=normal");
const head = git("rev-parse", "HEAD");
const canonicalRemote = /^(git@github\.com:|https:\/\/github\.com\/)AgentWorkforce\/(sessions-pipeline|96BullSessions)(\.git)?$/.test(git("remote", "get-url", "origin"));
if (!canonicalRemote && !allowDirtyTest) throw new Error("Canonical Lambda release builder requires the original repository remote");
if (sourceSha !== head || !allowDirtyTest && status()) throw new Error("Lambda source must be the exact clean checked-out commit");
const before = status();
const output = resolve(root, option("--out") ?? `dist/lambda/${service}-${sourceSha}`);
const relativeOutput = relative(join(root, "dist"), output);
if (relativeOutput.startsWith("..") || relativeOutput === "" || relativeOutput.startsWith("/")) throw new Error("Lambda output must be a dedicated directory below dist/");
const packageDir = join(output, "package");
let checked = root;
for (const part of relative(root, packageDir).split("/")) {
  checked = join(checked, part);
  try { if ((await lstat(checked)).isSymbolicLink()) throw new Error("Lambda output must not traverse a symlink"); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
}
await mkdir(output, { recursive: true });
await rm(packageDir, { recursive: true, force: true });
await mkdir(join(packageDir, "assets"), { recursive: true });
const digest = (bytes: string | Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const assetPaths = new Set<string>();
const embedded = await packagedDerivedSchemaSource(path => assetPaths.add(path));
const schema = await derivedSchemaAssets();
const assets = [];
for (const path of [...assetPaths].sort()) {
  const rel = relative(root, path);
  if (rel.startsWith("..") || rel.startsWith("/")) throw new Error("Schema asset escaped canonical source");
  const bytes = await readFile(path);
  const destination = join(packageDir, "assets", rel);
  await mkdir(resolve(destination, ".."), { recursive: true });
  await writeFile(destination, bytes);
  assets.push({ path: rel, sha256: digest(bytes), bytes: bytes.length });
}
const entry = join(root, `src/lambda-artifact/${service === "pipeline" ? "pipeline" : "read-api"}-entry.ts`);
const built = await Bun.build({
  entrypoints: [entry], target: "node", format: "esm", minify: false,
  external: ["pg-native"],
  plugins: [{ name: "embedded-derived-schema", setup(build) {
    build.onLoad({ filter: /\/session-pipeline\/derived-schema\.ts$/ }, () => ({ contents: embedded, loader: "ts" }));
  } }],
});
if (!built.success || built.outputs.length !== 1) throw new Error("Lambda bundle failed");
const code = new Uint8Array(await built.outputs[0]!.arrayBuffer());
await writeFile(join(packageDir, "index.mjs"), code);
for (const path of ["LICENSE", "NOTICE"]) await writeFile(join(packageDir, path), await readFile(join(root, path)));
const manifest = {
  format: "agent-relay-lambda-artifact-v1", repository: canonicalRemote ? "AgentWorkforce/sessions-pipeline" : "unverified-test-source", sourceSha,
  sourceStatus: before ? "dirty-test-only" : allowDirtyTest ? "test-only" : "clean", service, runtime: "nodejs24.x", architecture,
  handler: "index.handler", builder: { bun: Bun.version, platform: process.platform, arch: process.arch },
  dependencyLock: { path: "bun.lock", sha256: digest(await readFile(join(root, "bun.lock"))) },
  entrypoint: relative(root, entry), bundle: { path: "index.mjs", sha256: digest(code), bytes: code.length },
  provider: { kind: "legacy-clickhouse-service", implementationSource: sourceSha, externalProviderBundled: false },
  compositionAbi: "agent-relay-lambda-composition-v1",
  schema: { generation: schema.expected.generation, names: schema.expected.names, embedded: true, assets },
};
if (git("rev-parse", "HEAD") !== head || status() !== before || await packagedDerivedSchemaSource() !== embedded) throw new Error("Lambda source changed during build");
await writeFile(join(packageDir, "artifact-manifest.json"), JSON.stringify(manifest, null, 2) + "\n");
// Fixed timestamps, modes and ordering make a repeat build comparable. The
// package hash is outside the ZIP to avoid a self-referential manifest.
const zip = join(output, `${service}.zip`);
execFileSync("python3", ["-c", `import pathlib,sys,zipfile
root=pathlib.Path(sys.argv[1])
with zipfile.ZipFile(sys.argv[2], 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
 for path in sorted(root.rglob('*')):
  if path.is_file():
   info=zipfile.ZipInfo(path.relative_to(root).as_posix(), (1980,1,1,0,0,0))
   info.compress_type=zipfile.ZIP_DEFLATED
   info.external_attr=0o100644 << 16
   archive.writestr(info,path.read_bytes(),compress_type=zipfile.ZIP_DEFLATED,compresslevel=9)
`, packageDir, zip], { stdio: "pipe" });
const archive = await readFile(zip);
const receipt = { ...manifest, archive: { file: `${service}.zip`, sha256: digest(archive), sourceCodeHash: createHash("sha256").update(archive).digest("base64"), bytes: archive.length }, publication: "none", runtimeProof: "required-separately" };
await writeFile(join(output, "receipt.json"), JSON.stringify(receipt, null, 2) + "\n");
console.log(JSON.stringify({ output, sourceSha, sourceStatus: manifest.sourceStatus, service, archiveSha256: receipt.archive.sha256, runtimeProof: receipt.runtimeProof }));
