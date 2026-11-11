#!/usr/bin/env bun
/**
 * Build receipt for the vendored relayscrubber package (docs/scrub-engine.md,
 * "Cutover: pinned relayscrubber 0.5.0-alpha.6").
 *
 * Reads packages/pipeline/scrub-bindgen/ as data (nothing in it is executed),
 * recomputes the sha256 of every vendored file, and compares them to
 * packages/pipeline/scrub-bindgen-pin.json AND the release's own generated/artifact.json:
 * pin digests (wasm, glue, loader), artifact.json file lists, no unlisted files,
 * manifest verbatim, package version = crateVersion, scrub_profile = scrubProfile,
 * pin release.sha256sums["artifact.json"] = the vendored artifact.json digest.
 *
 * Usage: bun scripts/scrub-bindgen-receipt.ts [--out <path>] [--tarball <path>]
 *   --out      receipt path (default: scrub-bindgen-receipt.json)
 *   --tarball  the release tarball named by pin.release. When given, the receipt's `release`
 *              block is the provenance verification (packages/pipeline/src/lib/
 *              scrub-bindgen-provenance.ts): tarball sha256 = pin, package/package.json
 *              name+version = pin, every vendored file byte-equal to its package/ entry,
 *              no extra entries. The tarball is read in memory as data, never executed.
 *              Without it (CI: no network, no tarball) the block is
 *              `{ ...pin.release, verified: false, reason: "tarball not supplied" }` and
 *              does not by itself fail the receipt.
 * Writes the receipt (ok:false with the mismatches when anything differs, including a
 * supplied tarball that fails verification: release.verified:false with its mismatches),
 * prints it, and exits 1 on any mismatch.
 */
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { isDeepStrictEqual } from "node:util";
import { verifyReleaseTarball } from "../packages/pipeline/src/lib/scrub-bindgen-provenance";

const ROOT = resolve(import.meta.dir, "..");
const PIN_FILE = join(ROOT, "packages/pipeline/scrub-bindgen-pin.json");
const DIR = join(ROOT, "packages/pipeline/scrub-bindgen");
const args = process.argv.slice(2);
const at = args.indexOf("--out");
const out = resolve(at >= 0 && args[at + 1] ? args[at + 1]! : "scrub-bindgen-receipt.json");
const tat = args.indexOf("--tarball");
if (tat >= 0 && !args[tat + 1]) {
  console.error("--tarball needs a path");
  process.exit(2);
}
const tarballPath = tat >= 0 ? resolve(args[tat + 1]!) : undefined;
if (tarballPath && !isFile(tarballPath)) {
  console.error(`--tarball ${tarballPath} is not a readable file`);
  process.exit(2);
}

function isFile(path: string): boolean {
  try {
    return statSync(path).isFile();
  } catch {
    return false;
  }
}
const sha = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");
const mismatches: string[] = [];
const check = (ok: boolean, what: string) => { if (!ok) mismatches.push(what); };

function walk(dir: string): string[] {
  return readdirSync(dir).flatMap((n) => (statSync(join(dir, n)).isDirectory() ? walk(join(dir, n)) : [relative(DIR, join(dir, n))])).sort();
}

const pinBytes = readFileSync(PIN_FILE);
const pin = JSON.parse(pinBytes.toString("utf8"));
const artifactBytes = readFileSync(join(DIR, "generated/artifact.json"));
const artifact = JSON.parse(artifactBytes.toString("utf8"));
const present = walk(DIR);
const digests: Record<string, string> = Object.fromEntries(present.map((rel) => [rel, sha(readFileSync(join(DIR, rel)))]));

// Pin vs vendored bytes.
for (const name of ["wasm", "glue", "loader"] as const) {
  const rel = pin.paths?.[name];
  check(typeof rel === "string" && digests[rel] !== undefined, `pin path ${name} (${rel}) is not vendored`);
  check(digests[rel] === pin[`${name}_sha256`], `${name}: vendored ${digests[rel]} != pin ${pin[`${name}_sha256`]}`);
}
// artifact.json vs vendored bytes, and no unlisted files.
const listed: Record<string, string> = {
  ...Object.fromEntries(Object.entries(artifact.files ?? {}).map(([f, d]) => [`generated/${f}`, d as string])),
  ...Object.fromEntries(Object.entries(artifact.adapterFiles ?? {}).map(([f, d]) => [f, d as string])),
};
for (const [rel, d] of Object.entries(listed)) check(digests[rel] === d, `${rel}: vendored ${digests[rel] ?? "missing"} != artifact.json ${d}`);
const allowed = new Set([...Object.keys(listed), "generated/artifact.json", ".gitattributes"]);
for (const rel of present) check(allowed.has(rel), `unlisted vendored file ${rel}`);
// Pin vs artifact.json identity.
check(isDeepStrictEqual(pin.manifest, artifact.manifest), "pin manifest != artifact.json manifest");
check(pin.package_version === artifact.manifest?.crateVersion, `package_version ${pin.package_version} != crateVersion ${artifact.manifest?.crateVersion}`);
check(pin.scrub_profile === artifact.manifest?.scrubProfile, `scrub_profile ${pin.scrub_profile} != scrubProfile ${artifact.manifest?.scrubProfile}`);
check(
  typeof pin.release === "object" && pin.release !== null && pin.release.sha256sums?.["artifact.json"] === sha(artifactBytes),
  `pin release.sha256sums["artifact.json"] ${pin.release?.sha256sums?.["artifact.json"]} != vendored artifact.json ${sha(artifactBytes)}`,
);
check(!("placeholder" in pin) && !Object.keys(pin).some((k) => /placeholder/i.test(k)), "pin still carries a placeholder marker");

// Release provenance (optional: needs the tarball, which CI does not have).
let release: Record<string, unknown> = { ...pin.release, verified: false, reason: "tarball not supplied" };
if (tarballPath) {
  const result = verifyReleaseTarball({ tarball: readFileSync(tarballPath), pin, vendoredDir: DIR });
  for (const m of result.mismatches) mismatches.push(`release: ${m}`);
  release = {
    ...pin.release,
    verified: result.ok,
    ...(result.ok ? {} : { reason: "tarball verification failed", mismatches: result.mismatches }),
    tarballSuppliedSha256: result.tarballSha256,
    entries: result.entries,
  };
}

const git = (...a: string[]) => spawnSync("git", a, { cwd: ROOT, encoding: "utf8" }).stdout.trim() || null;
const receipt = {
  schema: 1,
  kind: "scrub-bindgen-receipt",
  ok: mismatches.length === 0,
  mismatches,
  generatedAt: new Date().toISOString(),
  gitHead: git("rev-parse", "HEAD"),
  gitTreeClean: git("status", "--porcelain", "--", "packages/pipeline/scrub-bindgen", "packages/pipeline/scrub-bindgen-pin.json") === null,
  package: pin.package,
  packageVersion: pin.package_version,
  pinFile: { path: relative(ROOT, PIN_FILE), sha256: sha(pinBytes) },
  release,
  pinDigests: { wasm: pin.wasm_sha256, glue: pin.glue_sha256, loader: pin.loader_sha256 },
  scrubProfile: pin.scrub_profile,
  crateManifest: artifact.manifest,
  toolchain: { rust: artifact.toolchain, wasmBindgen: artifact.wasmBindgen },
  artifactJson: { path: "packages/pipeline/scrub-bindgen/generated/artifact.json", sha256: sha(artifactBytes) },
  vendored: digests,
};
writeFileSync(out, JSON.stringify(receipt, null, 2) + "\n");
console.log(JSON.stringify(receipt, null, 2));
if (!receipt.ok) process.exit(1);
