#!/usr/bin/env bun
/**
 * Build receipt for the vendored relayscrubber package (docs/scrub-engine.md,
 * "Cutover: pinned relayscrubber 0.5.0-alpha.4").
 *
 * Reads packages/pipeline/scrub-bindgen/ as data (nothing in it is executed),
 * recomputes the sha256 of every vendored file, and compares them to
 * packages/pipeline/scrub-bindgen-pin.json AND the release's own generated/artifact.json:
 * pin digests (wasm, glue, loader), artifact.json file lists, no unlisted files,
 * manifest verbatim, package version = crateVersion, scrub_profile = scrubProfile.
 *
 * Usage: bun scripts/scrub-bindgen-receipt.ts [--out <path>]   (default: scrub-bindgen-receipt.json)
 * Writes the receipt (ok:false with the mismatches when anything differs), prints it,
 * and exits 1 on any mismatch.
 */
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { isDeepStrictEqual } from "node:util";

const ROOT = resolve(import.meta.dir, "..");
const PIN_FILE = join(ROOT, "packages/pipeline/scrub-bindgen-pin.json");
const DIR = join(ROOT, "packages/pipeline/scrub-bindgen");
const args = process.argv.slice(2);
const at = args.indexOf("--out");
const out = resolve(at >= 0 && args[at + 1] ? args[at + 1]! : "scrub-bindgen-receipt.json");

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
check(!("placeholder" in pin) && !Object.keys(pin).some((k) => /placeholder/i.test(k)), "pin still carries a placeholder marker");

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
