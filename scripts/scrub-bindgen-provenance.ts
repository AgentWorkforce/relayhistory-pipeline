#!/usr/bin/env bun
/**
 * Release provenance check for the vendored relayscrubber package (docs/scrub-engine.md,
 * "Provenance"). Same check as `bun scripts/scrub-bindgen-receipt.ts --tarball <path>`,
 * without writing a receipt.
 *
 * Usage: bun scripts/scrub-bindgen-provenance.ts --tarball <path>
 *
 * Verifies, against packages/pipeline/scrub-bindgen-pin.json `release` and the bytes under
 * packages/pipeline/scrub-bindgen/: tarball sha256 = pin release.tarball_sha256;
 * package/package.json name + version = pin; every vendored file byte-equal to its
 * package/ entry; no extra archive entries. The tarball is gunzipped and parsed in memory
 * as data; nothing from it is extracted, executed or imported.
 *
 * Prints a JSON result (verified, mismatches, per-entry sha256) and exits 0 when verified,
 * 1 on any mismatch, 2 on bad usage (no --tarball, or the path is not a file).
 */
import { readFileSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import { verifyReleaseTarball } from "../packages/pipeline/src/lib/scrub-bindgen-provenance";

const ROOT = resolve(import.meta.dir, "..");
const args = process.argv.slice(2);
const at = args.indexOf("--tarball");
if (at < 0 || !args[at + 1]) {
  console.error("usage: bun scripts/scrub-bindgen-provenance.ts --tarball <path>");
  process.exit(2);
}
const tarballPath = resolve(args[at + 1]!);
let isFile = false;
try {
  isFile = statSync(tarballPath).isFile();
} catch {}
if (!isFile) {
  console.error(`--tarball ${tarballPath} is not a readable file`);
  process.exit(2);
}
const pin = JSON.parse(readFileSync(join(ROOT, "packages/pipeline/scrub-bindgen-pin.json"), "utf8"));
const result = verifyReleaseTarball({
  tarball: readFileSync(tarballPath),
  pin,
  vendoredDir: join(ROOT, "packages/pipeline/scrub-bindgen"),
});
console.log(
  JSON.stringify(
    {
      kind: "scrub-bindgen-provenance",
      verified: result.ok,
      tarball: tarballPath,
      release: pin.release,
      tarballSha256: result.tarballSha256,
      mismatches: result.mismatches,
      entries: result.entries,
    },
    null,
    2,
  ),
);
if (!result.ok) process.exit(1);
