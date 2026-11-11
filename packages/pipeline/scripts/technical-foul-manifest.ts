// Writes packages/pipeline/technical-foul.json and the separate cutover entry
// packages/pipeline/technical-foul-cutover.json, or with --check verifies both.
// technical-foul.json is immutable TS history: the cutover never relabels it.
// Run from packages/pipeline: `bun run technical-foul:manifest` / `technical-foul:check`.
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import {
  computeCutoverManifest,
  computeManifest,
  CUTOVER_MANIFEST_PATH,
  MANIFEST_PATH,
  serializeCutoverManifest,
  serializeManifest,
} from "../src/lib/technical-foul-manifest.js";

const targets: Array<{ path: string; name: string; expected: string; hint: string }> = [
  {
    path: MANIFEST_PATH,
    name: "technical-foul.json",
    expected: serializeManifest(computeManifest()),
    hint: "bump SCRUB_RULESET_VERSION if the ruleset changed, then run `bun run technical-foul:manifest` and commit it.",
  },
  {
    path: CUTOVER_MANIFEST_PATH,
    name: "technical-foul-cutover.json",
    expected: serializeCutoverManifest(computeCutoverManifest()),
    hint: "the pin, vendored scrub-bindgen bytes or bindgen sealer sources changed; review, then run `bun run technical-foul:manifest` and commit it.",
  },
];

if (process.argv.includes("--check")) {
  let failed = false;
  for (const { path, name, expected, hint } of targets) {
    const actual = existsSync(path) ? readFileSync(path, "utf8") : "";
    if (actual === expected) {
      console.log(`${name} is up to date`);
      continue;
    }
    failed = true;
    const want = expected.split("\n");
    const have = actual.split("\n");
    console.error(`${name} is stale: ${hint}`);
    for (let i = 0; i < Math.max(want.length, have.length); i++) {
      if (want[i] !== have[i]) {
        console.error(`  line ${i + 1}: checked-in ${JSON.stringify(have[i])} != computed ${JSON.stringify(want[i])}`);
      }
    }
  }
  process.exit(failed ? 1 : 0);
}

for (const { path, expected } of targets) {
  writeFileSync(path, expected);
  console.log(`wrote ${path}`);
}
