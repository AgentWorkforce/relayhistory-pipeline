// Writes packages/pipeline/technical-foul.json, or with --check verifies it.
// Run from packages/pipeline: `bun run technical-foul:manifest` / `technical-foul:check`.
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import {
  computeManifest,
  MANIFEST_PATH,
  serializeManifest,
} from "../src/lib/technical-foul-manifest.js";

const expected = serializeManifest(computeManifest());

if (process.argv.includes("--check")) {
  const actual = existsSync(MANIFEST_PATH) ? readFileSync(MANIFEST_PATH, "utf8") : "";
  if (actual === expected) {
    console.log("technical-foul.json is up to date");
    process.exit(0);
  }
  const want = expected.split("\n");
  const have = actual.split("\n");
  console.error("technical-foul.json is stale: bump SCRUB_RULESET_VERSION if the ruleset changed, then run `bun run technical-foul:manifest` and commit it.");
  for (let i = 0; i < Math.max(want.length, have.length); i++) {
    if (want[i] !== have[i]) {
      console.error(`  line ${i + 1}: checked-in ${JSON.stringify(have[i])} != computed ${JSON.stringify(want[i])}`);
    }
  }
  process.exit(1);
}

writeFileSync(MANIFEST_PATH, expected);
console.log(`wrote ${MANIFEST_PATH}`);
