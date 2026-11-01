import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { SCRUB_RULESET_VERSION } from "../src/lib/pipeline/raw-chunks.js";

/**
 * Every `lib/scrub.ts` that has sealed raw chunks, by `raw.scrub.version`.
 * Stored chunks record the version they were scrubbed with and are never
 * rewritten, so a rule change must come with a new version. When this test
 * fails after editing `lib/scrub.ts`: bump `SCRUB_RULESET_VERSION` in
 * `lib/pipeline/raw-chunks.ts` and add the new version and digest here. Never
 * edit an existing entry.
 */
const SCRUB_RULESETS: Record<number, string> = {
  1: "db9d65c7ab230d67488900203bb78ca8bd3ad16949a0a51b5d3d117d0cff49f8",
  // Same rules; raw-chunks.ts now scrubs texts past MAX_SCRUB_CHARS whole, in
  // segments (scrubLong), where version 1 refused them.
  2: "db9d65c7ab230d67488900203bb78ca8bd3ad16949a0a51b5d3d117d0cff49f8",
};

function scrubSourceDigest() {
  const path = fileURLToPath(
    new URL("../src/lib/scrub.ts", import.meta.url).href,
  );
  // Line endings depend on the checkout, not the rules.
  const source = readFileSync(path, "utf8").replace(/\r\n/g, "\n");
  return createHash("sha256").update(source).digest("hex");
}

describe("scrub ruleset version", () => {
  it("is bumped with every change to lib/scrub.ts", () => {
    const latest = Math.max(...Object.keys(SCRUB_RULESETS).map(Number));
    expect(
      SCRUB_RULESET_VERSION,
      "SCRUB_RULESET_VERSION must be the newest entry in SCRUB_RULESETS",
    ).toBe(latest);
    expect(
      scrubSourceDigest(),
      "lib/scrub.ts changed: bump SCRUB_RULESET_VERSION and add its digest to SCRUB_RULESETS",
    ).toBe(SCRUB_RULESETS[SCRUB_RULESET_VERSION]);
  });
});
