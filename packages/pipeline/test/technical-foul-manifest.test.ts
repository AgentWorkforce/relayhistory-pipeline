import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { SCRUB_RULESET_VERSION } from "../src/lib/pipeline/raw-chunks.js";
import {
  computeManifest,
  MANIFEST_PATH,
  redactionToken,
  serializeManifest,
} from "../src/lib/technical-foul-manifest.js";

const readTest = (name: string) =>
  readFileSync(fileURLToPath(new URL(name, import.meta.url).href), "utf8");

describe("technical-foul.json", () => {
  const checkedIn = readFileSync(MANIFEST_PATH, "utf8");

  it("matches the manifest computed from source (regenerate: bun run technical-foul:manifest)", () => {
    expect(checkedIn).toBe(serializeManifest(computeManifest()));
    expect(checkedIn).not.toContain("\r");
  });

  it("pins the current ruleset version and the corpus digest the corpus test pins", () => {
    const manifest = JSON.parse(checkedIn);
    expect(manifest.rulesetVersion).toBe(SCRUB_RULESET_VERSION);
    const corpusPin = /const CORPUS_SHA256 =\s*"([0-9a-f]{64})"/.exec(
      readTest("./scrub-secret-corpus.test.ts"),
    );
    expect(corpusPin, "CORPUS_SHA256 pin not found").not.toBeNull();
    expect(manifest.corpusSha256).toBe(corpusPin![1]);
    const rulesetPin = new RegExp(`\\n  ${SCRUB_RULESET_VERSION}: "([0-9a-f]{64})"`).exec(
      readTest("./scrub-ruleset-version.test.ts"),
    );
    expect(rulesetPin, "SCRUB_RULESETS pin not found").not.toBeNull();
    expect(manifest.rulesetSourceSha256).toBe(rulesetPin![1]);
  });

  it("derives the redaction token from scrub.ts", () => {
    expect(redactionToken()).toBe("[REDACTED]");
    expect(JSON.parse(checkedIn).redactionToken).toBe("[REDACTED]");
  });
});
