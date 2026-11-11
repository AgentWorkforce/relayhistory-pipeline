import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { SCRUB_RULESET_VERSION } from "../src/lib/pipeline/raw-chunks.js";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import {
  computeCutoverManifest,
  computeManifest,
  CUTOVER_MANIFEST_PATH,
  MANIFEST_PATH,
  redactionToken,
  serializeCutoverManifest,
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

describe("technical-foul-cutover.json (new entry; TS history untouched)", () => {
  const checkedIn = readFileSync(CUTOVER_MANIFEST_PATH, "utf8");
  const cutover = JSON.parse(checkedIn);
  const pin = JSON.parse(
    readFileSync(fileURLToPath(new URL("../scrub-bindgen-pin.json", import.meta.url).href), "utf8"),
  );

  it("matches the cutover manifest computed from the pin, vendored bytes and sealer sources", () => {
    expect(checkedIn).toBe(serializeCutoverManifest(computeCutoverManifest()));
    expect(checkedIn).not.toContain("\r");
  });

  it("is a separate file and never relabels the TS entry", () => {
    expect(CUTOVER_MANIFEST_PATH).not.toBe(MANIFEST_PATH);
    const ts = JSON.parse(readFileSync(MANIFEST_PATH, "utf8"));
    expect(ts.rulesetVersion).toBe(8);
    expect(ts).not.toHaveProperty("engine");
    expect(ts).not.toHaveProperty("pinDigests");
    expect(cutover.tsReference.rulesetVersion).toBe(ts.rulesetVersion);
    expect(cutover.tsReference.rulesetSourceSha256).toBe(ts.rulesetSourceSha256);
    expect(cutover.tsReference.manifestSha256).toBe(
      createHash("sha256").update(readFileSync(MANIFEST_PATH)).digest("hex"),
    );
  });

  it("records the exact alpha.4 pin values", () => {
    expect(cutover.engine).toBe("wasm-bindgen");
    expect(cutover.packageVersion).toBe("0.5.0-alpha.4");
    expect(cutover.pinDigests).toEqual({
      wasm: "bd181f94e2d03d7a5381a31488aa5ffbc991e4d41132c7f8f08095031608b5da",
      glue: "55dd6fc6a44ed35f846655ab08bf3161ffe105123cbe11d3e529923d6bdeae5b",
      loader: "ff139ab391a958475b455ef483e7119d66215d172647223965dc35348f912490",
    });
    expect(cutover.crateManifest).toEqual(pin.manifest);
    expect(cutover.crateManifest.rulesetVersion).toBe(10);
    expect(cutover.scrubProfile).toBe("relayhistory-secrets-v10");
    expect(cutover.artifactJsonSha256).toBe(
      "cf43f67ad3033cca73d8a36a67a707f258a51517e1c13fa1dc1df8f66f49a9ed",
    );
    expect(cutover.toolchain).toEqual({ rust: "1.88.0", wasmBindgen: "0.2.104" });
  });

  it("keeps the sealed raw-chunks.ts digest equal to the committed base", () => {
    const repoRoot = fileURLToPath(new URL("../../../", import.meta.url).href);
    let base: Buffer | undefined;
    try {
      base = execFileSync("git", ["show", "HEAD:packages/pipeline/src/lib/pipeline/raw-chunks.ts"], {
        cwd: repoRoot,
      });
    } catch {
      // Not a git checkout (e.g. a packed image): the computed-vs-checked-in test still pins it.
    }
    if (base) {
      expect(cutover.tsReference.rawChunksSha256).toBe(
        createHash("sha256").update(base.toString("utf8").replace(/\r\n/g, "\n")).digest("hex"),
      );
    }
  });
});
