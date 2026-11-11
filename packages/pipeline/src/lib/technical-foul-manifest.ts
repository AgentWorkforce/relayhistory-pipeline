import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { SCRUB_RULESET_VERSION } from "./pipeline/raw-chunks.js";
import { MAX_SCRUB_CHARS } from "./scrub.js";

/**
 * Technical Foul (secret scrub) shared-artifact manifest. Tooling only (the
 * generator script and its test): nothing in the runtime imports this file.
 * The digest definitions mirror test/scrub-ruleset-version.test.ts and
 * test/scrub-secret-corpus.test.ts exactly (CRLF normalised to LF).
 */

/** Repository root (this file is packages/pipeline/src/lib/). */
const REPO_ROOT = new URL("../../../../", import.meta.url);

export const MANIFEST_PATH = fileURLToPath(new URL("packages/pipeline/technical-foul.json", REPO_ROOT).href);

const RULESET_PATH = "packages/pipeline/src/lib/scrub.ts";
const RAW_CHUNKS_PATH = "packages/pipeline/src/lib/pipeline/raw-chunks.ts";
const CORPUS_PATH = "packages/pipeline/test/fixtures/scrub-secret-corpus.json";

const readLf = (repoRel: string) =>
  readFileSync(fileURLToPath(new URL(repoRel, REPO_ROOT).href), "utf8").replace(/\r\n/g, "\n");

const sha256 = (s: string) => createHash("sha256").update(s).digest("hex");

/** The segmenter's source: everything between the markers in raw-chunks.ts. */
export function segmenterSource(): string {
  const m = /\/\/ <scrub-segmenter>[\s\S]*?\/\/ <\/scrub-segmenter>/.exec(readLf(RAW_CHUNKS_PATH));
  if (!m) throw new Error("raw-chunks.ts lost its <scrub-segmenter> markers");
  return m[0];
}

/** The redaction token, read from the `const REDACTED = "...";` declaration in scrub.ts. */
export function redactionToken(): string {
  const matches = [...readLf(RULESET_PATH).matchAll(/^const REDACTED = "([^"\\\n]*)";$/gm)];
  if (matches.length !== 1) {
    throw new Error(
      `scrub.ts must declare exactly one \`const REDACTED = "...";\` line; found ${matches.length}`,
    );
  }
  return matches[0][1];
}

export interface TechnicalFoulManifest {
  schema: 1;
  rulesetVersion: number;
  rulesetSourceSha256: string;
  rulesetPath: string;
  segmenterPath: string;
  segmenterSha256: string;
  corpusSha256: string;
  corpusPath: string;
  redactionToken: string;
  maxScrubChars: number;
}

export function computeManifest(): TechnicalFoulManifest {
  const segmenter = segmenterSource();
  return {
    schema: 1,
    rulesetVersion: SCRUB_RULESET_VERSION,
    rulesetSourceSha256: sha256(`${readLf(RULESET_PATH)}\n${segmenter}`),
    rulesetPath: RULESET_PATH,
    segmenterPath: `${RAW_CHUNKS_PATH}#scrub-segmenter`,
    segmenterSha256: sha256(segmenter),
    corpusSha256: sha256(readLf(CORPUS_PATH)),
    corpusPath: CORPUS_PATH,
    redactionToken: redactionToken(),
    maxScrubChars: MAX_SCRUB_CHARS,
  };
}

export const serializeManifest = (m: TechnicalFoulManifest) => `${JSON.stringify(m, null, 2)}\n`;

/*
 * Cutover entry (relayscrubber 0.5.0-alpha.4 via wasm-bindgen). This is a NEW,
 * separate manifest: technical-foul.json above is immutable TS history and is
 * never relabelled. The cutover manifest binds the pin, the vendored bytes the
 * pin names, the bindgen sealer sources, and a reference to the TS entry it
 * sits beside (including the unchanged raw-chunks.ts digest).
 */

export const CUTOVER_MANIFEST_PATH = fileURLToPath(
  new URL("packages/pipeline/technical-foul-cutover.json", REPO_ROOT).href,
);

const PIN_PATH = "packages/pipeline/scrub-bindgen-pin.json";
const BINDGEN_DIR = "packages/pipeline/scrub-bindgen";
const ARTIFACT_JSON_PATH = `${BINDGEN_DIR}/generated/artifact.json`;
const CUTOVER_SOURCES = [
  "packages/pipeline/src/lib/pipeline/engine-raw-chunks.ts",
  "packages/pipeline/src/lib/scrub-engine/bindgen-engine.ts",
  "packages/pipeline/src/lib/scrub-engine/bindgen-admission.ts",
  "packages/pipeline/src/lib/scrub-engine/index.ts",
] as const;

/** Raw-byte digest (vendored artifacts are data; never LF-normalised). */
const sha256Bytes = (repoRel: string) =>
  createHash("sha256").update(readFileSync(fileURLToPath(new URL(repoRel, REPO_ROOT).href))).digest("hex");

export interface TechnicalFoulCutoverManifest {
  schema: 1;
  entry: "cutover-relayscrubber-0.5.0-alpha.4";
  engine: "wasm-bindgen";
  envTriple: { SCRUB_ENGINE: "wasm-bindgen"; SCRUB_ENGINE_REQUIRED: "wasm-bindgen"; READER_MAX_RULESET: "10" };
  package: string;
  packageVersion: string;
  pinPath: string;
  pinFileSha256: string;
  /** The GitHub release the vendored bytes were published as (pin `release` block, verbatim). */
  release: {
    source: string;
    repository: string;
    tag: string;
    tarball: string;
    tarball_sha256: string;
    sha256sums: Record<string, string>;
  };
  pinDigests: { wasm: string; glue: string; loader: string };
  scrubProfile: string;
  crateManifest: Record<string, unknown>;
  toolchain: { rust: string; wasmBindgen: string };
  artifactJsonSha256: string;
  vendoredFiles: Record<string, string>;
  sealerSources: Record<string, string>;
  tsReference: {
    manifestPath: string;
    manifestSha256: string;
    rulesetVersion: number;
    rulesetSourceSha256: string;
    rawChunksPath: string;
    rawChunksSha256: string;
  };
}

export function computeCutoverManifest(): TechnicalFoulCutoverManifest {
  const pin = JSON.parse(readLf(PIN_PATH));
  const artifact = JSON.parse(readLf(ARTIFACT_JSON_PATH));
  const vendoredFiles: Record<string, string> = {};
  for (const rel of [
    "index.js",
    "index.d.ts",
    "generated/relayscrubber.js",
    "generated/relayscrubber.d.ts",
    "generated/relayscrubber_bg.wasm",
    "generated/relayscrubber_bg.wasm.d.ts",
    "generated/artifact.json",
  ]) {
    vendoredFiles[rel] = sha256Bytes(`${BINDGEN_DIR}/${rel}`);
  }
  // Fail closed: the entry is only computable when the bytes match the pin.
  const pinned: Array<[string, string]> = [
    [pin.paths.wasm, pin.wasm_sha256],
    [pin.paths.glue, pin.glue_sha256],
    [pin.paths.loader, pin.loader_sha256],
  ];
  for (const [rel, want] of pinned) {
    if (vendoredFiles[rel] !== want) {
      throw new Error(`vendored ${rel} sha256 ${vendoredFiles[rel]} != pin ${want}`);
    }
  }
  if (pin.release?.sha256sums?.["artifact.json"] !== vendoredFiles["generated/artifact.json"]) {
    throw new Error("scrub-bindgen-pin.json release.sha256sums artifact.json differs from the vendored artifact.json");
  }
  if (JSON.stringify(pin.manifest) !== JSON.stringify(artifact.manifest)) {
    throw new Error("scrub-bindgen-pin.json manifest differs from vendored artifact.json manifest");
  }
  const sealerSources: Record<string, string> = {};
  for (const rel of CUTOVER_SOURCES) sealerSources[rel] = sha256(readLf(rel));
  const ts = computeManifest();
  return {
    schema: 1,
    entry: "cutover-relayscrubber-0.5.0-alpha.4",
    engine: "wasm-bindgen",
    envTriple: { SCRUB_ENGINE: "wasm-bindgen", SCRUB_ENGINE_REQUIRED: "wasm-bindgen", READER_MAX_RULESET: "10" },
    package: pin.package,
    packageVersion: pin.package_version,
    pinPath: PIN_PATH,
    pinFileSha256: sha256Bytes(PIN_PATH),
    release: pin.release,
    pinDigests: { wasm: pin.wasm_sha256, glue: pin.glue_sha256, loader: pin.loader_sha256 },
    scrubProfile: pin.scrub_profile,
    crateManifest: pin.manifest,
    toolchain: { rust: artifact.toolchain, wasmBindgen: artifact.wasmBindgen },
    artifactJsonSha256: vendoredFiles["generated/artifact.json"],
    vendoredFiles,
    sealerSources,
    tsReference: {
      manifestPath: "packages/pipeline/technical-foul.json",
      manifestSha256: sha256(serializeManifest(ts)),
      rulesetVersion: ts.rulesetVersion,
      rulesetSourceSha256: ts.rulesetSourceSha256,
      rawChunksPath: RAW_CHUNKS_PATH,
      rawChunksSha256: sha256(readLf(RAW_CHUNKS_PATH)),
    },
  };
}

export const serializeCutoverManifest = (m: TechnicalFoulCutoverManifest) => `${JSON.stringify(m, null, 2)}\n`;
