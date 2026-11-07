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
