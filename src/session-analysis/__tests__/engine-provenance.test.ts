/**
 * The vendored full-analysis engine is exactly what SOURCE.json says it is, and its
 * prompt, models and versions are relayhistory's production ones.
 */
import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ANALYSIS_INSTRUCTIONS } from "../engine/analysis-prompt.ts";
import { ANALYZER_VERSION } from "../engine/model.ts";
import { SESSION_ANALYSIS_SCHEMA_VERSION } from "../engine/analysis-contract.ts";
import {
  SESSION_ANALYSIS_MODEL_VERSION,
  SESSION_ANALYSIS_RESULT_SCHEMA_VERSION,
  SESSION_ANALYSIS_VERSION,
} from "../versions.ts";

const dir = join(import.meta.dir, "../engine");
const source = JSON.parse(readFileSync(join(dir, "SOURCE.json"), "utf8")) as {
  analysis_prompt_sha256: string;
  files: Record<string, { sha256: string; upstream_sha256: string; adaptation: string }>;
};
const sha = (b: string | Buffer) => createHash("sha256").update(b).digest("hex");

describe("vendored relayhistory full-analysis engine", () => {
  test("every vendored file matches its recorded hash; verbatim files match upstream", () => {
    expect(Object.keys(source.files).length).toBe(8);
    for (const [file, entry] of Object.entries(source.files)) {
      const local = sha(readFileSync(join(dir, file)));
      expect(local, file).toBe(entry.sha256);
      if (entry.adaptation === "verbatim") expect(local, file).toBe(entry.upstream_sha256);
    }
  });

  test("an adapted file differs from upstream only in import specifiers", () => {
    // Undo the recorded specifier rewrites; the result must hash to upstream.
    const undo = (text: string) =>
      text
        .replaceAll('"./session-analysis-result.ts"', '"@relayhistory/cloud-client/contract"')
        .replaceAll('"../../session-brief/engine/summary-contract.ts"', '"./summary-contract"')
        .replaceAll('"../../session-brief/engine/summary-prompt.ts"', '"./summary-prompt"')
        .replaceAll('"../../session-brief/engine/normalized-history.ts"', '"./normalized-history"')
        .replace(/from "\.\/([a-z-]+)\.ts"/g, 'from "./$1"');
    for (const [file, entry] of Object.entries(source.files)) {
      if (entry.adaptation === "verbatim") continue;
      expect(sha(undo(readFileSync(join(dir, file), "utf8"))), file).toBe(entry.upstream_sha256);
    }
  });

  test("the prompt, model and versions are relayhistory's", () => {
    expect(sha(ANALYSIS_INSTRUCTIONS)).toBe(source.analysis_prompt_sha256);
    expect(source.analysis_prompt_sha256).toBe(
      "a743a93d40727f68e51e508a5325aa46ee9e8d1304d24c196f5d2a4357121626",
    );
    expect(ANALYZER_VERSION).toBe("session-analysis-v2.4");
    expect(SESSION_ANALYSIS_SCHEMA_VERSION).toBe(2);
    expect(SESSION_ANALYSIS_RESULT_SCHEMA_VERSION).toBe(2);
    expect(SESSION_ANALYSIS_MODEL_VERSION).toBe("gpt-5.4");
    expect(SESSION_ANALYSIS_VERSION).toBe("session-analysis-v2.4:96bulls-s3-v1");
  });
});
