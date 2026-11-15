/**
 * Read-only live diff for `transcriptStore.adoptIdentities` (stacks/aws/adopt-identities-live.ts; docs/aws-stage.md
 * "Adopt identities"), for the operator to run before the capture. The capture and the deploy run the same diff
 * themselves and refuse on any difference; this prints the report and its stable sha256 so they can be compared.
 *
 *   bun scripts/adopt-identities-preapply.ts [--inputs <path>]
 *
 * Exit 0: clean. Exit 1: refused (differences listed, fields only). Exit 2: error. IAM/STS/KMS reads only.
 */
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { awsCliRun, preApplyReport } from "../stacks/aws/adopt-identities-live.ts";
import { TRANSCRIPT_STAGE } from "../stacks/aws/transcript-store.ts";

if (import.meta.main) {
  try {
    const argv = process.argv.slice(2);
    for (const a of argv) if (a.startsWith("--") && a !== "--inputs") throw new Error(`unknown flag ${a}`);
    const at = argv.indexOf("--inputs");
    const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
    const path = at >= 0 ? argv[at + 1]! : join(root, ".96bulls", TRANSCRIPT_STAGE, "aws-inputs.json");
    const report = preApplyReport(JSON.parse(readFileSync(path, "utf8")), awsCliRun());
    console.log(JSON.stringify(report, null, 2));
    process.exitCode = report.adoptIdentitiesPreApply === "clean" ? 0 : 1;
  } catch (e) {
    console.error(`adopt-identities pre-apply refused: ${(e as Error).message}`);
    process.exitCode = 2;
  }
}
