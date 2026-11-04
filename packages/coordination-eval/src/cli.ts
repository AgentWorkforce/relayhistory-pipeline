import { mkdir, open, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { buildReport, renderMarkdown } from "./report.ts";
import { prepareAssessment } from "./intake.ts";
import { parseAssessment } from "./contract.ts";
import { buildAnalysisPackets } from "./packets.ts";

const MAX_INPUT_BYTES = 16 * 1024 * 1024;

export async function run(
  inputPath: string,
  outputPath: string,
  mode: "assessment" | "sessions" = "assessment",
): Promise<void> {
  const handle = await open(inputPath, "r");
  let data: Buffer;
  try {
    const info = await handle.stat();
    if (!info.isFile() || info.size > MAX_INPUT_BYTES)
      throw new Error("Input must be a regular JSON file of at most 16 MiB.");
    data = Buffer.alloc(info.size + 1);
    let bytesRead = 0;
    while (bytesRead < data.length) {
      const chunk = await handle.read(
        data,
        bytesRead,
        data.length - bytesRead,
        bytesRead,
      );
      if (!chunk.bytesRead) break;
      bytesRead += chunk.bytesRead;
    }
    const after = await handle.stat();
    if (
      bytesRead !== info.size ||
      after.size !== info.size ||
      after.mtimeMs !== info.mtimeMs
    )
      throw new Error(
        "Input changed while reading; create a stable export first.",
      );
    data = data.subarray(0, bytesRead);
  } finally {
    await handle.close();
  }
  let input: unknown;
  try {
    input = JSON.parse(data.toString("utf8"));
  } catch {
    throw new Error("Input is not valid JSON.");
  }
  const prepared = mode === "sessions" ? prepareAssessment(input) : null;
  const assessment = prepared ? prepared.assessment : parseAssessment(input);
  const report = buildReport(assessment);
  const packets = buildAnalysisPackets(assessment);
  const output = resolve(outputPath);
  await mkdir(dirname(output), { recursive: true, mode: 0o700 });
  await mkdir(output, { mode: 0o700 });
  const save = async (name: string, value: string) =>
    writeFile(`${output}/${name}`, value, { flag: "wx", mode: 0o600 });
  const files = [
    "report.json",
    "report.md",
    "review-queue.json",
    "review-plan.json",
    "assessment.json",
    "analysis-packets.json",
  ];
  await save("assessment.json", JSON.stringify(assessment, null, 2) + "\n");
  await save("analysis-packets.json", JSON.stringify(packets, null, 2) + "\n");
  if (prepared) {
    await save(
      "inventory.json",
      JSON.stringify(prepared.inventory, null, 2) + "\n",
    );
    files.push("inventory.json");
  }
  await save("report.json", JSON.stringify(report, null, 2) + "\n");
  await save("report.md", renderMarkdown(report));
  await save(
    "review-queue.json",
    JSON.stringify(
      report.findings.filter(
        (f) =>
          f.review.verdict === "pending" || f.review.verdict === "uncertain",
      ),
      null,
      2,
    ) + "\n",
  );
  await save(
    "review-plan.json",
    JSON.stringify(
      {
        workstreamIds: report.workstreams
          .filter((w) => w.assessment === "pending")
          .map((w) => w.id),
        observationIds: report.observations.map((o) => o.id),
        findingIds: report.findings
          .filter(
            (f) =>
              f.review.verdict === "pending" ||
              f.review.verdict === "uncertain",
          )
          .map((f) => f.id),
        instructions:
          "Inspect complete source context for each observation. Record a supported finding or a rejection; overlap alone is not evidence of waste. Complete the workstream assessment before adding it to the baseline.",
      },
      null,
      2,
    ) + "\n",
  );
  // A manifest is the completion marker; consumers ignore runs without one.
  await save(
    "manifest.json",
    JSON.stringify(
      {
        status: "complete",
        evaluatorVersion: report.evaluatorVersion,
        rubricVersion: report.rubricVersion,
        inputSha256: report.inputSha256,
        synthetic: report.synthetic,
        files,
      },
      null,
      2,
    ) + "\n",
  );
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  if (
    args.length !== 4 ||
    !["--input", "--sessions"].includes(args[0]!) ||
    args[2] !== "--out"
  ) {
    process.stderr.write(
      "Usage: bun run coordination:report (--input assessment.json | --sessions export.json) --out NEW_RUN_DIRECTORY\n",
    );
    process.exitCode = 1;
  } else {
    try {
      await run(
        args[1]!,
        args[3]!,
        args[0] === "--sessions" ? "sessions" : "assessment",
      );
      process.stdout.write(
        "Coordination report complete. Review report.md and manifest.json in the output directory.\n",
      );
    } catch (error) {
      // Node filesystem errors include paths; do not echo them or customer input.
      const message =
        error instanceof Error && !("code" in error)
          ? error.message
          : "Could not read input or create a new output directory. Existing runs are never overwritten.";
      process.stderr.write(`${message}\n`);
      process.exitCode = 1;
    }
  }
}
