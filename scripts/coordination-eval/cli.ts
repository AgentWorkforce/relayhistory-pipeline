#!/usr/bin/env bun
import { dirname, join, resolve } from "node:path";
import { evaluate, writeReports } from "./harness.ts";

function usage(): never {
  throw new Error("usage: bun run coordination-eval --snapshot <dir> --truth <split.json> [--out <dir>] [--projection current|snapshot]");
}

function argumentsOf(argv: string[]) {
  const values = new Map<string, string>();
  for (let index = 0; index < argv.length; index += 1) {
    const name = argv[index]!;
    if (!["--snapshot", "--truth", "--out", "--projection"].includes(name)) usage();
    const value = argv[index + 1];
    if (!value || value.startsWith("--")) usage();
    values.set(name, value);
    index += 1;
  }
  const snapshot = values.get("--snapshot"), truth = values.get("--truth");
  if (!snapshot || !truth) usage();
  const projection = values.get("--projection") ?? "snapshot";
  if (projection !== "snapshot" && projection !== "current") usage();
  const snapshotDir = resolve(snapshot);
  return {
    snapshotDir,
    truthFile: resolve(truth),
    outDir: resolve(values.get("--out") ?? join(dirname(snapshotDir), "coordination-eval-output", projection)),
    projection,
  } as const;
}

try {
  const options = argumentsOf(process.argv.slice(2));
  const { report, wallClockMs } = await evaluate(options);
  await writeReports(options.outDir, report);
  process.stderr.write(`coordination-eval: ${report.status}; ${wallClockMs.toFixed(1)} ms; reports written to ${options.outDir}\n`);
} catch (error) {
  process.stderr.write(`coordination-eval: ${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
}
