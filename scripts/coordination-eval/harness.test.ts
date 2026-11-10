import { describe, expect, test } from "bun:test";
import { appendFile, cp, mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { evaluate, reportsAreByteIdentical, writeReports } from "./harness.ts";

const fixture = join(import.meta.dir, "fixtures");
const options = (projection: "snapshot" | "current") => ({
  snapshotDir: join(fixture, "snapshot"),
  truthFile: join(fixture, "truth", "split.json"),
  projection,
});

describe("coordination evaluation harness", () => {
  test("scores every detector and outcome tier from the synthetic snapshot", async () => {
    const { report } = await evaluate(options("snapshot"));
    expect(report.status).toBe("complete");
    expect(report.detectors.map((detector) => detector.name)).toEqual(["outcome-facts", "shared-write-60m"]);
    const outcome = report.detectors[0]!;
    expect(Object.keys(outcome.tiers)).toEqual(["overall", "T1", "T2", "T3"]);
    expect(outcome.tiers.T1!.dev.incidents.foundIds).toEqual(["fixture-dev-incident"]);
    expect(outcome.tiers.T2!.dev.alerts).toBe(0);
    expect(outcome.tiers.T3!.heldout.incidents.foundIds).toEqual(["fixture-heldout-incident"]);
    expect(JSON.stringify(report)).not.toContain('"session_id"');
    expect(JSON.stringify(report)).not.toContain("dev-a");
  });

  test("uses current projection code for full invented raw tool inputs", async () => {
    const { report } = await evaluate(options("current"));
    expect(report.status).toBe("complete");
    expect(report.coverage.notes).toContain("Stored snapshot files_written was not used for current projection.");
    const outcome = report.detectors.find((detector) => detector.name === "outcome-facts")!;
    expect(outcome.tiers.T1!.dev.incidents.found).toBe(1);
    expect(outcome.tiers.T3!.heldout.incidents.found).toBe(1);
  });

  test("writes byte-identical deterministic reports", async () => {
    const left = await mkdtemp(join(tmpdir(), "coordination-eval-a-"));
    const right = await mkdtemp(join(tmpdir(), "coordination-eval-b-"));
    const first = await evaluate(options("snapshot"));
    const second = await evaluate(options("snapshot"));
    await writeReports(left, first.report);
    await writeReports(right, second.report);
    expect(await reportsAreByteIdentical(left, right)).toBe(true);
  });

  test("refuses a snapshot payload that no longer matches its manifest", async () => {
    const root = await mkdtemp(join(tmpdir(), "coordination-eval-tamper-"));
    const snapshotDir = join(root, "snapshot");
    await cp(join(fixture, "snapshot"), snapshotDir, { recursive: true });
    await appendFile(join(snapshotDir, "sessions.ndjson"), "\n");
    await expect(evaluate({ ...options("snapshot"), snapshotDir })).rejects.toThrow("manifest byte mismatch at entry");
  });
});
