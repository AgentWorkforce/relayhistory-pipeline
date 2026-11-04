import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import sessions from "../fixtures/sessions-example.json";
import example from "../fixtures/example.json";
import { run } from "../src/cli.ts";
import { parseAssessment } from "../src/contract.ts";

test("session intake produces a reviewable draft and bounded packets without manufacturing findings", async () => {
  const directory = await mkdtemp(join(tmpdir(), "coordination-process-"));
  try {
    const source = join(directory, "sessions.json");
    const output = join(directory, "draft");
    await writeFile(source, JSON.stringify(sessions), { mode: 0o600 });
    await run(source, output, "sessions");
    const read = async (name: string) =>
      JSON.parse(await readFile(join(output, name), "utf8"));
    const report = await read("report.json");
    expect(report.reportStage).toBe("draft");
    expect(report.review.confirmed).toBe(0);
    const assessment = parseAssessment(await read("assessment.json"));
    expect(assessment.findings).toHaveLength(0);
    expect(assessment.observations.length).toBeGreaterThan(0);
    expect(
      assessment.evidence.some((e) =>
        e.filesTouched?.includes("pagination.ts"),
      ),
    ).toBe(true);
    expect(
      assessment.sessions.some(
        (s) => s.context?.parentSessionKey === "producer",
      ),
    ).toBe(true);
    expect(
      (await read("review-plan.json")).workstreamIds.length,
    ).toBeGreaterThan(0);
    expect(
      (await read("analysis-packets.json")).packets.length,
    ).toBeGreaterThan(0);
    expect((await read("inventory.json")).sessions).toBe(5);
    const manifest = await read("manifest.json");
    expect(manifest.files).toContain("assessment.json");
    expect(manifest.files).toContain("analysis-packets.json");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("overlap observations cannot cite unrelated sessions or missing evidence", () => {
  const observation = {
    id: "overlap",
    kind: "shared_surface",
    workstreamIds: ["pagination"],
    sessionKeys: ["backend", "frontend"],
    description: "Potential shared surface.",
    evidenceIds: ["api-decision", "client-action"],
  };
  expect(
    parseAssessment({ ...example, observations: [observation] }).observations,
  ).toHaveLength(1);
  expect(() =>
    parseAssessment({
      ...example,
      observations: [{ ...observation, sessionKeys: ["backend", "producer"] }],
    }),
  ).toThrow("session outside workstreams");
  expect(() =>
    parseAssessment({
      ...example,
      observations: [{ ...observation, evidenceIds: ["api-decision"] }],
    }),
  ).toThrow("each observed session requires evidence");
});
