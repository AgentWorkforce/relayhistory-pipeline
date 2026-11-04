import { describe, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import example from "../fixtures/example.json";
import { buildReport, renderMarkdown } from "../src/report.ts";
import { run } from "../src/cli.ts";

const input = () => structuredClone(example);

describe("coordination evidence", () => {
  test("reports reviewed baseline and successful controls without counting targeted candidates", () => {
    const report = buildReport(input());
    expect(report.synthetic).toBe(true);
    expect(report.baseline.reviewedWorkstreams).toBe(2);
    expect(
      report.baseline.metrics.find((m) => m.kind === "conflicting_decisions"),
    ).toEqual({
      kind: "conflicting_decisions",
      confirmedIncidents: 1,
      affectedWorkstreams: 1,
      denominator: 2,
      affectedWorkstreamFraction: 0.5,
    });
    expect(
      report.baseline.metrics.find((m) => m.kind === "repeated_investigation")!
        .confirmedIncidents,
    ).toBe(0);
    expect(report.findings[0]!.replay.timingEligible).toBe(true);
    expect(report.findings.every((f) => f.replay.status === "not_run")).toBe(
      true,
    );
    expect(renderMarkdown(report)).toContain("SYNTHETIC DATA");
  });

  test("excludes targeted confirmed cases and never treats an unreviewed workstream as clean", () => {
    const a = input();
    a.workstreams[0]!.selection = "targeted";
    a.workstreams[1]!.assessment = "pending";
    const report = buildReport(a);
    expect(report.baseline.reviewedWorkstreams).toBe(0);
    expect(report.baseline.pendingWorkstreams).toBe(1);
    expect(
      report.baseline.metrics.every(
        (m) =>
          m.confirmedIncidents === 0 && m.affectedWorkstreamFraction === null,
      ),
    ).toBe(true);
  });

  test("rejects cross-tenant sessions and unknown fields", () => {
    const a = input();
    a.sessions[0]!.workspaceId = "another-workspace";
    expect(() => buildReport(a)).toThrow("cross-scope session");
    expect(() => buildReport({ ...input(), hoursSaved: 8 })).toThrow(
      "unknown field",
    );
  });

  test("rejects duplicate evidence identities and duplicate incidents", () => {
    const a = input();
    a.evidence.push({ ...a.evidence[0]!, id: "new-alias" });
    expect(() => buildReport(a)).toThrow("duplicate identity");
    const b = input();
    b.findings[2]!.incidentId = b.findings[0]!.incidentId;
    expect(() => buildReport(b)).toThrow("duplicate identity");
  });

  test("rejects citations outside the selected workstream", () => {
    const a = input();
    a.findings[0]!.factEvidenceIds = ["ready-message"];
    expect(() => buildReport(a)).toThrow("outside the workstream");
  });

  test("rejects future facts and evidence beyond the report cutoff", () => {
    const a = input();
    a.findings[0]!.checkpoint = "2026-11-02T10:00:00.000Z";
    expect(() => buildReport(a)).toThrow("fact occurs after checkpoint");
    const b = input();
    b.evidence[0]!.occurredAt = b.window.until;
    expect(() => buildReport(b)).toThrow("beyond the reporting cutoff");
  });

  test("retrospectively uploaded or unknown availability cannot qualify for historical replay", () => {
    const a = input();
    a.evidence[0]!.availableAt = "2026-11-04T08:30:00.000Z";
    expect(buildReport(a).findings[0]!.replay.exclusions).toContain(
      "fact_available_after_checkpoint",
    );
    const b: unknown = {
      ...input(),
      evidence: input().evidence.map((e, i) =>
        i === 0 ? { ...e, availableAt: null } : e,
      ),
    };
    expect(buildReport(b).findings[0]!.replay.exclusions).toContain(
      "historical_availability_unknown",
    );
  });

  test("requires a reviewer and complete evidence before confirmation", () => {
    const a = input();
    a.findings[0]!.review.reviewer = null;
    expect(() => buildReport(a)).toThrow("requires reviewer");
    const b = input();
    b.evidence[0]!.truncated = true;
    expect(() => buildReport(b)).toThrow("cannot be truncated");
    const c = input();
    c.findings[0]!.outcomeEvidenceIds = [];
    expect(() => buildReport(c)).toThrow("requires fact, action and outcome");
  });

  test("rejects outcomes preceding actions and pending cases in reviewed workstreams", () => {
    const a = input();
    a.evidence[2]!.occurredAt = "2026-11-02T10:31:00.000Z";
    expect(() => buildReport(a)).toThrow("outcome precedes cited action");
    const b = input();
    b.workstreams[2]!.assessment = "reviewed";
    expect(() => buildReport(b)).toThrow(
      "reviewed workstream has pending findings",
    );
  });

  test("does not label unobserved communication as a successful control", () => {
    const a = input();
    a.findings[1]!.communication = "unknown";
    expect(() => buildReport(a)).toThrow("requires observed communication");
  });

  test("retains rejected and uncertain cases without counting them as confirmed incidents", () => {
    const a = input();
    a.findings[0]!.review.verdict = "rejected";
    a.findings[1]!.review.verdict = "uncertain";
    const report = buildReport(a);
    expect(report.review.rejected).toBe(1);
    expect(report.baseline.uncertainFindings).toBe(1);
    expect(
      report.baseline.metrics.every((m) => m.confirmedIncidents === 0),
    ).toBe(true);
  });

  test("rejects unsafe text without echoing it and escapes Markdown content", () => {
    const a = input();
    const secret = "ghp_" + "z".repeat(30);
    a.evidence[0]!.text = secret;
    expect(() => buildReport(a)).toThrow("requires upstream redaction");
    try {
      buildReport(a);
    } catch (e) {
      expect(String(e)).not.toContain(secret);
    }
    const b = input();
    b.evidence[0]!.text =
      "<script>alert(1)</script> [click](javascript:alert(1))";
    const markdown = renderMarkdown(buildReport(b));
    expect(markdown).not.toContain("<script>");
    expect(markdown).not.toContain("[click](javascript:");
  });

  test("keeps reproducible input fingerprints and changes them when evidence changes", () => {
    const a = input();
    expect(buildReport(a).inputSha256).toBe(buildReport(input()).inputSha256);
    a.evidence[0]!.text += " Additional evidence.";
    expect(buildReport(a).inputSha256).not.toBe(
      buildReport(input()).inputSha256,
    );
  });

  test("writes private reports, review queue and a completion marker without overwriting runs", async () => {
    const directory = await mkdtemp(join(tmpdir(), "coordination-eval-"));
    try {
      const source = join(directory, "input.json");
      const output = join(directory, "run");
      await writeFile(source, JSON.stringify(input()), { mode: 0o600 });
      await run(source, output);
      expect(
        JSON.parse(await readFile(join(output, "manifest.json"), "utf8"))
          .status,
      ).toBe("complete");
      expect(
        JSON.parse(await readFile(join(output, "review-queue.json"), "utf8")),
      ).toHaveLength(1);
      expect((await stat(join(output, "report.json"))).mode & 0o777).toBe(
        0o600,
      );
      const before = await readFile(join(output, "report.md"), "utf8");
      await expect(run(source, output)).rejects.toThrow();
      expect(await readFile(join(output, "report.md"), "utf8")).toBe(before);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
});
