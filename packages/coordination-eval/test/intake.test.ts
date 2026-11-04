import { describe, expect, test } from "bun:test";
import fixture from "../fixtures/sessions-example.json";
import { prepareAssessment } from "../src/intake.ts";
import { buildReport } from "../src/report.ts";

const input = () => structuredClone(fixture);

describe("scoped session intake", () => {
  test("builds evidence-backed pending workstreams and structural signals without invented findings", () => {
    const { assessment, inventory } = prepareAssessment(input());
    expect(assessment.findings).toEqual([]);
    expect(assessment.workstreams).toHaveLength(2);
    expect(
      assessment.workstreams.every(
        (w) => w.assessment === "pending" && w.state === "unknown",
      ),
    ).toBe(true);
    expect(
      assessment.observations.some((o) => o.kind === "shared_surface"),
    ).toBe(true);
    expect(assessment.observations.some((o) => o.kind === "shared_task")).toBe(
      true,
    );
    expect(inventory.sessions).toBe(5);
    expect(inventory.declaredSessionRoots).toBe(4);
    expect(buildReport(assessment).baseline.reviewedWorkstreams).toBe(0);
  });

  test("is independent of export ordering", () => {
    const reordered = input();
    reordered.sessions.reverse();
    reordered.events.reverse();
    expect(prepareAssessment(reordered)).toEqual(prepareAssessment(input()));
  });

  test("does not treat a delegated helper as independent overlapping work", () => {
    const a = input();
    a.sessions = a.sessions.filter((s) =>
      ["producer", "helper"].includes(s.key),
    );
    a.events = a.events.filter((e) =>
      ["producer", "helper"].includes(e.sessionKey),
    );
    const result = prepareAssessment(a);
    expect(result.inventory.declaredSessionRoots).toBe(1);
    expect(result.assessment.observations).toEqual([]);
  });

  test("never matches the same relative file across different projects", () => {
    const a = input();
    a.sessions = a.sessions.filter((s) =>
      ["backend", "frontend"].includes(s.key),
    );
    a.events = a.events.filter((e) =>
      ["backend", "frontend"].includes(e.sessionKey),
    );
    a.sessions[1]!.projectKey = "github.com/example/unrelated";
    expect(prepareAssessment(a).assessment.observations).toEqual([]);
  });

  test("missing project metadata isolates independent roots even with the same task label", () => {
    const a = input();
    a.sessions.forEach((s) => {
      s.projectKey = null;
    });
    expect(prepareAssessment(a).assessment.observations).toEqual([]);
  });

  test("checks the proximity window rather than assuming a session was continuously active", () => {
    const a = input();
    a.sessions = a.sessions.filter((s) =>
      ["backend", "frontend"].includes(s.key),
    );
    a.events = a.events.filter((e) =>
      ["api-decision", "client-revision"].includes(e.id),
    );
    expect(prepareAssessment(a).assessment.observations).toEqual([]);
  });

  test("rejects cross-workspace evidence, missing parents and cycles", () => {
    const a = input();
    a.sessions[0]!.workspaceId = "foreign-workspace";
    expect(() => prepareAssessment(a)).toThrow("cross-scope");
    const b = input();
    b.sessions.find((s) => s.key === "helper")!.parentSessionKey = "missing";
    expect(() => prepareAssessment(b)).toThrow("parent session is missing");
    const c = input();
    c.sessions.find((s) => s.key === "producer")!.parentSessionKey = "helper";
    expect(() => prepareAssessment(c)).toThrow("cyclic parent");
  });

  test("preserves unavailable and truncated evidence as limitations", () => {
    const a = input();
    a.events[0]!.truncated = true;
    const result = prepareAssessment({
      ...a,
      events: a.events.map((e, i) => (i ? e : { ...e, availableAt: null })),
    });
    expect(result.inventory.truncatedEvents).toBe(1);
    expect(result.inventory.unknownAvailabilityEvents).toBe(1);
    expect(result.assessment.findings).toEqual([]);
  });

  test("lists sessions without in-window evidence rather than manufacturing summaries", () => {
    const a = input();
    a.events = [];
    const result = prepareAssessment(a);
    expect(result.assessment.workstreams).toEqual([]);
    expect(result.inventory.sessionsWithoutWindowEvidence).toHaveLength(5);
  });

  test("rejects unsanitized metadata, duplicate aliases and oversized exports", () => {
    const a = input();
    a.sessions[0]!.title = "Bearer " + "a".repeat(30);
    expect(() => prepareAssessment(a)).toThrow("requires upstream redaction");
    const b = input();
    b.events.push({ ...b.events[0]!, id: "alias-for-same-event" });
    expect(() => prepareAssessment(b)).toThrow("duplicate identity");
    expect(() =>
      prepareAssessment({
        ...input(),
        events: Array(10001).fill(input().events[0]),
      }),
    ).toThrow("supported bound");
  });

  test("discloses the observation limit instead of claiming exhaustive detection", () => {
    const a = input();
    a.sessions = Array.from({ length: 30 }, (_, i) => ({
      ...a.sessions[0]!,
      key: `session-${i}`,
      sessionId: `source-session-${i}`,
      taskRefs: [],
    }));
    a.events = a.sessions.map((s, i) => ({
      ...a.events[0]!,
      id: `evidence-${i}`,
      sessionKey: s.key,
      eventId: `event-${i}`,
    }));
    const result = prepareAssessment(a);
    expect(result.assessment.observations).toHaveLength(200);
    expect(result.inventory.observations.limitReached).toBe(true);
    expect(result.assessment.coverage.notes).toContain(
      "not an exhaustive overlap inventory",
    );
  });
});
