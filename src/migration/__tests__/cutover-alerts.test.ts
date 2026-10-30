import { describe, expect, test } from "bun:test";
import { AlertTracker, EVIDENCE_SCOPE, evidenceFor, evidenceTarget, findProblems, sendEvidence, type Alert } from "../cutover-alerts.ts";
import type { MetricPoint } from "../cutover-metrics.ts";

const ok = (kind: string, value = 1): MetricPoint => ({ metric: "cutover.metrics.read_ok", value, unit: "1", kind });
const healthy = [ok("jobs"), ok("dead_letters"), ok("events"), ok("errors"), { metric: "cutover.metrics.window_seconds", value: 300, unit: "s" }] as MetricPoint[];
const dead = (stage: string, value: number, state = "dead"): MetricPoint => ({ metric: "pipeline.dlq.depth", value, unit: "1", stage, state });
const lag = (value: number, stage = "session-indexed"): MetricPoint => ({ metric: "pipeline.stage.lag_seconds", value, unit: "s", stage });
const keys = (points: MetricPoint[]) => findProblems(points).map((p) => p.key);
const context = { environment: "production", version: "abc", now: new Date("2026-10-30T12:00:30Z") };

describe("findProblems", () => {
  test("a healthy sample has no problems", () => {
    expect(keys([...healthy, dead("session-indexed", 0), lag(5)])).toEqual([]);
  });
  test("a blind source alerts, and asserts nothing about what it couldn't read", () => {
    const points = [ok("jobs", 0), ok("dead_letters"), ok("events"), ok("errors"), dead("session-indexed", 3, "dead")];
    expect(keys(points)).toEqual(["metrics_blind:jobs"]);
  });
  test("dead and pending letters count, replaying ones are already being handled", () => {
    expect(findProblems([...healthy, dead("object-landed", 2), dead("object-landed", 1, "pending"), dead("object-landed", 9, "replaying")])[0]).toMatchObject({
      key: "dlq_depth:object-landed", value: 3,
    });
  });
  test("lag alerts on the pointer path only", () => {
    expect(keys([...healthy, lag(61), lag(9000, "summary-long")])).toEqual(["stage_lag:session-indexed"]);
  });
  test("ready jobs with no ok event is a stall, and only if the events were read", () => {
    const ready: MetricPoint = { metric: "pipeline.stage.ready_depth", value: 4, unit: "1", stage: "object-landed" };
    expect(keys([...healthy, ready])).toEqual(["stage_stalled:object-landed"]);
    expect(keys([...healthy, ready, { metric: "pipeline.stage.events", value: 1, unit: "1", stage: "object-landed", status: "ok" }])).toEqual([]);
    expect(keys([ok("jobs"), ok("dead_letters"), ok("events", 0), ok("errors"), ready])).toEqual(["metrics_blind:events"]);
  });
});

describe("AlertTracker", () => {
  const run = (tracker: AlertTracker, points: MetricPoint[]) => tracker.update(findProblems(points), points);
  test("a threshold must hold for two samples, and a fix recovers once", () => {
    const tracker = new AlertTracker();
    expect(run(tracker, [...healthy, lag(70)]).firing).toEqual([]);
    expect(run(tracker, [...healthy, lag(70)]).firing.map((a) => a.key)).toEqual(["stage_lag:session-indexed"]);
    expect(run(tracker, [...healthy, lag(1)]).recovered.map((a) => a.key)).toEqual(["stage_lag:session-indexed"]);
    expect(run(tracker, [...healthy, lag(1)]).recovered).toEqual([]);
  });
  test("a one-sample blip never alerts or recovers", () => {
    const tracker = new AlertTracker();
    run(tracker, [...healthy, lag(70)]);
    expect(run(tracker, [...healthy, lag(1)])).toEqual({ firing: [], recovered: [] });
    expect(run(tracker, [...healthy, lag(70)]).firing).toEqual([]);
  });
  test("blindness alerts at once and doesn't read as recovery", () => {
    const tracker = new AlertTracker();
    run(tracker, [...healthy, lag(70)]);
    run(tracker, [...healthy, lag(70)]);
    const blind = [ok("jobs", 0), ok("dead_letters"), ok("events"), ok("errors")];
    const result = run(tracker, blind);
    expect(result.firing.map((a) => a.key)).toEqual(["metrics_blind:jobs"]);
    expect(result.recovered).toEqual([]);
    expect(run(tracker, [...healthy, lag(1)]).recovered.map((a) => a.key).sort()).toEqual(["metrics_blind:jobs", "stage_lag:session-indexed"]);
  });
});

describe("evidence", () => {
  const alert: Alert = { key: "dlq_depth:session-indexed", rule: "dlq_depth", subject: "session-indexed", value: 3, summary: "3 dead letters" };
  test("matches cloud-runtime-evidence/2 and has one incident key", () => {
    const evidence = evidenceFor(alert, "firing", context);
    expect(evidence).toMatchObject({
      schemaVersion: "cloud-runtime-evidence/2", kind: "dlq_dead_letter", outcome: "dlq", category: "platform", terminal: false,
      incidentFingerprint: "96bulls:cutover:dlq_depth:session-indexed",
      workspace: { id: "unknown", relayId: EVIDENCE_SCOPE },
    });
    expect(evidence.unknownReason.length).toBeGreaterThan(0);
    expect(Number.isInteger(evidence.severity)).toBe(true);
  });
  test("a retry is idempotent, a later minute is a new occurrence, recovery closes the same incident", () => {
    const later = { ...context, now: new Date("2026-10-30T12:01:30Z") };
    expect(evidenceFor(alert, "firing", context).occurrenceId).toBe(evidenceFor(alert, "firing", { ...context, now: new Date("2026-10-30T12:00:45Z") }).occurrenceId);
    expect(evidenceFor(alert, "firing", later).occurrenceId).not.toBe(evidenceFor(alert, "firing", context).occurrenceId);
    const recovered = evidenceFor(alert, "recovered", context);
    expect(recovered.outcome).toBe("ok");
    expect(recovered.incidentFingerprint).toBe(evidenceFor(alert, "firing", context).incidentFingerprint);
  });
});

describe("evidenceTarget", () => {
  const url = "https://nightcto.example/webhooks/cloud-runtime";
  test("unset is none, and half a configuration is an error", () => {
    expect(evidenceTarget({})).toBeUndefined();
    expect(evidenceTarget({ NIGHTCTO_EVIDENCE_URL: url, NIGHTCTO_EVIDENCE_TOKEN: "t" })).toEqual({ url, token: "t" });
    expect(() => evidenceTarget({ NIGHTCTO_EVIDENCE_URL: url })).toThrow();
    expect(() => evidenceTarget({ NIGHTCTO_EVIDENCE_TOKEN: "t" })).toThrow();
  });
  test("refuses placeholders and any other endpoint, without echoing the token", () => {
    expect(() => evidenceTarget({ NIGHTCTO_EVIDENCE_URL: url, NIGHTCTO_EVIDENCE_TOKEN: "PLACEHOLDER_REPLACE_ME_X" })).toThrow();
    expect(() => evidenceTarget({ NIGHTCTO_EVIDENCE_URL: "http://nightcto.example/webhooks/cloud-runtime", NIGHTCTO_EVIDENCE_TOKEN: "planted" })).toThrow();
    try {
      evidenceTarget({ NIGHTCTO_EVIDENCE_URL: "https://x.example/other", NIGHTCTO_EVIDENCE_TOKEN: "planted" });
    } catch (err) {
      expect(String(err)).not.toContain("planted");
    }
  });
});

describe("sendEvidence", () => {
  const target = { url: "https://n.example/webhooks/cloud-runtime", token: "t" };
  test("sends the exact bearer form the webhook compares", async () => {
    let auth = "";
    const status = await sendEvidence(target, { a: 1 }, (async (_: string, init: RequestInit) => {
      auth = new Headers(init.headers).get("authorization")!;
      return new Response(null, { status: 202 });
    }) as never);
    expect(status).toBe(202);
    expect(auth).toBe("Bearer t");
  });
  test("a network failure is a status of 0, and an oversized body is never sent", async () => {
    expect(await sendEvidence(target, {}, (async () => { throw new Error("down"); }) as never)).toBe(0);
    let called = false;
    expect(await sendEvidence(target, { a: "x".repeat(17_000) }, (async () => { called = true; return new Response(); }) as never)).toBe(0);
    expect(called).toBe(false);
  });
});
