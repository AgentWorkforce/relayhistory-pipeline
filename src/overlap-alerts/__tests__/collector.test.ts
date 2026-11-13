import { describe, expect, test } from "bun:test";
import data from "../fixtures/synthetic.json";
import cases from "../fixtures/cases.json";
import { parseDataset, sessionKey, type Judge } from "../contracts.ts";
import { collectAlerts } from "../collector.ts";
import { fileBaseline, validateVerdict } from "../judgment.ts";
import { findCandidates } from "../candidates.ts";
import { evidenceAt, fileKey, repositoryKey } from "../evidence.ts";
import { parseCases, evaluate } from "../evaluation.ts";
import { renderAlerts } from "../report.ts";

const dataset = () => parseDataset(structuredClone(data));
const opts = { asOf: "2026-11-07T12:00:00Z", mode: "replay" as const, lookbackHours: 168, maxPairs: 200 };
describe("coordination collector", () => {
  test("baseline finds observed shared writes, not shared reads or repository basenames", async () => {
    const d = dataset(), r = await collectAlerts(d, fileBaseline, opts);
    expect(r.alerts).toHaveLength(1);
    expect(r.alerts[0]!.sessionKeys).toContain(sessionKey(d.sessions[0]!));
    expect(r.alerts[0]!.kind).toBe("conflicting_changes");
    expect(r.alerts[0]!.status).toBe("candidate");
  });
  test("same user, private sessions and known collaborations do not become pairs", async () => {
    for (const mutate of [(d: ReturnType<typeof dataset>) => d.sessions[1]!.userId = d.sessions[0]!.userId,
      (d: ReturnType<typeof dataset>) => d.sessions[1]!.visibility = "private",
      (d: ReturnType<typeof dataset>) => (d.sessions[0]!.relatedSessionKeys = [sessionKey(d.sessions[1]!)], d.sessions[0]!.relationshipsAvailableAt = "2026-11-07T10:00:00Z")] ) {
      const d = dataset(); mutate(d);
      expect((await collectAlerts(d, fileBaseline, opts)).alerts).toHaveLength(0);
    }
  });
  test("retrieval includes cross-repository semantic opportunities", () => {
    const d = dataset();
    const cs = findCandidates(d.scope, evidenceAt(d, opts.asOf, "replay", 168).sessions);
    for (const name of ["contract", "discovery", "feature", "ownership"]) expect(cs.some(c => c.left.id === `${name}-a` && c.right.id === `${name}-b`)).toBe(true);
  });
  test("replay refuses unknown arrival time and evidence arriving after the cutoff", async () => {
    const d = dataset();
    d.sessions[0]!.evidence[0]!.availableAt = "2026-11-07T14:00:00Z";
    expect((await collectAlerts(d, fileBaseline, opts)).alerts).toHaveLength(0);
    d.sessions[0]!.evidence[0]!.availableAt = null;
    const replay = await collectAlerts(d, fileBaseline, opts);
    expect(replay.alerts).toHaveLength(0); expect(replay.coverage.unknownAvailability).toBe(1);
    expect((await collectAlerts(d, fileBaseline, { ...opts, mode: "retrospective" })).alerts).toHaveLength(1);
  });
  test("future occurrence and current titles cannot contaminate an earlier comparison", async () => {
    const d = dataset(); d.sessions[0]!.evidence[0]!.at = "2026-11-08T00:00:00Z"; d.sessions[0]!.evidence[0]!.availableAt = "2026-11-08T00:01:00Z";
    d.sessions[0]!.title = d.sessions[1]!.title = "Future matching billing change";
    expect((await collectAlerts(d, fileBaseline, opts)).alerts).toHaveLength(0);
  });
  test("old activity and future collaboration metadata cannot mislead replay", async () => {
    const d = dataset();
    for (const session of d.sessions) for (const e of session.evidence) {
      e.at = "2026-11-06T10:00:00Z"; e.availableAt = "2026-11-06T10:01:00Z";
    }
    expect((await collectAlerts(d, fileBaseline, opts)).alerts).toHaveLength(0);
    const current = dataset();
    current.sessions[0]!.relatedSessionKeys = [sessionKey(current.sessions[1]!)];
    current.sessions[0]!.relationshipsAvailableAt = "2026-11-07T14:00:00Z";
    expect((await collectAlerts(current, fileBaseline, opts)).alerts).toHaveLength(1);
  });
  test("stable IDs dedupe runs but fingerprints preserve changed evidence", async () => {
    const d = dataset(); const a = await collectAlerts(d, fileBaseline, opts);
    const b = await collectAlerts({ ...d, scope: { workspaceId: d.scope.workspaceId, orgId: d.scope.orgId }, sessions: [...d.sessions].reverse() }, fileBaseline, opts);
    expect(a.alerts[0]!.id).toBe(b.alerts[0]!.id);
    d.sessions[0]!.evidence[0]!.text += " The retry cap is now three.";
    const c = await collectAlerts(d, fileBaseline, opts);
    expect(c.alerts[0]!.id).toBe(a.alerts[0]!.id);
    expect(c.alerts[0]!.evidenceFingerprint).not.toBe(a.alerts[0]!.evidenceFingerprint);
    d.scope.workspaceId = "another-workspace";
    expect((await collectAlerts(d, fileBaseline, opts)).alerts[0]!.id).not.toBe(a.alerts[0]!.id);
  });
  test("model failures and fabricated citations cannot produce alerts", async () => {
    const d = dataset();
    for (const assess of [async () => { throw Error("secret provider body"); }, async () => ({ decision: "suggest", kind: "dependency", confidence: "high", reason: "Invented", citations: [] })]) {
      const r = await collectAlerts(d, { version: "broken", assess } as Judge, opts);
      expect(r.alerts).toHaveLength(0); expect(r.coverage.failedPairs).toBeGreaterThan(0);
      expect(JSON.stringify(r)).not.toContain("secret provider");
    }
    const c = findCandidates(d.scope, d.sessions)[0]!;
    const verdict = await fileBaseline.assess(c); verdict.citations[0]!.quote = "not in transcript";
    expect(() => validateVerdict(verdict, c)).toThrow("ungrounded_citation");
  });
  test("caps and incomplete inputs remain visible", async () => {
    const d = dataset(); d.sessions[0]!.complete = false;
    const r = await collectAlerts(d, fileBaseline, { ...opts, maxPairs: 1 });
    expect(r.coverage.sourceComplete).toBe(false); expect(r.coverage.omittedPairs).toBeGreaterThan(0);
    const output = renderAlerts(r, d);
    expect(output).toContain("Collection only"); expect(output).toContain("Source: synthetic");
  });
  test("path identity excludes absolute paths, lockfiles and traversal", () => {
    for (const p of ["/home/a/src/a.ts", "C:\\repo\\a.ts", "../a.ts", "src/../a.ts", "package-lock.json", "node_modules/x/a.ts"]) expect(fileKey(p)).toBeNull();
    expect(fileKey("src\\index.ts")).toBe("src/index.ts");
    expect(repositoryKey("git@git.example.test:team/repo.git")).toBe(repositoryKey("https://git.example.test/team/repo"));
    expect(repositoryKey("repo")).toBeNull(); expect(repositoryKey("/home/team/repo")).toBeNull();
  });
});
describe("evaluation integrity", () => {
  test("baseline measures semantic misses rather than claiming perfect detection", async () => {
    const d = dataset(); const labels = parseCases(cases, d);
    const r = await evaluate(d, labels, fileBaseline);
    expect(r.development.truePositive + r.heldOut.truePositive).toBe(1);
    expect(r.development.falseNegative + r.heldOut.falseNegative).toBe(5);
    expect(r.development.falsePositive + r.heldOut.falsePositive).toBe(0);
    expect(r.labelCounts.synthetic).toBe(12);
  });
  test("reused workstreams across development and held-out are rejected", () => {
    const labels = structuredClone(cases); labels[1]!.workstream = labels[0]!.workstream;
    expect(() => parseCases(labels, dataset())).toThrow("workstream_split_leakage");
  });
  test("positive replay labels cannot rely on evidence that arrived later", () => {
    const d = dataset(); d.sessions[0]!.evidence[0]!.availableAt = "2026-11-07T14:00:00Z";
    expect(() => parseCases(cases, d)).toThrow("unknown_or_future_label_evidence");
  });
  test("cross-user and duplicate source identities are rejected at the input boundary", () => {
    const d = dataset(); d.sessions[0]!.userId = "unselected-person";
    expect(() => parseDataset(d)).toThrow("invalid_session");
    const duplicate = dataset(); duplicate.sessions.push(duplicate.sessions[0]!);
    expect(() => parseDataset(duplicate)).toThrow("duplicate_session");
  });
});
