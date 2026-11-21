/** Synthetic cross-repository actions and adversarial sidecars; no source traces or private maps. */
import { describe, expect, test } from "bun:test";
import { Hasher } from "../import/normalize.ts";
import { actionArtifacts, actionContext, actionRecord, PROVENANCE_BOUNDS, type ActionRecord } from "../import/provenance.ts";
import { ACTION_FILE, readActionScope, resolveAction } from "../patterns/action-scope.ts";
import { analyseCorpus } from "../patterns/analyse.ts";
import { buildCorpus } from "../patterns/corpus.ts";
import { parseSession, type Snapshot, type SnapshotSession } from "../replay/snapshot.ts";
import { canonicalJson, sha256Hex } from "../replay/util.ts";
import { session } from "./helpers.ts";

const now = new Date("2026-11-02T00:00:00.000Z"), at = (m: string) => `2026-11-01T09:${m}:00.000Z`;
const hasher = new Hasher("synthetic-action-scope-test-salt");
const write = (eventId: string, ts = at("01"), path = "old/session/path.ts") => ({ eventId, ts, path, kind: "write", branch: null });
const row = (id: string, extra: Record<string, unknown> = {}) => parseSession(session(id, { repo: "wrong-final-session-repo", ...extra }), "fixture");
const fact = (s: SnapshotSession, kind: ActionRecord["kind"], f: { eventId: string; ts: string }, repo = "a", target = "src/shared.ts", observedAt = f.ts): ActionRecord => {
  const root = `/fixture/${repo}`, repository = `example.test/team/${repo}`;
  const ctx = actionContext(root, "tool_workdir", { file: 0, line: 1 }, f.ts);
  ctx.target = kind === "write" ? `${root}/${target}` : null;
  return actionRecord(ctx, { harness: s.harness, sessionId: s.sessionId, eventId: f.eventId, factAt: f.ts, kind }, hasher,
    [{ directory: root, repository, observedAt }]).record;
};
function snapshot(sessions: SnapshotSession[], records: ActionRecord[], pin = "a".repeat(64)): Snapshot {
  const sessionsHash = sha256Hex(sessions.map((s) => JSON.stringify(s)).join("\n") + "\n");
  const body = actionArtifacts([{ records, locations: {} }], "2026-11-10T00:00:00.000Z", null,
    { sessionsSha256: sessionsHash, implementationSha256: "b".repeat(64) }).body;
  return { dir: "/unused", sessions, manifestSha256: pin, extraFiles: { [ACTION_FILE]: body }, manifest: {
    format: "session-intel-snapshot/1", createdAt: now.toISOString(), deleteBy: "2026-11-10T00:00:00.000Z",
    window: { since: at("00"), until: at("30") }, files: { "sessions.jsonl": sessionsHash, [ACTION_FILE]: sha256Hex(body) },
  } };
}
function change(s: Snapshot, edit: (raw: any) => void): Snapshot {
  const copy = structuredClone(s), raw = JSON.parse(copy.extraFiles![ACTION_FILE]!);
  edit(raw); copy.extraFiles![ACTION_FILE] = JSON.stringify(raw); copy.manifest.files[ACTION_FILE] = sha256Hex(copy.extraFiles![ACTION_FILE]!);
  return copy;
}
const analyse = (s: Snapshot) => analyseCorpus(buildCorpus([s], { actionScope: true, now }));

describe("manifest-bound action scope", () => {
  test("real capture output binds to fact kind, timestamp, session and exact sessions bytes", () => {
    const s = row("a", { writes: [write("w1")] }), r = fact(s, "write", s.writes[0]!);
    const input = snapshot([s], [r]);
    expect(readActionScope(input, now).size).toBe(1);
    const tampered = structuredClone(input); tampered.extraFiles![ACTION_FILE] += " ";
    expect(() => readActionScope(tampered, now)).toThrow("values withheld");
    for (const edit of [
      (raw: any) => { raw.sessionsSha256 = "c".repeat(64); },
      (raw: any) => { raw.records[0].kind = "commit"; },
      (raw: any) => { raw.records[0].factAt = at("02"); },
      (raw: any) => { raw.records[0].sessionId = "another"; },
      (raw: any) => { raw.records.push(raw.records[0]); },
      (raw: any) => { raw.counts.records++; },
    ]) expect(() => readActionScope(change(input, edit), now)).toThrow("values withheld");
  });
  test("unknown fields, raw private locations, unsupported rules, malformed times and identities are refused without values", () => {
    const s = row("a", { writes: [write("w1")] }), input = snapshot([s], [fact(s, "write", s.writes[0]!)]);
    for (const edit of [
      (r: any) => { r.locations = { private: "PRIVATE VALUE" }; },
      (r: any) => { r.records[0].directory = "PRIVATE VALUE"; },
      (r: any) => { r.records[0].target = "/PRIVATE/VALUE"; },
      (r: any) => { r.rules = "unknown"; },
      (r: any) => { r.bounds.actions++; },
      (r: any) => { r.records[0].availableAt = "yesterday"; },
      (r: any) => { r.records[0].availableAt = at("00"); },
      (r: any) => { r.records[0].repository = "example.test/raw/repo"; },
      (r: any) => { r.records[0].repositoryClaims = [{ repository: "r_" + "a".repeat(24), basis: "operator_declared", availableAt: at("01") }]; },
      (r: any) => { r.records[0].reason = "projection_scope_unavailable"; },
      (r: any) => { r.deleteBy = "2026-11-11T00:00:00.000Z"; },
      (r: any) => { r.deleteBy = "2026-11-01T00:00:00.000Z"; },
    ]) {
      const bad = change(input, edit);
      expect(() => readActionScope(bad, now)).toThrow("values withheld");
      expect(() => readActionScope(bad, now)).not.toThrow("PRIVATE VALUE");
    }
  });
  test("unbounded or missing public sidecars fail closed, no private reverse map or session fallback", () => {
    const s = row("a"), input = snapshot([s], []);
    delete input.extraFiles![ACTION_FILE];
    expect(() => buildCorpus([input], { actionScope: true, now })).toThrow("manifest-bound");
    expect(buildCorpus([input], { now }).sessions).toHaveLength(1);
    input.extraFiles![ACTION_FILE] = " ".repeat(PROVENANCE_BOUNDS.exportBytes + 1);
    input.manifest.files[ACTION_FILE] = sha256Hex(input.extraFiles![ACTION_FILE]!);
    expect(() => readActionScope(input, now)).toThrow("values withheld");
  });
  test("repository-basis guards reject inconsistent records even with recomputed valid counts", () => {
    const s = row("a", { writes: [write("w1")] }), valid = fact(s, "write", s.writes[0]!);
    const a = valid.repository!, b = hasher.repo("example.test/team/b");
    const claims = [
      { repository: a, basis: "operator_declared" as const, availableAt: at("01") },
      { repository: b, basis: "trace_declared" as const, availableAt: at("01") },
    ];
    const negatives: ActionRecord[] = [
      { ...valid, repositoryBasis: "unattributed", repositoryClaims: [], repositoryTarget: null },
      { ...valid, repositoryBasis: "unattributed", repository: null, repositoryClaims: [claims[0]!], repositoryTarget: null },
      { ...valid, repositoryBasis: "conflicting_claims", repositoryClaims: claims, repositoryTarget: null },
      { ...valid, repositoryBasis: "conflicting_claims", repository: null, repositoryClaims: [claims[0]!], repositoryTarget: null },
      { ...valid, repositoryBasis: "conflicting_claims", repository: null, repositoryClaims: [claims[0]!, { ...claims[1]!, repository: a }], repositoryTarget: null },
    ];
    for (const bad of negatives) expect(() => readActionScope(snapshot([s], [bad]), now)).toThrow("values withheld");
    // Controls: the same public shape with consistent bases does load.
    expect(readActionScope(snapshot([s], [{ ...valid, repositoryBasis: "unattributed", repository: null, repositoryClaims: [], repositoryTarget: null }]), now).size).toBe(1);
    expect(readActionScope(snapshot([s], [{ ...valid, repositoryBasis: "conflicting_claims", repository: null, repositoryClaims: claims, repositoryTarget: null }]), now).size).toBe(1);
  });
  test("each availability guard rejects independently with otherwise consistent clocks and counts", () => {
    const s = row("a", { writes: [write("w1", at("02"))] }), valid = fact(s, "write", s.writes[0]!);
    const earlyClaim = { ...valid.repositoryClaims[0]!, availableAt: at("00") };
    const negatives: ActionRecord[] = [
      // availableAt < factAt only: source and claim are already visible.
      { ...valid, availableAt: at("01"), sourceAt: at("00"), repositoryClaims: [earlyClaim] },
      // availableAt < sourceAt only: fact and claim are already visible.
      { ...valid, availableAt: at("03"), sourceAt: at("04"), repositoryClaims: [earlyClaim] },
      // claim later than availableAt only: source and fact are already visible.
      { ...valid, repositoryClaims: [{ ...earlyClaim, availableAt: at("03") }] },
    ];
    for (const bad of negatives) expect(() => readActionScope(snapshot([s], [bad]), now)).toThrow("values withheld");
    expect(readActionScope(snapshot([s], [valid]), now).size).toBe(1);
  });
  test("ambiguous source facts cannot be joined by event id alone", () => {
    const s = row("a", { writes: [write("one"), write("one", at("01"), "different/path.ts")] });
    expect(() => readActionScope(snapshot([s], [fact(s, "write", s.writes[0]!)]), now)).toThrow("values withheld");
  });
  test("duplicate sessions require equal action provenance too, independent of record order", () => {
    const a = row("a", { writes: [write("w1"), write("w2", at("02"))] }), rs = a.writes.map((f) => fact(a, "write", f));
    const first = snapshot([a], rs), second = snapshot([a], [...rs].reverse(), "c".repeat(64));
    expect(buildCorpus([first, second], { actionScope: true, sameIdentitySpace: true, now }).duplicateSessions).toBe(1);
    expect(() => buildCorpus([first, snapshot([a], [], "d".repeat(64))], { actionScope: true, sameIdentitySpace: true, now })).toThrow("duplicate action provenance");
    expect(() => buildCorpus([first, snapshot([a], a.writes.map((f) => fact(a, "write", f, "b")), "d".repeat(64))], { actionScope: true, sameIdentitySpace: true, now })).toThrow("duplicate action provenance");
  });
});

describe("per-action retrospective analysis", () => {
  test("one original session can work in multiple repositories without splitting its identity", () => {
    const a = row("a", { writes: [write("a1"), write("a2")], parentSessionIds: ["parent"] });
    const b = row("b", { repo: "different-final-repo", writes: [write("b1", at("01"), "different/legacy/path.ts")], parentSessionIds: ["parent"] });
    const r = analyse(snapshot([a, b], [fact(a, "write", a.writes[0]!, "a"), fact(a, "write", a.writes[1]!, "b"), fact(b, "write", b.writes[0]!, "b")]));
    expect(r.format).toBe("session-intel-patterns/2");
    expect(r.candidates).toHaveLength(1);
    expect(r.candidates[0]).toMatchObject({ parentLink: "shared_parent", branchRelation: "unknown", participants: [{ harness: "claude", sessionId: "a" }, { harness: "claude", sessionId: "b" }] });
    expect(r.profile.byRepository.map((p) => p.sessions).sort()).toEqual([1, 2]);
    expect(r.profile.all).toMatchObject({ sessions: 2, writes: 3 });
    expect(JSON.stringify(r.input)).not.toContain("repositoryClaims");
    expect(r.candidates[0]!.evidence[0]!.b.path).toBe("different/legacy/path.ts");
  });
  test("equal legacy scope/path in different action repos never forms a pair", () => {
    const a = row("a", { writes: [write("a1")] }), b = row("b", { writes: [write("b1")] });
    const input = snapshot([a, b], [fact(a, "write", a.writes[0]!, "a"), fact(b, "write", b.writes[0]!, "b")]);
    expect(analyse(input).candidates).toHaveLength(0);
    expect(analyseCorpus(buildCorpus([input], { now })).candidates).toHaveLength(1);
  });
  test("missing, conflict, unattributed projection and missing target stay unknown with full coverage", () => {
    const s = row("a", { writes: ["missing", "conflict", "projection", "no-target"].map((id) => write(id)) });
    const ctx = actionContext("/fixture/a", "tool_workdir", { file: 0, line: 1 }, at("01"), { repository: "example.test/team/b", root: "/fixture/a", observedAt: at("01") });
    const conflict = actionRecord(ctx, { harness: s.harness, sessionId: s.sessionId, eventId: "conflict", kind: "write", factAt: at("01") }, hasher,
      [{ directory: "/fixture/a", repository: "example.test/team/a", observedAt: at("01") }]).record;
    const projection = actionRecord(null, { harness: s.harness, sessionId: s.sessionId, eventId: "projection", kind: "write", factAt: at("01") }, hasher).record;
    projection.source = "code_mode_projection"; projection.reason = "projection_scope_unavailable";
    const noTarget = fact(s, "write", s.writes[3]!); noTarget.repositoryTarget = null;
    const r = analyse(snapshot([s], [conflict, projection, noTarget]));
    expect(r.coverage.actionScope).toMatchObject({ records: 3, missing: 1, conflicting: 1, unattributed: 1, writesWithoutTarget: 4 });
    expect(r.profile.all.writes).toBe(4); expect(r.profile.files).toEqual([]);
    expect(r.profile.byRepository.reduce((n, p) => n + p.writes, 0)).toBe(4);
    expect(r.profile.byRepository.map((p) => p.key).sort()).toEqual(["unscoped", JSON.stringify({ kind: "repo", value: noTarget.repository })].sort());
    expect(JSON.stringify(r.profile)).not.toContain(s.repo!);
  });
  test("late declarations are descriptive only; causal access rejects equality and earlier clocks", () => {
    const a = row("a", { writes: [write("a1")] }), b = row("b", { writes: [write("b1")] });
    const input = snapshot([a, b], [fact(a, "write", a.writes[0]!, "a", "src/shared.ts", at("20")), fact(b, "write", b.writes[0]!, "a")]);
    const scope = readActionScope(input, now), f = a.writes[0]!;
    expect(resolveAction(scope, a, "write", f, new Date(at("02"))).status).toBe("not_available");
    expect(resolveAction(scope, a, "write", f, new Date(at("20"))).repository).toBeNull();
    expect(resolveAction(scope, a, "write", f, new Date(at("02"))).availableAt).toBeNull();
    expect(resolveAction(scope, a, "write", f, new Date(at("20"))).availableAt).toBeNull();
    expect(resolveAction(scope, a, "write", f, new Date(Date.parse(at("20")) + 1)).status).toBe("attributed");
    expect(analyse(input).coverage.actionScope!.late).toBe(1);
    expect(analyse(input).candidates).toHaveLength(1);
    expect(analyse(input).candidates[0]!.firstObservedAt).toBe(at("20"));
    expect(analyse(input).candidates[0]!.evidence[0]!.a).toMatchObject({ at: at("01"), availableAt: at("20") });
    expect(analyse(input).limitations.some((l) => l.includes("no early-warning credit"))).toBe(true);
  });
  test("commit/outcome scope is action-specific; claims and summaries are never assigned to the final repo", () => {
    const s = row("a", { commits: [{ eventId: "c", ts: at("02"), sha: "abcdef12" }], outcomes: [{ eventId: "o", ts: at("03"), key: "abcdef12", reverted: true }],
      claims: [{ at: at("01"), kind: "intent", text: "private fixture text", confidence: 1, evidenceIds: [] }], shortSummary: "private summary" });
    const r = analyse(snapshot([s], [fact(s, "commit", s.commits[0]!, "a"), fact(s, "outcome", s.outcomes[0]!, "b")]));
    expect(r.profile.byRepository).toHaveLength(2);
    expect(r.profile.byRepository.reduce((n, p) => n + p.commits + p.outcomes, 0)).toBe(2);
    expect(r.profile.byRepository.every((p) => !p.claims && !p.sessionsWithSummaries)).toBe(true);
    expect(r.profile.all.claims).toBe(1); expect(r.coverage.actionScope!.claimsWithoutActionScope).toBe(1);
  });
  test("record order does not change the report and nearby cutoff +1 ms is excluded", () => {
    const a = row("a", { writes: [write("a1")] }), b = row("b", { writes: [write("b1", "2026-11-01T09:02:00.001Z")] });
    const records = [fact(a, "write", a.writes[0]!), fact(b, "write", b.writes[0]!)];
    const c = buildCorpus([snapshot([a, b], records)], { actionScope: true, now });
    const r = analyseCorpus(c, { nearbyMs: 60_000 });
    expect(r.candidates).toHaveLength(0);
    expect(canonicalJson(analyseCorpus(buildCorpus([snapshot([a, b], [...records].reverse())], { actionScope: true, now }), { nearbyMs: 60_000 }))).toBe(canonicalJson(r));
  });
});
