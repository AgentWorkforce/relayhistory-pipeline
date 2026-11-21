/** Real pure T1/T2 detector, synthetic /2 capture, no model, database or runtime delivery. */
import { expect, test } from "bun:test";
import { SessionBuilder, Hasher, emptyStats, toSnapshotSession } from "../import/normalize.ts";
import { actionArtifacts, actionContext, type RepositoryRoot } from "../import/provenance.ts";
import { actionScopedDetector, ACTION_SCOPED_DETECTOR } from "../replay/action-scoped.ts";
import { ActionFrame, ACTION_FRAME_BOUNDS } from "../replay/action-frame.ts";
import { replaySnapshot } from "../replay/replay.ts";
import { assertScorable } from "../replay/score.ts";
import type { Snapshot } from "../replay/snapshot.ts";
import { ReplayView } from "../replay/views.ts";
import { canonicalJson, sha256Hex } from "../replay/util.ts";

const now = new Date("2026-11-02T00:00:00.000Z"), at = (m: number) => new Date(Date.parse("2026-11-01T09:00:00Z") + m * 60_000).toISOString();
const h = new Hasher("synthetic-action-detector-salt");
interface Event { repo: string; minute?: number; known?: number; path?: string; commit?: string; revert?: string }
function row(id: string, events: Event[], parents: string[] = []) {
  const b = new SessionBuilder("codex", id, emptyStats(), true);
  b.cwd = "/fixture/wrong-final-repo";
  b.touch(at(0), { file: 0, line: 1 });
  for (const p of parents) b.parent(p);
  for (const [i, e] of events.entries()) {
    const ts = at(e.minute ?? 1), line = { file: 0, line: i + 2 };
    const ctx = actionContext(`/fixture/${e.repo}`, "tool_workdir", line, at(e.known ?? e.minute ?? 1));
    if (e.path) b.write(e.path, ts, line, ctx.directory, "old-final-branch", "write", ctx);
    if (e.commit) b.commit(e.commit, ts, line, ctx);
    if (e.revert) b.outcome(e.revert, true, ts, line, ctx);
  }
  return b;
}
function snapshot(builders: SessionBuilder[], roots: string[] = ["a", "b"], until = 30): Snapshot {
  const map: RepositoryRoot[] = roots.map((repo) => ({ directory: `/fixture/${repo}`, repository: `example.test/team/${repo}`, observedAt: at(0) }));
  const rows = builders.map((b) => toSnapshotSession(b, h, { orgId: "fixture", workspaceId: "fixture" }, map, { repositoryPaths: true })!);
  const sessionsHash = sha256Hex(rows.map((r) => JSON.stringify(r.line)).join("\n") + "\n");
  const body = actionArtifacts(rows.map((r) => r.provenance!), "2026-11-10T00:00:00.000Z", null, { sessionsSha256: sessionsHash, implementationSha256: "a".repeat(64) }, { repositoryPaths: true }).body;
  return { dir: "/unused", manifestSha256: "b".repeat(64), sessions: rows.map((r) => r.session), extraFiles: { "action-provenance.json": body }, manifest: {
    format: "session-intel-snapshot/1", createdAt: now.toISOString(), deleteBy: "2026-11-10T00:00:00.000Z", window: { since: at(0), until: at(until) }, files: { "sessions.jsonl": sessionsHash, "action-provenance.json": sha256Hex(body) },
  } };
}
const migration = (repo: string, suffix: string, extra: Partial<Event> = {}): Event => ({ repo, path: `supabase/migrations/0042_${suffix}.sql`, ...extra });
const detector = (options: Parameters<typeof actionScopedDetector>[0] = {}) => actionScopedDetector({ now, ...options });

test("one original session can alert in two repositories without collapsing lane IDs", async () => {
  const s = snapshot([row("one", [migration("a", "one"), migration("b", "one")]), row("two", [migration("a", "two")]), row("three", [migration("b", "three")])]);
  const before = canonicalJson(s.sessions), result = await replaySnapshot(s, { detectors: [detector()], intervalMs: 60_000 });
  expect(result.alerts).toHaveLength(2);
  expect(result.incomplete).toEqual([]);
  expect(result.alerts.every((a) => a.detector === ACTION_SCOPED_DETECTOR && a.kind === "duplicate_work" && a.channel === "pull")).toBe(true);
  expect(result.alerts.every((a) => a.pair.includes(`codex:${h.session("codex", "one")}`))).toBe(true);
  expect(new Set(result.alerts.map((a) => a.alertId)).size).toBe(2);
  expect(canonicalJson(s.sessions)).toBe(before);
});
test("fact and scope equality stay hidden; a late declaration does not backdate an alert", async () => {
  const s = snapshot([row("one", [migration("a", "one", { known: 10 })]), row("two", [migration("a", "two")])]);
  const view = new ReplayView(s), d = detector();
  expect(await d.run(view, new Date(at(1)))).toEqual([]);
  expect(await d.run(view, new Date(at(10)))).toEqual([]);
  const alerts = await d.run(view, new Date(Date.parse(at(10)) + 1));
  expect(alerts).toHaveLength(1); expect(alerts[0]!.alertAt).toBe(at(10));
  expect(alerts[0]!.sentAt).toBe(new Date(Date.parse(at(10)) + 1).toISOString());
  expect(alerts[0]!.evidence.every((e) => e.ts === at(1))).toBe(true);
  expect(await d.run(view, new Date(at(11)))).toEqual([]);
});
test("equal migration numbers in different action repositories do not become a pair", async () => {
  const result = await replaySnapshot(snapshot([row("one", [migration("a", "one")]), row("two", [migration("b", "two")])]), { detectors: [detector()] });
  expect(result.alerts).toEqual([]); expect(result.incomplete).toEqual([]);
});
test("unknown mapped scope does not inherit session metadata and blocks held-out completeness", async () => {
  const result = await replaySnapshot(snapshot([row("one", [migration("unknown", "one")]), row("two", [migration("unknown", "two")])]), { detectors: [detector()] });
  expect(result.alerts).toEqual([]);
  expect(result.detectors[0]!.diagnostics.missingScope).toBe(2);
  expect(result.incomplete).toContain(`${ACTION_SCOPED_DETECTOR} rejected=2`);
  expect(() => assertScorable(result, "heldout")).toThrow("incomplete");
});
test("T2 commit and revert match only within their declared repository, after both are available", async () => {
  const s = snapshot([row("one", [{ repo: "a", commit: "abcdef12", known: 5 }]), row("two", [{ repo: "a", revert: "abcdef12", minute: 2 }]), row("other", [{ repo: "b", revert: "abcdef12", minute: 2 }])]);
  const result = await replaySnapshot(s, { detectors: [detector()], intervalMs: 60_000 });
  expect(result.alerts).toHaveLength(1); expect(result.alerts[0]!.kind).toBe("superseded_work");
  expect(result.alerts[0]!.sentAt).toBe(at(6)); expect(result.alerts[0]!.alertAt).toBe(at(5));
  expect(result.alerts[0]!.pair).not.toContain(`codex:${h.session("codex", "other")}`);
});
test("parent links through metadata-only ancestors in another repository still suppress fleets", async () => {
  const s = snapshot([row("root", []), row("middle", [], ["root"]), row("one", [migration("a", "one")], ["middle"]), row("two", [migration("a", "two")], ["root"])]);
  const result = await replaySnapshot(s, { detectors: [detector()] });
  expect(result.alerts).toEqual([]); expect(result.detectors[0]!.diagnostics.skippedFleet).toBeGreaterThan(0);
});
test("per-pair cooldown and per-recipient caps are shared across repository partitions", async () => {
  const s = snapshot([row("one", [migration("a", "one"), migration("b", "one")]), row("two", [migration("a", "two"), migration("b", "two")])], ["a", "b"], 70);
  const result = await replaySnapshot(s, { detectors: [detector({ pairCooldownMs: 60 * 60_000, sessionHourlyCap: 1 })], intervalMs: 60_000 });
  expect(result.alerts).toHaveLength(2);
  expect(Date.parse(result.alerts[1]!.sentAt) - Date.parse(result.alerts[0]!.sentAt)).toBe(60 * 60_000);
});
test("each global limiter works independently across repositories", async () => {
  const s = snapshot([row("one", [migration("a", "one"), migration("b", "one")]), row("two", [migration("a", "two"), migration("b", "two")])], ["a", "b"], 70);
  for (const options of [{ pairCooldownMs: 60 * 60_000, sessionHourlyCap: 100 }, { pairCooldownMs: 1, sessionHourlyCap: 1 }]) {
    const result = await replaySnapshot(s, { detectors: [detector(options)], intervalMs: 60_000 });
    expect(result.alerts).toHaveLength(2);
    expect(Date.parse(result.alerts[1]!.sentAt) - Date.parse(result.alerts[0]!.sentAt)).toBe(60 * 60_000);
  }
});
test("partition lanes exclude session and write branch, title, usage and claim metadata", () => {
  const s = snapshot([row("one", [migration("a", "one")])]);
  const original = s.sessions[0]!;
  original.branch = "session-final-branch";
  const frame = new ActionFrame(s, now), lanes = [...frame.advance(new Date(at(2))).values()].flat();
  expect(lanes).toHaveLength(1);
  expect(lanes[0]).toMatchObject({ branch: null, taskRef: null, title: null, userId: null, usage: [] });
  expect(lanes[0]!.writes[0]!.branch).toBeNull();
  expect(lanes[0]).not.toHaveProperty("claims");
});
test("missing semantic paths and unknown commit identities are explicit gaps", async () => {
  const s = snapshot([row("one", [{ repo: "a", commit: "unknown" }, { repo: "a", revert: "unknown" }, migration("a", "one")])]);
  const sidecar = JSON.parse(s.extraFiles!["action-provenance.json"]!);
  sidecar.records.find((r: { kind: string }) => r.kind === "write").repositoryPath = null;
  const body = JSON.stringify(sidecar);
  s.extraFiles!["action-provenance.json"] = body; s.manifest.files["action-provenance.json"] = sha256Hex(body);
  const result = await replaySnapshot(s, { detectors: [detector()] });
  expect(result.alerts).toEqual([]);
  expect(result.detectors[0]!.diagnostics).toMatchObject({ rejected: 3, missingPath: 1, unkeyedCommits: 2 });
});
test("parent traversal handles cycles and refuses to guess after its distinct-ancestor cap", async () => {
  const cyclic = snapshot([row("one", [migration("a", "one")], ["two"]), row("two", [migration("a", "two")], ["one"])]);
  expect((await replaySnapshot(cyclic, { detectors: [detector()] })).alerts).toEqual([]);
  const ancestors = Array.from({ length: ACTION_FRAME_BOUNDS.ancestors + 1 }, (_, i) => row(`parent-${i}`, [], i ? [`parent-${i - 1}`] : []));
  const s = snapshot([...ancestors, row("one", [migration("a", "one")], [`parent-${ancestors.length - 1}`]), row("two", [migration("a", "two")])]);
  const result = await replaySnapshot(s, { detectors: [detector()] });
  expect(result.alerts).toEqual([]); expect(result.incomplete).toContain(`${ACTION_SCOPED_DETECTOR} rejected=1`);
});
test("retention, tenant and conflicting duplicate checks run before partition indexing", () => {
  const s = snapshot([row("one", [migration("a", "one")]), row("two", [migration("a", "two")])]);
  expect(() => new ActionFrame(s, new Date("2026-11-11T00:00:00.000Z"))).toThrow("deleteBy");
  s.sessions[1]!.workspaceId = "other-fixture";
  expect(() => new ActionFrame(s, now)).toThrow("one tenant");
  s.sessions[1]!.workspaceId = s.sessions[0]!.workspaceId;
  s.sessions.push({ ...s.sessions[0]!, branch: "conflicting-duplicate" });
  expect(() => new ActionFrame(s, now)).toThrow("conflicting duplicate");
});
test("work and per-repository fact caps fail closed with explicit incomplete diagnostics", async () => {
  const s = snapshot([row("one", [migration("a", "one")]), row("two", [migration("a", "two")])]);
  const result = await replaySnapshot(s, { detectors: [detector({ workBudget: 1 })] });
  expect(result.alerts).toEqual([]); expect(result.incomplete.length).toBeGreaterThan(0);
  const many = snapshot([row("one", Array.from({ length: ACTION_FRAME_BOUNDS.repositoryFacts + 1 }, (_, i) => ({ repo: "a", path: `src/file-${i}.ts` }))) ]);
  const frame = new ActionFrame(many, now);
  expect(frame.advance(new Date(at(2))).size).toBe(0); expect(frame.exhausted).toBe(true); expect(frame.stats.rejected).toBeGreaterThan(0);
});
test("unchanged ticks avoid rerunning quadratic detection; input order does not change alerts", async () => {
  const rows = [row("one", [migration("a", "one")]), row("two", [migration("a", "two")])];
  const s = snapshot(rows), d = detector(), view = new ReplayView(s);
  const first = await d.run(view, new Date(at(2))), work = d.diagnostics!(view).work;
  expect(await d.run(view, new Date(at(3)))).toEqual([]); expect(d.diagnostics!(view).work).toBe(work);
  const other = await replaySnapshot(snapshot([...rows].reverse()), { detectors: [detector()], intervalMs: 60_000 });
  expect(other.alerts).toEqual(first);
  await expect(d.run(view, new Date(at(0)))).rejects.toThrow("backwards");
});
