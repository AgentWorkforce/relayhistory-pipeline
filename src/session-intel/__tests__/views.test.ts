/** The as-of views: nothing from the future is visible, and the store mirrors the 062000/062070 semantics. */
import { describe, expect, test } from "bun:test";
import type { AdvisoryRowInput } from "../../coordination-advisor/scan.ts";
import { loadSnapshot } from "../replay/snapshot.ts";
import { activityAsOf, asOfSession, ReplayAdvisorStore, ReplayView, snapshotCoordinationBackend, snapshotDirectory, summaryAsOf, type SimClock } from "../replay/views.ts";
import { at, FIXTURE_DIR, session, tmp, writeSnapshot } from "./helpers.ts";

const leakSession = session("s1", {
  createdAt: at("10:00"), lastActivityAt: at("12:00"),
  writes: [{ path: "src/a/x.ts", ts: at("10:30"), eventId: "w1", kind: "tool" }, { path: "src/a/y.ts", ts: at("11:30"), eventId: "w2", kind: "tool" }],
  shortSummary: { timeline: [{ at: at("10:05"), text: "first" }, { at: at("11:00"), text: "second" }] },
});

async function view(sessions: unknown[]) {
  return new ReplayView(await loadSnapshot(writeSnapshot(tmp(), sessions)));
}

describe("as-of session", () => {
  test("before createdAt the session does not exist; facts at or after now are invisible", async () => {
    const v = await view([leakSession]);
    const s = v.snapshot.sessions[0]!;
    expect(asOfSession(s, new Date(at("09:59")))).toBeNull();
    const early = asOfSession(s, new Date(at("10:30")))!;
    expect(early.writes).toEqual([]); // ts == now is not yet visible (production reads ts < until)
    expect(early.lastActivityAt).toBe(at("10:00"));
    const mid = asOfSession(s, new Date(at("11:00")))!;
    expect(mid.writes.map((w) => w.eventId)).toEqual(["w1"]);
    expect(mid.lastActivityAt).toBe(at("10:30"));
    expect(asOfSession(s, new Date(at("12:00")))!.lastActivityAt).toBe(at("12:00"));
  });

  test("the short summary is the latest timeline entry at or before now", async () => {
    const s = (await view([leakSession])).snapshot.sessions[0]!;
    expect(summaryAsOf(s, at("10:04"))).toBeNull();
    expect(summaryAsOf(s, at("10:05"))).toBe("first");
    expect(summaryAsOf(s, at("10:59"))).toBe("first");
    expect(summaryAsOf(s, at("11:00"))).toBe("second");
    expect(activityAsOf(s, at("11:31"))).toBe(at("11:30"));
  });

  test("candidates: only created, active-in-window sessions with a project, with their as-of summary", async () => {
    const v = await view([
      leakSession,
      session("late", { createdAt: at("11:00"), lastActivityAt: at("11:30") }),
      session("noproject", { projectId: null }),
      session("other-ws", { workspaceId: "ws_other" }),
    ]);
    const clock: SimClock = { now: new Date(at("10:45")) };
    const store = new ReplayAdvisorStore(v, clock);
    const c = await store.candidates(new Date(at("08:45")), 100, null);
    expect(c.map((x) => x.sessionId).sort()).toEqual(["other-ws", "s1"]);
    expect(c.find((x) => x.sessionId === "s1")).toMatchObject({ shortSummary: "first", lastActivityAt: at("10:30") });
    expect((await store.candidates(new Date(at("08:45")), 100, ["ws_fixture"])).map((x) => x.sessionId)).toEqual(["s1"]);
  });

  test("the backend never returns a fact at or after the clock, even when asked for a later until", async () => {
    const v = await view([leakSession, session("s2", { writes: [{ path: "src/a/x.ts", ts: at("11:45"), eventId: "w3", kind: "tool" }] })]);
    const clock: SimClock = { now: new Date(at("11:00")) };
    const facts = await snapshotCoordinationBackend(v, clock).readFacts(snapshotDirectory(v, clock),
      { orgId: "org_fixture", workspaceId: "ws_fixture", since: new Date(at("09:00")), until: new Date(at("13:00")) });
    const writes = facts.lanes.flatMap((l) => l.writes.map((w) => w.eventId));
    expect(writes).toEqual(["w1"]);
    expect(facts.lanes.map((l) => l.sessionId)).toEqual(["s1"]);
    // The directory row carries the as-of activity, not the final one.
    expect(facts.lanes[0]!.lastActionAt).toBe(at("10:30"));
    const other = await snapshotCoordinationBackend(v, clock).readFacts(snapshotDirectory(v, clock),
      { orgId: "org_fixture", workspaceId: "ws_other", since: new Date(at("09:00")), until: new Date(at("11:00")) });
    expect(other.lanes).toEqual([]);
  });

  test("the fixture's future facts stay invisible early", async () => {
    const v = new ReplayView(await loadSnapshot(FIXTURE_DIR));
    const h = v.snapshot.sessions.find((s) => s.sessionId === "sess-h")!;
    const before = asOfSession(h, new Date(at("11:58")))!;
    expect(before.shortSummary).toBe("update the marketing copy on the landing page");
    expect(before.writes.map((w) => w.path)).toEqual(["web/landing/copy.md"]);
    expect(asOfSession(h, new Date(at("12:31")))!.writes.map((w) => w.path)).toEqual(["web/landing/copy.md", "src/leak/retry.ts"]);
  });
});

const row = (over: Partial<AdvisoryRowInput> = {}): AdvisoryRowInput => ({
  advisory_id: "adv1", harness: "claude", session_id: "a", peer_harness: "claude", peer_session_id: "b",
  urgency: "low", kind: "nearby_work", artifact: "src/a", evidence: [], ...over,
});

describe("ReplayAdvisorStore", () => {
  test("upsert: insert, same key again is existing, LOW to HIGH upgrades, acked and other tenants are skipped", async () => {
    const clock: SimClock = { now: new Date(at("10:00")) };
    const store = new ReplayAdvisorStore(await view([leakSession]), clock);
    expect(await store.upsert("o", "w", row())).toEqual({ inserted: 1, upgraded: 0, skipped: 0 });
    expect(await store.upsert("o", "w", row())).toEqual({ inserted: 0, upgraded: 0, skipped: 1 });
    expect(await store.upsert("o", "w2", row({ urgency: "high" }))).toEqual({ inserted: 0, upgraded: 0, skipped: 1 });
    expect(await store.upsert("o", "w", row({ urgency: "high" }))).toEqual({ inserted: 0, upgraded: 1, skipped: 0 });
    expect(await store.upsert("o", "w", row({ urgency: "low" }))).toEqual({ inserted: 0, upgraded: 0, skipped: 1 }); // never downgraded
    expect(store.rows.get("adv1\u0000claude\u0000a")!.row.urgency).toBe("high");
    expect(await store.markDelivered("o", "w", "adv1", { harness: "claude", sessionId: "a" })).toBe(true);
    expect(store.rows.get("adv1\u0000claude\u0000a")!.status).toBe("delivered");
    expect(await store.markDelivered("o", "w2", "adv1", { harness: "claude", sessionId: "a" })).toBe(false);
    store.rows.get("adv1\u0000claude\u0000a")!.status = "acked";
    expect(await store.upsert("o", "w", row({ advisory_id: "adv1", urgency: "high" }))).toEqual({ inserted: 0, upgraded: 0, skipped: 1 });
    expect(store.writes.map((w) => w.result)).toEqual(["inserted", "skipped", "skipped", "upgraded", "skipped", "skipped"]);
  });

  test("upsert refuses what the table refuses: over-long artifact (23514) and more than 10 evidence entries (22023)", async () => {
    const store = new ReplayAdvisorStore(await view([leakSession]), { now: new Date(at("10:00")) });
    await expect(store.upsert("o", "w", row({ artifact: "x".repeat(513) }))).rejects.toMatchObject({ code: "23514" });
    const ev = { sessionId: "a", eventId: "e", ts: at("10:00"), kind: "k" };
    await expect(store.upsert("o", "w", row({ evidence: Array(11).fill(ev) }))).rejects.toMatchObject({ code: "22023" });
  });

  test("recent derives the pair and recipient rows by created_at, per tenant", async () => {
    const clock: SimClock = { now: new Date(at("10:00")) };
    const store = new ReplayAdvisorStore(await view([leakSession]), clock);
    await store.upsert("o", "w", row());
    clock.now = new Date(at("10:30"));
    await store.upsert("o", "w", row({ advisory_id: "adv2" }));
    await store.upsert("o", "w", row({ advisory_id: "adv3", session_id: "c" }));
    clock.now = new Date(at("11:15"));
    const recent = await store.recent("o", "w", new Date(at("05:00")));
    expect(recent.filter((r) => r.kind === "pair").map((r) => [r.recipient.sessionId, r.latestAt])).toEqual([
      ["a", Date.parse(at("10:30"))], ["c", Date.parse(at("10:30"))],
    ]);
    // Only the 10:30 rows are inside the last hour.
    expect(recent.filter((r) => r.kind === "recipient")).toEqual([
      { kind: "recipient", recipient: { harness: "claude", sessionId: "a" }, latestAt: Date.parse(at("10:30")), sentLastHour: 1 },
      { kind: "recipient", recipient: { harness: "claude", sessionId: "c" }, latestAt: Date.parse(at("10:30")), sentLastHour: 1 },
    ]);
    expect(await store.recent("o", "other", new Date(at("05:00")))).toEqual([]);
  });
});
