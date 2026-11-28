/**
 * T130 slice 1(c): persisted consumed-record membership (slot 062510). The records are fed through
 * REAL generation (the worker builds the input from manifests and calls the model); membership is
 * answered by sessions.session_brief_membership_for through the read client, one record at a time,
 * against the served brief and its live binding.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import type { ManifestV1 } from "../../shared/vendor.ts";
import { armWakeup, briefSourceBinding, livePointers, recordBrief } from "../store.ts";
import { briefTick } from "../worker.ts";
import { FakeModel, KEY, database, dueNow, hoopDb, indexedFrom, mapReader, pointer, read, tempStore, testConfig, transcript } from "./harness.ts";
import { TENANT_SETTINGS_SQL, type TenantRunner } from "../../session-read-api/liveness.ts";
import { BriefSummaryError, briefMembershipResponse, sqlBriefSummary } from "../../session-read-api/brief-summary.ts";

setDefaultTimeout(60_000);
const BINDING = expect.stringMatching(/^[0-9a-f]{64}$/);
const open: PGlite[] = [];
afterEach(async () => { await Promise.all(open.splice(0).map((db) => db.close())); });
const ROLE = read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n");

const line = (uuid: string, role: "user" | "assistant", text: string, ts: string) =>
  role === "user"
    ? { type: "user", uuid, timestamp: ts, message: { role: "user", content: text } }
    : { type: "assistant", uuid, timestamp: ts, message: { role: "assistant", model: "claude-x", content: [{ type: "text", text }] } };
const TOOL = { type: "user", uuid: "tool-1", timestamp: "2026-10-29T10:00:03.000Z", message: { role: "user", content: [{ type: "tool_result", content: "TOOL-OUTPUT" }] } };
const U1 = line("u1", "user", "please fix the flaky queue test", "2026-10-29T10:00:01.000Z");
const A1 = line("a1", "assistant", "Looking at the lease renewal.", "2026-10-29T10:00:02.000Z");
// E2 is appended in revision 2 but BACKDATED before A1.
const E2 = line("e2", "user", "also check the retry path", "2026-10-29T10:00:01.500Z");

async function setup(membership = true) {
  const pg = await database(); open.push(pg);
  await pg.exec(ROLE);
  const store = tempStore(); const manifests = new Map<string, ManifestV1>(); const model = new FakeModel();
  const upload = async (lines: unknown[], revision: number) => {
    const manifestKey = `manifest/${KEY.sessionId}/m-1/r${revision}`;
    manifests.set(manifestKey, await transcript(store, lines, { revision }));
    await pointer(pg, { revision, manifestKey, ...indexedFrom(manifests.get(manifestKey)!) });
    await armWakeup(hoopDb(pg), KEY, { idleSeconds: 600, maxWaitSeconds: 3600 });
    await dueNow(pg);
  };
  const tick = () => briefTick({ reader: mapReader(manifests), store, transport: model, config: testConfig({ throttleSeconds: 0, ...(membership ? { membership: true } : {}) }) })(hoopDb(pg), new Date());
  const runner: TenantRunner = async (org, ws, work) => {
    await pg.exec("SET ROLE bulls96_read_api; BEGIN READ ONLY");
    try { await pg.query(TENANT_SETTINGS_SQL, [org, ws]); const r = await work(pg as never); await pg.exec("COMMIT"); return r; }
    catch (e) { await pg.exec("ROLLBACK"); throw e; } finally { await pg.exec("RESET ROLE"); }
  };
  const ask = async (record: string, client = sqlBriefSummary(runner), binding?: string) =>
    briefMembershipResponse((await client.membership!(KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId, record, binding))!, { orgId: KEY.orgId, workspaceId: KEY.workspaceId, source: KEY.harness, sessionId: KEY.sessionId });
  return { pg, model, upload, tick, ask, runner };
}

test("E2 through generation: not_member in the old consumed input, unknown while uncovered, member once consumed; tool output never a member", async () => {
  const s = await setup();
  await s.upload([U1, A1, TOOL], 1);
  expect(await s.tick()).toEqual({ generated: 1 });
  expect(await s.ask("u1")).toEqual({ status: "member", revision: 1, sourceRevisions: { "m-1": 1 }, binding: BINDING });
  expect(await s.ask("a1")).toMatchObject({ status: "member", revision: 1 });
  expect(await s.ask("tool-1")).toMatchObject({ status: "not_member", revision: 1 }); // filtered out of the input
  expect(await s.ask("e2")).toMatchObject({ status: "not_member", revision: 1 }); // not in the old consumed input
  // E2 commits (revision 2): nothing covers it yet, so no brief is served and nothing is claimed.
  await s.upload([U1, E2, A1, TOOL], 2);
  expect(await s.ask("e2")).toEqual({ status: "unknown", revision: null, sourceRevisions: null, binding: null });
  // The generator consumes E2.
  expect(await s.tick()).toEqual({ generated: 1 });
  const input = JSON.parse(s.model.requests[1]!.input);
  expect(input.messages.map((m: { text: string }) => m.text)).toContain("also check the retry path");
  expect(await s.ask("e2")).toEqual({ status: "member", revision: 2, sourceRevisions: { "m-1": 2 }, binding: BINDING });
});

test("same-revision replacement interval: v2 still serves the stored A brief, membership answers unknown against the served binding", async () => {
  const s = await setup();
  await s.upload([U1, A1], 1);
  expect(await s.tick()).toEqual({ generated: 1 });
  expect(await s.ask("u1")).toMatchObject({ status: "member", revision: 1 });
  // Re-created at the SAME revision with other content; B has not published.
  await s.pg.exec("UPDATE sessions.session_index SET content_sha256 = decode(repeat('cd', 32), 'hex'), manifest_etag = 'etag-b'");
  const served = await s.runner(KEY.orgId, KEY.workspaceId, async (q) =>
    (await q.query<{ brief_revision: number }>("SELECT brief_revision FROM sessions.session_brief_summary_v2_for($1,$2,$3)", [KEY.workspaceId, KEY.harness, KEY.sessionId])).rows);
  expect(served).toEqual([{ brief_revision: 1 }]); // provenance as served (the v2 currency limitation)
  expect(await s.ask("u1")).toEqual({ status: "unknown", revision: 1, sourceRevisions: { "m-1": 1 }, binding: BINDING });
  expect(await s.ask("never-seen")).toMatchObject({ status: "unknown" }); // no negative without binding-current evidence
});

test("stale A refused after B: the served brief B and its evidence survive, and the repeat read still answers for B", async () => {
  const s = await setup();
  await s.upload([U1, A1], 1);
  const bindingA = briefSourceBinding(await livePointers(hoopDb(s.pg), KEY));
  expect(await s.tick()).toEqual({ generated: 1 }); // B (the current incarnation) publishes with evidence
  expect(await s.ask("a1")).toMatchObject({ status: "member", revision: 1 });
  await s.pg.exec("UPDATE sessions.session_index SET manifest_etag = 'etag-b2'");
  const bindingB = briefSourceBinding(await livePointers(hoopDb(s.pg), KEY));
  const text = { title: "B", description: "Brief B.", state: "completed" as const, scrubRuleset: "relayhistory-scrub", scrubVersion: 1, fingerprint: "b".repeat(64), promptVersion: "session-summary-v3", engineVersion: "session-brief-v3", model: "gpt-5.6-luna", messageCount: 1, shortenedMessages: 0, inputTokens: 1, outputTokens: 1, durationMs: 1, generatedAt: new Date("2026-11-09T00:00:00Z") };
  expect(await recordBrief(hoopDb(s.pg), KEY, { "m-1": 1 }, { status: "ready", text }, { sourceBinding: bindingB, membership: { members: ["b-only"], complete: true } })).toEqual({ written: true });
  expect(await s.ask("b-only")).toMatchObject({ status: "member" });
  // A finishes late over the old incarnation, with its own (different) members: refused, nothing written.
  expect(await recordBrief(hoopDb(s.pg), KEY, { "m-1": 1 }, { status: "ready", text: { ...text, description: "Brief A.", fingerprint: "a".repeat(64) } }, { sourceBinding: bindingA, membership: { members: ["a-only"], complete: true } })).toEqual({ written: false });
  expect(await s.ask("b-only")).toMatchObject({ status: "member" });
  expect(await s.ask("a-only")).toMatchObject({ status: "not_member" });
  expect((await s.pg.query<{ description: string }>("SELECT description FROM sessions.session_briefs")).rows).toEqual([{ description: "Brief B." }]);
});

test("evidence is retired with its brief: a quarantine clears it, a deleted brief cascades, incomplete evidence never answers not_member", async () => {
  const s = await setup();
  await s.upload([U1, A1], 1);
  expect(await s.tick()).toEqual({ generated: 1 });
  const count = async () => (await s.pg.query<{ n: number }>("SELECT (SELECT count(*) FROM sessions.session_brief_evidence)::int + (SELECT count(*) FROM sessions.session_brief_members)::int AS n")).rows[0]!.n;
  expect(await count()).toBe(3); // one evidence row, two members
  await s.pg.exec("UPDATE sessions.session_brief_evidence SET complete = false");
  expect(await s.ask("never-seen")).toMatchObject({ status: "unknown" });
  expect(await s.ask("u1")).toMatchObject({ status: "member" });
  await recordBrief(hoopDb(s.pg), KEY, { "m-1": 1 }, { status: "quarantined", error: "session_quarantined" });
  expect(await count()).toBe(0);
  expect(await s.ask("u1")).toEqual({ status: "unknown", revision: null, sourceRevisions: null, binding: null });
  // A fresh brief, then the brief row itself is deleted: the evidence goes with it.
  await s.pg.exec("DELETE FROM sessions.session_briefs");
  await armWakeup(hoopDb(s.pg), KEY, { idleSeconds: 0, maxWaitSeconds: 0 });
  await dueNow(s.pg);
  expect(await s.tick()).toEqual({ generated: 1 });
  expect(await count()).toBe(3);
  await s.pg.exec("DELETE FROM sessions.session_briefs");
  expect(await count()).toBe(0);
});

test("flag off writes no evidence (legacy brief answers unknown); authority is v2's; read-role-only EXECUTE; no table grants", async () => {
  const s = await setup(false);
  await s.upload([U1, A1], 1);
  expect(await s.tick()).toEqual({ generated: 1 });
  expect(await s.ask("u1")).toEqual({ status: "unknown", revision: 1, sourceRevisions: { "m-1": 1 }, binding: BINDING });
  const reason = (p: Promise<unknown>) => p.then(() => "resolved", (e: unknown) => (e instanceof BriefSummaryError ? e.reason : `other:${String(e)}`));
  const foreign = sqlBriefSummary((org, _ws, work) => s.runner(org, "foreign", work));
  expect(await reason(foreign.membership!(KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId, "u1"))).toBe("refused");
  expect(await reason(sqlBriefSummary(s.runner).membership!(KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId, ""))).toBe("invalid_argument");
  expect(await reason(sqlBriefSummary(s.runner).membership!(KEY.orgId, KEY.workspaceId, KEY.harness, "", "u1"))).toBe("invalid_argument");
  const acl = await s.pg.query<{ role: string; allowed: boolean }>(
    "SELECT rolname AS role, has_function_privilege(oid,'sessions.session_brief_membership_for(text,text,text,text,text)','EXECUTE') AS allowed FROM pg_roles WHERE rolname IN ('bulls96_read_api','anon','authenticated','service_role','search_consumer_runtime')");
  expect(acl.rows.length).toBeGreaterThan(0);
  for (const r of acl.rows) expect(r.allowed).toBe(r.role === "bulls96_read_api");
  for (const t of ["session_brief_evidence", "session_brief_members"])
    expect((await s.pg.query<{ allowed: boolean }>(`SELECT has_table_privilege('bulls96_read_api','sessions.${t}','SELECT') AS allowed`)).rows[0]!.allowed).toBe(false);
});

test("three reads across an equal-revision replacement (B replaces A's content; B paused before publication, then published, then late A refused)", async () => {
  const s = await setup();
  const manifestKey = `manifest/${KEY.sessionId}/m-1/r1`;
  await s.upload([U1, A1], 1); // A's content at revision 1
  expect(await s.tick()).toEqual({ generated: 1 });
  const bindingA = briefSourceBinding(await livePointers(hoopDb(s.pg), KEY));
  // B re-creates the session at the SAME revision with other content (a B-only record b1).
  const B1 = line("b1", "user", "B-only request", "2026-10-29T10:00:01.000Z");
  const store = tempStore();
  const manifestB = await transcript(store, [B1, A1], { revision: 1 });
  const reads: Array<{ at: string; served: string[]; provenance: number | null; b1: string; a1: string }> = [];
  const look = async (at: string) => {
    const served = await s.runner(KEY.orgId, KEY.workspaceId, async (q) =>
      (await q.query<{ description: string; brief_revision: number }>("SELECT description, brief_revision FROM sessions.session_brief_summary_v2_for($1,$2,$3)", [KEY.workspaceId, KEY.harness, KEY.sessionId])).rows);
    const b1 = await s.ask("b1"), a1 = await s.ask("a1");
    reads.push({ at, served: served.map((r) => r.description), provenance: served[0]?.brief_revision ?? null, b1: b1.status, a1: a1.status });
  };
  // The harness reader serves whatever the map holds for the key: B's object replaces A's.
  const reader = new Map<string, ManifestV1>([[manifestKey, manifestB]]);
  await pointer(s.pg, { revision: 1, manifestKey, ...indexedFrom(manifestB) });
  await armWakeup(hoopDb(s.pg), KEY, { idleSeconds: 0, maxWaitSeconds: 0 });
  await dueNow(s.pg);
  const model = new FakeModel(); model.reply = { ...model.reply, description: "Brief B." };
  const complete = model.complete.bind(model);
  model.complete = async (req) => { await look("1: B paused before publication"); return complete(req); };
  const tickB = briefTick({ reader: mapReader(reader), store, transport: model, config: testConfig({ throttleSeconds: 0, membership: true }) });
  expect(await tickB(hoopDb(s.pg), new Date())).toEqual({ generated: 1 });
  expect(JSON.parse(model.requests[0]!.input).messages.map((m: { text: string }) => m.text)).toContain("B-only request");
  await look("2: B published");
  // A's run finishes late over the replaced incarnation: refused.
  const textA = { title: "A", description: "Brief A (late).", state: "completed" as const, scrubRuleset: "relayhistory-scrub", scrubVersion: 1, fingerprint: "a".repeat(64), promptVersion: "session-summary-v3", engineVersion: "session-brief-v3", model: "gpt-5.6-luna", messageCount: 2, shortenedMessages: 0, inputTokens: 1, outputTokens: 1, durationMs: 1, generatedAt: new Date("2026-11-09T00:00:00Z") };
  expect(await recordBrief(hoopDb(s.pg), KEY, { "m-1": 1 }, { status: "ready", text: textA }, { sourceBinding: bindingA, membership: { members: ["u1", "a1"], complete: true } })).toEqual({ written: false });
  await look("3: late A refused");
  const descA = new FakeModel().reply.description;
  expect(reads).toEqual([
    // The revision-map predicate cannot tell A from B at equal revisions: A's brief is still SERVED
    // with revision 1 during the interval. Membership, bound to the incarnation, refuses to vouch.
    { at: "1: B paused before publication", served: [descA], provenance: 1, b1: "unknown", a1: "unknown" },
    { at: "2: B published", served: ["Brief B."], provenance: 1, b1: "member", a1: "member" },
    { at: "3: late A refused", served: ["Brief B."], provenance: 1, b1: "member", a1: "member" },
  ]);
});

test("drift guard: summaryMessages selects exactly the messages the vendored summaryInput sends (count, order, text)", async () => {
  const { summaryMessages } = await import("../input.ts");
  const { summaryInput } = await import("../engine/summary-input.ts");
  const ev = (id: number, over: Record<string, unknown>) => ({ id, source: KEY.harness, sessionId: KEY.sessionId, role: "user", kind: "text", text: `t${id}`, tsMs: 1000, eventUid: `u${id}`, ...over });
  const events = [
    ev(1, { tsMs: 3000 }), ev(2, { role: "assistant", tsMs: 1000 }), ev(3, { kind: "tool_result", tsMs: 1500 }),
    ev(4, { text: "   ", tsMs: 1600 }), ev(5, { sessionId: "other", tsMs: 1700 }), ev(6, { source: "codex", tsMs: 1800 }),
    ev(7, { role: "system", tsMs: 1900 }), ev(8, { role: "assistant", tsMs: 1000 }), ev(9, { text: "  padded  ", tsMs: 2500 }),
  ] as never[];
  const picked = summaryMessages(KEY.harness, KEY.sessionId, events);
  const sent = summaryInput(KEY.harness, KEY.sessionId, events).messages;
  expect(picked.map((e: { text: string }) => e.text.trim())).toEqual(sent.map((m) => m.text));
  expect(picked.map((e: { eventUid: string }) => e.eventUid)).toEqual(["u2", "u8", "u9", "u1"]);
});

test("caller correlation: a caller holding brief A asks after B published at the same revision; with A's binding the answer is changed, never B's member", async () => {
  const s = await setup();
  await s.upload([U1, A1], 1);
  expect(await s.tick()).toEqual({ generated: 1 });
  // Read 1 (summary, parsed): the caller now holds A and its binding.
  const summaryRows = (await sqlBriefSummary(s.runner).read(KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId))!;
  const { briefSummaryBinding } = await import("../../session-read-api/brief-summary.ts");
  const bindingA = briefSummaryBinding(summaryRows)!;
  expect(bindingA).toMatch(/^[0-9a-f]{64}$/);
  expect(await s.ask("a1", undefined, bindingA)).toMatchObject({ status: "member", binding: bindingA });
  // Between the caller's two requests B replaces the content at the SAME revision and publishes.
  const B1 = line("b1", "user", "B-only request", "2026-10-29T10:00:01.000Z");
  const store = tempStore();
  const manifestB = await transcript(store, [B1, A1], { revision: 1 });
  const manifestKey = `manifest/${KEY.sessionId}/m-1/r1`;
  await pointer(s.pg, { revision: 1, manifestKey, ...indexedFrom(manifestB) });
  await armWakeup(hoopDb(s.pg), KEY, { idleSeconds: 0, maxWaitSeconds: 0 });
  await dueNow(s.pg);
  const model = new FakeModel(); model.reply = { ...model.reply, description: "Brief B." };
  expect(await briefTick({ reader: mapReader(new Map([[manifestKey, manifestB]])), store, transport: model, config: testConfig({ throttleSeconds: 0, membership: true }) })(hoopDb(s.pg), new Date())).toEqual({ generated: 1 });
  // Read 2 with A's binding: the brief changed under the caller. No definitive answer about B.
  const stale = await s.ask("b1", undefined, bindingA);
  expect(stale.status).toBe("changed");
  expect(stale.binding).not.toBe(bindingA);
  // Without a binding (or with B's) the answer is about B, and says which brief it is.
  const fresh = await s.ask("b1");
  expect(fresh).toMatchObject({ status: "member", revision: 1 });
  expect(await s.ask("b1", undefined, fresh.binding!)).toMatchObject({ status: "member", binding: fresh.binding });
  // An invalid binding is refused like an invalid key.
  await expect(sqlBriefSummary(s.runner).membership!(KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId, "b1", "nothex")).rejects.toMatchObject({ reason: "invalid_argument" });
});
