/**
 * T130 1b (Pipeline review at 05c68784): canonical publication is bound to the generation the
 * consumed snapshot was verified against. The pointer binding (key, revision, digest, ETag) is held
 * FIXED here; only a field it does not cover changes (bucket), which moves canonicalSourceGeneration.
 * Park A, serve B, resume A, for both canonical builders (events and JSONL transcript).
 */
import { expect, setDefaultTimeout, test } from "bun:test";
import { briefInputFromCanonical, briefInputFromTranscript } from "../canonical-input.ts";
import { briefSession } from "../worker.ts";
import { armWakeup, briefSourceBinding, claimWakeups, livePointers, liveCanonicalGeneration, recordBrief } from "../store.ts";
import { database, hoopDb, pointer, KEY, FakeModel, testConfig, read } from "./harness.ts";
import { TENANT_SETTINGS_SQL } from "../../session-read-api/liveness.ts";
import type { BriefInput, BriefPointer } from "../input.ts";
import type { PGlite } from "@electric-sql/pglite";

setDefaultTimeout(60_000);
const ROLE = read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const events = (gen: string) => ({ exists: true, generation: gen, legacyCoverage: null, rows: [
  { table: "convergence_events", row: { session_id: KEY.sessionId, source: KEY.harness, event_id: "u1", machine_id: "m-1", ts: "2026-11-07T01:00:00Z", kind: "history", type: "user", content: "Investigate the queue retry race." } },
  { table: "convergence_events", row: { session_id: KEY.sessionId, source: KEY.harness, event_id: "a1", machine_id: "m-1", ts: "2026-11-07T01:00:01Z", kind: "history", actor_role: "assistant", content: "The retry completed after awaiting its lease." } },
] }) as never;
const transcript = (gen: string) => ({ generation: gen, records: [
  { machineId: "m-1", index: 0, record: { type: "user", sessionId: KEY.sessionId, timestamp: "2026-11-07T01:00:00Z", message: { role: "user", content: "Investigate the queue retry race." } } },
  { machineId: "m-1", index: 1, record: { type: "assistant", sessionId: KEY.sessionId, timestamp: "2026-11-07T01:00:01Z", message: { role: "assistant", content: [{ type: "text", text: "The retry completed after awaiting its lease." }] } } },
] }) as never;
const builders: Record<string, (ptrs: readonly BriefPointer[], gen: string) => BriefInput> = {
  events: (ptrs, gen) => ({ ...briefInputFromCanonical(KEY, ptrs, events(gen)), canonicalSessionId: KEY.sessionId, canonicalGeneration: gen }),
  jsonl: (ptrs, gen) => ({ ...briefInputFromTranscript(KEY, ptrs, transcript(gen)), canonicalSessionId: KEY.sessionId, canonicalGeneration: gen }),
};
async function served(pg: PGlite) {
  await pg.exec("SET ROLE bulls96_read_api; BEGIN READ ONLY");
  try {
    await pg.query(TENANT_SETTINGS_SQL, [KEY.orgId, KEY.workspaceId]);
    const r = await pg.query<{ description: string }>("SELECT description FROM sessions.session_brief_summary_v2_for($1,$2,$3)", [KEY.workspaceId, KEY.harness, KEY.sessionId]);
    await pg.exec("COMMIT"); return r.rows.map((x) => x.description);
  } catch (e) { await pg.exec("ROLLBACK"); throw e; } finally { await pg.exec("RESET ROLE"); }
}
async function run(path: string, interleave: boolean) {
  const pg = await database(); const db = hoopDb(pg);
  try {
    await pg.exec(ROLE);
    await pointer(pg, { revision: 1, manifestKey: "manifest/s-1/m-1/r1", manifestEtag: "etag-1" });
    const genA = await pg.transaction(async (tx) => liveCanonicalGeneration(tx as never, KEY, KEY.sessionId));
    await armWakeup(db, KEY, { idleSeconds: 0, maxWaitSeconds: 0 });
    const wakeup = (await claimWakeups(db, 1, 300))[0]!;
    const model = new FakeModel();
    const complete = model.complete.bind(model);
    model.complete = async (req) => {
      if (interleave) {
        // Canonical re-publication that keeps key, revision, digest and ETag (the pointer binding)
        // but moves the generation; B is built and served before A resumes.
        const before = briefSourceBinding(await livePointers(db, KEY));
        await pg.exec("UPDATE sessions.session_index SET bucket = 'bucket-b'");
        expect(briefSourceBinding(await livePointers(db, KEY))).toEqual(before);
        const genB = await pg.transaction(async (tx) => liveCanonicalGeneration(tx as never, KEY, KEY.sessionId));
        expect(genB).not.toBe(genA);
        const text = { title: "B", description: "Brief B.", state: "completed" as const, scrubRuleset: "relayhistory-scrub", scrubVersion: 1, fingerprint: "b".repeat(64), promptVersion: "session-summary-v3", engineVersion: "session-brief-v3", model: "gpt-5.6-luna", messageCount: 2, shortenedMessages: 0, inputTokens: 1, outputTokens: 1, durationMs: 1, generatedAt: new Date("2026-11-09T00:00:00Z") };
        expect(await recordBrief(db, KEY, { "m-1": 1 }, { status: "ready", text }, { canonicalSessionId: KEY.sessionId, sourceBinding: before, canonicalGeneration: genB })).toEqual({ written: true });
        expect(await served(pg)).toEqual(["Brief B."]);
      }
      return complete(req);
    };
    const result = await briefSession({ db, reader: {} as never, store: {} as never, transport: model, config: testConfig({ throttleSeconds: 0 }),
      canonicalInput: async (_db, _key, ptrs) => builders[path]!(ptrs, genA) }, wakeup);
    return { result, served: await served(pg), calls: model.requests.length };
  } finally { await pg.close(); }
}
for (const path of ["events", "jsonl"]) {
  test(`${path}: control, the consumed generation is live, the brief publishes and is served`, async () => {
    const r = await run(path, false);
    expect(r.result).toBe("generated");
    expect(r.served).toEqual([new FakeModel().reply.description]);
  });
  test(`${path}: park A, serve B (pointer binding unchanged, generation moved), resume A: refused, B still served`, async () => {
    const r = await run(path, true);
    expect(r.result).toBe("stale");
    expect(r.calls).toBe(1);
    expect(r.served).toEqual(["Brief B."]);
  });
}
