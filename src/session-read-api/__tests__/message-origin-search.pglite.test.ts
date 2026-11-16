/**
 * Human-only search on the self-host path, end to end: Desktop-shaped delivery
 * records (relayhistory `session_event` payloads carrying `origin_kind`) go
 * through the one record-to-event projection the canonical indexer and the
 * buffered feed run (projectEventRows -> projectedToNeonRow -> toIndexRow) into
 * the self-host Postgres search index, and `/v1/events?origin=human` answers
 * only the human-typed messages. Agent-to-agent traffic (subagent prompts,
 * peer messages, task notifications) and messages with no recorded origin are
 * indexed but never answered to a human-only search. All of it is default off
 * (MESSAGE_ORIGIN_LABELS_ENABLED): off, rows and responses are exactly as before.
 */
import { expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { createPostgresSearchIndex, TENANT_SETTINGS_SQL, type SqlClient } from "@rhc/search-index/postgres.ts";
import type { SearchIndexAdapter } from "@rhc/search-index/contracts.ts";
import type { RelayhistoryStreamRecord } from "@rhc/lib/pipeline/relayhistory-transform.ts";
import { createReadApi } from "../app.ts";
import type { Liveness } from "../page.ts";
import { projectEventRows } from "../canonical-projection.ts";
import { searchDocument, type SearchServiceBinding } from "../../session-pipeline/search-adapter.ts";
import { buildCandidateQuery, buildNewestQuery } from "../queries.ts";

const tenant = { orgId: "org-a", workspaceId: "ws-a" };
const scope = { ...tenant, harness: "claude", sessionId: "sess-1" };
const liveness: Liveness = { async erased() { return new Set(); }, async sessionStart() { return { pointers: 1, live: 1, createdAtMs: 1 }; }, async livePointerSessions(_o, _w, ids) { return new Set(ids); } };
const T0 = 1_767_225_600_000;

/** One `session_event` row as relayhistory stores and the desktop uploads it. */
function event(id: number, role: string, text: string, over: Record<string, unknown> = {}): RelayhistoryStreamRecord {
  return {
    kind: "session_event", origin_id: "origin-1", record_id: `rec-${id}`, revision: "1", operation: "upsert",
    received_at: new Date(T0 + 60_000).toISOString(),
    payload: {
      session_id: "sess-1", source: "claude", id, event_index: id, message_id: `m-${id}`, event_uid: `m-${id}:0`,
      role, kind: "text", raw_kind: role, text, ts_ms: T0 + id * 1000, is_meta: 0, is_sidechain: 0,
      control_kind: null, agent_id: null, subagent_session_id: null, provider: "anthropic", model: null,
      ...over,
    },
  };
}

const RECORDS = [
  event(1, "user", "yes, ship the release notes", { origin_kind: "human" }),
  event(2, "assistant", "yes, shipping the release notes now"),
  event(3, "user", "yes, ship it: subagent brief", { is_sidechain: 1, agent_id: "agent-7" }),
  event(4, "user", "yes, ship from a peer agent", { origin_kind: "peer" }),
  // relayhistory classifies Claude's task notifications as control rows; one it
  // missed still carries its origin.
  event(5, "user", "<task-notification>yes, ship finished</task-notification>", { origin_kind: "task-notification" }),
  event(8, "user", "<task-notification>yes, ship control row</task-notification>", { origin_kind: "task-notification", control_kind: "task_notification" }),
  event(6, "user", "yes, ship (uploaded before origin capture)"),
  event(7, "user", "confirmed: go ahead", { origin_kind: "human" }),
  // A typed prompt is both a prompt-log row and its own session_event row (event 1):
  // the history row is not labelled, so a human-only search answers the prompt once.
  {
    kind: "history", origin_id: "origin-1", record_id: "rec-h1", revision: "1", operation: "upsert",
    received_at: new Date(T0 + 60_000).toISOString(),
    payload: { session_id: "sess-1", source: "claude", id: 1, prompt: "yes, ship the release notes", timestamp_ms: T0 + 1000, project: "/p" },
  },
];

async function indexed(originLabels = true) {
  const db = new PGlite({ extensions: { pg_trgm } });
  await db.exec("CREATE EXTENSION pg_trgm");
  const index = createPostgresSearchIndex({ client: db as unknown as SqlClient, tenantRunner: (o, w, work) => db.transaction(async tx => { await tx.query(TENANT_SETTINGS_SQL, [o, w]); return work(tx as unknown as SqlClient); }) });
  await index.ensureSchema();
  const projected = projectEventRows(RECORDS, scope, { sessionId: "sess-1", machineId: "machine:1", userId: "u-1" }, 1, undefined, originLabels);
  if ("error" in projected) throw projected.error;
  await index.upsert(projected.events.map(e => searchDocument(e.indexRow)));
  return { db, index, rows: projected.events.map(e => e.indexRow) };
}

function api(index: SearchIndexAdapter, messageOrigin = true) {
  const adapter: SearchServiceBinding = { index, state: { async fromMs() { return 0; }, async record() {}, async partials() { return { sessions: [], overflow: false }; } } };
  const app = createReadApi({ adapter, liveness, completeSinceMs: 0, ...(messageOrigin ? { messageOrigin: true } : {}), verify: async () => ({ ...tenant, userId: "u-1", scopes: ["rth:read"] }), log: () => {} });
  return (path: string) => app.request(path, { headers: { authorization: "Bearer synthetic" } });
}

test("the projection labels who wrote each event and keeps the uploaded origin in the stored record", async () => {
  const { db, rows } = await indexed();
  // Sidechain user rows (an agent prompting its subagent) and control rows are
  // not projected as events at all (the hosted projection's rule), so they never
  // reach search; every other user message is labeled or left unknown.
  const byText = Object.fromEntries(rows.filter(r => r.kind === "session_event").map(r => [r.content, [r.actor_name, r.subagent_id]]));
  expect(byText).toEqual({
    "yes, ship the release notes": ["human", ""],
    "yes, shipping the release notes now": ["agent", ""],
    "yes, ship from a peer agent": ["peer", ""],
    "<task-notification>yes, ship finished</task-notification>": ["task-notification", ""],
    "yes, ship (uploaded before origin capture)": ["", ""],
    "confirmed: go ahead": ["human", ""],
  });
  // The prompt-log copy of the typed prompt carries no label.
  expect(rows.filter(r => r.kind === "history").map(r => r.actor_name)).toEqual([""]);
  // The uploaded column itself survives into the projected event's record.
  const projected = projectEventRows(RECORDS, scope, { sessionId: "sess-1", machineId: "machine:1", userId: "u-1" }, 1, undefined, true);
  if ("error" in projected) throw projected.error;
  const human = projected.events.find(e => e.indexRow.content === "confirmed: go ahead")!;
  expect((human.row.record as { payload: Record<string, unknown> }).payload.origin_kind).toBe("human");
  await db.close();
});

test("/v1/events?origin=human answers human-typed messages only, on its own and with q", async () => {
  const { db, index } = await indexed();
  const get = api(index);
  const contents = async (path: string) => {
    const r = await get(path);
    if (r.status !== 200) throw new Error(`${path}: ${r.status} ${await r.text()}`);
    return ((await r.json()) as { events: { content: string; actorName: string }[] }).events.map(e => e.content);
  };
  // The typed prompt once: its prompt-log copy carries no label.
  expect(await contents("/v1/events?workspace=ws-a&q=yes&limit=10&origin=human")).toEqual(["yes, ship the release notes"]);
  // An answered filtered page says so; an unfiltered page does not.
  expect((await get("/v1/events?workspace=ws-a&q=yes&limit=10&origin=human")).headers.get("X-Sessions-Read-Origin")).toBe("human");
  expect((await get("/v1/events?workspace=ws-a&q=yes&limit=10")).headers.get("X-Sessions-Read-Origin")).toBeNull();
  expect(await contents("/v1/events?workspace=ws-a&session=sess-1&limit=10&origin=human")).toEqual(["confirmed: go ahead", "yes, ship the release notes"]);
  // Without the filter the same search answers every author, agent traffic and the prompt log included.
  expect((await contents("/v1/events?workspace=ws-a&q=yes&limit=10")).length).toBe(6);
  expect(await contents("/v1/events?workspace=ws-a&q=yes&limit=10&origin=peer")).toEqual(["yes, ship from a peer agent"]);
  expect(await contents("/v1/events?workspace=ws-a&q=yes&limit=10&origin=task-notification")).toEqual(["<task-notification>yes, ship finished</task-notification>"]);
  await db.close();
});

test("an unknown origin is a 400; a backend that cannot filter by author refuses instead of answering unfiltered", async () => {
  const { db, index } = await indexed();
  const bad = await api(index)("/v1/events?workspace=ws-a&q=yes&limit=10&origin=robot");
  expect(bad.status).toBe(400);
  let queried = false;
  const blind: SearchIndexAdapter = { ...index, capabilities: () => ({ ...index.capabilities(), filters: index.capabilities().filters.filter(f => f !== "actor") }), query: async (q, c) => { queried = true; return index.query(q, c); } };
  const refused = await api(blind)("/v1/events?workspace=ws-a&q=yes&limit=10&origin=human");
  expect(refused.status).toBe(501);
  expect(((await refused.json()) as { error: { reason: string } }).error.reason).toBe("param_origin");
  expect(queried).toBe(false);
  expect(refused.headers.get("X-Sessions-Read-Origin")).toBeNull();
  // The owner-buffered/session events route does not take the filter.
  const session = await api(index)("/v1/sessions/sess-1/events?workspace=ws-a&origin=human");
  expect(session.status).toBe(501);
  await db.close();
});

test("the hosted ClickHouse queries bind the same filter on both round trips", () => {
  const input = { ...tenant, q: "yes", actorName: "human", order: "desc" as const, limit: 10 };
  const candidates = buildCandidateQuery("db", input);
  expect(candidates.query).toContain("actor_name = {actor:String}");
  expect(candidates.params.actor).toBe("human");
  const newest = buildNewestQuery("db", input, [{ tsMs: 1, sessionId: "s", source: "claude", machineId: "m", kind: "session_event", eventId: "e" }]);
  expect(newest.query).toContain("actor_name = {actor:String}");
  expect(newest.params.actor).toBe("human");
  const unfiltered = buildCandidateQuery("db", { ...input, actorName: undefined });
  expect(unfiltered.query).not.toContain("actor_name =");
});

test("default off: MESSAGE_ORIGIN_LABELS_ENABLED unset writes rows exactly as before and refuses origin=", async () => {
  const { db, index, rows } = await indexed(false);
  // No labels and no derived subagent: the pre-flag rows, fingerprints included.
  expect(rows.every(r => r.actor_name === "" && r.subagent_id === "")).toBe(true);
  const on = await indexed(true);
  const fp = (list: typeof rows) => Object.fromEntries(list.map(r => [r.event_id, r.fingerprint]));
  const offPrints = fp(rows), onPrints = fp(on.rows);
  // Only labelled rows change fingerprint when the flag turns on (each scope rewrites them once).
  const changed = on.rows.filter(r => offPrints[r.event_id] !== onPrints[r.event_id]).map(r => r.actor_name).sort();
  expect(changed).toEqual(["agent", "human", "human", "peer", "task-notification"]);
  await on.db.close();
  let queried = false;
  const watched: SearchIndexAdapter = { ...index, query: async (q, c) => { queried = true; return index.query(q, c); } };
  const r = await api(watched, false)("/v1/events?workspace=ws-a&q=yes&limit=10&origin=human");
  expect(r.status).toBe(501);
  expect(((await r.json()) as { error: { reason: string } }).error.reason).toBe("param_origin");
  expect(r.headers.get("X-Sessions-Read-Origin")).toBeNull();
  expect(queried).toBe(false);
  // Without origin= the off reader answers as before.
  expect((await api(index, false)("/v1/events?workspace=ws-a&q=yes&limit=10")).status).toBe(200);
  await db.close();
}, 60_000);
