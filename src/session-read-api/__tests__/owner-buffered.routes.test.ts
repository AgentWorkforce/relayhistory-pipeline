/**
 * Owner-only buffered read through the assembled read API (createReadApi): GET /v1/sessions
 * union (filters, dedupe with canonical winning, composite cursor, fenced header, strict
 * sessionSummary keys), GET /v1/sessions/:id/turns and /metadata (buffered served, fenced 503,
 * erasure race, hidden admission), cross-user 500, flag/allowlist off (no gate call, bytes
 * unchanged), events never buffered, and the pre-v4 stored object regression.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type TurnsDeps, type VerifiedIdentity } from "../app.ts";
import { decodeCompositeListCursor, encodeCompositeListCursor, type Directory, type DirectoryListRow } from "../directory.ts";
import { BufferedFenced, sqlOwnerBufferedReads, type ControlHeadGate, type OwnerBufferedDeps, type OwnerBufferedReads } from "../owner-buffered.ts";
import { memoryLiveness } from "./support.ts";
import { E1, SESSION, authority, bufferedRow, fencedGate, putStage, scratch, scriptedGate, scriptedReads, turn, unzip } from "./owner-buffered-support.ts";

const ID: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };
const cleanup: Array<() => void> = [];
afterEach(() => {
  for (const done of cleanup.splice(0)) done();
});
function store() {
  const s = scratch();
  cleanup.push(s.done);
  return s.store;
}

function dirRow(over: Partial<DirectoryListRow> & { cursor_ts?: string } = {}): DirectoryListRow & { cursor_ts?: string } {
  return {
    org_id: "org-a", workspace_id: "ws-a", harness: "codex", session_id: "s-1",
    created_at: new Date("2026-10-01T00:00:00Z"), last_activity_at: new Date("2026-10-02T03:04:05.123Z"),
    cursor_ts: "2026-10-02T03:04:05.123456Z",
    title: "A title", task_title: "Fix the thing", agent_names: [], project_id: "github.com/acme/api",
    project_key: null, project_key_method: null, git_repo: null, git_branch: null, initial_commit: null,
    originator: null, agent_version: null, models: ["gpt-5"], state: "unknown", event_count: "12",
    input_tokens: "100", output_tokens: 7, cost_usd_micros: null, machine_ids: ["m-1"], user_ids: ["u-1"],
    parent_session_ids: [], child_session_count: 0, short_summary: null, short_summary_generated_at: null,
    catalog_projected_at: null, projected_at: null,
    ...over,
  } as DirectoryListRow & { cursor_ts?: string };
}

interface Harness {
  enabled?: boolean;
  gate?: ControlHeadGate;
  reads?: OwnerBufferedReads;
  store?: OwnerBufferedDeps["store"];
  rows?: Array<DirectoryListRow & { cursor_ts?: string }>;
  workspaces?: ReadonlySet<string> | "*" | undefined;
  pointers?: Array<{ harness: string; pointers: number; live: number }>;
  liveSessions?: Record<string, number>;
  noOwnerBuffered?: boolean;
}
function api(h: Harness = {}) {
  const logs: Record<string, unknown>[] = [];
  const dirCalls: unknown[] = [];
  const directory: Directory = {
    async list(_o, _w, args) {
      dirCalls.push(args);
      const rows = h.rows ?? [dirRow()];
      const c = args.cursor;
      return c ? rows.filter((r) => (r.cursor_ts ?? "") < c.lastActivityAt) : rows;
    },
    async activity() {
      return [];
    },
  };
  const turns: TurnsDeps = {
    harnesses: async () => h.pointers ?? [],
    source: async () => {
      throw new Error("canonical source not used by these tests");
    },
  };
  const ownerBuffered: OwnerBufferedDeps | undefined = h.noOwnerBuffered ? undefined : {
    enabled: h.enabled ?? true,
    reads: h.reads ?? scriptedReads().reads,
    gate: h.gate ?? scriptedGate(authority("7")),
    store: h.store ?? store(),
    decompress: unzip,
  };
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return ID;
    },
    ch: { database: "relay_search", async command() {}, async insert() {}, async select() { throw new Error("no ClickHouse in these tests"); } } as never,
    liveness: memoryLiveness({ createdAt: h.liveSessions }).liveness,
    completeSinceMs: 0,
    log: (f) => logs.push(f),
    directory,
    directoryWorkspaces: "workspaces" in h ? h.workspaces : new Set(["ws-a"]),
    turns,
    ownerBuffered,
  });
  const get = (path: string) => app.request(path, { headers: { authorization: "Bearer good" } });
  return { get, logs, dirCalls };
}
const json = async (r: Response) => (await r.json()) as Record<string, any>;
const strip = (b: Record<string, any>) => ({ ...b, correlationId: undefined });

/** cloud-client 0.2.0 sessionSummary (contract/sessions.ts), strict: exactly these keys, these types. */
const SUMMARY_SCHEMA: Record<string, (v: unknown) => boolean> = {
  title: (v) => v === null || typeof v === "string", sessionId: (v) => typeof v === "string", userIds: (v) => Array.isArray(v) && v.every((x) => typeof x === "string"),
  source: (v) => typeof v === "string", projectId: (v) => v === null || typeof v === "string", machineIds: (v) => Array.isArray(v),
  firstTs: (v) => typeof v === "string" && !Number.isNaN(Date.parse(v as string)), lastTs: (v) => typeof v === "string" && !Number.isNaN(Date.parse(v as string)),
  workState: (v) => v === null || typeof v === "string", eventCount: (v) => Number.isInteger(v), kinds: Array.isArray, models: Array.isArray,
  taskTitle: (v) => v === null || typeof v === "string", summary: (v) => v === null || typeof v === "string", summarySource: (v) => v === null || typeof v === "string",
  taskRefs: Array.isArray, totalCostUsdMicros: (v) => v === null || Number.isInteger(v), totalInputTokens: Number.isInteger, totalOutputTokens: Number.isInteger,
  totalReasoningTokens: Number.isInteger, totalCacheReadTokens: Number.isInteger, totalCacheCreateTokens: Number.isInteger,
  totalCacheCreate5mTokens: (v) => v === null || Number.isInteger(v), totalCacheCreate1hTokens: (v) => v === null || Number.isInteger(v),
};
const strictSummary = (s: Record<string, unknown>) =>
  JSON.stringify(Object.keys(s).sort()) === JSON.stringify(Object.keys(SUMMARY_SCHEMA).sort()) && Object.entries(SUMMARY_SCHEMA).every(([k, ok]) => ok(s[k]));

const L = "/v1/sessions?workspace=ws-a";
describe("GET /v1/sessions with owner buffered rows", () => {
  test("union newest-first; buffered row is a strict sessionSummary; ids header; body keys unchanged", async () => {
    const gate = scriptedGate(authority("7"));
    const { get } = api({ gate, reads: scriptedReads({ list: [bufferedRow()] }).reads });
    const r = await get(L);
    expect(r.status).toBe(200);
    const b = await json(r);
    expect(Object.keys(b).sort()).toEqual(["correlationId", "nextCursor", "sessions"]);
    expect(b.sessions.map((s: any) => [s.source, s.sessionId])).toEqual([["claude", SESSION], ["codex", "s-1"]]);
    for (const s of b.sessions) expect(strictSummary(s)).toBe(true);
    expect(b.sessions[0]).toMatchObject({ userIds: ["u-1"], title: null, summary: null, eventCount: 0, firstTs: "2026-11-05T12:00:00.000Z", lastTs: "2026-11-05T12:05:00.000Z" });
    expect(r.headers.get("x-relayhistory-buffered-ids")).toBe(`claude:${SESSION}`);
    expect(r.headers.get("x-relayhistory-buffered")).toBeNull();
    expect(gate.calls).toHaveLength(2);
  });

  test("authority fenced: buffered rows dropped, header fenced, canonical page still served", async () => {
    const { reads, calls } = scriptedReads({ list: [bufferedRow()] });
    const { get, logs } = api({ gate: fencedGate("gate_timeout"), reads });
    const r = await get(L);
    expect(r.status).toBe(200);
    expect(r.headers.get("x-relayhistory-buffered")).toBe("fenced");
    expect(r.headers.get("x-relayhistory-buffered-ids")).toBeNull();
    expect((await json(r)).sessions.map((s: any) => s.sessionId)).toEqual(["s-1"]);
    expect(calls).toHaveLength(0);
    expect(logs).toContainEqual(expect.objectContaining({ reason: "buffered_fenced", gate: "gate_timeout" }));
  });

  test("gate moves between observations: fenced header, buffered rows dropped", async () => {
    const { get } = api({ gate: scriptedGate(authority("7"), authority("8")), reads: scriptedReads({ list: [bufferedRow()] }).reads });
    const r = await get(L);
    expect(r.headers.get("x-relayhistory-buffered")).toBe("fenced");
    expect((await json(r)).sessions).toHaveLength(1);
  });

  test("hidden admissions (ABA, NULL) are never listed", async () => {
    const { get } = api({ reads: scriptedReads({ list: [bufferedRow({ admission_version: "5" }), bufferedRow({ session_id: "n", admission_epoch: null })] }).reads });
    const r = await get(L);
    expect((await json(r)).sessions.map((s: any) => s.sessionId)).toEqual(["s-1"]);
    expect(r.headers.get("x-relayhistory-buffered-ids")).toBeNull();
  });

  test("dedupe: canonical wins on the same (source, session) and when a live pointer exists", async () => {
    const same = bufferedRow({ source: "codex", session_id: "s-1" });
    const pointed = bufferedRow({ session_id: "s-live" });
    const { get } = api({ reads: scriptedReads({ list: [same, pointed] }).reads, liveSessions: { "claude\u0000s-live": 1 } });
    const b = await json(await get(L));
    expect(b.sessions.map((s: any) => [s.source, s.sessionId, s.title])).toEqual([["codex", "s-1", "A title"]]);
  });

  test("filters apply to buffered rows; user=other, agent, projectContains skip buffered entirely", async () => {
    const reads = () => scriptedReads({ list: [bufferedRow()] });
    const ids = async (q: string, gate = scriptedGate(authority("7"))) => {
      const { get } = api({ gate, reads: reads().reads });
      const b = await json(await get(`${L}${q}`));
      return { ids: b.sessions.map((s: any) => s.sessionId), gateCalls: gate.calls.length };
    };
    expect((await ids("&source=codex")).ids).toEqual(["s-1"]);
    expect((await ids("&source=claude")).ids).toContain(SESSION);
    expect((await ids("&notSource=claude")).ids).toEqual(["s-1"]);
    expect((await ids("&lastTsBefore=2026-11-05T12:05:00Z")).ids).toEqual(["s-1"]);
    expect((await ids("&lastTsAfter=2026-11-05T12:05:00.001Z")).ids).not.toContain(SESSION);
    expect((await ids("&firstTsAfter=2026-11-05T12:00:00.001Z")).ids).not.toContain(SESSION);
    expect((await ids("&firstTsBefore=2026-11-05T12:00:00Z")).ids).toEqual(["s-1"]);
    expect((await ids("&user=u-1")).ids).toContain(SESSION);
    for (const q of ["&user=u-2", "&agent=Lead", "&projectContains=acme"]) expect((await ids(q)).gateCalls).toBe(0);
  });

  test("composite cursor: buffered then canonical across pages, no duplicates", async () => {
    const list = [bufferedRow({ session_id: "b-2", cursor_ts: "2026-11-06T00:00:00.000000Z", last_ts: "2026-11-06T00:00:00.000Z" }), bufferedRow({ session_id: "b-1" })];
    const reads: OwnerBufferedReads = {
      async list(_s, _a, args) {
        const c = args.cursor;
        return list.filter((r) => !c || r.cursor_ts < c.ts || (r.cursor_ts === c.ts && r.session_id < c.sessionId)).slice(0, args.limit);
      },
      async stages() {
        return [];
      },
    };
    const { get } = api({ reads });
    const seen: string[] = [];
    let cursor: string | null = null;
    for (let i = 0; i < 5; i++) {
      const b: Record<string, any> = await json(await get(`${L}&limit=1${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`));
      seen.push(...b.sessions.map((s: any) => s.sessionId));
      cursor = b.nextCursor;
      if (!cursor) break;
      expect(typeof decodeCompositeListCursor(cursor)).toBe("object");
    }
    expect(seen).toEqual(["b-2", "b-1", "s-1"]);
  });

  test("composite (4-part) cursor is accepted only with the flag on; flag off, no deps or other filters answer 400 as main", async () => {
    const buffered = { ts: "2026-11-05T12:05:00.000000Z", sessionId: "x", source: "claude" };
    const canonical = { lastActivityAt: "2026-10-02T03:04:05.123456Z", sessionId: "s-1", harness: "codex" };
    for (const cursor of [encodeCompositeListCursor(undefined, buffered), encodeCompositeListCursor(canonical, buffered)]) {
      const q = `${L}&cursor=${encodeURIComponent(cursor)}`;
      for (const h of [{ enabled: false }, { noOwnerBuffered: true }]) {
        const gate = scriptedGate(authority("7"));
        const r = await api({ ...h, gate }).get(q);
        expect(r.status).toBe(400);
        expect(gate.calls).toHaveLength(0);
      }
      const main = await api({ noOwnerBuffered: true }).get(q);
      const off = await api({ enabled: false }).get(q);
      expect(strip(await json(off))).toEqual(strip(await json(main)));
      // A filter that skips buffered rows takes main's cursor path too.
      expect((await api({}).get(`${q}&user=u-2`)).status).toBe(400);
      expect((await api({ reads: scriptedReads({ list: [] }).reads }).get(q)).status).toBe(200);
    }
  });

  test("another user's row from the RPC is a 500, never a page", async () => {
    const run = (async (_o: string, _w: string, work: (q: unknown) => Promise<unknown>) =>
      work({ async query(sql: string) { return { rows: sql.includes("buffered_sessions_for") ? [{ ...bufferedRow(), user_id: "u-2", first_ts: new Date(), last_ts: new Date() }] : [] }; } })) as never;
    const { get } = api({ reads: sqlOwnerBufferedReads(run) });
    const r = await get(L);
    expect(r.status).toBe(500);
    expect(JSON.stringify(await json(r))).not.toContain(SESSION);
  });

  test("RPC timeout (57014) or any RPC error is fenced, never a 500: canonical page with the fenced header", async () => {
    for (const [err, gate] of [[Object.assign(new Error("canceling statement due to statement timeout"), { code: "57014" }), "rpc_timeout"], [new Error("connection terminated"), "rpc_error"]] as const) {
      const run = (async (_o: string, _w: string, work: (q: unknown) => Promise<unknown>) =>
        work({ async query(sql: string) { if (sql.includes("buffered_sessions_for")) throw err; return { rows: [] }; } })) as never;
      const { get, logs } = api({ reads: sqlOwnerBufferedReads(run) });
      const r = await get(L);
      expect(r.status).toBe(200);
      expect(r.headers.get("x-relayhistory-buffered")).toBe("fenced");
      expect(logs).toContainEqual(expect.objectContaining({ reason: "buffered_fenced", gate }));
    }
  });

  test("flag off, no deps, or workspace not allowlisted: gate never called, bytes as without the feature", async () => {
    const base = await json(await api({ noOwnerBuffered: true }).get(L));
    for (const h of [{ enabled: false }, { noOwnerBuffered: true }]) {
      const gate = scriptedGate(authority("7"));
      const { get } = api({ ...h, gate, reads: scriptedReads({ list: [bufferedRow()] }).reads });
      const r = await get(L);
      expect(strip(await json(r))).toEqual(strip(base));
      expect(r.headers.get("x-relayhistory-buffered-ids")).toBeNull();
      expect(gate.calls).toHaveLength(0);
    }
    for (const workspaces of [undefined, new Set<string>(), new Set(["ws-b"])]) {
      const gate = scriptedGate(authority("7"));
      const r = await api({ workspaces, gate }).get(L);
      expect(r.status).toBe(501);
      expect(gate.calls).toHaveLength(0);
    }
    const gate = scriptedGate(authority("7"));
    const r = await api({ workspaces: "*", gate, reads: scriptedReads({ list: [bufferedRow()] }).reads }).get(L);
    expect(r.headers.get("x-relayhistory-buffered-ids")).toBe(`claude:${SESSION}`);
  });
});

const T = `/v1/sessions/${SESSION}/turns?workspace=ws-a&source=claude`;
const M = `/v1/sessions/${SESSION}/metadata?workspace=ws-a&source=claude`;
describe("GET /v1/sessions/:id/turns and /metadata from buffered stages", () => {
  test("no canonical pointer, own kept stages: served with X-Relayhistory-Buffered: 1", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0), turn(1)]);
    const { get } = api({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads });
    const r = await get(T);
    expect(r.status).toBe(200);
    expect(r.headers.get("x-relayhistory-buffered")).toBe("1");
    const b = await json(r);
    expect(Object.keys(b).sort()).toEqual(["correlationId", "sessionId", "turns"]);
    expect(b.turns.map((t: any) => t.content)).toEqual(["buffered 0", "buffered 1"]);
    const m = await get(M);
    expect(m.status).toBe(200);
    expect(m.headers.get("x-relayhistory-buffered")).toBe("1");
    expect(await json(m)).toEqual({ nativeCli: "claude", nativeResumeId: null, sessionOwner: "u-1", originNode: "node-b" });
  });

  test("gate down before the DB read: 501 turns_no_pointers exactly as main (flag off), logged buffered_unverified, no RPC", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0, { content: "UNVERIFIED-CONTENT" })]);
    for (const path of [T, M]) {
      const main = await api({ enabled: false }).get(path);
      const mainBody = strip(await json(main));
      for (const gateOf of [() => fencedGate("gate_unbound"), () => fencedGate("gate_timeout"), () => fencedGate("gate_shape")]) {
        // A caller with no buffered stages, and one with stages: the gate failing first answers like main either way.
        for (const stages of [[[]], [[st.row]]] as const) {
          const { reads, calls } = scriptedReads({ stages: stages as never });
          const { get, logs } = api({ store: s, gate: gateOf(), reads });
          const r = await get(path);
          expect(r.status).toBe(main.status);
          expect(r.status).toBe(501);
          expect(r.headers.get("retry-after")).toBeNull();
          expect(r.headers.get("x-relayhistory-buffered")).toBeNull();
          const text = await r.text();
          expect(strip(JSON.parse(text))).toEqual(mainBody);
          expect(text).not.toContain("UNVERIFIED-CONTENT");
          expect(calls).toHaveLength(0);
          expect(logs.some((l) => l.reason === "buffered_unverified")).toBe(true);
        }
      }
    }
  });

  test("fenced after a buffered candidate was found (gate moved or failed at the final observation): 503 buffered_fenced, retryable, no bytes", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0, { content: "FENCED-CONTENT" })]);
    const gates = [() => scriptedGate(authority("7"), authority("8")), () => scriptedGate(authority("7"), new BufferedFenced("gate_pending")), () => scriptedGate(authority("7"), new BufferedFenced("gate_unbound"))];
    for (const gateOf of gates) {
      for (const path of [T, M]) {
        const { get } = api({ store: s, gate: gateOf(), reads: scriptedReads({ stages: [[st.row]] }).reads });
        const r = await get(path);
        expect(r.status).toBe(503);
        expect(r.headers.get("retry-after")).toBe("1");
        const text = await r.text();
        expect(JSON.parse(text).error).toMatchObject({ code: "buffered_fenced", reason: "buffered_fenced" });
        expect(text).not.toContain("FENCED-CONTENT");
      }
    }
  });

  test("RPC timeout (57014) or any RPC error on the stage reader is fenced, never a 500: 501 before a candidate, 503 at the recheck", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0, { content: "RPC-ERROR-CONTENT" })]);
    for (const err of [Object.assign(new Error("canceling statement due to statement timeout"), { code: "57014" }), Object.assign(new Error("function does not exist"), { code: "42883" })]) {
      for (const path of [T, M]) {
        // First stages() call fails: nothing known, answered as main (501), logged buffered_unverified.
        const first = (async (_o: string, _w: string, work: (q: unknown) => Promise<unknown>) =>
          work({ async query(sql: string) { if (sql.includes("buffered_session_stages_for")) throw err; return { rows: [] }; } })) as never;
        const { get, logs } = api({ store: s, reads: sqlOwnerBufferedReads(first) });
        const r1 = await get(path);
        expect(r1.status).toBe(501);
        expect(logs.some((l) => l.reason === "buffered_unverified")).toBe(true);
        // The recheck fails after a candidate was found: 503 buffered_fenced, no bytes.
        let n = 0;
        const recheck: OwnerBufferedReads = {
          list: async () => [],
          stages: async (scope, a, source, id) => {
            if (n++ === 0) return [st.row];
            return sqlOwnerBufferedReads(first).stages(scope, a, source, id);
          },
        };
        const r2 = await api({ store: s, reads: recheck }).get(path);
        expect(r2.status).toBe(503);
        const text = await r2.text();
        expect(JSON.parse(text).error).toMatchObject({ code: "buffered_fenced" });
        expect(text).not.toContain("RPC-ERROR-CONTENT");
      }
    }
  });

  test("hidden admission or no stage: 501 turns_no_pointers as today", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0)], { admission: { epoch: E1, version: "5" } });
    for (const reads of [scriptedReads({ stages: [[st.row]] }).reads, scriptedReads().reads]) {
      const r = await api({ store: s, reads }).get(T);
      expect(r.status).toBe(501);
      expect((await json(r)).error.reason).toBe("turns_no_pointers");
    }
  });

  test("erasure race: stages gone at the recheck -> empty turns and metadata 404", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0, { content: "ERASED" })]);
    const t = await api({ store: s, reads: scriptedReads({ stages: [[st.row], []] }).reads }).get(T);
    expect(t.status).toBe(200);
    expect((await json(t)).turns).toEqual([]);
    const m = await api({ store: s, reads: scriptedReads({ stages: [[st.row], []] }).reads }).get(M);
    expect(m.status).toBe(404);
  });

  test("over the byte bounds: 501 turns_no_pointers (hidden, never partial), logged buffered_too_large", async () => {
    const s = store();
    const small = await putStage(s, 1, [turn(0, { content: "SMALL-STAGE" })]);
    const bomb = await putStage(s, 2, [turn(1, { content: "y".repeat(16 * 1024 * 1024 + 1) })]);
    for (const path of [T, M]) {
      const { get, logs } = api({ store: s, reads: scriptedReads({ stages: [[small.row, bomb.row]] }).reads });
      const r = await get(path);
      expect(r.status).toBe(501);
      const text = await r.text();
      expect(JSON.parse(text).error.reason).toBe("turns_no_pointers");
      expect(text).not.toContain("SMALL-STAGE");
      expect(logs.some((l) => l.reason === "buffered_too_large")).toBe(true);
    }
  });

  test("source is required for the buffered path; canonical pointers win; flag off never calls the gate", async () => {
    const gate = scriptedGate(authority("7"));
    expect((await api({ gate }).get(`/v1/sessions/${SESSION}/turns?workspace=ws-a`)).status).toBe(501);
    expect((await api({ gate, enabled: false }).get(T)).status).toBe(501);
    expect((await api({ gate, pointers: [{ harness: "claude", pointers: 1, live: 0 }] }).get(T)).status).toBe(200);
    expect(gate.calls).toHaveLength(0);
  });

  test("a stage of another user is a 500", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0, { content: "OTHER-USER" })], { header: { user_id: "u-2" } });
    const r = await api({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads }).get(T);
    expect(r.status).toBe(500);
    expect(await r.text()).not.toContain("OTHER-USER");
  });

  test("events are never buffered: same answer with the flag on, gate not called", async () => {
    const P = `/v1/sessions/${SESSION}/events?workspace=ws-a&source=claude`;
    const off = await api({ noOwnerBuffered: true }).get(P);
    const gate = scriptedGate(authority("7"));
    const on = await api({ gate }).get(P);
    expect(on.status).toBe(off.status);
    expect(strip(await json(on))).toEqual(strip(await json(off)));
    expect(on.headers.get("x-relayhistory-buffered")).toBeNull();
    expect(gate.calls).toHaveLength(0);
  });
});

describe("regression: a stored pre-v4 buffered object with a partially redacted over-cap secret", () => {
  // The v3 shape on disk: scrub.ts v3 redacted the first 4096 units of an over-cap quoted value
  // and left the rest as plaintext; the object header even claims a newer ruleset. Synthetic.
  const SUFFIX = "zzV3TAILzz5k4j3h2g1f";
  const closed = `run {"api_key": "[REDACTED]${SUFFIX}"} end`;
  const unclosed = `log "client_secret": "[REDACTED]${SUFFIX} ${SUFFIX}-again and more`;
  const meta = JSON.stringify({ nativeCli: "claude", originNode: `node {"api_key": "[REDACTED]${SUFFIX}"}`, nativeResumeId: `"password": "[REDACTED]${SUFFIX} x` });

  async function stored(admission?: { epoch: string; version: string }) {
    const s = store();
    const st = await putStage(s, 1, [turn(0, { content: closed, metadata_json: meta }), turn(1, { content: unclosed, metadata_json: meta, actor_name: `"token": "[REDACTED]${SUFFIX}` })], {
      header: { scrub: { ruleset: "relayhistory-scrub", version: 4 } },
      admission,
    });
    // Precondition: the stored bytes really hold the suffix in plaintext.
    const raw = unzip(new Uint8Array(await new Response((await s.get((await import("../../shared/vendor.ts")).objectKey(st.key))) as never).arrayBuffer()));
    expect(new TextDecoder().decode(raw)).toContain(SUFFIX);
    return { s, st };
  }

  test("session_turns and session_metadata emit no byte of the suffix; v4-redacted form", async () => {
    const { s, st } = await stored();
    const t = await api({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads }).get(T);
    expect(t.status).toBe(200);
    const tText = await t.text();
    expect(tText).not.toContain(SUFFIX);
    expect(tText).not.toContain("zzV3TAIL");
    const turns = JSON.parse(tText).turns;
    expect(turns.map((x: any) => x.content)).toEqual([`run {"api_key": "[REDACTED]"} end`, `log "client_secret": "[REDACTED]`]);
    expect(turns[1].actorName ?? turns[1].actor_name).toBe(`"token": "[REDACTED]`);
    const m = await api({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads }).get(M);
    expect(m.status).toBe(200);
    const mText = await m.text();
    expect(mText).not.toContain(SUFFIX);
    expect(JSON.parse(mText)).toEqual({ nativeCli: "claude", sessionOwner: "u-1", originNode: `node {"api_key": "[REDACTED]"}`, nativeResumeId: `"password": "[REDACTED]` });
  });

  test("the stored ruleset version does not grant eligibility: hidden admission is still not served", async () => {
    const { s, st } = await stored({ epoch: E1, version: "5" });
    for (const path of [T, M]) {
      const r = await api({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads }).get(path);
      expect(r.status).toBe(501);
      expect(await r.text()).not.toContain(SUFFIX);
    }
    const fenced = await api({ store: s, gate: scriptedGate(authority("7"), new BufferedFenced("gate_pending")), reads: scriptedReads({ stages: [[st.row]] }).reads }).get(T);
    expect(fenced.status).toBe(503);
    expect(await fenced.text()).not.toContain(SUFFIX);
  });
});
