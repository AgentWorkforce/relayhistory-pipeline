/**
 * Owner-only buffered read through the assembled read API (createReadApi): GET /v1/sessions
 * union (filters, dedupe with canonical winning, composite cursor, fenced header, strict
 * sessionSummary keys), GET /v1/sessions/:id/turns and /metadata (buffered served, fenced 503,
 * erasure race, hidden admission), cross-user 500, flag/allowlist off (no gate call, bytes
 * unchanged), events never buffered without the events flag, and the pre-v4 stored object
 * regression; then GET /v1/sessions/:id/events with READ_API_OWNER_BUFFERED_EVENTS=on (served,
 * precedence, gating order, backend transitions, hidden/fenced outcomes).
 */
import { readFileSync } from "node:fs";
import { afterEach, describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type TurnsDeps, type VerifiedIdentity } from "../app.ts";
import { decodeCompositeListCursor, encodeCompositeListCursor, type Directory, type DirectoryListRow } from "../directory.ts";
import { BUFFERED_USER_SQL, BufferedFenced, serviceBindingControlHead, sqlOwnerBufferedReads, type BufferedStageRow, type ControlHeadGate, type OwnerBufferedDeps, type OwnerBufferedReads } from "../owner-buffered.ts";
import { memoryLiveness } from "./support.ts";
import { logLine } from "../../shared/log.ts";
import { BUFFERED_STAGE_MAX_BYTES } from "../owner-buffered.ts";
import { encodeBufferedCursor, encodeCursor } from "../cursor.ts";
import { deliveryMachine } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import { TURN_ORIGIN_ID } from "../../session-ingest/turns.ts";
import { at } from "./support.ts";
import { E1, SESSION, authority, bufferedRow, deferred, fencedGate, headBody, putStage, scratch, scriptedGate, scriptedReads, turn, unzip } from "./owner-buffered-support.ts";

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
  /** READ_API_OWNER_BUFFERED_WORKSPACES; default "*" (the directory allowlist decides). */
  obWorkspaces?: ReadonlySet<string> | "*";
  pointers?: Array<{ harness: string; pointers: number; live: number }>;
  liveSessions?: Record<string, number>;
  noOwnerBuffered?: boolean;
  livePointers?: (sessionIds: string[]) => Promise<Set<string>>;
  /** The verified identity the token maps to; default ID. */
  identity?: VerifiedIdentity;
  /** READ_API_OWNER_BUFFERED_EVENTS=on (ownerBuffered.events); default off. */
  events?: boolean;
  /** READ_API_OWNER_BUFFERED_PHASE_LOG=1 (ownerBuffered.phaseLog); default off. */
  phaseLog?: boolean;
  /** READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY (ownerBuffered.stageConcurrency); default absent (serial). */
  stageConcurrency?: number;
  /** READ_API_COMPLETE_SINCE as ms; default 0. */
  completeSinceMs?: number;
  /** Erased canonical sessions (tombstones: pointers without a live pointer). */
  erased?: Array<{ source: string; sessionId: string }>;
  /**
   * A fake canonical ClickHouse: the partial-session query (needs_full = 1) answers these
   * session ids, every other query no rows. Absent: every ClickHouse call throws.
   */
  canonical?: { partial?: string[] };
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
    workspaces: h.obWorkspaces ?? "*",
    reads: h.reads ?? scriptedReads().reads,
    gate: h.gate ?? scriptedGate(authority("7")),
    store: h.store ?? store(),
    decompress: unzip,
    ...(h.events ? { events: true } : {}),
    ...(h.phaseLog ? { phaseLog: true } : {}),
    ...(h.stageConcurrency ? { stageConcurrency: h.stageConcurrency } : {}),
  };
  const chQueries: string[] = [];
  const select = h.canonical
    ? async (query: string) => {
        chQueries.push(query);
        return /needs_full/u.test(query) ? (h.canonical!.partial ?? []).map((session_id) => ({ session_id })) : [];
      }
    : async () => {
        throw new Error("no ClickHouse in these tests");
      };
  const live = memoryLiveness({ createdAt: h.liveSessions, erased: h.erased });
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return h.identity ?? ID;
    },
    ch: { database: "relay_search", async command() {}, async insert() {}, select } as never,
    liveness: h.livePointers
      ? { ...live.liveness, livePointerSessions: (_o: string, _w: string, ids: string[]) => h.livePointers!(ids) }
      : live.liveness,
    completeSinceMs: h.completeSinceMs ?? 0,
    log: (f) => logs.push(f),
    directory,
    directoryWorkspaces: "workspaces" in h ? h.workspaces : new Set(["ws-a"]),
    turns,
    ownerBuffered,
  });
  const get = (path: string, signal?: AbortSignal) =>
    signal ? app.request(new Request(`http://localhost${path}`, { headers: { authorization: "Bearer good" }, signal })) : app.request(path, { headers: { authorization: "Bearer good" } });
  return { get, logs, dirCalls, chQueries };
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
    expect(logs).toContainEqual(expect.objectContaining({ reason: "buffered_fenced", error_code: "gate_timeout" }));
  });

  test("gate moves between observations: fenced header, buffered rows dropped", async () => {
    const { get } = api({ gate: scriptedGate(authority("7"), authority("8")), reads: scriptedReads({ list: [bufferedRow()] }).reads });
    const r = await get(L);
    expect(r.headers.get("x-relayhistory-buffered")).toBe("fenced");
    expect((await json(r)).sessions).toHaveLength(1);
  });

  describe("causal interleaving: the final control-head check runs after the live-pointer await", () => {
    const ok = (b: unknown) => new Response(JSON.stringify(b), { status: 200, headers: { "content-type": "application/json", "cache-control": "private, no-store" } });
    /** The real binding gate over a mutable producer answer; live-pointer lookup blocks until released. */
    function interleaved(change: (() => Response) | null) {
      let respond: () => Response = () => ok(headBody());
      const observed: string[] = [];
      const gate = serviceBindingControlHead({ fetch: async () => (observed.push("observe"), respond()) });
      let release!: () => void;
      const released = new Promise<void>((r) => (release = r));
      let entered!: () => void;
      const blocked = new Promise<void>((r) => (entered = r));
      const livePointers = async () => {
        observed.push("liveness");
        entered();
        await released;
        return new Set<string>();
      };
      const harness = api({ gate, livePointers, reads: scriptedReads({ list: [bufferedRow()] }).reads });
      const run = async () => {
        const pending = harness.get(L);
        await blocked;
        if (change) respond = change; // the head moves while liveness is blocked
        release();
        return { r: await pending, observed, logs: harness.logs };
      };
      return run;
    }
    const changes: Array<[string, () => Response, string]> = [
      ["new version", () => ok(headBody({ sourceControlVersion: "8" })), "gate_moved"],
      ["pending=true", () => ok(headBody({ pending: true })), "gate_pending"],
      ["503", () => new Response("{}", { status: 503, headers: { "cache-control": "no-store" } }), "gate_status"],
    ];
    for (const [name, change, gateReason] of changes)
      test(`head changes while liveness is blocked (${name}): fenced, no buffered rows`, async () => {
        const { r, observed, logs } = await interleaved(change)();
        expect(observed).toEqual(["observe", "liveness", "observe"]);
        expect(r.status).toBe(200);
        expect(r.headers.get("x-relayhistory-buffered")).toBe("fenced");
        expect(r.headers.get("x-relayhistory-buffered-ids")).toBeNull();
        const body = await json(r);
        expect(body.sessions.map((s: any) => s.sessionId)).toEqual(["s-1"]);
        expect(JSON.stringify(body)).not.toContain(SESSION);
        expect(logs).toContainEqual(expect.objectContaining({ reason: "buffered_fenced", error_code: gateReason }));
      });
    test("mirror: nothing changes while liveness is blocked: the buffered row is emitted", async () => {
      const { r, observed } = await interleaved(null)();
      expect(observed).toEqual(["observe", "liveness", "observe"]);
      expect(r.status).toBe(200);
      expect(r.headers.get("x-relayhistory-buffered")).toBeNull();
      expect(r.headers.get("x-relayhistory-buffered-ids")).not.toBeNull();
      expect((await json(r)).sessions.map((s: any) => s.sessionId)).toContain(SESSION);
    });
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
    for (const [err, subtype, sqlstate] of [[Object.assign(new Error("canceling statement due to statement timeout"), { code: "57014" }), "rpc_timeout", "57"], [new Error("connection terminated"), "rpc_error", undefined]] as const) {
      const run = (async (_o: string, _w: string, work: (q: unknown) => Promise<unknown>) =>
        work({ async query(sql: string) { if (sql.includes("buffered_sessions_for")) throw err; return { rows: [] }; } })) as never;
      const { get, logs } = api({ reads: sqlOwnerBufferedReads(run) });
      const r = await get(L);
      expect(r.status).toBe(200);
      expect(r.headers.get("x-relayhistory-buffered")).toBe("fenced");
      const fenced = logs.find((l) => l.reason === "buffered_fenced");
      expect(fenced).toMatchObject({ error_code: subtype });
      expect(fenced?.sqlstate).toBe(sqlstate);
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

  test("the owner-buffered canary allowlist is required on top of the directory allowlist", async () => {
    const base = await json(await api({ noOwnerBuffered: true }).get(L));
    // Directory-enabled workspace, but not on READ_API_OWNER_BUFFERED_WORKSPACES: exactly as without the flag.
    for (const obWorkspaces of [new Set<string>(), new Set(["ws-b"])]) {
      const gate = scriptedGate(authority("7"));
      const r = await api({ obWorkspaces, gate, reads: scriptedReads({ list: [bufferedRow()] }).reads }).get(L);
      expect(strip(await json(r))).toEqual(strip(base));
      expect(r.headers.get("x-relayhistory-buffered-ids")).toBeNull();
      expect(gate.calls).toHaveLength(0);
    }
    // On both lists: served.
    const gate = scriptedGate(authority("7"));
    const r = await api({ obWorkspaces: new Set(["ws-a"]), gate, reads: scriptedReads({ list: [bufferedRow()] }).reads }).get(L);
    expect(r.headers.get("x-relayhistory-buffered-ids")).toBe(`claude:${SESSION}`);
    // On the owner-buffered list but not the directory list: still 501, gate never called.
    const gate2 = scriptedGate(authority("7"));
    const r2 = await api({ workspaces: new Set(["ws-b"]), obWorkspaces: new Set(["ws-a"]), gate: gate2 }).get(L);
    expect(r2.status).toBe(501);
    expect(gate2.calls).toHaveLength(0);
  });
});

const T = `/v1/sessions/${SESSION}/turns?workspace=ws-a&source=claude`;
const M = `/v1/sessions/${SESSION}/metadata?workspace=ws-a&source=claude`;
describe("buffered fence logs: closed subtype as error_code, SQLSTATE class only, through the real log filter", () => {
  const SECRET = "sk-live-FENCESECRET0123456789abcdef";
  const line = (fields: Record<string, unknown>) => JSON.parse(logLine("info", "read api", fields)) as Record<string, unknown>;
  const fencedLine = (logs: Record<string, unknown>[]) => {
    const fields = logs.find((l) => l.reason === "buffered_fenced");
    expect(fields).toBeDefined();
    return { fields: fields!, text: logLine("info", "read api", fields!) };
  };
  const rpcFailing = (err: unknown) =>
    sqlOwnerBufferedReads((async (_o: string, _w: string, work: (q: unknown) => Promise<unknown>) =>
      work({ async query(sql: string) { if (sql.includes("buffered_sessions_for")) throw err; return { rows: [] }; } })) as never);

  test("a known gate subtype survives the log filter as error_code; no gate field", async () => {
    const { get, logs } = api({ gate: fencedGate("gate_status"), reads: scriptedReads({ list: [bufferedRow()] }).reads });
    expect((await get(L)).headers.get("x-relayhistory-buffered")).toBe("fenced");
    const { fields, text } = fencedLine(logs);
    expect(line(fields)).toMatchObject({ reason: "buffered_fenced", error_code: "gate_status" });
    expect("gate" in line(fields)).toBe(false);
    expect("sqlstate" in line(fields)).toBe(false);
    expect(text).not.toContain('"gate"');
  });

  test("an unknown reason (long, secret-looking) is logged as fence_other and never appears", async () => {
    const unknown = `gate_${SECRET}_${"x".repeat(300)}`;
    const { get, logs } = api({ gate: fencedGate(unknown), reads: scriptedReads({ list: [bufferedRow()] }).reads });
    expect((await get(L)).headers.get("x-relayhistory-buffered")).toBe("fenced");
    const { fields, text } = fencedLine(logs);
    expect(line(fields)).toMatchObject({ reason: "buffered_fenced", error_code: "fence_other" });
    expect(text).not.toContain("FENCESECRET");
    expect(text).not.toContain("xxxxxxxxxx");
    expect(JSON.stringify(logs)).not.toContain("FENCESECRET");
  });

  test("rpc_error from a Postgres error 42501: error_code rpc_error, sqlstate 42, no message or full code", async () => {
    const err = Object.assign(new Error(`permission denied for function buffered_sessions_for ${SECRET}`), { code: "42501", detail: SECRET });
    const { get, logs } = api({ reads: rpcFailing(err) });
    const r = await get(L);
    expect(r.status).toBe(200);
    expect(r.headers.get("x-relayhistory-buffered")).toBe("fenced");
    const { fields, text } = fencedLine(logs);
    expect(line(fields)).toMatchObject({ reason: "buffered_fenced", error_code: "rpc_error", sqlstate: "42" });
    expect(text).not.toContain("FENCESECRET");
    expect(text).not.toContain("permission denied");
    expect(text).not.toContain("42501");
    expect(JSON.stringify(logs)).not.toContain("FENCESECRET");
  });

  test("early path: the initial stages RPC failing logs buffered_unverified with the subtype and SQLSTATE class only", async () => {
    const stagesFailing = (err: unknown) =>
      sqlOwnerBufferedReads((async (_o: string, _w: string, work: (q: unknown) => Promise<unknown>) =>
        work({ async query(sql: string) { if (sql.includes("buffered_session_stages_for")) throw err; return { rows: [] }; } })) as never);
    const cases: Array<[unknown, string, string | undefined, string]> = [
      [Object.assign(new Error(`permission denied ${SECRET}`), { code: "42501" }), "rpc_error", "42", "42501"],
      [Object.assign(new Error(`canceling statement due to statement timeout ${SECRET}`), { code: "57014" }), "rpc_timeout", "57", "57014"],
      [Object.assign(new Error(`socket hang up ${SECRET}`), { code: "ECONNRESET" }), "rpc_error", undefined, "ECONNRESET"],
    ];
    for (const path of [T, M]) {
      const main = await api({ enabled: false }).get(path);
      const mainBody = strip(await json(main));
      for (const [err, subtype, sqlstate, code] of cases) {
        const { get, logs } = api({ reads: stagesFailing(err) });
        const r = await get(path);
        expect(r.status).toBe(main.status);
        expect(r.status).toBe(501);
        expect(r.headers.get("x-relayhistory-buffered")).toBeNull();
        expect(strip(await json(r))).toEqual(mainBody);
        const fields = logs.find((l) => l.reason === "buffered_unverified");
        expect(fields).toBeDefined();
        const text = logLine("info", "read api", fields!);
        const out = line(fields!);
        expect(out).toMatchObject({ reason: "buffered_unverified", error_code: subtype });
        expect(out.sqlstate).toBe(sqlstate);
        expect("sqlstate" in out).toBe(sqlstate !== undefined);
        expect("gate" in out).toBe(false);
        expect(text).not.toContain("FENCESECRET");
        expect(text).not.toContain(code);
        expect(JSON.stringify(logs)).not.toContain("FENCESECRET");
      }
    }
  });

  test("a malformed code yields no sqlstate field", async () => {
    for (const code of ["x", "4", "ab501", " 42", "ECONNRESET", "42-this-is-not-sqlstate", "42", "4250", "42501x", 42501, null, { toString: () => "42501" }]) {
      const { get, logs } = api({ reads: rpcFailing(Object.assign(new Error("boom"), { code })) });
      expect((await get(L)).headers.get("x-relayhistory-buffered")).toBe("fenced");
      const { fields, text } = fencedLine(logs);
      expect("sqlstate" in fields).toBe(false);
      expect(line(fields)).toMatchObject({ error_code: "rpc_error" });
      expect("sqlstate" in line(fields)).toBe(false);
      expect(text).not.toContain("boom");
    }
  });
});

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
      for (const subtype of ["gate_unbound", "gate_timeout", "gate_shape"]) {
        // A caller with no buffered stages, and one with stages: the gate failing first answers like main either way.
        for (const stages of [[[]], [[st.row]]] as const) {
          const { reads, calls } = scriptedReads({ stages: stages as never });
          const { get, logs } = api({ store: s, gate: fencedGate(subtype), reads });
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
          expect(logs.find((l) => l.reason === "buffered_unverified")).toMatchObject({ error_code: subtype });
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

  test("events are never buffered without READ_API_OWNER_BUFFERED_EVENTS: same answer with owner-buffered on, gate not called", async () => {
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

/**
 * All-workspaces mode: READ_API_OWNER_BUFFERED_WORKSPACES="*" with READ_API_DIRECTORY_WORKSPACES="*"
 * (render-deploy-config.ts accepts "*" only then). The mode widens the workspace allowlist and
 * nothing else: every buffered row still comes from the SECURITY DEFINER readers, which bind the
 * verified user per call and gate on that user's own G2 parity row (rule 4, mandatory) in
 * sessions.owner_buffered_read_scope, and from the control-head gate for exactly that user.
 */
describe("all-workspaces mode does not bypass the per-user privacy gate", () => {
  const ALL = { workspaces: "*" as const, obWorkspaces: "*" as const };
  const MIGRATION = readFileSync(new URL("../../../supabase/migrations/20261105043700_owner_buffered_read.sql", import.meta.url), "utf8");

  /**
   * A TenantRunner standing in for the read role: rows only for a user with a G2 parity row in
   * this (org, workspace), as owner_buffered_read_scope's gate_open; records every binding.
   */
  function parityRunner(parity: Set<string>, stageRow?: unknown) {
    const bound: Array<{ org: string; ws: string; user: unknown; sql: string; params: unknown[] }> = [];
    const run = (async (org: string, ws: string, work: (q: unknown) => Promise<unknown>) => {
      let user: unknown = null;
      return work({
        async query(sql: string, params: unknown[] = []) {
          if (sql === BUFFERED_USER_SQL) user = params[0];
          bound.push({ org, ws, user, sql, params });
          const open = typeof user === "string" && parity.has(`${org}/${ws}/${user}`);
          if (!open) return { rows: [] };
          if (sql.includes("buffered_sessions_for")) return { rows: [{ ...bufferedRow(), user_id: user, first_ts: new Date("2026-11-05T12:00:00Z"), last_ts: new Date("2026-11-05T12:05:00Z") }] };
          if (sql.includes("buffered_session_stages_for") && stageRow) return { rows: [stageRow] };
          return { rows: [] };
        },
      });
    }) as never;
    return { run, bound };
  }

  test("a user without a G2 parity row sees no buffered row, turns or metadata in all-workspaces mode", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0)]);
    const base = await json(await api({ ...ALL, noOwnerBuffered: true }).get(L));
    // Parity exists only for another user and for this user in another workspace.
    const { run, bound } = parityRunner(new Set(["org-a/ws-a/u-2", "org-a/ws-b/u-1"]), st.row);
    const gate = scriptedGate(authority("7"));
    const { get } = api({ ...ALL, gate, store: s, reads: sqlOwnerBufferedReads(run) });
    const r = await get(L);
    expect(r.status).toBe(200);
    expect(strip(await json(r))).toEqual(strip(base));
    expect(r.headers.get("x-relayhistory-buffered-ids")).toBeNull();
    expect((await get(T)).status).toBe(501);
    expect((await get(M)).status).not.toBe(200);
    // Every reader call was bound to the verified user and the token's workspace, never a wildcard.
    const rpcs = bound.filter((x) => x.sql.includes("sessions.buffered_"));
    expect(rpcs.some((b) => b.sql.includes("buffered_sessions_for"))).toBe(true);
    expect(rpcs.some((b) => b.sql.includes("buffered_session_stages_for"))).toBe(true);
    for (const b of rpcs) {
      expect([b.org, b.ws, b.user]).toEqual(["org-a", "ws-a", "u-1"]);
      expect(b.params[0]).toBe("ws-a");
    }
    expect(bound.some((b) => b.ws === "*" || b.params.includes("*"))).toBe(false);
    // The control-head gate is observed for exactly the caller.
    for (const scope of gate.calls) expect(scope).toEqual({ orgId: "org-a", workspaceId: "ws-a", userId: "u-1" });
  });

  test("mirror: the same user with their own G2 parity row is served in all-workspaces mode", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0)]);
    const { run } = parityRunner(new Set(["org-a/ws-a/u-1"]), st.row);
    const { get } = api({ ...ALL, store: s, reads: sqlOwnerBufferedReads(run) });
    const r = await get(L);
    expect(r.headers.get("x-relayhistory-buffered-ids")).toBe(`claude:${SESSION}`);
    const t = await get(T);
    expect(t.status).toBe(200);
    expect(t.headers.get("x-relayhistory-buffered")).toBe("1");
  });

  test("a token without a user gets no buffered read in all-workspaces mode: gate and readers never called", async () => {
    const base = await json(await api({ ...ALL, noOwnerBuffered: true, identity: { ...ID, userId: "" } }).get(L));
    const { run, bound } = parityRunner(new Set(["org-a/ws-a/u-1", "org-a/ws-a/"]));
    const gate = scriptedGate(authority("7"));
    const r = await api({ ...ALL, gate, identity: { ...ID, userId: "" }, reads: sqlOwnerBufferedReads(run) }).get(L);
    expect(strip(await json(r))).toEqual(strip(base));
    expect(r.headers.get("x-relayhistory-buffered-ids")).toBeNull();
    expect(gate.calls).toHaveLength(0);
    expect(bound).toHaveLength(0);
  });

  test("the authority gate still fences in all-workspaces mode", async () => {
    const { run } = parityRunner(new Set(["org-a/ws-a/u-1"]));
    const r = await api({ ...ALL, gate: fencedGate("gate_timeout"), reads: sqlOwnerBufferedReads(run) }).get(L);
    expect(r.status).toBe(200);
    expect(r.headers.get("x-relayhistory-buffered")).toBe("fenced");
    expect(r.headers.get("x-relayhistory-buffered-ids")).toBeNull();
  });

  test("gate source: both readers go through owner_buffered_read_scope, which requires the caller's own parity row and workspace", () => {
    const scope = MIGRATION.slice(MIGRATION.indexOf("CREATE FUNCTION sessions.owner_buffered_read_scope("), MIGRATION.indexOf("$scope$;"));
    expect(scope).toContain("FROM sessions.owner_buffered_parity AS bp");
    expect(scope).toMatch(/WHERE bp\.org_id = v_org AND bp\.workspace_id = v_workspace AND bp\.user_id = v_user;\s+v_open := FOUND;/);
    expect(scope).toContain("p_workspace_id IS DISTINCT FROM v_workspace");
    for (const reader of ["buffered_sessions_for", "buffered_session_stages_for"]) {
      const at = MIGRATION.indexOf(`CREATE FUNCTION sessions.${reader}(`);
      expect(at).toBeGreaterThan(0);
      expect(MIGRATION.slice(at, at + 2500)).toContain("FROM sessions.owner_buffered_read_scope(p_workspace,");
    }
  });
});

describe("GET /v1/sessions/:id/events with READ_API_OWNER_BUFFERED_EVENTS=on", () => {
  const TS = 1_793_000_000_000;
  const P = `/v1/sessions/${SESSION}/events?workspace=ws-a&source=claude`;
  const KEY = at({ source: "claude", sessionId: SESSION });
  const ev = (id: string, over: Record<string, unknown> = {}) => ({ kind: "session_event", origin_id: TURN_ORIGIN_ID, record_id: id, revision: "1", operation: "upsert", received_at: "2026-11-05T12:00:00Z", payload: { text: `buffered ${id}`, ts_ms: TS, role: "user", ...over } });
  async function staged(opts: Parameters<typeof putStage>[3] = {}) {
    const s = store();
    const st = await putStage(s, 1, [ev("e1"), ev("e2"), ev("e3", { ts_ms: TS + 1 })], opts);
    return { s, st, ...scriptedReads({ stages: [[st.row]] }) };
  }
  /** Status, buffered header and body without the per-request correlation id. */
  const answer = async (r: Response) => ({ status: r.status, buffered: r.headers.get("x-relayhistory-buffered"), body: strip(await json(r)) });
  const offAnswer = (h: Harness = {}, path = P) => api({ ...h, noOwnerBuffered: true }).get(path).then(answer);

  test("served: header, the projected rows in canonical order, buffered cursor pages, gate observed twice per read", async () => {
    const { s, reads, calls } = await staged();
    const gate = scriptedGate(authority("7"));
    const { get, logs } = api({ events: true, store: s, reads, gate });
    const r = await get(P);
    expect(r.status).toBe(200);
    expect(r.headers.get("x-relayhistory-buffered")).toBe("1");
    const b = await json(r);
    expect(Object.keys(b).sort()).toEqual(["correlationId", "events", "nextCursor", "sessionId"]);
    expect(b.events.map((e: any) => [e.eventId, e.sessionId, e.source, e.userId])).toEqual([
      [`${deliveryMachine("ws-a", TURN_ORIGIN_ID)}:e1`, SESSION, "claude", "u-1"],
      [`${deliveryMachine("ws-a", TURN_ORIGIN_ID)}:e2`, SESSION, "claude", "u-1"],
      [`${deliveryMachine("ws-a", TURN_ORIGIN_ID)}:e3`, SESSION, "claude", "u-1"],
    ]);
    expect(b.nextCursor).toBeNull();
    expect(gate.calls).toHaveLength(2);
    expect(calls.map((c) => c.fn)).toEqual(["stages", "stages"]);
    expect(logs.some((l) => l.reason === "buffered_events_served")).toBe(true);
    expect(JSON.stringify(b) + JSON.stringify(logs)).not.toMatch(/buffered-batches|stage_key|ordinal/u);

    const first = await json(await get(`${P}&limit=2`));
    expect(first.events).toHaveLength(2);
    expect(typeof first.nextCursor).toBe("string");
    const second = await get(`${P}&limit=2&cursor=${encodeURIComponent(first.nextCursor)}`);
    expect(second.headers.get("x-relayhistory-buffered")).toBe("1");
    const sb = await json(second);
    expect(sb.events.map((e: any) => e.eventId)).toEqual([b.events[2].eventId]);
    expect(sb.nextCursor).toBeNull();
    const desc = await json(await get(`${P}&order=desc`));
    expect(desc.events.map((e: any) => e.eventId)).toEqual([...b.events].reverse().map((e: any) => e.eventId));
  });

  test("gating: flag off, owner-buffered off, no source, workspace off either allowlist: no gate or RPC call, bytes identical to no owner-buffered", async () => {
    const cases: Array<[string, Harness, string]> = [
      ["events flag off", { events: false }, P],
      ["owner-buffered disabled", { events: true, enabled: false }, P],
      ["no source", { events: true }, `/v1/sessions/${SESSION}/events?workspace=ws-a`],
      ["not on the owner-buffered allowlist", { events: true, obWorkspaces: new Set(["ws-other"]) }, P],
      ["not on the directory allowlist", { events: true, workspaces: new Set(["ws-other"]) }, P],
    ];
    for (const [, h, path] of cases) {
      const { s, reads, calls } = await staged();
      const gate = scriptedGate(authority("7"));
      const on = await api({ ...h, store: s, reads, gate }).get(path).then(answer);
      expect(on).toEqual(await offAnswer(h, path));
      expect(gate.calls).toHaveLength(0);
      expect(calls).toHaveLength(0);
    }
  });

  test("history floor: flag off a buffered-only session is 501 history_floor_unknown_session; on, the buffered answer serves (decided before sessionGate)", async () => {
    const off = await offAnswer({ completeSinceMs: TS });
    expect(off.status).toBe(501);
    expect(off.body.error.reason).toBe("history_floor_unknown_session");
    const { s, reads } = await staged();
    const flagOff = await api({ completeSinceMs: TS, store: s, reads }).get(P).then(answer);
    expect(flagOff).toEqual(off);
    const on = await api({ events: true, completeSinceMs: TS, store: s, reads }).get(P);
    expect(on.status).toBe(200);
    expect((await json(on)).events).toHaveLength(3);
  });

  test("precedence: any canonical pointer and canonical serves the whole response; the buffered gate and RPC are never called", async () => {
    const { s, reads, calls } = await staged();
    const gate = scriptedGate(authority("7"));
    const h: Harness = { events: true, store: s, reads, gate, liveSessions: { [KEY]: TS }, canonical: {} };
    const on = await api(h).get(P).then(answer);
    expect(on).toEqual(await offAnswer(h));
    expect(on.status).toBe(200);
    expect(on.buffered).toBeNull();
    expect(gate.calls).toHaveLength(0);
    expect(calls).toHaveLength(0);
  });

  test("transition buffered -> canonical: pointers appear (needs_full = 1 too); a buffered cursor is refused 409, never continued into canonical", async () => {
    const { s, reads } = await staged();
    const first = await json(await api({ events: true, store: s, reads }).get(`${P}&limit=1`));
    const cursor = encodeURIComponent(first.nextCursor);
    for (const partial of [[SESSION], []]) {
      const gate = scriptedGate(authority("7"));
      const { get, chQueries } = api({ events: true, store: s, reads, gate, liveSessions: { [KEY]: TS }, canonical: { partial } });
      const fresh = await get(P);
      if (partial.length) {
        expect(fresh.status).toBe(501);
        expect((await json(fresh)).error.reason).toBe("session_partial");
      } else expect(fresh.status).toBe(200);
      const before = chQueries.length;
      const r = await get(`${P}&cursor=${cursor}`);
      expect(r.status).toBe(409);
      const b = await json(r);
      expect(b.error.code).toBe("cursor_backend_transition");
      expect(r.headers.get("x-relayhistory-buffered")).toBeNull();
      expect(chQueries.length).toBe(before);
      expect(gate.calls).toHaveLength(0);
    }
  });

  test("canonical tombstone: no cursor answers the canonical empty page (no buffered read); a buffered cursor is refused 409 (no resurrection)", async () => {
    const { s, reads, calls } = await staged();
    const first = await json(await api({ events: true, store: s, reads }).get(`${P}&limit=1`));
    const gate = scriptedGate(authority("7"));
    const before = calls.length;
    const h: Harness = { events: true, store: s, reads, gate, erased: [{ source: "claude", sessionId: SESSION }] };
    const r = await api(h).get(P).then(answer);
    expect(r).toEqual(await offAnswer(h));
    expect(r).toMatchObject({ status: 200, buffered: null, body: { events: [], nextCursor: null } });
    const resumed = await api(h).get(`${P}&cursor=${encodeURIComponent(first.nextCursor)}`);
    expect(resumed.status).toBe(409);
    expect((await json(resumed)).error.code).toBe("cursor_backend_transition");
    expect(gate.calls).toHaveLength(0);
    expect(calls.length).toBe(before);
  });

  test("transition canonical -> buffered: a canonical cursor on a buffered session is refused 409", async () => {
    const { s, reads } = await staged();
    const c = encodeCursor({ tsMs: TS, sessionId: SESSION, source: "claude", machineId: "m", kind: "message", eventId: "e0" });
    const r = await api({ events: true, store: s, reads }).get(`${P}&cursor=${encodeURIComponent(c)}`);
    expect(r.status).toBe(409);
    expect((await json(r)).error.code).toBe("cursor_backend_transition");
  });

  test("a buffered cursor where the buffered session is hidden: 409; with the events flag off: 400 bad_request (invalid cursor) as today; tampered or foreign: as today", async () => {
    const b = encodeBufferedCursor({ tsMs: TS, sessionId: SESSION, source: "claude", machineId: "m", kind: "message", eventId: "e0" });
    const old = await staged({ admission: { epoch: E1, version: "5" } });
    const hidden = await api({ events: true, store: old.s, reads: old.reads }).get(`${P}&cursor=${encodeURIComponent(b)}`);
    expect(hidden.status).toBe(409);
    const off = await api({ store: old.s, reads: old.reads }).get(`${P}&cursor=${encodeURIComponent(b)}`).then(answer);
    expect(off.status).toBe(400);
    expect(off.body.error.code).toBe("bad_request");
    expect(off).toEqual(await offAnswer({}, `${P}&cursor=${encodeURIComponent(b)}`));
    const { s, reads } = await staged();
    for (const bad of [`b96e1:${Buffer.from(JSON.stringify([TS, SESSION, "claude", "m", "k", "e", "ob9"])).toString("base64url")}`, "b96e1:%%%", "zz:1"]) {
      const path = `${P}&cursor=${encodeURIComponent(bad)}`;
      const r = await api({ events: true, store: s, reads }).get(path).then(answer);
      expect(r).toEqual(await offAnswer({}, path));
      expect(r.status).toBe(bad === "zz:1" ? 501 : 400);
    }
  });

  test("another user's session (the RPC returns none of their stages): the same answer as the flag off", async () => {
    const reads = scriptedReads({ stages: [[]] });
    const other: VerifiedIdentity = { ...ID, userId: "u-2" };
    const on = await api({ events: true, identity: other, reads: reads.reads }).get(P).then(answer);
    expect(on).toEqual(await offAnswer({ identity: other }));
    expect(reads.calls.every((c) => c.scope.userId === "u-2")).toBe(true);
  });

  test("a stage whose header names another user: 500, no content", async () => {
    const { s, reads } = await staged({ header: { user_id: "u-2" } });
    const r = await api({ events: true, store: s, reads }).get(P);
    expect(r.status).toBe(500);
    expect(await r.text()).not.toContain("buffered e1");
  });

  test("fenced after a candidate (head moved, pending): 503 buffered_fenced, no content", async () => {
    for (const second of [authority("8"), new BufferedFenced("gate_pending")]) {
      const { s, reads } = await staged();
      const r = await api({ events: true, store: s, reads, gate: scriptedGate(authority("7"), second) }).get(P);
      expect(r.status).toBe(503);
      const text = await r.text();
      expect(JSON.parse(text).error.code).toBe("buffered_fenced");
      expect(text).not.toContain("buffered e1");
    }
  });

  test("hidden outcomes answer exactly as the flag off: unverified first gate, too large, old admission, recheck changed, projection refused", async () => {
    const off = await offAnswer();
    const big = await staged();
    const variants: Array<[string, Harness]> = [
      ["unverified", { gate: fencedGate("gate_timeout"), ...(await staged()) }],
      ["too large", { store: big.s, reads: scriptedReads({ stages: [[{ ...big.st.row, stage_bytes: BUFFERED_STAGE_MAX_BYTES + 1 }]] }).reads }],
      ["old admission", await staged({ admission: { epoch: E1, version: "5" } })],
    ];
    const changed = await staged();
    const other = await putStage(changed.s, 2, [ev("e9")]);
    variants.push(["recheck changed", { store: changed.s, reads: scriptedReads({ stages: [[changed.st.row, other.row], [changed.st.row]] }).reads }]);
    const conv = store();
    const cst = await putStage(conv, 1, [{ ...ev("c1"), kind: "convergence_event", payload: { source: "claude", session_id: SESSION, machine_id: "x", user_id: "u-1", event_id: "c1", kind: "prompt", ts: "2026-11-05T12:00:00.000Z", content: "direct" } }]);
    variants.push(["projection refused", { store: conv, reads: scriptedReads({ stages: [[cst.row]] }).reads }]);
    for (const [name, h] of variants) {
      const on = await api({ events: true, ...h }).get(P).then(answer);
      expect([name, on]).toEqual([name, off]);
    }
  });

  test("erased at the recheck: an empty page with the buffered header", async () => {
    const { s, st } = await staged();
    const r = await api({ events: true, store: s, reads: scriptedReads({ stages: [[st.row], []] }).reads }).get(P);
    expect(r.status).toBe(200);
    expect(r.headers.get("x-relayhistory-buffered")).toBe("1");
    expect(strip(await json(r))).toEqual({ sessionId: SESSION, events: [], nextCursor: null, correlationId: undefined });
  });
});

describe("GET /v1/sessions/:id/events: READ_API_OWNER_BUFFERED_PHASE_LOG and READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY", () => {
  const TS = 1_793_000_000_000;
  const P = `/v1/sessions/${SESSION}/events?workspace=ws-a&source=claude`;
  const FIELDS = ["session_start_ms", "control_head_1_ms", "stages_sql_ms", "s3_fetch_ms", "s3_stage_count", "recheck_ms", "control_head_2_ms", "fold_ms"];
  const NEW = [...FIELDS, "aborted_phase"];
  const ev = (id: string, over: Record<string, unknown> = {}) => ({ kind: "session_event", origin_id: TURN_ORIGIN_ID, record_id: id, revision: "1", operation: "upsert", received_at: "2026-11-05T12:00:00Z", payload: { text: `buffered ${id}`, ts_ms: TS, role: "user", ...over } });
  async function staged(n = 3) {
    const s = store();
    const rows: BufferedStageRow[] = [];
    for (let i = 1; i <= n; i++) rows.push((await putStage(s, i, [ev(`e${i}`, { ts_ms: TS + i })])).row);
    return { s, rows };
  }
  const requestLine = (logs: Record<string, unknown>[]) => {
    // The middleware's request line: status and duration, no reason.
    const lines = logs.filter((l) => "duration_ms" in l && !("reason" in l));
    expect(lines).toHaveLength(1);
    return lines[0]!;
  };

  test("phase log off (default): the request line carries no new field, served or failed", async () => {
    const { s, rows } = await staged();
    for (const gate of [scriptedGate(authority("7")), scriptedGate(authority("7"), authority("8"))]) {
      const { get, logs } = api({ events: true, store: s, reads: scriptedReads({ stages: [rows] }).reads, gate, stageConcurrency: 2 });
      await get(P);
      expect(Object.keys(requestLine(logs)).sort()).toEqual(["correlation_id", "duration_ms", "route", "status"]);
      for (const l of logs) for (const f of NEW) expect(l, f).not.toHaveProperty(f);
    }
  });

  test("phase log on, served: every phase field a non-negative integer, s3_stage_count the stage count, no aborted_phase, nothing else added", async () => {
    for (const stageConcurrency of [undefined, 2]) {
      const { s, rows } = await staged(3);
      const { get, logs } = api({ events: true, phaseLog: true, store: s, reads: scriptedReads({ stages: [rows] }).reads, stageConcurrency });
      const r = await get(P);
      expect(r.status).toBe(200);
      expect((await json(r)).events).toHaveLength(3);
      const line = requestLine(logs);
      expect(Object.keys(line).sort()).toEqual(["correlation_id", "duration_ms", "route", "status", ...FIELDS].sort());
      for (const f of FIELDS) expect(Number.isInteger(line[f]) && (line[f] as number) >= 0, f).toBe(true);
      expect(line.s3_stage_count).toBe(3);
      const text = JSON.stringify(logs.map(({ correlation_id: _c, ...l }) => l));
      expect(text).not.toMatch(/buffered-batches|stage_key|ordinal|sess-|"e1|buffered e|u-1|org-a|ws-a/u);
    }
  });

  test("phase log on, failed: the request line carries the reached phases and aborted_phase; the status is unchanged from flag off", async () => {
    const { s, rows } = await staged(2);
    const moved = () => scriptedGate(authority("7"), authority("8"));
    const off = await api({ events: true, store: s, reads: scriptedReads({ stages: [rows] }).reads, gate: moved() }).get(P);
    const { get, logs } = api({ events: true, phaseLog: true, store: s, reads: scriptedReads({ stages: [rows] }).reads, gate: moved() });
    const r = await get(P);
    expect(r.status).toBe(off.status);
    const line = requestLine(logs);
    expect(line).toMatchObject({ aborted_phase: "control_head_2", s3_stage_count: 2 });
    for (const f of ["session_start_ms", "control_head_1_ms", "stages_sql_ms", "s3_fetch_ms", "recheck_ms", "control_head_2_ms"]) expect(typeof line[f], f).toBe("number");

    // A failed S3 fetch under concurrency: same status as serial, aborted_phase s3_fetch, later phases absent.
    const failing = { ...s, getObject: async () => { throw Object.assign(new Error("synthetic"), { name: "S3Down" }); } } as typeof s;
    const serial = await api({ events: true, store: failing, reads: scriptedReads({ stages: [rows] }).reads }).get(P);
    const h = api({ events: true, phaseLog: true, stageConcurrency: 2, store: failing, reads: scriptedReads({ stages: [rows] }).reads });
    const fr = await h.get(P);
    expect(fr.status).toBe(serial.status);
    expect(await strip(await json(fr))).toEqual(strip(await json(serial)));
    const fl = requestLine(h.logs);
    expect(fl).toMatchObject({ aborted_phase: "s3_fetch", s3_stage_count: 2 });
    expect(fl).not.toHaveProperty("recheck_ms");
    expect(fl).not.toHaveProperty("control_head_2_ms");
  });

  test("phase log on, canceled request: one request_aborted line with the phase in progress; the request line does not repeat the fields", async () => {
    const { s, rows } = await staged(4);
    const held = deferred();
    let started = 0;
    const slow = new Proxy(s, {
      get(t, prop) {
        if (prop === "getObject") return async (...a: Parameters<typeof s.getObject>) => (started++, await held.promise, t.getObject(...a));
        const v = Reflect.get(t, prop, t);
        return typeof v === "function" ? v.bind(t) : v;
      },
    });
    const ac = new AbortController();
    const { get, logs } = api({ events: true, phaseLog: true, stageConcurrency: 2, store: slow, reads: scriptedReads({ stages: [rows] }).reads });
    const pending = get(P, ac.signal);
    while (started < 2) await new Promise((r) => setTimeout(r, 1));
    ac.abort();
    held.resolve();
    const r = await pending;
    expect(r.status).toBe(503);
    expect(started).toBe(2);
    const aborted = logs.filter((l) => l.reason === "request_aborted");
    expect(aborted).toHaveLength(1);
    expect(aborted[0]).toMatchObject({ route: "/v1/sessions/:sessionId/events", aborted_phase: "s3_fetch", s3_stage_count: 4 });
    expect(typeof aborted[0]!.duration_ms).toBe("number");
    expect(typeof aborted[0]!.correlation_id).toBe("string");
    for (const l of logs) if (l !== aborted[0]) for (const f of NEW) expect(l, f).not.toHaveProperty(f);
  });

  test("phase log on, canceled after the response: no request_aborted line (the listener is removed)", async () => {
    const { s, rows } = await staged(1);
    const ac = new AbortController();
    const { get, logs } = api({ events: true, phaseLog: true, store: s, reads: scriptedReads({ stages: [rows] }).reads });
    expect((await get(P, ac.signal)).status).toBe(200);
    ac.abort();
    expect(logs.some((l) => l.reason === "request_aborted")).toBe(false);
    expect(requestLine(logs)).toHaveProperty("fold_ms");
  });
});
