/**
 * The per-hit R2 privacy gate for buffered-origin rows (buffered-search-gate.ts):
 * every fail-closed case drops the hit, the positive case keeps it, canonical
 * rows are untouched, and the control head is asked for the hit's OWNER.
 * The ledger verdicts (sessions.buffered_search_gate_for, 043860) are stubbed
 * through GateStore; the head producer through a stub binding. The reader's
 * SQL behaviour is proven in buffered-search-gate.pglite.test.ts.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ClickHouseError, type ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import {
  CONTROL_HEAD_PATH,
  GATE_SQL,
  GATE_SQL_REASONS,
  MAX_GATE_HITS,
  MAX_GATE_OWNERS,
  createBufferedSearchGate,
  parseBufferedSearchGate,
  parseControlHead,
  refKey,
  serviceBindingControlHeads,
  sqlGateStore,
  type AdmissionRef,
  type GateSqlReason,
  type GateStore,
  type GateRow,
  type SessionReadControlHead,
  type Owner,
  type ServiceBinding,
} from "../buffered-search-gate.ts";
import type { TenantRunner } from "../liveness.ts";
import { buildCandidateQuery, buildNewestQuery } from "../queries.ts";
import { memoryLiveness } from "./support.ts";

const EPOCH = "a".repeat(64);
const OTHER_EPOCH = "b".repeat(64);
const BASELINE = "c".repeat(64);
const DIGEST = "d".repeat(64);
const CLAIM = "11111111-2222-4333-8444-555555555555";
const CLAIM_2 = "11111111-2222-4333-8444-666666666666";
const ORG = "org-a";
const WS = "ws-a";

function head(owner: Owner, over: Record<string, unknown> = {}) {
  return {
    protocol: "relay-session-read-control-head-v1",
    version: 1,
    issuer: "relayhistory-cloud",
    tenant: { orgId: owner.orgId, workspaceId: owner.workspaceId, userId: owner.userId },
    controlEpoch: EPOCH,
    cutover: { epoch: EPOCH, status: "active", mode: "ingest" },
    privacyBaselineSha256: BASELINE,
    sourceControlEpoch: EPOCH,
    sourceControlVersion: "7",
    sourceControlDigest: DIGEST,
    sourceControlKind: "selection",
    pending: false,
    ...over,
  };
}

/** A stub SessionReadPrivacyControls binding: answers per owner, records each request. */
function binding(answer: (owner: Owner, req: Request) => Response | Promise<Response> = (o) => Response.json(head(o))) {
  const requests: Request[] = [];
  const b: ServiceBinding = {
    async fetch(req) {
      requests.push(req);
      const owner = { orgId: req.headers.get("x-relayhistory-org-id")!, workspaceId: req.headers.get("x-relayhistory-workspace-id")!, userId: req.headers.get("x-relayhistory-user-id")! };
      return answer(owner, req);
    },
  };
  return { b, requests };
}

type Verdict = GateSqlReason | "admission_unknown_reason" | null;

/** A stub GateStore: per-ref verdict (default visible; undefined = no row), records each call. */
function store(over: { verdict?: (ref: AdmissionRef, head: SessionReadControlHead) => Verdict | undefined; fail?: boolean } = {}) {
  const calls: { owner: string; head: SessionReadControlHead; refs: AdmissionRef[] }[] = [];
  const s: GateStore = {
    async verdicts(org, ws, owner, h, refs) {
      expect([org, ws]).toEqual([ORG, WS]);
      calls.push({ owner, head: h, refs });
      if (over.fail) throw new Error("permission denied for function buffered_search_gate_for");
      const out = new Map<string, Verdict>();
      for (const ref of refs) {
        expect(ref.userId).toBe(owner);
        const v = over.verdict ? over.verdict(ref, h) : null;
        if (v !== undefined) out.set(refKey(ref), v);
      }
      return out;
    },
  };
  return { s, calls };
}

const buffered = (id: string, user = "u-2", claim = CLAIM, session = "s-1"): GateRow & { id: string } =>
  ({ id, org_id: ORG, workspace_id: WS, session_id: session, source: "codex", user_id: user, origin: "buffered", origin_claim_token: claim });
const canonical = (id: string, user = "u-2"): GateRow & { id: string } =>
  ({ id, org_id: ORG, workspace_id: WS, session_id: "s-9", source: "codex", user_id: user, origin: "canonical", origin_claim_token: "" });

async function run(rows: (GateRow & { id: string })[], opts: { bind?: ReturnType<typeof binding>; st?: ReturnType<typeof store>; deadlineMs?: number; noBinding?: boolean } = {}) {
  const bind = opts.bind ?? binding();
  const st = opts.st ?? store();
  const gate = createBufferedSearchGate({ controlHeads: opts.noBinding ? undefined : serviceBindingControlHeads(bind.b), store: st.s, deadlineMs: opts.deadlineMs });
  const result = await gate.filter(ORG, WS, rows);
  return { ...result, ids: result.kept.map((r) => r.id), bind, st };
}

describe("parseControlHead: exact producer shape, exact tenant echo", () => {
  const owner = { orgId: ORG, workspaceId: WS, userId: "u-2" };
  test("the producer's 200 body parses", () => {
    expect(parseControlHead(head(owner), owner)?.sourceControlVersion).toBe("7");
    expect(parseControlHead(head(owner, { sourceControlVersion: "0", sourceControlEpoch: null, sourceControlDigest: null, sourceControlKind: null }), owner)?.sourceControlVersion).toBe("0");
  });
  test.each([
    ["extra key", { extra: 1 }],
    ["wrong protocol", { protocol: "v2" }],
    ["wrong version", { version: 2 }],
    ["wrong issuer", { issuer: "someone" }],
    ["tenant extra key", { tenant: { orgId: ORG, workspaceId: WS, userId: "u-2", role: "x" } }],
    ["tenant user mismatch (reader echoed)", { tenant: { orgId: ORG, workspaceId: WS, userId: "u-1" } }],
    ["tenant workspace mismatch", { tenant: { orgId: ORG, workspaceId: "ws-b", userId: "u-2" } }],
    ["cutover epoch differs", { cutover: { epoch: OTHER_EPOCH, status: "active", mode: "ingest" } }],
    ["cutover not active", { cutover: { epoch: EPOCH, status: "draining", mode: "ingest" } }],
    ["cutover extra key", { cutover: { epoch: EPOCH, status: "active", mode: "ingest", x: 1 } }],
    ["non-hex epoch", { controlEpoch: "Z".repeat(64), cutover: { epoch: "Z".repeat(64), status: "active", mode: "ingest" } }],
    ["numeric version", { sourceControlVersion: 7 }],
    ["leading-zero version", { sourceControlVersion: "07" }],
    ["version above bigint", { sourceControlVersion: "9223372036854775808" }],
    ["pending not boolean", { pending: "false" }],
    ["version 0 with a digest", { sourceControlVersion: "0" }],
    ["version >0 without a digest", { sourceControlDigest: null }],
    ["version >0 with another source epoch", { sourceControlEpoch: OTHER_EPOCH }],
    ["unknown kind", { sourceControlKind: "erase" }],
  ])("%s → refused", (_name, over) => {
    expect(parseControlHead(head(owner, over), owner)).toBeNull();
  });
  test("a missing key → refused", () => {
    const h = head(owner) as Record<string, unknown>;
    delete h.privacyBaselineSha256;
    expect(parseControlHead(h, owner)).toBeNull();
    expect(parseControlHead(null, owner)).toBeNull();
    expect(parseControlHead([head(owner)], owner)).toBeNull();
  });
});

describe("the gate: positive case and owner identity", () => {
  test("a buffered hit whose every check passes is kept; the head is asked for the OWNER, not the reader", async () => {
    // The reader is u-1 (not visible to the gate); the hit belongs to u-2.
    const r = await run([buffered("e-1", "u-2")]);
    expect(r.ids).toEqual(["e-1"]);
    expect(r.dropped).toEqual({});
    expect(r.bind.requests).toHaveLength(1);
    const req = r.bind.requests[0]!;
    expect(req.method).toBe("GET");
    expect(new URL(req.url).pathname).toBe(CONTROL_HEAD_PATH);
    expect(new URL(req.url).search).toBe("");
    expect(req.headers.get("x-relayhistory-org-id")).toBe(ORG);
    expect(req.headers.get("x-relayhistory-workspace-id")).toBe(WS);
    expect(req.headers.get("x-relayhistory-user-id")).toBe("u-2");
    expect(r.st.calls).toHaveLength(1);
    expect(r.st.calls[0]!.owner).toBe("u-2");
    expect(r.st.calls[0]!.head.sourceControlVersion).toBe("7");
    expect(r.st.calls[0]!.refs).toEqual([{ claimToken: CLAIM, userId: "u-2", source: "codex", sessionId: "s-1" }]);
  });

  test("one head per distinct owner per page; each owner is judged on its own head", async () => {
    const bind = binding((o) => (o.userId === "u-3" ? Response.json(head(o, { pending: true })) : Response.json(head(o))));
    const r = await run([buffered("e-1", "u-2"), buffered("e-2", "u-3"), buffered("e-3", "u-2", CLAIM_2), buffered("e-4", "u-3")], { bind });
    expect(r.ids).toEqual(["e-1", "e-3"]);
    expect(r.dropped).toEqual({ control_pending: 2 });
    expect(r.bind.requests.map((q) => q.headers.get("x-relayhistory-user-id")).sort()).toEqual(["u-2", "u-3"]);
    // A pending owner gets no gate call; the other owner gets exactly one, with only its hits.
    expect(r.st.calls.map((c) => [c.owner, c.refs.length])).toEqual([["u-2", 2]]);
  });

  test("no caching across requests: a second page asks again", async () => {
    const bind = binding();
    const gate = createBufferedSearchGate({ controlHeads: serviceBindingControlHeads(bind.b), store: store().s });
    await gate.filter(ORG, WS, [buffered("e-1")]);
    await gate.filter(ORG, WS, [buffered("e-1")]);
    expect(bind.requests).toHaveLength(2);
  });

  test("a hit cannot borrow another user's admission: no reader row for the claim drops it", async () => {
    // The reader joins the claim on (pinned org, pinned workspace, owner); another user's claim yields no row.
    const st = store({ verdict: () => undefined });
    const r = await run([buffered("e-1", "u-2")], { st });
    expect(r.ids).toEqual([]);
    expect(r.dropped).toEqual({ admission_missing: 1 });
  });

  test("a verdict for another session than the hit's does not keep the hit", async () => {
    // Keys bind (claim, owner, source, session): a row for session s-2 never matches a hit on s-1.
    const st = store();
    st.s.verdicts = async (_o, _w, owner) => new Map([[refKey({ claimToken: CLAIM, userId: owner, source: "codex", sessionId: "s-2" }), null]]);
    const r = await run([buffered("e-1", "u-2", CLAIM, "s-1")], { st });
    expect(r.dropped).toEqual({ admission_missing: 1 });
  });
});

describe("the gate: canonical rows are untouched", () => {
  test("canonical rows pass with no head, ledger or parity call", async () => {
    const r = await run([canonical("c-1"), canonical("c-2", "u-1")]);
    expect(r.ids).toEqual(["c-1", "c-2"]);
    expect(r.buffered).toBe(0);
    expect(r.bind.requests).toHaveLength(0);
    expect(r.st.calls).toHaveLength(0);
  });

  test("canonical rows survive every buffered failure, in page order", async () => {
    const bind = binding(() => new Response("nope", { status: 503 }));
    const r = await run([canonical("c-1"), buffered("e-1"), canonical("c-2")], { bind, st: store({ fail: true }) });
    expect(r.ids).toEqual(["c-1", "c-2"]);
    expect(r.buffered).toBe(1);
  });

  test("a row without a known origin marker is not provably canonical: dropped", async () => {
    const r = await run([{ ...canonical("x-1"), origin: undefined }, { ...canonical("x-2"), origin: "replica" }]);
    expect(r.ids).toEqual([]);
    expect(r.dropped).toEqual({ origin_unknown: 2 });
  });
});

describe("the gate: every fail-closed case drops the hit", () => {
  const cases: [string, Parameters<typeof run>[1], string][] = [
    ["non-200", { bind: binding(() => Response.json({ version: 1, error: { code: "control_head_unavailable" } }, { status: 503 })) }, "control_head_unavailable"],
    ["a 200 that is not JSON", { bind: binding(() => new Response("<html>", { status: 200 })) }, "control_head_unavailable"],
    ["a binding that throws", { bind: binding(() => { throw new Error("binding down"); }) }, "control_head_unavailable"],
    ["an extra key", { bind: binding((o) => Response.json({ ...head(o), debug: true })) }, "control_head_unavailable"],
    ["a missing key", { bind: binding((o) => { const h = head(o) as Record<string, unknown>; delete h.pending; return Response.json(h); }) }, "control_head_unavailable"],
    ["a tenant mismatch (reader echoed)", { bind: binding((o) => Response.json(head(o, { tenant: { orgId: ORG, workspaceId: WS, userId: "u-1" } }))) }, "control_head_unavailable"],
    ["no binding configured", { noBinding: true }, "control_head_unavailable"],
    ["pending", { bind: binding((o) => Response.json(head(o, { pending: true }))) }, "control_pending"],
    ["no admission row for the claim", { st: store({ verdict: () => undefined }) }, "admission_missing"],
    ["the gate call fails (043860 not applied, role or pin refusal)", { st: store({ fail: true }) }, "admission_unavailable"],
    ["an unknown reason from the reader", { st: store({ verdict: () => "admission_unknown_reason" }) }, "admission_unknown_reason"],
    ...([
      "admission_epoch_mismatch",
      "admission_version_mismatch",
      "admission_session_mismatch",
      "gate_closed",
      "parity_missing",
      "parity_stale",
      "deleted",
      "batch_not_buffered",
      "reset",
      "superseded",
    ] as const).map((reason): [string, Parameters<typeof run>[1], string] => [`reader verdict ${reason}`, { st: store({ verdict: () => reason }) }, reason]),
  ];
  test.each(cases)("%s", async (_name, opts, reason) => {
    const r = await run([buffered("e-1")], opts);
    expect(r.ids).toEqual([]);
    expect(r.dropped).toEqual({ [reason]: 1 });
  });

  test("a head slower than the deadline drops the hit (and the call is aborted)", async () => {
    let aborted = false;
    const bind = binding((_o, req) => new Promise<Response>(() => { req.signal.addEventListener("abort", () => { aborted = true; }); }));
    const started = Date.now();
    const r = await run([buffered("e-1")], { bind, deadlineMs: 50 });
    expect(Date.now() - started).toBeLessThan(1_000);
    expect(r.ids).toEqual([]);
    expect(r.dropped).toEqual({ control_head_unavailable: 1 });
    expect(aborted).toBe(true);
  });

  test("a gate call slower than the deadline drops the hit", async () => {
    const st = store();
    st.s.verdicts = () => new Promise(() => {});
    const r = await run([buffered("e-1")], { st, deadlineMs: 50 });
    expect(r.dropped).toEqual({ admission_unavailable: 1 });
  });

  test("a malformed claim token or an empty owner is dropped before any call", async () => {
    const r = await run([buffered("e-1", "u-2", "not-a-uuid"), buffered("e-2", "", CLAIM), buffered("e-3", "u-2", "ABCDEF12-2222-4333-8444-555555555555")]);
    expect(r.ids).toEqual([]);
    expect(r.dropped).toEqual({ claim_token_invalid: 3 });
    expect(r.bind.requests).toHaveLength(0);
  });

  test("an empty source or session is dropped before any call and does not poison the owner's other hits", async () => {
    const r = await run([buffered("e-1", "u-2", CLAIM, ""), { ...buffered("e-2"), source: "" }, buffered("e-3")]);
    expect(r.ids).toEqual(["e-3"]);
    expect(r.dropped).toEqual({ claim_token_invalid: 2 });
  });

  test("owner fan-out is bounded: hits of owners past the cap are dropped, never checked", async () => {
    const rows = Array.from({ length: MAX_GATE_OWNERS + 3 }, (_, i) => buffered(`e-${i}`, `u-${i}`));
    const r = await run(rows);
    expect(r.ids).toHaveLength(MAX_GATE_OWNERS);
    expect(r.dropped).toEqual({ owner_fanout: 3 });
    expect(r.bind.requests).toHaveLength(MAX_GATE_OWNERS);
    expect(r.st.calls).toHaveLength(MAX_GATE_OWNERS);
  });
});

describe("the read API with the gate", () => {
  const ID: VerifiedIdentity = { userId: "u-1", orgId: ORG, workspaceId: WS, scopes: ["rth:read"] };
  const cand = (id: string, ts: number) => ({ ts_ms: ts, session_id: "s-1", source: "codex", machine_id: "m-1", kind: "message", event_id: id });
  const newest = (id: string, ts: number, origin: string, user: string) => ({
    ...cand(id, ts), org_id: ORG, workspace_id: WS, type: "assistant", user_id: user, actor_name: "", actor_role: "", subagent_id: "", project_id: "",
    task_title: "", task_ref: "", tags: [], files_touched: [], tool_name: "", tool_status: "", content: "needle", in_budget: 1,
    origin, origin_claim_token: origin === "buffered" ? CLAIM : "",
  });
  function stubCh(newestRows: unknown[], candidates: unknown[]) {
    const queries: string[] = [];
    const ch: ClickHouseClient = {
      database: "relay_search",
      async command() {},
      async insert() {},
      async select<T>(query: string) {
        queries.push(query);
        if (query.includes("in_workspace") || query.includes("search_ingest_session_state")) return [] as T[];
        return (query.includes("bytes_before") ? newestRows : candidates) as T[];
      },
    };
    return { ch, queries };
  }
  function api(gate: boolean, ch: ClickHouseClient, bind = binding()) {
    const logs: Record<string, unknown>[] = [];
    const app = createReadApi({
      verify: async (t) => { if (t !== "good") throw new AuthError(401, "invalid_token", "bad"); return ID; },
      ch,
      liveness: memoryLiveness({ createdAt: { "codex\u0000s-1": Date.parse("2026-10-01T00:00:00Z") } }).liveness,
      completeSinceMs: 0,
      log: (f) => logs.push(f),
      ...(gate ? { bufferedSearchGate: createBufferedSearchGate({ controlHeads: serviceBindingControlHeads(bind.b), store: store({ verdict: (ref) => (ref.userId === "u-2" ? null : "parity_missing") }).s }) } : {}),
    });
    return { get: (p: string) => app.request(p, { headers: { authorization: "Bearer good" } }), logs, bind };
  }

  test("gate not wired (gate off, or the Lambda composition): canonical-only SQL, and no buffered row is served", async () => {
    // A stub that ignores the SQL filter: the row-level defense still drops buffered and unmarked rows.
    const rows = [newest("e-3", 3, "canonical", "u-1"), newest("e-2", 2, "buffered", "u-2"), { ...newest("e-1", 1, "canonical", "u-1"), origin: undefined }];
    const { ch, queries } = stubCh(rows, [cand("e-3", 3), cand("e-2", 2), cand("e-1", 1)]);
    const off = api(false, ch);
    for (const path of ["/v1/events?workspace=ws-a&q=needle&limit=10", "/v1/events?workspace=ws-a&session=s-1", "/v1/sessions/s-1/events?workspace=ws-a"]) {
      const b = await (await off.get(path)).json() as Record<string, any>;
      expect(b.events.map((e: { eventId: string }) => e.eventId)).toEqual(["e-3"]);
    }
    const event = queries.filter((q) => !q.includes("search_ingest_session_state") && !q.includes("in_workspace"));
    const candidates = event.filter((q) => !q.includes("bytes_before"));
    const newestQs = event.filter((q) => q.includes("bytes_before"));
    expect(candidates.length).toBeGreaterThan(0);
    for (const q of candidates) expect(q).toContain("origin = 'canonical'");
    // The newest query filters AFTER picking the newest version, never before it.
    for (const q of newestQs) expect(q).toMatch(/LIMIT 1 BY[\s\S]*\) WHERE origin = 'canonical'/u);
    expect(off.bind.requests).toHaveLength(0);
  });

  test("the Lambda never wires the gate, so it always reads canonical-only", async () => {
    const source = await Bun.file(new URL("../lambda.ts", import.meta.url)).text();
    expect(source).not.toContain("bufferedSearchGate");
    expect(source).toContain("createReadApi({");
  });

  test("gate off on a table without the origin columns: answered unfiltered only after the probe finds none", async () => {
    const rows = [newest("e-3", 3, "canonical", "u-1")].map(({ origin: _o, origin_claim_token: _t, ...r }) => r);
    for (const [columns, status] of [[0, 200], [2, 503]] as const) {
      const queries: string[] = [];
      const ch: ClickHouseClient = {
        database: "relay_search",
        async command() {},
        async insert() {},
        async select<T>(query: string) {
          queries.push(query);
          if (query.includes("in_workspace") || query.includes("search_ingest_session_state")) return [] as T[];
          if (query.includes("origin = 'canonical'")) throw new ClickHouseError(404, "47", "UNKNOWN_IDENTIFIER");
          if (query.includes("system.columns")) return [{ n: columns }] as T[];
          return (query.includes("bytes_before") ? rows : [cand("e-3", 3)]) as T[];
        },
      };
      const r = await api(false, ch).get("/v1/events?workspace=ws-a&q=needle&limit=10");
      expect(r.status).toBe(status);
      if (status === 200) expect(((await r.json()) as Record<string, any>).events.map((e: { eventId: string }) => e.eventId)).toEqual(["e-3"]);
      // The legacy read names no origin column, and the probe runs after it.
      const legacy = queries.filter((q) => q.includes("transcript_events") && !q.includes("origin"));
      expect(legacy.length).toBeGreaterThan(0);
      expect(queries.at(-1)).toContain("system.columns");
    }
  });

  test("gate on: the page shrinks, the cursor is the unfiltered page's, canonical rows stay", async () => {
    // Three candidates fill limit=3, so the cursor sits at the last candidate (e-1).
    const rows = [newest("e-3", 3, "canonical", "u-1"), newest("e-2", 2, "buffered", "u-3"), newest("e-1", 1, "buffered", "u-2")];
    const cands = [cand("e-3", 3), cand("e-2", 2), cand("e-1", 1)];
    const off = await (await api(false, stubCh(rows, cands).ch).get("/v1/events?workspace=ws-a&q=needle&limit=3")).json() as Record<string, any>;
    const { ch, queries } = stubCh(rows, cands);
    const on = api(true, ch);
    const res = await on.get("/v1/events?workspace=ws-a&q=needle&limit=3");
    const b = await res.json() as Record<string, any>;
    expect(res.status).toBe(200);
    // u-3 has no parity row: its hit drops. u-2 (another user than the reader) is visible.
    expect(b.events.map((e: { eventId: string }) => e.eventId)).toEqual(["e-3", "e-1"]);
    expect(b.nextCursor).toBe(off.nextCursor);
    expect(b.nextCursor).not.toBeNull();
    expect(queries.find((q) => q.includes("bytes_before"))).toContain("origin, origin_claim_token");
    // The response shape carries no origin field.
    expect(Object.keys(b.events[0])).not.toContain("origin");
    expect(on.logs.find((l) => l.reason === "buffered_gate")).toMatchObject({ buffered_checked: 2, buffered_dropped: { parity_missing: 1 } });
    expect(on.bind.requests.map((q) => q.headers.get("x-relayhistory-user-id")).sort()).toEqual(["u-2", "u-3"]);
  });

  test("gate on with the portable adapter binding refuses to start", () => {
    expect(() => createReadApi({
      verify: async () => ID,
      adapter: {} as never,
      liveness: memoryLiveness().liveness,
      completeSinceMs: 0,
      bufferedSearchGate: createBufferedSearchGate({ store: store().s }),
    })).toThrow("buffered search gate requires the ClickHouse binding");
  });
});

describe("configuration and SQL", () => {
  test("READ_API_BUFFERED_SEARCH_GATE: only 'on' enables", () => {
    expect(parseBufferedSearchGate(undefined)).toBe(false);
    expect(parseBufferedSearchGate("")).toBe(false);
    expect(parseBufferedSearchGate("true")).toBe(false);
    expect(parseBufferedSearchGate("1")).toBe(false);
    expect(parseBufferedSearchGate(" ON ")).toBe(true);
  });

  test("origin modes: canonical filters, gate selects the marker unfiltered, legacy never names it", () => {
    const input = { orgId: ORG, workspaceId: WS, order: "desc" as const, limit: 10, q: "needle" };
    const pos = [{ tsMs: 1, sessionId: "s", source: "codex", machineId: "m", kind: "k", eventId: "e" }];
    expect(buildNewestQuery("db", input, pos).query).toBe(buildNewestQuery("db", input, pos, "canonical").query);
    expect(buildNewestQuery("db", input, pos).query).toContain("origin = 'canonical'");
    expect(buildCandidateQuery("db", input).query).toContain("origin = 'canonical'");
    expect(buildNewestQuery("db", input, pos, "gate").query).toContain("origin, origin_claim_token");
    expect(buildNewestQuery("db", input, pos, "gate").query).not.toContain("origin = 'canonical'");
    expect(buildCandidateQuery("db", input, "gate").query).not.toContain("origin");
    expect(buildNewestQuery("db", input, pos, "legacy").query).not.toContain("origin");
    expect(buildCandidateQuery("db", input, "legacy").query).not.toContain("origin");
  });

  const owner = { orgId: ORG, workspaceId: WS, userId: "u-2" };
  const parsed = (over: Record<string, unknown> = {}) => parseControlHead(head(owner, over), owner)!;
  function recordingRunner(answer: (params: unknown[]) => unknown[] = () => []) {
    const seen: { orgId: string; ws: string; text: string; params: unknown[] }[] = [];
    const runner: TenantRunner = async (orgId, ws, work) => work({
      async query<R>(text: string, params?: unknown[]) {
        seen.push({ orgId, ws, text, params: params ?? [] });
        return { rows: answer(params ?? []) as R[] };
      },
    });
    return { runner, seen };
  }
  const ref = { claimToken: CLAIM, userId: "u-2", source: "codex", sessionId: "s-1" };

  test("the SQL store makes one reader call for the owner, with the full head tuple, and maps the verdicts", async () => {
    const { runner, seen } = recordingRunner(() => [
      { claim_token: CLAIM, source: "codex", session_id: "s-1", drop_reason: null },
      { claim_token: CLAIM_2, source: "codex", session_id: "s-1", drop_reason: "parity_stale" },
      { claim_token: CLAIM, source: "codex", session_id: "s-3", drop_reason: "something_new" },
    ]);
    const got = await sqlGateStore(runner).verdicts(ORG, WS, "u-2", parsed(), [ref, { ...ref, claimToken: CLAIM_2 }, { ...ref, sessionId: "s-3" }]);
    expect(got.get(refKey(ref))).toBeNull();
    expect(got.get(refKey({ ...ref, claimToken: CLAIM_2 }))).toBe("parity_stale");
    expect(got.get(refKey({ ...ref, sessionId: "s-3" }))).toBe("admission_unknown_reason");
    expect(seen).toHaveLength(1);
    expect(seen[0]).toMatchObject({ orgId: ORG, ws: WS, text: GATE_SQL });
    expect(seen[0]!.params.slice(0, 10)).toEqual([ORG, WS, "u-2", EPOCH, EPOCH, "7", DIGEST, "selection", false, BASELINE]);
    expect(JSON.parse(seen[0]!.params[10] as string)).toEqual([
      { claim_token: CLAIM, source: "codex", session_id: "s-1" },
      { claim_token: CLAIM_2, source: "codex", session_id: "s-1" },
      { claim_token: CLAIM, source: "codex", session_id: "s-3" },
    ]);
  });

  test("a no-head control head is passed in 043700's encoding (four NULLs, not pending)", async () => {
    const { runner, seen } = recordingRunner();
    await sqlGateStore(runner).verdicts(ORG, WS, "u-2", parsed({ sourceControlVersion: "0", sourceControlEpoch: null, sourceControlDigest: null, sourceControlKind: null }), [ref]);
    expect(seen[0]!.params.slice(3, 10)).toEqual([EPOCH, null, null, null, null, false, BASELINE]);
  });

  test("more hits than the reader's cap are split into calls of at most MAX_GATE_HITS in one transaction", async () => {
    const { runner, seen } = recordingRunner();
    const refs = Array.from({ length: MAX_GATE_HITS + 1 }, (_, i) => ({ ...ref, sessionId: `s-${i}` }));
    await sqlGateStore(runner).verdicts(ORG, WS, "u-2", parsed(), refs);
    expect(seen.map((q) => (JSON.parse(q.params[10] as string) as unknown[]).length)).toEqual([MAX_GATE_HITS, 1]);
    expect(MAX_GATE_HITS).toBe(500);
  });

  test("the gate's only SQL is a SELECT from sessions.buffered_search_gate_for(: no direct ledger-table read", async () => {
    const { runner, seen } = recordingRunner();
    const st = sqlGateStore(runner);
    const gate = createBufferedSearchGate({ controlHeads: serviceBindingControlHeads(binding().b), store: st });
    await gate.filter(ORG, WS, [buffered("e-1", "u-2"), buffered("e-2", "u-3", CLAIM_2), canonical("c-1")]);
    await st.verdicts(ORG, WS, "u-2", parsed({ sourceControlVersion: "0", sourceControlEpoch: null, sourceControlDigest: null, sourceControlKind: null }), [ref]);
    expect(seen.length).toBe(3);
    for (const q of seen) {
      const sql = q.text.trim();
      expect(sql.startsWith("SELECT")).toBe(true);
      expect(sql).toMatch(/\bFROM sessions\.buffered_search_gate_for\(/u);
      expect(sql.match(/\bFROM\b/giu)).toHaveLength(1);
      expect(sql).not.toMatch(/ingest_claim_attempts|ingest_lazy|owner_buffered_parity|upload_deletions|upload_resets|ingest_frozen_source_deletions|;/u);
    }
    // The module's source names no other SQL statement.
    const source = await Bun.file(new URL("../buffered-search-gate.ts", import.meta.url)).text();
    expect(source.match(/\bq\.query</gu)).toHaveLength(1);
    expect(source).not.toMatch(/FROM sessions\.(?!buffered_search_gate_for\()/u);
  });

  test("an empty ref list makes no call", async () => {
    const { runner, seen } = recordingRunner();
    expect((await sqlGateStore(runner).verdicts(ORG, WS, "u-2", parsed(), [])).size).toBe(0);
    expect(seen).toHaveLength(0);
  });
});

test("GATE_SQL_REASONS is exactly the set of drop reasons 043860 can return", () => {
  const sql = readFileSync(join(import.meta.dir, "../../../supabase/migrations/20261105043860_buffered_search_gate.sql"), "utf8");
  const body = sql.slice(sql.indexOf("AS $gate$"), sql.lastIndexOf("$gate$;"));
  const returned = new Set([
    ...[...body.matchAll(/THEN '([a-z_]+)'/g)].map((m) => m[1]),
    ...[...body.matchAll(/v_gate := '([a-z_]+)'/g)].map((m) => m[1]),
  ]);
  expect([...returned].sort()).toEqual([...GATE_SQL_REASONS].sort());
});
