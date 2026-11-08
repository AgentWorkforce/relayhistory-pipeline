/**
 * The per-hit R2 privacy gate for buffered-origin rows (buffered-search-gate.ts):
 * every fail-closed case drops the hit, the positive case keeps it, canonical
 * rows are untouched, and the control head is asked for the hit's OWNER.
 * The G2 parity table (sessions.owner_buffered_parity) and the buffered ledger
 * are stubbed through AdmissionStore; the head producer through a stub binding.
 */
import { describe, expect, test } from "bun:test";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import {
  ADMISSIONS_SQL,
  CONTROL_HEAD_PATH,
  MAX_GATE_OWNERS,
  PARITY_SQL,
  createBufferedSearchGate,
  parseBufferedSearchGate,
  parseControlHead,
  refKey,
  serviceBindingControlHeads,
  sqlAdmissionStore,
  type Admission,
  type AdmissionRef,
  type AdmissionStore,
  type GateRow,
  type Owner,
  type ServiceBinding,
} from "../buffered-search-gate.ts";
import type { TenantRunner } from "../liveness.ts";
import { buildNewestQuery } from "../queries.ts";
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

const goodAdmission = (userId: string): Admission => ({ claimUserId: userId, admissionEpoch: EPOCH, admissionVersion: "7", versionConsistent: true, deleted: false, resetBehind: false });

function store(over: { admission?: (ref: AdmissionRef) => Admission | undefined; parity?: Set<string>; failAdmissions?: boolean; failParity?: boolean } = {}) {
  const calls = { admissions: [] as AdmissionRef[][], parity: [] as string[][] };
  const s: AdmissionStore = {
    async admissions(org, ws, refs) {
      expect([org, ws]).toEqual([ORG, WS]);
      calls.admissions.push(refs);
      if (over.failAdmissions) throw new Error("permission denied");
      const out = new Map<string, Admission>();
      for (const ref of refs) {
        const a = over.admission ? over.admission(ref) : goodAdmission(ref.userId);
        if (a) out.set(refKey(ref), a);
      }
      return out;
    },
    async parity(org, ws, userIds) {
      expect([org, ws]).toEqual([ORG, WS]);
      calls.parity.push(userIds);
      if (over.failParity) throw new Error("relation does not exist");
      return new Set(userIds.filter((u) => (over.parity ?? new Set(userIds)).has(u)));
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
    expect(r.st.calls.admissions[0]).toEqual([{ claimToken: CLAIM, userId: "u-2", source: "codex", sessionId: "s-1" }]);
    expect(r.st.calls.parity[0]).toEqual(["u-2"]);
  });

  test("one head per distinct owner per page; each owner is judged on its own head", async () => {
    const bind = binding((o) => (o.userId === "u-3" ? Response.json(head(o, { pending: true })) : Response.json(head(o))));
    const r = await run([buffered("e-1", "u-2"), buffered("e-2", "u-3"), buffered("e-3", "u-2", CLAIM_2), buffered("e-4", "u-3")], { bind });
    expect(r.ids).toEqual(["e-1", "e-3"]);
    expect(r.dropped).toEqual({ control_pending: 2 });
    expect(r.bind.requests.map((q) => q.headers.get("x-relayhistory-user-id")).sort()).toEqual(["u-2", "u-3"]);
  });

  test("no caching across requests: a second page asks again", async () => {
    const bind = binding();
    const gate = createBufferedSearchGate({ controlHeads: serviceBindingControlHeads(bind.b), store: store().s });
    await gate.filter(ORG, WS, [buffered("e-1")]);
    await gate.filter(ORG, WS, [buffered("e-1")]);
    expect(bind.requests).toHaveLength(2);
  });

  test("a hit cannot borrow another user's admission: the claim must be the owner's", async () => {
    const st = store({ admission: () => goodAdmission("u-9") });
    const r = await run([buffered("e-1", "u-2")], { st });
    expect(r.ids).toEqual([]);
    expect(r.dropped).toEqual({ admission_owner_mismatch: 1 });
  });
});

describe("the gate: canonical rows are untouched", () => {
  test("canonical rows pass with no head, ledger or parity call", async () => {
    const r = await run([canonical("c-1"), canonical("c-2", "u-1")]);
    expect(r.ids).toEqual(["c-1", "c-2"]);
    expect(r.buffered).toBe(0);
    expect(r.bind.requests).toHaveLength(0);
    expect(r.st.calls.admissions).toHaveLength(0);
    expect(r.st.calls.parity).toHaveLength(0);
  });

  test("canonical rows survive every buffered failure, in page order", async () => {
    const bind = binding(() => new Response("nope", { status: 503 }));
    const r = await run([canonical("c-1"), buffered("e-1"), canonical("c-2")], { bind, st: store({ failAdmissions: true, failParity: true }) });
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
    ["head epoch differs from the admission epoch", { st: store({ admission: (r) => ({ ...goodAdmission(r.userId), admissionEpoch: OTHER_EPOCH }) }) }, "admission_epoch_mismatch"],
    ["admission epoch unknown (policy identity mismatch)", { st: store({ admission: (r) => ({ ...goodAdmission(r.userId), admissionEpoch: null }) }) }, "admission_epoch_mismatch"],
    ["head version differs from the admission version", { bind: binding((o) => Response.json(head(o, { sourceControlVersion: "8" }))) }, "admission_version_mismatch"],
    ["admission version missing", { st: store({ admission: (r) => ({ ...goodAdmission(r.userId), admissionVersion: null }) }) }, "admission_version_mismatch"],
    ["ACK decisions disagree with the claim's version", { st: store({ admission: (r) => ({ ...goodAdmission(r.userId), versionConsistent: false }) }) }, "admission_version_mismatch"],
    ["no admission row for the claim", { st: store({ admission: () => undefined }) }, "admission_missing"],
    ["the ledger read fails", { st: store({ failAdmissions: true }) }, "admission_unavailable"],
    ["no G2 parity row", { st: store({ parity: new Set() }) }, "parity_missing"],
    ["the parity read fails (043700 not applied)", { st: store({ failParity: true }) }, "parity_unavailable"],
    ["deleted (upload_deletions / frozen source deletions)", { st: store({ admission: (r) => ({ ...goodAdmission(r.userId), deleted: true }) }) }, "deleted"],
    ["reset since admission (behind upload_resets)", { st: store({ admission: (r) => ({ ...goodAdmission(r.userId), resetBehind: true }) }) }, "reset"],
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

  test("a ledger read slower than the deadline drops the hit", async () => {
    const st = store();
    st.s.admissions = () => new Promise(() => {});
    const r = await run([buffered("e-1")], { st, deadlineMs: 50 });
    expect(r.dropped).toEqual({ admission_unavailable: 1 });
  });

  test("a malformed claim token or an empty owner is dropped before any call", async () => {
    const r = await run([buffered("e-1", "u-2", "not-a-uuid"), buffered("e-2", "", CLAIM), buffered("e-3", "u-2", "ABCDEF12-2222-4333-8444-555555555555")]);
    expect(r.ids).toEqual([]);
    expect(r.dropped).toEqual({ claim_token_invalid: 3 });
    expect(r.bind.requests).toHaveLength(0);
  });

  test("owner fan-out is bounded: hits of owners past the cap are dropped, never checked", async () => {
    const rows = Array.from({ length: MAX_GATE_OWNERS + 3 }, (_, i) => buffered(`e-${i}`, `u-${i}`));
    const r = await run(rows);
    expect(r.ids).toHaveLength(MAX_GATE_OWNERS);
    expect(r.dropped).toEqual({ owner_fanout: 3 });
    expect(r.bind.requests).toHaveLength(MAX_GATE_OWNERS);
    expect(r.st.calls.parity[0]).toHaveLength(MAX_GATE_OWNERS);
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
      ...(gate ? { bufferedSearchGate: createBufferedSearchGate({ controlHeads: serviceBindingControlHeads(bind.b), store: store({ parity: new Set(["u-2"]) }).s }) } : {}),
    });
    return { get: (p: string) => app.request(p, { headers: { authorization: "Bearer good" } }), logs, bind };
  }

  test("gate off: the newest query never names the origin columns", async () => {
    const { ch, queries } = stubCh([newest("e-1", 3, "canonical", "u-1")].map(({ origin: _o, origin_claim_token: _t, ...r }) => r), [cand("e-1", 3)]);
    const r = await api(false, ch).get("/v1/events?workspace=ws-a&q=needle&limit=10");
    expect(r.status).toBe(200);
    expect(queries.some((q) => q.includes("origin"))).toBe(false);
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

  test("buildNewestQuery is byte-identical with the origin option off", () => {
    const input = { orgId: ORG, workspaceId: WS, order: "desc" as const, limit: 10, q: "needle" };
    const pos = [{ tsMs: 1, sessionId: "s", source: "codex", machineId: "m", kind: "k", eventId: "e" }];
    expect(buildNewestQuery("db", input, pos, {}).query).toBe(buildNewestQuery("db", input, pos).query);
    expect(buildNewestQuery("db", input, pos).query).not.toContain("origin");
  });

  test("the SQL store binds the verified tenant and the hit's owner, and maps the ledger row", async () => {
    const seen: { orgId: string; ws: string; text: string; params: unknown[] }[] = [];
    const runner: TenantRunner = async (orgId, ws, work) => work({
      async query<R>(text: string, params?: unknown[]) {
        seen.push({ orgId, ws, text, params: params ?? [] });
        if (text === PARITY_SQL) return { rows: [{ user_id: "u-2" }] as R[] };
        return { rows: [{ claim_token: CLAIM, user_id: "u-2", source: "codex", session_id: "s-1", claim_user_id: "u-2", admission_epoch: EPOCH, admission_version: "7", version_consistent: true, deleted: false, reset_behind: false }] as R[] };
      },
    });
    const s = sqlAdmissionStore(runner);
    const ref = { claimToken: CLAIM, userId: "u-2", source: "codex", sessionId: "s-1" };
    const got = await s.admissions(ORG, WS, [ref]);
    expect(got.get(refKey(ref))).toEqual(goodAdmission("u-2"));
    expect(await s.parity(ORG, WS, ["u-2", "u-3"])).toEqual(new Set(["u-2"]));
    expect(seen[0]).toMatchObject({ orgId: ORG, ws: WS, text: ADMISSIONS_SQL });
    expect(seen[0]!.params.slice(0, 2)).toEqual([ORG, WS]);
    expect(JSON.parse(seen[0]!.params[2] as string)).toEqual([{ claim_token: CLAIM, user_id: "u-2", source: "codex", session_id: "s-1" }]);
    expect(seen[1]!.params).toEqual([ORG, WS, ["u-2", "u-3"]]);
    // The claim join is bound to the owner, and every blocker defaults closed.
    expect(ADMISSIONS_SQL).toContain("a.user_id = refs.user_id");
    expect(ADMISSIONS_SQL).toContain("a.reset_epoch IS NULL OR");
    expect(ADMISSIONS_SQL).toContain("lp.source_identity_sha256 = a.source_identity_sha256");
    expect(ADMISSIONS_SQL).toContain("ingest_frozen_source_deletions");
  });

  test("a NULL blocker from SQL reads as blocked", async () => {
    const runner: TenantRunner = async (_o, _w, work) => work({
      async query<R>() {
        return { rows: [{ claim_token: CLAIM, user_id: "u-2", source: "codex", session_id: "s-1", claim_user_id: "u-2", admission_epoch: EPOCH, admission_version: "7", version_consistent: null, deleted: null, reset_behind: null }] as R[] };
      },
    });
    const ref = { claimToken: CLAIM, userId: "u-2", source: "codex", sessionId: "s-1" };
    const a = (await sqlAdmissionStore(runner).admissions(ORG, WS, [ref])).get(refKey(ref))!;
    expect(a).toMatchObject({ versionConsistent: false, deleted: true, resetBehind: true });
  });
});
