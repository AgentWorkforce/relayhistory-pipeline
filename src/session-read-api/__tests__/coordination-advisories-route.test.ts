/**
 * GET /v1/coordination/advisories and POST /v1/coordination/advisories/:advisoryId/ack
 * (docs/coordination-live-advisories.md, "Delivery contract"), mirroring the overlaps route tests:
 * the exact flag gate, auth and workspace isolation, parameters, the frozen contract, the ack's
 * 204/404, CORS, the SQL source's transactions, and the self-host service's POST passthrough.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import {
  PENDING_SQL, PENDING_V1_DELIVERY_SQL, PENDING_V2_ABSENT_TTL_MS, PENDING_V2_SIGNATURE, PENDING_V2_SQL, pendingV2Cache,
  poolTenantWriter, sqlCoordinationAdvisories,
  type AdvisoriesSource, type AdvisoryRecipient, type AdvisoryRow,
} from "../coordination-advisories.ts";
import { startReadApiService, type ReadApiService } from "../server.ts";
import { TENANT_SETTINGS_SQL } from "../liveness.ts";
import { ADVISORY_MESSAGE_MAX } from "../../coordination-advisor/template.ts";
import { memoryLiveness } from "./support.ts";
import { parseAdvisoryNotifyRows } from "../advisory-priority.ts";

const ID: VerifiedIdentity = { userId: "u", orgId: "org", workspaceId: "ws", scopes: ["rth:read"] };
const auth = { authorization: "Bearer good" };
const verify = async (token: string) => {
  if (token !== "good") throw new AuthError(401, "invalid_token", "invalid");
  return ID;
};
const ADV = "9f3c2a71d04be6e5";
const base = "/v1/coordination/advisories?workspace=ws&source=codex&sessionId=s-1";
const ackPath = (id = ADV, q = "workspace=ws") => `/v1/coordination/advisories/${id}/ack?${q}`;
const ackBody = JSON.stringify({ source: "codex", sessionId: "s-1" });

function row(i: number, over: Partial<AdvisoryRow> = {}): AdvisoryRow {
  return {
    advisoryId: `${ADV.slice(0, 15)}${i.toString(16)}`, harness: "codex", sessionId: "s-1", peerHarness: "claude", peerSessionId: `p-${i}`,
    urgency: "high", kind: "concurrent_edit", artifact: "src/shared.ts",
    evidence: [{ sessionId: "s-1", eventId: `e-${i}`, ts: "2026-11-12T09:58:12.000Z", kind: "tool" }],
    createdAt: new Date(Date.UTC(2026, 10, 12, 10, i)).toISOString(), deliveredAt: null, ...over,
  };
}

/** In-memory recipient store with the 062000 semantics: pending/delivered listed, ack idempotent, other recipients 404. */
function memorySource(rows: AdvisoryRow[] = [row(0)]) {
  const acked = new Set<string>();
  const calls: { op: string; recipient: AdvisoryRecipient; advisoryId?: string }[] = [];
  const mine = (r: AdvisoryRecipient, a: AdvisoryRow) => r.orgId === "org" && r.workspaceId === "ws" && a.harness === r.source && a.sessionId === r.sessionId;
  const source: AdvisoriesSource = {
    async pending(r) {
      calls.push({ op: "pending", recipient: r });
      return rows.filter((a) => mine(r, a) && !acked.has(a.advisoryId)).slice(0, 10);
    },
    async ack(r, advisoryId) {
      calls.push({ op: "ack", recipient: r, advisoryId });
      const hit = rows.find((a) => a.advisoryId === advisoryId && mine(r, a));
      if (hit) acked.add(advisoryId);
      return hit !== undefined;
    },
  };
  return { source, calls, acked };
}

const api = (enabled: boolean | undefined, source = memorySource().source, cors?: ReadonlySet<string>, canary: ReadonlySet<string> = new Set([ID.workspaceId]), notifyRows?: ReadonlySet<string>, priority?: boolean) => createReadApi({
  verify, adapter: {} as never, liveness: memoryLiveness().liveness, completeSinceMs: 0, directoryWorkspaces: canary,
  ...(notifyRows ? { advisoryNotifyRows: notifyRows } : {}),
  ...(priority === undefined ? {} : { advisoryPriority: priority }),
  ...(enabled === undefined ? {} : { coordinationAdvisories: { enabled, source } }),
  ...(cors ? { corsAllowedOrigins: cors } : {}),
});
const post = (app: ReturnType<typeof api>, path = ackPath(), body: string = ackBody, headers: Record<string, string> = auth) =>
  app.request(path, { method: "POST", body, headers: { ...headers, "content-type": "application/json" } });

describe("GET /v1/coordination/advisories", () => {
  test("off (absent, not enabled) is the standard 501 unsupported_by_backend envelope, reason advisories_disabled, for both routes, and nothing is read", async () => {
    const disabled = (body: unknown) => {
      const { error, correlationId } = body as { error: Record<string, unknown>; correlationId: unknown };
      expect(error).toEqual({ code: "unsupported_by_backend", message: "not served by this backend: advisories_disabled", reason: "advisories_disabled" });
      expect(typeof correlationId).toBe("string");
      expect(String(correlationId).length).toBeGreaterThan(0);
    };
    for (const enabled of [undefined, false]) {
      const mem = memorySource();
      const app = api(enabled, mem.source);
      const get = await app.request(base, { headers: auth });
      expect(get.status).toBe(501);
      disabled(await get.json());
      const ack = await post(app);
      expect(ack.status).toBe(501);
      disabled(await ack.json());
      expect(mem.calls).toEqual([]);
    }
    // Auth stays first, as on every /v1 read.
    expect((await api(false).request(base)).status).toBe(401);
  });

  test("auth, workspace isolation and the coordination canary are unchanged", async () => {
    const mem = memorySource();
    const app = api(true, mem.source);
    expect((await app.request(base)).status).toBe(401);
    expect((await app.request(base, { headers: { authorization: "Bearer bad" } })).status).toBe(401);
    expect((await app.request(base.replace("workspace=ws", "workspace=other"), { headers: auth })).status).toBe(403);
    expect((await app.request(base.replace("workspace=ws&", ""), { headers: auth })).status).toBe(501);
    expect((await app.request(`${base}&workspace=ws`, { headers: auth })).status).toBe(400);
    expect((await app.request(`${base}&tag=x`, { headers: auth })).status).toBe(501);
    const offCanary = api(true, mem.source, undefined, new Set(["other"]));
    const notEnabled = await offCanary.request(base, { headers: auth });
    expect(notEnabled.status).toBe(501);
    expect((await notEnabled.json() as any).error.reason).toBe("coordination_workspace_not_enabled");
    expect((await post(offCanary)).status).toBe(501);
    expect(mem.calls).toEqual([]);
  });

  test("source and sessionId are each required exactly once; status is pending only", async () => {
    const mem = memorySource();
    const app = api(true, mem.source);
    for (const path of [
      "/v1/coordination/advisories?workspace=ws&sessionId=s-1",
      "/v1/coordination/advisories?workspace=ws&source=codex",
      `${base}&source=claude`,
      `${base}&sessionId=s-2`,
      "/v1/coordination/advisories?workspace=ws&source=&sessionId=s-1",
      `${base}&status=acked`,
      `${base}&status=pending&status=pending`,
    ]) expect([path, (await app.request(path, { headers: auth })).status]).toEqual([path, 400]);
    expect(mem.calls).toEqual([]);
    expect((await app.request(`${base}&status=pending`, { headers: auth })).status).toBe(200);
  });

  test("the frozen contract: tenant from the token, at most 10 oldest first, templated message, null cursor", async () => {
    const rows = Array.from({ length: 12 }, (_, i) => row(i));
    const mem = memorySource(rows);
    const r = await api(true, mem.source).request(base, { headers: auth });
    expect(r.status).toBe(200);
    const body = await r.json() as any;
    expect(Object.keys(body)).toEqual(["contract", "advisories", "nextCursor"]);
    expect(body.contract).toBe("coordination-advisories/1");
    expect(body.nextCursor).toBeNull();
    expect(body.advisories).toHaveLength(10);
    expect(body.advisories.map((a: any) => a.advisoryId)).toEqual(rows.slice(0, 10).map((a) => a.advisoryId));
    const first = body.advisories[0];
    // The priority switch is off by default: exactly the eight pre-M3-7 keys.
    expect(Object.keys(first)).toEqual(["advisoryId", "urgency", "kind", "artifact", "peer", "createdAt", "message", "evidence"]);
    expect(first.peer).toEqual({ source: "claude", sessionId: "p-0" });
    expect(first.evidence).toEqual([{ sessionId: "s-1", eventId: "e-0", ts: "2026-11-12T09:58:12.000Z", kind: "tool" }]);
    expect(first.message.length).toBeLessThanOrEqual(ADVISORY_MESSAGE_MAX);
    expect(first.message).toContain("codex:s-1 and claude:p-0 both wrote src/shared.ts");
    expect(first.message).toContain("first seen 2026-11-12T09:58:12.000Z");
    expect(mem.calls).toEqual([{ op: "pending", recipient: { orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" } }]);
  });

  test("switch off (absent or false): the response bytes are exactly the pre-M3-7 ones, whatever the notify rows say", async () => {
    const one = [row(0, { deliveredAt: "2026-11-12T10:05:00.000Z" })];
    const expected = '{"contract":"coordination-advisories/1","advisories":[{"advisoryId":"9f3c2a71d04be6e0","urgency":"high","kind":"concurrent_edit","artifact":"src/shared.ts","peer":{"source":"claude","sessionId":"p-0"},"createdAt":"2026-11-12T10:00:00.000Z","message":"relayhistory-coordination: possible duplicate work.\\nYour session codex:s-1 and claude:p-0 both wrote src/shared.ts\\n(concurrent_edit, first seen 2026-11-12T09:58:12.000Z). Please check with the other agent before continuing.\\nAdvisory 9f3c2a71d04be6e0; ack it at the end of your turn.","evidence":[{"sessionId":"s-1","eventId":"e-0","ts":"2026-11-12T09:58:12.000Z","kind":"tool"}]}],"nextCursor":null}';
    for (const priority of [undefined, false]) {
      const r = await api(true, memorySource(one).source, undefined, undefined, parseAdvisoryNotifyRows("concurrent_edit:high"), priority).request(base, { headers: auth });
      expect(await r.text()).toBe(expected);
    }
    const on = await (await api(true, memorySource(one).source, undefined, undefined, undefined, true).request(base, { headers: auth })).json() as any;
    expect(Object.keys(on.advisories[0])).toEqual(["advisoryId", "urgency", "kind", "artifact", "peer", "createdAt", "message", "evidence", "priority", "notifyEligible", "delivery"]);
    expect(on.advisories[0]).toMatchObject({ priority: 2, notifyEligible: false, delivery: { dm: true, deliveredAt: "2026-11-12T10:05:00.000Z" } });
  });

  test("M3-7: priority by urgency and kind, and delivery from delivered_at (dm true only once the DM set it)", async () => {
    const cases: Array<[Partial<AdvisoryRow>, number, { dm: boolean; deliveredAt: string | null }]> = [
      [{ urgency: "high", kind: "duplicate_work", deliveredAt: "2026-11-12T10:05:00.000Z" }, 1, { dm: true, deliveredAt: "2026-11-12T10:05:00.000Z" }],
      [{ urgency: "high", kind: "superseded_work" }, 1, { dm: false, deliveredAt: null }],
      [{ urgency: "high", kind: "constraint_violation" }, 1, { dm: false, deliveredAt: null }],
      [{ urgency: "high", kind: "intent_conflict" }, 1, { dm: false, deliveredAt: null }],
      [{ urgency: "high", kind: "concurrent_edit" }, 2, { dm: false, deliveredAt: null }],
      [{ urgency: "high", kind: "stale_base" }, 2, { dm: false, deliveredAt: null }],
      [{ urgency: "low", kind: "intent_conflict" }, 3, { dm: false, deliveredAt: null }],
      [{ urgency: "low", kind: "nearby_work" }, 3, { dm: false, deliveredAt: null }],
    ];
    const rows = cases.map(([over], i) => row(i, over));
    const body = await (await api(true, memorySource(rows).source, undefined, undefined, undefined, true).request(base, { headers: auth })).json() as any;
    expect(body.contract).toBe("coordination-advisories/1");
    expect(body.advisories.map((a: any) => [a.kind, a.urgency, a.priority, a.delivery])).toEqual(
      cases.map(([over, p, d]) => [over.kind, over.urgency, p, d]));
  });

  test("notifyEligible: default empty allowlist means every item false; one allowed row makes exactly its (kind, urgency) true", async () => {
    const rows = [
      row(0, { urgency: "high", kind: "duplicate_work" }), row(1, { urgency: "low", kind: "duplicate_work" }),
      row(2, { urgency: "high", kind: "superseded_work" }), row(3, { urgency: "high", kind: "concurrent_edit" }),
    ];
    const read = async (notifyRows?: ReadonlySet<string>) =>
      ((await (await api(true, memorySource(rows).source, undefined, undefined, notifyRows, true).request(base, { headers: auth })).json()) as any).advisories.map((a: any) => a.notifyEligible);
    expect(await read()).toEqual([false, false, false, false]);
    expect(await read(parseAdvisoryNotifyRows(""))).toEqual([false, false, false, false]);
    expect(await read(parseAdvisoryNotifyRows("duplicate_work:high"))).toEqual([true, false, false, false]);
    expect(await read(parseAdvisoryNotifyRows(" duplicate_work:high , concurrent_edit:high "))).toEqual([true, false, false, true]);
    // Flag off stays the 501 envelope whatever the allowlist says.
    const off = await api(false, memorySource(rows).source, undefined, undefined, parseAdvisoryNotifyRows("duplicate_work:high")).request(base, { headers: auth });
    expect(off.status).toBe(501);
    expect(((await off.json()) as any).error.reason).toBe("advisories_disabled");
  });

  test("without evidence the message's first-seen is the row's createdAt", async () => {
    const r = await api(true, memorySource([row(1, { urgency: "low", evidence: [] })]).source).request(base, { headers: auth });
    const a = (await r.json() as any).advisories[0];
    expect(a.message).toContain(`first seen ${a.createdAt}`);
    expect(a.message).toContain("may be working near");
  });
});

describe("POST /v1/coordination/advisories/:advisoryId/ack", () => {
  test("204 for the recipient, idempotent, and the row leaves the pending list", async () => {
    const mem = memorySource([row(0), row(1)]);
    const app = api(true, mem.source);
    const id = row(0).advisoryId;
    const first = await post(app, ackPath(id));
    expect(first.status).toBe(204);
    expect(await first.text()).toBe("");
    expect((await post(app, ackPath(id))).status).toBe(204);
    const list = await (await app.request(base, { headers: auth })).json() as any;
    expect(list.advisories.map((a: any) => a.advisoryId)).toEqual([row(1).advisoryId]);
    expect(mem.calls.filter((c) => c.op === "ack")[0]).toEqual({ op: "ack", advisoryId: id, recipient: { orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" } });
  });

  test("404 for another recipient or an unknown id, never disclosing which", async () => {
    const mem = memorySource();
    const app = api(true, mem.source);
    const other = await post(app, ackPath(), JSON.stringify({ source: "codex", sessionId: "s-2" }));
    const unknown = await post(app, ackPath("0000000000000000"));
    expect(other.status).toBe(404);
    expect(unknown.status).toBe(404);
    const strip = (b: any) => ({ ...b, correlationId: undefined });
    expect(strip(await other.json())).toEqual(strip(await unknown.json()));
    // A malformed id is the same 404 and is never handed to the database.
    const before = mem.calls.length;
    expect((await post(app, ackPath("NOT-AN-ID"))).status).toBe(404);
    expect(mem.calls.length).toBe(before);
    expect(mem.acked.size).toBe(0);
  });

  test("the same gates as the GET, and a strict body", async () => {
    const mem = memorySource();
    const app = api(true, mem.source);
    expect((await post(app, ackPath(), ackBody, {})).status).toBe(401);
    expect((await post(app, ackPath(ADV, "workspace=other"))).status).toBe(403);
    expect((await post(app, ackPath(ADV, ""))).status).toBe(501);
    for (const body of ["", "not json", "[]", "null", '{"source":"codex"}', '{"sessionId":"s-1"}', '{"source":1,"sessionId":"s-1"}', '{"source":"codex","sessionId":"s-1","workspace":"ws"}'])
      expect([body, (await post(app, ackPath(), body)).status]).toEqual([body, 400]);
    const big = await post(app, ackPath(), JSON.stringify({ source: "codex", sessionId: "x".repeat(9 * 1024) }));
    expect(big.status).toBe(413);
    expect(mem.calls).toEqual([]);
  });
});

describe("CORS", () => {
  const origins = new Set(["https://app.example.test"]);
  const preflight = (app: ReturnType<typeof api>, path: string) =>
    app.request(path, { method: "OPTIONS", headers: { origin: "https://app.example.test", "access-control-request-method": "POST" } });

  test("the ack path allows POST only while the routes are enabled; every other path stays GET", async () => {
    const on = await preflight(api(true, undefined, origins), ackPath());
    expect(on.status).toBe(204);
    expect(on.headers.get("access-control-allow-methods")).toBe("GET, POST");
    expect(on.headers.get("access-control-allow-headers")).toBe("authorization, content-type, x-correlation-id");
    const list = await preflight(api(true, undefined, origins), base);
    expect(list.headers.get("access-control-allow-methods")).toBe("GET");
    for (const enabled of [undefined, false]) {
      const off = await preflight(api(enabled, undefined, origins), ackPath());
      expect(off.headers.get("access-control-allow-methods")).toBe("GET");
      expect(off.headers.get("access-control-allow-headers")).toBe("authorization, x-correlation-id");
    }
  });
});

describe("sqlCoordinationAdvisories", () => {
  type Client = { log: string[]; query(text: string, params?: unknown[]): Promise<{ rows: unknown[] }>; release(): void };
  function pool(answer: (text: string) => unknown[]) {
    const log: string[] = [];
    let released = 0;
    const client: Client = {
      log,
      async query(text, params) {
        log.push(text === TENANT_SETTINGS_SQL ? `TENANT ${JSON.stringify(params)}` : text.startsWith("SELECT") ? `${text.split("(")[0]} ${JSON.stringify(params)}` : text);
        return { rows: answer(text) };
      },
      release: () => void released++,
    };
    return { log, released: () => released, pool: { connect: async () => client } };
  }

  test("switch off (the default): only 062000's v1, the exact pre-M3-7 statement; no v2 statement is ever issued", async () => {
    const texts: string[] = [];
    const read = async <T,>(_o: string, _w: string, work: (q: any) => Promise<T>) =>
      work({ query: async (text: string, params?: unknown[]) => { texts.push(`${text} ${JSON.stringify(params)}`); return { rows: [] }; } });
    for (const opts of [{}, { priority: false }]) {
      await sqlCoordinationAdvisories(read as never, (() => { throw new Error("no write"); }) as never, opts)
        .pending({ orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" });
    }
    expect(PENDING_SQL).toBe(`SELECT advisory_id, harness, session_id, peer_harness, peer_session_id, urgency, kind, artifact,
       evidence, created_at
  FROM sessions.coordination_advisories_pending_for_v1($1, $2, $3, $4)`);
    expect(texts).toEqual([`${PENDING_SQL} ["org","ws","codex","s-1"]`, `${PENDING_SQL} ["org","ws","codex","s-1"]`]);
    expect(texts.join("\n")).not.toMatch(/pending_for_v2|to_regprocedure/);
  });

  test("switch on: pending reads the 062190 priority-first function in the given READ ONLY runner with the recipient and the page bound", async () => {
    const p = pool((t) => t.includes("pending_for_v2") ? [{
      advisory_id: ADV, harness: "codex", session_id: "s-1", peer_harness: "claude", peer_session_id: "p", urgency: "low",
      kind: "concurrent_edit", artifact: "src", evidence: [{ sessionId: "s-1", eventId: "e", ts: "2026-11-12T09:58:12.000Z", kind: "tool" }],
      created_at: new Date("2026-11-12T10:04:00Z"),
    }] : []);
    const seen: string[] = [];
    const read = async <T,>(org: string, ws: string, work: (q: any) => Promise<T>) => {
      seen.push(`${org}/${ws}`);
      const c = await p.pool.connect();
      return work({ query: async (text: string, params?: unknown[]) => c.query(text, params) });
    };
    const src = sqlCoordinationAdvisories(read as never, (() => { throw new Error("no write"); }) as never, { priority: true });
    const rows = await src.pending({ orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" });
    expect(seen).toEqual(["org/ws"]);
    expect(p.log).toEqual([expect.stringContaining('sessions.coordination_advisories_pending_for_v2 ["org","ws","codex","s-1",10]')]);
    expect(rows).toEqual([{ advisoryId: ADV, harness: "codex", sessionId: "s-1", peerHarness: "claude", peerSessionId: "p", urgency: "low", kind: "concurrent_edit", artifact: "src", evidence: [{ sessionId: "s-1", eventId: "e", ts: "2026-11-12T09:58:12.000Z", kind: "tool" }], createdAt: "2026-11-12T10:04:00.000Z", deliveredAt: null }]);
  });

  test("pending selects the reader's delivered_at and maps it (M3-7)", async () => {
    const p = pool((t) => t.includes("pending_for_v2") ? [{
      advisory_id: ADV, harness: "codex", session_id: "s-1", peer_harness: "claude", peer_session_id: "p", urgency: "high",
      kind: "duplicate_work", artifact: "m.sql", evidence: [], created_at: new Date("2026-11-12T10:04:00Z"), delivered_at: new Date("2026-11-12T10:05:00Z"),
    }] : []);
    const read = async <T,>(_org: string, _ws: string, work: (q: any) => Promise<T>) => {
      const c = await p.pool.connect();
      return work({ query: async (text: string, params?: unknown[]) => c.query(text, params) });
    };
    const rows = await sqlCoordinationAdvisories(read as never, (() => { throw new Error("no write"); }) as never, { priority: true })
      .pending({ orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" });
    expect(p.log[0]).toMatch(/created_at, delivered_at\s+FROM sessions\.coordination_advisories_pending_for_v2/);
    expect(rows[0]!.deliveredAt).toBe("2026-11-12T10:05:00.000Z");
  });

  test("switch on, 062190 not installed: one probe, then v1 for the TTL with no failed statement; the warning at most once per TTL", async () => {
    const calls: string[] = [];
    const warns: Record<string, unknown>[] = [];
    let t = 1_000;
    const read = async <T,>(_org: string, _ws: string, work: (q: any) => Promise<T>) => {
      calls.push("BEGIN");
      return work({ query: async (text: string, params?: unknown[]) => {
        if (text === PENDING_V2_SQL) {
          calls.push("v2");
          throw Object.assign(new Error("function sessions.coordination_advisories_pending_for_v2 does not exist"), { code: "42883" });
        }
        if (text.includes("to_regprocedure")) { calls.push(`probe ${JSON.stringify(params)}`); return { rows: [{ installed: false }] }; }
        expect(text).toBe(PENDING_V1_DELIVERY_SQL);
        calls.push("v1");
        return { rows: [{ advisory_id: ADV, harness: "codex", session_id: "s-1", peer_harness: "claude", peer_session_id: "p", urgency: "high",
          kind: "duplicate_work", artifact: "m.sql", evidence: [], created_at: new Date(0), delivered_at: null }] };
      } });
    };
    // A shared cache: the Worker builds a source per request.
    const cache = pendingV2Cache();
    const make = () => sqlCoordinationAdvisories(read as never, (() => { throw new Error("no write"); }) as never, { priority: true, cache, now: () => t, warn: (f) => { warns.push(f); } });
    const r = { orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" };
    expect((await make().pending(r)).map((a) => a.advisoryId)).toEqual([ADV]);
    expect(calls).toEqual(["BEGIN", "v2", "BEGIN", `probe ${JSON.stringify([PENDING_V2_SIGNATURE])}`, "BEGIN", "v1"]);
    calls.length = 0;
    t += 10_000;
    expect((await make().pending(r)).length).toBe(1);
    t += 10_000;
    expect((await make().pending(r)).length).toBe(1);
    expect(calls).toEqual(["BEGIN", "v1", "BEGIN", "v1"]);
    expect(warns).toEqual([{ reason: "coordination_advisories_v1_fallback", error_code: "42883", fallbacks: 1 }]);
    // After the TTL it probes v2 again, and warns again (with the running count).
    calls.length = 0;
    t += PENDING_V2_ABSENT_TTL_MS;
    await make().pending(r);
    expect(calls).toEqual(["BEGIN", "v2", "BEGIN", `probe ${JSON.stringify([PENDING_V2_SIGNATURE])}`, "BEGIN", "v1"]);
    expect(warns).toEqual([
      { reason: "coordination_advisories_v1_fallback", error_code: "42883", fallbacks: 1 },
      { reason: "coordination_advisories_v1_fallback", error_code: "42883", fallbacks: 4 },
    ]);
    // Through the route with the switch on: the fallback still answers 200 with the M3-7 item shape.
    t += PENDING_V2_ABSENT_TTL_MS;
    const body = await (await api(true, make(), undefined, undefined, undefined, true).request(base, { headers: auth })).json() as any;
    expect(body.advisories.map((a: any) => [a.advisoryId, a.priority])).toEqual([[ADV, 1]]);
  });

  test("switch on: a 42883 while pending_for_v2 IS installed (signature drift) is rethrown, never a fallback; other errors too", async () => {
    const calls: string[] = [];
    const warns: unknown[] = [];
    let code = "42883";
    const read = async <T,>(_org: string, _ws: string, work: (q: any) => Promise<T>) => work({ query: async (text: string) => {
      if (text === PENDING_V2_SQL) { calls.push("v2"); throw Object.assign(new Error("drift"), { code }); }
      if (text.includes("to_regprocedure")) { calls.push("probe"); return { rows: [{ installed: true }] }; }
      calls.push("v1"); return { rows: [] };
    } });
    const src = sqlCoordinationAdvisories(read as never, (() => { throw new Error("no write"); }) as never, { priority: true, warn: (f) => { warns.push(f); } });
    const r = { orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" };
    await expect(src.pending(r)).rejects.toThrow("drift");
    expect(calls).toEqual(["v2", "probe"]);
    // Not cached as absent: the next GET tries v2 again.
    code = "57014";
    await expect(src.pending(r)).rejects.toThrow("drift");
    expect(calls).toEqual(["v2", "probe", "v2"]);
    expect(warns).toEqual([]);
  });

  test("ack runs in one READ WRITE transaction with the tenant bound, and rolls back on error", async () => {
    let ok: unknown = true;
    const p = pool((t) => t.includes("ack_v1") ? [{ ok }] : []);
    const src = sqlCoordinationAdvisories((() => { throw new Error("no read"); }) as never, poolTenantWriter(p.pool));
    const r = { orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" };
    expect(await src.ack(r, ADV)).toBe(true);
    expect(p.log).toEqual(["BEGIN READ WRITE", 'TENANT ["org","ws"]', expect.stringContaining(`coordination_advisories_ack_v1 ["org","ws","${ADV}","codex","s-1"]`), "COMMIT"]);
    ok = false;
    expect(await src.ack(r, ADV)).toBe(false);
    expect(p.released()).toBe(2);
    const failing = pool((t) => { if (t.includes("ack_v1")) throw new Error("boom"); return []; });
    await expect(sqlCoordinationAdvisories((() => { throw new Error("no read"); }) as never, poolTenantWriter(failing.pool)).ack(r, ADV)).rejects.toThrow("boom");
    expect(failing.log.at(-1)).toBe("ROLLBACK");
    expect(failing.released()).toBe(1);
  });
});

describe("self-host service entry", () => {
  const running: ReadApiService[] = [];
  afterEach(async () => {
    for (const s of running.splice(0)) await s.close();
  });
  type Seen = { method: string; path: string; body: string };
  async function start(flag: string | undefined) {
    const seen: Seen[] = [];
    const env = { RELAYHISTORY_SELFHOST_READ_API: "true", RELAYHISTORY_READ_API_SECRET: "{}", ...(flag === undefined ? {} : { READ_API_COORDINATION_ADVISORIES_ENABLED: flag }) };
    const service = await startReadApiService(env, {
      host: "127.0.0.1", port: 0,
      build: async () => ({
        app: { async fetch(req: Request) {
          seen.push({ method: req.method, path: new URL(req.url).pathname, body: await req.text() });
          return new Response(null, { status: 204 });
        } },
        close: async () => {},
      }),
    });
    running.push(service);
    return { seen, url: (path: string) => `http://127.0.0.1:${service.port}${path}` };
  }

  test("the flag exactly \"true\" lets the ack POST through with its body; nothing else changes", async () => {
    const { seen, url } = await start("true");
    const r = await fetch(url(ackPath()), { method: "POST", body: ackBody, headers: { "content-type": "application/json" } });
    expect(r.status).toBe(204);
    expect(seen).toEqual([{ method: "POST", path: `/v1/coordination/advisories/${ADV}/ack`, body: ackBody }]);
    for (const path of ["/v1/coordination/advisories", `/v1/coordination/advisories/${ADV}/ack/`, `/v1/coordination/advisories/${ADV}/x/ack`, `/v1/coordination/advisories/${ADV}/%61ck`]) {
      const other = await fetch(url(path), { method: "POST", body: ackBody });
      expect([path, other.status, other.headers.get("allow")]).toEqual([path, 405, "GET"]);
    }
    const tooBig = await fetch(url(ackPath()), { method: "POST", body: "x".repeat(8 * 1024 + 1) });
    expect(tooBig.status).toBe(413);
    expect(seen).toHaveLength(1);
  });

  test("flag off (unset, \"\", \"false\", \"1\", \"TRUE\") keeps the ack POST a 405 Allow GET that never reaches the app", async () => {
    for (const flag of [undefined, "", "false", "1", "TRUE"]) {
      const { seen, url } = await start(flag);
      const r = await fetch(url(ackPath()), { method: "POST", body: ackBody, headers: { "content-type": "application/json" } });
      expect([flag, r.status, r.headers.get("allow")]).toEqual([flag, 405, "GET"]);
      expect(seen).toEqual([]);
    }
  });
});
