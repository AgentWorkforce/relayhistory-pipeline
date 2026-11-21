/**
 * READ_API_ADVISORY_INSTANCE_ENABLED (20261105062220) on the advisory routes: off, the GET and the ack are
 * byte-identical to before (contract coordination-advisories/1, no incarnation, v1 ack, a body with `incarnation` is a
 * 400); on, the GET answers contract "advisories/v3" with each item's `incarnation`, an ack with an incarnation goes
 * to ack_v2 (200 acked / 200 already_acked / 404 no match) and one without (or with null) to v1. Notify eligibility is
 * evaluated per row, so a re-created instance of an allowlisted class is eligible afresh.
 */
import { describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import {
  ACK_V2_SQL, ADVISORIES_INSTANCE_CONTRACT, PENDING_V3_SQL, poolTenantWriter, sqlCoordinationAdvisories,
  type AdvisoriesSource, type AdvisoryAckInstanceResult, type AdvisoryRecipient, type AdvisoryRow,
} from "../coordination-advisories.ts";
import { TENANT_SETTINGS_SQL } from "../liveness.ts";
import { memoryLiveness } from "./support.ts";
import { parseAdvisoryNotifyRows } from "../advisory-priority.ts";

const ID: VerifiedIdentity = { userId: "u", orgId: "org", workspaceId: "ws", scopes: ["rth:read"] };
const auth = { authorization: "Bearer good" };
const verify = async (token: string) => {
  if (token !== "good") throw new AuthError(401, "invalid_token", "invalid");
  return ID;
};
const ADV = "9f3c2a71d04be6e0";
const INC1 = "1b4e28ba-2fa1-41d2-883f-0016d3cca427";
const INC2 = "6fa459ea-ee8a-4ca4-894e-db77e160355e";
const base = "/v1/coordination/advisories?workspace=ws&source=codex&sessionId=s-1";
const ackPath = (id = ADV) => `/v1/coordination/advisories/${id}/ack?workspace=ws`;
const ackBody = (extra: Record<string, unknown> = {}) => JSON.stringify({ source: "codex", sessionId: "s-1", ...extra });

function row(over: Partial<AdvisoryRow> = {}): AdvisoryRow {
  return {
    advisoryId: ADV, harness: "codex", sessionId: "s-1", peerHarness: "claude", peerSessionId: "p-0",
    urgency: "high", kind: "concurrent_edit", artifact: "src/shared.ts",
    evidence: [{ sessionId: "s-1", eventId: "e-0", ts: "2026-11-12T09:58:12.000Z", kind: "tool" }],
    createdAt: "2026-11-12T10:00:00.000Z", deliveredAt: "2026-11-12T10:05:00.000Z", ...over,
  };
}

/**
 * In-memory 062220 semantics: rows carry an incarnation (null = legacy). pending() is the legacy read (no
 * incarnation), pendingInstances() the v3 read; ack() matches legacy rows only (the v1 fence); ackInstance() matches
 * exactly (id, recipient, incarnation). `recreate` replaces a row's instance (promotion or sweep + re-insert).
 */
function instanceSource(initial: AdvisoryRow[]) {
  let rows = initial.map((r) => ({ ...r, acked: false }));
  const calls: string[] = [];
  const mine = (r: AdvisoryRecipient, a: AdvisoryRow) => r.orgId === "org" && r.workspaceId === "ws" && a.harness === r.source && a.sessionId === r.sessionId;
  const strip = ({ acked: _a, incarnation: _i, ...rest }: AdvisoryRow & { acked: boolean }) => rest;
  const source: AdvisoriesSource = {
    async pending(r) { calls.push("pending"); return rows.filter((a) => mine(r, a) && !a.acked).map(strip); },
    async ack(r, id) {
      calls.push(`ack ${id}`);
      const hit = rows.find((a) => a.advisoryId === id && mine(r, a) && (a.incarnation ?? null) === null);
      if (hit) hit.acked = true;
      return hit !== undefined;
    },
    async pendingInstances(r) {
      calls.push("pendingInstances");
      return rows.filter((a) => mine(r, a) && !a.acked).map(({ acked: _a, ...rest }) => ({ ...rest, incarnation: rest.incarnation ?? null }));
    },
    async ackInstance(r, id, inc): Promise<AdvisoryAckInstanceResult> {
      calls.push(`ackInstance ${id} ${inc}`);
      const hit = rows.find((a) => a.advisoryId === id && mine(r, a) && a.incarnation === inc);
      if (!hit) return "no_match";
      if (hit.acked) return "already_acked";
      hit.acked = true;
      return "acked";
    },
  };
  const recreate = (id: string, over: Partial<AdvisoryRow>) => {
    rows = rows.map((a) => a.advisoryId === id ? { ...a, ...over, acked: false } : a);
  };
  return { source, calls, recreate };
}

const api = (source: AdvisoriesSource, opts: { instance?: boolean; priority?: boolean; notifyRows?: ReadonlySet<string> } = {}) => createReadApi({
  verify, adapter: {} as never, liveness: memoryLiveness().liveness, completeSinceMs: 0, directoryWorkspaces: new Set([ID.workspaceId]),
  coordinationAdvisories: { enabled: true, source },
  ...(opts.notifyRows ? { advisoryNotifyRows: opts.notifyRows } : {}),
  ...(opts.priority === undefined ? {} : { advisoryPriority: opts.priority }),
  ...(opts.instance === undefined ? {} : { advisoryInstance: opts.instance }),
});
const post = (app: ReturnType<typeof api>, body: string, path = ackPath()) =>
  app.request(path, { method: "POST", body, headers: { ...auth, "content-type": "application/json" } });

// The exact pre-062220 GET bytes for row() (the same expectation as the M3-7 route test's switch-off case).
const LEGACY_GET = '{"contract":"coordination-advisories/1","advisories":[{"advisoryId":"9f3c2a71d04be6e0","urgency":"high","kind":"concurrent_edit","artifact":"src/shared.ts","peer":{"source":"claude","sessionId":"p-0"},"createdAt":"2026-11-12T10:00:00.000Z","message":"relayhistory-coordination: possible duplicate work.\\nYour session codex:s-1 and claude:p-0 both wrote src/shared.ts\\n(concurrent_edit, first seen 2026-11-12T09:58:12.000Z). Please check with the other agent before continuing.\\nAdvisory 9f3c2a71d04be6e0; ack it at the end of your turn.","evidence":[{"sessionId":"s-1","eventId":"e-0","ts":"2026-11-12T09:58:12.000Z","kind":"tool"}]}],"nextCursor":null}';

describe("READ_API_ADVISORY_INSTANCE_ENABLED off", () => {
  test("the GET bytes are exactly the pre-062220 ones (no incarnation even on a protected row); the instance reader is never called", async () => {
    for (const instance of [undefined, false]) {
      const mem = instanceSource([row({ incarnation: INC1 })]);
      const r = await api(mem.source, instance === undefined ? {} : { instance }).request(base, { headers: auth });
      expect(r.status).toBe(200);
      expect(await r.text()).toBe(LEGACY_GET);
      expect(mem.calls).toEqual(["pending"]);
    }
  });

  test("the ack is v1 only: a body with incarnation is the same 400 as before, never ack_v2; without it 204/404 as before", async () => {
    for (const instance of [undefined, false]) {
      const mem = instanceSource([row(), row({ advisoryId: "9f3c2a71d04be6e1", incarnation: INC1 })]);
      const app = api(mem.source, instance === undefined ? {} : { instance });
      const refused = await post(app, ackBody({ incarnation: INC1 }));
      expect(refused.status).toBe(400);
      expect((await refused.json() as any).error).toEqual({ code: "bad_request", message: "body accepts source and sessionId only" });
      const legacy = await post(app, ackBody());
      expect(legacy.status).toBe(204);
      expect(await legacy.text()).toBe("");
      // The protected row: v1 is fenced, so 404 (nothing disclosed).
      expect((await post(app, ackBody(), ackPath("9f3c2a71d04be6e1"))).status).toBe(404);
      expect(mem.calls.filter((c) => c.startsWith("ackInstance") || c === "pendingInstances")).toEqual([]);
    }
  });
});

describe("READ_API_ADVISORY_INSTANCE_ENABLED on", () => {
  test("the GET reads the instance reader and answers contract advisories/v3 with each item's incarnation (null on a legacy row), last", async () => {
    const mem = instanceSource([row({ incarnation: INC1 }), row({ advisoryId: "9f3c2a71d04be6e1" })]);
    const r = await api(mem.source, { instance: true }).request(base, { headers: auth });
    expect(r.status).toBe(200);
    const body = await r.json() as any;
    expect(ADVISORIES_INSTANCE_CONTRACT).toBe("advisories/v3");
    expect(Object.keys(body)).toEqual(["contract", "advisories", "nextCursor"]);
    expect(body.contract).toBe("advisories/v3");
    expect(body.advisories.map((a: any) => [a.advisoryId, a.incarnation])).toEqual([[ADV, INC1], ["9f3c2a71d04be6e1", null]]);
    expect(Object.keys(body.advisories[0])).toEqual(["advisoryId", "urgency", "kind", "artifact", "peer", "createdAt", "message", "evidence", "incarnation"]);
    // Apart from the contract value and the added key, the item is the legacy item.
    const legacy = JSON.parse(LEGACY_GET).advisories[0];
    const { incarnation: _i, ...rest } = body.advisories[0];
    expect(rest).toEqual(legacy);
    expect(mem.calls).toEqual(["pendingInstances"]);
    // With the priority switch too: the M3-7 fields, then incarnation.
    const both = await (await api(instanceSource([row({ incarnation: INC1 })]).source, { instance: true, priority: true }).request(base, { headers: auth })).json() as any;
    expect(Object.keys(both.advisories[0])).toEqual(["advisoryId", "urgency", "kind", "artifact", "peer", "createdAt", "message", "evidence", "priority", "notifyEligible", "delivery", "incarnation"]);
  });

  test("POST with an incarnation acks that instance through ack_v2: 200 acked, then 200 already_acked; a stale or unknown instance is 404", async () => {
    const mem = instanceSource([row({ incarnation: INC1 })]);
    const app = api(mem.source, { instance: true });
    const stale = await post(app, ackBody({ incarnation: INC2 }));
    expect(stale.status).toBe(404);
    expect((await stale.json() as any).error).toEqual({ code: "not_found", message: "advisory not found" });
    const first = await post(app, ackBody({ incarnation: INC1.toUpperCase() }));
    expect(first.status).toBe(200);
    expect(await first.json()).toEqual({ contract: "advisories/v3", result: "acked" });
    const again = await post(app, ackBody({ incarnation: INC1 }));
    expect(again.status).toBe(200);
    expect(await again.json()).toEqual({ contract: "advisories/v3", result: "already_acked" });
    // The incarnation is passed lowercased; v1 is never called on this path.
    expect(mem.calls).toEqual([`ackInstance ${ADV} ${INC2}`, `ackInstance ${ADV} ${INC1}`, `ackInstance ${ADV} ${INC1}`]);
    // An unknown id with a well-formed incarnation: 404 as well.
    expect((await post(app, ackBody({ incarnation: INC1 }), ackPath("0000000000000000"))).status).toBe(404);
  });

  test("POST without an incarnation (or with null) is the v1 ack: 204 on a legacy row, 404 on a protected one", async () => {
    const mem = instanceSource([row(), row({ advisoryId: "9f3c2a71d04be6e1", incarnation: INC1 })]);
    const app = api(mem.source, { instance: true });
    const legacy = await post(app, ackBody());
    expect(legacy.status).toBe(204);
    expect(await legacy.text()).toBe("");
    expect((await post(app, ackBody({ incarnation: null }))).status).toBe(204);
    expect((await post(app, ackBody(), ackPath("9f3c2a71d04be6e1"))).status).toBe(404);
    expect((await post(app, ackBody({ incarnation: null }), ackPath("9f3c2a71d04be6e1"))).status).toBe(404);
    expect(mem.calls).toEqual([`ack ${ADV}`, `ack ${ADV}`, "ack 9f3c2a71d04be6e1", "ack 9f3c2a71d04be6e1"]);
  });

  test("a malformed incarnation or an unknown key is a 400 and nothing is acked", async () => {
    const mem = instanceSource([row({ incarnation: INC1 })]);
    const app = api(mem.source, { instance: true });
    for (const incarnation of ["", "not-a-uuid", `${INC1}x`, INC1.replaceAll("-", ""), 42, true, {}, [INC1]]) {
      const r = await post(app, ackBody({ incarnation }));
      expect([incarnation, r.status, (await r.json() as any).error.message]).toEqual([incarnation, 400, "incarnation must be a UUID"]);
    }
    const extra = await post(app, ackBody({ incarnation: INC1, generation: 2 }));
    expect(extra.status).toBe(400);
    expect((await extra.json() as any).error.message).toBe("body accepts source, sessionId and incarnation only");
    expect(mem.calls).toEqual([]);
  });

  test("notify eligibility is evaluated per row: a re-created (or promoted) instance of an allowlisted class is eligible afresh, never carried", async () => {
    const notifyRows = parseAdvisoryNotifyRows("duplicate_work:high");
    const mem = instanceSource([row({ kind: "duplicate_work", incarnation: INC1 })]);
    const app = api(mem.source, { instance: true, priority: true, notifyRows });
    const get = async () => (await (await app.request(base, { headers: auth })).json() as any).advisories;
    expect((await get()).map((a: any) => [a.incarnation, a.notifyEligible])).toEqual([[INC1, true]]);
    expect((await post(app, ackBody({ incarnation: INC1 }))).status).toBe(200);
    expect(await get()).toEqual([]);
    // The sweep deleted it and the scan re-created the same id: a new instance, eligible again.
    mem.recreate(ADV, { incarnation: INC2 });
    expect((await get()).map((a: any) => [a.advisoryId, a.incarnation, a.notifyEligible])).toEqual([[ADV, INC2, true]]);
    // A LOW instance of a class off the allowlist is not eligible; its HIGH promotion (a new instance) is.
    const low = instanceSource([row({ kind: "duplicate_work", urgency: "low", incarnation: INC1 })]);
    const lowApp = api(low.source, { instance: true, priority: true, notifyRows });
    const lowGet = async () => (await (await lowApp.request(base, { headers: auth })).json() as any).advisories.map((a: any) => [a.urgency, a.incarnation, a.notifyEligible]);
    expect(await lowGet()).toEqual([["low", INC1, false]]);
    low.recreate(ADV, { urgency: "high", incarnation: INC2 });
    expect(await lowGet()).toEqual([["high", INC2, true]]);
  });

  test("a source without the instance methods is an error with the switch on (the generic 503), never a silent legacy answer", async () => {
    const mem = instanceSource([row({ incarnation: INC1 })]);
    const { pendingInstances: _p, ackInstance: _a, ...legacyOnly } = mem.source;
    const app = api(legacyOnly, { instance: true });
    expect((await app.request(base, { headers: auth })).status).toBe(503);
    expect((await post(app, ackBody({ incarnation: INC1 }))).status).toBe(503);
    expect(mem.calls).toEqual([]);
  });
});

describe("instance switch on, 062220 not applied", () => {
  test("the GET and an instance ack answer 503 and log the missing function by name", async () => {
    const lines: Record<string, unknown>[] = [];
    const undefinedFn = () => Promise.reject(Object.assign(new Error("function does not exist"), { code: "42883" }));
    const source: AdvisoriesSource = { pending: async () => [], ack: async () => false, pendingInstances: undefinedFn, ackInstance: undefinedFn };
    const app = createReadApi({
      verify, adapter: {} as never, liveness: memoryLiveness().liveness, completeSinceMs: 0, directoryWorkspaces: new Set([ID.workspaceId]),
      coordinationAdvisories: { enabled: true, source }, advisoryInstance: true, log: (f: Record<string, unknown>) => { lines.push(f); },
    } as never);
    expect((await app.request(base, { headers: auth })).status).toBe(503);
    expect((await post(app as never, ackBody({ incarnation: INC1 }))).status).toBe(503);
    expect(lines.filter((l) => String(l.reason).endsWith("_missing")).map((l) => [l.reason, l.error_code, l.status]))
      .toEqual([["coordination_advisories_pending_for_v3_missing", "42883", 503], ["coordination_advisories_ack_v2_missing", "42883", 503]]);
  });
});

describe("sqlCoordinationAdvisories: the 062220 statements", () => {
  type Client = { query(text: string, params?: unknown[]): Promise<{ rows: unknown[] }>; release(): void };
  function pool(answer: (text: string) => unknown[]) {
    const log: string[] = [];
    const client: Client = {
      async query(text, params) {
        log.push(text === TENANT_SETTINGS_SQL ? `TENANT ${JSON.stringify(params)}` : text.startsWith("SELECT") ? `${text} ${JSON.stringify(params)}` : text);
        return { rows: answer(text) };
      },
      release: () => {},
    };
    return { log, pool: { connect: async () => client } };
  }
  const r = { orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" };

  test("pendingInstances reads pending_for_v3 in the READ ONLY runner with the page bound and maps incarnation (null kept null)", async () => {
    expect(PENDING_V3_SQL).toBe(`SELECT advisory_id, harness, session_id, peer_harness, peer_session_id, urgency, kind, artifact,
       evidence, created_at, delivered_at, incarnation
  FROM sessions.coordination_advisories_pending_for_v3($1, $2, $3, $4, $5)`);
    const texts: string[] = [];
    const db = (inc: string | null) => ({ advisory_id: ADV, harness: "codex", session_id: "s-1", peer_harness: "claude", peer_session_id: "p",
      urgency: "high", kind: "duplicate_work", artifact: "a", evidence: [], created_at: new Date("2026-11-12T10:00:00Z"), delivered_at: null, incarnation: inc });
    const read = async <T,>(_o: string, _w: string, work: (q: any) => Promise<T>) =>
      work({ query: async (text: string, params?: unknown[]) => { texts.push(`${text} ${JSON.stringify(params)}`); return { rows: [db(INC1), db(null)] }; } });
    const got = await sqlCoordinationAdvisories(read as never, (() => { throw new Error("no write"); }) as never).pendingInstances!(r);
    expect(got.map((a) => a.incarnation)).toEqual([INC1, null]);
    expect(texts).toEqual([`${PENDING_V3_SQL} ["org","ws","codex","s-1",10]`]);
  });

  test("ackInstance runs ack_v2 in one READ WRITE transaction with the tenant bound; an unexpected answer throws and rolls back", async () => {
    expect(ACK_V2_SQL).toBe("SELECT sessions.coordination_advisories_ack_v2($1, $2, $3, $4, $5, $6::uuid) AS result");
    let result: unknown = "acked";
    const p = pool((t) => t === ACK_V2_SQL ? [{ result }] : []);
    const src = sqlCoordinationAdvisories((() => { throw new Error("no read"); }) as never, poolTenantWriter(p.pool));
    for (const want of ["acked", "already_acked", "no_match"] as const) {
      result = want;
      expect(await src.ackInstance!(r, ADV, INC1)).toBe(want);
    }
    expect(p.log.slice(0, 4)).toEqual(["BEGIN READ WRITE", 'TENANT ["org","ws"]', `${ACK_V2_SQL} ["org","ws","${ADV}","codex","s-1","${INC1}"]`, "COMMIT"]);
    result = "true";
    await expect(src.ackInstance!(r, ADV, INC1)).rejects.toThrow("unexpected ack_v2 result");
    expect(p.log.at(-1)).toBe("ROLLBACK");
  });
});
