/**
 * Babysitter evidence reader (Neon GET /v1/delivery/records parity for its kinds) over the
 * native-order key-index page: query contract, account check, native (origin_id, record_id)
 * cursor, tombstones, exact-object GETs, every page bound resuming exactly, the route.
 * The SQL ordering/visibility (fence, reset, expiry, supersession, tombstones, scan bound)
 * is proven differentially on real PG17 in
 * src/session-ingest/__tests__/babysitter-retention.postgres.test.ts.
 */
import { describe, expect, test } from "bun:test";
import { createReadApi } from "../app.ts";
import { evidenceAccount, evidenceBucketPrefix, MAX_OBJECTS_PER_PAGE, readBabysitterEvidence, sqlEvidenceDeps, type EvidenceDeps, type EvidencePageRequest, type EvidencePageRow } from "../babysitter-evidence.ts";
import { deliveryAccount } from "../../session-ingest/vendor/delivery/prepare.ts";

const SCOPE = { orgId: "org-a", workspaceId: "ws-a" };
const ACCOUNT = evidenceAccount(SCOPE);
const M = "srv_" + "1".repeat(32);
const REF = "_bx2026112114-" + "a".repeat(16);
const obj = (i: number) => `b=${i.toString(16).padStart(32, "0")}.ndjson.zst`;
const keyOf = (o: string) => evidenceBucketPrefix("96bulls", SCOPE, { machineId: M, ref: REF }) + "raw/" + o;
const line = (r: string, rev = "1", over: Record<string, unknown> = {}) => JSON.stringify({ kind: "turn_receipt", origin_id: "o1", record_id: r, revision_id: `${r}:${rev}`, revision: rev,
  source: "claude", session_id: "s1", operation: "upsert", received_at: "2026-11-07T00:00:00.000Z", payload: { id: `p-${r}` }, ...over });
const up = (r: string, o = obj(0), l = 0, rev = "1"): EvidencePageRow => ({ originId: "o1", recordId: r, revision: rev, revisionId: `${r}:${rev}`, operation: "upsert", source: "claude", sessionId: "s1", machineId: M, ref: REF, object: o, line: l });
const del = (r: string, rev = "2"): EvidencePageRow => ({ originId: "o1", recordId: r, revision: rev, revisionId: `${r}:${rev}`, operation: "delete", source: "claude", sessionId: "s1" });

/** A page() double over pre-ordered rows (as SQL returns them): `after`, limit+1 and an optional scan bound. */
function deps(rows: EvidencePageRow[], objects: Record<string, string[]>, scanCap = Infinity) {
  const calls: string[] = [], reqs: EvidencePageRequest[] = [], gets: string[] = [];
  const d: EvidenceDeps = {
    keyRoot: "96bulls",
    page: async (_scope, req) => {
      calls.push("db:page"); reqs.push(req);
      const after = req.after ? rows.findIndex((r) => r.originId === req.after![0] && r.recordId === req.after![1]) + 1 : 0;
      const rest = rows.slice(after);
      const scanned = rest.slice(0, scanCap);
      const out = scanned.slice(0, req.limit);
      const more = rest.length > out.length && (out.length === req.limit || scanned.length < rest.length);
      const last = (out.length === req.limit ? out : scanned).at(-1);
      return { rows: out, more, scannedTo: last ? [last.originId, last.recordId] : null };
    },
    object: async (key) => {
      calls.push("s3:get"); gets.push(key);
      const name = key.slice(key.lastIndexOf("/") + 1);
      return new TextEncoder().encode(objects[name]!.join("\n") + "\n");
    },
  };
  return { d, calls, reqs, gets };
}

describe("babysitter evidence reader (native order over the key index)", () => {
  test("serves the live payload in Neon's envelope and record shape; GETs exactly the indexed object", async () => {
    const { d, gets } = deps([up("r1")], { [obj(0)]: [line("r1")] });
    const page = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt" }, ACCOUNT);
    expect(page).toEqual({ protocolVersion: 1, listing: "live", nextCursor: null, records: [{ schema_version: 1, origin_id: "o1", record_id: "r1",
      revision_id: "r1:1", revision: 1, kind: "turn_receipt", source: "claude", session_id: "s1", operation: "upsert", payload: { id: "p-r1" } }] });
    expect(gets).toEqual([keyOf(obj(0))]);
    expect(keyOf(obj(0))).toBe(`96bulls/org-a/v1/ws=ws-a/m=${M}/h=_babysitter/d=2026/11/21/s=${REF}/raw/${obj(0)}`);
  });

  test("a delete is Neon's tombstone exactly once (operation delete, payload null, the delete's revision); no object is read for it", async () => {
    const { d, gets } = deps([del("r1")], {});
    const page = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", include_deleted: "true" }, ACCOUNT);
    expect(page.records).toEqual([{ schema_version: 1, origin_id: "o1", record_id: "r1", revision_id: "r1:2", revision: 2, kind: "turn_receipt", source: "claude", session_id: "s1", operation: "delete", payload: null }]);
    expect(gets).toEqual([]);
  });

  test("native order and lines are taken from the index, not from storage order (several rows share one object at different lines)", async () => {
    const o = { [obj(0)]: [line("r3"), line("r1")], [obj(1)]: [line("r2")] };
    const { d, gets } = deps([up("r1", obj(0), 1), up("r2", obj(1), 0), up("r3", obj(0), 0)], o);
    expect((await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt" }, ACCOUNT)).records.map((r) => r.record_id)).toEqual(["r1", "r2", "r3"]);
    expect(new Set(gets).size).toBe(2);   // each object read once
  });

  test("paging: limit+1 decides more; the cursor is Neon's (origin_id, record_id) position, bound to the filter scope", async () => {
    const o = { [obj(0)]: [line("r1"), line("r2"), line("r3")] };
    const { d, reqs } = deps([up("r1", obj(0), 0), up("r2", obj(0), 1), up("r3", obj(0), 2)], o);
    const first = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", limit: "2" }, ACCOUNT);
    expect(first.records.map((r) => r.record_id)).toEqual(["r1", "r2"]);
    expect(reqs[0]).toEqual({ kind: "turn_receipt", source: null, sessionId: null, after: null, limit: 3, includeDeleted: false });
    const second = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", limit: "2", cursor: first.nextCursor! }, ACCOUNT);
    expect(reqs[1]!.after).toEqual(["o1", "r2"]);
    expect(second).toMatchObject({ nextCursor: null, records: [expect.objectContaining({ record_id: "r3" })] });
    await expect(readBabysitterEvidence(d, SCOPE, { kind: "session_lineage", cursor: first.nextCursor! }, ACCOUNT)).rejects.toMatchObject({ status: 400 });
    await expect(readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", include_deleted: "true", cursor: first.nextCursor! }, ACCOUNT)).rejects.toMatchObject({ status: 400 });
  });

  test("a scan-bounded SQL page (no rows, more) returns a resumable cursor at scannedTo, never a terminal empty page", async () => {
    const rows = [up("r1"), up("r2"), up("r3")];
    const capped = deps(rows, { [obj(0)]: [line("r1"), line("r2"), line("r3")] }, 1);
    capped.d.page = async (_s, req) => { capped.reqs.push(req); return { rows: [], more: true, scannedTo: ["o1", "r1"] }; };
    const q = await readBabysitterEvidence(capped.d, SCOPE, { kind: "turn_receipt" }, ACCOUNT);
    expect(q.records).toEqual([]);
    expect(q.nextCursor).not.toBeNull();
    capped.d.page = async (_s, req) => { capped.reqs.push(req); return { rows: [up("r2", obj(0), 1)], more: false, scannedTo: ["o1", "r2"] }; };
    const r = await readBabysitterEvidence(capped.d, SCOPE, { kind: "turn_receipt", cursor: q.nextCursor! }, ACCOUNT);
    expect(capped.reqs.at(-1)!.after).toEqual(["o1", "r1"]);
    expect(r).toMatchObject({ nextCursor: null, records: [expect.objectContaining({ record_id: "r2" })] });
    // "more" without a decided position is a backend fault, never a guessed cursor or a false end.
    capped.d.page = async () => ({ rows: [], more: true, scannedTo: null });
    await expect(readBabysitterEvidence(capped.d, SCOPE, { kind: "turn_receipt" }, ACCOUNT)).rejects.toMatchObject({ status: 503 });
  });

  test(`at most ${MAX_OBJECTS_PER_PAGE} distinct objects per page: the page ends at the last fully decided record and resumes there`, async () => {
    const n = MAX_OBJECTS_PER_PAGE + 8;
    const rows: EvidencePageRow[] = [], objects: Record<string, string[]> = {};
    for (let i = 0; i < n; i++) { const r = `r${String(i).padStart(3, "0")}`; rows.push(up(r, obj(i), 0)); objects[obj(i)] = [line(r)]; }
    const { d, gets } = deps(rows, objects);
    const first = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt" }, ACCOUNT);
    expect(first.records).toHaveLength(MAX_OBJECTS_PER_PAGE);
    expect(gets).toHaveLength(MAX_OBJECTS_PER_PAGE);
    const second = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", cursor: first.nextCursor! }, ACCOUNT);
    expect([...first.records, ...second.records].map((r) => r.record_id)).toEqual(rows.map((r) => r.recordId));
    expect(second.nextCursor).toBeNull();
  });

  test("an indexed line that is not exactly that revision of that record fails closed (503), never a wrong payload", async () => {
    for (const bad of [line("r9"), line("r1", "2"), line("r1", "1", { revision_id: "x" }), line("r1", "1", { kind: "session_lineage" }), line("r1", "1", { session_id: "other" }), "not json"]) {
      const { d } = deps([up("r1")], { [obj(0)]: [bad] });
      await expect(readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt" }, ACCOUNT)).rejects.toMatchObject({ status: 503, code: "backend_unavailable" });
    }
  });

  test("Neon's query contract: unknown filter 400, bad limit 400, non-Babysitter kind 501, bad cursor 400", async () => {
    const { d } = deps([up("r1")], { [obj(0)]: [line("r1")] });
    await expect(readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", nope: "1" }, ACCOUNT)).rejects.toMatchObject({ status: 400 });
    await expect(readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", limit: "0" }, ACCOUNT)).rejects.toMatchObject({ status: 400 });
    await expect(readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", limit: "101" }, ACCOUNT)).rejects.toMatchObject({ status: 400 });
    await expect(readBabysitterEvidence(d, SCOPE, { kind: "session_event" }, ACCOUNT)).rejects.toMatchObject({ status: 501 });
    await expect(readBabysitterEvidence(d, SCOPE, {}, ACCOUNT)).rejects.toMatchObject({ status: 501 });
    await expect(readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", cursor: "x!" }, ACCOUNT)).rejects.toMatchObject({ status: 400 });
  });

  test("the deadline reaches the object GET (the store is bound to the request signal) and maps to 503, never a hang", async () => {
    const seen: AbortSignal[] = [];
    const run = (async (_o: string, _w: string, work: (q: unknown) => unknown) => work({
      query: async () => ({ rows: [{ v: { rows: [up("r1")], more: false, scannedTo: ["o1", "r1"] } }] }),
    })) as never;
    const storeFor = (signal: AbortSignal) => ({ getObject: () => { seen.push(signal); return new Promise<never>((_, reject) => signal.addEventListener("abort", () => reject(signal.reason))); } });
    const d = { ...sqlEvidenceDeps(run, storeFor as never, async () => new Uint8Array(), (b) => b, (k) => k as never, "96bulls"), deadlineMs: 30 };
    const started = Date.now();
    await expect(readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt" }, ACCOUNT)).rejects.toMatchObject({ status: 503, code: "backend_unavailable" });
    expect(Date.now() - started).toBeLessThan(2000);
    expect(seen).toHaveLength(1);
    expect(seen[0]!.aborted).toBe(true);
  });

  test("backend errors map faithfully: a failing page RPC, a malformed page or a missing store is 503 with no message leak", async () => {
    const failing = (async () => { throw Object.assign(new Error("secret dsn host"), { code: "54000" }); }) as never;
    const malformed = (async (_o: string, _w: string, work: (q: unknown) => unknown) => work({ query: async () => ({ rows: [{ v: { rows: [{ originId: "o1" }], more: false } }] }) })) as never;
    for (const run of [failing, malformed]) {
      const d = sqlEvidenceDeps(run, (() => undefined) as never, async () => new Uint8Array(), (b) => b, (k) => k as never, "96bulls");
      const err = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt" }, ACCOUNT).catch((e) => e);
      expect(err).toMatchObject({ status: 503, code: "backend_unavailable" });
      expect(String(err.message)).not.toContain("secret");
    }
  });
});

describe("X-RelayHistory-Expected-Account (Neon requireDeliveryAccount parity)", () => {
  test("the account is exactly Neon's (the vendored relayhistory deliveryAccount)", async () => {
    expect(evidenceAccount(SCOPE)).toBe(await deliveryAccount(SCOPE));
    expect(evidenceAccount({ orgId: "oé", workspaceId: "" })).toBe(await deliveryAccount({ orgId: "oé", workspaceId: "" }));
  });
  for (const [label, header] of [["missing", undefined], ["mismatched", evidenceAccount({ orgId: "org-a", workspaceId: "ws-b" })]] as const)
    test(`a ${label} header is Neon's 403 delivery_account_mismatch with zero DB/S3 calls`, async () => {
      const { d, calls } = deps([up("r1")], { [obj(0)]: [line("r1")] });
      await expect(readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt" }, header)).rejects.toMatchObject({
        status: 403, code: "delivery_account_mismatch", message: "Delivery account does not match authenticated tenant" });
      expect(calls).toEqual([]);
    });
  test("a matching header serves (and only then are the DB and S3 read)", async () => {
    const { d, calls } = deps([up("r1")], { [obj(0)]: [line("r1")] });
    expect((await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt" }, ACCOUNT)).records).toHaveLength(1);
    expect(calls).toEqual(["db:page", "s3:get"]);
  });
});

describe("GET /v1/delivery/records route", () => {
  const verify = async () => ({ orgId: "org-a", workspaceId: "ws-a", userId: "u", scopes: ["rth:read"] }) as never;
  const req = (q: string, account: string | null = ACCOUNT) =>
    new Request(`https://read.example/v1/delivery/records${q}`, { headers: { authorization: "Bearer t", ...(account === null ? {} : { "X-RelayHistory-Expected-Account": account }) } });
  test("default off: 501 without the deps", async () => {
    const app = createReadApi({ verify, ch: {} as never, liveness: {} as never, completeSinceMs: 0 });
    expect((await app.fetch(req("?kind=turn_receipt"))).status).toBe(501);
  });
  test("tenant from the verified token; repeated filters 400; the route returns exactly the direct reader's pages (live, tombstone, cursor)", async () => {
    const rows = [up("r1", obj(0), 0), del("r2"), up("r3", obj(0), 1)];
    const objects = { [obj(0)]: [line("r1"), line("r3")] };
    const { d } = deps(rows, objects);
    const app = createReadApi({ verify, ch: {} as never, liveness: {} as never, completeSinceMs: 0, babysitter: d });
    expect((await app.fetch(req("?kind=turn_receipt&kind=turn_receipt"))).status).toBe(400);
    let cursor = "";
    const viaRoute: unknown[] = [], direct: unknown[] = [];
    for (let i = 0; i < 4; i++) {
      const res = await app.fetch(req(`?kind=turn_receipt&include_deleted=true&limit=1${cursor ? `&cursor=${cursor}` : ""}`));
      expect(res.status).toBe(200);
      const body = await res.json() as { records: unknown[]; nextCursor: string | null };
      const same = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", include_deleted: "true", limit: "1", ...(cursor ? { cursor } : {}) }, ACCOUNT);
      expect(body).toEqual(same as never);
      viaRoute.push(...body.records); direct.push(...same.records);
      if (!body.nextCursor) break;
      cursor = body.nextCursor;
    }
    expect(viaRoute.map((r) => (r as { record_id: string; operation: string }).record_id + ":" + (r as { operation: string }).operation)).toEqual(["r1:upsert", "r2:delete", "r3:upsert"]);
    expect(viaRoute).toEqual(direct);
  });
  test("route: missing or mismatched account header is Neon's 403 envelope before any read; matching serves", async () => {
    const { d, calls } = deps([up("r1")], { [obj(0)]: [line("r1")] });
    const app = createReadApi({ verify, ch: {} as never, liveness: {} as never, completeSinceMs: 0, babysitter: d });
    for (const account of [null, evidenceAccount({ orgId: "org-b", workspaceId: "ws-a" }), ""]) {
      const res = await app.fetch(req("?kind=turn_receipt", account));
      expect(res.status).toBe(403);
      expect((await res.json()).error).toEqual({ code: "delivery_account_mismatch", message: "Delivery account does not match authenticated tenant" });
    }
    expect(calls).toEqual([]);
    expect((await app.fetch(req("?kind=turn_receipt"))).status).toBe(200);
  });
});
