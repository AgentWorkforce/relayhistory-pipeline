/**
 * Babysitter evidence reader (Neon GET /v1/delivery/records parity for its kinds): query
 * contract, tenant pin, expiry, supersession/tombstones, include_deleted, paging, the
 * default-off route. The SQL visibility (fence, reset, erase) is proven on real PG17 in
 * src/session-ingest/__tests__/babysitter-retention.postgres.test.ts.
 */
import { describe, expect, test } from "bun:test";
import { createReadApi } from "../app.ts";
import { evidenceAccount, evidenceBucketPrefix, MAX_OBJECTS_PER_PAGE, readBabysitterEvidence, sqlEvidenceDeps, type EvidenceBucket, type EvidenceDeps } from "../babysitter-evidence.ts";
import { deliveryAccount } from "../../session-ingest/vendor/delivery/prepare.ts";

const SCOPE = { orgId: "org-a", workspaceId: "ws-a" };
const ACCOUNT = evidenceAccount(SCOPE);
const M = "srv_" + "1".repeat(32);
const NOW = Date.UTC(2026, 10, 20);
const line = (over: Record<string, unknown>) => JSON.stringify({ kind: "turn_receipt", origin_id: "o1", record_id: "r1", revision_id: "r1:1", revision: "1",
  source: "claude", session_id: "s1", operation: "upsert", received_at: "2026-11-07T00:00:00.000Z", payload: { id: "tr", expires_at_ms: NOW + 86_400_000 }, ...over });

/** buckets: ref -> one object's lines, or several objects (string[][]); keyset-paged like the SQL RPC. */
function deps(buckets: Record<string, string[] | string[][]>, committed: Record<string, [bigint, string]> = {}): EvidenceDeps & { scopes: unknown[]; calls: string[]; reads: string[] } {
  const scopes: unknown[] = [];
  const calls: string[] = [];
  const reads: string[] = [];
  const cmp = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
  const list: EvidenceBucket[] = Object.keys(buckets).sort(cmp).map((ref) => ({ machineId: M, ref, source: "claude", sessionId: "s1" }));
  const objects = (ref: string): string[][] => { const v = buckets[ref]!; return v.length && Array.isArray(v[0]) ? (v as string[][]) : [v as string[]]; };
  return {
    scopes,
    calls,
    reads,
    keyRoot: "96bulls",
    now: () => NOW,
    buckets: async (scope, _filter, after, limit) => {
      calls.push("db:buckets"); scopes.push(scope);
      return list.filter((b) => !after || cmp(b.ref, after.ref) > 0 || (after.inclusive && b.ref === after.ref)).slice(0, limit);
    },
    revisions: async (_scope, keys) => {
      calls.push("db:revisions");
      const out = new Map<string, { revision: bigint; revisionId: string }>();
      for (const [o, r] of keys) { const c = committed[`${o}|${r}`]; if (c) out.set(JSON.stringify([o, r]), { revision: c[0], revisionId: c[1] }); }
      return out;
    },
    objectKeys: async (prefix) => {
      calls.push("s3:list");
      const ref = /s=(_bx[^/]+)\//.exec(prefix)![1]!;
      return objects(ref).map((_, i) => `${prefix}raw/b=${String(i).padStart(4, "0")}.ndjson.zst`);
    },
    object: async (key) => {
      calls.push("s3:get"); reads.push(key);
      const ref = /s=(_bx[^/]+)\//.exec(key)![1]!, i = Number(/b=(\d+)\./.exec(key)![1]);
      return new TextEncoder().encode(objects(ref)[i]!.join("\n") + "\n");
    },
  };
}
const ref = (i: number) => `_bx2026112114-${i.toString(16).padStart(16, "0")}`;
const A = "_bx2026112114-" + "a".repeat(16), B = "_bx2026112215-" + "b".repeat(16);

describe("babysitter evidence reader", () => {
  test("serves the live payload in Neon's envelope and record shape, tenant-pinned", async () => {
    const d = deps({ [A]: [line({})] });
    const page = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt" }, ACCOUNT);
    expect(d.scopes).toEqual([SCOPE]);
    expect(page).toEqual({ protocolVersion: 1, listing: "live", nextCursor: null, records: [{ schema_version: 1, origin_id: "o1", record_id: "r1",
      revision_id: "r1:1", revision: 1, kind: "turn_receipt", source: "claude", session_id: "s1", operation: "upsert", payload: { id: "tr", expires_at_ms: NOW + 86_400_000 } }] });
    expect(evidenceBucketPrefix("96bulls", SCOPE, { machineId: M, ref: A, source: "claude", sessionId: "s1" })).toBe(`96bulls/org-a/v1/ws=ws-a/m=${M}/h=_babysitter/d=2026/11/21/s=${A}/`);
  });

  test("expiry: an expired record is unreadable (include_deleted: a payload-free tombstone)", async () => {
    const d = deps({ [A]: [line({ payload: { expires_at_ms: NOW - 1 } })] });
    expect((await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt" }, ACCOUNT)).records).toEqual([]);
    const t = (await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", include_deleted: "true" }, ACCOUNT)).records;
    expect(t).toEqual([expect.objectContaining({ operation: "delete", payload: null, revision: 1 })]);
  });

  test("supersession: the committed revision's line wins; an older line is never served, not even under include_deleted", async () => {
    const two = deps({ [A]: [line({})], [B]: [line({ revision: "2", revision_id: "r1:2", payload: { id: "v2", expires_at_ms: NOW + 1000 } })] }, { "o1|r1": [2n, "r1:2"] });
    expect((await readBabysitterEvidence(two, SCOPE, { kind: "turn_receipt" }, ACCOUNT)).records.map((r) => r.payload?.id)).toEqual(["v2"]);
    // No delete@2 beside the upsert@2 (a page cannot know the newer revision is live elsewhere).
    expect((await readBabysitterEvidence(two, SCOPE, { kind: "turn_receipt", include_deleted: "true" }, ACCOUNT)).records)
      .toEqual([expect.objectContaining({ operation: "upsert", revision: 2 })]);
    const tomb = deps({ [A]: [line({})] }, { "o1|r1": [3n, "r1:3"] });
    expect((await readBabysitterEvidence(tomb, SCOPE, { kind: "turn_receipt" }, ACCOUNT)).records).toEqual([]);
    expect((await readBabysitterEvidence(tomb, SCOPE, { kind: "turn_receipt", include_deleted: "true" }, ACCOUNT)).records).toEqual([]);
    // A committed revision equal to the line (the common case) keeps it visible.
    const same = deps({ [A]: [line({})] }, { "o1|r1": [1n, "r1:1"] });
    expect((await readBabysitterEvidence(same, SCOPE, { kind: "turn_receipt" }, ACCOUNT)).records).toHaveLength(1);
  });

  test("kind filter; a line whose (source, session) disagrees with its bucket row is never served", async () => {
    const d = deps({ [A]: [line({}), line({ record_id: "r2", kind: "session_lineage" }), line({ record_id: "r3", session_id: "other" })] });
    expect((await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt" }, ACCOUNT)).records.map((r) => r.record_id)).toEqual(["r1"]);
    expect((await readBabysitterEvidence(d, SCOPE, { kind: "session_lineage" }, ACCOUNT)).records.map((r) => r.record_id)).toEqual(["r2"]);
  });

  test("Neon's query contract: unknown filter 400, bad limit 400, non-Babysitter kind 501, bad cursor 400", async () => {
    const d = deps({ [A]: [line({})] });
    await expect(readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", nope: "1" }, ACCOUNT)).rejects.toMatchObject({ status: 400 });
    await expect(readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", limit: "0" }, ACCOUNT)).rejects.toMatchObject({ status: 400 });
    await expect(readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", limit: "101" }, ACCOUNT)).rejects.toMatchObject({ status: 400 });
    await expect(readBabysitterEvidence(d, SCOPE, { kind: "session_event" }, ACCOUNT)).rejects.toMatchObject({ status: 501 });
    await expect(readBabysitterEvidence(d, SCOPE, {}, ACCOUNT)).rejects.toMatchObject({ status: 501 });
    await expect(readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", cursor: "x!" }, ACCOUNT)).rejects.toMatchObject({ status: 400 });
  });

  test("paging: stable (bucket, object, line) order, opaque cursor bound to the filter scope", async () => {
    const lines = ["r3", "r1", "r2"].map((r) => line({ record_id: r, revision_id: `${r}:1` }));
    const d = deps({ [A]: lines });
    const first = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", limit: "2" }, ACCOUNT);
    expect(first.records.map((r) => r.record_id)).toEqual(["r3", "r1"]);
    const second = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", limit: "2", cursor: first.nextCursor! }, ACCOUNT);
    expect(second).toMatchObject({ nextCursor: null, records: [expect.objectContaining({ record_id: "r2" })] });
    await expect(readBabysitterEvidence(d, SCOPE, { kind: "session_lineage", cursor: first.nextCursor! }, ACCOUNT)).rejects.toMatchObject({ status: 400 });
  });

  test("P1: 3000 buckets with limit=1 is ONE page reading O(limit) objects (no 2000-bucket raise, no full scan)", async () => {
    const many: Record<string, string[]> = {};
    for (let i = 0; i < 3000; i++) many[ref(i)] = [line({ record_id: `r${i}`, revision_id: `r${i}:1` })];
    const d = deps(many);
    const page = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", limit: "1" }, ACCOUNT);
    expect(page.records.map((r) => r.record_id)).toEqual(["r0"]);
    expect(page.nextCursor).not.toBeNull();
    expect(d.calls.filter((c) => c === "db:buckets")).toHaveLength(1);
    expect(d.reads).toHaveLength(2);   // the record and the limit+1 lookahead
    // Continuation: never rereads an object before the cursor.
    d.reads.length = 0;
    const next = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", limit: "1", cursor: page.nextCursor! }, ACCOUNT);
    expect(next.records.map((r) => r.record_id)).toEqual(["r1"]);
    expect(d.reads.some((k) => k.includes(ref(0)))).toBe(false);
    expect(d.reads.length).toBeLessThanOrEqual(2);
  });

  test("P1: walking every page yields each record exactly once, across buckets, objects and lines", async () => {
    const set: Record<string, string[][]> = {};
    const want: string[] = [];
    for (let b = 0; b < 70; b++) {   // > one 64-bucket SQL page
      set[ref(b)] = [0, 1].map((o) => [0, 1, 2].map((l) => { const id = `r${b}-${o}-${l}`; want.push(id); return line({ record_id: id, revision_id: `${id}:1` }); }));
    }
    const d = deps(set);
    const got: string[] = [];
    let cursor: string | null = null, pages = 0;
    do {
      const page = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", limit: "4", ...(cursor ? { cursor } : {}) }, ACCOUNT);
      got.push(...page.records.map((r) => r.record_id));
      cursor = page.nextCursor;
      expect(++pages).toBeLessThan(200);
    } while (cursor);
    expect(got).toEqual(want);
  });

  test("P1: a run of invisible objects ends the page at the work bound with a cursor (bounded work, no lost records)", async () => {
    const set: Record<string, string[]> = {};
    for (let i = 0; i < MAX_OBJECTS_PER_PAGE + 5; i++) set[ref(i)] = [line({ record_id: `x${i}`, kind: "session_lineage" })];
    set[ref(MAX_OBJECTS_PER_PAGE + 5)] = [line({ record_id: "last" })];
    const d = deps(set);
    const first = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt" }, ACCOUNT);
    expect(first.records).toEqual([]);
    expect(first.nextCursor).not.toBeNull();
    expect(d.reads).toHaveLength(MAX_OBJECTS_PER_PAGE);
    const second = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", cursor: first.nextCursor! }, ACCOUNT);
    expect(second).toMatchObject({ nextCursor: null, records: [expect.objectContaining({ record_id: "last" })] });
  });

  test("P1: the deadline aborts store.list (the signal reaches the store) and maps to 503, never a hang", async () => {
    const seen: AbortSignal[] = [];
    const run = (async (_o: string, _w: string, work: (q: unknown) => unknown) => work({
      query: async (sql: string) => ({ rows: [{ v: sql.includes("buckets_for") ? [{ machineId: M, ref: A, source: "claude", sessionId: "s1" }] : [] }] }),
    })) as never;
    const storeFor = (signal: AbortSignal) => ({
      list: () => { seen.push(signal); return new Promise<never>((_, reject) => signal.addEventListener("abort", () => reject(signal.reason))); },
      getObject: async () => { throw new Error("unreached"); },
    });
    const d = { ...sqlEvidenceDeps(run, storeFor as never, async () => new Uint8Array(), (b) => b, (k) => k as never, "96bulls"), deadlineMs: 30 };
    const started = Date.now();
    await expect(readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt" }, ACCOUNT)).rejects.toMatchObject({ status: 503, code: "backend_unavailable" });
    expect(Date.now() - started).toBeLessThan(2000);
    expect(seen).toHaveLength(1);
    expect(seen[0]!.aborted).toBe(true);
  });

  test("backend errors map faithfully: a failing bucket RPC or store is 503 backend_unavailable with no message leak", async () => {
    const run = (async () => { throw Object.assign(new Error("secret dsn host"), { code: "54000" }); }) as never;
    const d = sqlEvidenceDeps(run, (() => undefined) as never, async () => new Uint8Array(), (b) => b, (k) => k as never, "96bulls");
    const err = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt" }, ACCOUNT).catch((e) => e);
    expect(err).toMatchObject({ status: 503, code: "backend_unavailable" });
    expect(String(err.message)).not.toContain("secret");
  });
});

describe("X-RelayHistory-Expected-Account (Neon requireDeliveryAccount parity)", () => {
  test("the account is exactly Neon's (the vendored relayhistory deliveryAccount)", async () => {
    expect(evidenceAccount(SCOPE)).toBe(await deliveryAccount(SCOPE));
    expect(evidenceAccount({ orgId: "o\u00e9", workspaceId: "" })).toBe(await deliveryAccount({ orgId: "o\u00e9", workspaceId: "" }));
  });
  for (const [label, header] of [["missing", undefined], ["mismatched", evidenceAccount({ orgId: "org-a", workspaceId: "ws-b" })]] as const)
    test(`a ${label} header is Neon's 403 delivery_account_mismatch with zero DB/S3 calls`, async () => {
      const d = deps({ [A]: [line({})] });
      await expect(readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt" }, header)).rejects.toMatchObject({
        status: 403, code: "delivery_account_mismatch", message: "Delivery account does not match authenticated tenant" });
      expect(d.calls).toEqual([]);
    });
  test("a matching header serves (and only then are the DB and S3 read)", async () => {
    const d = deps({ [A]: [line({})] });
    expect((await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt" }, ACCOUNT)).records).toHaveLength(1);
    expect(d.calls).toEqual(["db:buckets", "s3:list", "s3:get", "db:revisions"]);
  });
});

describe("GET /v1/delivery/records route", () => {
  const verify = async () => ({ orgId: "org-a", workspaceId: "ws-a", userId: "u", scopes: ["rth:read"] }) as never;
  const req = (q: string, account: string | null = ACCOUNT) =>
    new Request(`https://read.example/v1/delivery/records${q}`, { headers: { authorization: "Bearer t", ...(account === null ? {} : { "X-RelayHistory-Expected-Account": account }) } });
  test("default off: 501 without the deps", async () => {
    const app = createReadApi({ verify, ch: {} as never, liveness: {} as never, completeSinceMs: 0 });
    const res = await app.fetch(req("?kind=turn_receipt"));
    expect(res.status).toBe(501);
  });
  test("tenant from the verified token; repeated filters 400; served in Neon's envelope", async () => {
    const d = deps({ [A]: [line({})] });
    const app = createReadApi({ verify, ch: {} as never, liveness: {} as never, completeSinceMs: 0, babysitter: d });
    expect((await app.fetch(req("?kind=turn_receipt&kind=turn_receipt"))).status).toBe(400);
    const res = await app.fetch(req("?kind=turn_receipt"));
    expect(res.status).toBe(200);
    expect((await res.json()).records).toHaveLength(1);
    expect(d.scopes).toEqual([{ orgId: "org-a", workspaceId: "ws-a" }]);
  });
  test("route: missing or mismatched account header is Neon's 403 envelope before any read; matching serves", async () => {
    const d = deps({ [A]: [line({})] });
    const app = createReadApi({ verify, ch: {} as never, liveness: {} as never, completeSinceMs: 0, babysitter: d });
    for (const account of [null, evidenceAccount({ orgId: "org-b", workspaceId: "ws-a" }), ""]) {
      const res = await app.fetch(req("?kind=turn_receipt", account));
      expect(res.status).toBe(403);
      expect((await res.json()).error).toEqual({ code: "delivery_account_mismatch", message: "Delivery account does not match authenticated tenant" });
    }
    expect(d.calls).toEqual([]);
    expect((await app.fetch(req("?kind=turn_receipt"))).status).toBe(200);
  });
});
