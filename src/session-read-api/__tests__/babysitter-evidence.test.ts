/**
 * Babysitter evidence reader (Neon GET /v1/delivery/records parity for its kinds): query
 * contract, tenant pin, expiry, supersession/tombstones, include_deleted, paging, the
 * default-off route. The SQL visibility (fence, reset, erase) is proven on real PG17 in
 * src/session-ingest/__tests__/babysitter-retention.postgres.test.ts.
 */
import { describe, expect, test } from "bun:test";
import { createReadApi } from "../app.ts";
import { evidenceAccount, evidenceBucketPrefix, readBabysitterEvidence, type EvidenceBucket, type EvidenceDeps } from "../babysitter-evidence.ts";
import { deliveryAccount } from "../../session-ingest/vendor/delivery/prepare.ts";

const SCOPE = { orgId: "org-a", workspaceId: "ws-a" };
const ACCOUNT = evidenceAccount(SCOPE);
const M = "srv_" + "1".repeat(32);
const NOW = Date.UTC(2026, 10, 20);
const line = (over: Record<string, unknown>) => JSON.stringify({ kind: "turn_receipt", origin_id: "o1", record_id: "r1", revision_id: "r1:1", revision: "1",
  source: "claude", session_id: "s1", operation: "upsert", received_at: "2026-11-07T00:00:00.000Z", payload: { id: "tr", expires_at_ms: NOW + 86_400_000 }, ...over });

function deps(buckets: Record<string, string[]>, committed: Record<string, [bigint, string]> = {}): EvidenceDeps & { scopes: unknown[]; calls: string[] } {
  const scopes: unknown[] = [];
  const calls: string[] = [];
  const list: EvidenceBucket[] = Object.keys(buckets).map((ref) => ({ machineId: M, ref, source: "claude", sessionId: "s1" }));
  return {
    scopes,
    calls,
    keyRoot: "96bulls",
    now: () => NOW,
    buckets: async (scope) => { calls.push("db:buckets"); scopes.push(scope); return list; },
    revisions: async (_scope, keys) => {
      calls.push("db:revisions");
      const out = new Map<string, { revision: bigint; revisionId: string }>();
      for (const [o, r] of keys) { const c = committed[`${o}|${r}`]; if (c) out.set(JSON.stringify([o, r]), { revision: c[0], revisionId: c[1] }); }
      return out;
    },
    objects: async (prefix) => {
      calls.push("s3:objects");
      const ref = /s=(_bx[^/]+)\//.exec(prefix)![1]!;
      return [new TextEncoder().encode(buckets[ref]!.join("\n") + "\n")];
    },
  };
}
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

  test("supersession: the newest bucket line wins; a newer committed revision (update or tombstone) hides it", async () => {
    const two = deps({ [A]: [line({})], [B]: [line({ revision: "2", revision_id: "r1:2", payload: { id: "v2", expires_at_ms: NOW + 1000 } })] });
    expect((await readBabysitterEvidence(two, SCOPE, { kind: "turn_receipt" }, ACCOUNT)).records.map((r) => r.payload?.id)).toEqual(["v2"]);
    const tomb = deps({ [A]: [line({})] }, { "o1|r1": [3n, "r1:3"] });
    expect((await readBabysitterEvidence(tomb, SCOPE, { kind: "turn_receipt" }, ACCOUNT)).records).toEqual([]);
    expect((await readBabysitterEvidence(tomb, SCOPE, { kind: "turn_receipt", include_deleted: "true" }, ACCOUNT)).records)
      .toEqual([expect.objectContaining({ operation: "delete", payload: null, revision: 3, revision_id: "r1:3" })]);
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

  test("paging: ordered by (origin, record), opaque cursor bound to the filter scope", async () => {
    const lines = ["r3", "r1", "r2"].map((r) => line({ record_id: r, revision_id: `${r}:1` }));
    const d = deps({ [A]: lines });
    const first = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", limit: "2" }, ACCOUNT);
    expect(first.records.map((r) => r.record_id)).toEqual(["r1", "r2"]);
    const second = await readBabysitterEvidence(d, SCOPE, { kind: "turn_receipt", limit: "2", cursor: first.nextCursor! }, ACCOUNT);
    expect(second).toMatchObject({ nextCursor: null, records: [expect.objectContaining({ record_id: "r3" })] });
    await expect(readBabysitterEvidence(d, SCOPE, { kind: "session_lineage", cursor: first.nextCursor! }, ACCOUNT)).rejects.toMatchObject({ status: 400 });
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
    expect(d.calls).toEqual(["db:buckets", "s3:objects", "db:revisions"]);
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
