/**
 * Grant lifecycle against a fake RelayAuth grant client, a fixture membership
 * source shaped like Cloud's tables, and a fixture identity preflight. No real
 * RelayAuth or Cloud, no minting, no network.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash, randomBytes } from "node:crypto";
import { mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { deriveMigrationGrantPlan } from "../vendor/relayauth-migration-plan.ts";
import type { UploadInput } from "../relayhistory-export-upload.ts";
import {
  applyGrants,
  canonicalizeUploads,
  FixtureIdentityPreflight,
  FixtureMembershipSource,
  lifecycleDryRun,
  lifecyclePlan,
  main,
  resolveCloudId,
  revokeGrants,
  shardIdempotencyKey,
  isValidIdempotencyKey,
  GrantClientError,
  validateIdMapping,
  EMPTY_ID_MAPPING,
  type ApplyConfig,
  type GrantClient,
  type IdMappingLedger,
  type MembershipFixture,
  type PlanInput,
} from "../relayhistory-export-grants.ts";

// Each test shards 2,500 sessions through the oracle; CI runners are slower.
setDefaultTimeout(30_000);

const EXPORTED_AT = "2026-10-30T00:00:00.000Z";
const READ_AT = "2026-10-30T12:00:00.000Z";
const NOW = Date.parse("2026-10-30T12:30:00.000Z");
const sha = (s: string) => createHash("sha256").update(s).digest("hex");
/** A deterministic canonical UUID for a label. */
const uuid = (label: string) => {
  const h = sha(label);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-8${h.slice(17, 20)}-${h.slice(20, 32)}`;
};
const ORG = uuid("org");
const OTHER_ORG = uuid("org-other");
const WS1 = uuid("ws1");
const WS2 = uuid("ws2");
const USERS = Array.from({ length: 10 }, (_, i) => uuid(`user${i}`));
const AUDIENCE = "https://history.example.com";
const GRANT = { orgId: ORG, apiKeyId: "ak_test", audience: AUDIENCE, notAfter: "2026-11-30T00:00:00.000Z" };

function upload(session: string, user: string, ws: string, org = ORG): UploadInput {
  return {
    key: `v1/ws=${ws}/m=m1/h=claude/d=2026/10/01/s=${session}/manifest.json`,
    org_id: org,
    workspace_id: ws,
    source_user_id: user,
    session: { machine_id: "m1", harness: "claude", session_id: session, started_at: "2026-10-01T00:00:00.000Z" },
    state: "closed",
    chunks: [{ seq: 0, epoch: 0, start_offset: 0, bytes: 1000, sha256: sha(session), records: 1 }],
  };
}

/** 2,500 sessions; even users in WS1, odd in WS2: three shards, 10 pairs. */
const UPLOADS = Array.from({ length: 2500 }, (_, i) => upload(`s${String(i).padStart(4, "0")}`, USERS[i % 10]!, i % 2 ? WS2 : WS1));

const membershipFixture = (over: Partial<MembershipFixture> = {}): MembershipFixture => ({
  source: "fixture:cloud",
  read_at: READ_AT,
  workspaces: [{ id: WS1, organization_id: ORG }, { id: WS2, organization_id: ORG }],
  users: USERS.map((id) => ({ id })),
  memberships: USERS.map((user_id) => ({ organization_id: ORG, user_id, status: "active" })),
  ...over,
});
const identityFixture = (over: Partial<{ identities: { user_id: string; org_id: string; kind: string; status: string }[] }> = {}) => ({
  source: "fixture:relayauth-identities",
  read_at: READ_AT,
  identities: USERS.map((user_id) => ({ user_id, org_id: ORG, kind: "human", status: "active" })),
  ...over,
});

const input = (over: Partial<PlanInput> = {}): PlanInput => ({
  uploads: UPLOADS,
  exportedAt: EXPORTED_AT,
  grant: GRANT,
  membership: new FixtureMembershipSource(membershipFixture()),
  identities: new FixtureIdentityPreflight(identityFixture()),
  ...over,
});

/**
 * RelayAuth #111 + #112 grant routes, in memory: Idempotency-Key is required
 * and validated; the same key with the same request returns the original
 * grant (replayed), the same key with another request is 409
 * idempotency_key_conflict. The raw key is held only to compare, like the
 * server's receipt hash.
 */
class FakeRelayAuth implements GrantClient {
  grants = new Map<string, { revokedAt: string | null; key: string; fingerprint: string; createdAt: string }>();
  creates = 0;
  revokes = 0;
  keys: string[] = [];
  replays = 0;
  /** The n-th create is refused with this answer. */
  refuseCreateAt?: { n: number; code: string; status: number };
  /** The n-th create (or every create from n on) lands but its response is lost. */
  loseCreateAt?: number;
  loseFrom?: number;
  async createGrant(req: Parameters<GrantClient["createGrant"]>[0]) {
    this.creates += 1;
    this.keys.push(req.idempotencyKey);
    if (!isValidIdempotencyKey(req.idempotencyKey)) throw new GrantClientError("invalid_idempotency_key", 400);
    if (this.refuseCreateAt?.n === this.creates) throw new GrantClientError(this.refuseCreateAt.code, this.refuseCreateAt.status);
    const fingerprint = sha(JSON.stringify({ a: req.apiKeyId, u: req.audience, n: req.notAfter, p: req.plan }));
    const existing = [...this.grants].find(([, g]) => g.key === req.idempotencyKey);
    if (existing && existing[1].fingerprint === "") existing[1].fingerprint = fingerprint; // seeded orphan
    if (existing && existing[1].fingerprint !== fingerprint) throw new GrantClientError("idempotency_key_conflict", 409);
    const grantId = existing?.[0] ?? `mgr_${randomBytes(16).toString("hex")}`;
    if (!existing) this.grants.set(grantId, { revokedAt: null, key: req.idempotencyKey, fingerprint, createdAt: new Date(NOW).toISOString() });
    else this.replays += 1;
    if (this.loseCreateAt === this.creates || (this.loseFrom !== undefined && this.creates >= this.loseFrom)) throw new Error("response lost");
    const g = this.grants.get(grantId)!;
    const derived = deriveMigrationGrantPlan({ grantId, orgId: ORG, plan: req.plan });
    return { grantId, orgId: ORG, planDigest: derived.planDigest, allowedPairs: derived.allowedPairs, planEntries: derived.plan.manifests.length, createdAt: g.createdAt, revokedAt: g.revokedAt, replayed: !!existing };
  }
  /** The n-th revoke lands but its response is lost (or every one from n on). */
  loseRevokeAt?: number;
  loseRevokesFrom?: number;
  /** relayauth #112: 404 migration_grant_not_found; 409 migration_grant_revoked when already revoked. */
  async revokeGrant(grantId: string) {
    this.revokes += 1;
    const g = this.grants.get(grantId);
    if (!g) throw new GrantClientError("migration_grant_not_found", 404);
    if (g.revokedAt) throw new GrantClientError("migration_grant_revoked", 409);
    g.revokedAt = new Date(NOW + this.revokes).toISOString();
    if (this.loseRevokeAt === this.revokes || (this.loseRevokesFrom !== undefined && this.revokes >= this.loseRevokesFrom)) throw new Error("response lost");
    return { grantId, revokedAt: g.revokedAt };
  }
  live() {
    return [...this.grants.values()].filter((g) => !g.revokedAt).length;
  }
}

const dirs: string[] = [];
afterEach(async () => {
  await Promise.all(dirs.splice(0).map((d) => rm(d, { recursive: true, force: true })));
});

async function approved(over: Partial<ApplyConfig> = {}, client = new FakeRelayAuth()) {
  const dir = await mkdtemp(join(tmpdir(), "grants-"));
  dirs.push(dir);
  const base = input(over);
  const plan = await lifecyclePlan(base);
  const cfg: ApplyConfig = {
    ...base,
    execute: true,
    approvedDigest: plan.aggregate_digest,
    approvedSnapshotId: plan.source_snapshot.snapshot_id,
    allowedAudiences: [AUDIENCE],
    maxSnapshotAgeMs: 24 * 3600_000,
    client,
    stateDir: dir,
    now: () => NOW,
    ...over,
  };
  return { plan, client, cfg, dir };
}

describe("plan", () => {
  test("the aggregate digest is deterministic and pins both snapshots and the mapping", async () => {
    const a = await lifecyclePlan(input());
    const b = await lifecyclePlan(input({
      uploads: [...UPLOADS].reverse(),
      membership: new FixtureMembershipSource(membershipFixture({ memberships: [...membershipFixture().memberships].reverse() })),
    }));
    expect(b.aggregate_digest).toBe(a.aggregate_digest);
    expect(a.source_snapshot.snapshot_id).toMatch(/^fixture:cloud:[0-9a-f]{64}$/);
    expect(a.shards).toHaveLength(3);
    expect(a.pair_counts).toEqual({ total: 10, matched: 10, exceptions: 0 });
    expect(a.run_blocked).toBe(false);
    expect(a.shards.every((s) => !s.blocked)).toBe(true);
    const c = await lifecyclePlan(input({ grant: { ...GRANT, notAfter: "2026-12-01T00:00:00.000Z" } }));
    expect(c.aggregate_digest).not.toBe(a.aggregate_digest);
    const d = await lifecyclePlan(input({ membership: new FixtureMembershipSource(membershipFixture({ read_at: "2026-10-30T12:00:01.000Z" })) }));
    expect(d.aggregate_digest).not.toBe(a.aggregate_digest); // read_at is part of the snapshot
  });

  test("the plan output never contains an idempotency key", async () => {
    const plan = await lifecyclePlan(input());
    const text = JSON.stringify(plan);
    for (const s of plan.shards) expect(text).not.toContain(shardIdempotencyKey(s.shard_id, plan.aggregate_digest));
  });
});

describe("membership blockers (each blocks its shard and the run)", () => {
  const blockersOf = async (over: Partial<PlanInput>) => {
    const plan = await lifecyclePlan(input(over));
    expect(plan.run_blocked).toBe(true);
    expect(plan.shards.some((s) => s.blocked)).toBe(true);
    return plan;
  };

  test("a relay_-prefixed alias is not stripped into a Cloud id", async () => {
    const plan = await blockersOf({ uploads: [...UPLOADS.slice(1), upload("s0000", `relay_${USERS[0]}`, WS1)] });
    expect(plan.exceptions).toContainEqual(expect.objectContaining({ kind: "id", id_kind: "user", id: `relay_${USERS[0]}`, reason: "alias_id" }));
  });

  test("a non-UUID legacy id, an uppercase UUID and the default workspace are exceptions", async () => {
    const plan = await blockersOf({
      uploads: [...UPLOADS.slice(3), upload("s0000", "user_legacy", WS1), upload("s0001", USERS[1]!.toUpperCase(), WS2), upload("s0002", USERS[2]!, "default")],
    });
    const reasons = plan.exceptions.filter((e) => e.kind === "id").map((e) => (e.kind === "id" ? `${e.id_kind}:${e.reason}` : ""));
    expect(reasons.sort()).toEqual(["user:invalid_id", "user:invalid_id", "workspace:default_workspace"]);
  });

  test("a non-UUID org blocks every shard", async () => {
    const uploads = UPLOADS.map((u) => ({ ...u, org_id: "org_legacy" }));
    const plan = await blockersOf({ uploads, grant: { ...GRANT, orgId: "org_legacy" } });
    expect(plan.exceptions).toContainEqual({ kind: "org", org_id: "org_legacy", reason: "invalid_id" });
    expect(plan.shards.every((s) => s.blocked)).toBe(true);
  });

  test("inactive, missing, cross-org workspace, unknown workspace and unknown user", async () => {
    const f = membershipFixture();
    const plan = await blockersOf({
      membership: new FixtureMembershipSource({
        ...f,
        workspaces: [{ id: WS1, organization_id: ORG }], // WS2 unknown
        users: f.users.filter((u) => u.id !== USERS[4]),
        memberships: f.memberships.filter((m) => m.user_id !== USERS[2]).map((m) => (m.user_id === USERS[0] ? { ...m, status: "revoked" } : m)),
      }),
    });
    const verdicts = plan.exceptions.filter((e) => e.kind === "membership").map((e) => (e.kind === "membership" ? `${e.workspace_id === WS1 ? "ws1" : "ws2"}:${USERS.indexOf(e.source_user_id)}:${e.verdict}` : ""));
    expect(verdicts).toContain("ws1:0:membership_inactive");
    expect(verdicts).toContain("ws1:2:not_member");
    expect(verdicts).toContain("ws1:4:user_unknown");
    expect(verdicts.filter((v) => v.startsWith("ws2:")).every((v) => v.endsWith("workspace_unknown"))).toBe(true);
    const cross = await blockersOf({ membership: new FixtureMembershipSource(membershipFixture({ workspaces: [{ id: WS1, organization_id: ORG }, { id: WS2, organization_id: OTHER_ORG }] })) });
    expect(cross.exceptions.filter((e) => e.kind === "membership").every((e) => e.kind === "membership" && e.verdict === "workspace_other_org")).toBe(true);
  });

  test("duplicate rows are ambiguous; a Cloud id reached through two legacy forms is ambiguous", async () => {
    const f = membershipFixture();
    const dup = await blockersOf({ membership: new FixtureMembershipSource({ ...f, memberships: [...f.memberships, { organization_id: ORG, user_id: USERS[0]!, status: "revoked" }] }) });
    expect(dup.exceptions).toContainEqual(expect.objectContaining({ kind: "membership", verdict: "ambiguous", source_user_id: USERS[0] }));
    const mapping: IdMappingLedger = { version: 1, reviewed_by: "reviewer", reviewed_at: READ_AT, entries: [{ kind: "user", legacy_id: "user_legacy0", cloud_id: USERS[0]! }] };
    const twoForms = await blockersOf({ uploads: [...UPLOADS, upload("extra", "user_legacy0", WS1)], idMapping: mapping });
    expect(twoForms.exceptions).toContainEqual(expect.objectContaining({ kind: "ambiguous_id", id_kind: "user", cloud_id: USERS[0], legacy_ids: [USERS[0], "user_legacy0"].sort() }));
  });

  test("the identity preflight: inactive, non-human, other-org and unknown identities", async () => {
    const ids = identityFixture().identities;
    const plan = await blockersOf({
      identities: new FixtureIdentityPreflight(identityFixture({
        identities: ids
          .filter((i) => i.user_id !== USERS[3])
          .map((i) => (i.user_id === USERS[0] ? { ...i, status: "suspended" } : i.user_id === USERS[1] ? { ...i, kind: "agent" } : i.user_id === USERS[2] ? { ...i, org_id: OTHER_ORG } : i)),
      })),
    });
    const verdicts = plan.exceptions.filter((e) => e.kind === "identity").map((e) => (e.kind === "identity" ? `${USERS.indexOf(e.source_user_id)}:${e.verdict}` : ""));
    expect(verdicts.sort()).toEqual(["0:inactive", "1:not_human", "2:other_org", "3:unknown"]);
  });

  test("a read that was not REPEATABLE READ READ ONLY, or no read at all, is refused", async () => {
    await expect(lifecyclePlan(input({ membership: new FixtureMembershipSource(membershipFixture({ isolation: "READ COMMITTED" })) }))).rejects.toMatchObject({ code: "snapshot_isolation" });
    await expect(lifecyclePlan(input({ membership: new FixtureMembershipSource(undefined) }))).rejects.toMatchObject({ code: "snapshot_missing" });
    await expect(lifecyclePlan(input({ identities: new FixtureIdentityPreflight(undefined) }))).rejects.toMatchObject({ code: "identity_preflight_missing" });
  });

  test("an unshardable session is an exception and the dry run counts it", async () => {
    const big: UploadInput = { ...upload("big", USERS[0]!, WS1), chunks: Array.from({ length: 4097 }, (_, i) => ({ seq: i, epoch: 0, start_offset: i * 10, bytes: 10, sha256: sha(`b${i}`), records: 0 })) };
    const dry = await lifecycleDryRun(input({ uploads: [...UPLOADS.slice(0, 10), big] }));
    expect(dry.exceptions).toEqual([expect.objectContaining({ kind: "oversized_session", chunks: 4097 })]);
    expect(dry.run_blocked).toBe(true);
  });
});

describe("id mapping ledger", () => {
  const entry = (legacy_id: string, cloud_id: string, kind: "user" | "workspace" | "org" = "user") => ({ kind, legacy_id, cloud_id });
  const ledger = (entries: IdMappingLedger["entries"], over: Partial<IdMappingLedger> = {}): IdMappingLedger => ({ version: 1, reviewed_by: "reviewer", reviewed_at: READ_AT, entries, ...over });

  test("is empty by default: no alias resolves", () => {
    expect(EMPTY_ID_MAPPING.entries).toEqual([]);
    expect(resolveCloudId("user", "user_legacy", EMPTY_ID_MAPPING)).toEqual({ ok: false, reason: "invalid_id" });
    expect(resolveCloudId("user", USERS[0]!, EMPTY_ID_MAPPING)).toEqual({ ok: true, cloud_id: USERS[0], via: "uuid" });
  });

  test("refuses unreviewed, non-UUID targets, UUID legacy ids and collisions", () => {
    expect(() => validateIdMapping(ledger([entry("a", USERS[0]!)], { reviewed_by: "" }))).toThrow("reviewed_by");
    expect(() => validateIdMapping(ledger([entry("a", "not-a-uuid")]))).toThrow("non-UUID");
    expect(() => validateIdMapping(ledger([entry(USERS[1]!, USERS[0]!)]))).toThrow("non-UUID alias");
    expect(() => validateIdMapping(ledger([entry("a", USERS[0]!), entry("a", USERS[1]!)]))).toThrow("mapped twice");
    expect(() => validateIdMapping(ledger([entry("a", USERS[0]!), entry("b", USERS[0]!)]))).toThrow("two legacy ids");
    expect(validateIdMapping(ledger([entry("a", USERS[0]!), entry("a", WS1, "workspace")]))).toBeDefined(); // kinds are separate
  });

  test("a reviewed mapping lets a legacy alias match, and is pinned in the digest", async () => {
    const mapping = ledger([entry("user_legacy0", USERS[0]!)]);
    const uploads = UPLOADS.map((u) => (u.source_user_id === USERS[0] ? { ...u, source_user_id: "user_legacy0" } : u));
    const plan = await lifecyclePlan(input({ uploads, idMapping: mapping }));
    expect(plan.run_blocked).toBe(false);
    expect(plan.pair_counts.matched).toBe(10);
    const without = await lifecyclePlan(input({ uploads }));
    expect(without.run_blocked).toBe(true);
    expect(without.aggregate_digest).not.toBe(plan.aggregate_digest);
  });
});

describe("apply fails closed", () => {
  test("on an approval digest that is not the recomputed one", async () => {
    const { cfg, client } = await approved({ approvedDigest: "0".repeat(64) });
    await expect(applyGrants(cfg)).rejects.toMatchObject({ code: "digest_drift" });
    expect(client.creates).toBe(0);
  });

  test("on a missing, changed or stale snapshot", async () => {
    const { cfg, client } = await approved();
    await expect(applyGrants({ ...cfg, membership: new FixtureMembershipSource(undefined) })).rejects.toMatchObject({ code: "snapshot_missing" });
    await expect(applyGrants({ ...cfg, membership: new FixtureMembershipSource(membershipFixture({ read_at: "2026-10-30T12:10:00.000Z" })) })).rejects.toMatchObject({ code: "snapshot_changed" });
    await expect(applyGrants({ ...cfg, now: () => NOW + 2 * 24 * 3600_000 })).rejects.toMatchObject({ code: "snapshot_stale" });
    expect(client.creates).toBe(0);
  });

  test("on any exception, even when the digest and snapshot are the approved ones", async () => {
    const membership = new FixtureMembershipSource(membershipFixture({ memberships: membershipFixture().memberships.slice(1) }));
    const { cfg, client } = await approved({ membership });
    await expect(applyGrants(cfg)).rejects.toMatchObject({ code: "pair_exceptions" });
    expect(client.creates).toBe(0);
  });

  test("on an unrecognized target origin, and without --execute", async () => {
    const { cfg, client } = await approved();
    await expect(applyGrants({ ...cfg, allowedAudiences: ["https://other.example.com"] })).rejects.toMatchObject({ code: "unrecognized_origin" });
    await expect(applyGrants({ ...cfg, grant: { ...GRANT, audience: "http://history.example.com" }, allowedAudiences: ["http://history.example.com"] })).rejects.toMatchObject({ code: "unrecognized_origin" });
    await expect(applyGrants({ ...cfg, execute: false })).rejects.toMatchObject({ code: "execute_required" });
    expect(client.creates).toBe(0);
  });

  test("retries exhausted on lost responses: halt, compensating revoke, then previous_run_failed", async () => {
    const { cfg, client } = await approved();
    client.loseFrom = 2;
    const first = await applyGrants(cfg);
    expect(first.failed).toMatchObject({ error: "response lost" });
    expect(client.creates).toBe(1 + 4); // shard 2: the first try and 3 retries, all with one key
    expect(new Set(client.keys.slice(1)).size).toBe(1);
    expect(first.compensation!.revoked).toHaveLength(1);
    expect(first.compensation!.unresolved).toHaveLength(1);
    await expect(applyGrants(cfg)).rejects.toMatchObject({ code: "previous_run_failed" });
  });
});

describe("Idempotency-Key", () => {
  test("is deterministic per shard and approved plan version, and distinct per shard", async () => {
    const plan = await lifecyclePlan(input());
    const keys = plan.shards.map((s) => shardIdempotencyKey(s.shard_id, plan.aggregate_digest));
    expect(keys).toEqual(plan.shards.map((s) => shardIdempotencyKey(s.shard_id, plan.aggregate_digest)));
    expect(new Set(keys).size).toBe(keys.length);
    expect(keys.every(isValidIdempotencyKey)).toBe(true);
    expect(keys.every((k) => /^[A-Za-z0-9_-]{43}$/.test(k))).toBe(true); // base64url sha256
    // A re-approved plan (another not-after) gets new keys for the same shards.
    const other = await lifecyclePlan(input({ grant: { ...GRANT, notAfter: "2026-12-01T00:00:00.000Z" } }));
    expect(other.shards.map((s) => s.shard_id)).toEqual(plan.shards.map((s) => s.shard_id));
    expect(other.shards.map((s) => shardIdempotencyKey(s.shard_id, other.aggregate_digest)).some((k) => keys.includes(k))).toBe(false);
  });

  test("every create sends the shard's key; it is never stored or put in evidence", async () => {
    const { cfg, client, dir, plan } = await approved();
    const applied = await applyGrants(cfg);
    expect(client.keys).toEqual(plan.shards.map((s) => shardIdempotencyKey(s.shard_id, plan.aggregate_digest)));
    await revokeGrants({ execute: true, stateDir: dir, client, aggregateDigest: plan.aggregate_digest, plan: cfg });
    const files = await readdir(dir);
    const everything = (await Promise.all(files.map((f) => readFile(join(dir, f), "utf8")))).join("\n") + JSON.stringify(applied);
    for (const k of client.keys) expect(everything).not.toContain(k);
  });

  test("a lost response is recovered by retrying with the same key: the original grant is replayed, no duplicate, no halt", async () => {
    const { cfg, client } = await approved();
    client.loseCreateAt = 2;
    const result = await applyGrants(cfg);
    expect(result.failed).toBeUndefined();
    expect(result.created).toHaveLength(3);
    expect(client.creates).toBe(4); // one retry
    expect(client.keys[1]).toBe(client.keys[2]); // same key on the retry
    expect(client.grants.size).toBe(3); // no duplicate grant
    expect(client.replays).toBe(1); // Idempotent-Replayed on the retry
    expect(client.live()).toBe(3);
  });

  test("409 idempotency_key_conflict halts: no retry, compensating revoke, then previous_run_failed", async () => {
    const { cfg, client } = await approved();
    client.refuseCreateAt = { n: 2, code: "idempotency_key_conflict", status: 409 };
    const result = await applyGrants(cfg);
    expect(result.failed).toMatchObject({ error: "relayauth refused: idempotency_key_conflict" });
    expect(client.creates).toBe(2);
    expect(result.compensation!.revoked).toHaveLength(1);
    expect(client.live()).toBe(0);
    await expect(applyGrants(cfg)).rejects.toMatchObject({ code: "previous_run_failed" });
  });

  test("a real conflict (same key, another request) is refused by the fake and halts the run", async () => {
    const { cfg, client, plan } = await approved();
    // Someone used shard 0's key for another request first.
    client.grants.set(`mgr_${"b".repeat(32)}`, { revokedAt: null, key: shardIdempotencyKey(plan.shards[0]!.shard_id, plan.aggregate_digest), fingerprint: "other", createdAt: new Date(NOW).toISOString() });
    const result = await applyGrants(cfg);
    expect(result.failed).toMatchObject({ shard_id: plan.shards[0]!.shard_id, error: "relayauth refused: idempotency_key_conflict" });
    expect(result.created).toEqual([]);
  });

  test("a replayed grant that is already revoked halts the run", async () => {
    const { cfg, client, plan, dir } = await approved();
    const first = await applyGrants({ ...cfg, maxNewShards: 1 });
    const id = first.created[0]!.grant_id;
    client.grants.get(id)!.revokedAt = new Date(NOW).toISOString(); // revoked out of band
    // The ledger lost its record (pending): the retry replays the revoked grant.
    const path = join(dir, "grant-ledger.json");
    const ledger = JSON.parse(await readFile(path, "utf8"));
    ledger.shards[plan.shards[0]!.shard_id] = { status: "pending", run_id: "dead", started_at: new Date(NOW).toISOString() };
    await writeFile(path, JSON.stringify(ledger));
    const result = await applyGrants(cfg);
    expect(result.failed).toMatchObject({ error: "RelayAuth replayed a grant that is already revoked" });
  });

  test("the key never appears in stdout, stderr, the ledger, evidence or results", async () => {
    const { cfg, client, dir, plan } = await approved();
    client.loseCreateAt = 2;
    client.refuseCreateAt = { n: 4, code: "idempotency_key_conflict", status: 409 };
    const written: string[] = [];
    const out = process.stdout.write.bind(process.stdout);
    const err = process.stderr.write.bind(process.stderr);
    process.stdout.write = ((chunk: string | Uint8Array) => (written.push(String(chunk)), true)) as typeof process.stdout.write;
    process.stderr.write = ((chunk: string | Uint8Array) => (written.push(String(chunk)), true)) as typeof process.stderr.write;
    let result: unknown;
    try {
      result = await applyGrants(cfg);
      await revokeGrants({ execute: true, stateDir: dir, client, aggregateDigest: plan.aggregate_digest, plan: cfg }).catch(() => undefined);
    } finally {
      process.stdout.write = out;
      process.stderr.write = err;
    }
    const files = await readdir(dir);
    const everything = [written.join(""), JSON.stringify(result), ...(await Promise.all(files.map((f) => readFile(join(dir, f), "utf8"))))].join("\n");
    expect(client.keys.length).toBeGreaterThan(2);
    for (const k of new Set(client.keys)) expect(everything).not.toContain(k);
  });

  test("a shard left pending by a killed process is resumed with the same key: the original grant comes back", async () => {
    const { cfg, client, dir, plan } = await approved();
    // The create landed in RelayAuth, then the process died before recording it.
    const orphan = `mgr_${"a".repeat(32)}`;
    const shard0 = plan.shards[0]!.shard_id;
    // Fingerprint "": the fake adopts the first request's (it is that shard's own create).
    client.grants.set(orphan, { revokedAt: null, key: shardIdempotencyKey(shard0, plan.aggregate_digest), fingerprint: "", createdAt: new Date(NOW).toISOString() });
    await writeFile(join(dir, "grant-ledger.json"), JSON.stringify({ version: 1, aggregate_digest: plan.aggregate_digest, source_snapshot_id: plan.source_snapshot.snapshot_id, shards: { [shard0]: { status: "pending", run_id: "dead", started_at: new Date(NOW).toISOString() } } }));
    const result = await applyGrants(cfg);
    expect(result.created.find((c) => c.shard_id === shard0)?.grant_id).toBe(orphan); // recovered, not duplicated
    expect(result.created).toHaveLength(3);
    expect(client.grants.size).toBe(3);
  });
});

describe("apply, idempotency and compensation", () => {
  test("re-applying the same approved plan creates no duplicate grant", async () => {
    const { cfg, client } = await approved();
    const first = await applyGrants(cfg);
    expect(first.created).toHaveLength(3);
    const second = await applyGrants(cfg);
    expect(second.created).toEqual([]);
    expect(second.skipped).toEqual(first.created.map((c) => c.shard_id));
    expect(client.creates).toBe(3);
    expect(client.live()).toBe(3);
  });

  test("a create failing partway revokes the grants created in that run, with evidence", async () => {
    const { cfg, client, dir } = await approved();
    client.refuseCreateAt = { n: 3, code: "bad_request", status: 400 };
    const result = await applyGrants(cfg);
    expect(result.failed).toMatchObject({ error: "relayauth refused: bad_request" });
    expect(result.compensation).toMatchObject({ reason: "compensating", unresolved: [{ shard_id: expect.any(String) }], failed: [] });
    expect(result.compensation!.revoked.map((r) => r.grant_id).sort()).toEqual(result.created.map((c) => c.grant_id).sort());
    expect(client.live()).toBe(0);
    expect((await readdir(dir)).some((f) => f.startsWith("revocation-compensating-"))).toBe(true);
    await expect(applyGrants(cfg)).rejects.toMatchObject({ code: "previous_run_failed" });
  });

  test("interrupted between shards, then resumed: every shard once, no duplicate", async () => {
    const { cfg, client } = await approved();
    const first = await applyGrants({ ...cfg, maxNewShards: 1 });
    expect(first).toMatchObject({ interrupted: true });
    const resumed = await applyGrants(cfg);
    expect(resumed.skipped).toEqual([first.created[0]!.shard_id]);
    expect(resumed.created).toHaveLength(2);
    expect(client.creates).toBe(3);
    expect(client.live()).toBe(3);
  });

  test("interrupted between shards, then rolled back: revocation evidence per shard, re-apply refused", async () => {
    const { cfg, client, plan, dir } = await approved();
    const first = await applyGrants({ ...cfg, maxNewShards: 2 });
    const evidence = await revokeGrants({ execute: true, stateDir: dir, client, aggregateDigest: plan.aggregate_digest, plan: cfg });
    expect(evidence.reason).toBe("rollback");
    expect(evidence.revoked.map((r) => [r.shard_id, r.grant_id])).toEqual(
      first.created.map((c) => [c.shard_id, c.grant_id]).sort((a, b) => (a[0]! < b[0]! ? -1 : 1)),
    );
    expect(evidence.revoked.every((r) => !Number.isNaN(Date.parse(r.revoked_at)))).toBe(true);
    expect(client.live()).toBe(0);
    const again = await revokeGrants({ execute: true, stateDir: dir, client, aggregateDigest: plan.aggregate_digest, plan: cfg });
    expect(again.revoked).toEqual([]);
    expect(again.already_revoked).toHaveLength(2);
    await expect(applyGrants(cfg)).rejects.toMatchObject({ code: "plan_rolled_back" });
  });

  test("revoke of chosen shards, and refusals", async () => {
    const { cfg, client, plan, dir } = await approved();
    const applied = await applyGrants(cfg);
    const one = applied.created[1]!;
    const evidence = await revokeGrants({ execute: true, stateDir: dir, client, aggregateDigest: plan.aggregate_digest, plan: cfg, shardIds: [one.shard_id] });
    expect(evidence).toMatchObject({ reason: "revoke", revoked: [{ shard_id: one.shard_id, grant_id: one.grant_id }] });
    expect(client.live()).toBe(2);
    await expect(revokeGrants({ execute: false, stateDir: dir, client, aggregateDigest: plan.aggregate_digest, plan: cfg })).rejects.toMatchObject({ code: "execute_required" });
    await expect(revokeGrants({ execute: true, stateDir: dir, client, aggregateDigest: "1".repeat(64), plan: cfg })).rejects.toMatchObject({ code: "ledger_mismatch" });
  });
});

describe("CLI", () => {
  test("apply and revoke are refused even with --execute: nothing is wired", async () => {
    await expect(main(["apply"])).rejects.toMatchObject({ code: "execute_required" });
    await expect(main(["apply", "--execute"])).rejects.toMatchObject({ code: "not_wired" });
    await expect(main(["revoke", "--execute"])).rejects.toMatchObject({ code: "not_wired" });
  });
});

describe("#56 HOLD (1): the ledger is validated before it is trusted", () => {
  const ledgerFile = (dir: string) => join(dir, "grant-ledger.json");
  const edit = async (dir: string, f: (l: Record<string, any>) => void) => {
    const l = JSON.parse(await readFile(ledgerFile(dir), "utf8"));
    f(l);
    await writeFile(ledgerFile(dir), JSON.stringify(l));
  };

  test("a forged 'created' record (wrong plan digest) is refused, not skipped", async () => {
    const { cfg, client, dir, plan } = await approved();
    await applyGrants({ ...cfg, maxNewShards: 1 });
    const s1 = plan.shards[1]!.shard_id;
    await edit(dir, (l) => (l.shards[s1] = { status: "created", run_id: "x", grant_id: `mgr_${"c".repeat(32)}`, plan_digest: "0".repeat(64), created_at: new Date(NOW).toISOString() }));
    await expect(applyGrants(cfg)).rejects.toMatchObject({ code: "ledger_invalid" });
    expect(client.creates).toBe(1); // shard 1 was neither created nor silently skipped
  });

  test("a valid-looking record whose digest belongs to another shard is refused", async () => {
    const { cfg, dir, plan } = await approved();
    const first = await applyGrants({ ...cfg, maxNewShards: 1 });
    const own = first.created[0]!;
    await edit(dir, (l) => (l.shards[plan.shards[1]!.shard_id] = { status: "created", run_id: "x", grant_id: own.grant_id, plan_digest: own.plan_digest, created_at: new Date(NOW).toISOString() }));
    await expect(applyGrants(cfg)).rejects.toMatchObject({ code: "ledger_invalid" });
  });

  test("another version, unknown shard, unknown field, bad status or corrupt JSON are refused", async () => {
    const cases: [string, (l: Record<string, any>) => void][] = [
      ["version", (l) => (l.version = 99)],
      ["unknown shard", (l) => (l.shards["e".repeat(32)] = { status: "pending", run_id: "r", started_at: new Date(NOW).toISOString() })],
      ["unknown field", (l) => (l.extra = true)],
      ["bad status", (l) => (l.shards[Object.keys(l.shards)[0]!].status = "done")],
      ["extra record field", (l) => (l.shards[Object.keys(l.shards)[0]!].note = "x")],
    ];
    for (const [, f] of cases) {
      const { cfg, dir } = await approved();
      await applyGrants({ ...cfg, maxNewShards: 1 });
      await edit(dir, f);
      await expect(applyGrants(cfg)).rejects.toMatchObject({ code: "ledger_invalid" });
    }
    const { cfg, dir } = await approved();
    await applyGrants({ ...cfg, maxNewShards: 1 });
    await writeFile(ledgerFile(dir), "{not json");
    await expect(applyGrants(cfg)).rejects.toMatchObject({ code: "ledger_invalid" });
  });

  test("a ledger written under another source snapshot is refused", async () => {
    const { cfg, dir } = await approved();
    await applyGrants({ ...cfg, maxNewShards: 1 });
    await edit(dir, (l) => (l.source_snapshot_id = "fixture:cloud:" + "9".repeat(64)));
    await expect(applyGrants(cfg)).rejects.toMatchObject({ code: "ledger_mismatch" });
  });

  test("revoke validates the ledger too", async () => {
    const { cfg, client, dir, plan } = await approved();
    await applyGrants(cfg);
    await edit(dir, (l) => (l.shards[plan.shards[0]!.shard_id].plan_digest = "0".repeat(64)));
    await expect(revokeGrants({ execute: true, stateDir: dir, client, aggregateDigest: plan.aggregate_digest, plan: cfg })).rejects.toMatchObject({ code: "ledger_invalid" });
    expect(client.revokes).toBe(0);
  });
});

describe("#56 HOLD (2): reviewed aliases bind the signed plan and the request", () => {
  const mapping: IdMappingLedger = {
    version: 1, reviewed_by: "reviewer", reviewed_at: READ_AT,
    entries: [
      { kind: "org", legacy_id: "org_legacy", cloud_id: ORG },
      { kind: "workspace", legacy_id: "ws_legacy", cloud_id: WS1 },
      { kind: "user", legacy_id: "user_legacy0", cloud_id: USERS[0]! },
    ],
  };
  // Legacy ids on every upload of user 0 in WS1, and the legacy org everywhere.
  const legacy = UPLOADS.map((u) => {
    const v = { ...u, org_id: "org_legacy" };
    if (u.workspace_id !== WS1) return v;
    return { ...v, workspace_id: "ws_legacy", key: u.key.replace(`ws=${WS1}/`, "ws=ws_legacy/"), ...(u.source_user_id === USERS[0] ? { source_user_id: "user_legacy0" } : {}) };
  });

  test("the plan and every create request carry only Cloud ids; RelayAuth's response verifies", async () => {
    const sent: Parameters<GrantClient["createGrant"]>[0][] = [];
    const client = new FakeRelayAuth();
    const create = client.createGrant.bind(client);
    client.createGrant = async (req) => (sent.push(req), create(req));
    const { cfg } = await approved({ uploads: legacy, idMapping: mapping, grant: { ...GRANT, orgId: "org_legacy" } }, client);
    const plan = await lifecyclePlan(cfg);
    expect(plan.run_blocked).toBe(false);
    expect(plan.org_id).toBe(ORG);
    const result = await applyGrants(cfg);
    expect(result.failed).toBeUndefined();
    expect(result.created).toHaveLength(3);
    const text = JSON.stringify(sent.map((r) => r.plan));
    for (const alias of ["org_legacy", "ws_legacy", "user_legacy0"]) expect(text).not.toContain(alias);
    for (const r of sent) for (const m of r.plan.manifests) {
      expect([m.org_id, m.workspace_id, m.source_user_id].every((id) => /^[0-9a-f-]{36}$/.test(id))).toBe(true);
      expect(m.key.startsWith(`v1/ws=${m.workspace_id}/`)).toBe(true);
    }
  });

  test("the shard plan used for grants equals the canonical uploads' plan (the courier must export the same keys)", async () => {
    const shardsFromAliases = (await lifecyclePlan(input({ uploads: legacy, idMapping: mapping, grant: { ...GRANT, orgId: "org_legacy" } }))).shards.map((s) => s.body_sha256);
    const shardsFromCanonical = (await lifecyclePlan(input({ uploads: canonicalizeUploads(legacy, mapping), idMapping: mapping, grant: GRANT }))).shards.map((s) => s.body_sha256);
    expect(shardsFromAliases).toEqual(shardsFromCanonical);
  });
});

describe("#56 HOLD (3): revoke converges against relayauth #112's real semantics", () => {
  test("the fake answers an already-revoked grant with 409 migration_grant_revoked, as #112 does", async () => {
    const { cfg, client } = await approved();
    const applied = await applyGrants(cfg);
    const id = applied.created[0]!.grant_id;
    await client.revokeGrant(id);
    await expect(client.revokeGrant(id)).rejects.toMatchObject({ code: "migration_grant_revoked", status: 409 });
  });

  test("a lost revoke response converges: the retry's 409 is resolved by the idempotent replay, with RelayAuth's revoked_at", async () => {
    const { cfg, client, dir, plan } = await approved();
    const applied = await applyGrants(cfg);
    client.loseRevokeAt = 2;
    const evidence = await revokeGrants({ execute: true, stateDir: dir, client, aggregateDigest: plan.aggregate_digest, plan: cfg });
    expect(evidence.failed).toEqual([]);
    expect(evidence.revoked).toHaveLength(3);
    const viaReplay = evidence.revoked.filter((r) => r.via === "replay");
    expect(viaReplay).toHaveLength(1);
    expect(viaReplay[0]!.revoked_at).toBe(client.grants.get(viaReplay[0]!.grant_id)!.revokedAt!);
    expect(client.live()).toBe(0);
    expect(client.grants.size).toBe(applied.created.length); // the replay created nothing
    // Converged: the ledger says revoked, and a second rollback just reports it.
    const again = await revokeGrants({ execute: true, stateDir: dir, client, aggregateDigest: plan.aggregate_digest, plan: cfg });
    expect(again.already_revoked).toHaveLength(3);
  });

  test("every revoke response lost: the final replay still reads the revoked state back", async () => {
    const { cfg, client, dir, plan } = await approved();
    await applyGrants(cfg);
    client.loseRevokesFrom = 1;
    const evidence = await revokeGrants({ execute: true, stateDir: dir, client, aggregateDigest: plan.aggregate_digest, plan: cfg });
    expect(evidence.failed).toEqual([]);
    expect(evidence.revoked.every((r) => r.via === "replay" || r.via === "revoke")).toBe(true);
    expect(client.live()).toBe(0);
  });

  test("a grant revoked out of band converges too; a grant RelayAuth does not know is a failure", async () => {
    const { cfg, client, dir, plan } = await approved();
    const applied = await applyGrants(cfg);
    await client.revokeGrant(applied.created[0]!.grant_id); // someone else revoked it
    client.grants.delete(applied.created[1]!.grant_id); // gone at RelayAuth
    const evidence = await revokeGrants({ execute: true, stateDir: dir, client, aggregateDigest: plan.aggregate_digest, plan: cfg });
    expect(evidence.revoked.map((r) => r.via).sort()).toEqual(["replay", "revoke"]);
    expect(evidence.failed).toEqual([{ shard_id: applied.created[1]!.shard_id, grant_id: applied.created[1]!.grant_id, error: "relayauth refused: migration_grant_not_found" }]);
  });
});
