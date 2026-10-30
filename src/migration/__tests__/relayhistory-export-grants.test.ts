/**
 * Grant lifecycle against a fake RelayAuth grant client and a fixture
 * membership source shaped like Cloud's tables. No real RelayAuth, no
 * minting, no network.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash, randomBytes } from "node:crypto";
import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { deriveMigrationGrantPlan } from "../vendor/relayauth-migration-plan.ts";
import type { UploadInput } from "../relayhistory-export-upload.ts";
import {
  applyGrants,
  FixtureMembershipSource,
  lifecycleDryRun,
  lifecyclePlan,
  main,
  revokeGrants,
  type ApplyConfig,
  type GrantClient,
  type MembershipFixture,
} from "../relayhistory-export-grants.ts";

const EXPORTED_AT = "2026-10-30T00:00:00.000Z";
const READ_AT = "2026-10-30T12:00:00.000Z";
const NOW = Date.parse("2026-10-30T12:30:00.000Z");
const ORG = "org_1";
const AUDIENCE = "https://history.example.com";
const GRANT = { orgId: ORG, apiKeyId: "ak_test", audience: AUDIENCE, notAfter: "2026-11-30T00:00:00.000Z" };
const sha = (s: string) => createHash("sha256").update(s).digest("hex");

function upload(session: string, user: string, ws = "ws_1"): UploadInput {
  return {
    key: `v1/ws=${ws}/m=m1/h=claude/d=2026/10/01/s=${session}/manifest.json`,
    org_id: ORG,
    workspace_id: ws,
    source_user_id: user,
    session: { machine_id: "m1", harness: "claude", session_id: session, started_at: "2026-10-01T00:00:00.000Z" },
    state: "closed",
    chunks: [{ seq: 0, epoch: 0, start_offset: 0, bytes: 1000, sha256: sha(session), records: 1 }],
  };
}

/** 2,500 sessions over 10 users and 2 workspaces: three shards. */
const UPLOADS = Array.from({ length: 2500 }, (_, i) => upload(`s${String(i).padStart(4, "0")}`, `user_${i % 10}`, i % 2 ? "ws_2" : "ws_1"));
const fixture = (over: Partial<MembershipFixture> = {}): MembershipFixture => ({
  source: "fixture:cloud.organization_memberships+workspaces",
  read_at: READ_AT,
  workspaces: [{ workspace_id: "ws_1", organization_id: ORG }, { workspace_id: "ws_2", organization_id: ORG }],
  memberships: Array.from({ length: 10 }, (_, i) => ({ organization_id: ORG, user_id: `user_${i}`, status: "active" })),
  ...over,
});

class FakeRelayAuth implements GrantClient {
  grants = new Map<string, { revokedAt: string | null }>();
  creates = 0;
  revokes = 0;
  failCreateAt?: number;
  loseCreateAt?: number;
  async createGrant(req: Parameters<GrantClient["createGrant"]>[0]) {
    this.creates += 1;
    if (this.failCreateAt === this.creates) throw new Error("relayauth 503");
    const grantId = `mgr_${randomBytes(16).toString("hex")}`;
    const derived = deriveMigrationGrantPlan({ grantId, orgId: ORG, plan: req.plan });
    this.grants.set(grantId, { revokedAt: null });
    if (this.loseCreateAt === this.creates) throw new Error("response lost");
    return { grantId, orgId: ORG, planDigest: derived.planDigest, allowedPairs: derived.allowedPairs, planEntries: derived.plan.manifests.length, createdAt: new Date(NOW).toISOString() };
  }
  async revokeGrant(grantId: string) {
    this.revokes += 1;
    const g = this.grants.get(grantId);
    if (!g) throw new Error("grant_not_found");
    g.revokedAt = new Date(NOW + this.revokes).toISOString();
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

async function approved(over: Partial<ApplyConfig> = {}) {
  const dir = await mkdtemp(join(tmpdir(), "grants-"));
  dirs.push(dir);
  const membership = new FixtureMembershipSource(fixture());
  const plan = await lifecyclePlan({ uploads: UPLOADS, exportedAt: EXPORTED_AT, grant: GRANT, membership });
  const client = new FakeRelayAuth();
  const cfg: ApplyConfig = {
    execute: true,
    uploads: UPLOADS,
    exportedAt: EXPORTED_AT,
    grant: GRANT,
    membership,
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
  test("the aggregate digest is deterministic and pins the snapshot", async () => {
    const membership = new FixtureMembershipSource(fixture());
    const a = await lifecyclePlan({ uploads: UPLOADS, exportedAt: EXPORTED_AT, grant: GRANT, membership });
    const b = await lifecyclePlan({ uploads: [...UPLOADS].reverse(), exportedAt: EXPORTED_AT, grant: GRANT, membership: new FixtureMembershipSource(fixture({ memberships: [...fixture().memberships].reverse() })) });
    expect(b.aggregate_digest).toBe(a.aggregate_digest);
    expect(a.aggregate_digest).toMatch(/^[0-9a-f]{64}$/);
    expect(a.source_snapshot.snapshot_id).toMatch(/^fixture:cloud\.organization_memberships\+workspaces:[0-9a-f]{64}$/);
    expect(a.shards).toHaveLength(3);
    expect(a.pair_counts).toEqual({ total: 10, matched: 10, exceptions: 0 }); // even users in ws_1, odd in ws_2
    expect(a.limits.manifests).toBe(1000);
    // Another snapshot or other grant parameters change the digest.
    const c = await lifecyclePlan({ uploads: UPLOADS, exportedAt: EXPORTED_AT, grant: GRANT, membership: new FixtureMembershipSource(fixture({ memberships: [...fixture().memberships, { organization_id: ORG, user_id: "user_x", status: "revoked" }] })) });
    expect(c.aggregate_digest).not.toBe(a.aggregate_digest);
    const d = await lifecyclePlan({ uploads: UPLOADS, exportedAt: EXPORTED_AT, grant: { ...GRANT, notAfter: "2026-12-01T00:00:00.000Z" }, membership });
    expect(d.aggregate_digest).not.toBe(a.aggregate_digest);
  });

  test("mismatches are listed row by row; matched pairs are only counted", async () => {
    const membership = new FixtureMembershipSource(fixture({
      workspaces: [{ workspace_id: "ws_1", organization_id: ORG }, { workspace_id: "ws_2", organization_id: "org_other" }],
      memberships: [
        ...Array.from({ length: 8 }, (_, i) => ({ organization_id: ORG, user_id: `user_${i}`, status: "active" })),
        { organization_id: ORG, user_id: "user_8", status: "revoked" },
      ],
    }));
    const plan = await lifecyclePlan({ uploads: UPLOADS, exportedAt: EXPORTED_AT, grant: GRANT, membership });
    expect(plan.pair_counts).toEqual({ total: 10, matched: 4, exceptions: 6 });
    const verdicts = plan.exceptions.map((e) => (e.kind === "membership" ? `${e.workspace_id}/${e.source_user_id}:${e.verdict}` : e.kind));
    expect(verdicts).toContain("ws_1/user_8:membership_inactive");
    expect(verdicts.filter((v) => v.startsWith("ws_2/"))).toHaveLength(5);
    expect(verdicts.filter((v) => v.startsWith("ws_2/")).every((v) => v.endsWith("workspace_other_org"))).toBe(true);
    expect(plan.exceptions.every((e) => e.kind !== "membership" || e.shard_ids.length > 0)).toBe(true);
  });

  test("an unshardable session is an exception and the dry run counts it", async () => {
    const big: UploadInput = { ...upload("big", "user_0"), chunks: Array.from({ length: 4097 }, (_, i) => ({ seq: i, epoch: 0, start_offset: i * 10, bytes: 10, sha256: sha(`b${i}`), records: 0 })) };
    const dry = await lifecycleDryRun({ uploads: [...UPLOADS.slice(0, 10), big], exportedAt: EXPORTED_AT, grant: GRANT, membership: new FixtureMembershipSource(fixture()) });
    expect(dry.exceptions).toEqual([expect.objectContaining({ kind: "oversized_session", chunks: 4097 })]);
    expect(dry.dry_run).toBe(true);
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
    await expect(applyGrants({ ...cfg, membership: new FixtureMembershipSource(fixture({ memberships: [...fixture().memberships, { organization_id: ORG, user_id: "user_new", status: "active" }] })) })).rejects.toMatchObject({ code: "snapshot_changed" });
    await expect(applyGrants({ ...cfg, now: () => NOW + 2 * 24 * 3600_000 })).rejects.toMatchObject({ code: "snapshot_stale" });
    expect(client.creates).toBe(0);
  });

  test("on any pair exception, even when the digest and snapshot are the approved ones", async () => {
    const membership = new FixtureMembershipSource(fixture({ memberships: fixture().memberships.slice(1) }));
    const plan = await lifecyclePlan({ uploads: UPLOADS, exportedAt: EXPORTED_AT, grant: GRANT, membership });
    const { cfg, client } = await approved({ membership, approvedDigest: plan.aggregate_digest, approvedSnapshotId: plan.source_snapshot.snapshot_id });
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

  test("on partial shard state: a create with a lost response stays pending and blocks re-apply", async () => {
    const { cfg, client } = await approved();
    client.loseCreateAt = 2; // the grant exists in RelayAuth, the answer never came
    const first = await applyGrants(cfg);
    expect(first.failed).toMatchObject({ error: "response lost" });
    expect(first.compensation!.revoked).toHaveLength(1); // the grant this run did record
    expect(first.compensation!.unresolved).toHaveLength(1); // the orphan a human must check
    await expect(applyGrants(cfg)).rejects.toMatchObject({ code: "partial_shard_state" });
    expect(client.creates).toBe(2);
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
    client.failCreateAt = 3;
    const result = await applyGrants(cfg);
    expect(result.failed).toMatchObject({ error: "relayauth 503" });
    expect(result.compensation).toMatchObject({ reason: "compensating", unresolved: [{ shard_id: expect.any(String) }], failed: [] });
    expect(result.compensation!.revoked.map((r) => r.grant_id).sort()).toEqual(result.created.map((c) => c.grant_id).sort());
    expect(result.compensation!.revoked.every((r) => r.revoked_at)).toBe(true);
    expect(client.live()).toBe(0);
    expect((await readdir(dir)).some((f) => f.startsWith("revocation-compensating-"))).toBe(true);
  });

  test("interrupted between shards, then resumed: every shard once, no duplicate", async () => {
    const { cfg, client } = await approved();
    const first = await applyGrants({ ...cfg, maxNewShards: 1 });
    expect(first).toMatchObject({ interrupted: true });
    expect(first.created).toHaveLength(1);
    const resumed = await applyGrants(cfg);
    expect(resumed.skipped).toEqual([first.created[0]!.shard_id]);
    expect(resumed.created).toHaveLength(2);
    expect(client.creates).toBe(3);
    expect(client.live()).toBe(3);
  });

  test("interrupted between shards, then rolled back: revocation evidence per shard, re-apply refused", async () => {
    const { cfg, client, plan, dir } = await approved();
    const first = await applyGrants({ ...cfg, maxNewShards: 2 });
    expect(first.created).toHaveLength(2);
    const evidence = await revokeGrants({ execute: true, stateDir: dir, client, aggregateDigest: plan.aggregate_digest });
    expect(evidence.reason).toBe("rollback");
    expect(evidence.revoked.map((r) => [r.shard_id, r.grant_id])).toEqual(
      first.created.map((c) => [c.shard_id, c.grant_id]).sort((a, b) => (a[0]! < b[0]! ? -1 : 1)),
    );
    expect(evidence.revoked.every((r) => !Number.isNaN(Date.parse(r.revoked_at)))).toBe(true);
    expect(client.live()).toBe(0);
    // A second rollback is idempotent and says so.
    const again = await revokeGrants({ execute: true, stateDir: dir, client, aggregateDigest: plan.aggregate_digest });
    expect(again.revoked).toEqual([]);
    expect(again.already_revoked).toHaveLength(2);
    await expect(applyGrants(cfg)).rejects.toMatchObject({ code: "plan_rolled_back" });
    const ledger = JSON.parse(await readFile(join(dir, "grant-ledger.json"), "utf8"));
    expect(JSON.stringify(ledger)).not.toMatch(/ak_|Bearer|token/i);
  });

  test("revoke of chosen shards, and refusals", async () => {
    const { cfg, client, plan, dir } = await approved();
    const applied = await applyGrants(cfg);
    const one = applied.created[1]!;
    const evidence = await revokeGrants({ execute: true, stateDir: dir, client, aggregateDigest: plan.aggregate_digest, shardIds: [one.shard_id] });
    expect(evidence).toMatchObject({ reason: "revoke", revoked: [{ shard_id: one.shard_id, grant_id: one.grant_id }] });
    expect(client.live()).toBe(2);
    await expect(revokeGrants({ execute: false, stateDir: dir, client, aggregateDigest: plan.aggregate_digest })).rejects.toMatchObject({ code: "execute_required" });
    await expect(revokeGrants({ execute: true, stateDir: dir, client, aggregateDigest: "1".repeat(64) })).rejects.toMatchObject({ code: "ledger_mismatch" });
  });
});

describe("CLI", () => {
  test("apply and revoke are refused even with --execute: nothing is wired", async () => {
    await expect(main(["apply"])).rejects.toMatchObject({ code: "execute_required" });
    await expect(main(["apply", "--execute"])).rejects.toMatchObject({ code: "not_wired" });
    await expect(main(["revoke", "--execute"])).rejects.toMatchObject({ code: "not_wired" });
  });
});

test("a user with no membership row at all is not_member", async () => {
  const membership = new FixtureMembershipSource(fixture({ memberships: fixture().memberships.filter((m) => m.user_id !== "user_3") }));
  const plan = await lifecyclePlan({ uploads: UPLOADS, exportedAt: EXPORTED_AT, grant: GRANT, membership });
  expect(plan.exceptions).toEqual([expect.objectContaining({ kind: "membership", verdict: "not_member", workspace_id: "ws_2", source_user_id: "user_3" })]);
});
