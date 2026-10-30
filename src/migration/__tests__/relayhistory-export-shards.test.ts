/**
 * Grant sharding against relayauth #111's own admission function (vendored),
 * on synthetic, payload-free plan inputs. No minting, no network.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  canonicalMigrationJson,
  deriveMigrationGrantPlan,
  migrationGrantSerializedBytes,
  migrationUploadIdentity,
} from "../vendor/relayauth-migration-plan.ts";
import { derivePlanContract, type UploadInput } from "../relayhistory-export-upload.ts";
import {
  planShards,
  requireShardable,
  runShards,
  shardReport,
  ShardPlanningError,
  type ShardPlan,
} from "../relayhistory-export-shards.ts";

// The oracle derives whole candidate plans; shared CI runners are 3-5x slower.
setDefaultTimeout(30_000);

const EXPORTED_AT = "2026-10-30T00:00:00.000Z";
const sha = (s: string) => createHash("sha256").update(s).digest("hex");

interface Spec {
  org?: string;
  ws?: string;
  user?: string;
  machine?: string;
  harness?: string;
  session: string;
  chunks?: number;
  records?: number;
}

function upload(spec: Spec): UploadInput {
  const ws = spec.ws ?? "ws_1";
  const machine = spec.machine ?? "m1";
  const harness = spec.harness ?? "claude";
  return {
    key: `v1/ws=${ws}/m=${machine}/h=${harness}/d=2026/10/01/s=${spec.session}/manifest.json`,
    org_id: spec.org ?? "org_1",
    workspace_id: ws,
    source_user_id: spec.user ?? "user_1",
    session: { machine_id: machine, harness, session_id: spec.session, started_at: "2026-10-01T00:00:00.000Z" },
    state: "closed",
    chunks: Array.from({ length: spec.chunks ?? 1 }, (_, i) => ({
      seq: i,
      epoch: 0,
      start_offset: i * 1000,
      bytes: 1000,
      sha256: sha(`${spec.session}/${machine}/${i}`),
      records: spec.records ?? 0,
    })),
  };
}

const ids = (plan: ShardPlan) => plan.shards.map((s) => s.shard_id);
const measure = (uploads: UploadInput[], org = "org_1") => {
  const d = deriveMigrationGrantPlan({ grantId: `mgr_${"0".repeat(32)}`, orgId: org, plan: { exported_at: EXPORTED_AT, manifests: uploads } });
  return migrationGrantSerializedBytes(d.plan, d.allowedPairs);
};
function shuffle<T>(xs: T[], seed = 7): T[] {
  const out = [...xs];
  let s = seed;
  for (let i = out.length - 1; i > 0; i--) {
    s = (s * 1103515245 + 12345) % 2 ** 31;
    const j = s % (i + 1);
    [out[i], out[j]] = [out[j]!, out[i]!];
  }
  return out;
}

const dirs: string[] = [];
afterEach(async () => {
  await Promise.all(dirs.splice(0).map((d) => rm(d, { recursive: true, force: true })));
});

describe("vendored relayauth #111 oracle", () => {
  const vectors = JSON.parse(readFileSync(join(import.meta.dir, "fixtures", "relayhistory-export-plan-entry-v1.vectors.json"), "utf8"));
  const derived = deriveMigrationGrantPlan({
    grantId: vectors.grant_id,
    orgId: "org_synthetic",
    plan: { exported_at: vectors.exported_at, manifests: vectors.inputs },
  });

  test("reproduces the contract-v1 vectors byte for byte", () => {
    expect(canonicalMigrationJson(derived.plan)).toBe(vectors.plan.canonical_json);
    expect(derived.planDigest).toBe(vectors.plan.plan_digest);
    derived.plan.manifests.forEach((entry, i) => {
      expect(canonicalMigrationJson(entry)).toBe(vectors.entries[i].canonical_json);
      expect(migrationUploadIdentity(entry, derived.planDigest)).toEqual(vectors.upload_identities[i]);
    });
    // relayauth documents the fixture at 1,695 bytes combined.
    expect(migrationGrantSerializedBytes(derived.plan, derived.allowedPairs)).toBe(1695);
  });

  test("agrees with the courier's derivePlanContract on a shard", () => {
    const plan = planShards({ exportedAt: EXPORTED_AT, uploads: [1, 2, 3].map((i) => upload({ session: `s${i}`, chunks: i, user: `user_${i}` })) });
    const shard = plan.shards[0]!;
    const oracle = deriveMigrationGrantPlan({ grantId: shard.measure_grant_id, orgId: shard.org_id, plan: shard.plan });
    const courier = derivePlanContract({ grantId: shard.measure_grant_id, exportedAt: EXPORTED_AT, uploads: shard.plan.manifests });
    expect(courier.planDigest).toBe(oracle.planDigest);
  });
});

describe("determinism and stable ids", () => {
  const base = Array.from({ length: 2500 }, (_, i) => upload({ session: `s${String(i).padStart(5, "0")}`, user: `user_${i % 40}` }));

  test("the same input in any order gives the same shards", () => {
    const a = planShards({ exportedAt: EXPORTED_AT, uploads: base });
    const b = planShards({ exportedAt: EXPORTED_AT, uploads: shuffle(base) });
    expect(a.shards.length).toBeGreaterThan(1);
    expect(canonicalMigrationJson(b)).toBe(canonicalMigrationJson(a));
  });

  test("ids hash contents, not position: a session sorting last leaves earlier shard ids unchanged", () => {
    const a = planShards({ exportedAt: EXPORTED_AT, uploads: base });
    const b = planShards({ exportedAt: EXPORTED_AT, uploads: [...base, upload({ session: "s99999", user: "user_zz" })] });
    expect(ids(b).slice(0, -1)).toEqual(ids(a).slice(0, -1));
    expect(ids(b).at(-1)).not.toBe(ids(a).at(-1)); // the shard it joined changed
  });

  test("a session in another org changes no id of this org", () => {
    const a = planShards({ exportedAt: EXPORTED_AT, uploads: base });
    const b = planShards({ exportedAt: EXPORTED_AT, uploads: [upload({ org: "org_0", session: "x1" }), ...base] });
    expect(ids(b).filter((id) => ids(a).includes(id))).toEqual(ids(a));
    expect(b.shards[0]!.org_id).toBe("org_0");
  });

  test("what changes ids: the joined shard and, by greedy packing, that org's later shards; exported_at changes all", () => {
    const a = planShards({ exportedAt: EXPORTED_AT, uploads: base });
    // user_0 sorts first: its new session lands in the first shard.
    const b = planShards({ exportedAt: EXPORTED_AT, uploads: [...base, upload({ session: "s00000b", user: "user_0" })] });
    expect(ids(b)[0]).not.toBe(ids(a)[0]);
    const c = planShards({ exportedAt: "2026-10-31T00:00:00.000Z", uploads: base });
    expect(ids(c).some((id) => ids(a).includes(id))).toBe(false);
  });
});

describe("every limit at its boundary (the oracle decides)", () => {
  test("64 distinct source users fit one grant; 65 split", () => {
    const at = planShards({ exportedAt: EXPORTED_AT, uploads: Array.from({ length: 64 }, (_, i) => upload({ session: `s${i}`, user: `user_${i}` })) });
    expect(at.shards).toHaveLength(1);
    expect(at.shards[0]).toMatchObject({ source_users: 64, closed_by: "end" });
    const over = planShards({ exportedAt: EXPORTED_AT, uploads: Array.from({ length: 65 }, (_, i) => upload({ session: `s${i}`, user: `user_${i}` })) });
    expect(over.shards.map((s) => s.source_users)).toEqual([64, 1]);
    expect(over.shards[0]!.closed_by).toBe("migration_plan_source_user_limit");
  });

  test("1,000 manifests fit one grant; 1,001 split", () => {
    const at = planShards({ exportedAt: EXPORTED_AT, uploads: Array.from({ length: 1000 }, (_, i) => upload({ session: `s${i}` })) });
    expect(at.shards.map((s) => s.manifests)).toEqual([1000]);
    const over = planShards({ exportedAt: EXPORTED_AT, uploads: Array.from({ length: 1001 }, (_, i) => upload({ session: `s${i}` })) });
    expect(over.shards.map((s) => s.manifests)).toEqual([1000, 1]);
    expect(over.shards[0]!.closed_by).toContain("1 to 1000 entries");
  });

  test("4,096 chunks in a manifest are admitted; 4,097 make the session unshardable", () => {
    const at = planShards({ exportedAt: EXPORTED_AT, uploads: [upload({ session: "big", chunks: 4096 })] });
    expect(at.shards.map((s) => s.chunks)).toEqual([4096]);
    expect(at.unshardable).toEqual([]);
    const over = planShards({ exportedAt: EXPORTED_AT, uploads: [upload({ session: "big", chunks: 4097 }), upload({ session: "ok" })] });
    expect(over.shards.map((s) => s.manifests)).toEqual([1]);
    expect(over.unshardable).toEqual([
      expect.objectContaining({ code: "oversized_session", session: "org_1/ws_1/claude/big", manifests: 1, chunks: 4097, source_users: 1 }),
    ]);
    expect(over.unshardable[0]!.oracle_message).toContain("1 to 4096 chunks");
  });

  test("exactly 1,000,000 serialized bytes fit one grant; 1,000,001 split", () => {
    // As many 80-chunk sessions as fit, then pad `records` digits until the
    // oracle measures exactly 1,000,000 bytes.
    const uploads: UploadInput[] = [];
    for (let i = 0; ; i++) {
      const next = upload({ session: `s${String(i).padStart(3, "0")}`, chunks: 80 });
      try {
        measure([...uploads, next]);
      } catch {
        break;
      }
      uploads.push(next);
    }
    let delta = 1_000_000 - measure(uploads);
    expect(delta).toBeGreaterThan(0);
    for (const u of uploads)
      for (const c of u.chunks) {
        if (delta <= 0) break;
        const add = Math.min(delta, 8); // records 0 -> 10^add adds `add` bytes
        c.records = 10 ** add;
        delta -= add;
      }
    expect(measure(uploads)).toBe(1_000_000);
    const at = planShards({ exportedAt: EXPORTED_AT, uploads });
    expect(at.shards.map((s) => s.serialized_bytes)).toEqual([1_000_000]);
    // One more byte on the last session (in sort order): it moves to a second grant.
    const last = uploads.at(-1)!;
    const chunk = last.chunks[0]!;
    chunk.records = chunk.records === 0 ? 10 : chunk.records * 10; // one more digit: +1 byte
    expect(() => measure(uploads)).toThrow("exceed 1000000 bytes"); // the oracle refuses 1,000,001
    const over = planShards({ exportedAt: EXPORTED_AT, uploads });
    expect(over.shards.map((s) => s.sessions)).toEqual([uploads.length - 1, 1]);
    expect(over.shards[0]!.closed_by).toBe("migration_plan_too_large");
    expect(over.shards[0]!.serialized_bytes).toBeLessThanOrEqual(1_000_000);
  });

  test("the 50,000-chunk plan cap never binds: bytes bind first, at the implied per-grant chunk count", () => {
    const uploads = Array.from({ length: 6 }, (_, i) => upload({ session: `s${i}`, chunks: 4096 }));
    const plan = planShards({ exportedAt: EXPORTED_AT, uploads });
    expect(plan.shards.every((s) => s.chunks < 50_000)).toBe(true);
    expect(plan.shards.map((s) => s.closed_by).slice(0, -1).every((c) => c === "migration_plan_too_large")).toBe(true);
    // One 4,096-chunk manifest is ~600 KB canonical, so a grant holds one.
    expect(Math.max(...plan.shards.map((s) => s.chunks))).toBe(4096);
    expect(plan.shards).toHaveLength(6);
  }, 60_000); // ~25k chunks through the oracle; slow on shared CI runners
});

describe("indivisible sessions", () => {
  test("a session's manifests (one per machine) never straddle grants", () => {
    const uploads = Array.from({ length: 900 }, (_, i) =>
      Array.from({ length: 1 + (i % 3) }, (_, m) => upload({ session: `s${String(i).padStart(4, "0")}`, machine: `m${m}`, user: `user_${i % 7}` })),
    ).flat();
    const plan = planShards({ exportedAt: EXPORTED_AT, uploads });
    expect(plan.shards.length).toBeGreaterThan(1);
    const home = new Map<string, string>();
    for (const shard of plan.shards)
      for (const m of shard.plan.manifests) {
        const session = m.session.session_id;
        expect(home.get(session) ?? shard.shard_id).toBe(shard.shard_id);
        home.set(session, shard.shard_id);
      }
    expect(plan.shards.reduce((n, s) => n + s.manifests, 0)).toBe(uploads.length);
  });

  test("a session whose manifests each fit but together exceed a grant is oversized_session, never split", () => {
    const uploads = [0, 1, 2].map((m) => upload({ session: "wide", machine: `m${m}`, chunks: 3000 }));
    const plan = planShards({ exportedAt: EXPORTED_AT, uploads: [...uploads, upload({ session: "small" })] });
    expect(plan.unshardable).toHaveLength(1);
    expect(plan.unshardable[0]).toMatchObject({ code: "oversized_session", manifests: 3, chunks: 9000, oracle_code: "migration_plan_too_large" });
    expect(plan.shards.flatMap((s) => s.plan.manifests.map((m) => m.session.session_id))).toEqual(["small"]);
    expect(shardReport(plan).aggregate.unshardable_sessions).toBe(1);
  });

  test("requireShardable fails closed with a typed error of ids and counts only", () => {
    const plan = planShards({ exportedAt: EXPORTED_AT, uploads: [upload({ session: "big", chunks: 4097 })] });
    let caught: unknown;
    try {
      requireShardable(plan);
    } catch (error) {
      caught = error;
    }
    expect(caught).toBeInstanceOf(ShardPlanningError);
    const e = caught as ShardPlanningError;
    expect(e.code).toBe("oversized_session");
    expect(e.sessions).toHaveLength(1);
    const text = JSON.stringify(e.sessions);
    expect(text).not.toMatch(/[0-9a-f]{64}/); // no chunk digests, no content
    expect(Object.keys(e.sessions[0]!).sort()).toEqual(["chunks", "manifests", "oracle_code", "oracle_message", "org_id", "session", "source_users"]);
  });

  test("malformed input is invalid_session, not silently dropped", () => {
    const bad = { ...upload({ session: "s1" }), key: "v1/ws=ws_other/m=m1/h=claude/d=2026/10/01/s=s1/manifest.json" };
    expect(() => planShards({ exportedAt: EXPORTED_AT, uploads: [bad] })).toThrow(ShardPlanningError);
  });
});

describe("resume per shard", () => {
  const uploads = Array.from({ length: 2100 }, (_, i) => upload({ session: `s${String(i).padStart(4, "0")}` }));

  test("a crash re-runs only unfinished shards, each with its own checkpoint", async () => {
    const dir = await mkdtemp(join(tmpdir(), "shards-"));
    dirs.push(dir);
    const plan = planShards({ exportedAt: EXPORTED_AT, uploads });
    expect(plan.shards).toHaveLength(3);
    const calls: { id: string; checkpoint: string }[] = [];
    const crashing = async ({ shard, checkpointPath }: { shard: { shard_id: string }; checkpointPath: string }) => {
      calls.push({ id: shard.shard_id, checkpoint: checkpointPath });
      if (calls.length === 2) throw new Error("crash");
      return { grantId: `mgr_${shard.shard_id}` };
    };
    await expect(runShards(plan, { stateDir: dir, runShard: crashing })).rejects.toThrow("crash");
    calls.length = 0;
    const second = await runShards(plan, { stateDir: dir, runShard: async ({ shard, checkpointPath }) => {
      calls.push({ id: shard.shard_id, checkpoint: checkpointPath });
      return { grantId: `mgr_${shard.shard_id}` };
    } });
    expect(second.skipped).toEqual([plan.shards[0]!.shard_id]);
    expect(second.ran).toEqual([plan.shards[1]!.shard_id, plan.shards[2]!.shard_id]);
    expect(new Set(calls.map((c) => c.checkpoint)).size).toBe(2);
    expect(calls[0]!.checkpoint).toContain(plan.shards[1]!.shard_id);
    const third = await runShards(plan, { stateDir: dir, runShard: async () => {
      throw new Error("must not run");
    } });
    expect(third).toEqual({ ran: [], skipped: ids(plan) });
    expect((await readdir(dir)).filter((f) => f.endsWith(".done.json"))).toHaveLength(3);
  });

  test("an unshardable plan runs nothing", async () => {
    const dir = await mkdtemp(join(tmpdir(), "shards-"));
    dirs.push(dir);
    const plan = planShards({ exportedAt: EXPORTED_AT, uploads: [...uploads.slice(0, 5), upload({ session: "big", chunks: 4097 })] });
    let ran = 0;
    await expect(runShards(plan, { stateDir: dir, runShard: async () => (ran++, { grantId: "x" }) })).rejects.toMatchObject({ code: "oversized_session" });
    expect(ran).toBe(0);
  });
});

describe("dry-run report", () => {
  test("aggregate and per-shard maxima, ids and counts only", () => {
    const plan = planShards({ exportedAt: EXPORTED_AT, uploads: Array.from({ length: 1500 }, (_, i) => upload({ session: `s${i}`, user: `user_${i % 100}`, chunks: 1 + (i % 4) })) });
    const report = shardReport(plan);
    expect(report.aggregate).toMatchObject({ manifests: 1500, sessions: 1500, source_users: 100, unshardable_sessions: 0 });
    expect(report.aggregate.shards).toBe(plan.shards.length);
    expect(report.max.source_users).toBeLessThanOrEqual(64);
    expect(report.max.manifests).toBeLessThanOrEqual(1000);
    expect(report.max.serialized_bytes).toBeLessThanOrEqual(1_000_000);
    expect(JSON.stringify(report)).not.toMatch(/[0-9a-f]{64}/);
  });
});
