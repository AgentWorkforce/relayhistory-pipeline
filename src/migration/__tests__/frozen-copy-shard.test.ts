// src/migration/__tests__/frozen-copy-shard.test.ts (pure; no DB, CLI or network)
import { describe, expect, test } from "bun:test";
import { orgSetSha256, parseOrdinals, selectShard, shardArgs, stdinLoginPassword, ShardRefusal } from "../frozen-copy-shard.ts";

const ORGS = ["0539eb82-a2ad-4f8e-ba5f-c3543f25008d", "1aaa", "2bbb", "3ccc", "4ddd"];
const SHA = orgSetSha256(ORGS);
const code = (f: () => unknown) => { try { f(); return "ok"; } catch (e) { return e instanceof ShardRefusal ? e.code : "other"; } };

describe("selectShard", () => {
  test("keeps planner order and maps ordinals to the exact C-ordered list", () => {
    expect(selectShard(ORGS, "4,0,2", SHA, 5)).toEqual([ORGS[4], ORGS[0], ORGS[2]]);
  });
  test("refuses a changed org list (count or content) before any copy", () => {
    expect(code(() => selectShard(ORGS.slice(0, 4), "0", SHA, 5))).toBe("org_set_changed");
    expect(code(() => selectShard([...ORGS.slice(0, 4), "4eee"], "0", SHA, 5))).toBe("org_set_changed");
    expect(code(() => selectShard([ORGS[1]!, ORGS[0]!, ...ORGS.slice(2)], "0", SHA, 5))).toBe("org_set_changed");
  });
  test("refuses out-of-range, duplicate and malformed ordinals", () => {
    expect(code(() => selectShard(ORGS, "5", SHA, 5))).toBe("invalid_ordinals");
    expect(code(() => selectShard(ORGS, "1,1", SHA, 5))).toBe("invalid_ordinals");
    for (const bad of ["", "01", "-1", "1,,2", "1 ,2", "a", "1e3", "1,2,"]) expect(code(() => parseOrdinals(bad))).toBe("invalid_ordinals");
  });
  test("refuses malformed sha and count", () => {
    expect(code(() => selectShard(ORGS, "0", "ABC", 5))).toBe("invalid_org_set_sha");
    expect(code(() => selectShard(ORGS, "0", SHA, 0))).toBe("invalid_org_count");
  });
});

describe("shardArgs", () => {
  const ok = ["--copy", "--concurrency", "2", "--orgs", "3,1", "--org-set-sha256", SHA, "--org-count", "5", "--target-login", "stdin"];
  test("accepts the exact cloud shard argument set", () => {
    expect(shardArgs(ok, "copy")).toEqual({ shard: { ordinals: "3,1", sha: SHA, count: 5 }, stdinLogin: true, resumeDir: null, seed: "leader" });
    expect(shardArgs([...ok, "--resume-dir", "/home/copyrunner/custody-copy/cursors", "--seed-control", "skip"], "copy"))
      .toEqual({ shard: { ordinals: "3,1", sha: SHA, count: 5 }, stdinLogin: true, resumeDir: "/home/copyrunner/custody-copy/cursors", seed: "skip" });
    expect(shardArgs(["--copy", "--resume-dir", "/r"], "copy")).toEqual({ shard: null, stdinLogin: false, resumeDir: "/r", seed: "leader" });
  });
  test("absent flags keep the unchanged serial path", () => { expect(shardArgs(["--copy"], "copy")).toBeNull(); });
  test("refusals", () => {
    expect(code(() => shardArgs(ok, "seal"))).toBe("shard_copy_only");
    expect(code(() => shardArgs(ok, "plan"))).toBe("shard_copy_only");
    expect(code(() => shardArgs([...ok, "--org", "x"], "copy"))).toBe("shard_with_single_org");
    expect(code(() => shardArgs(["--copy", "--orgs", "1"], "copy"))).toBe("shard_args_incomplete");
    expect(code(() => shardArgs(["--copy", "--target-login", "stdin"], "copy"))).toBe("shard_args_incomplete");
    expect(code(() => shardArgs([...ok.slice(0, -2), "--target-login", "cli"], "copy"))).toBe("invalid_target_login");
    expect(code(() => shardArgs([...ok, "--orgs", "2"], "copy"))).toBe("duplicate_arg");
    expect(code(() => shardArgs(ok.map((a) => (a === "5" ? "05" : a)), "copy"))).toBe("invalid_org_count");
    expect(code(() => shardArgs(["--copy", "--seed-control", "skip"], "copy"))).toBe("seed_skip_needs_shard");
    expect(code(() => shardArgs([...ok, "--seed-control", "none"], "copy"))).toBe("invalid_seed_control");
    for (const d of ["rel", "/a/../b", "/a b", ""]) expect(code(() => shardArgs([...ok, "--resume-dir", d], "copy"))).toBe("invalid_resume_dir");
    expect(code(() => shardArgs(["--seal", "--resume-dir", "/r"], "seal"))).toBe("shard_copy_only");
    expect(code(() => shardArgs([...ok, "--resume-dir", "/r", "--resume-dir", "/s"], "copy"))).toBe("duplicate_arg");
  });
});

describe("stdinLoginPassword", () => {
  test("shape only", () => {
    expect(stdinLoginPassword({ supabaseLoginPassword: "x".repeat(32) })).toBe("x".repeat(32));
    for (const bad of [undefined, 7, "short", "x".repeat(257), "abc\ndefghijklmnopq", "<redacted>-0123456789"]) {
      expect(code(() => stdinLoginPassword({ supabaseLoginPassword: bad }))).toBe("target_login_shape");
    }
  });
});
