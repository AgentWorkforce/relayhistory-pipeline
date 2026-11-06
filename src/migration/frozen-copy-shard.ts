/**
 * PROPOSED (staged draft, not committed): src/migration/frozen-copy-shard.ts
 *
 * Disjoint org sharding and stdin target login for `scripts/frozen-source-copy.ts --copy`. Pure helpers only.
 *
 * Data semantics are unchanged: a shard runs the SAME copyOrg() on a subset of the SAME C-ordered org list, with the
 * same pages, ON CONFLICT DO NOTHING inserts, seal-skip and throttle. The subset is bound to the exact org list by
 * count and SHA-256, so an ordinal can never silently point at a different org. The completeness seal is untouched:
 * `--seal` still verifies EVERY Neon org, so a shard plan that missed an org can only make the seal refuse.
 */
import { createHash } from "node:crypto";

export class ShardRefusal extends Error {
  constructor(readonly code: string) { super(code); }
}

/** sha256 of the C-ordered org ids joined by "\n" (what `--plan` prints as orgSetSha256). */
export function orgSetSha256(orgs: readonly string[]): string {
  return createHash("sha256").update(orgs.join("\n")).digest("hex");
}

/** "17,3,0" -> [17, 3, 0]: order kept (the planner lists largest first), no duplicates, decimal only. */
export function parseOrdinals(raw: string): number[] {
  if (!/^(0|[1-9][0-9]{0,5})(,(0|[1-9][0-9]{0,5}))*$/u.test(raw)) throw new ShardRefusal("invalid_ordinals");
  const ids = raw.split(",").map(Number);
  if (new Set(ids).size !== ids.length) throw new ShardRefusal("invalid_ordinals");
  return ids;
}

/** The shard's org subset, refusing unless the live org list is exactly the one the plan was computed from. */
export function selectShard(all: readonly string[], ordinals: string, expectSha: string, expectCount: number): string[] {
  if (!/^[0-9a-f]{64}$/u.test(expectSha)) throw new ShardRefusal("invalid_org_set_sha");
  if (!Number.isSafeInteger(expectCount) || expectCount < 1) throw new ShardRefusal("invalid_org_count");
  if (all.length !== expectCount) throw new ShardRefusal("org_set_changed");
  if (orgSetSha256(all) !== expectSha) throw new ShardRefusal("org_set_changed");
  const ids = parseOrdinals(ordinals);
  if (ids.some((i) => i >= all.length)) throw new ShardRefusal("invalid_ordinals");
  return ids.map((i) => all[i]!);
}

export interface CopyArgs {
  shard: { ordinals: string; sha: string; count: number } | null;
  stdinLogin: boolean;
  resumeDir: string | null;   // rev 2: per-page resume checkpoints (frozen-copy-resume.ts)
  seed: "leader" | "skip";    // rev 2: only the leader seeds privacy/control rows; "skip" requires a shard
}

/** Argument contract, checked before stdin is read. Returns null when no rev 1/rev 2 flag is present (unchanged path). */
export function shardArgs(args: readonly string[], mode: string): CopyArgs | null {
  const has = (k: string) => args.includes(k);
  const val = (k: string) => { const i = args.indexOf(k); return i >= 0 && i + 1 < args.length ? args[i + 1]! : ""; };
  const all = ["--orgs", "--org-set-sha256", "--org-count", "--target-login", "--resume-dir", "--seed-control"];
  const shard = ["--orgs", "--org-set-sha256", "--org-count"].filter(has);
  if (!all.some(has)) return null;
  if (mode !== "copy") throw new ShardRefusal("shard_copy_only");
  for (const k of all) if (args.filter((a) => a === k).length > 1) throw new ShardRefusal("duplicate_arg");
  if (shard.length && has("--org")) throw new ShardRefusal("shard_with_single_org");
  if (shard.length && shard.length !== 3) throw new ShardRefusal("shard_args_incomplete");
  if (has("--target-login") && !shard.length) throw new ShardRefusal("shard_args_incomplete");
  if (has("--target-login") && val("--target-login") !== "stdin") throw new ShardRefusal("invalid_target_login");
  let parsed: CopyArgs["shard"] = null;
  if (shard.length) {
    const countRaw = val("--org-count");
    if (!/^[1-9][0-9]{0,5}$/u.test(countRaw)) throw new ShardRefusal("invalid_org_count");
    parseOrdinals(val("--orgs"));
    if (!/^[0-9a-f]{64}$/u.test(val("--org-set-sha256"))) throw new ShardRefusal("invalid_org_set_sha");
    parsed = { ordinals: val("--orgs"), sha: val("--org-set-sha256"), count: Number(countRaw) };
  }
  const resumeDir = has("--resume-dir") ? val("--resume-dir") : null;
  if (resumeDir !== null && (!/^\/[A-Za-z0-9._/-]{1,200}$/u.test(resumeDir) || resumeDir.includes(".."))) throw new ShardRefusal("invalid_resume_dir");
  const seedRaw = has("--seed-control") ? val("--seed-control") : "leader";
  if (seedRaw !== "leader" && seedRaw !== "skip") throw new ShardRefusal("invalid_seed_control");
  if (seedRaw === "skip" && !parsed) throw new ShardRefusal("seed_skip_needs_shard");
  return { shard: parsed, stdinLogin: has("--target-login"), resumeDir, seed: seedRaw };
}

/** The pre-minted temporary-login password from the stdin JSON line (cloud runner). Shape only; never logged. */
export function stdinLoginPassword(input: Record<string, unknown>): string {
  const p = input.supabaseLoginPassword;
  if (typeof p !== "string" || p.length < 16 || p.length > 256 || /[\x00-\x1f\x7f]/u.test(p) || /redact|\*\*\*/iu.test(p))
    throw new ShardRefusal("target_login_shape");
  return p;
}
