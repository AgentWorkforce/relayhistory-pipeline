// Release target anchoring: binds a held-step release to a real commit (target_sha)
// whose tree ends at the target. Read-only, from the local Git object database only
// (cat-file, rev-parse, merge-base, ls-tree): never a fetch, never a lazy fetch, never
// a network protocol. A shallow clone or a missing object refuses (fail closed).
import { spawnSync } from "node:child_process";
import {
  RELEASE_TREE,
  manifestGeneration,
  releaseManifestDigest,
  sha256Text,
  type ReleaseManifest,
  type ReleaseTargetBinding,
} from "./migrate.ts";

/** A full, lowercase 40-hex commit id; abbreviations and refs are refused. */
export const FULL_COMMIT_SHA = /^[0-9a-f]{40}$/;

function refuse(code: string, message: string): never {
  throw Object.assign(new Error(`96bulls: ${message}`), { code });
}

function git(repo: string, args: string[]): { status: number | null; stdout: Buffer } {
  const r = spawnSync(
    "git",
    ["-C", repo, "-c", "protocol.allow=never", "-c", "core.fsmonitor=false", ...args],
    {
      env: {
        PATH: process.env.PATH ?? "/usr/bin:/bin",
        GIT_TERMINAL_PROMPT: "0",
        GIT_NO_LAZY_FETCH: "1",
        GIT_NO_REPLACE_OBJECTS: "1",
        GIT_CONFIG_NOSYSTEM: "1",
      },
      maxBuffer: 256 * 1024 * 1024,
    },
  );
  return { status: r.status, stdout: r.stdout ?? Buffer.alloc(0) };
}

function gitText(repo: string, args: string[], code: string, message: string): string {
  const r = git(repo, args);
  if (r.status !== 0) refuse(code, message);
  return r.stdout.toString("utf8").trim();
}

/**
 * target_sha is a full 40-hex commit present in the local object database and an
 * ancestor of (or equal to) the release sha. Refuses release_target_sha_invalid,
 * release_target_sha_unknown or release_target_sha_not_ancestor.
 */
export function requireTargetCommit(repo: string, releaseSha: string, targetSha: string): void {
  if (!FULL_COMMIT_SHA.test(targetSha))
    refuse("release_target_sha_invalid", "target_sha must be a full 40-hex lowercase commit id");
  if (!FULL_COMMIT_SHA.test(releaseSha))
    refuse("release_target_sha_invalid", "the release sha must be a full 40-hex lowercase commit id");
  for (const sha of [targetSha, releaseSha]) {
    const type = git(repo, ["cat-file", "-t", sha]);
    if (type.status !== 0 || type.stdout.toString("utf8").trim() !== "commit")
      refuse("release_target_sha_unknown", "a release commit is not in the local object database");
    const resolved = git(repo, ["rev-parse", "--verify", "--quiet", "--end-of-options", `${sha}^{commit}`]);
    if (resolved.status !== 0 || resolved.stdout.toString("utf8").trim() !== sha)
      refuse("release_target_sha_unknown", "a release commit does not resolve to itself");
  }
  const anc = git(repo, ["merge-base", "--is-ancestor", targetSha, releaseSha]);
  if (anc.status === 1)
    refuse("release_target_sha_not_ancestor", "target_sha is not an ancestor of the release sha");
  if (anc.status !== 0)
    refuse("release_target_sha_unknown", "target_sha ancestry could not be read from the local object database");
}

interface TreeEntry {
  mode: string;
  type: string;
  oid: string;
  path: string;
}

function lsTree(repo: string, sha: string, path: string): TreeEntry[] {
  const r = git(repo, ["ls-tree", "-z", "--full-tree", sha, "--", path]);
  if (r.status !== 0) refuse("release_target_sha_unknown", "the target commit's tree could not be read");
  const out: TreeEntry[] = [];
  for (const rec of r.stdout.toString("utf8").split("\0")) {
    if (!rec) continue;
    const tab = rec.indexOf("\t");
    const [mode, type, oid] = rec.slice(0, tab).split(" ");
    out.push({ mode: mode!, type: type!, oid: oid!, path: rec.slice(tab + 1) });
  }
  return out;
}

function blobText(repo: string, oid: string): string {
  const r = git(repo, ["cat-file", "blob", oid]);
  if (r.status !== 0) refuse("release_target_sha_unknown", "a target blob is not in the local object database");
  // The same decoding generation() applies (readFile(path, "utf8")).
  return r.stdout.toString("utf8");
}

const regularBlob = (e: TreeEntry) => e.type === "blob" && (e.mode === "100644" || e.mode === "100755");

/**
 * The schema steps of a commit's tree, in migrator order (schemaSteps: each lane's
 * *.sql names sorted, upstream lane first), as [name, sha256 of the utf8 text]. A
 * *.sql entry that is not a regular file refuses (the migrator could not read it the
 * same way).
 */
export function treeSchemaSteps(repo: string, sha: string): [string, string][] {
  const steps: [string, string][] = [];
  for (const lane of RELEASE_TREE.lanes) {
    const entries = lsTree(repo, sha, `${lane.dir}/`).filter((e) => e.path.endsWith(".sql"));
    if (entries.some((e) => !regularBlob(e)))
      refuse("release_target_tree_mismatch", "a target schema step is not a regular file");
    const byName = new Map(entries.map((e) => [e.path.slice(lane.dir.length + 1), e.oid]));
    for (const f of [...byName.keys()].sort())
      steps.push([`${lane.prefix}${f}`, sha256Text(blobText(repo, byName.get(f)!))]);
  }
  return steps;
}

/** The commit's phase 2 verifier files (RELEASE_TREE.verifiers order); each must exist. */
export function treeVerifiers(repo: string, sha: string): { name: string; sql: string; sha256: string }[] {
  return RELEASE_TREE.verifiers.map((v) => {
    const [entry, ...rest] = lsTree(repo, sha, v.path);
    if (!entry || rest.length || entry.path !== v.path || !regularBlob(entry))
      refuse("release_target_verifier_missing", `the target commit has no regular file ${v.path}`);
    const sql = blobText(repo, entry.oid);
    return { name: v.name, sql, sha256: sha256Text(sql) };
  });
}

export interface ReleaseTargetAnchor extends ReleaseTargetBinding {
  targetGeneration: string;
  manifest: ReleaseManifest;
}

/**
 * Binds a release target to target_sha. requireTargetCommit; then target_sha's tree
 * must contain the target with no step of the target's own lane after it
 * (release_target_tail), and its schema steps must equal `prefix` (the release tree's
 * steps without the held ones, [name, sha256] in migrator order) exactly, by name and
 * digest (release_target_tree_mismatch); its 7 verifier files must exist. Returns the
 * manifest, its canonical digest, the target generation (what --generation prints on a
 * checkout of target_sha) and the verifier SQL the migrator runs after its own.
 */
export function releaseTargetBinding(input: {
  repo: string;
  releaseSha: string;
  targetSha: string;
  target: string;
  prefix: readonly (readonly [string, string])[];
}): ReleaseTargetAnchor {
  requireTargetCommit(input.repo, input.releaseSha, input.targetSha);
  const steps = treeSchemaSteps(input.repo, input.targetSha);
  const at = steps.findIndex(([n]) => n === input.target);
  const lane = (n: string) => n.slice(0, n.indexOf(":") + 1);
  if (at < 0 || steps.slice(at + 1).some(([n]) => lane(n) === lane(input.target)))
    refuse("release_target_tail", "target_sha's tree does not end its lane at the release target");
  if (
    steps.length !== input.prefix.length ||
    steps.some(([n, d], i) => n !== input.prefix[i]![0] || d !== input.prefix[i]![1])
  )
    refuse("release_target_tree_mismatch", "target_sha's schema steps differ from the release prefix");
  const verifiers = treeVerifiers(input.repo, input.targetSha);
  const manifest: ReleaseManifest = {
    targetSha: input.targetSha,
    target: input.target,
    steps,
    verifiers: verifiers.map((v) => [v.name, v.sha256]),
  };
  return {
    targetSha: input.targetSha,
    targetGeneration: manifestGeneration(manifest),
    manifest,
    manifestDigest: releaseManifestDigest(manifest),
    verifiers,
  };
}

/** Plan/apply shape check of a manifest; returns it typed. */
export function parseReleaseManifest(value: unknown): ReleaseManifest | undefined {
  const pair = (p: unknown) =>
    Array.isArray(p) && p.length === 2 && typeof p[0] === "string" && typeof p[1] === "string" && /^[0-9a-f]{64}$/.test(p[1]);
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const m = value as Record<string, unknown>;
  if (Object.keys(m).sort().join() !== "steps,target,targetSha,verifiers") return undefined;
  if (typeof m.targetSha !== "string" || !FULL_COMMIT_SHA.test(m.targetSha) || typeof m.target !== "string")
    return undefined;
  if (!Array.isArray(m.steps) || !m.steps.every(pair)) return undefined;
  if (
    !Array.isArray(m.verifiers) ||
    m.verifiers.length !== RELEASE_TREE.verifiers.length ||
    !m.verifiers.every((p, i) => pair(p) && p[0] === RELEASE_TREE.verifiers[i]!.name)
  )
    return undefined;
  return m as unknown as ReleaseManifest;
}
