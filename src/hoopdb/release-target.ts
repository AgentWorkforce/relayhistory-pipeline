// Release target anchoring: binds a held-step release to a real commit (target_sha)
// whose tree ends at the target. Read-only, from the local Git object database only
// (cat-file, rev-parse, rev-list, ls-tree, diff-tree, for-each-ref): never a fetch,
// never a lazy fetch, never a network protocol. A shallow clone or a missing object
// refuses (fail closed).
//
// Production binds through releaseAnchorBinding: a held-step target_sha must be the tip
// of the protected release/schema-<slot> branch for the target (requireReleaseAnchor).
// releaseTargetBinding (a target_sha on the release sha's first-parent main history) is
// kept for the rehearsal's seed (R0: the commit that landed the baseline) and tests.
import { spawnSync } from "node:child_process";
import { SESSION_BOOTSTRAP_FILES } from "./session-bootstrap-chain.ts";
import { RELEASE_ANCHOR_BRANCH_PREFIX, releaseAnchorBranch } from "./schema-release-contract.ts";
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
 * target_sha is a full 40-hex commit present in the local object database that is the
 * release sha itself or on the release sha's first-parent (main) history. A commit
 * reachable only through a merged side parent is refused even when its schema and
 * verifier bytes match: images are published at target_sha, and only first-parent main
 * commits carry the release's CI provenance. Checks in order: format, object existence,
 * resolve-to-self, first-parent membership. Refuses release_target_sha_invalid,
 * release_target_sha_unknown (including any git error while reading the chain) or
 * release_target_sha_not_ancestor (not on the first-parent chain).
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
  if (targetSha === releaseSha) return;
  // The release sha's first-parent chain, newest first, one full id per line. Exact
  // full-id match only; any git error (a missing parent object included) refuses.
  const chain = git(repo, ["rev-list", "--first-parent", releaseSha, "--"]);
  if (chain.status !== 0)
    refuse("release_target_sha_unknown", "the release sha's first-parent history could not be read from the local object database");
  if (!chain.stdout.toString("utf8").split("\n").includes(targetSha))
    refuse("release_target_sha_not_ancestor", "target_sha is not on the release sha's first-parent chain");
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

/** Format, object existence and resolve-to-self of full commit ids (requireTargetCommit's checks). */
function requireCommits(repo: string, shas: readonly string[]): void {
  for (const sha of shas)
    if (!FULL_COMMIT_SHA.test(sha))
      refuse("release_target_sha_invalid", "release commits must be full 40-hex lowercase commit ids");
  for (const sha of shas) {
    const type = git(repo, ["cat-file", "-t", sha]);
    if (type.status !== 0 || type.stdout.toString("utf8").trim() !== "commit")
      refuse("release_target_sha_unknown", "a release commit is not in the local object database");
    const resolved = git(repo, ["rev-parse", "--verify", "--quiet", "--end-of-options", `${sha}^{commit}`]);
    if (resolved.status !== 0 || resolved.stdout.toString("utf8").trim() !== sha)
      refuse("release_target_sha_unknown", "a release commit does not resolve to itself");
  }
}

/** The local namespace the release workflow's full checkout fetches branches into. */
export const RELEASE_ANCHOR_REF_NAMESPACE = "refs/remotes/origin/";
/** The longest anchor chain walked off main (anchors stacked on anchors) before refusing. */
export const RELEASE_ANCHOR_MAX_CHAIN = 64;

/**
 * The fixed paths an anchor may change relative to its main base, besides the schema
 * lane steps (supabase/migrations/*.sql, src/hoopdb/migrations/*.sql), the rollback of
 * an upstream step in the anchor's own prefix (supabase/rollbacks/<step>.down.sql) and
 * the function-boundary lane modules (scripts/sessions-function-boundary/lanes/*.ts):
 * the five bootstrap chain files, the seven phase 2 verifiers, the function-boundary
 * generator inputs and every output it writes (scripts/sync-sessions-function-boundary.ts),
 * the migration sentinels, the bootstrap contract modules the bootstrap stage reads
 * from the anchor, and the one test-only clock repair required by old anchors. Every
 * one must also be byte-identical to the release sha.
 */
export const RELEASE_ANCHOR_FIXED_PATHS: readonly string[] = [
  ...new Set([
    ...SESSION_BOOTSTRAP_FILES,
    ...RELEASE_TREE.verifiers.map((v) => v.path),
    // Function-boundary generator inputs (scripts/sync-sessions-function-boundary.ts).
    "scripts/sync-sessions-function-boundary.ts",
    "scripts/sessions-function-boundary/generate.ts",
    "scripts/sessions-function-boundary/sql.ts",
    "scripts/sessions-function-boundary/types.ts",
    "scripts/sessions-function-boundary/verifiers.ts",
    // Its generated outputs (rollbacks are matched per prefix step below).
    "src/hoopdb/migrate-guards.ts",
    "src/hoopdb/sessions-function-acl.json",
    "supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql",
    "supabase/bootstrap/acquisition_installer_verify.sql",
    "supabase/bootstrap/babysitter_retention_verify.sql",
    "supabase/bootstrap/buffered_search_feed_runtime_verify.sql",
    "supabase/bootstrap/canonical_project_events_verify.sql",
    "supabase/bootstrap/privacy_eraser_verify.sql",
    "supabase/bootstrap/session_directory_search_worker_verify.sql",
    "supabase/bootstrap/session_ingest_verify.sql",
    "supabase/bootstrap/session_read_api_role_verify.sql",
    // Migration sentinels and the bootstrap contract modules read from the anchor.
    "src/hoopdb/migration-sentinels.ts",
    "src/hoopdb/session-bootstrap-chain.ts",
    "src/hoopdb/session-bootstrap-roles.ts",
    "src/hoopdb/session-bootstrap-consumers.ts",
    // Test-only clock repair needed by old anchors after their fixed fixture expiry.
    // As with every fixed path, the anchor bytes must equal the release sha.
    "src/session-ingest/__tests__/rth-verifier.test.ts",
  ]),
];
const BOUNDARY_LANE_MODULE = /^scripts\/sessions-function-boundary\/lanes\/[a-z0-9-]+\.ts$/;
const ROLLBACK = /^supabase\/rollbacks\/([0-9]{14}_[a-z0-9_]+)\.down\.sql$/;

/**
 * Whether an anchor may change `path` relative to its main base. `anchorSteps` is the
 * anchor tree's schema step names: a rollback is allowed only for an upstream step the
 * anchor carries.
 */
export function releaseAnchorPathAllowed(path: string, anchorSteps: ReadonlySet<string>): boolean {
  if (RELEASE_ANCHOR_FIXED_PATHS.includes(path) || BOUNDARY_LANE_MODULE.test(path)) return true;
  for (const lane of RELEASE_TREE.lanes) {
    const rest = path.startsWith(`${lane.dir}/`) ? path.slice(lane.dir.length + 1) : "";
    if (rest && !rest.includes("/") && rest.endsWith(".sql")) return true;
  }
  const rb = ROLLBACK.exec(path);
  return !!rb && anchorSteps.has(`upstream:${rb[1]}.sql`);
}

/**
 * The two workflow files an anchor may carry, and only in one form: its main base's
 * bytes after releaseAnchorWorkflowTransform. A push runs the workflow files of the
 * pushed tree, so an anchor built on a main base from before ci.yml and pg-proof.yml ran
 * on release/schema-* pushes (for example 113dc657) needs exactly that one change to
 * get the push ci and pg-proof runs scripts/schema-release-github.ts requires
 * (release_target_ci). Not in RELEASE_ANCHOR_FIXED_PATHS: releaseAnchorPathAllowed
 * refuses them, and requireReleaseAnchor checks them byte for byte against the main base
 * (release_anchor_workflow_drift), never against the release sha.
 */
export const RELEASE_ANCHOR_WORKFLOW_PATHS: readonly string[] = [".github/workflows/ci.yml", ".github/workflows/pg-proof.yml"];
const RELEASE_ANCHOR_WORKFLOW_DIR = ".github/workflows/";
/** The main-only push branch selector the transform replaces (two-space YAML indent). */
export const RELEASE_ANCHOR_WORKFLOW_PUSH_MAIN = "    branches: [main]";
/** Its replacement: the exact hunk main's 42797f3c added to both files. */
export const RELEASE_ANCHOR_WORKFLOW_PUSH_RELEASE: readonly string[] = [
  "    # release/schema-*: the protected release anchor branches (a held-step target_sha",
  "    # needs green push runs on its own branch; scripts/schema-release-github.ts).",
  "    branches: [main, 'release/schema-*']",
];

/**
 * The one change an anchor may make to ci.yml or pg-proof.yml (pure; text split on
 * "\n", so a caller passing latin1-decoded bytes gets a byte-exact result). `base` must
 * contain the line `    branches: [main]` exactly once, directly under the line
 * `  push:`, inside the top-level `on:` block (the lines between `on:` and `  push:` are
 * empty or indented). That one line is replaced by RELEASE_ANCHOR_WORKFLOW_PUSH_RELEASE
 * (two comment lines and `    branches: [main, 'release/schema-*']`); every other byte,
 * line endings and the final newline included, is kept. Otherwise refuses
 * release_anchor_workflow_drift.
 */
export function releaseAnchorWorkflowTransform(base: string): string {
  const lines = base.split("\n");
  const at = lines.flatMap((l, i) => (l === RELEASE_ANCHOR_WORKFLOW_PUSH_MAIN ? [i] : []));
  if (at.length !== 1)
    refuse("release_anchor_workflow_drift", "the main base workflow does not have exactly one main-only push branch selector");
  const i = at[0]!;
  if (lines[i - 1] !== "  push:")
    refuse("release_anchor_workflow_drift", "the main base workflow's branch selector is not directly under push");
  let on = i - 2;
  while (on >= 0 && (lines[on] === "" || lines[on]!.startsWith(" "))) on--;
  if (on < 0 || lines[on] !== "on:")
    refuse("release_anchor_workflow_drift", "the main base workflow's push trigger is not in the top-level on block");
  return [...lines.slice(0, i), ...RELEASE_ANCHOR_WORKFLOW_PUSH_RELEASE, ...lines.slice(i + 1)].join("\n");
}

/**
 * An anchor's ci.yml or pg-proof.yml (a path its diff from the main base changes): a
 * regular 100644 blob at both, and target_sha's bytes exactly
 * releaseAnchorWorkflowTransform(main base bytes). release_anchor_workflow_drift otherwise.
 */
function requireAnchorWorkflow(repo: string, mainBase: string, targetSha: string, path: string): void {
  const blob = (sha: string): Buffer => {
    const es = lsTree(repo, sha, path).filter((e) => e.path === path);
    if (es.length !== 1 || es[0]!.type !== "blob" || es[0]!.mode !== "100644")
      refuse("release_anchor_workflow_drift", "an anchor workflow file is not a regular 100644 file at the main base and target_sha");
    const r = git(repo, ["cat-file", "blob", es[0]!.oid]);
    if (r.status !== 0) refuse("release_target_sha_unknown", "a target blob is not in the local object database");
    return r.stdout;
  };
  const base = blob(mainBase);
  const anchor = blob(targetSha);
  // latin1 maps each byte to one code unit and back, so the comparison is byte for byte.
  if (!Buffer.from(releaseAnchorWorkflowTransform(base.toString("latin1")), "latin1").equals(anchor))
    refuse("release_anchor_workflow_drift", "an anchor workflow file is not its main base with only the release/schema-* push branches added");
}

/** Migrator order (compareSteps): lane first (RELEASE_TREE.lanes), then the name. */
function stepOrder(a: string, b: string): number {
  const lane = (n: string) => RELEASE_TREE.lanes.findIndex((l) => n.startsWith(l.prefix));
  const d = lane(a) - lane(b);
  return d !== 0 ? d : a < b ? -1 : a > b ? 1 : 0;
}

/** release/schema-* branch -> tip commit, from the local refs under `namespace`. */
function anchorRefs(repo: string, namespace: string): Map<string, { oid: string; type: string }> {
  const r = git(repo, ["for-each-ref", "--format=%(objectname) %(objecttype) %(refname)", `${namespace}${RELEASE_ANCHOR_BRANCH_PREFIX.split("/")[0]}/`]);
  if (r.status !== 0) refuse("release_anchor_ref_missing", "the local release anchor refs could not be read");
  const out = new Map<string, { oid: string; type: string }>();
  for (const line of r.stdout.toString("utf8").split("\n")) {
    const [oid, type, ref] = line.split(" ");
    if (!oid || !type || !ref?.startsWith(namespace + RELEASE_ANCHOR_BRANCH_PREFIX)) continue;
    out.set(ref.slice(namespace.length), { oid, type });
  }
  return out;
}

function parentsOf(repo: string, sha: string): string[] {
  const r = git(repo, ["rev-list", "--parents", "-n", "1", sha, "--"]);
  if (r.status !== 0) refuse("release_target_sha_unknown", "an anchor commit's parents could not be read");
  return r.stdout.toString("utf8").trim().split(" ").slice(1);
}

function entryAt(repo: string, sha: string, path: string): string {
  const [e, ...rest] = lsTree(repo, sha, path).filter((x) => x.path === path);
  if (rest.length) refuse("release_anchor_file_drift", "an anchor path is ambiguous");
  return e ? `${e.mode} ${e.type} ${e.oid}` : "absent";
}

export interface ReleaseAnchorChain {
  /** The protected branch whose tip is target_sha. */
  branch: string;
  /** The first commit reached on the release sha's first-parent (main) history. */
  mainBase: string;
  /** Lower anchors between target_sha and the main base, nearest first, as their branches. */
  below: string[];
}

/**
 * Conditions (1)-(3) of a held-step anchor, in order, from the local object database:
 *  - format and existence (release_target_sha_invalid / _unknown);
 *  - the local release/schema-<slot> branch (`namespace`, the full checkout's
 *    refs/remotes/origin/ by default) exists (release_anchor_ref_missing; also a slot
 *    the release tree does not name exactly once: release_anchor_ref_ambiguous) and its
 *    tip is target_sha (release_anchor_ref_mismatch);
 *  - target_sha is not itself on the release sha's first-parent history, has exactly
 *    one parent, and every commit on its first-parent walk until main's first-parent
 *    history is a single-parent tip of a lower release/schema-<t> branch, t strictly
 *    below the previous one in migrator order (release_anchor_nonlinear for a merge or
 *    root commit; release_anchor_chain otherwise);
 *  - target_sha's seven verifier files exist (release_target_verifier_missing);
 *  - every path that differs between the main base and target_sha is allowlisted
 *    (releaseAnchorPathAllowed; release_anchor_extra_file) and byte-identical, mode
 *    included, to the release sha's (release_anchor_file_drift; absent at both counts as
 *    identical); the five SESSION_BOOTSTRAP_FILES must match the release sha even when
 *    the anchor does not change them. Exception: .github/workflows/ci.yml and
 *    pg-proof.yml may differ, only as releaseAnchorWorkflowTransform of the main base's
 *    bytes, mode 100644 at both (release_anchor_workflow_drift; also for any other
 *    .github/workflows/ path).
 */
export function requireReleaseAnchor(input: {
  repo: string;
  releaseSha: string;
  targetSha: string;
  target: string;
  namespace?: string;
}): ReleaseAnchorChain {
  const { repo, releaseSha, targetSha, target } = input;
  const namespace = input.namespace ?? RELEASE_ANCHOR_REF_NAMESPACE;
  requireCommits(repo, [targetSha, releaseSha]);
  // A shallow clone would show a grafted commit as parentless: refuse it as unreadable
  // history rather than report a nonlinear anchor.
  const shallow = git(repo, ["rev-parse", "--is-shallow-repository"]);
  if (shallow.status !== 0 || shallow.stdout.toString("utf8").trim() !== "false")
    refuse("release_target_sha_unknown", "the anchor history needs a full (non-shallow) clone");
  const branch = releaseAnchorBranch(target);
  if (!branch) refuse("release_anchor_ref_missing", "the release target has no anchor branch");
  // branch -> the release tree step it names; a slot the release tree names twice
  // (6-digit slots repeat across dates) cannot anchor.
  const releaseSteps = treeSchemaSteps(repo, releaseSha).map(([n]) => n);
  const stepOf = new Map<string, string | null>();
  for (const n of releaseSteps) {
    const b = releaseAnchorBranch(n);
    if (b) stepOf.set(b, stepOf.has(b) ? null : n);
  }
  if (stepOf.get(branch) !== target)
    refuse("release_anchor_ref_ambiguous", "the target's anchor slot does not name exactly this release step");
  const refs = anchorRefs(repo, namespace);
  const tip = refs.get(branch);
  if (!tip) refuse("release_anchor_ref_missing", "the target's release anchor branch is not in the local refs");
  if (tip.type !== "commit" || tip.oid !== targetSha)
    refuse("release_anchor_ref_mismatch", "the release anchor branch tip is not target_sha");
  const chain = git(repo, ["rev-list", "--first-parent", releaseSha, "--"]);
  if (chain.status !== 0)
    refuse("release_target_sha_unknown", "the release sha's first-parent history could not be read from the local object database");
  const main = new Set(chain.stdout.toString("utf8").split("\n").filter(Boolean));
  if (main.has(targetSha)) refuse("release_anchor_chain", "target_sha is a main commit, not an anchor commit");
  // Walk first parents off main: target_sha, then lower anchors only.
  const below: string[] = [];
  let cur = targetSha;
  let floor = target;
  let mainBase = "";
  for (let i = 0; i <= RELEASE_ANCHOR_MAX_CHAIN; i++) {
    const parents = parentsOf(repo, cur);
    if (parents.length !== 1) refuse("release_anchor_nonlinear", "an anchor commit does not have exactly one parent");
    const p = parents[0]!;
    if (main.has(p)) {
      mainBase = p;
      break;
    }
    // p must be the tip of a lower anchor branch: its step sorts strictly below the last.
    const lower = [...refs]
      .filter(([b, t]) => t.oid === p && t.type === "commit" && b !== branch)
      .map(([b]) => [b, stepOf.get(b)] as const)
      .filter((x): x is readonly [string, string] => typeof x[1] === "string" && stepOrder(x[1], floor) < 0)
      .sort((a, b) => stepOrder(b[1], a[1]));
    if (!lower.length) refuse("release_anchor_chain", "an anchor's parent is neither main nor the tip of a lower release anchor");
    below.push(lower[0]![0]);
    floor = lower[0]![1];
    cur = p;
  }
  if (!mainBase) refuse("release_anchor_chain", "the anchor chain does not reach main");
  // Missing verifiers first: a verifier absent at target_sha is its own refusal.
  treeVerifiers(repo, targetSha);
  const anchorSteps = new Set(treeSchemaSteps(repo, targetSha).map(([n]) => n));
  const diff = git(repo, ["diff-tree", "-r", "-z", "--no-renames", "--no-ext-diff", "--name-only", mainBase, targetSha, "--"]);
  if (diff.status !== 0) refuse("release_target_sha_unknown", "the anchor diff could not be read");
  const paths = diff.stdout.toString("utf8").split("\0").filter(Boolean);
  // ci.yml and pg-proof.yml: only main base -> releaseAnchorWorkflowTransform, checked
  // against the main base (not the release sha); any other workflow file refuses.
  const workflows = paths.filter((p) => p.startsWith(RELEASE_ANCHOR_WORKFLOW_DIR));
  for (const path of paths)
    if (path.startsWith(RELEASE_ANCHOR_WORKFLOW_DIR)) {
      if (!RELEASE_ANCHOR_WORKFLOW_PATHS.includes(path))
        refuse("release_anchor_workflow_drift", "the anchor changes a workflow file other than ci.yml and pg-proof.yml");
    } else if (!releaseAnchorPathAllowed(path, anchorSteps))
      refuse("release_anchor_extra_file", "the anchor changes a path outside the release anchor allowlist");
  for (const path of workflows) requireAnchorWorkflow(repo, mainBase, targetSha, path);
  // The other diff paths, and the five bootstrap files whether or not the anchor changes
  // them (an anchor that leaves them at main base bytes would bootstrap something else).
  for (const path of new Set([...paths.filter((p) => !workflows.includes(p)), ...SESSION_BOOTSTRAP_FILES]))
    if (entryAt(repo, targetSha, path) !== entryAt(repo, releaseSha, path))
      refuse("release_anchor_file_drift", "an anchor file differs from the release sha");
  return { branch, mainBase, below };
}

/**
 * Binds a release target to target_sha. requireTargetCommit; then target_sha's tree
 * must contain the target with no step of the target's own lane after it
 * (release_target_tail; applied other-lane steps may follow, bound by the manifest),
 * and its schema steps must equal `prefix` (the release tree's steps without the held ones, [name, sha256] in migrator order) exactly, by name and
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
  return bindTargetTree(input);
}

/**
 * The production binding (scripts/schema-release-database.ts targetAnchor, the operator
 * plan). target_sha === the release sha (nothing held): releaseTargetBinding. Otherwise
 * target_sha must be the release anchor (requireReleaseAnchor), and the release sha's own
 * tree must prove the same ordered prefix through the target: `prefix` is exactly the
 * release tree's steps of those names, by name and sha256, in migrator order, and holds
 * every release step of the target's lane up to the target
 * (release_anchor_prefix_mismatch). Then target_sha's tree is bound to the same prefix as
 * releaseTargetBinding binds it (release_target_tail, release_target_tree_mismatch: no
 * step above the target in its lane, nothing else).
 */
export function releaseAnchorBinding(input: {
  repo: string;
  releaseSha: string;
  targetSha: string;
  target: string;
  prefix: readonly (readonly [string, string])[];
  namespace?: string;
}): ReleaseTargetAnchor {
  if (input.targetSha === input.releaseSha) return releaseTargetBinding(input);
  requireReleaseAnchor(input);
  const names = new Set(input.prefix.map(([n]) => n));
  const release = treeSchemaSteps(input.repo, input.releaseSha);
  const through = release.filter(([n]) => names.has(n));
  const lane = (n: string) => n.slice(0, n.indexOf(":") + 1);
  if (
    !names.has(input.target) ||
    through.length !== input.prefix.length ||
    through.some(([n, d], i) => n !== input.prefix[i]![0] || d !== input.prefix[i]![1]) ||
    release.some(([n]) => lane(n) === lane(input.target) && stepOrder(n, input.target) <= 0 && !names.has(n))
  )
    refuse("release_anchor_prefix_mismatch", "the release sha's tree does not prove the release prefix through the target");
  return bindTargetTree(input);
}

function bindTargetTree(input: {
  repo: string;
  targetSha: string;
  target: string;
  prefix: readonly (readonly [string, string])[];
}): ReleaseTargetAnchor {
  const steps = treeSchemaSteps(input.repo, input.targetSha);
  const at = steps.findIndex(([n]) => n === input.target);
  // Per-lane tail: no step of the target's own lane may follow the target in
  // target_sha's tree. Already-applied steps of the other lane may follow (an upstream
  // target with 96bulls steps after it): they are in the manifest and the prefix
  // comparison below binds them by name and sha256. Every held step (either lane, i.e.
  // every pending step after the target) is absent from the prefix, so it must be
  // absent from target_sha's tree too (release_target_tree_mismatch otherwise).
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
