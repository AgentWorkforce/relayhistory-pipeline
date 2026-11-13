/**
 * The release target on the real migrator (runMigrations) in in-process PGlite:
 * steps after the target are held (never applied, never ledgered). A release that holds
 * steps is anchored to a real commit (target_sha) whose tree ends at the target, read
 * from the Git object database (release-target.ts) of temporary repositories built here.
 * Phase 2 runs this tree's verifiers, then target_sha's, and marks PASS for target_sha's
 * generation only after both sets pass, never for the full-tree one. A target_sha off the
 * release sha's first-parent chain (a non-ancestor or a merged side commit), an
 * abbreviated or unknown target_sha, a tree that does not end at the target or differs
 * from the prefix, a missing verifier, a manifest that drifted between plan and apply
 * or under the lock, a pending set or target generation other than the approved one,
 * an applied/unknown target or a plan without a target all refuse before any write.
 * The real-tree tests hold the current upstream tail on a full SELFHOSTED migrate,
 * anchored to a snapshot commit whose verifier files differ from this tree's, and check
 * readiness (schemaVerdict) and the 043700 G2 parity generation against a generation
 * computed independently from the snapshot's own files.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import {
  expectedSchema,
  generation,
  generationOfSteps,
  heldScope,
  migrationTargetSplit,
  plan,
  RELEASE_TREE,
  runMigrations,
  schemaStepDigests,
  type MigrationClient,
  type MigrationProfile,
  type ReleaseTargetBinding,
} from "../migrate.ts";
import { releaseAnchorBinding, releaseTargetBinding, requireTargetCommit, treeSchemaSteps, treeVerifiers, type ReleaseTargetAnchor } from "../release-target.ts";
import { createdSessionsFunctions, sessionsFunctionAclDrift } from "../migrate-guards.ts";
import { CONSUMER_RUNTIME_FUNCTIONS, RUNTIME_APPLICATION_FUNCTIONS } from "../migrate-guards.ts";
import { schemaVerdict, sessionsFunctionGateSql, type ExpectedSchema, type Transact } from "../../shared/db.ts";

setDefaultTimeout(120_000);
const LOCAL: MigrationProfile = { mode: "local", shim: false, bootstrap: false, verify: true, runtimeRole: "postgres" };
const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});
function client(db: PGlite, writes?: string[]): MigrationClient {
  return {
    async query<R>(text: string, params?: unknown[]) {
      if (writes && /^\s*(CREATE|ALTER|GRANT|REVOKE|INSERT|UPDATE|DELETE|DROP)\b/i.test(text)) writes.push(text);
      if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
      const results = await db.exec(text);
      return { rows: (results.at(-1)?.rows ?? []) as R[] };
    },
  };
}
const dir = mkdtempSync(join(tmpdir(), "migration-target-"));
const step = (name: string, sql: string) => {
  const path = join(dir, name.replace(/[:/]/g, "_"));
  writeFileSync(path, sql);
  const table = /sessions\.(\w+)/.exec(sql)![1]!;
  // A real catalog sentinel: the step's table exists.
  const sentinel = { evidence: table, sql: `SELECT to_regclass('sessions.${table}') IS NOT NULL AS applied` };
  return { name, path, sentinel };
};
const u = (n: string) => `upstream:2099010100000${n}_t${n}.sql`;
const steps = [
  step(u("1"), "CREATE TABLE sessions.t1 (x int);"),
  step(u("2"), "CREATE TABLE sessions.t2 (x int);"),
  step(u("3"), "CREATE TABLE sessions.t3 (x int);"),
  step(u("4"), "CREATE TABLE sessions.t4 (x int);"),
  step("verify:probe", "CREATE TABLE IF NOT EXISTS sessions.verify_ran (x int);"),
];
const fresh = async () => {
  const db = new PGlite();
  open.push(db);
  return db;
};
const ledger = async (db: PGlite) =>
  (await db.query<{ name: string }>("SELECT name FROM sessions.__96bulls_migrations ORDER BY name")).rows.map((r) => r.name);
const marker = async (db: PGlite) =>
  (await db.query<{ generation: string; status: string }>("SELECT generation, status FROM sessions.__96bulls_role_validation")).rows;
const exists = async (db: PGlite, rel: string) =>
  (await db.query<{ r: string | null }>("SELECT to_regclass($1)::text AS r", [rel])).rows[0]!.r !== null;
const failure = (p: Promise<unknown>) => p.then(() => null, (e: { code?: string }) => e.code ?? "no_code");
/** Setup: ledger exactly these fixture steps (a run without a target over a subset; nothing held). */
const applyOnly = (db: PGlite, names: string[]) =>
  runMigrations(client(db), { profile: { ...LOCAL, verify: false }, steps: steps.filter((s) => names.includes(s.name)) });
const tables = async (db: PGlite, prefix: string) =>
  (await db.query<{ t: string }>("SELECT tablename AS t FROM pg_tables WHERE schemaname = 'sessions' AND tablename LIKE $1 ORDER BY 1", [`${prefix}%`])).rows.map((r) => r.t);

// ---- Temporary Git repositories (the object database release-target.ts reads) --------

const REPO_ROOT = join(import.meta.dir, "../../..");
const sha256 = (text: string) => createHash("sha256").update(text).digest("hex");
// Independent of migrate.ts: the generation algorithm, spelled out, over [name, text].
const independentGeneration = (parts: [string, string][]) =>
  sha256(JSON.stringify(parts.map(([name, text]) => [name, sha256(text)])));
const VERIFY_NAMES = [
  "verify:session_directory_search_worker",
  "verify:session_token_verifier",
  "verify:session_ingest_seeder",
  "verify:session_ingest",
  "verify:canonical_project_events",
  "verify:acquisition_installer",
  "verify:buffered_search_feed_runtime",
];
const git = (repo: string, ...args: string[]) =>
  execFileSync(
    "git",
    ["-C", repo, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false", ...args],
    { encoding: "utf8" },
  ).trim();
/** A new repository and a commit helper (path -> content; null removes the file). */
const newRepo = () => {
  const repo = mkdtempSync(join(tmpdir(), "release-target-repo-"));
  git(repo, "init", "-q", "-b", "main");
  const commit = (files: Record<string, string | null>, message = "c") => {
    for (const [path, content] of Object.entries(files)) {
      const abs = join(repo, path);
      if (content === null) rmSync(abs);
      else {
        mkdirSync(dirname(abs), { recursive: true });
        writeFileSync(abs, content);
      }
    }
    git(repo, "add", "-A");
    git(repo, "commit", "-q", "--allow-empty", "-m", message);
    return git(repo, "rev-parse", "HEAD");
  };
  return { repo, commit };
};
const code = (f: () => unknown) => {
  try {
    f();
    return null;
  } catch (e) {
    return (e as { code?: string }).code ?? "no_code";
  }
};
const bindingOf = (a: ReleaseTargetAnchor): ReleaseTargetBinding => ({
  targetSha: a.targetSha,
  manifestDigest: a.manifestDigest,
  verifiers: a.verifiers,
});

// ---- Fixture tree in Git ---------------------------------------------------------------

const UPSTREAM_DIR = RELEASE_TREE.lanes[0].dir;
const fileOf = (name: string) => `${UPSTREAM_DIR}/${name.slice("upstream:".length)}`;
const contentOf = (name: string) => readFileSync(steps.find((s) => s.name === name)!.path, "utf8");
/** Seven verifier files (RELEASE_TREE paths); each creates sessions.<tag>_<i>; failAt fails. */
const verifierFiles = (tag: string, failAt = -1) =>
  Object.fromEntries(
    RELEASE_TREE.verifiers.map((v, i) => [
      v.path,
      i === failAt ? "SELECT 1/0;" : `CREATE TABLE IF NOT EXISTS sessions.${tag}_${i} (x int);`,
    ]),
  );
const verifierTexts = (files: Record<string, string>) =>
  RELEASE_TREE.verifiers.map((v, i) => [VERIFY_NAMES[i]!, files[v.path]!] as [string, string]);
/**
 * main: A (u1, u2; verifiers "ta") -> B (+u3; verifiers "tf", the 6th fails) -> R (+u4;
 * verifiers "tr"), the release commit. C is a side commit on A, not an ancestor of R.
 */
const fx = newRepo();
const taFiles = verifierFiles("ta");
const A = fx.commit({ [fileOf(u("1"))]: contentOf(u("1")), [fileOf(u("2"))]: contentOf(u("2")), ...taFiles }, "A");
const B = fx.commit({ [fileOf(u("3"))]: contentOf(u("3")), ...verifierFiles("tf", 5) }, "B");
const R = fx.commit({ [fileOf(u("4"))]: contentOf(u("4")), ...verifierFiles("tr") }, "R");
git(fx.repo, "checkout", "-q", "-b", "side", A);
const C = fx.commit({ "unrelated.txt": "x" }, "C");
git(fx.repo, "checkout", "-q", "main");

/** The on-disk prefix: [name, sha256] of the fixture schema steps that are not held. */
const fixturePrefix = (held: string[], from = steps) =>
  from
    .filter((s) => s.name.startsWith("upstream:") && !held.includes(s.name))
    .map((s) => [s.name, sha256(readFileSync(s.path, "utf8"))] as [string, string]);
const anchor = (targetSha: string, target: string, held: string[], from = steps, repo = fx.repo, releaseSha = R) =>
  releaseTargetBinding({ repo, releaseSha, targetSha, target, prefix: fixturePrefix(held, from) });

test("steps after the target are held: never applied or ledgered; both verifier sets run and PASS binds target_sha's generation only", async () => {
  const db = await fresh();
  const gen = await generation();
  const a = anchor(A, u("2"), [u("3"), u("4")]);
  // Non-circular: commit A's generation from the texts committed there, not from the
  // release tree (whose verifier files differ).
  const fromA = independentGeneration([[u("1"), contentOf(u("1"))], [u("2"), contentOf(u("2"))], ...verifierTexts(taFiles)]);
  expect(a.targetGeneration).toBe(fromA);
  expect(a.targetGeneration).not.toBe(gen);
  expect(a.targetGeneration).not.toBe(await generationOfSteps(steps.filter((s) => s.name === u("1") || s.name === u("2"))));
  const first = await runMigrations(client(db), {
    profile: LOCAL, steps, target: u("2"), expectedPending: [u("1"), u("2")],
    targetGeneration: a.targetGeneration, targetBinding: bindingOf(a),
  });
  expect(first).toEqual({ applied: 2, skipped: 0, held: [u("3"), u("4")] });
  expect(await ledger(db)).toEqual([u("1"), u("2")]);
  expect(await exists(db, "sessions.t3")).toBe(false);
  expect(await exists(db, "sessions.t4")).toBe(false);
  // This tree's verifier, then all seven of target_sha's.
  expect(await exists(db, "sessions.verify_ran")).toBe(true);
  expect(await tables(db, "ta_")).toEqual(RELEASE_TREE.verifiers.map((_, i) => `ta_${i}`));
  expect(await marker(db)).toEqual([{ generation: a.targetGeneration, status: "pass" }]);
  // The next release targets the tail: nothing held, no binding, generation() unchanged.
  const second = await runMigrations(client(db), { profile: LOCAL, steps, target: u("4"), expectedPending: [u("3"), u("4")], targetGeneration: gen });
  expect(second).toEqual({ applied: 2, skipped: 2, held: [] });
  expect(await ledger(db)).toEqual([u("1"), u("2"), u("3"), u("4")]);
  expect(await marker(db)).toEqual([{ generation: gen, status: "pass" }]);
});

test("condition (6), protected anchor: phase 2 runs target_sha's verifiers from the anchor commit's own objects and PASS binds the anchor generation only; held steps stay unledgered", async () => {
  // Anchor D off main at A: A's steps, verifiers at the release sha's bytes ("tr"); the
  // tip of release/schema-<u2 slot> in the release namespace. Not on main's history.
  git(fx.repo, "checkout", "-q", "-b", "anchor-d", A);
  const trFiles = verifierFiles("tr");
  const D = fx.commit(trFiles, "D");
  git(fx.repo, "checkout", "-q", "main");
  git(fx.repo, "update-ref", "refs/remotes/origin/release/schema-000002", D);
  try {
    const held = [u("3"), u("4")];
    const a = releaseAnchorBinding({ repo: fx.repo, releaseSha: R, targetSha: D, target: u("2"), prefix: fixturePrefix(held) });
    const fromD = independentGeneration([[u("1"), contentOf(u("1"))], [u("2"), contentOf(u("2"))], ...verifierTexts(trFiles)]);
    expect(a.targetSha).toBe(D);
    expect(a.targetGeneration).toBe(fromD);
    expect(a.targetGeneration).not.toBe(anchor(A, u("2"), held).targetGeneration);
    const db = await fresh();
    const gen = await generation();
    expect(a.targetGeneration).not.toBe(gen);
    const run = await runMigrations(client(db), {
      profile: LOCAL, steps, target: u("2"), expectedPending: [u("1"), u("2")],
      targetGeneration: a.targetGeneration, targetBinding: bindingOf(a),
    });
    expect(run).toEqual({ applied: 2, skipped: 0, held });
    expect(await ledger(db)).toEqual([u("1"), u("2")]);
    expect(await exists(db, "sessions.t3")).toBe(false);
    expect(await exists(db, "sessions.t4")).toBe(false);
    // D's seven verifiers ran (its "tr" texts), not A's.
    expect(await tables(db, "tr_")).toEqual(RELEASE_TREE.verifiers.map((_, i) => `tr_${i}`));
    expect(await tables(db, "ta_")).toEqual([]);
    expect(await marker(db)).toEqual([{ generation: a.targetGeneration, status: "pass" }]);
    // The same commit without its ref is refused.
    git(fx.repo, "update-ref", "-d", "refs/remotes/origin/release/schema-000002");
    expect(code(() => releaseAnchorBinding({ repo: fx.repo, releaseSha: R, targetSha: D, target: u("2"), prefix: fixturePrefix(held) }))).toBe(
      "release_anchor_ref_missing",
    );
  } finally {
    git(fx.repo, "update-ref", "-d", "refs/remotes/origin/release/schema-000002");
  }
});

test("target_sha: abbreviated, unknown, missing from a shallow clone, not on the first-parent chain, not ending at the target, differing steps or a missing verifier refuse", () => {
  const held = [u("3"), u("4")];
  expect(code(() => anchor(A.slice(0, 12), u("2"), held))).toBe("release_target_sha_invalid");
  expect(code(() => anchor(A.toUpperCase(), u("2"), held))).toBe("release_target_sha_invalid");
  expect(code(() => anchor("HEAD", u("2"), held))).toBe("release_target_sha_invalid");
  expect(code(() => anchor(`${A}\n`, u("2"), held))).toBe("release_target_sha_invalid");
  expect(code(() => anchor("0".repeat(40), u("2"), held))).toBe("release_target_sha_unknown");
  // A tree or blob id is not a commit.
  expect(code(() => anchor(git(fx.repo, "rev-parse", `${A}^{tree}`), u("2"), held))).toBe("release_target_sha_unknown");
  // Missing object: a shallow clone of R lacks A (fail closed, never a fetch).
  const shallow = mkdtempSync(join(tmpdir(), "release-target-shallow-"));
  execFileSync("git", ["clone", "-q", "--depth", "1", "--branch", "main", `file://${fx.repo}`, shallow]);
  expect(code(() => requireTargetCommit(shallow, R, A))).toBe("release_target_sha_unknown");
  expect(code(() => anchor(A, u("2"), held, steps, shallow))).toBe("release_target_sha_unknown");
  // C ends at u2 as well but is not an ancestor of the release commit R at all.
  expect(code(() => requireTargetCommit(fx.repo, R, C))).toBe("release_target_sha_not_ancestor");
  expect(code(() => anchor(C, u("2"), held))).toBe("release_target_sha_not_ancestor");
  // Tail mismatch: B's tree has u3 after u2; A's tree has no u3.
  expect(code(() => anchor(B, u("2"), held))).toBe("release_target_tail");
  expect(code(() => anchor(A, u("3"), [u("4")]))).toBe("release_target_tail");
  // Tree mismatch: the prefix's u2 differs from A's, or the prefix has an extra step.
  const drifted = fixturePrefix(held).map(([n, d]) => [n, n === u("2") ? sha256("other") : d] as [string, string]);
  expect(code(() => releaseTargetBinding({ repo: fx.repo, releaseSha: R, targetSha: A, target: u("2"), prefix: drifted }))).toBe("release_target_tree_mismatch");
  const extra: [string, string][] = [["upstream:20990101000000_t0.sql", sha256("x")], ...fixturePrefix(held)];
  expect(code(() => releaseTargetBinding({ repo: fx.repo, releaseSha: R, targetSha: A, target: u("2"), prefix: extra }))).toBe("release_target_tree_mismatch");
  // A verifier file absent at target_sha.
  const g = newRepo();
  const partial: Record<string, string | null> = verifierFiles("tg");
  delete partial[RELEASE_TREE.verifiers[3]!.path];
  const G = g.commit({ [fileOf(u("1"))]: contentOf(u("1")), [fileOf(u("2"))]: contentOf(u("2")), ...partial });
  const G2 = g.commit({ [fileOf(u("3"))]: contentOf(u("3")) });
  expect(code(() => anchor(G, u("2"), held, steps, g.repo, G2))).toBe("release_target_verifier_missing");
  // The release commit itself is a valid target only when its tree ends at the target.
  expect(requireTargetCommit(fx.repo, R, R)).toBeUndefined();
  expect(anchor(R, u("4"), []).targetSha).toBe(R);
});

test("target_sha: a side commit merged into the release (an ancestor via a second parent) with identical schema and verifier bytes is refused; its first-parent twin is accepted", () => {
  // main: P (u1, u2; verifiers "tp") -> Q (+u3) -> M (merge of S, first parent Q) -> R2 (+u4).
  // side: S on P changes only non-schema application bytes, so its schema steps and 7
  // verifier files equal P's exactly. S is an ancestor of R2, but only via M's second
  // parent: it does not carry the release's main/CI provenance.
  const m = newRepo();
  const tpFiles = verifierFiles("tp");
  const P = m.commit({ [fileOf(u("1"))]: contentOf(u("1")), [fileOf(u("2"))]: contentOf(u("2")), "app.ts": "main\n", ...tpFiles }, "P");
  const Q = m.commit({ [fileOf(u("3"))]: contentOf(u("3")) }, "Q");
  git(m.repo, "checkout", "-q", "-b", "side", P);
  const S = m.commit({ "app.ts": "side branch application bytes\n" }, "S");
  git(m.repo, "checkout", "-q", "main");
  git(m.repo, "merge", "-q", "--no-ff", "-m", "M", S);
  const M = git(m.repo, "rev-parse", "HEAD");
  const R2 = m.commit({ [fileOf(u("4"))]: contentOf(u("4")) }, "R2");
  // Premises: S is an ancestor of R2 (merge-base agrees), M's second parent is S, and S's
  // schema steps and verifier bytes equal P's (the manifest alone cannot tell them apart).
  expect(git(m.repo, "rev-parse", `${M}^2`)).toBe(S);
  expect(git(m.repo, "rev-parse", `${M}^1`)).toBe(Q);
  git(m.repo, "merge-base", "--is-ancestor", S, R2);
  expect(treeSchemaSteps(m.repo, S)).toEqual(treeSchemaSteps(m.repo, P));
  expect(treeVerifiers(m.repo, S)).toEqual(treeVerifiers(m.repo, P));
  expect(git(m.repo, "rev-parse", `${S}^{tree}`)).not.toBe(git(m.repo, "rev-parse", `${P}^{tree}`));
  const held = [u("3"), u("4")];
  // Refused: S is not on R2's first-parent chain.
  expect(code(() => requireTargetCommit(m.repo, R2, S))).toBe("release_target_sha_not_ancestor");
  expect(code(() => anchor(S, u("2"), held, steps, m.repo, R2))).toBe("release_target_sha_not_ancestor");
  let message = "";
  try {
    requireTargetCommit(m.repo, R2, S);
  } catch (e) {
    message = (e as Error).message;
  }
  expect(message).toContain("first-parent chain");
  // Accepted: P (same schema and verifier bytes) is on the first-parent chain, as are Q,
  // the merge commit M itself and the release commit R2.
  const p = anchor(P, u("2"), held, steps, m.repo, R2);
  expect(p.targetSha).toBe(P);
  expect(p.manifest.steps).toEqual(treeSchemaSteps(m.repo, S));
  for (const ok of [P, Q, M, R2]) expect(requireTargetCommit(m.repo, R2, ok)).toBeUndefined();
  // S as its own release sha: S itself and P (S's first parent) are accepted.
  expect(requireTargetCommit(m.repo, S, S)).toBeUndefined();
  expect(requireTargetCommit(m.repo, S, P)).toBeUndefined();
  // A later main commit is not on an earlier release's chain.
  expect(code(() => requireTargetCommit(m.repo, Q, R2))).toBe("release_target_sha_not_ancestor");
});

test("per-lane tail: an upstream target is accepted with 96bulls lane steps after it in the same tree; a later upstream step refuses", () => {
  // Already-applied steps of the other lane may follow the target: they are part of the
  // manifest (the prefix comparison binds them). Only the target's own lane must end at
  // the target; held steps (either lane) are absent from the prefix and so from the tree.
  const OURS_DIR = RELEASE_TREE.lanes[1].dir;
  const ours = "96bulls:0001_lane_two.sql";
  const oursSql = "CREATE TABLE sessions.lane_two (x int);";
  const l = newRepo();
  const X = l.commit({
    [fileOf(u("1"))]: contentOf(u("1")), [fileOf(u("2"))]: contentOf(u("2")),
    [`${OURS_DIR}/0001_lane_two.sql`]: oursSql, ...verifierFiles("tx"),
  }, "X");
  const Y = l.commit({ [fileOf(u("3"))]: contentOf(u("3")) }, "Y");
  const R3 = l.commit({ [fileOf(u("4"))]: contentOf(u("4")) }, "R3");
  const prefix = [...fixturePrefix([u("3"), u("4")]), [ours, sha256(oursSql)] as [string, string]];
  const bind = (targetSha: string) =>
    releaseTargetBinding({ repo: l.repo, releaseSha: R3, targetSha, target: u("2"), prefix });
  // Accepted: the 96bulls step follows the upstream target in X's tree and in the manifest.
  const x = bind(X);
  expect(x.manifest.steps.map(([n]) => n)).toEqual([u("1"), u("2"), ours]);
  expect(x.manifest.steps.at(-1)![0]).toBe(ours);
  // Refused: Y has a later upstream (same-lane) step u3 after the target.
  expect(code(() => bind(Y))).toBe("release_target_tail");
  // Refused: the 96bulls step bound by the manifest differs (drifted other-lane bytes).
  const drifted = prefix.map(([n, d]) => [n, n === ours ? sha256("other") : d] as [string, string]);
  expect(code(() => releaseTargetBinding({ repo: l.repo, releaseSha: R3, targetSha: X, target: u("2"), prefix: drifted }))).toBe("release_target_tree_mismatch");
});

test("held steps need the binding; a wrong generation, a drifted or replayed binding, or no in-process verifiers refuse before any write", async () => {
  const db = await fresh();
  await applyOnly(db, [u("1")]);
  const ledgerBefore = await ledger(db);
  const markerBefore = await marker(db);
  const full = await generation();
  const a = anchor(B, u("3"), [u("4")]);
  const right = a.targetGeneration;
  const ok = bindingOf(a);
  const pend = { target: u("3"), expectedPending: [u("2"), u("3")] };
  // Verifier drift: SQL that no longer matches its digest, or a re-digested other set.
  const otherSql = ok.verifiers.map((v, i) => (i === 2 ? { ...v, sql: `${v.sql}\n-- drift` } : v));
  const otherSet = otherSql.map((v) => ({ ...v, sha256: sha256(v.sql) }));
  for (const [opts, expected] of [
    [{ ...pend, targetGeneration: right }, "release_target_binding_required"],
    [{ ...pend }, "release_target_binding_required"],
    // The full-tree generation while steps are held: never accepted as the target.
    [{ ...pend, targetGeneration: full, targetBinding: ok }, "release_target_generation_changed"],
    // "This tree without the held steps" (this tree's verifiers): not target_sha's.
    [{ ...pend, targetGeneration: await generationOfSteps(steps.filter((s) => s.name.startsWith("upstream:") && s.name !== u("4"))), targetBinding: ok }, "release_target_generation_changed"],
    [{ ...pend, targetGeneration: right.toUpperCase(), targetBinding: ok }, "release_target_generation_changed"],
    [{ ...pend, targetGeneration: "", targetBinding: ok }, "release_target_generation_changed"],
    [{ targetGeneration: right }, "release_target_required"],
    [{ targetBinding: ok }, "release_target_required"],
    [{ ...pend, targetGeneration: right, targetBinding: { ...ok, verifiers: otherSql } }, "release_target_manifest_changed"],
    [{ ...pend, targetGeneration: right, targetBinding: { ...ok, verifiers: otherSet } }, "release_target_manifest_changed"],
    [{ ...pend, targetGeneration: right, targetBinding: { ...ok, verifiers: ok.verifiers.slice(1) } }, "release_target_manifest_changed"],
    [{ ...pend, targetGeneration: right, targetBinding: { ...ok, verifiers: [...ok.verifiers].reverse() } }, "release_target_manifest_changed"],
    // Plan-vs-apply drift: the approved digest belongs to another target commit or target.
    [{ ...pend, targetGeneration: right, targetBinding: { ...ok, targetSha: A } }, "release_target_manifest_changed"],
    [{ ...pend, targetGeneration: right, targetBinding: { ...ok, manifestDigest: anchor(A, u("2"), [u("3"), u("4")]).manifestDigest } }, "release_target_manifest_changed"],
  ] as const) {
    const writes: string[] = [];
    let gate = false;
    expect(
      await failure(runMigrations(client(db, writes), { profile: LOCAL, steps, ...opts, beforeWrite: async () => { gate = true; } })),
    ).toBe(expected);
    expect(writes).toEqual([]);
    expect(gate).toBe(false);
  }
  // A binding needs this tree's in-process phase 2 (verify off: refused).
  const noVerify = steps.filter((s) => !s.name.startsWith("verify:"));
  expect(await failure(runMigrations(client(db), { profile: { ...LOCAL, verify: false }, steps: noVerify, ...pend, targetBinding: ok }))).toBe("release_target_verify_required");
  expect(await ledger(db)).toEqual(ledgerBefore);
  expect(await marker(db)).toEqual(markerBefore);
});

test("manifest drift: a prefix file changed after the plan refuses at the apply recompute and under the lock, before any write", async () => {
  // A private copy of the fixture steps, so the drift does not leak into other tests.
  const copyDir = mkdtempSync(join(tmpdir(), "migration-target-drift-"));
  const mine = steps.map((s) => {
    const path = join(copyDir, s.name.replace(/[:/]/g, "_"));
    writeFileSync(path, readFileSync(s.path, "utf8"));
    return { ...s, path };
  });
  const planned = anchor(A, u("2"), [u("3"), u("4")], mine);
  // The on-disk u2 changes after the plan (e.g. a different checkout at apply).
  writeFileSync(mine[1]!.path, "CREATE TABLE sessions.t2 (x int, y int);");
  // Apply-time recompute (the database job, before connecting): A's tree no longer
  // equals the prefix.
  expect(code(() => anchor(A, u("2"), [u("3"), u("4")], mine))).toBe("release_target_tree_mismatch");
  // Under the lock (the migrator recomputes the manifest from the steps it would run).
  const db = await fresh();
  const writes: string[] = [];
  let gate = false;
  expect(
    await failure(runMigrations(client(db, writes), {
      profile: LOCAL, steps: mine, target: u("2"), expectedPending: [u("1"), u("2")],
      targetGeneration: planned.targetGeneration, targetBinding: bindingOf(planned),
      beforeWrite: async () => { gate = true; },
    })),
  ).toBe("release_target_manifest_changed");
  expect(writes).toEqual([]);
  expect(gate).toBe(false);
  expect(await exists(db, "sessions.__96bulls_migrations")).toBe(false);
});

test("target_sha's verifiers fail after this tree's pass: the marker stays pending for the target generation, never PASS", async () => {
  const db = await fresh();
  const b = anchor(B, u("3"), [u("4")]);
  const err = await failure(runMigrations(client(db), {
    profile: LOCAL, steps, target: u("3"), expectedPending: [u("1"), u("2"), u("3")],
    targetGeneration: b.targetGeneration, targetBinding: bindingOf(b),
  }));
  expect(err).not.toBeNull();
  expect(await ledger(db)).toEqual([u("1"), u("2"), u("3")]);
  expect(await exists(db, "sessions.t4")).toBe(false);
  // This tree's verifier passed, then target_sha's ran in order up to the failing one.
  expect(await exists(db, "sessions.verify_ran")).toBe(true);
  expect(await tables(db, "tf_")).toEqual(["tf_0", "tf_1", "tf_2", "tf_3", "tf_4"]);
  expect(await marker(db)).toEqual([{ generation: b.targetGeneration, status: "pending" }]);
});

test("nothing held but target_sha's verifiers differ from this tree's: refused before any write", async () => {
  const db = await fresh();
  // R's tree ends at u4 (nothing held) but its verifiers are not this tree's.
  const r = anchor(R, u("4"), []);
  const writes: string[] = [];
  expect(await failure(runMigrations(client(db, writes), { profile: LOCAL, steps, target: u("4"), targetBinding: bindingOf(r) }))).toBe("release_target_generation_changed");
  expect(writes).toEqual([]);
});

test("a pending set other than the approved one refuses under the lock before any write", async () => {
  const db = await fresh();
  await applyOnly(db, [u("1")]);
  const ledgerBefore = await ledger(db);
  const markerBefore = await marker(db);
  for (const expectedPending of [[u("2")], [u("1"), u("2")], [u("2"), u("3"), u("4")], [u("3"), u("2")], [u("3")], []]) {
    const writes: string[] = [];
    let gate = false;
    expect(
      await failure(
        runMigrations(client(db, writes), {
          profile: LOCAL,
          steps,
          target: u("3"),
          expectedPending,
          beforeWrite: async () => {
            gate = true;
          },
        }),
      ),
    ).toBe("release_target_pending_changed");
    expect(writes).toEqual([]);
    expect(gate).toBe(false);
  }
  expect(await ledger(db)).toEqual(ledgerBefore);
  expect(await marker(db)).toEqual(markerBefore);
});

test("unknown, applied or missing targets refuse before any write; no target keeps the full run", async () => {
  const db = await fresh();
  await applyOnly(db, [u("1"), u("2")]);
  for (const [opts, code] of [
    [{ target: "upstream:20990101000009_t9.sql" }, "release_target_unknown"],
    [{ target: "verify:probe" }, "release_target_unknown"],
    [{ target: u("2") }, "release_target_not_pending"],
    [{ target: u("1"), expectedPending: [] }, "release_target_not_pending"],
    [{ expectedPending: [u("3")] }, "release_target_required"],
  ] as const) {
    const writes: string[] = [];
    expect(await failure(runMigrations(client(db, writes), { profile: LOCAL, steps, ...opts }))).toBe(code);
    expect(writes).toEqual([]);
  }
  expect(await ledger(db)).toEqual([u("1"), u("2")]);
  // Without a target the migrator is unchanged: every pending step and phase 2.
  expect(await runMigrations(client(db), { profile: LOCAL, steps })).toEqual({ applied: 2, skipped: 2, held: [] });
  expect((await marker(db))[0]!.status).toBe("pass");
});


// ---- Real tree: hold the current upstream tail on a full SELFHOSTED migrate ----------

const SELFHOSTED: MigrationProfile = { mode: "selfhosted", shim: true, bootstrap: true, verify: true, runtimeRole: "history_rt", runtimeLogin: false, functionBoundary: "global-default-revoke" };
const TARGET = "upstream:20261105047000_coordination_fact_readers.sql";
const ERASE = "upstream:20261105050000_operator_session_erase.sql";
const ERASE_FENCE = "upstream:20261105050010_operator_session_erase_fence_check.sql";
const ERASE_RUNTIME = "upstream:20261105050020_operator_session_erase_runtime.sql";
/** A consumer-gate allowlist entry only the held 050020 creates (#530). */
const RUNTIME_UPSERT = "sessions.ingest_operator_runtime_upsert(jsonb,text,text)";
const transact = (db: PGlite): Transact => (work) => db.transaction((tx) => work(tx as never)) as never;
const realDb = async () => {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(`CREATE ROLE history_rt NOLOGIN NOINHERIT; ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
    DO $$ BEGIN EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC',current_database()); END $$;`);
  return db;
};
const schemaNames = async () => (await plan(SELFHOSTED)).map((s) => s.name).filter((n) => n.startsWith("upstream:") || n.startsWith("96bulls:"));
const laneFile = (name: string) => {
  const lane = RELEASE_TREE.lanes.find((l) => name.startsWith(l.prefix))!;
  return `${lane.dir}/${name.slice(lane.prefix.length)}`;
};
/**
 * A snapshot of this tree in a temporary repository: commit T has this tree's schema
 * files without `held` and the 7 verifier files with a comment appended (so they differ
 * from this tree's), and its child R (the release commit) adds the held files and this
 * tree's verifier files. Returns the binding as the release plan computes it, and the
 * generation computed independently from T's working-tree files.
 */
const snapshots = new Map<string, { anchor: ReleaseTargetAnchor; independent: string }>();
async function snapshotBinding(held: string[], target = TARGET) {
  const key = `${target}|${held.join()}`;
  if (snapshots.has(key)) return snapshots.get(key)!;
  const names = await schemaNames();
  const r = newRepo();
  const kept = names.filter((n) => !held.includes(n));
  const files: Record<string, string> = {};
  for (const n of kept) files[laneFile(n)] = readFileSync(join(REPO_ROOT, laneFile(n)), "utf8");
  for (const v of RELEASE_TREE.verifiers)
    files[v.path] = `${readFileSync(join(REPO_ROOT, v.path), "utf8")}\n-- snapshot of the target commit\n`;
  const T = r.commit(files, "T");
  const independent = independentGeneration([
    ...kept.map((n) => [n, readFileSync(join(r.repo, laneFile(n)), "utf8")] as [string, string]),
    ...RELEASE_TREE.verifiers.map((v, i) => [VERIFY_NAMES[i]!, readFileSync(join(r.repo, v.path), "utf8")] as [string, string]),
  ]);
  const later: Record<string, string> = {};
  for (const n of held) later[laneFile(n)] = readFileSync(join(REPO_ROOT, laneFile(n)), "utf8");
  for (const v of RELEASE_TREE.verifiers) later[v.path] = readFileSync(join(REPO_ROOT, v.path), "utf8");
  const releaseSha = r.commit(later, "R");
  const a = releaseTargetBinding({ repo: r.repo, releaseSha, targetSha: T, target, prefix: await schemaStepDigests(held) });
  const out = { anchor: a, independent };
  snapshots.set(key, out);
  return out;
}
/**
 * The production shape: every step except TARGET and the tail after it (both lanes'
 * earlier steps) is ledgered. Setup applies them with the verifier off; its closing
 * full-manifest ACL check refuses (the 047000+ functions are missing, nothing held)
 * after every step committed, and the marker stays pending.
 */
async function prefixDatabase() {
  const db = await realDb();
  const names = await schemaNames();
  const tail = names.slice(names.indexOf(TARGET)).filter((n) => n.startsWith("upstream:"));
  const setup = (await plan(SELFHOSTED)).filter((s) => !tail.includes(s.name) && !s.name.startsWith("verify:"));
  expect(await failure(runMigrations(client(db), { profile: { ...SELFHOSTED, verify: false }, steps: setup }))).toBe("function_acl_drift");
  const have = new Set(await ledger(db));
  const split = migrationTargetSplit(names.map((name) => ({ name, pending: !have.has(name) })), TARGET);
  const { anchor: binding, independent } = await snapshotBinding(split.held);
  return { db, names, split, binding, independent, targetGeneration: binding.targetGeneration };
}
const verdict = async (db: PGlite, expected: ExpectedSchema) => {
  await db.exec("SET ROLE history_rt");
  try {
    return await schemaVerdict(expected, transact(db));
  } finally {
    await db.exec("RESET ROLE");
  }
};
const parityGeneration = async (db: PGlite) =>
  (await db.query<{ g: string | null }>(
    "SELECT target_generation AS g FROM sessions.owner_buffered_parity_expected('org_t','ws_t','user_t','epoch_t',NULL,NULL,NULL)",
  )).rows[0]!.g;
const runTarget = (db: PGlite, s: { split: { expectedPending: string[] }; binding: ReleaseTargetAnchor }, extra: object = {}) =>
  runMigrations(client(db), {
    profile: SELFHOSTED, target: TARGET, expectedPending: s.split.expectedPending,
    targetGeneration: s.binding.targetGeneration, targetBinding: bindingOf(s.binding), ...extra,
  });
const heldProcs = (db: PGlite) =>
  db.query<{ n: number }>(
    "SELECT count(*)::int n FROM pg_proc WHERE proname LIKE 'ingest_operator_erase%' OR proname LIKE 'ingest_operator_runtime%' OR proname LIKE 'ingest_erase_fence%' OR proname LIKE 'upload_consent%' OR proname IN ('buffered_session_stages_for_target', 'buffered_session_stages_for_target_paged', 'session_summary_activity_for', 'session_directory_users_for')",
  ).then((r) => r.rows[0]!.n);

test("real tree, target 047000: the target generation is target_sha's own (independently recomputed), not this tree's with or without the held steps", async () => {
  const names = await schemaNames();
  const split = migrationTargetSplit(names.map((name) => ({ name, pending: !name.startsWith("96bulls:") && names.indexOf(name) >= names.indexOf(TARGET) })), TARGET);
  expect(split.held).toContain(ERASE);
  expect(split.held).toContain(ERASE_FENCE);
  // Every upstream step after the target (as of this tree: 047010, 047020, 050000, 050010, 050020, 056000, 057000, 057010).
  expect(split.held).toEqual(names.filter((n) => n.startsWith("upstream:") && n > TARGET));
  expect(split.held).toContain("upstream:20261105047010_buffered_exact_session_stages.sql");
  expect(split.held).toContain(ERASE_RUNTIME);
  expect(split.held).toContain("upstream:20261105056000_session_summary_activity.sql");
  expect(split.held).toContain("upstream:20261105057000_session_directory_users_aggregate.sql");
  expect(split.held.every((n) => n.startsWith("upstream:") && n > TARGET)).toBe(true);
  const { anchor: a, independent } = await snapshotBinding(split.held);
  // Non-circular: the snapshot's verifier files differ from this tree's, and the expected
  // value comes from the snapshot's files, not from migrate.ts or release-target.ts.
  expect(a.targetGeneration).toBe(independent);
  expect(a.targetGeneration).not.toBe(await generation());
  const all = (await plan(SELFHOSTED)).filter((s) => s.name.startsWith("upstream:") || s.name.startsWith("96bulls:"));
  expect(a.targetGeneration).not.toBe(await generationOfSteps(all.filter((s) => !split.held.includes(s.name))));
  // The manifest's steps are this tree's prefix exactly; nothing held = generation().
  expect(a.manifest.steps).toEqual(await schemaStepDigests(split.held));
  expect(a.manifest.steps.at(-1)![0]).toBe(names.filter((n) => n.startsWith("96bulls:")).at(-1)!);
  expect(await generationOfSteps(all)).toBe(await generation());
  expect(await failure(schemaStepDigests(["upstream:20990101000000_not_in_tree.sql"]))).toBe("release_target_unknown");
});

test("real tree (1)-(3): steps held, readiness is OK for the target image, schema_pending for the full image, G2 parity binds target_sha's generation", async () => {
  const s = await prefixDatabase();
  const full: ExpectedSchema = { ...(await expectedSchema()), dbMode: "selfhosted" };
  // The target image's readiness scope: the held suffix, bound to the approved prefix manifest.
  const prefix: ExpectedSchema = {
    ...full, names: full.names.filter((n) => !s.split.held.includes(n)), generation: s.independent,
    held: await heldScope(s.split.held, s.binding.manifest.steps),
  };
  expect(s.split.held).toContain(ERASE);
  expect(s.split.held).toContain(ERASE_FENCE);
  expect(s.split.held).toContain(ERASE_RUNTIME);
  expect(prefix.held!.find((h) => h.name === ERASE_RUNTIME)!.creates).toContain(RUNTIME_UPSERT);
  expect(s.targetGeneration).toBe(s.independent);
  expect(s.targetGeneration).not.toBe(full.generation);
  // Before the run: fenced (TARGET pending).
  expect(await verdict(s.db, prefix)).toEqual({ ok: false, reason: "schema_pending", count: 1 });
  expect(await parityGeneration(s.db)).toBeNull();
  const result = await runTarget(s.db, s);
  expect(result.held).toEqual(s.split.held);
  expect(result.applied).toBe(1);
  // (1) PASS for exactly target_sha's generation after both verifier sets; an image built
  //     at target_sha (its own --generation, recomputed independently) is ready.
  expect(await marker(s.db)).toEqual([{ generation: s.independent, status: "pass" }]);
  expect(await verdict(s.db, prefix)).toEqual({ ok: true });
  // Without the held scope the live function gate is strict: the consumer allowlist names
  // 050020's (held, absent) writer, and its ::regprocedure cast raises 42883.
  const { held: _held, ...unscoped } = prefix;
  expect(await verdict(s.db, unscoped)).toEqual({ ok: false, reason: "role_validation_failed", error_code: "42883" });
  // (2) the full-tree image stays fenced: its held steps are not in the ledger.
  expect(await verdict(s.db, full)).toEqual({ ok: false, reason: "schema_pending", count: s.split.held.length });
  // ... and never matches the target PASS even if its names were present.
  expect(await verdict(s.db, { ...prefix, generation: full.generation })).toEqual({ ok: false, reason: "validation_pending" });
  // (3) 043700 G2: owner_buffered_parity_expected reads the PASS generation, target_sha's.
  expect(await parityGeneration(s.db)).toBe(s.independent);
  // Held steps: no ledger row, no catalog object.
  for (const n of s.split.held) expect(await ledger(s.db)).not.toContain(n);
  expect(await exists(s.db, "sessions.session_erasure_fences")).toBe(false);
  expect(await heldProcs(s.db)).toBe(0);
});

test("real tree (4): the next release applies the held steps and PASS moves to the full generation", async () => {
  const s = await prefixDatabase();
  await runTarget(s.db, s);
  const full: ExpectedSchema = { ...(await expectedSchema()), dbMode: "selfhosted" };
  const prefix: ExpectedSchema = { ...full, names: full.names.filter((n) => !s.split.held.includes(n)), generation: s.targetGeneration };
  const last = s.split.held.at(-1)!;
  const tailSplit = migrationTargetSplit(s.names.map((name) => ({ name, pending: s.split.held.includes(name) })), last);
  expect(tailSplit).toEqual({ expectedPending: s.split.held, held: [] });
  // target_sha's generation is not the full one: a stale value refuses before any write.
  expect(await failure(runMigrations(client(s.db), { profile: SELFHOSTED, target: last, expectedPending: tailSplit.expectedPending, targetGeneration: s.targetGeneration }))).toBe("release_target_generation_changed");
  expect(await marker(s.db)).toEqual([{ generation: s.targetGeneration, status: "pass" }]);
  const result = await runMigrations(client(s.db), { profile: SELFHOSTED, target: last, expectedPending: tailSplit.expectedPending, targetGeneration: full.generation });
  expect(result).toMatchObject({ applied: s.split.held.length, held: [] });
  expect(await marker(s.db)).toEqual([{ generation: full.generation, status: "pass" }]);
  expect(await verdict(s.db, full)).toEqual({ ok: true });
  expect(await parityGeneration(s.db)).toBe(full.generation);
  // The target image is no longer ready once the full tree passed (a generation change).
  expect(await verdict(s.db, prefix)).toEqual({ ok: false, reason: "validation_pending" });
});

test("real tree (5): a failing phase 2 leaves the marker pending for target_sha's generation and everything fenced", async () => {
  const s = await prefixDatabase();
  const full: ExpectedSchema = { ...(await expectedSchema()), dbMode: "selfhosted" };
  const prefix: ExpectedSchema = { ...full, names: full.names.filter((n) => !s.split.held.includes(n)), generation: s.targetGeneration };
  const boom = Object.assign(new Error("verifier down"), { code: "verify_failed_test" });
  expect(await failure(runTarget(s.db, s, { beforeVerify: async () => { throw boom; } }))).toBe("verify_failed_test");
  // TARGET committed (its own transaction); held steps absent; never PASS.
  expect(await ledger(s.db)).toContain(TARGET);
  expect(await ledger(s.db)).not.toContain(ERASE);
  expect(await marker(s.db)).toEqual([{ generation: s.targetGeneration, status: "pending" }]);
  expect(await verdict(s.db, prefix)).toEqual({ ok: false, reason: "validation_pending" });
  expect(await verdict(s.db, full)).toEqual({ ok: false, reason: "schema_pending", count: s.split.held.length });
  expect(await parityGeneration(s.db)).toBeNull();
});

test("real tree: a prefix the phase 2 verifiers cannot validate (later-lane steps held) fails closed, marker pending", async () => {
  const db = await realDb();
  const names = await schemaNames();
  const split = migrationTargetSplit(names.map((name) => ({ name, pending: true })), TARGET);
  // From empty, an upstream target also holds the whole 96bulls lane, whose objects
  // the verifiers require; the snapshot commit has no 96bulls lane at all.
  expect(split.held.some((n) => n.startsWith("96bulls:"))).toBe(true);
  const { anchor: a } = await snapshotBinding(split.held);
  expect(await failure(runMigrations(client(db), {
    profile: SELFHOSTED, target: TARGET, expectedPending: split.expectedPending,
    targetGeneration: a.targetGeneration, targetBinding: bindingOf(a),
  }))).not.toBeNull();
  expect(await marker(db)).toEqual([{ generation: a.targetGeneration, status: "pending" }]);
  expect(await parityGeneration(db)).toBeNull();
});

test("real tree: the prefix-scoped ACL check excuses only absent functions a held step creates", async () => {
  const s = await prefixDatabase();
  await runTarget(s.db, s);
  const heldSql = await Promise.all(
    (await plan(SELFHOSTED)).filter((st) => s.split.held.includes(st.name)).map((st) => Bun.file(st.path).text()),
  );
  const c = client(s.db);
  expect(await sessionsFunctionAclDrift(c, "history_rt", heldSql)).toEqual([]);
  // Unscoped, the full manifest names every held function as missing.
  const unscoped = await sessionsFunctionAclDrift(c, "history_rt");
  expect(unscoped.length).toBeGreaterThan(0);
  expect(unscoped.every((d) => /^sessions\.(buffered_session_stages_for_target|ingest_operator_erase_|operator_user_erase_|ingest_operator_runtime_|ingest_erase_fence_|session_summary_activity_for|session_directory_users_for|upload_consent_|coordination_advisories_)\w*\(.*\): missing/.test(d))).toBe(true);
  expect(unscoped.some((d) => d.startsWith("sessions.ingest_operator_runtime_upsert(jsonb,text,text): missing"))).toBe(true);
  expect(unscoped.some((d) => d.startsWith("sessions.buffered_session_stages_for_target("))).toBe(true);
  // A function a held step only replaces (it exists) is still compared exactly.
  await s.db.exec("GRANT EXECUTE ON FUNCTION sessions.ingest_commit_batch(jsonb) TO PUBLIC");
  expect((await sessionsFunctionAclDrift(c, "history_rt", heldSql)).join(";")).toContain("sessions.ingest_commit_batch(jsonb)");
  await s.db.exec("REVOKE EXECUTE ON FUNCTION sessions.ingest_commit_batch(jsonb) FROM PUBLIC");
  expect(await sessionsFunctionAclDrift(c, "history_rt", heldSql)).toEqual([]);
  // A held step's function that already exists (created early) is compared exactly too.
  await s.db.exec("CREATE FUNCTION sessions.ingest_erase_fence_index_lock() RETURNS void LANGUAGE sql AS 'SELECT'; GRANT EXECUTE ON FUNCTION sessions.ingest_erase_fence_index_lock() TO history_rt");
  expect((await sessionsFunctionAclDrift(c, "history_rt", heldSql)).join(";")).toContain("sessions.ingest_erase_fence_index_lock()");
});

/** The refusal's code and message (failure() keeps only the code). */
const refusal = (p: Promise<unknown>) =>
  p.then(() => null, (e: { code?: string; message?: string }) => ({ code: e.code ?? "no_code", message: String(e.message) }));
const heldSqlOf = async (held: readonly string[]) =>
  Promise.all((await plan(SELFHOSTED)).filter((st) => held.includes(st.name)).map((st) => Bun.file(st.path).text()));

test("real tree: an absent listed runtime function that no held step creates refuses function_acl_drift on a release-target run, never PASS", async () => {
  const s = await prefixDatabase();
  const fn = "sessions.clean_root_commit(text)";
  expect(RUNTIME_APPLICATION_FUNCTIONS).toContain(fn);
  expect((await s.db.query<{ sig: string }>(`SELECT oid::regprocedure::text AS sig FROM pg_proc WHERE oid = to_regprocedure('${fn}')`)).rows).toHaveLength(1);
  // Not excused by the held scope: no held step creates it.
  expect(createdSessionsFunctions(await heldSqlOf(s.split.held)).has(fn)).toBe(false);
  await s.db.exec(`DROP FUNCTION ${fn}`);
  const err = await refusal(runTarget(s.db, s));
  expect(err?.code).toBe("function_acl_drift");
  expect(err?.message).toContain("runtime application functions missing from the schema");
  expect(err?.message).toContain(fn);
  expect((await marker(s.db)).some((m) => m.status === "pass")).toBe(false);
});

test("real tree: after the full tree including 062000, a dropped coordination advisories worker function refuses function_acl_drift on the next run", async () => {
  const s = await prefixDatabase();
  await runTarget(s.db, s);
  const CA = "upstream:20261105062000_coordination_advisories.sql";
  expect(s.split.held).toContain(CA);
  const last = s.split.held.at(-1)!;
  const tailSplit = migrationTargetSplit(s.names.map((name) => ({ name, pending: s.split.held.includes(name) })), last);
  const full: ExpectedSchema = { ...(await expectedSchema()), dbMode: "selfhosted" };
  await runMigrations(client(s.db), { profile: SELFHOSTED, target: last, expectedPending: tailSplit.expectedPending, targetGeneration: full.generation });
  expect(await ledger(s.db)).toContain(CA);
  expect(await marker(s.db)).toEqual([{ generation: full.generation, status: "pass" }]);
  const fn = "sessions.coordination_advisories_sweep_v1()";
  expect(RUNTIME_APPLICATION_FUNCTIONS).toContain(fn);
  await s.db.exec(`DROP FUNCTION ${fn}`);
  // Nothing is held now, so nothing excuses the absent function.
  const err = await refusal(runMigrations(client(s.db), { profile: SELFHOSTED }));
  expect(err?.code).toBe("function_acl_drift");
  expect(err?.message).toContain("runtime application functions missing from the schema");
  expect(err?.message).toContain(fn);
  expect((await marker(s.db)).some((m) => m.status === "pass")).toBe(false);
});

test("real tree: the live gate excuses only absent signatures a held, unledgered step creates", async () => {
  const s = await prefixDatabase();
  await runTarget(s.db, s);
  const full: ExpectedSchema = { ...(await expectedSchema()), dbMode: "selfhosted" };
  const held = await heldScope(s.split.held, s.binding.manifest.steps);
  const prefix: ExpectedSchema = { ...full, names: full.names.filter((n) => !s.split.held.includes(n)), generation: s.independent, held };
  expect(await verdict(s.db, prefix)).toEqual({ ok: true });
  const absentOk = new Set(held.flatMap((h) => h.creates));
  const gate = async (consumer: readonly string[]) => {
    await s.db.exec("SET ROLE history_rt");
    try {
      return await failure(s.db.exec(sessionsFunctionGateSql(absentOk, { runtime: RUNTIME_APPLICATION_FUNCTIONS, consumer })));
    } finally {
      await s.db.exec("RESET ROLE");
    }
  };
  expect(await gate(CONSUMER_RUNTIME_FUNCTIONS)).toBeNull();
  // A typo in an allowlist entry is never excused.
  expect(await gate([...CONSUMER_RUNTIME_FUNCTIONS, "sessions.ingest_operator_runtime_upsrt(jsonb,text,text)"])).toBe("42883");
  // Nor a wrong overload of a held step's function (right name, other argument types).
  expect(await gate([...CONSUMER_RUNTIME_FUNCTIONS, "sessions.ingest_operator_runtime_upsert(jsonb,text)"])).toBe("42883");
  // A held entry the ledger has excuses nothing: scoped to a ledgered step only, the
  // gate stays strict for the real (unledgered, unscoped) held steps.
  const ledgered = prefix.names.filter((n) => n.startsWith("upstream:")).at(-1)!;
  const stale: ExpectedSchema = { ...prefix, names: prefix.names.filter((n) => n !== ledgered), held: await heldScope([ledgered]) };
  expect(await verdict(s.db, stale)).toEqual({ ok: false, reason: "role_validation_failed", error_code: "42883" });
  // A held name that is also one of the image's steps is a mismatch, refused.
  expect(await verdict(s.db, { ...prefix, held: await heldScope([...s.split.held, ledgered]) }))
    .toEqual({ ok: false, reason: "role_validation_failed", error_code: "release_target_held_mismatch" });
  // ... as is a held name that the image's step digests do not know.
  const { [s.split.held[0]!]: _dropped, ...shas } = prefix.shas!;
  expect(await verdict(s.db, { ...prefix, shas }))
    .toEqual({ ok: false, reason: "role_validation_failed", error_code: "release_target_held_mismatch" });
  // Held list vs the bound manifest: one step fewer, or a manifest that is not the prefix, refuses.
  expect(await failure(heldScope(s.split.held.slice(1), s.binding.manifest.steps))).toBe("release_target_manifest_changed");
  expect(await failure(heldScope(s.split.held, s.binding.manifest.steps.slice(0, -1)))).toBe("release_target_manifest_changed");
  expect(await failure(heldScope(s.split.held, s.binding.manifest.steps.map(([n], i) => [n, i ? "0".repeat(64) : "f".repeat(64)] as [string, string]))))
    .toBe("release_target_manifest_changed");
  expect(await failure(heldScope([...s.split.held, "upstream:20990101000000_not_in_tree.sql"]))).toBe("release_target_unknown");
  // A function an applied prefix step owns is never excused, even when a held step also
  // (re)creates it: missing from the applied prefix, the gate raises 42883.
  const prefixOwned = "sessions.output_facts_state(text,text,text,text[],text)";
  expect(held.flatMap((h) => h.creates)).not.toContain(prefixOwned);
  await s.db.exec(`DROP FUNCTION ${prefixOwned}`);
  expect(await verdict(s.db, prefix)).toEqual({ ok: false, reason: "role_validation_failed", error_code: "42883" });
});

test("real tree: schemaVerdict trusts only a heldScope() map; forged, copied or mutated scopes are refused before any SQL", async () => {
  const s = await prefixDatabase();
  await runTarget(s.db, s);
  const full: ExpectedSchema = { ...(await expectedSchema()), dbMode: "selfhosted" };
  const held = await heldScope(s.split.held, s.binding.manifest.steps);
  const prefix: ExpectedSchema = { ...full, names: full.names.filter((n) => !s.split.held.includes(n)), generation: s.independent, held };
  const untrusted = { ok: false, reason: "role_validation_failed", error_code: "release_target_held_untrusted" };
  const runtime = held.find((h) => h.name === ERASE_RUNTIME)!;
  expect(runtime.creates).toContain(RUNTIME_UPSERT);
  // Deep-frozen: the array, each entry and each creates list.
  expect(Object.isFrozen(held)).toBe(true);
  expect(held.every((h) => Object.isFrozen(h) && Object.isFrozen(h.creates))).toBe(true);
  const prefixOwned = "sessions.output_facts_state(text,text,text,text[],text)";
  // Mutation of a legit scope throws (ES modules are strict) and changes nothing.
  expect(() => (runtime.creates as string[]).push(prefixOwned)).toThrow();
  expect(() => { (runtime as { creates: readonly string[] }).creates = [prefixOwned]; }).toThrow();
  expect(() => (held as { name: string; creates: string[] }[]).push({ name: ERASE_RUNTIME, creates: [prefixOwned] })).toThrow();
  expect(held.flatMap((h) => h.creates)).not.toContain(prefixOwned);
  expect(await verdict(s.db, prefix)).toEqual({ ok: true });
  // Copies with the same content are refused, before any SQL (the transaction never opens).
  const plain = held.map((h) => ({ name: h.name, creates: [...h.creates] }));
  let opened = 0;
  const counting: Transact = async () => { opened++; throw new Error("transaction must not open"); };
  for (const copy of [plain, [...held], structuredClone(held), held.slice(), JSON.parse(JSON.stringify(held)), new Proxy(held, {})]) {
    expect(await schemaVerdict({ ...prefix, held: copy }, counting)).toEqual(untrusted);
    expect(await verdict(s.db, { ...prefix, held: copy })).toEqual(untrusted);
  }
  expect(await schemaVerdict({ ...prefix, held: [] }, counting)).toEqual(untrusted);
  expect(opened).toBe(0);
  // verify-replica's probe: the prefix-owned function dropped, the legit scope is strict
  // (42883); the same map with that signature appended to the held 047010 entry's
  // creates is refused, never ok:true.
  await s.db.exec(`DROP FUNCTION ${prefixOwned}`);
  expect(await verdict(s.db, prefix)).toEqual({ ok: false, reason: "role_validation_failed", error_code: "42883" });
  const stages = "upstream:20261105047010_buffered_exact_session_stages.sql";
  expect(held.some((h) => h.name === stages)).toBe(true);
  const forged = held.map((h) => ({ name: h.name, creates: h.name === stages ? [...h.creates, prefixOwned] : [...h.creates] }));
  expect(await verdict(s.db, { ...prefix, held: forged })).toEqual(untrusted);
  // A fresh array of registered entries is not a scope either.
  expect(await verdict(s.db, { ...prefix, held: [...held.filter((h) => h !== runtime), runtime] })).toEqual(untrusted);
  // Without a held scope the gate stays strict.
  const { held: _held, ...unscoped } = prefix;
  expect(await verdict(s.db, unscoped)).toEqual({ ok: false, reason: "role_validation_failed", error_code: "42883" });
});

test("real tree: schemaVerdict reads `expected` once; a property swapped after the first SQL await changes nothing", async () => {
  const s = await prefixDatabase();
  await runTarget(s.db, s);
  const full: ExpectedSchema = { ...(await expectedSchema()), dbMode: "selfhosted" };
  const held = await heldScope(s.split.held, s.binding.manifest.steps);
  const prefix: ExpectedSchema = { ...full, names: full.names.filter((n) => !s.split.held.includes(n)), generation: s.independent, held };
  // The caller's object is mutated in place once the FIRST awaited statement has
  // returned, i.e. after the provenance check and before any read of the swapped value
  // that a post-await re-read would see.
  const swapped = async (swap: (e: ExpectedSchema) => void) => {
    const expected: ExpectedSchema = { ...prefix, names: [...prefix.names], shas: { ...prefix.shas } };
    let statements = 0;
    const racing: Transact = (work) =>
      transact(s.db)((tx) =>
        work({
          ...tx,
          query: (async (text: string, params?: unknown[]) => {
            const r = await tx.query(text, params as never);
            if (++statements === 1) swap(expected);
            return r;
          }) as typeof tx.query,
        }),
      );
    await s.db.exec("SET ROLE history_rt");
    try {
      return await schemaVerdict(expected, racing);
    } finally {
      await s.db.exec("RESET ROLE");
      expect(statements).toBeGreaterThan(1);
    }
  };
  expect(await verdict(s.db, prefix)).toEqual({ ok: true });
  // Swapped to undefined: the captured legit scope still applies, ok:true (a re-read
  // would make the gate strict, 42883, for the absent held functions).
  expect(await swapped((e) => { e.held = undefined; })).toEqual({ ok: true });
  // Swapped to a different registered scope (one held name is also an image step, a
  // re-read would be release_target_held_mismatch): the captured scope applies, ok:true.
  const ledgered = prefix.names.filter((n) => n.startsWith("upstream:")).at(-1)!;
  const other = await heldScope([...s.split.held, ledgered]);
  expect(await verdict(s.db, { ...prefix, held: other }))
    .toEqual({ ok: false, reason: "role_validation_failed", error_code: "release_target_held_mismatch" });
  expect(await swapped((e) => { e.held = other; })).toEqual({ ok: true });
  // The other gate inputs are captured the same way: a swapped generation (re-read:
  // validation_pending), names (re-read: schema_pending), shas without a held step
  // (re-read: release_target_held_mismatch) or dbMode change nothing.
  expect(await swapped((e) => { e.generation = full.generation; })).toEqual({ ok: true });
  expect(await swapped((e) => { e.names = full.names; })).toEqual({ ok: true });
  expect(await swapped((e) => { e.names.push(s.split.held[0]!); })).toEqual({ ok: true });
  const { [s.split.held[0]!]: _dropped, ...shas } = prefix.shas!;
  expect(await swapped((e) => { e.shas = shas; })).toEqual({ ok: true });
  expect(await swapped((e) => { delete e.shas![s.split.held[0]!]; })).toEqual({ ok: true });
  expect(await swapped((e) => { e.dbMode = "supabase"; })).toEqual({ ok: true });
  // The reviewer's probe: the prefix-owned function dropped, the legit scope is strict
  // (42883). Swapping `held` to an unregistered copy that appends that signature to the
  // held 047010 entry's creates still yields the captured scope's 42883, never ok:true.
  const prefixOwned = "sessions.output_facts_state(text,text,text,text[],text)";
  await s.db.exec(`DROP FUNCTION ${prefixOwned}`);
  const strict = { ok: false, reason: "role_validation_failed", error_code: "42883" };
  expect(await verdict(s.db, prefix)).toEqual(strict);
  const stages = "upstream:20261105047010_buffered_exact_session_stages.sql";
  const forged = held.map((h) => ({ name: h.name, creates: h.name === stages ? [...h.creates, prefixOwned] : [...h.creates] }));
  expect(forged.flatMap((h) => h.creates)).toContain(prefixOwned);
  expect(await swapped((e) => { e.held = forged; })).toEqual(strict);
  // Undefined and the other registered scope: the captured scope, still 42883.
  expect(await swapped((e) => { e.held = undefined; })).toEqual(strict);
  expect(await swapped((e) => { e.held = other; })).toEqual(strict);
});
