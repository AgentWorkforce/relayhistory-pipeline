/**
 * The bootstrap stage (runBootstrapStage) driven with a fake client that records every
 * query and end(): proves, query by query, that a refusal or error never lets chain SQL
 * reach the client outside BEGIN..ROLLBACK, that COMMIT is issued at most once and only
 * after the readback verdict, and that the lock is released before the client ends.
 * No database is touched.
 */
import { test, expect } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import {
  BOOTSTRAP_LOCK,
  BOOTSTRAP_UNLOCK,
  bootstrapChainRoles,
  bootstrapSection,
  bootstrapTargetChain,
  requireBootstrapRoles,
  requireBootstrapTargetChain,
  runBootstrapStage,
  stripPsqlMeta,
  type BootstrapEdge,
  type BootstrapSection,
  type BootstrapState,
} from "../schema-release-bootstrap.ts";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
import { SESSION_BOOTSTRAP_CONSUMERS } from "../session-bootstrap-consumers.ts";

const repo = new URL("../../../", import.meta.url);
const realBytes = (file: string) => readFileSync(new URL(file, repo));
const admit = "bulls96_analysis_admit";
const admitFile = bootstrapChainRoles().find((r) => r.name === admit)!.file;
const chainNames = bootstrapChainRoles().map((r) => r.name);

const role = (rolname: string, o: Record<string, unknown> = {}) => ({
  rolname,
  rolsuper: false,
  rolinherit: false,
  rolcreaterole: false,
  rolcreatedb: false,
  rolcanlogin: false,
  rolreplication: false,
  rolbypassrls: false,
  rolconnlimit: -1,
  rolvaliduntil: null,
  ...o,
});
const creatorEdge = (r: string): BootstrapEdge => ({
  role: r,
  member: "postgres",
  member_inherit: true,
  grantor: "supabase_admin",
  grantor_super: true,
  admin_option: true,
  inherit_option: false,
  set_option: false,
});
const workerSetting = {
  role: "session_directory_search_worker",
  global: true,
  kind: "statement_timeout=2s" as const,
  database: "0",
  digest: "d".repeat(64),
};
function state(absent: string[] = [], o: Partial<BootstrapState> = {}): BootstrapState {
  return {
    serverVersionNum: 170004,
    roles: [
      role("postgres", { rolcreaterole: true, rolcanlogin: true, rolbypassrls: true }),
      role("supabase_admin", { rolsuper: true, rolcanlogin: true }),
      role("authenticator", { rolcanlogin: true }),
      ...chainNames
        .filter((n) => !absent.includes(n))
        .map((n) => role(n, n === "bulls96_read_api" ? { rolcanlogin: true } : {})),
    ],
    edges: [],
    settings: [workerSetting],
    ...o,
  };
}
/** Anchor and release bytes: real HEAD bytes unless overridden (`release` overrides only the release side). */
const files = (o: Record<string, string> = {}, release: Record<string, string | undefined> = {}) =>
  SESSION_BOOTSTRAP_FILES.map((file) => {
    const bytes = file in o ? Buffer.from(o[file]) : realBytes(file);
    const rel = file in release ? (release[file] === undefined ? undefined : Buffer.from(release[file]!)) : bytes;
    return { file, release: rel, target: bytes };
  });
/** The anchor's ordered schema steps: the HEAD upstream migrations (fixture, no database). */
const STEPS = readdirSync(new URL("supabase/migrations/", repo))
  .filter((f) => f.endsWith(".sql"))
  .sort()
  .map((f) => `upstream:${f}`);
const T047 = "upstream:20261105047000_coordination_fact_readers.sql";
const T_MID = "upstream:20261105045040_selfhost_output_facts.sql"; // between 045000 and 046000
const T_LOW = "upstream:20261105033000_canonical_project_events.sql"; // below 034000
const T_EARLY = "upstream:20261104020100_auth_session_verify.sql"; // below every multi-role file
const TSHA = "3".repeat(40);
const readApiFile = "supabase/bootstrap/session_read_api_roles.sql";
const BEFORE = state([admit]);
const AFTER = state([], { edges: [creatorEdge(admit)] });

const chainSql = (sql: string) => /CREATE\s+ROLE|\bALTER\s|\bGRANT\s|\bDO\b/i.test(sql);
interface Script {
  before?: BootstrapState;
  after?: BootstrapState;
  xids?: string[];
  /** A query equal to this text (or, when longer than 8 characters, containing it) throws. */
  throwOn?: string;
}
function fakeClient(s: Script = {}) {
  const log: string[] = [];
  let ran = false;
  const xids = [...(s.xids ?? ["777", "777"])];
  const client = {
    async query(sql: string, _params?: unknown[]) {
      log.push(sql);
      if (s.throwOn && (sql === s.throwOn || (s.throwOn.length > 8 && sql.includes(s.throwOn))))
        throw new Error("boom");
      // Chain text (which itself mentions catalogs) flips the fake catalog to `after`.
      if (chainSql(sql)) {
        ran = true;
        return { rows: [] };
      }
      const st = ran ? (s.after ?? AFTER) : (s.before ?? BEFORE);
      if (sql.includes("server_version_num")) return { rows: [{ v: st.serverVersionNum }] };
      if (sql.includes("FROM pg_roles ORDER BY rolname")) return { rows: st.roles };
      if (sql.includes("FROM pg_auth_members")) return { rows: st.edges };
      if (sql.includes("FROM pg_db_role_setting")) return { rows: st.settings };
      if (sql.includes("txid_current")) return { rows: [{ x: xids.shift() }] };
      return { rows: [] };
    },
    async end() {
      log.push("<end>");
    },
  };
  return { client, log };
}

/** No chain SQL outside BEGIN..ROLLBACK/COMMIT; at most one COMMIT. */
function expectContained(log: string[]) {
  let inTx = false;
  for (const q of log) {
    if (q === "BEGIN") inTx = true;
    else if (q === "ROLLBACK" || q === "COMMIT") inTx = false;
    else if (chainSql(q)) expect({ outside: q.slice(0, 60), inTx }).toEqual({ outside: q.slice(0, 60), inTx: true });
  }
  expect(log.filter((q) => q === "COMMIT").length).toBeLessThanOrEqual(1);
  expect(log.filter((q) => q === "BEGIN").length).toBeLessThanOrEqual(1);
  expect(log.at(-1)).toBe("<end>");
  expect(log.filter((q) => q === "<end>").length).toBe(1);
}
const control = (log: string[]) =>
  log.filter((q) => ["BEGIN", "ROLLBACK", "COMMIT", BOOTSTRAP_LOCK, BOOTSTRAP_UNLOCK, "<end>"].includes(q) || chainSql(q))
    .map((q) => (chainSql(q) ? "<chain>" : q));

const plan = (f = files(), st = BEFORE, target = T047, steps = STEPS) =>
  bootstrapSection({ target, targetSha: TSHA, steps, files: f, state: st, supabaseMode: true, log: () => {} });
async function run(
  s: Script,
  o: {
    planned?: BootstrapSection;
    files?: ReturnType<typeof files>;
    steps?: string[];
    target?: string;
    targetSha?: string;
    approvalChain?: string;
    underLock?: () => Promise<{ files: ReturnType<typeof files>; steps: string[] }>;
  } = {},
) {
  const planned = o.planned ?? plan();
  const { client, log } = fakeClient(s);
  const evidence: Record<string, unknown>[] = [];
  const lines: string[] = [];
  const err = console.error;
  console.error = process.env.STAGE_DEBUG ? err : () => {};
  let code: string | undefined;
  let result: string | undefined;
  try {
    result = await runBootstrapStage({
      client,
      target: o.target ?? planned.target,
      targetSha: o.targetSha ?? planned.target_sha,
      planned,
      approvalChain: o.approvalChain ?? planned.chain_digest,
      supabaseMode: true,
      underLock: o.underLock ?? (async () => ({ files: o.files ?? files(), steps: o.steps ?? STEPS })),
      writeEvidence: async (e) => {
        evidence.push(e);
      },
      header: { version: 1 },
      log: (l) => lines.push(l),
    });
  } catch (e) {
    code = (e as { code?: string }).code ?? (e as Error).message;
  } finally {
    console.error = err;
  }
  expectContained(log);
  return { log, evidence, lines, code, result };
}
const refused = [BOOTSTRAP_LOCK, "BEGIN", "ROLLBACK", BOOTSTRAP_UNLOCK, "<end>"];
/** Refused under the lock before BEGIN: no transaction, no catalog read, unlock and end. */
const underLockRefused = [BOOTSTRAP_LOCK, BOOTSTRAP_UNLOCK, "<end>"];

test("stage success: chain SQL in order inside the transaction, COMMIT once after the readback, then unlock and end", async () => {
  const r = await run({});
  expect(r.code).toBeUndefined();
  expect(r.result).toBe("verified");
  const expected = stripPsqlMeta(realBytes(admitFile).toString("utf8"));
  const begin = r.log.indexOf("BEGIN");
  const chainAt = r.log.indexOf(expected);
  const lastReadback = r.log.findLastIndex((q) => q.includes("FROM pg_db_role_setting"));
  const commit = r.log.indexOf("COMMIT");
  expect(begin).toBe(1);
  expect(r.log[0]).toBe(BOOTSTRAP_LOCK);
  expect(chainAt).toBeGreaterThan(begin);
  expect(lastReadback).toBeGreaterThan(chainAt);
  expect(commit).toBeGreaterThan(lastReadback);
  expect(r.log.slice(commit + 1)).toEqual([BOOTSTRAP_UNLOCK, "<end>"]);
  expect(r.log).not.toContain("ROLLBACK");
  // Exactly the one file provisioning the absent role ran.
  expect(r.log.filter(chainSql)).toEqual([expected]);
  expect(r.evidence).toHaveLength(1);
  expect(r.evidence[0]).toMatchObject({
    version: 1,
    ran: [admitFile],
    created: [admit],
    target: T047,
    target_sha: TSHA,
    required_roles: [...chainNames].sort(),
    excluded: [],
  });
  expect(JSON.stringify(r.evidence)).not.toContain("d".repeat(64));
  expect(r.lines).toEqual(["release_bootstrap_created 1", "release_bootstrap_verified"]);
});

test("stage skip: nothing absent rolls back, runs no chain SQL, writes skipped evidence", async () => {
  const r = await run({ before: state() }, { planned: plan(files(), state()) });
  expect(r.result).toBe("skipped");
  expect(control(r.log)).toEqual(refused);
  expect(r.evidence).toHaveLength(1);
  expect(r.evidence[0]).toMatchObject({ skipped: true, created: [] });
  expect(r.lines).toEqual(["release_bootstrap_skipped"]);
});

test("stage refusals roll back, never commit, unlock and end, with no chain SQL outside the transaction", async () => {
  // Where the refusal lands: "lock" before BEGIN (git-side recompute under the lock),
  // "tx" inside the transaction before chain SQL, "chain" after chain SQL ran.
  const cases: [string, Script, Parameters<typeof run>[1], string, "lock" | "tx" | "chain"][] = [
    // The plan saw a different absent set.
    ["plan changed", {}, { planned: plan(files(), state([admit, "bulls96_read_api"])) }, "release_bootstrap_plan_changed", "tx"],
    // The anchor bytes changed since the plan: refused under the lock, before BEGIN.
    [
      "plan changed (bytes)",
      {},
      { files: files({ [admitFile]: realBytes(admitFile).toString("utf8") + "\n-- other\n" }) },
      "release_bootstrap_plan_changed",
      "lock",
    ],
    // A present chain role drifted from its contract.
    [
      "role drift",
      { before: state([admit], { roles: [...state([admit]).roles.filter((x) => x.rolname !== "bulls96_read_api"), role("bulls96_read_api", { rolsuper: true })] }) },
      {},
      "release_bootstrap_role_drift",
      "tx",
    ],
    // The verdict found a side effect (an unrelated role altered).
    [
      "readback mismatch",
      { after: { ...AFTER, roles: AFTER.roles.map((x) => (x.rolname === "authenticator" ? { ...x, rolbypassrls: true } : x)) } },
      {},
      "release_bootstrap_readback",
      "chain",
    ],
    // A chain file ended the transaction: a new txid.
    ["txid changed", { xids: ["777", "778"] }, {}, "release_bootstrap_readback", "chain"],
    // The chain SQL itself errors.
    ["sql error", { throwOn: "CREATE ROLE" }, {}, "boom", "chain"],
  ];
  for (const [name, script, o, code, at] of cases) {
    const r = await run(script, o);
    expect({ name, code: r.code }).toEqual({ name, code });
    expect({ name, control: control(r.log) }).toEqual({
      name,
      control:
        at === "chain"
          ? [BOOTSTRAP_LOCK, "BEGIN", "<chain>", "ROLLBACK", BOOTSTRAP_UNLOCK, "<end>"]
          : at === "tx"
            ? refused
            : underLockRefused,
    });
    expect({ name, evidence: r.evidence }).toEqual({ name, evidence: [] });
  }
});

test("stage chain_unsafe: a top-level END in a chain file is refused before any chain SQL is sent", async () => {
  const unsafe = files({ [admitFile]: realBytes(admitFile).toString("utf8") + "\nDO $$ BEGIN PERFORM 1; END $$;\nEND;\n" });
  const r = await run({}, { files: unsafe, planned: plan(unsafe) });
  expect(r.code).toBe("release_bootstrap_chain_unsafe");
  expect(control(r.log)).toEqual(refused);
  expect(r.log.some(chainSql)).toBe(false);
  expect(r.log.some((q) => q.includes("txid_current"))).toBe(false);
  expect(r.evidence).toEqual([]);
});

test("stage underLock refusal: no transaction, unlock and end; a lock failure ends without unlocking", async () => {
  const gate = await run({}, { underLock: async () => { throw Object.assign(new Error("gate"), { code: "release_github_gate" }); } });
  expect(gate.code).toBe("release_github_gate");
  expect(gate.log).toEqual([BOOTSTRAP_LOCK, BOOTSTRAP_UNLOCK, "<end>"]);
  const lock = await run({ throwOn: "pg_advisory_lock" });
  expect(lock.code).toBe("boom");
  expect(lock.log).toEqual([BOOTSTRAP_LOCK, "<end>"]);
  // A failing BEGIN (no transaction): unlock and end, nothing else.
  const begin = await run({ throwOn: "BEGIN" });
  expect(begin.code).toBe("boom");
  expect(begin.log).toEqual([BOOTSTRAP_LOCK, "BEGIN", BOOTSTRAP_UNLOCK, "<end>"]);
});

test("stage COMMIT failure: the error propagates, evidence is not written, unlock and end", async () => {
  const r = await run({ throwOn: "COMMIT" });
  expect(r.code).toBe("boom");
  expect(r.evidence).toEqual([]);
  expect(r.log.slice(-3)).toEqual(["ROLLBACK", BOOTSTRAP_UNLOCK, "<end>"]);
});

// ---- Target-bound bootstrap (DB owner ruling, point 7) ----

const code = (fn: () => unknown) => {
  try {
    fn();
    return "no refusal";
  } catch (e) {
    return (e as { code?: string }).code;
  }
};
/** Zero mutation: no chain SQL sent, no COMMIT, no evidence. */
function expectNoMutation(r: Awaited<ReturnType<typeof run>>) {
  expect(r.log.some(chainSql)).toBe(false);
  expect(r.log).not.toContain("COMMIT");
  expect(r.evidence).toEqual([]);
}
const ingestFile = "supabase/bootstrap/session_ingest_roles.sql";
const consumerFile = "supabase/bootstrap/session_consumer_roles.sql";
const workerFile = "supabase/bootstrap/session_directory_search_worker.sql";
const verifierFile = "supabase/bootstrap/session_token_verifier.sql";
const verifier = "session_token_verifier";
const aboveEarly = chainNames.filter((n) => n !== verifier && n !== "session_directory_search_worker");

test("negative: a role above the target is not required, not created and not drift-checked", async () => {
  // Every role first consumed above the target is absent (or drifted); none is touched.
  const before = state(aboveEarly.filter((n) => n !== "bulls96_read_api"), {
    roles: [
      ...state(aboveEarly).roles,
      role("bulls96_read_api", { rolsuper: true }),
    ],
  });
  const planned = plan(files(), before, T_EARLY);
  expect(planned.required_roles).toEqual(["session_directory_search_worker", verifier]);
  expect(planned.will_create).toEqual([]);
  expect(planned.excluded).toEqual([ingestFile, readApiFile, consumerFile]);
  for (const r of planned.roles.filter((x) => aboveEarly.includes(x.name)))
    expect({ n: r.name, required: r.required, will_create: r.will_create }).toEqual({ n: r.name, required: false, will_create: false });
  const r = await run({ before }, { planned });
  expect(r.code).toBeUndefined();
  expect(r.result).toBe("skipped");
  expect(control(r.log)).toEqual(refused);
  expect(r.log.some(chainSql)).toBe(false);
  expect(r.evidence[0]).toMatchObject({ skipped: true, created: [], excluded: [ingestFile, readApiFile, consumerFile] });
  // The apply guard for this target checks only its required roles.
  requireBootstrapRoles(before, planned.required_roles, () => {});
  expect(code(() => requireBootstrapRoles(before, chainNames, () => {}))).toBe("release_bootstrap_required");
});

test("negative: an unmapped, duplicate or conflicting manifest entry refuses (no client reached)", () => {
  const base = [...SESSION_BOOTSTRAP_CONSUMERS];
  const section = (consumers: (readonly [string, string])[]) => () =>
    bootstrapSection({ target: T047, targetSha: TSHA, steps: STEPS, files: files(), state: BEFORE, supabaseMode: true, consumers, log: () => {} });
  expect(code(section(base.slice(1)))).toBe("release_bootstrap_manifest");
  expect(code(section([...base, base[3]]))).toBe("release_bootstrap_manifest");
  expect(code(section([...base, [base[3][0], "20261105047000_coordination_fact_readers.sql"]]))).toBe("release_bootstrap_manifest");
  expect(code(section(base))).toBe("no refusal");
});

test("negative: a consumer missing from the anchor's steps up to the target refuses under the lock", async () => {
  const missing = STEPS.filter((s) => s !== "upstream:20261105034000_session_directory_read_api.sql");
  expect(code(() => plan(files(), BEFORE, T047, missing))).toBe("release_bootstrap_manifest");
  const r = await run({}, { steps: missing });
  expect(r.code).toBe("release_bootstrap_manifest");
  expect(r.log).toEqual(underLockRefused);
  expectNoMutation(r);
  // The target itself missing from the anchor.
  const noTarget = await run({}, { steps: STEPS.filter((s) => s !== T047) });
  expect(noTarget.code).toBe("release_bootstrap_manifest");
  expect(noTarget.log).toEqual(underLockRefused);
});

test("negative + positive: a target between 045000 and 046000 makes session_read_api_roles.sql mixed; the stage refuses it", async () => {
  const planned = plan(files(), BEFORE, T_MID);
  expect(planned.mixed).toEqual([readApiFile]);
  expect(planned.files.find((f) => f.file === readApiFile)!.scope).toBe("mixed");
  expect(planned.required_roles).not.toContain(admit);
  expect(planned.required_roles).toContain("bulls96_digest_narrative");
  expect(planned.required_roles).toContain("bulls96_read_api");
  expect(planned.will_create).toEqual([]);
  const r = await run({}, { planned });
  expect(r.code).toBe("release_bootstrap_chain_mixed");
  expect(r.log).toEqual(underLockRefused);
  expectNoMutation(r);
  // Even with a required role absent, the mixed file never runs (DO blocks are not split).
  const absent = state(["bulls96_read_api"]);
  const p2 = plan(files(), absent, T_MID);
  expect(p2.will_create).toEqual(["bulls96_read_api"]);
  const r2 = await run({ before: absent }, { planned: p2 });
  expect(r2.code).toBe("release_bootstrap_chain_mixed");
  expectNoMutation(r2);
});

test("negative: release bytes that differ from the anchor's for a running file refuse; an excluded file's drift does not", async () => {
  const drift = files({}, { [admitFile]: realBytes(admitFile).toString("utf8") + "\n-- release edit\n" });
  const planned = plan(drift);
  expect(planned.release_drift).toEqual([admitFile]);
  expect(planned.files.find((f) => f.file === admitFile)!.differs).toBe(true);
  // The CHAIN digest is over the anchor bytes: identical to the no-drift plan.
  expect(planned.chain_digest).toBe(plan().chain_digest);
  const r = await run({}, { planned, files: drift });
  expect(r.code).toBe("release_bootstrap_release_drift");
  expect(r.log).toEqual(underLockRefused);
  expectNoMutation(r);
  // Release-side absence of a running file is drift too.
  const gone = files({}, { [admitFile]: undefined });
  const r2 = await run({}, { planned: plan(gone), files: gone });
  expect(r2.code).toBe("release_bootstrap_release_drift");
  expectNoMutation(r2);
  // At an early target the read-api file is excluded: its release drift is reported nowhere and runs nothing.
  const early = files({}, { [readApiFile]: "-- changed\n" });
  const p3 = plan(early, BEFORE, T_EARLY);
  expect(p3.release_drift).toEqual([]);
  const r3 = await run({}, { planned: p3, files: early });
  expect(r3.result).toBe("skipped");
});

test("negative: target or manifest drift before connecting refuses (pure git-side binding)", () => {
  const planned = plan();
  const chain = (target = T047, targetSha = TSHA, steps = STEPS, f = files()) =>
    bootstrapTargetChain({ target, targetSha, steps, files: f });
  expect(requireBootstrapTargetChain(chain(), planned, planned.chain_digest)).toBe(planned.chain_digest);
  // Another target, another target sha, an approval CHAIN that does not match.
  expect(code(() => requireBootstrapTargetChain(chain(T_MID), planned, planned.chain_digest))).toBe("release_bootstrap_plan_changed");
  expect(code(() => requireBootstrapTargetChain(chain(T047, "4".repeat(40)), planned, planned.chain_digest))).toBe("release_bootstrap_plan_changed");
  expect(code(() => requireBootstrapTargetChain(chain(), planned, "0".repeat(64)))).toBe("release_bootstrap_plan_changed");
  // The anchor's bytes changed since the plan.
  const edited = files({ [admitFile]: realBytes(admitFile).toString("utf8") + "\n-- edit\n" });
  expect(code(() => requireBootstrapTargetChain(chain(T047, TSHA, STEPS, edited), planned, planned.chain_digest))).toBe("release_bootstrap_plan_changed");
  // A plan whose required set was tampered with no longer matches.
  const tampered = { ...planned, required_roles: planned.required_roles.filter((n) => n !== admit) };
  expect(code(() => requireBootstrapTargetChain(chain(), tampered, planned.chain_digest))).toBe("release_bootstrap_plan_changed");
  // The anchor no longer carries a required consumer.
  expect(code(() => chain(T047, TSHA, STEPS.filter((s) => !s.includes("20261105046000_"))))).toBe("release_bootstrap_manifest");
  // A different required set changes CHAIN (the set is folded into the digest).
  expect(plan(files(), BEFORE, T_LOW).chain_digest).not.toBe(planned.chain_digest);
});

test("negative: target, target_sha or approval drift under the lock refuses before BEGIN", async () => {
  const cases: [string, Parameters<typeof run>[1], string][] = [
    ["target", { target: T_MID }, "release_bootstrap_plan_changed"],
    ["target_sha", { targetSha: "4".repeat(40) }, "release_bootstrap_plan_changed"],
    ["approval chain", { approvalChain: "0".repeat(64) }, "release_bootstrap_plan_changed"],
    [
      "anchor bytes",
      { files: files({ [admitFile]: realBytes(admitFile).toString("utf8") + "\n-- moved\n" }) },
      "release_bootstrap_plan_changed",
    ],
    ["anchor steps", { steps: STEPS.filter((s) => !s.includes("20261105045000_")) }, "release_bootstrap_manifest"],
    ["chain order", { files: [...files()].reverse() }, "release_bootstrap_chain"],
  ];
  for (const [name, o, want] of cases) {
    const r = await run({}, o);
    expect({ name, code: r.code }).toEqual({ name, code: want });
    expect({ name, log: r.log }).toEqual({ name, log: underLockRefused });
    expectNoMutation(r);
  }
});

test("positive: target 047000 requires all 14 roles and creates only the absent ones", async () => {
  const absent = [admit, "bulls96_read_api", verifier];
  const before = state(absent);
  // A freshly created bulls96_read_api is NOLOGIN (its operator file adds LOGIN after migrate).
  const fresh = state([], { edges: absent.map(creatorEdge) });
  const after = { ...fresh, roles: fresh.roles.map((x) => (x.rolname === "bulls96_read_api" ? { ...x, rolcanlogin: false } : x)) };
  const planned = plan(files(), before);
  expect(planned.required_roles).toEqual([...chainNames].sort());
  expect(planned.required_roles).toHaveLength(14);
  expect(planned.excluded).toEqual([]);
  expect(planned.mixed).toEqual([]);
  expect(planned.will_create).toEqual([...absent].sort());
  const r = await run({ before, after }, { planned });
  expect(r.code).toBeUndefined();
  expect(r.result).toBe("verified");
  expect(r.evidence[0]).toMatchObject({ ran: [verifierFile, readApiFile], created: [...absent].sort() });
  expect(r.log.filter(chainSql)).toEqual([verifierFile, readApiFile].map((f) => stripPsqlMeta(realBytes(f).toString("utf8"))));
});

test("positive: a target below 034000 excludes session_read_api_roles.sql whole", () => {
  const planned = plan(files(), state(), T_LOW);
  expect(planned.files.find((f) => f.file === readApiFile)!.scope).toBe("excluded");
  expect(planned.excluded).toContain(readApiFile);
  expect(planned.excluded).toContain(consumerFile);
  for (const r of planned.roles.filter((x) => x.file === readApiFile)) expect(r.required).toBe(false);
  expect(planned.required_roles).not.toContain("bulls96_read_api");
  // The excluded file's anchor bytes are not in CHAIN.
  const edited = files({ [readApiFile]: realBytes(readApiFile).toString("utf8") + "\n-- edit\n" });
  expect(plan(edited, state(), T_LOW).chain_digest).toBe(planned.chain_digest);
  // session_ingest_roles.sql spans 022000..043870, so at this target it is mixed.
  expect(planned.mixed).toEqual([ingestFile]);
  // An anchor without the excluded file (an older anchor) is accepted at an early target...
  const older = files().map((f) => (f.file === readApiFile ? { ...f, target: undefined, release: undefined } : f));
  expect(plan(older, state(), T_EARLY).files.find((f) => f.file === readApiFile)!.sha256_target).toBeNull();
  // ...and refused when one of its roles is required.
  expect(code(() => plan(older, state(), T047))).toBe("release_bootstrap_bytes_missing");
});

test("positive: an early target creates only its required absent role and runs only that file", async () => {
  const before = state([verifier, admit]);
  const after = state([admit], { edges: [creatorEdge(verifier)] });
  const planned = plan(files(), before, T_EARLY);
  expect(planned.will_create).toEqual([verifier]);
  const r = await run({ before, after }, { planned });
  expect(r.result).toBe("verified");
  expect(r.evidence[0]).toMatchObject({ ran: [verifierFile], created: [verifier] });
  expect(r.log.filter(chainSql)).toEqual([stripPsqlMeta(realBytes(verifierFile).toString("utf8"))]);
  expect(workerFile).toBe(SESSION_BOOTSTRAP_FILES[0]);
});
