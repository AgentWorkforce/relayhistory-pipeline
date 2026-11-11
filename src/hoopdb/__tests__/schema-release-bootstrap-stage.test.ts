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
  bootstrapContractCoverage,
  bootstrapContractDigests,
  bootstrapContractSource,
  bootstrapSection,
  bootstrapTargetChain,
  requireBootstrapRoles,
  requireBootstrapTargetChain,
  runBootstrapStage,
  stripPsqlMeta,
  type BootstrapAnchorContract,
  type BootstrapEdge,
  type BootstrapSection,
  type BootstrapState,
} from "../schema-release-bootstrap.ts";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
import { SESSION_BOOTSTRAP_ROLES } from "../session-bootstrap-roles.ts";
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

/** The anchor's contract modules: the HEAD module bytes (source "anchor"); null: an absent consumers module. */
const ROLES_MODULE_TEXT = realBytes("src/hoopdb/session-bootstrap-roles.ts").toString("utf8");
const CONSUMERS_MODULE_TEXT = realBytes("src/hoopdb/session-bootstrap-consumers.ts").toString("utf8");
const anchorContract = (rolesModule = ROLES_MODULE_TEXT, consumersModule: string | null = CONSUMERS_MODULE_TEXT): BootstrapAnchorContract => ({
  rolesModule,
  consumersModule: consumersModule ?? undefined,
});
/** A legacy anchor (Release A shape): neither a roles module nor a consumers module. */
const LEGACY: BootstrapAnchorContract = {
  rolesModule: undefined,
  consumersModule: undefined,
};

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

const plan = (f = files(), st = BEFORE, target = T047, steps = STEPS, contract = anchorContract()) =>
  bootstrapSection({ target, targetSha: TSHA, steps, files: f, contract, state: st, supabaseMode: true, log: () => {} });
async function run(
  s: Script,
  o: {
    planned?: BootstrapSection;
    files?: ReturnType<typeof files>;
    steps?: string[];
    target?: string;
    targetSha?: string;
    approvalChain?: string;
    /** The anchor contract modules the default underLock returns (default: HEAD's). */
    contract?: BootstrapAnchorContract;
    underLock?: () => Promise<{ files: ReturnType<typeof files>; steps: string[]; contract: BootstrapAnchorContract }>;
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
      underLock:
        o.underLock ??
        (async () => ({ files: o.files ?? files(), steps: o.steps ?? STEPS, contract: o.contract ?? anchorContract() })),
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
    bootstrapSection({ target: T047, targetSha: TSHA, steps: STEPS, files: files(), contract: LEGACY, state: BEFORE, supabaseMode: true, consumers, log: () => {} });
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
    bootstrapTargetChain({ target, targetSha, steps, files: f, contract: anchorContract() });
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

// ---- Contract binding (review finding P2-1; DB owner ruling on the contract digest) ----

/** The HEAD roles module text with `from` replaced by `to` (exactly once). */
const rolesText = (...edits: [string, string][]) =>
  edits.reduce((t, [from, to]) => {
    expect(t.split(from).length).toBe(2);
    return t.replace(from, to);
  }, ROLES_MODULE_TEXT);
const consumersText = (pairs: readonly (readonly [string, string])[]) =>
  "export const SESSION_BOOTSTRAP_CONSUMERS: readonly (readonly [role: string, firstConsumer: string])[] = [\n" +
  pairs.map(([r, f]) => `  [${JSON.stringify(r)}, ${JSON.stringify(f)}],\n`).join("") +
  "];\n";
const READ_API_LINE = 'bulls96_read_api: bare("allowed", "none"),';
/** Same SQL chain, different contract semantics: bulls96_read_api's inbound edge set. */
const SEMANTIC_DRIFT = rolesText([READ_API_LINE, 'bulls96_read_api: bare("allowed", "unchecked"),']);

test("contract: CHAIN folds the composite contract digest; a contract or manifest change with identical SQL changes CHAIN", () => {
  const d = bootstrapContractDigests();
  expect(d.contract_digest).toBe(
    new Bun.CryptoHasher("sha256")
      .update(`bootstrap-contract-v1\nmap ${d.contract_map_digest}\nmanifest ${d.contract_manifest_digest}\n`)
      .digest("hex"),
  );
  const planned = plan();
  expect(planned.contract_source).toBe("anchor");
  expect({ m: planned.contract_map_digest, n: planned.contract_manifest_digest, c: planned.contract_digest }).toEqual({
    m: d.contract_map_digest,
    n: d.contract_manifest_digest,
    c: d.contract_digest,
  });
  // Legacy anchor: the release's contract, the same digests and CHAIN (CHAIN binds the
  // contract content; the source is bound by the plan comparison, contract (e)).
  const legacy = plan(files(), BEFORE, T047, STEPS, LEGACY);
  expect(legacy.contract_source).toBe("release");
  expect(legacy.contract_digest).toBe(d.contract_digest);
  expect(legacy.chain_digest).toBe(planned.chain_digest);
  // (a) One contract field changed (same SQL bytes): the map digest and CHAIN change.
  const map = structuredClone(SESSION_BOOTSTRAP_ROLES) as Record<string, Record<string, { login: string; inbound: string; settings: string }>>;
  map[readApiFile].bulls96_read_api.inbound = "unchecked";
  const sec = (o: { map?: typeof SESSION_BOOTSTRAP_ROLES; consumers?: (readonly [string, string])[] }) =>
    bootstrapSection({ target: T047, targetSha: TSHA, steps: STEPS, files: files(), contract: LEGACY, state: BEFORE, supabaseMode: true, log: () => {}, ...o });
  const mapped = sec({ map: map as unknown as typeof SESSION_BOOTSTRAP_ROLES });
  expect(mapped.contract_map_digest).not.toBe(legacy.contract_map_digest);
  expect(mapped.contract_manifest_digest).toBe(legacy.contract_manifest_digest);
  expect(mapped.chain_digest).not.toBe(legacy.chain_digest);
  // (b) One manifest pair moved (still at or below the target): the manifest digest and CHAIN change.
  const consumers = SESSION_BOOTSTRAP_CONSUMERS.map(([r, f]): [string, string] =>
    r === admit ? [r, "20261105047000_coordination_fact_readers.sql"] : [r, f],
  );
  const moved = sec({ consumers });
  expect(moved.contract_manifest_digest).not.toBe(legacy.contract_manifest_digest);
  expect(moved.contract_map_digest).toBe(legacy.contract_map_digest);
  expect(moved.chain_digest).not.toBe(legacy.chain_digest);
  // The plan exposes digests only, never contract contents.
  const exposed = JSON.stringify([planned.contract_source, planned.contract_map_digest, planned.contract_manifest_digest, planned.contract_digest]);
  expect(exposed).not.toContain("worker-2s");
});

test("contract (a): partial anchor modules refuse release_bootstrap_contract_drift before connecting and under the lock", async () => {
  const partial: [string, BootstrapAnchorContract][] = [
    ["roles module without consumers module", anchorContract(ROLES_MODULE_TEXT, null)],
    ["consumers module without roles module", { rolesModule: undefined, consumersModule: CONSUMERS_MODULE_TEXT }],
    ["roles module without its SESSION_BOOTSTRAP_ROLES declaration", anchorContract(rolesText(["export const SESSION_BOOTSTRAP_ROLES:", "export const SESSION_BOOTSTRAP_ROLE_MAP:"]))],
    ["map that does not parse", anchorContract(rolesText([READ_API_LINE, "bulls96_read_api: bare(`allowed`, `none`),"]))],
    ["consumers that do not parse", anchorContract(ROLES_MODULE_TEXT, CONSUMERS_MODULE_TEXT.replace("];", "].map((x) => x);"))],
    ["bare redefined", anchorContract(rolesText(['settings: "unchecked" })', 'settings: "worker-2s" })']))],
    ["prototype key", anchorContract(rolesText([READ_API_LINE, READ_API_LINE + ' __proto__: bare("allowed", "none"),']))],
  ];
  for (const [name, contract] of partial) {
    expect({ name, pure: code(() => bootstrapContractSource(contract)) }).toEqual({ name, pure: "release_bootstrap_contract_drift" });
    expect({ name, plan: code(() => plan(files(), BEFORE, T047, STEPS, contract)) }).toEqual({ name, plan: "release_bootstrap_contract_drift" });
    const r = await run({}, { contract });
    expect({ name, code: r.code, log: r.log }).toEqual({ name, code: "release_bootstrap_contract_drift", log: underLockRefused });
    expectNoMutation(r);
  }
});

test("contract (b): semantic drift with an identical SQL chain refuses before connecting and under the lock", async () => {
  const drifted: [string, BootstrapAnchorContract][] = [
    ["map value", anchorContract(SEMANTIC_DRIFT)],
    [
      "manifest pair",
      anchorContract(
        ROLES_MODULE_TEXT,
        consumersText(SESSION_BOOTSTRAP_CONSUMERS.map(([r, f]) => (r === admit ? [r, "20261105047000_coordination_fact_readers.sql"] : [r, f]))),
      ),
    ],
  ];
  for (const [name, contract] of drifted) {
    // Same anchor SQL bytes (files()) in every case.
    expect({ name, pre: code(() => bootstrapTargetChain({ target: T047, targetSha: TSHA, steps: STEPS, files: files(), contract })) }).toEqual({
      name,
      pre: "release_bootstrap_contract_drift",
    });
    const r = await run({}, { contract });
    expect({ name, code: r.code, log: r.log }).toEqual({ name, code: "release_bootstrap_contract_drift", log: underLockRefused });
    expectNoMutation(r);
  }
});

test("contract (c): key order, role order, file order, pair order and bare vs explicit are equal (anchor, same CHAIN)", async () => {
  const workerFields = 'login: "never",\n      inbound: "worker",\n      settings: "worker-2s",';
  const ingestPair = '    bulls96_privacy_eraser: bare("allowed", "unchecked"),\n    bulls96_babysitter_retention: bare("allowed", "unchecked"),\n';
  const workerBlock = ROLES_MODULE_TEXT.slice(
    ROLES_MODULE_TEXT.indexOf('  "supabase/bootstrap/session_directory_search_worker.sql": {'),
    ROLES_MODULE_TEXT.indexOf('  "supabase/bootstrap/session_token_verifier.sql": {'),
  );
  // The worker file's block moves last (file order), then the field/role/bare edits.
  const end = ROLES_MODULE_TEXT.lastIndexOf("\n};");
  const moved = ROLES_MODULE_TEXT.slice(0, end + 1) + workerBlock + ROLES_MODULE_TEXT.slice(end + 1);
  const reordered = [
    [workerFields, 'settings: "worker-2s", inbound: "worker", login: "never"'],
    [ingestPair, '    bulls96_babysitter_retention: bare("allowed", "unchecked"),\n    bulls96_privacy_eraser: bare("allowed", "unchecked"),\n'],
    [READ_API_LINE, '/* explicit */ bulls96_read_api: { inbound: "none", settings: "unchecked", login: "allowed", },'],
  ].reduce((t, [from, to]) => {
    expect(t.split(from).length).toBe(2);
    return t.replace(from, to);
  }, moved.replace(workerBlock, ""));
  const key = (f: string) => reordered.indexOf(`  "supabase/bootstrap/${f}.sql": {`);
  expect(key("session_directory_search_worker")).toBeGreaterThan(key("session_consumer_roles"));
  expect(reordered).not.toBe(ROLES_MODULE_TEXT);
  const contract = anchorContract(reordered, consumersText([...SESSION_BOOTSTRAP_CONSUMERS].reverse()));
  expect(bootstrapContractSource(contract)).toBe("anchor");
  const p = plan(files(), BEFORE, T047, STEPS, contract);
  expect(p.chain_digest).toBe(plan().chain_digest);
  expect(p.contract_digest).toBe(bootstrapContractDigests().contract_digest);
  // Under the lock the reordered anchor matches a plan made from the HEAD module text.
  const r = await run({}, { contract });
  expect(r.code).toBeUndefined();
  expect(r.result).toBe("verified");
});

test("contract (d): an extra or missing role in the release map or the anchor refuses; legacy coverage is proved", async () => {
  // Release map with a role the anchor's map (and chain) does not have.
  const extra = structuredClone(SESSION_BOOTSTRAP_ROLES) as Record<string, Record<string, unknown>>;
  extra[readApiFile].bulls96_extra = { login: "allowed", inbound: "none", settings: "unchecked" };
  const pre = (map: unknown, contract = anchorContract()) => () =>
    bootstrapTargetChain({ target: T047, targetSha: TSHA, steps: STEPS, files: files(), contract, map: map as typeof SESSION_BOOTSTRAP_ROLES });
  expect(code(pre(extra))).toBe("release_bootstrap_contract_drift");
  // Anchor map or manifest missing a required role (same SQL).
  const narrative = '    bulls96_digest_narrative: bare("allowed", "unchecked"),\n';
  expect(code(() => bootstrapContractSource(anchorContract(rolesText([narrative, ""]))))).toBe("release_bootstrap_contract_drift");
  expect(
    code(() => bootstrapContractSource(anchorContract(ROLES_MODULE_TEXT, consumersText(SESSION_BOOTSTRAP_CONSUMERS.filter(([r]) => r !== admit))))),
  ).toBe("release_bootstrap_contract_drift");
  const r = await run({}, { contract: anchorContract(rolesText([narrative, ""])) });
  expect(r.code).toBe("release_bootstrap_contract_drift");
  expect(r.log).toEqual(underLockRefused);
  expectNoMutation(r);
  // Legacy fallback: the release map with an extra role refuses (it has no manifest entry:
  // release_bootstrap_manifest refuses before the coverage proof).
  expect(code(pre(extra, LEGACY))).toBe("release_bootstrap_manifest");
  // The coverage proof itself (the backstop under the per-file checks).
  const scopes = SESSION_BOOTSTRAP_FILES.map((file) => ({ file, scope: "run" as const }));
  const required = [...chainNames].sort();
  const cover = (o: Partial<Parameters<typeof bootstrapContractCoverage>[0]>) => () =>
    bootstrapContractCoverage({ target: T047, files: files(), scopes, required_roles: required, map: SESSION_BOOTSTRAP_ROLES, ...o });
  expect(code(cover({}))).toBe("no refusal");
  const unmapped = structuredClone(SESSION_BOOTSTRAP_ROLES) as Record<string, Record<string, unknown>>;
  delete unmapped[readApiFile].bulls96_digest_narrative;
  expect(code(cover({ map: unmapped as typeof SESSION_BOOTSTRAP_ROLES }))).toBe("release_bootstrap_contract_drift");
  expect(code(cover({ consumers: SESSION_BOOTSTRAP_CONSUMERS.filter(([r]) => r !== admit) }))).toBe("release_bootstrap_contract_drift");
  expect(code(cover({ required_roles: [...required, "bulls96_extra"] }))).toBe("release_bootstrap_contract_drift");
  expect(code(cover({ target: T_MID }))).toBe("release_bootstrap_contract_drift");
  expect(code(cover({ scopes: scopes.map((s) => (s.file === readApiFile ? { ...s, scope: "excluded" as const } : s)) }))).toBe(
    "release_bootstrap_contract_drift",
  );
  // Legacy positive: grandfathered release contract, coverage proved, the stage verifies.
  const legacy = plan(files(), BEFORE, T047, STEPS, LEGACY);
  const ok = await run({}, { planned: legacy, contract: LEGACY });
  expect(ok.result).toBe("verified");
  expect(ok.evidence[0]).toMatchObject({ contract_source: "release", contract_digest: bootstrapContractDigests().contract_digest });
});

test("contract (e): a contract swapped under the lock refuses before BEGIN with zero mutation", async () => {
  const anchorPlan = plan();
  const legacyPlan = plan(files(), BEFORE, T047, STEPS, LEGACY);
  const cases: [string, BootstrapSection, BootstrapAnchorContract, string][] = [
    ["anchor plan, drifted anchor under the lock", anchorPlan, anchorContract(SEMANTIC_DRIFT), "release_bootstrap_contract_drift"],
    ["anchor plan, partial anchor under the lock", anchorPlan, anchorContract(ROLES_MODULE_TEXT, null), "release_bootstrap_contract_drift"],
    ["anchor plan, legacy anchor under the lock", anchorPlan, LEGACY, "release_bootstrap_plan_changed"],
    ["legacy plan, anchor modules under the lock", legacyPlan, anchorContract(), "release_bootstrap_plan_changed"],
  ];
  for (const [name, planned, contract, want] of cases) {
    const r = await run({}, { planned, contract });
    expect({ name, code: r.code, log: r.log }).toEqual({ name, code: want, log: underLockRefused });
    expectNoMutation(r);
  }
  // A plan whose contract digest was tampered with (or made from another map) no longer matches.
  for (const k of ["contract_map_digest", "contract_manifest_digest", "contract_digest"] as const) {
    const r = await run({}, { planned: { ...anchorPlan, [k]: "0".repeat(64) } });
    expect({ k, code: r.code, log: r.log }).toEqual({ k, code: "release_bootstrap_plan_changed", log: underLockRefused });
    expectNoMutation(r);
  }
  // Success evidence records the source and digests, never the contract contents.
  const ok = await run({});
  expect(ok.evidence[0]).toMatchObject({ contract_source: "anchor", contract_digest: anchorPlan.contract_digest });
  expect(JSON.stringify(ok.evidence)).not.toContain("worker-2s");
  expect(JSON.stringify(ok.evidence)).not.toContain("claimed-60s");
});
