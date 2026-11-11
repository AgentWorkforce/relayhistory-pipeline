/**
 * The bootstrap stage (runBootstrapStage) driven with a fake client that records every
 * query and end(): proves, query by query, that a refusal or error never lets chain SQL
 * reach the client outside BEGIN..ROLLBACK, that COMMIT is issued at most once and only
 * after the readback verdict, and that the lock is released before the client ends.
 * No database is touched.
 */
import { test, expect } from "bun:test";
import { readFileSync } from "node:fs";
import {
  BOOTSTRAP_LOCK,
  BOOTSTRAP_UNLOCK,
  bootstrapChainRoles,
  bootstrapSection,
  runBootstrapStage,
  stripPsqlMeta,
  type BootstrapEdge,
  type BootstrapSection,
  type BootstrapState,
} from "../schema-release-bootstrap.ts";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";

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
const files = (o: Record<string, string> = {}) =>
  SESSION_BOOTSTRAP_FILES.map((file) => {
    const bytes = file in o ? Buffer.from(o[file]) : realBytes(file);
    return { file, release: bytes, target: bytes };
  });
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

const plan = (f = files(), st = BEFORE) => bootstrapSection({ files: f, state: st, supabaseMode: true });
async function run(
  s: Script,
  o: { planned?: BootstrapSection; files?: ReturnType<typeof files>; underLock?: () => Promise<void> } = {},
) {
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
      files: o.files ?? files(),
      planned: o.planned ?? plan(),
      supabaseMode: true,
      underLock: o.underLock ?? (async () => {}),
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
  expect(r.evidence[0]).toMatchObject({ version: 1, ran: [admitFile], created: [admit] });
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
  const cases: [string, Script, Parameters<typeof run>[1], string, boolean][] = [
    // The plan saw a different absent set.
    ["plan changed", {}, { planned: plan(files(), state([admit, "bulls96_read_api"])) }, "release_bootstrap_plan_changed", false],
    // The release bytes changed since the plan.
    ["plan changed (bytes)", {}, { files: files({ [admitFile]: "-- other\n" }) }, "release_bootstrap_plan_changed", false],
    // A present chain role drifted from its contract.
    [
      "role drift",
      { before: state([admit], { roles: [...state([admit]).roles.filter((x) => x.rolname !== "bulls96_read_api"), role("bulls96_read_api", { rolsuper: true })] }) },
      {},
      "release_bootstrap_role_drift",
      false,
    ],
    // The verdict found a side effect (an unrelated role altered).
    [
      "readback mismatch",
      { after: { ...AFTER, roles: AFTER.roles.map((x) => (x.rolname === "authenticator" ? { ...x, rolbypassrls: true } : x)) } },
      {},
      "release_bootstrap_readback",
      true,
    ],
    // A chain file ended the transaction: a new txid.
    ["txid changed", { xids: ["777", "778"] }, {}, "release_bootstrap_readback", true],
    // The chain SQL itself errors.
    ["sql error", { throwOn: "CREATE ROLE" }, {}, "boom", true],
  ];
  for (const [name, script, o, code, chainRan] of cases) {
    const r = await run(script, o);
    expect({ name, code: r.code }).toEqual({ name, code });
    expect({ name, control: control(r.log) }).toEqual({
      name,
      control: chainRan ? [BOOTSTRAP_LOCK, "BEGIN", "<chain>", "ROLLBACK", BOOTSTRAP_UNLOCK, "<end>"] : refused,
    });
    expect({ name, evidence: r.evidence }).toEqual({ name, evidence: [] });
  }
});

test("stage chain_unsafe: a top-level END in a chain file is refused before any chain SQL is sent", async () => {
  const unsafe = files({ [admitFile]: "DO $$ BEGIN PERFORM 1; END $$;\nEND;\nCREATE ROLE x;\n" });
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
