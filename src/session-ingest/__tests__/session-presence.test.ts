/**
 * presence-dry (backlog-ops' dry-only session presence mode): dispatch, the SESSION_PRESENCE_SESSION secret, the
 * child binding, the tool's argv/stdin parsing, the transaction shape and the counts-only receipt. A fake SQL client;
 * the seeded counts and the no-leak run are in session-presence.pglite.test.ts.
 */
import { expect, test } from "bun:test";
import { backlogOpsChildSpawn, toolDbUrl } from "../../../scripts/backlog-ops.ts";
import { SCHEMA_RELEASE as P } from "../../hoopdb/schema-release-contract.ts";
import {
  BACKLOG_OPS_DRY_ENVIRONMENT,
  BACKLOG_OPS_MODES,
  backlogOpsBindings,
  backlogOpsDispatch,
  backlogOpsEnvironment,
  backlogOpsPresenceSession,
  backlogOpsReceipt,
  backlogOpsRole,
  backlogOpsToolArgs,
  checkBoundReceipt,
  countsOnly,
  type BacklogOpsDispatch,
  type BacklogOpsMode,
} from "../backlog-ops.ts";
import {
  SESSION_PRESENCE_COUNTS,
  SESSION_PRESENCE_GENERATION_MARKER,
  parseSessionPresenceArgs,
  parseSessionPresenceInput,
  sessionPresence,
  sessionPresenceMain,
  type PresenceSql,
} from "../session-presence.ts";

const sha = "1".repeat(40);
// Test fixtures only (not a production session).
const SESSION = "0f0e0d0c-0b0a-4908-8706-050403020100", TOKEN = "Fixture_session-token_42";
const ORG1 = "00000000-0000-4000-8000-000000000001", ORG2 = "00000000-0000-4000-8000-000000000002";
const codeSync = (fn: () => unknown) => {
  try { fn(); return null; } catch (e) { return (e as { code?: string }).code ?? (e as Error).message; }
};
const codeOf = async (fn: () => Promise<unknown>) => {
  try { await fn(); return null; } catch (e) { return (e as { code?: string }).code ?? (e as Error).message; }
};
const target = {
  BACKLOG_OPS_BUCKET: "96bulls-aws-sessions-prod-transcripts-000000000000",
  BACKLOG_OPS_REGION: "us-east-1",
  BACKLOG_OPS_KMS_KEY_ARN: "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-4000-8000-000000000000",
  BACKLOG_OPS_DRY_ROLE_ARN: "arn:aws:iam::000000000000:role/96bulls-aws-sessions-prod-backlog-ops-read",
  BACKLOG_OPS_EXECUTE_ROLE_ARN: "arn:aws:iam::000000000000:role/96bulls-aws-sessions-prod-backlog-ops-write",
};
const presence = (extra: Record<string, string> = {}) => ({ BACKLOG_OPS_MODE: "presence-dry", BACKLOG_OPS_SHA: sha, ...extra });
const ownerUrl = `postgresql://${P.owner}.${P.project}:p%40ss%2Fw0rd@${P.host}:${P.port}/${P.database}?sslmode=verify-full`;
const aws = { accessKeyId: "ASIAAAAAAAAAAAAAAAAA", secretAccessKey: "s", sessionToken: "t" };

// ---- dispatch ----

test("dispatch: presence-dry is accepted while executes are dormant; it is dry, unbound, dry environment, dry role", () => {
  expect((BACKLOG_OPS_MODES as readonly string[]).includes("presence-dry")).toBe(true);
  expect((BACKLOG_OPS_MODES as readonly string[]).includes("presence-execute")).toBe(false);
  const d = backlogOpsDispatch(presence());
  expect(d).toEqual({ mode: "presence-dry", sha, dryRun: null, backfillRun: null, exportRun: null, maxObjects: null, confirmation: "" });
  expect(backlogOpsEnvironment("presence-dry")).toBe(BACKLOG_OPS_DRY_ENVIRONMENT);
  expect(backlogOpsBindings(d)).toEqual([]);
  expect(backlogOpsRole(d, { bucket: "", region: "", kmsKeyArn: "", dryRoleArn: "dry", executeRoleArn: "exec" })).toBe("dry");
});

test("dispatch: presence-execute is refused with its own code, enabled or not; presence-dry refuses execute-only inputs", () => {
  for (const enabled of [false, true])
    expect(codeSync(() => backlogOpsDispatch(presence({ BACKLOG_OPS_MODE: "presence-execute" }), enabled))).toBe("backlog_ops_presence_dry_only");
  expect(codeSync(() => backlogOpsDispatch(presence({ BACKLOG_OPS_DRY_RUN: "111" }), true))).toBe("backlog_ops_dry_run");
  expect(codeSync(() => backlogOpsDispatch(presence({ BACKLOG_OPS_BACKFILL_RUN: "222" }), true))).toBe("backlog_ops_backfill_run");
  expect(codeSync(() => backlogOpsDispatch(presence({ BACKLOG_OPS_EXPORT_RUN: "333" }), true))).toBe("backlog_ops_export_run");
  expect(codeSync(() => backlogOpsDispatch(presence({ BACKLOG_OPS_MAX_OBJECTS: "10" }), true))).toBe("backlog_ops_max_objects");
  expect(codeSync(() => backlogOpsDispatch(presence({ BACKLOG_OPS_CONFIRMATION: "PRESENCE-DRY" }), true))).toBe("backlog_ops_confirmation");
  // Forced past the dispatch (a hand-built dispatch): the role, the argv and the environment still refuse it.
  const forged = { mode: "presence-execute" as BacklogOpsMode, sha, dryRun: "1", backfillRun: null, exportRun: null, maxObjects: null, confirmation: "" } as BacklogOpsDispatch;
  expect(codeSync(() => backlogOpsRole(forged, { bucket: "", region: "", kmsKeyArn: "", dryRoleArn: "dry", executeRoleArn: "exec" }))).toBe("backlog_ops_presence_dry_only");
  expect(codeSync(() => backlogOpsToolArgs(forged, { bucket: "", region: "", kmsKeyArn: "", dryRoleArn: "", executeRoleArn: "" }))).toBe("backlog_ops_presence_dry_only");
});

// ---- the secret ----

test("SESSION_PRESENCE_SESSION: required and shape-checked for presence-dry, refused for every other mode, never echoed", () => {
  const d = backlogOpsDispatch(presence());
  expect(backlogOpsPresenceSession({ SESSION_PRESENCE_SESSION: SESSION }, d)).toBe(SESSION);
  expect(backlogOpsPresenceSession({ SESSION_PRESENCE_SESSION: ` ${TOKEN}\n` }, d)).toBe(TOKEN);
  expect(backlogOpsPresenceSession({ SESSION_PRESENCE_SESSION: "abcd_123" }, d)).toBe("abcd_123");
  expect(backlogOpsPresenceSession({ SESSION_PRESENCE_SESSION: "x".repeat(128) }, d)).toBe("x".repeat(128));
  for (const v of [undefined, "", "   "]) expect(codeSync(() => backlogOpsPresenceSession({ SESSION_PRESENCE_SESSION: v }, d))).toBe("backlog_ops_presence_session_missing");
  for (const v of ["short_7", "x".repeat(129), "has space1", `${SESSION},${SESSION}`, "semi;colon1", "quote'd1234", "dots.in.it", "slash/path1", "unicodeéabc"]) {
    const c = codeSync(() => backlogOpsPresenceSession({ SESSION_PRESENCE_SESSION: v }, d));
    expect(c).toBe("backlog_ops_presence_session_invalid");
  }
  for (const mode of BACKLOG_OPS_MODES.filter((m) => m !== "presence-dry")) {
    expect(backlogOpsPresenceSession({}, { mode })).toBeNull();
    expect(codeSync(() => backlogOpsPresenceSession({ SESSION_PRESENCE_SESSION: SESSION }, { mode }))).toBe("backlog_ops_presence_session_unexpected");
  }
});

// ---- argv, child binding and bound receipts ----

test("presence argv: dry run, the bounded timeout and the scope's orgs; no bucket, key or session; stdin carries the session", () => {
  const d = backlogOpsDispatch(presence());
  const t = { bucket: "b", region: "r", kmsKeyArn: "k", dryRoleArn: "d", executeRoleArn: "e" };
  expect(backlogOpsToolArgs(d, t)).toEqual({ script: "scripts/session-presence.ts", args: ["--dry-run", "--statement-timeout-ms", "120000"], needsDb: true });
  expect(backlogOpsToolArgs(d, t, { orgs: [ORG1, ORG2], id: "x" }).args).toEqual(["--dry-run", "--statement-timeout-ms", "120000", "--org", ORG1, "--org", ORG2]);
  const env = { ...target, PATH: "/bin", GITHUB_TOKEN: "ghs_x", SESSION_PRESENCE_SESSION: SESSION };
  const spawn = backlogOpsChildSpawn(env, d, aws, toolDbUrl(ownerUrl), SESSION);
  expect(Object.keys(JSON.parse(spawn.input!))).toEqual(["dbUrl", "session"]);
  expect(JSON.parse(spawn.input!).session).toBe(SESSION);
  expect(spawn.args.join(" ")).not.toContain(SESSION);
  expect(spawn.args[0]).toEndWith("scripts/session-presence.ts");
  expect(JSON.stringify(spawn.env)).not.toContain(SESSION);
  expect(Object.keys(spawn.env).sort()).toEqual(["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "PATH"]);
  // The session goes to presence-dry only, always with the database.
  expect(codeSync(() => backlogOpsChildSpawn(env, d, aws, toolDbUrl(ownerUrl), null))).toBe("backlog_ops_session_binding");
  expect(codeSync(() => backlogOpsChildSpawn(env, d, aws, null, SESSION))).toBe("backlog_ops_db_binding");
  const backfill = backlogOpsDispatch({ BACKLOG_OPS_MODE: "backfill-dry", BACKLOG_OPS_SHA: sha });
  expect(codeSync(() => backlogOpsChildSpawn(env, backfill, aws, toolDbUrl(ownerUrl), SESSION))).toBe("backlog_ops_session_binding");
  // A presence receipt can never be bound as an execute's prerequisite.
  // (A receipt that is otherwise well formed for the expected mode: only the presence rule refuses it.)
  const wellFormed = JSON.stringify({ contract: "backlog-ops-receipt/v1", mode: "presence-dry", sha, run: "1", exitCode: 0, scope: "all", summary: { mode: "dry_run" } });
  expect(codeSync(() => checkBoundReceipt(wellFormed, { mode: "presence-dry", sha, run: "1", scope: "all", kind: "dry" }))).toBe("backlog_ops_bound_presence_receipt");
  // A forged presence dispatch carrying a bound run binds nothing.
  for (const k of ["dryRun", "backfillRun", "exportRun"] as const)
    expect(codeSync(() => backlogOpsBindings({ ...d, [k]: "5" }))).toBe("backlog_ops_bindings");
  expect(backlogOpsBindings(d)).toEqual([]);
});

test("tool argv and stdin: fixed shapes, fixed codes, the session never in an error", () => {
  expect(parseSessionPresenceArgs(["--dry-run", "--statement-timeout-ms", "120000"])).toEqual({ orgs: [], statementTimeoutMs: 120000 });
  expect(parseSessionPresenceArgs(["--dry-run", "--statement-timeout-ms", "1000", "--org", ORG2, "--org", ORG1]).orgs).toEqual([ORG1, ORG2]);
  for (const argv of [[], ["--statement-timeout-ms", "120000"], ["--dry-run"], ["--execute", "--statement-timeout-ms", "120000"],
    ["--dry-run", "--statement-timeout-ms", "999"], ["--dry-run", "--statement-timeout-ms", "600001"], ["--dry-run", "--dry-run", "--statement-timeout-ms", "1000"],
    ["--dry-run", "--statement-timeout-ms", "1000", "--org", "ORG"], ["--dry-run", "--statement-timeout-ms", "1000", "--org", ORG1, "--org", ORG1],
    ["--dry-run", "--statement-timeout-ms", "1000", "--session", SESSION]])
    expect(codeSync(() => parseSessionPresenceArgs(argv))).toBe("session_presence_args");
  expect(parseSessionPresenceInput(JSON.stringify({ dbUrl: "postgresql://x", session: SESSION }) + "\n")).toEqual({ dbUrl: "postgresql://x", session: SESSION });
  const bad: Array<[string, string]> = [
    ["", "stdin_invalid"], ["[]", "stdin_invalid"], [JSON.stringify({ dbUrl: "x" }), "stdin_invalid"],
    [JSON.stringify({ dbUrl: "x", session: SESSION, extra: 1 }), "stdin_invalid"], [JSON.stringify({ dbUrl: "", session: SESSION }), "db_url_required"],
    [JSON.stringify({ dbUrl: "x", session: "" }), "session_presence_session_missing"], [JSON.stringify({ dbUrl: "x", session: "bad value!" }), "session_presence_session_invalid"],
    ["x".repeat(64 * 1024 + 1), "stdin_too_large"],
  ];
  for (const [input, c] of bad) expect(codeSync(() => parseSessionPresenceInput(input))).toBe(c);
});

// ---- the transaction ----

/** A fake client: records statements, answers counts with `n`, the marker with markers[i], and can fail chosen queries. */
function fake(o: { n?: number; ro?: string; markers?: Array<{ rows: number; marker: string }>; fail?: (text: string) => unknown; ownership?: number } = {}) {
  const log: Array<{ text: string; params?: unknown[] }> = [];
  let marks = 0;
  const sql: PresenceSql = {
    async query(text, params) {
      log.push({ text, params });
      const err = o.fail?.(text);
      if (err) throw err;
      if (text === "SHOW transaction_read_only") return { rows: [{ transaction_read_only: o.ro ?? "on" }] };
      if (text.includes("AS marker")) return { rows: [(o.markers ?? [])[marks++] ?? { rows: 0, marker: "" }] };
      const c = SESSION_PRESENCE_COUNTS.find((c) => c.sql(true) === text || c.sql(false) === text);
      if (c) return { rows: [Object.fromEntries(c.columns.map((k) => [k, k === "ownership" ? (o.ownership ?? o.n ?? 1) : (o.n ?? 1)]))] };
      return { rows: [] };
    },
  };
  return { sql, log };
}
const opts = { session: SESSION, orgs: null, statementTimeoutMs: 120000 };
const timeout = Object.assign(new Error("canceling statement due to statement timeout"), { code: "57014" });
const M = (m: string) => ({ rows: 2, marker: m.repeat(32) });

test("transaction: REPEATABLE READ READ ONLY, read-only check, bounded timeout, marker first and last, savepoints, ROLLBACK", async () => {
  const { sql, log } = fake({ markers: [M("a"), M("a")] });
  const out = await sessionPresence(sql, opts);
  const texts = log.map((l) => l.text);
  expect(texts.slice(0, 3)).toEqual(["BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", "SHOW transaction_read_only", "SET LOCAL statement_timeout = 120000"]);
  expect(texts.at(-1)).toBe("ROLLBACK");
  expect(texts.filter((t) => t === "COMMIT")).toEqual([]);
  expect(texts[4]).toBe(SESSION_PRESENCE_GENERATION_MARKER(false));
  expect(texts.at(-3)).toBe(SESSION_PRESENCE_GENERATION_MARKER(false));
  expect(texts.filter((t) => t === "SAVEPOINT session_presence_count").length).toBe(SESSION_PRESENCE_COUNTS.length + 2);
  // Every data statement is parameterised: the session only as $1, never in the text.
  for (const l of log) expect(l.text).not.toContain(SESSION);
  for (const l of log.filter((l) => l.params)) expect(l.params).toEqual([SESSION]);
  for (const c of SESSION_PRESENCE_COUNTS) { expect(c.sql(false)).toMatch(/^SELECT count\(/u); expect(c.sql(true)).toContain("= ANY($2::text[])"); }
  expect(out).toEqual({
    ownership: 1, claims: 1, claim_sessions: 1, buffered_stages: 1, outbox: 1, outbox_buffered: 1, lazy_slots: 1, revisions: 1,
    heads: 1, published_heads: 1, published_objects: 1, canonical_pointers: 1, canonical_pointers_live: 1,
    dispositions: { heads_building: 1, heads_live: 1, heads_retired: 1 },
    scope: "all", generation_stable: true, complete: true, incomplete_reason: "none",
  });
  // Scoped: $2 is the org list, the scope code is a count.
  const scoped = fake({ markers: [M("b"), M("b")] });
  const s = await sessionPresence(scoped.sql, { ...opts, orgs: [ORG1, ORG2] });
  expect(s.scope).toBe("orgs_2");
  for (const l of scoped.log.filter((l) => l.params)) expect(l.params).toEqual([SESSION, [ORG1, ORG2]]);
});

test("transaction: a timed-out count is null with count_timeout; other errors refuse; ROLLBACK on every path", async () => {
  const t = fake({ markers: [M("a"), M("a")], fail: (x) => x.includes("ingest_lazy_native_slots") && !x.includes("AS marker") ? timeout : null });
  const out = await sessionPresence(t.sql, opts);
  expect(out.lazy_slots).toBeNull();
  expect(out.revisions).toBe(1);
  expect([out.complete, out.incomplete_reason, out.generation_stable]).toEqual([false, "count_timeout", true]);
  expect(t.log.map((l) => l.text)).toContain("ROLLBACK TO SAVEPOINT session_presence_count");
  expect(t.log.at(-1)!.text).toBe("ROLLBACK");
  const e = fake({ fail: (x) => x.includes("ingest_record_revisions") ? Object.assign(new Error("boom"), { code: "42P01" }) : null });
  expect(await codeOf(() => sessionPresence(e.sql, opts))).toBe("42P01");
  expect(e.log.at(-1)!.text).toBe("ROLLBACK");
  const ro = fake({ ro: "off" });
  expect(await codeOf(() => sessionPresence(ro.sql, opts))).toBe("session_presence_not_read_only");
  expect(ro.log.map((l) => l.text)).toEqual(["BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", "SHOW transaction_read_only", "ROLLBACK"]);
  const shape = fake({ n: -1 });
  expect(await codeOf(() => sessionPresence(shape.sql, opts))).toBe("session_presence_count_shape");
  const frac = fake({ n: 1.5 });
  expect(await codeOf(() => sessionPresence(frac.sql, opts))).toBe("session_presence_count_shape");
  const mark = fake({ markers: [{ rows: 1, marker: "not-hex" }] });
  expect(await codeOf(() => sessionPresence(mark.sql, opts))).toBe("session_presence_marker_shape");
  expect(mark.log.at(-1)!.text).toBe("ROLLBACK");
});

test("fail closed: an unstable or unobserved generation marker and multiple ownership rows are complete=false with a code", async () => {
  const unstable = await sessionPresence(fake({ markers: [M("a"), M("c")] }).sql, opts);
  expect([unstable.generation_stable, unstable.complete, unstable.incomplete_reason, unstable.ownership]).toEqual([false, false, "generation_unstable", 1]);
  const rowsMoved = await sessionPresence(fake({ markers: [{ rows: 2, marker: "a".repeat(32) }, { rows: 3, marker: "a".repeat(32) }] }).sql, opts);
  expect(rowsMoved.incomplete_reason).toBe("generation_unstable");
  let n = 0;
  const lost = await sessionPresence(fake({ markers: [M("a"), M("a")], fail: (x) => x.includes("AS marker") && n++ === 1 ? timeout : null }).sql, opts);
  expect([lost.generation_stable, lost.complete, lost.incomplete_reason]).toEqual([false, false, "generation_unobserved"]);
  const multi = await sessionPresence(fake({ markers: [M("a"), M("a")], ownership: 2 }).sql, opts);
  expect([multi.generation_stable, multi.complete, multi.incomplete_reason, multi.ownership]).toEqual([true, false, "multi_ownership", 2]);
  // Precedence: the generation outranks ownership, ownership outranks a timed-out count.
  const both = await sessionPresence(fake({ markers: [M("a"), M("d")], ownership: 3 }).sql, opts);
  expect(both.incomplete_reason).toBe("generation_unstable");
  const own = await sessionPresence(fake({ markers: [M("a"), M("a")], ownership: 2, fail: (x) => x.includes("ingest_record_revisions") && !x.includes("AS marker") ? timeout : null }).sql, opts);
  expect(own.incomplete_reason).toBe("multi_ownership");
  // Empty marker on both ends (no head, no pointer) is stable.
  const none = await sessionPresence(fake({ n: 0, markers: [{ rows: 0, marker: "" }, { rows: 0, marker: "" }] }).sql, opts);
  expect([none.generation_stable, none.complete, none.incomplete_reason]).toEqual([true, true, "none"]);
});

test("input checks in the library itself: session, scope and timeout", async () => {
  const { sql, log } = fake();
  expect(await codeOf(() => sessionPresence(sql, { ...opts, session: "bad value" }))).toBe("session_presence_session_invalid");
  expect(await codeOf(() => sessionPresence(sql, { ...opts, orgs: ["ORG"] }))).toBe("session_presence_scope");
  expect(await codeOf(() => sessionPresence(sql, { ...opts, orgs: [ORG1, ORG1] }))).toBe("session_presence_scope");
  expect(await codeOf(() => sessionPresence(sql, { ...opts, statementTimeoutMs: 999 }))).toBe("session_presence_timeout");
  expect(await codeOf(() => sessionPresence(sql, { ...opts, statementTimeoutMs: 600001 }))).toBe("session_presence_timeout");
  expect(log).toEqual([]);
});

// ---- receipt ----

test("receipt: the summary passes countsOnly; the tool's refusal lines are codes; a leaked id refuses the receipt", async () => {
  const main = await sessionPresenceMain(["--dry-run", "--statement-timeout-ms", "120000"], JSON.stringify({ dbUrl: "postgresql://x", session: SESSION }),
    async () => fake({ markers: [M("a"), M("a")] }).sql);
  expect(main.code).toBe(0);
  const d = backlogOpsDispatch(presence());
  const r = backlogOpsReceipt(d, "9", main.code, main.line + "\n");
  expect(r.exitCode).toBe(0);
  expect(countsOnly(r.summary)).toEqual(JSON.parse(main.line));
  expect((r.summary as Record<string, unknown>).incomplete_reason).toBe("none");
  expect(JSON.stringify(r)).not.toContain(SESSION);
  // A summary carrying an id or a digest refuses the receipt.
  // (countsOnly cannot tell a lowercase token from a code: "a".repeat(32) passes it. That is why the tool never reads
  // an id or digest into its output, and the pglite no-leak test checks the value itself.)
  for (const leak of [{ ownership: SESSION }, { marker: "0".repeat(32) }, { generation: "f".repeat(32) + "-x" }, { url: "postgresql://x" }])
    expect(codeSync(() => countsOnly(leak))).toBe("backlog_ops_receipt_string");
  // Refusals: closed codes only, never a pg message, a URL or the session.
  const pgErr = await sessionPresenceMain(["--dry-run", "--statement-timeout-ms", "120000"], JSON.stringify({ dbUrl: "postgresql://x", session: SESSION }),
    async () => ({ query: async () => { throw Object.assign(new Error(`relation for ${SESSION} at postgresql://host`), { code: "42501" }); } }));
  expect(pgErr).toEqual({ line: JSON.stringify({ refused: "sqlstate_42501" }), code: 1 });
  const lettered = await sessionPresenceMain(["--dry-run", "--statement-timeout-ms", "120000"], JSON.stringify({ dbUrl: "postgresql://x", session: SESSION }),
    async () => ({ query: async () => { throw Object.assign(new Error("x"), { code: "42P01" }); } }));
  expect(lettered).toEqual({ line: JSON.stringify({ refused: "sqlstate_42p01" }), code: 1 });
  const odd = await sessionPresenceMain(["--dry-run", "--statement-timeout-ms", "120000"], JSON.stringify({ dbUrl: "postgresql://x", session: SESSION }),
    async () => { throw new Error(`could not connect for ${SESSION}`); });
  expect(odd).toEqual({ line: JSON.stringify({ refused: "internal_error" }), code: 1 });
  const badArgs = await sessionPresenceMain([], "", async () => fake().sql);
  expect(badArgs).toEqual({ line: JSON.stringify({ refused: "session_presence_args" }), code: 1 });
  for (const out of [pgErr, lettered, odd, badArgs]) expect(countsOnly(backlogOpsReceipt(d, "9", out.code, out.line).summary)).toEqual(JSON.parse(out.line));
});
