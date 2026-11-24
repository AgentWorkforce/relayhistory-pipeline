import { describe, expect, test } from "bun:test";
import { CONFIRMATION, parseUserEraseRequest, RELAYFACTS_USER_ERASE_FUNCTION, RelayfactsEraseFailed, runUserErase, sqlUserEraseRelayfacts, USER_ERASE_LOCK_TIMEOUT_MS, USER_ERASE_STATEMENT_TIMEOUT_MS, userErasePrefixes, UserEraseRefused } from "../user-erase.ts";
import { errorEvent, exitCodeForError, exitCodeForResult, relayfactsUserEraseEnabled, runUserEraseMain, settleMs, userEraseEnabled } from "../user-erase-main.ts";
import { liveMachineId } from "../../migration/neon-s3-erasure/live.ts";
import { LEGACY_EXPORT_ORIGIN_ID } from "../../shared/legacy-export-machine.ts";

const code = (f: () => unknown) => { try { f(); return null; } catch (e) { return e instanceof UserEraseRefused ? e.code : String(e); } };
const good = { request_id: "dsr-1", org_id: "org-a", workspace_id: "ws-a", user_id: "user-1", confirmation: CONFIRMATION };

describe("user-erase entry", () => {
  test("flag: exactly 1 enables; unset, empty and 0 are off; anything else refuses", () => {
    expect(userEraseEnabled({})).toBe(false);
    expect(userEraseEnabled({ OPERATOR_USER_ERASE_ENABLED: "" })).toBe(false);
    expect(userEraseEnabled({ OPERATOR_USER_ERASE_ENABLED: "0" })).toBe(false);
    expect(userEraseEnabled({ OPERATOR_USER_ERASE_ENABLED: "1" })).toBe(true);
    expect(code(() => userEraseEnabled({ OPERATOR_USER_ERASE_ENABLED: "true" }))).toBe("flag_invalid");
  });

  test("disabled: returns before reading the request, any config or credential", async () => {
    let read = false;
    const r = await runUserEraseMain({}, () => { read = true; return "{}"; });
    expect(r).toEqual({ enabled: false });
    expect(read).toBe(false);
    expect(exitCodeForResult(r)).toBe(0);
  });

  test("request: exact keys, the confirmation phrase, well-formed ids; values never echoed", () => {
    expect(parseUserEraseRequest(JSON.stringify(good))).toEqual({ requestId: "dsr-1", orgId: "org-a", workspaceId: "ws-a", userId: "user-1" });
    expect(code(() => parseUserEraseRequest(JSON.stringify({ ...good, confirmation: "yes" })))).toBe("confirmation_required");
    expect(code(() => parseUserEraseRequest(JSON.stringify({ ...good, extra: 1 })))).toBe("request_invalid");
    expect(code(() => parseUserEraseRequest(JSON.stringify({ ...good, user_id: "" })))).toBe("request_invalid");
    expect(code(() => parseUserEraseRequest(JSON.stringify({ ...good, org_id: "a\nb" })))).toBe("request_invalid");
    expect(code(() => parseUserEraseRequest("not json"))).toBe("request_invalid");
    expect(code(() => parseUserEraseRequest("[]"))).toBe("request_invalid");
  });

  test("prefixes: the four layouts per origin, each under the user's own machine id, then the workspace's synthesis directory", () => {
    const req = { requestId: "r", orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
    const m = liveMachineId("ws-a", "o1", "user-1");
    const legacy = liveMachineId("ws-a", LEGACY_EXPORT_ORIGIN_ID, "user-1");
    expect(userErasePrefixes("96bulls", req, ["o1"])).toEqual([
      `96bulls/org-a/buffered-batches/v1/ws=ws-a/m=${m}/`,
      `96bulls/org-a/live-batches/v1/ws=ws-a/m=${m}/`,
      `96bulls/org-a/v1/ws=ws-a/m=${m}/`,
      `96bulls/org-a/live-sessions/v1/ws=ws-a/m=${m}/`,
      `96bulls/org-a/buffered-batches/v1/ws=ws-a/m=${legacy}/`,
      `96bulls/org-a/live-batches/v1/ws=ws-a/m=${legacy}/`,
      `96bulls/org-a/v1/ws=ws-a/m=${legacy}/`,
      `96bulls/org-a/live-sessions/v1/ws=ws-a/m=${legacy}/`,
      // The workspace's derived synthesis artifacts, every window (session-analysis/workspace-synthesis-layout.ts).
      "96bulls/org-a/v1/ws=ws-a/normalized/session-analysis-v2/workspace-synthesis-v1/",
    ]);
    expect(userErasePrefixes("96bulls", req, [])).toEqual([
      `96bulls/org-a/buffered-batches/v1/ws=ws-a/m=${legacy}/`,
      `96bulls/org-a/live-batches/v1/ws=ws-a/m=${legacy}/`,
      `96bulls/org-a/v1/ws=ws-a/m=${legacy}/`,
      `96bulls/org-a/live-sessions/v1/ws=ws-a/m=${legacy}/`,
      "96bulls/org-a/v1/ws=ws-a/normalized/session-analysis-v2/workspace-synthesis-v1/",
    ]);
    expect(liveMachineId("ws-a", "o1", "user-2")).not.toBe(m);
  });

  test("prefixes: a workspace id the erase accepts but no synthesis can have gets the base prefixes only, never a throw", () => {
    for (const workspaceId of ["_ws", "ws a", "ws/a", "ws@a", "w".repeat(200)]) {
      const req = { requestId: "r", orgId: "org-a", workspaceId, userId: "user-1" };
      const prefixes = userErasePrefixes("96bulls", req, ["o1"]);
      expect([workspaceId, prefixes.length]).toEqual([workspaceId, 8]);
      expect(prefixes.every((p) => /\/m=srv_[0-9a-f]{32}\/$/.test(p))).toBe(true);
    }
    // An admitted id with `_` and `:` gets its injective synthesis component.
    expect(userErasePrefixes("96bulls", { requestId: "r", orgId: "org-a", workspaceId: "ws_a:b", userId: "user-1" }, []).at(-1))
      .toBe("96bulls/org-a/v1/ws=ws__a_cb/normalized/session-analysis-v2/workspace-synthesis-v1/");
  });

  test("settle and exit codes", () => {
    expect(settleMs({})).toBe(60_000);
    expect(settleMs({ OPERATOR_USER_ERASE_SETTLE_MS: "5" })).toBe(5);
    expect(code(() => settleMs({ OPERATOR_USER_ERASE_SETTLE_MS: "-1" }))).toBe("config_invalid");
    expect(exitCodeForError(new UserEraseRefused("x"))).toBe(2);
    expect(exitCodeForError(Object.assign(new Error("published"), { code: "RH040" }))).toBe(2);
    expect(exitCodeForError(Object.assign(new Error("busy"), { code: "55000" }))).toBe(2);
    expect(exitCodeForError(new Error("network"))).toBe(4);
  });
  test("every call bounds its own transaction: SET LOCAL lock_timeout and statement_timeout right after BEGIN", async () => {
    expect(USER_ERASE_LOCK_TIMEOUT_MS).toBe(30_000);
    expect(USER_ERASE_STATEMENT_TIMEOUT_MS).toBe(900_000);
    const seen: string[] = [];
    const view = { state: "bytes_verified", origins: [], tokens_revoked: 0, deleted: {}, done: true };
    const db = { query: async (text: string) => { seen.push(text); return { rows: text.startsWith("SELECT") ? [{ r: view }] : [] }; } };
    const eraser = { eraseStep: async () => { throw new Error("no bytes phase expected"); } };
    const r = await runUserErase(db, { requestId: "r", orgId: "o", workspaceId: "w", userId: "u" }, { keyRoot: "96bulls", eraser: eraser as never });
    expect(r.state).toBe("bytes_verified");
    expect(seen).toEqual([
      "BEGIN ISOLATION LEVEL READ COMMITTED",
      "SET LOCAL lock_timeout = 30000",
      "SET LOCAL statement_timeout = 900000",
      "SELECT sessions.operator_user_erase_begin($1,$2,$3,$4) AS r",
      "COMMIT",
    ]);
    // A failed statement still rolls the transaction (and with it the SET LOCALs) back.
    seen.length = 0;
    const failing = { query: async (text: string) => { seen.push(text); if (text.startsWith("SELECT")) throw Object.assign(new Error("lock"), { code: "55P03" }); return { rows: [] }; } };
    await expect(runUserErase(failing, { requestId: "r", orgId: "o", workspaceId: "w", userId: "u" }, { keyRoot: "96bulls", eraser: eraser as never })).rejects.toThrow("lock");
    expect(seen.at(-1)).toBe("ROLLBACK");
  });
});

describe("user-erase relayfacts step (20261105062100, SUMMARY_DECISION_RECORDS_ENABLED)", () => {
  const ENV = {
    OPERATOR_USER_ERASE_ENABLED: "1", SESSION_ERASE_S3_BUCKET: "bkt", SESSION_ERASE_S3_REGION: "us-east-1",
    SESSION_ERASE_S3_ACCESS_KEY_ID: "erase-key", SESSION_ERASE_S3_SECRET_ACCESS_KEY: "erase-secret-x",
  };
  const COUNTS = { sessions: 2, fencesAdded: 3, evidenceRows: 4, recordsDeleted: 1, recordsRewritten: 1 };
  const VIEW = { state: "bytes_verified", origins: [], tokens_revoked: 0, deleted: {}, done: true };
  const eraser = () => ({ eraseStep: async () => { throw new Error("no bytes phase expected"); } }) as never;
  const PREFLIGHT = "SELECT to_regprocedure($1) IS NOT NULL AS r";
  /** A fake owner connection: records every statement; `fail` maps a SQL prefix to a thrown error; `applied: false`
   * answers the preflight as if 20261105062100 were not applied. */
  function fakeDb(opts: { relayfacts?: unknown; fail?: Record<string, unknown>; applied?: boolean } = {}) {
    const seen: { text: string; params?: unknown[] }[] = [];
    return {
      seen,
      query: async (text: string, params?: unknown[]) => {
        seen.push({ text, ...(params ? { params } : {}) });
        for (const [prefix, err] of Object.entries(opts.fail ?? {})) if (text.startsWith(prefix)) throw err;
        if (text === PREFLIGHT) return { rows: [{ r: opts.applied ?? true }] };
        if (text.startsWith("SELECT sessions.relayfacts_user_erase_v1")) return { rows: [{ r: "relayfacts" in opts ? opts.relayfacts : COUNTS }] };
        return { rows: text.startsWith("SELECT") ? [{ r: VIEW }] : [] };
      },
    };
  }
  const run = (env: Record<string, string | undefined>, db: ReturnType<typeof fakeDb>, logs: Record<string, unknown>[] = []) =>
    runUserEraseMain(env, () => JSON.stringify(good), { db, eraser, log: (e) => logs.push(e) });

  test("flag: exactly \"true\" enables; unset, \"1\", \"TRUE\" and empty are off", () => {
    expect(relayfactsUserEraseEnabled({})).toBe(false);
    for (const v of ["1", "TRUE", "", "yes", " true"]) expect(relayfactsUserEraseEnabled({ SUMMARY_DECISION_RECORDS_ENABLED: v })).toBe(false);
    expect(relayfactsUserEraseEnabled({ SUMMARY_DECISION_RECORDS_ENABLED: "true" })).toBe(true);
  });

  test("flag off: no SQL names relayfacts, no relayfacts key in the result, no relayfacts log event", async () => {
    for (const env of [ENV, { ...ENV, SUMMARY_DECISION_RECORDS_ENABLED: "1" }, { ...ENV, SUMMARY_DECISION_RECORDS_ENABLED: "TRUE" }]) {
      const db = fakeDb(), logs: Record<string, unknown>[] = [];
      const r = await run(env, db, logs);
      expect(db.seen.some((q) => /relayfacts/i.test(q.text))).toBe(false);
      expect(r).not.toHaveProperty("relayfacts");
      expect(Object.keys(r)).toEqual(["enabled", "tokensRevoked", "steps", "leaseWaits", "state", "verified", "deleted", "prefixes", "versionsDeleted", "byteRounds"]);
      expect(logs.some((e) => String(e.event).includes("relayfacts"))).toBe(false);
      expect(db.seen.map((q) => q.text)).toEqual([
        "BEGIN ISOLATION LEVEL READ COMMITTED", "SET LOCAL lock_timeout = 30000", "SET LOCAL statement_timeout = 900000",
        "SELECT sessions.operator_user_erase_begin($1,$2,$3,$4) AS r", "COMMIT",
      ]);
    }
  });

  test("flag on: the relayfacts call runs first, in its own bounded transaction, before begin; counts in the result and the log", async () => {
    const db = fakeDb(), logs: Record<string, unknown>[] = [];
    const r = await run({ ...ENV, SUMMARY_DECISION_RECORDS_ENABLED: "true" }, db, logs);
    expect(db.seen.map((q) => q.text)).toEqual([
      PREFLIGHT,
      "BEGIN ISOLATION LEVEL READ COMMITTED", "SET LOCAL lock_timeout = 30000", "SET LOCAL statement_timeout = 900000",
      "SELECT sessions.relayfacts_user_erase_v1($1,$2,$3,$4) AS r", "COMMIT",
      "BEGIN ISOLATION LEVEL READ COMMITTED", "SET LOCAL lock_timeout = 30000", "SET LOCAL statement_timeout = 900000",
      "SELECT sessions.operator_user_erase_begin($1,$2,$3,$4) AS r", "COMMIT",
    ]);
    expect(db.seen[0]!.params).toEqual(["sessions.relayfacts_user_erase_v1(text,text,text,text)"]);
    expect(db.seen[4]!.params).toEqual(["dsr-1", "org-a", "ws-a", "user-1"]);
    expect(r).toMatchObject({ enabled: true, verified: true, relayfacts: COUNTS });
    expect(logs[0]).toEqual({ event: "user_erase_relayfacts", ...COUNTS });
    expect(logs[1]).toMatchObject({ event: "user_erase_begin" });
  });

  test("flag on, 20261105062100 not applied: refused relayfacts_migration_missing (exit 2) after the preflight alone", async () => {
    expect(RELAYFACTS_USER_ERASE_FUNCTION).toBe("sessions.relayfacts_user_erase_v1(text,text,text,text)");
    let erased = 0;
    const db = fakeDb({ applied: false }), logs: Record<string, unknown>[] = [];
    const err = await runUserEraseMain({ ...ENV, SUMMARY_DECISION_RECORDS_ENABLED: "true" }, () => JSON.stringify(good), {
      db, log: (e) => logs.push(e),
      eraser: () => ({ eraseStep: async () => { erased += 1; throw new Error("no bytes phase expected"); } }) as never,
    }).then(() => null, (e: unknown) => e);
    expect(err).toBeInstanceOf(UserEraseRefused);
    expect((err as UserEraseRefused).code).toBe("relayfacts_migration_missing");
    expect(exitCodeForError(err)).toBe(2);
    expect(errorEvent(err)).toEqual({ event: "user_erase", error: "relayfacts_migration_missing" });
    expect(db.seen).toEqual([{ text: PREFLIGHT, params: [RELAYFACTS_USER_ERASE_FUNCTION] }]);
    expect(logs).toEqual([]);
    expect(erased).toBe(0);
    // Flag off: no preflight at all, even where the function is absent.
    const off = fakeDb({ applied: false });
    await run(ENV, off);
    expect(off.seen.some((q) => q.text === PREFLIGHT)).toBe(false);
  });

  test("a failed relayfacts step: RelayfactsEraseFailed with the SQLSTATE only, rolled back, begin never called; 22023 exits 2, else 4", async () => {
    for (const [sqlstate, exit] of [["RH015", 4], ["55P03", 4], ["40P01", 4], ["22023", 2]] as const) {
      const cause = Object.assign(new Error("secret detail"), { code: sqlstate });
      const db = fakeDb({ fail: { "SELECT sessions.relayfacts_user_erase_v1": cause } });
      const logs: Record<string, unknown>[] = [];
      const err = await run({ ...ENV, SUMMARY_DECISION_RECORDS_ENABLED: "true" }, db, logs).then(() => null, (e: unknown) => e);
      expect(err).toBeInstanceOf(RelayfactsEraseFailed);
      expect((err as RelayfactsEraseFailed).sqlstate).toBe(sqlstate);
      expect((err as Error).message).toBe("relayfacts_erase_failed");
      expect((err as Error).cause).toBe(cause);
      expect(db.seen.at(-1)!.text).toBe("ROLLBACK");
      expect(db.seen.some((q) => q.text.includes("operator_user_erase_begin"))).toBe(false);
      expect(logs).toEqual([{ event: "user_erase_relayfacts", outcome: "failed", sqlstate }]);
      expect(errorEvent(err)).toEqual({ event: "user_erase", error: "relayfacts_erase_failed", sqlstate });
      expect(JSON.stringify(errorEvent(err))).not.toContain("secret");
      expect(exitCodeForError(err)).toBe(exit);
    }
    // A failure without a SQLSTATE (a dropped connection) is rerun, and logs no sqlstate key.
    expect(exitCodeForError(new RelayfactsEraseFailed())).toBe(4);
    expect(errorEvent(new RelayfactsEraseFailed())).toEqual({ event: "user_erase", error: "relayfacts_erase_failed" });
    // Only a five-character code is ever kept: anything else (a driver code, a long or lowercase string) is dropped.
    for (const code of ["ECONNRESET", "rh015", "RH0150", "user-1"]) {
      const db = fakeDb({ fail: { "SELECT sessions.relayfacts_user_erase_v1": Object.assign(new Error("x"), { code }) } });
      const logs: Record<string, unknown>[] = [];
      const err = await run({ ...ENV, SUMMARY_DECISION_RECORDS_ENABLED: "true" }, db, logs).then(() => null, (e: unknown) => e);
      expect((err as RelayfactsEraseFailed).sqlstate).toBeUndefined();
      expect(logs).toEqual([{ event: "user_erase_relayfacts", outcome: "failed" }]);
      expect(exitCodeForError(err)).toBe(4);
    }
  });

  test("RH040 from begin after a successful relayfacts step exits 2; the relayfacts counts were logged first", async () => {
    const db = fakeDb({ fail: { "SELECT sessions.operator_user_erase_begin": Object.assign(new Error("published"), { code: "RH040" }) } });
    const logs: Record<string, unknown>[] = [];
    const err = await run({ ...ENV, SUMMARY_DECISION_RECORDS_ENABLED: "true" }, db, logs).then(() => null, (e: unknown) => e);
    expect((err as { code?: string }).code).toBe("RH040");
    expect(exitCodeForError(err)).toBe(2);
    expect(logs).toEqual([{ event: "user_erase_relayfacts", ...COUNTS }]);
    expect(db.seen.filter((q) => q.text === "COMMIT").length).toBe(1);
  });

  test("the SQL port refuses a malformed result: not an object, a missing, extra, negative or fractional count", async () => {
    const req = { requestId: "r", orgId: "o", workspaceId: "w", userId: "u" };
    expect(await sqlUserEraseRelayfacts(fakeDb()).erase(req)).toEqual(COUNTS);
    for (const bad of [null, [], "x", { ...COUNTS, sessions: -1 }, { ...COUNTS, evidenceRows: 1.5 }, { ...COUNTS, extra: 0 },
      { sessions: 0, fencesAdded: 0, evidenceRows: 0, recordsDeleted: 0 }, { ...COUNTS, recordsRewritten: "1" }]) {
      const db = fakeDb({ relayfacts: bad });
      await expect(sqlUserEraseRelayfacts(db).erase(req)).rejects.toThrow("malformed result");
      // Checked before COMMIT: the transaction (and with it any fence) is rolled back, never committed.
      expect(db.seen.map((q) => q.text).slice(3)).toEqual(["SELECT sessions.relayfacts_user_erase_v1($1,$2,$3,$4) AS r", "ROLLBACK"]);
    }
    // Through runUserErase a malformed result is a failed relayfacts step: begin is never called.
    const db = fakeDb({ relayfacts: { ...COUNTS, sessions: -1 } });
    const err = await runUserErase(db, req, { keyRoot: "96bulls", eraser: eraser(), relayfacts: sqlUserEraseRelayfacts(db) }).then(() => null, (e: unknown) => e);
    expect(err).toBeInstanceOf(RelayfactsEraseFailed);
    expect(exitCodeForError(err)).toBe(4);
    expect(db.seen.some((q) => q.text.includes("operator_user_erase_begin"))).toBe(false);
  });
});
