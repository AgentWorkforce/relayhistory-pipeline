import { describe, expect, test } from "bun:test";
import { CONFIRMATION, parseUserEraseRequest, runUserErase, USER_ERASE_LOCK_TIMEOUT_MS, USER_ERASE_STATEMENT_TIMEOUT_MS, userErasePrefixes, UserEraseRefused } from "../user-erase.ts";
import { exitCodeForError, exitCodeForResult, runUserEraseMain, settleMs, userEraseEnabled } from "../user-erase-main.ts";
import { liveMachineId } from "../../migration/neon-s3-erasure/live.ts";

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

  test("prefixes: the four layouts per origin, each under the user's own machine id", () => {
    const req = { requestId: "r", orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
    const m = liveMachineId("ws-a", "o1", "user-1");
    expect(userErasePrefixes("96bulls", req, ["o1"])).toEqual([
      `96bulls/org-a/buffered-batches/v1/ws=ws-a/m=${m}/`,
      `96bulls/org-a/live-batches/v1/ws=ws-a/m=${m}/`,
      `96bulls/org-a/v1/ws=ws-a/m=${m}/`,
      `96bulls/org-a/live-sessions/v1/ws=ws-a/m=${m}/`,
    ]);
    expect(userErasePrefixes("96bulls", req, [])).toEqual([]);
    expect(liveMachineId("ws-a", "o1", "user-2")).not.toBe(m);
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
