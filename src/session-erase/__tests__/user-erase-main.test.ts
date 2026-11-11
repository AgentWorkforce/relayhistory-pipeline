import { describe, expect, test } from "bun:test";
import { CONFIRMATION, parseUserEraseRequest, userErasePrefixes, UserEraseRefused } from "../user-erase.ts";
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
});
