// Operations port (P2): verification against the cloud-class pinned keys,
// workspace binding from introspection, error mapping, bigints.
import { expect, test } from "bun:test";
import { SQL_ERROR_CODES, classifySqlError, httpForError, parseSqlJson } from "../installer.ts";
import { ORG, WS, call, cloudInstall, cloudRevoke, envelope, leaks, nativeKey, opsHarness, pgError } from "./helpers.ts";

const P = "/v1/acquisition/ops";
const statusBody = (over: Record<string, unknown> = {}) => ({ orgId: ORG, workspaceId: WS, source: "droid", sessionIds: ["sess-0001"], ...over });

test("install then revoke: verified, digests computed, SQL result passed through as {result}", async () => {
  const h = opsHarness();
  const r = await call(h.app, "POST", `${P}/install`, { token: "sh_wk_ops", body: envelope("install", cloudInstall()) });
  expect([r.status, r.json]).toEqual([200, { result: { outcome: "installed", grantId: "00000000-0000-4000-8000-000000000001", ownerEpoch: "1", revision: "1", state: "active" } }]);
  expect(h.sql.calls[0]!.p.requestSha256).toBe("fc501fbafdb4618311ff3ecc20b82e75f0d005e4a652a1a09492931613ebac29");
  const rv = await call(h.app, "POST", `${P}/revoke`, { token: "sh_wk_ops", body: envelope("revoke", cloudRevoke()) });
  expect([rv.status, rv.json.result.outcome]).toEqual([200, "draining"]);
});

test("ops port rejects a native-class key and a bad signature with 403, before SQL", async () => {
  const h = opsHarness();
  const native = envelope("install", cloudInstall({ ownerKind: "native", bindingUuid: "", installationId: "00000000-0000-4000-8000-000000000003", issuerKeyId: "native-key-1" }), nativeKey);
  const a = await call(h.app, "POST", `${P}/install`, { token: "sh_wk_ops", body: native });
  expect([a.status, a.json]).toEqual([403, { error: { code: "not_authorized" } }]);
  const good = envelope("install", cloudInstall());
  const flipped = good.signature[0] === "A" ? "B" + good.signature.slice(1) : "A" + good.signature.slice(1);
  const b = await call(h.app, "POST", `${P}/install`, { token: "sh_wk_ops", body: { ...good, signature: flipped } });
  expect([b.status, b.json]).toEqual([403, { error: { code: "not_authorized" } }]);
  // Cloud key used to claim ownerKind native, unknown key id: 403. Malformed envelope: 400.
  const c = await call(h.app, "POST", `${P}/install`, { token: "sh_wk_ops", body: envelope("install", cloudInstall({ issuerKeyId: "unknown-key" })) });
  expect(c.status).toBe(403);
  const d = await call(h.app, "POST", `${P}/install`, { token: "sh_wk_ops", body: { ...good, extra: 1 } });
  expect([d.status, d.json]).toEqual([400, { error: { code: "invalid_request" } }]);
  // Rebind is native-class only, so it can never verify on the ops port.
  const rb = envelope("rebind", { operationUuid: "00000000-0000-4000-8000-000000000021", orgId: ORG, workspaceId: WS, source: "droid", sessionId: "sess-0001", revision: "3",
    targetGrantId: "00000000-0000-4000-8000-000000000001", targetOwnerEpoch: "1", ownerUserId: "u", installationId: "00000000-0000-4000-8000-000000000003", oldOriginId: "a", newOriginId: "b", issuerKeyId: "test-key-1" });
  expect((await call(h.app, "POST", `${P}/rebind`, { token: "sh_wk_ops", body: rb })).status).toBe(403);
  expect(h.sql.calls.length).toBe(0);
});

test("target_mismatch: payload/status orgId or workspaceId differing from the token is 403 before SQL", async () => {
  const h = opsHarness();
  for (const over of [{ orgId: "other_org" }, { workspaceId: "other_ws" }]) {
    const r = await call(h.app, "POST", `${P}/install`, { token: "sh_wk_ops", body: envelope("install", cloudInstall(over)) });
    expect([r.status, r.json]).toEqual([403, { error: { code: "target_mismatch" } }]);
    const s = await call(h.app, "POST", `${P}/status`, { token: "sh_wk_ops", body: statusBody(over) });
    expect([s.status, s.json]).toEqual([403, { error: { code: "target_mismatch" } }]);
  }
  expect(h.sql.calls.length).toBe(0);
});

test("status: {result:{sessions}} with bigints exact, from jsonb numbers and from text", async () => {
  const h = opsHarness();
  for (const q of ["", '"']) {
    h.sql.statusText = `[{"sessionId":"sess-0001","state":"active","revision":${q}9007199254740993${q},"tombstoneRevision":${q}9223372036854775807${q},"ownerEpoch":${q}9223372036854775807${q},"ownerKind":"cloud","grantId":"g1","grantSha256":"secret-hash"}]`;
    const r = await call(h.app, "POST", `${P}/status`, { token: "sh_wk_ops", body: statusBody() });
    expect(r.status).toBe(200);
    expect(r.text).toContain('"revision":"9007199254740993"');
    expect(r.json).toEqual({ result: { sessions: [{ sessionId: "sess-0001", state: "active", revision: "9007199254740993", tombstoneRevision: "9223372036854775807", ownerEpoch: "9223372036854775807", ownerKind: "cloud", grantId: "g1" }] } });
  }
  // Mutation results too.
  expect(parseSqlJson('{"revision":9223372036854775807,"ownerEpoch":9007199254740993,"s":"a\\"1"}')).toEqual({ revision: "9223372036854775807", ownerEpoch: "9007199254740993", s: 'a"1' });
  expect(() => parseSqlJson('{"x":1.5}')).toThrow();
  const bad = await call(h.app, "POST", `${P}/status`, { token: "sh_wk_ops", body: statusBody({ sessionIds: Array.from({ length: 501 }, (_, i) => `s${i}`) }) });
  expect(bad.status).toBe(400);
});

test("every SQL error code maps per the contract table (ops shape)", async () => {
  for (const [sqlstate, codes] of Object.entries(SQL_ERROR_CODES)) {
    for (const code of codes) {
      const h = opsHarness();
      h.sql.failNext(pgError(sqlstate, code));
      const r = await call(h.app, "POST", `${P}/install`, { token: "sh_wk_ops", body: envelope("install", cloudInstall()) });
      const want =
        sqlstate === "22023" ? [400, { error: { code } }]
        : sqlstate === "42501" ? [403, { error: { code } }]
        : [409, { error: { code, retryable: code === "session_busy" } }];
      expect([code, r.status, r.json]).toEqual([code, ...want]);
    }
  }
  // Transport failure: 503 retryable. Unknown SQLSTATE: 500 defect. Unknown message under a contract SQLSTATE: defect.
  const transport = httpForError(Object.assign(new Error("connect ECONNREFUSED"), { code: "ECONNREFUSED" }), "ops");
  expect(transport).toEqual({ status: 503, body: { error: { code: "acquisition_unavailable", retryable: true } } });
  expect(httpForError(Object.assign(new Error("syntax"), { code: "42601" }), "ops").status).toBe(500);
  expect(httpForError(Object.assign(new Error("connection lost"), { code: "08006" }), "ops").status).toBe(503);
  expect(classifySqlError(Object.assign(new Error("acquisition: Not A Code"), { code: "55000" })).name).toBe("AcquisitionSqlDefect");
  // Native shape for the same refusal.
  expect(httpForError(pgError("55000", "owner_draining"), "native")).toEqual({ status: 409, body: { error: { code: "acquisition_refused", reason: "owner_draining", retryable: false } } });
});

test("no signature, key or bearer in ops log lines", async () => {
  const h = opsHarness();
  const env = envelope("install", cloudInstall());
  await call(h.app, "POST", `${P}/install`, { token: "sh_wk_ops", body: env });
  const lines = h.captured.lines.map((l) => JSON.stringify(l));
  expect(lines.length).toBe(1);
  expect(lines.some((l) => l.includes(env.signature) || l.includes("sh_wk_ops"))).toBe(false);
  expect(leaks(lines)).toEqual([]);
});
