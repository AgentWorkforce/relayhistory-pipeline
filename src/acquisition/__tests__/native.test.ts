// Native exchange (P3 + P3a): installation proof, claims, revocations, rebinds,
// status, durable cleanup and the reconciler, through the HTTP app.
import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { MAX_REQUEST_TARGET_BYTES } from "../app.ts";
import { NativeIssuer } from "../native-issuer.ts";
import { AcquisitionSql } from "../installer.ts";
import { call, intentKeyBytes, leaks, nativeHarness, newProof, pgError, principal, secrets } from "./helpers.ts";

const P = "/v1/acquisition/native";
type H = ReturnType<typeof nativeHarness>;

async function register(h: H, proof = newProof(), requestUuid = randomUUID(), token = "sh_at_alice", originId = "origin_a") {
  const r = await call(h.app, "POST", `${P}/installations`, { token, body: { requestUuid, originId, installationProof: proof } });
  return { r, proof, requestUuid, installationId: r.json?.installationId as string };
}
async function claim(h: H, inst: { installationId: string; proof: string }, over: Record<string, unknown> = {}, token = "sh_at_alice") {
  const body = { operationUuid: randomUUID(), installationId: inst.installationId, source: "droid", sessionId: "sess-1", originId: "origin_a", ...over };
  const r = await call(h.app, "POST", `${P}/claims`, { token, body, proof: inst.proof });
  if (typeof r.json?.grant === "string") secrets.add(r.json.grant);
  return { r, body };
}
const statusPath = (installationId: string, sessionIds = "sess-1") => `${P}/status?installationId=${installationId}&source=droid&sessionIds=${sessionIds}`;
function allLogs(h: H) {
  return h.captured.lines.map((l) => JSON.stringify(l));
}

// ------------------------------------------------------------ P3a

test("registration: idempotent by requestUuid + proof; different proof 409; bad proof 400; proof never echoed", async () => {
  const h = nativeHarness();
  const a = await register(h);
  expect(a.r.status).toBe(200);
  expect(a.r.json).toEqual({ installationId: a.installationId, originId: "origin_a", issuerKeyId: "native-key-1" });
  // Lost response: the same request + proof returns the same installationId.
  const again = await register(h, a.proof, a.requestUuid);
  expect([again.r.status, again.installationId]).toEqual([200, a.installationId]);
  const other = await register(h, newProof(), a.requestUuid);
  expect([other.r.status, other.r.json]).toEqual([409, { error: { code: "installation_proof_mismatch", retryable: false } }]);
  for (const bad of [undefined, "", "abc", Buffer.alloc(31).toString("base64url"), Buffer.alloc(33).toString("base64url"), Buffer.alloc(32).toString("base64") ]) {
    const body: Record<string, unknown> = { requestUuid: randomUUID(), originId: "o" };
    if (bad !== undefined) body.installationProof = bad;
    const r = await call(h.app, "POST", `${P}/installations`, { token: "sh_at_alice", body });
    expect([r.status, r.json]).toEqual([400, { error: { code: "invalid_request" } }]);
  }
  // Only the hash is stored.
  const stored = [...h.store.installations.values()].map((i) => JSON.stringify(i)).join("");
  expect(stored.includes(a.proof)).toBe(false);
  expect(a.r.text.includes(a.proof)).toBe(false);
  expect(leaks(allLogs(h))).toEqual([]);
});

test("same requestUuid by another user of the workspace is a proof mismatch (hash binds org/ws/user)", async () => {
  const h = nativeHarness();
  h.auth.tokens.set("sh_at_bob", principal({ userId: "user_bob" }));
  const a = await register(h);
  const b = await register(h, a.proof, a.requestUuid, "sh_at_bob");
  expect(b.r.status).toBe(409);
});

test("wrong or missing proof: 403 installation_not_proven with zero SQL and zero intent-store reads", async () => {
  const h = nativeHarness();
  const a = await register(h);
  const { r: ok } = await claim(h, a);
  expect(ok.status).toBe(200);
  const sqlBefore = h.sql.calls.length, intentBefore = h.store.intentCalls;
  const wrong = { installationId: a.installationId, proof: newProof() };
  const attempts = [
    await claim(h, wrong),
    await claim(h, { installationId: a.installationId, proof: "" }),
    await claim(h, { installationId: randomUUID(), proof: a.proof }), // unknown installation
    { r: await call(h.app, "GET", statusPath(a.installationId), { token: "sh_at_alice", proof: wrong.proof }) },
    { r: await call(h.app, "POST", `${P}/revocations`, { token: "sh_at_alice", proof: wrong.proof, body: { operationUuid: randomUUID(), installationId: a.installationId, source: "droid", sessionId: "sess-1", targetGrantId: randomUUID() } }) },
    { r: await call(h.app, "POST", `${P}/rebinds`, { token: "sh_at_alice", proof: wrong.proof, body: { operationUuid: randomUUID(), installationId: a.installationId, source: "droid", sessionId: "sess-1", grantId: randomUUID(), originId: "o2", grant: "rhg_" + "0".repeat(64) } }) },
  ];
  for (const { r } of attempts) expect([r.status, r.json]).toEqual([403, { error: { code: "installation_not_proven" } }]);
  expect(h.sql.calls.length).toBe(sqlBefore);
  expect(h.store.intentCalls).toBe(intentBefore);
  expect(leaks(allLogs(h))).toEqual([]);
});

test("a second installation of the same user with known UUIDs gets no G, no replay and never mine=true", async () => {
  const h = nativeHarness();
  const mine = await register(h);
  const first = await claim(h, mine);
  expect(first.r.json.outcome).toBe("installed");
  const G = first.r.json.grant as string;
  const thief = await register(h, newProof(), randomUUID(), "sh_at_alice", "origin_b");
  // Its own proof, my installationId: not proven.
  const crossed = await claim(h, { installationId: mine.installationId, proof: thief.proof }, { operationUuid: first.body.operationUuid });
  expect(crossed.r.status).toBe(403);
  // Its own installation, my operationUuid: conflict, no grant.
  const replay = await claim(h, thief, { operationUuid: first.body.operationUuid });
  expect([replay.r.status, replay.r.json]).toEqual([409, { error: { code: "acquisition_refused", reason: "operation_conflict", retryable: false } }]);
  expect(replay.r.text.includes(G)).toBe(false);
  // Status: grant id hidden, mine=false.
  const st = await call(h.app, "GET", statusPath(thief.installationId), { token: "sh_at_alice", proof: thief.proof });
  expect(st.status).toBe(200);
  expect(st.json.result.sessions[0].mine).toBe(false);
  expect(st.json.result.sessions[0].grantId).toBe(null);
  // Revoke or rebind my grant: not this installation's grant -> 403 not_installer (never not_authorized:
  // Desktop reads that as a missing rth:acquire scope).
  const rv = await call(h.app, "POST", `${P}/revocations`, { token: "sh_at_alice", proof: thief.proof, body: { operationUuid: randomUUID(), installationId: thief.installationId, source: "droid", sessionId: "sess-1", targetGrantId: first.r.json.grantId } });
  expect([rv.status, rv.json]).toEqual([403, { error: { code: "not_installer" } }]);
  const rb = await call(h.app, "POST", `${P}/rebinds`, { token: "sh_at_alice", proof: thief.proof, body: { operationUuid: randomUUID(), installationId: thief.installationId, source: "droid", sessionId: "sess-1", grantId: first.r.json.grantId, originId: "origin_b", grant: G } });
  expect([rb.status, rb.json]).toEqual([403, { error: { code: "not_installer" } }]);
  // An unknown grant id is not_installer too (no existence oracle beyond "not yours").
  const unknown = await call(h.app, "POST", `${P}/revocations`, { token: "sh_at_alice", proof: thief.proof, body: { operationUuid: randomUUID(), installationId: thief.installationId, source: "droid", sessionId: "sess-1", targetGrantId: randomUUID() } });
  expect([unknown.status, unknown.json]).toEqual([403, { error: { code: "not_installer" } }]);
  // And my own status says mine=true.
  const own = await call(h.app, "GET", statusPath(mine.installationId), { token: "sh_at_alice", proof: mine.proof });
  expect(own.json.result.sessions[0]).toMatchObject({ mine: true, grantId: first.r.json.grantId, ownerEpoch: "1", revision: "1" });
});

// ------------------------------------------------------------ claims

test("claim: installed returns G once; replay returns the same G without re-signing; different payload 409", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  const first = await claim(h, inst);
  expect(first.r.status).toBe(200);
  expect(first.r.json).toMatchObject({ outcome: "installed", grantId: first.body.operationUuid, ownerEpoch: "1", revision: "1", expiresAt: null });
  expect(first.r.json.grant).toMatch(/^rhg_[0-9a-f]{64}$/);
  const installCall = h.sql.calls.find((c) => c.fn === "install")!;
  expect(installCall.p).toMatchObject({ expectedRevision: "0", expectedOwnerEpoch: "0", expectedPredecessorGrantId: "", ownerKind: "native", ownerUserId: "user_test", installationId: inst.installationId, issuerKeyId: "native-key-1" });
  // The reserved revoke was signed and committed with the install's applied mark.
  expect(h.store.calls).toContain("createInstallIntent");
  const stored = h.store.intents.get(first.body.operationUuid)!;
  expect([...h.store.intents.values()].filter((i) => i.kind === "revoke" && i.status === "reserved" && i.targetGrantId === first.body.operationUuid).length).toBe(1);
  expect(Buffer.from(stored.grantCiphertext!).toString("utf8").includes(first.r.json.grant)).toBe(false);
  const sqlBefore = h.sql.calls.length;
  const replay = await call(h.app, "POST", `${P}/claims`, { token: "sh_at_alice", proof: inst.proof, body: first.body });
  expect(replay.json).toEqual({ ...first.r.json, outcome: "replay" });
  expect(h.sql.calls.length).toBe(sqlBefore); // applied: no SQL, no new signature
  expect(Buffer.from(h.store.intents.get(first.body.operationUuid)!.signature).equals(Buffer.from(stored.signature))).toBe(true);
  const conflict = await claim(h, inst, { operationUuid: first.body.operationUuid, sessionId: "sess-2" });
  expect([conflict.r.status, conflict.r.json]).toEqual([409, { error: { code: "acquisition_refused", reason: "operation_conflict", retryable: false } }]);
  expect(leaks([...allLogs(h), JSON.stringify(h.sql.calls)])).toEqual([]);
});

test("claim retry after session_busy resends the stored A/S byte-for-byte (never re-signs)", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  h.sql.failNext(pgError("40001", "session_busy"), "install");
  const busy = await claim(h, inst);
  expect([busy.r.status, busy.r.json]).toEqual([409, { error: { code: "acquisition_refused", reason: "session_busy", retryable: true } }]);
  // A replay before the backoff elapses is not sent (it would spend the attempt budget): 503, no SQL call.
  const early = await call(h.app, "POST", `${P}/claims`, { token: "sh_at_alice", proof: inst.proof, body: busy.body });
  expect([early.status, early.json, early.headers.get("Retry-After")]).toEqual([503, { error: { code: "acquisition_unavailable", retryable: true } }, "5"]);
  expect(h.sql.calls.filter((c) => c.fn === "install").length).toBe(1);
  h.clock.now += 30_000; // retryDelayMs(1)
  h.sql.failNext(Object.assign(new Error("connect ECONNREFUSED"), { code: "ECONNREFUSED" }), "install");
  const down = await call(h.app, "POST", `${P}/claims`, { token: "sh_at_alice", proof: inst.proof, body: busy.body });
  expect([down.status, down.json]).toEqual([503, { error: { code: "acquisition_unavailable", retryable: true } }]);
  h.clock.now += 60_000; // retryDelayMs(2)
  const ok = await call(h.app, "POST", `${P}/claims`, { token: "sh_at_alice", proof: inst.proof, body: busy.body });
  expect(ok.status).toBe(200);
  expect(h.store.intents.get(busy.body.operationUuid as string)!.attempts).toBe(2);
  const installs = h.sql.calls.filter((c) => c.fn === "install");
  expect(installs.length).toBe(3);
  expect(new Set(installs.map((c) => c.p.evidenceSha256)).size).toBe(1);
  expect(new Set(installs.map((c) => c.p.requestSha256)).size).toBe(1);
});

test("claim: a terminal SQL refusal is recorded, reserves no revoke and replays as the same refusal", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  h.sql.failNext(pgError("55000", "owner_draining"), "install");
  const r = await claim(h, inst);
  expect([r.r.status, r.r.json]).toEqual([409, { error: { code: "acquisition_refused", reason: "owner_draining", retryable: false } }]);
  const again = await call(h.app, "POST", `${P}/claims`, { token: "sh_at_alice", proof: inst.proof, body: r.body });
  expect(again.json.error.reason).toBe("owner_draining");
  expect(h.sql.calls.filter((c) => c.fn === "install").length).toBe(1);
  expect([...h.store.intents.values()].filter((i) => i.kind === "revoke").length).toBe(0);
});

test("F8: revoking a grant whose install was refused is 403 not_owner; another installation's grant is 403 not_installer", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  h.sql.failNext(pgError("55000", "owner_draining"), "install");
  const c = await claim(h, inst);
  expect(c.r.status).toBe(409);
  const body = { operationUuid: randomUUID(), installationId: inst.installationId, source: "droid", sessionId: "sess-1", targetGrantId: c.body.operationUuid };
  const r = await call(h.app, "POST", `${P}/revocations`, { token: "sh_at_alice", proof: inst.proof, body });
  expect([r.status, r.json]).toEqual([403, { error: { code: "not_owner" } }]);
  const other = await register(h, newProof(), randomUUID(), "sh_at_alice", "origin_b");
  const r2 = await call(h.app, "POST", `${P}/revocations`, { token: "sh_at_alice", proof: other.proof, body: { ...body, operationUuid: randomUUID(), installationId: other.installationId } });
  expect([r2.status, r2.json]).toEqual([403, { error: { code: "not_installer" } }]);
});

test("native error mapping: 22023 -> 400, 42501 -> 403, 55000/40001 -> 409 acquisition_refused", async () => {
  const cases: Array<[string, string, number, unknown]> = [
    ["22023", "request_digest_mismatch", 400, { error: { code: "request_digest_mismatch" } }],
    ["42501", "not_owner", 403, { error: { code: "not_owner" } }],
    ["55000", "handover_not_ready", 409, { error: { code: "acquisition_refused", reason: "handover_not_ready", retryable: false } }],
    ["40001", "revision_cas_lost", 409, { error: { code: "acquisition_refused", reason: "revision_cas_lost", retryable: false } }],
    ["40001", "predecessor_mismatch", 409, { error: { code: "acquisition_refused", reason: "predecessor_mismatch", retryable: false } }],
  ];
  for (const [state, code, status, body] of cases) {
    const h = nativeHarness();
    const inst = await register(h);
    h.sql.failNext(pgError(state, code), "install");
    const r = await claim(h, inst);
    expect([code, r.r.status, r.r.json]).toEqual([code, status, body]);
  }
});

// ------------------------------------------------------------ revocations + rebinds

test("revocation activates the reserved pre-signed revoke; replay by operationUuid", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  const c = await claim(h, inst);
  const reserved = [...h.store.intents.values()].find((i) => i.kind === "revoke")!;
  const body = { operationUuid: randomUUID(), installationId: inst.installationId, source: "droid", sessionId: "sess-1", targetGrantId: c.r.json.grantId };
  const r = await call(h.app, "POST", `${P}/revocations`, { token: "sh_at_alice", proof: inst.proof, body });
  expect([r.status, r.json]).toEqual([200, { outcome: "draining", revision: "2", ownerEpoch: "1", state: "draining" }]);
  const sent = h.sql.calls.filter((c) => c.fn === "revoke");
  expect(sent.length).toBe(1);
  expect(sent[0]!.p.evidenceSha256).toBe(reserved.evidenceSha256); // the reserved bytes, not a new signature
  const again = await call(h.app, "POST", `${P}/revocations`, { token: "sh_at_alice", proof: inst.proof, body });
  expect(again.json.outcome).toBe("replay");
  const other = await call(h.app, "POST", `${P}/revocations`, { token: "sh_at_alice", proof: inst.proof, body: { ...body, targetGrantId: randomUUID() } });
  expect(other.json.error.reason).toBe("operation_conflict");
  const scope = await call(h.app, "POST", `${P}/revocations`, { token: "sh_at_alice", proof: inst.proof, body: { ...body, operationUuid: randomUUID(), sessionId: "sess-9" } });
  expect([scope.status, scope.json]).toEqual([403, { error: { code: "target_mismatch" } }]);
});

test("rebind requires G, refuses the same origin, signs once", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  const c = await claim(h, inst);
  const base = { operationUuid: randomUUID(), installationId: inst.installationId, source: "droid", sessionId: "sess-1", grantId: c.r.json.grantId, originId: "origin_new" };
  const noG = await call(h.app, "POST", `${P}/rebinds`, { token: "sh_at_alice", proof: inst.proof, body: base });
  expect(noG.status).toBe(400);
  const wrongG = await call(h.app, "POST", `${P}/rebinds`, { token: "sh_at_alice", proof: inst.proof, body: { ...base, grant: "rhg_" + "1".repeat(64) } });
  expect([wrongG.status, wrongG.json]).toEqual([403, { error: { code: "not_owner" } }]); // owner proof (G) failed
  const same = await call(h.app, "POST", `${P}/rebinds`, { token: "sh_at_alice", proof: inst.proof, body: { ...base, originId: "origin_a", grant: c.r.json.grant } });
  expect(same.status).toBe(400);
  const ok = await call(h.app, "POST", `${P}/rebinds`, { token: "sh_at_alice", proof: inst.proof, body: { ...base, grant: c.r.json.grant } });
  expect([ok.status, ok.json]).toEqual([200, { outcome: "rebound", revision: "2", ownerEpoch: "1" }]);
  const rebind = h.sql.calls.find((x) => x.fn === "rebind_origin")!;
  expect(rebind.p).toMatchObject({ oldOriginId: "origin_a", newOriginId: "origin_new", targetGrantId: c.r.json.grantId, ownerUserId: "user_test" });
  const replay = await call(h.app, "POST", `${P}/rebinds`, { token: "sh_at_alice", proof: inst.proof, body: { ...base, grant: c.r.json.grant } });
  expect(replay.json.outcome).toBe("replay");
  expect(h.sql.calls.filter((x) => x.fn === "rebind_origin").length).toBe(1);
  expect(leaks(allLogs(h))).toEqual([]);
});

// ------------------------------------------------------------ bigints

test("native status and claim keep bigints exact as decimal strings (jsonb numbers and text)", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  h.sql.statusText = `[{"sessionId":"sess-1","state":"active","revision":9007199254740993,"tombstoneRevision":9223372036854775807,"ownerEpoch":"9223372036854775807","ownerKind":"cloud","grantId":null}]`;
  const st = await call(h.app, "GET", statusPath(inst.installationId), { token: "sh_at_alice", proof: inst.proof });
  expect(st.status).toBe(200);
  expect(st.json).toEqual({ result: { sessions: [{ sessionId: "sess-1", state: "active", revision: "9007199254740993", tombstoneRevision: "9223372036854775807", ownerEpoch: "9223372036854775807", ownerKind: "cloud", grantId: null, mine: false }] } });
  // A claim derives its lineage from the exact values (int64 max itself cannot be advanced: 400, see below).
  h.sql.statusText = `[{"sessionId":"sess-1","state":"active","revision":9007199254740993,"tombstoneRevision":null,"ownerEpoch":"9007199254740993","ownerKind":"cloud","grantId":null}]`;
  const c = await claim(h, inst);
  expect([c.r.status, c.r.json?.error]).toEqual([200, undefined]);
  const p = h.sql.calls.find((x) => x.fn === "install")!.p;
  expect(p).toMatchObject({ expectedRevision: "9007199254740993", revision: "9007199254740994", expectedOwnerEpoch: "9007199254740993" });
  // At int64 max the reserved revoke's epoch would overflow: refused 400 before any intent is written.
  h.sql.statusText = `[{"sessionId":"sess-2","state":"active","revision":1,"tombstoneRevision":null,"ownerEpoch":9223372036854775807,"ownerKind":"cloud","grantId":null}]`;
  const writes = h.store.intents.size;
  const max = await claim(h, inst, { sessionId: "sess-2" });
  expect([max.r.status, max.r.json]).toEqual([400, { error: { code: "invalid_request" } }]);
  expect(h.store.intents.size).toBe(writes);
});

// ------------------------------------------------------------ durable cleanup

test("session removal and retirement activate the reserved revoke with no network or private key; reconciler sends it", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  const c1 = await claim(h, inst);
  const c2 = await claim(h, inst, { sessionId: "sess-2" });
  // An issuer whose signer and SQL both throw can still activate (local row update only).
  const offline = new NativeIssuer({
    store: h.store,
    sql: new AcquisitionSql({ call: async () => { throw new Error("no network"); } }),
    signer: { issuerKeyId: "native-key-1", publicKey: h.signer.publicKey, sign: () => { throw new Error("no private key"); } },
    intentKey: intentKeyBytes,
  });
  const sqlBefore = h.sql.calls.length;
  expect(await offline.removeSession(inst.installationId, c1.r.json.grantId)).toBe(true);
  expect(h.sql.calls.length).toBe(sqlBefore);
  // Still selected: the reconciler skips it and sends nothing.
  expect(await h.issuer.reconcile({ stillSelected: async () => true })).toEqual({ sent: 0, skipped: 1, retry: 0, stalled: 0, stale: 0 });
  expect(h.sql.calls.length).toBe(sqlBefore);
  expect(await h.issuer.reconcile()).toEqual({ sent: 0, skipped: 0, retry: 0, stalled: 0, stale: 0 }); // a skip defers it, uncounted
  h.clock.now += 30_000;
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  const revoke = h.sql.calls.at(-1)!;
  expect([revoke.fn, revoke.p.targetGrantId]).toEqual(["revoke", c1.r.json.grantId]);
  // Retirement activates the rest in one step; reconciler sends only revokes.
  expect(await offline.retireInstallation(inst.installationId)).toBe(1);
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  expect(h.sql.calls.at(-1)!.p.targetGrantId).toBe(c2.r.json.grantId);
  expect(h.sql.calls.filter((x) => x.fn === "install").length).toBe(2);
  // A retired installation is no longer proven.
  expect((await claim(h, inst, { sessionId: "sess-3" })).r.status).toBe(403);
});

test("no G, proof, bearer, intent key or private key in any captured log line", async () => {
  const h = nativeHarness();
  secrets.add("sh_at_alice");
  const inst = await register(h);
  const c = await claim(h, inst);
  await call(h.app, "POST", `${P}/rebinds`, { token: "sh_at_alice", proof: inst.proof, body: { operationUuid: randomUUID(), installationId: inst.installationId, source: "droid", sessionId: "sess-1", grantId: c.r.json.grantId, originId: "o2", grant: c.r.json.grant } });
  await claim(h, { installationId: inst.installationId, proof: newProof() });
  expect(h.captured.lines.length).toBeGreaterThan(3);
  expect(leaks(allLogs(h))).toEqual([]);
  secrets.delete("sh_at_alice");
});

// ------------------------------------------------------------ §9 key retention

test("key rotation must retain the native key while a reserved revoke still references it", async () => {
  const { assertKeysRetained } = await import("../key-registry.ts");
  const { registry } = await import("./helpers.ts");
  const h = nativeHarness();
  const inst = await register(h);
  const c = await claim(h, inst);
  expect(c.r.status).toBe(200);
  // The install is applied, but its reserved pre-signed revoke (native-key-1) is outstanding.
  expect([...(await h.store.outstandingIssuerKeyIds())]).toEqual(["native-key-1"]);
  const rotated = new Map([...registry].filter(([id]) => id !== "native-key-1"));
  const outstanding = await h.store.outstandingIssuerKeyIds();
  expect(() => assertKeysRetained(rotated, outstanding, "native")).toThrow(/must retain native issuer keys .*native-key-1/);
  const reclassed = new Map([...rotated, ["native-key-1", { ...registry.get("native-key-1")!, ownerKind: "cloud" as const }]]);
  expect(() => assertKeysRetained(reclassed, ["native-key-1"], "native")).toThrow(/native-key-1/);
  assertKeysRetained(registry, await h.store.outstandingIssuerKeyIds(), "native");
  // Once the reserved revoke has been activated and sent, nothing references the key: it may be dropped.
  expect(await h.issuer.retireInstallation(inst.installationId)).toBe(1);
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  expect((await h.store.outstandingIssuerKeyIds()).size).toBe(0);
  assertKeysRetained(rotated, await h.store.outstandingIssuerKeyIds(), "native");
});

test("Postgres store extracts issuerKeyId as the last U+001F field (PGlite)", async () => {
  const { PGlite } = await import("../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js");
  const { PgAcquisitionStore, assertionIssuerKeyId } = await import("../intent-store.ts");
  const db = new PGlite();
  try {
    await db.exec("CREATE SCHEMA sessions; CREATE TABLE sessions.ingest_acquisition_native_intents(assertion bytea, status text)");
    const a = Buffer.from(["relayhistory-acquisition-assertion-revoke-v1", "op", "org", "", "native-key-1"].join("\u001f"));
    const b = Buffer.from(["relayhistory-acquisition-assertion-install-v1", "op2", "native-key-0"].join("\u001f"));
    const c = Buffer.from(["relayhistory-acquisition-assertion-install-v1", "op3", "native-key-9"].join("\u001f"));
    await db.query("INSERT INTO sessions.ingest_acquisition_native_intents VALUES ($1,'reserved'),($2,'pending'),($3,'applied')", [a, b, c]);
    const store = new PgAcquisitionStore({ query: (text: string, params?: unknown[]) => db.query(text, params) as never } as never);
    expect([...(await store.outstandingIssuerKeyIds())].sort()).toEqual(["native-key-0", "native-key-1"]);
    expect(assertionIssuerKeyId(a)).toBe("native-key-1");
  } finally {
    await db.close();
  }
});

// ------------------------------------------------------------ reserved revoke epoch + bounded retries

/** A's fields after the domain (U+001F split); revoke: [5] revision, [7] targetOwnerEpoch. */
const fieldsOf = (A: Uint8Array) => Buffer.from(A).toString("utf8").split("\u001f").slice(1);
const down = () => Object.assign(new Error("connect ECONNREFUSED"), { code: "ECONNREFUSED" });

test("reserved revoke is signed against the epoch the install RESULT reports (the design's exactly expectedOwnerEpoch+1)", async () => {
  // The design mandates expectedOwnerEpoch = current and a landed epoch of exactly +1, so the
  // reserved revoke's epoch is deterministic; the issuer still signs from the result's epoch.
  const h = nativeHarness();
  const inst = await register(h);
  const c = await claim(h, inst);
  expect(h.sql.calls.find((x) => x.fn === "install")!.p.expectedOwnerEpoch).toBe("0");
  expect([c.r.status, c.r.json.ownerEpoch]).toEqual([200, "1"]);
  expect(h.store.intents.get(c.body.operationUuid)!.reservation).toBe("signed");
  const reserved = [...h.store.intents.values()].filter((i) => i.kind === "revoke");
  expect(reserved.length).toBe(1);
  expect([reserved[0]!.status, fieldsOf(reserved[0]!.assertion)[7], fieldsOf(reserved[0]!.assertion)[5]]).toEqual(["reserved", "1", "2"]);
  const body = { operationUuid: randomUUID(), installationId: inst.installationId, source: "droid", sessionId: "sess-1", targetGrantId: c.body.operationUuid };
  const r = await call(h.app, "POST", `${P}/revocations`, { token: "sh_at_alice", proof: inst.proof, body });
  expect(r.status).toBe(200);
  expect(h.sql.calls.filter((x) => x.fn === "revoke").map((x) => x.p.targetOwnerEpoch)).toEqual(["1"]);
});

test("F6 no_epoch: the install stays pending cleanup debt (503 retryable, key kept, cancel kept) and finalizes once a revoke can be reserved", async () => {
  const h = nativeHarness();
  h.sql.installEpoch = () => null;
  const inst = await register(h);
  const c = await claim(h, inst);
  const op = c.body.operationUuid;
  expect([c.r.status, c.r.json]).toEqual([503, { error: { code: "acquisition_unavailable", retryable: true } }]);
  expect(h.store.intents.get(op)!).toMatchObject({ status: "pending", reservation: "no_epoch", attempts: 1, leaseUntil: null, leaseToken: null, result: null });
  expect([...h.store.intents.values()].filter((i) => i.kind === "revoke").length).toBe(0);
  expect([...(await h.store.outstandingIssuerKeyIds())]).toEqual([h.signer.issuerKeyId]); // the debt keeps the issuer key
  const replay = await call(h.app, "POST", `${P}/claims`, { token: "sh_at_alice", proof: inst.proof, body: c.body });
  expect([replay.status, replay.json.error.code]).toEqual([503, "acquisition_unavailable"]); // backing off: not resent
  // Retirement lands while it is debt: recorded on the install (cancel ordering kept).
  expect(await h.issuer.retireInstallation(inst.installationId)).toBe(1);
  expect(h.store.intents.get(op)!.cancelReason).toBe("installation_retired");
  h.clock.now += 30_000;
  expect(await h.issuer.reconcile()).toEqual({ sent: 0, skipped: 0, retry: 1, stalled: 0, stale: 0 });
  expect(h.store.intents.get(op)!).toMatchObject({ status: "pending", attempts: 2, reservation: "no_epoch" });
  // The authority now reports the epoch: the same bytes finalize it and the revoke is created active.
  h.sql.installed.set(op, h.sql.installed.get(op)!.replace('"revision"', '"ownerEpoch":1,"revision"'));
  h.clock.now += 60_000;
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  expect(h.store.intents.get(op)!).toMatchObject({ status: "applied", reservation: "signed", leaseToken: null });
  const revoke = [...h.store.intents.values()].find((i) => i.kind === "revoke")!;
  expect([revoke.status, revoke.activationReason]).toEqual(["pending", "installation_retired"]);
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  expect(h.sql.calls.filter((x) => x.fn === "revoke").map((x) => [x.p.targetGrantId, x.p.targetOwnerEpoch])).toEqual([[op, "1"]]);
  const installs = h.sql.calls.filter((x) => x.fn === "install");
  expect([installs.length, new Set(installs.map((x) => x.p.evidenceSha256)).size]).toEqual([3, 1]); // never re-signed
  expect((await h.store.outstandingIssuerKeyIds()).size).toBe(0);
});

test("F6 sign_failed: a signer failure keeps the install as debt; a session removal is recorded and becomes the revoke's reason", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  // The install is signed at claim time; the reserved revoke's signature fails (an issuer sharing store and SQL).
  let failSign = false;
  const flaky = new NativeIssuer({
    store: h.store, sql: new AcquisitionSql(h.sql), intentKey: intentKeyBytes, now: () => h.clock.now,
    signer: { issuerKeyId: h.signer.issuerKeyId, publicKey: h.signer.publicKey, sign: (A: Uint8Array) => { if (failSign) throw new Error("hsm unavailable"); return h.signer.sign(A); } },
  });
  const installation = (await h.store.getInstallation(inst.installationId))!;
  const op = randomUUID();
  const hook = h.sql.hold("install");
  const pending = flaky.claim(principal(), installation, { operationUuid: op, installationId: inst.installationId, source: "droid", sessionId: "sess-1", originId: "origin_a" });
  await hook.reached;
  failSign = true;
  hook.release();
  await expect(pending).rejects.toMatchObject({ code: "acquisition_unavailable" });
  expect(h.store.intents.get(op)!).toMatchObject({ status: "pending", reservation: "sign_failed", attempts: 1 });
  expect([...(await h.store.outstandingIssuerKeyIds())]).toEqual([h.signer.issuerKeyId]);
  expect(await h.issuer.removeSession(inst.installationId, op)).toBe(true);
  failSign = false;
  h.clock.now += 30_000;
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  const revoke = [...h.store.intents.values()].find((i) => i.kind === "revoke")!;
  expect([revoke.status, revoke.activationReason]).toEqual(["pending", "session_removed"]);
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  expect((await h.store.outstandingIssuerKeyIds()).size).toBe(0);
});

test("F6 stalled debt: an explicit revoke signs against the authority row, then the install resend finalizes against that revoke", async () => {
  const h = nativeHarness();
  h.sql.installEpoch = () => null;
  const inst = await register(h);
  const c = await claim(h, inst);
  const op = c.body.operationUuid;
  const body = { operationUuid: randomUUID(), installationId: inst.installationId, source: "droid", sessionId: "sess-1", targetGrantId: op };
  const busy = await call(h.app, "POST", `${P}/revocations`, { token: "sh_at_alice", proof: inst.proof, body });
  expect([busy.status, busy.json.error.reason]).toEqual([409, "session_busy"]); // debt, not yet stalled
  for (let i = 0; i < 8; i++) {
    h.clock.now += 3_600_000;
    await h.issuer.reconcile();
  }
  expect(h.store.intents.get(op)!.status).toBe("pending");
  const r = await call(h.app, "POST", `${P}/revocations`, { token: "sh_at_alice", proof: inst.proof, body });
  expect([r.status, r.json.outcome]).toEqual([200, "draining"]);
  expect(h.sql.calls.filter((x) => x.fn === "revoke").map((x) => [x.p.targetOwnerEpoch, x.p.revision])).toEqual([["1", "2"]]);
  h.clock.now += 3_600_000;
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 }); // the existing revoke covers it
  expect(h.store.intents.get(op)!.status).toBe("applied");
  expect((await h.store.outstandingIssuerKeyIds()).size).toBe(0);
});

test("reconciler resends a pending install and a pending rebind (same bytes, after backoff)", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  h.sql.failNext(down(), "install");
  const c = await claim(h, inst);
  expect(c.r.status).toBe(503);
  expect(h.store.intents.get(c.body.operationUuid)!).toMatchObject({ status: "pending", attempts: 1 });
  expect(await h.issuer.reconcile()).toEqual({ sent: 0, skipped: 0, retry: 0, stalled: 0, stale: 0 }); // backing off
  h.clock.now += 30_000;
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  const installs = h.sql.calls.filter((x) => x.fn === "install");
  expect([installs.length, new Set(installs.map((x) => x.p.evidenceSha256)).size]).toEqual([2, 1]);
  expect([...h.store.intents.values()].filter((i) => i.kind === "revoke" && i.status === "reserved").length).toBe(1);
  const replay = await call(h.app, "POST", `${P}/claims`, { token: "sh_at_alice", proof: inst.proof, body: c.body });
  expect([replay.status, replay.json.outcome]).toEqual([200, "replay"]);
  secrets.add(replay.json.grant);

  const base = { installationId: inst.installationId, source: "droid", sessionId: "sess-1", grantId: c.body.operationUuid, originId: "origin_new", grant: replay.json.grant };
  h.sql.failNext(down(), "rebind_origin");
  const first = { ...base, operationUuid: randomUUID() };
  expect((await call(h.app, "POST", `${P}/rebinds`, { token: "sh_at_alice", proof: inst.proof, body: first })).status).toBe(503);
  const other = await call(h.app, "POST", `${P}/rebinds`, { token: "sh_at_alice", proof: inst.proof, body: { ...base, operationUuid: randomUUID() } });
  expect(other.json.error.reason).toBe("session_busy");
  h.clock.now += 30_000;
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  const again = await call(h.app, "POST", `${P}/rebinds`, { token: "sh_at_alice", proof: inst.proof, body: first });
  expect([again.status, again.json.outcome]).toEqual([200, "replay"]);
  expect(h.sql.calls.filter((x) => x.fn === "rebind_origin").length).toBe(2);
});

test("stalled, never terminal: an install failing MAX_SEND_ATTEMPTS times keeps its key and keeps retrying (alerted), replays answer 503, a revoke still proceeds", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  h.sql.failNext(down(), "install");
  const c = await claim(h, inst);
  expect(c.r.status).toBe(503);
  expect([...(await h.store.outstandingIssuerKeyIds())]).toEqual(["native-key-1"]);
  const results = [];
  for (let i = 0; i < 7; i++) {
    h.clock.now += 3_600_000;
    h.sql.failNext(pgError("40001", "session_busy"), "install");
    results.push(await h.issuer.reconcile());
  }
  expect(results.at(-2)).toEqual({ sent: 0, skipped: 0, retry: 1, stalled: 0, stale: 0 });
  expect(results.at(-1)).toEqual({ sent: 0, skipped: 0, retry: 1, stalled: 1, stale: 0 }); // the caller alerts
  expect(h.store.intents.get(c.body.operationUuid)!).toMatchObject({ status: "pending", attempts: 8, leaseUntil: null });
  // Its SQL may have committed: the intent, its G and its key stay (G recovery by replay remains possible).
  expect(h.store.intents.get(c.body.operationUuid)!.grantCiphertext).not.toBeNull();
  expect([...(await h.store.outstandingIssuerKeyIds())]).toEqual(["native-key-1"]);
  // Still retried, at the 1 h cap.
  h.clock.now += 3_599_999;
  expect(await h.issuer.reconcile()).toEqual({ sent: 0, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  h.clock.now += 1;
  h.sql.failNext(down(), "install");
  expect(await h.issuer.reconcile()).toEqual({ sent: 0, skipped: 0, retry: 1, stalled: 1, stale: 0 });
  expect(h.sql.calls.filter((x) => x.fn === "install").length).toBe(9);
  // A replay before it is due answers retryable 503 (no attempt spent), never a terminal refusal.
  const replay = await call(h.app, "POST", `${P}/claims`, { token: "sh_at_alice", proof: inst.proof, body: c.body });
  expect([replay.status, replay.json]).toEqual([503, { error: { code: "acquisition_unavailable", retryable: true } }]);
  expect(h.store.intents.get(c.body.operationUuid)!.attempts).toBe(9);
  // Revoke is not session_busy forever. The authority does not hold this grant -> 409 grant_not_installed,
  // and the exclude is recorded on the install so a late apply creates its revoke active.
  const body = { operationUuid: randomUUID(), installationId: inst.installationId, source: "droid", sessionId: "sess-1", targetGrantId: c.body.operationUuid };
  const none = await call(h.app, "POST", `${P}/revocations`, { token: "sh_at_alice", proof: inst.proof, body });
  expect([none.status, none.json.error.reason]).toEqual([409, "grant_not_installed"]);
  expect(h.store.intents.get(c.body.operationUuid)!.cancelReason).toBe("explicit_exclude");
  // The grant did land (the answer was lost): the revoke signs against the authority row and is sent.
  h.sql.sessions.set("droid/sess-1", { state: "active", revision: "1", tombstoneRevision: null, ownerEpoch: "7", ownerKind: "native", grantId: c.body.operationUuid });
  const ok = await call(h.app, "POST", `${P}/revocations`, { token: "sh_at_alice", proof: inst.proof, body: { ...body, operationUuid: randomUUID() } });
  expect([ok.status, ok.json.outcome]).toEqual([200, "draining"]);
  expect(h.sql.calls.filter((x) => x.fn === "revoke").map((x) => [x.p.targetOwnerEpoch, x.p.revision])).toEqual([["7", "2"]]);
  // The install's resend later answers: applied, and the existing revoke is kept (one revoke per grant).
  h.clock.now += 3_600_000;
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  expect(h.store.intents.get(c.body.operationUuid)!.status).toBe("applied");
  expect([...h.store.intents.values()].filter((i) => i.kind === "revoke" && i.targetGrantId === c.body.operationUuid).map((i) => i.status)).toEqual(["applied"]);
  expect((await h.store.outstandingIssuerKeyIds()).size).toBe(0);
  expect(leaks(allLogs(h))).toEqual([]);
});

test("no starvation: 100 poisoned pending revokes do not block a later revoke; each stays pending (stalled, alerted) and keeps its key", async () => {
  const h = nativeHarness();
  const a = await register(h);
  for (let i = 0; i < 100; i++) expect((await claim(h, a, { sessionId: `sess-p${i}` })).r.status).toBe(200);
  // Poison every reserved revoke of installation A (stored digests no longer match A/S).
  for (const [op, rec] of [...h.store.intents]) if (rec.kind === "revoke") h.store.intents.set(op, Object.freeze({ ...rec, evidenceSha256: "0".repeat(64) }));
  expect(await h.issuer.retireInstallation(a.installationId)).toBe(100);
  h.clock.now += 1;
  const b = await register(h, newProof(), randomUUID());
  const good = await claim(h, b, { sessionId: "sess-good" });
  expect(await h.issuer.retireInstallation(b.installationId)).toBe(1);
  const first = await h.issuer.reconcile({ limit: 100 });
  expect(first).toEqual({ sent: 0, skipped: 0, retry: 100, stalled: 0, stale: 0 });
  const second = await h.issuer.reconcile({ limit: 100 }); // the poisoned ones are backing off: the later revoke is next
  expect(second).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  expect(h.sql.calls.filter((x) => x.fn === "revoke").map((x) => x.p.targetGrantId)).toEqual([good.body.operationUuid]);
  const passes = [];
  for (let pass = 0; pass < 10; pass++) {
    h.clock.now += 3_600_000;
    passes.push(await h.issuer.reconcile({ limit: 100 }));
  }
  // Attempts 2..11: stalled from the 8th failure on, every pass after that.
  expect(passes.map((p) => p.stalled)).toEqual([0, 0, 0, 0, 0, 0, 100, 100, 100, 100]);
  expect(passes.at(-1)).toEqual({ sent: 0, skipped: 0, retry: 100, stalled: 100, stale: 0 });
  const poisoned = [...h.store.intents.values()].filter((i) => i.kind === "revoke" && i.status === "pending");
  expect([poisoned.length, new Set(poisoned.map((i) => i.attempts))]).toEqual([100, new Set([11])]);
  expect(h.sql.calls.filter((x) => x.fn === "revoke").length).toBe(1); // a bad stored intent never reaches SQL
  expect([...(await h.store.outstandingIssuerKeyIds())]).toEqual(["native-key-1"]);
});

// ------------------------------------------------------------ lifecycle races (F1) and the send lease (F3)

const revokesOf = (h: H, grantId: string) => [...h.store.intents.values()].filter((i) => i.kind === "revoke" && i.targetGrantId === grantId);

test("lost install response: the reconciler resends the same bytes, the SQL replays, the install is applied and its revoke reserved", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  h.sql.loseNext("install");
  const c = await claim(h, inst);
  expect(c.r.status).toBe(503);
  expect(h.sql.sessions.get("droid/sess-1")!.grantId).toBe(c.body.operationUuid); // it committed
  expect(h.store.intents.get(c.body.operationUuid)!).toMatchObject({ status: "pending", attempts: 1, leaseUntil: null });
  expect(revokesOf(h, c.body.operationUuid).length).toBe(0);
  h.clock.now += 30_000;
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  const installs = h.sql.calls.filter((x) => x.fn === "install");
  expect([installs.length, new Set(installs.map((x) => x.p.evidenceSha256)).size]).toEqual([2, 1]);
  expect(h.store.intents.get(c.body.operationUuid)!.status).toBe("applied");
  const reserved = revokesOf(h, c.body.operationUuid);
  expect([reserved.length, reserved[0]!.status, fieldsOf(reserved[0]!.assertion)[7]]).toEqual([1, "reserved", "1"]);
  const replay = await call(h.app, "POST", `${P}/claims`, { token: "sh_at_alice", proof: inst.proof, body: c.body });
  expect([replay.status, replay.json.grantId, replay.json.ownerEpoch]).toEqual([200, c.body.operationUuid, "1"]);
  secrets.add(replay.json.grant);
});

test("retirement after a lost install response: the late apply creates the revoke ACTIVE (installation_retired) and the reconciler sends it", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  h.sql.loseNext("install");
  const c = await claim(h, inst);
  expect(c.r.status).toBe(503);
  // No reserved revoke exists yet: retirement records the cancel on the pending install.
  expect(await h.issuer.retireInstallation(inst.installationId)).toBe(1);
  expect(h.store.intents.get(c.body.operationUuid)!.cancelReason).toBe("installation_retired");
  h.clock.now += 30_000;
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 }); // the install replays
  const [revoke] = revokesOf(h, c.body.operationUuid);
  expect([revoke!.status, revoke!.activationReason]).toEqual(["pending", "installation_retired"]);
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  expect(h.sql.calls.filter((x) => x.fn === "revoke").map((x) => [x.p.targetGrantId, x.p.targetOwnerEpoch])).toEqual([[c.body.operationUuid, "1"]]);
  expect(h.sql.sessions.get("droid/sess-1")!.state).toBe("draining");
  expect((await h.store.outstandingIssuerKeyIds()).size).toBe(0);
});

test("session removal of a still-pending install records it; the late apply creates the revoke active (session_removed)", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  h.sql.loseNext("install");
  const c = await claim(h, inst);
  expect(c.r.status).toBe(503);
  expect(await h.issuer.removeSession(inst.installationId, c.body.operationUuid)).toBe(true);
  expect(await h.issuer.removeSession("other-installation", c.body.operationUuid)).toBe(false); // scoped to its installation
  expect(await h.issuer.removeSession(inst.installationId, randomUUID())).toBe(false);
  expect(h.store.intents.get(c.body.operationUuid)!.cancelReason).toBe("session_removed");
  h.clock.now += 30_000;
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  const [revoke] = revokesOf(h, c.body.operationUuid);
  expect([revoke!.status, revoke!.activationReason]).toEqual(["pending", "session_removed"]);
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  expect(h.sql.calls.filter((x) => x.fn === "revoke").map((x) => x.p.targetGrantId)).toEqual([c.body.operationUuid]);
});

test("retirement while a claim awaits status: createInstallIntent refuses it (403 installation_not_proven), no intent, no install sent", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  const held = h.sql.hold("status");
  const pending = claim(h, inst);
  await held.reached;
  expect(await h.issuer.retireInstallation(inst.installationId)).toBe(0);
  held.release();
  const c = await pending;
  expect([c.r.status, c.r.json]).toEqual([403, { error: { code: "installation_not_proven" } }]);
  expect(h.store.intents.has(c.body.operationUuid)).toBe(false);
  expect(h.sql.calls.filter((x) => x.fn === "install").length).toBe(0);
});

test("retirement while the install SQL is in flight: the apply creates the revoke active and the reconciler sends it", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  const held = h.sql.hold("install");
  const pending = claim(h, inst);
  await held.reached;
  expect(await h.issuer.retireInstallation(inst.installationId)).toBe(1);
  held.release();
  const c = await pending;
  expect(c.r.status).toBe(200);
  const [revoke] = revokesOf(h, c.body.operationUuid);
  expect([revoke!.status, revoke!.activationReason]).toEqual(["pending", "installation_retired"]);
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
  expect(h.sql.calls.filter((x) => x.fn === "revoke").map((x) => x.p.targetGrantId)).toEqual([c.body.operationUuid]);
});

test("send lease: concurrent replays and the reconciler never send one intent twice at once, and do not spend attempts", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  const body = { operationUuid: randomUUID(), installationId: inst.installationId, source: "droid", sessionId: "sess-1", originId: "origin_a" };
  const post = () => call(h.app, "POST", `${P}/claims`, { token: "sh_at_alice", proof: inst.proof, body });
  const held = h.sql.hold("install");
  const first = post();
  await held.reached;
  // attempts is still 0 (no backoff applies): only the lease stops these from sending.
  const replays = await Promise.all([post(), post(), post()]);
  expect(replays.map((r) => [r.status, r.json])).toEqual(Array(3).fill([503, { error: { code: "acquisition_unavailable", retryable: true } }]));
  expect(await h.issuer.reconcile()).toEqual({ sent: 0, skipped: 0, retry: 0, stalled: 0, stale: 0 }); // leased: not due
  expect(h.store.intents.get(body.operationUuid)!.attempts).toBe(0);
  expect(h.sql.calls.filter((x) => x.fn === "install").length).toBe(1);
  held.release();
  const answered = await first;
  expect(answered.status).toBe(200);
  secrets.add(answered.json.grant);
  expect(h.store.intents.get(body.operationUuid)!.leaseUntil).toBeNull();
  const again = await post();
  expect([again.status, again.json.outcome, again.json.grant]).toEqual([200, "replay", answered.json.grant]);
  expect(h.sql.calls.filter((x) => x.fn === "install").length).toBe(1);
});

test("send lease (memory store): CAS, released by the outcome, and reclaimable once it expires (a crashed sender)", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  h.sql.failNext(down(), "install");
  const c = await claim(h, inst);
  const op = c.body.operationUuid;
  expect(await h.store.claimSend(op, 1_000)).toBeNull(); // backing off
  h.clock.now += 30_000;
  const lease = (await h.store.claimSend(op, 1_000))!;
  expect([lease.leaseUntil, typeof lease.leaseToken]).toEqual([h.clock.now + 1_000, "string"]);
  expect(await h.store.claimSend(op, 1_000)).toBeNull();
  expect(await h.store.dueIntents(10)).toEqual([]);
  h.clock.now += 1_000;
  const taken = (await h.store.claimSend(op, 1_000))!; // expired: taken over
  expect([taken.leaseUntil, taken.leaseToken !== lease.leaseToken]).toEqual([h.clock.now + 1_000, true]);
  expect(await h.store.markRetry(op, taken.leaseToken!)).toBe("recorded");
  expect(h.store.intents.get(op)!).toMatchObject({ leaseUntil: null, leaseToken: null });
});

test("F7 stale completion (memory store): A expires, B holds the lease; A's late success, retry and refusal are no-ops and C cannot claim", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  h.sql.failNext(down(), "install");
  const c = await claim(h, inst);
  const op = c.body.operationUuid;
  h.clock.now += 30_000;
  const a = (await h.store.claimSend(op, 300_000))!;
  h.clock.now += 300_000;
  const b = (await h.store.claimSend(op, 300_000))!;
  expect(b.leaseToken).not.toBe(a.leaseToken);
  const before = { ...h.store.intents.get(op)! };
  expect(await h.store.markRetry(op, a.leaseToken!)).toBe("stale");
  expect(await h.store.markRefused(op, a.leaseToken!, "owner_draining")).toBe("stale");
  expect(await h.store.markInstallApplied(op, a.leaseToken!, "{}", null, "no_epoch")).toBe("stale");
  expect(await h.store.markApplied(op, a.leaseToken!, "{}")).toBe("stale");
  expect(h.store.intents.get(op)!).toEqual(before); // B's lease and the attempts are untouched
  expect(await h.store.claimSend(op, 300_000)).toBeNull(); // C cannot claim while B holds it
  expect(await h.store.markRetry(op, b.leaseToken!)).toBe("recorded");
  expect(await h.store.markRetry(op, b.leaseToken!)).toBe("stale"); // a token is single-use
});

for (const path of ["success", "retry", "refusal"] as const) {
  test(`F7 stale completion (issuer): a late ${path} after the lease was taken over is a no-op, counted and logged`, async () => {
    const h = nativeHarness();
    const events: unknown[] = [];
    const issuer = new NativeIssuer({ store: h.store, sql: new AcquisitionSql(h.sql), signer: h.signer, intentKey: intentKeyBytes, now: () => h.clock.now, onStaleCompletion: (e) => void events.push(e) });
    const inst = await register(h);
    h.sql.failNext(down(), "install");
    const c = await claim(h, inst);
    const op = c.body.operationUuid;
    h.clock.now += 30_000;
    // A: the reconciler's send parks in the SQL call.
    if (path === "retry") h.sql.failNext(down(), "install");
    if (path === "refusal") h.sql.failNext(pgError("55000", "owner_draining"), "install");
    const heldA = h.sql.hold("install");
    const passA = issuer.reconcile();
    await heldA.reached;
    // A's lease expires; B takes it over and parks too.
    h.clock.now += 300_000;
    const b = (await h.store.claimSend(op, 300_000))!;
    expect(b).not.toBeNull();
    const before = { ...h.store.intents.get(op)! };
    heldA.release();
    expect(await passA).toEqual({ sent: 0, skipped: 0, retry: 0, stalled: 0, stale: 1 });
    expect(h.store.intents.get(op)!).toEqual(before); // B's lease intact, nothing recorded by A
    expect([...h.store.intents.values()].filter((i) => i.kind === "revoke").length).toBe(0);
    expect(await h.store.claimSend(op, 300_000)).toBeNull(); // C
    expect([issuer.staleCompletions, events]).toEqual([1, [{ kind: "install", completion: ({ success: "applied", retry: "retry", refusal: "refused" } as const)[path] }]]);
  });
}

test("proposed DDL (PGlite): role checks 42704/55000, installer-only grants, and PgAcquisitionStore end to end", async () => {
  const { readFileSync } = await import("node:fs");
  const { PGlite } = await import("../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js");
  const { PgAcquisitionStore, MAX_SEND_ATTEMPTS } = await import("../intent-store.ts");
  const { createIssuerSigner } = await import("../key-registry.ts");
  const { FakeSql, nativeKey, registry } = await import("./helpers.ts");
  const ddl = readFileSync(new URL("../../../supabase/acquisition-intents.proposed.sql", import.meta.url), "utf8");
  const ROLES = "CREATE ROLE anon NOLOGIN; CREATE ROLE authenticated NOLOGIN; CREATE ROLE service_role NOLOGIN; CREATE ROLE bulls96_session_ingest NOLOGIN; CREATE ROLE bulls96_session_ingest_seeder NOLOGIN;";
  const sqlstate = async (db: { exec: (s: string) => Promise<unknown> }, s: string) => db.exec(s).then(() => "ok", (e: { code?: string }) => e.code);

  const bare = new PGlite();
  try {
    await bare.exec(`CREATE SCHEMA sessions; ${ROLES}`);
    expect(await sqlstate(bare, ddl)).toBe("42704"); // installer role absent
    await bare.exec("CREATE ROLE bulls96_acquisition_installer LOGIN NOINHERIT");
    expect(await sqlstate(bare, ddl)).toBe("55000"); // can log in
    await bare.exec("ALTER ROLE bulls96_acquisition_installer NOLOGIN INHERIT");
    expect(await sqlstate(bare, ddl)).toBe("55000"); // inherits
    await bare.exec("ALTER ROLE bulls96_acquisition_installer NOINHERIT; GRANT anon TO bulls96_acquisition_installer");
    expect(await sqlstate(bare, ddl)).toBe("55000"); // a member of another role
  } finally {
    await bare.close();
  }

  const db = new PGlite();
  try {
    await db.exec(`CREATE SCHEMA sessions; ${ROLES} CREATE ROLE bulls96_acquisition_installer NOLOGIN NOINHERIT;`);
    expect(await sqlstate(db, ddl)).toBe("ok");
    expect((await db.query<{ n: number }>("SELECT count(*)::int AS n FROM pg_policies WHERE schemaname='sessions'")).rows[0]!.n).toBe(0);
    await db.exec("SET ROLE anon");
    expect(await sqlstate(db, "SELECT 1 FROM sessions.ingest_acquisition_native_intents")).toBe("42501");
    await db.exec("RESET ROLE; SET ROLE bulls96_acquisition_installer");
    expect(await sqlstate(db, "DELETE FROM sessions.ingest_acquisition_native_intents")).toBe("42501");
    expect(await sqlstate(db, "UPDATE sessions.ingest_acquisition_native_intents SET assertion='x'")).toBe("42501");

    // Everything below runs as the installer role.
    const store = new PgAcquisitionStore({
      query: (text: string, params?: unknown[]) => db.query(text, params) as never,
      transaction: (work: (tx: never) => Promise<unknown>) => db.transaction((tx: unknown) => work(tx as never)) as never,
    } as never);
    const fake = new FakeSql();
    const issuer = new NativeIssuer({ store, sql: new AcquisitionSql(fake), signer: createIssuerSigner("native-key-1", nativeKey, registry), intentKey: intentKeyBytes });
    const id = { orgId: "org_test", workspaceId: "ws_test", userId: "user_test" };
    const inst = await issuer.registerInstallation(id, { requestUuid: randomUUID(), originId: "origin_a", installationProof: newProof() });
    const installation = (await store.getInstallation(inst.installationId))!;

    // An install (landing at expectedOwnerEpoch+1): applied and its reserved revoke inserted in one transaction.
    const op = randomUUID();
    const answer = await issuer.claim(id, installation, { operationUuid: op, installationId: inst.installationId, source: "droid", sessionId: "s1", originId: "origin_a" });
    secrets.add(String(answer.grant));
    expect([answer.ownerEpoch, (await store.getIntent(op))!.reservation]).toEqual(["1", "signed"]);
    const reserved = (await store.reservedRevokeFor(op))!;
    expect([reserved.status, Buffer.from(reserved.assertion).toString("utf8").split("\u001f")[8]]).toEqual(["reserved", "1"]);

    // A failing install: backoff, then stalled at MAX_SEND_ATTEMPTS but never terminal, keys retained.
    const bad = randomUUID();
    fake.failNext(Object.assign(new Error("down"), { code: "ECONNREFUSED" }), "install");
    await expect(issuer.claim(id, installation, { operationUuid: bad, installationId: inst.installationId, source: "droid", sessionId: "s2", originId: "origin_a" })).rejects.toThrow();
    const after1 = (await store.getIntent(bad))!;
    expect([after1.status, after1.attempts, after1.nextAttemptAt - Date.now() > 20_000]).toEqual(["pending", 1, true]);
    expect((await store.dueIntents(10)).map((i) => i.operationUuid)).toEqual([]); // backing off
    await db.exec("RESET ROLE");
    await db.query(`UPDATE sessions.ingest_acquisition_native_intents SET attempts = ${MAX_SEND_ATTEMPTS} WHERE operation_uuid = $1`, [bad]);
    await db.exec("SET ROLE bulls96_acquisition_installer");
    expect((await store.getIntent(bad))!).toMatchObject({ status: "pending", attempts: MAX_SEND_ATTEMPTS, leaseUntil: null, leaseToken: null });
    expect([...(await store.outstandingIssuerKeyIds())]).toEqual(["native-key-1"]); // reserved revoke + stalled install

    // Send lease (CAS): a pending intent not yet due cannot be claimed; once due, exactly one claimSend wins.
    expect(await store.claimSend(bad, 300_000)).toBeNull();
    await db.exec("RESET ROLE");
    await db.query("UPDATE sessions.ingest_acquisition_native_intents SET next_attempt_at = clock_timestamp() - interval '1 second' WHERE operation_uuid = $1", [bad]);
    await db.exec("SET ROLE bulls96_acquisition_installer");
    const lease = (await store.claimSend(bad, 300_000))!;
    expect(lease.leaseUntil! - Date.now()).toBeGreaterThan(200_000);
    expect(await store.claimSend(bad, 300_000)).toBeNull(); // held
    expect((await store.dueIntents(10)).map((i) => i.operationUuid)).toEqual([]); // leased rows are not due
    expect(await store.markRetry(bad, randomUUID())).toBe("stale"); // a foreign token is a no-op
    // F7: A's lease expires and B takes it over; A's late completions are CAS no-ops.
    await db.exec("RESET ROLE");
    await db.query("UPDATE sessions.ingest_acquisition_native_intents SET lease_until = clock_timestamp() - interval '1 second' WHERE operation_uuid = $1", [bad]);
    await db.exec("SET ROLE bulls96_acquisition_installer");
    const leaseB = (await store.claimSend(bad, 300_000))!;
    expect(leaseB.leaseToken).not.toBe(lease.leaseToken);
    expect(await store.markRetry(bad, lease.leaseToken!)).toBe("stale");
    expect(await store.markRefused(bad, lease.leaseToken!, "owner_draining")).toBe("stale");
    expect(await store.markInstallApplied(bad, lease.leaseToken!, "{}", null, "no_epoch")).toBe("stale");
    expect(await store.markApplied(bad, lease.leaseToken!, "{}")).toBe("stale");
    expect((await store.getIntent(bad))!).toMatchObject({ status: "pending", attempts: MAX_SEND_ATTEMPTS, leaseToken: leaseB.leaseToken, result: null });
    expect(await store.claimSend(bad, 300_000)).toBeNull(); // C
    expect(await store.markRetry(bad, leaseB.leaseToken!)).toBe("recorded"); // releases the lease and backs off, still counted
    expect((await store.getIntent(bad))!).toMatchObject({ status: "pending", attempts: MAX_SEND_ATTEMPTS + 1, leaseUntil: null, leaseToken: null });
    expect(await store.claimSend(bad, 300_000)).toBeNull(); // backoff gates the next claim

    // Retirement: cancels the pending install and activates the reserved revoke (one transaction).
    expect(await issuer.retireInstallation(inst.installationId)).toBe(2);
    expect((await store.getIntent(bad))!.cancelReason).toBe("installation_retired");
    expect((await store.dueIntents(10)).map((i) => i.operationUuid)).toEqual([reserved.operationUuid]);
    expect(await issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
    expect(fake.calls.filter((c) => c.fn === "revoke").map((c) => c.p.targetOwnerEpoch)).toEqual(["1"]);
    // A claim after retirement is fenced by createInstallIntent; a replay of an existing op is still answered.
    await expect(store.createInstallIntent({ ...(await store.getIntent(op))!, operationUuid: randomUUID() } as never)).rejects.toMatchObject({ code: "installation_retired" });
    expect((await store.createInstallIntent({ ...(await store.getIntent(op))! } as never)).created).toBe(false);

    // The cancelled install's late answer: applied, and its revoke inserted pending (installation_retired), sent next pass.
    await db.exec("RESET ROLE");
    await db.query("UPDATE sessions.ingest_acquisition_native_intents SET next_attempt_at = clock_timestamp() - interval '1 second' WHERE operation_uuid = $1", [bad]);
    await db.exec("SET ROLE bulls96_acquisition_installer");
    expect(await issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
    expect((await store.getIntent(bad))!.status).toBe("applied");
    const lateRevoke = (await store.reservedRevokeFor(bad))!;
    expect([lateRevoke.status, lateRevoke.activationReason]).toEqual(["pending", "installation_retired"]);
    expect(await issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
    expect(fake.calls.filter((c) => c.fn === "revoke").map((c) => c.p.targetGrantId)).toEqual([op, bad]);
    expect((await store.outstandingIssuerKeyIds()).size).toBe(0);

    // F6 (Postgres): an install result without an epoch stays pending debt in the same transaction rules.
    const inst2 = await issuer.registerInstallation(id, { requestUuid: randomUUID(), originId: "origin_a", installationProof: newProof() });
    const installation2 = (await store.getInstallation(inst2.installationId))!;
    fake.installEpoch = () => null;
    const debt = randomUUID();
    await expect(issuer.claim(id, installation2, { operationUuid: debt, installationId: inst2.installationId, source: "droid", sessionId: "s3", originId: "origin_a" })).rejects.toThrow();
    expect((await store.getIntent(debt))!).toMatchObject({ status: "pending", reservation: "no_epoch", attempts: 1, leaseUntil: null, leaseToken: null, result: null });
    expect(await store.reservedRevokeFor(debt)).toBeNull();
    expect([...(await store.outstandingIssuerKeyIds())]).toEqual(["native-key-1"]);
    fake.installed.set(debt, fake.installed.get(debt)!.replace('"revision"', '"ownerEpoch":1,"revision"'));
    await db.exec("RESET ROLE");
    await db.query("UPDATE sessions.ingest_acquisition_native_intents SET next_attempt_at = clock_timestamp() - interval '1 second' WHERE operation_uuid = $1", [debt]);
    await db.exec("SET ROLE bulls96_acquisition_installer");
    expect(await issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0, stalled: 0, stale: 0 });
    expect([(await store.getIntent(debt))!.status, (await store.reservedRevokeFor(debt))!.status]).toEqual(["applied", "reserved"]);
  } finally {
    await db.close();
  }
});

// ------------------------------------------------------------ status query encoding

test("native status: session ids are opaque (repeated params only, a comma-containing id is never split)", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  const ids = ["a,b", "plain", "x,,y"];
  const query = ids.map((id) => `sessionIds=${encodeURIComponent(id)}`).join("&") + `&sessionId=${encodeURIComponent("solo,1")}`;
  const st = await call(h.app, "GET", `${P}/status?installationId=${inst.installationId}&source=droid&${query}`, { token: "sh_at_alice", proof: inst.proof });
  expect(st.status).toBe(200);
  const sent = h.sql.calls.find((x) => x.fn === "status")!.p as { sessionIds: string[] };
  expect(sent.sessionIds).toEqual([...ids, "solo,1"]);
});

test("native status: request target over MAX_REQUEST_TARGET_BYTES is 414 request_uri_too_long before auth or SQL", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  const before = h.sql.calls.length + h.store.calls.length;
  const base = `${P}/status?installationId=${inst.installationId}&source=droid`;
  // 20 ids of 400 bytes: under the 500-id cap, over the byte cap.
  const big = base + Array.from({ length: 20 }, (_, i) => `&sessionIds=${String(i).padStart(400, "s")}`).join("");
  expect(big.length).toBeGreaterThan(MAX_REQUEST_TARGET_BYTES);
  for (const token of ["sh_at_alice", undefined]) {
    const r = await call(h.app, "GET", big, { token, proof: inst.proof });
    expect([r.status, r.json]).toEqual([414, { error: { code: "request_uri_too_long" } }]);
  }
  expect(h.sql.calls.length + h.store.calls.length).toBe(before);
  const fits = base + Array.from({ length: 10 }, (_, i) => `&sessionIds=${String(i).padStart(400, "s")}`).join("");
  expect(fits.length).toBeLessThan(MAX_REQUEST_TARGET_BYTES);
  expect((await call(h.app, "GET", fits, { token: "sh_at_alice", proof: inst.proof })).status).toBe(200);
});

// ------------------------------------------------------------ F10 status epochs

test("F10 status: owned and never-owned sessions in one answer; never-owned carries ownerEpoch null, never \"0\"", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  const c = await claim(h, inst);
  const r = await call(h.app, "GET", statusPath(inst.installationId, "sess-1&sessionIds=sess-new"), { token: "sh_at_alice", proof: inst.proof });
  expect(r.status).toBe(200);
  expect(r.json.result.sessions).toEqual([
    { sessionId: "sess-1", state: "active", revision: "1", tombstoneRevision: null, ownerEpoch: "1", ownerKind: "native", grantId: c.body.operationUuid, mine: true },
    { sessionId: "sess-new", state: "unowned", revision: "0", tombstoneRevision: null, ownerEpoch: null, ownerKind: null, grantId: null, mine: false },
  ]);
  expect(r.text).not.toContain('"ownerEpoch":"0"');
});

test("F10 status: a session with no owner row answers state unknown (200, never 500); an epoch-0 row is never mine", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  const c = await claim(h, inst);
  // The authority answers one row with no state/epoch, omits another, and reports epoch 0 for our grant.
  h.sql.statusText = JSON.stringify([
    { sessionId: "sess-1", state: "active", revision: 1, tombstoneRevision: null, ownerEpoch: 0, ownerKind: "native", grantId: c.body.operationUuid },
    { sessionId: "sess-norow", state: null, revision: null, tombstoneRevision: null, ownerEpoch: null, ownerKind: null, grantId: null },
  ]);
  const r = await call(h.app, "GET", statusPath(inst.installationId, "sess-1&sessionIds=sess-norow&sessionIds=sess-missing"), { token: "sh_at_alice", proof: inst.proof });
  expect(r.status).toBe(200);
  expect(r.json.result.sessions).toEqual([
    { sessionId: "sess-1", state: "active", revision: "1", tombstoneRevision: null, ownerEpoch: null, ownerKind: "native", grantId: null, mine: false },
    { sessionId: "sess-norow", state: "unknown", revision: "0", tombstoneRevision: null, ownerEpoch: null, ownerKind: null, grantId: null, mine: false },
    { sessionId: "sess-missing", state: "unknown", revision: "0", tombstoneRevision: null, ownerEpoch: null, ownerKind: null, grantId: null, mine: false },
  ]);
});

test("F10 claim/rebind success epochs are strictly positive: an applied install reporting epoch 0 is not answered as a grant", async () => {
  const h = nativeHarness();
  h.sql.installEpoch = () => 0n;
  const inst = await register(h);
  const c = await claim(h, inst);
  expect([c.r.status, c.r.json.error.code]).toEqual([503, "acquisition_unavailable"]); // epoch 0 cannot reserve a revoke: debt
  expect(h.store.intents.get(c.body.operationUuid)!).toMatchObject({ status: "pending", reservation: "no_epoch" });
});
