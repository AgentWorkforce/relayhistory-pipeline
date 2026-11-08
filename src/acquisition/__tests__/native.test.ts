// Native exchange (P3 + P3a): installation proof, claims, revocations, rebinds,
// status, durable cleanup and the reconciler, through the HTTP app.
import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
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
  // Revoke or rebind my grant: not authorized.
  const rv = await call(h.app, "POST", `${P}/revocations`, { token: "sh_at_alice", proof: thief.proof, body: { operationUuid: randomUUID(), installationId: thief.installationId, source: "droid", sessionId: "sess-1", targetGrantId: first.r.json.grantId } });
  expect([rv.status, rv.json]).toEqual([403, { error: { code: "not_authorized" } }]);
  const rb = await call(h.app, "POST", `${P}/rebinds`, { token: "sh_at_alice", proof: thief.proof, body: { operationUuid: randomUUID(), installationId: thief.installationId, source: "droid", sessionId: "sess-1", grantId: first.r.json.grantId, originId: "origin_b", grant: G } });
  expect(rb.status).toBe(403);
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
  // The install intent and its reserved revoke were committed together.
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
  h.sql.failNext(Object.assign(new Error("connect ECONNREFUSED"), { code: "ECONNREFUSED" }), "install");
  const down = await call(h.app, "POST", `${P}/claims`, { token: "sh_at_alice", proof: inst.proof, body: busy.body });
  expect([down.status, down.json]).toEqual([503, { error: { code: "acquisition_unavailable", retryable: true } }]);
  const ok = await call(h.app, "POST", `${P}/claims`, { token: "sh_at_alice", proof: inst.proof, body: busy.body });
  expect(ok.status).toBe(200);
  const installs = h.sql.calls.filter((c) => c.fn === "install");
  expect(installs.length).toBe(3);
  expect(new Set(installs.map((c) => c.p.evidenceSha256)).size).toBe(1);
  expect(new Set(installs.map((c) => c.p.requestSha256)).size).toBe(1);
});

test("claim: a terminal SQL refusal is recorded, voids the reserved revoke and replays as the same refusal", async () => {
  const h = nativeHarness();
  const inst = await register(h);
  h.sql.failNext(pgError("55000", "owner_draining"), "install");
  const r = await claim(h, inst);
  expect([r.r.status, r.r.json]).toEqual([409, { error: { code: "acquisition_refused", reason: "owner_draining", retryable: false } }]);
  const again = await call(h.app, "POST", `${P}/claims`, { token: "sh_at_alice", proof: inst.proof, body: r.body });
  expect(again.json.error.reason).toBe("owner_draining");
  expect(h.sql.calls.filter((c) => c.fn === "install").length).toBe(1);
  expect([...h.store.intents.values()].find((i) => i.kind === "revoke")!.status).toBe("void");
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
  expect([wrongG.status, wrongG.json]).toEqual([403, { error: { code: "not_authorized" } }]);
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
  expect(await h.issuer.reconcile({ stillSelected: async () => true })).toEqual({ sent: 0, skipped: 1, retry: 0 });
  expect(h.sql.calls.length).toBe(sqlBefore);
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0 });
  const revoke = h.sql.calls.at(-1)!;
  expect([revoke.fn, revoke.p.targetGrantId]).toEqual(["revoke", c1.r.json.grantId]);
  // Retirement activates the rest in one step; reconciler sends only revokes.
  expect(await offline.retireInstallation(inst.installationId)).toBe(1);
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0 });
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
  expect(await h.issuer.reconcile()).toEqual({ sent: 1, skipped: 0, retry: 0 });
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
