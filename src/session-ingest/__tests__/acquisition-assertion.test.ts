import {expect, test} from "bun:test";
import {createHash, createPrivateKey, createPublicKey, generateKeyPairSync, sign} from "node:crypto";
import {
  AcquisitionAssertionError, acquisitionAssertionBytes, acquisitionAssertionDigests,
  acquisitionGrantDigest, acquisitionPayload, verifyAcquisitionAssertion,
  type AcquisitionOperation, type TrustedAcquisitionIssuer,
} from "../acquisition-assertion.ts";

// Public RFC 8032 test vector 1: deterministic test material, never a deployed issuer key.
const privateKey = createPrivateKey({format: "der", type: "pkcs8", key: Buffer.from(
  "302e020100300506032b657004220420" + "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60", "hex")});
const publicKey = createPublicKey(privateKey);
const scope = {orgId: "org_test", workspaceId: "ws_test", source: "droid", sessionId: "sess-0001"};
const operationUuid = "00000000-0000-4000-8000-000000000001";
const bindingUuid = "00000000-0000-4000-8000-000000000002";
const installationId = "00000000-0000-4000-8000-000000000003";
const grant = "rhg_" + "0123456789abcdef".repeat(4);
const grantSha256 = "eca3f7669845e8b736db441074fcbaa51183bfb167f7cc71003d4d564496d227";
const common = {operationUuid, ...scope, revision: "1", issuerKeyId: "test-key-1"};
const install = {...common, expectedRevision: "0", expectedOwnerEpoch: "0", expectedPredecessorGrantId: "",
  ownerKind: "cloud", ownerUserId: "user_test", ownerOriginId: "origin_test", bindingUuid,
  installationId: "", grantSha256, expiresAt: ""};
const revoke = {...common, targetGrantId: operationUuid, targetOwnerEpoch: "1", targetOwnerKind: "cloud",
  targetBindingUuid: bindingUuid, targetInstallationId: ""};
const rebind = {...common, targetGrantId: operationUuid, targetOwnerEpoch: "1", ownerUserId: "user_test",
  installationId, oldOriginId: "old_origin", newOriginId: "new_origin"};
const registry = (ownerKind: "cloud" | "native" = "cloud") =>
  new Map<string, TrustedAcquisitionIssuer>([["test-key-1", {ownerKind, publicKey}]]);
function envelope(kind: AcquisitionOperation, payload: unknown) {
  const assertion = acquisitionAssertionBytes(kind, payload), signature = sign(null, assertion, privateKey);
  return {payload: {...acquisitionPayload(kind, payload), ...acquisitionAssertionDigests(assertion, signature)},
    signature: signature.toString("base64url")};
}
function authorized(received: typeof scope) { return JSON.stringify(received) === JSON.stringify(scope); }
function denied(work: () => unknown, code = "not_authorized") {
  expect(work).toThrow(AcquisitionAssertionError);
  try {work();} catch (error) {expect((error as AcquisitionAssertionError).code).toBe(code);}
}

/** Recompute public hashes after tampering, without possessing the issuer's signing key. */
function forged(kind: AcquisitionOperation, original: ReturnType<typeof envelope>, changes: Record<string, string>) {
  const {evidenceSha256: _, requestSha256: __, ...unsigned} = original.payload;
  const changed = {...unsigned, ...changes};
  const signature = Buffer.from(original.signature, "base64url");
  return {signature: original.signature, payload: {...changed,
    ...acquisitionAssertionDigests(acquisitionAssertionBytes(kind, changed), signature)}};
}

test("published grant vectors bind the exact session and workspace position", () => {
  expect(acquisitionGrantDigest(scope, operationUuid, grant)).toBe(grantSha256);
  expect(acquisitionGrantDigest({...scope, sessionId: "sess-0002"}, operationUuid, grant))
    .toBe("359de6c40574725c3256fb529e69c51c4cdefbe06c1cd3558c1068b8bd9019b4");
  expect(acquisitionGrantDigest({...scope, workspaceId: ""}, operationUuid, grant)).not.toBe(grantSha256);
});

test("install assertion, Ed25519 envelope and SQL request match the published cross-language vector", () => {
  const assertion = acquisitionAssertionBytes("install", install), signed = envelope("install", install);
  expect(createHash("sha256").update(assertion).digest("hex"))
    .toBe("54e325f1f60abce8e8666d2737de68903a7b4f564fed091e22806f939d3ffc75");
  expect(Buffer.from(signed.signature, "base64url").toString("hex").startsWith("fa06607856b9cc7b")).toBe(true);
  expect(signed.payload.evidenceSha256).toBe("9fabebd5cbc741c975fbfd3bb58ea4f127a187240979a1c5ea5960df006138af");
  expect(signed.payload.requestSha256).toBe("fc501fbafdb4618311ff3ecc20b82e75f0d005e4a652a1a09492931613ebac29");
  const verified = verifyAcquisitionAssertion("install", signed, registry(), authorized);
  expect(verified.payload).toEqual(signed.payload);
  expect("signature" in verified.payload).toBe(false);
  expect("grant" in verified.payload).toBe(false);
  expect(Object.isFrozen(verified)).toBe(true);
  expect(Object.isFrozen(verified.payload)).toBe(true);
});

test("all operations verify under their issuer class and remain domain separated", () => {
  for (const [kind, payload, owner] of [["install", install, "cloud"], ["revoke", revoke, "cloud"],
    ["rebind", rebind, "native"]] as const) {
    const signed = envelope(kind, payload);
    expect(verifyAcquisitionAssertion(kind, signed, registry(owner), authorized).kind).toBe(kind);
    denied(() => verifyAcquisitionAssertion(kind, signed, registry(owner === "cloud" ? "native" : "cloud"), authorized));
  }
  const native = {...install, ownerKind: "native", bindingUuid: "", installationId};
  expect(verifyAcquisitionAssertion("install", envelope("install", native), registry("native"), authorized).kind).toBe("install");
  const nativeRevoke = {...revoke, targetOwnerKind: "native", targetBindingUuid: "", targetInstallationId: installationId};
  expect(verifyAcquisitionAssertion("revoke", envelope("revoke", nativeRevoke), registry("native"), authorized).kind).toBe("revoke");
});

test("every install operation, scope, CAS and target field is covered by the signature", () => {
  const original = envelope("install", install);
  for (const field of Object.keys(install)) {
    const altered = {...original, payload: {...original.payload, [field]: "tampered"}};
    expect(() => verifyAcquisitionAssertion("install", altered, registry(), authorized)).toThrow();
  }
  // A validly shaped different session still cannot borrow the original signature/digests.
  denied(() => verifyAcquisitionAssertion("install", {...original, payload: {...original.payload, sessionId: "sess-0002"}}, registry(), () => true));
});

test("validly shaped tampering cannot be laundered by recomputing public envelope hashes", () => {
  const alternatives = {operationUuid: installationId, orgId: "other_org", workspaceId: "other_ws", source: "other_source",
    sessionId: "other_session", revision: "2", issuerKeyId: "second-key", expectedRevision: "1", expectedOwnerEpoch: "1",
    expectedPredecessorGrantId: bindingUuid, ownerUserId: "other_user", ownerOriginId: "other_origin",
    bindingUuid: installationId, grantSha256: "0".repeat(64), expiresAt: "2026-12-31T23:59:59Z"};
  const keys = registry(); keys.set("second-key", {ownerKind: "cloud", publicKey});
  const signed = envelope("install", install);
  for (const [field, value] of Object.entries(alternatives))
    denied(() => verifyAcquisitionAssertion("install", forged("install", signed, {[field]: value}), keys, () => true));
  denied(() => verifyAcquisitionAssertion("install", forged("install", signed,
    {ownerKind: "native", bindingUuid: "", installationId}), registry("native"), () => true));
  const revoking = envelope("revoke", revoke);
  for (const [field, value] of Object.entries({targetGrantId: installationId, targetOwnerEpoch: "2", targetBindingUuid: installationId}))
    denied(() => verifyAcquisitionAssertion("revoke", forged("revoke", revoking, {[field]: value}), registry(), () => true));
  denied(() => verifyAcquisitionAssertion("revoke", forged("revoke", revoking,
    {targetOwnerKind: "native", targetBindingUuid: "", targetInstallationId: installationId}), registry("native"), () => true));
  const rebinding = envelope("rebind", rebind);
  for (const [field, value] of Object.entries({targetGrantId: bindingUuid, targetOwnerEpoch: "2", installationId: bindingUuid,
    oldOriginId: "other_old", newOriginId: "other_new", ownerUserId: "other_user"}))
    denied(() => verifyAcquisitionAssertion("rebind", forged("rebind", rebinding, {[field]: value}), registry("native"), () => true));
});

test("server-pinned keys, exact Ed25519 algorithm and independent scope authority are mandatory", () => {
  const signed = envelope("install", install);
  denied(() => verifyAcquisitionAssertion("install", signed, new Map(), authorized));
  const other = generateKeyPairSync("ed25519").publicKey;
  denied(() => verifyAcquisitionAssertion("install", signed, new Map([["test-key-1", {ownerKind: "cloud", publicKey: other}]]), authorized));
  const wrongAlgorithm = generateKeyPairSync("ec", {namedCurve: "prime256v1"}).publicKey;
  denied(() => verifyAcquisitionAssertion("install", signed, new Map([["test-key-1", {ownerKind: "cloud", publicKey: wrongAlgorithm}]]), authorized));
  denied(() => verifyAcquisitionAssertion("install", signed, registry(), () => false));
  denied(() => verifyAcquisitionAssertion("install", signed, registry(), () => {throw Error(grant);}));
  try {verifyAcquisitionAssertion("install", signed, registry(), () => {throw Error(grant);});}
  catch (error) {expect(String(error)).not.toContain(grant);}
  let scopeChecks = 0;
  denied(() => verifyAcquisitionAssertion("install", {...signed, signature: "A".repeat(86)}, registry(), () => {scopeChecks++; return true;}));
  expect(scopeChecks).toBe(0);
});

test("digest and signature spellings are exact and the verified payload is detached", () => {
  const signed = envelope("install", install);
  for (const field of ["evidenceSha256", "requestSha256"]) {
    denied(() => verifyAcquisitionAssertion("install", {...signed, payload: {...signed.payload, [field]: "0".repeat(64)}}, registry(), authorized));
  }
  for (const signature of [signed.signature + "=", signed.signature.slice(0, -1), "!".repeat(86)])
    denied(() => verifyAcquisitionAssertion("install", {...signed, signature}, registry(), authorized));
  const verified = verifyAcquisitionAssertion("install", signed, registry(), authorized);
  signed.payload.sessionId = "modified_after_verify";
  expect(verified.payload.sessionId).toBe(scope.sessionId);
});

test("decimal bigint fields retain precision and reject normalization and overflow", () => {
  for (const revision of ["9007199254740993", "9223372036854775807"]) {
    const signed = envelope("install", {...install, revision});
    expect(verifyAcquisitionAssertion("install", signed, registry(), authorized).payload.revision).toBe(revision);
    expect(acquisitionAssertionBytes("install", {...install, revision}).toString()).toContain("\u001f" + revision + "\u001f");
  }
  for (const revision of [0, 1, "0", "01", "+1", "-1", "1.0", "9223372036854775808", null])
    denied(() => acquisitionAssertionBytes("install", {...install, revision}), "invalid_request");
  denied(() => acquisitionAssertionBytes("revoke", {...revoke, targetOwnerEpoch: "0"}), "invalid_request");
});

test("identifiers are byte exact and reject delimiter, control and invalid Unicode aliases", () => {
  for (const sessionId of ["", "bad\u001fboundary", "bad\u0000value", "bad\u007fvalue", "\ud800", "x".repeat(513), "é".repeat(257)])
    denied(() => acquisitionAssertionBytes("install", {...install, sessionId}), "invalid_request");
  const accepted = {...install, workspaceId: "", sessionId: "Case-é-😀"};
  const result = verifyAcquisitionAssertion("install", envelope("install", accepted), registry(), () => true);
  expect(result.payload.workspaceId).toBe("");
  expect(result.payload.sessionId).toBe(accepted.sessionId);
  expect(acquisitionAssertionBytes("install", {...install, sessionId: "Case"}))
    .not.toEqual(acquisitionAssertionBytes("install", {...install, sessionId: "case"}));
});

test("unknown keys, missing fields, UUID aliases and contradictory owner lineage are refused", () => {
  const {issuerKeyId: _, ...missing} = install;
  for (const payload of [missing, {...install, extra: true}, {...install, operationUuid: operationUuid.toUpperCase().replace("4000", "A000")},
    {...install, bindingUuid: null}, {...install, installationId}, {...install, ownerKind: "Native"}])
    denied(() => acquisitionAssertionBytes("install", payload), "invalid_request");
  const signed = envelope("install", install);
  denied(() => verifyAcquisitionAssertion("install", {...signed, publicKey: publicKey.export({format: "pem", type: "spki"})}, registry(), authorized), "invalid_request");
  denied(() => acquisitionAssertionBytes("revoke", {...revoke, targetInstallationId: installationId}), "invalid_request");
});

test("expiry preserves UTC-second spelling and rejects calendar normalization", () => {
  const expiresAt = "2026-12-31T23:59:59Z";
  expect(acquisitionPayload("install", {...install, expiresAt}).expiresAt).toBe(expiresAt);
  for (const expiresAt of ["2026-02-31T00:00:00Z", "2026-01-01T24:00:00Z", "0000-01-01T00:00:00Z",
    "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00+00:00", "tomorrow"])
    denied(() => acquisitionAssertionBytes("install", {...install, expiresAt}), "invalid_request");
});

test("canonical scalars reject trailing line terminators accepted by JavaScript dollar anchors", () => {
  for (const suffix of ["\n", "\r", "\u2028", "\u2029"]) {
    for (const [field, value] of Object.entries({operationUuid, revision: "1", grantSha256}))
      denied(() => acquisitionAssertionBytes("install", {...install, [field]: value + suffix}), "invalid_request");
    denied(() => acquisitionGrantDigest(scope, operationUuid, grant + suffix), "invalid_request");
    const signed = envelope("install", install);
    denied(() => verifyAcquisitionAssertion("install", {...signed, signature: signed.signature + suffix}, registry(), authorized));
  }
  for (const [field, value] of Object.entries({expectedPredecessorGrantId: null, bindingUuid: null, installationId: null, expiresAt: null}))
    denied(() => acquisitionAssertionBytes("install", {...install, [field]: value}), "invalid_request");
});
