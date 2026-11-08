// §3 vectors through the REAL peer codec (src/session-ingest/acquisition-assertion.ts, #322),
// plus the exchange's own use of it (signer, stored-assertion round trip).
import { expect, test } from "bun:test";
import { createHash, sign } from "node:crypto";
import {
  acquisitionAssertionBytes,
  acquisitionAssertionDigests,
  acquisitionGrantDigest,
  verifyAcquisitionAssertion,
} from "../../session-ingest/acquisition-assertion.ts";
import { createIssuerSigner, parseIssuerPrivateKey, parsePinnedKeys } from "../key-registry.ts";
import { CLOUD_SEED_HEX, NATIVE_KEY_ID, NATIVE_SEED_HEX, cloudInstall, cloudKey, envelope, nativeKey, pinnedKeysJson, registry } from "./helpers.ts";

const sha = (b: Uint8Array | string) => createHash("sha256").update(b).digest("hex");
const scope = { orgId: "org_test", workspaceId: "ws_test", source: "droid", sessionId: "sess-0001" };
const grant = "rhg_" + "0123456789abcdef".repeat(4);
const vectorInstall = { ...cloudInstall(), ownerUserId: "user_test" };

test("SHA-256(A), evidence and request digests match the §3 vectors (RFC 8032 key 1)", () => {
  const A = acquisitionAssertionBytes("install", vectorInstall);
  expect(sha(A)).toBe("54e325f1f60abce8e8666d2737de68903a7b4f564fed091e22806f939d3ffc75");
  const S = sign(null, A, cloudKey);
  const d = acquisitionAssertionDigests(A, S);
  expect(d.evidenceSha256).toBe("9fabebd5cbc741c975fbfd3bb58ea4f127a187240979a1c5ea5960df006138af");
  expect(d.requestSha256).toBe("fc501fbafdb4618311ff3ecc20b82e75f0d005e4a652a1a09492931613ebac29");
});

test("grant vector and its negative", () => {
  const gid = "00000000-0000-4000-8000-000000000001";
  expect(acquisitionGrantDigest(scope, gid, grant)).toBe("eca3f7669845e8b736db441074fcbaa51183bfb167f7cc71003d4d564496d227");
  expect(acquisitionGrantDigest({ ...scope, sessionId: "sess-0002" }, gid, grant)).toBe("359de6c40574725c3256fb529e69c51c4cdefbe06c1cd3558c1068b8bd9019b4");
});

test("an envelope built by the test helper verifies with the real verifier", () => {
  const verified = verifyAcquisitionAssertion("install", envelope("install", vectorInstall), registry, () => true);
  expect(verified.payload.requestSha256).toBe("fc501fbafdb4618311ff3ecc20b82e75f0d005e4a652a1a09492931613ebac29");
});

test("key parsing: pinned keys, seed and PEM private keys, signer pinned as native", () => {
  const pinned = parsePinnedKeys(pinnedKeysJson);
  expect([...pinned.keys()].sort()).toEqual(["native-key-1", "test-key-1"]);
  const seed = parseIssuerPrivateKey(Buffer.from(NATIVE_SEED_HEX, "hex").toString("base64"));
  const pem = parseIssuerPrivateKey(nativeKey.export({ format: "pem", type: "pkcs8" }).toString());
  const a = createIssuerSigner(NATIVE_KEY_ID, seed, pinned), b = createIssuerSigner(NATIVE_KEY_ID, pem, pinned);
  expect(a.sign(Buffer.from("x")).equals(b.sign(Buffer.from("x")))).toBe(true);
  // A cloud-class id, or a key that does not match the pin, fails at startup.
  expect(() => createIssuerSigner("test-key-1", seed, pinned)).toThrow();
  expect(() => createIssuerSigner(NATIVE_KEY_ID, parseIssuerPrivateKey(Buffer.from(CLOUD_SEED_HEX, "hex").toString("base64")), pinned)).toThrow();
  // The signer never serializes key material.
  expect(JSON.stringify(a)).toBe(JSON.stringify({ issuerKeyId: NATIVE_KEY_ID }));
  expect(() => parsePinnedKeys("[]")).toThrow();
  expect(() => parsePinnedKeys(JSON.stringify([{ issuerKeyId: "k", issuerClass: "other", publicKeyBase64: "" }]))).toThrow();
});
