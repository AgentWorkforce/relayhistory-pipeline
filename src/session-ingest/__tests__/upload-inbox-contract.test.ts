/**
 * relay-upload-inbox-v1 (docs/contracts/relay-upload-inbox-v1.md). The vectors file is regenerated here and must
 * be byte-identical to the checked-in copy; set UPDATE_UPLOAD_INBOX_VECTORS=1 to rewrite it after a deliberate change.
 */
import { describe, expect, test } from "bun:test";
import { createHash, createHmac } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { zstdDecompressSync } from "node:zlib";
import { rawZstdFrame } from "@rhc/lib/pipeline/raw-chunks.ts";
import { journalKey } from "../async-admission.ts";
import { assertTenantSegment } from "../writer.ts";
import { DELIVERY_MAPPING_VERSION, MAX_DELIVERY_BYTES } from "../vendor/delivery/delivery-contracts.ts";
import { deliveryAccount, parseDeliveryRequest } from "../vendor/delivery/prepare.ts";
import {
  INBOX_UPLOAD_NAMESPACE, UPLOAD_ADMISSION_STATES, UPLOAD_CONTRACT, UPLOAD_OBJECT_SUFFIX, UPLOAD_REFUSAL_CODES, UPLOAD_STATUS_HTTP_ERRORS,
  UPLOAD_WITNESS_DOMAIN, UPLOAD_WITNESS_PREFIX, UploadContractError, batchSegment, bindUploadToWitness, checkUploadObjectBytes,
  encodeUploadWitness, isUploadRefusal, parseUploadAdmissionStatus, parseUploadCredentialsResponse, parseUploadKey, parseUploadKeyPrefix,
  parseUploadWitnessKey, readUploadWitnessUnverified, rfc3339UtcMs, uploadChecksumSha256, uploadKeyPrefix, uploadObjectKey,
  uploadObjectSha256Hex, userSegment, verifyUploadWitness, type UploadKeyParts, type UploadWitnessPayload,
} from "../upload-inbox-contract.ts";

const VECTORS_PATH = fileURLToPath(new URL("../../../docs/contracts/relay-upload-inbox-v1.vectors.json", import.meta.url));

// ---- fixed synthetic inputs (never production ids) ----
const KEY_ROOT = "96bulls";
const ORG = "org-synthetic";
const WS = "ws-synthetic";
const USER = "user-synthetic-a";
const ORIGIN = "origin-synthetic";
const BATCH_ID = "batch-synthetic-0001";
/** TEST ONLY: bytes 0x00..0x1f. Never a real UPLOAD_WITNESS_KEY. */
const TEST_KEY_TEXT = Buffer.from(Array.from({ length: 32 }, (_, i) => i)).toString("base64url");
const TEST_KEY = parseUploadWitnessKey(TEST_KEY_TEXT);
const SEQ = 7;
const EXP = 1_794_355_200; // 2026-11-11T00:00:00Z, a fixed instant
const NOW = 1_762_819_200; // 2025-11-11T00:00:00Z, a fixed consumer clock
const PAYLOAD: UploadWitnessPayload = { v: 1, org: ORG, ws: WS, user: USER, seq: SEQ, exp: EXP };

const ok = <T,>(v: T | { code: string }): T => {
  if (isUploadRefusal(v)) throw new Error(`unexpected refusal ${v.code}`);
  return v as T;
};
const b64urlJson = (v: unknown) => Buffer.from(typeof v === "string" ? v : JSON.stringify(v)).toString("base64url");
/** Sign an arbitrary payload text with the test key, bypassing encodeUploadWitness's own payload checks. */
const reMac = (payloadText: string) => {
  const body = b64urlJson(payloadText);
  return `${UPLOAD_WITNESS_PREFIX}${body}.${createHmac("sha256", TEST_KEY).update(UPLOAD_WITNESS_DOMAIN + body).digest("base64url")}`;
};

async function buildVectors() {
  const account = await deliveryAccount({ orgId: ORG, workspaceId: WS });
  const request = {
    protocolVersion: 1,
    batch: {
      schema_version: 1, origin_id: ORIGIN, batch_id: BATCH_ID, job_id: "job-synthetic", generation: 1, destination_id: "dest-synthetic",
      instance_id: "inst-synthetic", account_id: account, mapping_version: DELIVERY_MAPPING_VERSION,
      records: [{ schema_version: 1, origin_id: ORIGIN, record_id: "rec-synthetic-1", revision_id: "rec-synthetic-1:r1", revision: 1, kind: "session_event",
        source: "claude", session_id: "sess-synthetic", operation: "upsert", payload: { text: "hello", ts_ms: 1_761_955_200_000 } }],
    },
  };
  const requestJson = JSON.stringify(request);
  const objectBytes = rawZstdFrame(new TextEncoder().encode(requestJson));
  const objectSha256Hex = uploadObjectSha256Hex(objectBytes);
  const keyPrefix = uploadKeyPrefix({ keyRoot: KEY_ROOT, orgId: ORG, workspaceId: WS, userId: USER });
  const objectKey = uploadObjectKey(keyPrefix, { originId: ORIGIN, batchId: BATCH_ID, objectSha256Hex, witnessSeq: SEQ });
  const witness = encodeUploadWitness(TEST_KEY, PAYLOAD);
  const relativeKey = objectKey.slice(keyPrefix.length);
  const credentialsSts = {
    contract: UPLOAD_CONTRACT, kind: "sts", bucket: "example-upload-bucket", region: "us-east-1", endpoint: null, keyPrefix, kmsKeyId: "kms-key-placeholder",
    credentials: { accessKeyId: "SYNTHETICACCESSKEY00", secretAccessKey: "synthetic-not-a-secret", sessionToken: "synthetic-not-a-session-token", expiresAt: "2026-11-11T01:00:00Z" },
    witness, witnessSeq: SEQ, maxObjectBytes: MAX_DELIVERY_BYTES, refreshAfter: "2026-11-11T00:45:00Z",
  };
  const credentialsPresigned = {
    contract: UPLOAD_CONTRACT, kind: "presigned", bucket: "example-upload-bucket", region: "us-east-1", endpoint: null, keyPrefix, kmsKeyId: "kms-key-placeholder",
    maxObjectBytes: MAX_DELIVERY_BYTES, refreshAfter: "2026-11-11T00:10:00Z", witness, witnessSeq: SEQ,
    urls: [{ key: relativeKey, url: "https://s3.example.invalid/example-upload-bucket/placeholder?X-Amz-Signature=synthetic", expiresAt: "2026-11-11T00:15:00Z" }],
  };
  const receiptId = "rhr_" + "0".repeat(64);
  const statuses = {
    pending: { contract: UPLOAD_CONTRACT, uploadKey: objectKey, state: "pending", receivedAtMs: 1_762_819_200_000, retryAfterSeconds: 15 },
    admitted: { contract: UPLOAD_CONTRACT, uploadKey: objectKey, state: "admitted", receiptId, receivedAtMs: 1_762_819_200_000, decidedAtMs: 1_762_819_203_000 },
    refused: { contract: UPLOAD_CONTRACT, uploadKey: objectKey, state: "refused", code: "delivery_privacy_superseded", receiptId, receivedAtMs: 1_762_819_200_000, decidedAtMs: 1_762_819_203_000 },
  };
  const parsed = ok(parseUploadKey(objectKey));
  const witnessOtherUser = encodeUploadWitness(TEST_KEY, { ...PAYLOAD, user: "user-synthetic-b" });
  const witnessOtherSeq = encodeUploadWitness(TEST_KEY, { ...PAYLOAD, seq: SEQ + 1 });
  const witnessExpired = encodeUploadWitness(TEST_KEY, { ...PAYLOAD, exp: NOW });
  const tampered = witness.slice(0, -1) + (witness.endsWith("A") ? "B" : "A");
  const refusals = [
    { fn: "parseUploadKey", input: objectKey.replace(UPLOAD_OBJECT_SUFFIX, ".ndjson.zst"), code: (parseUploadKey(objectKey.replace(UPLOAD_OBJECT_SUFFIX, ".ndjson.zst")) as { code: string }).code },
    { fn: "parseUploadKey", input: objectKey.replace(`/${ORG}/`, "/.org-synthetic/"), code: (parseUploadKey(objectKey.replace(`/${ORG}/`, "/.org-synthetic/")) as { code: string }).code },
    { fn: "verifyUploadWitness", input: tampered, nowSeconds: NOW, code: (verifyUploadWitness(TEST_KEY, tampered, NOW) as { code: string }).code },
    { fn: "verifyUploadWitness", input: witnessExpired, nowSeconds: NOW, code: (verifyUploadWitness(TEST_KEY, witnessExpired, NOW) as { code: string }).code },
    { fn: "bindUploadToWitness", input: { key: objectKey, witness: witnessOtherUser }, code: (bindUploadToWitness(parsed, ok(verifyUploadWitness(TEST_KEY, witnessOtherUser, NOW))) as { code: string }).code },
    { fn: "bindUploadToWitness", input: { key: objectKey, witness: witnessOtherSeq }, code: (bindUploadToWitness(parsed, ok(verifyUploadWitness(TEST_KEY, witnessOtherSeq, NOW))) as { code: string }).code },
    { fn: "checkUploadObjectBytes", input: { key: objectKey, objectBytesBase64: Buffer.from(rawZstdFrame(new TextEncoder().encode(requestJson + " "))).toString("base64") },
      code: (checkUploadObjectBytes(parsed, rawZstdFrame(new TextEncoder().encode(requestJson + " "))) as { code: string }).code },
  ];
  return {
    contract: UPLOAD_CONTRACT,
    note: "Synthetic ids only. Generated by src/session-ingest/__tests__/upload-inbox-contract.test.ts (buildVectors) from src/session-ingest/upload-inbox-contract.ts. The witness key is TEST ONLY (bytes 0x00..0x1f) and must never be configured anywhere.",
    inputs: { keyRoot: KEY_ROOT, orgId: ORG, workspaceId: WS, userId: USER, originId: ORIGIN, batchId: BATCH_ID },
    userSegment: { hashInput: JSON.stringify(["admission-user-v1", WS, USER]), uh: userSegment(WS, USER), journalKey: journalKey(KEY_ROOT, { orgId: ORG, workspaceId: WS, userId: USER }) },
    deliveryAccount: { orgId: ORG, workspaceId: WS, canonicalInput: JSON.stringify([ORG, WS]), account },
    object: {
      requestJson, objectBytesBase64: Buffer.from(objectBytes).toString("base64"), objectBytes: objectBytes.byteLength, objectSha256Hex,
      checksumSha256Header: uploadChecksumSha256(objectBytes), batchSegment: batchSegment(BATCH_ID),
    },
    keyPrefix,
    objectKey,
    parsedKey: parsed,
    witness: { testOnlyKey: TEST_KEY_TEXT, payload: PAYLOAD, payloadJson: JSON.stringify(PAYLOAD), macInput: `${UPLOAD_WITNESS_DOMAIN}${b64urlJson(PAYLOAD)}`, token: witness, verifiedAtSeconds: NOW },
    credentials: { sts: credentialsSts, presigned: credentialsPresigned },
    admissionStatus: statuses,
    refusals,
  };
}

const serialize = (v: unknown) => JSON.stringify(v, null, 2) + "\n";

describe("relay-upload-inbox-v1 vectors", () => {
  test("regenerated vectors are byte-identical to docs/contracts/relay-upload-inbox-v1.vectors.json", async () => {
    const fresh = serialize(await buildVectors());
    if (process.env.UPDATE_UPLOAD_INBOX_VECTORS === "1") writeFileSync(VECTORS_PATH, fresh);
    expect(readFileSync(VECTORS_PATH, "utf8")).toBe(fresh);
  });

  test("the vector object is a real delivery request and decodes from its zstd frame", async () => {
    const v = await buildVectors();
    const bytes = Buffer.from(v.object.objectBytesBase64, "base64");
    const decoded = new TextDecoder("utf-8", { fatal: true }).decode(zstdDecompressSync(bytes));
    expect(decoded).toBe(v.object.requestJson);
    const batch = parseDeliveryRequest(JSON.parse(decoded));
    expect(batch.account_id).toBe(v.deliveryAccount.account);
    expect(batch.origin_id).toBe(ok(parseUploadKey(v.objectKey)).originId);
    expect(v.object.checksumSha256Header).toBe(Buffer.from(v.object.objectSha256Hex, "hex").toString("base64"));
    expect(ok(checkUploadObjectBytes(ok(parseUploadKey(v.objectKey)), new Uint8Array(bytes)))).toEqual({ ok: true });
  });
});

describe("constants and enums", () => {
  test("names", () => {
    expect(UPLOAD_CONTRACT).toBe("relay-upload-inbox-v1");
    expect(INBOX_UPLOAD_NAMESPACE).toBe(".upload-inbox");
    expect(UPLOAD_OBJECT_SUFFIX).toBe(".json.zst");
    expect(UPLOAD_WITNESS_PREFIX).toBe("v1.");
    expect(UPLOAD_WITNESS_DOMAIN).toBe("relay-upload-witness-v1\n");
    expect([...UPLOAD_ADMISSION_STATES]).toEqual(["pending", "admitted", "refused"]);
    expect(UPLOAD_STATUS_HTTP_ERRORS).toEqual({ 400: "invalid_delivery", 403: "upload_tenant_mismatch", 404: "upload_unknown", 503: "delivery_unavailable" });
  });

  test("the refusal enum is exactly the frozen set, without duplicates", () => {
    expect(new Set(UPLOAD_REFUSAL_CODES).size).toBe(UPLOAD_REFUSAL_CODES.length);
    expect([...UPLOAD_REFUSAL_CODES].sort()).toEqual([
      "admission_failed", "admission_invalid", "delivery_account_mismatch", "delivery_budget_exceeded", "delivery_privacy_superseded", "delivery_too_large",
      "invalid_delivery", "tenant_not_a_segment", "unsupported_encoding", "unsupported_evidence", "unsupported_mapping", "unsupported_protocol", "unsupported_schema",
      "upload_digest_mismatch", "upload_key_invalid", "upload_origin_mismatch", "upload_tenant_mismatch", "upload_witness_expired", "upload_witness_invalid",
      "upload_witness_seq_mismatch",
    ]);
  });

  test("every refusal code has a one-line meaning in the module and a row in the contract doc", () => {
    const src = readFileSync(fileURLToPath(new URL("../upload-inbox-contract.ts", import.meta.url)), "utf8");
    const doc = readFileSync(fileURLToPath(new URL("../../../docs/contracts/relay-upload-inbox-v1.md", import.meta.url)), "utf8");
    for (const code of UPLOAD_REFUSAL_CODES) {
      expect(src).toMatch(new RegExp(`^  "${code}", //\\s+\\S`, "m"));
      expect(doc).toContain(`| \`${code}\` |`);
    }
  });
});

describe("keys", () => {
  const prefix = uploadKeyPrefix({ keyRoot: KEY_ROOT, orgId: ORG, workspaceId: WS, userId: USER });
  const sha = "a".repeat(64);
  const key = uploadObjectKey(prefix, { originId: ORIGIN, batchId: BATCH_ID, objectSha256Hex: sha, witnessSeq: SEQ });

  test("uh is the privacy journal's srv_ segment without the prefix (async-admission.ts journalKey)", () => {
    const journal = journalKey(KEY_ROOT, { orgId: ORG, workspaceId: WS, userId: USER });
    expect(journal).toBe(`${KEY_ROOT}/.admission-inbox/${ORG}/v1/ws=${WS}/m=srv_${userSegment(WS, USER)}/privacy-journal.json`);
    expect(userSegment(WS, USER)).toBe(createHash("sha256").update(JSON.stringify(["admission-user-v1", WS, USER])).digest("hex").slice(0, 32));
  });

  test("round trip", () => {
    expect(prefix).toBe(`96bulls/.upload-inbox/${ORG}/v1/ws=${WS}/u=${userSegment(WS, USER)}/`);
    expect(key).toBe(`${prefix}o=${ORIGIN}/b=${batchSegment(BATCH_ID)}/d=${"a".repeat(32)}-w${SEQ}.json.zst`);
    expect(ok(parseUploadKey(key))).toEqual({ key, keyRoot: KEY_ROOT, orgId: ORG, workspaceId: WS, userSegment: userSegment(WS, USER), prefix,
      originId: ORIGIN, batchSegment: batchSegment(BATCH_ID), digestSegment: "a".repeat(32), witnessSeq: SEQ });
    expect(parseUploadKeyPrefix(prefix)?.orgId).toBe(ORG);
    expect(ok(parseUploadKey(uploadObjectKey(prefix, { originId: ORIGIN, batchId: BATCH_ID, objectSha256Hex: sha, witnessSeq: 0 }))).witnessSeq).toBe(0);
    expect(ok(parseUploadKey(uploadObjectKey(prefix, { originId: ORIGIN, batchId: BATCH_ID, objectSha256Hex: sha, witnessSeq: Number.MAX_SAFE_INTEGER }))).witnessSeq).toBe(Number.MAX_SAFE_INTEGER);
  });

  test("the segment rule matches writer.ts assertTenantSegment", () => {
    for (const org of ["org-a", "A.b_c-9", "x".repeat(128), ".org", "x".repeat(129), "a/b", "", "a b", "é", "a..b", "-a"]) {
      let writer = true;
      try { assertTenantSegment(KEY_ROOT, org); } catch { writer = false; }
      let ours = true;
      try { uploadKeyPrefix({ keyRoot: KEY_ROOT, orgId: org, workspaceId: WS, userId: USER }); } catch { ours = false; }
      expect(`${org}:${ours}`).toBe(`${org}:${writer}`);
    }
  });

  test("builders throw on non-segment input", () => {
    for (const bad of [{ orgId: ".org" }, { orgId: "a/b" }, { orgId: "x".repeat(129) }, { workspaceId: ".ws" }, { workspaceId: "w/s" }, { userId: "" }, { userId: "a\nb" }, { keyRoot: "96Bulls" }, { keyRoot: "a/b" }])
      expect(() => uploadKeyPrefix({ keyRoot: KEY_ROOT, orgId: ORG, workspaceId: WS, userId: USER, ...bad })).toThrow(UploadContractError);
    for (const bad of [{ originId: ".o" }, { originId: "o/x" }, { originId: "x".repeat(129) }, { batchId: "" }, { objectSha256Hex: "A".repeat(64) }, { objectSha256Hex: "a".repeat(63) }, { witnessSeq: -1 }, { witnessSeq: 1.5 }, { witnessSeq: 2 ** 53 }])
      expect(() => uploadObjectKey(prefix, { originId: ORIGIN, batchId: BATCH_ID, objectSha256Hex: sha, witnessSeq: SEQ, ...bad })).toThrow(UploadContractError);
    expect(() => uploadObjectKey(prefix.slice(0, -1), { originId: ORIGIN, batchId: BATCH_ID, objectSha256Hex: sha, witnessSeq: SEQ })).toThrow(UploadContractError);
  });

  test("the parser refuses everything else", () => {
    const bad = [
      key.replace(".json.zst", ".ndjson.zst"), key.replace(".json.zst", ".json"), key + "x", "/" + key, key.replace("96bulls/", "96Bulls/"),
      key.replace(".upload-inbox", ".admission-inbox"), key.replace(".upload-inbox", "upload-inbox"),
      key.replace(`/${ORG}/`, "/.org/"), key.replace(`/${ORG}/`, "/a/b/"), key.replace(`/${ORG}/`, `/${"x".repeat(129)}/`),
      key.replace(`ws=${WS}`, "ws=.ws"), key.replace(`ws=${WS}`, `ws=${"x".repeat(129)}`), key.replace(`o=${ORIGIN}`, "o=.o"), key.replace(`o=${ORIGIN}`, "o=a/b"),
      key.replace(`o=${ORIGIN}`, `o=${"x".repeat(129)}`), key.replace("/v1/", "/v2/"), key.replace(`-w${SEQ}`, "-w07"), key.replace(`-w${SEQ}`, "-w"),
      key.replace(`-w${SEQ}`, "-w9007199254740992"), key.replace(`-w${SEQ}`, "-w-1"), key.replace(/\/d=[0-9a-f]{32}/, "/d=" + "A".repeat(32)),
      key.replace(/\/b=[0-9a-f]{32}/, "/b=" + "a".repeat(31)), key.replace(/\/u=[0-9a-f]{32}/, "/u=" + "a".repeat(33)), key.replace(`/u=`, "/m=srv_"), "", 42, null, undefined,
    ];
    for (const k of bad) expect(parseUploadKey(k)).toEqual({ code: "upload_key_invalid" });
  });
});

describe("witness", () => {
  const witness = encodeUploadWitness(TEST_KEY, PAYLOAD);

  test("round trip and exact form", () => {
    expect(verifyUploadWitness(TEST_KEY, witness, NOW)).toEqual(PAYLOAD);
    expect(witness.startsWith("v1.")).toBe(true);
    expect(witness).toMatch(/^v1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}$/);
    expect(Buffer.from(witness.split(".")[1]!, "base64url").toString()).toBe(`{"v":1,"org":"${ORG}","ws":"${WS}","user":"${USER}","seq":${SEQ},"exp":${EXP}}`);
    expect(readUploadWitnessUnverified(witness)).toEqual(PAYLOAD);
    expect(verifyUploadWitness(TEST_KEY, witness, EXP - 1)).toEqual(PAYLOAD);
  });

  test("the MAC is compared in constant time", () => {
    const src = readFileSync(fileURLToPath(new URL("../upload-inbox-contract.ts", import.meta.url)), "utf8");
    expect(src).toContain("timingSafeEqual(mac, witnessMac(key, body))");
    expect(src).not.toMatch(/mac(Text)?\s*===|===\s*b64url\(witnessMac/);
  });

  test("refusals", () => {
    const invalid = { code: "upload_witness_invalid" };
    const otherKey = parseUploadWitnessKey(Buffer.alloc(32, 9).toString("base64url"));
    const [, body, mac] = witness.split(".") as [string, string, string];
    for (const t of [
      undefined, null, "", 42, "v2." + witness.slice(3), witness + ".x", `v1.${body}`, `v1.${body}.${mac}=`, `v1.${body}=.${mac}`, `v1.${body}.${mac.slice(0, -1)}`,
      witness.slice(0, -1) + (witness.endsWith("A") ? "B" : "A"), "v1." + "A".repeat(1100) + "." + mac, `v1.${body}.${mac}\n`,
      reMac(JSON.stringify({ ...PAYLOAD, extra: 1 })),
      reMac(JSON.stringify({ org: ORG, v: 1, ws: WS, user: USER, seq: SEQ, exp: EXP })),
      reMac(JSON.stringify({ v: 1, org: ORG, ws: WS, user: USER, seq: SEQ })),
      reMac(JSON.stringify(PAYLOAD, null, 1)),
      reMac(JSON.stringify({ ...PAYLOAD, v: 2 })),
      reMac(JSON.stringify({ ...PAYLOAD, org: ".org" })),
      reMac(JSON.stringify({ ...PAYLOAD, seq: -1 })),
      reMac(JSON.stringify({ ...PAYLOAD, seq: "7" })),
      reMac(JSON.stringify({ ...PAYLOAD, exp: 1.5 })),
      reMac(`{"v":1,"org":"${ORG}","ws":"${WS}","user":"user-synthetic-\\u0061","seq":${SEQ},"exp":${EXP}}`),
    ]) expect(verifyUploadWitness(TEST_KEY, t, NOW)).toEqual(invalid);
    expect(verifyUploadWitness(otherKey, witness, NOW)).toEqual(invalid);
    expect(verifyUploadWitness(TEST_KEY, witness, EXP)).toEqual({ code: "upload_witness_expired" });
    expect(verifyUploadWitness(TEST_KEY, witness, EXP + 1)).toEqual({ code: "upload_witness_expired" });
    // reMac itself signs a valid payload correctly (the refusals above are the payload's, not the helper's).
    expect(verifyUploadWitness(TEST_KEY, reMac(JSON.stringify(PAYLOAD)), NOW)).toEqual(PAYLOAD);
  });

  test("witness keys: exactly 32 bytes of canonical unpadded base64url", () => {
    expect(parseUploadWitnessKey(TEST_KEY_TEXT).byteLength).toBe(32);
    for (const bad of ["", Buffer.alloc(31).toString("base64url"), Buffer.alloc(33).toString("base64url"), Buffer.alloc(32).toString("base64"), TEST_KEY_TEXT + "=", TEST_KEY_TEXT.slice(0, -1) + "B", "not base64url!"])
      expect(() => parseUploadWitnessKey(bad)).toThrow(UploadContractError);
    expect(() => verifyUploadWitness(new Uint8Array(31), witness, NOW)).toThrow(UploadContractError);
    expect(() => encodeUploadWitness(new Uint8Array(33), PAYLOAD)).toThrow(UploadContractError);
    expect(() => encodeUploadWitness(TEST_KEY, { ...PAYLOAD, user: "x".repeat(1000) })).toThrow(UploadContractError);
    expect(() => encodeUploadWitness(TEST_KEY, { ...PAYLOAD, extra: 1 } as UploadWitnessPayload)).toThrow(UploadContractError);
  });

  test("binding: tenant first, then seq", () => {
    const key = ok(parseUploadKey(uploadObjectKey(uploadKeyPrefix({ keyRoot: KEY_ROOT, orgId: ORG, workspaceId: WS, userId: USER }), { originId: ORIGIN, batchId: BATCH_ID, objectSha256Hex: "b".repeat(64), witnessSeq: SEQ })));
    expect(bindUploadToWitness(key, PAYLOAD)).toEqual({ ok: true });
    for (const w of [{ ...PAYLOAD, org: "org-other" }, { ...PAYLOAD, ws: "ws-other" }, { ...PAYLOAD, user: "user-other" }, { ...PAYLOAD, user: "user-other", seq: SEQ + 1 }])
      expect(bindUploadToWitness(key, w)).toEqual({ code: "upload_tenant_mismatch" });
    expect(bindUploadToWitness(key, { ...PAYLOAD, seq: SEQ + 1 })).toEqual({ code: "upload_witness_seq_mismatch" });
    expect(bindUploadToWitness(key, { ...PAYLOAD, seq: SEQ - 1 })).toEqual({ code: "upload_witness_seq_mismatch" });
  });

  test("object bytes: size, then digest", () => {
    const bytes = new TextEncoder().encode("frame");
    const prefix = uploadKeyPrefix({ keyRoot: KEY_ROOT, orgId: ORG, workspaceId: WS, userId: USER });
    const key = ok(parseUploadKey(uploadObjectKey(prefix, { originId: ORIGIN, batchId: BATCH_ID, objectSha256Hex: uploadObjectSha256Hex(bytes), witnessSeq: SEQ }))) as UploadKeyParts;
    expect(checkUploadObjectBytes(key, bytes)).toEqual({ ok: true });
    expect(checkUploadObjectBytes(key, new TextEncoder().encode("frame!"))).toEqual({ code: "upload_digest_mismatch" });
    expect(checkUploadObjectBytes(key, new Uint8Array(MAX_DELIVERY_BYTES + 1))).toEqual({ code: "delivery_too_large" });
  });
});

describe("credentials and admission status validators", () => {
  const vectors = () => JSON.parse(readFileSync(VECTORS_PATH, "utf8"));

  test("accept the vectors", () => {
    const v = vectors();
    expect(parseUploadCredentialsResponse(v.credentials.sts)).toEqual(v.credentials.sts);
    expect(parseUploadCredentialsResponse(v.credentials.presigned)).toEqual(v.credentials.presigned);
    for (const s of Object.values(v.admissionStatus)) expect(parseUploadAdmissionStatus(s)).toEqual(s as object);
    expect(parseUploadCredentialsResponse({ ...v.credentials.sts, endpoint: "https://objects.example.invalid/" })).not.toBeNull();
  });

  test("refuse an extra or a missing key, and inconsistent fields", () => {
    const v = vectors();
    for (const base of [v.credentials.sts, v.credentials.presigned]) {
      expect(parseUploadCredentialsResponse({ ...base, extra: 1 })).toBeNull();
      for (const k of Object.keys(base)) {
        const { [k]: _, ...missing } = base;
        expect(parseUploadCredentialsResponse(missing)).toBeNull();
      }
      for (const over of [{ contract: "relay-upload-inbox-v2" }, { maxObjectBytes: MAX_DELIVERY_BYTES + 1 }, { witnessSeq: SEQ + 1 }, { refreshAfter: "2026-11-11T00:45:00+00:00" },
        { refreshAfter: "2026-02-30T00:00:00Z" }, { refreshAfter: "2027-01-01T00:00:00Z" }, { endpoint: "https://user:pw@objects.example.invalid/" }, { endpoint: "ftp://x/" },
        { keyPrefix: base.keyPrefix.slice(0, -1) }, { keyPrefix: uploadKeyPrefix({ keyRoot: KEY_ROOT, orgId: ORG, workspaceId: WS, userId: "someone-else" }) },
        { witness: "v1.e30.AAAA" }, { kmsKeyId: "" }, { bucket: "Bad_Bucket" }])
        expect(parseUploadCredentialsResponse({ ...base, ...over })).toBeNull();
    }
    expect(parseUploadCredentialsResponse({ ...v.credentials.sts, credentials: { ...v.credentials.sts.credentials, extra: 1 } })).toBeNull();
    const { sessionToken: _, ...noToken } = v.credentials.sts.credentials;
    expect(parseUploadCredentialsResponse({ ...v.credentials.sts, credentials: noToken })).toBeNull();
    expect(parseUploadCredentialsResponse({ ...v.credentials.sts, kind: "presigned" })).toBeNull();
    const url = v.credentials.presigned.urls[0];
    for (const urls of [[], [url, url], [{ ...url, extra: 1 }], [{ key: url.key, url: url.url }], [{ ...url, key: url.key.replace(`-w${SEQ}`, `-w${SEQ + 1}`) }],
      [{ ...url, key: v.objectKey }], [{ ...url, url: "not a url" }], Array.from({ length: 101 }, (_, i) => ({ ...url, key: url.key.replace("o=", `o=x${i}`) }))])
      expect(parseUploadCredentialsResponse({ ...v.credentials.presigned, urls })).toBeNull();
  });

  test("admission status refusals", () => {
    const v = vectors();
    const { pending, admitted, refused } = v.admissionStatus;
    for (const s of [{ ...pending, extra: 1 }, { ...pending, contract: "x" }, { ...pending, state: "queued" }, { ...pending, uploadKey: "x" }, { ...pending, code: "admission_failed" },
      { ...pending, receiptId: admitted.receiptId }, { ...pending, decidedAtMs: 1 }, { ...admitted, retryAfterSeconds: 5 }, { ...admitted, code: "admission_failed" },
      { ...refused, code: "upload_unknown" }, { ...refused, code: undefined }, { ...pending, retryAfterSeconds: 0 }, { ...pending, receivedAtMs: -1 }, { ...admitted, receiptId: "rhr_x" }]) {
      const clean = JSON.parse(JSON.stringify(s));
      expect(parseUploadAdmissionStatus(clean)).toBeNull();
    }
    const { uploadKey: _, ...noKey } = pending;
    expect(parseUploadAdmissionStatus(noKey)).toBeNull();
    const { state: __, ...noState } = admitted;
    expect(parseUploadAdmissionStatus(noState)).toBeNull();
    expect(parseUploadAdmissionStatus({ contract: UPLOAD_CONTRACT, uploadKey: pending.uploadKey, state: "pending" })).not.toBeNull();
  });

  test("rfc3339UtcMs", () => {
    expect(rfc3339UtcMs("2026-11-11T00:00:00Z")).toBe(Date.UTC(2026, 10, 11));
    expect(rfc3339UtcMs("2026-11-11T00:00:00.123Z")).toBe(Date.UTC(2026, 10, 11) + 123);
    for (const bad of ["2026-11-11T00:00:00", "2026-11-11 00:00:00Z", "2026-13-01T00:00:00Z", "2026-11-11T24:00:00Z", "2026-11-11t00:00:00z", 1]) expect(rfc3339UtcMs(bad)).toBeNull();
  });
});
