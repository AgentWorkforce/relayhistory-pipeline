import { generateKeyPairSync, sign } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import * as Effect from "effect/Effect";
import {
  GCP_APPLY_LEASE_MS,
  GCP_APPLY_MIN_REMAINING_SECONDS,
  applyNonceDigest,
  assertGcpApplyLeaseActive,
  boundGcpApplyLease,
  consumeGcpApplyNonce,
  consumeGcpApplyNonceGlobally,
  gcpApplyAudience,
  verifyGcpApplyNonce,
} from "../apply-authorization.ts";

const executor = {
  name: "gcp-vpn-runner-01",
  mode: "vpn" as const,
  identity: "gcp-vpn-runner@agentrelay-96bulls-soc2-2610.iam.gserviceaccount.com",
};
const expected = {
  stage: "gcp-dev",
  projectId: "agentrelay-96bulls-soc2-2610",
  executor,
};
const nowMs = Date.UTC(2026, 9, 30, 12, 0, 0);
const nowSeconds = Math.floor(nowMs / 1000);
const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
const jwk = publicKey.export({ format: "jwk" });
const kid = "test-google-key";
const base64urlAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

function token(patch: Record<string, unknown> = {}) {
  const header = Buffer.from(JSON.stringify({ alg: "RS256", kid, typ: "JWT" })).toString("base64url");
  const claims = Buffer.from(
    JSON.stringify({
      iss: "https://accounts.google.com",
      aud: gcpApplyAudience(expected.projectId, expected.stage),
      sub: "123456789012345678901",
      email: executor.identity,
      email_verified: true,
      iat: nowSeconds - 10,
      exp: nowSeconds + 3600,
      ...patch,
    }),
  ).toString("base64url");
  const signature = sign("RSA-SHA256", Buffer.from(`${header}.${claims}`), privateKey).toString("base64url");
  return `${header}.${claims}.${signature}`;
}

function mutateUnusedSignatureBits(value: string): string {
  const parts = value.split(".");
  const signature = parts[2]!;
  const last = signature.at(-1)!;
  const index = base64urlAlphabet.indexOf(last);
  const alternate = (index & 0x30) | ((index + 1) & 0x0f);
  parts[2] = `${signature.slice(0, -1)}${base64urlAlphabet[alternate]}`;
  return parts.join(".");
}

function tamperSignature(value: string): string {
  const parts = value.split(".");
  const signature = Buffer.from(parts[2]!, "base64url");
  signature[0] ^= 1;
  parts[2] = signature.toString("base64url");
  return parts.join(".");
}

const fetchKeys = async () =>
  new Response(JSON.stringify({ keys: [{ ...jwk, kid, alg: "RS256", use: "sig" }] }), {
    status: 200,
    headers: { "content-type": "application/json" },
  });

describe("GCP apply authorization", () => {
  test("binds a fresh Google-signed nonce to stage, project, and executor identity", async () => {
    const value = token();
    const proof = await verifyGcpApplyNonce(value, expected, fetchKeys, nowMs);
    expect(proof).toMatchObject({
      proof: "gcp-apply-authorized",
      stage: expected.stage,
      projectId: expected.projectId,
      executor: executor.name,
      executorIdentity: executor.identity,
      nonceDigest: applyNonceDigest(value),
      leaseExpiresAt: nowSeconds + GCP_APPLY_LEASE_MS / 1000,
    });
  });

  test("requires enough signed lifetime for the entire bounded mutation lease", async () => {
    await expect(
      verifyGcpApplyNonce(
        token({ exp: nowSeconds + GCP_APPLY_MIN_REMAINING_SECONDS - 1 }),
        expected,
        fetchKeys,
        nowMs,
      ),
    ).rejects.toThrow(/expired or outside/);

    const proof = await verifyGcpApplyNonce(token(), expected, fetchKeys, nowMs);
    expect(() => assertGcpApplyLeaseActive(proof, proof.leaseExpiresAt * 1000 - 1)).not.toThrow();
    expect(() => assertGcpApplyLeaseActive(proof, proof.leaseExpiresAt * 1000)).toThrow(/mutation lease expired/);

    await expect(Effect.runPromise(boundGcpApplyLease(Effect.never, 1))).rejects.toThrow();
    expect(() => boundGcpApplyLease(Effect.void, GCP_APPLY_LEASE_MS + 1)).toThrow(/duration is invalid/);
  });

  test("rejects replay scope, identity, signature, and expiry mismatches", async () => {
    await expect(
      verifyGcpApplyNonce(token(), { ...expected, stage: "gcp-prod" }, fetchKeys, nowMs),
    ).rejects.toThrow(/stage, project, or executor identity/);
    await expect(
      verifyGcpApplyNonce(token({ email: "other@agentrelay-96bulls-soc2-2610.iam.gserviceaccount.com" }), expected, fetchKeys, nowMs),
    ).rejects.toThrow(/stage, project, or executor identity/);
    await expect(verifyGcpApplyNonce(tamperSignature(token()), expected, fetchKeys, nowMs)).rejects.toThrow(
      /signature/,
    );
    await expect(
      verifyGcpApplyNonce(token({ iat: nowSeconds - 400, exp: nowSeconds - 1 }), expected, fetchKeys, nowMs),
    ).rejects.toThrow(/expired or outside/);
  });

  test("rejects a noncanonical signature encoding before hashing it as a nonce", async () => {
    const canonical = token();
    const alternate = mutateUnusedSignatureBits(canonical);
    const canonicalSignature = canonical.split(".")[2]!;
    const alternateSignature = alternate.split(".")[2]!;

    expect(alternate).not.toBe(canonical);
    expect(Buffer.from(alternateSignature, "base64url")).toEqual(
      Buffer.from(canonicalSignature, "base64url"),
    );
    await expect(verifyGcpApplyNonce(alternate, expected, fetchKeys, nowMs)).rejects.toThrow(
      /malformed/,
    );
  });

  test("atomically consumes a verified nonce only once", async () => {
    const root = mkdtempSync(join(tmpdir(), "96bulls-nonce-ledger-"));
    try {
      const proof = await verifyGcpApplyNonce(token(), expected, fetchKeys, nowMs);
      expect(consumeGcpApplyNonce(proof, root).proof).toBe("gcp-apply-authorized");
      expect(() => consumeGcpApplyNonce(proof, root)).toThrow(/already consumed/);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("refuses path-like stage scopes before touching a ledger outside the root", async () => {
    const root = mkdtempSync(join(tmpdir(), "96bulls-nonce-scope-"));
    try {
      const proof = await verifyGcpApplyNonce(token(), expected, fetchKeys, nowMs);
      expect(() => consumeGcpApplyNonce({ ...proof, stage: "gcp-../../outside" }, root)).toThrow(/scope is invalid/);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("uses an atomic state-bucket marker for cross-machine single use", async () => {
    const proof = await verifyGcpApplyNonce(token(), expected, fetchKeys, nowMs);
    const credential = { bearer: "ya29.test-global-ledger-token" };
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    await expect(
      consumeGcpApplyNonceGlobally(proof, credential, async (url, init) => {
        calls.push({ url: String(url), init });
        return Response.json({ generation: "1" });
      }),
    ).resolves.toMatchObject({ nonceDigest: proof.nonceDigest });
    const marker = new URL(calls[0]!.url);
    expect(marker.origin).toBe("https://storage.googleapis.com");
    expect(marker.pathname).toBe(
      "/upload/storage/v1/b/alchemy-state-agentworkforce-agentrelay-96bulls-soc2-2610/o",
    );
    expect(marker.searchParams.get("ifGenerationMatch")).toBe("0");
    expect(marker.searchParams.get("name")).toMatch(/^96bulls\/apply-nonces\/gcp-dev\/[a-f0-9]{64}\.used$/);
    expect(String(calls[0]!.init?.body)).not.toContain(credential.bearer);
    expect(new Headers(calls[0]!.init?.headers).get("authorization")).toBe(`Bearer ${credential.bearer}`);

    await expect(
      consumeGcpApplyNonceGlobally(proof, credential, async () => new Response(null, { status: 412 })),
    ).rejects.toThrow(/already consumed/);
    let caught: Error | undefined;
    try {
      await consumeGcpApplyNonceGlobally(proof, credential, async () => {
        throw new Error(`leaked ${credential.bearer}`);
      });
    } catch (error) {
      caught = error as Error;
    }
    expect(caught?.message).toMatch(/global ledger is unavailable/);
    expect(caught?.message).not.toContain(credential.bearer);
  });
});
