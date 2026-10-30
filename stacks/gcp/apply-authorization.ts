import {
  createHash,
  createPublicKey,
  verify as verifySignature,
  type JsonWebKey as NodeJsonWebKey,
} from "node:crypto";
import { mkdirSync, openSync, closeSync, realpathSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";
import { GCP_APPLY_NONCE_ENV, type KubernetesExecutorInputs } from "./config.ts";

const GOOGLE_JWKS_URL = "https://www.googleapis.com/oauth2/v3/certs";
const GOOGLE_ISSUERS = new Set(["accounts.google.com", "https://accounts.google.com"]);
const JWT_PART = /^[A-Za-z0-9_-]+$/;
const KID = /^[A-Za-z0-9_-]{1,256}$/;

export interface GcpApplyAuthorizationInput {
  stage: string;
  projectId: string;
  executor: KubernetesExecutorInputs;
  nonceDigest: string;
  /** Forces the Action to execute, so persisted output cannot replay a nonce. */
  invocationId: string;
}

export interface GcpApplyAuthorizationProof {
  proof: "gcp-apply-authorized";
  stage: string;
  projectId: string;
  executor: string;
  executorIdentity: string;
  nonceDigest: string;
  expiresAt: number;
}

interface GoogleJwk {
  kty?: unknown;
  alg?: unknown;
  use?: unknown;
  kid?: unknown;
  n?: unknown;
  e?: unknown;
}

interface NonceClaims {
  iss?: unknown;
  aud?: unknown;
  sub?: unknown;
  email?: unknown;
  email_verified?: unknown;
  iat?: unknown;
  exp?: unknown;
}

export type ApplyAuthorizationFetch = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

export interface ApplyAuthorizationCredential {
  readonly bearer: string;
}

function fail(message: string): never {
  throw new Error(`96bulls: ${message} No GCP or Kubernetes mutations were authorized.`);
}

function decodePart(value: string): Buffer {
  if (!JWT_PART.test(value)) fail("GCP apply nonce is malformed.");
  try {
    const decoded = Buffer.from(value, "base64url");
    // Node accepts alternate encodings whose unused trailing bits are nonzero.
    // Require the unique base64url spelling before the raw token is hashed as
    // the replay-protection nonce.
    if (decoded.toString("base64url") !== value) fail("GCP apply nonce is malformed.");
    return decoded;
  } catch {
    fail("GCP apply nonce is malformed.");
  }
}

function jsonPart(value: string): Record<string, unknown> {
  try {
    const decoded = JSON.parse(decodePart(value).toString("utf8"));
    if (!decoded || typeof decoded !== "object" || Array.isArray(decoded)) throw new Error("shape");
    return decoded as Record<string, unknown>;
  } catch {
    fail("GCP apply nonce is malformed.");
  }
}

export function gcpApplyAudience(projectId: string, stage: string): string {
  return `https://96bulls.agentworkforce.dev/gcp-apply/${encodeURIComponent(projectId)}/${encodeURIComponent(stage)}`;
}

/** Non-secret fingerprint used to make the apply Action change for every nonce. */
export function applyNonceDigest(token = process.env[GCP_APPLY_NONCE_ENV]): string {
  if (!token) return "missing";
  return createHash("sha256").update(token).digest("hex");
}

async function googleKey(kid: string, fetcher: ApplyAuthorizationFetch): Promise<GoogleJwk> {
  const response = await fetcher(GOOGLE_JWKS_URL, {
    method: "GET",
    headers: { accept: "application/json" },
    redirect: "error",
  });
  if (!response.ok) fail("Google executor signing keys are unavailable.");
  let body: unknown;
  try {
    body = await response.json();
  } catch {
    fail("Google executor signing keys are malformed.");
  }
  const keys = (body as { keys?: unknown })?.keys;
  if (!Array.isArray(keys)) fail("Google executor signing keys are malformed.");
  const key = keys.find((candidate) => {
    const value = candidate as GoogleJwk;
    return value?.kid === kid && value.kty === "RSA" && value.alg === "RS256" && value.use === "sig";
  }) as GoogleJwk | undefined;
  if (!key || typeof key.n !== "string" || typeof key.e !== "string")
    fail("GCP apply nonce signing key is not approved.");
  return key;
}

/**
 * Verify a Google-signed service-account ID token. Its audience binds project
 * and stage; email binds the named executor identity. The signed token digest
 * is the one-use nonce because Google service-account ID tokens omit `jti`.
 */
export async function verifyGcpApplyNonce(
  token: string | undefined,
  expected: Pick<GcpApplyAuthorizationInput, "stage" | "projectId" | "executor">,
  fetcher: ApplyAuthorizationFetch = globalThis.fetch,
  nowMs = Date.now(),
): Promise<GcpApplyAuthorizationProof & { nonceId: string }> {
  if (!token || token.length > 16_384 || /\s/.test(token))
    fail(`set ${GCP_APPLY_NONCE_ENV} to a fresh Google-signed executor ID token.`);
  const parts = token.split(".");
  if (parts.length !== 3) fail("GCP apply nonce is malformed.");
  const [encodedHeader, encodedClaims, encodedSignature] = parts as [string, string, string];
  const header = jsonPart(encodedHeader);
  const claims = jsonPart(encodedClaims) as NonceClaims;
  if (header.alg !== "RS256" || typeof header.kid !== "string" || !KID.test(header.kid))
    fail("GCP apply nonce algorithm or key id is not approved.");
  const key = await googleKey(header.kid, fetcher);
  const signature = decodePart(encodedSignature);
  let valid = false;
  try {
    valid = verifySignature(
      "RSA-SHA256",
      Buffer.from(`${encodedHeader}.${encodedClaims}`),
      createPublicKey({ key: key as NodeJsonWebKey, format: "jwk" }),
      signature,
    );
  } catch {
    valid = false;
  }
  if (!valid) fail("GCP apply nonce signature is invalid.");

  const nowSeconds = Math.floor(nowMs / 1000);
  const audience = gcpApplyAudience(expected.projectId, expected.stage);
  if (
    !GOOGLE_ISSUERS.has(String(claims.iss)) ||
    claims.aud !== audience ||
    claims.email !== expected.executor.identity ||
    claims.email_verified !== true ||
    typeof claims.sub !== "string" ||
    !/^\d{6,32}$/.test(claims.sub) ||
    typeof claims.iat !== "number" ||
    typeof claims.exp !== "number"
  )
    fail("GCP apply nonce claims do not match the stage, project, or executor identity.");
  if (
    !Number.isInteger(claims.iat) ||
    !Number.isInteger(claims.exp) ||
    claims.iat > nowSeconds + 30 ||
    claims.iat < nowSeconds - 5 * 60 ||
    claims.exp <= nowSeconds ||
    claims.exp > nowSeconds + 60 * 60 ||
    claims.exp <= claims.iat
  )
    fail("GCP apply nonce is expired or outside the allowed freshness window.");
  const nonceDigest = createHash("sha256").update(token).digest("hex");
  return {
    proof: "gcp-apply-authorized",
    stage: expected.stage,
    projectId: expected.projectId,
    executor: expected.executor.name,
    executorIdentity: expected.executor.identity,
    nonceDigest,
    nonceId: nonceDigest,
    expiresAt: claims.exp,
  };
}

/** Atomic O_EXCL ledger: a verified nonce can authorize at most one apply. */
export function consumeGcpApplyNonce(
  proof: GcpApplyAuthorizationProof & { nonceId: string },
  ledgerRoot: string,
): GcpApplyAuthorizationProof {
  if (
    !/^gcp-[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(proof.stage) ||
    !/^[a-f0-9]{64}$/.test(proof.nonceId) ||
    proof.nonceId !== proof.nonceDigest
  )
    fail("GCP apply nonce ledger scope is invalid.");
  const scope = createHash("sha256")
    .update(`${proof.projectId}\0${proof.stage}\0${proof.executorIdentity}\0${proof.nonceId}`)
    .digest("hex");
  mkdirSync(ledgerRoot, { recursive: true, mode: 0o700 });
  const rootReal = realpathSync(ledgerRoot);
  const stageDirectory = join(rootReal, proof.stage);
  mkdirSync(stageDirectory, { recursive: true, mode: 0o700 });
  const stageReal = realpathSync(stageDirectory);
  const stageFromRoot = relative(rootReal, stageReal);
  if (stageFromRoot !== proof.stage)
    fail("GCP apply nonce ledger escaped its stage scope.");
  const directory = join(stageReal, "consumed-gcp-apply-nonces");
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const directoryReal = realpathSync(directory);
  const fromRoot = relative(rootReal, directoryReal);
  if (fromRoot.startsWith("..") || fromRoot === "" || fromRoot.startsWith("/"))
    fail("GCP apply nonce ledger escaped its stage scope.");
  const path = join(directory, `${scope}.used`);
  let descriptor: number;
  try {
    descriptor = openSync(path, "wx", 0o600);
  } catch {
    fail("GCP apply nonce was already consumed.");
  }
  try {
    writeFileSync(
      descriptor,
      JSON.stringify({
        projectId: proof.projectId,
        stage: proof.stage,
        executor: proof.executor,
        executorIdentity: proof.executorIdentity,
        nonceDigest: proof.nonceDigest,
        expiresAt: proof.expiresAt,
      }),
      { encoding: "utf8" },
    );
  } finally {
    closeSync(descriptor);
  }
  const { nonceId: _nonceId, ...publicProof } = proof;
  return publicProof;
}

/**
 * Cross-run/machine one-use gate. The existing Alchemy state bucket already
 * serializes this stack; an ifGenerationMatch=0 marker makes nonce reuse fail
 * even when a different named executor has no copy of the local ledger.
 */
export async function consumeGcpApplyNonceGlobally(
  proof: GcpApplyAuthorizationProof,
  credential: ApplyAuthorizationCredential,
  fetcher: ApplyAuthorizationFetch = globalThis.fetch,
): Promise<GcpApplyAuthorizationProof> {
  if (
    !/^[a-z][a-z0-9-]{4,28}[a-z0-9]$/.test(proof.projectId) ||
    !/^gcp-[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(proof.stage) ||
    !/^[a-f0-9]{64}$/.test(proof.nonceDigest) ||
    !credential?.bearer ||
    credential.bearer.length < 16 ||
    credential.bearer.length > 8192 ||
    /\s/.test(credential.bearer)
  )
    fail("GCP apply nonce global ledger input is invalid.");
  const bucket = `alchemy-state-agentworkforce-${proof.projectId}`;
  const scope = createHash("sha256")
    .update(`${proof.projectId}\0${proof.stage}\0${proof.executorIdentity}\0${proof.nonceDigest}`)
    .digest("hex");
  const object = `96bulls/apply-nonces/${proof.stage}/${scope}.used`;
  const url = new URL(`https://storage.googleapis.com/upload/storage/v1/b/${bucket}/o`);
  url.searchParams.set("uploadType", "media");
  url.searchParams.set("name", object);
  url.searchParams.set("ifGenerationMatch", "0");
  let response: Response;
  try {
    response = await fetcher(url, {
      method: "POST",
      headers: {
        authorization: `Bearer ${credential.bearer}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        proof: proof.proof,
        stage: proof.stage,
        projectId: proof.projectId,
        executor: proof.executor,
        executorIdentity: proof.executorIdentity,
        nonceDigest: proof.nonceDigest,
        expiresAt: proof.expiresAt,
      }),
      redirect: "error",
    });
  } catch {
    fail("GCP apply nonce global ledger is unavailable.");
  }
  if (response.status === 409 || response.status === 412)
    fail("GCP apply nonce was already consumed.");
  if (!response.ok)
    fail("GCP apply nonce global ledger refused the marker.");
  return proof;
}
