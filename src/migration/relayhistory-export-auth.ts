/**
 * Migration assertions for the Neon-to-S3 export (O2 in
 * docs/relayhistory-export-design.md). RelayAuth issues one short-lived,
 * single-use token per session upload (one manifest commit), bound to a
 * migration grant: the export's API key, one org, the users the plan allows,
 * the plan digest and the ingest origin. Endpoint:
 * `POST /v1/tokens/migration-assertion` (relayauth, scope
 * `relayauth:assertion:create:migration`).
 *
 * Unlike the courier's RelayAuthAssertionProvider, nothing is cached: every
 * call mints a new token with a new nonce, because the ingest verifier
 * consumes each one once. Retrying an upload is idempotent on the ingest
 * side and gets a fresh assertion; an assertion is never replayed.
 */
import { randomBytes } from "node:crypto";

export interface MigrationAssertionConfig {
  /** RelayAuth base URL (https; plain http only on loopback). */
  baseUrl: string;
  grantId: string;
  /** The migration API key, read from 1Password at call time; never stored. */
  apiKey: () => Promise<string>;
  expiresInSeconds?: number;
  fetcher?: typeof fetch;
}

export interface MigrationAssertion {
  token: string;
  expiresAt: Date;
  nonce: string;
}

const LOOPBACK = new Set(["localhost", "127.0.0.1", "[::1]"]);
const ID = /^[A-Za-z0-9._:-]{1,255}$/;

export class MigrationAssertionClient {
  readonly #cfg: Required<Pick<MigrationAssertionConfig, "expiresInSeconds" | "fetcher">> & MigrationAssertionConfig;

  constructor(cfg: MigrationAssertionConfig) {
    const url = new URL(cfg.baseUrl);
    if (url.username || url.password || url.search || url.hash)
      throw new Error("migration assertion: RelayAuth URL must not carry credentials, query or fragment");
    if (url.protocol !== "https:" && !(url.protocol === "http:" && LOOPBACK.has(url.hostname)))
      throw new Error("migration assertion: RelayAuth URL must use https");
    if (!ID.test(cfg.grantId)) throw new Error("migration assertion: grantId is malformed");
    const expiresInSeconds = cfg.expiresInSeconds ?? 60;
    if (!Number.isInteger(expiresInSeconds) || expiresInSeconds < 10 || expiresInSeconds > 60)
      throw new Error("migration assertion: lifetime must be 10 to 60 seconds");
    this.#cfg = { ...cfg, baseUrl: cfg.baseUrl.replace(/\/$/, ""), expiresInSeconds, fetcher: cfg.fetcher ?? fetch };
  }

  /**
   * A fresh single-use assertion for one session upload of `sourceUserId` in
   * `workspaceId`. `upload` names the planned upload (`{key}`, its manifest
   * key); RelayAuth derives and signs the full `meta.upload` identity from its
   * stored plan (docs/contracts/relayhistory-export-plan-entry-v1.md, #43).
   * The contract names the request's `upload` but not its shape; `{key}` is
   * this client's reading until relayauth#110 confirms it.
   */
  async mint(sourceUserId: string, workspaceId: string, upload?: object): Promise<MigrationAssertion> {
    if (!ID.test(sourceUserId) || !ID.test(workspaceId))
      throw new Error("migration assertion: user or workspace id is malformed");
    const apiKey = await this.#cfg.apiKey();
    if (!apiKey) throw new Error("migration assertion: API key source returned nothing");
    const nonce = randomBytes(24).toString("base64url");
    const response = await this.#cfg.fetcher(`${this.#cfg.baseUrl}/v1/tokens/migration-assertion`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": apiKey },
      redirect: "error",
      signal: AbortSignal.timeout(10_000),
      body: JSON.stringify({
        grantId: this.#cfg.grantId,
        sourceUserId,
        workspaceId,
        nonce,
        expiresIn: this.#cfg.expiresInSeconds,
        ...(upload ? { upload } : {}),
      }),
    });
    if (response.status !== 201)
      throw Object.assign(new Error("migration assertion: RelayAuth refused the mint"), {
        code: `relayauth_http_${response.status}`,
        // A refused grant or user is permanent for this session; only overload is retried.
        retryable: response.status === 429 || response.status >= 500,
      });
    const body = (await response.json()) as { accessToken?: unknown; accessTokenExpiresAt?: unknown; tokenType?: unknown };
    const expiresAt = typeof body.accessTokenExpiresAt === "string" ? new Date(body.accessTokenExpiresAt) : null;
    if (body.tokenType !== "Bearer" || typeof body.accessToken !== "string" || !body.accessToken || !expiresAt || Number.isNaN(expiresAt.getTime()))
      throw new Error("migration assertion: RelayAuth returned an invalid assertion");
    return { token: body.accessToken, expiresAt, nonce };
  }
}
