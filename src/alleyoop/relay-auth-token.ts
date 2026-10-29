import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const LOOPBACK_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);

export interface BearerTokenProvider {
  get(): Promise<string>;
  /** Returns true when retrying with another token can change the outcome. */
  invalidate(): boolean;
}

export function fixedBearerToken(token: string): BearerTokenProvider {
  if (!token) throw new Error("96bulls: bearer token is empty");
  return {
    async get() {
      return token;
    },
    invalidate() {
      return false;
    },
  };
}

export function mintedBearerToken(mint: () => string | Promise<string>): BearerTokenProvider {
  return {
    async get() {
      const token = await mint();
      if (!token) throw new Error("96bulls: bearer token minter returned an empty token");
      return token;
    },
    invalidate() {
      return true;
    },
  };
}

export interface RelayAuthAssertionConfig {
  baseUrl: string;
  historyOrigin: string;
  orgId: string;
  workspaceId: string;
  sponsorId: string;
  apiKey: () => Promise<string>;
  expiresInSeconds?: number;
  refreshSkewSeconds?: number;
  fetcher?: typeof fetch;
  now?: () => number;
}

interface RelayAuthAssertionResponse {
  accessToken?: unknown;
  accessTokenExpiresAt?: unknown;
  tokenType?: unknown;
}

/**
 * Mints origin-bound RelayAuth assertions just in time. The assertion and API
 * key exist only in process memory; callers supply the key from a central
 * broker, secret manager, or other non-persistent source.
 */
export class RelayAuthAssertionProvider implements BearerTokenProvider {
  readonly #cfg: Required<Pick<RelayAuthAssertionConfig, "expiresInSeconds" | "refreshSkewSeconds" | "fetcher" | "now">> & RelayAuthAssertionConfig;
  #cached?: { token: string; expiresAtMs: number };
  #minting?: Promise<string>;

  constructor(cfg: RelayAuthAssertionConfig) {
    const baseUrl = new URL(cfg.baseUrl);
    const historyOrigin = new URL(cfg.historyOrigin);
    if (baseUrl.username || baseUrl.password || baseUrl.search || baseUrl.hash) {
      throw new Error("96bulls: RELAYAUTH_URL must not contain credentials, query or fragment");
    }
    if (baseUrl.protocol !== "https:" && !(baseUrl.protocol === "http:" && LOOPBACK_HOSTS.has(baseUrl.hostname))) {
      throw new Error("96bulls: RELAYAUTH_URL must use https (plain http is loopback-only)");
    }
    if (historyOrigin.origin !== cfg.historyOrigin || historyOrigin.pathname !== "/") {
      throw new Error("96bulls: RELAYAUTH_HISTORY_ORIGIN must be an exact bare origin");
    }
    this.#cfg = {
      ...cfg,
      baseUrl: cfg.baseUrl.replace(/\/$/, ""),
      expiresInSeconds: cfg.expiresInSeconds ?? 60,
      refreshSkewSeconds: cfg.refreshSkewSeconds ?? 15,
      fetcher: cfg.fetcher ?? fetch,
      now: cfg.now ?? Date.now,
    };
    if (this.#cfg.expiresInSeconds < 30 || this.#cfg.expiresInSeconds > 300) {
      throw new Error("96bulls: RelayAuth assertion lifetime must be between 30 and 300 seconds");
    }
    if (this.#cfg.refreshSkewSeconds < 0 || this.#cfg.refreshSkewSeconds >= this.#cfg.expiresInSeconds) {
      throw new Error("96bulls: RelayAuth refresh skew must be shorter than the assertion lifetime");
    }
  }

  async get(): Promise<string> {
    const usableUntil = this.#cfg.now() + this.#cfg.refreshSkewSeconds * 1_000;
    if (this.#cached && this.#cached.expiresAtMs > usableUntil) return this.#cached.token;
    if (!this.#minting) this.#minting = this.#mint().finally(() => (this.#minting = undefined));
    return this.#minting;
  }

  invalidate(): boolean {
    this.#cached = undefined;
    return true;
  }

  async #mint(): Promise<string> {
    const apiKey = await this.#cfg.apiKey();
    if (!apiKey) throw new Error("96bulls: RelayAuth API key source returned an empty value");
    const response = await this.#cfg.fetcher(`${this.#cfg.baseUrl}/v1/tokens/relayhistory-assertion`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": apiKey },
      redirect: "error",
      signal: AbortSignal.timeout(10_000),
      body: JSON.stringify({
        orgId: this.#cfg.orgId,
        workspaceId: this.#cfg.workspaceId,
        sponsorId: this.#cfg.sponsorId,
        scopes: ["rth:sync", "rth:read"],
        expiresIn: this.#cfg.expiresInSeconds,
        historyOrigin: this.#cfg.historyOrigin,
      }),
    });
    if (response.status !== 201) {
      throw Object.assign(new Error("96bulls: RelayAuth assertion mint refused"), {
        code: `relayauth_http_${response.status}`,
        retryable: response.status === 429 || response.status >= 500,
      });
    }
    const body = (await response.json()) as RelayAuthAssertionResponse;
    const expiresAtMs = typeof body.accessTokenExpiresAt === "string" ? Date.parse(body.accessTokenExpiresAt) : Number.NaN;
    if (body.tokenType !== "Bearer" || typeof body.accessToken !== "string" || !body.accessToken || !Number.isFinite(expiresAtMs)) {
      throw Object.assign(new Error("96bulls: RelayAuth returned an invalid assertion"), {
        code: "relayauth_invalid_response",
        retryable: false,
      });
    }
    this.#cached = { token: body.accessToken, expiresAtMs };
    return body.accessToken;
  }
}

/**
 * Parse a JSON argv array rather than a shell command. The command's stdout is
 * the secret; stderr and the secret itself are never included in thrown errors.
 */
export function secretCommand(commandJson: string): () => Promise<string> {
  let command: unknown;
  try {
    command = JSON.parse(commandJson);
  } catch {
    throw new Error("96bulls: RELAYAUTH_API_KEY_COMMAND_JSON must be a JSON argv array");
  }
  if (!Array.isArray(command) || command.length === 0 || command.length > 32 || command.some((part) => typeof part !== "string" || !part)) {
    throw new Error("96bulls: RELAYAUTH_API_KEY_COMMAND_JSON must contain 1-32 non-empty strings");
  }
  const [file, ...args] = command as string[];
  return async () => {
    try {
      const { stdout } = await execFileAsync(file!, args, { encoding: "utf8", timeout: 10_000, maxBuffer: 16 * 1024 });
      const secret = stdout.trim();
      if (!secret || /\s/.test(secret)) throw new Error("invalid secret output");
      return secret;
    } catch {
      throw Object.assign(new Error("96bulls: RelayAuth API key command failed"), {
        code: "relayauth_api_key_source_failed",
        retryable: true,
      });
    }
  };
}
