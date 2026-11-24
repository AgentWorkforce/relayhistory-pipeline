/**
 * The Claude transport for workspace-synthesis Stage 2 (docs/session-intel-workspace-synthesis.md, "The Claude
 * transport"): one non-streaming `POST /v1/messages` per call, through an INJECTED fetch (never the global one).
 *
 * - The key is a string the caller passes (read by the caller from a 1Password-injected environment at runtime,
 *   `op run`); this module reads no environment, no file and no credential store. The key goes only into the
 *   `x-api-key` header; it is never logged, and every error message is built from fixed codes and is redacted of the
 *   key anyway.
 * - Request: an approved model only (STAGE2_APPROVED_MODELS), `max_tokens` = the run's output cap, effort `low` (so
 *   thinking, which counts as output, does not eat the cap), the reply schema as structured output
 *   (`output_config.format`, JSON schema) with the keywords structured outputs reject stripped (the local strict
 *   parser stays the enforcement of every bound), no tools and no tool choice, the request's AbortSignal.
 * - Response: the text block is the JSON reply; usage input = input + cache creation + cache read tokens, output =
 *   output tokens; the answering model is reported. `max_tokens`, `refusal`, any other non-`end_turn` stop, a
 *   0-input success, no text, a malformed body and every HTTP error are a typed ClaudeTransportError; no partial
 *   text ever reaches the parser.
 * - Retry: at most ONE, only on HTTP 429 or 529 that carries a `retry-after` of whole seconds no longer than
 *   CLAUDE_RETRY.maxRetryAfterSeconds, and only while the signal is not aborted. A 429 without `retry-after` (a spend
 *   cap) or with a longer one is not retried. Nothing else is retried.
 */
import type { ModelTransportResponse } from "../../session-analysis/engine/model.ts";
import { STAGE2_APPROVED_MODELS, type Stage2ModelRequest, type Stage2ModelTransport } from "./stage2.ts";

export const ANTHROPIC_BASE_URL = "https://api.anthropic.com";
export const ANTHROPIC_MESSAGES_PATH = "/v1/messages";
/** The API version the request is written for (no temperature/top_p/top_k, which 400 on it). */
export const ANTHROPIC_VERSION = "2026-10-21";
/** Low effort: thinking stays on (Sonnet 5.5 cannot disable it with `disabled`) but spends little of the output cap. */
export const STAGE2_EFFORT = "low";
export const CLAUDE_RETRY = { maxRetries: 1, statuses: [429, 529] as readonly number[], maxRetryAfterSeconds: 30 } as const;
/** A response body larger than this is refused unread past the bound (10,000 output tokens is far below it). */
export const CLAUDE_MAX_RESPONSE_BYTES = 1024 * 1024;

export type ClaudeTransportErrorCode =
  | "model_not_approved"
  | "request_invalid"
  | "auth_failed"
  | "billing_error"
  | "permission_denied"
  | "not_found"
  | "request_too_large"
  | "rate_limited"
  | "overloaded"
  | "server_error"
  | "http_error"
  | "network_error"
  | "aborted"
  | "response_invalid"
  | "max_tokens"
  | "refusal"
  | "unexpected_stop"
  | "zero_input"
  | "no_text";

/** A controlled failure: a code, the HTTP status when there was one, and nothing from the request, body or key. */
export class ClaudeTransportError extends Error {
  constructor(
    readonly code: ClaudeTransportErrorCode,
    readonly status: number | null = null,
    readonly attempts = 1,
    /** The billed usage of a 2xx reply that was refused (max_tokens, refusal, ...), when it was well formed. */
    readonly usage: { inputTokens: number; outputTokens: number } | null = null,
  ) {
    super(`claude transport: ${code}${status === null ? "" : ` (HTTP ${status})`}`);
    this.name = "ClaudeTransportError";
  }
}

export interface ClaudeTransportOptions {
  /** The API key, supplied by the caller at runtime. Never read here from anywhere else. */
  apiKey: string;
  /** Required: the transport never uses the global fetch. */
  fetch: typeof globalThis.fetch;
  /** Default ANTHROPIC_BASE_URL; https only, no path, query, fragment or credentials. */
  baseUrl?: string;
  /** The wait before the one retry (tests inject it); must reject when the signal aborts. */
  sleep?: (ms: number, signal: AbortSignal) => Promise<void>;
}

const abortableSleep = (ms: number, signal: AbortSignal) => new Promise<void>((resolve, reject) => {
  if (signal.aborted) return reject(new Error("aborted"));
  const t = setTimeout(() => { signal.removeEventListener("abort", onAbort); resolve(); }, ms);
  const onAbort = () => { clearTimeout(t); reject(new Error("aborted")); };
  signal.addEventListener("abort", onAbort, { once: true });
});

// ---------------------------------------------------------------- schema

/** Keywords structured outputs reject; the strict local parser enforces them instead. */
export const UNSUPPORTED_SCHEMA_KEYWORDS: readonly string[] = [
  "minLength", "maxLength", "maxItems", "uniqueItems", "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum",
  "multipleOf", "minProperties", "maxProperties", "contains", "minContains", "maxContains",
];

/**
 * The schema as sent: a deep copy with UNSUPPORTED_SCHEMA_KEYWORDS removed and `minItems` above 1 removed (only 0
 * and 1 are accepted). Only schema positions are walked (`properties` values, `items`, `anyOf`/`allOf` members,
 * `$defs`), so a property that happens to be named like a keyword is kept. Fails closed (throws) on what the API
 * would 400 anyway: an object schema without `additionalProperties: false`, `oneOf`, `not`, a non-object node.
 */
export function stripUnsupportedSchema(schema: unknown): Record<string, unknown> {
  const walk = (node: unknown, where: string): Record<string, unknown> => {
    if (!node || typeof node !== "object" || Array.isArray(node)) throw new Error(`claude transport: schema node ${where} is not an object`);
    const n = node as Record<string, unknown>;
    if ("oneOf" in n || "not" in n) throw new Error(`claude transport: schema ${where} uses oneOf or not`);
    if (n.type === "object" && n.additionalProperties !== false) throw new Error(`claude transport: schema ${where} must set additionalProperties false`);
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(n)) {
      if (UNSUPPORTED_SCHEMA_KEYWORDS.includes(k)) continue;
      if (k === "minItems" && !(v === 0 || v === 1)) continue;
      if (k === "properties" || k === "$defs") {
        if (!v || typeof v !== "object" || Array.isArray(v)) throw new Error(`claude transport: schema ${where}.${k}`);
        out[k] = Object.fromEntries(Object.entries(v as Record<string, unknown>).map(([p, s]) => [p, walk(s, `${where}.${k}.${p}`)]));
      } else if (k === "items") {
        out[k] = walk(v, `${where}.items`);
      } else if (k === "anyOf" || k === "allOf") {
        if (!Array.isArray(v)) throw new Error(`claude transport: schema ${where}.${k}`);
        out[k] = v.map((s, i) => walk(s, `${where}.${k}[${i}]`));
      } else {
        out[k] = structuredClone(v);
      }
    }
    return out;
  };
  return walk(schema, "$");
}

// ---------------------------------------------------------------- request

/** The exact request body for one Stage 2 call (the golden in the tests). */
export function claudeRequestBody(request: Stage2ModelRequest): Record<string, unknown> {
  if (!STAGE2_APPROVED_MODELS.includes(request.model)) throw new ClaudeTransportError("model_not_approved");
  if (!Number.isSafeInteger(request.maxOutputTokens) || request.maxOutputTokens < 1) throw new ClaudeTransportError("request_invalid");
  if (typeof request.instructions !== "string" || typeof request.input !== "string" || request.input === "") throw new ClaudeTransportError("request_invalid");
  let schema: Record<string, unknown>;
  try { schema = stripUnsupportedSchema(request.schema); } catch { throw new ClaudeTransportError("request_invalid"); }
  return {
    model: request.model,
    max_tokens: request.maxOutputTokens,
    system: request.instructions,
    messages: [{ role: "user", content: request.input }],
    output_config: { effort: STAGE2_EFFORT, format: { type: "json_schema", schema } },
  };
}

// ---------------------------------------------------------------- response

type Raw = Record<string, unknown>;
const isObj = (v: unknown): v is Raw => !!v && typeof v === "object" && !Array.isArray(v);
const tokenCount = (v: unknown): number | null => (typeof v === "number" && Number.isSafeInteger(v) && v >= 0 ? v : null);
/** An absent or null cache field is 0; a present malformed one is not. */
const optionalCount = (v: unknown): number | null => (v === undefined || v === null ? 0 : tokenCount(v));

/** Map a 2xx Messages body to the engine response, or throw a typed failure. Never returns partial text. */
export function parseClaudeResponse(body: unknown, requestId: string | null): ModelTransportResponse {
  if (!isObj(body) || body.type !== "message" || !Array.isArray(body.content) || !isObj(body.usage)) throw new ClaudeTransportError("response_invalid");
  const u = body.usage;
  const input = tokenCount(u.input_tokens), output = tokenCount(u.output_tokens);
  const created = optionalCount(u.cache_creation_input_tokens), read = optionalCount(u.cache_read_input_tokens);
  const inputTokens = input === null || created === null || read === null ? null : input + created + read;
  const usage = inputTokens !== null && output !== null && Number.isSafeInteger(inputTokens) ? { inputTokens, outputTokens: output } : null;
  // Stop reason first: a cut-off or refused reply is never handed on, whatever the content holds.
  if (body.stop_reason === "max_tokens") throw new ClaudeTransportError("max_tokens", null, 1, usage);
  if (body.stop_reason === "refusal") throw new ClaudeTransportError("refusal", null, 1, usage);
  if (body.stop_reason !== "end_turn") throw new ClaudeTransportError("unexpected_stop", null, 1, usage);
  if (usage === null) throw new ClaudeTransportError("response_invalid");
  // A success that bills no input cannot be this request: refuse it rather than record a free call.
  if (usage.inputTokens === 0) throw new ClaudeTransportError("zero_input", null, 1, usage);
  if (typeof body.model !== "string" || body.model === "") throw new ClaudeTransportError("response_invalid");
  // Thinking blocks are skipped; exactly one text block is the structured reply.
  const texts = body.content.filter((b): b is Raw => isObj(b) && b.type === "text");
  if (texts.length !== 1 || typeof texts[0]!.text !== "string" || texts[0]!.text === "") throw new ClaudeTransportError("no_text", null, 1, usage);
  return { outputText: texts[0]!.text as string, inputTokens: usage.inputTokens, outputTokens: usage.outputTokens, providerRequestId: requestId, model: body.model };
}

const STATUS_CODES: Record<number, ClaudeTransportErrorCode> = {
  400: "request_invalid", 401: "auth_failed", 402: "billing_error", 403: "permission_denied", 404: "not_found",
  413: "request_too_large", 429: "rate_limited", 529: "overloaded",
};
const statusCode = (status: number): ClaudeTransportErrorCode => STATUS_CODES[status] ?? (status >= 500 ? "server_error" : "http_error");

/** Whole seconds in [0, max], else null (an HTTP-date or a longer wait is not retried). */
export function retryAfterSeconds(header: string | null, max: number = CLAUDE_RETRY.maxRetryAfterSeconds): number | null {
  if (header === null || !/^[0-9]{1,6}$/u.test(header.trim())) return null;
  const n = Number(header.trim());
  return n <= max ? n : null;
}

async function readBody(res: Response): Promise<unknown> {
  if (!res.body) throw new ClaudeTransportError("response_invalid", res.status);
  const reader = res.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    length += value.length;
    if (length > CLAUDE_MAX_RESPONSE_BYTES) {
      await reader.cancel().catch(() => {});
      throw new ClaudeTransportError("response_invalid", res.status);
    }
    chunks.push(value);
  }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const c of chunks) { bytes.set(c, offset); offset += c.length; }
  try {
    return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  } catch {
    throw new ClaudeTransportError("response_invalid", res.status);
  }
}

// ---------------------------------------------------------------- the transport

const KEY = /^[\x21-\x7e]{1,512}$/u;

export function createClaudeTransport(options: ClaudeTransportOptions): Stage2ModelTransport {
  const apiKey = options.apiKey;
  // Checked once; the message never contains the value.
  if (typeof apiKey !== "string" || !KEY.test(apiKey)) throw new Error("claude transport: apiKey must be a non-empty printable string");
  if (typeof options.fetch !== "function") throw new Error("claude transport: fetch is required");
  const fetcher = options.fetch;
  const base = new URL(options.baseUrl ?? ANTHROPIC_BASE_URL);
  if (base.protocol !== "https:" || base.username || base.password || base.search || base.hash || (base.pathname !== "/" && base.pathname !== ""))
    throw new Error("claude transport: baseUrl must be an https origin");
  const url = `${base.origin}${ANTHROPIC_MESSAGES_PATH}`;
  const sleep = options.sleep ?? abortableSleep;
  /** Defense in depth: no message leaves with the key in it. */
  const redact = (e: ClaudeTransportError) => {
    if (e.message.includes(apiKey)) e.message = e.message.split(apiKey).join("[redacted]");
    return e;
  };

  return {
    async complete(request: Stage2ModelRequest): Promise<ModelTransportResponse> {
      try {
        const body = JSON.stringify(claudeRequestBody(request));
        const signal = request.signal;
        for (let attempt = 1; ; attempt++) {
          if (signal.aborted) throw new ClaudeTransportError("aborted", null, attempt);
          let res: Response;
          try {
            res = await fetcher(url, {
              method: "POST",
              signal,
              // Never follow a redirect with the key; a 3xx is an http_error below.
              redirect: "manual",
              headers: { "content-type": "application/json", "x-api-key": apiKey, "anthropic-version": ANTHROPIC_VERSION },
              body,
            });
          } catch {
            throw new ClaudeTransportError(signal.aborted ? "aborted" : "network_error", null, attempt);
          }
          if (res.status >= 200 && res.status < 300) {
            let parsed: unknown;
            try { parsed = await readBody(res); } catch (e) {
              if (signal.aborted) throw new ClaudeTransportError("aborted", null, attempt);
              throw e instanceof ClaudeTransportError ? new ClaudeTransportError(e.code, e.status, attempt) : new ClaudeTransportError("response_invalid", res.status, attempt);
            }
            try {
              return parseClaudeResponse(parsed, res.headers.get("request-id"));
            } catch (e) {
              const c = e as ClaudeTransportError;
              throw new ClaudeTransportError(c.code, res.status, attempt, c.usage);
            }
          }
          // The error body is not read into anything that leaves this function.
          await res.body?.cancel().catch(() => {});
          const wait = CLAUDE_RETRY.statuses.includes(res.status) && attempt <= CLAUDE_RETRY.maxRetries ? retryAfterSeconds(res.headers.get("retry-after")) : null;
          if (wait === null) throw new ClaudeTransportError(statusCode(res.status), res.status, attempt);
          try { await sleep(wait * 1000, signal); } catch { throw new ClaudeTransportError("aborted", res.status, attempt); }
        }
      } catch (e) {
        throw redact(e instanceof ClaudeTransportError ? e : new ClaudeTransportError("request_invalid"));
      }
    },
  };
}
