/** Bounded Responses transport for full, captured-source Bob work. No model is
 * selected or called by default. Contract: official OpenAI Structured Outputs,
 * https://developers.openai.com/api/docs/guides/structured-outputs .
 * Inputs are already authorized/scrubbed fragments, never raw pointers to fetch. */
import { MODEL_PATTERN, modelResponsesUrl } from "../session-brief/config.ts";

export const FULL_MODEL_REQUEST_BYTES = 128 * 1024;
export const FULL_MODEL_RESPONSE_BYTES = 256 * 1024;
export interface FullModelConfig { model: string; url: string; timeoutMs: number; maxOutputTokens: number }
export class FullModelError extends Error {
  constructor(readonly code: string, readonly retryable: boolean) { super(code); this.name = "FullModelError"; }
}
function no(code: string, retryable = false): never { throw new FullModelError(code, retryable); }
function integer(raw: string | undefined, fallback: number, min: number, max: number): number {
  if (raw === undefined || raw === "") return fallback;
  if (!/^[0-9]{1,6}$/.test(raw)) no("model_config_invalid");
  const n = Number(raw);
  if (n < min || n > max) no("model_config_invalid");
  return n;
}
/** Explicit model plus the existing approved Responses destination policy.
 * The caller supplies the existing managed-secret key from RAM, not this config. */
export function fullModelConfig(env: NodeJS.ProcessEnv = process.env): FullModelConfig | null {
  const model = (env.BOB_COSTAS_MODEL ?? "off").trim();
  if (!model || ["off", "extractive"].includes(model)) return null;
  if (!MODEL_PATTERN.test(model)) no("model_config_invalid");
  return Object.freeze({
    model, url: modelResponsesUrl(env),
    timeoutMs: integer(env.BOB_COSTAS_MODEL_TIMEOUT_MS, 45_000, 1_000, 60_000),
    maxOutputTokens: integer(env.BOB_COSTAS_MODEL_OUTPUT_TOKENS, 2_048, 64, 8_192),
  });
}
export interface FullModelRequest {
  instructions: string; input: string; schemaName: string;
  schema: Record<string, unknown>; signal: AbortSignal;
}
export interface FullModelResult {
  value: Record<string, unknown>; model: string;
  tokensIn: number; tokensOut: number; providerRequestId: string | null;
}
export interface FullModelTransport { complete(request: FullModelRequest): Promise<FullModelResult> }
const object = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
const tokenCount = (v: unknown): v is number => Number.isSafeInteger(v) && Number(v) >= 0;
function statusError(status: number): never {
  if (status === 401 || status === 403) no("model_auth_failed");
  if (status === 429) no("model_rate_limited", true);
  if (status === 408) no("model_timeout", true);
  if (status >= 500) no("model_unavailable", true);
  no("model_request_failed");
}
async function responseJson(response: Response, signal: AbortSignal): Promise<unknown> {
  const declared = response.headers.get("content-length");
  if (declared !== null && (!/^[0-9]+$/.test(declared) || Number(declared) > FULL_MODEL_RESPONSE_BYTES)) {
    void response.body?.cancel().catch(() => {});
    no("model_output_bound");
  }
  if (!response.body) no("model_output_invalid");
  const reader = response.body.getReader(), parts: Uint8Array[] = [];
  let bytes = 0;
  const cancel = () => { void reader.cancel().catch(() => {}); };
  signal.addEventListener("abort", cancel, { once: true });
  try {
    for (;;) {
      signal.throwIfAborted();
      const part = await reader.read();
      signal.throwIfAborted();
      if (part.done) break;
      bytes += part.value.byteLength;
      if (bytes > FULL_MODEL_RESPONSE_BYTES) no("model_output_bound");
      parts.push(part.value);
    }
    const all = new Uint8Array(bytes); let at = 0;
    for (const part of parts) { all.set(part, at); at += part.byteLength; }
    return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(all));
  } finally {
    signal.removeEventListener("abort", cancel); cancel(); reader.releaseLock();
  }
}
/** Real provider contract with an injectable HTTP port for offline proofs.
 * No redirect, prompt/output log, persistent conversation, or storage request. */
export function createFullModelTransport(options: {
  config: FullModelConfig; apiKey: () => string | undefined;
  fetch?: typeof fetch;
}): FullModelTransport {
  return {
    async complete(request) {
      request.signal.throwIfAborted();
      if (!/^[A-Za-z0-9_-]{1,64}$/.test(request.schemaName) || !object(request.schema) ||
          typeof request.instructions !== "string" || !request.instructions.trim() || typeof request.input !== "string")
        no("model_request_invalid");
      if (Buffer.byteLength(request.input) > FULL_MODEL_REQUEST_BYTES || Buffer.byteLength(request.instructions) > FULL_MODEL_REQUEST_BYTES) no("model_request_bound");
      const body = JSON.stringify({
        model: options.config.model, store: false, max_output_tokens: options.config.maxOutputTokens,
        instructions: request.instructions, input: request.input,
        text: { format: { type: "json_schema", name: request.schemaName, strict: true, schema: request.schema } },
      });
      if (Buffer.byteLength(body) > FULL_MODEL_REQUEST_BYTES) no("model_request_bound");
      let key: string | undefined;
      try { key = options.apiKey()?.trim(); } catch { no("model_not_configured"); }
      if (!key || key.endsWith("-disabled")) no("model_not_configured");
      const signal = AbortSignal.any([request.signal, AbortSignal.timeout(options.config.timeoutMs)]);
      let response: Response;
      try {
        response = await (options.fetch ?? fetch)(options.config.url, {
          method: "POST", redirect: "manual", signal,
          headers: { authorization: `Bearer ${key}`, "content-type": "application/json" }, body,
        });
      } catch { no(signal.aborted ? "model_timeout" : "model_unavailable", true); }
      if (!response.ok) { void response.body?.cancel().catch(() => {}); statusError(response.status); }
      let payload: unknown;
      try { payload = await responseJson(response, signal); }
      catch (error) { if (error instanceof FullModelError) throw error; no(signal.aborted ? "model_timeout" : "model_output_invalid", signal.aborted); }
      if (!object(payload) || !Array.isArray(payload.output)) no("model_output_invalid");
      if (payload.status !== "completed") no("model_output_incomplete");
      const text: string[] = [];
      for (const item of payload.output) {
        if (!object(item) || !Array.isArray(item.content)) continue;
        for (const part of item.content) {
          if (!object(part)) no("model_output_invalid");
          if (part.type === "refusal") no("model_refusal");
          if (part.type === "output_text" && typeof part.text === "string") text.push(part.text);
        }
      }
      let value: unknown;
      try { value = JSON.parse(text.join("")); } catch { no("model_output_invalid"); }
      if (!object(value) || !object(payload.usage) || !tokenCount(payload.usage.input_tokens) || !tokenCount(payload.usage.output_tokens))
        no("model_output_invalid");
      if (typeof payload.model !== "string" || !MODEL_PATTERN.test(payload.model)) no("model_output_invalid");
      const requestId = response.headers.get("x-request-id");
      return { value, model: payload.model, tokensIn: payload.usage.input_tokens, tokensOut: payload.usage.output_tokens,
        providerRequestId: requestId && /^[A-Za-z0-9_.:-]{1,200}$/.test(requestId) ? requestId : null };
    },
  };
}
