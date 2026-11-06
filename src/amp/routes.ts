import { timingSafeEqual } from "node:crypto";
import { Hono } from "hono";
import type { AppEnv } from "../shared/vendor.ts";
import { ampThreadId, AMP_CLI_VERSION, type AmpOperation } from "./provider.ts";
import { failure, runAmpProcess, type AmpResult } from "./process.ts";

const MAX_BODY = 100 * 1024;
function authorized(actual: string | undefined, token: string): boolean {
  const expected = Buffer.from(`Bearer ${token}`);
  const supplied = Buffer.from(actual ?? "");
  return (
    supplied.length === expected.length && timingSafeEqual(supplied, expected)
  );
}
function operationFrom(value: unknown): AmpOperation {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error();
  const op = value as Record<string, unknown>;
  if (op.kind === "discover" && Object.keys(op).length === 1)
    return { kind: "discover" };
  if (typeof op.threadId !== "string") throw new Error();
  const threadId = ampThreadId(op.threadId);
  if (op.kind === "export" && Object.keys(op).length === 2)
    return { kind: "export", threadId };
  if (
    op.kind === "inject" &&
    Object.keys(op).length === 3 &&
    typeof op.text === "string" &&
    op.text &&
    !op.text.includes(String.fromCharCode(0)) &&
    Buffer.byteLength(op.text) <= 64 * 1024
  )
    return { kind: "inject", threadId, text: op.text };
  throw new Error();
}
export function createAmpTransportRoutes(options: {
  token: () => string | undefined;
  run?: (key: string, operation: AmpOperation) => Promise<AmpResult>;
}) {
  const app = new Hono<AppEnv>();
  const run = options.run ?? runAmpProcess;
  let running = 0;
  app.post("/v1/internal/amp/operation", async (c) => {
    c.header("Cache-Control", "no-store");
    const token = options.token();
    if (!token || token === "unset" || token.length < 32)
      return c.json(failure("rejected", "amp_transport_not_configured"), 503);
    if (!authorized(c.req.header("Authorization"), token))
      return c.json(failure("rejected", "unauthorized"), 401);
    if (
      !c.req
        .header("Content-Type")
        ?.toLowerCase()
        .startsWith("application/json")
    )
      return c.json(failure("rejected", "amp_invalid_request"), 415);
    // Bound the actual stream too: Content-Length is optional and untrusted.
    const reader = c.req.raw.body?.getReader();
    if (!reader) return c.json(failure("rejected", "amp_invalid_request"), 400);
    const chunks: Uint8Array[] = [];
    let size = 0;
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > MAX_BODY)
          return c.json(failure("rejected", "amp_request_too_large"), 413);
        chunks.push(value);
      }
    } catch {
      return c.json(failure("rejected", "amp_invalid_request"), 400);
    } finally {
      await reader.cancel().catch(() => {});
    }
    let key: string;
    let operation: AmpOperation;
    try {
      const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      if (
        body.v !== 1 ||
        body.cliVersion !== AMP_CLI_VERSION ||
        Object.keys(body).length !== 4 ||
        typeof body.key !== "string" ||
        !/^sgamp_[A-Za-z0-9_-]{16,512}$/.test(body.key)
      )
        throw new Error();
      key = body.key;
      operation = operationFrom(body.operation);
    } catch {
      return c.json(failure("rejected", "amp_invalid_request"), 400);
    }
    if (running >= 4)
      return c.json(failure("rejected", "amp_transport_busy"), 429);
    running++;
    try {
      return c.json(await run(key, operation));
    } catch {
      return c.json(
        failure(
          operation.kind === "inject" ? "unknown" : "rejected",
          "amp_process_failed",
        ),
        502,
      );
    } finally {
      running--;
    }
  });
  return app;
}
