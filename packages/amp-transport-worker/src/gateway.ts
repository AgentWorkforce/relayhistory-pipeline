import { timingSafeEqual } from "node:crypto";
import { AMP_CLI_VERSION } from "../../../src/amp/provider.ts";

const MAX_BODY = 100 * 1024;
const response = (status: number, code: string) => Response.json(
  { v: 1, cliVersion: AMP_CLI_VERSION, ok: false, outcome: "rejected", code },
  { status, headers: { "Cache-Control": "no-store" } },
);

// Reject unauthorized/oversized requests before starting the singleton container.
export async function gateway(
  request: Request,
  token: string | undefined,
  forward: (bounded: Request) => Promise<Response>,
): Promise<Response> {
  const url = new URL(request.url);
  if (url.pathname !== "/v1/internal/amp/operation" || url.search)
    return response(404, "not_found");
  if (request.method !== "POST") return response(405, "method_not_allowed");
  if (!token || token === "unset" || token.length < 32)
    return response(503, "amp_transport_not_configured");
  const actual = Buffer.from(request.headers.get("Authorization") ?? "");
  const expected = Buffer.from(`Bearer ${token}`);
  if (actual.length !== expected.length || !timingSafeEqual(actual, expected))
    return response(401, "unauthorized");
  if (!request.headers.get("Content-Type")?.toLowerCase().startsWith("application/json"))
    return response(415, "amp_invalid_request");
  const reader = request.body?.getReader();
  if (!reader) return response(400, "amp_invalid_request");
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > MAX_BODY) return response(413, "amp_request_too_large");
      chunks.push(value);
    }
  } catch {
    return response(400, "amp_invalid_request");
  } finally {
    await reader.cancel().catch(() => {});
  }
  try {
    // No retry: a lost response may follow a successful native submission.
    const answer = await forward(new Request(request.url, {
      method: "POST", headers: request.headers, body: Buffer.concat(chunks),
    }));
    const headers = new Headers(answer.headers);
    headers.set("Cache-Control", "no-store");
    return new Response(answer.body, { status: answer.status, headers });
  } catch {
    // An invalid protocol envelope makes Cloud classify writes as unknown and
    // read-only operations as rejected. Acceptance is no longer knowable here.
    return Response.json({ error: { code: "amp_transport_unavailable" } }, {
      status: 502, headers: { "Cache-Control": "no-store" },
    });
  }
}
