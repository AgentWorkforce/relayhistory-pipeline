import { expect, test } from "bun:test";
import { gateway } from "./gateway.ts";

const token = "fixture-only-" + "a".repeat(40);
const url = "https://transport.example/v1/internal/amp/operation";
const headers = { Authorization: `Bearer ${token}`, "Content-Type": "application/json" };
const request = (extra: RequestInit = {}) => new Request(url, {
  method: "POST", headers, body: '{"private":"exact message"}', ...extra,
});

test("invalid route, method, configuration and authority never start native work", async () => {
  let forwarded = 0;
  const forward = async () => { forwarded++; return new Response(); };
  for (const [input, configured, status] of [
    [new Request("https://transport.example/healthz"), token, 404],
    [new Request(url + "?thread=other"), token, 404],
    [new Request(url.replace("/amp/", "/%61mp/")), token, 404],
    [new Request(url), token, 405],
    [request(), undefined, 503],
    [request(), "unset", 503],
    [request({ headers: { "Content-Type": "application/json" } }), token, 401],
    [request({ headers: { ...headers, Authorization: `Bearer ${token}x` } }), token, 401],
    [request({ headers: { ...headers, "Content-Type": "text/plain" } }), token, 415],
  ] as const) {
    const result = await gateway(input, configured, forward);
    expect(result.status).toBe(status);
    expect(result.headers.get("Cache-Control")).toBe("no-store");
  }
  expect(forwarded).toBe(0);
});

test("an oversized stream is cancelled before container acquisition", async () => {
  let cancelled = false;
  let forwarded = false;
  const body = new ReadableStream<Uint8Array>({
    start(controller) { controller.enqueue(new Uint8Array(100 * 1024 + 1)); },
    cancel() { cancelled = true; },
  });
  const result = await gateway(request({ body }), token, async () => {
    forwarded = true; return new Response();
  });
  expect(result.status).toBe(413);
  expect((await result.json()).outcome).toBe("rejected");
  expect(cancelled).toBe(true);
  expect(forwarded).toBe(false);
});

test("the exact bounded body reaches the native service once", async () => {
  const input = request();
  const original = await input.clone().text();
  let forwards = 0;
  const result = await gateway(input, token, async bounded => {
    forwards++;
    expect(await bounded.text()).toBe(original);
    expect(bounded.headers.get("Authorization")).toBe(headers.Authorization);
    return new Response('{"ok":true,"value":"provider result"}', {
      headers: { "Content-Type": "application/json" },
    });
  });
  expect(forwards).toBe(1);
  expect(await result.json()).toEqual({ ok: true, value: "provider result" });
  expect(result.headers.get("Cache-Control")).toBe("no-store");
});

test("an ambiguous native failure is never retried or returned as success", async () => {
  let forwards = 0;
  const result = await gateway(request(), token, async () => {
    forwards++; throw new Error("private request material must not be returned");
  });
  expect(forwards).toBe(1);
  expect(result.status).toBe(502);
  expect(await result.json()).toEqual({ error: { code: "amp_transport_unavailable" } });
});

test("a broken request stream is rejected without native work", async () => {
  const body = new ReadableStream<Uint8Array>({
    start(controller) { controller.error(new Error("private stream error")); },
  });
  let forwards = 0;
  const result = await gateway(request({ body }), token, async () => {
    forwards++; return new Response();
  });
  expect(result.status).toBe(400);
  expect(forwards).toBe(0);
});
