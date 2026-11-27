/**
 * The hold harness's PUT gate against an in-process fake S3 upstream (no Docker): pass-through is byte-exact and keeps
 * the Host header (SigV4), a hold pauses exactly one matching request, and a held request is delivered late, after its
 * client gave up and after later requests passed. put-gate.minio.test.ts proves the same against real MinIO.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { startPutGate, type PutGate } from "./put-gate.ts";

interface Seen { method: string; url: string; host: string | undefined; body: string }

describe("hold harness: PUT gate (fake upstream)", () => {
  let upstream: http.Server, base: string, gate: PutGate;
  const objects = new Map<string, string>(), seen: Seen[] = [];

  beforeAll(async () => {
    upstream = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (c: Buffer) => chunks.push(c));
      req.on("end", () => {
        const body = Buffer.concat(chunks).toString("utf8"), url = req.url ?? "/";
        seen.push({ method: req.method ?? "", url, host: req.headers.host, body });
        if (req.method === "PUT") {
          // S3 semantics for create-only writes: 412 when the key exists.
          if (req.headers["if-none-match"] === "*" && objects.has(url)) { res.writeHead(412); res.end("PreconditionFailed"); return; }
          objects.set(url, body); res.writeHead(200, { etag: `"${body.length}"` }); res.end(); return;
        }
        if (objects.has(url)) { res.writeHead(200); res.end(objects.get(url)); } else { res.writeHead(404); res.end("NoSuchKey"); }
      });
    });
    await new Promise<void>((r) => upstream.listen(0, "127.0.0.1", r));
    base = `http://127.0.0.1:${(upstream.address() as AddressInfo).port}`;
    gate = await startPutGate({ upstream: base });
  });
  afterAll(async () => { await gate?.close(); await new Promise((r) => upstream?.close(r)); });

  test("refuses a non-loopback or credentialed upstream", async () => {
    await expect(startPutGate({ upstream: "http://example.com:9000" })).rejects.toThrow("put_gate_loopback_upstream_required");
    await expect(startPutGate({ upstream: "https://127.0.0.1:9000" })).rejects.toThrow("put_gate_loopback_upstream_required");
    await expect(startPutGate({ upstream: "http://u:p@127.0.0.1:9000" })).rejects.toThrow("put_gate_loopback_upstream_required");
  });

  test("pass-through: method, path, body and the client's Host header reach the upstream unchanged", async () => {
    const r = await fetch(`${gate.url}/b/pass/through.json?x=1`, { method: "PUT", body: "hello" });
    expect(r.status).toBe(200);
    expect(r.headers.get("etag")).toBe('"5"');
    expect(seen.at(-1)).toEqual({ method: "PUT", url: "/b/pass/through.json?x=1", host: new URL(gate.url).host, body: "hello" });
    expect(await (await fetch(`${gate.url}/b/pass/through.json?x=1`)).text()).toBe("hello");
    expect(gate.log.at(-2)).toEqual({ method: "PUT", key: "pass/through.json", held: false, status: 200 });
  });

  test("a held PUT is paused, survives its client giving up, and lands after a later read", async () => {
    const held = gate.hold({ method: "PUT", key: "org/marker.json" });
    const before = seen.length;
    const put = fetch(`${gate.url}/b/org/marker.json`, { method: "PUT", body: "old", headers: { "if-none-match": "*" } });
    const req = await held.arrived;
    expect(req).toMatchObject({ method: "PUT", key: "org/marker.json", path: "/b/org/marker.json" });
    expect(new TextDecoder().decode(req.body)).toBe("old");
    expect(seen.length).toBe(before); // not forwarded
    held.abandon();
    await expect(put).rejects.toThrow(); // the client sees a dropped connection, as after a timeout
    // ...and did not silently re-send it (fetch retries a reset on a reused keep-alive socket; the gate never reuses).
    expect(gate.log.filter((e) => e.key === "org/marker.json" && e.method === "PUT")).toEqual([{ method: "PUT", key: "org/marker.json", held: true }]);
    // A later read passes straight through and sees nothing.
    expect((await fetch(`${gate.url}/b/org/marker.json`)).status).toBe(404);
    const late = await held.deliver();
    expect(late.status).toBe(200);
    expect(await held.deliver()).toBe(late); // delivered once
    expect(seen.filter((s) => s.url === "/b/org/marker.json").map((s) => s.method)).toEqual(["GET", "PUT"]);
    expect(await (await fetch(`${gate.url}/b/org/marker.json`)).text()).toBe("old");
  });

  test("a held create-only PUT delivered after a later writer is refused by the upstream", async () => {
    const held = gate.hold({ key: (k) => k.endsWith("/race.json"), where: (r) => r.headers["if-none-match"] === "*" });
    const put = fetch(`${gate.url}/b/org/race.json`, { method: "PUT", body: "old", headers: { "if-none-match": "*" } });
    await held.arrived;
    // The hold matched exactly one request: a second create-only PUT on the same key passes.
    expect((await fetch(`${gate.url}/b/org/race.json`, { method: "PUT", body: "new", headers: { "if-none-match": "*" } })).status).toBe(200);
    const late = await held.deliver();
    expect(late.status).toBe(412);
    expect((await put).status).toBe(412); // the still-waiting client gets the upstream's late answer
    expect(await (await fetch(`${gate.url}/b/org/race.json`)).text()).toBe("new");
  });

  test("respond answers the client without forwarding; cancel disarms an unmatched hold", async () => {
    const held = gate.hold({ method: "PUT", key: "org/timeout.json" });
    const put = fetch(`${gate.url}/b/org/timeout.json`, { method: "PUT", body: "x" });
    await held.arrived;
    held.respond(503, "SlowDown");
    expect((await put).status).toBe(503);
    expect((await fetch(`${gate.url}/b/org/timeout.json`)).status).toBe(404);
    expect((await held.deliver()).status).toBe(200);
    const idle = gate.hold({ method: "DELETE" });
    idle.cancel();
    await expect(idle.arrived).rejects.toThrow("put_gate_hold_cancelled");
    expect(() => idle.deliver()).toThrow("put_gate_nothing_held");
    await fetch(`${gate.url}/b/org/timeout.json`, { method: "DELETE" });
    expect(gate.log.at(-1)).toMatchObject({ method: "DELETE", held: false }); // the cancelled hold matched nothing
  });
});
