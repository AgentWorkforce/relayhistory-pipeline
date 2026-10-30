import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { Hono } from "hono";
import { requestLog } from "../request-log.ts";

const app = () => {
  const root = new Hono();
  root.use("*", async (c, next) => {
    c.set("traceId" as never, "trace-1" as never);
    await next();
  });
  root.use("*", requestLog());
  const v1 = new Hono();
  v1.use("*", async (c, next) => (c.req.header("authorization") ? next() : c.json({}, 401)));
  v1.get("/pipeline/traces/:traceId", (c) => c.json({}));
  v1.post("/pipeline/chunks", (c) => c.json({}, 500));
  root.route("/v1", v1);
  return root;
};
const lineFor = async (path: string, init?: RequestInit) => {
  const lines: string[] = [];
  spyOn(process.stdout, "write").mockImplementation(((chunk: unknown) => (lines.push(String(chunk)), true)) as never);
  await app().request(path, init);
  return JSON.parse(lines.find((line) => line.includes("request completed"))!);
};
afterEach(() => (process.stdout.write as unknown as { mockRestore(): void }).mockRestore?.());
const auth = { headers: { authorization: "Bearer t" } };

describe("request log", () => {
  test("names the template, never the ids or query in the URL", async () => {
    const line = await lineFor("/v1/pipeline/traces/planted-trace-id?session=planted-session", auth);
    expect(line).toMatchObject({ route: "/v1/pipeline/traces/:traceId", status: 200, trace_id: "trace-1" });
    expect(JSON.stringify(line)).not.toContain("planted");
  });
  test("a request refused by auth counts against its route", async () => {
    expect(await lineFor("/v1/pipeline/chunks", { method: "POST" })).toMatchObject({ route: "/v1/pipeline/chunks", status: 401 });
  });
  test("a 5xx keeps its route", async () => {
    expect(await lineFor("/v1/pipeline/chunks", { method: "POST", ...auth })).toMatchObject({ route: "/v1/pipeline/chunks", status: 500 });
  });
  test("an unknown path cannot put its URL in a label", async () => {
    const line = await lineFor("/v1/planted-unknown/abc", auth);
    expect(line.status).toBe(404);
    expect(JSON.stringify(line)).not.toContain("planted");
  });
});
