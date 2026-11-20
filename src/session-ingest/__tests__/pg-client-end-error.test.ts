/** The "session ingest db error" line with class unknown / basis none / name TypeError that the
 * hosted ingest Worker writes on successful requests: the mechanism, reproduced with the REAL pg
 * Client and the REAL pg-cloudflare CloudflareSocket over an in-memory `cloudflare:sockets` fake
 * (no network, no Worker, no database).
 *
 * client.end() -> pg Connection.end() -> CloudflareSocket.end() -> Socket.close(). Closing the
 * socket rejects the read still pending in CloudflareSocket._listen(); pg-cloudflare forwards that
 * rejection as a stream 'error' (src/index.ts `this._listen().catch((e) => this.emit('error', e))`),
 * pg's Connection re-emits it (lib/connection.js reportStreamError drops only ECONNRESET/EPIPE while
 * ending, and this TypeError has no code) and the Client emits 'error' to the Worker's listener.
 * In workerd the rejection is `TypeError: This socket has been closed.` (seen in a local workerd at
 * the Worker's compatibility date with nodejs_compat); the fake below closes its readable with that
 * same error, which is the modelled part. Diagnostic only: nothing here changes a request's outcome.
 */
import { describe, expect, mock, test } from "bun:test";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import pg from "pg";
import { DB_CLIENT_PHASES, DB_ERROR_DETAILS, FRAME_NAME, dbClientErrorFields, errorFrame } from "../db-error-class.ts";
import { DB_CLIENT_PHASE_VALUES, DB_ERROR_DETAIL_VALUES, FRAME_NAME_SHAPE, logLine } from "../../shared/log.ts";

const CLOSED_TEXT = "This socket has been closed.";

/** Minimal Postgres backend: trust auth, simple-query SELECT, Terminate. */
function backend(push: (bytes: Uint8Array) => void) {
  let buf = Buffer.alloc(0);
  let started = false;
  const msg = (type: string, body: Buffer) => { const b = Buffer.alloc(5 + body.length); b.write(type, 0); b.writeInt32BE(4 + body.length, 1); body.copy(b, 5); return b; };
  const i32 = (n: number) => { const b = Buffer.alloc(4); b.writeInt32BE(n); return b; };
  const i16 = (n: number) => { const b = Buffer.alloc(2); b.writeInt16BE(n); return b; };
  const cstr = (s: string) => Buffer.concat([Buffer.from(s), Buffer.from([0])]);
  const row = Buffer.concat([
    msg("T", Buffer.concat([i16(1), cstr("n"), i32(0), i16(0), i32(23), i16(4), i32(-1), i16(0)])),
    msg("D", Buffer.concat([i16(1), i32(1), Buffer.from("1")])),
    msg("C", cstr("SELECT 1")), msg("Z", Buffer.from("I")),
  ]);
  const terminated = { value: false };
  const feed = (chunk: Uint8Array) => {
    buf = Buffer.concat([buf, Buffer.from(chunk)]);
    for (;;) {
      if (!started) {
        if (buf.length < 8 || buf.length < buf.readInt32BE(0)) return;
        buf = buf.subarray(buf.readInt32BE(0));
        started = true;
        push(Buffer.concat([msg("R", i32(0)), msg("K", Buffer.concat([i32(1), i32(2)])), msg("Z", Buffer.from("I"))]));
        continue;
      }
      if (buf.length < 5 || buf.length < 1 + buf.readInt32BE(1)) return;
      const type = String.fromCharCode(buf[0]!);
      buf = buf.subarray(1 + buf.readInt32BE(1));
      if (type === "Q") push(row);
      else if (type === "X") terminated.value = true;
    }
  };
  return { feed, terminated };
}

/** An in-memory stand-in for workerd's `cloudflare:sockets` Socket. close() ends the socket the way
 * workerd does for a pending read: the reader's read() rejects with a code-less TypeError. */
const sockets: { terminated: { value: boolean }; closeCalls: number }[] = [];
function connect() {
  let controller!: ReadableStreamDefaultController<Uint8Array>;
  const readable = new ReadableStream<Uint8Array>({ start(c) { controller = c; } });
  const server = backend((bytes) => controller.enqueue(new Uint8Array(bytes)));
  const writable = new WritableStream<Uint8Array>({ write(chunk) { server.feed(chunk); } });
  let resolveClosed!: () => void;
  const closed = new Promise<void>((r) => { resolveClosed = r; });
  const state = { terminated: server.terminated, closeCalls: 0 };
  sockets.push(state);
  return {
    readable, writable, closed,
    close() {
      state.closeCalls++;
      controller.error(new TypeError(CLOSED_TEXT));
      resolveClosed();
      return closed;
    },
    startTls() { throw new Error("not used"); },
  };
}
mock.module("cloudflare:sockets", () => ({ connect }));

/** pg-cloudflare's real CloudflareSocket. Its package default export is an empty stub outside
 * workerd, so load the built file next to its package.json, resolved through pg itself. */
function cloudflareSocketClass(): new (ssl: boolean) => unknown {
  const fromPg = createRequire(createRequire(import.meta.url).resolve("pg"));
  const dir = dirname(fromPg.resolve("pg-cloudflare/package.json"));
  return (fromPg(join(dir, "dist/index.js")) as { CloudflareSocket: new (ssl: boolean) => unknown }).CloudflareSocket;
}

/** worker.ts fetch(), reduced to its client lifecycle: one client, queries, then end() with the
 * phase flipped first. end() is awaited here; in the Worker it runs in waitUntil. */
async function request(): Promise<{ errors: unknown[]; lines: string[]; phases: string[] }> {
  const CloudflareSocket = cloudflareSocketClass();
  const client = new pg.Client({ host: "db.invalid", port: 5432, user: "u", database: "d", stream: () => new CloudflareSocket(false) } as never);
  const errors: unknown[] = [];
  const lines: string[] = [];
  const phases: string[] = [];
  let ending = false;
  client.on("error", (err: Error) => {
    errors.push(err);
    const fields = dbClientErrorFields(err, ending ? "ending" : "active");
    phases.push(fields.db_client_phase ?? "");
    lines.push(logLine("error", "session ingest db error", fields));
  });
  await client.connect();
  for (let i = 0; i < 3; i++) expect((await client.query("SELECT 1")).rows).toEqual([{ n: 1 }]);
  ending = true;
  await client.end();
  // Let the rejected read settle (it lands after end() resolves).
  for (let i = 0; i < 5; i++) await new Promise((r) => setTimeout(r, 0));
  return { errors, lines, phases };
}

describe("pg + pg-cloudflare: client.end() surfaces the closed socket's pending read as a Client 'error'", () => {
  test("one code-less TypeError per request, after end() began; the request's queries all succeeded", async () => {
    sockets.length = 0;
    const { errors, lines, phases } = await request();
    expect(sockets).toHaveLength(1);
    expect(sockets[0]!.terminated.value).toBe(true);
    expect(sockets[0]!.closeCalls).toBe(1);
    expect(errors).toHaveLength(1);
    const e = errors[0] as TypeError & { code?: unknown };
    expect(e).toBeInstanceOf(TypeError);
    expect(e.message).toBe(CLOSED_TEXT);
    expect(e.code).toBeUndefined();
    expect(phases).toEqual(["ending"]);
    // Before this change the line carried only these four fields, the exact production shape.
    const parsed = JSON.parse(lines[0]!) as Record<string, unknown>;
    expect(parsed).toMatchObject({ level: "error", message: "session ingest db error", error_code: "db_error", db_error_class: "unknown", db_error_basis: "none", error_name: "TypeError" });
    // Now: the closed message shape and the phase, never the text.
    expect(parsed).toMatchObject({ db_error_detail: "socket_closed", db_client_phase: "ending" });
    expect(lines[0]).not.toContain("socket has been closed");
    expect(lines[0]).not.toContain("db.invalid");
  });

  test("workerd's stack for this error yields the first frame's function name only", () => {
    // The stack shape a local workerd printed for this rejection (bundled file name and position
    // are illustrative); only the function name may leave.
    const e = new TypeError(CLOSED_TEXT);
    Object.defineProperty(e, "stack", { value: `TypeError: ${CLOSED_TEXT}\n    at async CloudflareSocket._listen (worker.js:3032:33)` });
    const fields = dbClientErrorFields(e, "ending");
    expect(fields).toEqual({ error_code: "db_error", db_error_class: "unknown", db_error_basis: "none", error_name: "TypeError", db_error_detail: "socket_closed", error_frame: "CloudflareSocket._listen", db_client_phase: "ending" });
    const line = logLine("error", "session ingest db error", fields);
    expect(JSON.parse(line)).toMatchObject({ error_frame: "CloudflareSocket._listen", db_client_phase: "ending", db_error_detail: "socket_closed" });
    for (const needle of ["worker.js", "3032", "socket has been closed"]) expect(line).not.toContain(needle);
  });
});

describe("the new closed fields: frames, shapes and mirrors", () => {
  const withStack = (name: string, message: string, stack: string) => {
    const e = Object.assign(new Error(message), { name });
    Object.defineProperty(e, "stack", { value: stack });
    return e;
  };
  const SECRET = "password=hunter2 postgres://alice:s3cr3t@db.internal:5432/sessions";

  test("a message cannot forge a frame: header equality strips it, a mismatched header is 'other'", () => {
    const forged = `oops\n    at hunter2 (x.js:1:1)`;
    // Header equals name + message: the forged line is part of the header and skipped.
    expect(errorFrame(withStack("Error", forged, `Error: ${forged}\n    at realFn (y.js:2:2)`))).toBe("realFn");
    // Header does not match (message changed after capture): nothing from the stack is trusted.
    expect(errorFrame(withStack("Error", "different", `Error: ${forged}\n    at realFn (y.js:2:2)`))).toBe("other");
    expect(errorFrame(withStack("Error", "m", `Error: mX\n    at realFn (y.js:2:2)`))).toBe("other");
  });

  test("frame forms: async/new/alias, anonymous, none, and anything outside the identifier shape", () => {
    const at = (frame: string) => errorFrame(withStack("TypeError", "m", `TypeError: m\n${frame}`));
    expect(at("    at async Promise.all (index 0)")).toBe("Promise.all");
    expect(at("    at new Client (client.js:1:1)")).toBe("Client");
    expect(at("    at Foo.bar [as baz] (f.js:1:1)")).toBe("Foo.bar");
    expect(at("    at worker.js:12:3")).toBe("anonymous");
    expect(at("    at Object.<anonymous> (f.js:1:1)")).toBe("other");
    expect(at(`    at ${"a".repeat(65)} (f.js:1:1)`)).toBe("other");
    expect(at("    at postgres://alice:s3cr3t@db (f.js:1:1)")).toBe("other");
    expect(at("not a frame")).toBe("other");
    expect(at(`    at ${"x [as ".repeat(2000)}`)).toBe("anonymous");
    expect(errorFrame(withStack("TypeError", "m", "TypeError: m"))).toBe("none");
    expect(errorFrame({})).toBe("none");
    expect(errorFrame(null)).toBe("none");
    const boom = () => { throw new Error(SECRET); };
    expect(errorFrame(Object.defineProperty(new Error("m"), "stack", { get: boom }))).toBe("none");
    expect(errorFrame(new Proxy({}, { get: boom }))).toBe("none");
  });

  test("message shapes are closed labels; secrets in the message never reach the line", () => {
    const detail = (m: string) => dbClientErrorFields(new TypeError(m)).db_error_detail;
    expect(detail(`This socket has been closed. ${SECRET}`)).toBe("socket_closed");
    expect(detail("This ReadableStream is currently locked to a reader.")).toBe("stream_state");
    expect(detail("Cannot read properties of undefined (reading 'x')")).toBe("read_property");
    expect(detail("this.stream.foo is not a function")).toBe("not_function");
    expect(detail(SECRET)).toBe("other");
    expect(detail("")).toBe("none");
    const line = logLine("error", "session ingest db error", dbClientErrorFields(new TypeError(`This socket has been closed. ${SECRET}`), "active"));
    for (const n of ["hunter2", "s3cr3t", "alice", "db.internal", "postgres://"]) expect(line).not.toContain(n);
    expect(JSON.parse(line)).toMatchObject({ db_error_detail: "socket_closed", db_client_phase: "active" });
  });

  test("phase is present only when given and closed; log.ts mirrors stay equal and drop anything else", () => {
    expect("db_client_phase" in dbClientErrorFields(new TypeError("x"))).toBe(false);
    expect("db_client_phase" in dbClientErrorFields(new TypeError("x"), "bogus" as never)).toBe(false);
    expect([...DB_ERROR_DETAIL_VALUES].sort()).toEqual([...DB_ERROR_DETAILS].sort());
    expect([...DB_CLIENT_PHASE_VALUES].sort()).toEqual([...DB_CLIENT_PHASES].sort());
    expect(FRAME_NAME_SHAPE.source).toBe(FRAME_NAME.source);
    const line = JSON.parse(logLine("error", "x", { db_error_detail: SECRET, db_client_phase: "later", error_frame: "a b" })) as Record<string, unknown>;
    expect(line).not.toHaveProperty("db_error_detail");
    expect(line).not.toHaveProperty("db_client_phase");
    expect(line).not.toHaveProperty("error_frame");
  });

  test("worker.ts fetch flips the phase before it calls client.end()", () => {
    // Source pin: the fetch handler's 'error' listener passes the phase, and `ending = true` precedes end().
    const src = readFileSync(join(import.meta.dir, "../worker.ts"), "utf8");
    expect(src).toContain(`client.on("error", (err: Error) => log.error("session ingest db error", dbClientErrorFields(err, ending ? "ending" : "active")));`);
    expect(src).toContain("if (connected) { ending = true; ctx.waitUntil(client.end().catch(() => {})); }");
  });
});
