/**
 * SERVICE=selfhost-control (src/selfhost-control/server.ts): the single-user
 * introspection stand-in. The server runs on 127.0.0.1:0 with an in-memory
 * config; the configuration loader is driven with a plain env object and temp
 * tokens files. The real relaycontrol introspector (the read API's client) is
 * run against it once, so the reply shape is pinned to what the client accepts.
 */
import { afterAll, beforeAll, expect, test } from "bun:test";
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { connect } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createRelaycontrolIntrospector } from "@rhc/auth/relaycontrol.ts";
import {
  INTROSPECT_PATH,
  loadSelfhostControlConfig,
  SelfhostControlConfigError,
  startSelfhostControl,
  type SelfhostControlServer,
  type SelfhostControlUser,
} from "../server.ts";

const ROOT = fileURLToPath(new URL("../../..", import.meta.url));
const ENTRY = join(ROOT, "src/selfhost-control/server.ts");
// Synthetic test values only.
const SECRET = "test-introspect-secret-0123456789abcdef";
const TOKEN = "sh_at_test-token-0123456789abcdefghijklmnop";
const TOKEN_2 = "sh_at_test-token-second-0123456789abcdefghij";
const USER: SelfhostControlUser = {
  sub: "user-local-1",
  org: "org-local",
  workspace: "ws-local",
  tokens: [
    { token: TOKEN, token_id: "tok-local-1", scopes: ["rth:read", "rth:sync"] },
    { token: TOKEN_2, token_id: "tok-local-2", scopes: ["rth:read"] },
  ],
};
const NOW_MS = Date.UTC(2026, 10, 17, 12, 0, 0);

let server: SelfhostControlServer;
let base = "";
let dir = "";
const logged: [string, number][] = [];

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), "selfhost-control-"));
  server = await startSelfhostControl({
    secret: SECRET,
    user: USER,
    host: "127.0.0.1",
    port: 0,
    now: () => NOW_MS,
    logRequest: (route, status) => logged.push([route, status]),
  });
  base = `http://127.0.0.1:${server.port}`;
});
afterAll(async () => {
  await server?.close();
  rmSync(dir, { recursive: true, force: true });
});

const introspect = (body: unknown, auth: string | null = `Bearer ${SECRET}`) =>
  fetch(`${base}${INTROSPECT_PATH}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...(auth === null ? {} : { authorization: auth }) },
    body: typeof body === "string" ? body : JSON.stringify(body),
  });

test("an active token gets every contract field, exactly", async () => {
  const res = await introspect({ token: TOKEN, aud: "https://history.local.test" });
  expect(res.status).toBe(200);
  expect(res.headers.get("content-type")).toBe("application/json");
  const nowS = Math.floor(NOW_MS / 1000);
  expect(await res.json()).toEqual({
    active: true,
    sub: "user-local-1",
    email: null,
    principal_type: "desktop-user",
    machine_id: null,
    org: "org-local",
    workspace: "ws-local",
    scopes: ["rth:read", "rth:sync"],
    aud: "https://history.local.test",
    iat: nowS - 10,
    exp: nowS + 3600,
    token_id: "tok-local-1",
  });
  const second = await (await introspect({ token: TOKEN_2, aud: "https://history.local.test", workspace: "ws-local" })).json();
  expect(second.active).toBe(true);
  expect(second.token_id).toBe("tok-local-2");
  expect(second.scopes).toEqual(["rth:read"]);
});

test("the real relaycontrol introspector accepts the reply", async () => {
  const verifier = createRelaycontrolIntrospector({ url: base, secret: SECRET, now: () => NOW_MS, positiveTtlMs: 0, log: () => {} });
  const auth = await verifier.verifyRelaycontrolToken(TOKEN, { aud: "https://history.local.test", workspace: "ws-local" });
  expect(auth).toMatchObject({ userId: "user-local-1", orgId: "org-local", workspaceId: "ws-local", scopes: ["rth:read", "rth:sync"] });
  await expect(verifier.verifyRelaycontrolToken(`${TOKEN}x`, { aud: "https://history.local.test" })).rejects.toThrow("token is not valid");
});

test("an unknown token is 200 active:false; a foreign workspace is inactive too", async () => {
  const unknown = await introspect({ token: `${TOKEN}-other`, aud: "https://history.local.test" });
  expect(unknown.status).toBe(200);
  expect(await unknown.json()).toEqual({ active: false, reason: "unknown" });
  const foreign = await introspect({ token: TOKEN, aud: "https://history.local.test", workspace: "ws-other" });
  expect(foreign.status).toBe(200);
  expect(await foreign.json()).toEqual({ active: false, reason: "workspace_mismatch" });
});

test("a 4096-character unknown sh_at_ token (the client's maximum) is 200 unknown, not 400", async () => {
  // Latin-1 letters are two UTF-8 bytes each: the largest body the client can send (about 8.2 KB).
  for (const fill of ["a", "\u00e9"]) {
    const token = `sh_at_${fill.repeat(4090)}`;
    expect(token.length).toBe(4096);
    const res = await introspect({ token, aud: "https://history.local.test", workspace: "ws-local" });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ active: false, reason: "unknown" });
  }
});

test("a machine string is accepted and never verified: machine_id stays null", async () => {
  const res = await introspect({ token: TOKEN, aud: "https://history.local.test", workspace: "ws-local", machine: "machine-local-1" });
  expect(res.status).toBe(200);
  const body = await res.json();
  expect(body.active).toBe(true);
  expect(body.machine_id).toBeNull();
});

test("a missing or over-long aud is 400", async () => {
  for (const body of [{ token: TOKEN }, { token: TOKEN, aud: "" }, { token: TOKEN, aud: `https://${"a".repeat(250)}.test` }]) {
    const res = await introspect(body);
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ error: "bad_request" });
  }
});

test("a wrong or missing introspection bearer is 401 with an empty object", async () => {
  for (const auth of [`Bearer ${SECRET}x`, `Bearer ${SECRET.slice(0, -1)}`, "Bearer ", `Basic ${SECRET}`, `bearer  ${SECRET}`, null]) {
    const res = await introspect({ token: TOKEN, aud: "https://history.local.test" }, auth);
    expect(res.status).toBe(401);
    const text = await res.text();
    expect(text).toBe("{}");
  }
});

test("an oversize, non-JSON or wrong-shape body is 400 and never echoed", async () => {
  const big = await introspect({ token: TOKEN, aud: "https://history.local.test", pad: "x".repeat(17 * 1024) });
  expect(big.status).toBe(400);
  expect(await big.text()).not.toContain(TOKEN);
  for (const body of ["not json", "[]", "null", JSON.stringify({ aud: "https://history.local.test" }), JSON.stringify({ token: TOKEN }), JSON.stringify({ token: TOKEN, aud: "https://h.test", workspace: 1 })]) {
    const res = await introspect(body);
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ error: "bad_request" });
  }
});

test("an oversize chunked body with no declared length is 400", async () => {
  const reply = await new Promise<string>((resolve, reject) => {
    const socket = connect(server.port, "127.0.0.1");
    let out = "";
    socket.on("data", (d) => (out += d.toString("utf8")));
    socket.on("close", () => resolve(out));
    socket.on("error", (e) => (out ? resolve(out) : reject(e)));
    socket.write(
      `POST ${INTROSPECT_PATH} HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer ${SECRET}\r\nTransfer-Encoding: chunked\r\n\r\n`,
    );
    const chunk = "y".repeat(1024);
    for (let i = 0; i < 20; i++) socket.write(`${chunk.length.toString(16)}\r\n${chunk}\r\n`);
  });
  expect(reply.startsWith("HTTP/1.1 400")).toBe(true);
});

test("GET /healthz is 200 {ok:true}, HEAD 200 with no body, POST 405; other paths 404, other methods 405", async () => {
  const health = await fetch(`${base}/healthz`);
  expect(health.status).toBe(200);
  expect(await health.json()).toEqual({ ok: true });
  const head = await fetch(`${base}/healthz`, { method: "HEAD" });
  expect(head.status).toBe(200);
  expect(await head.text()).toBe("");
  const post = await fetch(`${base}/healthz`, { method: "POST" });
  expect(post.status).toBe(405);
  expect(post.headers.get("allow")).toBe("GET, HEAD");
  expect((await fetch(`${base}/internal/v1/auth/other`, { method: "POST" })).status).toBe(404);
  expect((await fetch(`${base}/`)).status).toBe(404);
  const get = await fetch(`${base}${INTROSPECT_PATH}`, { headers: { authorization: `Bearer ${SECRET}` } });
  expect(get.status).toBe(405);
  expect(get.headers.get("allow")).toBe("POST");
});

test("logs carry only method, a fixed path label and the status", () => {
  expect(logged.length).toBeGreaterThan(0);
  for (const [route, status] of logged) {
    expect(route).toMatch(/^[A-Z]+ (\/healthz|\/internal\/v1\/auth\/introspect|other)$/);
    expect(typeof status).toBe("number");
  }
  expect(logged).toContainEqual(["GET other", 404]);
});

// ---- configuration refusals ----

const goodFile = (name: string, user: unknown = USER, mode = 0o600) => {
  const path = join(dir, name);
  writeFileSync(path, JSON.stringify(user));
  chmodSync(path, mode);
  return path;
};
const env = (over: Record<string, string | undefined> = {}) => ({
  RELAYHISTORY_SELFHOST_CONTROL: "true",
  SELFHOST_CONTROL_INTROSPECT_SECRET: SECRET,
  SELFHOST_CONTROL_TOKENS_FILE: goodFile("tokens.json"),
  ...over,
});
const refusal = (e: NodeJS.ProcessEnv) => {
  try {
    loadSelfhostControlConfig(e);
  } catch (error) {
    expect(error).toBeInstanceOf(SelfhostControlConfigError);
    const message = (error as Error).message;
    expect(message).not.toContain(SECRET.slice(0, 20));
    expect(message).not.toContain("test-token");
    return message;
  }
  throw new Error("expected a refusal");
};

test("a valid env loads, with the default listen address", () => {
  const config = loadSelfhostControlConfig(env());
  expect(config).toMatchObject({ secret: SECRET, host: "0.0.0.0", port: 8090, user: USER });
  expect(loadSelfhostControlConfig(env({ SELFHOST_CONTROL_LISTEN: "127.0.0.1:9100" }))).toMatchObject({ host: "127.0.0.1", port: 9100 });
  expect(loadSelfhostControlConfig(env({ SELFHOST_CONTROL_LISTEN: "[::1]:9100" }))).toMatchObject({ host: "::1", port: 9100 });
});

test("the flag must be exactly true", () => {
  for (const flag of [undefined, "", "1", "TRUE", "true ", "yes"])
    expect(refusal(env({ RELAYHISTORY_SELFHOST_CONTROL: flag }))).toContain("RELAYHISTORY_SELFHOST_CONTROL");
});

test("the entry refuses to start with the flag unset: exit 2, one stderr line, no value", () => {
  const r = Bun.spawnSync([process.execPath, "run", ENTRY], {
    cwd: ROOT,
    env: { PATH: process.env.PATH ?? "/usr/bin:/bin", SELFHOST_CONTROL_INTROSPECT_SECRET: SECRET },
    stdout: "pipe",
    stderr: "pipe",
  });
  expect(r.exitCode).toBe(2);
  const err = r.stderr.toString();
  expect(err).toContain("RELAYHISTORY_SELFHOST_CONTROL");
  expect(err).not.toContain(SECRET);
  expect(r.stdout.toString()).toBe("");
});

test("a tokens file readable by group or others is refused", () => {
  for (const mode of [0o644, 0o640, 0o604, 0o660])
    expect(refusal(env({ SELFHOST_CONTROL_TOKENS_FILE: goodFile(`open-${mode.toString(8)}.json`, USER, mode) }))).toContain(
      "SELFHOST_CONTROL_TOKENS_FILE must not be accessible by group or others",
    );
});

test("a token shorter than 32 characters, without sh_at_, or repeated is refused", () => {
  const withTokens = (tokens: unknown[]) => ({ ...USER, tokens });
  const cases: [unknown[], string][] = [
    [[{ token: "sh_at_short", token_id: "a", scopes: [] }], "tokens[0].token must be 32"],
    [[{ token: "x".repeat(40), token_id: "a", scopes: [] }], "must start with sh_at_"],
    [[{ token: TOKEN, token_id: "a", scopes: [] }, { token: TOKEN, token_id: "b", scopes: [] }], "repeats an earlier token"],
    [[{ token: TOKEN, token_id: "a", scopes: [] }, { token: TOKEN_2, token_id: "a", scopes: [] }], "repeats an earlier token_id"],
    [[{ token: TOKEN, token_id: "", scopes: [] }], "token_id must be"],
    [[{ token: TOKEN, token_id: "a", scopes: "rth:read" }], "scopes must be"],
    [[], "tokens must be a non-empty array"],
  ];
  cases.forEach(([tokens, expected], i) =>
    expect(refusal(env({ SELFHOST_CONTROL_TOKENS_FILE: goodFile(`bad-${i}.json`, withTokens(tokens)) }))).toContain(expected),
  );
  expect(refusal(env({ SELFHOST_CONTROL_TOKENS_FILE: goodFile("extra.json", { ...USER, users: [] }) }))).toContain("unknown top-level field");
  expect(refusal(env({ SELFHOST_CONTROL_TOKENS_FILE: goodFile("ws.json", { ...USER, workspace: "bad ws" }) }))).toContain("workspace must be");
});

test("a secret shorter than 32 characters, or missing, is refused", () => {
  for (const secret of [undefined, "", "short-secret", " ".repeat(40), SECRET.slice(0, 31)])
    expect(refusal(env({ SELFHOST_CONTROL_INTROSPECT_SECRET: secret }))).toContain(
      "SELFHOST_CONTROL_INTROSPECT_SECRET is required, at least 32 characters",
    );
  for (const secret of [`${SECRET} with spaces`, `${SECRET}\u00e9`])
    expect(refusal(env({ SELFHOST_CONTROL_INTROSPECT_SECRET: secret }))).toContain(
      "SELFHOST_CONTROL_INTROSPECT_SECRET must be printable ASCII with no spaces",
    );
});

test("a missing, relative, unreadable, non-JSON or over-64-KiB tokens file and a bad listen address are refused", () => {
  expect(refusal(env({ SELFHOST_CONTROL_TOKENS_FILE: undefined }))).toContain("must be an absolute path");
  expect(refusal(env({ SELFHOST_CONTROL_TOKENS_FILE: "tokens.json" }))).toContain("must be an absolute path");
  expect(refusal(env({ SELFHOST_CONTROL_TOKENS_FILE: join(dir, "absent.json") }))).toContain("cannot be read");
  expect(refusal(env({ SELFHOST_CONTROL_TOKENS_FILE: dir }))).toContain("must be a regular file");
  const notJson = join(dir, "not-json.json");
  writeFileSync(notJson, "{");
  chmodSync(notJson, 0o600);
  expect(refusal(env({ SELFHOST_CONTROL_TOKENS_FILE: notJson }))).toContain("cannot be read as JSON");
  // Valid JSON padded past 64 KiB: refused on size before it is read.
  const big = join(dir, "big.json");
  writeFileSync(big, `${JSON.stringify(USER)}${" ".repeat(64 * 1024)}`);
  chmodSync(big, 0o600);
  expect(refusal(env({ SELFHOST_CONTROL_TOKENS_FILE: big }))).toContain("SELFHOST_CONTROL_TOKENS_FILE is larger than 65536 bytes");
  for (const listen of ["8090", "0.0.0.0:0", "0.0.0.0:70000", "host:port", "a b:80"])
    expect(refusal(env({ SELFHOST_CONTROL_LISTEN: listen }))).toContain("SELFHOST_CONTROL_LISTEN must be host:port");
});
