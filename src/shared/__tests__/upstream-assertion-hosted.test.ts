/**
 * The relaygate v1 upstream assertion is checked by the self-host read API service and the self-host ingest edge
 * only. The hosted entries (read Lambda, read Worker, ingest Worker) refuse any READ_API_ / INGEST_
 * UPSTREAM_ASSERTION_MODE but off, naming the variable and never echoing the value, and they never read the key
 * variables. Off (unset, "" or "off") is unchanged. No network or database: fetch, pg.Pool and connect are stubs.
 */
import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import pg from "pg";
import { createReadApiLambdaHandler, type FunctionUrlEvent, READER_LOGIN_SQL, readApiDepsFromSecret } from "../../session-read-api/lambda.ts";
import { READER_LOGIN_ROW, readerLoginAnswer } from "../../session-read-api/__tests__/reader-login-support.ts";
import { handle, type Env } from "../../../packages/session-read-worker/src/index.ts";
import { ingestSettings } from "../../session-ingest/worker.ts";
import { assertUpstreamAssertionOff, INGEST_UPSTREAM_ASSERTION_PREFIX, READ_API_UPSTREAM_ASSERTION_PREFIX } from "../upstream-assertion.ts";

const READ_MODE = `${READ_API_UPSTREAM_ASSERTION_PREFIX}MODE`;
const INGEST_MODE = `${INGEST_UPSTREAM_ASSERTION_PREFIX}MODE`;
const SECRET = "s3cret-value";
const ON = ["log", "enforce"];
const OFF = [undefined, "", "off"];
/** Key variables set to garbage: a hosted entry must refuse on the mode alone and never parse these. */
const garbageKeys = (prefix: string) => ({ [`${prefix}PUBLIC_KEY`]: SECRET, [`${prefix}KID`]: "!bad kid!", [`${prefix}PUBLIC_KEY_PREVIOUS`]: SECRET });
const modeEnv = (name: string, value: string | undefined) => (value === undefined ? {} : { [name]: value });

describe("assertUpstreamAssertionOff", () => {
  test("off passes; log, enforce and a bad value refuse naming only the variable", () => {
    for (const value of OFF) expect(() => assertUpstreamAssertionOff(modeEnv(READ_MODE, value), READ_API_UPSTREAM_ASSERTION_PREFIX)).not.toThrow();
    for (const value of [...ON, SECRET]) {
      let message = "";
      try { assertUpstreamAssertionOff({ ...modeEnv(INGEST_MODE, value), ...garbageKeys(INGEST_UPSTREAM_ASSERTION_PREFIX) }, INGEST_UPSTREAM_ASSERTION_PREFIX); } catch (error) { message = (error as Error).message; }
      expect(message).toContain(INGEST_MODE);
      expect(message).not.toContain(SECRET);
    }
  });
});

describe("the hosted read Lambda", () => {
  const RealPool = pg.Pool;
  let pools = 0;
  const answer = async (text: string) => ({ rows: text === READER_LOGIN_SQL ? [{ ...READER_LOGIN_ROW }] : readerLoginAnswer(text) ?? [] });
  class RecordingPool {
    constructor() { pools++; }
    on() { return this; }
    async query(text: string) { return answer(text); }
    async connect() { return { query: async (text: string) => answer(text), release() {} }; }
    async end() {}
  }
  beforeEach(() => { pools = 0; pg.Pool = RecordingPool as unknown as typeof pg.Pool; });
  afterEach(() => { pg.Pool = RealPool; });
  const secret = JSON.stringify({ supabase_url: "postgresql://reader:pw@db.example.internal:5432/history", clickhouse: { url: "https://ch.example.internal:8443", user: "u", password: "p", database: "relay_search" } });
  const base = { READ_API_COMPLETE_SINCE: "epoch", ...garbageKeys(READ_API_UPSTREAM_ASSERTION_PREFIX) } as NodeJS.ProcessEnv;

  test("log, enforce or a bad mode refuses before any pool is built, without the value", async () => {
    for (const value of [...ON, SECRET]) {
      const error = await readApiDepsFromSecret(secret, { ...base, ...modeEnv(READ_MODE, value) }).then(() => null, (e: Error) => e);
      expect(error?.message).toContain(READ_MODE);
      expect(error?.message).not.toContain(SECRET);
      if (value !== SECRET) expect(error?.message).toBe(`96bulls: ${READ_MODE} is served only by the self-host service`);
    }
    expect(pools).toBe(0);
  });

  test("off (unset, empty, off) builds as before, ignoring the key variables", async () => {
    for (const value of OFF) {
      const built = await readApiDepsFromSecret(secret, { ...base, ...modeEnv(READ_MODE, value) });
      await built.close();
    }
    expect(pools).toBe(OFF.length);
  });
});

describe("the hosted read Lambda handler refuses before the secret lookup", () => {
  const VARS: Record<string, string> = {
    AWS_REGION: "us-east-1", SESSION_READ_API_SECRET_ARN: "arn:aws:secretsmanager:us-east-1:000000000000:secret:synthetic",
    AWS_ACCESS_KEY_ID: "synthetic-access-key", AWS_SECRET_ACCESS_KEY: "synthetic-secret-key", READ_API_COMPLETE_SINCE: "epoch",
  };
  const touched = [...Object.keys(VARS), "AWS_SESSION_TOKEN", READ_MODE];
  let saved: Record<string, string | undefined> = {};
  beforeEach(() => {
    saved = Object.fromEntries(touched.map((k) => [k, process.env[k]]));
    for (const k of touched) delete process.env[k];
    Object.assign(process.env, VARS);
  });
  afterEach(() => {
    for (const k of touched) {
      if (saved[k] === undefined) delete process.env[k];
      else process.env[k] = saved[k];
    }
    (globalThis.fetch as { mockRestore?: () => void }).mockRestore?.();
  });

  test("enforce: 503 with no secret lookup; unset: the lookup is attempted", async () => {
    const urls: string[] = [];
    spyOn(globalThis, "fetch").mockImplementation((async (input: Parameters<typeof fetch>[0]) => {
      urls.push(String(input instanceof Request ? input.url : input));
      throw new Error("no network in this test");
    }) as typeof fetch);
    const event = {} as FunctionUrlEvent;
    process.env[READ_MODE] = "enforce";
    expect((await createReadApiLambdaHandler()(event)).statusCode).toBe(503);
    expect(urls).toEqual([]);
    delete process.env[READ_MODE];
    expect((await createReadApiLambdaHandler()(event)).statusCode).toBe(503);
    expect(urls).toHaveLength(1);
  });
});

describe("the hosted read Worker", () => {
  afterEach(() => { (console.error as { mockRestore?: () => void }).mockRestore?.(); });
  const base = {
    CLICKHOUSE_URL: "https://ch.test.example", CLICKHOUSE_USER: "u", CLICKHOUSE_PASSWORD: "p", CLICKHOUSE_DATABASE: "d",
    READ_API_ORIGIN: "https://read.test.example", READ_API_COMPLETE_SINCE: "epoch",
    RELAYAUTH_JWKS_URL: "https://auth.test.example/jwks", RELAYAUTH_ISSUER: "https://relayauth.dev",
    ...garbageKeys(READ_API_UPSTREAM_ASSERTION_PREFIX),
  };
  const ctx = { waitUntil: () => {}, passThroughOnException: () => {} } as never;
  const overrides = { connect: async () => { throw new Error("no database in this test"); }, fetcher: (async () => new Response("{}", { status: 500 })) as never };
  const status = async (value?: string) =>
    (await handle(new Request("https://read.test.example/v1/sessions/s-1/events?workspace=ws"), { ...base, ...modeEnv(READ_MODE, value) } as unknown as Env, ctx, overrides as never)).status;

  test("log, enforce or a bad mode: every request 503, logged as missing_READ_API_UPSTREAM_ASSERTION_MODE, value never logged", async () => {
    const errors = spyOn(console, "error").mockImplementation(() => {});
    for (const value of [...ON, SECRET]) {
      errors.mockClear();
      expect(await status(value)).toBe(503);
      const lines = errors.mock.calls.map((c) => c.map(String).join(" "));
      expect(lines.some((l) => l.includes(`missing_${READ_MODE}`))).toBe(true);
      expect(lines.some((l) => l.includes(SECRET))).toBe(false);
    }
  });

  test("off (unset, empty, off): served as before (401 without a token)", async () => {
    for (const value of OFF) expect(await status(value)).toBe(401);
  });
});

describe("the hosted ingest Worker", () => {
  const full = {
    HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s",
    INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn", ...garbageKeys(INGEST_UPSTREAM_ASSERTION_PREFIX),
  };

  test("log, enforce or a bad mode refuses, naming the variable and never the value", async () => {
    for (const value of [...ON, SECRET]) {
      const error = await ingestSettings({ ...full, ...modeEnv(INGEST_MODE, value) }).then(() => null, (e: Error) => e);
      expect(error?.message).toBe(`session ingest: ${INGEST_MODE} is served only by the self-host ingest edge`);
    }
    // Refused even when the Worker is otherwise unconfigured (before the not-configured null).
    const bare = await ingestSettings({ ...modeEnv(INGEST_MODE, "enforce") }).then(() => null, (e: Error) => e);
    expect(bare?.message).toContain(INGEST_MODE);
  });

  test("off (unset, empty, off) builds settings as before", async () => {
    for (const value of OFF) expect(await ingestSettings({ ...full, ...modeEnv(INGEST_MODE, value) })).not.toBeNull();
  });
});
