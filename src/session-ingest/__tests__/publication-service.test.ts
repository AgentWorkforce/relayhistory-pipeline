/**
 * SERVICE=session-publication (publication-service.ts): default off, worker probe
 * shape, startup validation before the port opens, clean stop and failure exits.
 * Real loopback HTTP on port 0; publicationMain is replaced, so no AWS, DB or store.
 */
import { expect, test } from "bun:test";
import { publicationService, publicationLiveness, publicationReadiness, publicationHealthPort } from "../publication-service.ts";
import { publicationMain } from "../publication-main.ts";

// Synthetic deployment identity (AWS documentation account, placeholder Supabase ref and instance).
const ACCOUNT = "111122223333", REF = "abcdefghijklmnopqrst", INSTANCE = "i-0123456789abcdef0";
const ENABLED = {
  INGEST_PUBLISH_ENABLED: "true", AWS_REGION: "us-east-1", HOOPDB_DB_MODE: "supabase", INGEST_PUBLISH_BUCKET: "owned-bucket",
  INGEST_PUBLISH_EXPECT_GENERATION: "a".repeat(64), INGEST_PUBLISH_AWS_ACCOUNT_ID: ACCOUNT, INGEST_PUBLISH_SUPABASE_PROJECT_REF: REF,
  INGEST_PUBLISH_INSTANCE_ID: INSTANCE, INGEST_PUBLISH_SECRET_ARN: `arn:aws:secretsmanager:us-east-1:${ACCOUNT}:secret:96bulls/test/canonical-publisher-abcdef`,
  INGEST_PUBLISH_KMS_KEY_ARN: `arn:aws:kms:us-east-1:${ACCOUNT}:key/12345678-1234-1234-1234-123456789abc`,
  INGEST_PUBLISH_INSTANCE_PROFILE_ARN: `arn:aws:iam::${ACCOUNT}:instance-profile/owned-profile`, INGEST_PUBLISH_ROLE_NAME: "owned-role",
  INGEST_PUBLISH_BUDGET_MS: "10000",
};
const neverMain = async () => { throw new Error("main must not run"); };

function start(env: NodeJS.ProcessEnv, main: any = neverMain, now?: () => number) {
  const stop = new AbortController();
  let resolvePort!: (p: number) => void;
  const port = new Promise<number>((r) => (resolvePort = r));
  const done = publicationService(["--continuous"], env, { host: "127.0.0.1", port: 0, main, signal: stop.signal, onListening: resolvePort, ...(now ? { now } : {}) });
  return { stop, port, done };
}
async function get(port: number, path: string, method = "GET") {
  const r = await fetch(`http://127.0.0.1:${port}${path}`, { method });
  return { status: r.status, body: method === "GET" && r.headers.get("content-type") === "application/json" ? await r.json() : null };
}

test("dormant: no config or runtime work, both probes 200 enabled:false, stop resolves", async () => {
  for (const env of [{}, { INGEST_PUBLISH_ENABLED: "false" }]) {
    const s = start(env);
    const port = await s.port;
    expect(await get(port, "/healthz")).toEqual({ status: 200, body: { ok: true, enabled: false } });
    expect(await get(port, "/readyz")).toEqual({ status: 200, body: { ok: true, enabled: false } });
    expect((await get(port, "/")).status).toBe(404);
    expect((await get(port, "/healthz/x")).status).toBe(404);
    expect((await get(port, "/healthz", "POST")).status).toBe(405);
    s.stop.abort();
    expect(await s.done).toEqual({ enabled: false, cycles: 0 });
  }
});

test("startup refusals happen before the port opens: flag, port, arguments, enabled target", async () => {
  const refused = async (args: string[], env: NodeJS.ProcessEnv) => {
    let listened = false;
    const err = await publicationService(args, env, { host: "127.0.0.1", port: 0, main: neverMain, onListening: () => (listened = true) }).then(() => null, (e) => e);
    expect(listened).toBe(false);
    return err?.code ?? err?.message;
  };
  expect(await refused(["--continuous"], { INGEST_PUBLISH_ENABLED: "1" })).toBe("publication_enabled_invalid");
  expect(await refused(["--continuous"], { INGEST_PUBLISH_ENABLED: "TRUE" })).toBe("publication_enabled_invalid");
  expect(await refused(["--continuous"], { WORKER_HEALTH_PORT: "80a" })).toBe("publication_health_port_invalid");
  expect(await refused(["--continuous"], { WORKER_HEALTH_PORT: "70000" })).toBe("publication_health_port_invalid");
  for (const args of [[], ["--gc"], ["--continuous", "--gc"], ["--continuous", "--continuous"]]) expect(await refused(args, {})).toBe("publication_arguments");
  // Enabled off the AWS/Supabase target (a plain self-host env) fails startup, not later.
  expect(await refused(["--continuous"], { INGEST_PUBLISH_ENABLED: "true", HOOPDB_DB_MODE: "selfhosted" })).toBe("publication_target_required");
  const { INGEST_PUBLISH_BUCKET: _, ...noBucket } = ENABLED;
  expect(await refused(["--continuous"], noBucket)).toBe("publication_config_required");
});

test("health port defaults to the workers' 8081", () => {
  expect(publicationHealthPort({})).toBe(8081);
  expect(publicationHealthPort({ WORKER_HEALTH_PORT: "9100" })).toBe(9100);
});

test("enabled: ready after the first cycle, stalled past budget + wait + margin, stop is clean", async () => {
  let clock = 1_000_000;
  let hooks: any;
  let seenArgs: string[] = [];
  const main = (args: string[], _env: unknown, h: any) => {
    seenArgs = args;
    hooks = h;
    return new Promise((_r, reject) => h.signal.addEventListener("abort", () => reject(h.signal.reason), { once: true }));
  };
  const s = start(ENABLED, main, () => clock);
  const port = await s.port;
  expect(seenArgs).toEqual(["--continuous"]);
  expect(await get(port, "/healthz")).toEqual({ status: 200, body: { ok: true, enabled: true, reason: "starting" } });
  expect(await get(port, "/readyz")).toEqual({ status: 503, body: { ok: false, enabled: true, reason: "starting" } });
  hooks.onCycle({ examined: 0 });
  expect(await get(port, "/readyz")).toEqual({ status: 200, body: { ok: true, enabled: true } });
  expect(await get(port, "/healthz")).toEqual({ status: 200, body: { ok: true, enabled: true, reason: "ok" } });
  clock += 10_000 + 5_000 + 60_000; // exactly at the limit: still live
  expect((await get(port, "/healthz")).status).toBe(200);
  clock += 1;
  expect(await get(port, "/healthz")).toEqual({ status: 503, body: { ok: false, enabled: true, reason: "loop_stalled" } });
  hooks.onCycle({ examined: 1 });
  expect((await get(port, "/healthz")).status).toBe(200);
  s.stop.abort();
  expect(await s.done).toEqual({ enabled: true, cycles: 2 });
});

test("enabled: a failed cycle rejects (exit 1 path) and closes the probe port", async () => {
  let fail!: (e: unknown) => void;
  const main = () => new Promise((_r, reject) => (fail = reject));
  const s = start(ENABLED, main);
  const port = await s.port;
  fail(Object.assign(new Error("x"), { code: "publication_database_role" }));
  await expect(s.done).rejects.toMatchObject({ code: "publication_database_role" });
  await expect(fetch(`http://127.0.0.1:${port}/healthz`)).rejects.toThrow();
});

test("publicationMain honours an external stop before any runtime work", async () => {
  const stop = new AbortController();
  stop.abort();
  let cycles = 0;
  // An enabled env whose runtime would refuse the target: the abort is observed first, so nothing runs.
  await expect(publicationMain(["--continuous"], { INGEST_PUBLISH_ENABLED: "true" }, { signal: stop.signal, onCycle: () => cycles++ })).rejects.toMatchObject({ name: "AbortError" });
  expect(cycles).toBe(0);
  expect(await publicationMain(["--continuous"], {}, { signal: stop.signal })).toEqual({ enabled: false });
});

test("verdict helpers", () => {
  expect(publicationLiveness({ enabled: false, lastProgressAt: 0, cycles: 0 }, 1e12, 1)).toEqual({ ok: true, enabled: false });
  expect(publicationReadiness({ enabled: true, lastProgressAt: 0, cycles: 0 }).ok).toBe(false);
});

// INGEST_PUBLICATION_TARGET=portable (publication-portable.ts): a self-host env with no AWS/Supabase identity.
const PORTABLE = {
  INGEST_PUBLISH_ENABLED: "true", INGEST_PUBLICATION_TARGET: "portable", INGEST_PUBLISH_BUCKET: "selfhost-sessions",
  INGEST_PUBLISH_EXPECT_GENERATION: "a".repeat(64), INGEST_PUBLISH_S3_ENDPOINT: "http://minio.selfhost.svc:9000", ALLOW_IN_CLUSTER_HTTP: "true",
  INGEST_PUBLISH_S3_REGION: "us-east-1", INGEST_PUBLISH_S3_ACCESS_KEY_ID_FILE: "/run/secrets/session-publication/access-key-id",
  INGEST_PUBLISH_S3_SECRET_ACCESS_KEY_FILE: "/run/secrets/session-publication/secret-access-key",
  INGEST_PUBLISH_DATABASE_URL_FILE: "/run/secrets/session-publication/database-url", HOOPDB_DB_MODE: "selfhosted",
  SESSION_POSTGRES_TLS_PROFILE: "supabase", INGEST_PUBLISH_BUDGET_MS: "20000",
};

test("portable target passes the startup check, opens the port and uses its own budget for the stall limit", async () => {
  let clock = 5_000_000;
  let hooks: any;
  let target: string | undefined;
  const main = (_a: string[], env: NodeJS.ProcessEnv, h: any) => {
    target = env.INGEST_PUBLICATION_TARGET;
    hooks = h;
    return new Promise((_r, reject) => h.signal.addEventListener("abort", () => reject(h.signal.reason), { once: true }));
  };
  const s = start(PORTABLE, main, () => clock);
  const port = await s.port;
  expect(target).toBe("portable");
  expect(await get(port, "/healthz")).toEqual({ status: 200, body: { ok: true, enabled: true, reason: "starting" } });
  hooks.onCycle({ examined: 0 });
  expect(await get(port, "/readyz")).toEqual({ status: 200, body: { ok: true, enabled: true } });
  clock += 20_000 + 5_000 + 60_000; // the portable budget, not the hosted default
  expect((await get(port, "/healthz")).status).toBe(200);
  clock += 1;
  expect(await get(port, "/healthz")).toEqual({ status: 503, body: { ok: false, enabled: true, reason: "loop_stalled" } });
  s.stop.abort();
  expect(await s.done).toEqual({ enabled: true, cycles: 1 });
});

test("portable target startup refusals happen before the port opens", async () => {
  const refused = async (env: NodeJS.ProcessEnv) => {
    let listened = false;
    const err = await publicationService(["--continuous"], env, { host: "127.0.0.1", port: 0, main: neverMain, onListening: () => (listened = true) }).then(() => null, (e) => e);
    expect(listened).toBe(false);
    return err?.code ?? err?.message;
  };
  const { INGEST_PUBLISH_S3_ENDPOINT: _, ...noEndpoint } = PORTABLE;
  expect(await refused(noEndpoint)).toBe("publication_config_required");
  expect(await refused({ ...PORTABLE, INGEST_PUBLISH_SECRET_ARN: ENABLED.INGEST_PUBLISH_SECRET_ARN })).toBe("publication_target_ambiguous");
  expect(await refused({ ...PORTABLE, INGEST_PUBLICATION_TARGET: "minio" })).toBe("publication_target_invalid");
  // An explicit "aws" target is still the unchanged hosted check.
  const { INGEST_PUBLISH_BUCKET: __, ...hostedNoBucket } = ENABLED;
  expect(await refused({ ...hostedNoBucket, INGEST_PUBLICATION_TARGET: "aws" })).toBe("publication_config_required");
});
