/** INGEST_PUBLICATION_TARGET selection and the portable target's config/secret refusals.
 * Offline: no AWS, database or store; factories and the secret reader are injected. */
import { test, expect } from "bun:test";
import { mkdtempSync, writeFileSync, mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { publicationTargetKind } from "../publication-target.ts";
import { portablePublicationConfig, portablePublicationSecrets, portablePoolConfig, publicationConfigForTarget, runPortablePublicationRuntime, HOSTED_ONLY_VARS } from "../publication-portable.ts";
import { publicationRuntimeConfig } from "../publication-runtime.ts";
import { publicationMain } from "../publication-main.ts";
import { INGEST_LOGIN_IDENTITY_SQL, INGEST_LOGIN_PG16_SQL } from "../ingest-login.ts";

const codeOf = (f: () => unknown) => { try { f(); } catch (e) { return (e as { code?: string }).code ?? (e as Error).message; } return "no_throw"; };
const asyncCodeOf = (p: Promise<unknown>) => p.then(() => "resolved", (e: { code?: string; message?: string }) => e.code ?? e.message);
const GOOD = {
  INGEST_PUBLICATION_TARGET: "portable", INGEST_PUBLISH_BUCKET: "selfhost-sessions", INGEST_PUBLISH_EXPECT_GENERATION: "a".repeat(64),
  INGEST_PUBLISH_S3_ENDPOINT: "https://minio.example.test", INGEST_PUBLISH_S3_REGION: "us-east-1",
  INGEST_PUBLISH_S3_ACCESS_KEY_ID: "publisher-fixture", INGEST_PUBLISH_S3_SECRET_ACCESS_KEY: "fixture-secret-only",
  INGEST_PUBLISH_DATABASE_URL: "postgresql://bulls96_session_ingest:fixture-only@pg.example.test:5432/sessions",
  HOOPDB_DB_MODE: "selfhosted", SESSION_POSTGRES_TLS_PROFILE: "supabase",
} as Record<string, string>;
const without = (k: string) => { const { [k]: _, ...rest } = GOOD; return rest; };
const AWS_GOOD = {
  AWS_REGION: "us-east-1", HOOPDB_DB_MODE: "supabase", INGEST_PUBLISH_BUCKET: "owned-bucket", INGEST_PUBLISH_EXPECT_GENERATION: "a".repeat(64),
  INGEST_PUBLISH_AWS_ACCOUNT_ID: "111122223333", INGEST_PUBLISH_SUPABASE_PROJECT_REF: "abcdefghijklmnopqrst", INGEST_PUBLISH_INSTANCE_ID: "i-0123456789abcdef0",
  INGEST_PUBLISH_SECRET_ARN: "arn:aws:secretsmanager:us-east-1:111122223333:secret:96bulls/test/canonical-publisher-abcdef",
  INGEST_PUBLISH_KMS_KEY_ARN: "arn:aws:kms:us-east-1:111122223333:key/12345678-1234-1234-1234-123456789abc",
  INGEST_PUBLISH_INSTANCE_PROFILE_ARN: "arn:aws:iam::111122223333:instance-profile/owned-profile", INGEST_PUBLISH_ROLE_NAME: "owned-role",
};

test("target selector: unset or exactly aws is hosted, exactly portable is portable, anything else refuses", () => {
  expect(publicationTargetKind({})).toBe("aws");
  expect(publicationTargetKind({ INGEST_PUBLICATION_TARGET: "aws" })).toBe("aws");
  expect(publicationTargetKind({ INGEST_PUBLICATION_TARGET: "portable" })).toBe("portable");
  for (const v of ["", "AWS", "Portable", " portable", "minio", "selfhost"]) expect(codeOf(() => publicationTargetKind({ INGEST_PUBLICATION_TARGET: v }))).toBe("publication_target_invalid");
});

test("hosted config is unchanged and selected by default; portable never satisfies it", () => {
  expect(publicationConfigForTarget(AWS_GOOD)).toEqual(publicationRuntimeConfig(AWS_GOOD));
  expect(publicationConfigForTarget({ ...AWS_GOOD, INGEST_PUBLICATION_TARGET: "aws" })).toEqual(publicationRuntimeConfig(AWS_GOOD));
  // The hosted runtime still refuses a self-host env; the portable one refuses a hosted env.
  expect(codeOf(() => publicationRuntimeConfig(GOOD))).toBe("publication_target_required");
  expect(codeOf(() => publicationConfigForTarget({ ...AWS_GOOD, INGEST_PUBLICATION_TARGET: "portable" }))).toBe("publication_target_ambiguous");
  expect(publicationConfigForTarget(GOOD)).toMatchObject({ kind: "portable", budgetMs: 1200000, candidateLimit: 1, gcLimit: 20 });
});

test("publicationMain dispatches on the target only when enabled", async () => {
  expect(await publicationMain([], {})).toEqual({ enabled: false });
  expect(await publicationMain([], { INGEST_PUBLICATION_TARGET: "bogus" })).toEqual({ enabled: false });
  expect(await asyncCodeOf(publicationMain([], { INGEST_PUBLISH_ENABLED: "true" }))).toBe("publication_target_required");
  expect(await asyncCodeOf(publicationMain([], { INGEST_PUBLISH_ENABLED: "true", INGEST_PUBLICATION_TARGET: "bogus" }))).toBe("publication_target_invalid");
  expect(await asyncCodeOf(publicationMain([], { INGEST_PUBLISH_ENABLED: "true", INGEST_PUBLICATION_TARGET: "portable" }))).toBe("publication_config_required");
  expect(await asyncCodeOf(publicationMain(["--gc"], { INGEST_PUBLICATION_GC_ENABLED: "true", INGEST_PUBLICATION_TARGET: "portable", HOOPDB_DB_MODE: "selfhosted" }))).toBe("publication_config_required");
});

test("portable config: every required value refuses when missing or empty", () => {
  const c = portablePublicationConfig(GOOD);
  expect(c).toMatchObject({ kind: "portable", bucket: "selfhost-sessions", region: "us-east-1", endpoint: "https://minio.example.test", keyRoot: "96bulls", dbMode: "selfhosted", sse: undefined });
  expect(c.sources).toEqual({ accessKeyId: { env: "INGEST_PUBLISH_S3_ACCESS_KEY_ID" }, secretAccessKey: { env: "INGEST_PUBLISH_S3_SECRET_ACCESS_KEY" }, databaseUrl: { env: "INGEST_PUBLISH_DATABASE_URL" } });
  // The returned config carries no secret value.
  expect(JSON.stringify(c)).not.toContain("fixture-secret-only");
  expect(JSON.stringify(c)).not.toContain("fixture-only@");
  for (const k of ["INGEST_PUBLISH_BUCKET", "INGEST_PUBLISH_EXPECT_GENERATION", "INGEST_PUBLISH_S3_ENDPOINT", "INGEST_PUBLISH_S3_REGION", "INGEST_PUBLISH_S3_ACCESS_KEY_ID",
    "INGEST_PUBLISH_S3_SECRET_ACCESS_KEY", "INGEST_PUBLISH_DATABASE_URL", "HOOPDB_DB_MODE", "SESSION_POSTGRES_TLS_PROFILE"]) {
    expect(codeOf(() => portablePublicationConfig(without(k)))).toBe("publication_config_required");
    expect(codeOf(() => portablePublicationConfig({ ...GOOD, [k]: "" }))).toBe("publication_config_required");
  }
});

test("portable config: hosted identity, malformed target and budget values refuse", () => {
  for (const k of HOSTED_ONLY_VARS) expect(codeOf(() => portablePublicationConfig({ ...GOOD, [k]: "x" }))).toBe("publication_target_ambiguous");
  const invalid: Record<string, string>[] = [
    { INGEST_PUBLISH_BUCKET: "Bad_Bucket" }, { INGEST_PUBLISH_EXPECT_GENERATION: "bad" }, { INGEST_PUBLISH_S3_REGION: "US_EAST" }, { INGEST_PUBLISH_S3_REGION: "a".repeat(64) },
    { HOOPDB_DB_MODE: "neon" }, { HOOPDB_DB_MODE: "Selfhosted" }, { INGEST_PUBLISH_S3_SSE: "aws:kms" }, { INGEST_PUBLISH_S3_SSE: "" },
    // Endpoint: credentials, query, fragment, path, non-http(s), plaintext to a public or unflagged in-cluster host.
    { INGEST_PUBLISH_S3_ENDPOINT: "https://user:pw@minio.example.test" }, { INGEST_PUBLISH_S3_ENDPOINT: "https://minio.example.test/?x=1" },
    { INGEST_PUBLISH_S3_ENDPOINT: "https://minio.example.test/#f" }, { INGEST_PUBLISH_S3_ENDPOINT: "https://minio.example.test/bucket" },
    { INGEST_PUBLISH_S3_ENDPOINT: "ftp://minio.example.test" }, { INGEST_PUBLISH_S3_ENDPOINT: "http://minio.example.test" },
    { INGEST_PUBLISH_S3_ENDPOINT: "http://minio.selfhost.svc:9000" }, { INGEST_PUBLISH_S3_ENDPOINT: "http://10.0.0.5:9000", ALLOW_IN_CLUSTER_HTTP: "true" },
    { INGEST_PUBLISH_S3_ENDPOINT: "not a url" }, { ALLOW_IN_CLUSTER_HTTP: "yes" },
  ];
  for (const patch of invalid) expect([JSON.stringify(patch), codeOf(() => portablePublicationConfig({ ...GOOD, ...patch }))]).toEqual([JSON.stringify(patch), "publication_target_invalid"]);
  for (const ok of [{ INGEST_PUBLISH_S3_ENDPOINT: "http://127.0.0.1:9000" }, { INGEST_PUBLISH_S3_ENDPOINT: "http://minio.selfhost.svc:9000", ALLOW_IN_CLUSTER_HTTP: "true" },
    { INGEST_PUBLISH_S3_ENDPOINT: "http://minio.selfhost.svc.cluster.local:9000", ALLOW_IN_CLUSTER_HTTP: "true" }, { INGEST_PUBLISH_S3_SSE: "none" }])
    expect(codeOf(() => portablePublicationConfig({ ...GOOD, ...ok }))).toBe("no_throw");
  expect(portablePublicationConfig({ ...GOOD, INGEST_PUBLISH_S3_SSE: "AES256" }).sse).toBe("AES256");
  for (const p of [{ SESSION_POSTGRES_TLS_PROFILE: "custom-ca" }, { SESSION_POSTGRES_TLS_PROFILE: "disable" }, { SESSION_POSTGRES_CA_FILE: "/x" }])
    expect(codeOf(() => portablePublicationConfig({ ...GOOD, ...p }))).toBe("publication_database_tls");
  for (const p of [{ INGEST_PUBLISH_BUDGET_MS: "4999" }, { INGEST_PUBLISH_BUDGET_MS: "3600001" }, { INGEST_PUBLISH_BATCH_LIMIT: "21" }, { INGEST_PUBLICATION_GC_LIMIT: "0" }, { INGEST_PUBLISH_BUDGET_MS: "x" }])
    expect(codeOf(() => portablePublicationConfig({ ...GOOD, ...p }))).toBe("publication_budget_invalid");
});

test("portable config: provider follows the read API's Locker selection (LOCKER_PROVIDER), default s3", () => {
  expect(portablePublicationConfig(GOOD).provider).toBe("s3");
  expect(portablePublicationConfig({ ...GOOD, LOCKER_PROVIDER: "s3" }).provider).toBe("s3");
  expect(portablePublicationConfig({ ...GOOD, LOCKER_PROVIDER: "gcs" }).provider).toBe("gcs");
  expect(portablePublicationConfig({ ...GOOD, LOCKER_PROVIDER: " gcs " }).provider).toBe("gcs");
  for (const v of ["GCS", "azure", "r2", "fs"]) expect(codeOf(() => portablePublicationConfig({ ...GOOD, LOCKER_PROVIDER: v }))).toBe("publication_target_invalid");
});

test("portable secret sources: exactly one of value or absolute *_FILE", () => {
  for (const k of ["INGEST_PUBLISH_S3_ACCESS_KEY_ID", "INGEST_PUBLISH_S3_SECRET_ACCESS_KEY", "INGEST_PUBLISH_DATABASE_URL"]) {
    expect(codeOf(() => portablePublicationConfig({ ...GOOD, [k + "_FILE"]: "/run/secrets/x" }))).toBe("publication_secret_source_ambiguous");
    expect(codeOf(() => portablePublicationConfig({ ...GOOD, [k]: "", [k + "_FILE"]: "/run/secrets/x" }))).toBe("publication_secret_source_ambiguous");
    expect(codeOf(() => portablePublicationConfig({ ...without(k), [k + "_FILE"]: "relative/path" }))).toBe("publication_secret_file_invalid");
    expect(portablePublicationConfig({ ...without(k), [k + "_FILE"]: "/run/secrets/x" }).sources).toContainValue({ file: "/run/secrets/x" });
  }
});

test("portable secrets: mounted files are bounded, trimmed of one newline, validated, and never echoed", () => {
  const dir = mkdtempSync(join(tmpdir(), "portable-secrets-"));
  try {
    const f = (name: string, body: string | Uint8Array) => { const p = join(dir, name); writeFileSync(p, body); return p; };
    const env = { ...without("INGEST_PUBLISH_S3_ACCESS_KEY_ID"), INGEST_PUBLISH_S3_ACCESS_KEY_ID_FILE: f("ak", "publisher-fixture\n") };
    expect(portablePublicationSecrets(portablePublicationConfig(env), env).credentials).toEqual({ accessKeyId: "publisher-fixture", secretAccessKey: "fixture-secret-only" });
    mkdirSync(join(dir, "d"));
    for (const path of [join(dir, "missing"), join(dir, "d"), f("empty", ""), f("big", "x".repeat(65537))]) {
      const e2 = { ...env, INGEST_PUBLISH_S3_ACCESS_KEY_ID_FILE: path };
      expect(codeOf(() => portablePublicationSecrets(portablePublicationConfig(e2), e2))).toBe("publication_secret_file_invalid");
    }
    for (const body of ["\n", "two\nlines\n", "tab\there"]) {
      const e2 = { ...env, INGEST_PUBLISH_S3_ACCESS_KEY_ID_FILE: f("bad", body) };
      expect(codeOf(() => portablePublicationSecrets(portablePublicationConfig(e2), e2))).toBe("publication_secret_invalid");
    }
  } finally { rmSync(dir, { recursive: true, force: true }); }
  for (const patch of [{ INGEST_PUBLISH_S3_ACCESS_KEY_ID: "a b" }, { INGEST_PUBLISH_S3_ACCESS_KEY_ID: "ab" }, { INGEST_PUBLISH_S3_SECRET_ACCESS_KEY: "short" }, { INGEST_PUBLISH_S3_SECRET_ACCESS_KEY: "has space inside" }]) {
    const e = { ...GOOD, ...patch };
    expect(codeOf(() => portablePublicationSecrets(portablePublicationConfig(e), e))).toBe("publication_secret_invalid");
  }
});

test("portable pool: discrete fields, explicit TLS, only the ingest login, no URL retargeting", () => {
  const c = portablePoolConfig("postgresql://bulls96_session_ingest:p%40ss@pg.example.test:6543/sessions?sslmode=disable&sslrootcert=/x", GOOD);
  expect(c).toMatchObject({ host: "pg.example.test", port: 6543, user: "bulls96_session_ingest", password: "p@ss", database: "sessions", max: 4, connectionTimeoutMillis: 1000 });
  expect(c.connectionString).toBeUndefined();
  expect((c.ssl as { rejectUnauthorized: boolean }).rejectUnauthorized).toBe(true);
  expect(c.options).toBeTruthy();
  expect(portablePoolConfig("postgresql://bulls96_session_ingest:p@[::1]/sessions", GOOD).host).toBe("::1");
  for (const url of ["postgresql://bulls96_session_ingest@pg.example.test/sessions", "postgresql://bulls96_session_ingest:p@pg.example.test/",
    "postgresql://bulls96_session_ingest:p@pg.example.test/a/b", "postgresql://bulls96_session_ingest:p@pg.example.test/s?host=evil.test",
    "postgresql://bulls96_session_ingest:p@pg.example.test/s?application_name=x", "mysql://bulls96_session_ingest:p@pg.example.test/s", "nonsense"])
    expect([url, codeOf(() => portablePoolConfig(url, GOOD))]).toEqual([url, "publication_database_target"]);
  for (const user of ["postgres", "bulls96_session_ingest_seeder", "supabase_admin"])
    expect(codeOf(() => portablePoolConfig(`postgresql://${user}:p@pg.example.test/s`, GOOD))).toBe("publication_database_role");
});

test("portable runtime: disabled does no work; enabled binds only configured endpoint/keys and never IMDS or Secrets Manager", async () => {
  let calls = 0;
  const trap: any = { assets: () => { calls++; throw Error("unexpected"); }, pool: () => { calls++; throw Error("unexpected"); } };
  expect(await runPortablePublicationRuntime(GOOD, "publish", trap)).toEqual({ enabled: false });
  expect(await runPortablePublicationRuntime(GOOD, "gc", trap)).toEqual({ enabled: false });
  expect(calls).toBe(0);
  // Config refusals precede assets, secret reads and factories.
  expect(await asyncCodeOf(runPortablePublicationRuntime({ ...without("INGEST_PUBLISH_S3_ENDPOINT"), INGEST_PUBLISH_ENABLED: "true" }, "publish", trap))).toBe("publication_config_required");
  expect(calls).toBe(0);
  const assets = async () => ({ expected: { names: ["x"], generation: "a".repeat(64) }, verifiers: { searchWorker: "x", tokenVerifier: "x", ingestSeeder: "x", ingest: "x" } }) as any;
  expect(await asyncCodeOf(runPortablePublicationRuntime({ ...GOOD, INGEST_PUBLISH_ENABLED: "true", INGEST_PUBLISH_EXPECT_GENERATION: "b".repeat(64) }, "publish", { assets }))).toBe("publication_compiled_generation_mismatch");
  const fetched: string[] = [];
  const fetcher = (async (u: unknown) => { fetched.push(String(u)); return new Response("", { status: 500 }); }) as unknown as typeof fetch;
  let poolConfig: any;
  expect(await asyncCodeOf(runPortablePublicationRuntime({ ...GOOD, INGEST_PUBLISH_ENABLED: "true" }, "publish", { assets, fetcher,
    pool: (c) => { poolConfig = c; throw Object.assign(Error("stop"), { code: "stop_after_pool_config" }); } }))).toBe("stop_after_pool_config");
  expect(poolConfig).toMatchObject({ host: "pg.example.test", user: "bulls96_session_ingest", database: "sessions" });
  expect(fetched).toEqual([]);
  // A pool login other than the ingest role is refused before any store or RPC work.
  // Identity rows as recorded from real PostgreSQL 16 (see ingest-login.test.ts).
  const genuine = { session_user: "bulls96_session_ingest", current_user: "bulls96_session_ingest", role_setting: "none", server_version: 160015, backend_user: "bulls96_session_ingest" };
  const fakePool = (login: Record<string, unknown>, systemUser: string | null) => () => ({ options: { connectionTimeoutMillis: 1000 }, on() { return this; }, async end() {},
    connect(cb: any) { cb(undefined, { on() {}, removeListener() {}, release() {}, query(q: any) { queueMicrotask(() => {
      if (q.text === INGEST_LOGIN_IDENTITY_SQL) q.emit("row", login); else if (q.text === INGEST_LOGIN_PG16_SQL) q.emit("row", { system_user: systemUser });
      q.emit("end"); }); } }); } }) as any;
  let stores = 0;
  const refusedLogins: [Record<string, unknown>, string | null][] = [
    [{ ...genuine, backend_user: "postgres" }, "scram-sha-256:postgres"], // superuser + SET SESSION AUTHORIZATION
    [{ ...genuine, session_user: "postgres", role_setting: "bulls96_session_ingest", backend_user: "postgres" }, "scram-sha-256:postgres"], // SET ROLE
    [genuine, null], // trust auth on PG16: no authenticated identity
    [{ ...genuine, session_user: "postgres", current_user: "postgres", backend_user: "postgres" }, "scram-sha-256:postgres"],
  ];
  for (const [login, su] of refusedLogins)
    expect(await asyncCodeOf(runPortablePublicationRuntime({ ...GOOD, INGEST_PUBLISH_ENABLED: "true" }, "publish", { assets, fetcher, pool: fakePool(login, su), store: () => { stores++; throw Error("x"); } }))).toBe("publication_database_role");
  expect(stores).toBe(0);
  // With the ingest login: the store factory sees the endpoint, region and static keys; no KMS, SSE only when configured.
  const seen: any[] = [];
  const stop = (c: any) => { seen.push(c); throw Object.assign(Error("stop"), { code: "stop_after_store_config" }); };
  for (const sse of [undefined, "AES256"]) {
    const env = { ...GOOD, INGEST_PUBLISH_ENABLED: "true", ...(sse ? { INGEST_PUBLISH_S3_SSE: sse } : {}) };
    expect(await asyncCodeOf(runPortablePublicationRuntime(env, "publish", { assets, fetcher, pool: fakePool(genuine, "scram-sha-256:bulls96_session_ingest"), store: stop }))).toBe("stop_after_store_config");
  }
  expect(seen[0]).toMatchObject({ bucket: "selfhost-sessions", region: "us-east-1", endpoint: "https://minio.example.test", credentials: { accessKeyId: "publisher-fixture", secretAccessKey: "fixture-secret-only" } });
  expect(seen[0].serverSideEncryption).toBeUndefined();
  expect(seen[0].sseKmsKeyId).toBeUndefined();
  expect(seen[1].serverSideEncryption).toBe("AES256");
  expect(fetched).toEqual([]);
});
