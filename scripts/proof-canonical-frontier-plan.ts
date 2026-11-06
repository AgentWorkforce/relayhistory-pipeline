/** Disposable owned PostgreSQL 17 + versioned MinIO frontier rehearsal. */
import pg from "pg";
import { execFileSync, spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { basename, dirname, resolve } from "node:path";

const output = resolve(process.env.FRONTIER_PROOF_OUT ?? ".");
if (process.env.FRONTIER_DISPOSABLE !== "1" || dirname(output) !== "/home/khaliqgant/relay-briefs" || !/^canonical-frontier-plan-[A-Za-z0-9._-]+$/.test(basename(output))) throw new Error("frontier_owned_disposable_only");

interface OwnedContainer {
  Config: { Labels: Record<string, string>; Env: string[] };
  NetworkSettings: { Ports: Record<string, Array<{ HostIp: string; HostPort: string }> | null> };
}
function ownedContainer(name: string): OwnedContainer {
  const value = (JSON.parse(execFileSync("docker", ["inspect", name], { encoding: "utf8" })) as OwnedContainer[])[0];
  if (!value) throw new Error("frontier_fixture_absent");
  if (value.Config.Labels["com.96bulls.proof"] !== "canonical-publisher") throw new Error("frontier_fixture_owner");
  return value;
}
function port(value: OwnedContainer, mapping: string): number {
  const published = value.NetworkSettings.Ports[mapping]?.[0];
  if (!published || published.HostIp !== "127.0.0.1") throw new Error("frontier_fixture_not_loopback");
  return Number(published.HostPort);
}

mkdirSync(output, { recursive: false, mode: 0o700 });
const pgFixture = ownedContainer("canonical-publisher-pg17-20261104");
const s3Fixture = ownedContainer("canonical-publisher-minio-20261104");
const pgPort = port(pgFixture, "5432/tcp");
const s3Port = port(s3Fixture, "9000/tcp");
const fixtureEnv = Object.fromEntries((s3Fixture.Config.Env as string[]).map(value => { const at = value.indexOf("="); return [value.slice(0, at), value.slice(at + 1)]; }));
if (!fixtureEnv.MINIO_ROOT_USER || !fixtureEnv.MINIO_ROOT_PASSWORD) throw new Error("frontier_fixture_auth_missing");
const client = new pg.Client({ host: "127.0.0.1", port: pgPort, database: "postgres", user: "postgres", connectionTimeoutMillis: 1000 });
let serverVersion = "";
try {
  await client.connect();
  serverVersion = (await client.query("SHOW server_version_num")).rows[0].server_version_num;
} finally { await client.end(); }
if (!serverVersion.startsWith("17")) throw new Error("frontier_fixture_not_pg17");

const test = spawnSync(process.execPath, ["test", "src/session-ingest/__tests__/frontier-plan.postgres.test.ts"], {
  cwd: new URL("..", import.meta.url), encoding: "utf8",
  env: {
    PATH: process.env.PATH ?? "/usr/bin:/bin",
    TMPDIR: process.env.TMPDIR ?? "/tmp",
    NO_COLOR: "1",
    FRONTIER_TEST_PG_URL: `postgresql://postgres@127.0.0.1:${pgPort}/postgres`,
    FRONTIER_TEST_S3_ENDPOINT: `http://127.0.0.1:${s3Port}`,
    FRONTIER_TEST_S3_BUCKET: "canonical-publisher-fixture",
    FRONTIER_TEST_S3_ACCESS_KEY: fixtureEnv.MINIO_ROOT_USER,
    FRONTIER_TEST_S3_SECRET_KEY: fixtureEnv.MINIO_ROOT_PASSWORD,
  },
});
const result = {
  protocol: "canonical-publication-frontier-offline-proof-v1",
  head: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
  dirty: !!execFileSync("git", ["status", "--porcelain"], { encoding: "utf8" }).trim(),
  runtime: { engine: process.versions.bun ? "bun" : "node", version: process.versions.bun ?? process.version, nodeCompatibility: process.version },
  postgres: serverVersion,
  objectStore: "owned loopback MinIO with signed bucket-versioning read and same-response manifest version",
  qualification: "actual PG17 transaction + actual manifest/chunk/MinIO bytes; fixed seed RPC response and synthetic source schema; not production closure or actual migrated ingest_seed_scope_capture proof",
  test: "src/session-ingest/__tests__/frontier-plan.postgres.test.ts",
  status: test.status,
  stdout: test.stdout.trim(),
  stderr: test.stderr.trim(),
  pass: test.status === 0 && /1 pass/.test(test.stdout + test.stderr),
};
writeFileSync(`${output}/result.json`, JSON.stringify(result, null, 2) + "\n", { mode: 0o600 });
process.stdout.write(JSON.stringify({ ...result, stdout: undefined, stderr: undefined }) + "\n");
if (!result.pass) process.exitCode = 1;
