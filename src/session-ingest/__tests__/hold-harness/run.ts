#!/usr/bin/env bun
/**
 * Run hold-harness suites against disposable Docker fixtures: postgres:<major> for each requested major (trust auth,
 * loopback, random host port, tmpfs data) and one MinIO (pinned image, loopback, random port, tmpfs, throwaway root
 * credentials generated here and passed to the child only through its environment). Every container is removed on
 * exit, error, SIGINT or SIGTERM.
 *
 *   bun src/session-ingest/__tests__/hold-harness/run.ts                         # self-tests on PG 15, 16, 17 + MinIO
 *   bun src/session-ingest/__tests__/hold-harness/run.ts --pg 17 -- path/to/x.postgres.test.ts
 *   bun src/session-ingest/__tests__/hold-harness/run.ts --pg-url "$SESSION_INGEST_TEST_PG_URL" --pg 16   # pg-proof:
 *       reuse the job's PostgreSQL service (asserted to be major 16), start only MinIO
 *   --no-s3   skip MinIO (the S3 suites must then not be in the file list: they are REQUIRED here and would fail)
 *
 * The child runs `bun test --timeout 300000 <files>` with SESSION_INGEST_TEST_PG_URL, SESSION_INGEST_TEST_PG_MAJOR,
 * SESSION_INGEST_TEST_PG_REQUIRED=1 and the SESSION_INGEST_TEST_S3_* variables. A run that skipped any test fails.
 */
import { spawn, spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { MINIO_IMAGE } from "./s3.ts";

const ROOT = new URL("../../../../", import.meta.url).pathname;
const SELF_TESTS = [
  "src/session-ingest/__tests__/hold-harness/two-conn.postgres.test.ts",
  "src/session-ingest/__tests__/hold-harness/put-gate.minio.test.ts",
];

function parse(argv: string[]) {
  const o = { majors: ["15", "16", "17"], majorGiven: false, pgUrl: undefined as string | undefined, s3: true, files: [] as string[] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (a === "--pg") { o.majors = argv[++i]!.split(",").map((s) => s.trim()).filter(Boolean); o.majorGiven = true; }
    else if (a === "--pg-url") o.pgUrl = argv[++i];
    else if (a === "--no-s3") o.s3 = false;
    else if (a === "--") { o.files.push(...argv.slice(i + 1)); break; }
    else o.files.push(a);
  }
  if (o.majors.some((m) => !/^1[5-9]$/.test(m))) throw Error(`--pg: majors 15..19, got ${o.majors.join(",")}`);
  if (o.pgUrl && o.majorGiven && o.majors.length !== 1) throw Error("--pg-url takes at most one --pg major (the server's, asserted)");
  if (!o.files.length) o.files = o.s3 ? SELF_TESTS : SELF_TESTS.filter((f) => !f.includes(".minio."));
  return o;
}

const docker = (...args: string[]) => {
  const r = spawnSync("docker", args, { encoding: "utf8" });
  if (r.status !== 0) throw Error(`docker ${args[0]} failed: ${(r.stderr || r.stdout).trim()}`);
  return r.stdout.trim();
};
const containers = new Set<string>();
const removeAll = () => { for (const c of containers) spawnSync("docker", ["rm", "-f", c], { stdio: "ignore" }); containers.clear(); };
process.on("SIGINT", () => { removeAll(); process.exit(130); });
process.on("SIGTERM", () => { removeAll(); process.exit(143); });

/** docker run -d, loopback random port; returns the host port. Never inherits this process's environment. */
function run(name: string, image: string, port: number, args: string[], cmd: string[] = []): number {
  spawnSync("docker", ["rm", "-f", name], { stdio: "ignore" });
  containers.add(name);
  docker("run", "-d", "--name", name, "-p", `127.0.0.1::${port}`, "--tmpfs", "/data:rw,mode=1777", ...args, image, ...cmd);
  const mapped = docker("port", name, `${port}/tcp`).split("\n").find((l) => l.startsWith("127.0.0.1:"));
  if (!mapped) throw Error(`${name}: no loopback port`);
  return Number(mapped.split(":")[1]);
}

async function until(what: string, name: string, probe: () => Promise<boolean> | boolean, seconds = 90) {
  for (let i = 0; i < seconds * 2; i++) { if (await probe()) return; await Bun.sleep(500); }
  throw Error(`${what} not ready: ${spawnSync("docker", ["logs", "--tail", "20", name], { encoding: "utf8" }).stderr}`);
}

async function startPostgres(major: string, tag: string): Promise<string> {
  const name = `${PREFIX}-pg-${tag}-${major}`;
  const port = run(name, `postgres:${major}`, 5432, ["-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "PGDATA=/data/pg"]);
  // pg_isready over TCP from inside the container: the init-time unix-socket server answers before the real one.
  await until(`postgres:${major}`, name, () => spawnSync("docker", ["exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"]).status === 0);
  return `postgres://postgres@127.0.0.1:${port}/postgres`;
}

async function startMinio(tag: string): Promise<Record<string, string>> {
  const name = `${PREFIX}-minio-${tag}`;
  const user = `hold-${randomBytes(6).toString("hex")}`, secret = randomBytes(24).toString("hex");
  // -e NAME (no value) copies from the docker CLI's environment, so the secret is not on its command line.
  spawnSync("docker", ["rm", "-f", name], { stdio: "ignore" });
  containers.add(name);
  const ran = spawnSync("docker", ["run", "-d", "--name", name, "-p", "127.0.0.1::9000", "--tmpfs", "/data:rw,mode=1777",
    "-e", "MINIO_ROOT_USER", "-e", "MINIO_ROOT_PASSWORD", "-e", "MINIO_BROWSER=off", MINIO_IMAGE, "server", "/data", "--address", ":9000"],
  { encoding: "utf8", env: { ...process.env, MINIO_ROOT_USER: user, MINIO_ROOT_PASSWORD: secret } });
  if (ran.status !== 0) throw Error(`minio run failed: ${ran.stderr.trim()}`);
  const mapped = docker("port", name, "9000/tcp").split("\n").find((l) => l.startsWith("127.0.0.1:"));
  if (!mapped) throw Error("minio: no loopback port");
  const endpoint = `http://${mapped}`;
  await until("minio", name, async () => (await fetch(`${endpoint}/minio/health/ready`).catch(() => undefined))?.ok === true);
  return { SESSION_INGEST_TEST_S3_ENDPOINT: endpoint, SESSION_INGEST_TEST_S3_ACCESS_KEY: user, SESSION_INGEST_TEST_S3_SECRET_KEY: secret, SESSION_INGEST_TEST_S3_REQUIRED: "1" };
}

function bunTest(files: string[], env: Record<string, string>, label: string): Promise<boolean> {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, ["test", "--timeout", "300000", ...files], { cwd: ROOT, env: { ...process.env, ...env }, stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    const tee = (to: NodeJS.WriteStream) => (c: Buffer) => { out += c.toString(); to.write(c); };
    child.stdout.on("data", tee(process.stdout)); child.stderr.on("data", tee(process.stderr));
    child.on("close", (code) => {
      const skipped = /^\s*[1-9]\d* skip$/m.test(out), passed = /^\s*[1-9]\d* pass$/m.test(out);
      const ok = code === 0 && passed && !skipped;
      console.log(`# hold-harness ${label}: ${ok ? "ok" : `FAILED (exit ${code}${skipped ? ", tests skipped" : ""}${passed ? "" : ", nothing passed"})`}`);
      resolve(ok);
    });
  });
}

const o = parse(process.argv.slice(2));
// lane.sh passes its own container prefix, so its stale-container sweep also removes ours after a SIGKILL.
const PREFIX = process.env.RH_HOLD_CONTAINER_PREFIX || "rh-hold";
if (!/^[a-z0-9][a-z0-9-]{0,80}$/.test(PREFIX)) throw Error("RH_HOLD_CONTAINER_PREFIX invalid");
const tag = `${process.pid}-${randomBytes(3).toString("hex")}`;
let ok = true;
try {
  const s3 = o.s3 ? await startMinio(tag) : {};
  if (o.pgUrl) {
    const major = o.majorGiven ? { SESSION_INGEST_TEST_PG_MAJOR: o.majors[0]! } : {};
    ok = await bunTest(o.files, { ...s3, ...major, SESSION_INGEST_TEST_PG_URL: o.pgUrl, SESSION_INGEST_TEST_PG_REQUIRED: "1" }, `given PostgreSQL${o.majorGiven ? " " + o.majors[0] : ""}`);
  } else {
    for (const major of o.majors) {
      const url = await startPostgres(major, tag);
      ok = (await bunTest(o.files, { ...s3, SESSION_INGEST_TEST_PG_URL: url, SESSION_INGEST_TEST_PG_MAJOR: major, SESSION_INGEST_TEST_PG_REQUIRED: "1" }, `PostgreSQL ${major}`)) && ok;
      spawnSync("docker", ["rm", "-f", `${PREFIX}-pg-${tag}-${major}`], { stdio: "ignore" });
      containers.delete(`${PREFIX}-pg-${tag}-${major}`);
    }
  }
} catch (e) {
  console.error(e instanceof Error ? e.message : e);
  ok = false;
} finally {
  removeAll();
}
process.exit(ok ? 0 : 1);
