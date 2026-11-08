/**
 * SERVICE=session-publication with INGEST_PUBLICATION_TARGET=portable, as a real process:
 * docker/entrypoint.sh dispatches it (a `bun` shim on PATH maps the image's /app/ to this
 * checkout and execs the real bun), publication-service.ts validates the portable target
 * before the probe port opens, the probes answer over loopback HTTP, then the first cycle
 * runs the real portable runtime against mounted secret files and a Postgres that never
 * answers. That cycle fails closed (exit 1, a code-only error line) and SIGTERM stops cleanly
 * (exit 0). No AWS or MinIO is reached; the only database contact is a loopback
 * listener that accepts and never answers. Nothing leaves loopback.
 */
import { afterAll, beforeAll, expect, test, setDefaultTimeout } from "bun:test";
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer, type Server, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { generation } from "../../hoopdb/migrate.ts";
setDefaultTimeout(60_000);

const ROOT = fileURLToPath(new URL("../../..", import.meta.url)).replace(/\/$/, "");
const SCRIPT = join(ROOT, "docker/entrypoint.sh");
let dir = "", blackhole: Server, dbPort = 0, gen = "", accepted = 0;
const sockets = new Set<Socket>();

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), "publication-service-portable-"));
  // The shim keeps the entrypoint itself unmodified: only the image root is remapped.
  writeFileSync(join(dir, "bun"), `#!/bin/sh\nset -e\nfirst=1\nfor a in "$@"; do\n  if [ "$first" = 1 ]; then set --; first=0; fi\n  case "$a" in /app/*) a="${ROOT}/\${a#/app/}" ;; esac\n  set -- "$@" "$a"\ndone\nexec "${process.execPath}" "$@"\n`);
  chmodSync(join(dir, "bun"), 0o755);
  writeFileSync(join(dir, "access-key-id"), "publisher-fixture\n");
  writeFileSync(join(dir, "secret-access-key"), "publisher-fixture-secret-only\n");
  // A loopback listener that accepts and never answers: pg's 1 s connect bound fires.
  blackhole = createServer((s) => { accepted++; sockets.add(s); s.on("error", () => {}); s.on("close", () => sockets.delete(s)); });
  await new Promise<void>((r) => blackhole.listen(0, "127.0.0.1", r));
  dbPort = (blackhole.address() as { port: number }).port;
  writeFileSync(join(dir, "database-url"), `postgresql://bulls96_session_ingest:fixture-only@127.0.0.1:${dbPort}/sessions\n`);
  gen = await generation();
});
afterAll(async () => {
  for (const s of sockets) s.destroy();
  await new Promise<void>((r) => blackhole.close(() => r()));
  rmSync(dir, { recursive: true, force: true });
});

async function freePort(): Promise<number> {
  const s = createServer();
  await new Promise<void>((r) => s.listen(0, "127.0.0.1", r));
  const p = (s.address() as { port: number }).port;
  await new Promise<void>((r) => s.close(() => r()));
  return p;
}

function env(port: number, extra: Record<string, string> = {}): Record<string, string> {
  return {
    PATH: `${dir}:/usr/bin:/bin`, HOME: dir, SERVICE: "session-publication", WORKER_HEALTH_PORT: String(port),
    INGEST_PUBLISH_ENABLED: "true", INGEST_PUBLICATION_TARGET: "portable", INGEST_PUBLISH_BUCKET: "selfhost-sessions",
    INGEST_PUBLISH_EXPECT_GENERATION: gen, INGEST_PUBLISH_S3_ENDPOINT: "http://127.0.0.1:9", INGEST_PUBLISH_S3_REGION: "us-east-1",
    INGEST_PUBLISH_S3_ACCESS_KEY_ID_FILE: join(dir, "access-key-id"), INGEST_PUBLISH_S3_SECRET_ACCESS_KEY_FILE: join(dir, "secret-access-key"),
    INGEST_PUBLISH_DATABASE_URL_FILE: join(dir, "database-url"), HOOPDB_DB_MODE: "selfhosted", SESSION_POSTGRES_TLS_PROFILE: "supabase",
    INGEST_PUBLISH_BUDGET_MS: "30000", ...extra,
  };
}

function launch(e: Record<string, string>) {
  const p = Bun.spawn(["/bin/sh", SCRIPT], { env: e, stdout: "pipe", stderr: "pipe" });
  const text = (s: ReadableStream<Uint8Array>) => new Response(s).text();
  return { p, out: text(p.stdout), err: text(p.stderr) };
}

async function probe(port: number, path: string) {
  for (let i = 0; i < 200; i++) {
    try {
      const r = await fetch(`http://127.0.0.1:${port}${path}`, { signal: AbortSignal.timeout(500) });
      return { status: r.status, body: await r.json() };
    } catch { await Bun.sleep(10); }
  }
  throw new Error("probe port never opened");
}

test("portable target: entrypoint starts the service, probes answer, a failed first cycle exits 1 with a code only", async () => {
  const port = await freePort(), before = accepted;
  const { p, out, err } = launch(env(port));
  expect(await probe(port, "/healthz")).toEqual({ status: 200, body: { ok: true, enabled: true, reason: "starting" } });
  expect(await probe(port, "/readyz")).toEqual({ status: 503, body: { ok: false, enabled: true, reason: "starting" } });
  expect(await p.exited).toBe(1);
  const stdout = await out, stderr = await err;
  const started = stdout.split("\n").filter(Boolean).map((l) => JSON.parse(l));
  expect(started[0]).toEqual({ kind: "canonical-publication", mode: "publish", service: "session-publication", enabled: true, port });
  const failure = JSON.parse(stderr.trim().split("\n").at(-1)!);
  expect(failure).toMatchObject({ kind: "canonical-publication", healthy: false });
  expect(failure.code).toMatch(/^[A-Za-z0-9_]{1,64}$/);
  // The runtime reached the configured database (and nothing else) before failing closed.
  expect(accepted).toBeGreaterThan(before);
  for (const secret of ["publisher-fixture-secret-only", "fixture-only", "publisher-fixture"]) {
    expect(stdout).not.toContain(secret);
    expect(stderr).not.toContain(secret);
  }
});

test("portable target: SIGTERM while the first cycle is pending exits 0", async () => {
  const port = await freePort();
  const { p } = launch(env(port, { INGEST_PUBLISH_BUDGET_MS: "60000" }));
  expect((await probe(port, "/healthz")).status).toBe(200);
  p.kill("SIGTERM");
  expect(await p.exited).toBe(0);
});

test("portable target misconfiguration fails startup before the probe port opens", async () => {
  const port = await freePort();
  const { p, err } = launch(env(port, { INGEST_PUBLISH_S3_ENDPOINT: "http://minio.selfhost.svc:9000" })); // no ALLOW_IN_CLUSTER_HTTP
  expect(await p.exited).toBe(1);
  expect(JSON.parse((await err).trim().split("\n").at(-1)!)).toEqual({ kind: "canonical-publication", service: "session-publication", healthy: false, code: "publication_target_invalid" });
  await expect(fetch(`http://127.0.0.1:${port}/healthz`, { signal: AbortSignal.timeout(500) })).rejects.toThrow();
});
