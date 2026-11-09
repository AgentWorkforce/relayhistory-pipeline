/**
 * docker/entrypoint.sh, the services image dispatcher: every SERVICE runs exactly
 * its entry, every entry exists in this repo, and anything else fails loudly.
 * Runs the real script under /bin/sh with a stub `bun` on PATH that prints its argv.
 */
import { afterAll, beforeAll, expect, test } from "bun:test";
import { chmodSync, existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("../../..", import.meta.url));
const SCRIPT = join(ROOT, "docker/entrypoint.sh");

// The whole dispatch table. A SERVICE added to the script without a row here fails below.
const EXPECTED: Record<string, string[]> = {
  "hoopdb-migrate": ["run", "/app/src/hoopdb/migrate.ts"],
  "locker-init": ["run", "/app/src/locker/init.ts"],
  "alleyoop-ingest": ["run", "/app/src/alleyoop/ingest.ts"],
  "alleyoop-courier": ["run", "/app/src/alleyoop/courier.ts"],
  "hoopdb-indexer": ["run", "/app/src/hoopdb/indexer.ts"],
  referee: ["run", "/app/src/referee/main.ts"],
  "bob-costas": ["run", "/app/src/bob-costas/main.ts"],
  "session-brief": ["run", "/app/src/session-brief/main.ts"],
  "session-read-api": ["run", "/app/src/session-read-api/server.ts"],
  "session-ingest": ["run", "/app/src/session-ingest/selfhost-server.ts"],
  "session-publication": ["run", "/app/src/session-ingest/publication-service.ts", "--continuous"],
  "session-pipeline-consumer": ["run", "/app/src/session-pipeline/consumer-service.ts"],
  "session-pipeline-consumer-bootstrap": ["run", "/app/src/session-pipeline/consumer-bootstrap.ts"],
  acquisition: ["run", "/app/src/acquisition/main.ts"],
};

let bin = "";
beforeAll(() => {
  bin = mkdtempSync(join(tmpdir(), "entrypoint-"));
  writeFileSync(join(bin, "bun"), '#!/bin/sh\nfor a in "$@"; do printf "%s\\n" "$a"; done\n');
  chmodSync(join(bin, "bun"), 0o755);
});
afterAll(() => rmSync(bin, { recursive: true, force: true }));

function dispatch(service: string | undefined) {
  const env: Record<string, string> = { PATH: `${bin}:/usr/bin:/bin` };
  if (service !== undefined) env.SERVICE = service;
  const r = Bun.spawnSync(["/bin/sh", SCRIPT], { env, stdout: "pipe", stderr: "pipe" });
  return { code: r.exitCode, out: r.stdout.toString().split("\n").filter(Boolean), err: r.stderr.toString() };
}

test("the dispatch table in the script is exactly the expected one", () => {
  const cases = [...readFileSync(SCRIPT, "utf8").matchAll(/^\s*([a-z][a-z0-9-]*)\)\s+exec /gm)].map((m) => m[1]);
  expect(cases.sort()).toEqual(Object.keys(EXPECTED).sort());
});

for (const [service, argv] of Object.entries(EXPECTED))
  test(`SERVICE=${service} execs its entry, which exists in the image source`, () => {
    const r = dispatch(service);
    expect(r).toEqual({ code: 0, out: argv, err: "" });
    expect(existsSync(join(ROOT, argv[1]!.replace(/^\/app\//, "")))).toBe(true);
  });

test("unset, empty, unknown and near-miss SERVICE values exit 2 and run nothing", () => {
  for (const service of [undefined, "", "session-publication-gc", "Session-Publication", "session-publication ", "*", "publication-main"]) {
    const r = dispatch(service);
    expect(r.code).toBe(2);
    expect(r.out).toEqual([]);
    expect(r.err).toContain("96bulls: unknown SERVICE=");
  }
});
