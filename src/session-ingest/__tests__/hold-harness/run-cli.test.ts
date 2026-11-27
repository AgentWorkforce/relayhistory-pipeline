/**
 * run.ts argument handling, without Docker: a stub `docker` first on PATH records every call and fails. A bad
 * invocation (an empty or all-separator --pg matrix, a missing value, an unknown option) must exit 2 before any
 * container is touched; an invocation whose fixtures never start must exit nonzero, never pass vacuously.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { chmodSync, existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const RUN = new URL("./run.ts", import.meta.url).pathname;

describe("hold harness: run.ts arguments (no Docker)", () => {
  let dir: string, calls: string;
  beforeAll(() => {
    dir = mkdtempSync(join(tmpdir(), "hold-run-cli-"));
    calls = join(dir, "docker-calls.log");
    writeFileSync(join(dir, "docker"), `#!/bin/sh\necho "$*" >> "${calls}"\necho "stub docker refuses" >&2\nexit 1\n`);
    chmodSync(join(dir, "docker"), 0o755);
  });
  afterAll(() => rmSync(dir, { recursive: true, force: true }));

  const run = (args: string[]) => {
    rmSync(calls, { force: true });
    const { RH_HOLD_CONTAINER_PREFIX: _p, ...env } = process.env;
    const r = spawnSync(process.execPath, [RUN, ...args], {
      encoding: "utf8", timeout: 30_000,
      env: { ...env, PATH: `${dir}:${process.env.PATH ?? ""}`, SESSION_INGEST_TEST_PG_URL: "", SESSION_INGEST_TEST_S3_ENDPOINT: "" },
    });
    return { status: r.status, out: `${r.stdout}\n${r.stderr}`, docker: existsSync(calls) ? readFileSync(calls, "utf8") : "" };
  };

  const refused: Array<[string, string[]]> = [
    ["all-separator matrix", ["--pg", ","]],
    ["all-separator matrix, no S3 (the reported case)", ["--pg", ",", "--no-s3"]],
    ["empty matrix", ["--pg", "", "--no-s3"]],
    ["blank fields", ["--pg", " , ", "--no-s3"]],
    ["an empty field among majors", ["--pg", "15,,16", "--no-s3"]],
    ["a trailing separator", ["--pg", "15,", "--no-s3"]],
    ["--pg without a value", ["--pg"]],
    ["--pg followed by an option", ["--pg", "--no-s3"]],
    ["an unsupported major", ["--pg", "14", "--no-s3"]],
    ["a non-numeric major", ["--pg", "latest", "--no-s3"]],
    ["a duplicate major", ["--pg", "15,15", "--no-s3"]],
    ["--pg twice", ["--pg", "15", "--pg", "16", "--no-s3"]],
    ["--pg-url without a value", ["--pg-url"]],
    ["--pg-url with an empty value", ["--pg-url", "", "--no-s3"]],
    ["--pg-url with several majors", ["--pg-url", "postgres://postgres@127.0.0.1:1/postgres", "--pg", "15,16", "--no-s3"]],
    ["an unknown option", ["--bogus", "--no-s3"]],
    ["an empty file argument", ["--no-s3", "--", ""]],
  ];
  for (const [what, args] of refused) {
    test(`refused before any container: ${what}`, () => {
      const r = run(args);
      expect(r.status).toBe(2);
      expect(r.out).toContain("usage:");
      expect(r.docker).toBe("");
    });
  }

  test("fixtures that never start fail the run (no vacuous pass)", () => {
    const r = run(["--pg", "15", "--no-s3"]);
    expect(r.status).toBe(1);
    expect(r.docker).toContain("run -d");
    expect(r.out).not.toMatch(/# hold-harness .*: ok/);
  });
});
