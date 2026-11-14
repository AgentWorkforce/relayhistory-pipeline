/**
 * predicate-hook.ts fallback (docs/scrub-engine.md, "Import cycle"): before the seam loads,
 * predicateScrubText answers with the TS scrubber only where the seam's first selection would
 * pick TS without refusing (a TS grant AND SCRUB_ENGINE and SCRUB_ENGINE_REQUIRED each unset,
 * "" or "ts"); everywhere else it returns undefined, so the identifier predicate fails closed.
 * Each case runs in a clean child process (no preload, temporary cwd).
 */
import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const CHILD = join(import.meta.dir, "fixtures", "predicate-hook-child.ts");
const cwd = mkdtempSync(join(tmpdir(), "predicate-hook-fallback-"));
afterAll(() => rmSync(cwd, { recursive: true, force: true }));

function child(grant: string, env: Record<string, string> = {}): { grants: string[]; seamLoaded: boolean; result: string } {
  const proc = Bun.spawnSync([process.execPath, CHILD, grant], {
    cwd,
    env: { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "", ...env },
    stdout: "pipe",
    stderr: "pipe",
  });
  const last = proc.stdout.toString().trim().split("\n").at(-1) ?? "";
  if (proc.exitCode !== 0 || !last.startsWith("{")) {
    throw new Error(`child exited ${proc.exitCode}\n${proc.stdout.toString()}\n${proc.stderr.toString()}`);
  }
  const out = JSON.parse(last);
  expect(out.seamLoaded).toBe(false);
  return out;
}

describe("predicate-hook fallback before the seam loads", () => {
  test("no grant, engine unset (the wasm-bindgen default): undefined", () => {
    const out = child("none");
    expect(out.grants).toStrictEqual([]);
    expect(out.result).toBe("undefined");
  });

  test("SCRUB_ENGINE=ts without a grant (the seam refuses): undefined", () => {
    expect(child("none", { SCRUB_ENGINE: "ts" }).result).toBe("undefined");
  });

  test("hosted grant, engine unset: scrubbed by the TS scrubber", () => {
    const out = child("hosted-lambda");
    expect(out.grants).toStrictEqual(["hosted-lambda"]);
    expect(out.result).toBe("scrubbed");
  });

  test("hosted grant with SCRUB_ENGINE=ts and SCRUB_ENGINE_REQUIRED=ts: scrubbed", () => {
    expect(child("hosted-worker", { SCRUB_ENGINE: "ts", SCRUB_ENGINE_REQUIRED: "ts" }).result).toBe("scrubbed");
  });

  test("hosted grant with SCRUB_ENGINE_REQUIRED=wasm-bindgen (the seam refuses TS): undefined", () => {
    expect(child("hosted-lambda", { SCRUB_ENGINE_REQUIRED: "wasm-bindgen" }).result).toBe("undefined");
  });

  test("hosted grant with SCRUB_ENGINE=wasm (the seam selects a wasm engine): undefined", () => {
    expect(child("hosted-lambda", { SCRUB_ENGINE: "wasm" }).result).toBe("undefined");
  });
});
