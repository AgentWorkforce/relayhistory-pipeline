/**
 * Default flip (docs/scrub-engine.md, "Default flip"): every scenario runs in a clean
 * child process (`env -i`-style environment, temporary cwd, so neither test preload and
 * no inherited SCRUB_* variable applies) and asserts that no TS capability grant is
 * present at start. Covers: unset default (with routed seal + readback of a fixture
 * session body), explicit wasm-bindgen, explicit wasm, disallowed Node TS
 * (`scrub_engine_ts_not_allowed`), REQUIRED override, init failure, sticky refusal,
 * pre-init log refusal, a Node service entry, and the hosted handler-entry grant.
 */
import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const CHILD = join(import.meta.dir, "fixtures", "scrub-default-child.ts");
const cwd = mkdtempSync(join(tmpdir(), "scrub-default-flip-"));
afterAll(() => rmSync(cwd, { recursive: true, force: true }));

// biome-ignore lint/suspicious/noExplicitAny: child JSON
function child(scenario: string, env: Record<string, string> = {}): any {
  const proc = Bun.spawnSync([process.execPath, CHILD, scenario], {
    cwd,
    env: { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "", ...env },
    stdout: "pipe",
    stderr: "pipe",
  });
  const stdout = proc.stdout.toString().trim();
  const last = stdout.split("\n").at(-1) ?? "";
  if (proc.exitCode !== 0 || !last.startsWith("{")) {
    throw new Error(`child ${scenario} exited ${proc.exitCode}\n${stdout}\n${proc.stderr.toString()}`);
  }
  const out = JSON.parse(last);
  expect(out.scenario).toBe(scenario);
  expect(out.grantsAtStart).toStrictEqual([]);
  return out;
}

const V10 = { ruleset: "relayhistory-scrub", version: 10, profile: "relayhistory-secrets-v10" };
const refusal = (name: string, code: string, reason: string) => ({ ok: false, name, code, reason });
const TS_NOT_ALLOWED = refusal("ScrubEngineConfigError", "scrub_engine_invalid", "scrub_engine_ts_not_allowed");
const REQUIRED = refusal("ScrubEngineConfigError", "scrub_engine_invalid", "engine_selection_required");
const MISSING = refusal("ScrubEngineUnavailable", "scrub_engine_unavailable", "bindgen_artifact_missing_wasm");

describe("scrub default flip (clean child processes, no preload, no grant)", () => {
  test("unset selects the pinned wasm-bindgen engine; routed seal + readback of a fixture body", () => {
    const out = child("default");
    expect(out.preInitLog).toBe("[scrub-unavailable]");
    expect(out.preInitReady).toBe(false);
    expect(out.init).toStrictEqual({ ok: true, kind: "wasm-bindgen" });
    expect(out.second).toStrictEqual({ ok: true, kind: "wasm-bindgen" });
    expect(out.sameInstance).toBe(true);
    expect(out.ready).toBe(true);
    expect(out.marker).toStrictEqual(V10);
    expect(out.scrubbed).toBe(true);
    expect(out.postInitLogScrubbed).toBe(true);
    expect(out.seal.routed).toBe(true);
    expect(out.seal.scrub).toStrictEqual(V10);
    expect(out.seal.metadata["scrub-version"]).toBe("10");
    expect(out.seal.metadata["scrub-profile"]).toBe("relayhistory-secrets-v10");
    expect(out.seal.secretGone).toBe(true);
    expect(out.seal.markerKept).toBe(true);
    expect(out.seal.records).toBe(out.seal.lines);
    // Unwired seam seal paths keep refusing.
    expect(out.seal.unwired).toStrictEqual(refusal("ScrubEngineUnavailable", "scrub_engine_unavailable", "seal_path_not_routed"));
    expect(out.grantsAtEnd).toStrictEqual([]);
  });

  test("explicit SCRUB_ENGINE=wasm-bindgen matches the default", () => {
    const out = child("explicit-bindgen", { SCRUB_ENGINE: "wasm-bindgen" });
    expect(out.init).toStrictEqual({ ok: true, kind: "wasm-bindgen" });
    expect(out.ready).toBe(true);
    expect(out.marker).toStrictEqual(V10);
    expect(out.routed).toBe(true);
  });

  test("explicit SCRUB_ENGINE=wasm (unpinned) fails closed", () => {
    const out = child("explicit-wasm", { SCRUB_ENGINE: "wasm" });
    expect(out.init).toStrictEqual(refusal("ScrubEngineUnavailable", "scrub_engine_unavailable", "unpinned"));
    expect(out.ready).toBe(false);
    expect(out.routed).toBe(false);
  });

  test("SCRUB_ENGINE=ts without a capability grant refuses with scrub_engine_ts_not_allowed, sticky", () => {
    const out = child("ts-refused", { SCRUB_ENGINE: "ts" });
    for (const k of ["init", "second", "scrub", "routed", "afterGrant"]) expect(out[k], k).toStrictEqual(TS_NOT_ALLOWED);
    expect(out.log).toBe("[scrub-unavailable]");
    expect(out.ready).toBe(false);
  });

  test("SCRUB_ENGINE_REQUIRED=wasm-bindgen with SCRUB_ENGINE unset passes", () => {
    const out = child("required-ok", { SCRUB_ENGINE_REQUIRED: "wasm-bindgen" });
    expect(out.init).toStrictEqual({ ok: true, kind: "wasm-bindgen" });
    expect(out.second.value.kind).toBe("wasm-bindgen");
    expect(out.second.value.manifest.crateVersion).toBe("0.5.0-alpha.9");
    expect(out.ready).toBe(true);
    expect(out.routed).toStrictEqual({ ok: true, value: "no-throw" });
  });

  test("SCRUB_ENGINE_REQUIRED=wasm-bindgen with another kind refuses, latch holds", () => {
    const out = child("required-refused", { SCRUB_ENGINE_REQUIRED: "wasm-bindgen", SCRUB_ENGINE: "ts" });
    for (const k of ["init", "second", "scrub", "routed"]) expect(out[k], k).toStrictEqual(REQUIRED);
    expect(out.ready).toBe(false);
  });

  test("default init failure fails closed and is sticky; never TS or raw", () => {
    const out = child("init-failure", { SCRUB_BINDGEN_DIR: "/nonexistent-scrub-bindgen" });
    for (const k of ["init", "second", "scrub", "routed"]) expect(out[k], k).toStrictEqual(MISSING);
    expect(out.log).toBe("[scrub-unavailable]");
    expect(out.ready).toBe(false);
  });

  test("a pre-init log call latches nothing that breaks a later same-kind init", () => {
    const out = child("lazy-then-other-kind");
    expect(out.preInitLog).toBe("[scrub-unavailable]");
    expect(out.init).toMatchObject({ ok: false, name: "ScrubEngineConfigError", code: "scrub_engine_invalid" });
    expect(out.sameKind).toStrictEqual({ ok: true, kind: "wasm-bindgen" });
  });

  test("a Node self-host entry never reaches TS, even with SCRUB_ENGINE=ts", () => {
    const out = child("node-entry", { SCRUB_ENGINE: "ts" });
    expect(out.run).toStrictEqual(TS_NOT_ALLOWED);
    expect(out.factoryCalls).toBe(0);
    expect(out.grantsAfter).toStrictEqual([]);
  });

  test("hosted Lambda modules grant TS at handler entry only, not at import", () => {
    const out = child("hosted");
    expect(out.grantsAfterImport).toStrictEqual([]);
    expect(out.grantsAfterHandler).toStrictEqual(["hosted-lambda"]);
    expect(out.init).toStrictEqual({ ok: true, kind: "ts" });
    expect(out.marker).toStrictEqual({ ruleset: "relayhistory-scrub", version: 8, profile: "relayhistory-scrub/typescript" });
    expect(out.routed).toBe(false);
  });
});
