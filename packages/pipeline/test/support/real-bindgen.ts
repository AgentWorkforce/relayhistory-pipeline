/**
 * Test support for the REAL vendored relayscrubber 0.5.0-alpha.4 package
 * (packages/pipeline/scrub-bindgen/, pinned by scrub-bindgen-pin.json).
 *
 * - `faultGlue` wraps the real, verified glue module: every export delegates to it
 *   unless a case overrides one call. Faults are injected here, never by editing
 *   vendored bytes, so the digests still verify and the real wasm still initialises.
 * - `rawVendoredGlue` initialises the vendored glue directly (no adapter), for the
 *   Rust-side limit recheck that the JS preflight normally shadows.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { importGlueFromVerifiedBytes, type ImportBindgenGlue } from "../../src/lib/scrub-engine/bindgen-engine.js";
import pinJson from "../../scrub-bindgen-pin.json";

export const PIN = pinJson as typeof pinJson;
export const VENDOR_DIR = fileURLToPath(new URL("../../scrub-bindgen/", import.meta.url).href);
export const readVendored = (rel: string) => new Uint8Array(readFileSync(`${VENDOR_DIR}${rel}`));
export const sha256 = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

/** The AA self-host env triple. */
export const TRIPLE = Object.freeze({
  SCRUB_ENGINE: "wasm-bindgen",
  SCRUB_ENGINE_REQUIRED: "wasm-bindgen",
  READER_MAX_RULESET: "10",
});

export interface RealGlue {
  default(input: { module_or_path: unknown }): Promise<unknown>;
  initSync(input: { module: unknown }): unknown;
  manifestJson(): string;
  scrubTextChecked(text: string): string;
  scrubJson(text: string): string;
  scrubRecords(bytes: Uint8Array): string;
}

export type GlueOverrides = Partial<{
  [K in "scrubTextChecked" | "scrubJson" | "scrubRecords"]: (input: never, real: RealGlue) => unknown;
}>;

/**
 * An ImportBindgenGlue that evaluates the verified vendored glue and returns a
 * wrapper. `overrides` is read at call time, so a case can arm a fault after init.
 * `calls` counts every scrub call that reached the REAL module.
 */
export function faultGlue(overrides: GlueOverrides = {}) {
  const calls = { scrubTextChecked: 0, scrubJson: 0, scrubRecords: 0 };
  const importGlue: ImportBindgenGlue = async (verified) => {
    const real = (await importGlueFromVerifiedBytes(verified)) as RealGlue;
    const route = <K extends keyof typeof calls>(name: K) => (input: never) => {
      const o = overrides[name];
      if (o) return o(input, real);
      calls[name]++;
      return (real[name] as (i: never) => unknown)(input);
    };
    return {
      default: (i: { module_or_path: unknown }) => real.default(i),
      initSync: (i: { module: unknown }) => real.initSync(i),
      manifestJson: () => real.manifestJson(),
      scrubTextChecked: route("scrubTextChecked"),
      scrubJson: route("scrubJson"),
      scrubRecords: route("scrubRecords"),
    };
  };
  return { importGlue, overrides, calls };
}

/** The vendored glue, initialised directly with the vendored wasm bytes (no adapter, no JS preflight). */
export async function rawVendoredGlue(): Promise<RealGlue> {
  const glue = (await importGlueFromVerifiedBytes(readVendored(PIN.paths.glue))) as RealGlue;
  await glue.default({ module_or_path: readVendored(PIN.paths.wasm) });
  return glue;
}

// ---------------------------------------------------------------- stores and inputs

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createFsStore } from "../../../object-store/src/fs.js";
import type { ObjectStore } from "../../src/lib/pipeline/object-layout.js";
import type { SealRawChunk } from "../../src/lib/pipeline/raw-chunks.js";

/** A real filesystem object store in a fresh temp dir; `cleanup` removes every one created. */
export function tempStores() {
  const dirs: string[] = [];
  return {
    async store(): Promise<ObjectStore & { puts: string[] }> {
      const root = await mkdtemp(join(tmpdir(), "scrub-bindgen-")); dirs.push(root);
      const inner = createFsStore({ root });
      const puts: string[] = [];
      return {
        puts,
        backend: inner.backend, head: inner.head.bind(inner), get: inner.get.bind(inner), getObject: inner.getObject.bind(inner),
        list: inner.list.bind(inner), presignGet: inner.presignGet.bind(inner), delete: inner.delete.bind(inner),
        put: (key: string, ...rest: unknown[]) => { puts.push(key); return (inner.put as (...a: unknown[]) => ReturnType<ObjectStore["put"]>)(key, ...rest); },
      } as ObjectStore & { puts: string[] };
    },
    async cleanup() { await Promise.all(dirs.splice(0).map((d) => rm(d, { recursive: true, force: true }))); },
  };
}

export function sealInput(source: Uint8Array, seq = 0): SealRawChunk {
  return {
    location: { prefix: "96bulls/o", workspaceId: "w", machineId: "m", harness: "h", sessionId: "s", startDate: "2026-11-11" },
    format: "ndjson", seq, epoch: 0, startOffset: 0, source,
  } as SealRawChunk;
}
