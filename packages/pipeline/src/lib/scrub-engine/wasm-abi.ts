/**
 * The ONE file that knows the relayscrubber wasm export surface (ABI version 1,
 * docs/scrub-engine.md "ABI contract"). When the crate's real exports arrive,
 * re-point `bindScrubAbi` here; `WasmScrubEngine` only sees `ScrubAbi`.
 *
 * Bytes in, bytes out. The module is instantiated with NO imports. The host
 * copies the input into memory the module allocated (`scrub_alloc`), calls one
 * operation, copies the result out of the module's result buffer, then frees
 * both. All integers are i32 (pointers/lengths read as unsigned).
 */

/** ABI version this binding implements; the pin and the module must both say it. */
export const WASM_ABI_VERSION = 1;

/** Operation status codes returned by every `scrub_*` operation. */
export const ABI_STATUS = {
  OK: 0,
  /** Input bytes are not UTF-8 (`scrub_records`: the chunk_not_utf8 refusal). */
  INVALID_UTF8: 1,
  /** `scrub_json` input is not one JSON value. */
  INVALID_JSON: 2,
} as const;

export type AbiOperation = "scrub_text" | "scrub_long" | "scrub_json" | "scrub_records";

/** Every export the binding requires; a missing or non-callable one fails init. */
export const REQUIRED_EXPORTS = [
  "memory",
  "scrub_abi_version",
  "scrub_ruleset_version",
  "scrub_profile",
  "scrub_alloc",
  "scrub_free",
  "scrub_text",
  "scrub_long",
  "scrub_json",
  "scrub_records",
  "scrub_result_ptr",
  "scrub_result_len",
  "scrub_result_records",
  "scrub_result_free",
] as const;

/** The wasm exports as the binding sees them (a real WebAssembly.Memory has `buffer`). */
export interface ScrubWasmExports {
  memory: { readonly buffer: ArrayBuffer };
  scrub_abi_version(): number;
  scrub_ruleset_version(): number;
  /** Status; on OK the result buffer holds the profile id as UTF-8. */
  scrub_profile(): number;
  scrub_alloc(len: number): number;
  scrub_free(ptr: number, len: number): void;
  scrub_text(ptr: number, len: number): number;
  scrub_long(ptr: number, len: number): number;
  scrub_json(ptr: number, len: number): number;
  scrub_records(ptr: number, len: number): number;
  scrub_result_ptr(): number;
  scrub_result_len(): number;
  scrub_result_records(): number;
  scrub_result_free(): void;
}

export interface AbiResult {
  status: number;
  /** Present when status is OK. */
  bytes?: Uint8Array;
  /** `scrub_records` only: complete lines in the input. */
  records?: number;
}

/** What WasmScrubEngine uses. */
export interface ScrubAbi {
  abiVersion(): number;
  rulesetVersion(): number;
  profile(): AbiResult;
  call(op: AbiOperation, input: Uint8Array): AbiResult;
}

/** A required export is absent or has the wrong shape. Names the export, never data. */
export class AbiExportMissing extends Error {
  constructor(readonly exportName: string) {
    super(`scrub wasm export missing: ${exportName}`);
    this.name = "AbiExportMissing";
  }
}

/** Bind the instance exports to `ScrubAbi`, checking every required export first. */
export function bindScrubAbi(raw: Record<string, unknown>): ScrubAbi {
  for (const name of REQUIRED_EXPORTS) {
    const value = raw[name];
    if (name === "memory") {
      const buffer = (value as { buffer?: unknown } | null | undefined)?.buffer;
      if (!(buffer instanceof ArrayBuffer)) throw new AbiExportMissing(name);
    } else if (typeof value !== "function") throw new AbiExportMissing(name);
  }
  const x = raw as unknown as ScrubWasmExports;
  const u32 = (n: number) => n >>> 0;
  /** Copy the result buffer out (memory may grow later) and release it. */
  const takeResult = (withRecords: boolean): AbiResult => {
    try {
      const ptr = u32(x.scrub_result_ptr());
      const len = u32(x.scrub_result_len());
      const bytes = new Uint8Array(x.memory.buffer, ptr, len).slice();
      return withRecords ? { status: ABI_STATUS.OK, bytes, records: x.scrub_result_records() } : { status: ABI_STATUS.OK, bytes };
    } finally {
      x.scrub_result_free();
    }
  };
  return {
    abiVersion: () => x.scrub_abi_version(),
    rulesetVersion: () => x.scrub_ruleset_version(),
    profile: () => {
      const status = x.scrub_profile();
      if (status !== ABI_STATUS.OK) {
        x.scrub_result_free();
        return { status };
      }
      return takeResult(false);
    },
    call: (op, input) => {
      const len = input.byteLength;
      const ptr = u32(x.scrub_alloc(len));
      let status: number;
      try {
        // After alloc: the buffer may have grown.
        new Uint8Array(x.memory.buffer, ptr, len).set(input);
        status = x[op](ptr, len);
      } finally {
        x.scrub_free(ptr, len);
      }
      if (status !== ABI_STATUS.OK) {
        x.scrub_result_free();
        return { status };
      }
      return takeResult(op === "scrub_records");
    },
  };
}
