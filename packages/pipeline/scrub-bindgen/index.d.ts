export interface Manifest {
  schema: number; ruleset: string; rulesetVersion: number; rulesetSourceSha256: string;
  crateVersion: string; abiVersion: number; scrubProfile: string; policy: 'secrets' | 'pii'; redactionToken: string;
}
export interface Scrubber {
  readonly manifest: Readonly<Manifest>;
  /** Compatibility: oversize text returns exactly [REDACTED]. */
  scrubText(text: string): string;
  /** Throws text_too_large above 16 MiB UTF-8. */
  scrubTextChecked(text: string): string;
  scrubLong(text: string): string;
  /** Persist jsonText; value is a convenience JS view and may round large integers. */
  scrubJson(jsonText: string): { value: unknown; jsonText: string; redactionCount: number };
  scrubRecords(bytes: Uint8Array): { bytes: Uint8Array; recordCount: number; redactionCount: number };
}
export function loadScrubber(options: { bytes: Uint8Array; expectedManifest: Manifest; expectedWasmSha256: string }): Promise<Scrubber>;
export function loadCompiledScrubber(options: { module: WebAssembly.Module; expectedManifest: Manifest }): Scrubber;

export const MAX_RECORD_BYTES: number;
export const MAX_STREAM_BYTES: number;

export class ScrubberError extends Error {
  readonly code: string;
  readonly disposition: 'call_refusal' | 'instance_poison';
  readonly record?: number;
}
