/** Lossless bounded framing of the OFFICIAL canonical projection for model work.
 * This module does not parse/fold raw records or decide visibility. The caller
 * captures HistorySourceReference and applies its complete mask before projection. */
import { createHash } from "node:crypto";
import type { HistorySourceReference } from "../shared/history-source-authority.ts";
import type { ScopeRead } from "../session-read-api/canonical-projection.ts";

export type FullModelProjectionRow = ScopeRead["projectionRows"][number];
export const FULL_MODEL_INPUT_BYTES = 48 * 1024;
export const FULL_MODEL_ROW_BYTES = 64 * 1024 * 1024;
const MAX_FRAGMENT_UNITS = 16 * 1024;
const DEC = /^(0|[1-9][0-9]{0,18})$/;
function no(code: string): never { throw Object.assign(new Error(code), { code }); }
const sha = (value: string) => createHash("sha256").update(value).digest("hex");

/** Model-work cache domain, not a replacement for Native's SQL source evidence.
 * Explicit constructors avoid JSONB/object insertion-order drift. */
export function fullModelSourceBinding(c: HistorySourceReference): string {
  return sha(JSON.stringify(["bob-full-source-v1",
    { orgId: c.identity.orgId, workspaceId: c.identity.workspaceId, source: c.identity.source, sessionId: c.identity.sessionId },
    { epoch: c.source.epoch, fingerprint: c.source.fingerprint },
    c.authorityId, c.maskSha256, c.schemaGeneration, c.derivedAnchorKey]));
}
export interface PreparedModelRow { readonly json: string; readonly sha256: string; readonly units: number }
/** All projected fields are retained, including arbitrary native/tool objects.
 * Oversize is an explicit refusal, never an unchecked body-limit increase. */
export function prepareFullModelRow(row: FullModelProjectionRow): PreparedModelRow {
  const json = JSON.stringify(row);
  if (typeof json !== "string" || Buffer.byteLength(json) > FULL_MODEL_ROW_BYTES) no("summary_source_row_bound");
  return Object.freeze({ json, sha256: sha(json), units: json.length });
}
export interface ModelInputFragment {
  rowOrdinal: string; utf16Offset: number; totalUtf16: number; rowSha256: string; jsonFragment: string;
}
export interface ModelInputCursor { rowOrdinal: string; utf16Offset: number; rowSha256?: string }
export interface FullModelInput {
  protocol: "bob-canonical-input-v1"; sourceBinding: string; fragments: ModelInputFragment[];
}
export interface PreparedModelInput {
  readonly input: string; readonly sha256: string; readonly next: ModelInputCursor;
  readonly inputComplete: boolean; readonly rowsCompleted: number; readonly fragments: number;
}
function checkedCursor(cursor: ModelInputCursor) {
  if (!DEC.test(cursor.rowOrdinal) || !Number.isSafeInteger(cursor.utf16Offset) || cursor.utf16Offset < 0 ||
      (cursor.utf16Offset > 0 && !/^[a-f0-9]{64}$/.test(cursor.rowSha256 ?? ""))) no("summary_source_cursor");
}
function fragment(row: PreparedModelRow, cursor: ModelInputCursor, units: number): ModelInputFragment {
  return { rowOrdinal: cursor.rowOrdinal, utf16Offset: cursor.utf16Offset, totalUtf16: row.units,
    rowSha256: row.sha256, jsonFragment: row.json.slice(cursor.utf16Offset, cursor.utf16Offset + units) };
}
/** One invocation retains at most one bounded row plus one bounded input. readRow
 * uses the same captured canonical source/index and throws on unknown/gaps.
 * null means verified end, never a failed/missing object. No first-N sampling. */
export async function nextFullModelInput(args: {
  sourceBinding: string; cursor: ModelInputCursor;
  readRow(ordinal: string, signal: AbortSignal): Promise<FullModelProjectionRow | null>;
  signal: AbortSignal;
}): Promise<PreparedModelInput> {
  if (!/^[a-f0-9]{64}$/.test(args.sourceBinding)) no("summary_source_binding");
  checkedCursor(args.cursor);
  let cursor: ModelInputCursor = { ...args.cursor }, completed = 0, inputComplete = false;
  let cached: { ordinal: string; row: PreparedModelRow } | undefined;
  const envelope: FullModelInput = { protocol: "bob-canonical-input-v1", sourceBinding: args.sourceBinding, fragments: [] };
  const encode = (part?: ModelInputFragment) => JSON.stringify(part ? { ...envelope, fragments: [...envelope.fragments, part] } : envelope);
  while (envelope.fragments.length < 128) {
    args.signal.throwIfAborted();
    if (cached?.ordinal !== cursor.rowOrdinal) {
      const value = await args.readRow(cursor.rowOrdinal, args.signal);
      args.signal.throwIfAborted();
      if (value === null) {
        if (cursor.utf16Offset !== 0) no("summary_source_missing_row");
        inputComplete = true; break;
      }
      cached = { ordinal: cursor.rowOrdinal, row: prepareFullModelRow(value) };
    }
    const row = cached.row;
    if (cursor.utf16Offset > 0 && cursor.rowSha256 !== row.sha256) no("summary_source_row_changed");
    if (cursor.utf16Offset >= row.units) no("summary_source_cursor");
    let low = 1, high = Math.min(MAX_FRAGMENT_UNITS, row.units - cursor.utf16Offset), best = 0;
    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      if (Buffer.byteLength(encode(fragment(row, cursor, mid))) <= FULL_MODEL_INPUT_BYTES) { best = mid; low = mid + 1; }
      else high = mid - 1;
    }
    if (best === 0) {
      if (envelope.fragments.length === 0) no("summary_source_fragment_bound");
      break;
    }
    envelope.fragments.push(fragment(row, cursor, best));
    if (cursor.utf16Offset + best === row.units) {
      cursor = { rowOrdinal: (BigInt(cursor.rowOrdinal) + 1n).toString(), utf16Offset: 0 }; completed++;
    } else cursor = { ...cursor, utf16Offset: cursor.utf16Offset + best, rowSha256: row.sha256 };
  }
  const input = encode();
  if (!envelope.fragments.length && !inputComplete) no("summary_source_no_progress");
  if (Buffer.byteLength(input) > FULL_MODEL_INPUT_BYTES) no("summary_source_fragment_bound");
  return Object.freeze({ input, sha256: sha(input), next: Object.freeze(cursor), inputComplete, rowsCompleted: completed, fragments: envelope.fragments.length });
}
