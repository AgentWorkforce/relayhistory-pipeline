/**
 * RelayHistory export transform (switch-first E4/O4).
 *
 * Live post-T0 records and the later pre-T0 export share one append-only raw
 * stream. The export is a dedicated `backfill` epoch, so arrival order is not
 * semantic: this reader takes the highest numeric revision of each
 * `(origin_id, record_id)`, applies tombstones, and then orders the surviving
 * records by event time. The resulting object is content-addressed by the
 * raw manifest digest; a later manifest cannot overwrite an earlier result.
 */
import { sha256Hex } from "../../../../object-store/src/crypto.js";
import {
  manifestLocation,
  normalizedKey,
  objectKey,
  rawChunkKey,
  readAll,
  type ManifestV1,
  type ObjectStore,
} from "./object-layout.js";
import type { ManifestReader } from "./adapter.js";
import { PipelineError } from "./errors.js";
import type { RawPointer, SessionTransform } from "./stages.js";

export const RELAYHISTORY_NORMALIZED_VERSION = 1;
export const RELAYHISTORY_EXPORT_EXTENSION = "client.relayhistory_export";
const MAX_RELAYHISTORY_CHUNK_BYTES = 16 * 1024 * 1024;
const MAX_REVISION = 9_223_372_036_854_775_807n;

export interface RelayhistoryStreamRecord {
  kind: string;
  origin_id: string;
  record_id: string;
  revision: string;
  operation: "upsert" | "delete";
  received_at: string;
  payload: Record<string, unknown> | null;
}

export type ZstdDecompress = (stored: Uint8Array) => Uint8Array;

function fail(code: string, message: string): never {
  throw new PipelineError(code, message, false);
}

function exported(manifest: ManifestV1): boolean {
  const marker = manifest.extensions?.[RELAYHISTORY_EXPORT_EXTENSION];
  return (
    !!marker &&
    typeof marker === "object" &&
    !Array.isArray(marker) &&
    (marker as Record<string, unknown>).version === 1 &&
    (marker as Record<string, unknown>).source === "neon.delivery_records"
  );
}

function stringField(
  value: Record<string, unknown>,
  field: string,
): string {
  const fieldValue = value[field];
  if (typeof fieldValue !== "string" || !fieldValue || fieldValue.length > 1024)
    fail("relayhistory_record_invalid", `RelayHistory record ${field} is missing`);
  return fieldValue;
}

export function parseRelayhistoryRecord(line: string): RelayhistoryStreamRecord {
  let value: unknown;
  try {
    value = JSON.parse(line);
  } catch {
    fail("relayhistory_record_invalid", "RelayHistory export contains invalid JSON");
  }
  if (!value || typeof value !== "object" || Array.isArray(value))
    fail("relayhistory_record_invalid", "RelayHistory export record is not an object");
  const record = value as Record<string, unknown>;
  const revision = stringField(record, "revision");
  const receivedAt = stringField(record, "received_at");
  const operation = record.operation ?? "upsert";
  const payload = record.payload ?? null;
  if (
    !/^(?:0|[1-9][0-9]{0,18})$/.test(revision) ||
    BigInt(revision) > MAX_REVISION
  )
    fail("relayhistory_record_invalid", "RelayHistory record revision is not numeric");
  if (!Number.isFinite(Date.parse(receivedAt)))
    fail("relayhistory_record_invalid", "RelayHistory record received_at is invalid");
  if (operation !== "upsert" && operation !== "delete")
    fail("relayhistory_record_invalid", "RelayHistory record operation is invalid");
  if (
    payload !== null &&
    (typeof payload !== "object" || Array.isArray(payload))
  )
    fail("relayhistory_record_invalid", "RelayHistory record payload is invalid");
  if (operation === "upsert" && payload === null)
    fail("relayhistory_record_invalid", "RelayHistory upsert has no payload");
  return {
    kind: stringField(record, "kind"),
    origin_id: stringField(record, "origin_id"),
    record_id: stringField(record, "record_id"),
    revision,
    operation,
    received_at: receivedAt,
    payload: payload as Record<string, unknown> | null,
  };
}

function eventTime(record: RelayhistoryStreamRecord): number {
  const payload = record.payload ?? {};
  for (const field of [
    "ts_ms",
    "timestamp_ms",
    "created_ms",
    "spawned_at_ms",
    "updated_ms",
    "first_activity_ms",
    "last_activity_ms",
  ]) {
    const value = payload[field];
    if (typeof value === "number" && Number.isFinite(value) && value > 0)
      return value;
    if (typeof value === "string" && /^\d{10,16}$/.test(value)) {
      const parsed = Number(value);
      if (Number.isSafeInteger(parsed)) return parsed;
    }
  }
  return Date.parse(record.received_at);
}

function recordKey(record: RelayhistoryStreamRecord): string {
  return JSON.stringify([record.origin_id, record.record_id]);
}

function compareText(left: string, right: string): number {
  const leftPoints = left[Symbol.iterator]();
  const rightPoints = right[Symbol.iterator]();
  while (true) {
    const leftPoint = leftPoints.next();
    const rightPoint = rightPoints.next();
    if (leftPoint.done || rightPoint.done)
      return leftPoint.done === rightPoint.done ? 0 : leftPoint.done ? -1 : 1;
    const leftValue = leftPoint.value.codePointAt(0)!;
    const rightValue = rightPoint.value.codePointAt(0)!;
    if (leftValue !== rightValue) return leftValue < rightValue ? -1 : 1;
  }
}

function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([a], [b]) => compareText(a, b))
        .map(([key, child]) => [key, canonical(child)]),
    );
  return value;
}

/**
 * The same revision of one record. `received_at` is excluded: it says when each
 * path stored the record, so the export and the live client legitimately differ.
 */
function sameRecord(
  left: RelayhistoryStreamRecord,
  right: RelayhistoryStreamRecord,
): boolean {
  const content = ({ received_at: _at, ...rest }: RelayhistoryStreamRecord) =>
    JSON.stringify(canonical(rest));
  return content(left) === content(right);
}

/** Latest live records, deterministic regardless of live/backfill arrival order. */
export function normalizeRelayhistoryRecords(
  records: readonly RelayhistoryStreamRecord[],
): RelayhistoryStreamRecord[] {
  const latest = new Map<string, RelayhistoryStreamRecord>();
  for (const record of records) {
    const key = recordKey(record);
    const previous = latest.get(key);
    if (!previous || BigInt(record.revision) > BigInt(previous.revision)) {
      latest.set(key, record);
      continue;
    }
    if (
      BigInt(record.revision) === BigInt(previous.revision) &&
      !sameRecord(record, previous)
    )
      fail(
        "relayhistory_revision_conflict",
        "equal RelayHistory revisions contain different records",
      );
  }
  return [...latest.values()]
    .filter((record) => record.operation === "upsert" && record.payload !== null)
    .sort(
      (left, right) =>
        eventTime(left) - eventTime(right) ||
        compareText(left.origin_id, right.origin_id) ||
        compareText(left.record_id, right.record_id),
    );
}

async function defaultDecompress(stored: Uint8Array): Promise<Uint8Array> {
  try {
    const zlib = await import("node:zlib");
    if (typeof zlib.zstdDecompressSync !== "function")
      fail("zstd_unavailable", "this runtime cannot zstd-decompress raw chunks");
    return new Uint8Array(zlib.zstdDecompressSync(stored));
  } catch (error) {
    if (error instanceof PipelineError) throw error;
    fail("chunk_decompress_failed", "a RelayHistory raw chunk is not valid zstd");
  }
}

/** Read and verify every canonical-record chunk listed by an export manifest. */
export async function readRelayhistoryExport(
  manifest: ManifestV1,
  store: ObjectStore,
  decompress?: ZstdDecompress,
): Promise<RelayhistoryStreamRecord[]> {
  if (!exported(manifest))
    fail("relayhistory_export_marker_missing", "manifest is not a RelayHistory export");
  if (!manifest.raw.scrub)
    fail("relayhistory_export_quarantined", "RelayHistory export is not scrubbed");
  if (manifest.raw.format !== "ndjson")
    fail("relayhistory_export_format", "RelayHistory export must use ndjson");
  const loc = manifestLocation(manifest);
  const records: RelayhistoryStreamRecord[] = [];
  for (const chunk of manifest.raw.chunks) {
    if (
      typeof chunk.records !== "number" ||
      !Number.isSafeInteger(chunk.records) ||
      chunk.records < 0
    )
      fail(
        "relayhistory_record_count",
        "RelayHistory raw chunk record count is required",
      );
    if (
      chunk.bytes > MAX_RELAYHISTORY_CHUNK_BYTES ||
      chunk.stored_bytes > MAX_RELAYHISTORY_CHUNK_BYTES ||
      (chunk.stored_raw_bytes ?? 0) > MAX_RELAYHISTORY_CHUNK_BYTES
    )
      fail("relayhistory_chunk_too_large", "RelayHistory raw chunk exceeds the transform limit");
    const key = rawChunkKey(
      loc,
      chunk.seq,
      chunk.start_offset,
      chunk.sha256,
      manifest.raw.format,
    );
    let stored: Uint8Array;
    try {
      stored = await readAll(await store.get(key));
    } catch (error) {
      if ((error as { code?: unknown } | null)?.code === "not_found")
        throw new PipelineError("chunk_missing", "a listed raw chunk is missing", true);
      throw error;
    }
    if (
      stored.byteLength !== chunk.stored_bytes ||
      (await sha256Hex(stored)) !== chunk.stored_sha256
    )
      fail("chunk_digest_mismatch", "a RelayHistory raw chunk has the wrong stored digest");
    let raw: Uint8Array;
    try {
      raw = decompress ? decompress(stored) : await defaultDecompress(stored);
    } catch (error) {
      if (error instanceof PipelineError) throw error;
      fail("chunk_decompress_failed", "a RelayHistory raw chunk is not valid zstd");
    }
    if (
      raw.byteLength !== chunk.stored_raw_bytes ||
      (await sha256Hex(raw)) !== chunk.raw_sha256
    )
      fail("chunk_digest_mismatch", "a RelayHistory raw chunk has the wrong raw digest");
    let text: string;
    try {
      text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(raw);
    } catch {
      fail("relayhistory_record_invalid", "RelayHistory export is not UTF-8");
    }
    let count = 0;
    for (const line of text.split("\n")) {
      if (!line) continue;
      records.push(parseRelayhistoryRecord(line));
      count += 1;
    }
    if (chunk.records !== count)
      fail("relayhistory_record_count", "RelayHistory raw chunk record count does not match");
  }
  return normalizeRelayhistoryRecords(records);
}

function encodedRecords(records: readonly RelayhistoryStreamRecord[]): Uint8Array {
  const lines = records.map((record) =>
    JSON.stringify({
      kind: record.kind,
      origin_id: record.origin_id,
      record_id: record.record_id,
      revision: record.revision,
      received_at: record.received_at,
      payload: canonical(record.payload),
    }),
  );
  return new TextEncoder().encode(lines.length ? `${lines.join("\n")}\n` : "");
}

/**
 * Concrete file-normalize implementation for RelayHistory export manifests.
 * Non-export manifests are left for their harness-specific normalizers.
 */
export class RelayhistoryExportTransform implements SessionTransform {
  constructor(
    private readonly deps: {
      manifests: ManifestReader;
      store: ObjectStore;
      decompress?: ZstdDecompress;
    },
  ) {}

  async normalize(pointer: RawPointer): Promise<{ outputKey?: string }> {
    if (pointer.deleted) return {};
    const read = await this.deps.manifests.read(pointer.manifestKey);
    if (!read) return {};
    const manifest = read.manifest;
    if (!exported(manifest)) return {};
    const records = await readRelayhistoryExport(
      manifest,
      this.deps.store,
      this.deps.decompress,
    );
    const body = encodedRecords(records);
    const key = normalizedKey(
      manifestLocation(manifest),
      RELAYHISTORY_NORMALIZED_VERSION,
      `relayhistory-${manifest.raw.content_sha256}.ndjson`,
    );
    await this.deps.store.put(objectKey(key), body, {
      ifNoneMatch: true,
      contentType: "application/x-ndjson",
      metadata: {
        "trace-id": manifest.trace_id,
        "manifest-revision": String(manifest.revision),
        "source-content-sha256": manifest.raw.content_sha256,
        "source-epoch": String(manifest.raw.epoch),
      },
    });
    return { outputKey: key };
  }
}
