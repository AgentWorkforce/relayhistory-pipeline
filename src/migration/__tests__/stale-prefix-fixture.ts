/**
 * Synthetic session copies for the stale-prefix verifier and its receipt fence: a real ManifestV1 (parseManifest
 * against its own key) plus zstd ndjson raw chunks named by `rawChunkKey`, stored as versions with bodies in the
 * offline version provider. Every identifier here is synthetic.
 */
import { createHash } from "node:crypto";
import { zstdCompressSync } from "node:zlib";
import { rawChunkKey, type ChunkEntry, type ManifestV1 } from "../../shared/vendor.ts";
import { ATTESTATION_KIND, type ProtectionAttestation } from "../neon-s3-erasure/stale-prefix-verify.ts";
import type { FakeS3Versions } from "./fake-s3-versions.ts";

const PREFIX = /^(.+)\/v1\/ws=([^/]+)\/m=([^/]+)\/h=([^/]+)\/d=(\d{4})\/(\d{2})\/(\d{2})\/s=([^/]+)\/$/;
const sha = (b: Uint8Array | string) => createHash("sha256").update(b).digest("hex");
const enc = new TextEncoder();

/** The manifest's content_sha256 chain, synchronously (same byte layout as manifest.ts contentSha256Chain; parseManifest checks it). */
function contentSha256(chunks: readonly ChunkEntry[]): string {
  let h: Uint8Array = new Uint8Array(32);
  for (const c of chunks) {
    const buf = new Uint8Array(84);
    const view = new DataView(buf.buffer);
    buf.set(h, 0);
    view.setUint32(32, c.epoch);
    view.setBigUint64(36, BigInt(c.start_offset));
    view.setBigUint64(44, BigInt(c.bytes));
    buf.set(Buffer.from(c.sha256, "hex"), 52);
    h = new Uint8Array(createHash("sha256").update(buf).digest());
  }
  return Buffer.from(h).toString("hex");
}

/** One stream line exactly as buildSessionStream writes it (operation omitted: the stream holds upserts only). */
export function line(recordId: string, opts: { kind?: string; origin?: string; revision?: string; text?: string } = {}): string {
  return `${JSON.stringify({ kind: opts.kind ?? "history", origin_id: opts.origin ?? "origin-1", record_id: recordId, revision: opts.revision ?? "1", received_at: "2026-11-01T00:00:00.000Z" }).slice(0, -1)},"payload":${JSON.stringify({ text: opts.text ?? `text of ${recordId}` })}}`;
}
export const DEFAULT_CHUNKS = [[line("r1"), line("r2")], [line("r3")]];

export interface SessionCopy { manifestKey: string; manifest: Uint8Array; chunks: { key: string; body: Uint8Array; raw: Uint8Array }[] }

/** The objects of one session copy under `prefix` (a v1 session prefix ending in `/`), chunks of ndjson lines. */
export function buildCopy(prefix: string, chunks: readonly (readonly string[])[] = DEFAULT_CHUNKS): SessionCopy {
  const m = PREFIX.exec(prefix);
  if (!m) throw new Error("not a v1 session prefix");
  const [, root, ws, machine, harness, y, mo, d, session] = m;
  const startedAt = `${y}-${mo}-${d}T00:00:00.000Z`;
  const loc = { prefix: root!, workspaceId: ws!, machineId: machine!, harness: harness!, sessionId: session!, startDate: new Date(startedAt) };
  const entries: ChunkEntry[] = [];
  const out: SessionCopy["chunks"] = [];
  let offset = 0;
  chunks.forEach((lines, seq) => {
    const raw = enc.encode(lines.map((l) => `${l}\n`).join(""));
    const body = new Uint8Array(zstdCompressSync(raw));
    const h = sha(raw);
    entries.push({ seq, epoch: 0, start_offset: offset, bytes: raw.byteLength, stored_bytes: body.byteLength, sha256: h, sha256_8: h.slice(0, 8), stored_sha256: sha(body), compression: "zstd", records: lines.length });
    out.push({ key: String(rawChunkKey(loc, seq, offset, h, "ndjson")), body, raw });
    offset += raw.byteLength;
  });
  const manifest = {
    schema_v: 1, layout: "v1", trace_id: "01900000-0000-7000-8000-000000000001", revision: 1, created_at: startedAt, updated_at: startedAt,
    session: { workspace_id: ws, machine_id: machine, harness, session_id: session, started_at: startedAt },
    writer: { delivery_instance_id: "synthetic-writer", plugin_version: "test" },
    raw: {
      owner: "synthetic-writer", state: "closed", updated_at: startedAt, format: "ndjson", compression: "zstd", chunk_count: entries.length,
      raw_bytes: entries.reduce((n, c) => n + c.bytes, 0), stored_bytes: entries.reduce((n, c) => n + c.stored_bytes, 0),
      content_sha256: contentSha256(entries), epoch: 0,
      epochs: [{ epoch: 0, opened_at: startedAt, reason: "initial", first_seq: 0, opened_by: "synthetic-writer" }],
      next_offset: offset, chunks: entries,
    },
  } as unknown as ManifestV1;
  return { manifestKey: `${prefix}manifest.json`, manifest: enc.encode(JSON.stringify(manifest)), chunks: out };
}

/** Stores a copy as new versions (chunks first, then the manifest, as the exporter publishes). */
export function seedBodies(s: FakeS3Versions, prefix: string, at: number, chunks?: readonly (readonly string[])[]): SessionCopy {
  const copy = buildCopy(prefix, chunks);
  for (const c of copy.chunks) s.put(c.key, at, undefined, c.body);
  s.put(copy.manifestKey, at, undefined, copy.manifest);
  return copy;
}

/** A valid attestation for the fake bucket ("b") and key root, verified at `now`. */
export function attestation(now: number, over: Partial<ProtectionAttestation> = {}, keyRoot = "96bulls"): ProtectionAttestation {
  return {
    v: 1, kind: ATTESTATION_KIND, bucket_sha256: sha("b"), key_root: keyRoot, versioning: "Enabled",
    survivor_writers: { put_object: ["role/exporter"], delete_marker: ["role/eraser"], delete_version: ["role/eraser"], attested_by: "infra-synthetic" },
    lifecycle: { current_version_expiration: false, current_version_transition: false, noncurrent_version_expiration_days: 1 },
    lifecycle_change: { principals: ["role/break-glass"], break_glass: ["role/break-glass"], prevented: true, preventive_control: "scp-synthetic" },
    verified_at: new Date(now).toISOString(), change_control_ref: "CHG-SYNTHETIC-1",
    ...over,
  } as ProtectionAttestation;
}
export const BUCKET_SHA = sha("b");
