/**
 * Writes one Neon session to object storage in the 96Bulls pipeline's own
 * layout, with the pipeline's own code:
 *
 *   {keyRoot}/{org}/v1/ws=…/m=…/h=…/d=…/s=…/manifest.json
 *   {keyRoot}/{org}/v1/ws=…/m=…/h=…/d=…/s=…/raw/{seq:010}-{offset}-{sha8}.ndjson.zst
 *
 * - Chunks go through `sealRawChunk` (the ingest's scrub + zstd + create-only
 *   put, with the metadata `chunksAreServerSealed` checks), so a chunk here is
 *   indistinguishable from one the ingest sealed.
 * - The manifest is the cold-export shape of #40/#55 (`buildExportManifest`):
 *   one `initial` epoch opened by `migration:{grantId}`, the export marker
 *   with provenance, catalog and O4 `origin_revisions`, and the `96bulls`
 *   extension. It must pass `parseManifest` against its own key.
 * - The per-org key prefix (`96bulls/<org>`) is part of every key, manifest
 *   and chunk alike; the pipeline reads the org back from it
 *   (`PIPELINE_ORG_FROM_PREFIX`).
 *
 * Idempotent: an unchanged chunk is recognised from the stored manifest (same
 * seq, offset, length and source digest) and is neither sealed nor written; an
 * unchanged manifest is not rewritten. A changed session is rewritten as a new
 * manifest revision and every object under its prefix that the new manifest
 * no longer references is deleted (erasure; bucket versioning expires the
 * noncurrent versions).
 */
import { createHash } from "node:crypto";
import {
  SCRUB_RULESET,
  SCRUB_RULESET_VERSION,
  computeContentSha256,
  isObjectStoreError,
  manifestKey,
  objectKey,
  parseManifest,
  parseSessionKey,
  readAll,
  sealRawChunk,
  type ChunkEntry,
  type ManifestV1,
  type ObjectStore,
  type ObjectVersion,
  type SessionLocation,
} from "../../shared/vendor.ts";
import { MAX_CHUNK_BYTES } from "../relayhistory-export.ts";
import type { SessionUnit, StreamLine, UnitSource } from "./neon-reader.ts";

export const SYNC_PLUGIN_VERSION = "96bulls-neon-s3-sync/0.1.0";
export const EXPORT_MARKER = "client.relayhistory_export";
const SESSION_EXT = "96bulls";

export interface ExportTarget {
  store: ObjectStore;
  /** Deployment key root; objects land under `{keyRoot}/{org}/v1/...`. */
  keyRoot: string;
  /** Exporter identity: `raw.owner` is `migration:{grantId}`. */
  grantId: string;
  maxChunkBytes?: number;
  now?: () => Date;
}

/** Layout component: the pipeline's `[A-Za-z0-9._-]{1,128}`, not starting with `.` (same map as #55's courier). */
export const component = (s: string) =>
  s.replace(/[^A-Za-z0-9._-]/g, "_").slice(0, 128).replace(/^\./, "_");

const ORG = /^[A-Za-z0-9._-]{1,128}$/;
const KEY_ROOT = /^[a-z0-9][a-z0-9-]{0,62}$/;

/** `{keyRoot}/{org}`: the tenant prefix. Refuses an org that would not be one path segment. */
export function orgPrefix(keyRoot: string, orgId: string): string {
  if (!KEY_ROOT.test(keyRoot)) throw new Error("key root must be one lowercase path segment");
  if (!ORG.test(orgId) || orgId.startsWith(".")) throw new Error("org id is not a single safe path segment");
  return `${keyRoot}/${orgId}`;
}

const sha256Hex = (data: Uint8Array | string) => createHash("sha256").update(data).digest("hex");

/** UUIDv7-shaped id from a seed (as #40's `derivedUuidV7`). */
export function derivedUuidV7(seed: string, ms: number): string {
  const h = sha256Hex(seed);
  const ts = Math.max(0, Math.floor(ms)).toString(16).padStart(12, "0").slice(-12);
  const variant = ((parseInt(h[16]!, 16) & 0x3) | 0x8).toString(16);
  return `${ts.slice(0, 8)}-${ts.slice(8, 12)}-7${h.slice(1, 4)}-${variant}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

export interface MachineResult {
  manifestKey: string;
  written: boolean;
  chunksSealed: number;
  chunksReused: number;
  rawBytes: number;
  storedBytes: number;
  deletedObjects: number;
  truncatedStrings: number;
}

export interface UnitResult {
  fenced: boolean;
  manifests: MachineResult[];
  lines: number;
}

class MachineStream {
  readonly loc: SessionLocation;
  readonly key: string;
  readonly traceId: string;
  #existing: Promise<{ manifest: ManifestV1; version: ObjectVersion } | null> | undefined;
  #parts: Uint8Array[] = [];
  #len = 0;
  #offset = 0;
  #seq = 0;
  readonly entries: ChunkEntry[] = [];
  #originMax = new Map<string, number>();
  #lastOrigin: string | undefined;
  #chunkHasFeed = false;
  sealed = 0;
  reused = 0;
  truncated = 0;
  userId: string | null = null;

  constructor(
    readonly target: ExportTarget,
    readonly unit: SessionUnit,
    machine: string,
  ) {
    const startDate = new Date(unit.firstMs).toISOString().slice(0, 10);
    this.loc = {
      prefix: orgPrefix(target.keyRoot, unit.orgId),
      workspaceId: component(unit.workspaceId),
      machineId: component(machine),
      harness: component(unit.source),
      sessionId: component(unit.sessionId),
      startDate,
    };
    this.key = manifestKey(this.loc);
    // Stable while the key is stable, so a reused chunk's trace-id metadata
    // always equals the manifest's trace_id.
    this.traceId = derivedUuidV7(`trace\n${this.key}`, Date.parse(`${startDate}T00:00:00Z`));
  }

  existing() {
    this.#existing ??= readManifest(this.target.store, this.key);
    return this.#existing;
  }

  async push(line: StreamLine) {
    if (this.#len > 0 && this.#len + line.bytes.byteLength > (this.target.maxChunkBytes ?? MAX_CHUNK_BYTES))
      await this.flush();
    this.#parts.push(line.bytes);
    this.#len += line.bytes.byteLength;
    if (line.feed) {
      const { origin, revision } = line.feed;
      if (revision > (this.#originMax.get(origin) ?? -1)) this.#originMax.set(origin, revision);
      this.#lastOrigin = origin;
      this.#chunkHasFeed = true;
    }
    if (this.userId === null && line.userId) this.userId = line.userId;
    this.truncated += line.truncated ?? 0;
  }

  async flush() {
    if (this.#len === 0) return;
    const data = new Uint8Array(this.#len);
    let at = 0;
    for (const part of this.#parts) {
      data.set(part, at);
      at += part.byteLength;
    }
    this.#parts = [];
    this.#len = 0;
    const sha256 = sha256Hex(data);
    const seq = this.#seq;
    const startOffset = this.#offset;
    const prior = (await this.existing())?.manifest.raw.chunks[seq];
    let entry: ChunkEntry;
    if (
      prior &&
      prior.epoch === 0 &&
      prior.start_offset === startOffset &&
      prior.bytes === data.byteLength &&
      prior.sha256 === sha256
    ) {
      entry = { ...prior };
      this.reused += 1;
    } else {
      const sealed = await sealRawChunk(this.target.store, {
        location: this.loc,
        format: "ndjson",
        seq,
        epoch: 0,
        startOffset,
        source: data,
        traceId: this.traceId,
      });
      if (sealed.scrub.ruleset !== SCRUB_RULESET || sealed.scrub.version !== SCRUB_RULESET_VERSION)
        throw Object.assign(new Error("a stored chunk was sealed under another scrub rule set"), {
          code: "chunk_scrub_mismatch",
        });
      entry = { ...sealed.entry };
      this.sealed += 1;
    }
    // #51 resume marks: the origin of the chunk's last feed line and its max revision so far.
    delete (entry as Record<string, unknown>).source_origin;
    delete (entry as Record<string, unknown>).source_end;
    if (this.#chunkHasFeed && this.#lastOrigin !== undefined)
      Object.assign(entry, {
        source_origin: this.#lastOrigin,
        source_end: this.#originMax.get(this.#lastOrigin)!,
      });
    this.#chunkHasFeed = false;
    this.entries.push(entry);
    this.#seq += 1;
    this.#offset += data.byteLength;
  }

  originRevisions(): Record<string, number> {
    return Object.fromEntries([...this.#originMax].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
  }
}

async function readManifest(
  store: ObjectStore,
  key: string,
): Promise<{ manifest: ManifestV1; version: ObjectVersion } | null> {
  try {
    const object = await store.getObject(objectKey(key));
    const text = new TextDecoder().decode(await readAll(object.body));
    try {
      return { manifest: await parseManifest(text, key), version: object.meta.version };
    } catch {
      // An invalid stored manifest is replaced (compare-and-swap on its version), never read.
      return {
        manifest: { revision: 0, raw: { chunks: [] } } as unknown as ManifestV1,
        version: object.meta.version,
      };
    }
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    throw error;
  }
}

/** The manifest without the fields that change on every write. */
function comparable(manifest: ManifestV1): string {
  const copy = structuredClone(manifest) as unknown as Record<string, any>;
  delete copy.revision;
  delete copy.created_at;
  delete copy.updated_at;
  if (copy.raw) delete copy.raw.updated_at;
  if (copy.extensions?.[EXPORT_MARKER]) delete copy.extensions[EXPORT_MARKER].exported_at;
  return canonical(copy);
}

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object")
    return `{${Object.keys(value as object)
      .sort()
      .map((k) => `${JSON.stringify(k)}:${canonical((value as Record<string, unknown>)[k])}`)
      .join(",")}}`;
  return JSON.stringify(value);
}

async function buildManifest(
  stream: MachineStream,
  unit: SessionUnit,
  grantId: string,
  revision: number,
  createdAt: string,
  now: string,
): Promise<ManifestV1> {
  const owner = `migration:${grantId}`;
  const entries = stream.entries;
  const last = entries.at(-1)!;
  const startedAt = new Date(unit.firstMs).toISOString();
  const userId = stream.userId ?? "unknown";
  const orgId = unit.orgId;
  return {
    schema_v: 1,
    layout: "v1",
    trace_id: stream.traceId,
    revision,
    created_at: createdAt,
    updated_at: now,
    session: {
      workspace_id: stream.loc.workspaceId,
      machine_id: stream.loc.machineId,
      harness: stream.loc.harness,
      session_id: stream.loc.sessionId,
      started_at: startedAt,
      ended_at: new Date(Math.max(unit.lastMs, unit.firstMs)).toISOString(),
    },
    writer: { delivery_instance_id: owner, plugin_version: SYNC_PLUGIN_VERSION },
    raw: {
      owner,
      state: "closed",
      updated_at: now,
      format: "ndjson",
      compression: "zstd",
      chunk_count: entries.length,
      raw_bytes: entries.reduce((n, c) => n + c.bytes, 0),
      stored_bytes: entries.reduce((n, c) => n + c.stored_bytes, 0),
      content_sha256: await computeContentSha256(entries),
      epoch: 0,
      epochs: [{ epoch: 0, opened_at: startedAt, reason: "initial", first_seq: 0, opened_by: owner }],
      next_offset: last.start_offset + last.bytes,
      chunks: entries,
      scrub: { ruleset: SCRUB_RULESET, version: SCRUB_RULESET_VERSION },
    },
    extensions: {
      [SESSION_EXT]: {
        session_uuid: derivedUuidV7(`session\n${stream.key}`, unit.firstMs),
        session_uuid_source: "minted",
        tenant_id: orgId,
        user_id: userId,
      },
      [EXPORT_MARKER]: {
        version: 1,
        source: "neon.delivery_records",
        exported_at: now,
        grant_id: grantId,
        // Not a RelayAuth plan: the digest binds the exporter version and the org it wrote for.
        plan_digest: sha256Hex(`${SYNC_PLUGIN_VERSION}\n${orgId}`),
        source_user_id: userId,
        catalog: {
          neon_session_id: unit.sessionId,
          neon_workspace_id: unit.workspaceId,
          neon_source: unit.source,
          first_activity: startedAt,
          last_activity: new Date(unit.lastMs).toISOString(),
          ...unit.totals,
          ...(stream.truncated ? { truncated_strings: stream.truncated } : {}),
        },
        origin_revisions: stream.originRevisions(),
      },
    },
  } as ManifestV1;
}

/**
 * Delete everything under `prefix` except `keep`. The manifest goes first, so
 * a reader never sees a manifest whose chunks are gone.
 */
export async function deleteUnder(
  store: ObjectStore,
  prefix: string,
  keep: ReadonlySet<string> = new Set(),
): Promise<number> {
  const keys: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix.replace(/\/+$/, "")), cursor);
    for (const item of page.items) if (!keep.has(item.key)) keys.push(item.key);
    cursor = page.nextCursor;
  } while (cursor);
  keys.sort((a, b) => Number(b.endsWith("/manifest.json")) - Number(a.endsWith("/manifest.json")));
  for (const key of keys) await store.delete(objectKey(key));
  return keys.length;
}

/**
 * Remove an exported session prefix (a manifest key this exporter wrote) and
 * everything under it, after checking the key is under this org's prefix.
 */
export async function deleteExported(target: ExportTarget, orgId: string, key: string): Promise<number> {
  const prefix = orgPrefix(target.keyRoot, orgId);
  const loc = parseSessionKey(key);
  if (!loc || loc.prefix !== prefix || loc.rest !== "manifest.json")
    throw new Error("refusing to delete a key outside this org's export prefix");
  return deleteUnder(target.store, key.slice(0, -"manifest.json".length));
}

/** Export one unit. Returns the manifest keys it now owns (none when fenced or empty). */
export async function exportUnit(target: ExportTarget, unit: SessionUnit, source: UnitSource): Promise<UnitResult> {
  if (await source.fenced(unit)) return { fenced: true, manifests: [], lines: 0 };
  const streams = new Map<string, MachineStream>();
  let lines = 0;
  for await (const line of source.lines(unit)) {
    lines += 1;
    let stream = streams.get(line.machine);
    if (!stream) {
      stream = new MachineStream(target, unit, line.machine);
      // Two Neon machines that sanitize to one key would interleave: refuse.
      for (const other of streams.values())
        if (other.key === stream.key)
          throw Object.assign(new Error("two machines map to one key"), { code: "machine_key_collision" });
      streams.set(line.machine, stream);
    }
    await stream.push(line);
  }
  const now = (target.now?.() ?? new Date()).toISOString();
  const manifests: MachineResult[] = [];
  for (const stream of [...streams.values()].sort((a, b) => (a.key < b.key ? -1 : 1))) {
    await stream.flush();
    if (stream.entries.length === 0) continue;
    const existing = await stream.existing();
    const prior = existing?.manifest.revision ? existing.manifest : null;
    const manifest = await buildManifest(
      stream,
      unit,
      target.grantId,
      (prior?.revision ?? 0) + 1,
      prior?.created_at && Date.parse(prior.created_at) <= Date.parse(now) ? prior.created_at : now,
      now,
    );
    const text = JSON.stringify(manifest);
    // Our own validator first: never store a manifest the pipeline would refuse.
    await parseManifest(text, stream.key);
    const unchanged = prior !== null && comparable(prior) === comparable(manifest);
    if (!unchanged)
      await target.store.put(objectKey(stream.key), new TextEncoder().encode(text), {
        ...(existing ? { ifMatch: existing.version } : { ifNoneMatch: true }),
        contentType: "application/json",
        metadata: { "trace-id": manifest.trace_id },
      });
    // Orphans: chunks of an older revision or of an interrupted run, and
    // normalized output built from older content. Summaries are the
    // pipeline's; they are rebuilt from the new content.
    // Listed on every export (a read), so orphans of an interrupted run go too.
    let deleted = 0;
    {
      const keep = new Set([stream.key]);
      const raw = `${stream.key.slice(0, -"manifest.json".length)}raw/`;
      for (const c of manifest.raw.chunks)
        keep.add(`${raw}${String(c.seq).padStart(10, "0")}-${c.start_offset}-${c.sha256_8}.ndjson.zst`);
      deleted += await deleteUnder(target.store, raw, keep);
      const normalized = `${stream.key.slice(0, -"manifest.json".length)}normalized/`;
      deleted += await deleteUnderUnless(target.store, normalized, (k) => k.includes(manifest.raw.content_sha256));
    }
    manifests.push({
      manifestKey: stream.key,
      written: !unchanged,
      chunksSealed: stream.sealed,
      chunksReused: stream.reused,
      rawBytes: manifest.raw.raw_bytes,
      storedBytes: manifest.raw.stored_bytes,
      deletedObjects: deleted,
      truncatedStrings: stream.truncated,
    });
  }
  return { fenced: false, manifests, lines };
}

async function deleteUnderUnless(store: ObjectStore, prefix: string, keep: (key: string) => boolean): Promise<number> {
  const keys: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix.replace(/\/+$/, "")), cursor);
    for (const item of page.items) if (!keep(item.key)) keys.push(item.key);
    cursor = page.nextCursor;
  } while (cursor);
  for (const key of keys) await store.delete(objectKey(key));
  return keys.length;
}

/**
 * The production bucket's credential model (96BullSessions #100): the writer
 * (or exporter) principal may list, get and put but never delete; only the
 * eraser may delete, and it cannot read content. Deletes go to the eraser,
 * everything else to the writer.
 */
export function splitStore(writer: ObjectStore, eraser: ObjectStore): ObjectStore {
  return {
    backend: writer.backend,
    head: (k) => writer.head(k),
    get: (k, r) => writer.get(k, r),
    getObject: (k, r) => writer.getObject(k, r),
    list: (p, c) => writer.list(p, c),
    presignGet: (k, t) => writer.presignGet(k, t),
    put: (k, b, o) => writer.put(k, b, o),
    delete: (k) => eraser.delete(k),
  };
}
