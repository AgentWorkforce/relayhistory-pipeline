/**
 * Session manifest v1: parsing, validation and the `content_sha256` hash
 * chain. The specification is `plugins/object-store/rust/docs/manifest-v1.md`
 * in AgentWorkforce/relayhistory; this module enforces it for readers
 * (storage.event handler, transform, pointer upsert, fetch).
 */
import { z } from "zod";
import { fromHex, sha256, toHex } from "./crypto.js";
import { ObjectStoreError } from "./errors.js";
import type { ObjectKey } from "./key.js";
import {
  LAYOUT_VERSION,
  RAW_FORMATS,
  parseSessionKey,
  rawChunkKey,
  type SessionLocation,
} from "./layout.js";

export const MANIFEST_SCHEMA_VERSION = 1;

const hex64 = z.string().regex(/^[0-9a-f]{64}$/, "64 lowercase hex chars");
const rfc3339 = z.iso.datetime({ offset: true });
const uint = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);
const component = z
  .string()
  .regex(/^[A-Za-z0-9._-]{1,128}$/)
  .refine((s) => !s.startsWith("."));

export const ChunkEntrySchema = z
  .object({
    seq: uint,
    epoch: uint,
    start_offset: uint,
    bytes: uint.min(1),
    stored_bytes: uint.min(1),
    sha256: hex64,
    sha256_8: z.string().regex(/^[0-9a-f]{8}$/),
    stored_sha256: hex64,
    compression: z.literal("zstd"),
    // Required when raw.scrub is set (checked in validateManifest).
    stored_raw_bytes: uint.min(1).optional(),
    raw_sha256: hex64.optional(),
    records: uint.optional(),
    first_ts: rfc3339.optional(),
    last_ts: rfc3339.optional(),
  })
  .loose();

/**
 * `host/path` exactly as relayhistory's `canonicalize_remote_url` produces it;
 * a line-for-line port of the Rust `canonical_remote_shape` (relayhistory
 * #548) so both validators accept the same strings.
 */
export function isCanonicalRemote(url: string): boolean {
  const slash = url.indexOf("/");
  if (slash < 0) return false;
  const host = url.slice(0, slash);
  const path = url.slice(slash + 1);
  const hostOk = host.startsWith("[")
    ? host.length > 2 && host.endsWith("]") && !/[[\]]/.test(host.slice(1, -1))
    : // Starts alphanumeric or '_' (ssh aliases such as `_corp`), so "./src"
      // and "../src" are not hosts.
      /^[A-Za-z0-9_]/.test(host) &&
      host === host.toLowerCase() &&
      !/[:@]/.test(host);
  return (
    hostOk &&
    path.length > 0 &&
    !url.includes("@") &&
    // ':' '?' '#' anywhere in the path: no scheme remnants or query tokens.
    !/[:?#]/.test(path) &&
    !/[\s\p{Cc}]/u.test(url) &&
    !path.endsWith("/")
  );
}

/**
 * Repository evidence (client-owned). `remote_url` is the canonical
 * `host/path` form from relayhistory's `project_identity` (no scheme, no
 * credentials); `root_commit` is omitted for shallow clones.
 */
export const SessionRepoSchema = z
  .object({
    remote_url: z
      .string()
      .refine(isCanonicalRemote, {
        message:
          "must be canonical host/path: lowercase host (or [ipv6]) with no port, non-empty path without ':', '?' or '#', no '@', whitespace, control chars or trailing '/'",
      })
      .optional(),
    root_commit: z
      .string()
      .regex(/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/, "40 or 64 lowercase hex chars")
      .optional(),
  })
  .loose();

/**
 * The scrub rule set the stored chunks went through (spec: "Scrubbing").
 * `version` is the lowest version applied to any listed chunk.
 */
export const ScrubMarkerSchema = z
  .object({
    ruleset: z.string().min(1),
    version: uint.min(1).max(0xffff_ffff),
  })
  .loose();

export const EpochRecordSchema = z
  .object({
    epoch: uint,
    opened_at: rfc3339,
    // `backfill` is an append-only historical stream landed after live
    // delivery has already opened the session (the switch-first migration).
    reason: z.enum(["initial", "truncated", "rewritten", "backfill"]),
    first_seq: uint,
    opened_by: z.string().min(1),
  })
  .loose();

/** Server-owned section (spec: "server"). The client never writes it. */
export const ServerSectionSchema = z
  .object({
    updated_at: rfc3339,
    normalized: z
      .object({
        version: uint.min(1),
        source_content_sha256: hex64,
        source_epoch: uint,
        built_at: rfc3339,
        manifest_key: z.string().optional(),
      })
      .loose()
      .nullable(),
    lifecycle: z.enum(["active", "idle", "abandoned", "archived"]),
    lifecycle_at: rfc3339,
    lifecycle_reason: z.string().optional(),
    retention: z
      .object({
        delete_after: rfc3339.optional(),
        legal_hold: z.boolean().optional(),
      })
      .loose()
      .optional(),
  })
  .loose();

export const ManifestV1Schema = z
  .object({
    schema_v: z.literal(MANIFEST_SCHEMA_VERSION),
    layout: z.literal(LAYOUT_VERSION),
    trace_id: z
      .string()
      .regex(
        /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
        "UUIDv7",
      ),
    revision: uint.min(1),
    created_at: rfc3339,
    updated_at: rfc3339,
    session: z
      .object({
        workspace_id: component,
        machine_id: component,
        harness: component,
        session_id: component,
        started_at: rfc3339,
        ended_at: rfc3339.optional(),
        harness_version: z.string().optional(),
        cwd_sha256: hex64.optional(),
        parent_session_id: z.string().optional(),
        repo: SessionRepoSchema.optional(),
      })
      .loose(),
    writer: z
      .object({
        delivery_instance_id: z.string().min(1),
        plugin_version: z.string().min(1),
        host_version: z.string().optional(),
      })
      .loose(),
    raw: z
      .object({
        owner: z.string().min(1),
        state: z.enum(["open", "closed"]),
        updated_at: rfc3339,
        format: z.enum(RAW_FORMATS),
        compression: z.literal("zstd"),
        chunk_count: uint,
        raw_bytes: uint,
        stored_bytes: uint,
        content_sha256: hex64,
        epoch: uint,
        epochs: z.array(EpochRecordSchema).min(1),
        next_offset: uint,
        chunks: z.array(ChunkEntrySchema),
        scrub: ScrubMarkerSchema.optional(),
      })
      .loose(),
    server: ServerSectionSchema.nullable().optional(),
    extensions: z.record(z.string(), z.unknown()).optional(),
  })
  .loose();

export type ManifestV1 = z.infer<typeof ManifestV1Schema>;
export type ChunkEntry = z.infer<typeof ChunkEntrySchema>;
export type EpochRecord = z.infer<typeof EpochRecordSchema>;
export type ScrubMarker = z.infer<typeof ScrubMarkerSchema>;
export type ServerSection = z.infer<typeof ServerSectionSchema>;
export type EffectiveState =
  | "closed"
  | "active"
  | "idle"
  | "abandoned"
  | "archived";

export interface ManifestIssue {
  path: string;
  message: string;
}

/** The pointer-row fields derived from a manifest (see the spec's mapping table). */
export interface ManifestPointerFields {
  workspaceId: string;
  machineId: string;
  harness: string;
  sessionId: string;
  startedAt: string;
  endedAt?: string;
  /** `closed` if the client closed it, else the server's lifecycle (`active` if none). */
  state: EffectiveState;
  lifecycle: "active" | "idle" | "abandoned" | "archived";
  traceId: string;
  manifestRevision: number;
  rawBytes: number;
  storedBytes: number;
  chunkCount: number;
  rawEpoch: number;
  contentSha256: string;
  /** `session.repo.remote_url`, when the client recorded it. */
  gitRemoteUrl: string | null;
  /** `session.repo.root_commit`, when known (not for shallow clones). */
  gitRootCommit: string | null;
  /** `raw.scrub` is present. When false the row is quarantined. */
  scrubbed: boolean;
  normalizedVersion: number | null;
  /** True when normalized output exists but was built from older raw content. */
  normalizedStale: boolean;
}

/** Hash chain over the ordered chunk list (spec section `content_sha256`). */
export async function computeContentSha256(
  chunks: readonly Pick<
    ChunkEntry,
    "epoch" | "start_offset" | "bytes" | "sha256"
  >[],
): Promise<string> {
  let h: Uint8Array = new Uint8Array(32);
  for (const c of chunks) {
    const buf = new Uint8Array(32 + 4 + 8 + 8 + 32);
    const view = new DataView(buf.buffer);
    buf.set(h, 0);
    view.setUint32(32, c.epoch);
    view.setBigUint64(36, BigInt(c.start_offset));
    view.setBigUint64(44, BigInt(c.bytes));
    buf.set(fromHex(c.sha256), 52);
    h = await sha256(buf);
  }
  return toHex(h);
}

/**
 * Structural and invariant checks. Returns every problem found rather than the
 * first, so a corrupt manifest can be reported in one pass.
 */
export async function validateManifest(
  manifest: ManifestV1,
  manifestKey?: ObjectKey | string,
): Promise<ManifestIssue[]> {
  const issues: ManifestIssue[] = [];
  const issue = (path: string, message: string) =>
    issues.push({ path, message });
  const { raw, session } = manifest;
  const chunks = raw.chunks;

  if (raw.chunk_count !== chunks.length)
    issue(
      "raw.chunk_count",
      `is ${raw.chunk_count}, chunks has ${chunks.length}`,
    );
  const sum = (f: (c: ChunkEntry) => number) =>
    chunks.reduce((a, c) => a + f(c), 0);
  if (raw.raw_bytes !== sum((c) => c.bytes))
    issue("raw.raw_bytes", "does not equal the sum of chunks[].bytes");
  if (raw.stored_bytes !== sum((c) => c.stored_bytes))
    issue(
      "raw.stored_bytes",
      "does not equal the sum of chunks[].stored_bytes",
    );
  if (Date.parse(manifest.updated_at) < Date.parse(manifest.created_at))
    issue("updated_at", "is before created_at");

  const epochs = raw.epochs;
  // Chunks per epoch, counted once; first_seq is checked against running
  // totals below (O(chunks + epochs), not chunks x epochs).
  const perEpoch = new Map<number, number>();
  for (const c of chunks)
    perEpoch.set(c.epoch, (perEpoch.get(c.epoch) ?? 0) + 1);
  let precedingChunks = 0;
  epochs.forEach((e, i) => {
    if (e.epoch !== i)
      issue(`raw.epochs[${i}].epoch`, `is ${e.epoch}, expected ${i}`);
    if ((i === 0) !== (e.reason === "initial"))
      issue(`raw.epochs[${i}].reason`, 'only epoch 0 is "initial"');
    // Every epoch, including one with no chunks yet, starts where the
    // earlier epochs' chunks end.
    if (e.first_seq !== precedingChunks)
      issue(
        `raw.epochs[${i}].first_seq`,
        `is ${e.first_seq}, but ${precedingChunks} chunks precede epoch ${e.epoch}`,
      );
    precedingChunks += perEpoch.get(i) ?? 0;
  });
  if (raw.epoch !== epochs.length - 1)
    issue(
      "raw.epoch",
      `is ${raw.epoch}, raw.epochs has ${epochs.length} records`,
    );

  let prev: ChunkEntry | undefined;
  chunks.forEach((c, i) => {
    const at = `raw.chunks[${i}]`;
    if (c.seq !== i)
      issue(`${at}.seq`, `is ${c.seq}, expected ${i} (dense from 0)`);
    if (c.sha256_8 !== c.sha256.slice(0, 8))
      issue(`${at}.sha256_8`, "is not sha256[0..8]");
    if (c.epoch > raw.epoch)
      issue(`${at}.epoch`, `is ${c.epoch}, beyond raw.epoch`);
    if (prev && c.epoch < prev.epoch) issue(`${at}.epoch`, "decreases");
    const firstOfEpoch = !prev || c.epoch !== prev.epoch;
    if (firstOfEpoch && epochs[c.epoch] && epochs[c.epoch]!.first_seq !== c.seq)
      issue(
        `${at}.seq`,
        `opens epoch ${c.epoch} but raw.epochs[${c.epoch}].first_seq is ${epochs[c.epoch]!.first_seq}`,
      );
    const expected = firstOfEpoch ? 0 : prev!.start_offset + prev!.bytes;
    if (c.start_offset !== expected)
      issue(
        `${at}.start_offset`,
        `is ${c.start_offset}, expected ${expected} (contiguous within epoch ${c.epoch})`,
      );
    prev = c;
  });
  const current = chunks.filter((c) => c.epoch === raw.epoch);
  const tail = current.at(-1);
  const expectedNext = tail ? tail.start_offset + tail.bytes : 0;
  if (raw.next_offset !== expectedNext)
    issue(
      "raw.next_offset",
      `is ${raw.next_offset}, expected ${expectedNext} (end of the current epoch)`,
    );
  const ns = manifest.server?.normalized;
  if (ns && ns.source_epoch > raw.epoch)
    issue("server.normalized.source_epoch", "is beyond raw.epoch");

  if (raw.scrub)
    chunks.forEach((c, i) => {
      for (const f of ["stored_raw_bytes", "raw_sha256"] as const)
        if (c[f] === undefined)
          issue(`raw.chunks[${i}].${f}`, "is required when raw.scrub is set");
    });

  if ((await computeContentSha256(chunks)) !== raw.content_sha256)
    issue("raw.content_sha256", "does not match the hash chain over chunks");

  if (manifestKey !== undefined) {
    const loc = parseSessionKey(String(manifestKey));
    if (!loc || loc.rest !== "manifest.json") {
      issue("$key", `${manifestKey} is not a v1 manifest key`);
    } else {
      const pairs: [string, string, string][] = [
        ["session.workspace_id", session.workspace_id, loc.workspaceId],
        ["session.machine_id", session.machine_id, loc.machineId],
        ["session.harness", session.harness, loc.harness],
        ["session.session_id", session.session_id, loc.sessionId],
        // d= is the UTC date of started_at, whatever offset it was written with.
        [
          "session.started_at",
          new Date(session.started_at).toISOString().slice(0, 10),
          loc.startDate,
        ],
      ];
      for (const [p, got, want] of pairs) {
        if (got !== want) issue(p, `is ${got}, key says ${want}`);
      }
    }
  }
  return issues;
}

/**
 * Parse JSON text or a value into a validated manifest. Throws
 * `ObjectStoreError("invalid_argument")` listing every issue. A higher
 * `schema_v` is refused, never guessed at.
 */
export async function parseManifest(
  input: string | unknown,
  manifestKey?: ObjectKey | string,
): Promise<ManifestV1> {
  let value: unknown = input;
  if (typeof input === "string") {
    try {
      value = JSON.parse(input);
    } catch (cause) {
      throw new ObjectStoreError(
        "invalid_argument",
        "manifest",
        "not valid JSON",
        { cause },
      );
    }
  }
  const schemaV = (value as { schema_v?: unknown } | null)?.schema_v;
  if (typeof schemaV === "number" && schemaV > MANIFEST_SCHEMA_VERSION) {
    throw new ObjectStoreError(
      "unsupported",
      "manifest",
      `schema_v ${schemaV} is newer than this reader (${MANIFEST_SCHEMA_VERSION})`,
    );
  }
  const parsed = ManifestV1Schema.safeParse(value);
  if (!parsed.success) {
    const detail = parsed.error.issues
      .map((i) => `${i.path.join(".")}: ${i.message}`)
      .join("; ");
    throw new ObjectStoreError("invalid_argument", "manifest", detail);
  }
  const issues = await validateManifest(parsed.data, manifestKey);
  if (issues.length > 0) {
    throw new ObjectStoreError(
      "invalid_argument",
      "manifest",
      issues.map((i) => `${i.path}: ${i.message}`).join("; "),
    );
  }
  return parsed.data;
}

export function pointerFields(manifest: ManifestV1): ManifestPointerFields {
  const { session, raw } = manifest;
  const lifecycle = manifest.server?.lifecycle ?? "active";
  const normalized = manifest.server?.normalized ?? null;
  return {
    workspaceId: session.workspace_id,
    machineId: session.machine_id,
    harness: session.harness,
    sessionId: session.session_id,
    startedAt: session.started_at,
    ...(session.ended_at ? { endedAt: session.ended_at } : {}),
    state: raw.state === "closed" ? "closed" : lifecycle,
    lifecycle,
    traceId: manifest.trace_id,
    manifestRevision: manifest.revision,
    rawBytes: raw.raw_bytes,
    storedBytes: raw.stored_bytes,
    chunkCount: raw.chunk_count,
    rawEpoch: raw.epoch,
    contentSha256: raw.content_sha256,
    gitRemoteUrl: session.repo?.remote_url ?? null,
    gitRootCommit: session.repo?.root_commit ?? null,
    scrubbed: isScrubbed(manifest),
    normalizedVersion: normalized?.version ?? null,
    normalizedStale: normalized !== null && !normalizedIsCurrent(manifest),
  };
}

/**
 * Normalized output exists and was built from the current raw content **and**
 * epoch. Opening an epoch adds no chunk, so the digest alone cannot show that
 * the output belongs to the previous epoch.
 */
export function normalizedIsCurrent(manifest: ManifestV1): boolean {
  const n = manifest.server?.normalized;
  return (
    !!n &&
    n.source_content_sha256 === manifest.raw.content_sha256 &&
    n.source_epoch === manifest.raw.epoch
  );
}

/** The client recorded `raw.scrub`: the stored chunks were scrubbed. */
export function isScrubbed(manifest: ManifestV1): boolean {
  return manifest.raw.scrub !== undefined;
}

/**
 * An unscrubbed manifest (no `raw.scrub`): readers must not transform its
 * chunks or index their content. Quarantine is sticky: a client never adds
 * the marker after chunks were stored, so a reader that has seen chunks
 * without it keeps the session quarantined even if a later revision has it.
 */
export function quarantined(manifest: ManifestV1): boolean {
  return !isScrubbed(manifest);
}

/**
 * The stored pointer row's scrub columns (`session_index.scrubbed`,
 * `chunk_count`, `scrub_quarantined`; Supabase migration 20261025000200).
 */
export interface PreviousPointer {
  scrubbed: boolean;
  chunkCount: number;
  scrubQuarantined: boolean;
}

/** The scrub columns to write on a pointer upsert. */
export interface PointerScrub {
  scrubbed: boolean;
  scrubQuarantined: boolean;
}

/**
 * The scrub columns for a pointer upsert; the same rule as the Supabase
 * `session_index_scrub_sticky` trigger. Quarantine is sticky: once a row
 * recorded chunks without `raw.scrub`, a later revision carrying the marker
 * does not clear it, because the marker would vouch for chunks that were
 * stored unscrubbed. The evidence is the one-way `scrubQuarantined` flag, not
 * the current chunk count, so a revision that resets the count cannot erase
 * it. A row that had no chunks yet may become scrubbed; a row that already
 * had chunks when the column was added (scrubbed false) stays quarantined.
 */
export function scrubbedForPointer(
  previous: PreviousPointer | null,
  manifest: ManifestV1,
): PointerScrub {
  const scrubQuarantined =
    (previous?.scrubQuarantined ?? false) ||
    (previous !== null && !previous.scrubbed && previous.chunkCount > 0) ||
    (!isScrubbed(manifest) && manifest.raw.chunk_count > 0);
  return {
    scrubbed: !scrubQuarantined && isScrubbed(manifest),
    scrubQuarantined,
  };
}

/**
 * Whether the transform must (re)run: the manifest is scrubbed and its
 * normalized output is missing or stale. Never true for a quarantined one.
 */
export function needsTransform(manifest: ManifestV1): boolean {
  return !quarantined(manifest) && !normalizedIsCurrent(manifest);
}

/** The session location a manifest describes. */
export function manifestLocation(
  manifest: ManifestV1,
  prefix?: string,
): SessionLocation {
  return {
    ...(prefix ? { prefix } : {}),
    workspaceId: manifest.session.workspace_id,
    machineId: manifest.session.machine_id,
    harness: manifest.session.harness,
    sessionId: manifest.session.session_id,
    startDate: new Date(manifest.session.started_at),
  };
}

/** Object key of each chunk, in `seq` order. Pass `currentEpochOnly` for the live content. */
export function chunkKeys(
  manifest: ManifestV1,
  options: { prefix?: string; currentEpochOnly?: boolean } = {},
): { entry: ChunkEntry; key: ObjectKey }[] {
  const loc = manifestLocation(manifest, options.prefix);
  return manifest.raw.chunks
    .filter((c) => !options.currentEpochOnly || c.epoch === manifest.raw.epoch)
    .map((entry) => ({
      entry,
      key: rawChunkKey(
        loc,
        entry.seq,
        entry.start_offset,
        entry.sha256,
        manifest.raw.format,
      ),
    }));
}
