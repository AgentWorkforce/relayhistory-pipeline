/**
 * AlleyOop courier: local history (Concourse) to Locker. Reads sessions from
 * history.db, sends new transcript bytes through the ingest API's scrubbing
 * chunk hop (Technical Foul runs server-side, before any byte reaches the
 * Locker), writes the v1 manifest to the Locker, and tells the pipeline it
 * landed. Mints the session uuid (UUIDv7) once per session and keeps it in a
 * small state file next to the database.
 *
 * Runs once (`COURIER_ONCE=1`) or on an interval. Idempotent: the state file
 * records what was uploaded; a rerun uploads only new bytes.
 */
import { createHash } from "node:crypto";
import { mkdir, readFile, rename, stat, writeFile, open } from "node:fs/promises";
import { dirname, join } from "node:path";
import {
  computeContentSha256,
  createS3Store,
  manifestKey,
  validateManifest,
  type ChunkEntry,
  type ManifestV1,
  type SessionLocation,
} from "../shared/vendor.ts";
import { flag, integer, lockerEnv, optional, required } from "../shared/env.ts";
import { hexTraceId, isUuidV7, spanId, uuidv7 } from "../shared/ids.ts";
import { errorFields, log } from "../shared/log.ts";
import { flushTelemetry, initTelemetry, withSpan } from "../shared/telemetry.ts";
import { EXT } from "../shared/enrich.ts";

export const PLUGIN_VERSION = "96bulls-courier/0.1.0";
const MAX_CHUNK_BYTES = 4 * 1024 * 1024;

interface SessionRow {
  session_id: string;
  source: string;
  raw_path: string | null;
  first_activity_ms: number | null;
  last_activity_ms: number | null;
  cwd: string | null;
  agent_version: string | null;
}

interface SessionState {
  session_uuid: string;
  trace_id: string;
  revision: number;
  uploaded_offset: number;
  chunks: ChunkEntry[];
  created_at: string;
  started_at: string;
  scrub?: { ruleset: string; version: number };
  manifest_key?: string;
  closed?: boolean;
}

interface CourierState {
  machine_id: string;
  sessions: Record<string, SessionState>;
}

export interface CourierConfig {
  dbPath: string;
  statePath: string;
  ingestUrl: string;
  token: string;
  orgId: string;
  workspaceId: string;
  userId: string;
  machineId?: string;
  harnesses?: string[];
  closeAfterMinutes: number;
}

export function configFromEnv(): CourierConfig {
  const dbPath = required("CONCOURSE_DB");
  return {
    dbPath,
    statePath: optional("COURIER_STATE", join(dirname(dbPath), ".alleyoop-courier.json")),
    ingestUrl: required("INGEST_URL").replace(/\/$/, ""),
    token: required("ALLEYOOP_TOKEN"),
    orgId: required("TENANT_ORG_ID"),
    workspaceId: optional("WORKSPACE_ID", "default"),
    userId: optional("USER_ID", "unknown"),
    ...(process.env.MACHINE_ID ? { machineId: process.env.MACHINE_ID } : {}),
    ...(process.env.HARNESSES ? { harnesses: process.env.HARNESSES.split(",").map((s) => s.trim()).filter(Boolean) } : {}),
    closeAfterMinutes: integer("COURIER_CLOSE_AFTER_MINUTES", 30),
  };
}

async function loadState(path: string, machineId?: string): Promise<CourierState> {
  try {
    const s = JSON.parse(await readFile(path, "utf8")) as CourierState;
    if (machineId && s.machine_id !== machineId) log.warn("machine id differs from state; keeping state's", {});
    return s;
  } catch {
    return { machine_id: machineId ?? `m-${createHash("sha256").update(uuidv7()).digest("hex").slice(0, 16)}`, sessions: {} };
  }
}

async function saveState(path: string, state: CourierState) {
  await mkdir(dirname(path), { recursive: true });
  const tmp = `${path}.tmp`;
  await writeFile(tmp, JSON.stringify(state, null, 2));
  await rename(tmp, path);
}

/** Reads the session catalogue from history.db (read-only). */
export async function readSessions(dbPath: string, harnesses?: string[]): Promise<SessionRow[]> {
  const { DatabaseSync } = await import("node:sqlite");
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    const rows = db
      .prepare("SELECT session_id, source, raw_path, first_activity_ms, last_activity_ms, cwd, agent_version FROM sessions WHERE raw_path IS NOT NULL ORDER BY first_activity_ms")
      .all() as unknown as SessionRow[];
    return harnesses ? rows.filter((r) => harnesses.includes(r.source)) : rows;
  } finally {
    db.close();
  }
}

const isoDate = (ms: number) => new Date(ms).toISOString().slice(0, 10);
const component = (s: string) => s.replace(/[^A-Za-z0-9._-]/g, "_").slice(0, 128).replace(/^\./, "_");

/** The next contiguous range of whole records after `from`, capped per chunk. */
async function readNewRecords(path: string, from: number): Promise<{ ranges: Array<{ start: number; bytes: Buffer }>; size: number }> {
  const size = (await stat(path)).size;
  if (size <= from) return { ranges: [], size };
  const fh = await open(path, "r");
  try {
    const all = Buffer.alloc(size - from);
    await fh.read(all, 0, all.length, from);
    const lastNl = all.lastIndexOf(0x0a);
    if (lastNl < 0) return { ranges: [], size };
    const usable = all.subarray(0, lastNl + 1);
    const ranges: Array<{ start: number; bytes: Buffer }> = [];
    let cursor = 0;
    while (cursor < usable.length) {
      let end = Math.min(cursor + MAX_CHUNK_BYTES, usable.length);
      if (end < usable.length) {
        const nl = usable.lastIndexOf(0x0a, end - 1);
        if (nl <= cursor) throw new Error("96bulls: a single record exceeds the chunk size");
        end = nl + 1;
      }
      ranges.push({ start: from + cursor, bytes: Buffer.from(usable.subarray(cursor, end)) });
      cursor = end;
    }
    return { ranges, size };
  } finally {
    await fh.close();
  }
}

export async function runCourierOnce(cfg: CourierConfig): Promise<{ sessions: number; uploaded: number; chunks: number }> {
  const state = await loadState(cfg.statePath, cfg.machineId);
  const locker = lockerEnv();
  const store = createS3Store({
    bucket: locker.bucket,
    region: locker.region,
    endpoint: locker.endpoint,
    credentials: { accessKeyId: locker.accessKeyId, secretAccessKey: locker.secretAccessKey },
  });
  const rows = await readSessions(cfg.dbPath, cfg.harnesses);
  let uploaded = 0;
  let chunks = 0;
  for (const row of rows) {
    if (!row.raw_path || !row.first_activity_ms) continue;
    const key = `${row.source}:${row.session_id}`;
    const now = new Date();
    let s = state.sessions[key];
    if (!s) {
      const startedAt = new Date(row.first_activity_ms).toISOString();
      s = state.sessions[key] = {
        session_uuid: uuidv7(),
        trace_id: uuidv7(),
        revision: 0,
        uploaded_offset: 0,
        chunks: [],
        created_at: now.toISOString(),
        started_at: startedAt,
      };
      log.info("session first seen; minted ids", { harness: row.source, session_id: row.session_id, session_uuid: s.session_uuid, trace_id: s.trace_id, session_uuid_source: "minted" });
    }
    const session = s;
    const loc: SessionLocation = {
      workspaceId: cfg.workspaceId,
      machineId: state.machine_id,
      harness: component(row.source),
      sessionId: component(row.session_id),
      startDate: isoDate(row.first_activity_ms),
    };
    const traceparent = `00-${hexTraceId(session.trace_id)}-${spanId()}-01`;
    const idleMinutes = row.last_activity_ms ? (now.getTime() - row.last_activity_ms) / 60_000 : 0;
    const shouldClose = idleMinutes >= cfg.closeAfterMinutes;
    let result: { ranges: Array<{ start: number; bytes: Buffer }>; size: number };
    try {
      result = await readNewRecords(row.raw_path, session.uploaded_offset);
    } catch (err) {
      log.warn("transcript unreadable", { harness: row.source, session_id: row.session_id, ...errorFields(err) });
      continue;
    }
    if (result.size < session.uploaded_offset) {
      log.warn("transcript shrank; epochs are not handled yet, skipping", { harness: row.source, session_id: row.session_id });
      continue;
    }
    const nothingNew = result.ranges.length === 0;
    if (nothingNew && (session.closed || !shouldClose) && session.revision > 0) continue;
    if (nothingNew && session.revision === 0) continue;
    await withSpan("alleyoop upload", {
      traceparent,
      attrs: { stage: "ingest", trace_id: session.trace_id, session_uuid: session.session_uuid, tenant_id: cfg.orgId, workspace_id: cfg.workspaceId, harness: row.source, session_id: row.session_id },
    }, async () => {
      for (const range of result.ranges) {
        const seq = session.chunks.length;
        const q = new URLSearchParams({
          format: "jsonl", seq: String(seq), epoch: "0", offset: String(range.start), date: String(loc.startDate),
          machine: loc.machineId, harness: loc.harness, session: loc.sessionId,
        });
        const res = await fetch(`${cfg.ingestUrl}/v1/pipeline/chunks?${q}`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${cfg.token}`,
            "content-type": "application/octet-stream",
            "x-relay-trace-id": session.trace_id,
            traceparent,
          },
          body: new Uint8Array(range.bytes),
        });
        if (!res.ok) {
          const body = (await res.json().catch(() => ({}))) as { error?: { code?: string } };
          throw Object.assign(new Error("chunk hop refused"), { code: body.error?.code ?? `http_${res.status}`, retryable: res.status >= 500 });
        }
        const sealed = (await res.json()) as { key: string; entry: ChunkEntry; scrub: { ruleset: string; version: number }; created: boolean };
        session.chunks.push(sealed.entry);
        session.scrub = session.scrub && session.scrub.version < sealed.scrub.version ? session.scrub : sealed.scrub;
        session.uploaded_offset = range.start + range.bytes.length;
        chunks += 1;
        log.info("chunk sealed", {
          stage: "deliver.put", seq, offset: range.start, bytes_raw: sealed.entry.bytes, bytes_stored: sealed.entry.stored_bytes,
          scrub_ruleset: sealed.scrub.ruleset, scrub_version: sealed.scrub.version, trace_id: session.trace_id, session_uuid: session.session_uuid,
        });
      }
      session.revision += 1;
      session.closed = shouldClose;
      const last = session.chunks.at(-1)!;
      const nowIso = new Date().toISOString();
      const manifest: ManifestV1 = {
        schema_v: 1,
        layout: "v1",
        trace_id: session.trace_id,
        revision: session.revision,
        created_at: session.created_at,
        updated_at: nowIso,
        session: {
          workspace_id: loc.workspaceId,
          machine_id: loc.machineId,
          harness: loc.harness,
          session_id: loc.sessionId,
          started_at: session.started_at,
          ...(shouldClose && row.last_activity_ms ? { ended_at: new Date(row.last_activity_ms).toISOString() } : {}),
          ...(row.agent_version ? { harness_version: row.agent_version } : {}),
          ...(row.cwd ? { cwd_sha256: createHash("sha256").update(row.cwd).digest("hex") } : {}),
        },
        writer: { delivery_instance_id: state.machine_id, plugin_version: PLUGIN_VERSION },
        raw: {
          owner: state.machine_id,
          state: shouldClose ? "closed" : "open",
          updated_at: nowIso,
          format: "jsonl",
          compression: "zstd",
          chunk_count: session.chunks.length,
          raw_bytes: session.chunks.reduce((n, c) => n + c.bytes, 0),
          stored_bytes: session.chunks.reduce((n, c) => n + c.stored_bytes, 0),
          content_sha256: await computeContentSha256(session.chunks),
          epoch: 0,
          epochs: [{ epoch: 0, opened_at: session.started_at, reason: "initial", first_seq: 0, opened_by: state.machine_id }],
          next_offset: last.start_offset + last.bytes,
          chunks: session.chunks,
          ...(session.scrub ? { scrub: session.scrub } : {}),
        },
        extensions: {
          [EXT]: { session_uuid: session.session_uuid, session_uuid_source: "minted", tenant_id: cfg.orgId, user_id: cfg.userId },
        },
      };
      const mKey = manifestKey(loc);
      const issues = await validateManifest(manifest, mKey);
      if (issues.length) {
        log.error("manifest invalid", { count: issues.length, reason: issues[0]!.path });
        throw Object.assign(new Error("manifest invalid"), { code: "manifest_invalid", retryable: false });
      }
      const body = new TextEncoder().encode(JSON.stringify(manifest));
      await store.put(mKey, body, {
        contentType: "application/json",
        metadata: { "session-uuid": session.session_uuid, "trace-id": session.trace_id, revision: String(session.revision) },
      });
      session.manifest_key = mKey;
      const landed = await fetch(`${cfg.ingestUrl}/v1/pipeline/manifests`, {
        method: "POST",
        headers: { authorization: `Bearer ${cfg.token}`, "content-type": "application/json", "x-relay-trace-id": session.trace_id, traceparent },
        body: JSON.stringify({ key: mKey }),
      });
      if (landed.status !== 202) {
        const b = (await landed.json().catch(() => ({}))) as { error?: { code?: string } };
        throw Object.assign(new Error("manifest not accepted"), { code: b.error?.code ?? `http_${landed.status}`, retryable: landed.status >= 500 });
      }
      uploaded += 1;
      log.info("manifest landed", {
        stage: "object-landed", revision: session.revision, chunk_count: session.chunks.length, bytes_raw: manifest.raw.raw_bytes,
        trace_id: session.trace_id, session_uuid: session.session_uuid, manifest_key_sha256: createHash("sha256").update(mKey).digest("hex").slice(0, 16),
        state: manifest.raw.state,
      });
    }).catch((err) => {
      log.error("session upload failed", { harness: row.source, session_id: row.session_id, trace_id: session.trace_id, session_uuid: session.session_uuid, ...errorFields(err) });
    });
    await saveState(cfg.statePath, state);
  }
  await saveState(cfg.statePath, state);
  return { sessions: rows.length, uploaded, chunks };
}

if (import.meta.main) {
  process.env.OTEL_SERVICE_NAME ||= "alleyoop-courier";
  initTelemetry();
  const cfg = configFromEnv();
  const once = flag("COURIER_ONCE", false);
  const interval = integer("COURIER_INTERVAL_MS", 10_000);
  for (;;) {
    try {
      const r = await runCourierOnce(cfg);
      log.info("courier pass complete", { count: r.sessions, enqueued: r.uploaded, chunk_count: r.chunks });
    } catch (err) {
      log.error("courier pass failed", errorFields(err));
      if (process.env.DEBUG === "1") console.error(err);
    }
    await flushTelemetry();
    if (once) process.exit(0);
    await new Promise((r) => setTimeout(r, interval));
  }
}
