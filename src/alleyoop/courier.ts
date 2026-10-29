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
  manifestKey,
  validateManifest,
  type ChunkEntry,
  type ManifestV1,
  type SessionLocation,
} from "../shared/vendor.ts";
import { flag, integer, optional, required } from "../shared/env.ts";
import { mintToken } from "./auth.ts";
import { hexTraceId, isUuidV7, spanId, uuidv7 } from "../shared/ids.ts";
import { errorFields, log } from "../shared/log.ts";
import { flushTelemetry, initTelemetry, withSpan } from "../shared/telemetry.ts";
import { EXT } from "../shared/enrich.ts";
import {
  fixedBearerToken,
  mintedBearerToken,
  RelayAuthAssertionProvider,
  secretCommand,
  type BearerTokenProvider,
} from "./relay-auth-token.ts";

export const PLUGIN_VERSION = "96bulls-courier/0.2.0";
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

export function sessionsForStartMode<T extends Pick<SessionRow, "first_activity_ms">>(
  rows: T[],
  startMode: "all" | "new",
  enrolledAt?: string,
): T[] {
  if (startMode === "all" || !enrolledAt) return rows;
  const enrolledAtMs = Date.parse(enrolledAt);
  return Number.isFinite(enrolledAtMs)
    ? rows.filter((row) => row.first_activity_ms !== null && row.first_activity_ms >= enrolledAtMs)
    : rows;
}

export type ManifestReconciliation = "verified" | "reset" | "defer";

export function manifestReconciliation(status?: number): ManifestReconciliation {
  if (status === 204) return "verified";
  if (status === 404) return "reset";
  return "defer";
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
  /** Chunks were sealed but the manifest has not landed yet: retry even with no new bytes. */
  pending?: boolean;
}

interface CourierState {
  machine_id: string;
  sessions: Record<string, SessionState>;
  /** When set, sessions that already existed before host enrollment stay local. */
  enrolled_at?: string;
}

export interface CourierConfig {
  dbPath: string;
  statePath: string;
  ingestUrl: string;
  /** Fixed/local bearer auth or a JIT RelayAuth assertion provider. */
  token: BearerTokenProvider;
  orgId: string;
  workspaceId: string;
  userId: string;
  machineId?: string;
  harnesses?: string[];
  closeAfterMinutes: number;
  startMode: "all" | "new";
  /** Process-local startup checks: persisted upload state is reconciled once, not every pass. */
  verifiedManifests?: Set<string>;
}

export function configFromEnv(): CourierConfig {
  const dbPath = required("CONCOURSE_DB");
  const orgId = required("TENANT_ORG_ID");
  const workspaceId = optional("WORKSPACE_ID", "default");
  const userId = optional("USER_ID", "unknown");
  let token: BearerTokenProvider;
  if (process.env.RELAYAUTH_URL) {
    token = new RelayAuthAssertionProvider({
      baseUrl: required("RELAYAUTH_URL"),
      historyOrigin: required("RELAYAUTH_HISTORY_ORIGIN"),
      orgId,
      workspaceId,
      sponsorId: userId,
      apiKey: secretCommand(required("RELAYAUTH_API_KEY_COMMAND_JSON")),
      expiresInSeconds: integer("RELAYAUTH_ASSERTION_TTL_SECONDS", 60),
      refreshSkewSeconds: integer("RELAYAUTH_ASSERTION_REFRESH_SKEW_SECONDS", 15),
    });
  } else if (process.env.JWT_PRIVATE_KEY_PEM) {
    token = mintedBearerToken(() =>
      mintToken({ kid: required("JWT_KID"), privateKeyPem: required("JWT_PRIVATE_KEY_PEM") }, {
        issuer: optional("AUTH_ISSUER", "https://96bulls.local"),
        audience: optional("AUTH_AUDIENCE", "relayhistory"),
        orgId,
        workspaceId,
        userId,
        ttlSeconds: 3600,
      }));
  } else {
    token = fixedBearerToken(required("ALLEYOOP_TOKEN"));
  }
  const startMode = optional("COURIER_START_MODE", "all");
  if (startMode !== "all" && startMode !== "new") throw new Error("96bulls: COURIER_START_MODE must be all or new");
  return {
    dbPath,
    statePath: optional("COURIER_STATE", join(dirname(dbPath), ".alleyoop-courier.json")),
    ingestUrl: required("INGEST_URL").replace(/\/$/, ""),
    token,
    orgId,
    workspaceId,
    userId,
    ...(process.env.MACHINE_ID ? { machineId: process.env.MACHINE_ID } : {}),
    ...(process.env.HARNESSES ? { harnesses: process.env.HARNESSES.split(",").map((s) => s.trim()).filter(Boolean) } : {}),
    closeAfterMinutes: integer("COURIER_CLOSE_AFTER_MINUTES", 30),
    startMode,
  };
}

async function loadState(path: string, machineId: string | undefined, startMode: "all" | "new"): Promise<CourierState> {
  try {
    const s = JSON.parse(await readFile(path, "utf8")) as CourierState;
    if (machineId && s.machine_id !== machineId) log.warn("machine id differs from state; keeping state's", {});
    return s;
  } catch {
    return {
      machine_id: machineId ?? `m-${createHash("sha256").update(uuidv7()).digest("hex").slice(0, 16)}`,
      sessions: {},
      ...(startMode === "new" ? { enrolled_at: new Date().toISOString() } : {}),
    };
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
  const state = await loadState(cfg.statePath, cfg.machineId, cfg.startMode);
  const discovered = await readSessions(cfg.dbPath, cfg.harnesses);
  const rows = sessionsForStartMode(discovered, cfg.startMode, state.enrolled_at);
  const verified = (cfg.verifiedManifests ??= new Set());
  let uploaded = 0;
  let chunks = 0;
  const deferred = new Set<string>();
  // The ledger follows the Locker, not the other way round: if a recorded manifest is
  // gone (a fresh stack after `down`), the upload state resets while the ids stay.
  for (const [key, st] of Object.entries(state.sessions)) {
    if (!st.manifest_key || st.revision === 0) continue;
    if (verified.has(st.manifest_key)) continue;
    const q = new URLSearchParams({ key: st.manifest_key });
    const head = await authorizedFetch(cfg.token, `${cfg.ingestUrl}/v1/pipeline/manifests/status?${q}`, { method: "GET" }).catch(() => undefined);
    const reconciliation = manifestReconciliation(head?.status);
    if (reconciliation === "verified") {
      verified.add(st.manifest_key);
    } else if (reconciliation === "reset") {
      log.info("manifest missing in locker; re-uploading with the same ids", { session_uuid: st.session_uuid, trace_id: st.trace_id });
      state.sessions[key] = { ...st, revision: 0, uploaded_offset: 0, chunks: [], closed: false, pending: false, scrub: undefined, manifest_key: undefined };
    } else {
      log.warn("manifest status unavailable; keeping durable upload state", { session_uuid: st.session_uuid, trace_id: st.trace_id, status: head?.status });
      deferred.add(key);
    }
  }
  for (const row of rows) {
    if (!row.raw_path || !row.first_activity_ms) continue;
    const key = `${row.source}:${row.session_id}`;
    if (deferred.has(key)) continue;
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
    if (nothingNew && !session.pending) {
      if (session.revision === 0) continue;
      if (session.closed || !shouldClose) continue;
    }
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
        const res = await authorizedFetch(cfg.token, `${cfg.ingestUrl}/v1/pipeline/chunks?${q}`, {
          method: "POST",
          headers: {
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
        session.pending = true; // sealed chunks exist that no landed manifest lists yet
        chunks += 1;
        log.info("chunk sealed", {
          stage: "deliver.put", seq, offset: range.start, bytes_raw: sealed.entry.bytes, bytes_stored: sealed.entry.stored_bytes,
          scrub_ruleset: sealed.scrub.ruleset, scrub_version: sealed.scrub.version, trace_id: session.trace_id, session_uuid: session.session_uuid,
        });
      }
      // The revision and closed flag advance only once the manifest has landed (below).
      const revision = session.revision + 1;
      const last = session.chunks.at(-1)!;
      const nowIso = new Date().toISOString();
      const manifest: ManifestV1 = {
        schema_v: 1,
        layout: "v1",
        trace_id: session.trace_id,
        revision,
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
      session.manifest_key = mKey;
      const landed = await authorizedFetch(cfg.token, `${cfg.ingestUrl}/v1/pipeline/manifests`, {
        method: "POST",
        headers: { "content-type": "application/json", "x-relay-trace-id": session.trace_id, traceparent },
        body: JSON.stringify({ key: mKey, manifest }),
      });
      if (landed.status !== 202) {
        const b = (await landed.json().catch(() => ({}))) as { error?: { code?: string } };
        throw Object.assign(new Error("manifest not accepted"), { code: b.error?.code ?? `http_${landed.status}`, retryable: landed.status >= 500 });
      }
      session.revision = revision;
      session.closed = shouldClose;
      session.pending = false;
      verified.add(mKey);
      uploaded += 1;
      log.info("manifest landed", {
        stage: "object-landed", revision, chunk_count: session.chunks.length, bytes_raw: manifest.raw.raw_bytes,
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

async function authorizedFetch(provider: BearerTokenProvider, input: string, init: RequestInit): Promise<Response> {
  const send = async () => {
    const headers = new Headers(init.headers);
    headers.set("authorization", `Bearer ${await provider.get()}`);
    return fetch(input, { ...init, headers });
  };
  const response = await send();
  if (response.status !== 401 || !provider.invalidate()) return response;
  return send();
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
