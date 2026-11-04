import { createHash } from "node:crypto";
import {
  canonicalSnapshot,
  canonicalGeneration,
  type CanonicalDeps,
  type CanonicalIdentity,
} from "../session-read-api/canonical.ts";
import { scrubText } from "../../packages/pipeline/src/lib/scrub.ts";
import {
  prepareAssessment,
  type SessionExport,
  type ExportEvent,
  type ExportSession,
} from "../../packages/coordination-eval/src/intake.ts";

export const REPORT_SOURCE_VERSION = "canonical-coordination-source/0.1.0";
type Scope = SessionExport["scope"];
export interface ReportSourceRequest {
  scope: Scope;
  window: SessionExport["window"];
  maxSessions: number;
  maxEvents: number;
  excerptChars?: number;
}
export interface ReportDirectorySnapshot {
  /** Must cover membership, deletions, projection readiness and scope/window selection. */
  generation: string;
  complete: boolean;
  sessions: CanonicalIdentity[];
}
export interface ReportSourceDeps {
  /** Established by job configuration/authorization, never copied from request input. */
  trustedScope: Scope;
  /** Select by event-time intersection, including sessions subsequently active outside the window.
   * Must enforce tenant predicates, tombstones and projection readiness; return a sentinel
   * or complete=false on caps. No transcript content belongs in this inventory. */
  listSessions(request: ReportSourceRequest): Promise<ReportDirectorySnapshot>;
  canonical: CanonicalDeps;
  now(): string;
  signal?: AbortSignal;
}
export const MAX_REPORT_EXPORT_BYTES = 8 * 1024 * 1024;
export const MAX_REPORT_SCAN_BYTES = 128 * 1024 * 1024;

/** Reject promptly at the job deadline. The underlying read-only request can finish
 * in flight; late stream responses are cancelled and cannot dispatch another read. */
function abortable<T>(
  operation: () => Promise<T>,
  signal?: AbortSignal,
  late?: (value: T) => void,
): Promise<T> {
  signal?.throwIfAborted();
  if (!signal) return operation();
  return new Promise<T>((resolve, reject) => {
    let aborted = false;
    const stop = () => {
      aborted = true;
      signal.removeEventListener("abort", stop);
      reject(signal.reason);
    };
    signal.addEventListener("abort", stop, { once: true });
    Promise.resolve()
      .then(() => {
        signal.throwIfAborted();
        return operation();
      })
      .then(
        (value) => {
          signal.removeEventListener("abort", stop);
          if (aborted) late?.(value);
          else resolve(value);
        },
        (error) => {
          signal.removeEventListener("abort", stop);
          reject(error);
        },
      );
  });
}

/** One shared scan budget per report, charged before decoded bytes enter projection.
 * No object-store mutation, listing or presigned read capability is exposed. */
export function boundedCanonicalReads(
  canonical: CanonicalDeps,
  signal?: AbortSignal,
  maxScanBytes = MAX_REPORT_SCAN_BYTES,
): CanonicalDeps {
  let storedBytes = 0,
    decodedBytes = 0;
  const body = (stream: ReadableStream<Uint8Array>) => {
    if (signal?.aborted) {
      void stream.cancel(signal.reason).catch(() => {});
      signal.throwIfAborted();
    }
    const reader = stream.getReader();
    let ended = false;
    let stop: () => void;
    const finish = () => {
      ended = true;
      signal?.removeEventListener("abort", stop);
    };
    return new ReadableStream<Uint8Array>({
      start(controller) {
        stop = () => {
          if (ended) return;
          finish();
          void reader.cancel(signal?.reason).catch(() => {});
          controller.error(signal?.reason);
        };
        signal?.addEventListener("abort", stop, { once: true });
        if (signal?.aborted) stop();
      },
      async pull(controller) {
        if (ended) return;
        try {
          const next = await reader.read();
          if (ended) return;
          if (next.done) {
            finish();
            controller.close();
            return;
          }
          storedBytes += next.value.byteLength;
          if (storedBytes > maxScanBytes)
            throw new Error("coordination_source_scan_budget_exceeded");
          controller.enqueue(next.value);
        } catch (error) {
          if (!ended) {
            finish();
            void reader.cancel(error).catch(() => {});
            controller.error(error);
          }
        }
      },
      async cancel(reason) {
        finish();
        await reader.cancel(reason);
      },
    });
  };
  const cancel = (stream: ReadableStream<Uint8Array>) => {
    void stream.cancel(signal?.reason).catch(() => {});
  };
  const denied = async (): Promise<never> => {
    throw new Error("coordination_source_store_operation_denied");
  };
  return {
    ...canonical,
    pointers: (identity) =>
      abortable(() => canonical.pointers(identity), signal),
    decompress(bytes, maxBytes) {
      signal?.throwIfAborted();
      const decoded = canonical.decompress(
        bytes,
        Math.min(maxBytes, maxScanBytes - decodedBytes),
      );
      decodedBytes += decoded.byteLength;
      if (decodedBytes > maxScanBytes) fail("scan_budget_exceeded");
      signal?.throwIfAborted();
      return decoded;
    },
    store: {
      backend: canonical.store.backend,
      head: (key) => abortable(() => canonical.store.head(key), signal),
      get: async (key, range) =>
        body(
          await abortable(
            () => canonical.store.get(key, range),
            signal,
            cancel,
          ),
        ),
      getObject: async (key, range) => {
        const object = await abortable(
          () => canonical.store.getObject(key, range),
          signal,
          (value) => cancel(value.body),
        );
        return { ...object, body: body(object.body) };
      },
      list: denied,
      presignGet: denied,
      put: denied,
      delete: denied,
    },
  };
}

const hash = (v: unknown) =>
  createHash("sha256").update(JSON.stringify(v)).digest("hex");
const key = (s: CanonicalIdentity) => `session-${hash(s)}`;
const fail = (reason: string): never => {
  throw new Error(`coordination_source_${reason}`);
};
function timestamp(v: unknown): string {
  if (typeof v !== "string" || !Number.isFinite(Date.parse(v)))
    return fail("timestamp_invalid");
  return new Date(v).toISOString();
}
function safeIdentity(v: string): string {
  if (!/^[a-zA-Z0-9][a-zA-Z0-9_.:/@-]{0,255}$/.test(v) || scrubText(v) !== v)
    fail("identity_invalid");
  return v;
}
const clean = (v: string) =>
  scrubText(v).replace(
    /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g,
    "\ufffd",
  );
const meta = (v: unknown) =>
  typeof v === "string" && v.trim()
    ? clean(v).replace(/\s+/g, " ").trim().slice(0, 1024)
    : null;
function refs(v: unknown): string[] {
  return Array.isArray(v)
    ? [...new Set(v.map(meta).filter((x): x is string => x !== null))].sort()
    : [];
}
function inventory(s: ReportDirectorySnapshot, r: ReportSourceRequest): string {
  if (!s.complete || !s.generation || s.sessions.length > r.maxSessions)
    fail("inventory_incomplete");
  const seen = new Set<string>();
  for (const i of s.sessions) {
    if (
      i.orgId !== r.scope.organizationId ||
      i.workspaceId !== r.scope.workspaceId
    )
      fail("tenant_mismatch");
    Object.values(i).forEach(safeIdentity);
    if (seen.has(key(i))) fail("duplicate_session");
    seen.add(key(i));
  }
  return hash([s.generation, s.sessions.map(key).sort()]);
}
/** Trusted historical job adapter. Caps and concurrent source changes fail closed. */
export async function buildCanonicalReportExport(
  deps: ReportSourceDeps,
  request: ReportSourceRequest,
) {
  if (
    request.scope.organizationId !== deps.trustedScope.organizationId ||
    request.scope.workspaceId !== deps.trustedScope.workspaceId
  )
    fail("scope_denied");
  safeIdentity(request.scope.organizationId);
  safeIdentity(request.scope.workspaceId);
  if (
    timestamp(request.window.since) !== request.window.since ||
    timestamp(request.window.until) !== request.window.until ||
    request.window.since >= request.window.until
  )
    fail("window_invalid");
  for (const n of [request.maxSessions, request.maxEvents])
    if (!Number.isInteger(n) || n < 1 || n > 10000) fail("budget_invalid");
  const excerpt = request.excerptChars ?? 12000;
  if (!Number.isInteger(excerpt) || excerpt < 100 || excerpt > 12000)
    fail("budget_invalid");
  const exportedAt = timestamp(deps.now());
  if (exportedAt < request.window.until) fail("future_window");
  deps.signal?.throwIfAborted();
  const directory = await abortable(
      () => deps.listSessions(request),
      deps.signal,
    ),
    generation = inventory(directory, request);
  deps.signal?.throwIfAborted();
  const canonical = boundedCanonicalReads(deps.canonical, deps.signal);
  const snapshots: Array<{ identity: CanonicalIdentity; generation: string }> =
    [];
  const events: ExportEvent[] = [];
  const sessions: ExportSession[] = [];
  const relationships: Array<{
    sessionKey: string;
    metadata: Record<string, unknown>;
  }> = [];
  const eventMetadata: Array<{
    evidenceId: string;
    actorRole: string | null;
    subagentId: string | null;
    projectId: string | null;
    ingestedAt: string | null;
    taskRef: unknown;
  }> = [];
  let exportBytes = 0;
  const charge = (value: unknown) => {
    exportBytes += Buffer.byteLength(JSON.stringify(value));
    if (exportBytes > MAX_REPORT_EXPORT_BYTES) fail("export_budget_exceeded");
  };
  for (const identity of directory.sessions) {
    deps.signal?.throwIfAborted();
    const snapshot = await canonicalSnapshot(canonical, identity);
    deps.signal?.throwIfAborted();
    if (!snapshot.rows.length) fail("session_disappeared");
    const revision = { identity, generation: snapshot.generation };
    charge(revision);
    snapshots.push(revision);
    const sessionKey = key(identity);
    let title: string | null = null;
    const projects = new Set<string>();
    const eventProjects = new Set<string>();
    const tasks = new Set<string>();
    // Project and discard one full canonical session at a time. Only normalized,
    // byte-bounded output and its generation survive into the next session.
    for (const { table, row } of snapshot.rows) {
      deps.signal?.throwIfAborted();
      if (table === "session_catalog") {
        title ??= meta(row.title);
        const project = meta(row.project_key);
        if (project) projects.add(project);
      }
      if (table === "session_relationships") {
        const relation = {
          sessionKey,
          metadata: JSON.parse(clean(JSON.stringify(row))) as Record<
            string,
            unknown
          >,
        };
        charge(relation);
        relationships.push(relation);
      }
      if (table !== "convergence_events") continue;
      const occurredAt = timestamp(row.ts);
      if (
        occurredAt < request.window.since ||
        occurredAt >= request.window.until
      )
        continue;
      const task =
        row.task_ref && typeof row.task_ref === "object"
          ? meta((row.task_ref as Record<string, unknown>).id)
          : null;
      if (task) tasks.add(task);
      const eventId = safeIdentity(String(row.event_id ?? "")),
        machineId = safeIdentity(String(row.machine_id ?? ""));
      const kind = safeIdentity(String(row.kind ?? "event"));
      const actorRole =
        meta(row.actor_role) ??
        (["user", "assistant", "system", "developer", "tool"].includes(
          String(row.type),
        )
          ? String(row.type)
          : null);
      const roleLabel = actorRole
        ? `[Recorded role: ${actorRole}${actorRole === "user" ? "; human or automated authorship not established" : ""}]\n`
        : "";
      const content = clean(
        typeof row.content === "string" ? row.content : `[${kind}]`,
      );
      const projectId = meta(row.project_id);
      if (projectId) eventProjects.add(projectId);
      if (events.length === request.maxEvents) fail("event_budget_exceeded");
      const event: ExportEvent = {
        id: `event-${hash([sessionKey, machineId, kind, eventId])}`,
        sessionKey,
        machineId,
        eventId,
        kind,
        occurredAt,
        availableAt: null,
        text: roleLabel + (content.slice(0, excerpt) || `[${kind}]`),
        truncated:
          row.payload_truncated === true ||
          row.payload_truncated === 1 ||
          content.length > excerpt,
        filesTouched: refs(row.files_touched),
      };
      charge(event);
      events.push(event);
      const metadata = {
        evidenceId: event.id,
        actorRole,
        subagentId: meta(row.subagent_id),
        projectId,
        ingestedAt: row.ingested_at == null ? null : timestamp(row.ingested_at),
        taskRef:
          row.task_ref == null
            ? null
            : (JSON.parse(clean(JSON.stringify(row.task_ref))) as unknown),
      };
      charge(metadata);
      eventMetadata.push(metadata);
    }
    const session: ExportSession = {
      key: sessionKey,
      organizationId: identity.orgId,
      workspaceId: identity.workspaceId,
      source: identity.source,
      sessionId: identity.sessionId,
      title: title ?? identity.sessionId,
      projectKey:
        projects.size === 1
          ? [...projects][0]!
          : projects.size === 0 && eventProjects.size === 1
            ? [...eventProjects][0]!
            : null,
      taskRefs: [...tasks].sort(),
      parentSessionKey: null,
    };
    charge(session);
    sessions.push(session);
  }
  // Only explicit, unambiguous same-source parent relations can establish roots.
  for (const session of sessions) {
    const parents = relationships
      .filter(
        (r) =>
          r.metadata.child_session_id === session.sessionId &&
          r.metadata.relationship === "subagent",
      )
      .map(
        (r) =>
          sessions.find(
            (p) => p.key === r.sessionKey && p.source === session.source,
          )?.key,
      )
      .filter((p): p is string => !!p);
    const unique = [...new Set(parents)];
    if (unique.length === 1 && unique[0] !== session.key)
      session.parentSessionKey = unique[0]!;
  }
  deps.signal?.throwIfAborted();
  if (
    inventory(
      await abortable(() => deps.listSessions(request), deps.signal),
      request,
    ) !== generation
  )
    fail("directory_changed");
  // Recheck all pointers after the entire read, so erasure during another session's read cannot leak earlier content.
  for (const snapshot of snapshots) {
    deps.signal?.throwIfAborted();
    if (
      (await canonicalGeneration(canonical, snapshot.identity)) !==
      snapshot.generation
    )
      fail("source_changed");
  }
  deps.signal?.throwIfAborted();
  const exported: SessionExport = {
    schemaVersion: 1,
    synthetic: false,
    scope: request.scope,
    window: request.window,
    coverage: {
      status: "complete",
      notes:
        "Complete bounded canonical event-time inventory for this scope and window. Relationship metadata may be absent; missing relationships do not prove independence. Recorded user roles may be human or automated; authorship is not established. Historical authorized read availability is unknown; availableAt is null. Recorded source ingestion/receipt time is retained only as provenance and does not prove cross-agent visibility or first pipeline availability.",
    },
    provenance: { source: REPORT_SOURCE_VERSION, exportedAt, redacted: true },
    sessions,
    events: events.sort(
      (a, b) =>
        a.occurredAt.localeCompare(b.occurredAt) || a.id.localeCompare(b.id),
    ),
  };
  if (Buffer.byteLength(JSON.stringify(exported)) > MAX_REPORT_EXPORT_BYTES)
    fail("export_budget_exceeded");
  prepareAssessment(exported);
  return {
    export: exported,
    sourceSnapshot: {
      directoryGeneration: directory.generation,
      sessions: snapshots.map((s) => ({
        identity: s.identity,
        generation: s.generation,
      })),
      relationships,
      eventMetadata,
    },
  };
}
