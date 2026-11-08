/**
 * Bounded History-side adapter for the dormant canonical replay endpoint.
 * It is an explicit provider call, never a fallback from/to the public CH route.
 */
import {
  CONTENT_TRUNCATION_NOTE,
  type ShapedEvent,
} from "./page.ts";

const HEX = /^[a-f0-9]{64}$/u;
const SOURCE = /^[a-z0-9][a-z0-9._-]{0,63}$/u;
const MAX_HTTP_BYTES = 64 * 1024 * 1024;
const encoder = new TextEncoder();
const safeId = (value: unknown): value is string =>
  typeof value === "string" &&
  value.length > 0 &&
  value.length <= 255 &&
  !/[\x00-\x1f/\\]/u.test(value);
const object = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);

export class CanonicalHistoryAdapterError extends Error {
  constructor(
    readonly reason: string,
    readonly retryable = false,
  ) {
    super("Canonical History adapter refused the read");
    this.name = "CanonicalHistoryAdapterError";
  }
}
const fail = (reason: string, retryable = false): never => {
  throw new CanonicalHistoryAdapterError(reason, retryable);
};

export interface CanonicalHistoryScope {
  source: string;
  sessionId: string;
}
export interface CanonicalHistoryAdapterConfig {
  enabled: true;
  endpointOrigin: string;
  orgId: string;
  workspaceId: string;
  /** Exact canary coverage. No wildcard or empty session is accepted. */
  scopes: readonly CanonicalHistoryScope[];
  deadlineMs: number;
  maxResponseBytes: number;
  maxRowsPerPage: number;
}
export type CanonicalHistoryAdapterConfigResult =
  | { enabled: false }
  | CanonicalHistoryAdapterConfig;

export function canonicalHistoryAdapterConfig(
  env: NodeJS.ProcessEnv,
): CanonicalHistoryAdapterConfigResult {
  const enabled = env.HISTORY_CANONICAL_CANARY_ENABLED ?? "false";
  if (enabled === "false") return { enabled: false };
  if (enabled !== "true") fail("canonical_history_config");
  let url: URL;
  let scopes: unknown;
  try {
    url = new URL(env.HISTORY_CANONICAL_CANARY_ORIGIN!);
    scopes = JSON.parse(env.HISTORY_CANONICAL_CANARY_SCOPES_JSON!);
  } catch {
    return fail("canonical_history_config");
  }
  const orgId = env.HISTORY_CANONICAL_CANARY_ORG_ID;
  const workspaceId = env.HISTORY_CANONICAL_CANARY_WORKSPACE_ID;
  const deadlineMs = Number(env.HISTORY_CANONICAL_CANARY_DEADLINE_MS ?? "7500");
  const maxResponseBytes = Number(
    env.HISTORY_CANONICAL_CANARY_MAX_RESPONSE_BYTES ?? String(2 * 1024 * 1024),
  );
  const maxRowsPerPage = Number(
    env.HISTORY_CANONICAL_CANARY_MAX_ROWS ?? "200",
  );
  if (
    url.protocol !== "https:" ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    !["", "/"].includes(url.pathname) ||
    !safeId(orgId) ||
    !safeId(workspaceId) ||
    !Array.isArray(scopes) ||
    scopes.length < 1 ||
    scopes.length > 32 ||
    !Number.isSafeInteger(deadlineMs) ||
    deadlineMs < 100 ||
    deadlineMs > 8000 ||
    !Number.isSafeInteger(maxResponseBytes) ||
    maxResponseBytes < 1024 ||
    maxResponseBytes > MAX_HTTP_BYTES ||
    !Number.isSafeInteger(maxRowsPerPage) ||
    maxRowsPerPage < 1 ||
    maxRowsPerPage > 1000
  )
    fail("canonical_history_config");
  if (!Array.isArray(scopes)) fail("canonical_history_config");
  const parsedScopes = scopes as unknown[];
  const normalized: CanonicalHistoryScope[] = [];
  const seen = new Set<string>();
  for (const item of parsedScopes) {
    if (!object(item)) fail("canonical_history_config");
    const scope = item as Record<string, unknown>;
    if (
      typeof scope.source !== "string" ||
      !SOURCE.test(scope.source) ||
      !safeId(scope.sessionId) ||
      Object.keys(scope).some((key) => !["source", "sessionId"].includes(key))
    )
      fail("canonical_history_config");
    const key = `${scope.source}\u0000${scope.sessionId}`;
    if (seen.has(key)) fail("canonical_history_config");
    seen.add(key);
    normalized.push({
      source: scope.source as string,
      sessionId: scope.sessionId as string,
    });
  }
  return {
    enabled: true,
    endpointOrigin: url.origin,
    orgId: orgId as string,
    workspaceId: workspaceId as string,
    scopes: normalized,
    deadlineMs,
    maxResponseBytes,
    maxRowsPerPage,
  };
}

export interface CanonicalHistoryPageInput {
  orgId: string;
  workspaceId: string;
  source: string;
  sessionId: string;
  bearer: string;
  limit?: number;
  order?: "asc" | "desc";
  cursor?: string | null;
  generation?: string | null;
  maxContent?: number;
  signal?: AbortSignal;
}
export interface CanonicalHistoryPage {
  orgId: string;
  workspaceId: string;
  source: string;
  sessionId: string;
  generation: string;
  exists: boolean;
  events: ShapedEvent[];
  nextCursor: string | null;
}

async function boundedBody(
  response: Response,
  maxBytes: number,
  signal: AbortSignal,
): Promise<unknown> {
  const bodyStream = response.body;
  if (!bodyStream) fail("canonical_history_response", true);
  const reader = (bodyStream as ReadableStream<Uint8Array>).getReader();
  let bytes = 0;
  const chunks: Uint8Array[] = [];
  let rejectAbort: ((reason?: unknown) => void) | undefined;
  const onAbort = () => rejectAbort?.(signal.reason);
  const aborted = new Promise<never>((_resolve, reject) => {
    rejectAbort = reject;
    if (signal.aborted) onAbort();
    else signal.addEventListener("abort", onAbort, { once: true });
  });
  try {
    for (;;) {
      signal.throwIfAborted();
      const part = await Promise.race([reader.read(), aborted]);
      if (part.done) break;
      bytes += part.value.byteLength;
      if (bytes > maxBytes) {
        void reader.cancel().catch(() => {});
        fail("canonical_history_response_oversize");
      }
      chunks.push(part.value);
    }
    const body = new Uint8Array(bytes);
    let offset = 0;
    for (const chunk of chunks) {
      body.set(chunk, offset);
      offset += chunk.byteLength;
    }
    try {
      return JSON.parse(
        new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(body),
      );
    } catch {
      return fail("canonical_history_response");
    }
  } finally {
    signal.removeEventListener("abort", onAbort);
    void reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

const nullableString = (value: unknown): string | null =>
  value === null || value === undefined || value === ""
    ? null
    : typeof value === "string"
      ? value
      : fail("canonical_history_row");
const stringArray = (value: unknown): string[] => {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string"))
    fail("canonical_history_row");
  return value as string[];
};
function shaped(
  value: unknown,
  input: CanonicalHistoryPageInput,
): ShapedEvent {
  if (!object(value)) fail("canonical_history_row");
  const row = value as Record<string, unknown>;
  const required = ["event_id", "session_id", "source", "kind", "machine_id", "ts"];
  if (required.some((key) => typeof row[key] !== "string"))
    fail("canonical_history_row");
  if (
    (row.org_id !== undefined && row.org_id !== input.orgId) ||
    (row.workspace_id !== undefined && row.workspace_id !== input.workspaceId) ||
    row.source !== input.source ||
    row.session_id !== input.sessionId
  )
    fail("canonical_history_tenant_mismatch");
  const date = new Date(row.ts as string);
  if (!Number.isFinite(date.getTime())) fail("canonical_history_row");
  const taskRef = row.task_ref ?? {};
  if (!object(taskRef)) fail("canonical_history_row");
  let content = nullableString(row.content);
  let contentTruncated = false;
  if (
    input.maxContent !== undefined &&
    content !== null &&
    content.length > input.maxContent
  ) {
    content = `${content.slice(0, input.maxContent)}${CONTENT_TRUNCATION_NOTE}`;
    contentTruncated = true;
  }
  return {
    eventId: row.event_id as string,
    userId: nullableString(row.user_id),
    sessionId: row.session_id as string,
    source: row.source as string,
    kind: row.kind as string,
    type: nullableString(row.type) ?? (row.kind as string),
    ts: date.toISOString(),
    actorName: nullableString(row.actor_name),
    actorRole: nullableString(row.actor_role),
    subagentId: nullableString(row.subagent_id),
    machineId: row.machine_id as string,
    projectId: nullableString(row.project_id),
    trajectoryId: null,
    taskRef: taskRef as Record<string, unknown>,
    taskTitle: nullableString(row.task_title),
    content,
    contentTruncated,
    tags: stringArray(row.tags ?? []),
    toolName: nullableString(row.tool_name),
    toolStatus: nullableString(row.tool_status),
    filesTouched: stringArray(row.files_touched ?? []),
  };
}

export async function readCanonicalHistoryPage(
  config: CanonicalHistoryAdapterConfig,
  input: CanonicalHistoryPageInput,
  fetcher: typeof fetch = fetch,
): Promise<CanonicalHistoryPage> {
  const limit = input.limit ?? config.maxRowsPerPage;
  if (
    input.orgId !== config.orgId ||
    input.workspaceId !== config.workspaceId ||
    !config.scopes.some(
      (scope) =>
        scope.source === input.source && scope.sessionId === input.sessionId,
    )
  )
    fail("canonical_history_outside_coverage");
  if (
    typeof input.bearer !== "string" ||
    input.bearer.length < 16 ||
    input.bearer.length > 8192 ||
    /\s/u.test(input.bearer) ||
    !Number.isSafeInteger(limit) ||
    limit < 1 ||
    limit > config.maxRowsPerPage ||
    (input.order !== undefined && !["asc", "desc"].includes(input.order)) ||
    (input.cursor != null &&
      (input.cursor.length > 4096 || !input.cursor.startsWith("artifact-v1:"))) ||
    (input.generation != null && !HEX.test(input.generation)) ||
    (input.maxContent !== undefined &&
      (!Number.isSafeInteger(input.maxContent) ||
        input.maxContent < 1 ||
        input.maxContent > config.maxResponseBytes))
  )
    fail("canonical_history_parameter");
  const controller = new AbortController();
  const timer = setTimeout(
    () => controller.abort(new Error("canonical_history_deadline")),
    config.deadlineMs,
  );
  const signal = AbortSignal.any([
    controller.signal,
    ...(input.signal ? [input.signal] : []),
  ]);
  try {
    const url = new URL(
      `/v1/canonical/sessions/${encodeURIComponent(input.sessionId)}`,
      config.endpointOrigin,
    );
    url.searchParams.set("workspace", input.workspaceId);
    url.searchParams.set("source", input.source);
    url.searchParams.set("table", "convergence_events");
    url.searchParams.set("view", "replay");
    url.searchParams.set("limit", String(limit));
    url.searchParams.set("order", input.order ?? "asc");
    if (input.cursor) url.searchParams.set("cursor", input.cursor);
    if (input.generation) url.searchParams.set("generation", input.generation);
    if (input.maxContent !== undefined)
      url.searchParams.set("maxContent", String(input.maxContent));
    let response: Response;
    try {
      response = await fetcher(url, {
        method: "GET",
        redirect: "error",
        headers: { authorization: `Bearer ${input.bearer}` },
        signal,
      });
    } catch (error) {
      if (signal.aborted)
        return fail(
          controller.signal.aborted
            ? "canonical_history_deadline"
            : "canonical_history_aborted",
          controller.signal.aborted,
        );
      return fail("canonical_history_transport", true);
    }
    if (!response.ok) {
      void response.body?.cancel().catch(() => {});
      return fail(
        response.status === 400 || response.status === 401 || response.status === 403
          ? "canonical_history_rejected"
          : "canonical_history_unavailable",
        response.status >= 500 ||
          response.status === 408 ||
          response.status === 429,
      );
    }
    if (
      response.headers.get("x-relayhistory-workspace-id") !== input.workspaceId
    ) {
      void response.body?.cancel().catch(() => {});
      return fail("canonical_history_attestation");
    }
    let value: unknown;
    try {
      value = await boundedBody(response, config.maxResponseBytes, signal);
    } catch (error) {
      if (signal.aborted)
        return fail(
          controller.signal.aborted
            ? "canonical_history_deadline"
            : "canonical_history_aborted",
          controller.signal.aborted,
        );
      throw error;
    }
    if (!object(value)) fail("canonical_history_response");
    const body = value as Record<string, unknown>;
    if (
      body.orgId !== input.orgId ||
      body.workspaceId !== input.workspaceId ||
      body.source !== input.source ||
      body.sessionId !== input.sessionId ||
      !HEX.test(String(body.generation)) ||
      typeof body.exists !== "boolean" ||
      !Array.isArray(body.rows) ||
      body.rows.length > limit ||
      (body.nextCursor !== null &&
        (typeof body.nextCursor !== "string" ||
          body.nextCursor.length > 4096 ||
          !body.nextCursor.startsWith("artifact-v1:"))) ||
      (input.generation != null && body.generation !== input.generation)
    )
      fail("canonical_history_response");
    const rows = body.rows as unknown[];
    if (!body.exists && (rows.length !== 0 || body.nextCursor !== null))
      fail("canonical_history_response");
    const events = rows.map((item: unknown) => {
      if (!object(item) || item.table !== "convergence_events")
        fail("canonical_history_row");
      return shaped((item as Record<string, unknown>).row, input);
    });
    // Account for the mapped response too: a small wire response must not
    // inflate into an unbounded History object graph.
    if (encoder.encode(JSON.stringify(events)).byteLength > config.maxResponseBytes)
      fail("canonical_history_mapped_oversize");
    return {
      orgId: input.orgId,
      workspaceId: input.workspaceId,
      source: input.source,
      sessionId: input.sessionId,
      generation: body.generation as string,
      exists: body.exists as boolean,
      events,
      nextCursor: body.nextCursor as string | null,
    };
  } finally {
    clearTimeout(timer);
    controller.abort();
  }
}
