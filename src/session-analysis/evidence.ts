/**
 * The full analysis's evidence, from canonical S3 instead of Neon.
 *
 * Port of relayhistory-cloud `packages/relayhistory/src/lib/session-analysis/evidence.ts`
 * (see engine/SOURCE.json for the commit). Everything between the source read and the
 * returned `SessionSourceSnapshot` is relayhistory's code, unchanged: row validation,
 * `normalizedEvent`/`normalizedTurn`, tool and file-edit extraction, the image scrub,
 * the 32 MiB evidence budget, the event/turn/link caps and the source fingerprint.
 *
 * Only the read changed. relayhistory paged three Neon readers:
 *   - `getSessionEvents` (convergence_events, API camelCase rows, workspace provenance),
 *   - `listConversationTurns` (conversation_turns),
 *   - `getSessionThread` (session_links, session_outcomes, the session envelope).
 * Here one trusted producer snapshot of the canonical scope (`canonicalProducerSnapshot`,
 * the same projection that rebuilds those Neon tables from the scrubbed raw) supplies
 * the projected `convergence_events`, `conversation_turns`, `session_links` and
 * `session_outcomes` rows, and `apiEvent`/`apiTurn` give them the exact shapes those
 * readers returned (relayhistory recall.ts `shapeEvent`, turns.ts `listConversationTurns`).
 *
 * Scope difference (deliberate, documented in the F1 map): relayhistory read the session
 * id across the whole organization; a canonical scope is one (org, workspace, harness,
 * session). The requesting workspace is the scope. A session id that has more than one
 * harness in that workspace is refused exactly as relayhistory refused a source collision.
 *
 * Nothing here logs transcript text.
 */
import { createHash } from "node:crypto";
import type {
  CollectedEvidence,
  NormalizedEvent,
  NormalizedHydration,
  NormalizedToolCall,
  NormalizedFileEdit,
} from "../session-brief/engine/normalized-history.ts";
import { toolEvidenceId } from "../session-brief/engine/normalized-history.ts";

export class SessionSourceNotFoundError extends Error {
  readonly status = 404;
  constructor() {
    super("Session not found");
    this.name = "SessionSourceNotFoundError";
  }
}

export class SessionSourceIncompleteError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SessionSourceIncompleteError";
  }
}

type RecordValue = Record<string, unknown>;
export type SessionSourceSnapshot = {
  organizationId: string;
  workspaceId: string;
  sessionId: string;
  sourceProvider: string;
  sourceWorkspaceId: string | null;
  evidenceWorkspaceIds?: string[];
  sourceFingerprint: string;
  events: RecordValue[];
  turns: RecordValue[];
  thread: { outcomes: RecordValue[]; links: RecordValue[] };
  evidence: CollectedEvidence;
  hydration: NormalizedHydration;
};

const MAX_EVENTS = 20_000;
const MAX_TURNS = 20_000;
const MAX_LINKS = 10_000;
const MAX_JSON_BYTES = 32 * 1024 * 1024;

function record(value: unknown): value is RecordValue {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function requiredString(value: unknown, label: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new SessionSourceIncompleteError(`RelayHistory ${label} is missing`);
  }
  return value;
}

function string(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}

function evidenceSessionId(row: RecordValue, rootSessionId: string): string {
  const subagentId = string(row.subagentId);
  return subagentId ? `${rootSessionId}:subagent:${subagentId}` : rootSessionId;
}

function timestamp(value: unknown, label: string): number {
  const parsed = Date.parse(requiredString(value, label));
  if (!Number.isFinite(parsed)) {
    throw new SessionSourceIncompleteError(`RelayHistory ${label} is invalid`);
  }
  return parsed;
}

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (record(value)) {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value) ?? "null";
}

function assertSize(value: unknown): void {
  if (new TextEncoder().encode(canonical(value)).length > MAX_JSON_BYTES) {
    throw new SessionSourceIncompleteError(
      "RelayHistory session exceeds the analysis evidence size limit",
    );
  }
}

// Text analysis has no image decoder. Keep surrounding transcript text and an
// attachment marker, but never duplicate embedded raster bytes into its snapshot.
function textAnalysisValue(value: unknown): unknown {
  if (value instanceof Date) return value;
  if (typeof value === "string")
    return value.replace(
      /data:image\/(?:png|jpeg|gif|webp);base64,[A-Za-z0-9+/]+={0,2}/g,
      "[embedded image omitted from text analysis]",
    );
  if (Array.isArray(value)) return value.map(textAnalysisValue);
  if (record(value))
    return Object.fromEntries(
      Object.entries(value).map(([key, child]) => [
        key,
        textAnalysisValue(child),
      ]),
    );
  return value;
}

// Admit each cleaned input before retaining it. The final snapshot guard
// remains authoritative for the persisted envelope.
class EvidenceBudget {
  private bytes = 0;
  admit(value: unknown): void {
    this.bytes += new TextEncoder().encode(JSON.stringify(value)).length;
    if (this.bytes > MAX_JSON_BYTES)
      throw new SessionSourceIncompleteError(
        "RelayHistory session exceeds the analysis evidence size limit",
      );
  }
}

/** One trusted read of a canonical scope: the projected Neon-shaped rows. */
export interface CanonicalScopeRows {
  exists: boolean;
  /**
   * The legacy export's coverage marker (canonical-legacy.ts `legacy_scope`): null when the
   * scope does not prove Neon's conversation_turns / session_links / session_outcomes
   * were exported, so turns relayhistory would have read may be missing.
   */
  legacyCoverage: { version: 1; tables: string[] } | null;
  /**
   * The objects half of the never-in-Neon proof, from THIS snapshot's own manifest reads (each
   * bound to its pointer's etag): true when any manifest carries a Neon export marker or a
   * backfill epoch. Always present (false when nothing is derived): an absent objects half
   * refuses, it never counts as not-derived. Never re-derived from a second, unbound
   * manifest read.
   */
  neonDerived: boolean;
  /** canonicalSourceGeneration: pointer set identity, recorded as the job's source revision. */
  generation: string;
  rows: Array<{ table: string; row: RecordValue }>;
}

export interface AnalysisScope {
  orgId: string;
  workspaceId: string;
  sessionId: string;
}

export interface AnalysisEvidenceDeps {
  /** Live harnesses of (org, workspace, session) from sessions.session_index. */
  harnesses(scope: AnalysisScope): Promise<string[]>;
  /** canonicalProducerSnapshot of one canonical identity. */
  snapshot(identity: AnalysisScope & { source: string }): Promise<CanonicalScopeRows>;
  /**
   * The DATABASE half of the never-in-Neon proof: sessions-pipeline's own records show the
   * session was born after the sealed Neon freeze and is absent from the frozen Neon copy.
   * The objects half is the snapshot's own `neonDerived`. Absent or false: the marker is
   * required.
   */
  neonFree?(identity: AnalysisScope & { source: string }): Promise<boolean>;
}

/** Postgres timestamptz text or ISO, as the JS Date JSON relayhistory's readers returned. */
function iso(value: unknown): unknown {
  if (value instanceof Date) return value.toISOString();
  if (typeof value !== "string") return value;
  const m =
    /^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}(?::?\d{2})?)$/u.exec(
      value,
    );
  if (!m) return value;
  const ms = ((m[3] ?? "") + "000").slice(0, 3);
  const zone =
    m[4] === "Z"
      ? "Z"
      : m[4]!.length === 3
        ? `${m[4]}:00`
        : m[4]!.includes(":")
          ? m[4]!
          : `${m[4]!.slice(0, 3)}:${m[4]!.slice(3)}`;
  const at = new Date(`${m[1]}T${m[2]}.${ms}${zone}`);
  return Number.isNaN(at.getTime()) ? value : at.toISOString();
}

const num = (value: unknown) => (value === null || value === undefined ? 0 : Number(value));
const nullableNum = (value: unknown) =>
  value === null || value === undefined ? null : Number(value);
const nullable = (value: unknown) => (value === undefined ? null : value);

/**
 * A projected convergence_events row (snake_case Neon columns) as relayhistory
 * recall.ts `shapeEvent` returned it with no `maxContent` (so never truncated) and
 * `includeWorkspaceProvenance: true`, which is how loadSessionSource read it.
 */
export function apiEvent(row: RecordValue): RecordValue {
  const confidence = row.confidence_basis_points ?? row.confidence;
  return {
    eventId: row.event_id,
    userId: nullable(row.user_id),
    sessionId: row.session_id,
    source: row.source,
    lens: nullable(row.lens),
    kind: row.kind,
    type: row.type,
    ts: iso(row.ts),
    actorName: nullable(row.actor_name),
    actorRole: nullable(row.actor_role),
    subagentId: nullable(row.subagent_id),
    machineId: row.machine_id,
    projectId: nullable(row.project_id),
    trajectoryId: null,
    taskRef: row.task_ref ?? {},
    taskTitle: nullable(row.task_title),
    taskDescription: nullable(row.task_description),
    taskStatus: nullable(row.task_status),
    content: nullable(row.content),
    contentTruncated: false,
    significance: nullable(row.significance),
    confidence:
      confidence === null || confidence === undefined ? null : num(confidence) / 10_000,
    tags: row.tags ?? [],
    model: nullable(row.model),
    provider: nullable(row.provider),
    usage: {
      input: num(row.input_tokens),
      output: num(row.output_tokens),
      reasoning: num(row.reasoning_tokens),
      cacheRead: num(row.cache_read_tokens),
      cacheCreate: num(row.cache_create_tokens),
      cacheCreate5m: nullableNum(row.cache_create_5m_tokens),
      cacheCreate1h: nullableNum(row.cache_create_1h_tokens),
    },
    costUsdMicros: nullableNum(row.cost_usd_micros),
    toolName: nullable(row.tool_name),
    toolStatus: nullable(row.tool_status),
    toolCalls: row.tool_calls ?? [],
    filesTouched: row.files_touched ?? [],
    durationMs: nullable(row.duration_ms),
    workspaceId: row.workspace_id,
  };
}

/** A conversation_turns row as relayhistory turns.ts `listConversationTurns` selected it. */
export function apiTurn(row: RecordValue): RecordValue {
  return {
    id: String(row.id),
    sessionId: row.session_id,
    sessionOwner: row.session_owner,
    turnIndex: Number(row.turn_index),
    role: row.role,
    content: row.content,
    actorName: row.actor_name,
    actorRole: row.actor_role,
    metadata: row.metadata,
    // listConversationTurns selected no workspace: relayhistory's evidenceWorkspaceIds
    // records "unknown" for a session with turns, and so does this port.
    ts: iso(row.ts),
  };
}

const text = (v: unknown) => (typeof v === "string" ? v : String(v ?? ""));
/** Code-unit order (Neon's sessions schema columns are C-collated text). */
const cmp = (a: unknown, b: unknown) => {
  const x = text(a);
  const y = text(b);
  return x < y ? -1 : x > y ? 1 : 0;
};

/** recall.ts queryEvents ascending order: ts, event_id, machine_id, source, session_id, kind. */
function eventOrder(a: RecordValue, b: RecordValue): number {
  const ta = Date.parse(text(a.ts));
  const tb = Date.parse(text(b.ts));
  if (ta !== tb) return ta - tb;
  return (
    cmp(a.eventId, b.eventId) ||
    cmp(a.machineId, b.machineId) ||
    cmp(a.source, b.source) ||
    cmp(a.sessionId, b.sessionId) ||
    cmp(a.kind, b.kind)
  );
}

/** turns.ts listConversationTurns order: turn_index, workspace_id, source, id. */
function turnOrder(a: RecordValue, b: RecordValue): number {
  return (
    num(a.turn_index) - num(b.turn_index) ||
    cmp(a.workspace_id, b.workspace_id) ||
    cmp(a.legacy_stored_source ?? a.source, b.legacy_stored_source ?? b.source) ||
    cmp(a.id, b.id)
  );
}
function normalizedEvent(
  row: RecordValue,
  source: string,
  sessionId: string,
  id: number,
): NormalizedEvent {
  if (requiredString(row.sessionId, "event sessionId") !== sessionId) {
    throw new SessionSourceIncompleteError(
      "RelayHistory returned an event from another session",
    );
  }
  if (requiredString(row.source, "event source") !== source) {
    throw new SessionSourceIncompleteError(
      "RelayHistory sessionId collides across source providers",
    );
  }
  if (row.contentTruncated === true) {
    throw new SessionSourceIncompleteError(
      "RelayHistory returned truncated event content",
    );
  }
  const rawUsage = record(row.usage) ? row.usage : {};
  const tokens = (value: unknown) =>
    typeof value === "number" && Number.isFinite(value) ? value : 0;
  const cost = tokens(row.costUsdMicros) / 1_000_000;
  const usage =
    Object.keys(rawUsage).length || cost
      ? {
          input_tokens: tokens(rawUsage.input),
          output_tokens: tokens(rawUsage.output),
          cached_input_tokens: tokens(rawUsage.cacheRead),
          total_tokens: tokens(rawUsage.input) + tokens(rawUsage.output),
          // History defaults missing cost to zero, so a zero cannot establish
          // observed free usage. Omit it to preserve Skip's unknown-cost value.
          ...(cost > 0 ? { cost_usd: cost } : {}),
        }
      : null;
  const rawKind = string(row.kind) ?? "event";
  const rawRole = string(row.actorRole);
  const eventType = string(row.type);
  const role =
    rawKind === "prompt" || rawKind === "user"
      ? "user"
      : rawKind === "tool_result" || rawKind === "tool_output"
        ? "tool_result"
        : rawRole === "user" || rawRole === "assistant"
          ? rawRole
          : eventType === "user" || eventType === "assistant"
            ? eventType
            : ["response", "assistant"].includes(rawKind)
              ? "assistant"
              : (rawRole ?? "unknown");
  const kind =
    ["tool_result", "tool_output"].includes(rawKind) && role === "tool_result"
      ? "tool_result"
      : ["user", "assistant"].includes(role) &&
          [
            "prompt",
            "user",
            "response",
            "assistant",
            "message",
            "text",
            "turn",
            "history",
            "session_event",
          ].includes(rawKind)
        ? "text"
        : rawKind;
  return {
    id,
    source,
    sessionId: evidenceSessionId(row, sessionId),
    eventUid: requiredString(row.eventId, "eventId"),
    machineId: requiredString(row.machineId, "event machineId"),
    sourceKind: rawKind,
    tsMs: timestamp(row.ts, "event timestamp"),
    role,
    kind,
    text: string(row.content),
    model: string(row.model),
    tokenUsage: usage,
    messageId: null,
    // The imported event contract does not retain a tool-result parent ID.
    // Leave it unknown; hydration diagnostics disclose the missing linkage.
    parentId: null,
  };
}

function normalizedTurn(
  row: RecordValue,
  source: string,
  sessionId: string,
  id: number,
): NormalizedEvent {
  if (requiredString(row.sessionId, "turn sessionId") !== sessionId) {
    throw new SessionSourceIncompleteError(
      "RelayHistory returned a turn from another session",
    );
  }
  const metadata = record(row.metadata) ? row.metadata : {};
  const native = string(metadata.nativeCli) ?? string(metadata.source);
  if (native && native !== source) {
    throw new SessionSourceIncompleteError(
      "RelayHistory sessionId collides across source providers",
    );
  }
  return {
    id,
    source,
    sessionId: evidenceSessionId(metadata, sessionId),
    eventUid: requiredString(row.id, "turn id"),
    tsMs: timestamp(row.ts, "turn timestamp"),
    role: requiredString(row.role, "turn role"),
    kind: "text",
    text: typeof row.content === "string" ? row.content : null,
    model: string(metadata.model),
    tokenUsage: null,
    messageId: null,
    parentId: null,
  };
}

function toolsFromEvents(
  events: RecordValue[],
  source: string,
  sessionId: string,
): NormalizedToolCall[] {
  const tools: NormalizedToolCall[] = [];
  for (const row of events) {
    const calls = Array.isArray(row.toolCalls)
      ? row.toolCalls.filter(record)
      : [];
    if (!calls.length && string(row.toolName))
      calls.push({ name: row.toolName });
    for (const [index, call] of calls.entries()) {
      const name = string(call.name) ?? string(row.toolName);
      if (!name) continue;
      const args = [
        call.args,
        call.arguments,
        call.input,
        call.parameters,
        call.metadata,
      ].find(record);
      const status = string(call.status) ?? string(row.toolStatus);
      tools.push({
        id: tools.length + 1,
        source,
        sessionId: evidenceSessionId(row, sessionId),
        machineId: requiredString(row.machineId, "tool machineId"),
        sourceKind: requiredString(row.kind, "tool source kind"),
        eventUid: requiredString(row.eventId, "tool eventId"),
        callIndex: index,
        toolUseId:
          string(call.toolUseId) ??
          string(call.id) ??
          `${requiredString(row.eventId, "eventId")}:${index}`,
        name,
        target: string(call.target) ?? string(call.path) ?? null,
        args: args ?? null,
        isError:
          typeof call.isError === "boolean"
            ? call.isError
            : status === "error" || status === "failed"
              ? true
              : status === "success" || status === "completed"
                ? false
                : null,
        tsMs: timestamp(row.ts, "tool timestamp"),
      });
    }
  }
  return tools;
}

function editsFromEvents(
  events: RecordValue[],
  source: string,
  sessionId: string,
  tools: NormalizedToolCall[],
): NormalizedFileEdit[] {
  const edits: NormalizedFileEdit[] = [];
  for (const row of events) {
    const toolName = string(row.toolName);
    const mutating =
      row.kind === "file_edit" ||
      Boolean(
        toolName &&
        /^(edit|write|save|apply_patch|multi_edit)$/i.test(toolName),
      );
    if (!mutating || !Array.isArray(row.filesTouched)) continue;
    const identifiers = Array.isArray(row.toolCalls)
      ? row.toolCalls
          .filter(record)
          .map(
            (call, index) =>
              string(call.toolUseId) ??
              string(call.id) ??
              `${row.eventId}:${index}`,
          )
      : [`${row.eventId}:0`];
    const calls = tools.filter(
      (tool) =>
        tool.machineId === row.machineId &&
        tool.sourceKind === row.kind &&
        tool.eventUid === row.eventId &&
        identifiers.includes(tool.toolUseId),
    );
    for (const file of row.filesTouched) {
      const path =
        string(file) ??
        (record(file) ? (string(file.path) ?? string(file.filePath)) : null);
      if (!path) continue;
      edits.push({
        id: edits.length + 1,
        source,
        sessionId: evidenceSessionId(row, sessionId),
        toolUseId: calls.length === 1 ? calls[0]!.toolUseId : null,
        toolEvidenceId: calls.length === 1 ? toolEvidenceId(calls[0]!) : null,
        filePath: path,
        toolName,
        linesAdded: null,
        linesRemoved: null,
        structuredPatch: null,
        structuredPatchJson: null,
        userModified: null,
        tsMs: timestamp(row.ts, "file timestamp"),
      });
    }
  }
  return edits;
}

/**
 * relayhistory loadSessionSource over one canonical scope. Reads the whole scope before
 * returning; a partial snapshot is never published (the producer snapshot itself refuses
 * an incomplete projection).
 */
export async function loadSessionSource(
  deps: AnalysisEvidenceDeps,
  scope: AnalysisScope,
  options: {
    /**
     * SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS (operator switch, default off; the read API's
     * READ_API_TIMING_ASSUME_NO_TURNS pattern): analyze a scope without the coverage marker.
     */
    assumeNoLegacyTurns?: boolean;
  } = {},
): Promise<SessionSourceSnapshot & { sourceGeneration: string }> {
  const { sessionId } = scope;
  if (!sessionId.trim()) throw new SessionSourceNotFoundError();
  const harnesses = [...new Set(await deps.harnesses(scope))].sort();
  if (!harnesses.length) throw new SessionSourceNotFoundError();
  if (harnesses.length !== 1)
    throw new SessionSourceIncompleteError(
      "RelayHistory sessionId collides across source providers",
    );
  const harness = harnesses[0]!;
  const snapshot = await deps.snapshot({ ...scope, source: harness });
  if (!snapshot.exists) throw new SessionSourceNotFoundError();
  // Without the export's coverage marker a Neon-era scope may lack turns Neon held, so its
  // evidence is incomplete (the turns route's turns_unknown). A session that never existed
  // in Neon has no Neon-only evidence: it is complete from S3 alone (neonFree proves it).
  if (
    !snapshot.legacyCoverage &&
    options.assumeNoLegacyTurns !== true &&
    !(deps.neonFree && snapshot.neonDerived === false && (await deps.neonFree({ ...scope, source: harness })))
  )
    throw new SessionSourceIncompleteError("RelayHistory legacy evidence coverage is unknown");
  const budget = new EvidenceBudget();
  const byTable = (table: string) =>
    snapshot.rows.filter((r) => r.table === table).map((r) => r.row);

  const events = (
    textAnalysisValue(byTable("convergence_events").map(apiEvent)) as RecordValue[]
  ).sort(eventOrder);
  budget.admit(events);
  if (events.length > MAX_EVENTS) {
    throw new SessionSourceIncompleteError(
      `RelayHistory events exceed the analysis limit of ${MAX_EVENTS}`,
    );
  }
  const turns = (
    textAnalysisValue(byTable("conversation_turns").sort(turnOrder).map(apiTurn)) as RecordValue[]
  );
  budget.admit(turns);
  if (turns.length > MAX_TURNS) {
    throw new SessionSourceIncompleteError(
      `RelayHistory turns exceed the analysis limit of ${MAX_TURNS}`,
    );
  }
  if (!events.length && !turns.length) throw new SessionSourceNotFoundError();
  const sources = new Set(
    events.map((event) => requiredString(event.source, "event source")),
  );
  for (const turn of turns) {
    const metadata = record(turn.metadata) ? turn.metadata : {};
    const native = string(metadata.nativeCli) ?? string(metadata.source);
    if (native) sources.add(native);
  }
  if (sources.size !== 1) {
    throw new SessionSourceIncompleteError(
      sources.size
        ? "RelayHistory sessionId collides across source providers"
        : "RelayHistory session source is unknown",
    );
  }
  const sourceProvider = [...sources][0]!;
  // relayhistory's thread page: the session envelope, session_links and session_outcomes.
  // Provenance only: neither enters the model input nor the source fingerprint.
  const links = textAnalysisValue(byTable("session_links")) as RecordValue[];
  const outcomes = textAnalysisValue(byTable("session_outcomes")) as RecordValue[];
  budget.admit(links);
  budget.admit(outcomes);
  if (links.length > MAX_LINKS) {
    throw new SessionSourceIncompleteError(
      `RelayHistory links exceed the analysis limit of ${MAX_LINKS}`,
    );
  }
  const sourceWorkspaceId = scope.workspaceId;
  const thread = { outcomes, links };
  const normalizedEvents = events.map((row, index) =>
    normalizedEvent(row, sourceProvider, sessionId, index + 1),
  );
  const indexed = new Set(
    normalizedEvents.map(
      (row) => `${row.sessionId}:${row.tsMs}:${row.role}:${row.text ?? ""}`,
    ),
  );
  for (const turn of turns) {
    const next = normalizedTurn(
      turn,
      sourceProvider,
      sessionId,
      normalizedEvents.length + 1,
    );
    const key = `${next.sessionId}:${next.tsMs}:${next.role}:${next.text ?? ""}`;
    if (!indexed.has(key)) normalizedEvents.push(next);
    indexed.add(key);
  }
  normalizedEvents.sort(
    (a, b) => a.tsMs - b.tsMs || a.eventUid.localeCompare(b.eventUid),
  );
  const tools = toolsFromEvents(events, sourceProvider, sessionId);
  const childIds = [
    ...new Set(
      events
        .map((row) => string(row.subagentId))
        .filter((id): id is string => Boolean(id)),
    ),
  ];
  const evidence: CollectedEvidence = {
    tree: {
      nodes: [
        { source: sourceProvider, sessionId, relationship: null },
        ...childIds.map((id) => ({
          source: sourceProvider,
          sessionId: `${sessionId}:subagent:${id}`,
          relationship: null,
        })),
      ],
      unlinked: [],
      diagnostics: [
        "RelayHistory identifies subagent events but does not expose a full relationship tree",
      ],
      truncated: false,
    },
    events: normalizedEvents,
    tools,
    edits: editsFromEvents(events, sourceProvider, sessionId, tools),
  };
  const hydration: NormalizedHydration = {
    capability: "partial",
    status: "complete",
    indexedThrough: {
      sourceStamp: null,
      lastEventAtMs: normalizedEvents.at(-1)?.tsMs ?? null,
    },
    diagnostics: [
      "RelayHistory supplies event/tool/file metadata but no complete tool-result, edit, or subagent tree contract",
      "RelayHistory stores absent cost as zero; only positive imported cost is observable",
    ],
  };
  const sourceData = {
    organizationId: scope.orgId,
    workspaceId: scope.workspaceId,
    sessionId,
    sourceProvider,
    sourceWorkspaceId,
    evidenceWorkspaceIds: [
      ...new Set(
        [...events, ...turns].map(
          (row) => string(row.workspaceId) ?? "unknown",
        ),
      ),
    ].sort(),
    events,
    turns,
    thread,
  };
  assertSize(sourceData);
  // Links, outcomes and requesting workspace are provenance. Only normalized
  // inputs used by the two model passes define the canonical analysis identity.
  const fingerprintedSource = {
    sourceProvider,
    sessionId,
    evidence,
    hydration,
  };
  const sourceFingerprint = createHash("sha256")
    .update(canonical(fingerprintedSource))
    .digest("hex");
  return {
    ...sourceData,
    sourceFingerprint,
    evidence,
    hydration,
    sourceGeneration: snapshot.generation,
  };
}
