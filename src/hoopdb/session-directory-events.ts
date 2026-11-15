/** Content-free, complete canonical-event metadata for the logical directory.
 * Call only after all live manifests were verified/folded. Never aggregate the
 * ClickHouse window or pointer counters: both can be a strict subset of truth.
 */
import { createHash } from "node:crypto";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { canonicalTimestamp } from "@rhc/lib/pipeline/relayhistory-projection.ts";
import type { SessionDirectoryKey } from "./session-directory-summary.ts";
export const EVENT_PROJECTION_VERSION = 1;
const MAX_COUNT = 9223372036854775807n;
type Event = Record<string, unknown>;
export interface EventLocator {
  session_id: string;
  event_id: string;
  machine_id: string;
  kind: string;
  source: string;
  ts: string;
  fingerprint: string;
}
export interface DirectoryEvents {
  eventCount: string;
  eventTimes: string[];
  userIds: string[];
  machineIds: string[];
  models: string[];
  projectId: string | null;
  openingEvent: EventLocator | null;
  projectEvent: EventLocator | null;
  firstTs: string | null;
  lastTs: string | null;
  kinds: string[];
  taskRefs: unknown[];
  taskTitle: string | null;
  inputTokens: string;
  outputTokens: string;
  reasoningTokens: string;
  cacheReadTokens: string;
  cacheCreateTokens: string;
  cacheCreate5mTokens: string | null;
  cacheCreate1hTokens: string | null;
  costUsdMicros: string | null;
  workStatus: "active" | "idle" | "finished" | null;
  workStatusAt: string | null;
}
export interface DirectoryEventInput {
  sourceSessionId: string;
  expectedEpoch: string;
  sources: Array<{ manifestKey: string; revision: string }>;
  events: DirectoryEvents;
}
function invalid(): never {
  throw new Error("directory_event_metadata_invalid");
}
function count(value: unknown): bigint {
  if (value == null) return 0n;
  if (typeof value === "number" && (!Number.isSafeInteger(value) || value < 0))
    invalid();
  if (
    !["number", "string", "bigint"].includes(typeof value) ||
    !/^\d+$/u.test(String(value))
  )
    invalid();
  const n = BigInt(String(value));
  if (n > MAX_COUNT) invalid();
  return n;
}
function canonical(value: unknown, depth = 0): unknown {
  if (depth > 12) invalid();
  if (Array.isArray(value)) return value.map((v) => canonical(v, depth + 1));
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value)
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([k, v]) => [k, canonical(v, depth + 1)]),
    );
  if (typeof value === "number" && !Number.isFinite(value)) invalid();
  return value;
}
const workKinds = new Set([
  "prompt",
  "user",
  "assistant",
  "history",
  "session_event",
  "tool_call",
  "file_edit",
  "task",
]);
function status(row: Event): DirectoryEvents["workStatus"] {
  if (!workKinds.has(String(row.kind))) return null;
  const s = String(row.task_status ?? "").toLowerCase();
  if (["completed", "complete", "finished", "done", "succeeded"].includes(s))
    return "finished";
  if (
    [
      "idle",
      "paused",
      "waiting",
      "blocked",
      "cancelled",
      "canceled",
      "failed",
    ].includes(s)
  )
    return "idle";
  if (["active", "working", "running", "in_progress"].includes(s))
    return "active";
  return row.kind === "task" ? null : "active";
}
/** Position of a row in the canonical `(ts, event_id bytes)` order; `ordinal` is
 * the first-arrival index, the stable sort's tie-break. */
interface Position {
  ts: string;
  eventId: Buffer;
  ordinal: number;
}
function comparePosition(a: Position, b: Position): number {
  return (
    a.ts.length - b.ts.length ||
    (a.ts < b.ts ? -1 : a.ts > b.ts ? 1 : 0) ||
    Buffer.compare(a.eventId, b.eventId) ||
    a.ordinal - b.ordinal
  );
}
/** What `locator` reads of a chosen row, kept instead of the row. */
type LocatorFields = Pick<
  Event,
  | "_index_fingerprint"
  | "session_id"
  | "event_id"
  | "machine_id"
  | "kind"
  | "source"
> & { ts: string };
function locatorFields(row: Event, ts: string): LocatorFields {
  return {
    _index_fingerprint: row._index_fingerprint,
    session_id: row.session_id,
    event_id: row.event_id,
    machine_id: row.machine_id,
    kind: row.kind,
    source: row.source,
    ts,
  };
}
function locator(row: LocatorFields): EventLocator {
  if (
    typeof row._index_fingerprint !== "string" ||
    !/^s3[0-9a-f]{14}$/u.test(row._index_fingerprint)
  )
    invalid();
  for (const field of [
    "session_id",
    "event_id",
    "machine_id",
    "kind",
    "source",
  ] as const)
    if (typeof row[field] !== "string" || !row[field]) invalid();
  return {
    session_id: String(row.session_id),
    event_id: String(row.event_id),
    machine_id: String(row.machine_id),
    kind: String(row.kind),
    source: String(row.source),
    ts: row.ts,
    fingerprint: row._index_fingerprint,
  };
}
/** sha256 of a value's canonical JSON, or the error computing it raised. */
function contentDigest(row: Event): { digest: string } | { error: unknown } {
  try {
    return {
      digest: createHash("sha256")
        .update(JSON.stringify(canonical(row)))
        .digest("base64"),
    };
  } catch (error) {
    return { error };
  }
}
function textValue(row: Event, field: string): string | null {
  return typeof row[field] === "string" && row[field] !== ""
    ? (row[field] as string)
    : null;
}
const TOKEN_FIELDS = [
  "input_tokens",
  "output_tokens",
  "reasoning_tokens",
  "cache_read_tokens",
  "cache_create_tokens",
] as const;

/**
 * The directory's event metadata, built one convergence row at a time: a reader
 * that streams a session adds its rows chunk by chunk and never holds them all.
 * It keeps a digest per distinct row, the distinct times, kinds, task refs, user,
 * machine and model sets, running token sums and the few rows the metadata
 * points at; never a row's content.
 *
 * The rows may arrive in any order. `finish` returns exactly what
 * `directoryEventsFromProjection` returns for the same rows in list order, and
 * the builder
 * fails (`directory_event_metadata_invalid`, or the timestamp's own error) on
 * exactly the inputs that function fails on. A check that holds for every row
 * fails in `add`; one that depends on which row the order selects fails in
 * `finish`. With more than one fault in the input, which fault is reported can
 * depend on arrival order. Build once: `finish` ends the builder.
 *
 * List order only breaks ties between rows with the same time and event id. By
 * default it is arrival order; a reader that adds rows out of list order passes
 * each row's `order` (distinct numbers, ascending in list order) instead.
 */
export class DirectoryEventsBuilder {
  /** sha256(identity key) -> the first row's canonical digest, or its error. */
  readonly #seen = new Map<string, { digest: string } | { error: unknown }>();
  readonly #times = new Set<string>();
  readonly #kinds = new Set<string>();
  readonly #refs = new Map<string, unknown>();
  readonly #texts = {
    user_id: new Set<string>(),
    machine_id: new Set<string>(),
    model: new Set<string>(),
  };
  readonly #sums = new Map<string, bigint>();
  #count = 0;
  #first: Position | null = null;
  #last: Position | null = null;
  #opening: { at: Position; row: LocatorFields } | null = null;
  #project: {
    id: string;
    bytes: Buffer;
    at: Position;
    row: LocatorFields;
  } | null = null;
  #work: { at: Position; status: NonNullable<DirectoryEvents["workStatus"]> } | null = null;
  #title: { at: Position; value: unknown } | null = null;
  /** Earliest row whose split check is false, and earliest whose check fails. */
  #splitFalse: Position | null = null;
  #splitThrows: Position | null = null;
  /** Some row's 5m or 1h counter is malformed: fatal only with a complete split. */
  #splitInvalid = false;
  #anyCost = false;
  #finished = false;

  add(row: Event, order?: number): void {
    if (this.#finished) throw new Error("directory_events_builder_finished");
    const key = createHash("sha256")
      .update(
        JSON.stringify([
          row.machine_id,
          row.source,
          row.session_id,
          row.kind,
          row.event_id,
          row.ts,
        ]),
      )
      .digest("base64");
    const content = contentDigest(row);
    const prior = this.#seen.get(key);
    if (prior) {
      // A repeat must be the same row: then it adds nothing.
      if ("error" in prior) throw prior.error;
      if ("error" in content) throw content.error;
      if (prior.digest !== content.digest) invalid();
      return;
    }
    this.#seen.set(key, content);
    const ts = canonicalTimestamp(String(row.ts));
    const at: Position = {
      ts,
      eventId: Buffer.from(String(row.event_id)),
      ordinal: order ?? this.#count,
    };
    this.#count += 1;
    this.#times.add(ts);
    if (!this.#first || comparePosition(at, this.#first) < 0) this.#first = at;
    if (!this.#last || comparePosition(at, this.#last) > 0) this.#last = at;

    const kind = String(row.kind);
    this.#kinds.add(kind);
    if (
      this.#kinds.size > 64 ||
      !kind ||
      kind.length > 128 ||
      /[\u0000-\u001f\u007f]/u.test(kind)
    )
      invalid();

    const ref = row.task_ref;
    if (!(ref == null || JSON.stringify(ref) === "{}")) {
      if (typeof ref !== "object") invalid();
      const clean = canonical(ref);
      const refKey = JSON.stringify(clean);
      if (refKey.length > 16384) invalid();
      this.#refs.set(refKey, clean);
      if (this.#refs.size > 1000) invalid();
    }

    for (const [field, values] of Object.entries(this.#texts)) {
      const value = textValue(row, field);
      if (value === null) continue;
      if (value.length > 1024 || /[\u0000-\u001f\u007f]/u.test(value))
        invalid();
      values.add(value);
      if (values.size > 1000) invalid();
    }

    if (typeof row.project_id === "string") {
      const bytes = Buffer.from(row.project_id);
      const order = this.#project
        ? Buffer.compare(bytes, this.#project.bytes) ||
          comparePosition(at, this.#project.at)
        : 1;
      if (order > 0)
        this.#project = {
          id: row.project_id,
          bytes,
          at,
          row: locatorFields(row, ts),
        };
    }

    if (
      (["history", "prompt", "user"].includes(kind) ||
        (row.kind === "session_event" && row.actor_role === "user")) &&
      typeof row.content === "string" &&
      row.content.replace(/^ +| +$/gu, "").length > 0 &&
      (!this.#opening || comparePosition(at, this.#opening.at) < 0)
    )
      this.#opening = { at, row: locatorFields(row, ts) };

    for (const field of TOKEN_FIELDS)
      this.#sums.set(field, (this.#sums.get(field) ?? 0n) + count(row[field]));
    if (row.cost_usd_micros != null) {
      this.#anyCost = true;
      this.#sums.set(
        "cost_usd_micros",
        (this.#sums.get("cost_usd_micros") ?? 0n) + count(row.cost_usd_micros),
      );
    }
    let split5m = 0n;
    let split1h = 0n;
    let splitOk = true;
    try {
      split5m = count(row.cache_create_5m_tokens);
      split1h = count(row.cache_create_1h_tokens);
    } catch {
      splitOk = false;
      this.#splitInvalid = true;
    }
    if (splitOk) {
      this.#sums.set(
        "cache_create_5m_tokens",
        (this.#sums.get("cache_create_5m_tokens") ?? 0n) + split5m,
      );
      this.#sums.set(
        "cache_create_1h_tokens",
        (this.#sums.get("cache_create_1h_tokens") ?? 0n) + split1h,
      );
    }
    const create = count(row.cache_create_tokens);
    if (create !== 0n) {
      if (
        row.cache_create_5m_tokens == null &&
        row.cache_create_1h_tokens == null
      ) {
        if (!this.#splitFalse || comparePosition(at, this.#splitFalse) < 0)
          this.#splitFalse = at;
      } else if (!splitOk) {
        if (!this.#splitThrows || comparePosition(at, this.#splitThrows) < 0)
          this.#splitThrows = at;
      } else if (split5m + split1h !== create) {
        if (!this.#splitFalse || comparePosition(at, this.#splitFalse) < 0)
          this.#splitFalse = at;
      }
    }

    const work = status(row);
    if (work !== null && (!this.#work || comparePosition(at, this.#work.at) > 0))
      this.#work = { at, status: work };
    if (
      row.task_title != null &&
      (!this.#title || comparePosition(at, this.#title.at) > 0)
    )
      this.#title = { at, value: row.task_title };
  }

  finish(): DirectoryEvents {
    if (this.#finished) throw new Error("directory_events_builder_finished");
    this.#finished = true;
    const taskRefs = [...this.#refs.entries()]
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([, v]) => v);
    if (JSON.stringify(taskRefs).length > 131072) invalid();
    const projectId = this.#project?.id ?? null;
    const isPath =
      projectId !== null &&
      (/^(?:[\\/]|~|\.{1,2}[\\/]|file:)/iu.test(projectId) ||
        /^[A-Za-z]:[\\/]/u.test(projectId));
    if (
      projectId !== null &&
      !isPath &&
      (projectId.length > 1024 || /[\u0000-\u001f\u007f]/u.test(projectId))
    )
      invalid();
    const projectEvent =
      isPath && this.#project ? locator(this.#project.row) : null;
    const openingEvent = this.#opening ? locator(this.#opening.row) : null;
    // `every` stops at the first row, in canonical order, that is not a complete split.
    if (
      this.#splitThrows &&
      (!this.#splitFalse ||
        comparePosition(this.#splitThrows, this.#splitFalse) < 0)
    )
      invalid();
    const completeSplit = !this.#splitFalse && !this.#splitThrows;
    if (completeSplit && this.#splitInvalid) invalid();
    const total = (field: string) => {
      const n = this.#sums.get(field) ?? 0n;
      if (n > MAX_COUNT) invalid();
      return String(n);
    };
    const taskTitle = this.#title?.value ?? null;
    if (
      taskTitle !== null &&
      (typeof taskTitle !== "string" || [...taskTitle].length > 200)
    )
      invalid();
    const sorted = (values: Set<string>) => [...values].sort();
    return {
      eventCount: String(this.#count),
      eventTimes: [...this.#times].sort(
        (a, b) => a.length - b.length || (a < b ? -1 : a > b ? 1 : 0),
      ),
      openingEvent,
      userIds: sorted(this.#texts.user_id),
      machineIds: sorted(this.#texts.machine_id),
      models: sorted(this.#texts.model),
      projectId: isPath ? null : projectId,
      projectEvent,
      firstTs: this.#first?.ts ?? null,
      lastTs: this.#last?.ts ?? null,
      kinds: sorted(this.#kinds),
      taskRefs,
      taskTitle: taskTitle as string | null,
      inputTokens: total("input_tokens"),
      outputTokens: total("output_tokens"),
      reasoningTokens: total("reasoning_tokens"),
      cacheReadTokens: total("cache_read_tokens"),
      cacheCreateTokens: total("cache_create_tokens"),
      cacheCreate5mTokens: completeSplit
        ? total("cache_create_5m_tokens")
        : null,
      cacheCreate1hTokens: completeSplit
        ? total("cache_create_1h_tokens")
        : null,
      costUsdMicros: this.#anyCost ? total("cost_usd_micros") : null,
      workStatus: this.#work?.status ?? null,
      workStatusAt: this.#work?.at.ts ?? null,
    };
  }
}

/** Takes the complete surviving convergence rows, never catalog/content text. */
export function directoryEventsFromProjection(
  input: readonly Event[],
): DirectoryEvents {
  const builder = new DirectoryEventsBuilder();
  for (const row of input) builder.add(row);
  return builder.finish();
}
/** Lock/fence against tombstones and compare the entire live source set. A
 * concurrent pointer change invalidates readiness in its own transaction. */
export async function writeDirectoryEvents(
  db: SqlExecutor,
  key: SessionDirectoryKey,
  input: DirectoryEventInput,
): Promise<{ written: boolean }> {
  const expected = Object.fromEntries(
    input.sources.map((s) => [s.manifestKey, String(count(s.revision))]),
  );
  if (
    !input.sources.length ||
    Object.keys(expected).length !== input.sources.length
  )
    invalid();
  const e = input.events;
  const { rows } = await db.query<{ written: boolean }>(
    `WITH captured AS MATERIALIZED (
    SELECT m.source_session_id,m.epoch FROM sessions.session_directory_event_pending m
    WHERE m.org_id=$1 AND m.workspace_id=$2 AND m.harness=$3 AND m.source_session_id IN (
      SELECT p.session_id FROM sessions.session_index p WHERE p.org_id=$1 AND p.workspace_id=$2 AND p.harness=$3 AND coalesce(p.canonical_session_id,p.session_id)=$4 AND p.deleted_at IS NULL
    )
  ), marker AS MATERIALIZED (
    SELECT m.* FROM sessions.session_directory_event_pending m
    WHERE m.org_id=$1 AND m.workspace_id=$2 AND m.harness=$3
      AND (m.source_session_id,m.epoch) IN(SELECT source_session_id,epoch FROM captured)
      ORDER BY source_session_id FOR UPDATE
  ), live AS MATERIALIZED (
    SELECT d.org_id,d.workspace_id,d.harness,d.session_id FROM sessions.session_directory d,marker m
    WHERE d.org_id=$1 AND d.workspace_id=$2 AND d.harness=$3 AND d.session_id IN($4,$5) AND d.deleted_at IS NULL AND (SELECT count(*) FROM marker)=(SELECT count(*) FROM captured)
    ORDER BY (d.session_id=$4) DESC LIMIT 1 FOR UPDATE OF d
  ), sources AS (
    SELECT jsonb_object_agg(p.manifest_key,p.revision::text) AS revisions,count(*) AS n,
      count(DISTINCT coalesce(p.canonical_session_id,p.session_id)) AS identities,
      min(coalesce(p.canonical_session_id,p.session_id)) AS canonical_id
    FROM sessions.session_index p WHERE p.org_id=$1 AND p.workspace_id=$2 AND p.harness=$3
      AND coalesce(p.canonical_session_id,p.session_id)=$4 AND p.deleted_at IS NULL
  ), updated AS (
    UPDATE sessions.session_directory d SET
      event_count=$7::bigint,event_first_ts=$8::timestamptz,event_last_ts=$9::timestamptz,
      kinds=$10::text[],task_refs=$11::jsonb,task_title=$12,
      event_user_ids=$26::text[],event_machine_ids=$27::text[],event_models=$28::text[],event_project_id=$29,opening_event=$30::jsonb,event_project_event=$31::jsonb,project_requires_hydration=$32::boolean,
      input_tokens=$13::bigint,output_tokens=$14::bigint,reasoning_tokens=$15::bigint,
      cache_read_tokens=$16::bigint,cache_create_tokens=$17::bigint,cache_create_5m_tokens=$18::bigint,cache_create_1h_tokens=$19::bigint,cost_usd_micros=$20::bigint,
      work_status=$21,work_status_at=$22::timestamptz,event_projection_version=1,event_projection_ready=true,event_projected_at=now(),event_projection_sources=$6::jsonb,
      search_text=sessions.session_directory_search_text(d.title,$12,d.agent_name,d.project_id,d.git_repo,d.git_branch,d.model,d.short_summary)
    FROM live l,sources s WHERE d.org_id=l.org_id AND d.workspace_id=l.workspace_id AND d.harness=l.harness AND d.session_id=l.session_id
      AND s.revisions=$6::jsonb AND s.n=$23::integer AND s.identities=1 AND s.canonical_id=$4
      AND d.event_projection_epoch=$24::bigint
    RETURNING d.org_id,d.workspace_id,d.harness,d.session_id
  ), clean_marker AS (
    UPDATE sessions.session_directory_event_pending m SET dirty=false FROM updated u
    WHERE m.org_id=$1 AND m.workspace_id=$2 AND m.harness=$3 AND (m.source_session_id,m.epoch) IN(SELECT source_session_id,epoch FROM captured)
  ), drop_times AS (
    DELETE FROM sessions.session_directory_event_times t USING updated u
    WHERE t.org_id=u.org_id AND t.workspace_id=u.workspace_id AND t.harness=u.harness AND t.session_id=u.session_id
      AND NOT(t.event_ts=ANY($25::timestamptz[]))
  ), put_times AS (
    INSERT INTO sessions.session_directory_event_times(org_id,workspace_id,harness,session_id,event_ts)
    SELECT u.org_id,u.workspace_id,u.harness,u.session_id,x.ts FROM updated u,unnest($25::timestamptz[]) x(ts)
    ON CONFLICT DO NOTHING
  ) SELECT EXISTS(SELECT 1 FROM updated) AS written`,
    [
      key.orgId,
      key.workspaceId,
      key.harness,
      key.sessionId,
      input.sourceSessionId,
      JSON.stringify(expected),
      e.eventCount,
      e.firstTs,
      e.lastTs,
      e.kinds,
      JSON.stringify(e.taskRefs),
      e.taskTitle,
      e.inputTokens,
      e.outputTokens,
      e.reasoningTokens,
      e.cacheReadTokens,
      e.cacheCreateTokens,
      e.cacheCreate5mTokens,
      e.cacheCreate1hTokens,
      e.costUsdMicros,
      e.workStatus,
      e.workStatusAt,
      input.sources.length,
      String(count(input.expectedEpoch)),
      e.eventTimes,
      e.userIds,
      e.machineIds,
      e.models,
      e.projectId,
      e.openingEvent ? JSON.stringify(e.openingEvent) : null,
      e.projectEvent ? JSON.stringify(e.projectEvent) : null,
      e.projectEvent !== null,
    ],
  );
  return { written: rows[0]?.written === true };
}

/** Incomplete passes invalidate readiness without erasing the last good metadata. */
export async function invalidateDirectoryEvents(
  db: SqlExecutor,
  scope: SessionDirectoryKey,
): Promise<void> {
  await db.query(
    `UPDATE sessions.session_directory d SET event_projection_ready=false
    WHERE d.org_id=$1 AND d.workspace_id=$2 AND d.harness=$3
      AND (d.session_id=$4 OR d.session_id IN (
        SELECT coalesce(p.canonical_session_id,p.session_id) FROM sessions.session_index p
        WHERE p.org_id=$1 AND p.workspace_id=$2 AND p.harness=$3 AND p.session_id=$4))`,
    [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId],
  );
}

/** Empty scopes clear only the exact marker version locked by this statement.
 * A concurrent pointer mutation waits and dirties it again before committing. */
export async function completeErasedDirectoryEvents(
  db: SqlExecutor,
  scope: SessionDirectoryKey,
): Promise<void> {
  await db.query(
    `WITH captured AS MATERIALIZED (
   SELECT epoch FROM sessions.session_directory_event_pending WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND source_session_id=$4
 ), locked AS MATERIALIZED (
   SELECT m.* FROM sessions.session_directory_event_pending m WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND source_session_id=$4 AND epoch=(SELECT epoch FROM captured) FOR UPDATE
 ) UPDATE sessions.session_directory_event_pending m SET dirty=false FROM locked l
   WHERE m.org_id=l.org_id AND m.workspace_id=l.workspace_id AND m.harness=l.harness AND m.source_session_id=l.source_session_id
    AND NOT EXISTS(SELECT 1 FROM sessions.session_index p WHERE p.org_id=$1 AND p.workspace_id=$2 AND p.harness=$3 AND p.session_id=$4 AND p.deleted_at IS NULL)`,
    [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId],
  );
}
