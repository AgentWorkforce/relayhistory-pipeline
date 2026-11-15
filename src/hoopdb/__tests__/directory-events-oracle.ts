/**
 * Test oracle: `directoryEventsFromProjection` exactly as it was on main at
 * 1452b6ac, before it was rebuilt on DirectoryEventsBuilder (issue 610 Phase 2).
 * The builder parity test requires equal output, and a failure on exactly the
 * same inputs, for every generated input. Do not edit this to follow a change
 * in the builder: a deliberate change to the metadata needs its own oracle.
 */
import { canonicalTimestamp } from "@rhc/lib/pipeline/relayhistory-projection.ts";
import type { DirectoryEvents, EventLocator } from "../session-directory-events.ts";
const MAX_COUNT = 9223372036854775807n;
type Event = Record<string, unknown>;
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
function sum(values: bigint[]): string {
  const n = values.reduce((a, b) => a + b, 0n);
  if (n > MAX_COUNT) invalid();
  return String(n);
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
/** Takes the complete surviving convergence rows, never catalog/content text. */
export function legacyDirectoryEventsFromProjection(
  input: readonly Event[],
): DirectoryEvents {
  const unique = new Map<string, Event>();
  for (const row of input) {
    const key = JSON.stringify([
      row.machine_id,
      row.source,
      row.session_id,
      row.kind,
      row.event_id,
      row.ts,
    ]);
    const prior = unique.get(key);
    if (
      prior &&
      JSON.stringify(canonical(prior)) !== JSON.stringify(canonical(row))
    )
      invalid();
    unique.set(key, row);
  }
  const rows = [...unique.values()]
    .map<
      Event & { ts: string }
    >((row) => ({ ...row, ts: canonicalTimestamp(String(row.ts)) }))
    .sort(
      (a, b) =>
        a.ts.length - b.ts.length ||
        (a.ts < b.ts ? -1 : a.ts > b.ts ? 1 : 0) ||
        Buffer.compare(
          Buffer.from(String(a.event_id)),
          Buffer.from(String(b.event_id)),
        ),
    );
  const kinds = [...new Set(rows.map((r) => String(r.kind)))].sort();
  if (
    kinds.length > 64 ||
    kinds.some((k) => !k || k.length > 128 || /[\u0000-\u001f\u007f]/u.test(k))
  )
    invalid();
  const refs = new Map<string, unknown>();
  for (const row of rows) {
    const ref = row.task_ref;
    if (ref == null || JSON.stringify(ref) === "{}") continue;
    if (typeof ref !== "object") invalid();
    const clean = canonical(ref);
    const key = JSON.stringify(clean);
    if (key.length > 16384) invalid();
    refs.set(key, clean);
  }
  const taskRefs = [...refs.entries()]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([, v]) => v);
  if (taskRefs.length > 1000 || JSON.stringify(taskRefs).length > 131072)
    invalid();
  const textSet = (field: string) => {
    const values = [
      ...new Set(
        rows.flatMap((row) =>
          typeof row[field] === "string" && row[field] !== ""
            ? [row[field] as string]
            : [],
        ),
      ),
    ].sort();
    if (
      values.length > 1000 ||
      values.some(
        (value) => value.length > 1024 || /[\u0000-\u001f\u007f]/u.test(value),
      )
    )
      invalid();
    return values;
  };
  const compareText = (left: string, right: string) =>
    Buffer.compare(Buffer.from(left), Buffer.from(right));
  const projectRows = rows
    .filter((row) => typeof row.project_id === "string")
    .sort((a, b) => compareText(String(a.project_id), String(b.project_id)));
  const project = projectRows.at(-1);
  const projectId = project ? String(project.project_id) : null;
  const isPath =
    projectId !== null &&
    (/^(?:[\\/]|~|\.{1,2}[\\/]|file:)/iu.test(projectId) ||
      /^[A-Za-z]:[\\/]/u.test(projectId));
  function locator(row: Event & { ts: string }): EventLocator {
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
    ])
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
  if (
    projectId !== null &&
    !isPath &&
    (projectId.length > 1024 || /[\u0000-\u001f\u007f]/u.test(projectId))
  )
    invalid();
  const projectEvent = isPath && project ? locator(project) : null;
  const opening = rows.find(
    (row) =>
      (["history", "prompt", "user"].includes(String(row.kind)) ||
        (row.kind === "session_event" && row.actor_role === "user")) &&
      typeof row.content === "string" &&
      row.content.replace(/^ +| +$/gu, "").length > 0,
  );
  const openingEvent = opening ? locator(opening) : null;
  const totals = (key: string) => sum(rows.map((row) => count(row[key])));
  const completeSplit = rows.every(
    (row) =>
      count(row.cache_create_tokens) === 0n ||
      ((row.cache_create_5m_tokens != null ||
        row.cache_create_1h_tokens != null) &&
        count(row.cache_create_5m_tokens) +
          count(row.cache_create_1h_tokens) ===
          count(row.cache_create_tokens)),
  );
  const work = [...rows].reverse().find((row) => status(row) !== null);
  const taskTitle =
    [...rows]
      .reverse()
      .map((r) => r.task_title)
      .find((t) => t != null) ?? null;
  if (
    taskTitle !== null &&
    (typeof taskTitle !== "string" || [...taskTitle].length > 200)
  )
    invalid();
  return {
    eventCount: String(rows.length),
    eventTimes: [...new Set(rows.map((row) => row.ts))],
    openingEvent,
    userIds: textSet("user_id"),
    machineIds: textSet("machine_id"),
    models: textSet("model"),
    projectId: isPath ? null : projectId,
    projectEvent,
    firstTs: rows[0]?.ts ?? null,
    lastTs: rows.at(-1)?.ts ?? null,
    kinds,
    taskRefs,
    taskTitle: taskTitle as string | null,
    inputTokens: totals("input_tokens"),
    outputTokens: totals("output_tokens"),
    reasoningTokens: totals("reasoning_tokens"),
    cacheReadTokens: totals("cache_read_tokens"),
    cacheCreateTokens: totals("cache_create_tokens"),
    cacheCreate5mTokens: completeSplit
      ? totals("cache_create_5m_tokens")
      : null,
    cacheCreate1hTokens: completeSplit
      ? totals("cache_create_1h_tokens")
      : null,
    costUsdMicros: rows.some((r) => r.cost_usd_micros != null)
      ? totals("cost_usd_micros")
      : null,
    workStatus: work ? status(work) : null,
    workStatusAt: work?.ts ?? null,
  };
}
