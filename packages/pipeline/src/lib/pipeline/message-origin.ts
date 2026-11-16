/**
 * Who wrote a session event: a person at the keyboard, or another agent.
 *
 * Claude Code records each user message's origin (`origin.kind`: `human`,
 * `peer`, `task-notification`, ...). relayhistory stores it as the
 * `session_events.origin_kind` column, so it reaches the pipeline as a payload
 * field on `session_event` records. This module turns that field and the
 * columns relayhistory already sent (`role`, `is_sidechain`, `control_kind`,
 * `is_meta`) into one event-level label, stored as `actor_name` on the index
 * row, so search can keep human-typed messages apart from agent-to-agent
 * traffic.
 *
 * The label is applied where a projected event becomes an index row
 * (`projectedToNeonRow`), not in `relayhistory-projection.ts`, because that
 * file is the parity port of the hosted plpgsql projection and must keep
 * producing its rows. The hosted live projection leaves `actor_name` NULL.
 *
 * "human" is only ever claimed from positive evidence: an explicit
 * `origin_kind = human` on the message row. A `history` record (the harness's
 * prompt log) carries no origin and is not labelled: a prompt a broker typed
 * at the harness input lands there too, and the same prompt is already its own
 * `session_event` row. A user message with no origin recorded (an upload from a
 * client before origin capture, or a harness that records none) gets no label,
 * so a human-only search never includes a message whose author is unknown.
 *
 * Default off: MESSAGE_ORIGIN_LABELS_ENABLED (exactly "true") turns the
 * labelling on in the index writers and `origin=` on the read API
 * (docs/message-origin.md).
 */

/** The one switch for the labels (writers) and the `origin=` filter (readers). */
export const MESSAGE_ORIGIN_LABELS_VAR = "MESSAGE_ORIGIN_LABELS_ENABLED";

/** Only the exact string "true" enables; unset or anything else is off. */
export function messageOriginLabelsEnabled(raw: unknown): boolean {
  return raw === "true";
}

/** Event-level origin labels written to `actor_name`. */
export const MESSAGE_ORIGINS = [
  /** Typed by a person at the harness's input. */
  "human",
  /** The assistant's own output. */
  "agent",
  /** A parent agent's prompt to a subagent (a sidechain user message). */
  "subagent",
  /** A message from another agent session (Claude `origin.kind = peer`). */
  "peer",
  /** A background task's completion notice injected as a user message. */
  "task-notification",
  /** Harness-injected control or meta rows (slash-command output, caveats). */
  "system",
  /** An origin kind this version does not know. Never treated as human. */
  "other",
] as const;

export type MessageOrigin = (typeof MESSAGE_ORIGINS)[number];

const KNOWN = new Set<string>(MESSAGE_ORIGINS);

type Json = Record<string, unknown>;

function field(payload: Json, key: string): string | null {
  const value = payload[key];
  if (typeof value === "string") return value.trim() === "" ? null : value.trim();
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return null;
}

const truthy = (payload: Json, key: string) =>
  ["1", "true"].includes(field(payload, key) ?? "");

/** Claude's `origin.kind` (as stored by relayhistory) to a label. */
export function originKindLabel(kind: string): MessageOrigin {
  const normalized = kind.trim().toLowerCase().replace(/_/g, "-");
  if (normalized === "human") return "human";
  if (normalized === "peer") return "peer";
  if (normalized === "task-notification") return "task-notification";
  return "other";
}

/**
 * The origin label of one projected event, from its kind and delivery payload,
 * or null when the record does not say.
 */
export function messageOrigin(kind: string, payload: Json | null | undefined): MessageOrigin | null {
  if (!payload) return null;
  if (kind !== "session_event") return null;
  const role = field(payload, "role");
  if (role === "assistant") return "agent";
  if (role !== "user") return null;
  // A sidechain user message is the parent agent prompting a subagent, whatever
  // origin the subagent's transcript recorded.
  if (truthy(payload, "is_sidechain")) return "subagent";
  const control = field(payload, "control_kind");
  if (control === "task_notification") return "task-notification";
  const origin = field(payload, "origin_kind");
  if (origin !== null) {
    const label = originKindLabel(origin);
    // A control row is never a typed message, even if its origin says human.
    if (label === "human" && (control !== null || truthy(payload, "is_meta"))) return "system";
    return label;
  }
  if (control !== null || truthy(payload, "is_meta")) return "system";
  return null;
}

/**
 * The subagent an event belongs to: relayhistory's `subagent_session_id`, else,
 * for a sidechain row only, its `agent_id`. A root-session row's `agent_id` is
 * not used: there it can name the delegated child a tool result reports on, or
 * the root agent itself.
 */
export function messageSubagent(kind: string, payload: Json | null | undefined): string | null {
  if (!payload || kind !== "session_event") return null;
  const explicit = field(payload, "subagent_session_id");
  if (explicit !== null) return explicit;
  return truthy(payload, "is_sidechain") ? field(payload, "agent_id") : null;
}

/** Whether a stored `actor_name` is one of this module's labels. */
export function isMessageOrigin(value: unknown): value is MessageOrigin {
  return typeof value === "string" && KNOWN.has(value);
}

/**
 * The `actor_name` an index row stores with labelling on: the derived label,
 * else the projected value unless it spells a label. A client cannot claim a
 * label by uploading it (a direct `convergence_event` line keeps its own
 * fields), so `origin=human` only ever matches a derived `human`.
 */
export function labelledActorName(kind: string, payload: Json | null | undefined, projected: string | null): string | null {
  const derived = messageOrigin(kind, payload);
  if (derived !== null) return derived;
  return isMessageOrigin(projected) ? null : projected;
}
