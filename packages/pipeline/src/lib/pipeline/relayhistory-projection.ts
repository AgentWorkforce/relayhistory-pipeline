/**
 * The delivery projection for exported sessions: turns one session's latest
 * records into the rows the live triggers leave in `sessions`, so replay,
 * briefs and summaries read an exported session exactly like a live one.
 *
 * The live projection is plpgsql (project_delivery_activity_row 0060,
 * project_delivery_catalog_row 0058, settle_delivery_request 0024,
 * settle_delivery_tool_ts 0060, delivery_normalized_usage 0023), and the
 * transform has no database to run it in. This is its port, and those functions
 * stay the authority: `relayhistory-projection.test.ts` runs the same records
 * through the vendored migrations and requires equal rows, so a migration that
 * changes the projection fails until this file follows it.
 *
 * Rows hold no tenant columns. Delivery-projected rows also have no `user_id`
 * or `embedding`; the session index holds the owner.
 */
import { createHash } from "node:crypto";
import { PipelineError } from "./errors.js";
import {
  lastAccepted,
  type RelayhistoryStreamRecord,
} from "./relayhistory-transform.js";

type Json = Record<string, unknown>;

export interface ProjectionRow {
  table:
    | "session_catalog"
    | "session_catalog_agents"
    | "session_relationships"
    | "session_markers"
    | "session_commit_links"
    | "convergence_events";
  row: Json;
}

export interface SessionProjection {
  rows: ProjectionRow[];
  /** Records with no projection (presence, observations and the like). */
  unprojected: number;
}

export interface ProjectionContext {
  workspaceId: string;
  /** The record `source`, which is the session's harness. */
  source: string;
  sessionId: string;
  machineId: string;
  /** The session's user, from the export's provenance. */
  userId: string;
}

export const CONVERGENCE_EVENT_KIND = "convergence_event";
const ACTIVITY_KINDS = new Set([
  "history",
  "session_event",
  "tool_call",
  "file_edit",
  "trajectory",
]);
const MAX_TIME_MS = 8_640_000_000_000_000;
/** The largest value a bigint column holds. */
const MAX_COUNT = 9_223_372_036_854_775_807n;

/**
 * Every timestamp in the output: UTC to the microsecond, as Postgres writes it.
 * Accepts Postgres text (`2026-10-30 12:00:00.123456+00`, or a year past 9999,
 * which it writes bare) and ISO. Throws on anything else.
 */
export function canonicalTimestamp(value: string): string {
  const year = /^(\d{5,6})-/.exec(value)?.[1];
  const parseable = year
    ? `+${year.padStart(6, "0")}${value.slice(year.length).replace(" ", "T")}`
    : value;
  const parsed = Date.parse(parseable);
  if (!Number.isFinite(parsed)) throw new Error("invalid timestamp");
  const fraction = /:\d{2}\.(\d+)/.exec(value)?.[1] ?? "";
  const seconds = new Date(parsed)
    .toISOString()
    .replace(/\.\d+Z$/, "")
    .replace(/^\+0*/, "");
  return `${seconds}.${fraction.slice(0, 6).padEnd(6, "0")}Z`;
}

/** Order of two canonical timestamps: a longer year is a later one. */
function compareTimes(left: string, right: string): number {
  return left.length - right.length || compareStrings(left, right);
}

/** A whole-millisecond epoch time; the client's `*_ms` columns are integers. */
function timeFromMs(ms: number): string {
  return canonicalTimestamp(new Date(ms).toISOString());
}

/** `p->>'key'`. */
function text(payload: Json, key: string): string | null {
  const value = payload[key];
  if (value === undefined || value === null) return null;
  return typeof value === "object"
    ? JSON.stringify(value, (_key, child: unknown) =>
        typeof child === "bigint" ? child.toString() : child,
      )
    : String(value);
}

function nullIfEmpty(value: string | null): string | null {
  return value === "" ? null : value;
}

/**
 * `lower(agent_name)` (0067): one code point to one code point, with no
 * Unicode normalization. `toLowerCase` alone is context-sensitive and full
 * mapping, so it differs on a word-final sigma and on U+0130, the only code
 * point whose full lowercase is two.
 *
 * Advisory only: never a key, dedup input or comparison. On any write to
 * session_catalog_agents the session_catalog_agents_lookup_prepare trigger
 * replaces it with the target's own `lower()`, whose result depends on that
 * database's collation and Unicode tables. Known divergence: code points newer
 * than Postgres's table (U+A7CE, U+A7D2, U+A7D4, U+16EA0-16EB8 on PGlite's
 * pg_c_utf8), and rare ones that vary by libc.
 */
export function lowerAgentName(name: string): string {
  return Array.from(name, (point) =>
    point === "\u0130" ? "i" : point.toLowerCase(),
  ).join("");
}

/** sessions.delivery_catalog_text */
function catalogText(value: unknown): string | null {
  return ["string", "number", "bigint", "boolean"].includes(typeof value)
    ? nullIfEmpty(String(value))
    : null;
}

/** sessions.delivery_catalog_integer */
function catalogInteger(value: unknown): number | null {
  return typeof value === "number" &&
    Number.isInteger(value) &&
    value >= -2147483648 &&
    value <= 2147483647
    ? value
    : null;
}

/** sessions.delivery_catalog_time */
function catalogTime(value: unknown): string | null {
  return typeof value === "number" && value >= 0 && value <= MAX_TIME_MS
    ? timeFromMs(value)
    : null;
}

/** sessions.delivery_catalog_json */
function catalogJson(value: unknown): unknown {
  if (value !== null && typeof value === "object") return value;
  if (typeof value !== "string") return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

/** A JSON number as written, for Postgres's numeric arithmetic on it. */
class JsonNumber {
  constructor(readonly text: string) {}
}

function parseLossless(blob: string): unknown {
  return (JSON.parse as (text: string, reviver: unknown) => unknown)(
    blob,
    (_key: string, value: unknown, context: { source: string }) =>
      typeof value === "number" ? new JsonNumber(context.source) : value,
  );
}

/** The digits and power of ten a JSON number's text writes. */
function decimalParts(text: string) {
  const [, sign, whole, fraction = "", exponent = "0"] =
    /^(-?)(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/.exec(text)!;
  return {
    negative: sign === "-" && /[1-9]/.test(whole! + fraction),
    digits: BigInt(whole! + fraction),
    power: Number(exponent) - fraction.length,
  };
}

/** `digits * 10^power` for a power small enough to compute; past MAX_COUNT otherwise. */
function scaled(digits: bigint, power: number): bigint {
  if (digits === 0n) return 0n;
  return power > 20 ? MAX_COUNT + 1n : digits * 10n ** BigInt(power);
}

/**
 * `round(value * 10^places)` on a non-negative number, by its decimal text as
 * Postgres numerics do: binary arithmetic would round some halves down.
 */
function scaledRound(value: JsonNumber | number, places: number): bigint {
  const text = value instanceof JsonNumber ? value.text : String(value);
  const { digits, power } = decimalParts(text);
  const dropped = -power - places;
  if (dropped <= 0) return scaled(digits, -dropped);
  if (dropped > digits.toString().length) return 0n;
  const divisor = 10n ** BigInt(dropped);
  return (digits + divisor / 2n) / divisor;
}

/** A bigint column's value in JSON: exact, and a plain number while one can hold it. */
function exact(value: bigint): number | bigint {
  return value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : value;
}

/** sessions.delivery_activity_ms */
function activityMs(payload: Json): number | null {
  for (const key of [
    "updated_ms",
    "ts_ms",
    "timestamp_ms",
    "last_activity_ms",
  ]) {
    const value = payload[key];
    if (typeof value === "bigint") return null;
    if (typeof value === "number")
      return value >= 0 && value <= MAX_TIME_MS ? value : null;
  }
  return null;
}

/** sessions.delivery_project_id */
function projectId(payload: Json): string | null {
  const key = nullIfEmpty(
    text(payload, "project_key")?.replace(/^ +| +$/g, "") ?? null,
  );
  if (
    key !== null &&
    !["path", "path_fallback"].includes(
      text(payload, "project_key_method") ?? "",
    ) &&
    /^[^/~.\\:\s][^\\:\s]*\/[^\\:\s]+$/.test(key)
  )
    return /^[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+\/[^/]+\/./s.test(key)
      ? key.slice(key.indexOf("/") + 1)
      : key;
  return (
    nullIfEmpty(text(payload, "project_id")) ??
    text(payload, "project") ??
    text(payload, "cwd")
  );
}

/** sessions.delivery_usage_counter: throws where it raises. */
function usageCounter(value: unknown): bigint | null {
  if (value === undefined || value === null) return null;
  if (!(value instanceof JsonNumber))
    throw new Error("USAGE_NON_INTEGER_COUNTER");
  const { negative, digits, power } = decimalParts(value.text);
  if (negative) throw new Error("USAGE_NON_INTEGER_COUNTER");
  let count: bigint;
  if (power >= 0 || digits === 0n) count = scaled(digits, Math.max(power, 0));
  else {
    const divisor =
      10n ** BigInt(Math.min(-power, digits.toString().length + 1));
    if (digits % divisor !== 0n) throw new Error("USAGE_NON_INTEGER_COUNTER");
    count = digits / 10n ** BigInt(-power);
  }
  if (count > MAX_COUNT) throw new Error("USAGE_COUNT_NOT_REPRESENTABLE");
  return count;
}

/** sessions.delivery_usage_first */
function usageFirst(object: Json, keys: string[]): unknown {
  for (const key of keys)
    if (object[key] !== undefined && object[key] !== null) return object[key];
  return undefined;
}

function isObject(value: unknown): value is Json {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    !(value instanceof JsonNumber)
  );
}

interface Usage {
  input: bigint;
  output: bigint;
  reasoning: bigint;
  cacheRead: bigint;
  cacheCreate: bigint;
  cacheCreate5m: bigint | null;
  cacheCreate1h: bigint | null;
  costUsdMicros: bigint | null;
}

/**
 * sessions.delivery_normalized_usage. Evidence that cannot be normalized and
 * no evidence both settle to no totals, so both are null here.
 */
function normalizedUsage(source: string, blob: string | null): Usage | null {
  if (blob === null || !["claude", "codex", "grok", "muse"].includes(source))
    return null;
  let token: unknown;
  try {
    token = parseLossless(blob);
  } catch {
    return null;
  }
  if (!isObject(token)) return null;
  let input: bigint | null = null;
  let output: bigint | null;
  let reasoning: bigint | null;
  let cacheRead: bigint | null = null;
  let cacheWrite: bigint | null = null;
  let write5m: bigint | null = null;
  let write1h: bigint | null = null;
  let reported: boolean;
  try {
    output = usageCounter(token.output_tokens);
    reasoning = usageCounter(
      source === "muse"
        ? token.reasoning_tokens
        : token.reasoning_output_tokens,
    );
    const total = usageCounter(token.total_tokens);
    reported = output !== null || reasoning !== null || total !== null;
    if (source === "codex" || source === "muse") {
      input = usageCounter(token.input_tokens);
      if (source === "codex") {
        cacheRead = usageCounter(token.cached_input_tokens);
        cacheWrite = usageCounter(token.cache_write_input_tokens);
      } else {
        cacheRead =
          usageCounter(token.cache_read_tokens) ??
          usageCounter(token.cached_tokens);
        cacheWrite = usageCounter(token.cache_write_tokens);
      }
      reported ||= input !== null || cacheRead !== null || cacheWrite !== null;
      if ((cacheRead ?? 0n) > (input ?? 0n)) return null;
      input = (input ?? 0n) - (cacheRead ?? 0n);
    } else if (source === "claude") {
      input = usageCounter(token.input_tokens);
      cacheRead = usageCounter(token.cache_read_input_tokens);
      cacheWrite = usageCounter(token.cache_creation_input_tokens);
      if (isObject(token.cache_creation)) {
        write5m = usageCounter(token.cache_creation.ephemeral_5m_input_tokens);
        write1h = usageCounter(token.cache_creation.ephemeral_1h_input_tokens);
      }
      reported ||=
        input !== null ||
        cacheRead !== null ||
        cacheWrite !== null ||
        write5m !== null ||
        write1h !== null;
      cacheWrite ??= (write5m ?? 0n) + (write1h ?? 0n);
    } else {
      if (!Object.hasOwn(token, "usage")) return null;
      const breakdown = token.usage;
      if (!isObject(breakdown)) return null;
      const first = (...keys: string[]) =>
        usageCounter(usageFirst(breakdown, keys));
      input = first("inputTokens", "input_tokens", "promptTokens");
      output = first("outputTokens", "output_tokens", "completionTokens");
      cacheRead = first(
        "cachedReadTokens",
        "cacheReadTokens",
        "cache_read_input_tokens",
      );
      cacheWrite = first(
        "cachedWriteTokens",
        "cacheWriteTokens",
        "cacheCreationTokens",
        "cache_creation_input_tokens",
      );
      reasoning = first("reasoningTokens", "thoughtTokens", "thinkingTokens");
      const grokTotal = first("totalTokens", "total_tokens");
      reported =
        input !== null ||
        output !== null ||
        cacheRead !== null ||
        cacheWrite !== null ||
        reasoning !== null ||
        grokTotal !== null;
      if (input !== null) {
        if ((cacheRead ?? 0n) > input) return null;
        input -= cacheRead ?? 0n;
      }
    }
  } catch {
    return null;
  }
  const cost = usageFirst(token, ["cost_usd", "costUSD", "total_cost_usd"]);
  if (cost !== undefined) {
    if (!(cost instanceof JsonNumber) || decimalParts(cost.text).negative)
      return null;
    reported = true;
  }
  if (!reported) return null;
  const costUsdMicros =
    cost === undefined ? null : scaledRound(cost as JsonNumber, 6);
  for (const count of [
    input,
    output,
    reasoning,
    cacheRead,
    cacheWrite,
    costUsdMicros,
  ])
    if ((count ?? 0n) > MAX_COUNT) return null;
  return {
    input: input ?? 0n,
    output: output ?? 0n,
    reasoning: reasoning ?? 0n,
    cacheRead: cacheRead ?? 0n,
    cacheCreate: cacheWrite ?? 0n,
    cacheCreate5m: write5m,
    cacheCreate1h: write1h,
    costUsdMicros,
  };
}

/** The machine a delivery origin's events live on. */
export function deliveryMachine(workspaceId: string, originId: string): string {
  const key = `[${JSON.stringify(workspaceId)}, ${JSON.stringify(originId)}]`;
  return `delivery:${createHash("md5").update(key).digest("hex")}`;
}

function controlRow(kind: string, payload: Json): boolean {
  if (kind !== "session_event") return false;
  if (nullIfEmpty(text(payload, "control_kind")) !== null) return true;
  return ["1", "true"].includes(text(payload, "is_meta") ?? "");
}

/** sessions.delivery_sidechain_user_row */
function sidechainUserRow(kind: string, payload: Json): boolean {
  return (
    kind === "session_event" &&
    text(payload, "role") === "user" &&
    ["1", "true"].includes(text(payload, "is_sidechain") ?? "")
  );
}

function contentText(kind: string, payload: Json): string | null {
  switch (kind) {
    case "history":
      return text(payload, "prompt");
    case "session_event":
      return text(payload, "text");
    case "tool_call":
      return [text(payload, "name"), text(payload, "target")]
        .filter((part) => part !== null)
        .join(" ");
    case "file_edit":
      return text(payload, "file_path");
    case "trajectory":
      return text(payload, "task_description") ?? text(payload, "search_text");
    default:
      return null;
  }
}

function requestIdentity(kind: string, payload: Json): string | null {
  const recordId = nullIfEmpty(text(payload, "message_id"));
  if (
    kind !== "session_event" ||
    text(payload, "role") !== "assistant" ||
    !recordId
  )
    return null;
  const field = (key: string) => nullIfEmpty(text(payload, key));
  const requestId = field("request_id");
  if (requestId) return `request-id:${requestId}`;
  const providerId = field("provider_message_id");
  if (providerId) return `provider-message-id:${providerId}`;
  const span = field("request_span");
  return span ? `request-span:${span}` : `record-id:${recordId}`;
}

const MAX_WRITTEN_PATHS = 64;
const MAX_WRITTEN_PATH_CHARS = 512;
const MAX_EDIT_INPUT_CHARS = 1024 * 1024;
const PATCH_PATH_HEADERS = [
  "*** Add File: ",
  "*** Update File: ",
  "*** Delete File: ",
  "*** Move to: ",
] as const;
function validWrittenPath(path: string): boolean {
  return (
    path.length > 0 &&
    path.length <= MAX_WRITTEN_PATH_CHARS &&
    path.trim() === path &&
    !/[\u0000-\u001f\u007f]/.test(path)
  );
}

function normalizedToolName(name: string): string {
  return (name.split(/[.:/]/).at(-1) ?? "")
    .replaceAll(/[_-]/g, "")
    .toLowerCase();
}

/** A static JavaScript string literal and the first byte after it. */
function jsString(
  source: string,
  start: number,
): { value: string; end: number } | null {
  const quote = source[start];
  if (quote !== '"' && quote !== "'" && quote !== "`") return null;
  const value: string[] = [];
  for (let at = start + 1; at < source.length; at += 1) {
    const char = source[at]!;
    if (char === quote) return { value: value.join(""), end: at + 1 };
    if (quote === "`" && char === "$" && source[at + 1] === "{") return null;
    if (char !== "\\") {
      value.push(char);
      continue;
    }
    at += 1;
    if (at >= source.length) return null;
    const escaped = source[at]!;
    const simple: Record<string, string> = {
      b: "\b",
      f: "\f",
      n: "\n",
      r: "\r",
      t: "\t",
      v: "\v",
      "0": "\0",
    };
    if (simple[escaped] !== undefined) {
      value.push(simple[escaped]);
      continue;
    }
    if (escaped === "\n") continue;
    if (escaped === "\r") {
      if (source[at + 1] === "\n") at += 1;
      continue;
    }
    if (escaped === "x") {
      const digits = source.slice(at + 1, at + 3);
      if (!/^[0-9a-f]{2}$/i.test(digits)) return null;
      value.push(String.fromCharCode(Number.parseInt(digits, 16)));
      at += 2;
      continue;
    }
    if (escaped === "u") {
      const braced = /^\{([0-9a-f]{1,6})\}/i.exec(source.slice(at + 1, at + 9));
      if (braced) {
        const point = Number.parseInt(braced[1]!, 16);
        if (point > 0x10ffff) return null;
        value.push(String.fromCodePoint(point));
        at += braced[0].length;
        continue;
      }
      const digits = source.slice(at + 1, at + 5);
      if (!/^[0-9a-f]{4}$/i.test(digits)) return null;
      value.push(String.fromCharCode(Number.parseInt(digits, 16)));
      at += 4;
      continue;
    }
    // JavaScript's identity escapes, including escaped quotes and backslashes.
    value.push(escaped);
  }
  return null;
}

const identifierStart = (char: string | undefined) =>
  char !== undefined && /[A-Za-z_$]/.test(char);
const identifierPart = (char: string | undefined) =>
  char !== undefined && /[A-Za-z0-9_$]/.test(char);
const skipSpace = (source: string, start: number) => {
  let at = start;
  while (/\s/.test(source[at] ?? "")) at += 1;
  return at;
};
function identifier(source: string, start: number) {
  if (!identifierStart(source[start])) return null;
  let end = start + 1;
  while (identifierPart(source[end])) end += 1;
  return { value: source.slice(start, end), end };
}

/** Static patch strings passed to code-mode's `tools.apply_patch`. */
function execApplyPatchInputs(source: string): string[] {
  const variables = new Map<string, string>();
  const inputs: string[] = [];
  for (let at = 0; at < source.length;) {
    if (source.startsWith("//", at)) {
      const end = source.indexOf("\n", at + 2);
      at = end < 0 ? source.length : end + 1;
      continue;
    }
    if (source.startsWith("/*", at)) {
      const end = source.indexOf("*/", at + 2);
      at = end < 0 ? source.length : end + 2;
      continue;
    }
    const literal = jsString(source, at);
    if (literal) {
      at = literal.end;
      continue;
    }
    if (['"', "'", "`"].includes(source[at] ?? "")) return inputs;
    const token = identifier(source, at);
    if (!token) {
      at += 1;
      continue;
    }
    if (["const", "let", "var"].includes(token.value)) {
      const nameAt = skipSpace(source, token.end);
      const name = identifier(source, nameAt);
      const equals = name ? skipSpace(source, name.end) : nameAt;
      const valueAt = skipSpace(source, equals + 1);
      if (name && source[equals] === "=") {
        const value = jsString(source, valueAt);
        if (value) {
          variables.set(name.value, value.value);
          at = value.end;
          continue;
        }
        if (['"', "'", "`"].includes(source[valueAt] ?? "")) return inputs;
      }
    } else if (token.value === "tools") {
      let cursor = skipSpace(source, token.end);
      if (source[cursor] === ".") cursor = skipSpace(source, cursor + 1);
      else {
        at = token.end;
        continue;
      }
      const method = identifier(source, cursor);
      cursor = method ? skipSpace(source, method.end) : cursor;
      if (method?.value === "apply_patch" && source[cursor] === "(") {
        const argumentAt = skipSpace(source, cursor + 1);
        const argument = jsString(source, argumentAt);
        if (argument) inputs.push(argument.value);
        else {
          if (['"', "'", "`"].includes(source[argumentAt] ?? "")) return inputs;
          const name = identifier(source, argumentAt);
          const value = name ? variables.get(name.value) : undefined;
          if (value !== undefined) inputs.push(value);
        }
      }
    }
    at = token.end;
  }
  return inputs;
}

function patchPaths(inputs: readonly string[]): string[] {
  const paths: string[] = [];
  const seen = new Set<string>();
  for (const raw of inputs) {
    if (raw.length > MAX_EDIT_INPUT_CHARS) continue;
    let inside = false;
    let pending: string[] = [];
    let pendingSeen = new Set<string>();
    for (const line of raw
      .replace(/\r\n/g, "\n")
      .replace(/\r/g, "\n")
      .split("\n")) {
      if (line === "*** Begin Patch") {
        inside = true;
        pending = [];
        pendingSeen = new Set<string>();
        continue;
      }
      if (line === "*** End Patch") {
        if (inside) {
          for (const path of pending) {
            seen.add(path);
            paths.push(path);
          }
          if (paths.length === MAX_WRITTEN_PATHS) return paths;
        }
        inside = false;
        pending = [];
        pendingSeen = new Set<string>();
        continue;
      }
      if (!inside || pending.length + paths.length === MAX_WRITTEN_PATHS)
        continue;
      const header = PATCH_PATH_HEADERS.find((prefix) =>
        line.startsWith(prefix),
      );
      if (!header) continue;
      const path = line.slice(header.length);
      if (!validWrittenPath(path) || seen.has(path) || pendingSeen.has(path))
        continue;
      pendingSeen.add(path);
      pending.push(path);
    }
  }
  return paths;
}

function writtenPaths(
  source: string,
  name: string | null,
  args: unknown,
): string[] {
  if (!isObject(args)) return [];
  // Code-mode capture sees only the outer exec, not its nested apply_patch.
  if (
    name === "exec" &&
    typeof args.input === "string" &&
    args.input.length <= MAX_EDIT_INPUT_CHARS
  )
    return patchPaths(execApplyPatchInputs(args.input));
  const filePath = args.file_path;
  // Claude capture emits file_edit for Edit/Write/NotebookEdit, but not MultiEdit.
  if (
    source === "claude" &&
    name !== null &&
    typeof filePath === "string" &&
    normalizedToolName(name) === "multiedit" &&
    validWrittenPath(filePath)
  )
    return [filePath];
  return [];
}

function toolCall(payload: Json, toolUse: string | null, source: string) {
  const raw = text(payload, "is_error");
  const isError = ["1", "true"].includes(raw ?? "")
    ? true
    : ["0", "false"].includes(raw ?? "")
      ? false
      : null;
  let args: unknown = null;
  try {
    args = JSON.parse(text(payload, "args_json") ?? "null");
  } catch {
    args = null;
  }
  const status = isError === null ? null : isError ? "error" : "success";
  const call = Object.fromEntries(
    Object.entries({
      toolUseId: toolUse,
      name: text(payload, "name"),
      target: text(payload, "target"),
      args,
      isError,
      status,
    }).filter(([, value]) => value !== null),
  );
  return {
    call,
    status,
    writtenPaths:
      isError === true ? [] : writtenPaths(source, text(payload, "name"), args),
  };
}

function activityEvent(
  record: RelayhistoryStreamRecord,
  context: ProjectionContext,
): Json {
  const { kind, payload: maybePayload } = record;
  const p = maybePayload!;
  const received = canonicalTimestamp(record.received_at);
  const machine = deliveryMachine(context.workspaceId, record.origin_id);
  const ms = activityMs(p);
  const tool =
    kind === "tool_call"
      ? toolCall(p, nullIfEmpty(text(p, "tool_use_id")), context.source)
      : null;
  const filePath =
    kind === "file_edit" ? nullIfEmpty(text(p, "file_path")) : null;
  return {
    machine_id: machine,
    source: context.source,
    lens: null,
    session_id: context.sessionId,
    event_id: `${machine}:${record.record_id}`,
    kind,
    type: text(p, "role") ?? kind,
    ts: ms === null ? received : timeFromMs(ms),
    actor_name: null,
    actor_role: kind === "history" ? "user" : text(p, "role"),
    subagent_id: nullIfEmpty(text(p, "subagent_session_id")),
    trajectory_id: null,
    chapter_id: null,
    project_id: projectId(p),
    project_id_original: null,
    generated_at: null,
    workflow_id: null,
    task_ref: {},
    task_title: kind === "trajectory" ? text(p, "task_title") : null,
    task_description: null,
    task_status: null,
    content: contentText(kind, p),
    significance: null,
    confidence_basis_points: null,
    tags: [],
    model: text(p, "model"),
    provider: nullIfEmpty(text(p, "provider")),
    input_tokens: 0,
    output_tokens: 0,
    reasoning_tokens: 0,
    cache_read_tokens: 0,
    cache_create_tokens: 0,
    cache_create_5m_tokens: null,
    cache_create_1h_tokens: null,
    cost_usd_micros: null,
    tool_name: kind === "tool_call" ? text(p, "name") : null,
    tool_status: tool?.status ?? null,
    tool_calls: tool ? [tool.call] : [],
    retries: 0,
    duration_ms: null,
    files_touched: filePath ? [filePath] : (tool?.writtenPaths ?? []),
    code_churn: {},
    delivery_record_id: record.record_id,
    request_key: requestIdentity(kind, p),
    record: { deliveryRecordId: record.record_id, payload: p },
    ingested_at: received,
  };
}

const payloadOf = (event: Json) => (event.record as { payload: Json }).payload;

/** sessions.settle_delivery_tool_ts, for every tool at once. */
function settleToolTimes(events: Json[]) {
  const parents = new Map<string, string>();
  for (const event of events) {
    const toolUse = text(payloadOf(event), "tool_use_id");
    if (event.kind !== "session_event" || toolUse === null) continue;
    const seen = parents.get(toolUse);
    if (seen === undefined || compareTimes(event.ts as string, seen) < 0)
      parents.set(toolUse, event.ts as string);
  }
  for (const event of events) {
    const payload = payloadOf(event);
    const toolUse = nullIfEmpty(text(payload, "tool_use_id"));
    if (
      (event.kind === "tool_call" || event.kind === "file_edit") &&
      toolUse !== null &&
      activityMs(payload) === null
    )
      event.ts = parents.get(toolUse) ?? event.ingested_at;
  }
}

/**
 * sessions.settle_delivery_request, for every request at once. The carrier is
 * the smallest record id; SQL's `min` takes the database collation and this the
 * code-unit order, which can pick another member but not change the totals.
 */
function settleRequests(events: Json[], source: string) {
  const groups = new Map<string, Json[]>();
  for (const event of events) {
    const key = event.request_key as string | null;
    if (key === null) continue;
    const group = groups.get(key);
    if (group) group.push(event);
    else groups.set(key, [event]);
  }
  for (const [key, group] of groups) {
    const blobs = new Set(
      group
        .map((event) => text(payloadOf(event), "token_json"))
        .filter((blob) => blob !== null),
    );
    if (
      blobs.size !== 1 ||
      (source === "claude" && key.startsWith("record-id:"))
    )
      continue;
    const usage = normalizedUsage(source, [...blobs][0]!);
    if (!usage) continue;
    const carrier = group.reduce((least, event) =>
      (event.delivery_record_id as string) <
      (least.delivery_record_id as string)
        ? event
        : least,
    );
    const nullable = (value: bigint | null) =>
      value === null ? null : exact(value);
    Object.assign(carrier, {
      input_tokens: exact(usage.input),
      output_tokens: exact(usage.output),
      reasoning_tokens: exact(usage.reasoning),
      cache_read_tokens: exact(usage.cacheRead),
      cache_create_tokens: exact(usage.cacheCreate),
      cache_create_5m_tokens: nullable(usage.cacheCreate5m),
      cache_create_1h_tokens: nullable(usage.cacheCreate1h),
      cost_usd_micros: nullable(usage.costUsdMicros),
    });
  }
}

/** sessions.delivery_catalog_key: the row's key within the session, or null. */
function catalogKey(record: RelayhistoryStreamRecord): string | null {
  const p = record.payload!;
  const parts = {
    session: [],
    relationship: [p.relationship_uid],
    session_marker: [p.marker_uid],
    commit_link: [p.commit_sha, p.match_method],
  }[record.kind];
  if (!parts) return null;
  const text = parts.map(catalogText);
  return text.includes(null) ? null : JSON.stringify([record.kind, ...text]);
}

/** sessions.project_delivery_catalog_row */
function catalogRows(
  record: RelayhistoryStreamRecord,
  context: ProjectionContext,
): ProjectionRow[] {
  const p = record.payload!;
  const identity = {
    source: context.source,
    record_id: record.record_id,
    origin_id: record.origin_id,
    received_at: canonicalTimestamp(record.received_at),
  };
  switch (record.kind) {
    case "session": {
      const agent = catalogText(p.agent_name);
      return [
        {
          table: "session_catalog",
          row: {
            ...identity,
            session_id: context.sessionId,
            title: catalogText(p.title),
            cwd: catalogText(p.cwd),
            git_branch: catalogText(p.git_branch),
            repo_url: catalogText(p.repo_url),
            initial_commit: catalogText(p.initial_commit),
            first_prompt: catalogText(p.first_prompt),
            models: catalogJson(p.models_json),
            originator: catalogText(p.originator),
            agent_version: catalogText(p.agent_version),
            workspace_roots: catalogJson(p.workspace_roots_json),
            project_key: catalogText(p.project_key),
            project_key_method: catalogText(p.project_key_method),
            discovery_state: catalogText(p.discovery_state),
            first_activity_at: catalogTime(p.first_activity_ms),
            last_activity_at: catalogTime(p.last_activity_ms),
          },
        },
        ...(agent === null
          ? []
          : [
              {
                table: "session_catalog_agents" as const,
                row: {
                  source: context.source,
                  session_id: context.sessionId,
                  agent_name: agent,
                  // `last_ts` is left out: session_rollups owns it.
                  agent_name_normalized: lowerAgentName(agent),
                  record_id: record.record_id,
                },
              },
            ]),
      ];
    }
    case "relationship":
      return [
        {
          table: "session_relationships",
          row: {
            ...identity,
            parent_session_id: context.sessionId,
            relationship_uid: catalogText(p.relationship_uid),
            child_session_id: catalogText(p.child_session_id),
            relationship: catalogText(p.relationship),
            identity_status: catalogText(p.identity_status),
            child_agent_type: catalogText(p.child_agent_type),
            child_agent_name: catalogText(p.child_agent_name),
            child_model: catalogText(p.child_model),
            spawn_depth: catalogInteger(p.spawn_depth),
            evidence_kind: catalogText(p.evidence_kind),
            child_has_events:
              typeof p.child_has_events === "boolean"
                ? p.child_has_events
                : catalogInteger(p.child_has_events) === null
                  ? null
                  : p.child_has_events !== 0,
            spawned_at: catalogTime(p.spawned_at_ms),
            updated_at: catalogTime(p.updated_ms),
          },
        },
      ];
    case "session_marker":
      return [
        {
          table: "session_markers",
          row: {
            ...identity,
            session_id: context.sessionId,
            marker_uid: catalogText(p.marker_uid),
            marker_kind: catalogText(p.kind),
            subkind: catalogText(p.subkind),
            ts: catalogTime(p.ts_ms),
            message_id: catalogText(p.message_id),
            parent_id: catalogText(p.parent_id),
            turn_id: catalogText(p.turn_id),
            text: catalogText(p.text),
            payload: catalogJson(p.payload_json),
          },
        },
      ];
    case "commit_link": {
      const confidence = p.confidence;
      return [
        {
          table: "session_commit_links",
          row: {
            ...identity,
            session_id: context.sessionId,
            commit_sha: catalogText(p.commit_sha),
            match_method: catalogText(p.match_method),
            repo: catalogText(p.repo),
            branch: catalogText(p.branch),
            note_ref: catalogText(p.note_ref),
            confidence_basis_points:
              typeof confidence === "number" &&
              confidence >= 0 &&
              confidence <= 1
                ? Number(scaledRound(confidence, 4))
                : null,
            files: catalogJson(p.files_json),
            numstat: catalogJson(p.numstat_json),
            evidence: catalogJson(p.evidence_json),
            linked_at: catalogTime(p.created_at_ms),
          },
        },
      ];
    }
    default:
      return [];
  }
}

/**
 * A `convergence_event` line is already a row, but it comes from the stream, so
 * its identity must be the session the manifest and its provenance name. Only
 * its times are canonicalized.
 */
function directEvent(
  record: RelayhistoryStreamRecord,
  context: ProjectionContext,
): Json {
  const {
    org_id: _org,
    workspace_id: _workspace,
    embedding: _embedding,
    ts_ms: _ts,
    ...row
  } = record.payload!;
  if (
    row.source !== context.source ||
    row.session_id !== context.sessionId ||
    row.machine_id !== context.machineId ||
    row.user_id !== context.userId ||
    row.event_id !== record.record_id ||
    record.origin_id !== `machine:${context.machineId}`
  )
    throw new PipelineError(
      "relayhistory_event_identity",
      "a convergence_event line is not this session's",
      false,
    );
  try {
    return {
      ...row,
      ts: canonicalTimestamp(String(row.ts)),
      ingested_at: canonicalTimestamp(
        String(row.ingested_at ?? record.received_at),
      ),
    };
  } catch {
    throw new PipelineError(
      "relayhistory_event_invalid",
      "a convergence_event line has a malformed timestamp",
      false,
    );
  }
}

function compareStrings(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

/**
 * Project one session's surviving records (`normalizeRelayhistoryRecords`):
 * catalog rows first, then its events in the readers' `(ts, event_id)` order.
 */
export function projectRelayhistorySession(
  records: readonly RelayhistoryStreamRecord[],
  context: ProjectionContext,
): SessionProjection {
  const events: Json[] = [];
  const direct: Json[] = [];
  const catalog = new Map<string, RelayhistoryStreamRecord>();
  let unprojected = 0;
  for (const record of records) {
    const p = record.payload!;
    if (record.kind === CONVERGENCE_EVENT_KIND)
      direct.push(directEvent(record, context));
    else if (ACTIVITY_KINDS.has(record.kind)) {
      if (!controlRow(record.kind, p) && !sidechainUserRow(record.kind, p))
        events.push(activityEvent(record, context));
    } else {
      const key = catalogKey(record);
      if (key === null) {
        unprojected += 1;
        continue;
      }
      const held = catalog.get(key);
      catalog.set(key, held ? lastAccepted(held, record) : record);
    }
  }
  settleToolTimes(events);
  settleRequests(events, context.source);
  const ordered = [...events, ...direct].sort(
    (a, b) =>
      compareTimes(a.ts as string, b.ts as string) ||
      compareStrings(a.event_id as string, b.event_id as string),
  );
  return {
    rows: [
      ...[...catalog.values()].flatMap((record) =>
        catalogRows(record, context),
      ),
      ...ordered.map((row) => ({ table: "convergence_events" as const, row })),
    ],
    unprojected,
  };
}
