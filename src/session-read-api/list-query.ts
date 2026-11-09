/**
 * The Dashboard's session query (`q` on GET /v1/sessions and GET /v1/sessions/activity,
 * READ_API_SESSION_QUERY=true): GitHub-like free words, `-term`, `field:value`, `@agent` and
 * date terms, as the desktop's local session search reads them. Parsed here into terms split
 * by category; supabase/migrations/20261105046010_session_directory_read_api_query.sql maps
 * each category onto the session directory row.
 *
 * Grammar (the desktop's semantics, reimplemented):
 * - Words split on whitespace outside double quotes; `"` toggles quoting unless escaped by a
 *   preceding unescaped `\`. Quotes stay in the word.
 * - A word ending in `:` takes the next word as its value when that word has no `:` or starts
 *   with `"` (`title: "a b"`).
 * - A leading `-` (with more after it) excludes the term.
 * - `field:value` for title, project, harness, source, path, id, agent, is, upload, updated,
 *   after, before (field case-insensitive); anything else is one free word, colon included.
 * - A value wrapped in `"..."` loses the quotes; `\"` inside becomes `"`.
 * - A free or agent: value starting with `@` is an agent term (the `@`s dropped).
 * - harness/source, project and upload terms OR within their field; every other term ANDs.
 *   An excluded term that matches rejects the row.
 * - Dates (UTC, exactly YYYY-MM-DD): after:D = [D+1, inf), before:D = (-inf, D), and for
 *   updated: A..B = [A, B+1) (`*` or empty = open; start < end), >=D, >D, <=D, <D, D.
 *
 * Every value is lowercased (matching is case-insensitive). What this backend cannot answer
 * is `unsupported` (a 501 naming it); malformed input is `bad` (400).
 */
import { createHash } from "node:crypto";
import { normalizeSearchQuery } from "../hoopdb/search-query.ts";

/** At most this many terms in one q (the RPC's own cap). */
export const MAX_QUERY_TERMS = 16;
/** Each term value, in code points (the RPC's own cap). */
export const MAX_QUERY_VALUE_CHARS = 200;

const FIELDS = new Set(["title", "project", "harness", "source", "path", "id", "agent", "is", "upload", "updated", "after", "before"]);
const DATE_FIELDS = new Set(["updated", "after", "before"]);
const IS_VALUES = new Set(["active", "inactive", "on-relay", "off-relay"]);
const UPLOAD_VALUES = new Set([
  "in-progress", "none", "local", "discovering", "preparing", "waiting", "queued", "uploading", "uploaded", "checking", "paused", "blocked", "unknown",
]);
const HARNESS_NAME = /^[a-z0-9][a-z0-9._-]{0,63}$/u;
/** Unicode White_Space (what the desktop splits on); JS `\s` differs at U+0085 and U+FEFF. */
const WHITESPACE = /^[\t\n\v\f\r \u0085\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]$/u;
const DAY_MS = 86_400_000;
/** Bounds past 9999-12-31 are open: Postgres cannot read the extended-year ISO form. */
const MAX_BOUND_MS = Date.UTC(9999, 11, 31, 23, 59, 59, 999);

/** Lowercased values per category; `not*` are the excluded terms. */
export interface SessionQueryTerms {
  free: string[];
  notFree: string[];
  title: string[];
  notTitle: string[];
  project: string[];
  notProject: string[];
  harness: string[];
  notHarness: string[];
  agent: string[];
  notAgent: string[];
  id: string[];
  notId: string[];
  /** is:active / -is:inactive = true; is:inactive / -is:active = false. */
  active?: boolean;
  /** The date terms' intersection on last activity: [after, before). */
  lastTsAfter?: Date;
  lastTsBefore?: Date;
}

export type ParsedSessionQuery =
  | { kind: "terms"; terms: SessionQueryTerms }
  | { kind: "bad"; reason: string }
  | { kind: "unsupported"; reason: string };

interface RawTerm {
  field: string | undefined;
  value: string;
  excluded: boolean;
}

/** The words of q: whitespace outside quotes splits; quotes stay in the word. */
export function queryWords(q: string): string[] {
  const words: string[] = [];
  let word = "";
  let quoted = false;
  let escaped = false;
  for (const c of q) {
    if (!quoted && WHITESPACE.test(c)) {
      if (word) words.push(word);
      word = "";
    } else {
      word += c;
      if (c === '"' && !escaped) quoted = !quoted;
    }
    escaped = c === "\\" && !escaped;
  }
  if (word) words.push(word);
  return words;
}

function rawTerms(q: string): RawTerm[] {
  const words = queryWords(q);
  const terms: RawTerm[] = [];
  for (let i = 0; i < words.length; i++) {
    let raw = words[i]!;
    const next = words[i + 1];
    if (raw.endsWith(":") && next !== undefined && (!next.includes(":") || next.startsWith('"'))) {
      raw += next;
      i++;
    }
    const excluded = raw.startsWith("-") && raw.length > 1;
    const body = excluded ? raw.slice(1) : raw;
    const colon = body.indexOf(":");
    const name = colon < 0 ? undefined : body.slice(0, colon).toLowerCase();
    const field = name !== undefined && FIELDS.has(name) ? name : undefined;
    let value = field === undefined ? body : body.slice(colon + 1);
    if (value.length >= 2 && value.startsWith('"') && value.endsWith('"')) value = value.slice(1, -1);
    terms.push({ field, value: value.replaceAll('\\"', '"'), excluded });
  }
  return terms;
}

/** Midnight UTC of an exact YYYY-MM-DD (years 0001-9999), else undefined. */
function day(value: string): number | undefined {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(value);
  if (!m) return undefined;
  const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])];
  if (y < 1) return undefined;
  const date = new Date(0);
  date.setUTCFullYear(y, mo - 1, d);
  if (date.getUTCFullYear() !== y || date.getUTCMonth() !== mo - 1 || date.getUTCDate() !== d) return undefined;
  return date.getTime();
}

/** [start, end) in ms (infinities = open), or undefined for a malformed date. */
export function dateBounds(value: string, field: string): [number, number] | undefined {
  const at = (v: string) => day(v);
  if (field === "after") {
    const d = at(value);
    return d === undefined ? undefined : [d + DAY_MS, Infinity];
  }
  if (field === "before") {
    const d = at(value);
    return d === undefined ? undefined : [-Infinity, d];
  }
  const range = value.indexOf("..");
  if (range >= 0) {
    const a = value.slice(0, range);
    const b = value.slice(range + 2);
    const start = a === "" || a === "*" ? -Infinity : at(a);
    const endDay = b === "" || b === "*" ? Infinity : at(b);
    if (start === undefined || endDay === undefined) return undefined;
    const end = endDay === Infinity ? Infinity : endDay + DAY_MS;
    return start < end ? [start, end] : undefined;
  }
  for (const op of [">=", "<=", ">", "<"] as const) {
    if (!value.startsWith(op)) continue;
    const d = at(value.slice(op.length));
    if (d === undefined) return undefined;
    if (op === ">=") return [d, Infinity];
    if (op === ">") return [d + DAY_MS, Infinity];
    if (op === "<=") return [-Infinity, d + DAY_MS];
    return [-Infinity, d];
  }
  const d = at(value);
  return d === undefined ? undefined : [d, d + DAY_MS];
}

const DATE_HINT = "use a date like updated:2026-10-01, updated:>=2026-10-01 or updated:2026-10-01..2026-10-11 (UTC)";

/** q parsed into directory terms, or why it cannot be served (`bad` = 400, `unsupported` = 501). */
export function parseSessionQuery(q: string): ParsedSessionQuery {
  const raw = rawTerms(q);
  if (raw.length === 0) return { kind: "bad", reason: "q has no search terms" };
  if (raw.length > MAX_QUERY_TERMS) return { kind: "bad", reason: `q accepts at most ${MAX_QUERY_TERMS} terms` };
  const bad = (reason: string): ParsedSessionQuery => ({ kind: "bad", reason });

  // The desktop's own refusals first, in its order: opposite selections, then per term.
  for (const field of ["is", "upload"] as const) {
    const chosen = raw.filter((t) => !t.excluded && t.field === field).map((t) => t.value.toLowerCase());
    for (const a of chosen) {
      for (const b of chosen) {
        const pair = (x: string, y: string) => (a === x && b === y) || (a === y && b === x);
        if (field === "is" && pair("on-relay", "off-relay")) return bad("choose either is:on-relay or is:off-relay");
        if (field === "is" && pair("active", "inactive")) return bad("choose either is:active or is:inactive");
        if (field === "upload" && (pair("local", "in-progress") || (a !== b && ["none", "discovering"].some((v) => a === v || b === v))))
          return bad("choose compatible upload filters; opposite states cannot be combined");
      }
    }
  }
  for (const t of raw) {
    if (t.field === undefined) continue;
    if (t.value === "") return bad(`add a value after ${t.field}:`);
    if (DATE_FIELDS.has(t.field) && !dateBounds(t.value, t.field)) return bad(DATE_HINT);
    if (t.field === "is" && !IS_VALUES.has(t.value.toLowerCase())) return bad("use is:active, is:inactive, is:on-relay or is:off-relay");
    if (t.field === "upload" && !UPLOAD_VALUES.has(t.value.toLowerCase())) return bad("use upload:uploaded, upload:local, upload:in-progress or upload:discovering");
  }

  // What the directory has no column for: an explicit 501, never a silent drop.
  for (const t of raw) {
    if (t.field === "path") return { kind: "unsupported", reason: "q_filter_path" };
    if (t.field === "upload") return { kind: "unsupported", reason: "q_filter_upload" };
    if (t.field === "is" && ["on-relay", "off-relay"].includes(t.value.toLowerCase()))
      return { kind: "unsupported", reason: `q_filter_is_${t.value.toLowerCase()}` };
    // NOT (a date range) is two disjoint ranges; the directory takes one bound pair.
    if (t.field !== undefined && DATE_FIELDS.has(t.field) && t.excluded) return { kind: "unsupported", reason: "q_filter_date_exclusion" };
  }

  const terms: SessionQueryTerms = {
    free: [], notFree: [], title: [], notTitle: [], project: [], notProject: [],
    harness: [], notHarness: [], agent: [], notAgent: [], id: [], notId: [],
  };
  let start = -Infinity;
  let end = Infinity;
  const put = (key: "free" | "title" | "project" | "harness" | "agent" | "id", excluded: boolean, value: string) => {
    const list = excluded ? terms[`not${key[0]!.toUpperCase()}${key.slice(1)}` as keyof SessionQueryTerms] as string[] : terms[key];
    if (!list.includes(value)) list.push(value);
  };
  for (const t of raw) {
    let field = t.field;
    let value = t.value.toLowerCase();
    if ((field === undefined || field === "agent") && value.startsWith("@")) {
      field = "agent";
      value = value.replace(/^@+/u, "");
      if (value === "") return bad("add an agent name after @");
    }
    if (field !== undefined && DATE_FIELDS.has(field)) {
      const [s, e] = dateBounds(t.value, field)!;
      start = Math.max(start, s);
      end = Math.min(end, e);
      continue;
    }
    if (field === "is") {
      const active = (value === "active") !== t.excluded;
      if (terms.active !== undefined && terms.active !== active) return bad("choose either is:active or is:inactive");
      terms.active = active;
      continue;
    }
    if (Array.from(value).length > MAX_QUERY_VALUE_CHARS) return bad(`a q term must be ${MAX_QUERY_VALUE_CHARS} characters or fewer`);
    if (field === undefined) {
      const n = normalizeSearchQuery(value);
      if (!n.ok) return bad(n.reason === "too_short" ? "a search word needs at least 3 characters" : `a search word must be ${MAX_QUERY_VALUE_CHARS} characters or fewer`);
      put("free", t.excluded, n.query);
    } else if (field === "harness" || field === "source") {
      if (!HARNESS_NAME.test(value)) return bad(`${field}: must name a harness`);
      put("harness", t.excluded, value);
    } else {
      put(field as "title" | "project" | "agent" | "id", t.excluded, value);
    }
  }
  if (start >= end) return bad("the q date terms do not overlap");
  if (start > -Infinity && start <= MAX_BOUND_MS) terms.lastTsAfter = new Date(start);
  if (start > MAX_BOUND_MS) return bad(DATE_HINT);
  if (end < Infinity && end <= MAX_BOUND_MS) terms.lastTsBefore = new Date(end);
  return { kind: "terms", terms };
}

/** The list arguments a query page also depends on (everything but the cursor and limit). */
export interface QueryFingerprintArgs {
  sources?: string[];
  notSources?: string[];
  projectContains?: string;
  userIds?: string[];
  agent?: string;
  lastTsAfter?: Date;
  lastTsBefore?: Date;
  firstTsAfter?: Date;
  firstTsBefore?: Date;
}

/**
 * The query a `b96q1:` cursor belongs to: sha256 of the canonical JSON of the parsed terms and
 * the list filters (never the cursor or limit), first 32 hex characters. A cursor is only
 * honoured for the query that produced it.
 */
export function queryFingerprint(terms: SessionQueryTerms, args: QueryFingerprintArgs): string {
  const sorted = (v: string[] | undefined) => [...(v ?? [])].sort();
  const time = (d: Date | undefined) => (d ? d.toISOString() : null);
  const canonical = [
    "b96q1",
    sorted(terms.free), sorted(terms.notFree), sorted(terms.title), sorted(terms.notTitle),
    sorted(terms.project), sorted(terms.notProject), sorted(terms.harness), sorted(terms.notHarness),
    sorted(terms.agent), sorted(terms.notAgent), sorted(terms.id), sorted(terms.notId),
    terms.active ?? null, time(terms.lastTsAfter), time(terms.lastTsBefore),
    sorted(args.sources), sorted(args.notSources), args.projectContains ?? null, sorted(args.userIds), args.agent ?? null,
    time(args.lastTsAfter), time(args.lastTsBefore), time(args.firstTsAfter), time(args.firstTsBefore),
  ];
  return createHash("sha256").update(JSON.stringify(canonical)).digest("hex").slice(0, 32);
}
