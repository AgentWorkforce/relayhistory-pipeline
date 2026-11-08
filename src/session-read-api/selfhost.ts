/**
 * The self-host read surface relaycontrol proxies History reads through
 * (RELAYHISTORY_SELFHOST_READ_API, default off; docs/session-read-api-design.md
 * "Self-host login and catalog"):
 *
 *   POST /v1/cli/login                exchange a relaycontrol `sh_at_` token for a
 *                                     read session (pass-through, see app.ts)
 *   GET  /v1/sessions/:id/catalog     the session's catalog, children and commit
 *                                     links from its canonical S3 objects
 *
 * Off (unset, "" or "false"), neither route is registered and the app is the
 * same as without this file. Runtime-neutral: no AWS or Node imports.
 */
import type { CanonicalIdentity } from "./canonical.ts";
import type { CanonicalRows } from "./session-timing-source.ts";
import type { VerifiedIdentity } from "./app.ts";

export const SELFHOST_READ_API_ENV = "RELAYHISTORY_SELFHOST_READ_API";

/** Strict: unset, "" or "false" is off, exactly "true" is on, anything else throws at startup. Never echoes the value. */
export function parseSelfhostReadApi(env: Record<string, unknown>): boolean {
  const raw = env[SELFHOST_READ_API_ENV];
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw === "true") return true;
  throw new Error(`session read api: ${SELFHOST_READ_API_ENV} must be unset, "", "true" or "false"`);
}

/** What a successful login exchange proved about the presented token. */
export interface LoginGrant {
  identity: VerifiedIdentity;
  /** The token's own expiry (relaycontrol's `exp`), epoch ms. */
  expiresAtMs: number;
}

/** Introspects the presented token; throws AuthError (401/403/503) to refuse. */
export type LoginExchange = (agentRelayToken: string) => Promise<LoginGrant>;

/** The session's canonical rows, as the timing route reads them (TimingDeps without its timing options). */
export interface CatalogDeps {
  /** Pointer rows of the session, live or tombstoned (tenant-bound, read role). */
  state(identity: CanonicalIdentity): Promise<{ pointers: number; live: number }>;
  /** The session's canonical projection; `signal` aborts its object reads at the deadline. */
  source(identity: CanonicalIdentity, signal: AbortSignal): Promise<CanonicalRows>;
  /** Default 3000. */
  deadlineMs?: number;
}

export interface SelfhostDeps {
  login: LoginExchange;
  /** Absent: the catalog route answers 501 `catalog_not_configured`. */
  catalog?: CatalogDeps;
  /** Epoch ms; tests pin it. */
  now?: () => number;
}

/** The access token's advertised lifetime is capped at this (pass-through; see app.ts). */
export const LOGIN_MAX_TTL_MS = 15 * 60 * 1000;
export const LOGIN_MAX_BODY_BYTES = 8 * 1024;
export const LOGIN_MAX_TOKEN_CHARS = 4096;
export const LOGIN_MAX_LABEL_CHARS = 128;
/** Per array; more sets `truncated`. Same bound as the directory's relationship edges. */
export const CATALOG_MAX_ITEMS = 500;

export class LoginBodyError extends Error {
  constructor(readonly code: "invalid_body" | "invalid_agent_relay_token" | "invalid_label", message: string) {
    super(message);
  }
}

/** `{ agentRelayToken, label? }` and nothing else. Messages never echo a value. */
export function parseLoginBody(text: string): { agentRelayToken: string; label?: string } {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    throw new LoginBodyError("invalid_body", "body must be a JSON object");
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new LoginBodyError("invalid_body", "body must be a JSON object");
  const body = value as Record<string, unknown>;
  for (const key of Object.keys(body))
    if (key !== "agentRelayToken" && key !== "label") throw new LoginBodyError("invalid_body", "body has an unknown field");
  const token = body.agentRelayToken;
  if (typeof token !== "string" || token.length < 1 || token.length > LOGIN_MAX_TOKEN_CHARS)
    throw new LoginBodyError("invalid_agent_relay_token", "agentRelayToken must be a non-empty string");
  if (body.label === undefined) return { agentRelayToken: token };
  const label = body.label;
  // eslint-disable-next-line no-control-regex
  if (typeof label !== "string" || label.length < 1 || label.length > LOGIN_MAX_LABEL_CHARS || /[\u0000-\u001f\u007f]/u.test(label))
    throw new LoginBodyError("invalid_label", `label must be 1 to ${LOGIN_MAX_LABEL_CHARS} printable characters`);
  return { agentRelayToken: token, label };
}

/** A canonical row outside the requested session: not answerable exactly. */
export class CatalogInputError extends Error {
  constructor(readonly reason: string) {
    super("catalog input refused");
  }
}

type Row = Record<string, unknown>;

const CATALOG_FIELDS = [
  "source", "session_id", "title", "cwd", "git_branch", "repo_url", "initial_commit", "first_prompt", "models",
  "originator", "agent_version", "workspace_roots", "project_key", "project_key_method", "discovery_state",
  "first_activity_at", "last_activity_at", "received_at",
] as const;
const CHILD_FIELDS = [
  "source", "parent_session_id", "relationship_uid", "child_session_id", "relationship", "identity_status",
  "child_agent_type", "child_agent_name", "child_model", "spawn_depth", "evidence_kind", "child_has_events",
  "spawned_at", "updated_at", "received_at",
] as const;
const COMMIT_FIELDS = [
  "source", "session_id", "commit_sha", "match_method", "repo", "branch", "note_ref", "confidence_basis_points",
  "files", "numstat", "evidence", "linked_at", "received_at",
] as const;
/** Column → item key where Cloud's schema names the property differently from the camelCased column. */
const RENAMES: Record<string, string> = { confidence_basis_points: "confidence" };

const camel = (column: string) => RENAMES[column] ?? column.replace(/_([a-z])/gu, (_, ch: string) => ch.toUpperCase());

/** Only the listed columns (camelCased, Cloud's item keys); delivery provenance (record/origin ids) never leaves. */
function pick(row: Row, fields: readonly string[]): Row {
  const out: Row = {};
  for (const field of fields) out[camel(field)] = row[field] ?? null;
  return out;
}

const text = (v: unknown) => (typeof v === "string" ? v : "");
/** Later delivery wins, as relayhistory's upsert on the same key: received_at, then record_id. */
const newer = (a: Row, b: Row) => text(a.received_at) > text(b.received_at) || (text(a.received_at) === text(b.received_at) && text(a.record_id) > text(b.record_id));

function latestBy(rows: Row[], key: (row: Row) => string): Row[] {
  const byKey = new Map<string, Row>();
  for (const row of rows) {
    const k = key(row);
    const seen = byKey.get(k);
    if (!seen || newer(row, seen)) byKey.set(k, row);
  }
  return [...byKey.values()];
}

export interface CatalogPayload {
  catalog: Row | null;
  parents: Row[];
  children: Row[];
  commitLinks: Row[];
  truncated: boolean;
}

export const EMPTY_CATALOG: Readonly<CatalogPayload> = Object.freeze({ catalog: null, parents: [], children: [], commitLinks: [], truncated: false });

/**
 * The session's catalog from its canonical projection rows (relayhistory-projection.ts
 * `session`, `relationship` and `commit_link` records). `parents` is always empty: a
 * parent edge is delivered with the PARENT session's records, so this session's own
 * objects cannot answer it.
 */
export function catalogFromCanonical(scope: CanonicalIdentity, snapshot: CanonicalRows): CatalogPayload {
  const catalogs: Row[] = [];
  const agents: Row[] = [];
  const edges: Row[] = [];
  const commits: Row[] = [];
  for (const { table, row } of snapshot.rows) {
    if (table === "session_catalog" || table === "session_catalog_agents" || table === "session_commit_links") {
      if (row.session_id !== scope.sessionId || row.source !== scope.source) throw new CatalogInputError("row_scope");
      (table === "session_catalog" ? catalogs : table === "session_catalog_agents" ? agents : commits).push(row);
    } else if (table === "session_relationships") {
      if (row.parent_session_id !== scope.sessionId || row.source !== scope.source) throw new CatalogInputError("row_scope");
      edges.push(row);
    }
  }
  const [catalogRow] = latestBy(catalogs, () => "");
  const [agentRow] = latestBy(agents, () => "");
  const catalog = catalogRow
    ? { ...pick(catalogRow, CATALOG_FIELDS), agentName: typeof agentRow?.agent_name === "string" ? agentRow.agent_name : null }
    : null;
  const children = latestBy(edges, (r) => (typeof r.relationship_uid === "string" ? `u:${r.relationship_uid}` : `r:${text(r.record_id)}`))
    .sort((a, b) => text(a.spawned_at).localeCompare(text(b.spawned_at)) || text(a.relationship_uid).localeCompare(text(b.relationship_uid)))
    .map((r) => pick(r, CHILD_FIELDS));
  const commitLinks = latestBy(commits, (r) => JSON.stringify([r.commit_sha ?? null, r.match_method ?? null]))
    .sort((a, b) => text(a.linked_at).localeCompare(text(b.linked_at)) || text(a.commit_sha).localeCompare(text(b.commit_sha)))
    .map((r) => pick(r, COMMIT_FIELDS));
  const truncated = children.length > CATALOG_MAX_ITEMS || commitLinks.length > CATALOG_MAX_ITEMS;
  return { catalog, parents: [], children: children.slice(0, CATALOG_MAX_ITEMS), commitLinks: commitLinks.slice(0, CATALOG_MAX_ITEMS), truncated };
}
