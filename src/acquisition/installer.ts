/**
 * The installer side: the four SQL entrypoints of §3.1 behind one adapter,
 * the stable SQL error contract mapped to HTTP, and the operations-port
 * handler (verify a Cloud-issued envelope, compute digests, call SQL, pass
 * the result through).
 *
 * Bigints: every function is called as `...::text`, so the result never goes
 * through pg's JSON.parse; parseSqlJson keeps every integer literal as its
 * exact decimal string (revision, ownerEpoch, tombstoneRevision, rowVersion
 * may exceed 2^53). A value that already arrives as a string stays a string.
 */
import type pg from "pg";
import { AcquisitionAssertionError, verifyAcquisitionAssertion } from "../session-ingest/acquisition-assertion.ts";
import { isIdentifier, type AcquisitionKind, type AcquisitionSqlParams, type AuthorizeScope, type IssuerRegistry } from "./validation.ts";

export type SqlFunction = "install" | "revoke" | "rebind_origin" | "status";

/** Returns the function's jsonb result as Postgres prints it (text). Throws the driver's error. */
export interface AcquisitionSqlExecutor {
  call(fn: SqlFunction, p: Readonly<Record<string, unknown>>): Promise<string>;
}

export const SQL_FUNCTIONS: Readonly<Record<SqlFunction, string>> = Object.freeze({
  install: "sessions.ingest_acquisition_install",
  revoke: "sessions.ingest_acquisition_revoke",
  rebind_origin: "sessions.ingest_acquisition_rebind_origin",
  status: "sessions.ingest_acquisition_status",
});

/** pg executor for ACQUISITION_DATABASE_URL (role bulls96_acquisition_installer). Statement text is fixed. */
export function pgAcquisitionExecutor(pool: Pick<pg.Pool, "query">): AcquisitionSqlExecutor {
  return {
    async call(fn, p) {
      const name = SQL_FUNCTIONS[fn];
      const r = await pool.query<{ result: string }>(`SELECT ${name}($1::jsonb)::text AS result`, [JSON.stringify(p)]);
      const text = r.rows[0]?.result;
      if (typeof text !== "string") throw new AcquisitionUnexpectedResult();
      return text;
    },
  };
}

export class AcquisitionUnexpectedResult extends Error {
  readonly code = "acquisition_unexpected_result";
  constructor() {
    super("acquisition SQL returned an unexpected result");
    this.name = "AcquisitionUnexpectedResult";
  }
}

/**
 * JSON.parse that keeps every integer literal as its decimal string. A
 * fractional or exponent number is refused: no acquisition result has one.
 */
export function parseSqlJson(text: string): unknown {
  let out = "";
  let i = 0;
  while (i < text.length) {
    const ch = text[i]!;
    if (ch === '"') {
      let j = i + 1;
      while (j < text.length && text[j] !== '"') j += text[j] === "\\" ? 2 : 1;
      out += text.slice(i, j + 1);
      i = j + 1;
    } else if (ch === "-" || (ch >= "0" && ch <= "9")) {
      let j = i + 1;
      while (j < text.length && /[0-9.eE+-]/.test(text[j]!)) j += 1;
      const literal = text.slice(i, j);
      if (!/^-?(0|[1-9][0-9]*)$/.test(literal)) throw new AcquisitionUnexpectedResult();
      out += `"${literal}"`;
      i = j;
    } else {
      out += ch;
      i += 1;
    }
  }
  try {
    return JSON.parse(out);
  } catch {
    throw new AcquisitionUnexpectedResult();
  }
}

function resultObject(text: string): Record<string, unknown> {
  const value = parseSqlJson(text);
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new AcquisitionUnexpectedResult();
  return value as Record<string, unknown>;
}

/** A refusal raised by an acquisition SQL function: SQLSTATE + the stable code after "acquisition: ". */
export class AcquisitionSqlError extends Error {
  constructor(readonly sqlstate: string, readonly code: string) {
    super(`acquisition: ${code}`);
    this.name = "AcquisitionSqlError";
  }
}

/** The database or its connection failed: retryable, 503. */
export class AcquisitionUnavailableError extends Error {
  readonly code = "acquisition_unavailable";
  constructor() {
    super("the acquisition database is unavailable");
    this.name = "AcquisitionUnavailableError";
  }
}

export const SQL_ERROR_CODES: Readonly<Record<string, readonly string[]>> = Object.freeze({
  "22023": ["invalid_request", "request_digest_mismatch", "acquisition_unexpected", "session_required"],
  "42501": ["not_installer", "not_owner", "target_mismatch"],
  "55000": [
    "disabled", "not_enrolled", "owner_unknown", "owner_draining", "owner_released", "revoked_before_install",
    "handover_not_ready", "grant_expired", "physical_collision", "owner_changed", "drain_not_ready",
    "enrollment_not_quiescent", "owner_other_principal",
  ],
  "40001": ["session_busy", "revision_cas_lost", "stale_revision", "predecessor_mismatch", "operation_conflict", "origin_cas_lost"],
});

/** Correction 3: only session_busy (and transport failures) are retryable. */
export function isRetryableCode(code: string): boolean {
  return code === "session_busy";
}

/** Classify a driver error. Only `acquisition: <code>` with a contract SQLSTATE becomes a refusal. */
export function classifySqlError(error: unknown): AcquisitionSqlError | AcquisitionUnavailableError | Error {
  if (error instanceof AcquisitionSqlError || error instanceof AcquisitionUnavailableError || error instanceof AcquisitionUnexpectedResult) return error;
  const e = error as { code?: unknown; message?: unknown };
  const sqlstate = typeof e?.code === "string" ? e.code : "";
  const match = typeof e?.message === "string" ? /^acquisition: ([a-z_]{1,64})$/.exec(e.message) : null;
  if (match && SQL_ERROR_CODES[sqlstate]) return new AcquisitionSqlError(sqlstate, match[1]!);
  // A Postgres SQLSTATE outside connection/resource classes is a defect, not an outage.
  if (/^[0-9A-Z]{5}$/.test(sqlstate) && !/^(08|53|57|58)/.test(sqlstate)) {
    const defect = new Error("unexpected acquisition SQL error");
    defect.name = "AcquisitionSqlDefect";
    return defect;
  }
  return new AcquisitionUnavailableError();
}

/** An envelope or status body naming another org/workspace than the introspected token (403 target_mismatch, before SQL). */
export class AcquisitionTargetMismatch extends Error {
  readonly code = "target_mismatch";
  constructor() {
    super("payload scope differs from the authenticated workspace");
    this.name = "AcquisitionTargetMismatch";
  }
}

/** The introspected scope of an operations token: a per-workspace service principal. */
export interface OpsIdentity {
  readonly orgId: string;
  readonly workspaceId: string;
}

export type HttpAnswer = { status: 200 | 400 | 403 | 409 | 500 | 503; body: Record<string, unknown> };

/**
 * The HTTP answer for a classified error. Ops port: 409 {error:{code,retryable}};
 * native exchange: 409 {error:{code:"acquisition_refused",reason,retryable}}.
 */
export function httpForError(error: unknown, group: "ops" | "native"): HttpAnswer {
  if (error instanceof AcquisitionTargetMismatch) return { status: 403, body: { error: { code: "target_mismatch" } } };
  const e = error instanceof AcquisitionAssertionError ? error : classifySqlError(error);
  if (e instanceof AcquisitionAssertionError)
    return e.code === "invalid_request"
      ? { status: 400, body: { error: { code: "invalid_request" } } }
      : { status: 403, body: { error: { code: "not_authorized" } } };
  if (e instanceof AcquisitionSqlError) {
    if (e.sqlstate === "22023") return { status: 400, body: { error: { code: e.code } } };
    if (e.sqlstate === "42501") return { status: 403, body: { error: { code: e.code } } };
    const retryable = isRetryableCode(e.code);
    return group === "ops"
      ? { status: 409, body: { error: { code: e.code, retryable } } }
      : { status: 409, body: { error: { code: "acquisition_refused", reason: e.code, retryable } } };
  }
  if (e instanceof AcquisitionUnavailableError) return { status: 503, body: { error: { code: "acquisition_unavailable", retryable: true } } };
  return { status: 500, body: { error: { code: "internal_error" } } };
}

export interface StatusRow {
  sessionId: string;
  state: string;
  revision: string;
  tombstoneRevision: string | null;
  ownerEpoch: string;
  ownerKind: string | null;
  grantId: string | null;
}

const decimalOrNull = (v: unknown): string | null => (v === null || v === undefined ? null : typeof v === "string" && /^(0|[1-9][0-9]*)$/.test(v) ? v : (() => { throw new AcquisitionUnexpectedResult(); })());
const stringOrNull = (v: unknown): string | null => (v === null || v === undefined ? null : typeof v === "string" ? v : (() => { throw new AcquisitionUnexpectedResult(); })());

/** The typed adapter over the four functions. Never returns a grant hash (status drops unknown keys). */
export class AcquisitionSql {
  constructor(private readonly executor: AcquisitionSqlExecutor) {}

  private async run(fn: SqlFunction, p: Readonly<Record<string, unknown>>): Promise<string> {
    try {
      return await this.executor.call(fn, p);
    } catch (error) {
      throw classifySqlError(error);
    }
  }

  async mutate(kind: AcquisitionKind, p: AcquisitionSqlParams): Promise<Record<string, unknown>> {
    return resultObject(await this.run(kind === "rebind" ? "rebind_origin" : kind, p));
  }

  async status(p: { orgId: string; workspaceId: string; source: string; sessionIds: readonly string[] }): Promise<StatusRow[]> {
    const value = parseSqlJson(await this.run("status", p));
    const rows = Array.isArray(value) ? value : (value as { sessions?: unknown })?.sessions;
    if (!Array.isArray(rows)) throw new AcquisitionUnexpectedResult();
    return rows.map((row) => {
      if (row === null || typeof row !== "object") throw new AcquisitionUnexpectedResult();
      const r = row as Record<string, unknown>;
      if (typeof r.sessionId !== "string" || typeof r.state !== "string") throw new AcquisitionUnexpectedResult();
      return {
        sessionId: r.sessionId,
        state: r.state,
        revision: decimalOrNull(r.revision) ?? "0",
        tombstoneRevision: decimalOrNull(r.tombstoneRevision),
        ownerEpoch: decimalOrNull(r.ownerEpoch) ?? "0",
        ownerKind: stringOrNull(r.ownerKind),
        grantId: stringOrNull(r.grantId),
      };
    });
  }
}

export const MAX_STATUS_SESSIONS = 500;

/** {orgId, workspaceId, source, sessionIds[<=500]}: exact keys, identifiers, no duplicates. */
export function statusRequest(body: unknown): { orgId: string; workspaceId: string; source: string; sessionIds: string[] } {
  const bad = () => new AcquisitionAssertionError("invalid_request");
  if (body === null || typeof body !== "object" || Array.isArray(body)) throw bad();
  const b = body as Record<string, unknown>;
  if (Object.keys(b).sort().join(",") !== "orgId,sessionIds,source,workspaceId") throw bad();
  const { orgId, workspaceId, source, sessionIds } = b;
  if (typeof orgId !== "string" || !isIdentifier(orgId) || typeof workspaceId !== "string" || (workspaceId !== "" && !isIdentifier(workspaceId)))
    throw bad();
  if (typeof source !== "string" || !isIdentifier(source)) throw bad();
  if (!Array.isArray(sessionIds) || sessionIds.length === 0 || sessionIds.length > MAX_STATUS_SESSIONS) throw bad();
  if (!sessionIds.every((s) => typeof s === "string" && isIdentifier(s)) || new Set(sessionIds).size !== sessionIds.length) throw bad();
  return { orgId, workspaceId, source, sessionIds: sessionIds as string[] };
}

/** The operations port: Cloud-issued envelopes only, scoped to the token's workspace. */
export class Installer {
  /** `registry` must already be the cloud-class subset (registryOfClass). */
  constructor(private readonly sql: AcquisitionSql, private readonly registry: IssuerRegistry) {}

  /**
   * orgId/workspaceId come ONLY from the introspected token: a payload naming
   * another org/workspace is 403 target_mismatch before verification or SQL;
   * `authorizeScope` re-checks the same equality inside the verifier.
   */
  async apply(kind: AcquisitionKind, envelope: unknown, identity: OpsIdentity): Promise<Record<string, unknown>> {
    if (envelope === null || typeof envelope !== "object" || Array.isArray(envelope) || Object.keys(envelope).sort().join(",") !== "payload,signature")
      throw new AcquisitionAssertionError("invalid_request");
    const payload = (envelope as { payload: unknown }).payload;
    if (payload !== null && typeof payload === "object" && !Array.isArray(payload)) {
      const p = payload as Record<string, unknown>;
      if (p.orgId !== identity.orgId || p.workspaceId !== identity.workspaceId) throw new AcquisitionTargetMismatch();
    }
    const authorizeScope: AuthorizeScope = (scope) => scope.orgId === identity.orgId && scope.workspaceId === identity.workspaceId;
    const verified = verifyAcquisitionAssertion(kind, envelope as { payload: unknown; signature: unknown }, this.registry, authorizeScope);
    return this.sql.mutate(kind, verified.payload);
  }

  async status(body: unknown, identity: OpsIdentity): Promise<{ sessions: StatusRow[] }> {
    const request = statusRequest(body);
    if (request.orgId !== identity.orgId || request.workspaceId !== identity.workspaceId) throw new AcquisitionTargetMismatch();
    return { sessions: await this.sql.status(request) };
  }
}
