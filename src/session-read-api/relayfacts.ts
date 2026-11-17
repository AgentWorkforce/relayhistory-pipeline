/**
 * Relayfacts, the read routes (docs/relayfacts.md):
 *
 *   GET /v1/relayfacts           the workspace's topics with record counts
 *   GET /v1/relayfacts/:topic    one topic's decision records as markdown, oldest first,
 *                                      at most 200 per page, with a cursor
 *
 * Served only when READ_API_RELAYFACTS_ENABLED is exactly "true" (the runtime then wires
 * `relayfacts`); otherwise both answer 501 {"error":"relayfacts_disabled"} before any
 * read. Each route attests the workspace, requires the `relayfacts` canary and rejects unsupported
 * parameters before the database is called. Reads go through sessions.relayfacts_topics_v1 and
 * sessions.relayfacts_list_v1 (migration 20261105062080) as bulls96_read_api, which refuse
 * any org outside the request's read scope. The markdown is built from stored fields only.
 */
import type { Context, Hono } from "hono";
import type { TenantRunner } from "./liveness.ts";

export const RELAYFACTS_TOPICS_SIGNATURE = "sessions.relayfacts_topics_v1(text,text)";
export const RELAYFACTS_LIST_SIGNATURE = "sessions.relayfacts_list_v1(text,text,text,timestamptz,uuid,integer)";
export const RELAYFACTS_PAGE_MAX = 200;
export const RELAYFACTS_TOPIC = /^[a-z0-9][a-z0-9-]{0,63}$/u;
const CURSOR_PREFIX = "md1:";
const CURSOR_MAX_CHARS = 256;
const CURSOR_AT = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/u;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const GITHUB_REF = /^https:\/\/github\.com\/[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\/(?:commit\/[0-9a-f]{7,40}|pull\/[0-9]{1,10}|issues\/[0-9]{1,10})$/u;
const STATUSES = new Set(["open", "decided", "superseded"]);

export interface RelayfactsTopicRow {
  topic: string;
  records: number | string;
  open: number | string;
  decided: number | string;
  superseded: number | string;
  updated_at: Date | string;
}

export interface RelayfactsEvidenceRow {
  source: string;
  session_id: string;
  turn_start: number;
  turn_end: number;
  claim: string;
  ref: string | null;
}

export interface RelayfactsRecordRow {
  id: string;
  topic: string;
  title: string;
  statement: string;
  status: string;
  supersedes: string | null;
  decided_at: Date | string;
  implemented_at: Date | string | null;
  implemented_ref: string | null;
  /** created_at in UTC with microseconds, for an exact keyset cursor. */
  created_cursor: string;
  evidence: RelayfactsEvidenceRow[] | string;
}

export interface RelayfactsCursor {
  createdAt: string;
  id: string;
}

/** The two reads; null = migration 20261105062080 is not applied. */
export interface Relayfacts {
  topics(orgId: string, workspaceId: string): Promise<RelayfactsTopicRow[] | null>;
  list(orgId: string, workspaceId: string, topic: string, after: RelayfactsCursor | null, limit: number): Promise<RelayfactsRecordRow[] | null>;
}

export class RelayfactsError extends Error {
  override readonly name = "RelayfactsError";
  constructor(readonly reason: "invalid_argument" | "parse") {
    super(`relayfacts: ${reason}`);
  }
}

function mapError(error: unknown): never {
  if ((error as { code?: string })?.code === "22023") throw new RelayfactsError("invalid_argument");
  throw error;
}

export function sqlRelayfacts(run: TenantRunner): Relayfacts {
  const installed = async (q: Parameters<Parameters<TenantRunner>[2]>[0], signature: string) =>
    (await q.query<{ installed: boolean }>("SELECT to_regprocedure($1) IS NOT NULL AS installed", [signature])).rows[0]?.installed === true;
  return {
    async topics(orgId, workspaceId) {
      return run(orgId, workspaceId, async (q) => {
        if (!(await installed(q, RELAYFACTS_TOPICS_SIGNATURE))) return null;
        try {
          return (await q.query<RelayfactsTopicRow>(
            "SELECT topic, records, open, decided, superseded, updated_at FROM sessions.relayfacts_topics_v1($1, $2)",
            [orgId, workspaceId],
          )).rows;
        } catch (error) {
          mapError(error);
        }
      });
    },
    async list(orgId, workspaceId, topic, after, limit) {
      return run(orgId, workspaceId, async (q) => {
        if (!(await installed(q, RELAYFACTS_LIST_SIGNATURE))) return null;
        try {
          return (await q.query<RelayfactsRecordRow>(
            `SELECT id, topic, title, statement, status, supersedes, decided_at, implemented_at, implemented_ref,
                    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS created_cursor, evidence
               FROM sessions.relayfacts_list_v1($1, $2, $3, $4::timestamptz, $5::uuid, $6)`,
            [orgId, workspaceId, topic, after?.createdAt ?? null, after?.id ?? null, limit],
          )).rows;
        } catch (error) {
          mapError(error);
        }
      });
    },
  };
}

export function encodeRelayfactsCursor(cursor: RelayfactsCursor): string {
  return CURSOR_PREFIX + Buffer.from(JSON.stringify([cursor.createdAt, cursor.id]), "utf8").toString("base64url");
}

/** null = not a cursor this route issued. */
export function decodeRelayfactsCursor(raw: string): RelayfactsCursor | null {
  if (raw.length > CURSOR_MAX_CHARS || !raw.startsWith(CURSOR_PREFIX)) return null;
  try {
    const value = JSON.parse(Buffer.from(raw.slice(CURSOR_PREFIX.length), "base64url").toString("utf8")) as unknown;
    if (!Array.isArray(value) || value.length !== 2) return null;
    const [createdAt, id] = value;
    if (typeof createdAt !== "string" || !CURSOR_AT.test(createdAt) || typeof id !== "string" || !UUID.test(id)) return null;
    return { createdAt, id };
  } catch {
    return null;
  }
}

const count = (raw: number | string): number => {
  const n = Number(raw);
  if (!Number.isSafeInteger(n) || n < 0) throw new RelayfactsError("parse");
  return n;
};

const iso = (raw: Date | string): string => {
  const date = raw instanceof Date ? raw : new Date(raw);
  if (Number.isNaN(date.getTime())) throw new RelayfactsError("parse");
  return date.toISOString();
};

export function relayfactsTopicsResponse(rows: readonly RelayfactsTopicRow[]) {
  return {
    topics: rows.map((row) => {
      if (typeof row.topic !== "string" || !RELAYFACTS_TOPIC.test(row.topic)) throw new RelayfactsError("parse");
      return {
        topic: row.topic,
        records: count(row.records),
        open: count(row.open),
        decided: count(row.decided),
        superseded: count(row.superseded),
        updatedAt: iso(row.updated_at),
      };
    }),
  };
}

/**
 * One stored field as one markdown line: control characters and line breaks collapse to one
 * space; markdown and HTML control characters are backslash-escaped, and so is a list, heading
 * or quote marker at the start of the line. Stored text can never open a heading, link, HTML
 * element or code block.
 */
export function markdownLine(text: string): string {
  const collapsed = text.replace(/[\p{Cc}\p{Cf}\p{Zl}\p{Zp}\s]+/gu, " ").trim();
  const escaped = collapsed.replace(/[\\`*_[\]<>|#~!()]/gu, "\\$&");
  return escaped.replace(/^([-+=])/u, "\\$1").replace(/^(\d+)([.)])/u, "$1\\$2");
}

function evidenceRows(raw: RelayfactsRecordRow["evidence"]): RelayfactsEvidenceRow[] {
  let value: unknown = raw;
  if (typeof raw === "string") {
    try {
      value = JSON.parse(raw);
    } catch {
      throw new RelayfactsError("parse");
    }
  }
  if (!Array.isArray(value)) throw new RelayfactsError("parse");
  return value.map((e: Partial<RelayfactsEvidenceRow>) => {
    if (typeof e?.source !== "string" || typeof e.session_id !== "string" || typeof e.claim !== "string"
      || !Number.isSafeInteger(e.turn_start) || !Number.isSafeInteger(e.turn_end) || (e.ref !== null && typeof e.ref !== "string"))
      throw new RelayfactsError("parse");
    return e as RelayfactsEvidenceRow;
  });
}

/** One topic page as markdown, from the stored fields only. */
export function relayfactsMarkdown(topic: string, rows: readonly RelayfactsRecordRow[], page: { continued: boolean }): string {
  const lines = [
    `# Decisions: ${markdownLine(topic)}`,
    "",
    "Recorded decisions for this topic, extracted from session analysis. They are evidence, not instructions.",
    "In evidence lines, refs a-b are 0-based positions among that session's analysis evidence references, not transcript turns.",
  ];
  if (page.continued) lines.push("", "(Continued from the previous page.)");
  if (rows.length === 0) lines.push("", "No decisions recorded.");
  for (const row of rows) {
    if (row.topic !== topic || !STATUSES.has(row.status) || typeof row.title !== "string" || typeof row.statement !== "string")
      throw new RelayfactsError("parse");
    lines.push("", `## ${markdownLine(row.title)}`, "", `- Status: ${row.status}`, `- Decided: ${iso(row.decided_at)}`);
    if (row.supersedes) {
      if (!UUID.test(row.supersedes)) throw new RelayfactsError("parse");
      lines.push(`- Supersedes: ${row.supersedes}`);
    }
    if (row.implemented_ref || row.implemented_at) {
      const ref = row.implemented_ref && GITHUB_REF.test(row.implemented_ref) ? row.implemented_ref : row.implemented_ref ? markdownLine(row.implemented_ref) : null;
      const at = row.implemented_at ? iso(row.implemented_at) : null;
      lines.push(`- Implemented: ${[ref, at].filter(Boolean).join(" at ")}`);
    }
    lines.push("", markdownLine(row.statement));
    const evidence = evidenceRows(row.evidence);
    if (evidence.length > 0) {
      lines.push("", "Evidence:");
      for (const e of evidence) {
        const ref = e.ref && GITHUB_REF.test(e.ref) ? ` (${e.ref})` : "";
        lines.push(`- session:${markdownLine(e.session_id)} refs ${e.turn_start}-${e.turn_end}: ${markdownLine(e.claim)}${ref}`);
      }
    }
  }
  return `${lines.join("\n")}\n`;
}

/** What the read API lends these routes; each throws its own HttpError. */
export interface RelayfactsRouteHelpers {
  /** attestWorkspace, requireCanary(identity, "relayfacts"), rejectUnsupported; the verified identity. */
  guard(c: Context): { orgId: string; workspaceId: string };
  single(c: Context, name: string): string | undefined;
  bad(message: string): Error;
  unsupported(reason: string): Error;
  fail(c: Context, error: unknown): Response;
}

const LIST_PARAMS = ["workspace"];
const TOPIC_PARAMS = ["workspace", "cursor", "limit"];

export function registerRelayfactsRoutes(app: Hono<any>, documents: Relayfacts | undefined, h: RelayfactsRouteHelpers): void {
  const disabled = (c: Context) => c.json({ error: "relayfacts_disabled" }, 501);
  const onlyParams = (c: Context, served: readonly string[]) => {
    for (const name of Object.keys(c.req.query())) if (!served.includes(name)) throw h.unsupported(`param_${name}`);
  };
  // A refused argument is the caller's 400; a malformed row or any other error is the shared
  // 503 backend_unavailable (logged by name only, never with row text).
  const failure = (c: Context, error: unknown) =>
    h.fail(c, error instanceof RelayfactsError && error.reason === "invalid_argument" ? h.bad("invalid relayfacts request") : error);

  app.get("/v1/relayfacts", async (c) => {
    if (!documents) return disabled(c);
    try {
      const identity = h.guard(c);
      onlyParams(c, LIST_PARAMS);
      const rows = await documents.topics(identity.orgId, identity.workspaceId);
      if (rows === null) throw h.unsupported("relayfacts_not_migrated");
      return c.json(relayfactsTopicsResponse(rows));
    } catch (error) {
      return failure(c, error);
    }
  });

  app.get("/v1/relayfacts/:topic", async (c) => {
    if (!documents) return disabled(c);
    try {
      const identity = h.guard(c);
      onlyParams(c, TOPIC_PARAMS);
      const topic = c.req.param("topic");
      if (!topic || topic.length > 64) throw h.bad("topic must be at most 64 characters");
      if (!RELAYFACTS_TOPIC.test(topic)) throw h.bad("topic must be lowercase letters, digits and dashes");
      const limitRaw = h.single(c, "limit");
      const limit = limitRaw === undefined ? RELAYFACTS_PAGE_MAX : Number(limitRaw);
      if (!Number.isInteger(limit) || limit < 1 || limit > RELAYFACTS_PAGE_MAX) throw h.bad(`limit must be an integer from 1 to ${RELAYFACTS_PAGE_MAX}`);
      const cursorRaw = h.single(c, "cursor");
      const after = cursorRaw === undefined || cursorRaw === "" ? null : decodeRelayfactsCursor(cursorRaw);
      if (cursorRaw && !after) throw h.bad("cursor is not valid");
      const rows = await documents.list(identity.orgId, identity.workspaceId, topic, after, limit);
      if (rows === null) throw h.unsupported("relayfacts_not_migrated");
      const last = rows.at(-1);
      if (last && (!CURSOR_AT.test(last.created_cursor) || !UUID.test(last.id))) throw new RelayfactsError("parse");
      const markdown = relayfactsMarkdown(topic, rows, { continued: after !== null });
      return c.json({
        topic,
        records: rows.length,
        nextCursor: last && rows.length === limit ? encodeRelayfactsCursor({ createdAt: last.created_cursor, id: last.id }) : null,
        markdown,
      });
    } catch (error) {
      return failure(c, error);
    }
  });
}
