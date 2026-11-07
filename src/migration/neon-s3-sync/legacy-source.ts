import {
  resolvedPublicTurnSourceSql,
  selectedPublicTurnPredicateSql,
} from "../../shared/legacy-turn-source.ts";
/**
 * Legacy relayhistory evidence (conversation_turns, session_links,
 * session_outcomes) as canonical S3 records. Read-only; no reads occur unless
 * a caller (the one-shot `legacy-export` runner) asks for them.
 *
 * Each (org, workspace, source, session) scope gets ITS OWN manifest under the
 * dedicated machine id `RELAYHISTORY_LEGACY_MACHINE_ID`, holding only legacy
 * records plus one `legacy_scope` coverage marker. Ordinary delivery manifests
 * are never touched, so their keys (`d=` start date) and seed proofs are
 * unchanged (activation gate 2).
 */
import {
  buildSessionStream,
  type Queryable,
  type DeliveryRecord,
} from "../relayhistory-export.ts";
import {
  boundStrings,
  sessionUnits,
  type SessionUnit,
  type UnitSource,
  type PageHook,
} from "./neon-reader.ts";
import { RELAYHISTORY_LEGACY_MACHINE_ID } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
export { RELAYHISTORY_LEGACY_MACHINE_ID };

export const LEGACY_TABLES = [
  {
    table: "conversation_turns",
    kind: "legacy_conversation_turn",
    key: "id",
    cast: "uuid",
    time: "ts",
  },
  {
    table: "session_links",
    kind: "legacy_session_link",
    key: "id",
    cast: "bigint",
    time: "coalesce(link_ts,updated_at,created_at)",
  },
  {
    table: "session_outcomes",
    kind: "legacy_session_outcome",
    key: "commit_sha",
    cast: "text",
    time: "coalesce(shipped_at,ingested_at,created_at)",
  },
] as const;
export type LegacyTable = (typeof LEGACY_TABLES)[number]["table"];

export interface LegacyScope {
  orgId: string;
  workspaceId: string;
  source: string;
  sessionId: string;
}

const scopeSql =
  "org_id=$1 AND workspace_id=$2 AND source=$3 AND session_id=$4";
const turnScopeSql = `t.org_id=$1 AND t.workspace_id=$2 AND t.session_id=$4 AND ${selectedPublicTurnPredicateSql("t", "$3")}`;
const scopeParams = (s: LegacyScope) => [
  s.orgId,
  s.workspaceId,
  s.source,
  s.sessionId,
];

/**
 * The scope's user, for the manifest (activation gate 8), in a fixed order:
 * the delivery user of its own events, else an outcome's user_id, else a turn
 * session_owner that is a user rather than a harness label (it differs from
 * both its stored and its attributed source). `null` only when none exists;
 * the caller records that explicitly (never silently "unknown").
 */
export async function legacyScopeUser(
  db: Queryable,
  s: LegacyScope,
): Promise<string | null> {
  const { rows } = await db.query<{ user_id: string | null }>(
    `SELECT COALESCE(
  (SELECT min(e.user_id) FROM sessions.convergence_events e WHERE e.org_id=$1 AND e.workspace_id=$2 AND e.source=$3 AND e.session_id=$4 AND e.user_id<>''),
  (SELECT min(o.user_id) FROM sessions.session_outcomes o WHERE o.org_id=$1 AND o.workspace_id=$2 AND o.source=$3 AND o.session_id=$4 AND o.user_id<>''),
  (SELECT min(t.session_owner) FROM sessions.conversation_turns t WHERE ${turnScopeSql} AND t.session_owner<>'' AND t.session_owner<>t.source AND t.session_owner<>$3)
 ) AS user_id`,
    scopeParams(s),
  );
  return rows[0]?.user_id ?? null;
}

/**
 * The export unit of one scope. A scope that also has ordinary (rollup or
 * catalog) evidence keeps that unit's `firstMs`, so its legacy manifest sits
 * under the same `d=` start date as the ordinary ones; a legacy-only scope
 * starts at its oldest legacy row. `lastMs` covers both. Ordinary units are
 * read, never changed.
 */
export async function legacyUnit(
  db: Queryable,
  s: LegacyScope,
): Promise<SessionUnit | null> {
  const { rows } = await db.query<{
    first_ms: string | null;
    last_ms: string | null;
  }>(
    `WITH e AS (
  SELECT t.ts AS at FROM sessions.conversation_turns t WHERE ${turnScopeSql}
  UNION ALL SELECT coalesce(link_ts,updated_at,created_at) FROM sessions.session_links WHERE ${scopeSql}
  UNION ALL SELECT coalesce(shipped_at,ingested_at,created_at) FROM sessions.session_outcomes WHERE ${scopeSql})
 SELECT floor(extract(epoch FROM min(at))*1000)::bigint::text AS first_ms,
        floor(extract(epoch FROM max(at))*1000)::bigint::text AS last_ms FROM e`,
    scopeParams(s),
  );
  const first = rows[0]?.first_ms,
    last = rows[0]?.last_ms;
  if (first == null || last == null) return null;
  const ordinary = (await sessionUnits(db, s.orgId, s.sessionId)).find(
    (u) => u.workspaceId === s.workspaceId && u.source === s.source,
  );
  const firstMs = ordinary ? ordinary.firstMs : Number(first);
  const lastMs = Math.max(Number(last), ordinary?.lastMs ?? -Infinity, firstMs);
  if (!Number.isFinite(firstMs) || !Number.isFinite(lastMs))
    throw Object.assign(new Error("legacy evidence clock invalid"), {
      code: "legacy_clock_invalid",
    });
  return {
    orgId: s.orgId,
    workspaceId: s.workspaceId,
    source: s.source,
    sessionId: s.sessionId,
    firstMs,
    lastMs,
    machineIds: [],
    totals: {},
  };
}

/**
 * Deletion fence for a legacy scope: Neon `deleted_upload_sessions` for the
 * scope (the turn key uses `default` for a token without a workspace, the
 * fence `""`, so both spellings are checked).
 */
export async function legacyScopeFenced(
  db: Queryable,
  s: LegacyScope,
): Promise<boolean> {
  const { rows } = await db.query(
    `SELECT 1 FROM sessions.deleted_upload_sessions WHERE org_id=$1 AND source=$3 AND session_id=$4
  AND (workspace_id=$2 OR ($2='default' AND workspace_id='')) LIMIT 1`,
    scopeParams(s),
  );
  return rows.length > 0;
}

/**
 * A `UnitSource` yielding only the scope's legacy records and its coverage
 * marker, all on the legacy machine, for `exportUnit`. Per-table keyset pages
 * on each table's own key. Revisions stay "1": a legacy record is accepted
 * only from the single exporter-owned legacy manifest of its scope (gate 4,
 * enforced by the canonical reader), so there is never a second copy to fold.
 */
export function legacyUnitSource(
  db: Queryable,
  onPage: PageHook,
  pageRows = 500,
  stats?: { userUnknown: number },
): UnitSource {
  return {
    fenced: (unit) => legacyScopeFenced(db, unit),
    async *lines(unit) {
      const user = await legacyScopeUser(db, unit);
      if (user === null && stats) stats.userUnknown += 1;
      const userId = user ?? "unknown";
      const machine = RELAYHISTORY_LEGACY_MACHINE_ID;
      for (const spec of LEGACY_TABLES) {
        let after: string | undefined;
        for (;;) {
          const where =
            spec.table === "conversation_turns" ? turnScopeSql : scopeSql;
          // The public source is attribution; the original overloaded column
          // stays explicit. Content, IDs and turn fields stay exact.
          const attribution =
            spec.table === "conversation_turns"
              ? ",'source',$3::text,'legacy_stored_source',t.source"
              : "";
          const { rows } = await db.query<{
            key: string;
            payload_json: string;
            received_at: string;
          }>(
            `SELECT ${spec.key}::text AS key,(to_jsonb(t)||jsonb_build_object('${spec.key}',${spec.key}::text${attribution}))::text AS payload_json,
      (${spec.time})::text AS received_at FROM sessions.${spec.table} t
      WHERE ${where} ${after === undefined ? "" : `AND ${spec.key}>$6::${spec.cast}`} ORDER BY ${spec.key} LIMIT $5`,
            [
              ...scopeParams(unit),
              pageRows,
              ...(after === undefined ? [] : [after]),
            ],
          );
          await onPage(
            rows.length,
            rows.reduce((n, r) => n + r.payload_json.length, 0),
          );
          for (const row of rows) {
            const bounded = boundStrings(row.payload_json);
            const record: DeliveryRecord = {
              kind: spec.kind,
              origin_id: "legacy:" + spec.table,
              record_id: "legacy:" + spec.table + ":" + row.key,
              revision: "1",
              operation: "upsert",
              received_at: row.received_at,
              payload: {},
              payload_json: bounded.text,
            };
            yield {
              machine,
              bytes: buildSessionStream([record]).bytes,
              userId,
              ...(bounded.truncated
                ? {
                    truncated: bounded.truncated,
                    truncatedMaxChars: bounded.maxChars,
                  }
                : {}),
            };
          }
          if (rows.length < pageRows) break;
          after = rows.at(-1)!.key;
        }
      }
      // Even empty tables get an explicit, raw-hash-bound completeness statement.
      const marker: DeliveryRecord = {
        kind: "legacy_scope",
        origin_id: "legacy:scope",
        record_id: "legacy:scope:coverage",
        revision: "1",
        operation: "upsert",
        received_at: new Date(unit.lastMs).toISOString(),
        payload: {
          version: 1,
          org_id: unit.orgId,
          workspace_id: unit.workspaceId,
          source: unit.source,
          session_id: unit.sessionId,
          tables: LEGACY_TABLES.map((s) => s.table),
        },
      };
      yield { machine, userId, bytes: buildSessionStream([marker]).bytes };
    },
  };
}

/** One discovery row: a stored group of one legacy table. */
export interface LegacyGroup {
  table: LegacyTable;
  org_id: string;
  workspace_id: string;
  /** Stored source column (for turns, NOT the attributed source). */
  stored_source: string;
  session_id: string;
  rows: string;
  max_ms: string;
}

/**
 * Keyset discovery over ONE table inside ONE org (activation gate 3): no
 * cross-table union, no cross-tenant scan, ordered by the table's own unique
 * key prefix (turns: (org, workspace, source, session); links and outcomes:
 * (org, source, session), workspace third). `sinceMs`/`untilMs` keep groups
 * with any row whose time falls in [since, until).
 */
export async function legacyDiscoveryPage(
  db: Queryable,
  table: LegacyTable,
  orgId: string,
  after: Pick<LegacyGroup, "workspace_id" | "stored_source" | "session_id"> | null,
  window: { sinceMs?: number; untilMs?: number } = {},
  limit = 1000,
): Promise<LegacyGroup[]> {
  const spec = LEGACY_TABLES.find((s) => s.table === table)!;
  const turns = table === "conversation_turns";
  const order = turns
    ? ["workspace_id", "source", "session_id"]
    : ["source", "session_id", "workspace_id"];
  const params: unknown[] = [orgId, limit];
  const where = ["org_id=$1"];
  if (after) {
    const values: Record<string, string> = {
      workspace_id: after.workspace_id,
      source: after.stored_source,
      session_id: after.session_id,
    };
    const at = params.length;
    params.push(...order.map((c) => values[c]));
    where.push(
      `(${order.join(",")})>(${order.map((_, i) => `$${at + i + 1}`).join(",")})`,
    );
  }
  const having: string[] = [];
  if (window.sinceMs !== undefined) {
    params.push(new Date(window.sinceMs).toISOString());
    having.push(`max(${spec.time})>=$${params.length}::timestamptz`);
  }
  if (window.untilMs !== undefined) {
    params.push(new Date(window.untilMs).toISOString());
    having.push(`min(${spec.time})<$${params.length}::timestamptz`);
  }
  const { rows } = await db.query<Omit<LegacyGroup, "table">>(
    `SELECT org_id,workspace_id,source AS stored_source,session_id,count(*)::text AS rows,
  floor(extract(epoch FROM max(${spec.time}))*1000)::bigint::text AS max_ms
 FROM sessions.${table} WHERE ${where.join(" AND ")}
 GROUP BY ${order.join(",")},org_id ${having.length ? `HAVING ${having.join(" AND ")}` : ""}
 ORDER BY ${order.join(",")} LIMIT $2`,
    params,
  );
  return rows.map((r) => ({ ...r, table }));
}

/**
 * The attributed public sources of one stored group (activation gate 1). A
 * turn no rule can attribute is counted as `unbound` and skipped: it never
 * fails discovery or another scope. Links and outcomes carry their source.
 */
export async function legacyGroupScopes(
  db: Queryable,
  g: LegacyGroup,
): Promise<{ scopes: LegacyScope[]; unbound: number }> {
  const base = {
    orgId: g.org_id,
    workspaceId: g.workspace_id,
    sessionId: g.session_id,
  };
  if (g.table !== "conversation_turns")
    return { scopes: [{ ...base, source: g.stored_source }], unbound: 0 };
  const { rows } = await db.query<{ source: string | null; n: string }>(
    `SELECT ${resolvedPublicTurnSourceSql("t")} AS source,count(*)::text AS n
 FROM sessions.conversation_turns t WHERE t.org_id=$1 AND t.workspace_id=$2 AND t.source=$3 AND t.session_id=$4
 GROUP BY 1 ORDER BY 1`,
    [g.org_id, g.workspace_id, g.stored_source, g.session_id],
  );
  let unbound = 0;
  const scopes: LegacyScope[] = [];
  for (const r of rows)
    if (typeof r.source === "string" && r.source) scopes.push({ ...base, source: r.source });
    else unbound += Number(r.n);
  return { scopes, unbound };
}

/** Orgs present in a legacy table, by index skip-scan (no full scan). */
export async function legacyOrgs(db: Queryable, table: LegacyTable): Promise<string[]> {
  const { rows } = await db.query<{ org_id: string }>(
    `WITH RECURSIVE o(org_id) AS (
  (SELECT org_id FROM sessions.${table} ORDER BY org_id LIMIT 1)
  UNION ALL SELECT (SELECT t.org_id FROM sessions.${table} t WHERE t.org_id>o.org_id ORDER BY t.org_id LIMIT 1) FROM o WHERE o.org_id IS NOT NULL)
 SELECT org_id FROM o WHERE org_id IS NOT NULL`,
  );
  return rows.map((r) => r.org_id);
}
