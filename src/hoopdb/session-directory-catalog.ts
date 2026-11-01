/**
 * The write path for the session directory's catalog dimensions
 * (supabase/migrations/20261101050000_session_directory_list.sql): agent names, catalog
 * models, project key and method, initial commit, originator, agent version, and the
 * parent -> child relationship edges.
 *
 * They come from the delivered `session` and `relationship` records, which
 * sessions.session_index does not carry. `directoryCatalogFromProjection` reads them from
 * the rows `projectRelayhistorySession` (packages/pipeline relayhistory-projection.ts)
 * already builds, so the directory and relayhistory's session_catalog agree on one parse.
 *
 * Bounds match the migration's CHECKs and are applied here first. A local path is never
 * stored as a project key (`project_key_method = 'path'`, or a value that looks like an
 * absolute path): plaintext paths leak usernames (docs/session-search-projection.md).
 * Values are metadata only; no transcript text reaches these columns.
 */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { SessionDirectoryKey } from "./session-directory-summary.ts";

export const MAX_DIRECTORY_AGENTS = 64;
export const MAX_DIRECTORY_MODELS = 64;
export const MAX_DIRECTORY_RELATIONSHIPS = 500;

export type DirectoryRelationship = {
  relationshipUid: string;
  childSessionId: string | null;
};

export type DirectoryCatalog = {
  agentNames: string[];
  models: string[];
  projectKey: string | null;
  projectKeyMethod: string | null;
  initialCommit: string | null;
  originator: string | null;
  agentVersion: string | null;
  /** Edges where this session is the parent. */
  relationships: DirectoryRelationship[];
};

/** The subset of a relayhistory projection row this module reads. */
export type CatalogProjectionRow = {
  table: string;
  row: Record<string, unknown>;
};

const codePoints = (value: string) => Array.from(value).length;

function bounded(value: unknown, max: number): string | null {
  if (typeof value !== "string") return null;
  const text = value.trim();
  if (text === "" || codePoints(text) > max || /[\u0000-\u001f\u007f]/.test(text))
    return null;
  return text;
}

function looksLikeLocalPath(value: string): boolean {
  return (
    value.startsWith("/") ||
    value.startsWith("~") ||
    /^[A-Za-z]:[\\/]/.test(value) ||
    value.startsWith("\\\\") ||
    value.startsWith("file:")
  );
}

/** lower(btrim(x)) as PostgreSQL computes it for the agent companion table. */
export function normalizeDirectoryAgent(name: string): string {
  return name.trim().toLowerCase();
}

function models(value: unknown): string[] {
  const list = Array.isArray(value) ? value : [];
  return list
    .map((item) => bounded(item, 200))
    .filter((item): item is string => item !== null);
}

/**
 * The catalog of one session from its projection rows. The newest `session` row wins
 * every scalar (rows are in event order, as the live projection keeps them); agent
 * names and models are the union.
 */
export function directoryCatalogFromProjection(
  rows: readonly CatalogProjectionRow[],
): DirectoryCatalog {
  const catalog: DirectoryCatalog = {
    agentNames: [],
    models: [],
    projectKey: null,
    projectKeyMethod: null,
    initialCommit: null,
    originator: null,
    agentVersion: null,
    relationships: [],
  };
  const agents = new Map<string, string>();
  const modelSet = new Set<string>();
  const edges = new Map<string, string | null>();
  for (const { table, row } of rows) {
    if (table === "session_catalog") {
      for (const model of models(row.models)) modelSet.add(model);
      catalog.projectKeyMethod =
        bounded(row.project_key_method, 64) ?? catalog.projectKeyMethod;
      catalog.projectKey = bounded(row.project_key, 512) ?? catalog.projectKey;
      catalog.initialCommit =
        bounded(row.initial_commit, 64) ?? catalog.initialCommit;
      catalog.originator = bounded(row.originator, 200) ?? catalog.originator;
      catalog.agentVersion =
        bounded(row.agent_version, 200) ?? catalog.agentVersion;
    } else if (table === "session_catalog_agents") {
      const name = bounded(row.agent_name, 200);
      if (name) agents.set(normalizeDirectoryAgent(name), name);
    } else if (table === "session_relationships") {
      const uid = bounded(row.relationship_uid, 255);
      if (uid) edges.set(uid, bounded(row.child_session_id, 255));
    }
  }
  if (
    catalog.projectKey !== null &&
    (catalog.projectKeyMethod === "path" || looksLikeLocalPath(catalog.projectKey))
  )
    catalog.projectKey = null;
  catalog.agentNames = [...agents.entries()]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([, name]) => name)
    .slice(0, MAX_DIRECTORY_AGENTS);
  catalog.models = [...modelSet].sort().slice(0, MAX_DIRECTORY_MODELS);
  catalog.relationships = [...edges.entries()]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .slice(0, MAX_DIRECTORY_RELATIONSHIPS)
    .map(([relationshipUid, childSessionId]) => ({
      relationshipUid,
      childSessionId,
    }));
  return catalog;
}

/**
 * Store one session's catalog on its live directory row and replace its agent and
 * relationship rows, in ONE statement (atomic on any executor, no transaction needed).
 *
 * The statement first locks the live row (`SELECT ... FOR UPDATE`). Every write joins
 * that lock, so:
 *   - a tombstone that committed first leaves no live row: nothing is written
 *     (returns written: false);
 *   - a tombstone that arrives later blocks on the row lock until this statement
 *     commits, and its triggers (session_directory_forget_catalog) then remove what
 *     this wrote. No agent row or parent edge can outlive the tombstone (M3, #124 review).
 *
 * Agent rows copy the locked row's last_activity_at; the companions_follow trigger keeps
 * them in step afterwards. An edge naming a child whose directory row is a tombstone is
 * not written, so a re-delivered parent cannot reintroduce an erased child's id. (A child
 * whose row was hard-deleted is indistinguishable from one not uploaded yet.)
 */
export async function writeDirectoryCatalog(
  db: SqlExecutor,
  key: SessionDirectoryKey,
  input: DirectoryCatalog,
): Promise<{ written: boolean }> {
  // Re-derive through the same bounds, so a caller cannot bypass them.
  const catalog = directoryCatalogFromProjection([
    {
      table: "session_catalog",
      row: {
        models: input.models,
        project_key: input.projectKey,
        project_key_method: input.projectKeyMethod,
        initial_commit: input.initialCommit,
        originator: input.originator,
        agent_version: input.agentVersion,
      },
    },
    ...input.agentNames.map((agent_name) => ({
      table: "session_catalog_agents",
      row: { agent_name },
    })),
    ...input.relationships.map((edge) => ({
      table: "session_relationships",
      row: {
        relationship_uid: edge.relationshipUid,
        child_session_id: edge.childSessionId,
      },
    })),
  ]);
  const { rows } = await db.query<{ written: boolean }>(
    `WITH live AS MATERIALIZED (
       SELECT d.org_id, d.workspace_id, d.harness, d.session_id, d.last_activity_at
         FROM sessions.session_directory AS d
        WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3 AND d.session_id = $4
          AND d.deleted_at IS NULL
        FOR UPDATE
     ), updated AS (
       UPDATE sessions.session_directory AS d
          SET agent_names = $5::text[], catalog_models = $6::text[],
              project_key = $7, project_key_method = $8, initial_commit = $9,
              originator = $10, agent_version = $11,
              -- The scalar agent the search RPC filters on: keep one already projected,
              -- else the first catalog agent, and rebuild the search document with it.
              agent_name = coalesce(d.agent_name, ($5::text[])[1]),
              agent_name_normalized = lower(btrim(coalesce(d.agent_name, ($5::text[])[1]))),
              search_text = sessions.session_directory_search_text(
                d.title, d.task_title, coalesce(d.agent_name, ($5::text[])[1]), d.project_id,
                d.git_repo, d.git_branch, d.model, d.short_summary),
              catalog_projected_at = now()
         FROM live AS l
        WHERE d.org_id = l.org_id AND d.workspace_id = l.workspace_id
          AND d.harness = l.harness AND d.session_id = l.session_id
       RETURNING d.org_id
     ), drop_agents AS (
       DELETE FROM sessions.session_directory_agents AS a
        USING live AS l
        WHERE a.org_id = l.org_id AND a.workspace_id = l.workspace_id
          AND a.harness = l.harness AND a.session_id = l.session_id
          AND NOT (a.agent_name_normalized = ANY ($12::text[]))
     ), put_agents AS (
       INSERT INTO sessions.session_directory_agents
         (org_id, workspace_id, harness, session_id, agent_name, agent_name_normalized, last_activity_at)
       SELECT l.org_id, l.workspace_id, l.harness, l.session_id, x.name, lower(btrim(x.name)), l.last_activity_at
         FROM live AS l, unnest($5::text[]) AS x(name)
       ON CONFLICT (org_id, workspace_id, harness, session_id, agent_name_normalized)
       DO UPDATE SET agent_name = excluded.agent_name, last_activity_at = excluded.last_activity_at
     ), edges AS (
       SELECT l.org_id, l.workspace_id, l.harness, l.session_id, e.uid, e.child
         FROM live AS l, unnest($13::text[], $14::text[]) AS e(uid, child)
        WHERE e.child IS NULL OR NOT EXISTS (
          SELECT 1 FROM sessions.session_directory AS c
           WHERE c.org_id = l.org_id AND c.workspace_id = l.workspace_id
             AND c.harness = l.harness AND c.session_id = e.child AND c.deleted_at IS NOT NULL)
     ), drop_edges AS (
       DELETE FROM sessions.session_directory_relationships AS r
        USING live AS l
        WHERE r.org_id = l.org_id AND r.workspace_id = l.workspace_id
          AND r.harness = l.harness AND r.parent_session_id = l.session_id
          AND NOT (r.relationship_uid IN (SELECT e.uid FROM edges AS e))
     ), put_edges AS (
       INSERT INTO sessions.session_directory_relationships
         (org_id, workspace_id, harness, parent_session_id, relationship_uid, child_session_id)
       SELECT e.org_id, e.workspace_id, e.harness, e.session_id, e.uid, e.child FROM edges AS e
       ON CONFLICT (org_id, workspace_id, harness, parent_session_id, relationship_uid)
       DO UPDATE SET child_session_id = excluded.child_session_id
     )
     SELECT EXISTS (SELECT 1 FROM updated) AS written`,
    [
      key.orgId,
      key.workspaceId,
      key.harness,
      key.sessionId,
      catalog.agentNames,
      catalog.models,
      catalog.projectKey,
      catalog.projectKeyMethod,
      catalog.initialCommit,
      catalog.originator,
      catalog.agentVersion,
      catalog.agentNames.map(normalizeDirectoryAgent),
      catalog.relationships.map((edge) => edge.relationshipUid),
      catalog.relationships.map((edge) => edge.childSessionId),
    ],
  );
  return { written: rows[0]?.written === true };
}
