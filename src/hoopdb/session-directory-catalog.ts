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
 * relationship rows. A tombstoned or absent row is left alone (returns written: false):
 * the catalog is written after the pointer projection, never before it. The agent rows
 * carry the directory row's last_activity_at, which the projector keeps current.
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
  const params = [key.orgId, key.workspaceId, key.harness, key.sessionId];
  const updated = await db.query<{ last_activity_at: Date }>(
    `UPDATE sessions.session_directory AS d
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
      WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3 AND d.session_id = $4
        AND d.deleted_at IS NULL
      RETURNING d.last_activity_at`,
    [
      ...params,
      catalog.agentNames,
      catalog.models,
      catalog.projectKey,
      catalog.projectKeyMethod,
      catalog.initialCommit,
      catalog.originator,
      catalog.agentVersion,
    ],
  );
  if (updated.rows.length === 0) return { written: false };
  await db.query(
    `DELETE FROM sessions.session_directory_agents
      WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4
        AND NOT (agent_name_normalized = ANY ($5::text[]))`,
    [...params, catalog.agentNames.map(normalizeDirectoryAgent)],
  );
  if (catalog.agentNames.length > 0)
    await db.query(
      `INSERT INTO sessions.session_directory_agents
         (org_id, workspace_id, harness, session_id, agent_name, agent_name_normalized, last_activity_at)
       SELECT d.org_id, d.workspace_id, d.harness, d.session_id, a.name, lower(btrim(a.name)), d.last_activity_at
         FROM sessions.session_directory AS d, unnest($5::text[]) AS a(name)
        WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3 AND d.session_id = $4
       ON CONFLICT (org_id, workspace_id, harness, session_id, agent_name_normalized)
       DO UPDATE SET agent_name = excluded.agent_name, last_activity_at = excluded.last_activity_at`,
      [...params, catalog.agentNames],
    );
  await db.query(
    `DELETE FROM sessions.session_directory_relationships
      WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND parent_session_id = $4
        AND NOT (relationship_uid = ANY ($5::text[]))`,
    [...params, catalog.relationships.map((edge) => edge.relationshipUid)],
  );
  if (catalog.relationships.length > 0)
    await db.query(
      `INSERT INTO sessions.session_directory_relationships
         (org_id, workspace_id, harness, parent_session_id, relationship_uid, child_session_id)
       SELECT $1, $2, $3, $4, e.uid, e.child
         FROM unnest($5::text[], $6::text[]) AS e(uid, child)
       ON CONFLICT (org_id, workspace_id, harness, parent_session_id, relationship_uid)
       DO UPDATE SET child_session_id = excluded.child_session_id`,
      [
        ...params,
        catalog.relationships.map((edge) => edge.relationshipUid),
        catalog.relationships.map((edge) => edge.childSessionId),
      ],
    );
  return { written: true };
}
