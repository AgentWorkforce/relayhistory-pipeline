/**
 * Consumer-driven search index contract for the production Supabase data plane.
 *
 * This module is deliberately read-only. It does not create indexes: large-table
 * indexes must be built with the migration owner's concurrent rollout machinery.
 */
export type SearchIndexRequirement = {
  id: string;
  indexName: string;
  tableName: string;
  consumers: readonly string[];
  queryShape: string;
  definitionFragments: readonly string[];
  reloptionFragments?: readonly string[];
  expectedPredicate?: string;
  requiresAgentLookupRollout?: boolean;
};

export const REQUIRED_SEARCH_INDEXES = [
  {
    id: "workspace-recent-sessions",
    indexName: "session_rollups_workspace_recent_idx",
    tableName: "session_rollups",
    consumers: ["Cloud sessions dashboard", "Cloud MCP list_session_timings"],
    queryShape: "org + workspace equality; newest last_ts keyset page",
    definitionFragments: [
      "on sessions.session_rollups",
      "(org_id, workspace_id, last_ts desc, session_id desc, source desc)",
    ],
  },
  {
    id: "workspace-source-recent-sessions",
    indexName: "session_rollups_workspace_source_recent_idx",
    tableName: "session_rollups",
    consumers: ["Cloud sessions dashboard harness/source filter", "Cloud MCP list_session_timings"],
    queryShape: "org + workspace + source equality; newest last_ts keyset page",
    definitionFragments: [
      "on sessions.session_rollups",
      "(org_id, workspace_id, source, last_ts desc, session_id desc)",
    ],
  },
  {
    id: "workspace-project-recent-sessions",
    indexName: "session_rollups_workspace_project_recent_idx",
    tableName: "session_rollups",
    consumers: ["Cloud sessions dashboard project filter", "Cloud MCP list_session_timings"],
    queryShape: "org + workspace + project equality; newest last_ts keyset page",
    definitionFragments: [
      "on sessions.session_rollups",
      "(org_id, workspace_id, project_id, last_ts desc, session_id desc, source desc)",
    ],
  },
  {
    id: "workspace-event-time",
    indexName: "convergence_events_workspace_recent_idx",
    tableName: "convergence_events",
    consumers: ["Cloud MCP search_shared_sessions", "Cloud MCP context timestamp windows"],
    queryShape: "org + workspace equality; ts range and event keyset order",
    definitionFragments: [
      "on sessions.convergence_events",
      "(org_id, workspace_id, ts desc, event_id desc, machine_id desc, source desc, session_id desc, kind desc)",
    ],
  },
  {
    id: "workspace-source-event-time",
    indexName: "convergence_events_workspace_source_recent_idx",
    tableName: "convergence_events",
    consumers: ["Cloud MCP search_shared_sessions source filter", "Cloud MCP team activity"],
    queryShape: "org + workspace + source equality; ts range and event keyset order",
    definitionFragments: [
      "on sessions.convergence_events",
      "(org_id, workspace_id, source, ts desc, event_id desc, machine_id desc, session_id desc, kind desc)",
    ],
  },
  {
    id: "workspace-project-event-time",
    indexName: "convergence_events_workspace_project_recent_idx",
    tableName: "convergence_events",
    consumers: ["Cloud MCP search_shared_sessions project filter", "Cloud MCP list_session_timings"],
    queryShape: "org + workspace + project equality; ts range and event keyset order",
    definitionFragments: [
      "on sessions.convergence_events",
      "(org_id, workspace_id, project_id, ts desc, event_id desc, machine_id desc, source desc, session_id desc, kind desc)",
    ],
  },
  {
    id: "workspace-session-replay",
    indexName: "convergence_events_workspace_session_replay_idx",
    tableName: "convergence_events",
    consumers: ["Cloud MCP get_shared_session", "Cloud MCP get_shared_session_context", "Cloud MCP team activity"],
    queryShape: "org + workspace + session equality; optional source filter; ordered transcript replay",
    definitionFragments: [
      "on sessions.convergence_events",
      "(org_id, workspace_id, session_id, ts, event_id, machine_id, source, kind)",
    ],
  },
  {
    id: "session-opening-prompt",
    indexName: "convergence_events_opening_prompt_idx",
    tableName: "convergence_events",
    consumers: ["Cloud sessions dashboard summaries"],
    queryShape: "tenant/session/source/workspace equality; oldest nonblank user prompt",
    definitionFragments: [
      "on sessions.convergence_events",
      "(org_id, session_id, source, workspace_id, ts, event_id)",
    ],
    expectedPredicate:
      "(((kind = ANY (ARRAY['history'::text, 'prompt'::text, 'user'::text])) OR ((kind = 'session_event'::text) AND (actor_role = 'user'::text))) AND (length(TRIM(BOTH FROM COALESCE(content, ''::text))) > 0))",
  },
  {
    id: "transcript-content-substring",
    indexName: "convergence_events_content_prefix_trgm_idx",
    tableName: "convergence_events",
    consumers: ["Cloud MCP search_shared_sessions", "Cloud MCP get_shared_session_context query"],
    queryShape: "case-insensitive substring over left(content, 4096)",
    definitionFragments: [
      "on sessions.convergence_events using gin",
      "left(content, 4096)",
      "gin_trgm_ops",
    ],
    reloptionFragments: ["fastupdate=off"],
  },
  {
    id: "task-title-substring",
    indexName: "convergence_events_task_title_direct_trgm_idx",
    tableName: "convergence_events",
    consumers: ["Cloud MCP search_shared_sessions", "RelayHistory event search"],
    queryShape: "case-insensitive substring over task_title",
    definitionFragments: [
      "on sessions.convergence_events using gin",
      "task_title",
      "gin_trgm_ops",
    ],
    reloptionFragments: ["fastupdate=off"],
    expectedPredicate: "(task_title IS NOT NULL)",
  },
  {
    id: "project-substring",
    indexName: "session_rollups_project_id_trgm_idx",
    tableName: "session_rollups",
    consumers: ["RelayHistory projectContains", "Cloud sessions project search"],
    queryShape: "case-insensitive leading-wildcard project_id match",
    definitionFragments: [
      "on sessions.session_rollups using gin",
      "project_id",
      "gin_trgm_ops",
    ],
  },
  {
    id: "session-timing-marker-lookup",
    indexName: "session_markers_timing_lookup_idx",
    tableName: "session_markers",
    consumers: ["Cloud MCP list_session_timings", "Cloud session timing"],
    queryShape: "tenant + source + session equality during a cold timing-cache fill",
    definitionFragments: [
      "on sessions.session_markers",
      "(org_id, source, session_id)",
    ],
  },
  {
    id: "agent-name-lookup",
    indexName: "session_catalog_agents_lookup_idx",
    tableName: "session_catalog_agents",
    consumers: ["Cloud sessions agent filter", "Relay Desktop agent-name parity"],
    queryShape: "tenant/workspace normalized exact or prefix agent name; newest sessions first",
    definitionFragments: [
      "on sessions.session_catalog_agents",
      "(org_id, workspace_id, agent_name_normalized text_pattern_ops, last_ts desc, session_id desc, source desc)",
    ],
    expectedPredicate: "((agent_name_normalized IS NOT NULL) AND (last_ts IS NOT NULL))",
    requiresAgentLookupRollout: true,
  },
] as const satisfies readonly SearchIndexRequirement[];

export type SearchIndexCatalogRow = {
  table_name: string;
  index_name: string;
  index_definition: string;
  predicate: string | null;
  is_valid: boolean;
  is_ready: boolean;
  reloptions: string | null;
};

export const SEARCH_INDEX_CATALOG_SQL = `
SELECT table_rel.relname AS table_name,
       index_rel.relname AS index_name,
       pg_get_indexdef(index_rel.oid) AS index_definition,
       pg_get_expr(catalog.indpred, catalog.indrelid) AS predicate,
       catalog.indisvalid AS is_valid,
       catalog.indisready AS is_ready,
       array_to_string(index_rel.reloptions, ',') AS reloptions
  FROM pg_catalog.pg_index AS catalog
  JOIN pg_catalog.pg_class AS table_rel ON table_rel.oid = catalog.indrelid
  JOIN pg_catalog.pg_class AS index_rel ON index_rel.oid = catalog.indexrelid
  JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = table_rel.relnamespace
 WHERE namespace.nspname = 'sessions'
 ORDER BY table_rel.relname, index_rel.relname`;

function normalized(value: string | null | undefined): string {
  return (value ?? "")
    .toLowerCase()
    .replace(/["']/g, "")
    .replace(/\s+/g, " ")
    .replace(/\s*,\s*/g, ", ")
    .trim();
}

function normalizedPredicate(value: string | null | undefined): string {
  return (value ?? "")
    .toLowerCase()
    .replace(/"/g, "")
    .replace(/\s+/g, " ")
    .replace(/\s*,\s*/g, ", ")
    .trim();
}

export type SearchIndexReadiness = {
  agentLookupRolloutComplete: boolean;
};

export type SearchIndexAuditItem = {
  id: string;
  indexName: string;
  tableName: string;
  ready: boolean;
  reasons: string[];
};

export function auditSearchIndexes(
  rows: readonly SearchIndexCatalogRow[],
  readiness: SearchIndexReadiness,
): {
  passed: boolean;
  indexes: SearchIndexAuditItem[];
} {
  const byName = new Map(rows.map((row) => [row.index_name, row]));
  const indexes = REQUIRED_SEARCH_INDEXES.map((requirement) => {
    const row = byName.get(requirement.indexName);
    const reasons: string[] = [];
    if (!row) {
      reasons.push("missing");
    } else {
      if (row.table_name !== requirement.tableName) reasons.push("wrong table");
      if (!row.is_valid) reasons.push("invalid");
      if (!row.is_ready) reasons.push("not ready");
      const definition = normalized(row.index_definition);
      for (const fragment of requirement.definitionFragments) {
        if (!definition.includes(normalized(fragment))) reasons.push(`definition missing: ${fragment}`);
      }
      const expectedPredicate = "expectedPredicate" in requirement
        ? requirement.expectedPredicate
        : null;
      if (normalizedPredicate(row.predicate) !== normalizedPredicate(expectedPredicate)) {
        reasons.push(expectedPredicate ? "predicate mismatch" : "unexpected predicate");
      }
      const options = normalized(row.reloptions);
      const reloptionFragments = "reloptionFragments" in requirement
        ? requirement.reloptionFragments
        : [];
      for (const fragment of reloptionFragments) {
        if (!options.includes(normalized(fragment))) reasons.push(`option missing: ${fragment}`);
      }
    }
    if (
      "requiresAgentLookupRollout" in requirement &&
      requirement.requiresAgentLookupRollout &&
      !readiness.agentLookupRolloutComplete
    ) {
      reasons.push("agent lookup rollout incomplete");
    }
    return {
      id: requirement.id,
      indexName: requirement.indexName,
      tableName: requirement.tableName,
      ready: reasons.length === 0,
      reasons,
    };
  });
  return { passed: indexes.every((index) => index.ready), indexes };
}
