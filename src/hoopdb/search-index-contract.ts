/**
 * Consumer-driven metadata index contract for decision 0002.
 *
 * Canonical transcript/event content lives in object storage. This contract only
 * covers the rebuildable Supabase logical-session directory and its metadata.
 * Transcript search belongs to ClickHouse/ObsessionDB and MUST NOT be added to
 * session_directory.search_text. The one derived-content exception is the bounded,
 * scrubbed short_summary (G6). The helm-chart-chief coordinator recorded the user's
 * direct approval to implement bounded short_summary work in its controlling session.
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
  requiresDirectoryProjection?: boolean;
};

/** Prefixes shorter than this are too broad for the ordered btree contract. */
export const MIN_AGENT_PREFIX_CHARS = 3;

/** Deterministic gate for production-shaped prefix benchmarks. */
export const AGENT_PREFIX_BENCHMARK_GATE = {
  pageSize: 50,
  maxIndexRowsExaminedPerResult: 100,
} as const;

export const SESSION_DIRECTORY_SEARCH_BOUNDARY = {
  metadataOnly: true,
  fields: [
    "title",
    "task_title",
    "agent_name",
    "project_id",
    "git_repo",
    "git_branch",
    "model",
    // Scrubbed, <= 1000 characters, written only by session-directory-summary.ts.
    "short_summary",
  ],
  excluded: ["transcript content", "event content", "tool payloads", "long summary", "embeddings"],
  transcriptBackend: "ClickHouse/ObsessionDB (not yet available)",
} as const;

export function normalizeAgentName(value: string): string {
  return value.trim().toLowerCase();
}

export function isUsefulAgentPrefix(value: string): boolean {
  return Array.from(normalizeAgentName(value)).length >= MIN_AGENT_PREFIX_CHARS;
}

export function agentPrefixPattern(value: string): string {
  const normalized = normalizeAgentName(value);
  if (Array.from(normalized).length < MIN_AGENT_PREFIX_CHARS) {
    throw new RangeError(`agent prefix must contain at least ${MIN_AGENT_PREFIX_CHARS} characters`);
  }
  return `${normalized.replace(/[\\%_]/g, "\\$&")}%`;
}

/** Parameterized definitions shared by plan tests and future API adapters. */
export const SESSION_DIRECTORY_QUERIES = {
  workspaceRecent: `
SELECT * FROM sessions.session_directory
 WHERE org_id = $1 AND workspace_id = $2 AND deleted_at IS NULL
 ORDER BY last_activity_at DESC, session_id DESC, harness DESC
 LIMIT $3`,
  harnessRecent: `
SELECT * FROM sessions.session_directory
 WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND deleted_at IS NULL
 ORDER BY last_activity_at DESC, session_id DESC
 LIMIT $4`,
  projectRecent: `
SELECT * FROM sessions.session_directory
 WHERE org_id = $1 AND workspace_id = $2 AND project_id = $3 AND deleted_at IS NULL
 ORDER BY last_activity_at DESC, session_id DESC, harness DESC
 LIMIT $4`,
  withoutProjectRecent: `
SELECT * FROM sessions.session_directory
 WHERE org_id = $1 AND workspace_id = $2 AND project_id IS NULL AND deleted_at IS NULL
 ORDER BY last_activity_at DESC, session_id DESC, harness DESC
 LIMIT $3`,
  agentExact: `
SELECT * FROM sessions.session_directory
 WHERE org_id = $1 AND workspace_id = $2
   AND agent_name_normalized = $3 AND deleted_at IS NULL
 ORDER BY last_activity_at DESC, session_id DESC, harness DESC
 LIMIT $4`,
  agentPrefix: `
SELECT * FROM sessions.session_directory
 WHERE org_id = $1 AND workspace_id = $2
   AND agent_name_normalized LIKE $3 ESCAPE '\\' AND deleted_at IS NULL
 ORDER BY last_activity_at DESC, session_id DESC, harness DESC
 LIMIT $4`,
  createdRange: `
SELECT * FROM sessions.session_directory
 WHERE org_id = $1 AND workspace_id = $2 AND created_at >= $3
   AND created_at < $4 AND deleted_at IS NULL
 ORDER BY created_at DESC, session_id DESC, harness DESC
 LIMIT $5`,
  stateRecent: `
SELECT * FROM sessions.session_directory
 WHERE org_id = $1 AND workspace_id = $2 AND state = $3 AND deleted_at IS NULL
 ORDER BY last_activity_at DESC, session_id DESC, harness DESC
 LIMIT $4`,
  userRecent: `
SELECT d.* FROM sessions.session_directory_users AS membership
 JOIN sessions.session_directory AS d USING (org_id, workspace_id, harness, session_id)
 WHERE membership.org_id = $1 AND membership.workspace_id = $2
   AND membership.user_id = $3 AND d.deleted_at IS NULL
 ORDER BY membership.last_activity_at DESC, membership.session_id DESC, membership.harness DESC
 LIMIT $4`,
  machineRecent: `
SELECT d.* FROM sessions.session_directory_machines AS membership
 JOIN sessions.session_directory AS d USING (org_id, workspace_id, harness, session_id)
 WHERE membership.org_id = $1 AND membership.workspace_id = $2
   AND membership.machine_id = $3 AND d.deleted_at IS NULL
 ORDER BY membership.last_activity_at DESC, membership.session_id DESC, membership.harness DESC
 LIMIT $4`,
} as const;

/**
 * The tenant-bound search and list RPC: the only path over search_text (and so
 * short_summary), and the V1 directory path for every filter the RelayHistory read
 * surface exposes. Pinned contract for RelayHistory #226; changing any of it is a
 * contract change. Call through PostgREST as `POST /rest/v1/rpc/search_session_directory`
 * with `Content-Profile: sessions`, named `p_*` arguments, and a JWT whose `role` is the
 * Worker role and whose `app_metadata` carries `org_id` and `workspace_ids`.
 * `searchRpcRequest` (search-query.ts) builds a request under the same rules.
 */
export const SESSION_DIRECTORY_SEARCH_RPC = {
  name: "sessions.search_session_directory",
  args: {
    p_workspace_id: "text, NULL = organization-wide over the claimed workspaces",
    p_query: "text: substring of search_text (metadata + scrubbed short_summary)",
    p_harness: "text: exact harness (the read API's `source`)",
    p_project_id: "text: exact project",
    p_project_contains: "text: case-insensitive project substring",
    p_no_project: "boolean: sessions without a project",
    p_agent: "text: normalized agent name",
    p_agent_match: "'exact' (default) | 'prefix'",
    p_state: "'active' | 'done' | 'abandoned' | 'unknown'",
    p_user_id: "text: user membership",
    p_machine_id: "text: machine membership",
    p_since: "timestamptz: last_activity_at >= since",
    p_until: "timestamptz: created_at <= until (activity interval overlaps the window)",
    p_session_id: "text: exact session id",
    p_cursor_last_activity_at: "timestamptz", p_cursor_session_id: "text", p_cursor_harness: "text",
    p_cursor_workspace_id: "text: required for organization-wide pages, else equal to p_workspace_id or NULL",
    p_limit: "integer 1..50 (default 50); the RPC returns up to p_limit + 1 rows",
  },
  workerRole: "session_directory_search_worker",
  claims: { role: "session_directory_search_worker", orgId: "app_metadata.org_id", workspaceIds: "app_metadata.workspace_ids" },
  /** Meaningful characters after normalizeSearchQuery: not separators, format characters or marks. */
  minQueryChars: 3,
  maxQueryChars: 200,
  minAgentPrefixChars: 3,
  defaultLimit: 50,
  maxLimit: 50,
  /** At most maxLimit + 1 rows: a full page plus one row that signals more. */
  maxRows: 51,
  order: "last_activity_at DESC, session_id DESC, harness DESC (organization-wide: then workspace_id DESC)",
  cursor: "(last_activity_at, session_id, harness[, workspace_id]) of the last returned row; strictly older rows follow",
  returns: [
    "org_id", "workspace_id", "harness", "session_id", "created_at", "last_activity_at",
    "title", "task_title", "agent_name", "project_id", "git_repo", "git_branch", "model", "state",
    "event_count", "input_tokens", "output_tokens", "cost_usd_micros",
    "machine_ids", "user_ids", "short_summary", "projected_at",
  ],
  errors: {
    "42501": "caller is not the Worker role, or tenant claims missing, malformed, for another role, or not covering the workspace",
    "22023": "an invalid value: text under the meaningful-character rule, limit, state, agent match, harness, cursor, since > until, or a contradictory pair",
    "0A000": "an intentionally unsupported combination (see unsupported)",
    "57014": "statement_timeout (2s on the Worker role) exceeded",
  },
  /** Returned as 0A000, never as empty results. */
  unsupported: [
    "organization-wide (no p_workspace_id) with p_agent, p_project_id, p_project_contains, p_no_project, p_user_id or p_machine_id",
    "transcript/event text search (V2 search store)",
    "ordering other than recency",
  ],
  /** Returned as 22023. */
  contradictory: ["p_no_project with p_project_id or p_project_contains", "p_since later than p_until"],
} as const;

/**
 * The exact statement the RPC executes for a workspace-scoped text search with no other
 * filter (whitespace aside). Plan tests compare auto_explain's Query Text to it; other
 * shapes add the fixed fragments listed in the migration.
 */
export const SESSION_DIRECTORY_SEARCH_SQL = `
SELECT d.org_id, d.workspace_id, d.harness, d.session_id, d.created_at, d.last_activity_at,
 d.title, d.task_title, d.agent_name, d.project_id, d.git_repo, d.git_branch, d.model, d.state,
 d.event_count, d.input_tokens, d.output_tokens, d.cost_usd_micros,
 d.machine_ids, d.user_ids, d.short_summary, d.projected_at
 FROM sessions.session_directory AS d
 WHERE d.org_id = $1 AND d.deleted_at IS NULL AND d.workspace_id = $2
 AND d.search_text <> '' AND d.search_text ILIKE $4 ESCAPE '\\'
 ORDER BY d.last_activity_at DESC, d.session_id DESC, d.harness DESC LIMIT $19`;

export const REQUIRED_SEARCH_INDEXES = [
  {
    id: "workspace-recent-sessions", indexName: "session_directory_workspace_recent_idx",
    tableName: "session_directory", consumers: ["Cloud Sessions", "Cloud MCP list_session_timings"],
    queryShape: "org + workspace equality; logical-session activity keyset",
    definitionFragments: ["on sessions.session_directory", "(org_id, workspace_id, last_activity_at desc, session_id desc, harness desc)"],
    expectedPredicate: "(deleted_at IS NULL)", requiresDirectoryProjection: true,
  },
  {
    id: "org-recent-sessions", indexName: "session_directory_org_recent_idx",
    tableName: "session_directory", consumers: ["Cloud org-wide session list"],
    queryShape: "org equality; cross-workspace logical-session activity keyset",
    definitionFragments: ["on sessions.session_directory", "(org_id, last_activity_at desc, session_id desc, harness desc, workspace_id desc)"],
    expectedPredicate: "(deleted_at IS NULL)", requiresDirectoryProjection: true,
  },
  {
    id: "workspace-harness-recent-sessions", indexName: "session_directory_harness_recent_idx",
    tableName: "session_directory", consumers: ["Cloud Sessions harness/source filter", "Cloud MCP"],
    queryShape: "org + workspace + harness equality; activity keyset",
    definitionFragments: ["on sessions.session_directory", "(org_id, workspace_id, harness, last_activity_at desc, session_id desc)"],
    expectedPredicate: "(deleted_at IS NULL)", requiresDirectoryProjection: true,
  },
  {
    id: "workspace-project-recent-sessions", indexName: "session_directory_project_recent_idx",
    tableName: "session_directory", consumers: ["Cloud Sessions project filter", "Cloud MCP"],
    queryShape: "org + workspace + exact project; activity keyset",
    definitionFragments: ["on sessions.session_directory", "(org_id, workspace_id, project_id, last_activity_at desc, session_id desc, harness desc)"],
    expectedPredicate: "((project_id IS NOT NULL) AND (deleted_at IS NULL))", requiresDirectoryProjection: true,
  },
  {
    id: "workspace-no-project-recent-sessions", indexName: "session_directory_no_project_recent_idx",
    tableName: "session_directory", consumers: ["Cloud Sessions unassigned-project filter"],
    queryShape: "org + workspace + no project; activity keyset",
    definitionFragments: ["on sessions.session_directory", "(org_id, workspace_id, last_activity_at desc, session_id desc, harness desc)"],
    expectedPredicate: "((project_id IS NULL) AND (deleted_at IS NULL))", requiresDirectoryProjection: true,
  },
  {
    id: "agent-name-exact-or-prefix", indexName: "session_directory_agent_recent_idx",
    tableName: "session_directory", consumers: ["Cloud Sessions agent filter", "Relay Desktop remote parity"],
    queryShape: "normalized exact or >=3-character prefix agent name; activity keyset",
    definitionFragments: ["on sessions.session_directory", "(org_id, workspace_id, agent_name_normalized text_pattern_ops, last_activity_at desc, session_id desc, harness desc)"],
    expectedPredicate: "((agent_name_normalized IS NOT NULL) AND (deleted_at IS NULL))", requiresDirectoryProjection: true,
  },
  {
    id: "workspace-created-range", indexName: "session_directory_created_recent_idx",
    tableName: "session_directory", consumers: ["Cloud Sessions created filter"],
    queryShape: "org + workspace + created range; created keyset",
    definitionFragments: ["on sessions.session_directory", "(org_id, workspace_id, created_at desc, session_id desc, harness desc)"],
    expectedPredicate: "(deleted_at IS NULL)", requiresDirectoryProjection: true,
  },
  {
    id: "workspace-state-recent", indexName: "session_directory_state_recent_idx",
    tableName: "session_directory", consumers: ["Cloud Sessions state filter"],
    queryShape: "org + workspace + state; activity keyset",
    definitionFragments: ["on sessions.session_directory", "(org_id, workspace_id, state, last_activity_at desc, session_id desc, harness desc)"],
    expectedPredicate: "(deleted_at IS NULL)", requiresDirectoryProjection: true,
  },
  {
    id: "session-id-lookup", indexName: "session_directory_session_lookup_idx",
    tableName: "session_directory", consumers: ["Cloud session open", "Cloud MCP"],
    queryShape: "org + session id when workspace and harness are unknown",
    definitionFragments: ["on sessions.session_directory", "(org_id, session_id)"],
    requiresDirectoryProjection: true,
  },
  {
    id: "tenant-project-substring", indexName: "session_directory_tenant_project_trgm_idx",
    tableName: "session_directory", consumers: ["sessions.search_session_directory p_project_contains (RelayHistory projectContains)"],
    queryShape: "org + workspace scoped case-insensitive project substring",
    definitionFragments: ["on sessions.session_directory using gin", "(org_id, workspace_id, project_id", "gin_trgm_ops)"],
    expectedPredicate: "((project_id IS NOT NULL) AND (deleted_at IS NULL))", requiresDirectoryProjection: true,
  },
  {
    id: "tenant-search-substring", indexName: "session_directory_tenant_search_trgm_idx",
    tableName: "session_directory", consumers: ["sessions.search_session_directory (RelayHistory #226 metadata search)"],
    queryShape: "org + workspace scoped case-insensitive substring over search_text (metadata plus scrubbed short_summary)",
    definitionFragments: ["on sessions.session_directory using gin", "(org_id, workspace_id, search_text", "gin_trgm_ops)"],
    expectedPredicate: "((search_text <> ''::text) AND (deleted_at IS NULL))", requiresDirectoryProjection: true,
  },
  {
    id: "user-membership-recent", indexName: "session_directory_users_recent_idx",
    tableName: "session_directory_users", consumers: ["Cloud Sessions person filter", "Cloud MCP user filter"],
    queryShape: "org + workspace + user membership; activity keyset",
    definitionFragments: ["on sessions.session_directory_users", "(org_id, workspace_id, user_id, last_activity_at desc, session_id desc, harness desc)"],
    requiresDirectoryProjection: true,
  },
  {
    id: "machine-membership-recent", indexName: "session_directory_machines_recent_idx",
    tableName: "session_directory_machines", consumers: ["Cloud machine filter"],
    queryShape: "org + workspace + machine membership; activity keyset",
    definitionFragments: ["on sessions.session_directory_machines", "(org_id, workspace_id, machine_id, last_activity_at desc, session_id desc, harness desc)"],
    requiresDirectoryProjection: true,
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
  return (value ?? "").toLowerCase().replace(/["']/g, "").replace(/\s+/g, " ")
    .replace(/\s*,\s*/g, ", ").trim();
}

function normalizedPredicate(value: string | null | undefined): string {
  const literals: string[] = [];
  let masked = "";
  const input = value ?? "";
  for (let index = 0; index < input.length;) {
    if (input[index] !== "'") { masked += input[index]; index += 1; continue; }
    const start = index;
    index += 1;
    while (index < input.length) {
      if (input[index] !== "'") { index += 1; continue; }
      if (input[index + 1] === "'") { index += 2; continue; }
      index += 1;
      break;
    }
    masked += `\u0001${literals.length}\u0002`;
    literals.push(input.slice(start, index));
  }
  return masked.toLowerCase().replace(/"/g, "").replace(/\s+/g, " ")
    .replace(/\s*,\s*/g, ", ").trim()
    .replace(/\u0001(\d+)\u0002/g, (_marker, offset: string) => literals[Number(offset)] ?? "");
}

export type SearchIndexReadiness = { directoryProjectionComplete: boolean };
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
): { passed: boolean; indexes: SearchIndexAuditItem[] } {
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
      const expectedPredicate = "expectedPredicate" in requirement ? requirement.expectedPredicate : null;
      if (normalizedPredicate(row.predicate) !== normalizedPredicate(expectedPredicate)) {
        reasons.push(expectedPredicate ? "predicate mismatch" : "unexpected predicate");
      }
      const options = normalized(row.reloptions);
      const reloptionFragments: readonly string[] = "reloptionFragments" in requirement
        ? requirement.reloptionFragments as readonly string[]
        : [];
      for (const fragment of reloptionFragments) {
        if (!options.includes(normalized(fragment))) reasons.push(`option missing: ${fragment}`);
      }
    }
    if ("requiresDirectoryProjection" in requirement && requirement.requiresDirectoryProjection &&
        !readiness.directoryProjectionComplete) {
      reasons.push("directory projection incomplete");
    }
    return {
      id: requirement.id, indexName: requirement.indexName, tableName: requirement.tableName,
      ready: reasons.length === 0, reasons,
    };
  });
  return { passed: indexes.every((index) => index.ready), indexes };
}
