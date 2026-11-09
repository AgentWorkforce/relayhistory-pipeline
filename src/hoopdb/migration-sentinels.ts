/**
 * One immutable catalog sentinel per schema step: a read-only predicate that is false
 * before the step and true from the step on (it stays true after every later step).
 * The migrator refuses (unledgered_schema) when an unrecorded step's sentinel is
 * already true, which catches steps the CREATE inventory can't see: ALTER-only steps
 * and CREATE OR REPLACE replacements.
 *
 * Tests prove, on PGlite and on real PostgreSQL 15 and 17:
 * - the table names every schema step;
 * - each sentinel is false after the steps before it and true after it, and all of
 *   them are true after the full set;
 * - for each ALTER or replacement step, an honestly applied and ledgered state is
 *   accepted, and an applied-but-unledgered state is refused before any write.
 *
 * Adding a migration means adding its sentinel here; the coverage test fails until you
 * do.
 */
const relation = (name: string) =>
  `SELECT to_regclass('${name}') IS NOT NULL AS applied`;
const column = (table: string, col: string) =>
  `SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('${table}') AND attname = '${col}' AND NOT attisdropped) AS applied`;
const fn = (schema: string, name: string) =>
  `SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = '${schema}' AND p.proname = '${name}') AS applied`;

export interface Sentinel {
  /** What the step leaves behind that no earlier step does. */
  evidence: string;
  /** A read-only query returning one row with a boolean `applied`. */
  sql: string;
}

/** An index that an unledgered step may find already built out of band, and adopt. */
export interface AdoptableIndex {
  /** Index name, in the sessions schema. */
  name: string;
  /** The table it must be on, schema-qualified. */
  table: string;
  /** Exactly what pg_get_indexdef must return for it. */
  def: string;
}

const CLAIM_BOOTSTRAP_ORG_INDEXES_STEP =
  "upstream:20261105044020_claim_bootstrap_org_indexes.sql";
const AGENT_MATCH_STEP = "upstream:20261105050020_session_directory_agent_match.sql";
const orgIndex = (table: string, columns: string): AdoptableIndex => ({
  name: `${table}_org_id_idx`,
  table: `sessions.${table}`,
  def: `CREATE INDEX ${table}_org_id_idx ON sessions.${table} USING btree (${columns})`,
});

/**
 * The reviewed allowance for indexes built out of band before the migration that names
 * them is ledgered (an operator CREATE INDEX CONCURRENTLY on a large production table:
 * the migrator is transactional and cannot build concurrently). For the steps listed here,
 * and only for these names, sequencePreflight accepts a present-but-unledgered index when
 * it is the only index of that name, sits in sessions on exactly that table, is valid,
 * ready and live, and its pg_get_indexdef is exactly `def`. Anything else of that name is
 * still unledgered_schema. The step's migration adopts the same definitions (it skips an
 * exact valid index without locking its table, creates a missing one, and raises
 * otherwise); a test keeps the two byte-identical.
 *
 * Claim-bootstrap org_id probes, 20261105044020 (packet:
 * docs/packets/claim-bootstrap-org-index-packet-20261108.md), in the operator's build order.
 */
export const ADOPTABLE_INDEXES: Readonly<Record<string, readonly AdoptableIndex[]>> = {
  [CLAIM_BOOTSTRAP_ORG_INDEXES_STEP]: [
    orgIndex("ingest_history_mask_pages", "org_id"),
    orgIndex("ingest_history_plans", "org_id, workspace_id, user_id"),
    orgIndex("ingest_seed_captures", "org_id"),
    orgIndex("ingest_publication_attempts", "org_id, workspace_id, user_id"),
    orgIndex("ingest_publication_generations", "org_id, workspace_id, user_id"),
    orgIndex("ingest_stage_intents", "org_id, workspace_id, user_id"),
    orgIndex("ingest_custody_records", "org_id, workspace_id, user_id"),
    orgIndex("ingest_lazy_native_slots", "org_id"),
  ],
  // Agent-name prefix index, 20261105050020 (docs/agent-name-search-contract.md). That step also
  // creates a function overload: see ADOPTABLE_STEP_OTHER_OBJECTS.
  [AGENT_MATCH_STEP]: [
    {
      name: "session_directory_agents_prefix_idx",
      table: "sessions.session_directory_agents",
      def: "CREATE INDEX session_directory_agents_prefix_idx ON sessions.session_directory_agents USING btree (org_id, workspace_id, agent_name_normalized text_pattern_ops, last_activity_at DESC, session_id DESC, harness DESC)",
    },
  ],
};

/**
 * The reviewed non-index inventory of an adoptable step (keys as `kind:name`). By default the
 * allowance holds only while a step's whole CREATE inventory is its declared indexes; a step listed
 * here may also inventory exactly these objects. They get no allowance of their own: each is still
 * checked as usual, and the step's own catalog sentinel (not the declared-index sentinel) still
 * decides whether the step already ran, so an unledgered copy of what it creates is still refused.
 *
 * 20261105050020: its inventory names sessions.list_session_directory_for (the 17-argument overload;
 * the inventory is by name, which 20261105034000 owns); its sentinel is that exact overload.
 */
export const ADOPTABLE_STEP_OTHER_OBJECTS: Readonly<Record<string, readonly string[]>> = {
  [AGENT_MATCH_STEP]: ["function:sessions.list_session_directory_for"],
};

/**
 * Read-only catalog lookup (pg_class, pg_index, pg_constraint; no table lock): one row for the declared
 * index ($1 name, $2 schema-qualified table, $3 def), with `state` and `table_exists`. `state` mirrors the
 * step's own decision (20261105044020) and is one of:
 *   absent     no relation of that name in sessions (the step creates it, or refuses on a populated table);
 *   adoptable  the only index of that name anywhere, a plain index in sessions on exactly that table,
 *              pg_get_indexdef exactly `def`, valid, ready and live, and backing no constraint;
 *   not_ready  exact definition and table but INVALID or not ready: a failed CONCURRENTLY build, or one
 *              still running (check pg_stat_progress_create_index before dropping anything);
 *   mismatch   anything else of that name: another relkind, another table or definition, a second index
 *              of that name in another schema, or an index backing a constraint (an EXCLUDE ... WITH =
 *              btree index deparses exactly like the declared one; it is never adopted).
 */
export const ADOPTABLE_INDEX_STATE_SQL = `SELECT CASE
    WHEN r.oid IS NULL THEN 'absent'
    WHEN r.relkind <> 'i' THEN 'mismatch'
    WHEN (SELECT count(*) FROM pg_class WHERE relname = $1 AND relkind IN ('i', 'I')) <> 1 THEN 'mismatch'
    WHEN i.indexrelid IS NULL OR i.indrelid IS DISTINCT FROM to_regclass($2)
      OR pg_get_indexdef(i.indexrelid) IS DISTINCT FROM $3::text THEN 'mismatch'
    WHEN i.indisexclusion OR EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = i.indexrelid) THEN 'mismatch'
    WHEN NOT (i.indisvalid AND i.indisready AND i.indislive) THEN 'not_ready'
    ELSE 'adoptable' END AS state,
  to_regclass($2) IS NOT NULL AS table_exists
  FROM (SELECT 1) one
  LEFT JOIN pg_class r ON r.relnamespace = to_regnamespace('sessions') AND r.relname = $1
  LEFT JOIN pg_index i ON i.indexrelid = r.oid`;

/** All of a step's adoptable indexes are present, exact, valid and ready, and back no constraint. */
const adoptableIndexesSentinel = (step: string) =>
  `SELECT count(*) = ${ADOPTABLE_INDEXES[step]!.length} AS applied FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
    WHERE c.relnamespace = to_regnamespace('sessions') AND i.indisvalid AND i.indisready AND i.indislive
      AND NOT i.indisexclusion AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = i.indexrelid)
      AND (c.relname, i.indrelid, pg_get_indexdef(i.indexrelid)) IN (${ADOPTABLE_INDEXES[step]!.map((x) => `('${x.name}', to_regclass('${x.table}'), '${x.def}')`).join(", ")})`;

export const MIGRATION_SENTINELS: Record<string, Sentinel> = {
  "upstream:20261105041000_ingest_privacy_authority.sql": {
    evidence: "table sessions.ingest_privacy_authority + lazy-current adoption uses the EXACT source-table list (no prefix)",
    sql: "SELECT to_regclass('sessions.ingest_privacy_authority') IS NOT NULL AND (SELECT count(*)=2 FROM pg_proc WHERE oid IN(to_regprocedure('sessions.ingest_lazy_user(jsonb,text,jsonb)'),to_regprocedure('sessions.ingest_lazy_begin(jsonb,text)')) AND position('NOT IN(''ingest_frozen_source_records''' in prosrc)>0 AND position('''ingest_frozen_source_owned_sessions'')' in prosrc)>0 AND position('NOT LIKE ''ingest' in prosrc)=0) AS applied",
  },
  "upstream:20261105040000_ingest_lazy_source_tables.sql": {
    evidence: "replacement: lazy-current adoption checks exclude the source-side tables",
    sql: "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE oid=to_regprocedure('sessions.ingest_lazy_user(jsonb,text,jsonb)') AND prosrc LIKE '%ingest_source_control_heads%') AND EXISTS(SELECT 1 FROM pg_proc WHERE oid=to_regprocedure('sessions.ingest_lazy_begin(jsonb,text)') AND prosrc LIKE '%ingest_source_control_heads%') AS applied",
  },
  "upstream:20261105034000_session_directory_read_api.sql": {evidence:"function sessions.list_session_directory_for",sql:fn("sessions","list_session_directory_for")},
  "upstream:20261105039000_buffered_gc_single_candidate.sql": {
    evidence: "replacement: GC claim has its single-candidate advisory admission domain",
    sql: "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE oid=to_regprocedure('sessions.ingest_buffer_gc_claim(jsonb)') AND prosecdef AND prosrc LIKE '%96bulls:buffer-gc-user-v1%') AS applied",
  },
  "upstream:20261105043850_ingest_acquisition_owner.sql": { evidence: "table sessions.ingest_acquisition_owners", sql: relation("sessions.ingest_acquisition_owners") },
  "upstream:20261105043000_brief_titles.sql": { evidence: "column sessions.session_briefs.origin", sql: column("sessions.session_briefs", "origin") },
  "upstream:20261105043500_lazy_claim_clock_narrowing.sql": {
    evidence: "replacement: lazy claim reads the seat clock under KEY SHARE with one advisory lock per (org, workspace, origin)",
    sql: "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE oid=to_regprocedure('sessions.ingest_lazy_records(jsonb)') AND NOT prosecdef AND prosrc LIKE '%lazy-native-origin-v1%') AS applied",
  },
  "upstream:20261105043700_owner_buffered_read.sql": { evidence: "function sessions.buffered_sessions_for", sql: "SELECT to_regprocedure('sessions.buffered_sessions_for(text,timestamptz,text,text,integer,text,text,text,text,text,boolean,text)') IS NOT NULL AS applied" },
  "upstream:20261105043800_lazy_ack_clock_no_key_update.sql": {
    evidence: "replacement: staged ACK locks the seat clock FOR NO KEY UPDATE (claims' KEY SHARE no longer queues behind it), then the bounded origin advisory lock before any slot",
    sql: "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE oid=to_regprocedure('sessions.ingest_lazy_ack_batch(jsonb)') AND NOT prosecdef AND prosrc LIKE '%WHERE singleton FOR NO KEY UPDATE;%' AND prosrc LIKE '%pg_advisory_xact_lock(hashtextextended(jsonb_build_array(''96bulls:lazy-native-origin-v1'',p->>''org_id'',p->>''workspace_id'',p->>''origin_id'')::text,0));%') AS applied",
  },
  "upstream:20261105043860_buffered_search_gate.sql": { evidence: "function sessions.buffered_search_gate_for", sql: "SELECT to_regprocedure('sessions.buffered_search_gate_for(text,text,text,text,text,text,text,text,boolean,text,jsonb)') IS NOT NULL AS applied" },
  "upstream:20261105043870_buffered_search_feed_cursor.sql": { evidence: "table sessions.buffered_search_feed_cursors", sql: relation("sessions.buffered_search_feed_cursors") },
  "upstream:20261105043880_ingest_bookkeeping_cleanup.sql": {
    evidence: "table sessions.ingest_bookkeeping_retired_claims + Babysitter register carries the retirement interlock",
    sql: "SELECT to_regclass('sessions.ingest_bookkeeping_retired_claims') IS NOT NULL AND EXISTS(SELECT 1 FROM pg_proc WHERE oid=to_regprocedure('sessions.ingest_babysitter_retention_register(jsonb)') AND prosecdef AND prosrc LIKE '%ingest_bookkeeping_claim_lock_key%') AS applied",
  },
  "upstream:20261105043890_buffered_search_gate_retired_witness.sql": {
    evidence: "replacement: buffered_search_gate_for hides claims with an ingest_bookkeeping_retired_claims tombstone (drop reason retired)",
    sql: "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE oid=to_regprocedure('sessions.buffered_search_gate_for(text,text,text,text,text,text,text,text,boolean,text,jsonb)') AND prosecdef AND prosrc LIKE '%ingest_bookkeeping_retired_claims%') AS applied",
  },
  "upstream:20261105024000_session_brief_documents.sql": { evidence: "table sessions.session_brief_document_outbox", sql: relation("sessions.session_brief_document_outbox") },
  "upstream:20261105025000_canonical_feature_journal.sql": { evidence: "table sessions.canonical_feature_journal", sql: relation("sessions.canonical_feature_journal") },
  "upstream:20261105026000_canonical_history_lineage.sql": { evidence: "table sessions.ingest_history_carriers", sql: relation("sessions.ingest_history_carriers") },
  "upstream:20261105030000_canonical_feature_consumer_acks.sql": { evidence: "table sessions.canonical_feature_copy_state", sql: relation("sessions.canonical_feature_copy_state") },
  "upstream:20261105031000_desktop_catalog_forget_reproject.sql": { evidence: "table sessions.session_desktop_catalog_reproject", sql: relation("sessions.session_desktop_catalog_reproject") },
  "upstream:20261105032000_session_large_read_routing_fixes.sql": { evidence: "table sessions.session_large_read_scope_errors", sql: relation("sessions.session_large_read_scope_errors") },
  "upstream:20261105035000_semantic_copy_state.sql": {evidence:"table sessions.search_feature_copy_state",sql:relation("sessions.search_feature_copy_state")},
  "upstream:20261105036000_canonical_consumer_lease.sql": { evidence: "table sessions.canonical_feature_consumer_lease", sql: relation("sessions.canonical_feature_consumer_lease") },
  "upstream:20261105037000_session_directory_document_pool.sql": { evidence: "function sessions.canonical_feature_document_finish", sql: "SELECT to_regprocedure('sessions.canonical_feature_document_finish(bigint,jsonb,text,text,text,text,text)') IS NOT NULL AS applied" },
  "upstream:20261105038000_claim_attempt_records_retirement.sql": { evidence: "column sessions.ingest_claim_attempts.records_retired_at", sql: column("sessions.ingest_claim_attempts", "records_retired_at") },
  "upstream:20261105042000_babysitter_evidence_retention.sql": { evidence: "table sessions.babysitter_retention_objects", sql: relation("sessions.babysitter_retention_objects") },
  "upstream:20261105010000_frozen_source_custody.sql": {evidence:"table sessions.ingest_frozen_source_completeness",sql:relation("sessions.ingest_frozen_source_completeness")},
  "upstream:20261104055000_lazy_current_custody.sql": {evidence:"table sessions.ingest_lazy_policy",sql:relation("sessions.ingest_lazy_policy")},
  "upstream:20261104054000_ingest_custody_baseline.sql": {evidence:"table sessions.ingest_custody_imports",sql:relation("sessions.ingest_custody_imports")},
  "upstream:20261104053000_ingest_first_ack_seats.sql": {evidence:"table sessions.ingest_receipt_seat_clock",sql:relation("sessions.ingest_receipt_seat_clock")},
  "upstream:20261104052000_ordered_ingest_controls.sql": {evidence:"column sessions.ingest_buffer_admission.control_epoch",sql:column("sessions.ingest_buffer_admission","control_epoch")},
  "upstream:20261104060000_ingest_canonical_publication.sql": {evidence:"table sessions.ingest_publication_heads",sql:relation("sessions.ingest_publication_heads")},
  "upstream:20261104051000_ingest_buffer_gc.sql": {evidence:"column sessions.ingest_stage_intents.gc_lease_token",sql:column("sessions.ingest_stage_intents","gc_lease_token")},
  "upstream:20261105021000_session_large_read_jobs.sql": { evidence: "table sessions.session_large_read_jobs", sql: relation("sessions.session_large_read_jobs") },
  "upstream:20261105023000_session_analysis_publications.sql": { evidence: "table sessions.session_analysis_publications", sql: relation("sessions.session_analysis_publications") },
  "upstream:20261105022000_session_desktop_catalog.sql": { evidence: "column sessions.session_directory.catalog_read_version", sql: column("sessions.session_directory", "catalog_read_version") },
  "upstream:20261105020000_session_replay_pages.sql": { evidence: "table sessions.session_replay_publications", sql: relation("sessions.session_replay_publications") },
  "upstream:20261025000000_sessions_index.sql": {
    evidence: "table sessions.session_index",
    sql: relation("sessions.session_index"),
  },
  "upstream:20261025000100_repo_identity.sql": {
    evidence: "table sessions.repos",
    sql: relation("sessions.repos"),
  },
  "upstream:20261025000200_scrub_quarantine.sql": {
    evidence: "function sessions.session_index_scrub_sticky",
    sql: fn("sessions", "session_index_scrub_sticky"),
  },
  "upstream:20261025000300_upsert_keep_on_null.sql": {
    evidence:
      "replacement: sessions.upsert_session_index keeps stored values on NULL (its body has coalesce(excluded.title, s.title))",
    sql: `SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = 'sessions' AND p.proname = 'upsert_session_index'
              AND p.prosrc LIKE '%coalesce(excluded.title, s.title)%') AS applied`,
  },
  "upstream:20261025000600_pipeline_queue.sql": {
    evidence: "table sessions.pipeline_jobs",
    sql: relation("sessions.pipeline_jobs"),
  },
  "upstream:20261030000000_relayhistory_machines_compat.sql": {
    evidence: "ALTER-only: column sessions.machines.cursors_json",
    sql: column("sessions.machines", "cursors_json"),
  },
  "upstream:20261030010000_session_directory.sql": {
    evidence: "table sessions.session_directory",
    sql: relation("sessions.session_directory"),
  },
  "upstream:20261030020000_session_directory_short_summary.sql": {
    evidence: "column sessions.session_directory.short_summary",
    sql: column("sessions.session_directory", "short_summary"),
  },
  "upstream:20261031000000_migration_assertion_ledger.sql": {
    evidence: "table sessions.migration_assertion_uses",
    sql: relation("sessions.migration_assertion_uses"),
  },
  "upstream:20261031000100_manifest_readiness.sql": {
    evidence: "table sessions.manifest_readiness",
    sql: relation("sessions.manifest_readiness"),
  },
  "upstream:20261031001000_session_directory_reads_expand.sql": {
    evidence: "table sessions.session_directory_resurrection_fences",
    sql: relation("sessions.session_directory_resurrection_fences"),
  },
  "upstream:20261101050000_session_directory_list.sql": {
    evidence: "table sessions.session_directory_agents",
    sql: relation("sessions.session_directory_agents"),
  },
  "upstream:20261104020000_auth_sessions.sql": {
    evidence: "table sessions.auth_sessions",
    sql: relation("sessions.auth_sessions"),
  },
  "upstream:20261101000000_migration_ingest_audit_base_read.sql": {
    evidence: "table sessions.migration_base_read_uses",
    sql: relation("sessions.migration_base_read_uses"),
  },
  "upstream:20261104020100_auth_session_verify.sql": {
    evidence: "table sessions.auth_token_verifier_policy",
    sql: relation("sessions.auth_token_verifier_policy"),
  },
  "upstream:20261101000200_claimless_window_activations.sql": {
    evidence: "table sessions.claimless_window_activations",
    sql: relation("sessions.claimless_window_activations"),
  },
  "upstream:20261102000000_session_briefs.sql": {
    evidence: "table sessions.session_briefs",
    sql: relation("sessions.session_briefs"),
  },
  "upstream:20261102010000_session_directory_canonical_ids.sql": {
    evidence: "ALTER-only: column sessions.session_index.canonical_session_id",
    sql: column("sessions.session_index", "canonical_session_id"),
  },
  "upstream:20261104022000_session_ingest_ledger.sql": {
    evidence: "table sessions.ingest_batches",
    sql: relation("sessions.ingest_batches"),
  },
  "upstream:20261102020000_session_brief_state.sql": {
    evidence: "ALTER-only: column sessions.session_briefs.state (session_brief replaced to return it)",
    sql: column("sessions.session_briefs", "state"),
  },
  "upstream:20261104021000_session_directory_event_metadata.sql": {
    evidence: "table sessions.session_directory_event_times",
    sql: relation("sessions.session_directory_event_times"),
  },
  "upstream:20261104030000_ingest_fence_ownership.sql": {
    evidence: "table sessions.ingest_session_ownership",
    sql: relation("sessions.ingest_session_ownership"),
  },
  "upstream:20261104040000_ingest_s3_proof_seed.sql": {evidence:"table sessions.ingest_seed_captures",sql:relation("sessions.ingest_seed_captures")},
  "upstream:20261104050000_buffered_upload_receipts.sql": {
    evidence: "table sessions.ingest_stage_intents",
    sql: relation("sessions.ingest_stage_intents"),
  },
  "upstream:20261105033000_canonical_project_events.sql": { evidence: "table sessions.canonical_project_events", sql: relation("sessions.canonical_project_events") },
  "96bulls:0001_96bulls_observability.sql": {
    evidence: "column sessions.pipeline_jobs.traceparent",
    sql: column("sessions.pipeline_jobs", "traceparent"),
  },
  "96bulls:0002_session_ext_and_summaries.sql": {
    evidence: "table sessions.session_index_ext",
    sql: relation("sessions.session_index_ext"),
  },
  "96bulls:0003_fanout_ledger.sql": {
    evidence: "ALTER-only: column sessions.session_index_ext.fanout_revision",
    sql: column("sessions.session_index_ext", "fanout_revision"),
  },
  "96bulls:0004_summary_pointers.sql": {
    evidence:
      "DELETE + ALTER: column sessions.session_summaries.object_key (summary dropped)",
    sql: column("sessions.session_summaries", "object_key"),
  },
  "upstream:20261105044010_selfhost_erase_lock_fences.sql": {
    evidence: "replacement: ingest_selfhost_set_session_uploads takes the user's fence lock before reading ownership",
    sql: "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE oid=to_regprocedure('sessions.ingest_selfhost_set_session_uploads(jsonb)') AND prosecdef AND prosrc LIKE '%ingest_lock_fences(v_org,v_ws,v_user)%') AS applied",
  },
  "upstream:20261105043910_selfhost_session_selection_erase.sql": {
    evidence: "CREATE FUNCTION: sessions.ingest_selfhost_set_session_uploads(jsonb) (self-host per-session erase)",
    sql: "SELECT to_regprocedure('sessions.ingest_selfhost_set_session_uploads(jsonb)') IS NOT NULL AS applied",
  },
  "upstream:20261105043900_session_consumer_function_grants.sql": {
    evidence: "twelve search_consumer_runtime EXECUTEs (composed writer path)",
    sql: "SELECT count(*) = 12 AS applied FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace, aclexplode(p.proacl) a JOIN pg_roles r ON r.oid = a.grantee WHERE n.nspname = 'sessions' AND r.rolname = 'search_consumer_runtime' AND a.privilege_type = 'EXECUTE'",
  },
  // Adoptable index, but not an index-only step: this sentinel (the overload) decides, see ADOPTABLE_STEP_OTHER_OBJECTS.
  [AGENT_MATCH_STEP]: {
    evidence: "CREATE FUNCTION: sessions.list_session_directory_for(..., integer, text) (agent_match overload)",
    sql: "SELECT to_regprocedure('sessions.list_session_directory_for(text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz, text[], text[], timestamptz, text, text, integer, text)') IS NOT NULL AS applied",
  },
  "upstream:20261105044000_read_api_tokens.sql": {
    evidence: "table sessions.read_api_tokens",
    sql: relation("sessions.read_api_tokens"),
  },
  // Adoptable step: an unledgered "applied" here is accepted when every declared index is adoptable.
  [CLAIM_BOOTSTRAP_ORG_INDEXES_STEP]: {
    evidence: "the eight claim-bootstrap <table>_org_id_idx indexes, exact definition, valid and ready",
    sql: adoptableIndexesSentinel(CLAIM_BOOTSTRAP_ORG_INDEXES_STEP),
  },
};

/** The ALTER-only and replacement steps the CREATE inventory alone can't see. */
export const NON_CREATE_STEPS = [
  "upstream:20261105039000_buffered_gc_single_candidate.sql",
  "upstream:20261105043500_lazy_claim_clock_narrowing.sql",
  "upstream:20261105043800_lazy_ack_clock_no_key_update.sql",
  "upstream:20261025000300_upsert_keep_on_null.sql",
  "upstream:20261030000000_relayhistory_machines_compat.sql",
  "upstream:20261102010000_session_directory_canonical_ids.sql",
  "upstream:20261102020000_session_brief_state.sql",
  "96bulls:0003_fanout_ledger.sql",
  "96bulls:0004_summary_pointers.sql",
  "upstream:20261105043900_session_consumer_function_grants.sql",
] as const;
