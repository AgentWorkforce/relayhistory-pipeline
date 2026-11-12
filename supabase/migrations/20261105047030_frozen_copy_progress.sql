-- Frozen-source copy progress (slot 047030, assigned by the registry keeper) and the receipts privacy guard.
--
-- 1. sessions.__96bulls_frozen_copy_progress is the ONLY durable cursor of the P1 frozen-source copy operator
--    (src/migration/frozen-source-copy-progress.ts, scripts/frozen-source-copy.ts; PR #559). One row per
--    (org_id, dataset): the run binding (generation, source identity, control epoch, privacy baseline, operator sha256
--    and binding_sha256 over all five), the raw keyset cursor, page_seq, rows_copied and status. Each page and its
--    checkpoint commit in one transaction. The operator refuses frozen_copy_progress_table_missing until this table
--    exists, and frozen_copy_progress_table_unsafe unless it is owned by the operator's session, RLS is on, nothing
--    is granted to any other role and the columns are exactly these.
--    Who writes it: only the postgres owner, through that operator. No runtime role has any privilege on it, and
--    there is no function over it. RLS is enabled with NO policies, so even a later grant would expose no row.
--    The DDL below is PROGRESS_TABLE_DDL from that module, verbatim.
-- 2. sessions.ingest_frozen_source_receipts (20261105010000) gets the same ingest_privacy_authority_guard statement
--    trigger that 20261105041000 put on the other fenced frozen tables: once the privacy authority is 'active', only
--    a writer that set bulls96.privacy_authority_writer='authority' may write it. Reads (the definer
--    sessions.ingest_frozen_source_receipt) are unaffected. Dormant while no authority row is 'active'.
--
-- Table and trigger only: no function, no grant. Rollback: supabase/rollbacks/20261105047030_frozen_copy_progress.down.sql.
CREATE TABLE sessions.__96bulls_frozen_copy_progress(
 org_id text NOT NULL,
 dataset text NOT NULL CHECK(dataset IN('records','receipts')),
 binding_sha256 text NOT NULL CHECK(binding_sha256~'^[0-9a-f]{64}$'),
 generation text NOT NULL CHECK(generation~'^[0-9a-f]{64}$'),
 source_identity_sha256 text NOT NULL CHECK(source_identity_sha256~'^[0-9a-f]{64}$'),
 control_epoch text NOT NULL CHECK(control_epoch~'^[0-9a-f]{64}$'),
 privacy_baseline_sha256 text NOT NULL CHECK(privacy_baseline_sha256~'^[0-9a-f]{64}$'),
 operator_sha256 text NOT NULL CHECK(operator_sha256~'^[0-9a-f]{64}$'),
 cursor jsonb NOT NULL CHECK(jsonb_typeof(cursor)='array'),
 page_seq bigint NOT NULL CHECK(page_seq>=1),
 rows_copied bigint NOT NULL CHECK(rows_copied>=0),
 status text NOT NULL CHECK(status IN('in_progress','complete')),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,dataset)
);
ALTER TABLE sessions.__96bulls_frozen_copy_progress ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.__96bulls_frozen_copy_progress
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE TRIGGER ingest_privacy_authority_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON sessions.ingest_frozen_source_receipts
 FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_privacy_authority_guard();
