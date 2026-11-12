/** Actual target-side queries used by the frozen custody copy and sealer.
 * Keyset predicates and ORDER BY must use the same column collations. Inheriting
 * both from the columns also matches the existing primary-key index. The
 * manifest digest is order-independent; source/target scan orders may differ.
 */
export const SUPA_PAGE = `SELECT org_id,workspace_id,origin_id,record_id,revision_id,revision,digest,kind,source,session_id,operation,user_id,projection_accepted_seq,catalog_accepted_seq
 FROM sessions.ingest_frozen_source_records WHERE org_id=$1 AND (workspace_id,origin_id,record_id) > ($2,$3,$4)
 ORDER BY workspace_id,origin_id,record_id LIMIT $5`;

export const SUPA_RECEIPTS = `SELECT org_id,workspace_id,origin_id,batch_id,digest,receipt::text AS receipt FROM sessions.ingest_frozen_source_receipts
 WHERE org_id=$1 AND (workspace_id,origin_id,batch_id) > ($2,$3,$4) ORDER BY workspace_id,origin_id,batch_id LIMIT $5`;

export const SUPA_CONTROL_HEADS = "SELECT org_id,workspace_id,user_id,version::text v,digest,pending FROM sessions.ingest_source_control_heads ORDER BY org_id,workspace_id,user_id";

/** Target-side frozen privacy state the sealer digests (same column order as the Neon source queries). */
export const SUPA_DELETIONS = "SELECT org_id,workspace_id,user_id,origin_id,source,session_id FROM sessions.ingest_frozen_source_deletions";
export const SUPA_RESETS = "SELECT org_id,workspace_id,user_id,request_id,deleted::text AS deleted FROM sessions.ingest_frozen_source_resets";
