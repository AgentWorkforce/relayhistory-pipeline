/**
 * Closed classification of a Postgres SQLSTATE 40001 (serialization_failure) for the `db_error_reason` log field.
 * Diagnostic only: it never changes status or retry.
 *
 * The ingest SQL raises 40001 itself with fixed MESSAGE texts (every `ERRCODE='40001'` RAISE in supabase/migrations
 * and supabase/bootstrap; db-error-reason.test.ts regenerates that list from the SQL and pins it equal to the keys
 * below, so a new 40001 RAISE without a slug fails the test). Postgres's own serialization failures add three texts.
 * The message is matched EXACTLY; only the slug leaves this module, never the message. Anything unmatched is `other`.
 * The SQL pin reads every 40001 spelling (ERRCODE='40001', ERRCODE='serialization_failure', RAISE serialization_failure,
 * RAISE SQLSTATE '40001') and requires each to be the ERRCODE='40001',MESSAGE='...' form this map is keyed by.
 */
export const DB_SERIALIZATION_REASON_BY_MESSAGE: ReadonlyMap<string, string> = new Map([
  // Postgres's own 40001 texts (heapam / predicate.c).
  ["could not serialize access due to concurrent update", "pg_concurrent_update"],
  ["could not serialize access due to concurrent delete", "pg_concurrent_delete"],
  ["could not serialize access due to read/write dependencies among transactions", "pg_rw_dependency"],
  // RAISE ... USING ERRCODE='40001',MESSAGE='...' in supabase/migrations and supabase/bootstrap.
  ["acquisition: operation_conflict", "operation_conflict"],
  ["acquisition: origin_cas_lost", "origin_cas_lost"],
  ["acquisition: predecessor_mismatch", "predecessor_mismatch"],
  ["acquisition: revision_cas_lost", "revision_cas_lost"],
  ["acquisition: session_busy", "session_busy"],
  ["acquisition: stale_revision", "stale_revision"],
  ["babysitter retention: a claimed session is fenced now", "a_claimed_session_is_fenced_now"],
  ["babysitter retention: bucket is being erased", "bucket_is_being_erased"],
  ["babysitter retention: reset epoch changed", "reset_epoch_changed"],
  ["babysitter retention: session outside claim attempt", "babysitter_session_outside_claim_attempt"],
  ["buffer gc: active pointer changed", "active_pointer_changed"],
  ["buffer gc: claim lease expired", "claim_lease_expired"],
  ["buffer gc: current object cannot finish", "current_object_cannot_finish"],
  ["buffer gc: different registered replacement", "different_registered_replacement"],
  ["buffer gc: fence changed during copy", "fence_changed_during_copy"],
  ["buffer gc: identity changed", "identity_changed"],
  ["buffer gc: lease expired", "lease_expired"],
  ["buffer gc: not a live acknowledged plan", "not_a_live_acknowledged_plan"],
  ["buffer gc: not a replacement plan", "not_a_replacement_plan"],
  ["buffer gc: registered replacement required", "registered_replacement_required"],
  ["buffer gc: replacement authority changed", "replacement_authority_changed"],
  ["buffer gc: stale lease", "stale_lease"],
  ["buffer gc: unknown lease generation", "unknown_lease_generation"],
  ["buffer gc: unknown ticket", "unknown_ticket"],
  ["frozen source: snapshot is not the neon fingerprint", "snapshot_is_not_the_neon_fingerprint"],
  ["history: artifact intent changed", "artifact_intent_changed"],
  ["history: artifact source changed", "artifact_source_changed"],
  ["history: captured carrier changed", "captured_carrier_changed"],
  ["history: carrier lost references", "carrier_lost_references"],
  ["history: carrier outside immutable plan", "carrier_outside_immutable_plan"],
  ["history: carrier page expired", "carrier_page_expired"],
  ["history: derived source changed", "derived_source_changed"],
  ["history: durable staged receipt absent", "durable_staged_receipt_absent"],
  ["history: expired inventory cannot renew", "expired_inventory_cannot_renew"],
  ["history: expired plan cannot renew", "expired_plan_cannot_renew"],
  ["history: expired stage cannot renew", "expired_stage_cannot_renew"],
  ["history: final inventory expired", "final_inventory_expired"],
  ["history: final plan deadline expired", "final_plan_deadline_expired"],
  ["history: final source transaction permit required", "final_source_transaction_permit_required"],
  ["history: global carrier references changed", "global_carrier_references_changed"],
  ["history: immutable object intent changed", "history_immutable_object_intent_changed"],
  ["history: inventory incomplete", "inventory_incomplete"],
  ["history: inventory lease expired", "inventory_lease_expired"],
  ["history: inventory lease expired after page", "inventory_lease_expired_after_page"],
  ["history: inventory lease/source changed", "inventory_lease_source_changed"],
  ["history: logical scope busy", "logical_scope_busy"],
  ["history: mask child proof", "mask_child_proof"],
  ["history: mask source changed", "mask_source_changed"],
  ["history: mask source changed after read", "mask_source_changed_after_read"],
  ["history: missing inventory page", "missing_inventory_page"],
  ["history: no current qualified native custody", "no_current_qualified_native_custody"],
  ["history: original receipt/custody changed", "original_receipt_custody_changed"],
  ["history: plan expired after capture", "plan_expired_after_capture"],
  ["history: plan reference page expired", "plan_reference_page_expired"],
  ["history: registered encrypted summary required", "registered_encrypted_summary_required"],
  ["history: replayed page changed", "replayed_page_changed"],
  ["history: scope build lease lost", "scope_build_lease_lost"],
  ["history: scope source changed since capture", "scope_source_changed_since_capture"],
  ["history: selected carrier inventory changed", "selected_carrier_inventory_changed"],
  ["history: stage absent", "history_stage_absent"],
  ["history: stage busy or changed", "history_stage_busy_or_changed"],
  ["history: stage requires original-ordinal retention rewrite", "stage_requires_original_ordinal_retention_rewrite"],
  ["history: unknown mask page", "unknown_mask_page"],
  ["history: unknown plan lease", "unknown_plan_lease"],
  ["ingest seed: baseline no longer idle", "baseline_no_longer_idle"],
  ["ingest seed: source changed", "source_changed"],
  ["ingest seed: source or fence changed", "source_or_fence_changed"],
  ["operator erase: read-committed transaction required", "operator_erase_read_committed_transaction_required"],
  ["operator erase: writer requires a fresh read-committed snapshot", "writer_requires_a_fresh_read_committed_snapshot"],
  ["operator user erase: read-committed transaction required", "operator_user_erase_read_committed_transaction_required"],
  ["publication: GC authority changed", "gc_authority_changed"],
  ["publication: GC lease changed", "gc_lease_changed"],
  ["publication: GC lease expired", "gc_lease_expired"],
  ["publication: GC object absent", "gc_object_absent"],
  ["publication: admission expired", "admission_expired"],
  ["publication: admitted record lost its revision race", "admitted_record_lost_its_revision_race"],
  ["publication: another batch is building this head", "another_batch_is_building_this_head"],
  ["publication: canonical projector busy", "canonical_projector_busy"],
  ["publication: complete image set not prepared", "complete_image_set_not_prepared"],
  ["publication: complete record maxima changed", "complete_record_maxima_changed"],
  ["publication: completed request differs", "completed_request_differs"],
  ["publication: derived intent differs", "derived_intent_differs"],
  ["publication: derived source no longer current", "derived_source_no_longer_current"],
  ["publication: final acknowledgement expired", "final_acknowledgement_expired"],
  ["publication: final writes require fresh snapshots", "final_writes_require_fresh_snapshots"],
  ["publication: generation changed", "generation_changed"],
  ["publication: generation retired", "generation_retired"],
  ["publication: head CAS lost", "head_cas_lost"],
  ["publication: head changed", "head_changed"],
  ["publication: immutable image plan changed", "immutable_image_plan_changed"],
  ["publication: immutable object intent changed", "immutable_object_intent_changed"],
  ["publication: lease expired after head locks", "lease_expired_after_head_locks"],
  ["publication: manifest intent proof missing", "manifest_intent_proof_missing"],
  ["publication: no durable buffered receipt", "no_durable_buffered_receipt"],
  ["publication: object generation unavailable", "object_generation_unavailable"],
  ["publication: object registration expired", "object_registration_expired"],
  ["publication: old heartbeat lease expired", "old_heartbeat_lease_expired"],
  ["publication: original privacy fence changed", "original_privacy_fence_changed"],
  ["publication: pointer CAS did not apply", "pointer_cas_did_not_apply"],
  ["publication: pointer mutation busy", "pointer_mutation_busy"],
  ["publication: pointer mutation requires fresh snapshots", "pointer_mutation_requires_fresh_snapshots"],
  ["publication: pointer scope busy", "pointer_scope_busy"],
  ["publication: prepare expired", "prepare_expired"],
  ["publication: source outbox CAS lost", "source_outbox_cas_lost"],
  ["publication: stage absent", "stage_absent"],
  ["publication: stage busy or changed", "stage_busy_or_changed"],
  ["publication: stage needs privacy GC", "stage_needs_privacy_gc"],
  ["publication: stale stage lease", "stale_stage_lease"],
  ["publication: unknown attempt", "unknown_attempt"],
  ["relayfacts user erase: read-committed transaction required", "relayfacts_user_erase_read_committed_transaction_required"],
  ["session ingest: ACK seat lease expired", "ack_seat_lease_expired"],
  ["session ingest: actual control receipt required for ACK", "actual_control_receipt_required_for_ack"],
  ["session ingest: attempt lacks buffered proof", "attempt_lacks_buffered_proof"],
  ["session ingest: buffered batch not pending", "buffered_batch_not_pending"],
  ["session ingest: buffered intent refused", "buffered_intent_refused"],
  ["session ingest: buffered ordinal coverage changed", "buffered_ordinal_coverage_changed"],
  ["session ingest: claim generation changed", "claim_generation_changed"],
  ["session ingest: claim generation collision", "claim_generation_collision"],
  ["session ingest: claim records still reachable", "claim_records_still_reachable"],
  ["session ingest: conflicting control replay", "conflicting_control_replay"],
  ["session ingest: control digest mismatch", "control_digest_mismatch"],
  ["session ingest: control version gap", "control_version_gap"],
  ["session ingest: current acknowledged stage required", "current_acknowledged_stage_required"],
  ["session ingest: current original stage required", "current_original_stage_required"],
  ["session ingest: current seat stage required", "current_seat_stage_required"],
  ["session ingest: durable batch receipt required for ACK", "durable_batch_receipt_required_for_ack"],
  ["session ingest: lazy records require read committed", "lazy_records_require_read_committed"],
  ["session ingest: native slot raced", "native_slot_raced"],
  ["session ingest: no buffered batch", "no_buffered_batch"],
  ["session ingest: original candidate order changed", "original_candidate_order_changed"],
  ["session ingest: original source version changed", "original_source_version_changed"],
  ["session ingest: original witnessed attempt required", "original_witnessed_attempt_required"],
  ["session ingest: pending source control mismatch", "pending_source_control_mismatch"],
  ["session ingest: reset before buffered intent", "reset_before_buffered_intent"],
  ["session ingest: seat attempt authority changed", "seat_attempt_authority_changed"],
  ["session ingest: seat original stage authority changed", "seat_original_stage_authority_changed"],
  ["session ingest: session outside claim attempt", "session_outside_claim_attempt"],
  ["session ingest: source candidate metadata mismatch", "source_candidate_metadata_mismatch"],
  ["session ingest: source control gap", "source_control_gap"],
  ["session ingest: source control replay mismatch", "source_control_replay_mismatch"],
  ["session ingest: source control version changed", "source_control_version_changed"],
  ["session ingest: source control witness shape", "source_control_witness_shape"],
  ["session ingest: source version changed before ACK", "source_version_changed_before_ack"],
  ["session ingest: source witness batch mismatch", "source_witness_batch_mismatch"],
  ["session ingest: source witness identity mismatch", "source_witness_identity_mismatch"],
  ["session ingest: source witness rank coverage", "source_witness_rank_coverage"],
  ["session ingest: stage expired while assigning seats", "stage_expired_while_assigning_seats"],
  ["session ingest: unknown claim attempt", "unknown_claim_attempt"],
]);

/** The Postgres-builtin keys above (not raised by our SQL; the pin test excludes them from the SQL comparison). */
export const PG_BUILTIN_SERIALIZATION_MESSAGES: readonly string[] = Object.freeze([
  "could not serialize access due to concurrent update",
  "could not serialize access due to concurrent delete",
  "could not serialize access due to read/write dependencies among transactions",
]);

/** Every value `db_error_reason` may take (log.ts ENUM_FIELDS uses exactly this set). */
export const DB_ERROR_REASONS: readonly string[] = Object.freeze([...new Set(DB_SERIALIZATION_REASON_BY_MESSAGE.values()), "other"]);

/** The longest message classified; anything longer (no known text is near it) is `other` without being scanned. */
export const DB_REASON_MESSAGE_MAX = 512;

/** The slug of a 40001's exact message (`other` when unknown or absent). Never throws, never returns the message. */
export function dbSerializationReason(message: unknown): string {
  return (typeof message === "string" ? DB_SERIALIZATION_REASON_BY_MESSAGE.get(message) : undefined) ?? "other";
}

/**
 * SQLSTATE XX000 (internal_error) message FAMILIES for the sibling `db_internal_reason` field: Postgres's own
 * internal texts carry ids/names after a fixed head, so each family is an anchored prefix regex and only the family
 * slug is ever emitted. First match wins (could_not_open_relation before could_not_find). Unmatched: `other`.
 */
export const DB_INTERNAL_REASON_FAMILIES: readonly (readonly [RegExp, string])[] = Object.freeze([
  [/^cache lookup failed for /, "cache_lookup_failed"],
  [/^cached plan must not change result type$/, "cached_plan_result_type"],
  [/^could not open relation /, "could_not_open_relation"],
  [/^could not read block /, "could_not_read_block"],
  [/^could not find /, "could_not_find"],
  [/^tuple concurrently (?:updated|deleted)$/, "tuple_concurrently_modified"],
  [/^unrecognized node type: /, "unrecognized_node_type"],
] as const);

/** Every value `db_internal_reason` may take (log.ts ENUM_FIELDS uses exactly this set). */
export const DB_INTERNAL_REASONS: readonly string[] = Object.freeze([...new Set(DB_INTERNAL_REASON_FAMILIES.map(([, s]) => s)), "other"]);

/** The family of an XX000's message (`other` when unknown, absent or too long). Never throws, never returns text. */
export function dbInternalReason(message: unknown): string {
  if (typeof message !== "string" || message.length > DB_REASON_MESSAGE_MAX) return "other";
  for (const [re, slug] of DB_INTERNAL_REASON_FAMILIES) if (re.test(message)) return slug;
  return "other";
}

/** The error's message if it is a readable, bounded string; else undefined. Never throws (a getter may). */
function readMessage(err: unknown): string | undefined {
  try {
    if (err === null || (typeof err !== "object" && typeof err !== "function")) return undefined;
    const m: unknown = (err as { message?: unknown }).message;
    return typeof m === "string" && m.length <= DB_REASON_MESSAGE_MAX ? m : undefined;
  } catch {
    return undefined;
  }
}

/**
 * `{ db_error_reason }` for a 40001 and `{ db_internal_reason }` for an XX000; `{}` for any other code. `errorCode`
 * is the code the caller already captured (errorFields' error_code), so the error's `code` is not read again. The
 * message is read best-effort: a throwing getter, a non-string or an over-long message is `other`. Never throws.
 */
export function dbErrorReasonField(err: unknown, errorCode: unknown): { db_error_reason?: string; db_internal_reason?: string } {
  try {
    if (errorCode === "40001") return { db_error_reason: dbSerializationReason(readMessage(err)) };
    if (errorCode === "XX000") return { db_internal_reason: dbInternalReason(readMessage(err)) };
    return {};
  } catch {
    return errorCode === "40001" ? { db_error_reason: "other" } : errorCode === "XX000" ? { db_internal_reason: "other" } : {};
  }
}
