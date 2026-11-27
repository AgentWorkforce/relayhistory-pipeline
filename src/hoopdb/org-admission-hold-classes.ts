/**
 * Slot 062320 (per-org admission hold): the classification of every function a runtime role (bulls96_session_ingest,
 * bulls96_session_ingest_seeder) may EXECUTE, against the hold (docs/design/org-admission-hold.md section 2).
 *
 *   admission    writes NEW admission content for an org (batches, stage intents, outbox, native slots, revisions,
 *                ownership, claim attempts); refused (RH042) while the org has any non-released hold. Checked.
 *   publication  writes revisions of previously accepted content, or starts their canonical publication; refused once
 *                the org's hold is sealed. Checked.
 *   control      selection, reset, turn registration: removal state whose S3 mirror is Worker code; while sealed or
 *                later, only the mirror caller (member of bulls96_session_ingest_mirror) passes. Checked.
 *   erasure      never refused; the entry takes the org key SHARED so a release serializes with it. Checked.
 *   unaffected   writes no admission content (catch-up, history, GC, consent, custody, privacy, slots, masks, read-only
 *                and erasure-runtime steps after their begin). Not checked; each entry says why.
 *
 * The test (org-admission-hold-classes.test.ts) fails when a runtime-granted function in sessions-function-acl.json
 * is missing here, when an entry names a function that is not runtime-granted, or when a checked class's functions do
 * not carry the marked check in the 062320 migration. Built from the classification of the latest bodies at fde1fe24.
 */
export type HoldClass = "admission" | "publication" | "control" | "erasure" | "unaffected";

export interface HoldClassification {
  readonly class: HoldClass;
  /** The checked function whose body carries the check, when the entry delegates (origin wrappers, the sql wrapper). */
  readonly via?: string;
  readonly why: string;
}

const A = (why: string, via?: string): HoldClassification => ({ class: "admission", why, ...(via ? { via } : {}) });
const P = (why: string): HoldClassification => ({ class: "publication", why });
const C = (why: string): HoldClassification => ({ class: "control", why });
const E = (why: string): HoldClassification => ({ class: "erasure", why });
const U = (why: string): HoldClassification => ({ class: "unaffected", why });

export const ORG_ADMISSION_HOLD_CLASSES: Readonly<Record<string, HoldClassification>> = Object.freeze({
  "sessions.canonical_project_events_claim_v1(jsonb)": U("event-relay lease only"),
  "sessions.canonical_project_events_release_v1(jsonb)": U("event-relay lease only"),
  "sessions.canonical_project_events_sent_v1(jsonb)": U("event-relay lease only"),
  "sessions.ingest_acquire_slot(jsonb)": U("tenant concurrency slot (ingest_tenant_slots) only"),
  "sessions.ingest_babysitter_retention_register(jsonb)": U("babysitter retention bookkeeping, no admission table"),
  "sessions.ingest_babysitter_retention_written(jsonb)": U("babysitter retention bookkeeping, no admission table"),
  "sessions.ingest_buffer_gc_claim(jsonb)": U("GC lease on accepted stages (redaction path must keep working)"),
  "sessions.ingest_buffer_gc_finish(jsonb)": U("GC redaction of accepted bytes"),
  "sessions.ingest_buffer_gc_prepare(jsonb)": U("GC rewrite of accepted bytes (redaction); stage/outbox only"),
  "sessions.ingest_buffer_gc_swap(jsonb)": U("GC repoint of an accepted outbox"),
  "sessions.ingest_claim_batch(jsonb)": A("canonical claim", "sessions.ingest_claim_batch_mode"),
  "sessions.ingest_claim_buffered_batch(jsonb)": A("buffered claim (lazy begin, records, claim)"),
  "sessions.ingest_commit_batch(jsonb)": A("canonical commit; accepts committed rows and reaches revision writes"),
  "sessions.ingest_commit_buffered_batch(jsonb)": A("buffered commit; fires the seat-ACK slot writes"),
  "sessions.ingest_commit_revisions(jsonb)": A("canonical revision upsert; NULL batch id, never requires pending"),
  "sessions.ingest_fanout_fence(jsonb)": A("read-only, but authorizes the canonical fan-out between claim and commit"),
  "sessions.ingest_frozen_source_receipt(jsonb)": U("STABLE read-only receipt"),
  "sessions.ingest_frozen_source_witness(jsonb)": U("STABLE read-only witness"),
  "sessions.ingest_history_inventory_claim(jsonb)": U("history carriers and scan refs only"),
  "sessions.ingest_history_inventory_finish(jsonb)": U("history carriers only"),
  "sessions.ingest_history_inventory_heartbeat(jsonb)": U("history lease only"),
  "sessions.ingest_history_inventory_page(jsonb)": U("native locations and scan pages only"),
  "sessions.ingest_history_inventory_refs(jsonb)": U("read-only"),
  "sessions.ingest_history_mask_get(jsonb)": U("read-only mask"),
  "sessions.ingest_history_mask_register(jsonb)": U("mask pages only"),
  "sessions.ingest_history_plan_carriers_page(jsonb)": U("read-only"),
  "sessions.ingest_history_plan_claim(jsonb)": P("moves a batch to canonical publication and leases its stage"),
  "sessions.ingest_history_plan_heartbeat(jsonb)": U("stage lease only"),
  "sessions.ingest_history_plan_refs_page(jsonb)": U("read-only"),
  "sessions.ingest_history_scope_begin(jsonb)": U("scope builds only"),
  "sessions.ingest_history_scope_build_keys(jsonb)": U("scope keys, build records, scope owners only"),
  "sessions.ingest_history_scope_capture(jsonb)": U("replay publications and scope builds only"),
  "sessions.ingest_history_scope_install(jsonb)": U("scope versions and derived objects only"),
  "sessions.ingest_operator_erase_receipt(text,text,text,text)": U("STABLE read-only erase receipt"),
  "sessions.ingest_operator_runtime_authorize(text,text,text,text,uuid)": U("erasure run lease, after its checked begin"),
  "sessions.ingest_operator_runtime_begin(text,text,text,text,text,text,text,text,text,text)": E("session erase begin: records the erasure"),
  "sessions.ingest_operator_runtime_next(uuid)": U("erasure queue, after its checked begin"),
  "sessions.ingest_operator_runtime_purge(text,text,text,text,uuid)": U("erasure deletes, after its checked begin"),
  "sessions.ingest_operator_runtime_release(text,text,text,text,uuid)": U("erasure run lease, after its checked begin"),
  "sessions.ingest_operator_runtime_step(text,text,text,text,uuid,text,text,bigint,text)": U("erasure step, after its checked begin"),
  "sessions.ingest_operator_runtime_write_begin(jsonb)": U("writer obligation; an upload of a held org is refused at its claim"),
  "sessions.ingest_operator_runtime_write_failed(text,text,uuid)": U("closes a writer obligation; must never be refused"),
  "sessions.ingest_operator_runtime_write_finish(text,text,uuid)": U("closes a writer obligation; must never be refused"),
  "sessions.ingest_origin_claim_buffered_batch(jsonb)": A("origin claim wrapper", "sessions.ingest_claim_buffered_batch"),
  "sessions.ingest_origin_commit_buffered_batch(jsonb)": A("origin commit wrapper", "sessions.ingest_commit_buffered_batch"),
  "sessions.ingest_origin_prepare_buffer_stage(jsonb)": A("origin stage wrapper", "sessions.ingest_prepare_buffer_stage"),
  "sessions.ingest_prepare_buffer_stage(jsonb)": A("stage intent of a claim"),
  "sessions.ingest_privacy_authority_begin(jsonb)": U("privacy authority heads and frozen-source state only"),
  "sessions.ingest_privacy_authority_complete(jsonb)": U("privacy authority; a GC-priority bump only on stages"),
  "sessions.ingest_publication_abandon(jsonb)": U("stage lease only"),
  "sessions.ingest_publication_candidates(jsonb)": U("read-only"),
  "sessions.ingest_publication_claim(jsonb)": P("moves a batch to canonical publication and leases its stage"),
  "sessions.ingest_publication_defer(jsonb)": U("deferrals only"),
  "sessions.ingest_publication_gc_claim(jsonb)": U("publication GC only"),
  "sessions.ingest_publication_gc_finish(jsonb)": U("publication GC only"),
  "sessions.ingest_publication_heartbeat(jsonb)": U("stage lease only"),
  "sessions.ingest_publication_prepare(jsonb)": U("publication images and attempts only"),
  "sessions.ingest_publication_publish(jsonb)": P("first revision rows of accepted buffered content"),
  "sessions.ingest_publication_register_objects(jsonb)": U("publication objects only"),
  "sessions.ingest_release_slot(jsonb)": U("tenant concurrency slot only"),
  "sessions.ingest_reset_uploaded_sessions(jsonb)": C("reset (S3 mirror in the Worker)"),
  "sessions.ingest_seed_fence_baseline(jsonb)": U("seeds removal state (fences, resets, deletions) only"),
  "sessions.ingest_seed_proven_records(jsonb)": A("seeder import of revisions and ownership"),
  "sessions.ingest_seed_scope_capture(jsonb)": U("seed lease only; its import is checked"),
  "sessions.ingest_seed_scope_page(jsonb)": U("STABLE read-only"),
  "sessions.ingest_selfhost_set_session_uploads(jsonb)": C("self-host selection (disable only)"),
  "sessions.ingest_set_session_uploads(jsonb)": C("selection (S3 mirror in the Worker)"),
  "sessions.ingest_source_control_begin(jsonb)": U("source control heads only"),
  "sessions.ingest_source_control_complete(jsonb)": U("source control heads only"),
  "sessions.ingest_turn_capability(jsonb)": U("STABLE constant"),
  "sessions.ingest_turn_register(jsonb)": C("turn registration materializes SQL-only fence deletions"),
  "sessions.relayfacts_evidence_erase_session_v1(text,text,text,text,uuid)": U("relayfacts tables only"),
  "sessions.session_history_capture(jsonb)": U("read-only"),
  "sessions.session_history_large_job(jsonb)": U("large read jobs only"),
  "sessions.session_history_mask_page(jsonb)": U("read-only"),
  "sessions.session_history_register_artifact(jsonb)": U("derived objects only"),
  "sessions.session_history_register_object(jsonb)": U("derived objects only"),
  "sessions.session_history_replay_descriptor(jsonb)": U("read-only"),
  "sessions.session_history_replay_publish(jsonb)": U("derived replay pages only"),
  "sessions.session_history_source_lock(jsonb)": U("a set_config permit only"),
  "sessions.session_history_state_publish(jsonb)": U("history states and the directory state only"),
  "sessions.session_history_summary_get(jsonb)": U("read-only"),
  "sessions.session_history_summary_publish(jsonb)": U("history summaries only"),
  "sessions.upload_consent_apply_v1(text,text,text,jsonb,boolean,bigint)": U("consent tables only"),
  "sessions.upload_consent_snapshot_v1(text,text,text,text,jsonb,bigint)": U("consent first-seen rows only"),
});

/** Owner-only functions the migration also checks (erasure class), outside the runtime-granted set. */
export const ORG_ADMISSION_HOLD_OWNER_CHECKED: Readonly<Record<string, HoldClass>> = Object.freeze({
  "sessions.operator_user_erase_begin": "erasure",
  "sessions.ingest_claim_batch_mode": "admission",
});
