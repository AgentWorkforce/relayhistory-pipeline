/**
 * The exact receipt schemas of the backlog-ops authority receipts (backlog-ops.ts backlogOpsAuthorityReceipt:
 * reshard-dry, authority-dry, authority-execute, and a catch-up bound to an authority-dry). The credential step
 * (scripts/backlog-ops.ts) applies authorityReceiptSummary to the tool's summary BEFORE the receipt is persisted or
 * logged: a summary that does not conform exactly (every key, every list, every hash position, every code from a
 * closed set) is replaced by {"refused": "backlog_ops_receipt_schema"}; a refusal summary keeps its code only when it
 * is in that mode's closed refusal map, else it becomes the mode's generic code. countsOnly alone is not enough: it
 * passes any code-SHAPED string and any lowercase key.
 *
 * The one normalisation: a backfill section's `failed_codes` (keyed by an error's own code, lowercased) keeps only
 * the backfill's known keys; every other key's count is summed under `other`.
 */
import { AUTHORITY_REFUSALS } from "./authority-evidence.ts";
import { ERASURE_SOURCES, ERASURE_WINDOW_REASONS, orgHash, validErasureWindowProof } from "./erasure-window.ts";
const ERASURE_SOURCE_NAMES = Object.keys(ERASURE_SOURCES);

type S =
  | "count" | "bool" | "hash" | "digest"
  | { code: ReadonlySet<string> }
  | { nullable: S }
  | { list: S; max: number }
  | { obj: Record<string, S>; optional?: readonly string[] };

const COUNT = "count" as const, BOOL = "bool" as const, HASH = "hash" as const, DIGEST = "digest" as const;
const codes = (...c: readonly string[]): S => ({ code: new Set(c) });
const nul = (s: S): S => ({ nullable: s });
const list = (s: S, max = 64): S => ({ list: s, max });
const obj = (o: Record<string, S>, optional: readonly string[] = []): S => ({ obj: o, optional });
const counts = (...k: string[]): Record<string, S> => Object.fromEntries(k.map((x) => [x, COUNT]));

const REFUSAL = codes(...AUTHORITY_REFUSALS);
const CHECK = { ok: BOOL, code: nul(REFUSAL) };
const FINGERPRINT = obj({ revisions_count: COUNT, revisions_digest: DIGEST, ownership_count: COUNT, ownership_digest: DIGEST });

/** authority-evidence.ts AuthorityEvidenceReport, in receipt keys. */
export const EVIDENCE_SCHEMA: S = obj({
  status: codes("authority_evidence_corroborated", "authority_evidence_unbound", "authority_evidence_refused"),
  org_count: COUNT, observation_window_ms: COUNT, started_at_ms: COUNT, finished_at_ms: COUNT,
  ingest_deployment_hash: nul(HASH), relayhistory_deployment_hash: nul(HASH),
  orgs: list(obj({
    org_hash: HASH, ok: BOOL, refusals: list(REFUSAL, AUTHORITY_REFUSALS.length),
    stop: obj({ ...CHECK, ...counts("serving_versions", "deployments_walked", "stop_deployments"), effective_stop_ms: nul(COUNT), stop_deployment_hash: nul(HASH) }),
    mirror: obj(CHECK),
    queue_drained: obj({ ...CHECK, backlog_reads: list(COUNT, 2), dlq_backlog_reads: list(COUNT, 2), dlq_consumers: nul(COUNT), observed_window_ms: nul(COUNT) }),
    sql_quiet: obj({ ...CHECK, reads: COUNT,
      counts: nul(obj(counts("batches_claimed_since", "batches_committed_since", "batches_pending", "revisions_updated_since", "revisions_count", "ownership_count"))),
      fingerprint: nul(FINGERPRINT), revisions_stable: nul(BOOL), ownership_stable: nul(BOOL), bound: BOOL }),
    turns_neon: obj({ ...CHECK, serving_versions: COUNT, path: nul(codes("neon", "dual", "forward", "unknown")) }),
  })),
});

/** authority-evidence.ts AuthorityBaseline, in receipt keys. */
export const BASELINE_SCHEMA: S = obj({
  kind: codes("authority_baseline"), v: codes("1"), status: codes("authority_baseline_ready", "authority_baseline_refused"), org_count: COUNT,
  orgs: list(obj({
    org_hash: HASH, ok: BOOL, refusals: list(REFUSAL, AUTHORITY_REFUSALS.length), stop_deployment_hash: nul(HASH), effective_stop_ms: nul(COUNT),
    read_at_ms: nul(COUNT), snapshot: nul(obj({ xmin: COUNT, xmax: COUNT, xip: list(COUNT, 4096) })), fingerprint: nul(FINGERPRINT),
    stats: nul(obj(counts("batches_deleted", "stats_reset_ms", "started_ms"))),
  })),
});

/**
 * The backfill's known `failed_codes` keys (anything else is summed under `other`): its own codes and the object
 * store's error codes (failureCode lowercases an error's own code).
 */
export const BACKFILL_FAILED_CODES = ["fence_unreadable", "slot_read_too_long", "ledger_revision_contended", "error", "other", "not_found", "already_exists",
  "precondition_failed", "content_mismatch", "invalid_key", "invalid_argument", "unsupported", "unauthorized", "transient", "backend"] as const;
const FAILED_CODES = "failed_codes";
const BACKFILL_FIELDS: Record<string, S> = {
  mode: codes("dry_run", "execute"), users: COUNT,
  fences: obj(counts("deletion_rows", "reset_rows", "created", "updated", "unchanged", "would_create", "would_update", "pairs_added", "resets_added",
    "deletions_superseded", "deletions_unproven", "cold_resets", "needs_review", "unreadable", "contended", "tenant_invalid", "input_invalid", "failed", "resets_in_flight")),
  ownership: obj({ ...counts("rows", "registered", "would_register", "unchanged", "skipped_deleted", "skipped_no_fence", "invalid", "failed"),
    [FAILED_CODES]: obj(counts(...BACKFILL_FAILED_CODES), BACKFILL_FAILED_CODES) }),
  revisions: obj({ ...counts("rows", "seed_rows", "slot_rows", "slots_empty", "merged", "would_merge", "skipped_deleted", "skipped_no_fence", "invalid", "shards", "failed"),
    [FAILED_CODES]: obj(counts(...BACKFILL_FAILED_CODES), BACKFILL_FAILED_CODES) }),
  sections: obj({ fences: codes("complete", "incomplete", "not_run"), ownership: codes("complete", "incomplete", "not_run"), revisions: codes("complete", "incomplete", "not_run") }),
  fingerprint: obj(counts("revisions_count", "revisions_max_updated_ms", "revisions_hash", "ownership_count", "ownership_hash", "slots_owned_count",
    "slots_revision_sum", "slots_hash")),
  revisions_since_ms: COUNT,
};
/** upload-fence-backfill.ts BackfillSummary of every backfill mode (revisions_since_ms only on a catch-up), receipt keys. */
export const BACKFILL_SCHEMA: S = obj(BACKFILL_FIELDS, ["revisions_since_ms"]);
/** A catch-up bound to an authority-dry: the backfill summary with its t and the baseline. */
export const CATCHUP_SCHEMA: S = obj({ ...BACKFILL_FIELDS, authority_baseline: BASELINE_SCHEMA });

export const RESHARD_REFUSAL_CODES = ["reshard_prefix_refused", "reshard_concurrency_refused", "reshard_mode_refused", "reshard_failed"] as const;
const RESHARD_COUNTS = counts("listed", "legacy_shards", "entries", "merged", "would_merge", "shards", "invalid", "failed", "verified", "missing");
export const RESHARD_SCHEMA: S = obj({
  mode: codes("dry_run"), tool: codes("reshard_verify"), org_count: COUNT, verify_passed: BOOL,
  per_org: list(obj({ ...RESHARD_COUNTS, org_hash: HASH, verify_passed: BOOL, refused: nul(codes(...RESHARD_REFUSAL_CODES)) })),
});

export const AUTHORITY_DRY_SCHEMA: S = obj({ mode: codes("dry_run"), evidence: EVIDENCE_SCHEMA });

/** erasure-window.ts ErasureWindowProof (already receipt keys). */
const PROOF_SCHEMA: S = obj({
  v: codes("1"), kind: codes("erasure_window_proof"), status: codes("erasure_window_empty", "erasure_window_nonempty", "erasure_window_unproven"),
  reason: codes(...ERASURE_WINDOW_REASONS),
  org_hash: HASH, epoch_hash: nul(HASH), cutoff_ms: COUNT, read_at_ms: nul(COUNT), watermark_erasures: nul(COUNT),
  hold_state: nul(codes("held", "sealed", "marker_attempted")), sources: nul(obj(counts(...ERASURE_SOURCE_NAMES))),
  watermark_kind: codes("surviving_two_tables", "erasure_history_all_sources"),
});
export const BRIDGE_SCHEMA: S = obj({ mode: codes("dry_run"), tool: codes("bridge_window"), org_count: COUNT, all_empty: BOOL, per_org: list(PROOF_SCHEMA) });

/** backlog-authority.ts authorityExecute's own codes. */
export const EXECUTE_CODES = ["evidence_not_corroborated", "writer_fence_unavailable", "writer_fence_not_held", "marker_handoff_lapsed", "marker_conflict",
  "marker_org", "marker_access_denied", "marker_unreadable", "marker_write_failed", "marker_readback_mismatch", "sweeper_spec_missing", "sweeper_spec_stale",
  "sweeper_spec_future", "sweeper_spec_invalid", "sweeper_spec_unreadable", "sweeper_spec_access_denied", "sweeper_suspended", "sweeper_home_mismatch",
  "sweeper_out_of_scope", "sweeper_invalid_request", "sweeper_missing", "sweeper_malformed", "sweeper_mismatch", "sweeper_stale", "sweeper_future",
  "sweeper_access_denied", "sweeper_unreadable"] as const;
export const EXECUTE_SCHEMA: S = obj({
  mode: codes("execute"), status: codes("authority_written", "authority_refused", "authority_incomplete"), refused_code: nul(codes(...EXECUTE_CODES)),
  org_count: COUNT, evidence: nul(EVIDENCE_SCHEMA), markers: obj(counts("written", "conflict", "not_written")),
  per_org: list(obj({ org_hash: HASH, sweeper: codes("ok", "not_run", ...EXECUTE_CODES), marker: codes("written", "conflict", "not_written") })),
});

/**
 * The closed refusal map per kind of authority receipt: a `{"refused": code}` summary keeps `code` only when listed,
 * else the generic code. The tool's own refusals (scripts/backlog-authority.ts AUTHORITY_TOOL_REFUSALS) and the
 * backfill's ledger-home and baseline refusals.
 */
const LEDGER_HOME = ["ledger_bucket_invalid", "ledger_home_guard_invalid", "ledger_home_mismatch", "ledger_home_missing", "ledger_home_moved",
  "ledger_home_invalid", "ledger_home_unavailable", "ledger_home_store_error", "access_denied"];
export const AUTHORITY_TOOL_REFUSALS = ["usage", "orgs", "authority_execute_held", "stdin_too_large", "stdin_invalid", "db_url_required",
  "cloudflare_token_invalid", "cloudflare_config", "cloudflare_path", "aws_credentials_required", "backlog_ops_reshard_org", "internal_error",
  "bridge_release_held", "erasure_window_input", ...LEDGER_HOME] as const;
/**
 * Every code scripts/upload-fence-backfill.ts redactedReason can produce from the backfill's own throws (the closed
 * set, enumerated from its source, the library's and the tool store's), plus a digits-only SQLSTATE and the owner URL
 * codes. A code-shaped message from anywhere else (a pg, S3 or fetch error) is not in it.
 */
const BACKFILL_REFUSALS = ["backfill_authority_baseline_invalid", "backfill_authority_baseline_missing", "backfill_concurrency_invalid",
  "backfill_dry_run_write_refused", "backfill_fingerprint_invalid", "backfill_key_root_invalid", "backfill_merge_buffer_invalid", "backfill_not_read_only",
  "backfill_org_invalid", "backfill_page_size_invalid", "backfill_revisions_since_invalid", "backfill_revisions_since_sections", "backfill_row_invalid",
  "backfill_row_page_size_invalid", "backfill_sections_invalid", "backfill_slot_blocks_invalid", "backfill_slot_org_max_invalid", "backfill_slot_pace_invalid",
  "backfill_slot_window_invalid", "backfill_statement_timeout_invalid", "ledger_store_cursor_invalid", "ledger_store_same", "slot_read_too_long",
  "stdin_too_large", "stdin_invalid", "db_url_required", "aws_credentials_required", "db_connection_lost", "internal_error", ...LEDGER_HOME,
  ...["application_name", "database", "fragment", "host", "password", "port", "protocol", "query_params", "url", "user"].map((c) => `db_url_${c}`)];
const SQLSTATE = /^sqlstate_[0-9]{5}$/u;

/** `backfill`: every other backfill mode (a FULL backfill-execute feeds authority-execute; all share its receipt path). */
export type AuthorityReceiptKind = "reshard" | "authority_dry" | "authority_execute" | "catchup" | "backfill" | "bridge";
const SUCCESS: Record<AuthorityReceiptKind, S> = { reshard: RESHARD_SCHEMA, authority_dry: AUTHORITY_DRY_SCHEMA, authority_execute: EXECUTE_SCHEMA, catchup: CATCHUP_SCHEMA,
  backfill: BACKFILL_SCHEMA, bridge: BRIDGE_SCHEMA };
const REFUSED: Record<AuthorityReceiptKind, { allowed: ReadonlySet<string>; generic: string }> = {
  reshard: { allowed: new Set(AUTHORITY_TOOL_REFUSALS), generic: "internal_error" },
  authority_dry: { allowed: new Set(AUTHORITY_TOOL_REFUSALS), generic: "internal_error" },
  authority_execute: { allowed: new Set(AUTHORITY_TOOL_REFUSALS), generic: "internal_error" },
  catchup: { allowed: new Set(BACKFILL_REFUSALS), generic: "backfill_refused" },
  backfill: { allowed: new Set(BACKFILL_REFUSALS), generic: "backfill_refused" },
  bridge: { allowed: new Set(AUTHORITY_TOOL_REFUSALS), generic: "internal_error" },
};

const HASH_RE = /^h_[0-9a-f]{32}$/u, DIGEST_RE = /^h[0-9a-f]{32}$/u;
const isObj = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);

/** True when `v` conforms to `s` exactly. */
export function conforms(v: unknown, s: S, depth = 0): boolean {
  if (depth > 10) return false;
  if (s === COUNT) return typeof v === "number" && Number.isSafeInteger(v) && v >= 0;
  if (s === BOOL) return typeof v === "boolean";
  if (s === HASH) return typeof v === "string" && HASH_RE.test(v);
  if (s === DIGEST) return typeof v === "string" && DIGEST_RE.test(v);
  if ("code" in s) return (typeof v === "string" && s.code.has(v)) || (typeof v === "number" && s.code.has(String(v)));
  if ("nullable" in s) return v === null || conforms(v, s.nullable, depth + 1);
  if ("list" in s) return Array.isArray(v) && v.length <= s.max && v.every((x) => conforms(x, s.list, depth + 1));
  if (!isObj(v)) return false;
  const keys = Object.keys(s.obj), optional = s.optional ?? [];
  return Object.keys(v).every((k) => Object.hasOwn(s.obj, k)) && keys.every((k) => Object.hasOwn(v, k) || optional.includes(k))
    && Object.entries(v).every(([k, x]) => conforms(x, s.obj[k]!, depth + 1));
}

/**
 * A bridge receipt's semantics beyond its shape: its proofs are for exactly the requested orgs (`ctx.orgs`, the run's
 * scope), each once; every per-org proof valid in itself (validErasureWindowProof: an all-source empty counted 0 history
 * rows, empty iff reason ok, an empty one was read); org_count equal to their number; and all_empty exactly "every
 * proof empty". A contradictory receipt is never persisted or logged.
 */
function bridgeConsistent(s: unknown, orgs: readonly string[] | null | undefined): boolean {
  const r = s as { org_count: number; all_empty: boolean; per_org: unknown[] };
  // The proofs are for exactly the requested orgs, each once: a duplicate proof never stands in for another org.
  if (!orgs || orgs.length === 0) return false;
  const want = orgs.map(orgHash).sort(), got = r.per_org.map((p) => (p as { org_hash?: unknown }).org_hash).sort();
  if (new Set(want).size !== want.length || got.length !== want.length || !got.every((h, i) => h === want[i])) return false;
  return r.per_org.every((p) => validErasureWindowProof(p)) && r.org_count === r.per_org.length
    && r.all_empty === (r.per_org.length > 0 && r.per_org.every((p) => (p as { status: string }).status === "erasure_window_empty"));
}

/** A backfill section's failed_codes with only the known keys (others summed under `other`). */
function closedFailedCodes(v: unknown): unknown {
  if (!isObj(v)) return v;
  const out: Record<string, number> = {};
  for (const [k, n] of Object.entries(v)) {
    const key = (BACKFILL_FAILED_CODES as readonly string[]).includes(k) ? k : "other";
    // Every raw count a non-negative safe integer BEFORE folding: a negative or fractional count must not cancel or
    // round into a valid-looking total. Otherwise the input is returned as is, and the schema refuses it.
    if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 0) return v;
    const total = (out[key] ?? 0) + n;
    if (!Number.isSafeInteger(total)) return v;
    out[key] = total;
  }
  return out;
}

/**
 * The summary an authority receipt may carry: the tool's summary when it conforms exactly to the kind's success
 * schema; a refusal with its code mapped through the kind's closed map; otherwise {"refused": "backlog_ops_receipt_schema"}.
 * The second value says whether the summary was the tool's own success (false: the run must not exit 0).
 */
export function authorityReceiptSummary(kind: AuthorityReceiptKind, summary: unknown, ctx: { orgs?: readonly string[] | null } = {}): { summary: unknown; ok: boolean } {
  if (isObj(summary) && Object.keys(summary).length === 1 && Object.hasOwn(summary, "refused")) {
    const r = REFUSED[kind];
    const c = summary.refused;
    const keep = typeof c === "string" && (r.allowed.has(c) || ((kind === "catchup" || kind === "backfill") && SQLSTATE.test(c)));
    return { summary: { refused: keep ? c : r.generic }, ok: false };
  }
  let s = summary;
  if ((kind === "catchup" || kind === "backfill") && isObj(s)) {
    const sec = (k: string) => (isObj(s) && isObj((s as Record<string, unknown>)[k]) ? { ...(s as Record<string, any>)[k], [FAILED_CODES]: closedFailedCodes((s as Record<string, any>)[k][FAILED_CODES]) } : (s as Record<string, unknown>)[k]);
    s = { ...s, ownership: sec("ownership"), revisions: sec("revisions") };
  }
  if (!conforms(s, SUCCESS[kind]) || (kind === "bridge" && !bridgeConsistent(s, ctx.orgs))) return { summary: { refused: "backlog_ops_receipt_schema" }, ok: false };
  return { summary: s, ok: true };
}
