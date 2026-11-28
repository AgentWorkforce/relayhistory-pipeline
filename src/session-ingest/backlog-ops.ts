/**
 * Contract for the guarded backlog-ops workflow (.github/workflows/backlog-ops.yml): the one way the 0005 backlog
 * tools run against production. They are the fence backfill (scripts/upload-fence-backfill.ts), the accepted-stage
 * export (scripts/accepted-stage-export.ts) and the S3 drain (scripts/buffered-drain.ts); see
 * docs/decisions/0005-s3-native-upload-ledger.md "The buffered backlog" and docs/backlog-ops-workflow.md.
 *
 * Six modes, a dry and an execute of each tool. Every execute is dormant (BACKLOG_OPS_EXECUTE_ENABLED, flipped only
 * by a reviewed commit) and bound by a typed confirmation to earlier successful runs of this workflow at the same
 * commit:
 *   backfill-execute  DRY <backfill-dry run>
 *   export-execute    DRY <export-dry run> BACKFILL <backfill-execute run>
 *   drain-execute     DRY <drain-dry run>  BACKFILL <backfill-execute run> EXPORT <export-execute run>
 * The drain's BACKFILL must be fresh (BACKLOG_OPS_FRESH_BACKFILL_MS) and its EXPORT must come after it, because a
 * control made after a backfill is not in the fence (the drain's own --execute precondition).
 *
 * Refresh: a full backfill-execute takes longer than the drain's freshness window (its revisions section reads and
 * CAS-writes millions of shards), so the drain may instead bind a fresh REFRESH, its own two modes, which run the
 * backfill's fences and ownership sections only, beside the FULL backfill-execute it refreshes:
 *   backfill-refresh-dry      (binds nothing)
 *   backfill-refresh-execute  DRY <backfill-refresh-dry run>
 *   drain-execute             DRY <drain-dry run> BACKFILL <backfill-refresh-execute run> FULL <backfill-execute run> EXPORT <export-execute run>
 * A refresh's mode is in its run-name (display title) and its receipt's `mode`, so it can never satisfy a full-run
 * binding (export-execute's BACKFILL, the drain's FULL, or the drain's BACKFILL without FULL), nor a full run a
 * refresh binding. Its receipt must show fences and ownership complete and revisions not_run; every backfill-execute
 * receipt must show all three complete.
 * Why that is enough: after the scope's Worker HTTP admission stop (decision 0005's cutover order), no new upload
 * reaches SQL for those orgs, so nothing should write a revision, an ownership row or an owned lazy slot for them
 * (admission, and the SQL publish and seed paths that follow from what it admitted, write them); the full run's
 * revision shards and owner stubs then stay valid for its 24 h binding window. If any writer still moves one of those
 * rows, the fingerprint changes and the drain refuses: the argument is checked, not assumed. Only the controls can still move
 * (deletions and resets), reaching the drain through the fences, which the refresh re-runs with ownership; but they also
 * DELETE revision and ownership rows (ingest_apply_selection / ingest_apply_reset), so a control between FULL and the
 * refresh changes the fingerprint and the drain refuses stale: a new full run is then needed (fail-safe). The input fingerprint (upload-fence-backfill.ts
 * BackfillFingerprint: counts, the revisions' latest updated_at, the revision sum and content hashes of the ownership
 * and owned-slot rows, read at each run's start in one snapshot; with slots_read=org the owned-slot values are summed from
 * each org's slot cursor in that read's own snapshot instead) proves that assumption: FULL's and the refresh's must
 * be equal, field by field, or the drain refuses with backlog_ops_backfill_stale before any credential is read (also
 * when either receipt has no fingerprint, e.g. one from before this field). FULL must be under
 * BACKLOG_OPS_BINDING_MAX_AGE_MS old and created before the refresh; the refresh under BACKLOG_OPS_FRESH_BACKFILL_MS
 * old; the EXPORT created after the refresh (it records the fence views it read). The receipts of the
 * bound runs are downloaded and checked before any credential is read: every backfill section complete; the export
 * with failed = fence_late = orgs.incomplete = 0. The tools enforce the rest themselves: the export refuses while
 * the v2 claim-sessions reader gate is off (export_claim_sessions_gate_off), and the drain refuses beside a live
 * lease (96bulls/.drain/lock.json). The workflow never passes --checkpoint-file or --start-after: the drain's
 * create-only `drained` markers make a rerun skip what is written, so a restart only re-lists.
 *
 * Catch-up (decision 0005's two-phase cutover per org; docs/backlog-ops-workflow.md): phase A is a FULL
 * backfill-execute while the org still uploads through SQL; phase B, after the org's HTTP admission stop and the
 * Worker queue drained, is a CATCH-UP, its own two modes, binding that FULL:
 *   backfill-catchup-dry      FULL <backfill-execute run>                (no confirmation, as every dry mode)
 *   backfill-catchup-execute  DRY <backfill-catchup-dry run> FULL <backfill-execute run>
 *   drain-execute             DRY <drain-dry run> BACKFILL <backfill-catchup-execute run> FULL <backfill-execute run> EXPORT <export-execute run>
 * It runs all three sections, fences and ownership in full (as a refresh) and revisions with --revisions-since-ms t,
 * where t = FULL's created_at minus BACKLOG_OPS_CATCHUP_MARGIN_MS, computed by the gate from the GitHub API (never an
 * input) and handed to the credential step as a step output (BACKLOG_OPS_REVISIONS_SINCE_MS). Every revision row
 * changed after t is re-applied; every row unchanged since t existed when FULL began, so FULL read it; merges never
 * lower, so FULL + catch-up leave what one full run after the stop would. FULL: a successful backfill-execute at the
 * same sha, under BACKLOG_OPS_BINDING_MAX_AGE_MS old, exactly the catch-up's scope, created before the catch-up (and
 * before its DRY). The catch-up's receipt must show all three sections complete, failed 0 in each, and its
 * `revisions_since_ms` equal to t (so its DRY, and a drain's BACKFILL, bound the same FULL). A drain binds a catch-up
 * like a refresh (BACKFILL with FULL: fresh under BACKLOG_OPS_FRESH_BACKFILL_MS, FULL before it, the export after it)
 * but without the fingerprint check: the fingerprints differ by design (the org uploaded during FULL), and the
 * catch-up re-read everything that could have changed instead.
 *
 * Org scope (BACKLOG_OPS_ORGS): an optional environment secret on both environments (a secret, so this public
 * repository's logs mask it): comma-separated lowercase org UUIDs; unset or empty runs every org, as before. The org
 * ids never reach the repository, a log or a receipt: the receipt records only the scope's id (`all`, or `orgs_` +
 * 32 hex of the sorted list's sha256). Every bound run must carry the same scope as the run it feeds, except that an
 * unscoped (`all`) backfill or export may feed a scoped execute, but never as the drain's BACKFILL or FULL (exact
 * scope there: checkBoundReceipt `exact`). A receipt from before scoping has no scope and
 * counts as `all`. Scoping changes no tool rule: the drain still writes nothing for an org without its
 * fence-authority marker.
 *
 * Path B (no-head parity, docs/decisions/0005-s3-native-upload-ledger.md step 0b; default off): the export modes may
 * prove an absent control head by a fresh no_head parity row alone for the orgs in the environment SECRET
 * BACKLOG_OPS_NO_HEAD_PARITY_ORGS (same format as BACKLOG_OPS_ORGS, set identically on both environments or on
 * neither; every org must be in the run's BACKLOG_OPS_ORGS scope, so a scoped run is required), with the parity row's
 * maximum age in the environment variable BACKLOG_OPS_NO_HEAD_PARITY_MAX_AGE_SECONDS (digits, 60 to 21600, required
 * exactly when the list is set). Neither is a dispatch input: run inputs are public. The list reaches the export only
 * on its stdin pipe (never argv or a log); the age as --no-head-parity-max-age. The receipt records `no_head_parity`,
 * a hash of the sorted list and the age (`nhp_<32 hex>`), only when the list is set (so with it unset every receipt is
 * byte-identical to before). Binding, refusal-only: an export-execute refuses unless its dry run recorded the same
 * value (backlog_ops_bound_dry_no_head_parity), and a drain-execute refuses unless the export it binds recorded the
 * value its own environment gives (backlog_ops_bound_export_no_head_parity); a receipt without the field counts as
 * unset. The drain tool itself is unchanged and gets neither value. Every other mode refuses either variable set.
 * The parity age is checked by the export's SQL at each run's own verdict, so an execute re-checks it, never inherits
 * the dry run's.
 *
 * Receipts are counts only: every leaf a number, a boolean or a short lowercase code; any other string (an id, a
 * key, a URL) refuses the receipt. Errors carry fixed codes only.
 *
 * presence-dry (scripts/session-presence.ts, src/session-ingest/session-presence.ts) is a seventh, DRY-ONLY mode: a
 * counts-only look at ONE session for support diagnosis ("No conversation recorded yet"). There is no
 * presence-execute: the dispatch refuses it with backlog_ops_presence_dry_only before anything else, and every
 * execute-only path refuses the presence tool again. It binds nothing, runs in the dry environment with the read-only
 * role, and takes its session id only from the dry environment's secret SESSION_PRESENCE_SESSION
 * (backlogOpsPresenceSession), handed to the tool on its stdin pipe beside the database URL, never argv, a log or the
 * receipt.
 *
 * Authority (src/session-ingest/backlog-authority.ts; docs/backlog-ops-workflow.md "Authority"), scoped runs only:
 *   reshard-dry        DRY ONLY (reshard-execute refuses backlog_ops_reshard_dry_only), binds nothing: the revision
 *                      reshard's verify per org against the revision home (a ledger tool: it takes the ledger bucket).
 *   authority-dry      binds nothing: the unbound live evidence (Cloudflare token on stdin, read-only SQL).
 *   authority-execute  DRY <authority-dry> BACKFILL <backfill-catchup-execute> FULL <backfill-execute> RESHARD <reshard-dry>
 *                      HELD (BACKLOG_OPS_AUTHORITY_EXECUTE_ENABLED, refused backlog_ops_authority_held before any read).
 *   backfill-catchup-* may add AUTHORITY <authority-dry>: the catch-up then runs authorityBaseline in its input snapshot.
 * Every authority receipt records the ledger target (BACKLOG_OPS_LEDGER_ID_TOOLS) and binds the same, exact scope.
 *
 * Marker (src/session-ingest/marker-publication.ts; docs/backlog-ops-workflow.md "Marker publication"), scoped runs only:
 *   marker-dry      AUTHORITY <authority-dry> BACKFILL <backfill-catchup-execute> FULL <backfill-execute> RESHARD <reshard-dry>
 *                   (no confirmation): per org, the 062320 hold, the attestations and the bound evidence, read-only;
 *                   records each ready org's hold epoch hash and the wave id over them.
 *   marker-execute  DRY <marker-dry> BACKFILL .. FULL .. RESHARD .. AUTHORITY .. WAVE <wave id>, binding exactly the runs
 *                   its marker-dry bound; publishes each wave org's marker under its enforced hold. HELD
 *                   (BACKLOG_OPS_MARKER_EXECUTE_ENABLED, refused backlog_ops_marker_held before any read).
 */
import { createHash } from "node:crypto";

/** Execute modes stay refused until a reviewed commit flips this. Dry modes are always allowed. */
export const BACKLOG_OPS_EXECUTE_ENABLED: boolean = true;

export const BACKLOG_OPS_WORKFLOW = ".github/workflows/backlog-ops.yml";
export const BACKLOG_OPS_MODES = [
  "backfill-dry", "backfill-execute", "export-dry", "export-execute", "drain-dry", "drain-execute", "presence-dry",
  "backfill-refresh-dry", "backfill-refresh-execute", "backfill-catchup-dry", "backfill-catchup-execute",
  "reshard-dry", "authority-dry", "authority-execute", "bridge-dry", "bridge-execute", "marker-dry", "marker-execute",
  "hold-dry", "hold-install-execute", "hold-seal-execute", "hold-rollback-execute",
] as const;
export type BacklogOpsMode = (typeof BACKLOG_OPS_MODES)[number];
/** The backfill's fences+ownership refresh (its tool is `backfill`; see the header). */
export const isRefresh = (mode: BacklogOpsMode) => mode.startsWith("backfill-refresh-");
/** The backfill's catch-up after a FULL run (its tool is `backfill`; see the header). */
export const isCatchup = (mode: BacklogOpsMode) => mode.startsWith("backfill-catchup-");
/**
 * t = FULL's created_at minus this. FULL's tool starts after its created_at (gate, approval), so t is before FULL's
 * first read; the margin also covers clock skew between GitHub and the database, and a revision upsert's
 * transaction-start stamp (updated_at = now()) from a transaction begun up to this long before FULL was created.
 */
export const BACKLOG_OPS_CATCHUP_MARGIN_MS = 15 * 60 * 1000;
/** The input fingerprint every backfill receipt carries (upload-fence-backfill.ts BackfillFingerprint), field by field. */
export const BACKLOG_OPS_FINGERPRINT_FIELDS = [
  "revisions_count", "revisions_max_updated_ms", "revisions_hash", "ownership_count", "ownership_hash",
  "slots_owned_count", "slots_revision_sum", "slots_hash",
] as const;
export type BacklogOpsTool = "backfill" | "export" | "drain" | "presence" | "reshard" | "authority" | "bridge" | "marker" | "hold";
/** Tools with a dry mode only: no `<tool>-execute` exists, is accepted or can be enabled (refused `backlog_ops_<tool>_dry_only`). */
export const BACKLOG_OPS_DRY_ONLY_TOOLS: readonly BacklogOpsTool[] = ["presence", "reshard"];
/**
 * authority-execute (the one mode that writes a per-org fence-authority marker) has its own hold, on top of the dormant
 * switch: refused `backlog_ops_authority_held` before any GitHub read until a reviewed commit flips it. It stays off
 * until the live-evidence module (authority-evidence.ts) and the sweeper's serving-spec writer have passed review.
 */
export const BACKLOG_OPS_AUTHORITY_EXECUTE_ENABLED: boolean = false;
/** The tools that only run on a scoped run (BACKLOG_OPS_ORGS set): their verdicts are per org. */
export const BACKLOG_OPS_SCOPED_TOOLS: readonly BacklogOpsTool[] = ["reshard", "authority", "bridge", "marker", "hold"];
/**
 * bridge-execute (the admission hold's bridge_complete release, hold-bridge.ts / erasure-window.ts) has its own hold:
 * refused `backlog_ops_bridge_held` before any GitHub read until a reviewed commit flips it, after slot 062320 lands.
 */
export const BACKLOG_OPS_BRIDGE_EXECUTE_ENABLED: boolean = false;
/** The bridge modes: bridge-dry reads each org's erasure-window proof; bridge-execute (held) would release on it. */
export const isBridge = (mode: BacklogOpsMode) => mode.startsWith("bridge-");
/**
 * marker-execute (the per-org fence-authority marker under the org's enforced 062320 admission hold,
 * marker-publication.ts) has its own hold: refused `backlog_ops_marker_held` before any GitHub read until a reviewed
 * commit flips it (after the hold is installed and sealed for a wave under Khaliq's explicit OK).
 */
export const BACKLOG_OPS_MARKER_EXECUTE_ENABLED: boolean = false; // not consulted since the wave-1 enable: see requireScopeEnabled
/**
 * The 062320 hold's lifecycle executes (hold-lifecycle.ts: install, seal+bind, verified rollback) have their own hold:
 * refused `backlog_ops_hold_held` before any GitHub read until a reviewed commit flips it. Installing a hold on a real
 * database also needs Khaliq's explicit OK.
 */
export const BACKLOG_OPS_HOLD_EXECUTE_ENABLED: boolean = false; // not consulted since the wave-1 enable: see requireScopeEnabled
/**
 * WAVE 1 ENABLE (owner decision A' 252297350480269312, as Pipeline relays it; lane assignment 7aad4e6c9ec7). The
 * marker execute and the hold executes may run only for a run whose ENTIRE secret org scope hashes into these reviewed
 * allow-lists: org HASHES (authority-evidence.ts evidenceHash("org", id)), never ids, which this public repository must
 * not carry. Each list is empty or holds exactly two entries. Empty means held, exactly as the boolean switches above
 * (which stay false). The check runs three times: the dispatch (a non-empty list, or the switch), the credential step
 * before any credential (every scope org's hash listed), and the tool itself (its --org arguments). Enabling is code;
 * every dispatch still needs its environment approval, and a live install or marker needs Khaliq's explicit OK.
 * Install readiness is unaffected: fan-out stays unproven, so a hold install still refuses before _install.
 */
export const BACKLOG_OPS_MARKER_ENABLED_ORG_HASHES: readonly string[] = Object.freeze([]);
export const BACKLOG_OPS_HOLD_ENABLED_ORG_HASHES: readonly string[] = Object.freeze([]);
const ORG_HASH = /^h_[0-9a-f]{32}$/u;
/** Whether an allow-list is well-formed: empty, or exactly two distinct org hashes. */
export const validEnabledOrgHashes = (l: readonly string[]) => (l.length === 0 || l.length === 2) && l.every((h) => ORG_HASH.test(h)) && new Set(l).size === l.length;
/** The two lists, when populated, are the same pair (the hold and the marker are enabled for the same wave). */
export const enabledListsAgree = (a: readonly string[], b: readonly string[]) =>
  validEnabledOrgHashes(a) && validEnabledOrgHashes(b) && a.length === b.length && [...a].sort().every((h, i) => h === [...b].sort()[i]);
/**
 * How a run's scope (its secret orgs' hashes) stands against an allow-list, by EXACT set equality (Pipeline
 * 7c3ea742b1fa): `match` only when the scope is exactly the reviewed pair, each once; `mismatch` for an unset scope, a
 * singleton, a substituted, extra or repeated org; `disabled` when the list is empty. Never names an org.
 */
export function enabledScope(scopeOrgHashes: readonly string[] | null, allow: readonly string[]): "match" | "mismatch" | "disabled" {
  if (!validEnabledOrgHashes(allow)) return "mismatch";
  if (allow.length === 0) return "disabled";
  if (!scopeOrgHashes || scopeOrgHashes.length !== allow.length || new Set(scopeOrgHashes).size !== scopeOrgHashes.length) return "mismatch";
  const want = [...allow].sort(), got = [...scopeOrgHashes].sort();
  return got.every((h, i) => h === want[i]) ? "match" : "mismatch";
}
/** The two reviewed lists (production: the constants above; tests inject a synthetic pair). */
export interface EnabledOrgHashes { marker: readonly string[]; hold: readonly string[] }
export const BACKLOG_OPS_ENABLED_ORG_HASHES: EnabledOrgHashes = Object.freeze({ marker: BACKLOG_OPS_MARKER_ENABLED_ORG_HASHES, hold: BACKLOG_OPS_HOLD_ENABLED_ORG_HASHES });
/** The lists enable anything at all: both valid, non-empty and the same pair. */
export const enabledListsActive = (l: EnabledOrgHashes) => l.marker.length > 0 && enabledListsAgree(l.marker, l.hold);
/**
 * THE enable check for the marker and hold executes, and the only one: the lists are active (valid, non-empty,
 * agreeing) AND the scope is EXACTLY the pair (Pipeline 6ca1decc4efa). There is no switch path: the boolean switches
 * above are never consulted for these modes, so no future flip can bypass the secret-scope or the tool check.
 * Refuses `<code>` otherwise, never naming an org.
 */
export function requireScopeEnabled(scopeOrgHashes: readonly string[] | null, lists: EnabledOrgHashes, which: "marker" | "hold", code: string): void {
  if (!enabledListsActive(lists) || enabledScope(scopeOrgHashes, lists[which]) !== "match") refuse(code);
}
/** The hold modes: hold-dry (read only) and the three held executes, each binding a hold-dry as its DRY. */
export const isHold = (mode: BacklogOpsMode) => mode.startsWith("hold-");
/** The marker modes (marker-publication.ts): marker-dry reads readiness and pins the wave; marker-execute (held) publishes. */
export const isMarker = (mode: BacklogOpsMode) => mode.startsWith("marker-");
/**
 * The modes a per-org wave run may take (backlog-wave.ts; input per_org, BACKLOG_OPS_PER_ORG=true, a scoped run): the
 * credential step runs the unchanged tool once per scope org and writes a wave receipt with one verdict per org.
 */
export const BACKLOG_OPS_PER_ORG_MODES: readonly BacklogOpsMode[] = ["backfill-dry", "backfill-execute", "backfill-refresh-dry", "backfill-refresh-execute",
  "backfill-catchup-dry", "backfill-catchup-execute", "export-dry", "export-execute", "drain-dry", "drain-execute"];
/** A wave id (org-wave.ts waveId): the marker-execute confirmation's `WAVE` token. */
const WAVE_ID = /^h_[0-9a-f]{32}$/u;
/** The receipt artifact of every run (retention 30 days) and its single file. */
export const BACKLOG_OPS_RECEIPT_ARTIFACT = "backlog-ops-receipt";
export const BACKLOG_OPS_RECEIPT_FILE = "backlog-ops-receipt.json";
export const BACKLOG_OPS_RECEIPT_CONTRACT = "backlog-ops-receipt/v1";
/** A bound dry run, backfill or export older than this cannot feed an execute. */
export const BACKLOG_OPS_BINDING_MAX_AGE_MS = 24 * 60 * 60 * 1000;
/** The drain's backfill must be this fresh: "a rerun of the fence backfill immediately before". */
export const BACKLOG_OPS_FRESH_BACKFILL_MS = 3 * 60 * 60 * 1000;
/** The role session (360 min). The operate job's timeout (355 min) is below it, so a tool never outlives its credentials. */
export const BACKLOG_OPS_SESSION_SECONDS = 6 * 60 * 60;
/**
 * The protected GitHub environments. Dry modes run in the schema release environment; every execute runs in its own
 * environment, so the write role can trust only that environment's OIDC subject and no other job in the schema
 * environment (this workflow's dry runs, or the other guarded operators there) can assume it. The environment is
 * derived from the mode here, never taken from an input.
 */
export const BACKLOG_OPS_DRY_ENVIRONMENT = "session-schema-production";
export const BACKLOG_OPS_EXECUTE_ENVIRONMENT = "session-backlog-execute";
export const backlogOpsEnvironment = (mode: BacklogOpsMode) =>
  isExecute(mode) ? BACKLOG_OPS_EXECUTE_ENVIRONMENT : BACKLOG_OPS_DRY_ENVIRONMENT;
/** A role session must outlive the job (355 min timeout) with this margin, or the run refuses before the tool starts. */
export const BACKLOG_OPS_MIN_SESSION_REMAINING_MS = 5 * 60 * 60 * 1000;
export const BACKLOG_OPS_MAX_OBJECTS_CEILING = 1_000_000;
/** slot_pace_ms's ceiling (ms): upload-fence-backfill.ts SLOT_PACE_MAX_MS. */
export const BACKLOG_OPS_SLOT_PACE_MAX_MS = 5_000;

const SHA40 = /^[0-9a-f]{40}$/u;
const RUN_ID = /^[1-9][0-9]{0,19}$/u;
const BUCKET = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u;
const REGION = /^[a-z]{2}(-gov)?-[a-z]+-[0-9]$/u;
const KMS_KEY_ARN = /^arn:aws:kms:([a-z]{2}(?:-gov)?-[a-z]+-[0-9]):([0-9]{12}):key\/(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|mrk-[0-9a-f]{32})$/u;
const ROLE_ARN = /^arn:aws:iam::([0-9]{12}):role\/[A-Za-z0-9+=,.@_-]{1,64}$/u;
const CODE = /^[a-z][a-z0-9_]{0,39}$/u;
const ORG_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const SCOPE_ID = /^(all|orgs_[0-9a-f]{32})$/u;
const LEDGER_ID = /^(none|ledger_[0-9a-f]{32})$/u;
const NHP_ID = /^(none|nhp_[0-9a-f]{32})$/u;
/** The most orgs one scoped run may name. */
export const BACKLOG_OPS_MAX_ORGS = 64;
const KEY = /^[a-z][a-z0-9_]{0,63}$/u;

export class BacklogOpsRefusal extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "BacklogOpsRefusal";
  }
}
export function refuseBacklogOps(code: string): never {
  throw new BacklogOpsRefusal(code);
}
function refuse(code: string): never {
  throw new BacklogOpsRefusal(code);
}

export const toolOf = (mode: BacklogOpsMode): BacklogOpsTool => mode.split("-")[0] as BacklogOpsTool;
export const isExecute = (mode: BacklogOpsMode) => mode.endsWith("-execute");
/** An execute's own dry mode (`backfill-refresh-execute` -> `backfill-refresh-dry`). */
export const dryModeOf = (mode: BacklogOpsMode) => mode.replace(/-execute$/u, "-dry") as BacklogOpsMode;

export interface BacklogOpsDispatch {
  mode: BacklogOpsMode;
  sha: string;
  /** Execute only: the same tool's successful dry run at this commit. */
  dryRun: string | null;
  /** export-execute and drain-execute: a successful backfill-execute at this commit. */
  backfillRun: string | null;
  /** drain-execute: a successful export-execute at this commit, after the backfill. */
  exportRun: string | null;
  /**
   * drain-execute, optional: the FULL backfill-execute that `backfillRun` (then a backfill-refresh-execute or a
   * backfill-catchup-execute) refreshes or catches up. Null: the drain's backfill is a full backfill-execute, as
   * before. backfill-catchup-dry/-execute: required, the FULL the catch-up follows.
   */
  fullBackfillRun: string | null;
  /** authority-execute only (required): a successful reshard-dry at this commit whose verify passed for every org. */
  reshardRun: string | null;
  /**
   * backfill-catchup-dry/-execute only (optional): a successful authority-dry at this commit. With it the catch-up runs
   * authorityBaseline inside its input snapshot (authority-evidence.ts) and its receipt carries the baseline.
   */
  authorityRun: string | null;
  /** Drain modes only: the per-run object budget (empty = unbounded). */
  maxObjects: number | null;
  /**
   * Backfill modes only (BACKLOG_OPS_SLOTS_READ, input slots_read): "org" reads a scoped run's owned slots once per org
   * through the org index (upload-fence-backfill.ts --slots-by-org); "heap" (the default, empty input) keeps the heap
   * block windows, byte-identical to before.
   */
  slotsRead: "heap" | "org";
  /**
   * Backfill modes with slotsRead "org" only (BACKLOG_OPS_SLOT_PACE_MS, input slot_pace_ms): the pause in ms between
   * the org slot cursor's FETCHes (upload-fence-backfill.ts --slot-pace-ms). Null (empty input, or "0") is unpaced and
   * passes no flag, so the argv is byte-identical to before.
   */
  slotPaceMs: number | null;
  /**
   * marker-execute only (required, BACKLOG_OPS_WAVE_ID, input wave_id): the wave id its marker-dry recorded
   * (org-wave.ts waveId over the commit, scope, bound runs and each ready org's hold epoch). The confirmation names it
   * (`WAVE <id>`), so the environment approval covers exactly that wave.
   */
  waveId?: string | null;
  /** BACKLOG_OPS_PER_ORG_MODES only (input per_org = "true"): one child run and one verdict per scope org. */
  perOrg?: boolean;
  /**
   * A per-org EXECUTE only (input previous_run, optional): an earlier incomplete run of the same execute at this sha,
   * with the same bindings. Its completed orgs are carried; only the outstanding orgs run again (backlog-wave.ts).
   */
  previousRun?: string | null;
  confirmation: string;
}

/** The typed binding an execute must carry, exactly. */
export function backlogOpsConfirmation(d: Pick<BacklogOpsDispatch, "mode" | "sha" | "dryRun" | "backfillRun" | "exportRun"> &
  Partial<Pick<BacklogOpsDispatch, "fullBackfillRun" | "reshardRun" | "authorityRun" | "waveId" | "perOrg" | "previousRun">>): string {
  return [d.mode.toUpperCase(), d.sha, "DRY", d.dryRun,
    ...(d.backfillRun ? ["BACKFILL", d.backfillRun] : []), ...(d.fullBackfillRun ? ["FULL", d.fullBackfillRun] : []),
    ...(d.exportRun ? ["EXPORT", d.exportRun] : []), ...(d.reshardRun ? ["RESHARD", d.reshardRun] : []),
    ...(d.authorityRun ? ["AUTHORITY", d.authorityRun] : []), ...(d.waveId ? ["WAVE", d.waveId] : []),
    ...(d.previousRun ? ["PREVIOUS", d.previousRun] : []),
    // A per-org wave run changes the run's shape (independent verdicts, partial success): the approval names it.
    ...(d.perOrg ? ["PER_ORG"] : [])].join(" ");
}

/** The dispatch inputs, checked before any GitHub read or credential. */
export function backlogOpsDispatch(
  env: Record<string, string | undefined>,
  enabled: boolean = BACKLOG_OPS_EXECUTE_ENABLED,
  authorityEnabled: boolean = BACKLOG_OPS_AUTHORITY_EXECUTE_ENABLED,
  bridgeEnabled: boolean = BACKLOG_OPS_BRIDGE_EXECUTE_ENABLED,
  markerEnabled: boolean = BACKLOG_OPS_MARKER_EXECUTE_ENABLED,
  holdEnabled: boolean = BACKLOG_OPS_HOLD_EXECUTE_ENABLED,
  enabledOrgHashes: EnabledOrgHashes = BACKLOG_OPS_ENABLED_ORG_HASHES,
): BacklogOpsDispatch {
  const mode = env.BACKLOG_OPS_MODE ?? "", sha = env.BACKLOG_OPS_SHA ?? "";
  // A dry-only tool's execute is not a mode: refused with its own code, before the dormant switch is even read.
  for (const t of BACKLOG_OPS_DRY_ONLY_TOOLS) if (mode === `${t}-execute`) refuse(`backlog_ops_${t}_dry_only`);
  const dryRun = env.BACKLOG_OPS_DRY_RUN ?? "", backfillRun = env.BACKLOG_OPS_BACKFILL_RUN ?? "",
    exportRun = env.BACKLOG_OPS_EXPORT_RUN ?? "", maxObjects = env.BACKLOG_OPS_MAX_OBJECTS ?? "",
    fullBackfillRun = env.BACKLOG_OPS_FULL_BACKFILL_RUN ?? "", confirmation = env.BACKLOG_OPS_CONFIRMATION ?? "",
    slotsReadRaw = env.BACKLOG_OPS_SLOTS_READ ?? "", slotPaceRaw = env.BACKLOG_OPS_SLOT_PACE_MS ?? "",
    reshardRun = env.BACKLOG_OPS_RESHARD_RUN ?? "", authorityRun = env.BACKLOG_OPS_AUTHORITY_RUN ?? "", waveId = env.BACKLOG_OPS_WAVE_ID ?? "",
    perOrgRaw = env.BACKLOG_OPS_PER_ORG ?? "", previousRun = env.BACKLOG_OPS_PREVIOUS_RUN ?? "";
  if (!(BACKLOG_OPS_MODES as readonly string[]).includes(mode) || !SHA40.test(sha)) refuse("backlog_ops_inputs");
  const m = mode as BacklogOpsMode, tool = toolOf(m), execute = isExecute(m);
  if (execute && enabled !== true) refuse("backlog_ops_dormant");
  if (m === "authority-execute" && authorityEnabled !== true) refuse("backlog_ops_authority_held");
  if (m === "bridge-execute" && bridgeEnabled !== true) refuse("backlog_ops_bridge_held");
  // ONLY active, agreeing reviewed lists enable these (the switches are not consulted: no bypass); the scope itself is
  // checked by the credential step and the tool. markerEnabled/holdEnabled are kept in the signature, unused.
  void markerEnabled; void holdEnabled;
  if ((m === "marker-execute" || (isHold(m) && execute)) && !enabledListsActive(enabledOrgHashes)) refuse(m === "marker-execute" ? "backlog_ops_marker_held" : "backlog_ops_hold_held");
  // A marker mode binds the catch-up (BACKFILL) whose baseline the evidence is bound to, dry or execute.
  const wantsBackfill = (execute && tool !== "backfill" && tool !== "bridge" && tool !== "hold") || isMarker(m), wantsExport = execute && tool === "drain";
  // reshard-dry may bind the completed catch-up it verifies after (BACKFILL, optional; authority-execute requires it).
  const mayBackfill = m === "reshard-dry";
  if (execute ? !RUN_ID.test(dryRun) : dryRun !== "") refuse("backlog_ops_dry_run");
  if (wantsBackfill ? !RUN_ID.test(backfillRun) : backfillRun !== "" && !(mayBackfill && RUN_ID.test(backfillRun))) refuse("backlog_ops_backfill_run");
  if (wantsExport ? !RUN_ID.test(exportRun) : exportRun !== "") refuse("backlog_ops_export_run");
  // FULL: optional on drain-execute (absent = the drain's backfill is a full one, as before), required on a catch-up and
  // on authority-execute, refused everywhere else.
  if (isCatchup(m) || m === "authority-execute" || isBridge(m) || isMarker(m) || m === "hold-seal-execute" ? !RUN_ID.test(fullBackfillRun) : fullBackfillRun !== "" && (m !== "drain-execute" || !RUN_ID.test(fullBackfillRun)))
    refuse("backlog_ops_full_backfill_run");
  // RESHARD: authority-execute only, required. AUTHORITY: a catch-up only, optional.
  if (m === "authority-execute" || isMarker(m) ? !RUN_ID.test(reshardRun) : reshardRun !== "") refuse("backlog_ops_reshard_run");
  // AUTHORITY: a catch-up (optional) or a marker mode (required: the authority-dry the catch-up's baseline names).
  if (isMarker(m) ? !RUN_ID.test(authorityRun) : authorityRun !== "" && (!isCatchup(m) || !RUN_ID.test(authorityRun))) refuse("backlog_ops_authority_run");
  // WAVE: marker-execute only, required.
  // WAVE: marker-execute, hold-seal-execute and hold-rollback-execute (the approved orgs at their hold epochs).
  if (m === "marker-execute" || m === "hold-seal-execute" || m === "hold-rollback-execute" ? !WAVE_ID.test(waveId) : waveId !== "") refuse("backlog_ops_wave_id");
  // PER_ORG: "true" on a per-org-capable mode, else empty.
  if (!(perOrgRaw === "" || (perOrgRaw === "true" && BACKLOG_OPS_PER_ORG_MODES.includes(m)))) refuse("backlog_ops_per_org");
  // PREVIOUS: a per-org execute only, optional.
  if (previousRun !== "" && (perOrgRaw !== "true" || !execute || !RUN_ID.test(previousRun))) refuse("backlog_ops_previous_run");
  let budget: number | null = null;
  if (maxObjects !== "") {
    if (tool !== "drain" || !/^[1-9][0-9]{0,6}$/u.test(maxObjects) || Number(maxObjects) > BACKLOG_OPS_MAX_OBJECTS_CEILING)
      refuse("backlog_ops_max_objects");
    budget = Number(maxObjects);
  }
  if (!(slotsReadRaw === "" || slotsReadRaw === "heap" || (slotsReadRaw === "org" && tool === "backfill"))) refuse("backlog_ops_slots_read");
  const slotsRead: "heap" | "org" = slotsReadRaw === "org" ? "org" : "heap";
  // Digits only (no sign, space, exponent or leading zero), 0..BACKLOG_OPS_SLOT_PACE_MAX_MS, and only beside slots_read=org.
  if (slotPaceRaw !== "" && (slotsRead !== "org" || !/^(0|[1-9][0-9]{0,3})$/u.test(slotPaceRaw) || Number(slotPaceRaw) > BACKLOG_OPS_SLOT_PACE_MAX_MS))
    refuse("backlog_ops_slot_pace_ms");
  const slotPaceMs = slotPaceRaw === "" || slotPaceRaw === "0" ? null : Number(slotPaceRaw);
  const runs = [dryRun, backfillRun, exportRun, fullBackfillRun, reshardRun, authorityRun, previousRun].filter((r) => r !== "");
  if (new Set(runs).size !== runs.length) refuse("backlog_ops_bindings");
  const d: BacklogOpsDispatch = { mode: m, sha, dryRun: execute ? dryRun : null, backfillRun: wantsBackfill || (mayBackfill && backfillRun !== "") ? backfillRun : null,
    exportRun: wantsExport ? exportRun : null, fullBackfillRun: fullBackfillRun === "" ? null : fullBackfillRun,
    reshardRun: reshardRun === "" ? null : reshardRun, authorityRun: authorityRun === "" ? null : authorityRun, maxObjects: budget, slotsRead, slotPaceMs,
    ...(m === "marker-execute" || m === "hold-seal-execute" || m === "hold-rollback-execute" ? { waveId } : {}), ...(perOrgRaw === "true" ? { perOrg: true } : {}), ...(previousRun !== "" ? { previousRun } : {}), confirmation };
  if (execute ? confirmation !== backlogOpsConfirmation(d) : confirmation !== "") refuse("backlog_ops_confirmation");
  return d;
}

/** Bucket, region and key from the environment's protected variables, shape-checked. */
export interface BacklogOpsTarget { bucket: string; region: string; kmsKeyArn: string; dryRoleArn: string; executeRoleArn: string }
export function backlogOpsTarget(env: Record<string, string | undefined>): BacklogOpsTarget {
  const bucket = env.BACKLOG_OPS_BUCKET ?? "", region = env.BACKLOG_OPS_REGION ?? "", kmsKeyArn = env.BACKLOG_OPS_KMS_KEY_ARN ?? "",
    dryRoleArn = env.BACKLOG_OPS_DRY_ROLE_ARN ?? "", executeRoleArn = env.BACKLOG_OPS_EXECUTE_ROLE_ARN ?? "";
  if (!BUCKET.test(bucket) || bucket.includes("..")) refuse("backlog_ops_target_bucket");
  if (!REGION.test(region)) refuse("backlog_ops_target_region");
  const key = KMS_KEY_ARN.exec(kmsKeyArn);
  if (!key || key[1] !== region) refuse("backlog_ops_target_kms");
  const dry = ROLE_ARN.exec(dryRoleArn), exe = ROLE_ARN.exec(executeRoleArn);
  if (!dry || !exe || dry[1] !== key[2] || exe[1] !== key[2] || dryRoleArn === executeRoleArn) refuse("backlog_ops_target_roles");
  return { bucket, region, kmsKeyArn, dryRoleArn, executeRoleArn };
}

/**
 * The relocated upload ledger (docs/ledger-relocation.md) for the tools that write it: the environment's protected
 * variables BACKLOG_OPS_LEDGER_BUCKET and BACKLOG_OPS_LEDGER_REGION, both or neither (unset or empty: the ledger is in
 * BACKLOG_OPS_BUCKET). The bucket must differ from BACKLOG_OPS_BUCKET. Null when unset.
 */
export interface BacklogOpsLedgerTarget { bucket: string; region: string }
export function backlogOpsLedgerTarget(env: Record<string, string | undefined>, t: Pick<BacklogOpsTarget, "bucket">): BacklogOpsLedgerTarget | null {
  const bucket = env.BACKLOG_OPS_LEDGER_BUCKET ?? "", region = env.BACKLOG_OPS_LEDGER_REGION ?? "";
  if (bucket === "" && region === "") return null;
  if (!BUCKET.test(bucket) || bucket.includes("..") || bucket === t.bucket) refuse("backlog_ops_target_ledger_bucket");
  if (!REGION.test(region)) refuse("backlog_ops_target_ledger_region");
  return { bucket, region };
}

/**
 * The tools that take the ledger bucket: the backfill and the drain (they write the upload ledger's own keys,
 * ledger/**) and reshard-dry (it reads the revision shards where they live).
 */
export const BACKLOG_OPS_LEDGER_TOOLS: readonly BacklogOpsTool[] = ["backfill", "drain", "reshard"];
/**
 * The tools whose receipt records the ledger target (backlogOpsLedgerId) and whose executes bind it: the ledger tools
 * and the authority modes (which take no ledger bucket themselves but bind the backfills and the reshard verify that
 * ran against it: the marker is written over exactly that ledger).
 */
export const BACKLOG_OPS_LEDGER_ID_TOOLS: readonly BacklogOpsTool[] = [...BACKLOG_OPS_LEDGER_TOOLS, "authority", "bridge", "marker", "hold"];

/**
 * The ledger variables every S3 tool child gets (the backfill, the export and the drain; never presence-dry, which
 * reads no S3), added by scripts/backlog-ops.ts to its allowlisted environment:
 * INGEST_LEDGER_HOME_GUARD=on, ALWAYS, so a single-bucket backfill or drain refuses (`ledger_home_moved`) once a
 * relocation wrote "moved" into BACKLOG_OPS_BUCKET's marker, instead of writing stale copies. The accepted-stage export
 * gets it too but never reads the marker (it writes no ledger key), so for the export the variable changes nothing.
 * For the ledger tools with a ledger target, also INGEST_LEDGER_BUCKET / INGEST_LEDGER_REGION, which route their
 * ledger/** keys to that bucket and require both markers to name it (BACKLOG_OPS_BUCKET's and the ledger bucket's
 * own). No endpoint is ever passed (AWS S3).
 */
export function backlogOpsLedgerEnv(d: Pick<BacklogOpsDispatch, "mode">, ledger: BacklogOpsLedgerTarget | null): Record<string, string> {
  // The presence tool reads no S3 at all: its environment stays exactly the allowlist plus the session.
  if (toolOf(d.mode) === "presence") return {};
  return {
    INGEST_LEDGER_HOME_GUARD: "on",
    ...(ledger && BACKLOG_OPS_LEDGER_TOOLS.includes(toolOf(d.mode)) ? { INGEST_LEDGER_BUCKET: ledger.bucket, INGEST_LEDGER_REGION: ledger.region } : {}),
  };
}

/**
 * The ledger target a run's tool was given, as recorded in its receipt: `none` (the tool takes no ledger bucket: the
 * export, presence, or a backfill / drain with BACKLOG_OPS_LEDGER_* unset) or `ledger_<32 hex>`, a hash of the bucket
 * and region. Never the name itself: receipts are echoed to this public repository's logs. An execute refuses unless
 * the ledger tools it binds recorded the same target (checkBoundReceipts, `backlog_ops_bound_<kind>_ledger`).
 */
export const BACKLOG_OPS_LEDGER_NONE = "none";
export function backlogOpsLedgerId(d: Pick<BacklogOpsDispatch, "mode">, ledger: BacklogOpsLedgerTarget | null): string {
  if (!ledger || !BACKLOG_OPS_LEDGER_ID_TOOLS.includes(toolOf(d.mode))) return BACKLOG_OPS_LEDGER_NONE;
  return "ledger_" + createHash("sha256").update(`backlog-ops-ledger/v1\n${ledger.bucket}\n${ledger.region}`).digest("hex").slice(0, 32);
}

/**
 * The authority receipts (reshard-dry, authority-*, a catch-up bound to an authority-dry): the only receipts that may
 * hold lists, and the ones that record the store-home pair (backlogOpsHomeId) and bind it exactly.
 */
export const backlogOpsAuthorityReceipt = (d: Pick<BacklogOpsDispatch, "mode" | "authorityRun">) =>
  BACKLOG_OPS_SCOPED_TOOLS.includes(toolOf(d.mode)) || (isCatchup(d.mode) && d.authorityRun !== null);
const HOME_ID = /^home_[0-9a-f]{32}$/u;
/**
 * The store-home pair an authority receipt was produced against: the Locker (transcripts) bucket and region and the
 * ledger target (or none), hashed (never a name). Unlike the ledger id it is never `none`: a single-home deployment
 * still names its Locker bucket, so two environments' runs cannot stand in for each other.
 */
export function backlogOpsHomeId(t: Pick<BacklogOpsTarget, "bucket" | "region">, ledger: BacklogOpsLedgerTarget | null): string {
  return "home_" + createHash("sha256").update(`backlog-ops-home/v1\n${t.bucket}\n${t.region}\n${ledger?.bucket ?? "-"}\n${ledger?.region ?? "-"}`).digest("hex").slice(0, 32);
}

/** A run's org scope: the orgs (sorted, distinct) or null for every org, and its id for receipts. */
export interface BacklogOpsScope { orgs: string[] | null; id: string }
export const BACKLOG_OPS_SCOPE_ALL = "all";
/** BACKLOG_OPS_ORGS, from the environment secret: unset or empty = every org. */
export function backlogOpsScope(env: Record<string, string | undefined>): BacklogOpsScope {
  const raw = (env.BACKLOG_OPS_ORGS ?? "").trim();
  if (raw === "") return { orgs: null, id: BACKLOG_OPS_SCOPE_ALL };
  const parts = raw.split(",");
  if (parts.length > BACKLOG_OPS_MAX_ORGS || !parts.every((o) => ORG_UUID.test(o))) refuse("backlog_ops_scope");
  const orgs = [...new Set(parts)].sort();
  if (orgs.length !== parts.length) refuse("backlog_ops_scope");
  return { orgs, id: "orgs_" + createHash("sha256").update(orgs.join(",")).digest("hex").slice(0, 32) };
}

/** Path B (header): the allow-list, the parity row's maximum age and their receipt id. Null when off. */
export interface BacklogOpsNoHeadParity { orgs: string[]; maxAgeSeconds: number; id: string }
export const BACKLOG_OPS_NO_HEAD_PARITY_NONE = "none";
export const BACKLOG_OPS_NO_HEAD_PARITY_MIN_AGE_SECONDS = 60;
export const BACKLOG_OPS_NO_HEAD_PARITY_MAX_AGE_SECONDS = 6 * 60 * 60;
/** The tools that read Path B's variables: the export (uses them) and the drain (binds its export's record of them). */
const NHP_TOOLS: readonly BacklogOpsTool[] = ["export", "drain"];
export function backlogOpsNoHeadParity(env: Record<string, string | undefined>, d: Pick<BacklogOpsDispatch, "mode">, scope: BacklogOpsScope): BacklogOpsNoHeadParity | null {
  const raw = (env.BACKLOG_OPS_NO_HEAD_PARITY_ORGS ?? "").trim(), ageRaw = (env.BACKLOG_OPS_NO_HEAD_PARITY_MAX_AGE_SECONDS ?? "").trim();
  if (!NHP_TOOLS.includes(toolOf(d.mode))) {
    if (raw !== "" || ageRaw !== "") refuse("backlog_ops_no_head_parity_unexpected");
    return null;
  }
  if (raw === "") {
    if (ageRaw !== "") refuse("backlog_ops_no_head_parity_max_age");
    return null;
  }
  const parts = raw.split(",");
  if (parts.length > BACKLOG_OPS_MAX_ORGS || !parts.every((o) => ORG_UUID.test(o))) refuse("backlog_ops_no_head_parity_orgs");
  const orgs = [...new Set(parts)].sort();
  if (orgs.length !== parts.length) refuse("backlog_ops_no_head_parity_orgs");
  // At most the run's scope: a scoped run, every listed org in it.
  if (scope.orgs === null || !orgs.every((o) => scope.orgs!.includes(o))) refuse("backlog_ops_no_head_parity_scope");
  if (!/^[1-9][0-9]{1,4}$/u.test(ageRaw) || Number(ageRaw) < BACKLOG_OPS_NO_HEAD_PARITY_MIN_AGE_SECONDS || Number(ageRaw) > BACKLOG_OPS_NO_HEAD_PARITY_MAX_AGE_SECONDS)
    refuse("backlog_ops_no_head_parity_max_age");
  const maxAgeSeconds = Number(ageRaw);
  return { orgs, maxAgeSeconds, id: "nhp_" + createHash("sha256").update(`backlog-ops-no-head-parity/v1\n${orgs.join(",")}\n${maxAgeSeconds}`).digest("hex").slice(0, 32) };
}

/**
 * The backfill's paging for production. Its users page is a DISTINCT over six tables, one of which (ingest_batches)
 * has no index on the owner key, so every page scans in full: the tool's 30 s default timed out on the first page
 * (sqlstate_57014). Fewer, larger pages and the tool's maximum per-statement timeout. Each page is still its own
 * READ ONLY transaction.
 */
// History: --concurrency 16 (the tool's maximum) was used because a scoped full execute at 4 took over 3 h (~10.8M
// revision entries) and once had to bind the drain directly (BACKLOG_OPS_FRESH_BACKFILL_MS). The drain now binds a fresh
// refresh run plus a FULL run up to 24 h old, so the full run may be slow. S3 writes are per-key CAS with no shared hot
// key; SQL reads stay one transaction at a time on the connection.
// Middle load (2026-11-17, production owner's decision after the database IOPS raise to 12k): the backfill's native-slot
// scans were among the top statements by total time, so concurrency is halved to 8 at the original page sizes. A full
// run takes longer; it binds a drain only through the refresh path (FULL may be up to 24 h old).
export const BACKLOG_OPS_BACKFILL_PAGING = ["--page-size", "5000", "--row-page-size", "20000", "--statement-timeout-ms", "600000", "--concurrency", "8"] as const;
/**
 * The export's per-statement timeout (its reads over the buffer are as heavy as the backfill's), its page size and
 * concurrency. VERDICT_PAGE's per-stage work (the judged/kept CTEs filter materialized CTEs by
 * stage_token) grows with the page squared, so the whole export grows with the page size: the default 1000-key page
 * ran a scoped export-dry past the 355-min job limit, where 500 keys measured ~5 s a page on production. 250 keys x 16
 * kept the total near an hour; the lower-load 100-key pages below trade run time for database load (measure it on the dry run).
 */
// Lower load (2026-11-16): 100-key pages (per-page work grows with the page squared). The export must still be created
// after the refresh and the drain must start within 3 h of the refresh, so run it right after.
// Concurrency 16 (2026-11-19): --concurrency bounds only the export's object-store work (the per-user fence reads and
// the per-stage sidecar/proof reads and writes, accepted-stage-export.ts mapLimit); its SQL runs on one read-only
// connection, one VERDICT_PAGE at a time, whatever the concurrency. So the page size alone sets database load, and 4
// only serialized S3 round trips: a one-org production export ran past 3 h, mostly waiting on S3. 16 is the tool's
// maximum (MAX_EXPORT_CONCURRENCY).
export const BACKLOG_OPS_EXPORT_PAGING = ["--statement-timeout-ms", "600000", "--page-size", "100", "--concurrency", "16"] as const;

/**
 * The presence tool's per-statement timeout and total transaction budget. Its statements (the counts, the distinct-org
 * check and the two generation marker reads) run one after another in ONE REPEATABLE READ READ ONLY transaction on
 * the owner primary, each in its own savepoint. A statement that hits the 60 s timeout is null (count_timeout); the
 * 180 s budget caps the whole transaction. The final marker read keeps a reserve equal to the statement timeout, so
 * the counts and the distinct-org check share the budget minus the statement timeout (120 s) and the final marker
 * keeps up to 60 s. No statement starts with less than 1 s of its share left, and each statement's timeout is lowered
 * to what is left of its share, so the snapshot is held for about 3 min at most (budget_exhausted, counts null,
 * otherwise). Both flags are passed explicitly.
 */
export const BACKLOG_OPS_PRESENCE_TIMEOUT = ["--statement-timeout-ms", "60000", "--total-budget-ms", "180000"] as const;

/**
 * The catch-up's t (BACKLOG_OPS_REVISIONS_SINCE_MS, the gate's step output, never a dispatch input): an epoch ms, in the
 * past. Required for a catch-up and for a drain-execute with FULL (whose BACKFILL may be one); empty for every other
 * mode. Null when not needed.
 */
export function backlogOpsRevisionsSince(env: Record<string, string | undefined>, d: Pick<BacklogOpsDispatch, "mode" | "fullBackfillRun">, now = Date.now()): number | null {
  const raw = env.BACKLOG_OPS_REVISIONS_SINCE_MS ?? "";
  const wanted = isCatchup(d.mode) || isBridge(d.mode) || isMarker(d.mode) || d.mode === "hold-seal-execute" ||
    ((d.mode === "drain-execute" || d.mode === "authority-execute") && d.fullBackfillRun !== null);
  if (!wanted) {
    if (raw !== "") refuse("backlog_ops_revisions_since");
    return null;
  }
  if (!/^[1-9][0-9]{0,15}$/u.test(raw) || !Number.isSafeInteger(Number(raw)) || Number(raw) >= now) refuse("backlog_ops_revisions_since");
  return Number(raw);
}

/** Fixed argv for the tool: the scope's validated orgs only, never a checkpoint, a start key or an unvalidated value. */
export function backlogOpsToolArgs(d: BacklogOpsDispatch, t: BacklogOpsTarget, scope: BacklogOpsScope = { orgs: null, id: BACKLOG_OPS_SCOPE_ALL },
  revisionsSinceMs: number | null = null, noHeadParity: BacklogOpsNoHeadParity | null = null): { script: string; args: string[]; needsDb: boolean } {
  const tool = toolOf(d.mode), execute = isExecute(d.mode);
  if (execute && BACKLOG_OPS_DRY_ONLY_TOOLS.includes(tool)) refuse(`backlog_ops_${tool}_dry_only`);
  // The per-org tools, and a catch-up taking an authority baseline, need a scoped run.
  if ((BACKLOG_OPS_SCOPED_TOOLS.includes(tool) || d.authorityRun) && scope.orgs === null) refuse("backlog_ops_scope_required");
  // Path B reaches the export only (its orgs on stdin, backlogOpsChildSpawn; its age here).
  if (noHeadParity !== null && tool !== "export") refuse("backlog_ops_no_head_parity_unexpected");
  // A catch-up and a bridge mode need t (backlogOpsRevisionsSince: the FULL cutoff); nothing else passes one.
  if ((isCatchup(d.mode) || isBridge(d.mode)) !== (revisionsSinceMs !== null) || (revisionsSinceMs !== null && (!Number.isSafeInteger(revisionsSinceMs) || revisionsSinceMs < 1)))
    refuse("backlog_ops_revisions_since");
  if (tool === "presence") {
    // Dry only, no bucket, region or key (it reads no S3): the per-statement timeout, the budget and the scope's orgs.
    if (execute) refuse("backlog_ops_presence_dry_only");
    return { script: "scripts/session-presence.ts", args: ["--dry-run", ...BACKLOG_OPS_PRESENCE_TIMEOUT, ...(scope.orgs ?? []).flatMap((o) => ["--org", o])], needsDb: true };
  }
  if (tool === "reshard") {
    // reshard-dry: the revision reshard's --verify, per scope org, read-only (scripts/backlog-authority.ts).
    return { script: "scripts/backlog-authority.ts", args: ["reshard-verify", "--bucket", t.bucket, "--region", t.region, ...scope.orgs!.flatMap((o) => ["--org", o])], needsDb: false };
  }
  if (tool === "bridge") {
    // bridge-dry: each org's erasure-window proof at the FULL cutoff t, read by the tool itself; bridge-execute: held.
    return { script: "scripts/backlog-authority.ts", args: [execute ? "bridge-release" : "bridge-window", "--cutoff-ms", String(revisionsSinceMs),
      "--full-run", d.fullBackfillRun!, ...scope.orgs!.flatMap((o) => ["--org", o])], needsDb: true };
  }
  if (tool === "hold") {
    // hold-dry (read only) and the held executes (scripts/backlog-hold.ts); the approved orgs and the FULL on stdin.
    const step = d.mode === "hold-dry" ? "dry" : d.mode === "hold-install-execute" ? "install" : d.mode === "hold-seal-execute" ? "seal" : "rollback";
    return { script: "scripts/backlog-hold.ts", args: [step, "--sha", d.sha, ...(d.dryRun ? ["--dry-run", d.dryRun] : []), ...scope.orgs!.flatMap((o) => ["--org", o])], needsDb: true };
  }
  if (tool === "marker") {
    // marker-dry: readiness and the wave id; marker-execute (held): the markers under the 062320 hold (scripts/backlog-marker.ts).
    return { script: "scripts/backlog-marker.ts", args: [execute ? "execute" : "dry", "--bucket", t.bucket, "--region", t.region,
      ...(execute ? ["--kms-key-arn", t.kmsKeyArn] : []), "--sha", d.sha, "--authority-run", d.authorityRun!, "--backfill-run", d.backfillRun!,
      "--full-run", d.fullBackfillRun!, "--reshard-run", d.reshardRun!, ...scope.orgs!.flatMap((o) => ["--org", o])], needsDb: true };
  }
  if (tool === "authority") {
    // authority-dry: the unbound live evidence; authority-execute: the bound evidence, the sweeper, then the markers.
    return { script: "scripts/backlog-authority.ts", args: execute
      ? ["execute", "--bucket", t.bucket, "--region", t.region, "--kms-key-arn", t.kmsKeyArn, ...scope.orgs!.flatMap((o) => ["--org", o])]
      : ["evidence", ...scope.orgs!.flatMap((o) => ["--org", o])], needsDb: true };
  }
  const script = { backfill: "scripts/upload-fence-backfill.ts", export: "scripts/accepted-stage-export.ts", drain: "scripts/buffered-drain.ts" }[tool as "backfill" | "export" | "drain"];
  const args = [execute ? "--execute" : "--dry-run", "--bucket", t.bucket, "--region", t.region,
    ...(execute ? ["--kms-key-arn", t.kmsKeyArn] : []),
    ...(tool === "backfill" ? BACKLOG_OPS_BACKFILL_PAGING : tool === "export" ? BACKLOG_OPS_EXPORT_PAGING : []),
    // A refresh: the fences and ownership sections only (the tool's closed --section set); a full run passes none (all).
    ...(isRefresh(d.mode) ? ["--section", "fences", "--section", "ownership"] : []),
    // A catch-up: all three sections, the revisions only those updated since t.
    ...(isCatchup(d.mode) ? ["--section", "fences", "--section", "ownership", "--section", "revisions", "--revisions-since-ms", String(revisionsSinceMs)] : []),
    // Opt-in (slots_read=org), default off: the per-org index read of the owned slots.
    ...(tool === "backfill" && d.slotsRead === "org" ? ["--slots-by-org"] : []),
    // Opt-in (slot_pace_ms, with slots_read=org only): the paced org slot read.
    ...(tool === "backfill" && d.slotsRead === "org" && d.slotPaceMs !== null ? ["--slot-pace-ms", String(d.slotPaceMs)] : []),
    ...(tool === "drain" && d.maxObjects !== null ? ["--max-objects", String(d.maxObjects)] : []),
    // A catch-up bound to an authority-dry: the baseline in its input snapshot (the evidence on stdin).
    ...(isCatchup(d.mode) && d.authorityRun ? ["--authority-baseline"] : []),
    ...(scope.orgs ?? []).flatMap((o) => ["--org", o]),
    ...(noHeadParity !== null ? ["--no-head-parity-max-age", String(noHeadParity.maxAgeSeconds)] : [])];
  return { script, args, needsDb: tool !== "drain" };
}

/** The role for a mode: dry modes get the read-only role, executes the writer role. */
export const backlogOpsRole = (d: BacklogOpsDispatch, t: BacklogOpsTarget) => {
  if (isExecute(d.mode) && BACKLOG_OPS_DRY_ONLY_TOOLS.includes(toolOf(d.mode))) refuse(`backlog_ops_${toolOf(d.mode)}_dry_only`);
  return isExecute(d.mode) ? t.executeRoleArn : t.dryRoleArn;
};

/**
 * presence-dry's one session id, from the dry environment's secret SESSION_PRESENCE_SESSION (trimmed as
 * BACKLOG_OPS_ORGS is): a lowercase UUID or a bounded [A-Za-z0-9_-]{8,128} token. Missing or empty refuses
 * backlog_ops_presence_session_missing, any other shape backlog_ops_presence_session_invalid; the value is never in
 * an error. Every other mode must see it empty (the workflow passes it only to presence-dry):
 * backlog_ops_presence_session_unexpected. Returns null for those modes.
 */
export const SESSION_PRESENCE_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
export const SESSION_PRESENCE_TOKEN = /^[A-Za-z0-9_-]{8,128}$/u;
export const isPresenceSession = (v: string) => SESSION_PRESENCE_UUID.test(v) || SESSION_PRESENCE_TOKEN.test(v);
export function backlogOpsPresenceSession(env: Record<string, string | undefined>, d: Pick<BacklogOpsDispatch, "mode">): string | null {
  const raw = (env.SESSION_PRESENCE_SESSION ?? "").trim();
  if (toolOf(d.mode) !== "presence") {
    if (raw !== "") refuse("backlog_ops_presence_session_unexpected");
    return null;
  }
  if (raw === "") refuse("backlog_ops_presence_session_missing");
  if (!isPresenceSession(raw)) refuse("backlog_ops_presence_session_invalid");
  return raw;
}

/**
 * The authority modes' read-only Cloudflare API token (Workers Scripts:Read, Queues:Read on the account), the
 * environment SECRET BACKLOG_OPS_CLOUDFLARE_TOKEN on both backlog-ops environments, passed by the workflow to the tool
 * step for authority modes only and to the tool only on its stdin pipe. Missing refuses
 * backlog_ops_cloudflare_token_missing, a bad shape backlog_ops_cloudflare_token_invalid (never the value); every other
 * mode must see it empty (backlog_ops_cloudflare_token_unexpected). Null for those modes.
 */
export function backlogOpsCloudflareToken(env: Record<string, string | undefined>, d: Pick<BacklogOpsDispatch, "mode">): string | null {
  const raw = (env.BACKLOG_OPS_CLOUDFLARE_TOKEN ?? "").trim();
  if (toolOf(d.mode) !== "authority" && toolOf(d.mode) !== "marker") {
    if (raw !== "") refuse("backlog_ops_cloudflare_token_unexpected");
    return null;
  }
  if (raw === "") refuse("backlog_ops_cloudflare_token_missing");
  if (!/^[A-Za-z0-9_-]{32,256}$/u.test(raw)) refuse("backlog_ops_cloudflare_token_invalid");
  return raw;
}

/**
 * Counts only: every key a lowercase identifier, every leaf a finite number, a boolean, null or a short lowercase
 * code. A string that is not a code (an org id, a key, a URL, a message) refuses the whole receipt.
 */
export const BACKLOG_OPS_RECEIPT_MAX_ARRAY = 4096;
export function countsOnly(value: unknown, depth = 0, allowArrays = false): unknown {
  if (depth > 6) refuse("backlog_ops_receipt_shape");
  if (value === null || typeof value === "boolean") return value;
  if (typeof value === "number") { if (!Number.isFinite(value)) refuse("backlog_ops_receipt_shape"); return value; }
  if (typeof value === "string") { if (!CODE.test(value)) refuse("backlog_ops_receipt_string"); return value; }
  // A list: only in an authority receipt (backlogOpsAuthorityReceipt: per-org verdicts, refusal codes, snapshot xids),
  // bounded, each element checked alike. Every other receipt refuses a list, as before.
  if (Array.isArray(value)) {
    if (!allowArrays || value.length > BACKLOG_OPS_RECEIPT_MAX_ARRAY) refuse("backlog_ops_receipt_shape");
    return value.map((v) => countsOnly(v, depth + 1, allowArrays));
  }
  if (typeof value !== "object") refuse("backlog_ops_receipt_shape");
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
    if (!KEY.test(k)) refuse("backlog_ops_receipt_key");
    out[k] = countsOnly(v, depth + 1, allowArrays);
  }
  return out;
}

export interface BacklogOpsReceipt {
  contract: typeof BACKLOG_OPS_RECEIPT_CONTRACT;
  mode: BacklogOpsMode;
  sha: string;
  run: string;
  /** The org scope's id (backlogOpsScope): `all` or `orgs_<32 hex>`; never an org id. */
  scope: string;
  /** The ledger target's id (backlogOpsLedgerId): `none` or `ledger_<32 hex>`; never a bucket name. */
  ledger: string;
  /** Path B's id (backlogOpsNoHeadParity): present only when its allow-list is set; never an org id. */
  no_head_parity?: string;
  /** A catch-up bound to an authority-dry: that run's id (its receipt then carries the baseline). Absent otherwise. */
  authority_run?: string;
  /** The same catch-up: the exact FULL run it followed (authority-execute binds FULL by this id). Absent otherwise. */
  full_run?: string;
  /** A reshard-dry bound to a catch-up: that catch-up's run id (it completed before this run's gate). Absent otherwise. */
  after_run?: string;
  /** A marker mode: the reshard-dry it bound (with authority_run, full_run and after_run, its catch-up). Absent otherwise. */
  reshard_run?: string;
  /**
   * The store-home pair's id (backlogOpsHomeId): always on an authority receipt; on every other receipt of a ledger-id
   * tool (backfill, drain) when the credential step gives it (it always does), so a FULL backfill-execute attests it.
   */
  home?: string;
  exitCode: number;
  summary: unknown;
}

/** Parse the tool's last stdout line into a counts-only receipt (or a refused one). */
export function backlogOpsReceipt(d: BacklogOpsDispatch, run: string, exitCode: number, stdout: string, scope: string = BACKLOG_OPS_SCOPE_ALL,
  ledger: string = BACKLOG_OPS_LEDGER_NONE, noHeadParity: string = BACKLOG_OPS_NO_HEAD_PARITY_NONE, home: string | null = null): BacklogOpsReceipt {
  if (!SCOPE_ID.test(scope)) refuse("backlog_ops_scope");
  const authority = backlogOpsAuthorityReceipt(d);
  if ((authority && home === null) || (home !== null && (!HOME_ID.test(home) || !BACKLOG_OPS_LEDGER_ID_TOOLS.includes(toolOf(d.mode))))) refuse("backlog_ops_home_id");
  if (!LEDGER_ID.test(ledger)) refuse("backlog_ops_ledger_id");
  if (!NHP_ID.test(noHeadParity)) refuse("backlog_ops_no_head_parity_id");
  const lines = stdout.split("\n").map((l) => l.trim()).filter(Boolean);
  let summary: unknown = { refused: "no_summary" };
  const last = lines.at(-1);
  if (last !== undefined) {
    let parsed: unknown;
    try { parsed = JSON.parse(last); } catch { parsed = undefined; }
    summary = parsed === undefined ? { refused: "summary_unparsed" } : countsOnly(parsed, 0, authority);
  }
  // Unset: no field at all, so the receipt is byte-identical to one from before Path B.
  return { contract: BACKLOG_OPS_RECEIPT_CONTRACT, mode: d.mode, sha: d.sha, run, scope, ledger,
    ...(noHeadParity !== BACKLOG_OPS_NO_HEAD_PARITY_NONE ? { no_head_parity: noHeadParity } : {}),
    ...(isCatchup(d.mode) && d.authorityRun ? { authority_run: d.authorityRun, full_run: d.fullBackfillRun! } : {}),
    // A bridge mode: the exact FULL its cutoff came from (a later binding matches the run, not a derived time).
    ...(isBridge(d.mode) ? { full_run: d.fullBackfillRun! } : {}),
    ...(d.mode === "reshard-dry" && d.backfillRun ? { after_run: d.backfillRun } : {}),
    // A marker mode: every run its wave id covers (a marker-execute checks its marker-dry recorded exactly its own).
    ...(isMarker(d.mode) ? { authority_run: d.authorityRun!, full_run: d.fullBackfillRun!, after_run: d.backfillRun!, reshard_run: d.reshardRun! } : {}),
    ...(home !== null ? { home } : {}), exitCode, summary };
}

const obj = (v: unknown): Record<string, any> | null => (v && typeof v === "object" && !Array.isArray(v) ? v as Record<string, any> : null);

/**
 * A bound run's receipt, read from its downloaded artifact. It must name that run, this commit and the expected mode,
 * have exited 0, carry the execute's org scope (a bound backfill or export may also be unscoped; a receipt without a
 * scope is unscoped), and (per tool) pass its readiness check. `exact`: the scope must match exactly, never widened
 * from `all`: the drain's BACKFILL and FULL (an unscoped backfill, e.g. an all-orgs pre-warm, proves nothing about a
 * scoped org's admission stop or freshness, so it must never satisfy a scoped drain).
 */
export type BacklogOpsBindingKind = "dry" | "backfill" | "full" | "export" | "reshard" | "authority";
export function checkBoundReceipt(raw: string, expect: { mode: BacklogOpsMode; sha: string; run: string; scope?: string; kind?: BacklogOpsBindingKind; exact?: boolean; ledger?: string;
  noHeadParity?: string }): void {
  // A presence receipt never feeds an execute (presence binds nothing and nothing binds it).
  if (toolOf(expect.mode) === "presence") refuse("backlog_ops_bound_presence_receipt");
  let r: Record<string, any> | null;
  try { r = obj(JSON.parse(raw)); } catch { r = null; }
  if (!r || r.contract !== BACKLOG_OPS_RECEIPT_CONTRACT || r.mode !== expect.mode || r.sha !== expect.sha ||
      r.run !== expect.run || r.exitCode !== 0)
    refuse(`backlog_ops_bound_${toolOf(expect.mode)}_receipt`);
  const want = expect.scope ?? BACKLOG_OPS_SCOPE_ALL;
  const got = r.scope === undefined ? BACKLOG_OPS_SCOPE_ALL : r.scope;
  if (typeof got !== "string" || !SCOPE_ID.test(got) || !SCOPE_ID.test(want) ||
      (got !== want && (expect.exact === true || !(expect.kind !== "dry" && expect.kind !== undefined && got === BACKLOG_OPS_SCOPE_ALL))))
    refuse(`backlog_ops_bound_${toolOf(expect.mode)}_scope`);
  // The ledger target (only where the caller binds it: a ledger tool binding a ledger tool). A receipt from before the
  // field had no ledger bucket (`none`).
  if (expect.ledger !== undefined) {
    const gotLedger = r.ledger === undefined ? BACKLOG_OPS_LEDGER_NONE : r.ledger;
    if (typeof gotLedger !== "string" || !LEDGER_ID.test(gotLedger) || !LEDGER_ID.test(expect.ledger) || gotLedger !== expect.ledger)
      refuse(`backlog_ops_bound_${toolOf(expect.mode)}_ledger`);
  }
  // Path B's id (only where the caller binds it: an export-execute's DRY, a drain-execute's EXPORT). A receipt without
  // the field had the allow-list unset (`none`).
  if (expect.noHeadParity !== undefined) {
    const gotNhp = r.no_head_parity === undefined ? BACKLOG_OPS_NO_HEAD_PARITY_NONE : r.no_head_parity;
    if (typeof gotNhp !== "string" || !NHP_ID.test(gotNhp) || !NHP_ID.test(expect.noHeadParity) || gotNhp !== expect.noHeadParity)
      refuse(`backlog_ops_bound_${expect.kind ?? "dry"}_no_head_parity`);
  }
  const s = obj(r.summary);
  if (!s || s.refused !== undefined) refuse(`backlog_ops_bound_${toolOf(expect.mode)}_refused`);
  const wantMode = isExecute(expect.mode) ? "execute" : "dry_run";
  if (s.mode !== wantMode) refuse(`backlog_ops_bound_${toolOf(expect.mode)}_receipt`);
  const tool = toolOf(expect.mode);
  if (tool === "backfill") {
    const sec = obj(s.sections);
    if (!sec) refuse("backlog_ops_backfill_incomplete");
    if (isRefresh(expect.mode)) {
      // A refresh (receipt mode backfill-refresh-*, checked above): fences and ownership complete, revisions NOT RUN.
      if (sec.fences !== "complete" || sec.ownership !== "complete") refuse("backlog_ops_backfill_incomplete");
      if (sec.revisions !== "not_run") refuse("backlog_ops_backfill_sections");
    } else if (["fences", "ownership", "revisions"].some((k) => sec[k] !== "complete")) {
      // Every other backfill receipt: all three complete.
      refuse("backlog_ops_backfill_incomplete");
    }
    if (isCatchup(expect.mode)) {
      // A catch-up: failed 0 in every section, and its t recorded (checked against the gate's by the credential step).
      if (["fences", "ownership", "revisions"].some((k) => obj(s[k])?.failed !== 0)) refuse("backlog_ops_backfill_incomplete");
      if (!Number.isSafeInteger(s.revisions_since_ms) || s.revisions_since_ms < 1) refuse("backlog_ops_backfill_since");
    } else if (s.revisions_since_ms !== undefined) {
      // Every other backfill read every revision: a since-filtered summary never stands in for one.
      refuse("backlog_ops_backfill_since");
    }
  }
  if (tool === "export") {
    const orgs = obj(s.orgs);
    if (s.failed !== 0 || s.fence_late !== 0 || !orgs || orgs.incomplete !== 0) refuse("backlog_ops_export_not_ready");
  }
  if (tool === "drain") {
    if (typeof s.listed !== "number") refuse("backlog_ops_bound_drain_receipt");
  }
  // reshard and authority receipts: their per-org content is checked by backlog-authority.ts (checkReshardReceipt,
  // checkAuthorityDryReceipt), which the credential step runs right after this.
}

/**
 * Which earlier runs an execute (or a catch-up dry) binds, with the mode each must have been. An execute's DRY is its
 * own dry mode (a refresh's is backfill-refresh-dry, a catch-up's backfill-catchup-dry). The drain's BACKFILL is a
 * backfill-refresh-execute or (`altMode`) a backfill-catchup-execute exactly when FULL is given; FULL, like
 * export-execute's BACKFILL, is always a full backfill-execute.
 */
export interface BacklogOpsBinding { run: string; mode: BacklogOpsMode; kind: BacklogOpsBindingKind; altMode?: BacklogOpsMode }
export function backlogOpsBindings(d: BacklogOpsDispatch): BacklogOpsBinding[] {
  const tool = toolOf(d.mode), out: BacklogOpsBinding[] = [];
  if (tool === "reshard") {
    // Its optional BACKFILL: the catch-up execute it verifies after (completed when this run's gate checked it).
    if (d.dryRun || d.exportRun || d.fullBackfillRun || d.reshardRun || d.authorityRun) refuse("backlog_ops_bindings");
    return d.backfillRun ? [{ run: d.backfillRun, mode: "backfill-catchup-execute", kind: "backfill" }] : out;
  }
  if (tool === "presence" || d.mode === "authority-dry") {
    if (d.dryRun || d.backfillRun || d.exportRun || d.fullBackfillRun || d.reshardRun || d.authorityRun) refuse("backlog_ops_bindings");
    return out;
  }
  if (isBridge(d.mode)) {
    // FULL (the cutoff's run) always; bridge-execute also its bridge-dry.
    if (!d.fullBackfillRun || d.backfillRun || d.exportRun || d.reshardRun || d.authorityRun || (d.mode === "bridge-dry") !== (d.dryRun === null)) refuse("backlog_ops_bindings");
    return [...(d.dryRun ? [{ run: d.dryRun, mode: "bridge-dry" as BacklogOpsMode, kind: "dry" as const }] : []),
      { run: d.fullBackfillRun, mode: "backfill-execute", kind: "full" }];
  }
  if (isHold(d.mode)) {
    // Every hold execute binds a hold-dry (DRY); the seal also the FULL it seals and binds.
    if (d.backfillRun || d.exportRun || d.reshardRun || d.authorityRun || (d.mode === "hold-dry") !== (d.dryRun === null) ||
        (d.mode === "hold-seal-execute") !== (d.fullBackfillRun !== null)) refuse("backlog_ops_bindings");
    return [...(d.dryRun ? [{ run: d.dryRun, mode: "hold-dry" as BacklogOpsMode, kind: "dry" as const }] : []),
      ...(d.fullBackfillRun ? [{ run: d.fullBackfillRun, mode: "backfill-execute" as BacklogOpsMode, kind: "full" as const }] : [])];
  }
  if (isMarker(d.mode)) {
    // AUTHORITY the authority-dry the catch-up's baseline names, BACKFILL that catch-up, FULL its FULL, RESHARD the
    // reshard verify after it; marker-execute also its marker-dry (DRY).
    if (!d.authorityRun || !d.backfillRun || !d.fullBackfillRun || !d.reshardRun || d.exportRun || (d.mode === "marker-dry") !== (d.dryRun === null))
      refuse("backlog_ops_bindings");
    return [...(d.dryRun ? [{ run: d.dryRun, mode: "marker-dry" as BacklogOpsMode, kind: "dry" as const }] : []),
      { run: d.authorityRun, mode: "authority-dry", kind: "authority" }, { run: d.backfillRun, mode: "backfill-catchup-execute", kind: "backfill" },
      { run: d.fullBackfillRun, mode: "backfill-execute", kind: "full" }, { run: d.reshardRun, mode: "reshard-dry", kind: "reshard" }];
  }
  if (d.mode === "authority-execute") {
    // DRY the authority-dry (unbound evidence), BACKFILL the catch-up that ran the baseline, FULL the catch-up's FULL,
    // RESHARD the reshard verify.
    if (!d.dryRun || !d.backfillRun || !d.fullBackfillRun || !d.reshardRun || d.exportRun || d.authorityRun) refuse("backlog_ops_bindings");
    return [{ run: d.dryRun, mode: "authority-dry", kind: "dry" }, { run: d.backfillRun, mode: "backfill-catchup-execute", kind: "backfill" },
      { run: d.fullBackfillRun, mode: "backfill-execute", kind: "full" }, { run: d.reshardRun, mode: "reshard-dry", kind: "reshard" }];
  }
  if (d.reshardRun || (d.authorityRun && !isCatchup(d.mode))) refuse("backlog_ops_bindings");
  // Re-checked (backlogOpsDispatch already refuses it), so a hand-built dispatch cannot bind FULL anywhere else.
  if (isCatchup(d.mode) ? !d.fullBackfillRun : d.fullBackfillRun && (d.mode !== "drain-execute" || !d.backfillRun)) refuse("backlog_ops_full_backfill_run");
  if (d.dryRun) out.push({ run: d.dryRun, mode: dryModeOf(d.mode), kind: "dry" });
  if (d.backfillRun) out.push(d.fullBackfillRun
    ? { run: d.backfillRun, mode: "backfill-refresh-execute", altMode: "backfill-catchup-execute", kind: "backfill" }
    : { run: d.backfillRun, mode: "backfill-execute", kind: "backfill" });
  if (d.fullBackfillRun) out.push({ run: d.fullBackfillRun, mode: "backfill-execute", kind: "full" });
  if (d.exportRun) out.push({ run: d.exportRun, mode: "export-execute", kind: "export" });
  if (d.authorityRun) out.push({ run: d.authorityRun, mode: "authority-dry", kind: "authority" });
  return out;
}

/**
 * The input fingerprints of FULL and of the refresh (the drain's BACKFILL): every field present in both as a
 * non-negative safe integer, and equal. Anything else (a field missing, null or not a safe integer, a receipt without
 * a fingerprint at all, or any difference) refuses backlog_ops_backfill_stale: a revision, ownership or owned-slot row
 * changed after the full run began, which the refresh did not re-read, or it cannot be shown that none did. Both
 * fingerprints come from the same database and the same commit, under 24 h apart (hashtext is not a cross-version
 * contract). Called after checkBoundReceipt has accepted both receipts; also refuses unless both have one scope.
 */
export function checkRefreshFingerprint(fullRaw: string, refreshRaw: string): void {
  const read = (raw: string) => {
    let r: Record<string, any> | null;
    try { r = obj(JSON.parse(raw)); } catch { r = null; }
    const f = obj(obj(r?.summary)?.fingerprint);
    if (!r || !f || !BACKLOG_OPS_FINGERPRINT_FIELDS.every((k) => Number.isSafeInteger(f[k]) && f[k] >= 0))
      refuse("backlog_ops_backfill_stale");
    return { scope: r.scope === undefined ? BACKLOG_OPS_SCOPE_ALL : r.scope, f };
  };
  const full = read(fullRaw), refresh = read(refreshRaw);
  // Fingerprints are comparable only over the same scope.
  if (full.scope !== refresh.scope) refuse("backlog_ops_bound_full_scope");
  if (BACKLOG_OPS_FINGERPRINT_FIELDS.some((k) => full.f[k] !== refresh.f[k])) refuse("backlog_ops_backfill_stale");
}

/**
 * How old a bound run may be (the gate's freshness rule): the drain's, authority-execute's and a marker's BACKFILL
 * BACKLOG_OPS_FRESH_BACKFILL_MS, everything else BACKLOG_OPS_BINDING_MAX_AGE_MS. Also applied by the credential step
 * to a per-org slice CARRIED from an earlier run (backlog-wave.ts), measured from when that run was created.
 */
export function backlogOpsBindingMaxAgeMs(mode: BacklogOpsMode, kind: BacklogOpsBindingKind): number {
  return kind === "backfill" && (mode === "drain-execute" || mode === "authority-execute" || isMarker(mode)) ? BACKLOG_OPS_FRESH_BACKFILL_MS : BACKLOG_OPS_BINDING_MAX_AGE_MS;
}
/** A per-org retry's previous run must be this recent (it is retried promptly, and its carried orgs age with it). */
export const BACKLOG_OPS_PREVIOUS_MAX_AGE_MS = BACKLOG_OPS_FRESH_BACKFILL_MS;

/** The run-name the workflow gives every dispatch: the gate reads a bound run's mode back from it. */
export const backlogOpsRunName = (mode: string, sha: string) => `backlog ops ${mode} ${sha}`;

/** AssumeRoleWithWebIdentity (unsigned STS call) response: the three credential fields, or a refusal. */
export function parseStsCredentials(xml: string): { accessKeyId: string; secretAccessKey: string; sessionToken: string; expiration: string } {
  const pick = (tag: string) => {
    const m = new RegExp(`<${tag}>([^<]{1,4096})</${tag}>`, "u").exec(xml);
    return m ? m[1]! : "";
  };
  const accessKeyId = pick("AccessKeyId"), secretAccessKey = pick("SecretAccessKey"), sessionToken = pick("SessionToken"), expiration = pick("Expiration");
  if (!/^ASIA[A-Z0-9]{12,124}$/u.test(accessKeyId) || !secretAccessKey || !sessionToken || !expiration) refuse("backlog_ops_sts_response");
  return { accessKeyId, secretAccessKey, sessionToken, expiration };
}

/** The session must be valid now and for at least BACKLOG_OPS_MIN_SESSION_REMAINING_MS more. */
export function checkStsExpiration(expiration: string, now = Date.now()): void {
  const at = Date.parse(expiration);
  if (!/^\d{4}-\d{2}-\d{2}T/u.test(expiration) || !Number.isFinite(at) || at - now < BACKLOG_OPS_MIN_SESSION_REMAINING_MS)
    refuse("backlog_ops_sts_expiration");
}

/** A role session name that names the run, never a person or tenant. */
export const backlogOpsSessionName = (mode: BacklogOpsMode, run: string) =>
  `backlog-ops-${mode}-${run}`.slice(0, 64);
