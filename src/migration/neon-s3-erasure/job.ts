/**
 * `neon-s3-erasure`: the only process that deletes from the transcript
 * bucket, and the only one holding the eraser key. The exporter
 * (`../neon-s3-sync`) is write-only.
 *
 * Within the 48 h erasure SLA it removes, by version id (permanently):
 *  - whole session prefixes of groups Neon no longer has: its own sweep of the
 *    rollups (the exporter's units come only from rollups, so a group with no
 *    rollup has nothing left to keep), every 10 min;
 *  - whole session prefixes of groups a deletion fence covers
 *    (`deleted_upload_sessions`, `session_resets`), every 10 min;
 *  - the exact owner-bound `legacy:tables:v1` machine copy of each fenced
 *    legacy scope, including its live pointer, every 10 min;
 *  - what the exporter queued: objects a rewritten manifest no longer
 *    references, and session prefixes it no longer produces.
 *
 * It reads the privacy authority (read-only except for the narrow durable readiness heartbeat) and the shared watermark, never object content. The
 * authority is Neon today, or Supabase once C3 activated it (`./authority.ts`, S3X_PRIVACY_AUTHORITY).
 * A session prefix is leased while it is erased, so the exporter never reuses
 * a chunk the eraser is deleting.
 */
import type { Queryable } from "../relayhistory-export.ts";
import { orgPrefix } from "../neon-s3-sync/export-session.ts";
import type { LiveFencedGroup } from "../neon-s3-sync/neon-reader.ts";
import { LEGACY_EXPORT_ORIGIN_ID, legacyMachineId } from "../../shared/legacy-export-machine.ts";
import { isErasureAuthority, neonAuthority, type ErasureAuthority, type ErasurePresence, type LegacyFencedGroup } from "./authority.ts";
import { liveObjectPrefix, livePointerPrefix, liveSessionsIn, liveStagedPrefix, liveTargets } from "./live.ts";

/** Clock-skew margin between this host and S3 `LastModified`. */
export const LIVE_SKEW_MS = 2 * 60 * 1000;
/**
 * A staged batch (session ingest) holds several sessions, and its fan-out may
 * still be retrying or waiting for a DLQ redrive (runbooks/session-ingest-rollout.md:
 * redrive within 24 h). When a session of its (workspace, user, origin) is
 * tombstoned, its staged objects older than this are erased; younger ones on a
 * later sweep. The session's own manifests and chunks are erased at once, as before.
 */
export const STAGED_ERASE_AFTER_MS = 48 * 60 * 60 * 1000;

const DOT_SEGMENT = /(^|\/)\.{1,2}(\/|$)/;
import type { SyncState } from "../neon-s3-sync/state.ts";
import { errorCode, type Log } from "../neon-s3-sync/sync.ts";
import { inSessionAnyDate, type EraseCheckpoint, type Eraser, type ListedVersion, type LiveObjectProbe, type PagedEraser, type ReceiptFence, type VersionReader } from "./eraser.ts";
import {
  allowlistDigest, ATTESTATION_MAX_AGE_MS, attestationFresh, attestationProblem, protectionFacts, RECEIPT_KIND, RECEIPT_MAX_AGE_MS, sameItem, sameVersionSet, sealReceipt, sha256Hex as verifySha256, survivorPinsHold,
  verifyStalePrefix, versionSet, type IdentityReceipt, type ItemReceipt, type ProtectionAttestation, type VerificationReceipt, type VerifyReason,
} from "./stale-prefix-verify.ts";
import { terminalInvocations } from "./write-obligation.ts";
import { createHash } from "node:crypto";

/** A short-lived credential refused before signing (../short-lived-credentials.ts): stop the job for a renewal. */
function isCredentialFault(error: unknown): boolean {
  return errorCode(error).startsWith("short_lived_");
}

/** Per-item budget for one bounded erase step (the shared primitive also caps versions at 10k and pages at 100). */
export const ERASE_STEP_DEADLINE_MS = 60_000;
/** A durable scope for the shared paged eraser's checkpoint: operation + authority + tenant/session (or queued item). */
export function eraseScope(parts: readonly (string | number)[]): string {
  return createHash("sha256").update(JSON.stringify(["e3-erase-scope-v1", ...parts])).digest("hex");
}
const refusal = (code: string) => Object.assign(new Error(code), { code });

export interface ErasureConfig {
  keyRoot: string;
  sweepEveryMs: number;
  idleSleepMs: number;
  progressEveryMs: number;
  /** Logged as a breach when the oldest queued erasure is older than this. */
  slaWarnMs: number;
  /**
   * Whether the existence guard also counts legacy-only evidence (conversation
   * turns, links, outcomes). `S3X_ERASER_RECOGNIZES_LEGACY`. Legacy manifests
   * from `migration:legacy-export` are not in the ordinary exporter queue
   * (the dedicated legacy fence sweep below owns them), so this only changes
   * when a delivery group's ordinary prefixes may be erased.
   */
  recognizeLegacy: boolean;
  /**
   * Dedicated legacy-export fence feed. The process entry keeps this default
   * off until 062030 and the Supabase authority are activated together.
   * Every caller must opt in explicitly after the schema/authority gate.
   */
  legacyExportErasure: boolean;
  /**
   * TEST SEAM ONLY (never set by main.ts): whether exporter obligations may be treated as settled. Production has NO
   * value for it, so an exporter prefix or object is never marked erased (every genesis is unattested; there is no
   * unlock until a separately reviewed recovery contract exists). Tests of the completion path opt in explicitly.
   */
  obligationsSettled?: () => boolean;
}

export const DEFAULT_ERASURE_CONFIG: ErasureConfig = {
  keyRoot: "96bulls",
  sweepEveryMs: 10 * 60 * 1000,
  idleSleepMs: 30 * 1000,
  progressEveryMs: 60 * 1000,
  slaWarnMs: 24 * 60 * 60 * 1000,
  recognizeLegacy: false,
  legacyExportErasure: false,
};

const SESSION_PREFIX = /^[^/]+\/[^/]+\/v1\/ws=[^/]+\/m=[^/]+\/h=[^/]+\/d=\d{4}\/\d{2}\/\d{2}\/s=[^/]+\/$/;
const LEASE_HOLDER = `eraser:${process.pid}`;
const SESSION_PREFIX_PARTS = /^[^/]+\/[^/]+\/v1\/ws=([^/]+)\/m=([^/]+)\/h=([^/]+)\/d=(\d{4}\/\d{2}\/\d{2})\/s=([^/]+)\/$/;

/** A session prefix of the org's own tenant prefix, split into its identity segments and date (else undefined). */
export function sessionPrefixParts(keyRoot: string, orgId: string, prefix: string): { ws: string; m: string; h: string; d: string; s: string } | undefined {
  let root: string;
  try {
    root = `${orgPrefix(keyRoot, orgId)}/v1/`;
  } catch {
    return undefined;
  }
  if (!prefix.startsWith(root) || DOT_SEGMENT.test(prefix) || !SESSION_PREFIX.test(prefix)) return undefined;
  const m = SESSION_PREFIX_PARTS.exec(prefix);
  return m ? { ws: m[1]!, m: m[2]!, h: m[3]!, d: m[4]!, s: m[5]! } : undefined;
}

/**
 * The session identity of a stale-prefix queue entry, as the `--only-identity` allowlist names it:
 * sha256_hex(utf8("<org>/<ws>/<m>/<h>/<s>")) over the org id and the raw ws, m, h and s segment values of
 * `sessionPrefixParts` (the `d=` date dropped, so every date-moved prefix of one session shares one identity).
 * `key` must be a session prefix (`<root>/<org>/v1/ws=../m=../h=../d=YYYY/MM/DD/s=../`) of the org's own tenant
 * prefix; anything else is undefined. Operators compute the same value offline (runbooks/neon-s3-sync.md).
 */
export function staleprefixIdentityHash(keyRoot: string, orgId: string, key: string): string | undefined {
  const p = sessionPrefixParts(keyRoot, orgId, key);
  return p ? createHash("sha256").update(`${orgId}/${p.ws}/${p.m}/${p.h}/${p.s}`, "utf8").digest("hex") : undefined;
}

/** Most identities one stale-prefix pass may name. */
export const STALE_PREFIX_ALLOWLIST_MAX = 100;
const IDENTITY_HASH = /^[0-9a-f]{64}$/;

/** Logs name a key only by its hash (public-repo / tenant hygiene). */
const keyHash = (key: string) => createHash("sha256").update(key).digest("hex");

type QueuedItem = { key: string; kind: "object" | "prefix"; org_id: string; reason: string; enqueued_at: number };
type LeasedItemResult = {
  outcome: "deferred" | "busy" | "cancelled" | "skipped" | "erased" | "unresolved" | "not_erased" | "done_refused" | "failed" | "refused";
  /** The receipt fence's report (stale-prefix execute only). */
  fence?: FenceReport;
  /** Added to `drain`'s return value (an erase was attempted, or a failure recorded). */
  handled: number;
  deleted: number;
  skip?: string;
  status?: PagedOutcome;
  error?: string;
};

/** Why the stale-prefix-only mode leaves an entry queued (for the full job). */
export type StalePrefixSkip =
  | "shape_invalid" | "current_key" | "not_date_move" | "no_live_sibling" | "sibling_export_failed"
  | "sibling_manifest_missing" | "sibling_check_failed" | "lease_busy" | "state_busy" | "cancelled"
  | "not_in_identity_allowlist" | "identity_unresolved";

export interface StalePrefixResult {
  ok: boolean;
  execute: boolean;
  /** Queued entries with kind 'prefix' and reason 'stale_prefix'; `due` of them are past their backoff. */
  selected: number;
  due: number;
  backingOff: number;
  /** Every other queued entry: never read further, never changed. */
  otherEntries: number;
  /** Execute: erased. Dry: would pass the guard (store check only when `siblingChecked`). */
  eligible: number;
  erased: number;
  versionsDeleted: number;
  skipped: Partial<Record<StalePrefixSkip, number>>;
  siblingChecked: boolean;
  /** Unexpired leases held by another eraser process at the start. Execute refuses to start when any exist. */
  otherEraserLeases: number;
  /** Identities named by `onlyIdentities`. */
  allowlistSize: number;
  /** Allowlisted identities with at least one queued stale-prefix entry (`selected`, backing-off entries included). */
  allowlistMatchedIdentities: number;
  /** Allowlisted identities with no queued stale-prefix entry. */
  allowlistUnmatched: number;
  /**
   * Distinct allowlisted identities with at least one entry eligible THIS pass (due and passing every guard; with
   * `execute`, erased). The operator gate before an execute: equal to `allowlistSize`.
   */
  allowlistEligibleIdentities: number;
  /** Set when execute refused to start (nothing erased, nothing written). */
  refused?: string;
  /** Execute: the receipt fence's report for every item that reached it (hashes only). */
  fenced?: FenceReport[];
  stoppedBy?: string;
}

export interface StalePrefixPassOptions {
  execute: boolean;
  max?: number;
  probe?: LiveObjectProbe;
  onlyIdentities: readonly string[];
  /** Execute: the verified receipt (stale-prefix-verify.ts parseReceipt), its store fence and the bucket binding. */
  receipt?: VerificationReceipt;
  fence?: ReceiptFence;
  bucketSha256?: string;
  /** Execute: the operator's protection attestation (required; stale-prefix-verify.ts ProtectionAttestation). */
  attestation?: ProtectionAttestation;
  /** Execute: the read-only reader for the mandatory inline re-verification (GetObjectVersion). */
  reader?: VersionReader;
  /** Execute: this run's own target (state-file identity, verifier commit); the receipt's must equal it. */
  target?: { stateIdentity: string; verifierCommit: string };
  /** Execute: the owner-approved allowlist digest (allowlistDigest) and planned item count, from the decision entry. */
  approvedAllowlistSha256?: string;
  /** Planned old PREFIXES (the dry run's eligible count, the unit --max counts). */
  approvedMax?: number;
  /** IDENTITIES: the allowlist size. */
  approvedIdentityCount?: number;
  /** Multi-prefix mode ONLY (its own owner approval): planned prefixes when they exceed the identity count. */
  approvedPrefixCount?: number;
}

export type VersionOutcome = "deleted" | "not_attempted" | "absent_observed" | "present_after_error" | "unknown";

/**
 * One item's receipt-fence outcome. `refused_before_delete`: zero deletes issued, the item untouched (no state write).
 * `partial`: some receipt versions deleted, then stopped (`reason`), with exactly which versions were deleted, which
 * failed (a DELETE whose outcome is unknown) and which were not attempted. `completed`: every receipt version deleted,
 * the survivor re-verified and the old prefix proven empty. Keys appear only as sha256.
 */
export interface FenceReport {
  identity_hash: string;
  old_prefix_sha256: string;
  outcome: "refused_before_delete" | "partial" | "completed";
  reason?: string;
  deleted: number;
  /**
   * Per version: deleted (acknowledged); not_attempted (never sent); after a DELETE that errored (it may still have
   * applied) the exact relist's finding: absent_observed (absent, not attributed to our delete), present_after_error,
   * or unknown (the relist failed). The batch stops at any of the last three.
   */
  versions: { key_sha256: string; version_id: string; outcome: VersionOutcome; error_code?: string }[];
}

/**
 * The stale-prefix receipt fence's run context. What each protection rests on:
 *  PROVEN BY CODE (checked before the first delete, and before EVERY delete):
 *   - the old prefix's exact version set equals the receipt's (strict relist; drift refuses, zero deletes);
 *   - each survivor pin (manifest + every referenced chunk) is the CURRENT version with its pinned versionId, not a
 *     delete marker (a pin made NONCURRENT by an overwrite counts as drift: the bucket's lifecycle expires
 *     noncurrent versions after about a day, so only a current pin is protected by the lifecycle facts below);
 *   - this process holds the old-prefix lease, the survivor-prefix lease (the exporter takes the same per-prefix
 *     lease before it writes a session, so it cannot rewrite the survivor while it is held) and the eraser-wide run
 *     lease, each renewed; no other eraser holds an unexpired lease; the OFF-mode interlock and authority readiness;
 *   - the lifecycle configuration and versioning status, whenever the key can read them (GetBucketLifecycle /
 *     GetBucketVersioning), must show no current-version Expiration/Transition and versioning Enabled.
 *  BOUND TO THE OWNER-APPROVED ENTRY: the receipt and attestation files by their exact sha256 (main.ts
 *   boundOperatorFile), the allowlist digest and the planned item count (#receiptGate), the receipt's target
 *   (bucket, key root, state-file identity, verifier commit, produced within 6 h), and the MANDATORY inline
 *   re-verification under the leases (#fencedBatch phase C), which must reproduce the receipt exactly.
 *  RESTS ON EXTERNAL FACTS (the operator attestation, refused when missing, malformed, stale before ANY delete, or
 *   contradicted by what the code could read):
 *   - every principal that can PutObject / DeleteObject / DeleteObjectVersion under the key root or change
 *     lifecycle, versioning or the bucket policy, each blocked for the interval by a named PREVENTIVE control (an
 *     alarm is not one). The exporter is write-only and never loads the eraser key; its same-host lease is honoured,
 *     but cross-host writers, lifecycle actions (which take no lease) and every other actor are excluded ONLY by
 *     those attested controls: the per-delete checks narrow the window between check and delete, they cannot close
 *     it. Execute therefore stays refused until such controls exist and are attested;
 *   - the lifecycle facts themselves when the configuration cannot be read.
 */
interface FenceContext { receipt: VerificationReceipt; fence: ReceiptFence; attestation: ProtectionAttestation; probe: LiveObjectProbe }

/** The fence's clock: the hard deadline, the ACTUAL listing pages per prefix, the remaining work, the first gate refusal. */
interface FenceClock {
  protectedUntil: number;
  pages: Map<string, number>;
  remaining: { entry: ItemReceipt; oldPrefix: string; survivorPrefix: string; deletesLeft: number; first: boolean }[];
  maxLatencyMs: number;
  failure?: string;
}
/** Most pages one fence listing may take (strictInventory's own ceiling is 100); beyond it the listing aborts. */
export const FENCE_LIST_MAX_PAGES = 20;
/** Most milliseconds between a completed fence check and issuing its delete. */
export const CHECK_TO_DELETE_MAX_MS = 2_000;
/** Lease time kept beyond one delete's worst case at issuance. */
export const ISSUE_MARGIN_MS = 5_000;

/** The eraser-wide stale-prefix run lease: a leases row no session prefix can equal (object keys never start with ':'). */
export const STALE_RUN_LEASE = ":eraser-run:stale-prefixes-only";

/** The session prefix a queued key belongs to, if the key has this layout under the org's prefix. */
export function sessionPrefixOf(keyRoot: string, orgId: string, key: string): string | undefined {
  const root = `${orgPrefix(keyRoot, orgId)}/v1/`;
  if (!key.startsWith(root) || DOT_SEGMENT.test(key)) return undefined;
  const m = /^(.*?\/s=[^/]+\/)/.exec(key);
  return m && SESSION_PREFIX.test(m[1]!) ? m[1] : undefined;
}

/** One obligation-gated paged erasure's outcome (see ErasureJob#erasePaged). */
export type PagedOutcome = "incomplete" | "erased" | "unverified" | "unresolved" | "refused";

export class ErasureJob {
  readonly counters = { doneRefused: 0, unresolved: 0, reservationsRefused: 0, swept: 0, prefixesErased: 0, objectsErased: 0, versionsDeleted: 0, failed: 0, refused: 0, deferred: 0, fenceSweepFailed: 0, liveVersionsDeleted: 0, liveFenceFailed: 0, legacyVersionsDeleted: 0, legacyFenceFailed: 0, readinessHeartbeatFailed: 0, resumable: 0, unverified: 0 };
  #lastSweep = 0;
  #lastProgress = 0;
  readonly authority: ErasureAuthority;

  constructor(
    /** A Neon connection (today's authority) or an explicit `ErasureAuthority`. */
    db: Queryable | ErasureAuthority,
    readonly state: SyncState,
    readonly eraser: Eraser & PagedEraser,
    readonly log: Log,
    readonly cfg: ErasureConfig = DEFAULT_ERASURE_CONFIG,
    readonly now: () => number = Date.now,
    readonly sleep: (ms: number) => Promise<void> = (ms) => new Promise((r) => setTimeout(r, ms)),
    readonly stopped: () => boolean = () => false,
  ) {
    this.authority = isErasureAuthority(db) ? db : neonAuthority(db);
  }

  /**
   * Every legacy-fence failure is a privacy page, not merely an informational
   * log. The metric discriminator is the stable log-ingestion contract; the
   * exact error code remains available for the stricter Neon-authority alarm.
   */
  #legacyFenceFailure(message: string, error: unknown) {
    this.counters.legacyFenceFailed += 1;
    this.log({
      message,
      error_code: errorCode(error),
      "@metric": "legacy_fence_sweep_failed_total",
      value: 1,
      legacyFenceFailed: this.counters.legacyFenceFailed,
      alarm_route: "on-call",
    });
  }

  /**
   * One bounded, resumable, obligation-gated erasure of `prefix` through the shared strict paged eraser
   * (eraser.ts) and the shared unresolved-write model (write-obligation.ts v3), over SyncState:
   *  - resumes from the durable checkpoint bound to `scope` (a foreign scope restarts from the start);
   *  - `authorize` (the caller's still-fenced / queued / lease / authority re-check) runs before every page's deletes;
   *  - before ANY version is deleted, EVERY doomed version of the page is reserved atomically (compare-and-set on the
   *    prefix's revision, under the lock the exporter's publish takes): a writer change or an owned version refuses
   *    the page (nothing in it deleted), and a publish after the reservation is refused;
   *  - "erased" only after a fresh strict zero inventory AND no unresolved write invocation under the prefix: a
   *    dispatched PUT (unknown outcome) is never resolved by time, a zero inventory or a matching listed version;
   *    an acked one only when its exact version is absent from that complete inventory.
   */
  async #erasePaged(prefix: string, scope: string, select: ((v: ListedVersion) => boolean) | undefined, authorize: () => Promise<void>,
    keyFilter: (key: string) => boolean = () => true, exporterWritten = false, exactKey?: string,
    lease?: { prefix: string; holder: string; gen: string }): Promise<{ status: PagedOutcome; deleted: number; remaining: number; rev: number; proofReason?: string }> {
    const exact = exactKey !== undefined ? { exactKey } : {};
    // S3X_OBLIGATIONS off: the SAME strict canonical paged eraser (no permissive fallback), without the obligation
    // protocol (no reservations, invocations, retirement or genesis gate): done = a fresh strict zero inventory.
    const ob = this.state.obligations;
    if (!ob) {
      // The cross-process mode interlock, before ANY external effect and before every page's deletes.
      const inner = authorize;
      authorize = async () => {
        this.state.assertModeAllowed();
        await inner();
        // ... and AGAIN after the awaited authorization, immediately before this page's deletes.
        this.state.assertModeAllowed();
      };
      this.state.assertModeAllowed();
    }
    let rev = ob ? this.state.obligationRev(prefix) : 0;
    let lost = false;
    const budget = () => ({ deadlineAt: this.now() + ERASE_STEP_DEADLINE_MS });
    const beforeDelete = async (doomed: readonly ListedVersion[]) => {
      if (!doomed.length || !ob) return;
      const r = this.state.reserveErase(prefix, rev, scope, doomed.map((v) => ({ key: v.key, versionId: v.versionId })));
      if (!r.ok) {
        lost = true;
        throw refusal("erasure_reservation_refused");
      }
      rev = r.rev;
    };
    // Legacy migration: a checkpoint persisted under this scope in the OTHER mode (directory vs exact-key, e.g. written
    // before the exact-key wiring) is not resumable; restart the traversal from the start instead of being refused.
    // Progress only: invocations, reservations and the terminal / erased history are untouched.
    let checkpoint = this.state.eraseCheckpoint(scope) as (EraseCheckpoint & { exactKey?: string }) | null;
    if (checkpoint && (checkpoint.scope !== scope || checkpoint.prefix !== prefix || (checkpoint.exactKey ?? null) !== (exactKey ?? null))) {
      this.state.clearEraseCheckpoint(scope);
      checkpoint = null;
    }
    const run = (cp: EraseCheckpoint | null) => this.eraser.eraseStep(prefix, {
      scope, checkpoint: cp, ...(select ? { select } : {}), authorize, beforeDelete, budget: budget(), now: this.now, ...exact,
    });
    let step;
    try {
      try {
        step = await run(checkpoint);
      } catch (error) {
        // A stale / foreign stored checkpoint (e.g. another bucket) is ignored and the traversal restarted, never a stall.
        if (!checkpoint || errorCode(error) !== "erasure_checkpoint_scope") throw error;
        this.state.clearEraseCheckpoint(scope);
        step = await run(null);
      }
    } catch (error) {
      if (lost) {
        this.counters.reservationsRefused += 1;
        return { status: "refused", deleted: 0, remaining: -1, rev };
      }
      throw error;
    }
    if (step.status === "incomplete") {
      this.state.setEraseCheckpoint(scope, step.checkpoint, this.now());
      this.counters.resumable += 1;
      return { status: "incomplete", deleted: step.deleted, remaining: -1, rev };
    }
    this.state.clearEraseCheckpoint(scope);
    if (!ob) {
      const proof = await this.eraser.verifyErased(prefix, { ...(select ? { select } : {}), authorize, budget: budget(), now: this.now, ...exact });
      if (proof.status !== "erased") {
        this.counters.unverified += 1;
        return { status: "unverified", deleted: step.deleted, remaining: -1, rev, proofReason: proof.reason };
      }
      return { status: "erased", deleted: step.deleted, remaining: 0, rev };
    }
    // The reservation generation the proof below covers: captured BEFORE the inventory, so every reservation it can
    // retire existed when the inventory was taken (one added later stays reserved).
    const proofSeq = this.state.reservationHighWater(prefix);
    // A fresh strict COMPLETE inventory: which exact versions are present per key.
    // `present`: object versions (the invocation evidence); `listed`: EVERY listed entry INCLUDING delete markers (the
    // absence proof: a marker still listed is present, never "erased").
    const present = new Map<string, Set<string>>();
    const listed = new Map<string, Set<string>>();
    const proof = await this.eraser.verifyErased(prefix, {
      select: (v) => {
        if (keyFilter(v.key)) {
          (listed.get(v.key) ?? listed.set(v.key, new Set()).get(v.key)!).add(v.versionId);
          if (!v.deleteMarker) (present.get(v.key) ?? present.set(v.key, new Set()).get(v.key)!).add(v.versionId);
        }
        return select ? select(v) : true;
      },
      authorize, budget: budget(), now: this.now, ...exact,
    });
    let remaining = 0;
    for (const vs of present.values()) remaining += vs.size;
    if (this.state.obligationRev(prefix) !== rev) return { status: "refused", deleted: step.deleted, remaining, rev };
    // The obligations the subset depends on: those at its keys under the prefix, plus prefix-level ones covering the
    // prefix itself (an ancestor or equal: legacy imports, nonterminal), deduplicated.
    const seen = new Set<string>();
    const invocations = [...this.state.unresolvedWrites(prefix).filter((i) => keyFilter(i.key)), ...this.state.coveringWrites(prefix)].filter((i) => !seen.has(i.id) && !!seen.add(i.id));
    const owned = new Map<string, Set<string>>();
    for (const o of this.state.publishedUnder(prefix)) (owned.get(o.key) ?? owned.set(o.key, new Set()).get(o.key)!).add(o.versionId);
    const byKey = new Map<string, typeof invocations>();
    for (const i of invocations) (byKey.get(i.key) ?? byKey.set(i.key, []).get(i.key)!).push(i);
    const done: string[] = [];
    for (const [key, list] of byKey)
      done.push(...terminalInvocations(list, owned.get(key) ?? new Set(), proof.status === "erased" ? (present.get(key) ?? new Set()) : null));
    if (done.length) {
      const r = this.state.resolveWrites(prefix, rev, done);
      if (!r.ok) return { status: "refused", deleted: step.deleted, remaining, rev };
      rev = r.rev;
    }
    if (proof.status !== "erased") {
      this.counters.unverified += 1;
      return { status: "unverified", deleted: step.deleted, remaining, rev, proofReason: proof.reason };
    }
    // Retire EXACTLY the reserved versions this complete inventory shows absent (within keyFilter); every other
    // reservation (kept siblings, successors, other subsets) stays.
    // Bounded, resumable retirement of THIS scope's reservations proven absent (SQL-filtered by scope and exact key /
    // prefix; <= 1000 rows per transaction; durable cursor). Terminal completion only once every page is retired; at
    // the step deadline it stays incomplete and resumes from the cursor on the next run.
    const absent = (key: string, versionId: string) => keyFilter(key) && !(listed.get(key)?.has(versionId) ?? false);
    // The workset identity this retirement (and its durable cursor) is bound to: prefix, exact key, the reservation
    // generation and the COMPLETE listing it was proven against. Any change discards a saved cursor (restart).
    const proofId = createHash("sha256").update(JSON.stringify(["retire-v1", prefix, exactKey ?? null, proofSeq,
      [...listed].map(([k, vs]) => [k, [...vs].sort()]).sort((a, b) => (a[0]! < b[0]! ? -1 : a[0]! > b[0]! ? 1 : 0))])).digest("hex");
    const deadline = this.now() + ERASE_STEP_DEADLINE_MS;
    for (;;) {
      // Authority re-checked before EVERY page (async, outside the page's transaction; no network await inside it).
      await authorize();
      const page = this.state.retireReservationsPage(prefix, scope, exactKey, absent, 1000, proofSeq, proofId, rev, lease);
      // A write that landed during the awaited authority check (or since the proof) refuses the page: no done claim;
      // the next run takes a fresh proof, and the new pending effect is kept.
      if (!page.ok) {
        this.counters.reservationsRefused += 1;
        return { status: "refused", deleted: step.deleted, remaining, rev: page.rev };
      }
      rev = page.rev;
      if (page.done) break;
      if (this.now() >= deadline) {
        this.counters.resumable += 1;
        return { status: "incomplete", deleted: step.deleted, remaining, rev };
      }
    }
    // M1 recovery: this scope's reservations for versions the proof still LISTS were never deleted: release them (the
    // next run re-reserves what it deletes), so the exporter's claim / re-export is not wedged behind them.
    for (;;) {
      await authorize();
      const rel = this.state.releasePresentPage(prefix, scope, exactKey, (key, versionId) => keyFilter(key) && (listed.get(key)?.has(versionId) ?? false), proofSeq, lease, rev, 1000, proofId);
      if (!rel.ok) {
        this.counters.reservationsRefused += 1;
        return { status: "refused", deleted: step.deleted, remaining, rev: rel.rev };
      }
      rev = rel.rev;
      if (rel.done) break;
      if (this.now() >= deadline) return { status: "incomplete", deleted: step.deleted, remaining, rev };
    }
    // M1: the proven-absent reservations above are retired EVEN when completion is impossible (an unresolved PUT, or
    // the unsettled production genesis), so a writer's claim / cancel is never wedged behind reservations nobody will
    // retire. Only the done claim is gated here.
    // A prefix the exporter writes is never done while its obligations may be incomplete: an unattested genesis (a
    // lost / recreated state file) may have forgotten outstanding PUTs, so it fails closed.
    if (invocations.length - done.length > 0 || (exporterWritten && !(this.cfg.obligationsSettled?.() ?? false))) {
      this.counters.unresolved += 1;
      return { status: "unresolved", deleted: step.deleted, remaining, rev };
    }
    // Final assertion under the CURRENT proof: no eligible (proof-absent) reservation of this scope remains (bounded,
    // read-only pages within the same deadline). Anything left, or no time to check: incomplete, never done.
    for (let after: [string, string] | null = null, first = true; first || after !== null; first = false) {
      if (this.now() >= deadline) {
        this.counters.resumable += 1;
        return { status: "incomplete", deleted: step.deleted, remaining, rev };
      }
      const pg = this.state.eligibleRemainingPage(prefix, scope, exactKey, absent, proofSeq, after);
      if (pg.eligible > 0) return { status: "incomplete", deleted: step.deleted, remaining, rev };
      after = pg.next;
    }
    return { status: "erased", deleted: step.deleted, remaining, rev };
  }

  /**
   * The same gate for ONE object key (every version at exactly `key` that `select` accepts): listed under the key's
   * directory, reserved before delete, done only after a strict empty inventory of the selected versions and no
   * unresolved PUT at exactly that key. `remaining`: versions still at the key (e.g. successors kept by `select`).
   */
  #eraseKey(key: string, scope: string, select: (v: ListedVersion) => boolean, authorize: () => Promise<void>, exporterWritten = false,
    lease?: { prefix: string; holder: string; gen: string }) {
    const dir = key.slice(0, key.lastIndexOf("/") + 1);
    if (!dir || dir === key) throw refusal("erasure_refused_shape");
    // The shared primitive's exact-key mode: listing and verification cover exactly this key (cut at the first other
    // key), so unrelated siblings never count against the budget.
    return this.#erasePaged(dir, scope, (v) => v.key === key && select(v), authorize, (k) => k === key, exporterWritten, key, lease);
  }

  /** Queue the session prefixes of `keys` (manifest keys) and drop them from the group's watermark. */
  #queueGroup(org: string, session: string, keys: string[], reason: string) {
    this.state.enqueueErasure(
      keys.map((k) => ({ key: k.slice(0, -"manifest.json".length), kind: "prefix" as const, org_id: org, reason })),
      this.now(),
    );
    this.state.forgetKeys(org, session, keys);
  }

  /** With `dryRun`, reports what it would erase; deletes nothing and marks nothing done. */
  async sweep(opts: { dryRun?: boolean } = {}) {
    const startedAt = this.now();
    const dryRun = opts.dryRun ?? false;
    const authority = this.authority;
    authority.beginSweep?.();
    // The counters are process-lifetime totals. Snapshot them so both the
    // readiness decision and summary describe this sweep, including failures
    // caught inside a per-scope lane that do not reject the outer page walk.
    const failuresAtStart = {
      fence: this.counters.fenceSweepFailed,
      live: this.counters.liveFenceFailed,
      legacy: this.counters.legacyFenceFailed,
    };
    try {
      await authority.ready();
    } catch (error) {
      // Not the authority right now: erase nothing at all (a stale fence could destroy a re-enabled session).
      this.counters.fenceSweepFailed += 1;
      this.log({ message: "sweep refused: privacy authority not ready", authority: authority.kind, error_code: errorCode(error) });
      return;
    }
    const waiters = await authority.lockWaiters();
    if (waiters.nonAdvisory > 3 || waiters.total > 25) {
      this.log({ message: "sweep deferred: lock waiters", lock_waiters: waiters.total });
      return;
    }
    let healthy = true;
    // Live (srv_) erasure first and on its own: it reads only tombstones, so
    // a failure or an early return in the rollup and catalog walks below
    // must not hold it back.
    const live = await this.#eraseLiveFenced(dryRun).catch((error) => {
      if (isCredentialFault(error)) throw error; // an expiring eraser key stops the job (exit 75), never a zero count
      healthy = false;
      this.counters.liveFenceFailed += 1;
      this.log({ message: "live fence sweep failed", error_code: errorCode(error) });
      return { groups: 0, versions: 0 };
    });
    // The one-shot legacy exporter writes to a separate user-scoped srv_
    // machine and never enters the ordinary export queue.  Consume its own
    // scope-level fence feed independently of the live-origin feed.
    let legacy: { groups: number; versions: number };
    if (!this.cfg.legacyExportErasure) {
      // A sweep with the legacy feed disabled cannot attest that the exporter
      // is safe to activate, even when every ordinary live erasure succeeded.
      healthy = false;
      legacy = { groups: 0, versions: 0 };
    } else {
      legacy = await this.#eraseLegacyFenced(dryRun).catch((error) => {
        if (isCredentialFault(error)) throw error;
        healthy = false;
        this.#legacyFenceFailure("legacy fence sweep failed", error);
        return { groups: 0, versions: 0 };
      });
    }
    // Neon only: groups whose rollups and catalog rows vanished from Neon. Under the Supabase authority every
    // deletion is a fence row (a reset fences each owned session), so there is no presence sweep.
    let id = Number(this.state.meta("erasure_sweep_done") ?? 0);
    let seen = 0;
    let gone = 0;
    if (authority.presence) {
      const r = await this.#presenceSweep(authority.presence, startedAt, dryRun);
      if (!r) return;
      ({ id, seen, gone } = r);
    }
    let fenced = 0;
    try {
      for (const g of await authority.fencedGroups()) {
        const row = this.state.exported(g.org_id, g.session_id);
        if (row?.keys.length) {
          if (!opts.dryRun) this.#queueGroup(g.org_id, g.session_id, row.keys, "fenced");
          fenced += 1;
        }
      }
    } catch (error) {
      if (isCredentialFault(error)) throw error;
      healthy = false;
      this.counters.fenceSweepFailed += 1;
      this.log({ message: "fence sweep failed", error_code: errorCode(error) });
    }
    const sweepFailures = {
      fence: this.counters.fenceSweepFailed - failuresAtStart.fence,
      live: this.counters.liveFenceFailed - failuresAtStart.live,
      legacy: this.counters.legacyFenceFailed - failuresAtStart.legacy,
    };
    // Per-scope live/legacy failures are deliberately caught so neighbouring
    // scopes can proceed. They still make the sweep partial and must never
    // refresh the exporter's durable readiness heartbeat.
    if (sweepFailures.fence > 0 || sweepFailures.live > 0 || sweepFailures.legacy > 0)
      healthy = false;
    // The exporter's readiness signal is written only after a healthy real
    // Supabase sweep. The eraser-only RPC rechecks active authority and reads
    // the immutable legacy mapping before recording its durable timestamp.
    if (!dryRun && !this.stopped() && authority.kind === "supabase") {
      if (!healthy || !authority.recordSuccessfulSweep) {
        if (!authority.recordSuccessfulSweep) {
          this.counters.readinessHeartbeatFailed += 1;
          this.log({ message: "eraser readiness heartbeat failed", error_code: "eraser_readiness_heartbeat_unavailable" });
        }
      } else {
        try {
          await authority.recordSuccessfulSweep();
        } catch (error) {
          this.counters.readinessHeartbeatFailed += 1;
          this.log({ message: "eraser readiness heartbeat failed", error_code: errorCode(error) });
        }
      }
    }
    this.counters.swept += 1;
    if (!dryRun) this.state.setMeta("erasure_sweep_at", String(this.now()));
    this.log({
      message: dryRun ? "erasure sweep (dry run)" : "erasure sweep",
      authority: authority.kind,
      sweep: id,
      count: seen,
      gone,
      fenced,
      live_fenced: live.groups,
      live_versions: live.versions,
      legacy_fenced: legacy.groups,
      legacy_versions: legacy.versions,
      // Stable per-sweep alarm input as well as the edge metric emitted at
      // failure time. A monitor that consumes only summaries must not turn a
      // loud Neon-authority refusal into a silent successful sweep.
      legacy_fence_failed: sweepFailures.legacy,
      legacyFenceFailed: this.counters.legacyFenceFailed,
    });
  }

  /** The Neon presence sweep. Undefined: stopped, or a broken read (saw nothing); nothing is erased then. */
  async #presenceSweep(presence: ErasurePresence, startedAt: number, dryRun: boolean): Promise<{ id: number; seen: number; gone: number } | undefined> {
    const id = Number(this.state.meta("erasure_sweep_done") ?? 0) + 1;
    let after: { org_id: string; session_id: string } | null = null;
    let seen = 0;
    for (;;) {
      const rows = await presence.activityPage(after);
      // A dry run marks under a sweep id no real sweep uses (and the next real sweep clears).
      this.state.seeForErasure(dryRun ? -id : id, rows);
      seen += rows.length;
      if (rows.length === 0 || this.stopped()) break;
      after = rows.at(-1)!;
    }
    // Catalog sessions count as present too: the exporter exports them.
    let cat: Parameters<ErasurePresence["catalogPage"]>[0] = null;
    for (;;) {
      if (this.stopped()) break;
      const rows = await presence.catalogPage(cat);
      this.state.seeForErasure(dryRun ? -id : id, rows);
      seen += rows.length;
      if (rows.length === 0) break;
      cat = rows.at(-1)!;
    }
    if (this.stopped()) return undefined;
    // A sweep that saw nothing at all is a broken read, not an empty Neon: never erase on it.
    if (seen === 0) {
      this.log({ message: "sweep saw no rollups; nothing erased" });
      return undefined;
    }
    if (!dryRun) this.state.setMeta("erasure_sweep_done", String(id));
    let gone = 0;
    for (const g of this.state.vanishedExports(dryRun ? -id : id, startedAt, !dryRun)) {
      // Confirm on Neon right before erasing: no rollup and no catalog row now
      // (and, with recognizeLegacy, no legacy turn, link or outcome row).
      if (await presence.groupExists(g.org_id, g.session_id, this.cfg.recognizeLegacy)) continue;
      gone += 1;
      if (!dryRun) this.#queueGroup(g.org_id, g.session_id, g.keys, "gone_from_neon");
    }
    return { id, seen, gone };
  }

  /**
   * Tombstoned sessions written by relayhistory-api's live writer (live.ts),
   * erased directly on every sweep, not queued: the exporter never writes
   * or leases `m=srv_` prefixes.
   *
   * Cost: tombstones are paged by primary key (no cap) and grouped by
   * (workspace, user, origin); each group's objects and pointers are listed
   * once (two list calls), and only tombstones with a directory or a pointer
   * left are visited. A tombstone with nothing live (most, and every fully
   * erased one) costs a share of those two calls.
   *
   * Re-enable race: a page can be minutes old by the time a row is reached,
   * and a re-enabled session may already be re-uploading. Each tombstone is
   * re-read right before its erasure, and only versions last modified before
   * that read (less LIVE_SKEW_MS for clock skew) are erased. Anything newer is
   * kept, and so is the pointer while anything is kept, so the next sweep
   * finds the session again.
   */
  // UNQUALIFIED dependency: the live writer (relayhistory-api) and the ingest's staged batches are REMOTE writers
  // outside this local obligation ledger, so their unknown PUT outcomes are never settled here. Their tombstones are
  // re-swept every pass and never retired or reported complete; their own lanes must qualify their interlock.
  async #eraseLiveFenced(dryRun: boolean): Promise<{ groups: number; versions: number }> {
    let groups = 0;
    let versions = 0;
    let after: LiveFencedGroup | null = null;
    for (;;) {
      if (this.stopped()) break;
      let rows: LiveFencedGroup[];
      try {
        rows = await this.authority.fencedLivePage(after);
      } catch (error) {
        this.counters.liveFenceFailed += 1;
        this.log({ message: "live fence sweep failed", error_code: errorCode(error) });
        break;
      }
      if (rows.length === 0) break;
      after = rows.at(-1)!;
      // Per (workspace, user, origin), listed once per page: the session
      // directories that hold any object, and the pointers. A tombstone is
      // visited while either remains, so neither a directory left by a
      // re-upload nor an orphaned pointer is ever skipped for good.
      // A machine prefix with more versions than one strict listing allows (the shared primitive's 10k bound) is
      // "unknown" (null) rather than a failure: its tombstones are then always visited, and the paged eraser below
      // works through it across runs.
      const listed = new Map<string, { dirs: Set<string> | null; pointers: Set<string> | null; staged: string[] | null; stagedDone?: boolean } | null>();
      const bounded = async (prefix: string): Promise<string[] | null> => {
        try {
          return await this.eraser.listKeys(prefix);
        } catch (error) {
          if (errorCode(error) === "erasure_inventory") return null;
          throw error;
        }
      };
      for (const g of rows) {
        if (this.stopped()) break;
        let t;
        let objects: string;
        let pointerRoot: string;
        try {
          t = liveTargets(this.cfg.keyRoot, g);
          objects = liveObjectPrefix(this.cfg.keyRoot, g);
          pointerRoot = livePointerPrefix(this.cfg.keyRoot, g);
        } catch {
          // An id that is not one safe path segment was never written either.
          this.counters.refused += 1;
          continue;
        }
        // A failed listing is remembered for the page (null), so the group's
        // other tombstones wait for the next sweep instead of each retrying it.
        if (listed.has(objects) && listed.get(objects) === null) continue;
        let seen = listed.get(objects);
        try {
          if (!seen) {
            const dirs = await bounded(objects), pointers = await bounded(pointerRoot), staged = await bounded(liveStagedPrefix(this.cfg.keyRoot, g));
            seen = {
              dirs: dirs === null ? null : liveSessionsIn(objects, dirs),
              pointers: pointers === null ? null : new Set(pointers),
              // The session ingest's staged batches: a tombstone is revisited while any remain.
              staged: staged === null ? null : staged.filter((k) => k.endsWith(".staged.ndjson.zst")),
            };
            listed.set(objects, seen);
          }
        } catch (error) {
          if (isCredentialFault(error)) throw error;
          listed.set(objects, null);
          this.counters.liveFenceFailed += 1;
          this.log({ message: "live listing failed", error_code: errorCode(error) });
          continue;
        }
        const dirKey = `${t.base.slice(objects.length).replace(/\/$/, "")}/${t.sessionSegment}`;
        if (seen.dirs !== null && seen.pointers !== null && seen.staged !== null
          && !seen.dirs.has(dirKey) && !seen.pointers.has(t.pointerKey) && seen.staged.length === 0) continue;
        groups += 1;
        if (dryRun) continue;
        try {
          const checkedAt = this.now();
          if (!(await this.authority.stillFenced(g))) continue; // re-enabled since the page was read
          const before = new Date(checkedAt - LIVE_SKEW_MS);
          // The session ingest's staged copies of this (workspace, user, origin)'s batches,
          // once past the fan-out redrive window. Shared by the group's tombstones: once a page.
          const stagedBefore = new Date(checkedAt - STAGED_ERASE_AFTER_MS);
          if (seen.staged !== null) {
            const stagedLeft: string[] = [];
            for (const key of seen.staged) {
              // Per staged object, through the gate (reserve before delete, strict empty, no unresolved PUT).
              const st = await this.#eraseKey(key, eraseScope(["live-staged-object", this.authority.kind, g.org_id, g.workspace_id, g.user_id, g.origin_id, key]),
                (v) => v.lastModified !== undefined && v.lastModified < stagedBefore.getTime(),
                async () => {
                  if (!(await this.authority.stillFenced(g))) throw refusal("erasure_reenabled");
                  await this.authority.ready();
                });
              versions += st.deleted;
              this.counters.liveVersionsDeleted += st.deleted;
              if (st.status !== "erased" || st.remaining > 0) stagedLeft.push(key);
            }
            seen.staged = stagedLeft;
          } else if (!seen.stagedDone) {
            // Too many staged versions for one listing: the same retention through the paged eraser, resumable.
            seen.stagedDone = true;
            const stagedPrefix = liveStagedPrefix(this.cfg.keyRoot, g);
            const scope = eraseScope(["live-staged", this.authority.kind, g.org_id, g.workspace_id, g.user_id, g.origin_id]);
            // Same obligation-gated paged erasure (every doomed version reserved first); retention, so its outcome
            // only decides whether the next sweep resumes (incomplete) or traverses again.
            const st = await this.#erasePaged(stagedPrefix, scope,
              (v) => v.key.endsWith(".staged.ndjson.zst") && v.lastModified !== undefined && v.lastModified < stagedBefore.getTime(),
              async () => {
                if (!(await this.authority.stillFenced(g))) throw refusal("erasure_reenabled");
                await this.authority.ready();
              });
            versions += st.deleted;
            this.counters.liveVersionsDeleted += st.deleted;
          }
          // The session's directory under the (workspace, user, origin) machine prefix, every date, through the shared
          // strict paged eraser: bounded per run, resumable from a durable checkpoint scoped to this group and the
          // authority, authority (still fenced) re-checked before every page's deletes, versions newer than the
          // re-read kept (successors), and done only after a fresh strict zero inventory.
          const scope = eraseScope(["live-session", this.authority.kind, g.org_id, g.workspace_id, g.user_id, g.origin_id, g.source, g.session_id]);
          // Successors (newer than the re-read, less clock skew) are kept; a version without a timestamp is never
          // deleted (S3 listings always carry LastModified).
          const select = (v: ListedVersion) => inSessionAnyDate(t.base, t.sessionSegment, v.key) && v.lastModified !== undefined && v.lastModified < before.getTime();
          const authorize = async () => {
            if (!(await this.authority.stillFenced(g))) throw refusal("erasure_reenabled");
            await this.authority.ready();
          };
          const r = await this.#erasePaged(t.base, scope, select, authorize);
          versions += r.deleted;
          this.counters.liveVersionsDeleted += r.deleted;
          // incomplete: resumes from the checkpoint; unverified / unresolved / refused: the next sweep traverses again.
          if (r.status === "unverified")
            // Observability only (NOT a completion, NOT a waiver): the session is NOT complete; its pointer waits for a
            // fresh strict proof. `reason` is the primitive's own pending reason: "budget" (the verification budget ran
            // out, e.g. > 10k kept versions share the machine prefix) or "remaining" (selected versions are still listed).
            this.log({ message: "live erasure unverified", error_code: "erasure_verify_pending", reason: r.proofReason ?? "unknown",
              org_id: g.org_id, workspace_id: g.workspace_id, user_id: g.user_id, origin_id: g.origin_id, source: g.source, session_id: g.session_id });
          if (r.status !== "erased") continue;
          // The pointer last, through the same gate (versions newer than the re-read kept).
          const ptr = await this.#eraseKey(t.pointerKey, eraseScope(["live-pointer", this.authority.kind, g.org_id, g.workspace_id, g.user_id, g.origin_id, g.source, g.session_id]),
            (v) => v.lastModified !== undefined && v.lastModified < before.getTime(), authorize);
          versions += ptr.deleted;
          this.counters.liveVersionsDeleted += ptr.deleted;
        } catch (error) {
          // A re-enable during the run stops before any further delete; nothing to retry for it.
          if (errorCode(error) === "erasure_reenabled") continue;
          if (isCredentialFault(error)) throw error;
          this.counters.liveFenceFailed += 1;
          this.log({ message: "live erasure failed", error_code: errorCode(error) });
        }
      }
    }
    return { groups, versions };
  }

  /**
   * Erase the legacy export's one owner-bound machine copy for every fenced
   * scope.  A member other than the owner may have created the fence; the
   * authority supplies the exporter's bound owner, so this never hashes the
   * deleting member into the target machine.
   *
   * The transcript selection is the exact encoded `s=` segment under every
   * date.  In particular, a canonical id containing `:` is mapped with the
   * writer's component codec instead of being treated as a successful
   * refusal.  The pointer is exact-key erased only after the transcript has a
   * fresh zero-inventory proof.  Every page rechecks the exact scope fence.
   */
  async #eraseLegacyFenced(dryRun: boolean): Promise<{ groups: number; versions: number }> {
    const page = this.authority.legacyFencedPage;
    const stillFenced = this.authority.stillLegacyFenced;
    // In-process test authorities predating this lane have no legacy feed.
    // Production factories install both methods as a pair.
    if (!page && !stillFenced) return { groups: 0, versions: 0 };
    if (!page || !stillFenced) throw refusal("legacy_erasure_authority_incomplete");
    let groups = 0;
    let versions = 0;
    let after: LegacyFencedGroup | null = null;
    for (;;) {
      if (this.stopped()) break;
      const rows: LegacyFencedGroup[] = await page.call(this.authority, after);
      if (rows.length === 0) break;
      after = rows.at(-1)!;
      for (const g of rows) {
        if (this.stopped()) break;
        try {
          // The stored machine is evidence, not destructive authority by
          // itself: derive it again from the canonical owner tuple and refuse
          // corruption instead of following an arbitrary machine id. A bad
          // row remains loudly retryable but cannot wedge later valid scopes.
          const storageWorkspaces = g.workspace_id === "default" ? ["", "default"] : [g.workspace_id];
          const storageWorkspace = storageWorkspaces.find(
            (workspace) => legacyMachineId(workspace, g.legacy_user_id) === g.legacy_machine_id,
          );
          if (storageWorkspace === undefined)
            throw refusal("legacy_erasure_machine_mismatch");
          let t;
          try {
            t = liveTargets(this.cfg.keyRoot, {
              org_id: g.org_id,
              workspace_id: storageWorkspace,
              user_id: g.legacy_user_id,
              origin_id: LEGACY_EXPORT_ORIGIN_ID,
              source: g.source,
              session_id: g.session_id,
            });
          } catch {
            this.counters.refused += 1;
            throw refusal("legacy_erasure_scope_invalid");
          }
          groups += 1;
          if (dryRun) continue;
          const checkedAt = this.now();
          if (!(await stillFenced.call(this.authority, g))) continue;
          const before = checkedAt - LIVE_SKEW_MS;
          const matchesSession = (key: string) => inSessionAnyDate(t.base, t.sessionSegment, key);
          const authorize = async () => {
            if (!(await stillFenced.call(this.authority, g))) throw refusal("erasure_reenabled");
            await this.authority.ready();
          };
          const transcript = await this.#erasePaged(
            t.base,
            eraseScope(["legacy-session", this.authority.kind, g.org_id, g.workspace_id, g.legacy_user_id, g.source, g.session_id]),
            (v) => matchesSession(v.key) && v.lastModified !== undefined && v.lastModified < before,
            authorize,
            matchesSession,
          );
          versions += transcript.deleted;
          this.counters.legacyVersionsDeleted += transcript.deleted;
          // A successor written after the fence re-read is deliberately kept;
          // keep the pointer too so that successor remains discoverable and a
          // later sweep can erase it after the skew window.
          if (transcript.status !== "erased" || transcript.remaining > 0) continue;
          const pointer = await this.#eraseKey(
            t.pointerKey,
            eraseScope(["legacy-pointer", this.authority.kind, g.org_id, g.workspace_id, g.legacy_user_id, g.source, g.session_id]),
            (v) => v.lastModified !== undefined && v.lastModified < before,
            authorize,
          );
          versions += pointer.deleted;
          this.counters.legacyVersionsDeleted += pointer.deleted;
        } catch (error) {
          if (errorCode(error) === "erasure_reenabled") continue;
          if (isCredentialFault(error)) throw error;
          this.#legacyFenceFailure("legacy erasure failed", error);
        }
      }
    }
    return { groups, versions };
  }

  /** Erase what is queued and due. Returns how many items it erased or failed (not deferred). */
  async drain(limit = 100): Promise<number> {
    const items = this.state.nextErasures(this.now(), limit);
    let handled = 0;
    for (const item of items) {
      if (this.stopped()) break;
      const prefix =
        item.kind === "prefix"
          ? SESSION_PREFIX.test(item.key) && item.key.startsWith(`${orgPrefix(this.cfg.keyRoot, item.org_id)}/v1/`)
            ? item.key
            : undefined
          : sessionPrefixOf(this.cfg.keyRoot, item.org_id, item.key);
      try {
        if (!prefix) {
          // Never delete outside a session prefix of the item's own org.
          this.counters.refused += 1;
          this.state.erasureFailed(item.key, "erasure_refused_shape", this.now());
          handled += 1;
          continue;
        }
      } catch (error) {
        this.log({ message: "erasure state busy", error_code: errorCode(error) });
        continue;
      }
      handled += (await this.#leasedItem(item, prefix)).handled;
    }
    return handled;
  }

  /**
   * One queued item under its session-prefix lease: the per-item path shared by `drain` and the stale-prefix-only
   * mode, so both keep every per-delete protection (still queued, lease renewed, authority ready, the OFF mode
   * interlock, the strict paged eraser and its fresh proof, the done claim). `guard`, when given, runs inside the
   * lease after the still-queued check and before any erase; a returned reason skips the item untouched.
   */
  async #leasedItem(item: QueuedItem, prefix: string, guard?: () => Promise<string | undefined>): Promise<LeasedItemResult> {
    try {
      if (!this.state.acquireLease(prefix, LEASE_HOLDER, undefined, this.now())) {
        this.counters.deferred += 1;
        return { outcome: "deferred", handled: 0, deleted: 0 };
      }
    } catch (error) {
      this.log({ message: "erasure state busy", error_code: errorCode(error) });
      return { outcome: "busy", handled: 0, deleted: 0 };
    }
    // `handled` and `deleted` advance at exactly the points drain counted them before this method existed.
    let handled = 0, deleted = 0;
    try {
      // The exporter may have cancelled it before we took the lease.
      if (!this.state.erasureQueued(item.key, item.enqueued_at)) return { outcome: "cancelled", handled, deleted };
      if (guard) {
        const skip = await guard();
        if (skip !== undefined) return { outcome: "skipped", handled, deleted, skip };
      }
      // The lease generation this eraser acquired: every retirement page and the done claim re-validate it.
      const itemLease = { prefix, holder: LEASE_HOLDER, gen: this.state.leaseGen(prefix) ?? "" };
      if (item.kind === "object") {
        // One queued object, through the SAME gate as a prefix: reserve every version before its delete, done only
        // after a strict empty inventory of the key and no unresolved PUT at exactly that key.
        const objScope = eraseScope(["queued-object", this.authority.kind, item.org_id, item.key, item.reason, item.enqueued_at, "lease:" + prefix]);
        const r = await this.#eraseKey(item.key, objScope, () => true, async () => {
          if (!this.state.erasureQueued(item.key, item.enqueued_at)) throw refusal("erasure_item_cancelled");
          if (!this.state.renewLease(prefix, LEASE_HOLDER, this.now())) throw refusal("erasure_lease_lost");
          await this.authority.ready();
        }, true, itemLease);
        this.counters.versionsDeleted += r.deleted;
        deleted += r.deleted;
        handled += 1;
        if (r.status === "unresolved") {
          this.state.erasureFailed(item.key, (this.cfg.obligationsSettled?.() ?? false) ? "erasure_write_unresolved" : "erasure_obligations_unattested", this.now());
          return { outcome: "unresolved", handled, deleted };
        }
        if (r.status !== "erased") return { outcome: "not_erased", handled, deleted };
        // The done claim re-validates the revision in the SAME transaction as the queue-row removal.
        if (this.state.obligations ? !this.state.completeErasure(item.key, item.enqueued_at, item.key.slice(0, item.key.lastIndexOf("/") + 1), r.rev, itemLease) : (this.state.erasureDone(item.key, item.enqueued_at), false)) {
          this.counters.doneRefused += 1;
          this.state.erasureFailed(item.key, "erasure_done_refused", this.now());
          this.log({ message: "erasure done refused", error_code: "erasure_done_refused", kind: item.kind });
          return { outcome: "done_refused", handled, deleted };
        }
        this.counters.objectsErased += 1;
        return { outcome: "erased", handled, deleted };
      }
      // A session prefix: the shared strict paged eraser, resumable across runs from a durable checkpoint bound to
      // this queued item (key + enqueue time), the authority and the operation. Before every page's deletes the
      // item must still be queued, this process must still hold its lease, and the authority must still be ready.
      // Scope: operation, authority, tenant + queued item (its enqueue time is the item's generation) and the leased resource.
      const scope = eraseScope(["queued-prefix", this.authority.kind, item.org_id, item.key, item.reason, item.enqueued_at, "lease:" + prefix]);
      const authorize = async () => {
        if (!this.state.erasureQueued(item.key, item.enqueued_at)) throw refusal("erasure_item_cancelled");
        if (!this.state.renewLease(prefix, LEASE_HOLDER, this.now())) throw refusal("erasure_lease_lost");
        await this.authority.ready();
      };
      const r = await this.#erasePaged(item.key, scope, undefined, authorize, undefined, true, undefined, itemLease);
      this.counters.versionsDeleted += r.deleted;
      deleted += r.deleted;
      handled += 1;
      // Still queued unless proven: incomplete resumes from the checkpoint; unverified / refused retry on the next
      // drain; an unresolved write (a PUT whose outcome is unknown) keeps it queued, re-swept on the bounded backoff.
      if (r.status === "unresolved") {
        this.state.erasureFailed(item.key, (this.cfg.obligationsSettled?.() ?? false) ? "erasure_write_unresolved" : "erasure_obligations_unattested", this.now());
        return { outcome: "unresolved", handled, deleted };
      }
      if (r.status !== "erased") return { outcome: "not_erased", handled, deleted, status: r.status };
      // The done claim re-validates the revision in the SAME transaction as the queue-row removal.
      if (this.state.obligations ? !this.state.completeErasure(item.key, item.enqueued_at, item.key, r.rev, itemLease) : (this.state.erasureDone(item.key, item.enqueued_at), false)) {
        this.counters.doneRefused += 1;
        this.state.erasureFailed(item.key, "erasure_done_refused", this.now());
        this.log({ message: "erasure done refused", error_code: "erasure_done_refused", kind: item.kind });
        return { outcome: "done_refused", handled, deleted };
      }
      this.counters.prefixesErased += 1;
      return { outcome: "erased", handled, deleted };
    } catch (error) {
      // An expiring short-lived eraser key refused before signing is the credential's fault, not this item's: no
      // erasureFailed (the item stays queued with its attempts, and a paged erase keeps its durable checkpoint), the
      // lease is released below, and the refusal stops the whole job for a renewal (the CLI exits 75).
      if (isCredentialFault(error)) {
        this.log({ message: "erasure stopped: credential", error_code: errorCode(error), kind: item.kind });
        throw error;
      }
      this.counters.failed += 1;
      this.state.erasureFailed(item.key, errorCode(error), this.now());
      this.log({ message: "erasure failed", error_code: errorCode(error), kind: item.kind });
      handled += 1;
      return { outcome: "failed", handled, deleted, error: errorCode(error) };
    } finally {
      try {
        this.state.releaseLeases(LEASE_HOLDER, [prefix]);
      } catch {}
    }
  }

  /**
   * The execute preflight's protection facts (see FenceContext): the attestation (required, strict, fresh) and the
   * code-readable bucket facts, which must agree with it. Any problem refuses the whole pass before any delete.
   */
  async #protectionPreflight(opts: StalePrefixPassOptions): Promise<{ problems: string[]; notes: string[]; attestationUsed: boolean }> {
    const problems: string[] = [];
    if (opts.attestation) {
      const p = attestationProblem(opts.attestation, { bucketSha256: opts.bucketSha256!, keyRoot: this.cfg.keyRoot });
      if (p) problems.push(p);
      else if (!attestationFresh(opts.attestation, this.now())) problems.push("attestation_stale");
    }
    const read = async <T>(f: () => Promise<T>, fallback: (code: string) => T): Promise<T> => {
      try { return await f(); } catch (error) { if (isCredentialFault(error)) throw error; return fallback(errorCode(error)); }
    };
    const fence = opts.fence!;
    const lifecycle = await read(() => fence.bucketLifecycle(), (code) => ({ status: "unreadable" as const, code }));
    const versioning = await read(() => fence.bucketVersioning(), (code) => ({ status: "unreadable" as const, code }));
    const facts = protectionFacts(lifecycle, versioning, this.cfg.keyRoot, opts.attestation);
    return { problems: [...new Set([...problems, ...facts.problems])], notes: facts.notes, attestationUsed: facts.attestationUsed };
  }

  /**
   * The static receipt gate, before the queue is read (no state read or write, zero deletes). Refuses unless:
   *  - the receipt binds this bucket and key root, and its target (bucket, key root, allowlist digest, state-file
   *    identity, verifier commit) equals execute's own, and it is at most RECEIPT_MAX_AGE_MS old;
   *  - the run's allowlist, the receipt's allowlist and the receipt's identity set are EXACTLY equal, and their digest
   *    equals the owner-approved `approvedAllowlistSha256`;
   *  - every identity and every item is PASS;
   *  - units (stale_prefix_cap_units_mismatch): identities = allowlist size = `approvedIdentityCount`; planned
   *    prefixes = `max` = `approvedMax` (never derived from the receipt); planned prefixes = identities unless the
   *    separately owner-approved multi-prefix mode names them (`approvedPrefixCount`); each identity's item_count
   *    equals its items. So a cap can never end a partly completed batch.
   */
  #receiptGate(opts: StalePrefixPassOptions, ids: readonly string[]): void {
    const r = opts.receipt!;
    if (r.bucket_sha256 !== opts.bucketSha256 || r.key_root_sha256 !== verifySha256(this.cfg.keyRoot)) throw refusal("stale_prefix_receipt_invalid");
    const idSet = new Set(ids);
    const sameSet = (a: readonly string[]) => Array.isArray(a) && a.length === idSet.size && new Set(a).size === a.length && a.every((x) => idSet.has(x));
    if (!sameSet(r.allowlist) || !sameSet(r.identities.map((i) => i.identity_hash))) throw refusal("stale_prefix_receipt_not_all_pass");
    if (r.status !== "PASS" || r.identities.some((i) => i.status !== "PASS" || !i.items.length || i.items.some((it) => it.status !== "PASS" || !it.survivor || it.identity_hash !== i.identity_hash)))
      throw refusal("stale_prefix_receipt_not_all_pass");
    const digestNow = allowlistDigest(ids);
    if (opts.approvedAllowlistSha256 !== digestNow) throw refusal("stale_prefix_approved_allowlist_mismatch");
    const t = r.target, want = opts.target!;
    if (!t || t.allowlist_sha256 !== digestNow || t.bucket_sha256 !== opts.bucketSha256 || t.key_root !== this.cfg.keyRoot
        || t.state_identity !== want.stateIdentity || t.verifier_commit !== want.verifierCommit)
      throw refusal("stale_prefix_receipt_target_mismatch");
    const at = Date.parse(t.produced_at), now = this.now();
    if (!Number.isFinite(at) || at > now + 60_000 || now - at > RECEIPT_MAX_AGE_MS) throw refusal("stale_prefix_receipt_stale");
    // Two units, reconciled and never raised: IDENTITIES (the allowlist) and old PREFIXES (items; the dry run's
    // eligible count, which --max counts). Each identity's item_count must equal its items.
    if (r.identities.some((i) => i.item_count !== i.items.length)) throw refusal("stale_prefix_cap_units_mismatch");
    const planned = r.identities.reduce((n, i) => n + i.items.length, 0);
    if (r.identities.length !== ids.length || opts.approvedIdentityCount !== ids.length) throw refusal("stale_prefix_cap_units_mismatch");
    if (opts.max !== planned || opts.approvedMax !== planned) throw refusal("stale_prefix_cap_units_mismatch");
    // The owner decision's DEFAULT rule: planned prefixes equal the allowlist's identity count (one prefix each).
    // More prefixes than identities only under the separately owner-approved multi-prefix mode, whose
    // --approved-prefix-count must equal the planned prefixes.
    if (planned !== ids.length && opts.approvedPrefixCount !== planned) throw refusal("stale_prefix_cap_units_mismatch");
    if (opts.approvedPrefixCount !== undefined && opts.approvedPrefixCount !== planned) throw refusal("stale_prefix_cap_units_mismatch");
  }

  /**
   * Execute: the WHOLE batch through the receipt fence (FenceContext lists what is proven here and what rests on the
   * attestation). Every phase before the first delete refuses the WHOLE batch with zero deletes:
   *  A. inventory: the complete set of queued stale_prefix items of the allowlisted identities (old-prefix hash +
   *     enqueued_at, backing-off entries included) equals the receipt's items exactly, and every one is due; the
   *     stale guard passes for every item (read-only); the protection preflight holds;
   *  B. leases: the eraser-wide run lease, then every item's old-prefix lease and its survivor-prefix lease (the
   *     exporter's own per-prefix lease, so it cannot rewrite a survivor), all held to the end;
   *  C. under all leases, per item: still queued, the guard again, and a MANDATORY inline re-verification
   *     (verifyStalePrefix with the reader) that must reproduce the receipt's item EXACTLY (version set, pins,
   *     counts, status), so the supplied receipt is never trusted on its own.
   *  D. deletes, item by item, exactly the receipt's versions (manifests first), each preceded by #fenceCheck. Any
   *     stop ends the batch: the item is `partial` when anything was issued, every later item stays not_attempted.
   */
  async #fencedBatch(opts: StalePrefixPassOptions, allow: ReadonlySet<string>, res: StalePrefixResult, selected: readonly (QueuedItem & { next_try: number })[],
    skip: (reason: StalePrefixSkip, key: string) => void): Promise<Set<string>> {
    const receipt = opts.receipt!, fence = opts.fence!, attestation = opts.attestation!;
    const eligible = new Set<string>();
    const now0 = this.now();
    const entries = receipt.identities.flatMap((i) => i.items);
    const reports = entries.map((e): FenceReport => ({
      identity_hash: e.identity_hash, old_prefix_sha256: e.old_prefix_sha256, outcome: "refused_before_delete", deleted: 0,
      versions: e.old_versions.map((v) => ({ key_sha256: v.key_sha256, version_id: v.version_id, outcome: "not_attempted" as VersionOutcome })),
    }));
    res.fenced = reports;
    const refuseBatch = (code: string, extra: Record<string, unknown> = {}) => {
      res.ok = false;
      res.refused = code;
      for (const r of reports) if (r.outcome === "refused_before_delete") r.reason ??= code;
      this.log({ message: "stale prefix pass refused", error_code: code, ...extra });
      return eligible;
    };
    // ---- A. inventory, guard, preflight (no lease, no write)
    type Work = { item: QueuedItem; identity: string; entry: ItemReceipt; report: FenceReport; survivorPrefix?: string };
    const work: Work[] = [];
    for (const item of selected) {
      const identity = staleprefixIdentityHash(this.cfg.keyRoot, item.org_id, item.key);
      const isDue = item.next_try <= now0;
      if (identity === undefined) { if (isDue) skip("identity_unresolved", item.key); continue; }
      if (!allow.has(identity)) { if (isDue) skip("not_in_identity_allowlist", item.key); continue; }
      if (!isDue) return refuseBatch("stale_prefix_batch_not_due", { key_sha256: keyHash(item.key) });
      const at = entries.findIndex((e) => e.identity_hash === identity && e.old_prefix_sha256 === verifySha256(item.key) && e.enqueued_at === item.enqueued_at);
      if (at < 0 || work.some((w) => w.entry === entries[at])) return refuseBatch("stale_prefix_receipt_missing", { key_sha256: keyHash(item.key) });
      work.push({ item, identity, entry: entries[at]!, report: reports[at]! });
    }
    if (work.length !== entries.length) return refuseBatch("stale_prefix_receipt_missing");
    // Per identity, the queued (due, in-scope) prefixes equal the receipt's item_count.
    for (const id of receipt.identities)
      if (work.filter((w) => w.identity === id.identity_hash).length !== id.item_count) return refuseBatch("stale_prefix_cap_units_mismatch");
    // Process in the receipt's order (deterministic, and the order the owner approved).
    work.sort((a, b) => entries.indexOf(a.entry) - entries.indexOf(b.entry));
    const guardAll = async (): Promise<string | undefined> => {
      for (const w of work) {
        if (!sessionPrefixParts(this.cfg.keyRoot, w.item.org_id, w.item.key)) { skip("shape_invalid", w.item.key); return "shape_invalid"; }
        let reason: StalePrefixSkip | undefined;
        try {
          reason = await this.#staleGuard(w.item, opts.probe);
        } catch (error) {
          if (isCredentialFault(error)) throw error;
          // A state or store failure in the guard: recorded on that entry as drain records a failed erase.
          this.counters.failed += 1;
          this.state.erasureFailed(w.item.key, errorCode(error), this.now());
          this.log({ message: "erasure failed", error_code: errorCode(error), kind: w.item.kind });
          res.ok = false;
          res.stoppedBy = "failed";
          return "failed";
        }
        if (reason) { skip(reason, w.item.key); return reason; }
      }
      return undefined;
    };
    const pre = await this.#protectionPreflight(opts);
    if (pre.problems.length) return refuseBatch("stale_prefix_protection_unproven", { problems: pre.problems, notes: pre.notes, attestation_used: pre.attestationUsed });
    this.log({ message: "stale prefix protection preflight", proven: true, notes: pre.notes, attestation_used: pre.attestationUsed });
    const g = await guardAll();
    if (g === "failed") return eligible;
    if (g) return refuseBatch("stale_prefix_batch_guard_skip", { reason: g });
    // ---- B. leases, all held to the end
    const held: string[] = [];
    const take = (prefix: string) => {
      if (held.includes(prefix)) return true;
      if (!this.state.acquireLease(prefix, LEASE_HOLDER, undefined, this.now())) return false;
      held.push(prefix);
      return true;
    };
    let current: Work | undefined;
    const fs: FenceClock = { protectedUntil: Number.NEGATIVE_INFINITY, pages: new Map(), remaining: [], maxLatencyMs: 0 };
    /**
     * Every S3 request after the leases goes through the request gate, run by the eraser AFTER signing and
     * immediately before fetch; it returns the time left to the ABSOLUTE deadline, which caps the request's abort
     * timeout. A listing also counts its ACTUAL pages against the page budget.
     */
    const hook = (prefix?: string) => {
      let n = 0;
      return () => {
        this.#requestGate(fence, fs, held);
        if (prefix !== undefined) {
          if (++n > FENCE_LIST_MAX_PAGES) { fs.failure ??= "stale_prefix_listing_budget"; throw refusal("stale_prefix_listing_budget"); }
          fs.pages.set(prefix, Math.max(fs.pages.get(prefix) ?? 0, n));
        }
        return fs.protectedUntil - this.now();
      };
    };
    const list = (prefix: string) => fence.listVersions(prefix, { beforeRequest: hook(prefix) });
    const reader: VersionReader = {
      listVersions: (prefix) => opts.reader!.listVersions(prefix, { beforeRequest: hook(prefix) }),
      getVersion: (key, versionId, maxBytes) => opts.reader!.getVersion(key, versionId, maxBytes, { beforeFetch: hook() }),
    };
    try {
      if (!take(STALE_RUN_LEASE)) return refuseBatch("stale_prefix_mode_other_eraser_lease");
      for (const w of work) {
        if (!take(w.item.key)) { this.counters.deferred += 1; skip("lease_busy", w.item.key); return refuseBatch("stale_prefix_batch_lease_busy"); }
        const survivor = w.entry.survivor!;
        const key = this.#staleSiblings(w.item).siblings.find((k) => verifySha256(k) === survivor.manifest.key_sha256);
        const sp = key?.slice(0, -"manifest.json".length);
        if (!sp || verifySha256(sp) !== survivor.prefix_sha256) return refuseBatch("stale_prefix_survivor_drift");
        if (!take(sp)) return refuseBatch("stale_prefix_survivor_lease_busy");
        w.survivorPrefix = sp;
      }
      // The HARD batch deadline, fixed now: the earliest lease expiry or attestation expiry, minus a margin of two
      // worst-case requests. Every later S3 request (each list page, each GET, each delete) is gated on it; the
      // remaining-work TTL bound is only an estimate, this deadline is not.
      const perRequest = fence.requestDeadlineMs * fence.requestAttempts;
      const t0 = this.now();
      const leaseUntil = Math.min(...held.map((p) => t0 + this.state.leaseRemainingMs(p, LEASE_HOLDER, t0)));
      fs.protectedUntil = Math.min(leaseUntil, Date.parse(attestation.verified_at) + ATTESTATION_MAX_AGE_MS) - 2 * perRequest;
      const remainingFrom = (wi: number, deletesLeftInFirst?: number) => work.slice(wi).map((x, i) => ({
        entry: x.entry, oldPrefix: x.item.key, survivorPrefix: x.survivorPrefix!, deletesLeft: i === 0 && deletesLeftInFirst !== undefined ? deletesLeftInFirst : x.entry.old_versions.length, first: i === 0 && deletesLeftInFirst !== undefined,
      }));
      fs.remaining = remainingFrom(0);
      // ---- C. under all leases: queued, guard, mandatory inline re-verification
      for (const w of work) if (!this.state.erasureQueued(w.item.key, w.item.enqueued_at)) { skip("cancelled", w.item.key); return refuseBatch("stale_prefix_batch_cancelled"); }
      const g2 = await guardAll();
      if (g2 === "failed") return eligible;
      if (g2) return refuseBatch("stale_prefix_batch_guard_skip", { reason: g2 });
      for (const w of work) {
        // The exact old set and the survivor pins first (precise codes), then the full re-verification.
        if (!sameVersionSet(versionSet(await list(w.item.key)), w.entry.old_versions)) return refuseBatch("stale_prefix_receipt_drift", { old_prefix_sha256: w.entry.old_prefix_sha256 });
        if (!survivorPinsHold(await list(w.survivorPrefix!), w.entry.survivor!)) return refuseBatch("stale_prefix_survivor_drift", { old_prefix_sha256: w.entry.old_prefix_sha256 });
        const again = await verifyStalePrefix(reader, { identityHash: w.identity, oldPrefix: w.item.key, enqueuedAt: w.item.enqueued_at, survivorManifestKeys: this.#staleSiblings(w.item).siblings });
        if (fs.failure) throw refusal(fs.failure); // a gate refusal swallowed as "unreadable" inside the verifier
        if (!sameItem(again, w.entry)) return refuseBatch("stale_prefix_reverify_mismatch", { old_prefix_sha256: w.entry.old_prefix_sha256 });
      }
      // ---- D. deletes
      for (let wi = 0; wi < work.length; wi++) {
        const w = (current = work[wi]!);
        if (this.stopped()) { res.ok = false; res.stoppedBy = "signal"; break; }
        fs.remaining = remainingFrom(wi);
        const listing = await list(w.item.key);
        if (!sameVersionSet(versionSet(listing), w.entry.old_versions)) { this.#stopBatch(res, w, "stale_prefix_receipt_drift"); break; }
        const doomed = [...listing].sort((a, b) =>
          Number(b.key.endsWith("/manifest.json")) - Number(a.key.endsWith("/manifest.json")) || (a.key < b.key ? -1 : a.key > b.key ? 1 : a.versionId < b.versionId ? -1 : 1));
        w.report.versions = doomed.map((v) => ({ key_sha256: verifySha256(v.key), version_id: v.versionId, outcome: "not_attempted" as VersionOutcome }));
        let stop: string | undefined;
        let credential: unknown;
        for (let i = 0; i < doomed.length && !stop; i++) {
          fs.remaining = remainingFrom(wi, doomed.length - i);
          stop = await this.#fenceCheck(w, held, list);
          const checkDoneAt = this.now();
          if (stop) break;
          const v = doomed[i]!, out = w.report.versions[i]!;
          // Receipt membership of the very version about to be deleted.
          if (!w.entry.old_versions.some((e) => e.key_sha256 === out.key_sha256 && e.version_id === v.versionId)) { stop = "stale_prefix_receipt_drift"; break; }
          // IMMEDIATELY before issuing: attestation, check-to-delete interval, hard deadline, every lease's TTL.
          stop = this.#issueCheck(fence, fs, held, attestation, checkDoneAt);
          if (stop) break;
          const t1 = this.now();
          // The same issuance check again, AFTER signing and immediately before the DELETE is sent (eraser.ts
          // request): slow hashing or signing can never carry a delete past the deadline. Refused there, nothing
          // is sent and the version stays not_attempted.
          let gateRefused: string | undefined;
          const beforeFetch = () => {
            const code = this.#issueCheck(fence, fs, held, attestation, checkDoneAt);
            if (code) { gateRefused = code; throw refusal(code); }
            return fs.protectedUntil - this.now();
          };
          try {
            await fence.deleteVersion(v.key, v.versionId, { beforeFetch });
          } catch (error) {
            if (gateRefused) { stop = gateRefused; break; }
            // A short-lived key refused BEFORE signing: nothing was sent, the version stays not_attempted.
            if (isCredentialFault(error)) { credential = error; stop = errorCode(error); break; }
            // Anything else may still have applied: never not_attempted. Resolve by an exact relist (through the same
            // request gate and page budget), record, stop. A relist the gate refuses, or that fails, leaves unknown.
            out.outcome = "unknown";
            try {
              const after = await list(w.item.key);
              out.outcome = after.some((x) => x.key === v.key && x.versionId === v.versionId) ? "present_after_error" : "absent_observed";
            } catch {}
            out.error_code = errorCode(error);
            stop = "stale_prefix_delete_ambiguous";
            break;
          }
          fs.maxLatencyMs = Math.max(fs.maxLatencyMs, this.now() - t1);
          out.outcome = "deleted";
          w.report.deleted += 1;
        }
        if (!stop) {
          // After the item: the survivor again, and a strict proof that nothing is left under the old prefix.
          fs.remaining = remainingFrom(wi, 0);
          try {
            if (!survivorPinsHold(await list(w.survivorPrefix!), w.entry.survivor!)) stop = "stale_prefix_survivor_drift_after_erase";
            else if ((await list(w.item.key)).length) stop = "stale_prefix_post_residue";
          } catch (error) {
            if (isCredentialFault(error)) throw error;
            stop = errorCode(error);
          }
        }
        res.versionsDeleted += w.report.deleted;
        this.counters.versionsDeleted += w.report.deleted;
        if (stop) {
          this.#stopBatch(res, w, stop);
          if (credential) { this.log({ message: "erasure stopped: credential", error_code: errorCode(credential), kind: w.item.kind }); throw credential; }
          break;
        }
        w.report.outcome = "completed";
        this.state.erasureDone(w.item.key, w.item.enqueued_at);
        this.counters.prefixesErased += 1;
        res.erased += 1;
        res.eligible += 1;
        eligible.add(w.identity);
        this.log({ message: "stale prefix fence", ...this.#fenceLog(w.report) });
        this.log({ message: "stale prefix erased", key_sha256: keyHash(w.item.key), versions_deleted: w.report.deleted });
      }
    } catch (error) {
      if (isCredentialFault(error)) throw error;
      if (current) this.#stopBatch(res, current, errorCode(error));
      else return refuseBatch(errorCode(error));
    } finally {
      try { this.state.releaseLeases(LEASE_HOLDER, held); } catch {}
    }
    return eligible;
  }

  /**
   * The gate before EVERY S3 request once the leases are held: the request's own worst case (deadline x attempts)
   * must end by the hard deadline, and every lease must still cover the remaining work (#ttlBound, with the ACTUAL
   * page counts observed so far). Throws `stale_prefix_deadline` / `stale_prefix_lease_ttl_insufficient`.
   */
  #requestGate(fence: ReceiptFence, fs: FenceClock, held: readonly string[]): void {
    // Runs inside the eraser's request path after signing, immediately before fetch.
    const now = this.now();
    const fail = (code: string) => { fs.failure ??= code; throw refusal(code); };
    if (now + fence.requestDeadlineMs * fence.requestAttempts > fs.protectedUntil) fail("stale_prefix_deadline");
    const bound = this.#ttlBound(fence, fs);
    for (const p of held) if (this.state.leaseRemainingMs(p, LEASE_HOLDER, now) < bound) fail("stale_prefix_lease_ttl_insufficient");
  }

  /**
   * Every per-delete precondition that needs I/O, immediately before ONE delete; returns the coded stop reason, or
   * undefined: the item still queued; every held lease renewed; no other eraser lease; the OFF interlock; authority
   * ready; a strict relist of the old prefix EXACTLY equal to the receipt's set minus what was deleted (a version not
   * in the receipt, or one gone without our delete, is drift); and every survivor pin CURRENT at its pinned versionId
   * and not a delete marker (a pin made NONCURRENT is drift: the bucket expires noncurrent versions after about a
   * day). The time-based checks run AFTER this, at issuance (#issueCheck).
   */
  async #fenceCheck(w: { item: QueuedItem; entry: ItemReceipt; report: FenceReport; survivorPrefix?: string }, held: readonly string[],
    list: (prefix: string) => Promise<ListedVersion[]>): Promise<string | undefined> {
    if (!this.state.erasureQueued(w.item.key, w.item.enqueued_at)) return "erasure_item_cancelled";
    for (const p of held) if (!this.state.renewLease(p, LEASE_HOLDER, this.now())) return "erasure_lease_lost";
    if (this.state.otherEraserLeases(LEASE_HOLDER, this.now()) > 0) return "stale_prefix_mode_other_eraser_lease";
    this.state.assertModeAllowed();
    await this.authority.ready();
    const expected = w.entry.old_versions.filter((v) => !w.report.versions.some((r) => r.outcome === "deleted" && r.key_sha256 === v.key_sha256 && r.version_id === v.version_id));
    if (!sameVersionSet(versionSet(await list(w.item.key)), expected)) return "stale_prefix_receipt_drift";
    if (!survivorPinsHold(await list(w.survivorPrefix!), w.entry.survivor!)) return "stale_prefix_survivor_drift";
    this.state.assertModeAllowed();
    return undefined;
  }

  /**
   * At the instant of issuing ONE delete (after the authority check and both listings): the attestation still fresh;
   * at most CHECK_TO_DELETE_MAX_MS since the fence check completed; the delete's own worst case (deadline x attempts)
   * ends by the hard deadline; and every lease's remaining TTL covers that request plus ISSUE_MARGIN_MS and the
   * remaining-work bound. Otherwise the delete is NOT issued.
   */
  #issueCheck(fence: ReceiptFence, fs: FenceClock, held: readonly string[], attestation: ProtectionAttestation, checkDoneAt: number): string | undefined {
    const now = this.now(), perRequest = fence.requestDeadlineMs * fence.requestAttempts;
    if (!attestationFresh(attestation, now)) return "stale_prefix_protection_unproven";
    if (now - checkDoneAt > CHECK_TO_DELETE_MAX_MS) return "stale_prefix_deadline";
    if (now + perRequest > fs.protectedUntil) return "stale_prefix_deadline";
    const bound = this.#ttlBound(fence, fs);
    for (const p of held) {
      const left = this.state.leaseRemainingMs(p, LEASE_HOLDER, now);
      if (left < perRequest + ISSUE_MARGIN_MS) return "stale_prefix_deadline";
      if (left < bound) return "stale_prefix_lease_ttl_insufficient";
    }
    return undefined;
  }

  /**
   * Worst-case milliseconds from now through the LAST delete of the batch (an estimate; the hard deadline is the
   * guarantee): per remaining delete its check's two listings plus the delete, per item an initial relist (not for
   * the item in progress) and two post-checks. Listing pages are the larger of the S3 estimate (floor(n / 1000) + 1)
   * and the ACTUAL pages observed for that prefix; one request costs the fence's deadline x attempts (or the slowest
   * observed delete).
   */
  #ttlBound(fence: ReceiptFence, fs: FenceClock): number {
    const perRequest = Math.max(fence.requestDeadlineMs, fs.maxLatencyMs) * fence.requestAttempts;
    const pages = (prefix: string, est: number) => Math.max(Math.floor(est / 1000) + 1, fs.pages.get(prefix) ?? 0);
    let requests = 0;
    for (const r of fs.remaining) {
      const oldP = pages(r.oldPrefix, r.entry.old_versions.length), survP = pages(r.survivorPrefix, 2 + r.entry.survivor!.chunks.length);
      requests += (r.first ? 0 : oldP) + r.deletesLeft * (oldP + survP + 1) + oldP + survP;
    }
    return requests * perRequest;
  }

  /** The batch stops at item `w`: partial when anything was issued (backoff as a failed erase), else refused_before_delete. */
  #stopBatch(res: StalePrefixResult, w: { item: QueuedItem; report: FenceReport }, reason: string): void {
    res.ok = false;
    w.report.reason = reason;
    const issued = w.report.versions.some((v) => v.outcome !== "not_attempted");
    if (issued) {
      w.report.outcome = "partial";
      this.counters.failed += 1;
      try { this.state.erasureFailed(w.item.key, reason, this.now()); } catch {}
      this.log({ message: "erasure failed", error_code: reason, kind: w.item.kind });
      res.stoppedBy = "failed";
    } else if (res.erased === 0) {
      res.refused = reason;
      res.stoppedBy = "refused";
    } else res.stoppedBy = "failed";
    this.log({ message: "stale prefix fence", ...this.#fenceLog(w.report) });
    this.log({ message: "stale prefix pass stopped", outcome: w.report.outcome, error_code: reason, key_sha256: keyHash(w.item.key) });
  }

  #fenceLog(r: FenceReport) {
    const n = (o: VersionOutcome) => r.versions.filter((v) => v.outcome === o).length;
    return { identity_hash: r.identity_hash, old_prefix_sha256: r.old_prefix_sha256, outcome: r.outcome, ...(r.reason ? { error_code: r.reason } : {}),
      deleted: n("deleted"), not_attempted: n("not_attempted"), absent_observed: n("absent_observed"), present_after_error: n("present_after_error"),
      unknown: n("unknown"), versions: r.versions };
  }

  /**
   * The state half of the stale-prefix guard: (a) and the exporter's date-moved sibling manifests of the same session
   * identity whose group's last export succeeded (the candidates the verifier pins a survivor from).
   */
  #staleSiblings(item: { key: string; org_id: string }): { skip?: StalePrefixSkip; siblings: string[] } {
    const q = sessionPrefixParts(this.cfg.keyRoot, item.org_id, item.key);
    if (!q) return { skip: "shape_invalid", siblings: [] };
    const rows = this.state.exportedKeysOf(item.org_id);
    if (rows.some((r) => r.keys.some((k) => k.startsWith(item.key))) || this.state.pendingKeysOf(item.org_id).some((k) => k.startsWith(item.key)))
      return { skip: "current_key", siblings: [] };
    const siblings: string[] = [];
    let unsettled = false, otherMove = false;
    for (const r of rows)
      for (const k of r.keys) {
        if (!k.endsWith("/manifest.json")) continue;
        const p = sessionPrefixParts(this.cfg.keyRoot, item.org_id, k.slice(0, -"manifest.json".length));
        if (!p || p.s !== q.s) continue;
        if (p.ws !== q.ws || p.m !== q.m || p.h !== q.h) otherMove = true;
        else if (p.d !== q.d) {
          if (r.error === null) siblings.push(k);
          else unsettled = true;
        }
      }
    if (!siblings.length) return { skip: otherMove ? "not_date_move" : unsettled ? "sibling_export_failed" : "no_live_sibling", siblings };
    return { siblings };
  }

  /**
   * Stale-prefix guard, evaluated inside the item's lease right before its erase (or read-only in a dry pass):
   *  (a) the queued prefix holds none of the org's current exported or pending keys (`current_key`);
   *  (b) a current exported manifest of the same session identity (org, ws, m, h, s), differing ONLY in the date,
   *      exists in a group whose last export succeeded, and that manifest is live in the object store right now.
   * Without a probe (a dry run has no store credentials) (b)'s store check is not made; the caller reports it.
   */
  async #staleGuard(item: { key: string; org_id: string }, probe: LiveObjectProbe | undefined): Promise<StalePrefixSkip | undefined> {
    const { skip: stateSkip, siblings } = this.#staleSiblings(item);
    if (stateSkip) return stateSkip;
    if (!probe) return undefined;
    try {
      for (const k of siblings) if (await probe.objectLive(k)) return undefined;
    } catch (error) {
      // An expiring eraser key refused before signing is not an ordinary skip: it stops the pass (exit 75).
      if (isCredentialFault(error)) throw error;
      return "sibling_check_failed";
    }
    return "sibling_manifest_missing";
  }

  /** The allowlist as one dense snapshot, read index by index (Array#every skips holes, so a sparse array would pass it). */
  #allowlist(raw: unknown): string[] {
    if (!Array.isArray(raw)) throw refusal("stale_prefix_mode_allowlist_invalid");
    const len = raw.length;
    if (!Number.isSafeInteger(len) || len === 0 || len > STALE_PREFIX_ALLOWLIST_MAX) throw refusal("stale_prefix_mode_allowlist_invalid");
    const ids: string[] = [];
    for (let i = 0; i < len; i++) {
      if (!Object.prototype.hasOwnProperty.call(raw, i)) throw refusal("stale_prefix_mode_allowlist_invalid");
      const h: unknown = raw[i];
      if (typeof h !== "string" || !IDENTITY_HASH.test(h)) throw refusal("stale_prefix_mode_allowlist_invalid");
      ids.push(h);
    }
    if (new Set(ids).size !== ids.length) throw refusal("stale_prefix_mode_allowlist_invalid");
    return ids;
  }

  /**
   * `--stale-prefixes-only --verify-only`: READ-ONLY verification (stale-prefix-verify.ts) of every queued stale
   * prefix of every allowlisted identity (backing-off entries included) against its date-moved survivor. No lease, no
   * queue or state write, no delete: `reader` has no delete method. The receipt reports against the ORIGINAL
   * allowlist (an identity with no queued entry is FAIL `no_stale_prefix_entry`); logs carry counts and hashes only.
   */
  async staleprefixVerify(opts: { onlyIdentities: readonly string[]; reader: VersionReader; bucketSha256: string; target: { stateIdentity: string; verifierCommit: string } }): Promise<VerificationReceipt> {
    const ids = this.#allowlist(opts.onlyIdentities);
    if (!IDENTITY_HASH.test(opts.bucketSha256)) throw refusal("stale_prefix_verify_bucket_invalid");
    const selected = this.state.staleprefixErasures();
    const identities: IdentityReceipt[] = [];
    for (const id of ids) {
      const entries = selected
        .filter((i) => staleprefixIdentityHash(this.cfg.keyRoot, i.org_id, i.key) === id)
        .sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : a.enqueued_at - b.enqueued_at));
      const items: ItemReceipt[] = [];
      for (const e of entries) {
        const { siblings } = this.#staleSiblings(e);
        const it = await verifyStalePrefix(opts.reader, { identityHash: id, oldPrefix: e.key, enqueuedAt: e.enqueued_at, survivorManifestKeys: siblings });
        items.push(it);
        this.log({ message: "stale prefix verified", identity_hash: id, old_prefix_sha256: it.old_prefix_sha256, status: it.status, reasons: it.reasons, ...it.counts,
          old_versions_sha256: it.old_versions_sha256, ...(it.survivor ? { survivor_pin_sha256: it.survivor.pin_sha256 } : {}) });
      }
      const reasons: Partial<Record<VerifyReason, number>> = {};
      if (!items.length) reasons.no_stale_prefix_entry = 1;
      identities.push({ identity_hash: id, status: items.length && items.every((i) => i.status === "PASS") ? "PASS" : "FAIL", reasons, item_count: items.length, items });
    }
    const target = {
      bucket_sha256: opts.bucketSha256, key_root: this.cfg.keyRoot, allowlist_sha256: allowlistDigest(ids), state_identity: opts.target.stateIdentity,
      verifier_commit: opts.target.verifierCommit, produced_at: new Date(this.now()).toISOString(),
    };
    const receipt = sealReceipt({ v: 1, kind: RECEIPT_KIND, bucket_sha256: opts.bucketSha256, key_root_sha256: verifySha256(this.cfg.keyRoot), target, allowlist: ids, identities });
    this.log({ message: "stale prefix verification", status: receipt.status, receipt_sha256: receipt.receipt_sha256, allowlist_size: ids.length,
      identities_pass: identities.filter((i) => i.status === "PASS").length, identities_fail: identities.filter((i) => i.status === "FAIL").length,
      items: identities.reduce((n, i) => n + i.items.length, 0), allowlist_sha256: target.allowlist_sha256 });
    return receipt;
  }

  /**
   * `--stale-prefixes-only`: ONE pass over the queued session prefixes the exporter retired (kind 'prefix' AND reason
   * 'stale_prefix'), erasing an entry only when the stale-prefix guard proves a live date-moved sibling. No sweep of
   * any kind and no babysitter phase; every other queued entry is left exactly as it is. With `execute` each selected
   * entry goes through the SAME leased per-item path as `drain`; the pass stops after `max` erases and at the first
   * failed erase (recorded as `drain` records it; `ok` false). Without `execute` it reads only: no lease, no queue
   * write, no delete; it reports what is eligible and why the rest would be skipped.
   *
   * `onlyIdentities` (required, 1..100 distinct `staleprefixIdentityHash` values) scopes the pass: an entry whose
   * identity is not listed is skipped `not_in_identity_allowlist` (never leased, never erased, never written), one whose
   * identity cannot be derived `identity_unresolved`; listed entries meet every guard above unchanged.
   */
  async staleprefixPass(opts: StalePrefixPassOptions): Promise<StalePrefixResult> {
    if (this.state.obligations) throw refusal("stale_prefix_mode_requires_obligations_off");
    if (opts.max !== undefined && !(Number.isSafeInteger(opts.max) && opts.max > 0)) throw refusal("stale_prefix_mode_max_invalid");
    if (opts.execute && !opts.probe) throw refusal("stale_prefix_mode_requires_existence_check");
    // Execute erases ONLY through the receipt fence (#fencedBatch): a verified receipt, the store fence, the reader
    // for the mandatory inline re-verification, the bucket and target binding and the owner-approved values.
    if (opts.execute && (!opts.receipt || !opts.fence || !opts.reader || !opts.bucketSha256 || !opts.target)) throw refusal("stale_prefix_mode_requires_verified_receipt");
    if (opts.execute && (opts.approvedAllowlistSha256 === undefined || opts.approvedMax === undefined || opts.approvedIdentityCount === undefined || opts.max === undefined))
      throw refusal("stale_prefix_mode_requires_approval");
    const ids = this.#allowlist(opts.onlyIdentities);
    if (opts.execute) this.#receiptGate(opts, ids);
    // One dense snapshot (#allowlist); every check and the Set use that SAME snapshot, never the caller's array again.
    const allow = new Set(ids);
    const now = this.now();
    const selected = this.state.staleprefixErasures();
    const due = selected.filter((i) => i.next_try <= now);
    const queued = this.state.erasureBacklog(now).queued;
    const skipped: Partial<Record<StalePrefixSkip, number>> = {};
    const identityOf = (i: { key: string; org_id: string }) => staleprefixIdentityHash(this.cfg.keyRoot, i.org_id, i.key);
    const matched = new Set<string>();
    for (const i of selected) {
      const h = identityOf(i);
      if (h !== undefined && allow.has(h)) matched.add(h);
    }
    const res: StalePrefixResult = {
      ok: true, execute: opts.execute, selected: selected.length, due: due.length, backingOff: selected.length - due.length,
      otherEntries: queued - selected.length, eligible: 0, erased: 0, versionsDeleted: 0, skipped, siblingChecked: !!opts.probe,
      otherEraserLeases: this.state.otherEraserLeases(LEASE_HOLDER, now),
      allowlistSize: allow.size, allowlistMatchedIdentities: matched.size, allowlistUnmatched: allow.size - matched.size,
      allowlistEligibleIdentities: 0,
    };
    const eligibleIds = new Set<string>();
    // Another eraser may be mid-run (or crashed less than the lease TTL ago): execute never runs beside it and never
    // releases its leases. Exporter leases do not refuse; those prefixes skip as lease_busy. A dry run only reports.
    if (opts.execute && res.otherEraserLeases > 0) {
      res.ok = false;
      res.refused = "stale_prefix_mode_other_eraser_lease";
      this.log({ message: "stale prefix pass refused", error_code: res.refused, other_eraser_leases: res.otherEraserLeases });
      return res;
    }
    const skip = (reason: StalePrefixSkip, key: string) => {
      skipped[reason] = (skipped[reason] ?? 0) + 1;
      this.log({ message: "stale prefix skipped", reason, key_sha256: keyHash(key), execute: opts.execute });
    };
    if (opts.execute) {
      // The whole batch through the receipt fence: there is no per-item, unfenced or partial-scope execute path.
      for (const id of await this.#fencedBatch(opts, allow, res, selected, skip)) eligibleIds.add(id);
    } else {
      for (const item of due) {
        if (this.stopped()) { res.stoppedBy = "signal"; break; }
        // The allowlist comes before every other check: an unlisted entry is never read further or written.
        const identity = identityOf(item);
        if (identity === undefined) { skip("identity_unresolved", item.key); continue; }
        if (!allow.has(identity)) { skip("not_in_identity_allowlist", item.key); continue; }
        if (!sessionPrefixParts(this.cfg.keyRoot, item.org_id, item.key)) { skip("shape_invalid", item.key); continue; }
        const reason = await this.#staleGuard(item, opts.probe);
        if (reason) skip(reason, item.key);
        else {
          res.eligible += 1;
          eligibleIds.add(identity);
        }
      }
    }
    res.allowlistEligibleIdentities = eligibleIds.size;
    this.log({
      message: "stale prefix pass", execute: opts.execute, ok: res.ok, selected: res.selected, due: res.due, backing_off: res.backingOff,
      other_entries: res.otherEntries, eligible: res.eligible, erased: res.erased, versions_deleted: res.versionsDeleted, skipped,
      sibling_checked: res.siblingChecked, other_eraser_leases: res.otherEraserLeases, allowlist_size: res.allowlistSize,
      allowlist_matched_identities: res.allowlistMatchedIdentities, allowlist_unmatched: res.allowlistUnmatched,
      allowlist_eligible_identities: res.allowlistEligibleIdentities, ...(opts.max !== undefined ? { max: opts.max } : {}), ...(res.stoppedBy ? { stopped_by: res.stoppedBy } : {}),
    });
    return res;
  }

  progress(force = false) {
    const now = this.now();
    if (!force && now - this.#lastProgress < this.cfg.progressEveryMs) return;
    this.#lastProgress = now;
    const queue = this.state.erasureBacklog(now);
    const lastSweep = Number(this.state.meta("erasure_sweep_at") ?? 0);
    const sweepAgeH = lastSweep ? +((now - lastSweep) / 3_600_000).toFixed(2) : null;
    const stuck = this.state.stuckWithKeys();
    const breach =
      (queue.oldestAgeH !== null && queue.oldestAgeH * 3_600_000 > this.cfg.slaWarnMs) ||
      (sweepAgeH !== null && sweepAgeH > 6) ||
      stuck > 0;
    this.log({ message: breach ? "erasure SLA at risk" : "progress", ...this.counters, queue, sweep_age_h: sweepAgeH, stuck_with_keys: stuck });
  }

  async run(opts: { once?: boolean } = {}) {
    this.state.releaseStaleLeases("eraser:", LEASE_HOLDER);
    while (!this.stopped()) {
      if (this.now() - this.#lastSweep >= this.cfg.sweepEveryMs) {
        this.#lastSweep = this.now();
        await this.sweep();
      }
      const handled = await this.drain();
      this.progress();
      if (handled === 0) {
        if (opts.once) break;
        await this.sleep(this.cfg.idleSleepMs);
      }
    }
    this.progress(true);
  }
}
