/**
 * Dormant switch, fixed constants and pure validators for the exact-seven owner-buffered parity workflow
 * (.github/workflows/owner-buffered-parity.yml, scripts/owner-buffered-parity-ci.ts,
 * scripts/owner-buffered-parity-github.ts; docs/owner-buffered-parity-ci.md).
 *
 * While OWNER_PARITY_WRITE_ENABLED is false, `write` refuses owner_parity_write_dormant before any
 * credential is read; `census`, `readback` and the one-time `discovery` (all read-only) are always allowed.
 * `discovery` runs only while the roster is empty and emits the digests for a separate roster PR. Enabling write is a
 * separate reviewed one-line PR. Tenant tuples are private and never appear here, in a workflow input,
 * log line or artifact: the committed roster holds only the operator's per-tenant digests
 * (tenantSha256 in scripts/owner-buffered-parity-operator.ts). The generation is never an input either:
 * census discovers the live PASS marker generation and write re-reads it.
 */
import { createHash } from "node:crypto";

export const OWNER_PARITY_WRITE_ENABLED: boolean = false;

/** The roster must name exactly this many tenant digests, and every one must be found on the target ledger. */
export const OWNER_PARITY_EXACT_COUNT = 7;
/** A census older than this (by its run's created_at and by its capturedAt) cannot feed a write. */
export const OWNER_PARITY_CENSUS_MAX_AGE_MS = 24 * 60 * 60 * 1000;

export const OWNER_PARITY_WORKFLOW = ".github/workflows/owner-buffered-parity.yml";
export const OWNER_PARITY_ROSTER_FILE = "src/migration/owner-buffered-parity-roster.json";
/** Census artifact (retention 1 day) and its single file name, downloaded by `write` from the census run. */
export const OWNER_PARITY_CENSUS_ARTIFACT = "owner-buffered-parity-census";
export const OWNER_PARITY_CENSUS_FILE_NAME = "owner-buffered-parity-census.json";
/** Sanitized aggregate receipt (retention 30 days). */
export const OWNER_PARITY_RECEIPT_ARTIFACT = "owner-buffered-parity";
export const OWNER_PARITY_RECEIPT_FILE_NAME = "owner-buffered-parity.json";

/** The workflow's modes. The operator's `bulk` mode is deliberately absent and refused. */
export const OWNER_PARITY_MODES = ["census", "write", "readback", "discovery"] as const;
export type OwnerParityMode = (typeof OWNER_PARITY_MODES)[number];

/**
 * Per-mode dispatch policy. `readOnly` selects the target session (only `write` gets a writable one) and
 * `roster` selects the roster rule: `seven` is parseOwnerParityRoster (exactly seven digests), `empty` is
 * requireEmptyOwnerParityRoster (discovery is one-time: it refuses roster_present once a roster exists).
 */
export const OWNER_PARITY_MODE_POLICY: Readonly<Record<OwnerParityMode, { readOnly: boolean; roster: "seven" | "empty" }>> =
  Object.freeze({
    census: Object.freeze({ readOnly: true, roster: "seven" as const }),
    write: Object.freeze({ readOnly: false, roster: "seven" as const }),
    readback: Object.freeze({ readOnly: true, roster: "seven" as const }),
    discovery: Object.freeze({ readOnly: true, roster: "empty" as const }),
  });
export const OWNER_PARITY_CENSUS_SOURCES = ["frozen-supabase", "neon"] as const;
export type OwnerParityCensusSource = (typeof OWNER_PARITY_CENSUS_SOURCES)[number];

export const OWNER_PARITY_ROSTER_CONTRACT = "owner-buffered-parity-roster/v1";
export const OWNER_PARITY_CENSUS_CONTRACT = "owner-buffered-parity-census-artifact/v1";
export const OWNER_PARITY_RECEIPT_CONTRACT = "owner-buffered-parity-ci-receipt/v1";
export const OWNER_PARITY_DISCOVERY_CONTRACT = "owner-buffered-parity-discovery/v1";
/** Discovery receipt (retention 30 days): digests and dispositions only on success, counts only on refusal. */
export const OWNER_PARITY_DISCOVERY_ARTIFACT = "owner-buffered-parity-discovery";
export const OWNER_PARITY_DISCOVERY_FILE_NAME = "owner-buffered-parity-discovery.json";

const SHA40 = /^[0-9a-f]{40}$/u;
const HEX64 = /^[0-9a-f]{64}$/u;
const RUN_ID = /^[1-9][0-9]{0,19}$/u;
const CANONICAL_MS = /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$/u;

export class OwnerParityCiRefusal extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "OwnerParityCiRefusal";
  }
}

export function refuseOwnerParity(code: string): never {
  throw new OwnerParityCiRefusal(code);
}

export interface OwnerParityDispatch {
  mode: OwnerParityMode;
  sha: string;
  censusSource: OwnerParityCensusSource;
  /** write only: the census run id; null otherwise. */
  censusRun: string | null;
  confirmation: string;
}

/**
 * Workflow inputs (env OWNER_PARITY_*). Refuses anything but the four modes (so `bulk` is unreachable), a
 * non-40-hex sha, an unknown census source, and, while dormant, `write` before any credential is read.
 * `enabled` exists only for the synthetic proof and tests; production callers never pass it.
 */
export function ownerParityDispatch(
  env: Record<string, string | undefined>,
  enabled: boolean = OWNER_PARITY_WRITE_ENABLED,
): OwnerParityDispatch {
  const mode = env.OWNER_PARITY_MODE ?? "", sha = env.OWNER_PARITY_SHA ?? "",
    censusSource = env.OWNER_PARITY_CENSUS_SOURCE ?? "", censusRun = env.OWNER_PARITY_CENSUS_RUN ?? "",
    confirmation = env.OWNER_PARITY_CONFIRMATION ?? "";
  if (!(OWNER_PARITY_MODES as readonly string[]).includes(mode) || !SHA40.test(sha))
    refuseOwnerParity("owner_parity_inputs");
  if (!(OWNER_PARITY_CENSUS_SOURCES as readonly string[]).includes(censusSource))
    refuseOwnerParity("owner_parity_census_source");
  if (mode === "write" && enabled !== true) refuseOwnerParity("owner_parity_write_dormant");
  if (mode === "write" ? !RUN_ID.test(censusRun) : censusRun !== "") refuseOwnerParity("owner_parity_census_run");
  if (mode === "write" ? !confirmation.startsWith("WRITE-OWNER-PARITY ") : confirmation !== "")
    refuseOwnerParity("owner_parity_confirmation");
  return {
    mode: mode as OwnerParityMode, sha, censusSource: censusSource as OwnerParityCensusSource,
    censusRun: mode === "write" ? censusRun : null, confirmation,
  };
}

/**
 * The committed roster: {"contract", "comment"?, "tenants": [<tenantSha256>...]}. Empty refuses roster_empty;
 * anything but exactly OWNER_PARITY_EXACT_COUNT distinct lowercase digests refuses roster_incomplete.
 */
export function parseOwnerParityRoster(value: unknown): string[] {
  if (value === null || typeof value !== "object" || Array.isArray(value)) refuseOwnerParity("roster_invalid");
  const doc = value as Record<string, unknown>;
  for (const key of Object.keys(doc))
    if (key !== "contract" && key !== "comment" && key !== "tenants") refuseOwnerParity("roster_invalid");
  if (doc.contract !== OWNER_PARITY_ROSTER_CONTRACT || !Array.isArray(doc.tenants) ||
      (doc.comment !== undefined && typeof doc.comment !== "string"))
    refuseOwnerParity("roster_invalid");
  const tenants = doc.tenants as unknown[];
  if (tenants.length === 0) refuseOwnerParity("roster_empty");
  if (tenants.some((t) => typeof t !== "string" || !HEX64.test(t)) || new Set(tenants).size !== tenants.length)
    refuseOwnerParity("roster_invalid");
  if (tenants.length !== OWNER_PARITY_EXACT_COUNT) refuseOwnerParity("roster_incomplete");
  return [...(tenants as string[])];
}

/**
 * Discovery's roster rule: the same strict document shape, but it must be empty. Any listed digest refuses
 * roster_present (discovery is one-time; once a roster exists it is pointless); a malformed document still
 * refuses roster_invalid.
 */
export function requireEmptyOwnerParityRoster(value: unknown): void {
  try {
    parseOwnerParityRoster(value);
  } catch (error) {
    if (error instanceof OwnerParityCiRefusal && error.code === "roster_empty") return;
    if (error instanceof OwnerParityCiRefusal && error.code === "roster_invalid") throw error;
  }
  refuseOwnerParity("roster_present");
}

/**
 * Keep exactly the roster tenants out of enumerated candidates, keyed by digest. Candidates whose digest is
 * not on the roster are dropped here and never touched. Refuses roster_incomplete unless every roster digest
 * was found; returns the kept candidates in roster order.
 */
export function keepRosterTenants<T>(
  candidates: Iterable<{ digest: string; tenant: T }>,
  roster: readonly string[],
): Array<{ digest: string; tenant: T }> {
  const wanted = new Set(roster);
  const found = new Map<string, T>();
  for (const candidate of candidates) {
    if (!wanted.has(candidate.digest)) continue;
    if (found.has(candidate.digest)) refuseOwnerParity("roster_duplicate_candidate");
    found.set(candidate.digest, candidate.tenant);
  }
  if (roster.length !== OWNER_PARITY_EXACT_COUNT || found.size !== OWNER_PARITY_EXACT_COUNT ||
      roster.some((digest) => !found.has(digest)))
    refuseOwnerParity("roster_incomplete");
  return roster.map((digest) => ({ digest, tenant: found.get(digest)! }));
}

export type OwnerParityCensusStatus = "equal" | "mismatch";

export interface OwnerParityCensusEntry {
  tenantSha256: string;
  status: OwnerParityCensusStatus;
  /** The operator's publicCensusReceipt: no tenant identifiers. */
  census: Record<string, unknown>;
}

export interface OwnerParityCensusBody {
  contract: typeof OWNER_PARITY_CENSUS_CONTRACT;
  sourceSha: string;
  generation: string;
  censusSource: OwnerParityCensusSource;
  capturedAt: string;
  censusRun: string;
  tenants: OwnerParityCensusEntry[];
}

export interface OwnerParityCensusArtifact extends OwnerParityCensusBody {
  descriptorSha256: string;
}

const CENSUS_KEYS = ["contract", "sourceSha", "generation", "censusSource", "capturedAt", "censusRun", "tenants",
  "descriptorSha256"].sort();

/** Deterministic JSON: object keys sorted recursively, arrays kept in order. */
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return "[" + value.map(canonicalJson).join(",") + "]";
  if (value !== null && typeof value === "object")
    return "{" + Object.keys(value).sort().filter((k) => (value as Record<string, unknown>)[k] !== undefined)
      .map((k) => JSON.stringify(k) + ":" + canonicalJson((value as Record<string, unknown>)[k])).join(",") + "}";
  return JSON.stringify(value);
}

/** The descriptor binds every field of the census artifact (everything but the descriptor itself). */
export function ownerParityCensusDescriptor(body: OwnerParityCensusBody): string {
  const { descriptorSha256: _ignored, ...rest } = body as OwnerParityCensusBody & { descriptorSha256?: unknown };
  return createHash("sha256").update("owner-buffered-parity-census-descriptor/v1\0")
    .update(canonicalJson(rest)).digest("hex");
}

export function ownerParityConfirmation(census: OwnerParityCensusArtifact): string {
  return `WRITE-OWNER-PARITY ${census.sourceSha} GENERATION ${census.generation} SOURCE ${census.censusSource} ` +
    `DESCRIPTOR ${census.descriptorSha256} CENSUS ${census.censusRun}`;
}

function canonicalMs(value: unknown): number | null {
  if (typeof value !== "string" || !CANONICAL_MS.test(value)) return null;
  const ms = Date.parse(value);
  return Number.isFinite(ms) && new Date(ms).toISOString() === value ? ms : null;
}

/** Strict census artifact shape; the descriptor must recompute byte for byte. */
export function parseOwnerParityCensus(value: unknown): OwnerParityCensusArtifact {
  if (value === null || typeof value !== "object" || Array.isArray(value)) refuseOwnerParity("owner_parity_census_invalid");
  const doc = value as Record<string, unknown>;
  const keys = Object.keys(doc).sort();
  if (keys.length !== CENSUS_KEYS.length || keys.some((k, i) => k !== CENSUS_KEYS[i]) ||
      doc.contract !== OWNER_PARITY_CENSUS_CONTRACT || typeof doc.sourceSha !== "string" || !SHA40.test(doc.sourceSha) ||
      typeof doc.generation !== "string" || !HEX64.test(doc.generation) ||
      !(OWNER_PARITY_CENSUS_SOURCES as readonly unknown[]).includes(doc.censusSource) ||
      canonicalMs(doc.capturedAt) === null || typeof doc.censusRun !== "string" || !RUN_ID.test(doc.censusRun) ||
      typeof doc.descriptorSha256 !== "string" || !HEX64.test(doc.descriptorSha256) || !Array.isArray(doc.tenants) ||
      doc.tenants.length !== OWNER_PARITY_EXACT_COUNT)
    refuseOwnerParity("owner_parity_census_invalid");
  const seen = new Set<string>();
  for (const entry of doc.tenants as unknown[]) {
    if (entry === null || typeof entry !== "object" || Array.isArray(entry)) refuseOwnerParity("owner_parity_census_invalid");
    const e = entry as Record<string, unknown>;
    const census = e.census as Record<string, unknown> | null;
    if (Object.keys(e).sort().join(",") !== "census,status,tenantSha256" || typeof e.tenantSha256 !== "string" ||
        !HEX64.test(e.tenantSha256) || seen.has(e.tenantSha256) || (e.status !== "equal" && e.status !== "mismatch") ||
        census === null || typeof census !== "object" || Array.isArray(census) ||
        census.tenantSha256 !== e.tenantSha256 || census.equal !== (e.status === "equal"))
      refuseOwnerParity("owner_parity_census_invalid");
    seen.add(e.tenantSha256);
  }
  const artifact = doc as unknown as OwnerParityCensusArtifact;
  if (ownerParityCensusDescriptor(artifact) !== artifact.descriptorSha256) refuseOwnerParity("owner_parity_census_descriptor");
  return artifact;
}

/**
 * Everything a write can check before it reads a credential: the census is the one named by the dispatch, at
 * the dispatched sha and source, captured under 24h ago, for exactly the roster in roster order, all equal,
 * and the typed confirmation equals the phrase built from it byte for byte.
 */
export function verifyOwnerParityWriteInputs(
  dispatch: OwnerParityDispatch,
  census: OwnerParityCensusArtifact,
  roster: readonly string[],
  now: number = Date.now(),
): void {
  if (dispatch.mode !== "write") refuseOwnerParity("owner_parity_inputs");
  if (census.sourceSha !== dispatch.sha) refuseOwnerParity("owner_parity_census_sha_mismatch");
  if (census.censusRun !== dispatch.censusRun) refuseOwnerParity("owner_parity_census_run_mismatch");
  if (census.censusSource !== dispatch.censusSource) refuseOwnerParity("owner_parity_census_source_mismatch");
  const captured = canonicalMs(census.capturedAt)!;
  if (captured > now || now - captured >= OWNER_PARITY_CENSUS_MAX_AGE_MS) refuseOwnerParity("owner_parity_census_stale");
  if (census.tenants.length !== roster.length || census.tenants.some((t, i) => t.tenantSha256 !== roster[i]))
    refuseOwnerParity("owner_parity_census_roster_mismatch");
  if (census.tenants.some((t) => t.status !== "equal")) refuseOwnerParity("owner_parity_census_not_equal");
  if (dispatch.confirmation !== ownerParityConfirmation(census)) refuseOwnerParity("owner_parity_confirmation");
}

// ---------------------------------------------------------------------------------------------------------------
// Discovery (read-only, one-time): which buffered owners lack a canonical current parity row.
// ---------------------------------------------------------------------------------------------------------------

/** The readback vocabulary for an owner without a canonical current row. */
export type OwnerParityDisposition = "missing" | "stale";

export interface OwnerParityDiscoveryCounts {
  /** Buffered owners read from the snapshot cursor. */
  scanned: number;
  /** Owners whose stored row equals the fresh equal proof (readback `equal`). */
  canonicalCurrent: number;
  missing: number;
  stale: number;
  /** Cursor rows that are not a valid tenant tuple; any one refuses unexpected_scope. */
  unparsable: number;
}

export interface OwnerParityDiscoveryOwner {
  tenantSha256: string;
  disposition: OwnerParityDisposition;
}

export interface OwnerParityDiscoveryReceipt {
  contract: typeof OWNER_PARITY_DISCOVERY_CONTRACT;
  status: "ok" | "refused";
  code?: string;
  runId: string;
  sourceSha: string | null;
  generation: string | null;
  censusSource: OwnerParityCensusSource | null;
  counts: OwnerParityDiscoveryCounts;
  /** Sorted by digest; always empty unless status is ok. */
  owners: OwnerParityDiscoveryOwner[];
  capturedAt: string;
  credentialsPersisted: false;
}

export function emptyOwnerParityDiscoveryCounts(): OwnerParityDiscoveryCounts {
  return { scanned: 0, canonicalCurrent: 0, missing: 0, stale: 0, unparsable: 0 };
}

/**
 * The live scope must be exactly OWNER_PARITY_EXACT_COUNT missing/stale owners (both dispositions together),
 * every scanned row classified, and the owner list exactly those distinct digests. Anything else refuses
 * unexpected_scope; the caller then emits counts only.
 */
export function requireOwnerParityDiscoveryScope(
  counts: OwnerParityDiscoveryCounts,
  owners: readonly OwnerParityDiscoveryOwner[],
): void {
  const missing = owners.filter((o) => o.disposition === "missing").length;
  if (counts.missing + counts.stale !== OWNER_PARITY_EXACT_COUNT || counts.unparsable !== 0 ||
      counts.scanned !== counts.canonicalCurrent + counts.missing + counts.stale ||
      owners.length !== OWNER_PARITY_EXACT_COUNT || new Set(owners.map((o) => o.tenantSha256)).size !== owners.length ||
      owners.some((o) => !HEX64.test(o.tenantSha256)) || missing !== counts.missing ||
      owners.length - missing !== counts.stale)
    refuseOwnerParity("unexpected_scope");
}

const DISCOVERY_KEYS = ["contract", "status", "runId", "sourceSha", "generation", "censusSource", "counts", "owners",
  "capturedAt", "credentialsPersisted"];
const DISCOVERY_COUNT_KEYS = ["canonicalCurrent", "missing", "scanned", "stale", "unparsable"];
const CODE = /^[A-Za-z0-9_.:,-]{1,160}$/u;

/**
 * Strict discovery receipt shape (what the job summary step publishes): only the listed keys, hex digests,
 * the two dispositions, non-negative integer counts, owners only on ok (and then exactly the scope), none on
 * refusal. Anything else refuses owner_parity_discovery_receipt_invalid.
 */
export function parseOwnerParityDiscoveryReceipt(value: unknown): OwnerParityDiscoveryReceipt {
  const bad = () => refuseOwnerParity("owner_parity_discovery_receipt_invalid");
  if (value === null || typeof value !== "object" || Array.isArray(value)) bad();
  const doc = value as Record<string, unknown>;
  const keys = Object.keys(doc).sort();
  const expected = (doc.status === "refused" ? [...DISCOVERY_KEYS, "code"] : DISCOVERY_KEYS).sort();
  if (keys.join(",") !== expected.join(",") || doc.contract !== OWNER_PARITY_DISCOVERY_CONTRACT ||
      (doc.status !== "ok" && doc.status !== "refused") ||
      (doc.status === "refused" && (typeof doc.code !== "string" || !CODE.test(doc.code))) ||
      typeof doc.runId !== "string" || !/^(0|[1-9][0-9]{0,19})$/u.test(doc.runId) ||
      !(doc.sourceSha === null || (typeof doc.sourceSha === "string" && SHA40.test(doc.sourceSha))) ||
      !(doc.generation === null || (typeof doc.generation === "string" && HEX64.test(doc.generation))) ||
      !(doc.censusSource === null || (OWNER_PARITY_CENSUS_SOURCES as readonly unknown[]).includes(doc.censusSource)) ||
      canonicalMs(doc.capturedAt) === null || doc.credentialsPersisted !== false || !Array.isArray(doc.owners))
    bad();
  const counts = doc.counts as Record<string, unknown> | null;
  if (counts === null || typeof counts !== "object" || Array.isArray(counts) ||
      Object.keys(counts).sort().join(",") !== DISCOVERY_COUNT_KEYS.join(",") ||
      Object.values(counts).some((n) => typeof n !== "number" || !Number.isSafeInteger(n) || n < 0))
    bad();
  const owners = doc.owners as unknown[];
  for (const owner of owners) {
    const o = owner as Record<string, unknown> | null;
    if (o === null || typeof o !== "object" || Array.isArray(o) || Object.keys(o).sort().join(",") !== "disposition,tenantSha256" ||
        typeof o.tenantSha256 !== "string" || !HEX64.test(o.tenantSha256) || (o.disposition !== "missing" && o.disposition !== "stale"))
      bad();
  }
  const receipt = doc as unknown as OwnerParityDiscoveryReceipt;
  if (receipt.status === "refused") {
    if (owners.length !== 0) bad();
  } else {
    if (receipt.sourceSha === null || receipt.generation === null || receipt.censusSource === null) bad();
    const sorted = [...receipt.owners].map((o) => o.tenantSha256);
    if (sorted.some((d, i) => i > 0 && sorted[i - 1]! >= d)) bad();
    try {
      requireOwnerParityDiscoveryScope(receipt.counts, receipt.owners);
    } catch {
      bad();
    }
  }
  return receipt;
}
