/**
 * CI wrapper for .github/workflows/owner-buffered-parity.yml: the exact-seven owner-buffered parity census
 * (G1), the dormant G2 write and the read-only readback (docs/owner-buffered-parity-ci.md).
 *
 * It adds no parity SQL. Tenants are enumerated with the operator's bulk cursor SQL
 * (BULK_BUFFERED_TENANTS_SQL, read-only repeatable read), kept only when their tenantSha256 is on the committed
 * roster (src/migration/owner-buffered-parity-roster.json), and each kept tenant then goes through the
 * single-tenant path (readParityCensus, writeCurrentParityProof) in roster order. The operator's bulk runner is
 * never imported and `bulk` is not a mode here.
 *
 * Credentials. The workflow step runs `bun scripts/owner-buffered-parity-ci.ts` with SUPABASE_MIGRATION_OWNER_URL
 * in its environment. This parent first runs every check that needs no credential (mode, dormant switch, roster,
 * census artifact, confirmation, checkout pin), then removes the URL from its own environment and spawns
 * `bun scripts/owner-buffered-parity-ci.ts --operator-stdin` with an allowlisted environment and fixed argv, and
 * writes {"supabaseUrl": ...} to the child's stdin pipe from its own memory. The child refuses a credential in
 * its environment and reads the URL only through readStdinCredentials. Nothing credential-like is logged or
 * written: the receipt and the census artifact carry digests, counts and public proof fields only.
 *
 * Discovery (one-time, read-only, only while the roster is empty, frozen-supabase source only) holds ONE
 * read-only repeatable-read snapshot on the enumerator session from the in-snapshot marker read through the
 * same cursor, the canonical readback comparison of every owner (missing / stale / canonical current), the
 * exact-seven check and a final in-snapshot marker read, fenced by generation reads on the target before and
 * after. It emits only digests and dispositions of exactly seven missing-or-stale owners, so a separate
 * reviewed roster-only PR can be written from the receipt. It never writes, never selects a writable session
 * and refuses roster_present, generation_unstable and unexpected_scope (counts only on refusal).
 *
 * `--proof` runs the synthetic PGlite proof (all migrations, 7 roster tenants + 2 others) with no credentials.
 * `--discovery-summary` (no credentials) re-validates the discovery receipt and appends it to the job summary.
 */
import { spawn } from "node:child_process";
import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import {
  BULK_BUFFERED_TENANTS_SQL,
  BULK_CURSOR,
  BULK_TARGET_GENERATION_SQL,
  OwnerBufferedOperatorRefusal,
  connectNeon,
  generationMatches,
  operatorCredentialKeys,
  parseTenant,
  publicCensusReceipt,
  requireReadOnly,
  tenantSha256,
  validateCensusSourceInput,
  verifyCheckout,
} from "./owner-buffered-parity-operator.ts";
import { releaseClient } from "../src/hoopdb/schema-release-inspect.ts";
import { SCHEMA_RELEASE } from "../src/hoopdb/schema-release-contract.ts";
import { roleAudit, type ConnectedOwnerClient } from "../src/hoopdb/owner-operator-apply.ts";
import {
  ParityCensusRefusal,
  READ_OWNER_BUFFERED_PARITY_SQL,
  readParityCensus,
  storedProof,
  writeCurrentParityProof,
  type ParityCensusSource,
  type ParityTenant,
} from "../src/migration/owner-buffered-parity.ts";
import {
  CredentialRefusal,
  pinnedPgConfig,
  readStdinCredentials,
  refuseCredentialEnv,
  sessionAffinityUrl,
} from "../src/migration/stdin-credentials.ts";
import {
  OWNER_PARITY_CENSUS_CONTRACT,
  OWNER_PARITY_CENSUS_FILE_NAME,
  OWNER_PARITY_DISCOVERY_CONTRACT,
  OWNER_PARITY_DISCOVERY_FILE_NAME,
  OWNER_PARITY_EXACT_COUNT,
  OWNER_PARITY_MODE_POLICY,
  OWNER_PARITY_MODES,
  OWNER_PARITY_RECEIPT_CONTRACT,
  OWNER_PARITY_RECEIPT_FILE_NAME,
  OWNER_PARITY_ROSTER_FILE,
  OWNER_PARITY_WRITE_ENABLED,
  OwnerParityCiRefusal,
  canonicalJson,
  emptyOwnerParityDiscoveryCounts,
  keepRosterTenants,
  ownerParityCensusDescriptor,
  ownerParityDispatch,
  parseOwnerParityCensus,
  parseOwnerParityDiscoveryReceipt,
  parseOwnerParityRoster,
  refuseOwnerParity,
  requireEmptyOwnerParityRoster,
  requireOwnerParityDiscoveryScope,
  verifyOwnerParityWriteInputs,
  type OwnerParityCensusArtifact,
  type OwnerParityCensusBody,
  type OwnerParityCensusEntry,
  type OwnerParityDiscoveryCounts,
  type OwnerParityDiscoveryOwner,
  type OwnerParityDiscoveryReceipt,
  type OwnerParityDisposition,
  type OwnerParityDispatch,
} from "../src/migration/owner-buffered-parity-ci-contract.ts";
import type { Queryable } from "../src/migration/relayhistory-export.ts";

const ROOT = join(import.meta.dir, "..");
const HEX64 = /^[0-9a-f]{64}$/u;
const FETCH_PAGE = 100;
/** The workflow secret. Only the parent's own environment may hold it; it reaches the child over stdin. */
export const OWNER_PARITY_SECRET_ENV = "SUPABASE_MIGRATION_OWNER_URL";
/** The child's whole environment: no secret, no token. */
export const OWNER_PARITY_CHILD_ENV = [
  "PATH", "HOME", "LANG", "TMPDIR", "RUNNER_TEMP", "GITHUB_RUN_ID", "GITHUB_SHA",
  "OWNER_PARITY_MODE", "OWNER_PARITY_SHA", "OWNER_PARITY_CENSUS_SOURCE", "OWNER_PARITY_CENSUS_RUN",
  "OWNER_PARITY_CONFIRMATION",
] as const;
const CHILD_FLAG = "--operator-stdin";
const SUMMARY_FLAG = "--discovery-summary";

export interface OwnerParitySessions {
  /** Source-side reader: the fenced read-only frozen-copy owner session (or a read-only Neon session). */
  source: Queryable;
  /** Fenced read-only owner session holding the enumeration cursor. */
  enumerator: Queryable;
  /** Fenced target owner session; read-only except in write. */
  target: Queryable;
}

export interface OwnerParityDependencies {
  readCensus?: typeof readParityCensus;
  writeProof?: typeof writeCurrentParityProof;
}

export interface OwnerParityRunOptions {
  /** Raw roster document (the committed JSON). */
  roster: unknown;
  /** This run's id; becomes censusRun in a census artifact. */
  runId: string;
  /** write only: the downloaded census artifact, raw. */
  census?: unknown;
  now?: () => number;
  /** Test/proof injection only. Production never passes it; the workflow cannot set it. */
  writeEnabled?: boolean;
  deps?: OwnerParityDependencies;
}

export type OwnerParityTenantStatus =
  | "equal" | "mismatch" | "stale" | "missing" | "inserted" | "updated" | "unchanged" | "refused" | "not_attempted";

export interface OwnerParityCounts {
  roster: number;
  found: number;
  equal: number;
  mismatch: number;
  stale: number;
  missing: number;
  inserted: number;
  updated: number;
  unchanged: number;
  verified: number;
}

export interface OwnerParityReceipt {
  contract: typeof OWNER_PARITY_RECEIPT_CONTRACT;
  status: "ok" | "mismatch" | "refused";
  code?: string;
  mode: string;
  runId: string;
  sourceSha: string | null;
  generation: string | null;
  censusSource: string | null;
  counts: OwnerParityCounts;
  tenants: Array<{ tenantSha256: string; status: OwnerParityTenantStatus }>;
  at: string;
  credentialsPersisted: false;
}

export interface OwnerParityOutcome {
  receipt: OwnerParityReceipt;
  /** census mode, all seven equal: the artifact to publish. */
  census: OwnerParityCensusArtifact | null;
  exitCode: number;
}

interface Kept { digest: string; tenant: ParityTenant }

function emptyCounts(): OwnerParityCounts {
  return { roster: 0, found: 0, equal: 0, mismatch: 0, stale: 0, missing: 0, inserted: 0, updated: 0, unchanged: 0,
    verified: 0 };
}

/** Fixed codes only; anything else (driver text could carry a host) collapses to a constant. */
export function ownerParityCode(error: unknown): string {
  let code: string;
  if (error instanceof OwnerParityCiRefusal || error instanceof OwnerBufferedOperatorRefusal ||
      error instanceof ParityCensusRefusal || error instanceof CredentialRefusal) {
    code = error.code;
  } else {
    const driverCode = typeof error === "object" && error !== null && "code" in error
      ? (error as { code?: unknown }).code : undefined;
    // PostgreSQL SQLSTATE is a fixed five-character protocol field. It contains no query, host, role,
    // identifier or server message, so it is safe evidence for a fail-closed production diagnostic.
    if (typeof driverCode === "string" && /^[0-9A-Z]{5}$/u.test(driverCode)) {
      code = `owner_parity_pg_${driverCode}`;
    } else if (typeof driverCode === "string" && [
      "ECONNREFUSED", "ECONNRESET", "EPIPE", "ETIMEDOUT", "ENOTFOUND", "EAI_AGAIN",
    ].includes(driverCode)) {
      // Node's fixed connection classes are also content-free; arbitrary error.code values remain redacted.
      code = `owner_parity_connection_${driverCode.toLowerCase()}`;
    } else {
      code = "owner_parity_failed";
    }
  }
  return /^[A-Za-z0-9_.:,-]{1,160}$/u.test(code) ? code : "owner_parity_failed";
}

interface WorkflowOwnerIdentity {
  actor: string;
  login: string;
  db: string;
  pid: number;
  ro: string;
  schemaOwner: string;
  parityOwner: string;
  rolcanlogin: boolean;
  rolsuper: boolean;
  rolcreaterole: boolean;
  rolcreatedb: boolean;
  rolbypassrls: boolean;
  rolreplication: boolean;
}

/**
 * The CI workflow intentionally reuses SUPABASE_MIGRATION_OWNER_URL, whose reviewed identity is the direct
 * project owner (`postgres.<project>`). The standalone operator keeps its distinct short-lived
 * `cli_login_postgres` route. Fence the workflow identity, backend, role attributes and owned objects before and
 * after every application query so the two credential contracts can never be confused again.
 */
async function requireWorkflowOwnerIdentity(
  client: Queryable,
  pid?: number,
  readOnly?: boolean,
): Promise<WorkflowOwnerIdentity> {
  let rows: WorkflowOwnerIdentity[];
  try {
    rows = (await client.query<WorkflowOwnerIdentity>(`
      SELECT current_user AS actor,session_user AS login,current_database() AS db,
             pg_backend_pid() AS pid,current_setting('transaction_read_only') AS ro,
             pg_get_userbyid(n.nspowner) AS "schemaOwner",pg_get_userbyid(c.relowner) AS "parityOwner",
             r.rolcanlogin,r.rolsuper,r.rolcreaterole,r.rolcreatedb,r.rolbypassrls,r.rolreplication
        FROM pg_roles r
        JOIN pg_namespace n ON n.nspname='sessions'
        JOIN pg_class c ON c.relnamespace=n.oid AND c.relname='owner_buffered_parity' AND c.relkind='r'
       WHERE r.rolname=current_user`)).rows;
  } catch {
    throw new OwnerBufferedOperatorRefusal("workflow_owner_identity_unverifiable");
  }
  const row = rows[0];
  if (rows.length !== 1 || row?.actor !== SCHEMA_RELEASE.owner || row.login !== SCHEMA_RELEASE.owner ||
      row.db !== SCHEMA_RELEASE.database || row.schemaOwner !== SCHEMA_RELEASE.owner ||
      row.parityOwner !== SCHEMA_RELEASE.owner || !Number.isInteger(row.pid) ||
      (pid !== undefined && row.pid !== pid) || (readOnly !== undefined && row.ro !== (readOnly ? "on" : "off")) ||
      row.rolcanlogin !== true || row.rolsuper !== false || row.rolcreaterole !== true ||
      row.rolcreatedb !== true || row.rolbypassrls !== true || row.rolreplication !== true)
    throw new OwnerBufferedOperatorRefusal("workflow_owner_identity_refused");
  return row;
}

/** Exact direct-owner workflow session; never used by the standalone temporary-login operator. */
export async function withWorkflowParityOwnerSession<T>(
  target: ConnectedOwnerClient,
  readOnly: boolean,
  work: (target: Queryable) => Promise<T>,
): Promise<T> {
  let connected = false;
  try {
    await target.connect();
    connected = true;
    if (readOnly) await target.query("SET default_transaction_read_only = on");
    const identity = await requireWorkflowOwnerIdentity(target, undefined, readOnly);
    const fenced: Queryable = {
      async query<R>(sql: string, params?: unknown[]) {
        const cleanup = /^\s*ROLLBACK\s*;?\s*$/iu.test(sql);
        if (!cleanup) {
          roleAudit(sql);
          await requireWorkflowOwnerIdentity(target, identity.pid, readOnly);
        }
        const result = await target.query<R>(sql, params);
        if (!cleanup) await requireWorkflowOwnerIdentity(target, identity.pid, readOnly);
        return result;
      },
    };
    return await work(fenced);
  } finally {
    try {
      if (connected) await target.query("ROLLBACK").catch(() => undefined);
    } finally {
      await target.end().catch(() => undefined);
    }
  }
}

/**
 * Every check that needs no credential: inputs and the dormant switch, the roster, and for write the census
 * artifact and the typed confirmation. Runs in the parent before the secret is touched and again in the child.
 */
export function ownerParityPreflight(
  env: Record<string, string | undefined>,
  roster: unknown,
  census: unknown,
  enabled: boolean = OWNER_PARITY_WRITE_ENABLED,
  now: number = Date.now(),
): { dispatch: OwnerParityDispatch; roster: string[]; census: OwnerParityCensusArtifact | null } {
  const dispatch = ownerParityDispatch(env, enabled);
  if (OWNER_PARITY_MODE_POLICY[dispatch.mode].roster === "empty") {
    // Discovery: one-time, so it needs the empty roster and refuses roster_present once digests are listed.
    requireEmptyOwnerParityRoster(roster);
    // One snapshot covers the owner list and every comparison only when the source is the frozen copy in the
    // target project; a Neon source would be a second database (and no Neon secret exists).
    if (dispatch.censusSource !== "frozen-supabase") refuseOwnerParity("owner_parity_discovery_census_source");
    return { dispatch, roster: [], census: null };
  }
  const digests = parseOwnerParityRoster(roster);
  if (dispatch.mode !== "write") return { dispatch, roster: digests, census: null };
  const artifact = parseOwnerParityCensus(census);
  verifyOwnerParityWriteInputs(dispatch, artifact, digests, now);
  return { dispatch, roster: digests, census: artifact };
}

/** The live PASS marker generation; discovered, never an input. */
export async function readLiveGeneration(target: Queryable): Promise<string> {
  const rows = (await target.query<{ generation: unknown }>(BULK_TARGET_GENERATION_SQL)).rows;
  if (rows.length !== 1 || typeof rows[0]?.generation !== "string" || !HEX64.test(rows[0].generation))
    refuseOwnerParity("owner_parity_generation_unavailable");
  return rows[0].generation;
}

/**
 * One read-only repeatable-read snapshot of the target's buffered tenants (the operator's bulk cursor SQL),
 * filtered to roster digests as each bounded page arrives. Unlisted tenants are hashed and dropped; nothing
 * else is done with them. Refuses roster_incomplete unless all seven are found.
 */
export async function enumerateRosterTenants(enumerator: Queryable, roster: readonly string[]): Promise<Kept[]> {
  const wanted = new Set(roster);
  const candidates: Kept[] = [];
  await enumerator.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    await enumerator.query("SET LOCAL statement_timeout = '30s'");
    await enumerator.query(`DECLARE ${BULK_CURSOR} NO SCROLL CURSOR FOR ${BULK_BUFFERED_TENANTS_SQL}`);
    for (;;) {
      const rows = (await enumerator.query<Record<string, unknown>>(`FETCH FORWARD ${FETCH_PAGE} FROM ${BULK_CURSOR}`)).rows;
      if (rows.length > FETCH_PAGE) refuseOwnerParity("owner_parity_page_too_large");
      if (rows.length === 0) break;
      for (const row of rows) {
        let tenant: ParityTenant;
        try {
          tenant = parseTenant({ orgId: row.org_id, workspaceId: row.workspace_id, userId: row.user_id });
        } catch {
          continue;
        }
        const digest = tenantSha256(tenant);
        if (wanted.has(digest)) candidates.push({ digest, tenant });
      }
    }
    await enumerator.query(`CLOSE ${BULK_CURSOR}`);
    await enumerator.query("COMMIT");
  } catch (error) {
    await enumerator.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
  return keepRosterTenants(candidates, roster);
}

/**
 * G2 for the kept roster, in roster order: fresh G1 (equal, at the census generation and byte-equal to the
 * census entry), writeCurrentParityProof, then same-run exact readback of the stored row. The first refusal
 * stops the loop; rows already written stay and are reported. Fewer than seven verified rows fails closed.
 */
export async function writeOwnerParityTenants(
  sessions: OwnerParitySessions,
  kept: readonly Kept[],
  census: OwnerParityCensusArtifact,
  generation: string,
  counts: OwnerParityCounts,
  statuses: Map<string, OwnerParityTenantStatus>,
  deps: OwnerParityDependencies = {},
): Promise<void> {
  const readCensus = deps.readCensus ?? readParityCensus;
  const writeProof = deps.writeProof ?? writeCurrentParityProof;
  if (kept.length !== OWNER_PARITY_EXACT_COUNT) refuseOwnerParity("owner_parity_write_incomplete");
  const entries = new Map(census.tenants.map((entry) => [entry.tenantSha256, entry]));
  for (const { digest, tenant } of kept) {
    statuses.set(digest, "refused");
    const entry = entries.get(digest);
    if (!entry) refuseOwnerParity("owner_parity_census_roster_mismatch");
    const g1 = await readCensus(sessions.source, sessions.target, tenant, census.censusSource);
    if (!g1.equal || !g1.proof) refuseOwnerParity("owner_parity_write_parity_mismatch");
    if (!generationMatches(g1, generation)) refuseOwnerParity("owner_parity_generation_mismatch");
    if (canonicalJson(publicCensusReceipt(g1)) !== canonicalJson(entry.census)) refuseOwnerParity("owner_parity_census_drift");
    const result = await writeProof(sessions.source, sessions.target, tenant, generation, census.censusSource);
    if (!generationMatches(result.census, generation)) refuseOwnerParity("owner_parity_generation_postcondition");
    if (result.outcome === "insert") { counts.inserted++; statuses.set(digest, "inserted"); }
    else if (result.outcome === "update") { counts.updated++; statuses.set(digest, "updated"); }
    else { counts.unchanged++; statuses.set(digest, "unchanged"); }
    const stored = storedProof(tenant, (await sessions.target.query<Record<string, unknown>>(
      READ_OWNER_BUFFERED_PARITY_SQL, [tenant.orgId, tenant.workspaceId, tenant.userId])).rows[0]);
    const after = await readCensus(sessions.source, sessions.target, tenant, census.censusSource);
    if (stored === null || !after.equal || !after.proof || canonicalJson(stored) !== canonicalJson(after.proof) ||
        canonicalJson(stored) !== canonicalJson(result.census.proof))
      refuseOwnerParity("owner_parity_readback_mismatch");
    counts.verified++;
  }
  if (counts.verified !== OWNER_PARITY_EXACT_COUNT) refuseOwnerParity("owner_parity_write_incomplete");
}

/**
 * The workflow's database work over three sessions. Never throws a refusal: it returns a sanitized receipt
 * (status refused + fixed code) so rows already written are still reported.
 */
export async function runOwnerParity(
  env: Record<string, string | undefined>,
  sessions: OwnerParitySessions,
  options: OwnerParityRunOptions,
): Promise<OwnerParityOutcome> {
  const now = options.now ?? Date.now;
  const counts = emptyCounts();
  const statuses = new Map<string, OwnerParityTenantStatus>();
  let dispatch: OwnerParityDispatch | null = null;
  let generation: string | null = null;
  let censusOut: OwnerParityCensusArtifact | null = null;
  const receipt = (status: OwnerParityReceipt["status"], code?: string): OwnerParityReceipt => ({
    contract: OWNER_PARITY_RECEIPT_CONTRACT, status, ...(code === undefined ? {} : { code }),
    mode: dispatch?.mode ?? "refused", runId: /^[1-9][0-9]{0,19}$/u.test(options.runId) ? options.runId : "0",
    sourceSha: dispatch?.sha ?? null, generation, censusSource: dispatch?.censusSource ?? null, counts,
    tenants: [...statuses].map(([tenantSha256, s]) => ({ tenantSha256, status: s })),
    at: new Date(now()).toISOString(), credentialsPersisted: false,
  });
  try {
    const pre = ownerParityPreflight(env, options.roster, options.census,
      options.writeEnabled ?? OWNER_PARITY_WRITE_ENABLED, now());
    dispatch = pre.dispatch;
    if (dispatch.mode === "discovery") refuseOwnerParity("owner_parity_mode_route");
    counts.roster = pre.roster.length;
    for (const digest of pre.roster) statuses.set(digest, "not_attempted");
    const readCensus = options.deps?.readCensus ?? readParityCensus;
    generation = await readLiveGeneration(sessions.target);
    if (pre.census && pre.census.generation !== generation) refuseOwnerParity("owner_parity_generation_mismatch");
    const kept = await enumerateRosterTenants(sessions.enumerator, pre.roster);
    counts.found = kept.length;

    if (dispatch.mode === "write") {
      await writeOwnerParityTenants(sessions, kept, pre.census!, generation, counts, statuses, options.deps);
    } else {
      const entries: OwnerParityCensusEntry[] = [];
      for (const { digest, tenant } of kept) {
        const g1 = await readCensus(sessions.source, sessions.target, tenant, dispatch.censusSource);
        if (g1.equal && g1.proof && !generationMatches(g1, generation)) refuseOwnerParity("owner_parity_generation_mismatch");
        if (dispatch.mode === "census") {
          const status = g1.equal && g1.proof ? "equal" : "mismatch";
          counts[status]++;
          statuses.set(digest, status);
          entries.push({ tenantSha256: digest, status, census: publicCensusReceipt(g1) });
        } else {
          const stored = storedProof(tenant, (await sessions.target.query<Record<string, unknown>>(
            READ_OWNER_BUFFERED_PARITY_SQL, [tenant.orgId, tenant.workspaceId, tenant.userId])).rows[0]);
          const status = stored === null ? "missing"
            : g1.equal && g1.proof && canonicalJson(stored) === canonicalJson(g1.proof) ? "equal" : "stale";
          counts[status]++;
          statuses.set(digest, status);
        }
      }
      if (await readLiveGeneration(sessions.target) !== generation) refuseOwnerParity("owner_parity_generation_mismatch");
      if (dispatch.mode === "census" && counts.equal === OWNER_PARITY_EXACT_COUNT) {
        const body: OwnerParityCensusBody = {
          contract: OWNER_PARITY_CENSUS_CONTRACT, sourceSha: dispatch.sha, generation,
          censusSource: dispatch.censusSource, capturedAt: new Date(now()).toISOString(), censusRun: options.runId,
          tenants: entries,
        };
        censusOut = { ...body, descriptorSha256: ownerParityCensusDescriptor(body) };
      }
      if (counts.equal !== OWNER_PARITY_EXACT_COUNT) return { receipt: receipt("mismatch"), census: null, exitCode: 3 };
    }
    return { receipt: receipt("ok"), census: censusOut, exitCode: 0 };
  } catch (error) {
    return { receipt: receipt("refused", ownerParityCode(error)), census: null, exitCode: 1 };
  }
}


// ---------------------------------------------------------------------------------------------------------------
// Discovery: one read-only snapshot of every buffered owner, classified by the canonical readback comparison.
// ---------------------------------------------------------------------------------------------------------------

export interface OwnerParityDiscoveryOutcome {
  receipt: OwnerParityDiscoveryReceipt;
  exitCode: number;
}

export interface OwnerParityDiscoveryDependencies {
  readCensus?: typeof readParityCensus;
  /**
   * Test injection only (production never passes it): runs inside the snapshot after the cursor is closed and
   * before the first comparison, so a causal test can commit a concurrent change at exactly that point.
   */
  afterEnumeration?: () => Promise<void>;
}

export interface OwnerParityDiscoveryOptions {
  /** Raw roster document (the committed JSON); must be the empty roster. */
  roster: unknown;
  runId: string;
  now?: () => number;
  deps?: OwnerParityDiscoveryDependencies;
}

/** Discovery's census reads join the snapshot the caller holds; no other mode ever passes this. */
const DISCOVERY_SNAPSHOT_READ = Object.freeze({ transaction: "caller" as const });

/**
 * The operator's bulk cursor SQL in bounded pages, inside the caller's open transaction; the cursor is closed
 * before this returns. Rows that are not a valid tenant tuple are only counted.
 */
export async function scanBufferedOwners(snapshot: Queryable): Promise<{ tenants: ParityTenant[]; unparsable: number }> {
  const tenants: ParityTenant[] = [];
  let unparsable = 0;
  await snapshot.query(`DECLARE ${BULK_CURSOR} NO SCROLL CURSOR FOR ${BULK_BUFFERED_TENANTS_SQL}`);
  for (;;) {
    const rows = (await snapshot.query<Record<string, unknown>>(`FETCH FORWARD ${FETCH_PAGE} FROM ${BULK_CURSOR}`)).rows;
    if (rows.length > FETCH_PAGE) refuseOwnerParity("owner_parity_page_too_large");
    if (rows.length === 0) break;
    for (const row of rows) {
      try {
        tenants.push(parseTenant({ orgId: row.org_id, workspaceId: row.workspace_id, userId: row.user_id }));
      } catch {
        unparsable++;
      }
    }
  }
  await snapshot.query(`CLOSE ${BULK_CURSOR}`);
  return { tenants, unparsable };
}

/**
 * The whole classification in ONE `REPEATABLE READ READ ONLY` transaction on one session (the fenced read-only
 * enumerator): the PASS marker generation as the snapshot's first read (must equal `generation`), the bounded
 * cursor, every canonical comparison (readParityCensus source/target reads and the stored-proof read, all with
 * DISCOVERY_SNAPSHOT_READ, so none opens or ends a transaction), the exact-seven check and a final marker read,
 * then COMMIT. Every disposition therefore comes from the same snapshot as the owner list. Any refusal rolls back.
 * `counts` and `found` are filled in place so a refused receipt can still report counts.
 */
export async function discoverOwnersInSnapshot(
  snapshot: Queryable,
  generation: string,
  censusSource: ParityCensusSource,
  counts: OwnerParityDiscoveryCounts,
  found: OwnerParityDiscoveryOwner[],
  deps: OwnerParityDiscoveryDependencies = {},
): Promise<void> {
  const readCensus = deps.readCensus ?? readParityCensus;
  await snapshot.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    // Per statement, for the whole transaction; the census reads add no SET LOCAL of their own.
    await snapshot.query("SET LOCAL statement_timeout = '30s'");
    if (await readLiveGeneration(snapshot) !== generation) refuseOwnerParity("generation_unstable");
    const scan = await scanBufferedOwners(snapshot);
    counts.scanned = scan.tenants.length + scan.unparsable;
    counts.unparsable = scan.unparsable;
    await deps.afterEnumeration?.();
    for (const tenant of scan.tenants) {
      const g1 = await readCensus(snapshot, snapshot, tenant, censusSource, DISCOVERY_SNAPSHOT_READ);
      if (g1.equal && g1.proof && !generationMatches(g1, generation)) refuseOwnerParity("generation_unstable");
      const stored = storedProof(tenant, (await snapshot.query<Record<string, unknown>>(
        READ_OWNER_BUFFERED_PARITY_SQL, [tenant.orgId, tenant.workspaceId, tenant.userId])).rows[0]);
      const status = stored === null ? "missing"
        : g1.equal && g1.proof && canonicalJson(stored) === canonicalJson(g1.proof) ? "equal" : "stale";
      if (status === "equal") { counts.canonicalCurrent++; continue; }
      counts[status]++;
      found.push({ tenantSha256: tenantSha256(tenant), disposition: status satisfies OwnerParityDisposition });
      // Fail fast: the scope is already wrong, so stop paying four statements per remaining owner; counts only on refusal.
      if (counts.missing + counts.stale > OWNER_PARITY_EXACT_COUNT) refuseOwnerParity("unexpected_scope");
    }
    requireOwnerParityDiscoveryScope(counts, found);
    // The closing marker read, still in the snapshot: it can differ only if a read escaped the snapshot.
    if (generation !== await readLiveGeneration(snapshot)) refuseOwnerParity("generation_unstable");
    await snapshot.query("COMMIT");
  } catch (error) {
    await snapshot.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

/**
 * The read-only discovery run. Never throws a refusal and never writes: generation A on the target, then the
 * single-snapshot classification on the enumerator session (discoverOwnersInSnapshot: in-snapshot generation,
 * owner list, every comparison by the readback rule -- no stored row is missing, a stored row byte-equal to a
 * fresh equal proof at A is canonical current, anything else is stale -- the exact-seven check and a final
 * in-snapshot generation), then generation B on the target, which must equal A (generation_unstable). The source
 * and target sessions carry no comparison read; the target only answers the two external generation reads.
 * Exactly seven missing+stale owners or unexpected_scope; a refused receipt carries counts only, never digests.
 */
export async function runOwnerParityDiscovery(
  env: Record<string, string | undefined>,
  sessions: OwnerParitySessions,
  options: OwnerParityDiscoveryOptions,
): Promise<OwnerParityDiscoveryOutcome> {
  const now = options.now ?? Date.now;
  const counts: OwnerParityDiscoveryCounts = emptyOwnerParityDiscoveryCounts();
  const found: OwnerParityDiscoveryOwner[] = [];
  let dispatch: OwnerParityDispatch | null = null;
  let generation: string | null = null;
  const discoveryReceipt = (code?: string): OwnerParityDiscoveryReceipt => ({
    contract: OWNER_PARITY_DISCOVERY_CONTRACT, status: code === undefined ? "ok" : "refused",
    ...(code === undefined ? {} : { code }),
    runId: /^[1-9][0-9]{0,19}$/u.test(options.runId) ? options.runId : "0",
    sourceSha: dispatch?.sha ?? null, generation, censusSource: dispatch?.censusSource ?? null, counts,
    owners: code === undefined ? [...found].sort((a, b) => (a.tenantSha256 < b.tenantSha256 ? -1 : 1)) : [],
    capturedAt: new Date(now()).toISOString(), credentialsPersisted: false,
  });
  try {
    const pre = ownerParityPreflight(env, options.roster, undefined);
    dispatch = pre.dispatch;
    if (dispatch.mode !== "discovery") refuseOwnerParity("owner_parity_mode_route");
    generation = await readLiveGeneration(sessions.target);
    await discoverOwnersInSnapshot(sessions.enumerator, generation, dispatch.censusSource, counts, found, options.deps);
    if (await readLiveGeneration(sessions.target) !== generation) refuseOwnerParity("generation_unstable");
    return { receipt: discoveryReceipt(), exitCode: 0 };
  } catch (error) {
    return { receipt: discoveryReceipt(ownerParityCode(error)), exitCode: 1 };
  }
}

/** The generic receipt for a discovery run: aggregate counts only, never digests (those are in the discovery receipt). */
export function discoveryRunReceipt(discovery: OwnerParityDiscoveryReceipt): OwnerParityReceipt {
  return {
    contract: OWNER_PARITY_RECEIPT_CONTRACT, status: discovery.status, ...(discovery.code ? { code: discovery.code } : {}),
    mode: "discovery", runId: discovery.runId, sourceSha: discovery.sourceSha, generation: discovery.generation,
    censusSource: discovery.censusSource,
    counts: { ...emptyCounts(), found: discovery.counts.scanned, equal: discovery.counts.canonicalCurrent,
      missing: discovery.counts.missing, stale: discovery.counts.stale },
    tenants: [], at: discovery.capturedAt, credentialsPersisted: false,
  };
}

function refusedDiscoveryReceipt(env: Record<string, string | undefined>, code: string): OwnerParityDiscoveryReceipt {
  return {
    contract: OWNER_PARITY_DISCOVERY_CONTRACT, status: "refused", code,
    runId: /^[1-9][0-9]{0,19}$/u.test(env.GITHUB_RUN_ID ?? "") ? env.GITHUB_RUN_ID! : "0",
    sourceSha: null, generation: null, censusSource: null, counts: emptyOwnerParityDiscoveryCounts(), owners: [],
    capturedAt: new Date().toISOString(), credentialsPersisted: false,
  };
}

/** Job summary text for a validated discovery receipt: digests and dispositions on ok, counts only on refusal. */
export function ownerParityDiscoverySummary(raw: unknown): string {
  const r = parseOwnerParityDiscoveryReceipt(raw);
  const c = r.counts;
  const lines = [
    `### Owner buffered parity discovery (${OWNER_PARITY_DISCOVERY_CONTRACT})`, "",
    `- status: ${r.status}${r.code ? ` (${r.code})` : ""}`,
    `- sha: ${r.sourceSha ?? "-"}`, `- generation: ${r.generation ?? "-"}`, `- census source: ${r.censusSource ?? "-"}`,
    `- run: ${r.runId}`, `- captured at: ${r.capturedAt}`,
    `- scanned ${c.scanned}, canonical current ${c.canonicalCurrent}, missing ${c.missing}, stale ${c.stale}, unparsable ${c.unparsable}`,
  ];
  if (r.status === "ok") {
    lines.push("", "| tenantSha256 | disposition |", "| --- | --- |");
    for (const o of r.owners) lines.push(`| \`${o.tenantSha256}\` | ${o.disposition} |`);
    lines.push("", "Write the roster from these digests in a separate reviewed roster-only PR.");
  }
  lines.push("", "```json", JSON.stringify(r, null, 2), "```", "");
  return lines.join("\n");
}

/** `--discovery-summary`: no credential, no database; re-validates the receipt file before publishing it. */
function discoverySummary(): number {
  const env = process.env;
  try {
    const summary = env.GITHUB_STEP_SUMMARY ?? "";
    if (!summary.startsWith("/")) refuseOwnerParity("owner_parity_summary_path");
    const text = ownerParityDiscoverySummary(readJson(outputPaths(env).discovery, "owner_parity_discovery_receipt_invalid"));
    appendFileSync(summary, text);
    console.log("owner_buffered_parity_discovery_summary " + JSON.stringify({ status: "published" }));
    return 0;
  } catch (error) {
    console.log("owner_buffered_parity_discovery_summary " + JSON.stringify({ status: "refused", code: ownerParityCode(error) }));
    return 1;
  }
}

/** Spawn spec for the credential child: fixed argv, allowlisted env, the URL only in the stdin payload. */
export function ownerParityChildSpawn(
  env: Record<string, string | undefined>,
  supabaseUrl: string,
  script: string = join(ROOT, "scripts/owner-buffered-parity-ci.ts"),
): { command: string; args: string[]; env: Record<string, string>; input: string } {
  const childEnv: Record<string, string> = {};
  for (const key of OWNER_PARITY_CHILD_ENV) if (typeof env[key] === "string") childEnv[key] = env[key]!;
  return { command: process.execPath, args: [script, CHILD_FLAG], env: childEnv, input: JSON.stringify({ supabaseUrl }) };
}

/** The child holds no credential in its environment: the URL arrives only over the stdin pipe. */
export function refuseOwnerParityCredentialEnv(env: Record<string, string | undefined>): void {
  refuseCredentialEnv(env);
  for (const key of Object.keys(env))
    if (!(OWNER_PARITY_CHILD_ENV as readonly string[]).includes(key)) refuseOwnerParity("owner_parity_credential_env");
}

function outputPaths(env: Record<string, string | undefined>) {
  const temp = env.RUNNER_TEMP ?? "";
  if (!temp.startsWith("/")) refuseOwnerParity("owner_parity_runner_temp");
  return {
    census: join(temp, "owner-parity-census", OWNER_PARITY_CENSUS_FILE_NAME),
    out: join(temp, "owner-parity-out", OWNER_PARITY_CENSUS_FILE_NAME),
    receipt: join(temp, "owner-parity-out", OWNER_PARITY_RECEIPT_FILE_NAME),
    discovery: join(temp, "owner-parity-out", OWNER_PARITY_DISCOVERY_FILE_NAME),
  };
}

function writePrivate(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  writeFileSync(path, JSON.stringify(value) + "\n", { mode: 0o600 });
}

function readJson(path: string, code: string): unknown {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return refuseOwnerParity(code);
  }
}

function inputs(env: Record<string, string | undefined>) {
  const paths = outputPaths(env);
  const roster = readJson(join(ROOT, OWNER_PARITY_ROSTER_FILE), "roster_invalid");
  const census = env.OWNER_PARITY_MODE === "write" && existsSync(paths.census)
    ? readJson(paths.census, "owner_parity_census_invalid") : undefined;
  return { paths, roster, census };
}

function announce(receipt: OwnerParityReceipt): void {
  // Aggregate line only: no digests, URLs or identifiers.
  console.log("owner_buffered_parity_ci " + JSON.stringify({
    status: receipt.status, ...(receipt.code ? { code: receipt.code } : {}), mode: receipt.mode, counts: receipt.counts,
  }));
}

function refusedReceipt(env: Record<string, string | undefined>, code: string): OwnerParityReceipt {
  return {
    contract: OWNER_PARITY_RECEIPT_CONTRACT, status: "refused", code,
    mode: (OWNER_PARITY_MODES as readonly unknown[]).includes(env.OWNER_PARITY_MODE) ? env.OWNER_PARITY_MODE! : "refused",
    runId: /^[1-9][0-9]{0,19}$/u.test(env.GITHUB_RUN_ID ?? "") ? env.GITHUB_RUN_ID! : "0",
    sourceSha: null, generation: null, censusSource: null, counts: emptyCounts(), tenants: [],
    at: new Date().toISOString(), credentialsPersisted: false,
  };
}

function writeRefused(env: Record<string, string | undefined>, paths: ReturnType<typeof outputPaths> | null, code: string): void {
  const receipt = refusedReceipt(env, code);
  if (paths) {
    writePrivate(paths.receipt, receipt);
    if (env.OWNER_PARITY_MODE === "discovery") writePrivate(paths.discovery, refusedDiscoveryReceipt(env, code));
  }
  announce(receipt);
}

/** Workflow step: pre-credential checks, then pipe the one secret to the credential child. */
async function parent(): Promise<number> {
  const env = process.env;
  let paths: ReturnType<typeof outputPaths> | null = null;
  try {
    // The secret leaves the environment first (git subprocesses below inherit it otherwise).
    const url = env[OWNER_PARITY_SECRET_ENV];
    delete process.env[OWNER_PARITY_SECRET_ENV];
    const read = inputs(env);
    paths = read.paths;
    const { roster, census } = read;
    // Then the credential-free checks (dormant switch, roster, census, confirmation, checkout),
    // and only after those is the value examined.
    const pre = ownerParityPreflight(env, roster, census);
    verifyCheckout(pre.dispatch.sha);
    if (typeof url !== "string" || url === "") refuseOwnerParity("owner_parity_credential_missing");
    const spec = ownerParityChildSpawn(env, url);
    return await new Promise<number>((resolve) => {
      const child = spawn(spec.command, spec.args, { cwd: ROOT, env: spec.env, stdio: ["pipe", "inherit", "inherit"] });
      child.on("error", () => resolve(1));
      child.on("close", (code) => resolve(code === null ? 1 : code));
      child.stdin.on("error", () => undefined);
      child.stdin.end(spec.input);
    });
  } catch (error) {
    writeRefused(env, paths, ownerParityCode(error));
    return 1;
  }
}

/** Credential child: the URL arrives over stdin only, and every session is fenced. */
async function operatorChild(): Promise<number> {
  const env = process.env;
  let paths: ReturnType<typeof outputPaths> | null = null;
  try {
    const read = inputs(env);
    paths = read.paths;
    const { roster, census } = read;
    refuseOwnerParityCredentialEnv(env);
    const pre = ownerParityPreflight(env, roster, census);
    verifyCheckout(pre.dispatch.sha);
    const args = { mode: "bulk" as const, expectedSourceSha: pre.dispatch.sha, expectedTargetGeneration: "0".repeat(64),
      censusSource: pre.dispatch.censusSource, batchSize: FETCH_PAGE };
    const keys = operatorCredentialKeys(args);
    const input = await readStdinCredentials(keys.required, keys.allowed);
    validateCensusSourceInput(args, input);
    const supabaseUrl = sessionAffinityUrl(input.supabaseUrl, "supabaseUrl", "supabase");
    pinnedPgConfig(supabaseUrl, "supabaseUrl", "supabase");
    const neonUrl = pre.dispatch.censusSource === "neon" ? sessionAffinityUrl(input.neonUrl, "neonUrl", "neon") : null;
    if (neonUrl !== null) pinnedPgConfig(neonUrl, "neonUrl", "neon");
    // Only `write` has a writable target session (OWNER_PARITY_MODE_POLICY); discovery is read-only like census.
    const write = !OWNER_PARITY_MODE_POLICY[pre.dispatch.mode].readOnly;
    const discovery = pre.dispatch.mode === "discovery";
    let discovered: OwnerParityDiscoveryOutcome | null = null;
    const work = async (sessions: OwnerParitySessions): Promise<OwnerParityOutcome> => {
      if (!discovery) return runOwnerParity(env, sessions, { roster, census, runId: env.GITHUB_RUN_ID ?? "" });
      discovered = await runOwnerParityDiscovery(env, sessions, { roster, runId: env.GITHUB_RUN_ID ?? "" });
      return { receipt: discoveryRunReceipt(discovered.receipt), census: null, exitCode: discovered.exitCode };
    };
    const workflowClient = () => releaseClient(supabaseUrl) as ConnectedOwnerClient;
    const run = (source: Queryable) => withWorkflowParityOwnerSession(
      workflowClient(), true, (enumerator) =>
        withWorkflowParityOwnerSession(
          workflowClient(),
          !write, (target) => work({ source, enumerator, target })));
    let outcome: OwnerParityOutcome;
    if (neonUrl !== null) {
      const neon = await connectNeon(neonUrl);
      try {
        await requireReadOnly(neon, "neonUrl");
        outcome = await run(neon);
      } finally {
        await neon.end().catch(() => undefined);
      }
    } else {
      outcome = await withWorkflowParityOwnerSession(
        workflowClient(), true, async (source) => {
          return run(source);
        });
    }
    writePrivate(paths.receipt, outcome.receipt);
    if (outcome.census) writePrivate(paths.out, outcome.census);
    if (discovered) writePrivate(paths.discovery, (discovered as OwnerParityDiscoveryOutcome).receipt);
    announce(outcome.receipt);
    return outcome.exitCode;
  } catch (error) {
    writeRefused(env, paths, ownerParityCode(error));
    return 1;
  }
}

// ---------------------------------------------------------------------------------------------------------------
// Synthetic PGlite proof: no credentials, no network. Fixture identifiers only.
// ---------------------------------------------------------------------------------------------------------------

const H = (c: string) => c.repeat(64);
export const PROOF_SHA = "5".repeat(40);
export const PROOF_TENANTS: ParityTenant[] = Array.from({ length: 9 }, (_, i) => ({
  orgId: `fixture-org-${i + 1}`, workspaceId: `fixture-ws-${i + 1}`, userId: `fixture-user-${i + 1}`,
}));
/** Roster tenants are the first seven; tenants 8 and 9 are buffered but unlisted and must never be touched. */
export const PROOF_ROSTER = {
  contract: "owner-buffered-parity-roster/v1",
  comment: "synthetic proof roster",
  tenants: PROOF_TENANTS.slice(0, OWNER_PARITY_EXACT_COUNT).map(tenantSha256),
};

/** `'org','workspace','user'` for a fixture tenant (fixture identifiers only; never for real input). */
export const proofTenantLiteral = (t: ParityTenant) => `'${t.orgId}','${t.workspaceId}','${t.userId}'`;

/**
 * The synthetic proof seed, applied after the shim, bootstrap chain and migrations: the nine PROOF_TENANTS buffered
 * with frozen-source state at generation H("4"), frozen completeness and an active privacy authority. Shared by the
 * PGlite proof and the PostgreSQL discovery snapshot test.
 */
export function ownerParityProofSeedSql(): string {
  const lit = proofTenantLiteral;
  let seed = `CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);
    INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','${H("4")}','pass');
    INSERT INTO sessions.ingest_lazy_policy(singleton,baseline_mode,generation,source_identity_sha256,control_epoch,
      privacy_baseline_sha256,retention_contract_sha256,source_proof_sha256,issuer,valid_until)
    VALUES(true,'lazy-current','${H("4")}','${H("c")}','${H("1")}','${H("2")}','${H("b")}','${H("d")}',
      'relayhistory-cloud',clock_timestamp()+interval '1 hour');
    SET session_replication_role = replica;`;
  for (const [i, t] of PROOF_TENANTS.entries()) {
    const version = i + 1;
    seed += `
    INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id)
      VALUES(${lit(t)},'origin-${i}','claude','session-${i}');
    INSERT INTO sessions.upload_resets(org_id,workspace_id,user_id,request_id) VALUES(${lit(t)},'request-${i}');
    INSERT INTO sessions.ingest_control_receipts(org_id,workspace_id,user_id,epoch,version,digest,kind,result,schema_generation)
      SELECT ${lit(t)},'${H("1")}',n,'${H("3")}','selection','{}'::jsonb,'${H("4")}' FROM generate_series(1,${version}) AS n;
    INSERT INTO sessions.ingest_source_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending)
      VALUES(${lit(t)},'${H("1")}',${version},'${H("3")}','selection','{}'::jsonb,false);
    INSERT INTO sessions.ingest_frozen_source_deletions(org_id,workspace_id,user_id,origin_id,source,session_id)
      VALUES(${lit(t)},'origin-${i}','claude','session-${i}');
    INSERT INTO sessions.ingest_frozen_source_resets(org_id,workspace_id,user_id,request_id,deleted)
      VALUES(${lit(t)},'request-${i}',1);
    INSERT INTO sessions.ingest_buffer_outbox(org_id,workspace_id,origin_id,batch_id,user_id,digest,active_stage_token)
      VALUES('${t.orgId}','${t.workspaceId}','origin','batch-${i}','${t.userId}','${H("e")}',gen_random_uuid()),
            ('${t.orgId}','${t.workspaceId}','origin','batch-${i}-b','${t.userId}','${H("e")}',gen_random_uuid());`;
  }
  seed += `
    RESET session_replication_role;
    INSERT INTO sessions.ingest_frozen_source_completeness(singleton,source_identity_sha256,control_epoch,
      privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot)
      VALUES(true,'${H("c")}','${H("1")}','${H("2")}',9,0,'${H("e")}','{}'::jsonb);
    INSERT INTO sessions.ingest_privacy_authority(singleton,state,control_epoch,neon_fingerprint_sha256,activated_at)
      VALUES(true,'active','${H("1")}','${H("f")}',clock_timestamp());`;
  return seed;
}

export interface ProofCase { name: string; pass: boolean; detail?: string }

export async function ownerParityProof(): Promise<{ cases: ProofCase[]; failed: number }> {
  const { PGlite } = await import("@electric-sql/pglite");
  const { btree_gin } = await import("@electric-sql/pglite/contrib/btree_gin");
  const { pg_trgm } = await import("@electric-sql/pglite/contrib/pg_trgm");
  const { SESSION_BOOTSTRAP_FILES } = await import("../src/hoopdb/session-bootstrap-chain.ts");
  const { readdirSync } = await import("node:fs");
  const read = (path: string) => readFileSync(join(ROOT, path), "utf8");
  const extensions = { btree_gin, pg_trgm };
  const template = new PGlite({ extensions });
  await template.exec(read("supabase/tests/supabase_shim.sql"));
  for (const file of SESSION_BOOTSTRAP_FILES) await template.exec(read(file));
  for (const file of readdirSync(join(ROOT, "supabase/migrations")).filter((n) => n.endsWith(".sql")).sort())
    await template.exec(read(`supabase/migrations/${file}`));
  await template.exec(read("supabase/bootstrap/session_read_api_role.sql").split("\n")
    .filter((line) => !line.startsWith("\\")).join("\n"));
  const lit = proofTenantLiteral;
  await template.exec(ownerParityProofSeedSql());
  const image = await template.dumpDataDir();
  await template.close();

  type Db = InstanceType<typeof PGlite>;
  const open = async () => new PGlite({ extensions, loadDataDir: image });
  const queryable = (db: Db, log?: Array<{ sql: string; params: unknown[] }>): Queryable => ({
    query: async <R>(sql: string, params?: unknown[]) => {
      log?.push({ sql, params: params ?? [] });
      return (params?.length ? db.query(sql, params as never) : db.query(sql)) as Promise<{ rows: R[] }>;
    },
  });
  const cases: ProofCase[] = [];
  const check = async (name: string, work: () => Promise<void>) => {
    try {
      await work();
      cases.push({ name, pass: true });
    } catch (error) {
      cases.push({ name, pass: false, detail: error instanceof Error ? error.message.slice(0, 200) : "failed" });
    }
  };
  const assert = (ok: unknown, why: string) => { if (!ok) throw new Error(why); };
  const expectCode = (outcome: OwnerParityOutcome, code: string) =>
    assert(outcome.exitCode === 1 && outcome.receipt.status === "refused" && outcome.receipt.code === code,
      `expected ${code}, got ${outcome.receipt.status}/${outcome.receipt.code ?? "-"}`);
  const base = { OWNER_PARITY_SHA: PROOF_SHA, OWNER_PARITY_CENSUS_SOURCE: "frozen-supabase" };
  const env = (mode: string, extra: Record<string, string> = {}) => ({ ...base, OWNER_PARITY_MODE: mode, ...extra });

  // One database plays the target and the frozen copy (they are the same Supabase project), opened as three
  // independent PGlite instances from one image so the sessions do not share a transaction.
  let source = await open(), target = await open(), enumeratorDb = await open();
  const log: Array<{ sql: string; params: unknown[] }> = [];
  const sessions = () => ({ source: queryable(source, log), enumerator: queryable(enumeratorDb, log), target: queryable(target, log) });
  const parityRows = async () => (await target.query<{ user_id: string }>(
    "SELECT user_id FROM sessions.owner_buffered_parity ORDER BY user_id")).rows.map((r) => r.user_id);
  const T0 = Date.parse("2026-11-13T00:00:00.000Z");
  let census: OwnerParityCensusArtifact | null = null;
  const writeEnv = (c: OwnerParityCensusArtifact, extra: Record<string, string> = {}) =>
    env("write", { OWNER_PARITY_CENSUS_RUN: c.censusRun, OWNER_PARITY_CONFIRMATION:
      `WRITE-OWNER-PARITY ${c.sourceSha} GENERATION ${c.generation} SOURCE ${c.censusSource} DESCRIPTOR ${c.descriptorSha256} CENSUS ${c.censusRun}`,
      ...extra });
  const write = (c: unknown, e: Record<string, string>, now = T0 + 3600_000) =>
    runOwnerParity(e, sessions(), { roster: PROOF_ROSTER, runId: "202", census: c, now: () => now, writeEnabled: true });

  await check("census: exactly the seven roster tenants, all equal, at the live PASS generation", async () => {
    const outcome = await runOwnerParity(env("census"), sessions(), { roster: PROOF_ROSTER, runId: "101", now: () => T0 });
    assert(outcome.exitCode === 0 && outcome.receipt.status === "ok", "census not ok: " + outcome.receipt.code);
    census = outcome.census;
    assert(census && census.tenants.length === 7 && census.tenants.every((t) => t.status === "equal"), "census shape");
    assert(census!.generation === H("4") && census!.censusRun === "101" && census!.capturedAt === new Date(T0).toISOString(),
      "census binding");
    assert(JSON.stringify(census!.tenants.map((t) => t.tenantSha256)) === JSON.stringify(PROOF_ROSTER.tenants), "roster order");
    assert(parseOwnerParityCensus(JSON.parse(JSON.stringify(census))).descriptorSha256 === census!.descriptorSha256, "descriptor");
  });
  const c = () => { if (!census) throw new Error("no census"); return census; };

  await check("refuses write while dormant, before any database statement", async () => {
    const before = log.length;
    const outcome = await runOwnerParity(writeEnv(c()), sessions(), { roster: PROOF_ROSTER, runId: "202", census: c(), now: () => T0 });
    expectCode(outcome, "owner_parity_write_dormant");
    assert(log.length === before, "dormant write touched a session");
  });
  await check("refuses roster_empty", async () => {
    expectCode(await runOwnerParity(env("census"), sessions(), { roster: { ...PROOF_ROSTER, tenants: [] }, runId: "1" }), "roster_empty");
  });
  await check("refuses roster_incomplete (six listed)", async () => {
    expectCode(await runOwnerParity(env("census"), sessions(),
      { roster: { ...PROOF_ROSTER, tenants: PROOF_ROSTER.tenants.slice(0, 6) }, runId: "1" }), "roster_incomplete");
  });
  await check("refuses roster_incomplete (a listed tenant has no buffered row)", async () => {
    const roster = { ...PROOF_ROSTER, tenants: [...PROOF_ROSTER.tenants.slice(0, 6), tenantSha256({ orgId: "absent",
      workspaceId: "absent", userId: "absent" })] };
    expectCode(await runOwnerParity(env("census"), sessions(), { roster, runId: "1" }), "roster_incomplete");
  });
  await check("bulk is unreachable", async () => {
    expectCode(await runOwnerParity(env("bulk"), sessions(), { roster: PROOF_ROSTER, runId: "1", writeEnabled: true }),
      "owner_parity_inputs");
  });
  await check("refuses a wrong confirmation", async () => {
    expectCode(await write(c(), writeEnv(c(), { OWNER_PARITY_CONFIRMATION: writeEnv(c()).OWNER_PARITY_CONFIRMATION + " " })),
      "owner_parity_confirmation");
  });
  await check("refuses a stale census (24h)", async () => {
    expectCode(await write(c(), writeEnv(c()), T0 + 24 * 3600_000), "owner_parity_census_stale");
  });
  await check("refuses a census for another sha", async () => {
    expectCode(await write(c(), writeEnv(c(), { OWNER_PARITY_SHA: "6".repeat(40) })), "owner_parity_census_sha_mismatch");
  });
  await check("refuses a substituted roster digest in the census", async () => {
    const forged = JSON.parse(JSON.stringify(c()));
    forged.tenants[0].tenantSha256 = forged.tenants[0].census.tenantSha256 = tenantSha256(PROOF_TENANTS[7]!);
    expectCode(await write(forged, writeEnv(c())), "owner_parity_census_descriptor");
  });
  await check("refuses when the live PASS generation moved since the census", async () => {
    await target.exec(`UPDATE sessions.__96bulls_role_validation SET generation='${H("9")}'`);
    try {
      expectCode(await write(c(), writeEnv(c())), "owner_parity_generation_mismatch");
      assert((await parityRows()).length === 0, "rows written after generation drift");
    } finally {
      await target.exec(`UPDATE sessions.__96bulls_role_validation SET generation='${H("4")}'`);
    }
  });
  await check("write: seven rows, exact readback, the eighth and ninth tenants never touched", async () => {
    const outcome = await write(c(), writeEnv(c()));
    assert(outcome.exitCode === 0 && outcome.receipt.status === "ok", "write not ok: " + outcome.receipt.code);
    assert(outcome.receipt.counts.inserted === 7 && outcome.receipt.counts.verified === 7, "counts");
    const rows = await parityRows();
    assert(JSON.stringify(rows) === JSON.stringify(PROOF_TENANTS.slice(0, 7).map((t) => t.userId).sort()), "rows");
    for (const t of PROOF_TENANTS.slice(7))
      assert(!log.some((q) => q.params.includes(t.userId) || q.sql.includes(t.userId)), "unlisted tenant touched");
  });
  await check("write is idempotent: a second run reports unchanged", async () => {
    const outcome = await write(c(), writeEnv(c()));
    assert(outcome.exitCode === 0 && outcome.receipt.counts.unchanged === 7, "not unchanged");
  });
  await check("readback: equal for all seven", async () => {
    const outcome = await runOwnerParity(env("readback"), sessions(), { roster: PROOF_ROSTER, runId: "303" });
    assert(outcome.exitCode === 0 && outcome.receipt.counts.equal === 7, "readback");
  });
  await check("readback: a deleted row reads missing and a moved source reads stale", async () => {
    const t = PROOF_TENANTS[0]!, u = PROOF_TENANTS[1]!;
    await target.query("DELETE FROM sessions.owner_buffered_parity WHERE user_id=$1", [t.userId]);
    await source.exec(`INSERT INTO sessions.ingest_frozen_source_resets(org_id,workspace_id,user_id,request_id,deleted)
      SELECT ${lit(u)},'request-extra',1`).catch(async () => {
      await source.exec(`SET bulls96.privacy_authority_writer='authority';
        INSERT INTO sessions.ingest_frozen_source_resets(org_id,workspace_id,user_id,request_id,deleted)
        VALUES(${lit(u)},'request-extra',1); RESET bulls96.privacy_authority_writer;`);
    });
    const outcome = await runOwnerParity(env("readback"), sessions(), { roster: PROOF_ROSTER, runId: "304" });
    assert(outcome.exitCode === 3 && outcome.receipt.status === "mismatch", "readback status");
    const byDigest = new Map(outcome.receipt.tenants.map((x) => [x.tenantSha256, x.status]));
    assert(byDigest.get(tenantSha256(t)) === "missing" && byDigest.get(tenantSha256(u)) === "stale" &&
      outcome.receipt.counts.equal === 5, "readback per-digest " + JSON.stringify([...byDigest.values()]));
  });
  await check("receipt and census carry no private field names or values", async () => {
    const outcome = await runOwnerParity(env("readback"), sessions(), { roster: PROOF_ROSTER, runId: "305" });
    const text = JSON.stringify(outcome.receipt) + JSON.stringify(c());
    for (const key of ["orgId", "workspaceId", "userId", "neonUrl", "supabaseUrl", "org_id", "user_id"])
      assert(!text.includes(`"${key}"`), "key " + key);
    for (const t of PROOF_TENANTS) assert(!text.includes(t.orgId) && !text.includes(t.userId), "tenant value");
  });
  await check("discovery: refuses roster_present with a committed roster, before any statement", async () => {
    const before = log.length;
    const outcome = await runOwnerParityDiscovery(env("discovery"), sessions(), { roster: PROOF_ROSTER, runId: "401" });
    assert(outcome.exitCode === 1 && outcome.receipt.code === "roster_present" && outcome.receipt.owners.length === 0,
      "discovery roster_present: " + outcome.receipt.code);
    assert(log.length === before, "roster_present touched a session");
  });
  await check("discovery: three missing owners refuses unexpected_scope with counts only", async () => {
    // Discovery reads only its one snapshot session, so it runs on the instance holding the parity rows (the
    // target): tenant 1 deleted (missing), 2-7 written and unchanged there (current), 8-9 never written.
    const outcome = await runOwnerParityDiscovery(env("discovery"),
      { source: queryable(source, log), enumerator: queryable(target, log), target: queryable(target, log) },
      { roster: { ...PROOF_ROSTER, tenants: [] }, runId: "402" });
    const k = outcome.receipt.counts;
    assert(outcome.exitCode === 1 && outcome.receipt.code === "unexpected_scope" && outcome.receipt.owners.length === 0 &&
      k.scanned === 9 && k.canonicalCurrent === 6 && k.missing === 3 && k.stale === 0, "scope " + JSON.stringify(outcome.receipt));
    assert(!JSON.stringify(outcome.receipt).includes("tenantSha256"), "digests on refusal");
  });
  await Promise.all([source.close(), target.close(), enumeratorDb.close()]);

  await check("discovery: read-only snapshot on a fresh image finds exactly seven missing/stale owners and writes nothing", async () => {
    const setupSource = await open(), setupTarget = await open();
    let dSource: Db | null = null, dTarget: Db | null = null, dEnumerator: Db | null = null;
    try {
      // Setup only (not discovery): canonical rows for tenants 1-3, then tenant 2's frozen source moves (stale).
      // Both land on the setup target, whose image then backs every discovery session (one database, as in prod).
      for (const t of PROOF_TENANTS.slice(0, 3))
        await writeCurrentParityProof(queryable(setupSource), queryable(setupTarget), t, H("4"), "frozen-supabase");
      const u = PROOF_TENANTS[1]!;
      await setupTarget.exec(`SET bulls96.privacy_authority_writer='authority';
        INSERT INTO sessions.ingest_frozen_source_resets(org_id,workspace_id,user_id,request_id,deleted)
        VALUES(${lit(u)},'request-discovery',1); RESET bulls96.privacy_authority_writer;`).catch(() =>
        setupTarget.exec(`INSERT INTO sessions.ingest_frozen_source_resets(org_id,workspace_id,user_id,request_id,deleted)
          VALUES(${lit(u)},'request-discovery',1)`));
      const discoveryImage = await setupTarget.dumpDataDir();
      const openDiscovery = () => new PGlite({ extensions, loadDataDir: discoveryImage });
      [dSource, dTarget, dEnumerator] = await Promise.all([openDiscovery(), openDiscovery(), openDiscovery()]);
      // Discovery itself runs on sessions where every transaction defaults to read-only.
      for (const db of [dSource, dTarget, dEnumerator]) await db.exec("SET default_transaction_read_only = on");
      const dLog: Array<{ sql: string; params: unknown[] }> = [];
      const sourceLog: Array<{ sql: string; params: unknown[] }> = [], targetLog: Array<{ sql: string; params: unknown[] }> = [];
      const into = (db: Db, ...logs: Array<Array<{ sql: string; params: unknown[] }>>): Queryable => ({
        query: async <R>(sql: string, params?: unknown[]) => {
          for (const l of logs) l.push({ sql, params: params ?? [] });
          return queryable(db).query<R>(sql, params);
        },
      });
      const outcome = await runOwnerParityDiscovery(env("discovery"), {
        source: into(dSource, dLog, sourceLog), enumerator: into(dEnumerator, dLog), target: into(dTarget, dLog, targetLog),
      }, { roster: { ...PROOF_ROSTER, tenants: [] }, runId: "403", now: () => T0 });
      assert(outcome.exitCode === 0 && outcome.receipt.status === "ok", "discovery not ok: " + JSON.stringify(outcome.receipt));
      const k = outcome.receipt.counts;
      assert(k.scanned === 9 && k.canonicalCurrent === 2 && k.missing === 6 && k.stale === 1 && k.unparsable === 0,
        "counts " + JSON.stringify(k));
      const byDigest = new Map(outcome.receipt.owners.map((o) => [o.tenantSha256, o.disposition]));
      assert(byDigest.get(tenantSha256(u)) === "stale" && !byDigest.has(tenantSha256(PROOF_TENANTS[0]!)) &&
        !byDigest.has(tenantSha256(PROOF_TENANTS[2]!)) &&
        PROOF_TENANTS.slice(3).every((t) => byDigest.get(tenantSha256(t)) === "missing"), "dispositions");
      assert(outcome.receipt.generation === H("4") && outcome.receipt.sourceSha === PROOF_SHA, "binding");
      parseOwnerParityDiscoveryReceipt(JSON.parse(JSON.stringify(outcome.receipt)));
      assert(!dLog.some((q) => /\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE)\b/iu.test(q.sql)), "discovery issued a write statement");
      // One snapshot: exactly one BEGIN and one COMMIT, both on the enumerator; source idle; target fence reads only.
      assert(dLog.filter((q) => q.sql === "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").length === 1 &&
        dLog.filter((q) => /^(BEGIN|START|COMMIT|END|ROLLBACK)\b/iu.test(q.sql)).length === 2 &&
        dLog.filter((q) => q.sql === "COMMIT").length === 1, "not exactly one read-only snapshot");
      assert(sourceLog.length === 0, "the source session was used");
      assert(targetLog.length === 2 && targetLog.every((q) => q.sql === BULK_TARGET_GENERATION_SQL), "the target did more than the fence");
      const rows = (await dEnumerator.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.owner_buffered_parity")).rows[0]!.n;
      assert(rows === 3, "parity rows changed: " + rows);
      const text = JSON.stringify(outcome.receipt) + ownerParityDiscoverySummary(outcome.receipt);
      for (const t of PROOF_TENANTS) assert(!text.includes(t.orgId) && !text.includes(t.workspaceId) && !text.includes(t.userId), "identifier");
    } finally {
      await Promise.all([setupSource, setupTarget, dSource, dTarget, dEnumerator].map((db) => db?.close()));
    }
  });
  return { cases, failed: cases.filter((x) => !x.pass).length };
}

function dieQuietly(): never {
  console.log("owner_buffered_parity_ci " + JSON.stringify({ status: "refused", code: "owner_parity_failed" }));
  process.exit(1);
}

if (import.meta.main) {
  process.on("uncaughtException", dieQuietly);
  process.on("unhandledRejection", dieQuietly);
  if (process.argv.includes("--proof")) {
    const result = await ownerParityProof();
    for (const c of result.cases) console.log(`${c.pass ? "PASS" : "FAIL"} ${c.name}${c.detail ? ` | ${c.detail}` : ""}`);
    console.log(`owner_buffered_parity_ci_proof ${result.cases.length - result.failed}/${result.cases.length}`);
    process.exit(result.failed ? 1 : 0);
  }
  if (process.argv.includes(SUMMARY_FLAG)) process.exit(discoverySummary());
  process.exit(process.argv.includes(CHILD_FLAG) ? await operatorChild() : await parent());
}
