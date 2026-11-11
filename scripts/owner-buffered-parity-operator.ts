/**
 * Root-only Option C G1/G2 operator.
 *
 * Credentials and, for single-tenant modes, the private tenant tuple arrive as
 * one bounded JSON object on a pipe or socket, never argv, environment,
 * terminal, or a file:
 *   {"neonUrl":"...","supabaseUrl":"...","tenant":{"orgId":"...","workspaceId":"...","userId":"..."}}
 * Bulk input contains exactly neonUrl and supabaseUrl; its tenant set is read
 * from the target ledger under the table-owner session.
 *
 * G1 (strictly read-only):
 *   <approved RAM-only credential pipe> | bun run scripts/owner-buffered-parity-operator.ts census \
 *     --expect-source-sha <merged-main-sha> --expect-target-generation <hex64>
 *
 * G2 (writes one owner_buffered_parity row only after a fresh G1):
 *   <approved RAM-only credential pipe> | bun run scripts/owner-buffered-parity-operator.ts write \
 *     --expect-source-sha <merged-main-sha> --expect-target-generation <hex64> \
 *     --confirm-write owner-buffered-parity/v1
 *
 * Bulk G1+G2 (every distinct user with a live buffered outbox row):
 *   <approved RAM-only credential pipe> | bun run scripts/owner-buffered-parity-operator.ts bulk \
 *     --expect-source-sha <merged-main-sha> --expect-target-generation <hex64> \
 *     --batch-size 25 --confirm-write owner-buffered-parity-bulk/v1
 *
 * Bulk enumeration is a read-only repeatable-read cursor. Only one bounded
 * page is held in process memory. A stopped run is resumed by rerunning the
 * same pinned command: completed tenants are idempotently reported unchanged,
 * while the fresh cursor also includes buffered tenants added since the prior
 * snapshot. Bulk output is aggregate counts only; tenant identifiers and
 * per-tenant hashes never leave the process.
 * Both Supabase connections use the canonical temporary-login owner session:
 * reset baseline and backend PID are proven before work, then owner identity
 * is checked around every enumerator or writer query.
 *
 * Census source (step-6 edge 5). `--census-source neon` is the default and
 * keeps every byte above unchanged. `--census-source frozen-supabase` reads the
 * source side from the sealed frozen Supabase copy on its own fenced read-only
 * owner session instead of Neon. Its stdin is exactly
 *   {"supabaseUrl":"...","tenant":{...}}   (bulk: {"supabaseUrl":"..."})
 * A neonUrl key refuses with census_source_conflict; the census refuses with
 * frozen_source_unsealed or privacy_authority_not_active before reading the
 * target, and with frozen_source_changed_during_census if the copy moves.
 * Frozen-mode receipts carry "censusSource":"frozen-supabase" as their last field.
 *
 * Output is one aggregate JSON receipt. Single-tenant output contains a tenant
 * hash; bulk output contains counts only. Neither contains org/workspace/user
 * identifiers or database URLs. A write still requires the operator to have
 * Khaliq's direct production approval; the flag is an accidental-execution
 * guard, not approval by itself.
 */
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import pg from "pg";
import {
  PARITY_CENSUS_SOURCES,
  ParityCensusRefusal,
  readParityCensus,
  validParityTenant,
  writeCurrentParityProof,
  type ParityCensusReceipt,
  type ParityCensusSource,
  type ParityTenant,
} from "../src/migration/owner-buffered-parity.ts";
import {
  CredentialRefusal,
  pinnedPgClient,
  pinnedPgConfig,
  readStdinCredentials,
  refuseCredentialEnv,
  sessionAffinityUrl,
} from "../src/migration/stdin-credentials.ts";
import {
  ownerFencedClient,
  withOwnerSession,
  type ConnectedOwnerClient,
} from "../src/hoopdb/owner-operator-apply.ts";
import type { Queryable } from "../src/migration/relayhistory-export.ts";

const HEX40 = /^[0-9a-f]{40}$/u;
const HEX64 = /^[0-9a-f]{64}$/u;
const WRITE_CONFIRMATION = "owner-buffered-parity/v1";
const BULK_WRITE_CONFIRMATION = "owner-buffered-parity-bulk/v1";
const DEFAULT_BULK_BATCH_SIZE = 25;
const MAX_BULK_BATCH_SIZE = 100;
const BULK_CURSOR = "owner_buffered_parity_bulk_tenants";
const BULK_CURSOR_KEEPALIVE_SQL = "SELECT 1 AS owner_buffered_bulk_keepalive";

export const BULK_TARGET_GENERATION_SQL = `
SELECT generation
  FROM sessions.__96bulls_role_validation
 WHERE name='session_directory_search_worker' AND status='pass'`;

export const BULK_BUFFERED_TENANTS_SQL = `
SELECT org_id,workspace_id,user_id
  FROM (SELECT DISTINCT org_id,workspace_id,user_id
          FROM sessions.ingest_buffer_outbox
         WHERE state='buffered') AS buffered_tenants
 ORDER BY org_id COLLATE "C",workspace_id COLLATE "C",user_id COLLATE "C"`;

export class OwnerBufferedOperatorRefusal extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "OwnerBufferedOperatorRefusal";
  }
}

interface OperatorBaseArgs {
  expectedSourceSha: string;
  expectedTargetGeneration: string;
  censusSource: ParityCensusSource;
}

export interface SingleOperatorArgs extends OperatorBaseArgs {
  mode: "census" | "write";
}

export interface BulkOperatorArgs extends OperatorBaseArgs {
  mode: "bulk";
  batchSize: number;
}

export type OperatorArgs = SingleOperatorArgs | BulkOperatorArgs;

export function parseOperatorArgs(argv: readonly string[]): OperatorArgs {
  const [mode, ...rest] = argv;
  if (mode !== "census" && mode !== "write" && mode !== "bulk")
    throw new OwnerBufferedOperatorRefusal("usage");
  const allowed = new Set([
    "--expect-source-sha", "--expect-target-generation", "--confirm-write", "--batch-size", "--census-source",
  ]);
  const values = new Map<string, string>();
  for (let i = 0; i < rest.length; i += 2) {
    const key = rest[i];
    const value = rest[i + 1];
    if (!key || !allowed.has(key) || !value || values.has(key)) throw new OwnerBufferedOperatorRefusal("usage");
    values.set(key, value);
  }
  const expectedSourceSha = values.get("--expect-source-sha") ?? "";
  const expectedTargetGeneration = values.get("--expect-target-generation") ?? "";
  if (!HEX40.test(expectedSourceSha) || !HEX64.test(expectedTargetGeneration))
    throw new OwnerBufferedOperatorRefusal("usage");
  const confirmation = values.get("--confirm-write");
  if (mode === "write" ? confirmation !== WRITE_CONFIRMATION
    : mode === "bulk" ? confirmation !== BULK_WRITE_CONFIRMATION
    : confirmation !== undefined)
    throw new OwnerBufferedOperatorRefusal("usage");
  const batchText = values.get("--batch-size");
  if (mode !== "bulk" && batchText !== undefined) throw new OwnerBufferedOperatorRefusal("usage");
  const batchSize = batchText === undefined ? DEFAULT_BULK_BATCH_SIZE : Number(batchText);
  if (!Number.isSafeInteger(batchSize) || batchSize < 1 || batchSize > MAX_BULK_BATCH_SIZE)
    throw new OwnerBufferedOperatorRefusal("usage");
  const sourceText = values.get("--census-source") ?? "neon";
  if (!(PARITY_CENSUS_SOURCES as readonly string[]).includes(sourceText))
    throw new OwnerBufferedOperatorRefusal("census_source_invalid");
  const censusSource = sourceText as ParityCensusSource;
  return mode === "bulk"
    ? { mode, expectedSourceSha, expectedTargetGeneration, censusSource, batchSize }
    : { mode, expectedSourceSha, expectedTargetGeneration, censusSource };
}

/**
 * Stdin key contract per census source. Neon mode is unchanged (exact keys).
 * Frozen mode reads with no hard-required keys so a stray neonUrl is refused
 * as a source conflict instead of an unknown key; validateCensusSourceInput
 * then enforces the exact frozen shape.
 */
export function operatorCredentialKeys(args: OperatorArgs): { required: string[]; allowed: string[] } {
  if (args.censusSource === "neon") {
    const keys = args.mode === "bulk" ? ["neonUrl", "supabaseUrl"] : ["neonUrl", "supabaseUrl", "tenant"];
    return { required: keys, allowed: keys };
  }
  return { required: [], allowed: args.mode === "bulk" ? ["supabaseUrl", "neonUrl"] : ["supabaseUrl", "tenant", "neonUrl"] };
}

export function validateCensusSourceInput(args: OperatorArgs, input: Record<string, unknown>): void {
  if (args.censusSource === "neon") return;
  if (input.neonUrl !== undefined) throw new OwnerBufferedOperatorRefusal("census_source_conflict");
  const required = args.mode === "bulk" ? ["supabaseUrl"] : ["supabaseUrl", "tenant"];
  for (const key of required)
    if (input[key] === undefined) throw new CredentialRefusal(`credentials_missing_key.${key}`);
}

function parseTenant(value: unknown): ParityTenant {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new OwnerBufferedOperatorRefusal("tenant_invalid");
  const row = value as Record<string, unknown>;
  if (Object.keys(row).sort().join(",") !== "orgId,userId,workspaceId")
    throw new OwnerBufferedOperatorRefusal("tenant_invalid");
  const tenant = { orgId: row.orgId, workspaceId: row.workspaceId, userId: row.userId };
  if (!validParityTenant(tenant as ParityTenant))
    throw new OwnerBufferedOperatorRefusal("tenant_invalid");
  return tenant as ParityTenant;
}

function tenantSha256(tenant: ParityTenant): string {
  return createHash("sha256")
    .update("owner-buffered-parity-tenant-v1\0")
    .update(tenant.orgId).update("\0")
    .update(tenant.workspaceId).update("\0")
    .update(tenant.userId)
    .digest("hex");
}

/** Remove private tenant identifiers before the receipt can reach logs or Relay. */
export function publicCensusReceipt(receipt: ParityCensusReceipt): Record<string, unknown> {
  const proof = receipt.proof === null ? null : {
    contract: receipt.proof.contract,
    controlEpoch: receipt.proof.controlEpoch,
    privacyBaselineSha256: receipt.proof.privacyBaselineSha256,
    explicitNoHead: receipt.proof.explicitNoHead,
    head: receipt.proof.head,
    deletionSetSha256: receipt.proof.deletionSetSha256,
    resetSetSha256: receipt.proof.resetSetSha256,
    targetLineageDigest: receipt.proof.targetLineageDigest,
    targetGeneration: receipt.proof.targetGeneration,
  };
  return {
    contract: receipt.contract,
    tenantSha256: tenantSha256(receipt.tenant),
    equal: receipt.equal,
    mismatches: receipt.mismatches,
    proof,
  };
}

function verifyCheckout(expected: string): void {
  const options = { encoding: "utf8" as const, stdio: ["ignore", "pipe", "ignore"] as const };
  let head = "";
  let dirty = "";
  try {
    head = execFileSync("git", ["rev-parse", "HEAD"], options).trim();
    dirty = execFileSync("git", ["status", "--porcelain"], options).trim();
  } catch {
    throw new OwnerBufferedOperatorRefusal("checkout_unverifiable");
  }
  if (head !== expected || dirty) throw new OwnerBufferedOperatorRefusal("checkout_pin_mismatch");
}

function client(
  url: string,
  name: "neonUrl" | "supabaseUrl",
  trust: "neon" | "supabase",
  app: string,
): pg.Client {
  return pinnedPgClient(pg, url, name, trust, {
    application_name: app,
    statement_timeout: 5_000,
    lock_timeout: 2_000,
    idle_in_transaction_session_timeout: 15_000,
  });
}

async function connectNeon(url: string): Promise<pg.Client> {
  const neon = client(url, "neonUrl", "neon", "owner-buffered-g1-neon-readonly");
  await neon.connect();
  return neon;
}

async function requireReadOnly(client: Queryable, name: "neonUrl" | "supabaseUrl"): Promise<void> {
  await client.query("SET default_transaction_read_only = on");
  const row = (await client.query<{ ro: string }>(
    "SELECT current_setting('default_transaction_read_only') AS ro",
  )).rows[0];
  if (row?.ro !== "on") throw new OwnerBufferedOperatorRefusal(`${name}_not_read_only`);
}

/** Verify the canonical fenced owner session also owns the parity table. */
async function requireParityTableOwner(client: Queryable): Promise<void> {
  let rows: Array<{ login: string; actor: string; owner: string }>;
  try {
    rows = (await client.query<{ login: string; actor: string; owner: string }>(`
    SELECT session_user AS login,current_user AS actor,pg_get_userbyid(c.relowner) AS owner
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
     WHERE n.nspname='sessions' AND c.relname='owner_buffered_parity' AND c.relkind='r'`)).rows;
  } catch {
    throw new OwnerBufferedOperatorRefusal("target_owner_identity_unverifiable");
  }
  const row = rows[0];
  if (rows.length !== 1 || row?.login !== "cli_login_postgres" ||
      row.actor !== "postgres" || row.owner !== "postgres" || row.actor !== row.owner)
    throw new OwnerBufferedOperatorRefusal("target_owner_identity_refused");
}

/** Canonical owner route: reset-baseline proof, fixed backend and per-query identity fencing. */
export async function withParityOwnerSession<T>(
  target: ConnectedOwnerClient,
  readOnly: boolean,
  work: (target: Queryable) => Promise<T>,
): Promise<T> {
  return withOwnerSession(target, async (pid) => {
    const fenced = ownerFencedClient(target, pid);
    await requireParityTableOwner(fenced);
    if (readOnly) await requireReadOnly(fenced, "supabaseUrl");
    return work(fenced);
  });
}

/** Keep the enumerator and writer on separate, independently fenced owner sessions. */
export async function withBulkParityOwnerSessions<T>(
  enumerator: ConnectedOwnerClient,
  targetOwner: ConnectedOwnerClient,
  work: (enumerator: Queryable, targetOwner: Queryable) => Promise<T>,
): Promise<T> {
  return withParityOwnerSession(enumerator, true, (fencedEnumerator) =>
    withParityOwnerSession(targetOwner, false, (fencedTargetOwner) =>
      work(fencedEnumerator, fencedTargetOwner)));
}

function generationMatches(receipt: ParityCensusReceipt, expected: string): boolean {
  return receipt.proof !== null && receipt.proof.targetGeneration === expected;
}

export interface BulkParityCounts {
  batches: number;
  candidates: number;
  g1Equal: number;
  g1Mismatch: number;
  tenantRefused: number;
  inserted: number;
  updated: number;
  unchanged: number;
}

export function bulkCompletionStatus(counts: BulkParityCounts): "complete" | "partial" {
  return counts.g1Mismatch === 0 && counts.tenantRefused === 0 ? "complete" : "partial";
}

export interface BulkParityDependencies {
  readCensus?: typeof readParityCensus;
  writeProof?: typeof writeCurrentParityProof;
}

class BulkParityFatal extends OwnerBufferedOperatorRefusal {
  constructor(code: string, readonly counts: BulkParityCounts) { super(code); }
}

function emptyBulkCounts(): BulkParityCounts {
  return { batches: 0, candidates: 0, g1Equal: 0, g1Mismatch: 0, tenantRefused: 0,
    inserted: 0, updated: 0, unchanged: 0 };
}

function bulkTenant(row: Record<string, unknown>): ParityTenant | null {
  try {
    return parseTenant({ orgId: row.org_id, workspaceId: row.workspace_id, userId: row.user_id });
  } catch {
    return null;
  }
}

function fatalGeneration(code: string): boolean {
  return code === "target_generation_mismatch" || code === "bulk_target_generation_mismatch" ||
    code === "target_generation_postcondition_failed";
}

async function requireBulkGeneration(
  targetOwner: Queryable,
  expectedTargetGeneration: string,
  counts: BulkParityCounts,
): Promise<void> {
  const rows = (await targetOwner.query<{ generation: unknown }>(BULK_TARGET_GENERATION_SQL)).rows;
  if (rows.length !== 1 || rows[0]?.generation !== expectedTargetGeneration)
    throw new BulkParityFatal("bulk_target_generation_mismatch", counts);
}

/**
 * Enumerate one stable snapshot of every live buffered tenant, process only a
 * bounded FETCH page at a time, and isolate tenant parity/CAS refusals. The
 * writer's per-tenant repeatable-read transaction remains the authority.
 */
export async function runBulkParity(
  neon: Queryable,
  enumerator: Queryable,
  targetOwner: Queryable,
  expectedTargetGeneration: string,
  batchSize: number,
  dependencies: BulkParityDependencies = {},
  censusSource: ParityCensusSource = "neon",
): Promise<BulkParityCounts> {
  if (!HEX64.test(expectedTargetGeneration) || !Number.isSafeInteger(batchSize) ||
      batchSize < 1 || batchSize > MAX_BULK_BATCH_SIZE)
    throw new OwnerBufferedOperatorRefusal("bulk_arguments_invalid");
  const counts = emptyBulkCounts();
  const readCensus = dependencies.readCensus ?? readParityCensus;
  const writeProof = dependencies.writeProof ?? writeCurrentParityProof;
  await requireBulkGeneration(targetOwner, expectedTargetGeneration, counts);

  await enumerator.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    await enumerator.query("SET LOCAL statement_timeout = '30s'");
    await enumerator.query("SET LOCAL idle_in_transaction_session_timeout = '10min'");
    await enumerator.query(`DECLARE ${BULK_CURSOR} NO SCROLL CURSOR FOR ${BULK_BUFFERED_TENANTS_SQL}`);
    for (;;) {
      await requireBulkGeneration(targetOwner, expectedTargetGeneration, counts);
      const rows = (await enumerator.query<Record<string, unknown>>(
        `FETCH FORWARD ${batchSize} FROM ${BULK_CURSOR}`,
      )).rows;
      if (rows.length > batchSize) throw new OwnerBufferedOperatorRefusal("bulk_page_too_large");
      if (rows.length === 0) break;
      counts.batches++;
      for (const row of rows) {
        counts.candidates++;
        try {
          const tenant = bulkTenant(row);
          if (!tenant) {
            counts.tenantRefused++;
          } else {
            try {
              const g1 = await readCensus(neon, targetOwner, tenant, censusSource);
              if (!g1.equal || !g1.proof) {
                await requireBulkGeneration(targetOwner, expectedTargetGeneration, counts);
                counts.g1Mismatch++;
              } else {
                if (!generationMatches(g1, expectedTargetGeneration))
                  throw new BulkParityFatal("bulk_target_generation_mismatch", counts);
                counts.g1Equal++;
                const result = await writeProof(neon, targetOwner, tenant, expectedTargetGeneration, censusSource);
                if (!generationMatches(result.census, expectedTargetGeneration))
                  throw new BulkParityFatal("target_generation_postcondition_failed", counts);
                if (result.outcome === "insert") counts.inserted++;
                else if (result.outcome === "update") counts.updated++;
                else counts.unchanged++;
              }
            } catch (error) {
              if (error instanceof BulkParityFatal) throw error;
              if (error instanceof ParityCensusRefusal) {
                if (fatalGeneration(error.code)) throw new BulkParityFatal(error.code, counts);
                counts.tenantRefused++;
              } else {
                throw error;
              }
            }
          }
        } finally {
          // FETCH pages may take many minutes to process. Touch the read-only
          // cursor transaction after every tenant so its idle timeout cannot
          // deterministically expire between bounded pages.
          await enumerator.query(BULK_CURSOR_KEEPALIVE_SQL);
        }
      }
    }
    await requireBulkGeneration(targetOwner, expectedTargetGeneration, counts);
    await enumerator.query(`CLOSE ${BULK_CURSOR}`);
    await enumerator.query("COMMIT");
    return counts;
  } catch (error) {
    await enumerator.query("ROLLBACK").catch(() => undefined);
    if (error instanceof BulkParityFatal) throw error;
    throw new BulkParityFatal("bulk_enumeration_failed", counts);
  }
}

function output(fields: Record<string, unknown>): void {
  process.stdout.write(JSON.stringify({
    at: new Date().toISOString(),
    tool: "owner-buffered-parity-operator",
    ...fields,
  }) + "\n");
}

async function main(): Promise<number> {
  let args: OperatorArgs;
  try {
    args = parseOperatorArgs(process.argv.slice(2));
  } catch (error) {
    output({ status: "refused", code: error instanceof OwnerBufferedOperatorRefusal ? error.code : "usage" });
    return 64;
  }
  const clients: pg.Client[] = [];
  // Neon-mode receipts are byte-for-byte the pre-edge-5 shape; frozen mode appends its source.
  const emit = (fields: Record<string, unknown>): void =>
    output(args.censusSource === "neon" ? fields : { ...fields, censusSource: args.censusSource });
  try {
    refuseCredentialEnv();
    verifyCheckout(args.expectedSourceSha);
    const keys = operatorCredentialKeys(args);
    const input = await readStdinCredentials(keys.required, keys.allowed);
    validateCensusSourceInput(args, input);
    const tenant = args.mode === "bulk" ? null : parseTenant(input.tenant);
    // Validate every complete URL identity and TLS policy before opening any connection.
    const neonUrl = args.censusSource === "neon" ? sessionAffinityUrl(input.neonUrl, "neonUrl", "neon") : null;
    const supabaseUrl = sessionAffinityUrl(input.supabaseUrl, "supabaseUrl", "supabase");
    if (neonUrl !== null) pinnedPgConfig(neonUrl, "neonUrl", "neon");
    pinnedPgConfig(supabaseUrl, "supabaseUrl", "supabase");

    const runWithSource = async (neon: Queryable): Promise<number> => {
      if (args.mode === "bulk") {
        const enumerator = client(
          supabaseUrl, "supabaseUrl", "supabase", "owner-buffered-bulk-target-enumerator",
        );
        const targetOwner = client(
          supabaseUrl, "supabaseUrl", "supabase", "owner-buffered-bulk-target-owner",
        );
        // Await inside this try so owner-route failures are sanitized and Neon stays open until cleanup.
        return await withBulkParityOwnerSessions(enumerator, targetOwner, async (fencedEnumerator, fencedTargetOwner) => {
          try {
            const counts = await runBulkParity(
              neon, fencedEnumerator, fencedTargetOwner, args.expectedTargetGeneration, args.batchSize, {}, args.censusSource,
            );
            const status = bulkCompletionStatus(counts);
            emit({ status, mode: "bulk", sourceSha: args.expectedSourceSha,
              targetGeneration: args.expectedTargetGeneration, counts });
            return status === "complete" ? 0 : 3;
          } catch (error) {
            if (error instanceof BulkParityFatal) {
              emit({ status: "refused", mode: "bulk", code: error.code,
                sourceSha: args.expectedSourceSha, targetGeneration: args.expectedTargetGeneration, counts: error.counts });
              return 1;
            }
            throw error;
          }
        });
      }

      const target = client(
        supabaseUrl,
        "supabaseUrl",
        "supabase",
        args.mode === "census" ? "owner-buffered-g1-target-readonly" : "owner-buffered-g2-target-owner",
      );
      return await withParityOwnerSession(target, args.mode === "census", async (fencedTarget) => {
        const g1 = await readParityCensus(neon, fencedTarget, tenant!, args.censusSource);
        if (!g1.equal || !g1.proof) {
          emit({ status: "mismatch", mode: args.mode, sourceSha: args.expectedSourceSha, census: publicCensusReceipt(g1) });
          return 3;
        }
        if (!generationMatches(g1, args.expectedTargetGeneration)) {
          emit({
            status: "refused",
            code: "target_generation_mismatch",
            mode: args.mode,
            sourceSha: args.expectedSourceSha,
            census: publicCensusReceipt(g1),
          });
          return 1;
        }
        if (args.mode === "census") {
          emit({ status: "equal", mode: args.mode, sourceSha: args.expectedSourceSha, census: publicCensusReceipt(g1) });
          return 0;
        }

        // G2 performs its own fresh census and source recheck in the write transaction.
        const result = await writeCurrentParityProof(
          neon, fencedTarget, tenant!, args.expectedTargetGeneration, args.censusSource,
        );
        if (!generationMatches(result.census, args.expectedTargetGeneration))
          throw new OwnerBufferedOperatorRefusal("target_generation_postcondition_failed");
        emit({
          status: result.outcome === "unchanged" ? "unchanged" : "written",
          mode: args.mode,
          sourceSha: args.expectedSourceSha,
          outcome: result.outcome,
          writtenAt: result.writtenAt,
          census: publicCensusReceipt(result.census),
        });
        return 0;
      });
    };

    if (neonUrl !== null) {
      const neon = await connectNeon(neonUrl);
      clients.push(neon);
      await requireReadOnly(neon, "neonUrl");
      return await runWithSource(neon);
    }
    // Frozen source: the copy's tables are owner-only, so read them through their own
    // canonical fenced owner session, pinned read-only, separate from the target session.
    const frozenSource = client(supabaseUrl, "supabaseUrl", "supabase", "owner-buffered-frozen-source-readonly");
    return await withParityOwnerSession(frozenSource, true, runWithSource);
  } catch (error) {
    const code = error instanceof OwnerBufferedOperatorRefusal || error instanceof ParityCensusRefusal ||
      error instanceof CredentialRefusal ? error.code : "operator_failed";
    emit({ status: "refused", code: /^[A-Za-z0-9_.:,-]{1,160}$/u.test(code) ? code : "operator_failed" });
    return 1;
  } finally {
    for (const client of clients.reverse()) await client.end().catch(() => undefined);
  }
}

if (import.meta.main) process.exit(await main());
