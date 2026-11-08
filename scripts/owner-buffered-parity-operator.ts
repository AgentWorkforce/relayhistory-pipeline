/**
 * Root-only Option C G1/G2 operator.
 *
 * Credentials and the private tenant tuple arrive as one bounded JSON object on
 * a pipe or socket, never argv, environment, terminal, or a file:
 *   {"neonUrl":"...","supabaseUrl":"...","tenant":{"orgId":"...","workspaceId":"...","userId":"..."}}
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
 * Output is one aggregate JSON receipt. It contains a tenant hash, never the
 * org/workspace/user identifiers or database URLs. A write still requires the
 * operator to have Khaliq's direct production approval; the flag is an
 * accidental-execution guard, not approval by itself.
 */
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import pg from "pg";
import {
  ParityCensusRefusal,
  readParityCensus,
  writeCurrentParityProof,
  type ParityCensusReceipt,
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

const HEX40 = /^[0-9a-f]{40}$/u;
const HEX64 = /^[0-9a-f]{64}$/u;
const WRITE_CONFIRMATION = "owner-buffered-parity/v1";

export class OwnerBufferedOperatorRefusal extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "OwnerBufferedOperatorRefusal";
  }
}

export interface OperatorArgs {
  mode: "census" | "write";
  expectedSourceSha: string;
  expectedTargetGeneration: string;
}

export function parseOperatorArgs(argv: readonly string[]): OperatorArgs {
  const [mode, ...rest] = argv;
  if (mode !== "census" && mode !== "write") throw new OwnerBufferedOperatorRefusal("usage");
  const allowed = new Set(["--expect-source-sha", "--expect-target-generation", "--confirm-write"]);
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
  if (mode === "write" ? confirmation !== WRITE_CONFIRMATION : confirmation !== undefined)
    throw new OwnerBufferedOperatorRefusal("usage");
  return { mode, expectedSourceSha, expectedTargetGeneration };
}

function parseTenant(value: unknown): ParityTenant {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new OwnerBufferedOperatorRefusal("tenant_invalid");
  const row = value as Record<string, unknown>;
  if (Object.keys(row).sort().join(",") !== "orgId,userId,workspaceId")
    throw new OwnerBufferedOperatorRefusal("tenant_invalid");
  const tenant = { orgId: row.orgId, workspaceId: row.workspaceId, userId: row.userId };
  if (!Object.values(tenant).every((part) => typeof part === "string" && part.length >= 1 && part.length <= 128))
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

async function connect(
  url: string,
  name: "neonUrl" | "supabaseUrl",
  trust: "neon" | "supabase",
  app: string,
  readOnly: boolean,
): Promise<pg.Client> {
  const client = pinnedPgClient(pg, url, name, trust, {
    application_name: app,
    statement_timeout: 5_000,
    lock_timeout: 2_000,
    idle_in_transaction_session_timeout: 15_000,
  });
  await client.connect();
  if (readOnly) {
    await client.query("SET default_transaction_read_only = on");
    const row = (await client.query<{ ro: string }>("SELECT current_setting('default_transaction_read_only') AS ro")).rows[0];
    if (row?.ro !== "on") throw new OwnerBufferedOperatorRefusal(`${name}_not_read_only`);
  }
  return client;
}

async function requireParityTableOwner(client: pg.Client): Promise<void> {
  const row = (await client.query<{ actor: string; owner: string }>(`
    SELECT current_user AS actor,pg_get_userbyid(c.relowner) AS owner
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
     WHERE n.nspname='sessions' AND c.relname='owner_buffered_parity' AND c.relkind='r'`)).rows[0];
  if (!row || row.actor !== row.owner) throw new OwnerBufferedOperatorRefusal("target_not_parity_table_owner");
}

function generationMatches(receipt: ParityCensusReceipt, expected: string): boolean {
  return receipt.proof !== null && receipt.proof.targetGeneration === expected;
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
  try {
    refuseCredentialEnv();
    verifyCheckout(args.expectedSourceSha);
    const input = await readStdinCredentials(["neonUrl", "supabaseUrl", "tenant"]);
    const tenant = parseTenant(input.tenant);
    // Validate both complete URL identities and TLS policies before opening either connection.
    const neonUrl = sessionAffinityUrl(input.neonUrl, "neonUrl", "neon");
    const supabaseUrl = sessionAffinityUrl(input.supabaseUrl, "supabaseUrl", "supabase");
    pinnedPgConfig(neonUrl, "neonUrl", "neon");
    pinnedPgConfig(supabaseUrl, "supabaseUrl", "supabase");

    const neon = await connect(neonUrl, "neonUrl", "neon", "owner-buffered-g1-neon-readonly", true);
    clients.push(neon);
    const target = await connect(
      supabaseUrl,
      "supabaseUrl",
      "supabase",
      args.mode === "census" ? "owner-buffered-g1-target-readonly" : "owner-buffered-g2-target-owner",
      args.mode === "census",
    );
    clients.push(target);

    const g1 = await readParityCensus(neon, target, tenant);
    if (!g1.equal || !g1.proof) {
      output({ status: "mismatch", mode: args.mode, sourceSha: args.expectedSourceSha, census: publicCensusReceipt(g1) });
      return 3;
    }
    if (!generationMatches(g1, args.expectedTargetGeneration)) {
      output({
        status: "refused",
        code: "target_generation_mismatch",
        mode: args.mode,
        sourceSha: args.expectedSourceSha,
        census: publicCensusReceipt(g1),
      });
      return 1;
    }
    if (args.mode === "census") {
      output({ status: "equal", mode: args.mode, sourceSha: args.expectedSourceSha, census: publicCensusReceipt(g1) });
      return 0;
    }

    await requireParityTableOwner(target);
    // G2 performs its own fresh census and source recheck in the write transaction.
    const result = await writeCurrentParityProof(neon, target, tenant, args.expectedTargetGeneration);
    if (!generationMatches(result.census, args.expectedTargetGeneration))
      throw new OwnerBufferedOperatorRefusal("target_generation_postcondition_failed");
    output({
      status: result.outcome === "unchanged" ? "unchanged" : "written",
      mode: args.mode,
      sourceSha: args.expectedSourceSha,
      outcome: result.outcome,
      writtenAt: result.writtenAt,
      census: publicCensusReceipt(result.census),
    });
    return 0;
  } catch (error) {
    const code = error instanceof OwnerBufferedOperatorRefusal || error instanceof ParityCensusRefusal ||
      error instanceof CredentialRefusal ? error.code : "operator_failed";
    output({ status: "refused", code: /^[A-Za-z0-9_.:,-]{1,160}$/u.test(code) ? code : "operator_failed" });
    return 1;
  } finally {
    for (const client of clients.reverse()) await client.end().catch(() => undefined);
  }
}

if (import.meta.main) process.exit(await main());
