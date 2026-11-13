/**
 * CI operator for the DB half of a reviewed generation overlap (migration 20261105044030;
 * docs/design/zero-503-schema-windows.md section 3.2).
 *
 * The pair is never a dispatch input: it is the reviewed PRIVATE_INGEST_OVERLAP constant in
 * src/session-ingest/private-ingest-release.ts, the same constant the ingest Worker deploy reads. A
 * dormant (null) constant refuses every mode. Each mode is one owner transaction under the migrator's
 * advisory lock (961996, 1):
 *
 *   open      typed `OPEN-OVERLAP <G> <H> TTL <ttl> SHA <sha>`; refuses unless readiness is PASS at G,
 *             the lazy custody policy is at G, the contract is still bound to G, and no other unexpired
 *             window is open. admitDuringApply is always false here (the old generation is admitted
 *             only once H has PASSED). The approval digest is sha256(confirmation).
 *   readback  READ ONLY; empty confirmation and TTL. Refuses a foreign unexpired window.
 *   close     typed `CLOSE-OVERLAP <G> <H> SHA <sha>`; owner close of the contract pair only. Closing
 *             when nothing is open is an idempotent {closed:false}.
 *   rebind-dry-run
 *             typed `REBIND-DRY-RUN <G> <H> SHA <sha>`; rebindIngestGeneration(from G, to H, dryRun) in
 *             one owner transaction that is always rolled back. Requires the contract still bound to G
 *             (the post-apply recovery state: Worker G+H deployed, readiness PASS at H, no window needed).
 *   rebind    typed `REBIND <G> <H> SHA <sha> DRY <dry_run_id>`, where dry_run_id is a successful
 *             rebind-dry-run run of the workflow at the same SHA (checked by the GitHub gate); the same
 *             call committed. Moves the lazy policy and every admission row G -> H together and closes a
 *             matching open window in the same transaction (none is required).
 *             Both rebind modes take from/to only from the reviewed pair and print counts and flags only.
 *
 * Every other check (owner-only, TTL bound, append-only, no downgrade) stays in SQL. The rebind
 * (ingest-generation-rebind.ts) still closes the window in its own transaction.
 */
import { createHash } from "node:crypto";
import type { MigrationClient } from "./migrate.ts";
import { ROLE_VALIDATION_NAME, ROLE_VALIDATION_TABLE } from "../shared/db.ts";
import {
  OVERLAP_MAX_TTL_SECONDS,
  OVERLAP_MIN_TTL_SECONDS,
  OVERLAP_TABLE,
  closeGenerationOverlap,
  generationOverlapStatus,
  openGenerationOverlap,
  overlapInstalled,
} from "./ingest-generation-overlap.ts";
import { rebindIngestGeneration, type RebindResult } from "./ingest-generation-rebind.ts";
import {
  PRIVATE_INGEST_GENERATION_CONTRACT,
  type PrivateIngestGenerationContract,
} from "../session-ingest/private-ingest-release.ts";

export type OverlapOperatorMode = "open" | "readback" | "close" | "rebind-dry-run" | "rebind";
export const OVERLAP_OPERATOR_MODES: readonly OverlapOperatorMode[] = ["readback", "open", "close", "rebind-dry-run", "rebind"];
export const isRebindMode = (mode: OverlapOperatorMode): mode is "rebind-dry-run" | "rebind" =>
  mode === "rebind-dry-run" || mode === "rebind";
export interface OverlapOperatorInput {
  mode: OverlapOperatorMode;
  sha: string;
  from: string;
  to: string;
  ttlSeconds: number | null;
  approvalSha256: string | null;
  /** rebind only: the successful same-SHA rebind-dry-run run id the GitHub gate binds; null otherwise. */
  dryRunId: string | null;
}

const HEX64 = /^[0-9a-f]{64}$/u;
export const refuseOverlapOperator = (code: string): never => {
  throw Object.assign(new Error(code), { code });
};
const refuse = refuseOverlapOperator;

/** The exact typed confirmation for a mode, or "" for readback. */
export function overlapOperatorConfirmation(
  mode: OverlapOperatorMode, from: string, to: string, sha: string, ttl?: string, dryRunId?: string,
): string {
  if (mode === "open") return `OPEN-OVERLAP ${from} ${to} TTL ${ttl} SHA ${sha}`;
  if (mode === "close") return `CLOSE-OVERLAP ${from} ${to} SHA ${sha}`;
  if (mode === "rebind-dry-run") return `REBIND-DRY-RUN ${from} ${to} SHA ${sha}`;
  if (mode === "rebind") return `REBIND ${from} ${to} SHA ${sha} DRY ${dryRunId}`;
  return "";
}

/** Validate the dispatch inputs against the reviewed pair before any credential is used. */
export function overlapOperatorInputs(
  env: Record<string, string | undefined>,
  contract: PrivateIngestGenerationContract = PRIVATE_INGEST_GENERATION_CONTRACT,
): OverlapOperatorInput {
  const mode = env.OVERLAP_MODE as OverlapOperatorMode, sha = env.OVERLAP_SHA ?? "", ttl = env.OVERLAP_TTL_SECONDS ?? "",
    confirmation = env.OVERLAP_CONFIRMATION ?? "", dryRun = env.OVERLAP_DRY_RUN_ID ?? "";
  if (!OVERLAP_OPERATOR_MODES.includes(mode) || !/^[0-9a-f]{40}$/u.test(sha))
    refuse("overlap_operator_inputs");
  const pair = contract.overlap;
  if (pair === null) return refuse("overlap_operator_dormant");
  if (!HEX64.test(contract.generation) || !HEX64.test(pair.from) || !HEX64.test(pair.to) || pair.from === pair.to ||
      (contract.generation !== pair.from && contract.generation !== pair.to))
    refuse("overlap_operator_contract");
  let ttlSeconds: number | null = null;
  if (mode === "open") {
    if (contract.generation !== pair.from) refuse("overlap_operator_stale");
    ttlSeconds = /^[1-9][0-9]{1,4}$/u.test(ttl) ? Number(ttl) : NaN;
    if (!(ttlSeconds >= OVERLAP_MIN_TTL_SECONDS && ttlSeconds <= OVERLAP_MAX_TTL_SECONDS))
      refuse("overlap_operator_ttl");
  } else if (ttl !== "") refuse("overlap_operator_ttl");
  // The rebind is offered only in the step-1 state (contract still bound to G, Worker accepting {G, H}).
  if (isRebindMode(mode) && contract.generation !== pair.from) refuse("overlap_operator_stale");
  if (mode === "rebind" ? !/^[1-9][0-9]{0,19}$/u.test(dryRun) : dryRun !== "")
    refuse("overlap_operator_dry_run_id");
  if (confirmation !== overlapOperatorConfirmation(mode, pair.from, pair.to, sha, ttl, dryRun))
    refuse("overlap_operator_confirmation");
  return {
    mode,
    sha,
    from: pair.from,
    to: pair.to,
    ttlSeconds,
    approvalSha256: mode === "readback" ? null : createHash("sha256").update(confirmation).digest("hex"),
    dryRunId: mode === "rebind" ? dryRun : null,
  };
}

export interface OverlapOperatorReceipt {
  status: "OVERLAP_WINDOW_OPENED" | "OVERLAP_WINDOW_READBACK_PASS" | "OVERLAP_WINDOW_CLOSED" | "OVERLAP_WINDOW_NONE_OPEN";
  mode: OverlapOperatorMode;
  from: string;
  to: string;
  marker: { generation: string | null; status: string | null };
  policyGeneration: string | null;
  /** none | open | expired (still unclosed, admits nothing) for the contract pair after the mode ran. */
  window: "none" | "open" | "expired";
  windowId: string | null;
  openedAt: string | null;
  expiresAt: string | null;
  closedReason: string | null;
  approvalSha256: string | null;
}

async function state(client: MigrationClient, lock: boolean) {
  if (!(await overlapInstalled(client))) refuse("overlap_operator_not_installed");
  const marker = (await client.query<{ generation: string; status: string }>(
    `SELECT generation, status FROM ${ROLE_VALIDATION_TABLE} WHERE name = $1${lock ? " FOR SHARE" : ""}`,
    [ROLE_VALIDATION_NAME])).rows[0] ?? null;
  const policy = (await client.query<{ generation: string }>(
    `SELECT generation FROM sessions.ingest_lazy_policy WHERE singleton${lock ? " FOR SHARE" : ""}`)).rows[0] ?? null;
  return { marker, policy, window: await generationOverlapStatus(client) };
}

const iso = (value: unknown) => value == null ? null : value instanceof Date ? value.toISOString() : new Date(String(value)).toISOString();

/** Run one mode. The caller has already proved the owner identity and the dispatch gates. */
export async function runOverlapOperator(client: MigrationClient, input: OverlapOperatorInput): Promise<OverlapOperatorReceipt> {
  if (isRebindMode(input.mode)) refuse("overlap_operator_inputs");
  const readOnly = input.mode === "readback";
  await client.query(readOnly ? "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY" : "BEGIN");
  try {
    await client.query("SELECT pg_advisory_xact_lock(961996, 1)");
    const before = await state(client, !readOnly);
    const foreign = before.window !== null && (before.window.from !== input.from || before.window.to !== input.to);
    let closedReason: string | null = null, status: OverlapOperatorReceipt["status"];
    if (input.mode === "open") {
      if (before.marker?.generation !== input.from || before.marker?.status !== "pass")
        refuse("overlap_operator_marker_not_pass_at_from");
      if (before.policy?.generation !== input.from) refuse("overlap_operator_policy_not_at_from");
      // An expired foreign window admits nothing; the SQL open sweeps it closed as 'expired'.
      if (foreign && !before.window!.expired) refuse("overlap_operator_foreign_window");
      if (!foreign && before.window !== null && !before.window.expired) refuse("overlap_operator_already_open");
      await openGenerationOverlap(client, {
        from: input.from, to: input.to, ttlSeconds: input.ttlSeconds!, admitDuringApply: false, approvalSha256: input.approvalSha256!,
      });
      status = "OVERLAP_WINDOW_OPENED";
    } else if (input.mode === "close") {
      if (foreign) refuse("overlap_operator_foreign_window");
      const closed = await closeGenerationOverlap(client, { from: input.from, to: input.to, reason: "owner" });
      if (closed.closed && before.window) {
        closedReason = (await client.query<{ r: string }>(
          `SELECT closed_reason AS r FROM ${OVERLAP_TABLE} WHERE window_id::text = $1`, [before.window.windowId])).rows[0]?.r ?? null;
        if (closedReason !== "owner" && closedReason !== "expired") refuse("overlap_operator_close_postcondition");
      }
      status = closed.closed ? "OVERLAP_WINDOW_CLOSED" : "OVERLAP_WINDOW_NONE_OPEN";
    } else {
      if (foreign && !before.window!.expired) refuse("overlap_operator_foreign_window");
      status = "OVERLAP_WINDOW_READBACK_PASS";
    }
    const after = await state(client, false);
    const mine = after.window !== null && after.window.from === input.from && after.window.to === input.to ? after.window : null;
    if (input.mode === "open" && (!mine || mine.expired || mine.admitDuringApply !== false ||
        Date.parse(iso(mine.expiresAt)!) - Date.parse(iso(mine.openedAt)!) !== input.ttlSeconds! * 1000))
      refuse("overlap_operator_open_postcondition");
    if (input.mode === "close" && after.window !== null) refuse("overlap_operator_close_postcondition");
    await client.query(readOnly ? "ROLLBACK" : "COMMIT");
    return {
      status,
      mode: input.mode,
      from: input.from,
      to: input.to,
      marker: { generation: after.marker?.generation ?? null, status: after.marker?.status ?? null },
      policyGeneration: after.policy?.generation ?? null,
      window: mine === null ? "none" : mine.expired ? "expired" : "open",
      windowId: mine?.windowId ?? null,
      openedAt: iso(mine?.openedAt),
      expiresAt: iso(mine?.expiresAt),
      closedReason,
      approvalSha256: input.approvalSha256,
    };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

export interface RebindOperatorReceipt {
  status: "INGEST_REBIND_DRY_RUN_PASS" | "INGEST_REBIND_COMMITTED";
  mode: "rebind-dry-run" | "rebind";
  from: string;
  to: string;
  dryRunId: string | null;
  approvalSha256: string;
  /** Counts and flags only (no secrets): policy rebound/already, admission rows moved/total, overlapClosed. */
  rebind: RebindResult;
  /** Read after the transaction: committed state for rebind, the untouched state for rebind-dry-run. */
  marker: { generation: string | null; status: string | null };
  policyGeneration: string | null;
}

/**
 * rebind-dry-run / rebind: rebindIngestGeneration with from/to taken only from the reviewed pair. The
 * rebind itself refuses unless readiness is PASS at `to`, the policy is at `from` (or already `to`),
 * every admission row is at `from` or `to`, and any open window is exactly (from -> to). Afterwards a
 * read-only readback proves the committed (or, for the dry run, unchanged) policy generation.
 */
export async function runRebindOperator(client: MigrationClient, input: OverlapOperatorInput): Promise<RebindOperatorReceipt> {
  const mode = input.mode;
  if (!isRebindMode(mode) || input.approvalSha256 === null || (mode === "rebind") !== (input.dryRunId !== null))
    return refuse("overlap_operator_inputs");
  const dryRun = mode === "rebind-dry-run";
  const rebind = await rebindIngestGeneration(client, { from: input.from, to: input.to, dryRun });
  if (rebind.dryRun !== dryRun || rebind.from !== input.from || rebind.to !== input.to)
    refuse("overlap_operator_rebind_postcondition");
  await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    const marker = (await client.query<{ generation: string; status: string }>(
      `SELECT generation, status FROM ${ROLE_VALIDATION_TABLE} WHERE name = $1`, [ROLE_VALIDATION_NAME])).rows[0] ?? null;
    const policy = (await client.query<{ generation: string }>(
      "SELECT generation FROM sessions.ingest_lazy_policy WHERE singleton")).rows[0] ?? null;
    await client.query("ROLLBACK");
    // A committed rebind leaves the policy at H; a dry run whose policy step was 'rebound' left it at G.
    if (!dryRun && policy?.generation !== input.to) refuse("overlap_operator_rebind_postcondition");
    if (dryRun && rebind.policy === "rebound" && policy?.generation !== input.from)
      refuse("overlap_operator_rebind_postcondition");
    return {
      status: dryRun ? "INGEST_REBIND_DRY_RUN_PASS" : "INGEST_REBIND_COMMITTED",
      mode,
      from: input.from,
      to: input.to,
      dryRunId: input.dryRunId,
      approvalSha256: input.approvalSha256,
      rebind,
      marker: { generation: marker?.generation ?? null, status: marker?.status ?? null },
      policyGeneration: policy?.generation ?? null,
    };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}
