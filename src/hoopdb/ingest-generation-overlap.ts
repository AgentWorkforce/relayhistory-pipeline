/**
 * Owner-side helpers for the bounded generation overlap (migration 20261105053020; design
 * docs/design/zero-503-schema-windows.md).
 *
 * The owner opens ONE window (from = the current PASS generation, to = the generation the apply is about
 * to install) BEFORE the owner-apply, so the buffered ingest fence keeps admitting `from` while the marker
 * is pending/PASS for `to`, until the rebind (which closes the window in its own transaction) or the
 * window's expiry. Every check lives in SQL; these helpers only validate the shape early and run the
 * owner functions. They must run as the function owner (the rebind CLI's SET ROLE postgres route).
 */
import type { MigrationClient } from "./migrate.ts";
import type { OwnerApplyIntegration } from "./owner-operator-apply.ts";

const HEX64 = /^[0-9a-f]{64}$/;
export const OVERLAP_TABLE = "sessions.ingest_generation_overlap_windows";
export const OVERLAP_MIN_TTL_SECONDS = 60;
export const OVERLAP_MAX_TTL_SECONDS = 14400;

export interface OverlapOpenInput { from: string; to: string; ttlSeconds: number; admitDuringApply: boolean; approvalSha256: string }
export interface OverlapWindow { windowId: string; from: string; to: string; admitDuringApply: boolean; openedAt: string; expiresAt: string }

const refuse = (code: string): never => { throw Object.assign(new Error("ingest generation overlap: " + code), { code }); };

export function validateOverlapOpen(input: OverlapOpenInput): void {
  if (!HEX64.test(input.from) || !HEX64.test(input.to) || input.from === input.to) refuse("invalid_generations");
  if (!HEX64.test(input.approvalSha256)) refuse("invalid_approval");
  if (!Number.isInteger(input.ttlSeconds) || input.ttlSeconds < OVERLAP_MIN_TTL_SECONDS || input.ttlSeconds > OVERLAP_MAX_TTL_SECONDS) refuse("invalid_ttl");
  if (typeof input.admitDuringApply !== "boolean") refuse("invalid_admit_during_apply");
}

/** True once 20261105053020 is installed (the rebind and the open helpers degrade/refuse on this). */
export async function overlapInstalled(client: MigrationClient): Promise<boolean> {
  const r = await client.query<{ t: string | null }>(`SELECT to_regclass('${OVERLAP_TABLE}')::text AS t`);
  return r.rows[0]?.t != null;
}

export async function openGenerationOverlap(client: MigrationClient, input: OverlapOpenInput): Promise<OverlapWindow> {
  validateOverlapOpen(input);
  if (!(await overlapInstalled(client))) refuse("overlap_not_installed");
  const r = await client.query<{ r: OverlapWindow }>("SELECT sessions.ingest_generation_overlap_open($1::jsonb) AS r", [JSON.stringify({
    from: input.from, to: input.to, ttlSeconds: input.ttlSeconds, admitDuringApply: input.admitDuringApply, approvalSha256: input.approvalSha256 })]);
  return r.rows[0]!.r;
}

export async function closeGenerationOverlap(client: MigrationClient, input: { from: string; to: string; reason: "owner" | "rebind" }): Promise<{ closed: boolean; reason?: string }> {
  if (!HEX64.test(input.from) || !HEX64.test(input.to) || input.from === input.to) refuse("invalid_generations");
  const r = await client.query<{ r: { closed: boolean; reason?: string } }>("SELECT sessions.ingest_generation_overlap_close($1::jsonb) AS r", [JSON.stringify(input)]);
  return r.rows[0]!.r;
}

/** The open window, or null. Read-only. */
export async function generationOverlapStatus(client: MigrationClient): Promise<(OverlapWindow & { expired: boolean }) | null> {
  if (!(await overlapInstalled(client))) return null;
  const r = await client.query<any>(`SELECT window_id::text AS "windowId",from_generation AS "from",to_generation AS "to",admit_during_apply AS "admitDuringApply",
    opened_at AS "openedAt",expires_at AS "expiresAt",expires_at<=clock_timestamp() AS expired FROM ${OVERLAP_TABLE} WHERE closed_at IS NULL`);
  return r.rows[0] ?? null;
}

/**
 * An owner-apply integration (owner-operator-apply.ts OwnerApplyIntegration) that opens the window under the
 * migrator advisory lock, after every approval/source/identity check and BEFORE the pending marker write,
 * bound to the approved target generation and approval digest. Not wired into any stock CLI: a fixed entry
 * that uses it is its own reviewed change.
 */
export function generationOverlapIntegration(client: MigrationClient, opts: { from: string; ttlSeconds: number; admitDuringApply: boolean }): OwnerApplyIntegration {
  return Object.freeze({
    beforeWrite: async (context: { generation: string; approvalSha256: string }) => {
      await openGenerationOverlap(client, { from: opts.from, to: context.generation, ttlSeconds: opts.ttlSeconds, admitDuringApply: opts.admitDuringApply, approvalSha256: context.approvalSha256 });
    },
  });
}
