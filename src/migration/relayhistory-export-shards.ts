/**
 * Grant sharding for the Neon-to-S3 export. One RelayAuth migration grant
 * stores one canonical plan, and relayauth #111 bounds what a grant may hold
 * (distinct source users, canonical plan + allowed-pairs bytes, manifests,
 * chunks per manifest, chunks per plan). This planner splits an export into
 * grant-sized shards.
 *
 * - **Oracle, not a copy of the limits.** Every candidate shard is admitted
 *   only if relayauth's own `deriveMigrationGrantPlan` (vendored unchanged in
 *   vendor/relayauth-migration-plan.ts) accepts it. The planner never
 *   compares a count against a limit itself; it splits where the oracle first
 *   refuses, so whichever limit binds first decides.
 * - **Sessions are indivisible.** All manifests of one session (org,
 *   workspace, harness, session id; one per machine) go into the same shard.
 *   A session that no grant can hold alone is unshardable: it is reported as
 *   a typed `oversized_session` (ids and counts only, never content) and is
 *   never split or minted. `requireShardable` refuses the whole plan then.
 * - **Deterministic.** Sessions are ordered by (org, source user, workspace,
 *   harness, session id), so the input order never matters, and each shard is
 *   the longest run of sessions in that order the oracle accepts.
 * - **Stable ids.** A shard id is the hash of the planner version, the
 *   contract version, the org, `exported_at` and the shard's canonical,
 *   ordered entries. It does not depend on array position. What changes ids:
 *   a shard's id changes exactly when its contents change. Adding, removing
 *   or changing a session changes the shard it lands in and, because packing
 *   is greedy in that order, can shift the boundaries (and so the ids) of
 *   that org's later shards. Shards earlier in the order, and every shard of
 *   another org, keep their ids. A different `exported_at` or planner version
 *   changes every id.
 * - **Grant ids are the issuer's.** RelayAuth assigns the grant id (and with
 *   it the plan digest) when a grant is created. The planner measures with a
 *   placeholder of the same shape and length (`mgr_<shard id>`), so the byte
 *   count is exact for any real grant id.
 * - **Dry and payload-free.** The input is the contract's upload inputs (ids,
 *   offsets, sizes, digests); no chunk bytes, no minting, no network.
 * - **Resume per shard.** `runShards` records each finished shard under its
 *   id, skips it on a rerun, and gives each shard its own courier checkpoint.
 */
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { createHash } from "node:crypto";
import {
  canonicalMigrationId,
  canonicalMigrationJson,
  deriveMigrationGrantPlan,
  migrationGrantSerializedBytes,
  MigrationPlanError,
} from "./vendor/relayauth-migration-plan.ts";
import type { ExportPlan, ExportUploadResult, UploadInput } from "./relayhistory-export-upload.ts";
import { manifestKey } from "@rhc-object-store";

export const SHARD_PLANNER_VERSION = 1;
export const PLAN_CONTRACT = "relayhistory-export-plan-entry-v1";

/** What the oracle refuses when a candidate is too big (vs. malformed input). */
const LIMIT_CODES = new Set(["migration_plan_too_large", "migration_plan_source_user_limit"]);
const LIMIT_MESSAGE = /must contain 1 to \d+|more than \d+/;

export interface SessionUnit {
  /** `org/workspace/harness/session` ids. */
  session: string;
  org_id: string;
  uploads: UploadInput[];
}

export interface Shard {
  shard_id: string;
  org_id: string;
  /** Placeholder grant id used for measuring; RelayAuth assigns the real one. */
  measure_grant_id: string;
  /** The plan body a grant is created with (`{exported_at, manifests}`). */
  plan: { exported_at: string; manifests: UploadInput[] };
  sessions: number;
  manifests: number;
  chunks: number;
  source_users: number;
  serialized_bytes: number;
  /** Why the shard ended: the oracle's refusal of the next session, or "end". */
  closed_by: string;
}

export interface UnshardableSession {
  code: "oversized_session";
  org_id: string;
  session: string;
  manifests: number;
  chunks: number;
  source_users: number;
  /** The oracle's refusal of the session alone. */
  oracle_code: string;
  oracle_message: string;
}

export interface ShardPlan {
  planner_version: number;
  contract: string;
  exported_at: string;
  shards: Shard[];
  unshardable: UnshardableSession[];
}

export class ShardPlanningError extends Error {
  constructor(
    readonly code: "oversized_session" | "invalid_session",
    message: string,
    readonly sessions: readonly Omit<UnshardableSession, "code">[] = [],
  ) {
    super(message);
    this.name = "ShardPlanningError";
  }
}

const sha256Hex = (s: string) => createHash("sha256").update(s).digest("hex");
const cmp = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

/** Group uploads into indivisible sessions, in the planner's deterministic order. */
export function sessionUnits(uploads: readonly UploadInput[]): SessionUnit[] {
  const groups = new Map<string, SessionUnit>();
  for (const u of uploads) {
    const session = `${u.org_id}/${u.workspace_id}/${u.session.harness}/${u.session.session_id}`;
    const unit = groups.get(session) ?? { session, org_id: u.org_id, uploads: [] };
    unit.uploads.push(u);
    groups.set(session, unit);
  }
  const units = [...groups.values()];
  for (const unit of units) unit.uploads.sort((a, b) => cmp(a.key, b.key));
  // Malformed ids sort by their raw form; the oracle refuses them later.
  const user = (id: string) => {
    try {
      return canonicalMigrationId(id, "source user id");
    } catch {
      return id;
    }
  };
  const sortKey = (u: SessionUnit) => `${u.org_id}\u0000${u.uploads.map((x) => user(x.source_user_id)).sort()[0]}\u0000${u.session}`;
  return units.sort((a, b) => cmp(sortKey(a), sortKey(b)));
}

type Verdict = { ok: true; bytes: number; users: string[]; chunks: number } | { ok: false; code: string; message: string; limit: boolean };

/** The oracle: relayauth's own derivation, with a placeholder grant id. */
function admit(orgId: string, exportedAt: string, uploads: readonly UploadInput[], grantId = `mgr_${"0".repeat(32)}`): Verdict {
  try {
    const derived = deriveMigrationGrantPlan({ grantId, orgId, plan: { exported_at: exportedAt, manifests: uploads } });
    return {
      ok: true,
      bytes: migrationGrantSerializedBytes(derived.plan, derived.allowedPairs),
      users: [...new Set(derived.allowedPairs.map((p) => p.sourceUserId))],
      chunks: derived.plan.manifests.reduce((n, m) => n + m.chunks.length, 0),
    };
  } catch (error) {
    if (!(error instanceof MigrationPlanError)) throw error;
    return { ok: false, code: error.code, message: error.message, limit: LIMIT_CODES.has(error.code) || LIMIT_MESSAGE.test(error.message) };
  }
}

const flat = (units: readonly SessionUnit[]) => units.flatMap((u) => u.uploads);

function shardId(orgId: string, exportedAt: string, uploads: readonly UploadInput[]): string {
  // The oracle's normalized entries (sorted by key), without the grant id.
  const { plan } = deriveMigrationGrantPlan({ grantId: `mgr_${"0".repeat(32)}`, orgId, plan: { exported_at: exportedAt, manifests: uploads } });
  const entries = plan.manifests.map(({ grant_id: _g, ...entry }) => entry);
  return sha256Hex(canonicalMigrationJson({ planner: SHARD_PLANNER_VERSION, contract: PLAN_CONTRACT, org_id: orgId, exported_at: exportedAt, manifests: entries })).slice(0, 32);
}

/**
 * Shard an export plan into grants. Unshardable sessions are left out of
 * every shard and listed; any other refusal of a session alone (malformed
 * input) throws `invalid_session`.
 */
export function planShards(input: { exportedAt: string; uploads: readonly UploadInput[] }): ShardPlan {
  const units = sessionUnits(input.uploads);
  const shards: Shard[] = [];
  const unshardable: UnshardableSession[] = [];
  const byOrg = new Map<string, SessionUnit[]>();
  for (const unit of units) byOrg.set(unit.org_id, [...(byOrg.get(unit.org_id) ?? []), unit]);

  for (const [orgId, orgUnits] of [...byOrg].sort(([a], [b]) => cmp(a, b))) {
    // Each session alone first: the oracle decides what is unshardable.
    const fits: SessionUnit[] = [];
    for (const unit of orgUnits) {
      const alone = admit(orgId, input.exportedAt, unit.uploads);
      if (alone.ok) {
        fits.push(unit);
        continue;
      }
      const counts = {
        org_id: orgId,
        session: unit.session,
        manifests: unit.uploads.length,
        chunks: unit.uploads.reduce((n, u) => n + u.chunks.length, 0),
        source_users: new Set(unit.uploads.map((u) => u.source_user_id)).size,
        oracle_code: alone.code,
        oracle_message: alone.message,
      };
      if (!alone.limit || unit.uploads.some((u) => u.chunks.length === 0))
        throw new ShardPlanningError("invalid_session", `session ${unit.session} is not a valid plan entry: ${alone.code}`, [counts]);
      unshardable.push({ code: "oversized_session", ...counts });
    }
    // Greedy: the longest prefix the oracle accepts (gallop, then bisect).
    let start = 0;
    while (start < fits.length) {
      const accepts = (n: number) => admit(orgId, input.exportedAt, flat(fits.slice(start, start + n))).ok;
      let good = 1;
      let bad = 0;
      for (let n = 2; ; n *= 2) {
        if (start + n > fits.length) {
          if (accepts(fits.length - start)) good = fits.length - start;
          else bad = fits.length - start;
          break;
        }
        if (accepts(n)) good = n;
        else {
          bad = n;
          break;
        }
      }
      while (bad && bad - good > 1) {
        const mid = good + Math.floor((bad - good) / 2);
        if (accepts(mid)) good = mid;
        else bad = mid;
      }
      const members = fits.slice(start, start + good);
      const uploads = flat(members);
      const next = start + good < fits.length ? admit(orgId, input.exportedAt, flat(fits.slice(start, start + good + 1))) : undefined;
      const id = shardId(orgId, input.exportedAt, uploads);
      const measured = admit(orgId, input.exportedAt, uploads, `mgr_${id}`);
      if (!measured.ok) throw new Error("shard planner: an admitted shard was refused with its own id");
      shards.push({
        shard_id: id,
        org_id: orgId,
        measure_grant_id: `mgr_${id}`,
        plan: { exported_at: input.exportedAt, manifests: [...uploads].sort((a, b) => cmp(a.key, b.key)) },
        sessions: members.length,
        manifests: uploads.length,
        chunks: measured.chunks,
        source_users: measured.users.length,
        serialized_bytes: measured.bytes,
        closed_by: next && !next.ok ? next.code === "invalid_plan" ? next.message : next.code : "end",
      });
      start += good;
    }
  }
  return { planner_version: SHARD_PLANNER_VERSION, contract: PLAN_CONTRACT, exported_at: input.exportedAt, shards, unshardable };
}

/** Fail closed: refuse a plan with any unshardable session. */
export function requireShardable(plan: ShardPlan): ShardPlan {
  if (plan.unshardable.length)
    throw new ShardPlanningError(
      "oversized_session",
      `${plan.unshardable.length} session(s) cannot fit in one grant under any limit; nothing is minted`,
      plan.unshardable.map(({ code: _c, ...rest }) => rest),
    );
  return plan;
}

export interface ShardReport {
  dry_run: true;
  aggregate: { shards: number; sessions: number; manifests: number; chunks: number; source_users: number; serialized_bytes: number; unshardable_sessions: number };
  max: { source_users: number; serialized_bytes: number; manifests: number; chunks: number };
  shards: Omit<Shard, "plan" | "measure_grant_id">[];
  unshardable: UnshardableSession[];
}

/** The dry-run report: ids, counts and maxima only, no plan entries. */
export function shardReport(plan: ShardPlan): ShardReport {
  const users = new Set(plan.shards.flatMap((s) => s.plan.manifests.map((m) => `${s.org_id}\u0000${canonicalMigrationId(m.source_user_id, "source user id")}`)));
  const max = (f: (s: Shard) => number) => plan.shards.reduce((n, s) => Math.max(n, f(s)), 0);
  return {
    dry_run: true,
    aggregate: {
      shards: plan.shards.length,
      sessions: plan.shards.reduce((n, s) => n + s.sessions, 0),
      manifests: plan.shards.reduce((n, s) => n + s.manifests, 0),
      chunks: plan.shards.reduce((n, s) => n + s.chunks, 0),
      source_users: users.size,
      serialized_bytes: plan.shards.reduce((n, s) => n + s.serialized_bytes, 0),
      unshardable_sessions: plan.unshardable.length,
    },
    max: { source_users: max((s) => s.source_users), serialized_bytes: max((s) => s.serialized_bytes), manifests: max((s) => s.manifests), chunks: max((s) => s.chunks) },
    shards: plan.shards.map(({ plan: _p, measure_grant_id: _m, ...rest }) => rest),
    unshardable: plan.unshardable,
  };
}

/** The part of an export plan (with chunk bytes) that one shard covers. */
export function exportPlanForShard(plan: ExportPlan, shard: Shard): ExportPlan {
  const keys = new Set(shard.plan.manifests.map((m) => m.key));
  const manifests = plan.manifests.filter((m) => keys.has(manifestKey(m.location)));
  if (manifests.length !== keys.size) throw new Error("shard planner: the export plan does not cover this shard");
  if (plan.exportedAt !== shard.plan.exported_at) throw new Error("shard planner: exported_at differs from the shard's");
  return { exportedAt: plan.exportedAt, manifests };
}

// ---------------------------------------------------------------- resume

export interface ShardRecord {
  shard_id: string;
  grant_id: string;
  status: "done";
}

export interface ShardRunContext {
  shard: Shard;
  /** This shard's own courier checkpoint (runExportUpload's checkpointPath). */
  checkpointPath: string;
}

/** What a shard run must report: the courier's result for this shard's grant. */
export interface ShardRunResult {
  grantId: string;
  result: Pick<ExportUploadResult, "committed" | "skipped" | "failed" | "deferred" | "stopped">;
}

/**
 * Whether a shard's upload is complete: nothing failed, deferred or stopped,
 * and every one of the shard's manifests is committed (now or in a prior run).
 * Returns the reason it is not, or undefined.
 */
export function shardIncomplete(shard: Shard, r: ShardRunResult["result"]): string | undefined {
  if (r.stopped) return `stopped: ${r.stopped.code}`;
  if (r.failed.length) return `${r.failed.length} failed`;
  if (r.deferred.length) return `${r.deferred.length} deferred`;
  const done = new Set([...r.committed.map((c) => c.key), ...r.skipped]);
  const missing = shard.plan.manifests.filter((m) => !done.has(m.key)).length;
  return missing ? `${missing} manifest(s) not committed` : undefined;
}

/**
 * Run shards in plan order. A shard is recorded done only when its upload
 * result is complete (`shardIncomplete`); a done shard is skipped on a rerun,
 * an incomplete one runs again (its courier checkpoint skips what committed).
 * A stopped upload stops the run. Refuses an unshardable plan.
 */
export async function runShards(
  plan: ShardPlan,
  opts: { stateDir: string; runShard: (ctx: ShardRunContext) => Promise<ShardRunResult> },
): Promise<{ ran: string[]; skipped: string[]; incomplete: { shard_id: string; reason: string }[]; stopped?: string }> {
  requireShardable(plan);
  const ran: string[] = [];
  const skipped: string[] = [];
  const incomplete: { shard_id: string; reason: string }[] = [];
  await mkdir(opts.stateDir, { recursive: true });
  for (const shard of plan.shards) {
    const donePath = join(opts.stateDir, `${shard.shard_id}.done.json`);
    const prior = await readFile(donePath, "utf8").then((t) => JSON.parse(t) as ShardRecord, (e: { code?: string }) => {
      if (e.code === "ENOENT") return undefined;
      throw e;
    });
    if (prior?.status === "done" && prior.shard_id === shard.shard_id) {
      skipped.push(shard.shard_id);
      continue;
    }
    const out = await opts.runShard({ shard, checkpointPath: join(opts.stateDir, `${shard.shard_id}.checkpoint.json`) });
    const reason = shardIncomplete(shard, out.result);
    if (reason) {
      incomplete.push({ shard_id: shard.shard_id, reason });
      if (out.result.stopped) return { ran, skipped, incomplete, stopped: shard.shard_id };
      continue;
    }
    const record: ShardRecord = { shard_id: shard.shard_id, grant_id: out.grantId, status: "done" };
    await mkdir(dirname(donePath), { recursive: true });
    await writeFile(`${donePath}.tmp`, JSON.stringify(record), { mode: 0o600 });
    await rename(`${donePath}.tmp`, donePath);
    ran.push(shard.shard_id);
  }
  return { ran, skipped, incomplete };
}
