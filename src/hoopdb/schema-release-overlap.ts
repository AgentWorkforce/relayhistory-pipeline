/**
 * The guarded schema release's buffered-ingest overlap gate (docs/design/zero-503-schema-windows.md
 * section 3.2; runbooks/guarded-session-schema-release.md).
 *
 * A release that moves readiness from generation G to its targetGeneration H while buffered ingest
 * (sessions.ingest_lazy_policy, the admission rows and the ingest Worker) is still bound to G fails every
 * buffered claim with 55000 until a rebind, unless a generation overlap window G -> H is open. Two
 * production releases did exactly that. So the plan and the apply refuse release_ingest_overlap_required
 * when ALL of:
 *   - buffered ingest is installed (a sessions.ingest_lazy_policy row exists; none: the gate is a no-op);
 *   - the apply moves readiness (the live marker is not PASS at targetGeneration);
 *   - the lazy policy is not already at targetGeneration (G := the policy generation);
 * and NOT all of:
 *   - the overlap table (20261105044030) is installed and exactly one window is open, from G to exactly H,
 *     unexpired, with at least RELEASE_OVERLAP_MIN_REMAINING_SECONDS left by the database clock;
 *   - the release sha's reviewed PRIVATE_INGEST_OVERLAP (src/session-ingest/private-ingest-release.ts, read
 *     from the Git object database and parsed as data, never imported or executed) is exactly {from: G,
 *     to: H}: the source-side proxy for "the live ingest Worker accepts H" (the private-ingest deploy reads
 *     the same constant). It is not a live Worker readback.
 *
 * G is the policy generation, not the marker's: the policy (and the admission rows bound with it) is what
 * a buffered claim is checked against, and a retry after a failed apply sees the marker pending at H while
 * the window G -> H is still the one that protects ingest. The window's open (ingest-generation-overlap-
 * operator.ts) already required readiness PASS at G and the policy at G.
 *
 * A failed apply that leaves the marker pending at H after the window expired or closed is NOT recoverable
 * through this route: the gate keeps refusing (reason retry_marker_pending_no_window) and the 044030 SQL
 * open refuses a window while readiness is not PASS at G. The runbook's break-glass route covers it.
 *
 * The window cannot be closed while the apply runs: the owner close and the rebind take the migrator's
 * advisory lock (961996, 1), which the migrator holds for the whole apply. It can only expire, hence the
 * minimum remaining TTL, rechecked under that lock immediately before the first write.
 */
import { execFileSync } from "node:child_process";
import type { MigrationClient } from "./migrate.ts";
import { ROLE_VALIDATION_NAME, ROLE_VALIDATION_TABLE } from "../shared/db.ts";
import { OVERLAP_TABLE, generationOverlapStatus, overlapInstalled } from "./ingest-generation-overlap.ts";
import { refuse } from "./schema-release-contract.ts";

export const RELEASE_INGEST_OVERLAP_REQUIRED = "release_ingest_overlap_required";
/**
 * The least window lifetime the plan and the apply accept, by the database clock. 15 minutes: the apply's
 * own statements are bounded by statement_timeout=300s each and normally finish in a few minutes, and the
 * window must still be open when the post-apply rebind (a dry run plus a rebind dispatch) runs, or G uploads
 * fail closed again after PASS at H. It is a floor, not a recommendation: open the window with a TTL that
 * covers plan, review, apply and rebind (up to OVERLAP_MAX_TTL_SECONDS, 4 h).
 */
export const RELEASE_OVERLAP_MIN_REMAINING_SECONDS = 900;
export const PRIVATE_INGEST_RELEASE_MODULE = "src/session-ingest/private-ingest-release.ts";

const HEX64 = /^[0-9a-f]{64}$/;

/** The release sha's reviewed overlap pair: null (dormant), or undefined when it cannot be read as data. */
export type ReleaseOverlapContract = { from: string; to: string } | null | undefined;

/**
 * PRIVATE_INGEST_OVERLAP from the module's text, as data: exactly one declaration, either `= null;` or an
 * object literal of exactly two 64-hex string fields `from` and `to` (in that order, optional trailing
 * comma). Anything else (a computed value, a reference, a duplicate) is undefined, which the gate treats as
 * a mismatch when it is engaged.
 */
export function parseReleaseOverlapContract(text: string): ReleaseOverlapContract {
  const decl = /^export const PRIVATE_INGEST_OVERLAP\b/gm;
  if ([...text.matchAll(decl)].length !== 1) return undefined;
  const m =
    /^export const PRIVATE_INGEST_OVERLAP: PrivateIngestOverlap \| null = (?:(null)|\{\s*from: "([0-9a-f]{64})",\s*to: "([0-9a-f]{64})",?\s*\});$/m.exec(text);
  if (!m) return undefined;
  if (m[1] === "null") return null;
  if (m[2] === m[3]) return undefined;
  return { from: m[2]!, to: m[3]! };
}

/** The pair at `sha` from the Git object database (git cat-file argv, no shell, env limited to PATH). */
export function releaseOverlapContract(repo: string, sha: string): ReleaseOverlapContract {
  if (!/^[0-9a-f]{40}$/.test(sha)) return undefined;
  try {
    const text = execFileSync("git", ["cat-file", "blob", `${sha}:${PRIVATE_INGEST_RELEASE_MODULE}`], {
      cwd: repo,
      env: { PATH: process.env.PATH },
      stdio: ["ignore", "pipe", "ignore"],
      maxBuffer: 4 * 1024 * 1024,
      encoding: "utf8",
    });
    return parseReleaseOverlapContract(text);
  } catch {
    return undefined;
  }
}

export interface IngestOverlapState {
  /** null: no lazy policy (table absent or no row): buffered ingest is not installed. */
  policyGeneration: string | null;
  marker: { generation: string; status: string } | null;
  overlapInstalled: boolean;
  window: {
    from: string;
    to: string;
    admitDuringApply: boolean;
    expiresAt: string;
    expired: boolean;
    remainingSeconds: number;
  } | null;
}

const iso = (v: unknown) => (v instanceof Date ? v.toISOString() : new Date(String(v)).toISOString());

/** Plain reads (no transaction control, no lock): the caller holds the migrator lock. */
export async function readIngestOverlapState(client: MigrationClient): Promise<IngestOverlapState> {
  const hasPolicy =
    (await client.query<{ t: string | null }>("SELECT to_regclass('sessions.ingest_lazy_policy')::text AS t")).rows[0]?.t != null;
  const policy = hasPolicy
    ? ((await client.query<{ generation: string }>("SELECT generation FROM sessions.ingest_lazy_policy WHERE singleton")).rows[0] ?? null)
    : null;
  const hasMarker =
    (await client.query<{ t: string | null }>(`SELECT to_regclass('${ROLE_VALIDATION_TABLE}')::text AS t`)).rows[0]?.t != null;
  const marker = hasMarker
    ? ((await client.query<{ generation: string; status: string }>(
        `SELECT generation, status FROM ${ROLE_VALIDATION_TABLE} WHERE name = $1`,
        [ROLE_VALIDATION_NAME],
      )).rows[0] ?? null)
    : null;
  const installed = await overlapInstalled(client);
  let window: IngestOverlapState["window"] = null;
  if (installed) {
    const w = await generationOverlapStatus(client);
    if (w) {
      const left = (await client.query<{ s: string | number | null }>(
        `SELECT floor(extract(epoch FROM expires_at - clock_timestamp()))::bigint AS s FROM ${OVERLAP_TABLE} WHERE window_id::text = $1`,
        [w.windowId],
      )).rows[0]?.s;
      window = {
        from: w.from,
        to: w.to,
        admitDuringApply: w.admitDuringApply === true,
        expiresAt: iso(w.expiresAt),
        expired: w.expired === true,
        remainingSeconds: left == null ? 0 : Number(left),
      };
    }
  }
  return {
    policyGeneration: policy?.generation ?? null,
    marker: marker ? { generation: marker.generation, status: marker.status } : null,
    overlapInstalled: installed,
    window,
  };
}

export type IngestOverlapGate = "no_lazy_policy" | "no_generation_move" | "policy_at_target" | "overlap_ready";
export type IngestOverlapReason =
  | "overlap_not_installed"
  | "no_window"
  | "window_from_mismatch"
  | "window_to_mismatch"
  | "window_expired"
  | "window_ttl_short"
  | "contract_unreadable"
  | "contract_dormant"
  | "contract_mismatch"
  | "retry_marker_pending_no_window";

/** The plan artifact's ingestOverlap section: generations, window shape and verdict only (no ids, no secrets). */
export interface IngestOverlapEvidence {
  gate: IngestOverlapGate | "refused";
  reasons: IngestOverlapReason[];
  targetGeneration: string;
  policyGeneration: string | null;
  marker: { generation: string; status: string } | null;
  minRemainingSeconds: number;
  window: (NonNullable<IngestOverlapState["window"]> & { present: true }) | { present: false };
  contract: { state: "pair" | "dormant" | "unreadable"; from: string | null; to: string | null; match: boolean };
}

/** The pure verdict over the live state and the release sha's contract. Never throws. */
export function ingestOverlapVerdict(
  state: IngestOverlapState,
  input: { targetGeneration: string; contract: ReleaseOverlapContract },
): IngestOverlapEvidence {
  const H = input.targetGeneration;
  const G = state.policyGeneration;
  const c = input.contract;
  const contract: IngestOverlapEvidence["contract"] = {
    state: c === undefined ? "unreadable" : c === null ? "dormant" : "pair",
    from: c?.from ?? null,
    to: c?.to ?? null,
    match: !!c && G !== null && c.from === G && c.to === H,
  };
  const evidence: IngestOverlapEvidence = {
    gate: "refused",
    reasons: [],
    targetGeneration: H,
    policyGeneration: G,
    marker: state.marker,
    minRemainingSeconds: RELEASE_OVERLAP_MIN_REMAINING_SECONDS,
    window: state.window ? { present: true, ...state.window } : { present: false },
    contract,
  };
  if (G === null) return { ...evidence, gate: "no_lazy_policy" };
  if (state.marker?.status === "pass" && state.marker.generation === H) return { ...evidence, gate: "no_generation_move" };
  if (G === H) return { ...evidence, gate: "policy_at_target" };
  const reasons: IngestOverlapReason[] = [];
  const w = state.window;
  if (!state.overlapInstalled) reasons.push("overlap_not_installed");
  else if (!w) reasons.push("no_window");
  else {
    if (w.from !== G) reasons.push("window_from_mismatch");
    if (w.to !== H) reasons.push("window_to_mismatch");
    if (w.expired) reasons.push("window_expired");
    else if (!(w.remainingSeconds >= RELEASE_OVERLAP_MIN_REMAINING_SECONDS)) reasons.push("window_ttl_short");
  }
  if (c === undefined) reasons.push("contract_unreadable");
  else if (c === null) reasons.push("contract_dormant");
  else if (!contract.match) reasons.push("contract_mismatch");
  // A failed apply of this target left readiness pending at H with the policy still at G and no usable
  // window. Buffered ingest is already refused (no generation is PASS), and the window can no longer be
  // opened (the 044030 SQL open requires PASS at `from`), so the stock CI route cannot recover it: named
  // so the operator takes the break-glass route in runbooks/guarded-session-schema-release.md.
  if (
    state.marker?.status === "pending" &&
    state.marker.generation === H &&
    reasons.some((r) => r === "overlap_not_installed" || r === "no_window" || r.startsWith("window_"))
  )
    reasons.push("retry_marker_pending_no_window");
  return { ...evidence, gate: reasons.length ? "refused" : "overlap_ready", reasons };
}

/**
 * Refuses release_ingest_overlap_required unless the verdict passes; the reasons (fixed tokens only) go to
 * `log` first, as release_bootstrap_required does with role names.
 */
export function requireIngestOverlap(evidence: IngestOverlapEvidence, log: (line: string) => void = console.error) {
  if (evidence.gate === "refused") {
    log(RELEASE_INGEST_OVERLAP_REQUIRED + " " + evidence.reasons.join(","));
    refuse(RELEASE_INGEST_OVERLAP_REQUIRED);
  }
  return evidence;
}

/**
 * Read the live state, judge it and refuse if needed. `client` must already hold the migrator lock (the
 * plan's READ ONLY transaction under pg_advisory_xact_lock, or runMigrations' session lock in beforeWrite).
 */
export async function checkIngestOverlap(
  client: MigrationClient,
  input: { targetGeneration: string; contract: ReleaseOverlapContract },
  log?: (line: string) => void,
) {
  if (!HEX64.test(input.targetGeneration)) refuse(RELEASE_INGEST_OVERLAP_REQUIRED);
  return requireIngestOverlap(ingestOverlapVerdict(await readIngestOverlapState(client), input), log);
}
