/**
 * The per-hit R2 privacy gate for buffered-origin rows in `transcript_events`
 * (READ_API_BUFFERED_SEARCH_GATE, default off).
 *
 * A separate lane writes buffered (post-cutover, not yet canonically
 * published) transcripts into `transcript_events`, marked `origin = 'buffered'`
 * with the claim token of the buffered stage's original admission. Index rows
 * are not yet removed when the owner deletes or resets, so every buffered hit
 * must be proven visible at query time, against relayhistory-cloud's private
 * control head for the hit's OWNER (who, in a shared workspace, is often not
 * the reader). Canonical rows (`origin = 'canonical'`, the column default) pass
 * the gate untouched. Where the gate is NOT wired (the Lambda, or the flag off)
 * the event queries serve canonical rows only (queries.ts OriginMode, page.ts
 * readEventPage), so a buffered row is never served unchecked.
 *
 * A buffered hit is returned only when ALL of these hold; anything else drops
 * it (fail closed, never an error to the reader):
 *
 *   1. the owner's control head (SessionReadPrivacyControls service binding,
 *      GET /v1/session-read/control-head, owner identity headers, 2 s deadline)
 *      answered 200 with exactly the producer's shape (relayhistory-cloud
 *      private-session-read-controls.ts @ a06a58f9) and an exact tenant echo;
 *   2. the head is not `pending`;
 *   3. sessions.buffered_search_gate_for (one SECURITY DEFINER reader,
 *      supabase/migrations/20261105043860_buffered_search_gate.sql), called
 *      once per owner with that owner's head, returns a NULL drop_reason for
 *      the hit. It judges, for the claim of the same org/workspace/user:
 *      a. ADMISSION: the immutable ingest_lazy_policy.control_epoch (only when
 *         the policy's source_identity_sha256 equals the claim's) equals
 *         `controlEpoch`; the claim's source_control_version equals the head
 *         version (0 without a head) and every lazy ACK decision agrees;
 *      b. BINDING: the claim's immutable original records
 *         (ingest_claim_attempts.records, not retired) hold exactly the hit's
 *         (source, session_id); a claim token cannot vouch for another session;
 *      c. GATE AND G2 PARITY, exactly as 20261105043700's
 *         owner_buffered_read_scope: head epoch = cutover epoch, privacy
 *         authority not fenced, an unexpired policy with this control epoch and
 *         baseline, and an owner_buffered_parity row whose cutover epoch,
 *         baseline, full head tuple (or no_head), target generation,
 *         deletion/reset digests and target lineage equal the passed head and
 *         sessions.owner_buffered_parity_expected recomputed now;
 *      d. DELETIONS: (owner, source, session) is in neither upload_deletions
 *         nor ingest_frozen_source_deletions under ANY origin;
 *      e. BATCH (043700 stg): the claim's ingest_batches row exists, is the
 *         owner's, carries the claim's digest, and is committed, buffered and
 *         accepted (committed_at set); a missing row drops (batch_not_buffered);
 *      f. RESETS (043700 RULE 5(c)): the claim's reset_epoch and the hit's
 *         epochs entry's reset_epoch equal the owner's upload_resets count, and
 *         no reset of the owner is at or after the batch's committed_at;
 *      g. LATEST ADMISSION (043700 judged.live): the claim's epochs hold exactly
 *         one unambiguous entry for (source, session) whose epoch is still the
 *         session's ingest_fence_versions epoch on the claim's origin (0 with no
 *         row); a later fence bump supersedes it (superseded).
 *    Per-stage conditions of 043700 (outbox/stage acknowledgement, the GC
 *    ancestor chain, group shape, the lazy ACK native-slot owner) are not
 *    mirrored: the hit names a claim, not a stage or ordinal (see the
 *    condition table in the migration header).
 *
 * The Worker role (bulls96_read_api) never reads the gate tables directly: it
 * has no grant on them (043700 revokes owner_buffered_parity from it). GATE_SQL
 * is the only SQL here, and 043860 must be applied before
 * READ_API_BUFFERED_SEARCH_GATE is set to on; without it every call fails and
 * every buffered hit is dropped.
 *
 * Control heads are fetched once per distinct owner per request (in parallel,
 * at most MAX_GATE_OWNERS; hits of further owners are dropped) and never
 * cached across requests; each owner's gate call follows its head, so one
 * owner's worst case is two deadlines. The gate only removes rows: the page may shrink, its
 * cursor is the one the unfiltered page computed.
 */
import type { SqlQuery, TenantRunner } from "./liveness.ts";

export const SESSION_READ_CONTROL_PROTOCOL = "relay-session-read-control-head-v1";
export const CONTROL_HEAD_PATH = "/v1/session-read/control-head";
/** The producer's own deadline is 2 s; the caller's is the same. */
export const CONTROL_HEAD_DEADLINE_MS = 2_000;
/** Distinct owners checked per page; hits of further owners are dropped. */
export const MAX_GATE_OWNERS = 16;
/** The head is a few hundred bytes; anything far larger is not the head. */
const MAX_HEAD_BYTES = 8 * 1024;

export const ORIGIN_CANONICAL = "canonical";
export const ORIGIN_BUFFERED = "buffered";

/** READ_API_BUFFERED_SEARCH_GATE: only exactly "on" (trimmed, any case) enables the gate. */
export function parseBufferedSearchGate(raw: string | undefined): boolean {
  return (raw ?? "").trim().toLowerCase() === "on";
}

export interface Owner {
  orgId: string;
  workspaceId: string;
  userId: string;
}

export interface SessionReadControlHead {
  protocol: typeof SESSION_READ_CONTROL_PROTOCOL;
  version: 1;
  issuer: "relayhistory-cloud";
  tenant: { orgId: string; workspaceId: string; userId: string };
  controlEpoch: string;
  cutover: { epoch: string; status: "active"; mode: "ingest" };
  privacyBaselineSha256: string;
  sourceControlEpoch: string | null;
  sourceControlVersion: string;
  sourceControlDigest: string | null;
  sourceControlKind: "selection" | "reset" | null;
  pending: boolean;
}

const HEAD_KEYS = "controlEpoch,cutover,issuer,pending,privacyBaselineSha256,protocol,sourceControlDigest,sourceControlEpoch,sourceControlKind,sourceControlVersion,tenant,version";
const TENANT_KEYS = "orgId,userId,workspaceId";
const CUTOVER_KEYS = "epoch,mode,status";
const HASH = /^[0-9a-f]{64}$/u;
const DECIMAL = /^(0|[1-9][0-9]{0,18})$/u;
const CLAIM_TOKEN = /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/u;
const BIGINT_MAX = 9223372036854775807n;

const isObject = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
const keysOf = (v: Record<string, unknown>) => Object.keys(v).sort().join(",");
const isHash = (v: unknown): v is string => typeof v === "string" && HASH.test(v);

/** A strict sourceControlVersion: canonical decimal, within Postgres bigint. */
export function isControlVersion(v: unknown): v is string {
  return typeof v === "string" && DECIMAL.test(v) && BigInt(v) <= BIGINT_MAX;
}

/**
 * The head, or null unless the body is exactly the producer's 200 shape with
 * an exact tenant echo of `owner`. Mirrors the producer's own validation
 * (version-0 nulls; otherwise a digest, a kind and sourceControlEpoch ===
 * controlEpoch) so a head the producer would never send is never trusted.
 */
export function parseControlHead(body: unknown, owner: Owner): SessionReadControlHead | null {
  if (!isObject(body) || keysOf(body) !== HEAD_KEYS) return null;
  const { tenant, cutover } = body;
  if (body.protocol !== SESSION_READ_CONTROL_PROTOCOL || body.version !== 1 || body.issuer !== "relayhistory-cloud") return null;
  if (!isObject(tenant) || keysOf(tenant) !== TENANT_KEYS) return null;
  if (tenant.orgId !== owner.orgId || tenant.workspaceId !== owner.workspaceId || tenant.userId !== owner.userId) return null;
  if (!isHash(body.controlEpoch) || !isHash(body.privacyBaselineSha256)) return null;
  if (!isObject(cutover) || keysOf(cutover) !== CUTOVER_KEYS) return null;
  if (cutover.status !== "active" || cutover.mode !== "ingest" || cutover.epoch !== body.controlEpoch) return null;
  if (typeof body.pending !== "boolean") return null;
  if (!isControlVersion(body.sourceControlVersion)) return null;
  if (body.sourceControlVersion === "0") {
    if (body.sourceControlEpoch !== null || body.sourceControlDigest !== null || body.sourceControlKind !== null) return null;
  } else if (!isHash(body.sourceControlDigest) || body.sourceControlEpoch !== body.controlEpoch || (body.sourceControlKind !== "selection" && body.sourceControlKind !== "reset")) {
    return null;
  }
  return body as unknown as SessionReadControlHead;
}

/** Fetches one owner's control head; resolves null on any failure (never throws). */
export type ControlHeads = (owner: Owner, signal: AbortSignal) => Promise<SessionReadControlHead | null>;

/** The Workers service binding to relayhistory-cloud's named entrypoint SessionReadPrivacyControls. */
export interface ServiceBinding {
  fetch(request: Request): Promise<Response>;
}

/**
 * The control head over the SessionReadPrivacyControls binding. Service-binding
 * possession is the producer's authentication boundary; the identity headers
 * carry the hit's owner, never the reader. The URL host is not routed (a
 * binding fetch goes to the bound entrypoint); only the path and an empty query
 * reach the producer, which refuses anything else.
 */
export function serviceBindingControlHeads(binding: ServiceBinding): ControlHeads {
  return async (owner, signal) => {
    try {
      const response = await binding.fetch(new Request(`https://session-read-privacy-controls${CONTROL_HEAD_PATH}`, {
        method: "GET",
        headers: {
          "x-relayhistory-org-id": owner.orgId,
          "x-relayhistory-workspace-id": owner.workspaceId,
          "x-relayhistory-user-id": owner.userId,
          accept: "application/json",
        },
        signal,
      }));
      if (response.status !== 200) {
        await response.body?.cancel().catch(() => {});
        return null;
      }
      const text = await response.text();
      if (text.length > MAX_HEAD_BYTES) return null;
      return parseControlHead(JSON.parse(text) as unknown, owner);
    } catch {
      return null;
    }
  };
}

/** One buffered hit's admission reference: the claim it was admitted under, and its session. */
export interface AdmissionRef {
  claimToken: string;
  userId: string;
  source: string;
  sessionId: string;
}

export const refKey = (r: AdmissionRef) => `${r.claimToken}\u0000${r.userId}\u0000${r.source}\u0000${r.sessionId}`;

/** The reasons sessions.buffered_search_gate_for may return (NULL = visible). */
export const GATE_SQL_REASONS = [
  "admission_epoch_mismatch",
  "admission_version_mismatch",
  "admission_session_mismatch",
  "gate_closed",
  "parity_missing",
  "parity_stale",
  "deleted",
  "batch_not_buffered",
  "reset",
  "superseded",
] as const;
export type GateSqlReason = (typeof GATE_SQL_REASONS)[number];

/**
 * The gate's ledger verdicts for ONE owner's hits, judged against that owner's
 * (non-pending) control head. Keyed by refKey; a ref with no verdict is absent
 * (no claim of this tenant and owner, or the reader refused the input). Value
 * null = visible. Bound to the verified (org, workspace).
 */
export interface GateStore {
  verdicts(orgId: string, workspaceId: string, ownerUserId: string, head: SessionReadControlHead, refs: AdmissionRef[]): Promise<Map<string, GateSqlReason | "admission_unknown_reason" | null>>;
}

/** Hits per reader call; sessions.buffered_search_gate_for returns no rows above 500. */
export const MAX_GATE_HITS = 500;

/**
 * The ONLY SQL the gate runs. bulls96_read_api holds no grant on the buffered
 * ledger tables; it holds EXECUTE on this one SECURITY DEFINER reader
 * (20261105043860_buffered_search_gate.sql), which refuses any other caller and
 * any tenant but the transaction's bound one.
 */
export const GATE_SQL = `SELECT claim_token, source, session_id, drop_reason
  FROM sessions.buffered_search_gate_for($1, $2, $3, $4, $5, $6, $7, $8, $9::boolean, $10, $11::jsonb)`;

/** The reader's head arguments, in 20261105043700's encoding (no head = four NULLs). */
export function gateHeadParams(head: SessionReadControlHead): [string, string | null, string | null, string | null, string | null, boolean, string] {
  const noHead = head.sourceControlVersion === "0";
  return [
    head.controlEpoch,
    noHead ? null : head.sourceControlEpoch,
    noHead ? null : head.sourceControlVersion,
    noHead ? null : head.sourceControlDigest,
    noHead ? null : head.sourceControlKind,
    head.pending,
    head.privacyBaselineSha256,
  ];
}

interface GateSqlRow {
  claim_token: string;
  source: string;
  session_id: string;
  drop_reason: string | null;
}

const isGateSqlReason = (v: string): v is GateSqlReason => (GATE_SQL_REASONS as readonly string[]).includes(v);

/**
 * The store over the read role's tenant-bound READ ONLY transactions: one
 * transaction per owner. Any error (043860 not applied, a role or pin
 * refusal, a timeout) rejects, and the gate drops that owner's hits.
 *
 * Snapshot: each reader call is one STABLE statement, so it judges its hits
 * against one snapshot. With READ_API_REPEATABLE_READ=1 the Worker's runner
 * (requestTenantRunner) opens REPEATABLE READ READ ONLY, so the chunks of one
 * owner also share a single snapshot. With it off (the default, READ
 * COMMITTED, as in the Lambda's poolTenantRunner) each chunk takes its own
 * snapshot: every chunk still fails closed against the state it saw, so no
 * hit passes a check it failed, but a commit between two chunks can make
 * hits of one owner on one page be judged against different states.
 */
export function sqlGateStore(run: TenantRunner): GateStore {
  return {
    async verdicts(orgId, workspaceId, ownerUserId, head, refs) {
      const out = new Map<string, GateSqlReason | "admission_unknown_reason" | null>();
      if (refs.length === 0) return out;
      // One READ ONLY transaction per owner; one call per MAX_GATE_HITS hits (an event page is
      // at most 1000 rows, so at most two calls), all in that transaction and, with
      // READ_API_REPEATABLE_READ=1, all on that transaction's one snapshot.
      const chunks: AdmissionRef[][] = [];
      for (let i = 0; i < refs.length; i += MAX_GATE_HITS) chunks.push(refs.slice(i, i + MAX_GATE_HITS));
      const rows = await run(orgId, workspaceId, async (q: SqlQuery) => {
        const all: GateSqlRow[] = [];
        for (const chunk of chunks) {
          const hits = JSON.stringify(chunk.map((r) => ({ claim_token: r.claimToken, source: r.source, session_id: r.sessionId })));
          all.push(...(await q.query<GateSqlRow>(GATE_SQL, [orgId, workspaceId, ownerUserId, ...gateHeadParams(head), hits])).rows);
        }
        return all;
      });
      for (const r of rows) {
        const reason = r.drop_reason === null ? null : isGateSqlReason(r.drop_reason) ? r.drop_reason : "admission_unknown_reason";
        out.set(refKey({ claimToken: r.claim_token, userId: ownerUserId, source: r.source, sessionId: r.session_id }), reason);
      }
      return out;
    },
  };
}

/** The row fields the gate reads. `origin`/`origin_claim_token` are selected only while the gate is on. */
export interface GateRow {
  org_id: string;
  workspace_id: string;
  session_id: string;
  source: string;
  user_id: string;
  origin?: string;
  origin_claim_token?: string;
}

export type DropReason =
  | "origin_unknown"
  | "claim_token_invalid"
  | "owner_fanout"
  | "control_head_unavailable"
  | "control_pending"
  | "admission_unavailable"
  | "admission_missing"
  | "admission_unknown_reason"
  | GateSqlReason;

export interface GateResult<R> {
  kept: R[];
  /** Buffered hits checked (kept or dropped); canonical rows are not counted. */
  buffered: number;
  dropped: Partial<Record<DropReason, number>>;
}

export interface BufferedSearchGate {
  /** `rows` already passed the tenant recheck for (orgId, workspaceId). Never throws. */
  filter<R extends GateRow>(orgId: string, workspaceId: string, rows: R[]): Promise<GateResult<R>>;
}

export interface GateDeps {
  /** Absent (binding not configured) = no head is ever available, so every buffered hit drops. */
  controlHeads?: ControlHeads;
  store: GateStore;
  /** Per control-head call, and per owner's gate call. Default CONTROL_HEAD_DEADLINE_MS. */
  deadlineMs?: number;
  maxOwners?: number;
}

const SETTLED_TIMEOUT = Symbol("timeout");

/** `work` within `ms`, aborting its signal at the deadline; rejects on timeout. */
async function withDeadline<T>(ms: number, work: (signal: AbortSignal) => Promise<T>): Promise<T> {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const result = await Promise.race([
      work(controller.signal),
      new Promise<typeof SETTLED_TIMEOUT>((resolve) => { timer = setTimeout(() => { controller.abort(); resolve(SETTLED_TIMEOUT); }, ms); }),
    ]);
    if (result === SETTLED_TIMEOUT) throw new Error("deadline");
    return result;
  } finally {
    clearTimeout(timer);
  }
}

/** Resolves to the value, or `undefined` on any error or timeout. */
async function settle<T>(ms: number, work: (signal: AbortSignal) => Promise<T>): Promise<T | undefined> {
  try {
    return await withDeadline(ms, work);
  } catch {
    return undefined;
  }
}

export function createBufferedSearchGate(deps: GateDeps): BufferedSearchGate {
  const deadlineMs = deps.deadlineMs ?? CONTROL_HEAD_DEADLINE_MS;
  const maxOwners = deps.maxOwners ?? MAX_GATE_OWNERS;
  return {
    async filter<R extends GateRow>(orgId: string, workspaceId: string, rows: R[]): Promise<GateResult<R>> {
      const dropped: Partial<Record<DropReason, number>> = {};
      const drop = (reason: DropReason) => { dropped[reason] = (dropped[reason] ?? 0) + 1; };
      // Classify. Canonical rows pass untouched; a buffered row needs a well-formed claim
      // token and owner; any other marker (absent, unknown) is not provably canonical.
      const verdict: (boolean | AdmissionRef)[] = rows.map((r) => {
        if (r.origin === ORIGIN_CANONICAL) return true;
        if (r.origin !== ORIGIN_BUFFERED) { drop("origin_unknown"); return false; }
        const token = r.origin_claim_token ?? "";
        // An empty owner, source or session cannot be bound to a claim (and the gate SQL refuses
        // the whole call for it, which would drop this owner's other hits too).
        if (!CLAIM_TOKEN.test(token) || !r.user_id || !r.source || !r.session_id) { drop("claim_token_invalid"); return false; }
        return { claimToken: token, userId: r.user_id, source: r.source, sessionId: r.session_id };
      });
      const refs = verdict.filter((v): v is AdmissionRef => typeof v === "object");
      const buffered = rows.filter((r) => r.origin !== ORIGIN_CANONICAL).length;
      if (refs.length === 0) return { kept: rows.filter((_, i) => verdict[i] === true), buffered, dropped };

      // Bounded owner fan-out, in page order.
      const owners: string[] = [];
      for (const ref of refs) if (!owners.includes(ref.userId) && owners.length < maxOwners) owners.push(ref.userId);
      const checked = refs.filter((ref) => owners.includes(ref.userId));
      for (let i = 0; i < verdict.length; i++) {
        const v = verdict[i];
        if (typeof v === "object" && !owners.includes(v.userId)) { drop("owner_fanout"); verdict[i] = false; }
      }

      // Per owner, concurrently: the head, then ONE gate call judged on that head.
      // Every failure drops that owner's hits.
      const byOwner = new Map(owners.map((userId) => [userId, [...new Map(checked.filter((r) => r.userId === userId).map((r) => [refKey(r), r])).values()]]));
      type OwnerOutcome = { drop: DropReason } | { verdicts: Map<string, GateSqlReason | "admission_unknown_reason" | null> };
      const outcomes = await Promise.all(owners.map(async (userId): Promise<OwnerOutcome> => {
        const head = deps.controlHeads
          ? await settle(deadlineMs, (signal) => deps.controlHeads!({ orgId, workspaceId, userId }, signal))
          : undefined;
        if (!head) return { drop: "control_head_unavailable" };
        if (head.pending) return { drop: "control_pending" };
        const verdicts = await settle(deadlineMs, () => deps.store.verdicts(orgId, workspaceId, userId, head, byOwner.get(userId)!));
        return verdicts ? { verdicts } : { drop: "admission_unavailable" };
      }));
      const outcomeOf = new Map(owners.map((userId, i) => [userId, outcomes[i]!]));

      for (let i = 0; i < verdict.length; i++) {
        const ref = verdict[i];
        if (typeof ref !== "object") continue;
        verdict[i] = false;
        const outcome = outcomeOf.get(ref.userId)!;
        if ("drop" in outcome) { drop(outcome.drop); continue; }
        const key = refKey(ref);
        if (!outcome.verdicts.has(key)) { drop("admission_missing"); continue; }
        const reason = outcome.verdicts.get(key);
        if (reason !== null) { drop(reason ?? "admission_missing"); continue; }
        verdict[i] = true;
      }
      return { kept: rows.filter((_, i) => verdict[i] === true), buffered, dropped };
    },
  };
}
