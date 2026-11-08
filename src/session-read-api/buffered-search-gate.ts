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
 *   3. the row's ORIGINAL admission (sessions.ingest_claim_attempts by claim
 *      token, owned by the same org/workspace/user) has an epoch and version,
 *      and they equal the head's `controlEpoch` and `sourceControlVersion`.
 *      The epoch is the immutable ingest_lazy_policy.control_epoch, accepted
 *      only when the policy's source_identity_sha256 equals the claim's; the
 *      version is the claim's source_control_version, and every lazy ACK
 *      decision of the claim must carry the same version;
 *   4. the owner has a G2 parity row (sessions.owner_buffered_parity);
 *   5. the hit's (owner, origin, source, session) is not in
 *      sessions.ingest_frozen_source_deletions or sessions.upload_deletions,
 *      and the owner's upload_resets count still equals the claim's
 *      reset_epoch (no reset since admission).
 *
 * Control heads are fetched once per distinct owner per request (in parallel,
 * at most MAX_GATE_OWNERS; hits of further owners are dropped) and never
 * cached across requests. The gate only removes rows: the page may shrink, its
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

/**
 * The gate's contract id: what this module guarantees about every `origin = 'buffered'` row it
 * serves (rules 1-5 above, fail closed; event pages serve canonical rows only where the gate is
 * not wired). The buffered-stage feeder (src/session-pipeline/buffered-search-feed.ts) refuses to
 * write buffered rows unless its deployment declares exactly this value in
 * BUFFERED_SEARCH_FEED_GATE_DECLARED, i.e. an operator asserts the read side runs this gate.
 * Bump it whenever a rule above weakens or changes meaning, so a stale declaration refuses.
 */
export const BUFFERED_SEARCH_GATE_CONTRACT = "buffered-search-gate/v1";

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

/** The original admission of one AdmissionRef, as the buffered ledger records it. */
export interface Admission {
  /** The claim's owning user (must equal the hit's user_id). */
  claimUserId: string;
  /** ingest_lazy_policy.control_epoch when the claim's source identity matches the policy's; else null. */
  admissionEpoch: string | null;
  /** The claim's source_control_version as a decimal string; null when absent. */
  admissionVersion: string | null;
  /** Every lazy ACK decision of the claim carries the claim's version. */
  versionConsistent: boolean;
  /** In upload_deletions or ingest_frozen_source_deletions for (owner, origin, source, session). */
  deleted: boolean;
  /** The owner's upload_resets count differs from the claim's reset_epoch (or the epoch is missing). */
  resetBehind: boolean;
}

export const refKey = (r: AdmissionRef) => `${r.claimToken}\u0000${r.userId}\u0000${r.source}\u0000${r.sessionId}`;

/** The buffered ledger reads the gate needs. Bound to the verified (org, workspace). */
export interface AdmissionStore {
  /** Admissions found, keyed by refKey; a ref without a matching claim of this tenant is absent. */
  admissions(orgId: string, workspaceId: string, refs: AdmissionRef[]): Promise<Map<string, Admission>>;
  /** The subset of `userIds` with a G2 parity row in this workspace. */
  parity(orgId: string, workspaceId: string, userIds: string[]): Promise<Set<string>>;
}

/**
 * One statement over the refs. The claim must belong to this org and
 * workspace AND to the ref's user (the hit's owner): a hit cannot borrow
 * another user's admission. Missing authority (no policy row, identity
 * mismatch, NULL version or reset_epoch) yields a NULL or `true` blocker, so
 * the gate drops the hit.
 */
export const ADMISSIONS_SQL = `WITH refs AS (
  SELECT r.claim_token, r.user_id, r.source, r.session_id
    FROM jsonb_to_recordset($3::jsonb) AS r(claim_token uuid, user_id text, source text, session_id text)
), lp AS (
  SELECT p.control_epoch, p.source_identity_sha256 FROM sessions.ingest_lazy_policy AS p WHERE p.singleton
)
SELECT refs.claim_token::text AS claim_token, refs.user_id, refs.source, refs.session_id,
       a.user_id AS claim_user_id,
       (SELECT lp.control_epoch FROM lp
         WHERE a.source_identity_sha256 IS NOT NULL AND lp.source_identity_sha256 = a.source_identity_sha256) AS admission_epoch,
       a.source_control_version::text AS admission_version,
       NOT EXISTS (SELECT 1 FROM sessions.ingest_lazy_ack_decisions AS d
                    WHERE d.claim_token = a.claim_token
                      AND d.source_control_version IS DISTINCT FROM a.source_control_version) AS version_consistent,
       (EXISTS (SELECT 1 FROM sessions.upload_deletions AS ud
                 WHERE ud.org_id = a.org_id AND ud.workspace_id = a.workspace_id AND ud.user_id = a.user_id
                   AND ud.origin_id = a.origin_id AND ud.source = refs.source AND ud.session_id = refs.session_id)
        OR EXISTS (SELECT 1 FROM sessions.ingest_frozen_source_deletions AS fd
                    WHERE fd.org_id = a.org_id AND fd.workspace_id = a.workspace_id AND fd.user_id = a.user_id
                      AND fd.origin_id = a.origin_id AND fd.source = refs.source AND fd.session_id = refs.session_id)) AS deleted,
       (a.reset_epoch IS NULL OR a.reset_epoch IS DISTINCT FROM
         (SELECT count(*) FROM sessions.upload_resets AS ur
           WHERE ur.org_id = a.org_id AND ur.workspace_id = a.workspace_id AND ur.user_id = a.user_id)) AS reset_behind
  FROM refs
  JOIN sessions.ingest_claim_attempts AS a
    ON a.claim_token = refs.claim_token AND a.org_id = $1 AND a.workspace_id = $2 AND a.user_id = refs.user_id`;

/** The G2 parity rows (sessions.owner_buffered_parity, declared by pipeline PR #328 / slot 043700). */
export const PARITY_SQL = `SELECT DISTINCT p.user_id FROM sessions.owner_buffered_parity AS p
 WHERE p.org_id = $1 AND p.workspace_id = $2 AND p.user_id = ANY($3::text[])`;

interface AdmissionRow {
  claim_token: string;
  user_id: string;
  source: string;
  session_id: string;
  claim_user_id: string;
  admission_epoch: string | null;
  admission_version: string | null;
  version_consistent: boolean;
  deleted: boolean;
  reset_behind: boolean;
}

/**
 * The store over the read role's tenant-bound transactions. NOTE: the read
 * role holds no grant on the buffered ledger today, so until a SECURITY
 * DEFINER reader for these predicates lands (it needs a migration slot), each
 * call fails and the gate drops every buffered hit. That is the intended
 * fail-closed behaviour, not a fallback.
 */
export function sqlAdmissionStore(run: TenantRunner): AdmissionStore {
  return {
    async admissions(orgId, workspaceId, refs) {
      const out = new Map<string, Admission>();
      if (refs.length === 0) return out;
      const payload = JSON.stringify(refs.map((r) => ({ claim_token: r.claimToken, user_id: r.userId, source: r.source, session_id: r.sessionId })));
      const { rows } = await run(orgId, workspaceId, (q: SqlQuery) => q.query<AdmissionRow>(ADMISSIONS_SQL, [orgId, workspaceId, payload]));
      for (const r of rows) {
        out.set(refKey({ claimToken: r.claim_token, userId: r.user_id, source: r.source, sessionId: r.session_id }), {
          claimUserId: r.claim_user_id,
          admissionEpoch: r.admission_epoch,
          admissionVersion: r.admission_version,
          versionConsistent: r.version_consistent === true,
          deleted: r.deleted !== false,
          resetBehind: r.reset_behind !== false,
        });
      }
      return out;
    },
    async parity(orgId, workspaceId, userIds) {
      if (userIds.length === 0) return new Set();
      const { rows } = await run(orgId, workspaceId, (q: SqlQuery) => q.query<{ user_id: string }>(PARITY_SQL, [orgId, workspaceId, userIds]));
      return new Set(rows.map((r) => r.user_id));
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
  | "admission_owner_mismatch"
  | "admission_epoch_mismatch"
  | "admission_version_mismatch"
  | "parity_unavailable"
  | "parity_missing"
  | "deleted"
  | "reset";

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
  store: AdmissionStore;
  /** Per control-head call, and per ledger read. Default CONTROL_HEAD_DEADLINE_MS. */
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
        if (!CLAIM_TOKEN.test(token) || !r.user_id) { drop("claim_token_invalid"); return false; }
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

      // One head per owner, the ledger reads, all concurrently; each failure is a drop.
      const uniqueRefs = [...new Map(checked.map((r) => [refKey(r), r])).values()];
      const [heads, admissions, parity] = await Promise.all([
        Promise.all(owners.map((userId) => deps.controlHeads
          ? settle(deadlineMs, (signal) => deps.controlHeads!({ orgId, workspaceId, userId }, signal))
          : Promise.resolve(undefined))),
        settle(deadlineMs, () => deps.store.admissions(orgId, workspaceId, uniqueRefs)),
        settle(deadlineMs, () => deps.store.parity(orgId, workspaceId, owners)),
      ]);
      const headOf = new Map(owners.map((userId, i) => [userId, heads[i] ?? null]));

      for (let i = 0; i < verdict.length; i++) {
        const ref = verdict[i];
        if (typeof ref !== "object") continue;
        verdict[i] = false;
        const head = headOf.get(ref.userId);
        if (!head) { drop("control_head_unavailable"); continue; }
        if (head.pending) { drop("control_pending"); continue; }
        if (!admissions) { drop("admission_unavailable"); continue; }
        const a = admissions.get(refKey(ref));
        if (!a) { drop("admission_missing"); continue; }
        if (a.claimUserId !== ref.userId) { drop("admission_owner_mismatch"); continue; }
        if (a.admissionEpoch === null || a.admissionEpoch !== head.controlEpoch) { drop("admission_epoch_mismatch"); continue; }
        if (!a.versionConsistent || !isControlVersion(a.admissionVersion) || a.admissionVersion !== head.sourceControlVersion) { drop("admission_version_mismatch"); continue; }
        if (!parity) { drop("parity_unavailable"); continue; }
        if (!parity.has(ref.userId)) { drop("parity_missing"); continue; }
        if (a.deleted) { drop("deleted"); continue; }
        if (a.resetBehind) { drop("reset"); continue; }
        verdict[i] = true;
      }
      return { kept: rows.filter((_, i) => verdict[i] === true), buffered, dropped };
    },
  };
}
