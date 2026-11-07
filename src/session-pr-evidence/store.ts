/** Function-only port for the PR-evidence worker RPCs (migration 20261105044000).
 * No table queries and no owner fallback; every response is shape-checked.
 */
import type { SqlExecutor } from "../shared/vendor.ts";
import type { PullRequestEvidenceRow } from "./extract.ts";

export interface PrEvidenceScope { orgId: string; workspaceId: string; source: string; sessionId: string }
export interface PrEvidenceClaim {
  scope: PrEvidenceScope;
  requestVersion: string;
  leaseToken: string;
  exists: boolean;
  blocked: boolean;
  /** Present for a history-authoritative scope: the authority and mask the build must apply. */
  history: { ready: boolean; authorityId: string; maskSha256: string } | null;
  source: { epoch: string | null; fingerprint: string };
  manifestKeys: string[];
}
export type PrEvidenceInstall =
  | { status: "ready"; rows: PullRequestEvidenceRow[] }
  | { status: "incomplete"; reason: string };
export interface MaskPage { records: { recordId: string; winner: { originId: string; operation: "upsert" | "delete" } | null }[]; next: string | null }

export interface PrEvidenceStore {
  claim(limit: number): Promise<PrEvidenceClaim[]>;
  /** False when the lease was lost or the source moved; the wakeup stays pending. */
  install(claim: PrEvidenceClaim, extractorVersion: string, result: PrEvidenceInstall): Promise<{ ok: boolean; code?: string }>;
  release(claim: PrEvidenceClaim, code: string): Promise<boolean>;
  /** One page of the claim's history mask (session_history_mask_page, 05026000). */
  maskPage(claim: PrEvidenceClaim, generation: string, after: string): Promise<MaskPage>;
}

const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const SHA = /^[0-9a-f]{64}$/;
const BIGINT = /^[1-9][0-9]{0,18}$/;
const CODE = /^[a-z0-9_]{1,64}$/;
const invalid = (): never => { throw new Error("pr_evidence_authority_invalid"); };
const object = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);

function claimOf(v: unknown): PrEvidenceClaim {
  if (!object(v) || !object(v.scope) || !object(v.source)) return invalid();
  const s = v.scope, src = v.source;
  if (typeof s.orgId !== "string" || typeof s.workspaceId !== "string" || typeof s.source !== "string" || typeof s.sessionId !== "string" ||
      typeof v.requestVersion !== "string" || !BIGINT.test(v.requestVersion) || typeof v.leaseToken !== "string" || !UUID.test(v.leaseToken) ||
      typeof v.exists !== "boolean" || typeof v.blocked !== "boolean" ||
      !(v.history === null || (object(v.history) && typeof v.history.ready === "boolean" &&
        typeof v.history.authorityId === "string" && UUID.test(v.history.authorityId) &&
        typeof v.history.maskSha256 === "string" && SHA.test(v.history.maskSha256))) ||
      !(src.epoch === null || (typeof src.epoch === "string" && BIGINT.test(src.epoch))) ||
      typeof src.fingerprint !== "string" || !SHA.test(src.fingerprint) ||
      !Array.isArray(v.manifestKeys) || v.manifestKeys.some((k) => typeof k !== "string" || !k.endsWith("manifest.json")))
    return invalid();
  return {
    scope: { orgId: s.orgId, workspaceId: s.workspaceId, source: s.source, sessionId: s.sessionId },
    requestVersion: v.requestVersion, leaseToken: v.leaseToken, exists: v.exists, blocked: v.blocked,
    history: v.history === null ? null : { ready: (v.history as any).ready, authorityId: (v.history as any).authorityId, maskSha256: (v.history as any).maskSha256 },
    source: { epoch: src.epoch as string | null, fingerprint: src.fingerprint }, manifestKeys: v.manifestKeys as string[],
  };
}

export function prEvidenceStore(sql: SqlExecutor): PrEvidenceStore {
  const rpc = async (name: "claim" | "install" | "release", body: unknown) => {
    const { rows } = await sql.query<{ result: unknown }>(`SELECT sessions.session_pr_evidence_${name}_v1($1::jsonb) AS result`, [JSON.stringify(body)]);
    if (rows.length !== 1) return invalid();
    return rows[0]!.result;
  };
  return {
    async claim(limit) {
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 10) throw new Error("pr_evidence_limit_invalid");
      const r = await rpc("claim", { limit });
      if (!Array.isArray(r) || r.length > limit) return invalid();
      return r.map(claimOf);
    },
    async install(claim, extractorVersion, result) {
      if (claim.source.epoch === null) return invalid();
      if (result.status === "incomplete" && !CODE.test(result.reason)) throw new Error("pr_evidence_reason_invalid");
      const r = await rpc("install", {
        scope: claim.scope, requestVersion: claim.requestVersion, leaseToken: claim.leaseToken,
        source: { epoch: claim.source.epoch, fingerprint: claim.source.fingerprint },
        history: claim.history === null ? null : { authorityId: claim.history.authorityId, maskSha256: claim.history.maskSha256, maskApplied: true },
        extractorVersion,
        status: result.status, reason: result.status === "ready" ? null : result.reason,
        rows: result.status === "ready" ? result.rows : [],
      });
      if (!object(r) || typeof r.ok !== "boolean") return invalid();
      return r.ok ? { ok: true } : { ok: false, code: typeof r.code === "string" ? r.code : "refused" };
    },
    async maskPage(claim, generation, after) {
      if (!claim.history || claim.source.epoch === null || !SHA.test(generation) || !/^(0|[1-9][0-9]{0,18})$/.test(after)) return invalid();
      const { rows } = await sql.query<{ result: unknown }>("SELECT sessions.session_history_mask_page($1::jsonb) AS result", [JSON.stringify({
        generation, identity: claim.scope, source: { epoch: claim.source.epoch, fingerprint: claim.source.fingerprint },
        authorityId: claim.history.authorityId, maskSha256: claim.history.maskSha256, after })]);
      const r = rows[0]?.result;
      if (rows.length !== 1 || !object(r) || r.authorityId !== claim.history.authorityId || !Array.isArray(r.records) ||
          !(r.next === null || (typeof r.next === "string" && /^[1-9][0-9]{0,18}$/.test(r.next)))) return invalid();
      return {
        next: r.next as string | null,
        records: r.records.map((x) => {
          if (!object(x) || typeof x.recordId !== "string") return invalid();
          const w = x.winner;
          if (w === null) return { recordId: x.recordId, winner: null };
          if (!object(w) || typeof w.originId !== "string" || (w.operation !== "upsert" && w.operation !== "delete")) return invalid();
          return { recordId: x.recordId, winner: { originId: w.originId, operation: w.operation as "upsert" | "delete" } };
        }),
      };
    },
    async release(claim, code) {
      if (!CODE.test(code)) throw new Error("pr_evidence_code_invalid");
      const r = await rpc("release", { scope: claim.scope, leaseToken: claim.leaseToken, code });
      if (!object(r) || typeof r.ok !== "boolean") return invalid();
      return r.ok;
    },
  };
}
