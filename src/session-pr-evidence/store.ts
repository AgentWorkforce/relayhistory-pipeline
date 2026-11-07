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
  source: { epoch: string | null; fingerprint: string };
  manifestKeys: string[];
}
export type PrEvidenceInstall =
  | { status: "ready"; rows: PullRequestEvidenceRow[] }
  | { status: "incomplete"; reason: string }
  | { status: "absent"; reason: string };

export interface PrEvidenceStore {
  claim(limit: number): Promise<PrEvidenceClaim[]>;
  /** False when the lease was lost or the source moved; the wakeup stays pending. */
  install(claim: PrEvidenceClaim, extractorVersion: string, result: PrEvidenceInstall): Promise<{ ok: boolean; code?: string }>;
  release(claim: PrEvidenceClaim, code: string): Promise<boolean>;
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
      !(src.epoch === null || (typeof src.epoch === "string" && BIGINT.test(src.epoch))) ||
      typeof src.fingerprint !== "string" || !SHA.test(src.fingerprint) ||
      !Array.isArray(v.manifestKeys) || v.manifestKeys.some((k) => typeof k !== "string" || !k.endsWith("manifest.json")))
    return invalid();
  return {
    scope: { orgId: s.orgId, workspaceId: s.workspaceId, source: s.source, sessionId: s.sessionId },
    requestVersion: v.requestVersion, leaseToken: v.leaseToken, exists: v.exists, blocked: v.blocked,
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
      if (result.status !== "ready" && !CODE.test(result.reason)) throw new Error("pr_evidence_reason_invalid");
      const r = await rpc("install", {
        scope: claim.scope, requestVersion: claim.requestVersion, leaseToken: claim.leaseToken,
        source: { epoch: claim.source.epoch, fingerprint: claim.source.fingerprint }, extractorVersion,
        status: result.status, reason: result.status === "ready" ? null : result.reason,
        rows: result.status === "ready" ? result.rows : [],
      });
      if (!object(r) || typeof r.ok !== "boolean") return invalid();
      return r.ok ? { ok: true } : { ok: false, code: typeof r.code === "string" ? r.code : "refused" };
    },
    async release(claim, code) {
      if (!CODE.test(code)) throw new Error("pr_evidence_code_invalid");
      const r = await rpc("release", { scope: claim.scope, leaseToken: claim.leaseToken, code });
      if (!object(r) || typeof r.ok !== "boolean") return invalid();
      return r.ok;
    },
  };
}
