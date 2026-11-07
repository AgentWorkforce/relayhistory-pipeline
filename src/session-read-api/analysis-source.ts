/**
 * The read API's session-analysis source (lane F1):
 *   read       sessions.analysis_read_v1 as the read role (bulls96_read_api) in one READ ONLY
 *              tenant-bound transaction;
 *   harnesses  the session's live harnesses, same role (relayhistory selectedSource);
 *   result     the result objects, read and verified from the canonical store;
 *   admit      sessions.analysis_request_v1 as the SEPARATE admission principal
 *              (bulls96_analysis_admit, its own pool), in a READ WRITE transaction that binds
 *              the verified assertion's tenant, which the function's admit scope re-checks.
 */
import type { ObjectStore } from "../../packages/object-store/src/index.ts";
import type { AnalysisDeps } from "./app.ts";
import { TENANT_SETTINGS_SQL, type TenantRunner } from "./liveness.ts";
import { sqlSessionHarnesses } from "./session-turns-source.ts";
import { readAnalysisResult } from "../session-analysis/results.ts";
import type { AdmitRpc, ReadRpc } from "../session-analysis/read.ts";

export const ANALYSIS_READ_SQL = "SELECT sessions.analysis_read_v1($1, $2) AS r";
export const ANALYSIS_ADMIT_SQL = "SELECT sessions.analysis_request_v1($1, $2, $3, $4) AS r";

interface AdmitClient {
  query<R = Record<string, unknown>>(text: string, params?: unknown[]): Promise<{ rows: R[] }>;
  release(): void;
}
export interface AdmitPool {
  connect(): Promise<AdmitClient>;
}

/** One admission as the admission principal: BEGIN, bind the tenant, call, COMMIT. */
export function sqlAnalysisAdmit(pool: AdmitPool): NonNullable<AnalysisDeps["admit"]> {
  return async (identity, sessionId) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(TENANT_SETTINGS_SQL, [identity.orgId, identity.workspaceId]);
      const { rows } = await client.query<{ r: AdmitRpc }>(ANALYSIS_ADMIT_SQL, [identity.orgId, identity.workspaceId, identity.userId, sessionId]);
      await client.query("COMMIT");
      const value = rows[0]?.r;
      if (!value || !["accepted", "not_found", "rate_limited", "invalid_request"].includes(value.outcome))
        throw new Error("analysis admit contract");
      return value;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  };
}

export function sqlAnalysisSource(run: TenantRunner, store: ObjectStore, admit?: AdmitPool): AnalysisDeps {
  const harnesses = sqlSessionHarnesses(run);
  return {
    async read(identity, sessionId) {
      const { rows } = await run(identity.orgId, identity.workspaceId, (q) =>
        q.query<{ r: ReadRpc }>(ANALYSIS_READ_SQL, [identity.workspaceId, sessionId]),
      );
      const value = rows[0]?.r;
      if (!value || (value.outcome !== "ok" && value.outcome !== "not_found")) throw new Error("analysis read contract");
      return value;
    },
    async harnesses(identity, sessionId) {
      const states = await harnesses({ orgId: identity.orgId, workspaceId: identity.workspaceId, sessionId });
      return states.filter((h) => h.live > 0).map((h) => h.harness);
    },
    result: (ref) => readAnalysisResult(store, ref),
    ...(admit ? { admit: sqlAnalysisAdmit(admit) } : {}),
  };
}
