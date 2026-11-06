/**
 * The read API's session-analysis source (lane F1): sessions.analysis_read_v1 as the
 * read role in one READ ONLY tenant-bound transaction, and the result objects read and
 * verified from the canonical store. No admission here: POST needs a separate principal
 * (F1 map F1.4), and no brief lookup until lane F2 grants the read role its brief read.
 */
import type { ObjectStore } from "../../packages/object-store/src/index.ts";
import type { AnalysisDeps } from "./app.ts";
import type { TenantRunner } from "./liveness.ts";
import { readAnalysisResult } from "../session-analysis/results.ts";
import type { ReadRpc } from "../session-analysis/read.ts";

export const ANALYSIS_READ_SQL = "SELECT sessions.analysis_read_v1($1, $2) AS r";

export function sqlAnalysisSource(run: TenantRunner, store: ObjectStore): AnalysisDeps {
  return {
    async read(identity, sessionId) {
      const { rows } = await run(identity.orgId, identity.workspaceId, (q) =>
        q.query<{ r: ReadRpc }>(ANALYSIS_READ_SQL, [identity.workspaceId, sessionId]),
      );
      const value = rows[0]?.r;
      if (!value || (value.outcome !== "ok" && value.outcome !== "not_found")) throw new Error("analysis read contract");
      return value;
    },
    result: (ref) => readAnalysisResult(store, ref),
  };
}
