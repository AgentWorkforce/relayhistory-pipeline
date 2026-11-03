/** Shared immutable analysis-cache envelope. Public payload rows stay owned by
 * the RHC v1 SQL projection; private identity/ordinal fields are out of band. */
import type {ArtifactIdentity,ArtifactRef,ArtifactSource} from "./canonical-artifact.ts";
export const ANALYSIS_ARTIFACT_VERSION=1 as const;
export const ANALYSIS_ARTIFACT_TABLES=["session_analysis_requests","session_analyses"] as const;
export const ANALYSIS_MAX_HISTORY=50;
export const ANALYSIS_FRESH_SECONDS=120;
export interface SessionAnalysisIndex {
 version:1;kind:"session-analysis-index";codec:"canonical-analysis-graph-v1";
 projection:"read_session_analysis_v1/0031";outcome:"ok"|"not_found";
 identity:ArtifactIdentity;source:ArtifactSource;generation:string;
 snapshotAt:string;snapshotId:string;snapshotSha256:string;
 graph:ArtifactRef;counts:{requests:number;analyses:number};
}
export interface SessionAnalysisArtifactRow {
 org_id:string;workspace_id:string;source:string;session_id:string;
 ordinal:number;payload:Record<string,unknown>;
}
export interface SessionAnalysisSnapshot {
 snapshotAt:string;snapshotId:string;
 payload:{outcome:"not_found"}|{outcome:"ok";sessionId:string;requests:Record<string,unknown>[];analyses:Record<string,unknown>[]};
}

/** Fixed envelope key order on both sides; public row keys/values are preserved. */
export function analysisSnapshotJson(payload:SessionAnalysisSnapshot["payload"]):string {
 return payload.outcome==="not_found"?JSON.stringify({outcome:"not_found"}):JSON.stringify({outcome:"ok",sessionId:payload.sessionId,requests:payload.requests,analyses:payload.analyses});
}

/** Generation tuple is independent of JSONB/RPC property insertion order. */
export function analysisGenerationJson(identity:ArtifactIdentity,source:ArtifactSource,snapshotAt:string,snapshotId:string,snapshotSha256:string):string {
 return JSON.stringify(["session-analysis-cache-v1",{orgId:identity.orgId,workspaceId:identity.workspaceId,source:identity.source,sessionId:identity.sessionId},{epoch:source.epoch,fingerprint:source.fingerprint},snapshotAt,snapshotId,snapshotSha256]);
}
