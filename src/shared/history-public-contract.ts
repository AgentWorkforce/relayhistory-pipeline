/** Pure SQL/consumer wire authority for a scoped logical summary claim.
 * No credentials, key loading, object IO, or unscoped/arbitrary-key capability.
 * Consumers must compare the complete claim again AFTER authenticated decrypt.
 */
export const HISTORY_PUBLIC_SUMMARY_RPC="session_directory_history_summary" as const;
export const HISTORY_PUBLIC_SUMMARY_VERSION=1 as const;
export type HistoryPublicSummaryKind="short"|"long";
export interface HistorySummaryPointer{
 kind:HistoryPublicSummaryKind;status:"ready"|"disabled"|"quarantined"|"empty"|"unsupported";
 objectKey:string|null;objectSha256:string|null;objectBytes:number|null;keyId:string|null;model:string|null;builtAt:string;
}
export interface HistoryPublicSummaryArgs{
 p_workspace_id:string;p_harness:string;p_session_id:string;p_kind:HistoryPublicSummaryKind;
}
export interface HistoryPublicSummaryClaim{
 version:1;kind:HistoryPublicSummaryKind;exists:boolean;required:boolean;
 /** Current logical source, not a promise that a summary pointer is present. */
 ready:boolean;
 identity:{orgId:string;workspaceId:string;source:string;sessionId:string};
 source:{epoch:string|null;fingerprint:string};
 authority:null|{authorityId:string;maskSha256:string;recordCount:string;derivedAnchorKey:string;schemaGeneration:string};
 storage:null|{provider:"s3";bucket:string;region:string};
 pointer:HistorySummaryPointer|null;
 /** Only qualified logical Referee state. Model evidence alone cannot set it. */
 state:null|{state:"active"|"done"|"abandoned"|"unknown";classifiedAt:string};
}
