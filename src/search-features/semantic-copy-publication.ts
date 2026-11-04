/** Durable CH ACKs and read-back fences. No automatic DDL or target adoption. */
import type { ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import type { SemanticCopyLease } from "./semantic-copy-store.ts";
import { SemanticCopyError } from "./semantic-copy-store.ts";
import { SEMANTIC_COPY_AUTHORITY_TABLE, SEMANTIC_COPY_PROFILE_TABLE, semanticStatusVersion, semanticSha256, semanticAuthorityHashInput, parseSemanticProfile } from "./semantic-copy-protocol.ts";
const settings = { async_insert: 0, wait_for_async_insert: 1 };
function profileRow(c: SemanticCopyLease) { return { source_id: c.sourceId, source_authority_hash: c.sourceAuthorityHash, target_hash: c.targetHash, profile_hash: c.profileHash, profile_json: c.profileJson }; }
export interface SemanticFeatureTables {event:string;brief:string}
export async function publishSemanticCopyProfile(ch: ClickHouseClient, c: SemanticCopyLease, featureTables: SemanticFeatureTables) {
    const tables=Object.values(featureTables);
    parseSemanticProfile(c.profileJson);
    if (Object.keys(featureTables).sort().join(",")!=="brief,event" || new Set(tables).size!==2 || tables.some(t => !/^[a-z_][a-z0-9_]*$/u.test(t)))
        throw new SemanticCopyError("target_inventory_invalid");
    const prior = await ch.select<any>("SELECT source_id,source_authority_hash,target_hash,profile_hash,profile_json FROM " + SEMANTIC_COPY_PROFILE_TABLE + " FINAL LIMIT 2", { settings: { max_result_rows: 2, max_result_bytes: 524288 } });
    const row = profileRow(c), same = (v: any) => Object.entries(row).every(([k, value]) => v[k] === value);
    if (prior.length > 1 || prior.length === 1 && !same(prior[0]))
        throw new SemanticCopyError("target_binding_changed");
    if (!prior.length) {
        if (c.state.phase !== "baseline" || c.state.baselineH !== null || c.state.baselineCursor !== null || c.state.checkpoint !== "0" || c.state.observedRevision !== "0" || c.state.polled)
            throw new SemanticCopyError("checkpoint_without_target_binding");
        const counts = await ch.select<{
            n: string;
        }>("SELECT toString(sum(n)) n FROM (" + tables.map(t => "SELECT count() n FROM " + t + " FINAL").join(" UNION ALL ") + ")", { settings: { max_result_rows: 1, max_result_bytes: 1024 } });
        if (counts.length !== 1 || counts[0]!.n !== "0")
            throw new SemanticCopyError("unowned_target_not_empty");
    }
    // A global profile does not prove ownership of a rebuilt per-kind checkpoint.
    // Check BEFORE capturing H: a new baseline would otherwise skip old deletions.
    if (c.state.baselineH===null) {
        const kindCounts=await ch.select<{n:string}>("SELECT toString(count()) n FROM "+featureTables[c.kind]+" FINAL",{settings:{max_result_rows:1,max_result_bytes:1024}});
        if(kindCounts.length!==1||kindCounts[0]!.n!=="0")throw new SemanticCopyError("unowned_kind_target");
        const authorityCounts=await ch.select<{n:string}>("SELECT toString(count()) n FROM "+SEMANTIC_COPY_AUTHORITY_TABLE+" FINAL"+(prior.length?" WHERE kind={kind:String}":""),{params:prior.length?{kind:c.kind}:undefined,settings:{max_result_rows:1,max_result_bytes:1024}});
        if(authorityCounts.length!==1||authorityCounts[0]!.n!=="0")throw new SemanticCopyError("unowned_kind_authority");
    }
    await ch.insert(SEMANTIC_COPY_PROFILE_TABLE, [row], { settings });
    const read = await ch.select<any>("SELECT source_id,source_authority_hash,target_hash,profile_hash,profile_json FROM " + SEMANTIC_COPY_PROFILE_TABLE + " FINAL LIMIT 2", { settings: { max_result_rows: 2, max_result_bytes: 524288 } });
    if (read.length !== 1 || !same(read[0]) || await semanticSha256(read[0].profile_json) !== c.profileHash)
        throw new SemanticCopyError("profile_publication_unverified");
}
export async function publishSemanticCopyAuthority(ch: ClickHouseClient, c: SemanticCopyLease, dirty: boolean) {
    if (!dirty && (!c.freshUntil || !c.checkedAt))
        throw new SemanticCopyError("checkpoint_not_committed");
    const complete = dirty ? false : c.complete, authorityHash = await semanticSha256(semanticAuthorityHashInput(c.dataHash, c.generation, complete));
    const row = { source_id: c.sourceId, kind: c.kind, source_authority_hash: c.sourceAuthorityHash, target_hash: c.targetHash, profile_hash: c.profileHash, generation: c.generation, status_version: semanticStatusVersion(c.generation, dirty), dirty: dirty ? 1 : 0, complete: complete ? 1 : 0, state_json: c.stateJson, state_hash: c.stateHash, data_hash: c.dataHash, authority_hash: authorityHash, fresh_until: (dirty ? c.leaseExpiresAt : c.freshUntil!).replace("T", " ").replace(/Z$/u, "") };
    await ch.insert(SEMANTIC_COPY_AUTHORITY_TABLE, [row], { settings });
    const read = await ch.select<any>("SELECT source_id,kind,source_authority_hash,target_hash,profile_hash,toString(generation) generation,toString(status_version) status_version,dirty,complete,state_json,state_hash,data_hash,authority_hash,formatDateTime(fresh_until,'%Y-%m-%dT%H:%i:%S.%fZ','UTC') fresh_until FROM " + SEMANTIC_COPY_AUTHORITY_TABLE + " FINAL WHERE source_id={source:String} AND kind={kind:String} LIMIT 2", { params: { source: c.sourceId, kind: c.kind }, settings: { max_result_rows: 2, max_result_bytes: 65536 } });
    if (read.length !== 1 || !Object.entries(row).every(([k, v]) => k === "fresh_until" ? read[0][k] === String(v).replace(" ", "T") + "Z" : read[0][k] === v))
        throw new SemanticCopyError("authority_publication_unverified");
    return authorityHash;
}
