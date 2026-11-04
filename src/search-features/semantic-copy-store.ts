import type {SemanticScopedSql} from "./semantic-copy-postgres.ts";
/** Real metadata RPC adapter. The database owns lease/generation/checkpoint state. */
import { randomUUID } from "node:crypto";
import type { SqlExecutor } from "../../packages/pipeline/src/lib/pipeline/queue.ts";
import { semanticCheckpoint, semanticHash, semanticDecimal, semanticSha256, semanticDataHashInput, semanticAuthorityHashInput, type SemanticFeatureCheckpoint, type SemanticFeatureKind } from "./semantic-copy-protocol.ts";
export class SemanticCopyError extends Error {
    constructor(readonly reason: string) { super(reason); this.name = "SemanticCopyError"; }
}
export interface SemanticCopyLease {
    version: 1;
    sourceId: string;
    kind: SemanticFeatureKind;
    sourceAuthorityHash: string;
    targetHash: string;
    profileHash: string;
    profileJson: string;
    generation: string;
    leaseNonce: string;
    leaseExpiresAt: string;
    state: SemanticFeatureCheckpoint;
    stateJson: string;
    stateHash: string;
    dataHash: string;
    authorityHash: string;
    complete: boolean;
    checkedAt: string | null;
    freshUntil: string | null;
}
const fail = (): never => { throw new SemanticCopyError("checkpoint_ack_invalid"); };
const stamp = (v: unknown): v is string => typeof v === "string" && /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$/u.test(v) && Number.isFinite(Date.parse(v));
const stateTuple = (s: SemanticFeatureCheckpoint) => JSON.stringify([s.phase, s.baselineH, s.baselineCursor, s.checkpoint, s.observedRevision, s.polled]);
export async function checkedSemanticLease(raw: unknown, expected: {
    sourceId: string;
    kind: SemanticFeatureKind;
    sourceAuthorityHash: string;
    nonce: string;
    generation?: string;
}): Promise<SemanticCopyLease> {
    const c = raw as SemanticCopyLease;
    if (!c || c.version !== 1 || c.sourceId !== expected.sourceId || c.kind !== expected.kind || c.sourceAuthorityHash !== expected.sourceAuthorityHash || c.leaseNonce !== expected.nonce || expected.generation !== undefined && c.generation !== expected.generation || !semanticDecimal(c.generation) || c.generation === "0" || ![c.targetHash, c.profileHash, c.stateHash, c.dataHash, c.authorityHash].every(semanticHash) || !stamp(c.leaseExpiresAt) || typeof c.profileJson !== "string" || typeof c.stateJson !== "string" || typeof c.complete !== "boolean" || !(c.checkedAt === null || stamp(c.checkedAt)) || !(c.freshUntil === null || stamp(c.freshUntil)))
        return fail();
    let state, decoded;
    try {
        state = semanticCheckpoint(c.state);
        decoded = semanticCheckpoint(JSON.parse(c.stateJson));
    }
    catch {
        return fail();
    }
    if (stateTuple(state) !== stateTuple(decoded) || await semanticSha256(c.stateJson) !== c.stateHash || await semanticSha256(c.profileJson) !== c.profileHash || await semanticSha256(semanticDataHashInput(c)) !== c.dataHash || await semanticSha256(semanticAuthorityHashInput(c.dataHash, c.generation, c.complete)) !== c.authorityHash)
        return fail();
    if (c.complete && (state.phase !== "journal" || !state.polled || state.checkpoint !== state.observedRevision))
        return fail();
    return c;
}
export class SemanticCopyStore {
    constructor(readonly db: SqlExecutor, readonly sourceId: string, readonly sourceAuthorityHash: string) { }
    private async body(sql:string,args:unknown[],signal?:AbortSignal){const db=this.db as Partial<SemanticScopedSql>;const result=signal&&db.queryWithSignal?await db.queryWithSignal<{v:unknown}>(sql,args,signal):await this.db.query<{v:unknown}>(sql,args);return result.rows[0]?.v}
    private expected(kind: SemanticFeatureKind, nonce: string, generation?: string) { return { sourceId: this.sourceId, kind, sourceAuthorityHash: this.sourceAuthorityHash, nonce, generation }; }
    async register(targetHash: string, profileHash: string, profileJson: string) { if (await this.body("SELECT sessions.register_search_feature_source($1,$2,$3,$4,$5) v", [this.sourceId, targetHash, this.sourceAuthorityHash, profileHash, profileJson]) !== true)
        throw new SemanticCopyError("source_binding_changed"); }
    async claim(kind: SemanticFeatureKind): Promise<SemanticCopyLease | null> { const nonce = randomUUID(), v = await this.body("SELECT sessions.claim_search_feature_copy($1,$2,$3,$4::uuid) v", [this.sourceId, kind, this.sourceAuthorityHash, nonce]); return v === null ? null : checkedSemanticLease(v, this.expected(kind, nonce)); }
    async initialize(c: SemanticCopyLease, h: string) { const v = await this.body("SELECT sessions.initialize_search_feature_baseline($1,$2,$3::uuid,$4::bigint,$5,$6::bigint) v", [this.sourceId, c.kind, c.leaseNonce, c.generation, c.stateHash, h]); if (v === null)
        throw new SemanticCopyError("baseline_lease_lost"); return checkedSemanticLease(v, this.expected(c.kind, c.leaseNonce, c.generation)); }
    async heartbeat(c: SemanticCopyLease,signal?:AbortSignal) { const v = await this.body("SELECT sessions.heartbeat_search_feature_copy($1,$2,$3::uuid,$4::bigint) v", [this.sourceId, c.kind, c.leaseNonce, c.generation],signal); if (v === null)
        throw new SemanticCopyError("checkpoint_lease_lost"); return checkedSemanticLease(v, this.expected(c.kind, c.leaseNonce, c.generation)); }
    async commit(c: SemanticCopyLease, next: SemanticFeatureCheckpoint, checkedAt: string) { const v = await this.body("SELECT sessions.commit_search_feature_copy($1,$2,$3::uuid,$4::bigint,$5,$6::jsonb,$7::timestamptz) v", [this.sourceId, c.kind, c.leaseNonce, c.generation, c.stateHash, JSON.stringify(next), checkedAt]); if (v === null)
        throw new SemanticCopyError("checkpoint_cas_failed"); const result = await checkedSemanticLease(v, this.expected(c.kind, c.leaseNonce, c.generation)); if (stateTuple(result.state) !== stateTuple(next))
        throw new SemanticCopyError("checkpoint_state_mismatch"); return result; }
    async finish(c: SemanticCopyLease, error: string | null = null) { return await this.body("SELECT sessions.finish_search_feature_copy($1,$2,$3::uuid,$4::bigint,$5,$6) v", [this.sourceId, c.kind, c.leaseNonce, c.generation, c.authorityHash, error]) === true; }
}
