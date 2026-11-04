/** One real leased page. Production wiring supplies the generated source API and
 * actual event/brief stages; checkpoint persistence is never a callback. */
import type { FeatureReader, FeatureKind } from "./source.ts";
import { readFeatureJournalPage } from "./source.ts";
import type { ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import { SemanticCopyStore, SemanticCopyError, type SemanticCopyLease } from "./semantic-copy-store.ts";
import { semanticCheckpoint, semanticDecimal, type SemanticFeatureCheckpoint } from "./semantic-copy-protocol.ts";
import { publishSemanticCopyProfile, publishSemanticCopyAuthority, type SemanticFeatureTables } from "./semantic-copy-publication.ts";
export interface VerifiedFeatureSource {
    identitySha256: string;
    observedRevision: string;
}
export interface SemanticCopyPageDeps {
    store: SemanticCopyStore;
    source: FeatureReader;
    ch: ClickHouseClient;
    verifySource: () => Promise<VerifiedFeatureSource>;
    featureTables: SemanticFeatureTables;
    copyPage: (kind: FeatureKind, previous: SemanticFeatureCheckpoint) => Promise<{
        next: SemanticFeatureCheckpoint;
        published: number;
        complete: boolean;
    }>;
    signal?: AbortSignal;
    heartbeatMs?: number;
    now?: () => number;
}
export async function runSemanticCopyPage(deps: SemanticCopyPageDeps, kind: FeatureKind): Promise<{
    status: "idle" | "published";
    complete: boolean;
    published: number;
}> {
    const now = deps.now ?? Date.now, interval = deps.heartbeatMs ?? 30000;
    if (!Number.isInteger(interval) || interval < 100 || interval > 30000)
        throw new SemanticCopyError("heartbeat_bound");
    const verify = async () => { const proof = await deps.verifySource(); if (proof.identitySha256 !== deps.store.sourceAuthorityHash || !semanticDecimal(proof.observedRevision))
        throw new SemanticCopyError("source_authority_unverified"); return proof; };
    await verify();
    deps.signal?.throwIfAborted();
    let c = await deps.store.claim(kind);
    if (c === null)
        return { status: "idle", complete: false, published: 0 };
    const heartbeatAbort=new AbortController();
    let deadline = c.leaseExpiresAt, lost = false, beating: Promise<void> | undefined;
    const active = () => { deps.signal?.throwIfAborted(); if (lost || Date.parse(deadline) <= now())
        throw new SemanticCopyError("checkpoint_lease_lost"); };
    const beat = async () => { try {
        const n = await deps.store.heartbeat(c!,heartbeatAbort.signal);
        deadline = n.leaseExpiresAt;
    }
    catch {
        if(!heartbeatAbort.signal.aborted)lost = true;
    } };
    const timer = setInterval(() => { if (!beating)
        beating = beat().finally(() => { beating = undefined; }); }, interval);
    const stopHeartbeat=async()=>{clearInterval(timer);heartbeatAbort.abort();await beating};
    try {
        active();
        await publishSemanticCopyProfile(deps.ch, c, deps.featureTables);
        active();
        if (c.state.baselineH === null) {
            const h = (await readFeatureJournalPage(deps.source, kind, "0", 1)).observedRevision;
            c = await deps.store.initialize(c, h);
            active();
        }
        await publishSemanticCopyAuthority(deps.ch, c, true); // ACK dirty BEFORE any feature write.
        await beat();
        active();
        const result = await deps.copyPage(kind, c.state);
        active();
        const proof = await verify();
        const next = semanticCheckpoint(result.next);
        if (BigInt(proof.observedRevision) < BigInt(next.observedRevision))
            throw new SemanticCopyError("source_revision_reset");
        const actualNext = next.phase === "journal" && next.polled ? { ...next, observedRevision: proof.observedRevision } : next;
        await stopHeartbeat();
        active();
        c = await deps.store.commit(c, actualNext, new Date(now()).toISOString());
        active();
        await publishSemanticCopyAuthority(deps.ch, c, false);
        active();
        if (!await deps.store.finish(c))
            throw new SemanticCopyError("checkpoint_finish_failed");
        return { status: "published", complete: c.complete, published: result.published };
    }
    catch (error) {
        await stopHeartbeat();
        await deps.store.finish(c, "copy_page_failed").catch(() => false);
        throw new SemanticCopyError(error instanceof SemanticCopyError ? error.reason : "copy_page_failed");
    }
    finally {
        await stopHeartbeat();
    }
}
