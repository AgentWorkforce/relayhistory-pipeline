/** Actual bounded Node job: generated source verification, both real stages,
 * persistent RPC checkpoints, CH dirty/final ACKs. No runtime DDL or model calls. */
import type { SqlExecutor } from "../../packages/pipeline/src/lib/pipeline/queue.ts";
import { createClickHouseClient, type ClickHouseConfig, type ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import { FEATURE_SOURCE_AUTHORITY } from "./source-authority.ts";
import { verifyFeatureSourceAuthority, FeatureJournalError, type FeatureReader } from "./source.ts";
import { readLegacyCasefoldProfile } from "./casefold-source.ts";
import { copyEventFeaturePage } from "./event-cycle.ts";
import { copyBriefFeaturePage } from "./brief-cycle.ts";
import { EVENT_FEATURES_TABLE, eventFeatureSchemaStatements } from "./event-schema.ts";
import { BRIEF_FEATURES_TABLE, briefFeatureSchemaStatements } from "./brief-schema.ts";
import { verifySemanticReadonlyRole } from "./semantic-copy-source-pool.ts";
import { SemanticCopyStore, SemanticCopyError } from "./semantic-copy-store.ts";
import { runSemanticCopyPage } from "./semantic-copy-runtime.ts";
import { semanticSha256, semanticTargetHashInput, parseSemanticProfile, type SemanticFeatureKind } from "./semantic-copy-protocol.ts";
export async function runSemanticCopyTick(deps: {
    db: SqlExecutor;
    source: FeatureReader;
    ch: ClickHouseClient;
    sourceId: string;
    endpoint: string;
    signal?: AbortSignal;
}, options: {
    maxPages?: number;
    batch?: number;
    maxMs?: number;
    now?: () => number;
} = {}) {
    const maxPages = options.maxPages ?? 20, batch = options.batch ?? 200, maxMs = options.maxMs ?? 240000, now = options.now ?? Date.now;
    if (!Number.isInteger(maxPages) || maxPages < 1 || maxPages > 100 || !Number.isInteger(batch) || batch < 1 || batch > 200 || !Number.isFinite(maxMs) || maxMs < 1 || maxMs > 240000)
        throw new SemanticCopyError("job_bound");
    if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(deps.sourceId))
        throw new SemanticCopyError("source_id_invalid");
    const stop = now() + maxMs, controller = new AbortController(), abort = () => controller.abort();
    deps.signal?.addEventListener("abort", abort, { once: true });
    if (deps.signal?.aborted)
        abort();
    const timer = setTimeout(abort, maxMs);
    const active = () => { controller.signal.throwIfAborted(); if (now() >= stop)
        throw new SemanticCopyError("job_deadline"); };
    const source: FeatureReader = { async query<T>(sql: string, params?: unknown[]) { active(); const r = await deps.source.query<T>(sql, params); active(); return r; } };
    try {
        await verifySemanticReadonlyRole(source, FEATURE_SOURCE_AUTHORITY.catalog.relations.map(r => r.identity), FEATURE_SOURCE_AUTHORITY.catalog.functions.filter(f => f.securityDefiner).map(f => f.identity));
        const proof = await verifyFeatureSourceAuthority(source, FEATURE_SOURCE_AUTHORITY);
        if (proof.identitySha256 !== FEATURE_SOURCE_AUTHORITY.identitySha256)
            throw new SemanticCopyError("source_authority_unverified");
        const profile = await readLegacyCasefoldProfile(source), profileJson = JSON.stringify({ source: profile.source, entries: profile.entries });
        parseSemanticProfile(profileJson);
        if (await semanticSha256(profileJson) !== profile.sourceHash)
            throw new SemanticCopyError("profile_hash_invalid");
        const eventSchema = await semanticSha256(JSON.stringify(["event-feature-projection-v1", ...eventFeatureSchemaStatements()])), briefSchema = await semanticSha256(JSON.stringify(["brief-feature-projection-v1", ...briefFeatureSchemaStatements()]));
        const targetHash = await semanticSha256(semanticTargetHashInput(deps.endpoint, deps.ch.database, eventSchema, briefSchema));
        const store = new SemanticCopyStore(deps.db, deps.sourceId, FEATURE_SOURCE_AUTHORITY.identitySha256);
        await store.register(targetHash, profile.sourceHash, profileJson);
        const batches = { event: batch, brief: batch };
        const stats = { pages: 0, published: 0, eventComplete: false, briefComplete: false, failed: 0 }, idle = new Set<SemanticFeatureKind>();
        for (let n = 0; n < maxPages && idle.size < 2; n++) {
            active();
            const kind: SemanticFeatureKind = n % 2 === 0 ? "event" : "brief";
            if (idle.has(kind))
                continue;
            let result;
            try {
                result = await runSemanticCopyPage({ store, source, ch: deps.ch, featureTables: {event:EVENT_FEATURES_TABLE,brief:BRIEF_FEATURES_TABLE}, signal: controller.signal, now,
                    verifySource: () => verifyFeatureSourceAuthority(source, FEATURE_SOURCE_AUTHORITY),
                    copyPage: async (k, previous) => { let n = batches[k]; for (;;) {
                        try {
                            const result = k === "event" ? await copyEventFeaturePage({ db: source, ch: deps.ch, signal: controller.signal }, previous, n) : await copyBriefFeaturePage({ db: source, ch: deps.ch, signal: controller.signal }, previous, n);
                            batches[k] = n;
                            return result;
                        }
                        catch (error) {
                            if (!(error instanceof FeatureJournalError) || error.reason !== "page_bytes" || n <= 1)
                                throw error;
                            n = Math.max(1, Math.floor(n / 2));
                        }
                    } } }, kind);
            }
            catch {
                stats.failed++;
                idle.add(kind);
                continue;
            }
            if (result.status === "idle") {
                idle.add(kind);
                continue;
            }
            stats.pages++;
            stats.published += result.published;
            if (kind === "event")
                stats.eventComplete = result.complete;
            else
                stats.briefComplete = result.complete;
            if (result.complete)
                idle.add(kind);
        }
        return stats;
    }
    finally {
        clearTimeout(timer);
        deps.signal?.removeEventListener("abort", abort);
    }
}
export { createClickHouseClient, type ClickHouseConfig };
