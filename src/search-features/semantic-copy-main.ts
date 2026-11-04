/** Portable explicit Node entry; disabled startup opens no client or credential. */
import { parseSecret, supabasePool } from "../session-pipeline/lambda.ts";
import { createClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import { semanticReadonlyPool, semanticFeatureReader } from "./semantic-copy-source-pool.ts";
import { runSemanticCopyTick } from "./semantic-copy-job.ts";
export async function semanticCopyMain(env: NodeJS.ProcessEnv = process.env, factory = () => {
    const sourceId = env.SESSION_SEARCH_FEATURE_SOURCE_ID;
    if (!sourceId)
        throw new Error("semantic copier source identity missing");
    const secret = parseSecret(env.SESSION_PIPELINE_SECRET_JSON ?? "");
    if (!secret.clickhouse || !secret.analysis_source_readonly_url)
        throw new Error("semantic copier configuration incomplete");
    const db = supabasePool(secret.hoopdb_url, 2), sourcePool = semanticReadonlyPool(secret.analysis_source_readonly_url), source = semanticFeatureReader(sourcePool), ch = createClickHouseClient(secret.clickhouse), controller = new AbortController(), abort = () => controller.abort();
    process.once("SIGTERM", abort);
    process.once("SIGINT", abort);
    return { run: () => runSemanticCopyTick({ db, source, ch, sourceId, endpoint: secret.clickhouse!.url, signal: controller.signal }), close: async () => { process.removeListener("SIGTERM", abort); process.removeListener("SIGINT", abort); await Promise.allSettled([db.end(), sourcePool.end()]); } };
}) { if (env.SESSION_SEMANTIC_COPY_ENABLED !== "1")
    return { enabled: false }; const r = factory(); try {
    return { enabled: true, ...await r.run() };
}
finally {
    await r.close();
} }
if (import.meta.main)
    semanticCopyMain().then(r => { console.info(JSON.stringify({ message: "semantic copier", ...r })); if ("failed" in r && r.failed)
        process.exitCode = 1; }).catch(() => { console.error(JSON.stringify({ message: "semantic copier failed", code: "semantic_copy_failed" })); process.exitCode = 1; });
