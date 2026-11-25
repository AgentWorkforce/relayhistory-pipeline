/**
 * The session-analysis runner's store path on a real versioned MinIO: the startup versioning check, a
 * create-only result put through the endpoint store, and the sweeper's eraser over the same endpoint and
 * keys. Uses the disposable, loopback-only versioned MinIO fixture (FRONTIER_TEST_S3_*); skipped without it.
 */
import { describe, expect, test } from "bun:test";
import { analysisRunnerConfig, runnerAnalysisDeps, runnerSecrets } from "../runner.ts";
import { readBucketVersioning } from "../bucket-versioning.ts";
import type { HoopDb } from "../../shared/db.ts";

const endpoint = process.env.FRONTIER_TEST_S3_ENDPOINT;
const bucket = process.env.FRONTIER_TEST_S3_BUCKET;
const accessKeyId = process.env.FRONTIER_TEST_S3_ACCESS_KEY;
const secretAccessKey = process.env.FRONTIER_TEST_S3_SECRET_KEY;
const enabled = !!(endpoint && bucket && accessKeyId && secretAccessKey);
if (endpoint) {
  const u = new URL(endpoint);
  if (u.protocol !== "http:" || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname)
    || u.username || u.password || u.pathname !== "/" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}

describe.skipIf(!enabled)("session-analysis runner on versioned MinIO", () => {
  test("versioning check passes; a result put and the eraser use the same endpoint and keys", async () => {
    const env = {
      SESSION_ANALYSIS_ENABLED: "true", SESSION_ANALYSIS_S3_BUCKET: bucket!, SESSION_ANALYSIS_S3_REGION: "us-east-1",
      SESSION_ANALYSIS_S3_ENDPOINT: endpoint!, SESSION_ANALYSIS_S3_CREDENTIALS: "static",
      SESSION_ANALYSIS_S3_ACCESS_KEY_ID: accessKeyId!, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: secretAccessKey!,
      PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls",
      SESSION_ANALYSIS_DATABASE_URL: "postgresql://unused@127.0.0.1:5432/postgres", SESSION_ANALYSIS_OPENAI_API_KEY: "sk-disabled",
    } as NodeJS.ProcessEnv;
    const cfg = analysisRunnerConfig(env);
    const secrets = runnerSecrets(env);
    expect(await readBucketVersioning({ bucket: cfg.bucket, region: cfg.region, endpoint: cfg.endpoint!, credentials: secrets.s3! })).toBe("Enabled");
    const db = { end: async () => {} } as unknown as HoopDb;
    const { deps } = runnerAnalysisDeps(env, cfg, secrets, { pool: () => db });
    const prefix = `analysisproof${crypto.randomUUID().replaceAll("-", "")}/`;
    const key = `${prefix}org/v1/normalized/session-analysis-v2/result.json`;
    const bytes = new TextEncoder().encode('{"synthetic":true}');
    const first = await deps.store.put(key as never, bytes, { ifNoneMatch: true, contentType: "application/json" });
    expect(first.created).toBe(true);
    expect(first.version.version).toBeTruthy();
    // Create-only is idempotent for identical bytes: no new version, the same one reported back.
    const again = await deps.store.put(key as never, bytes, { ifNoneMatch: true, contentType: "application/json" });
    expect(again.created).toBe(false);
    expect(again.version.version).toBe(first.version.version);
    // Different bytes at the same key are refused: the write-once guarantee.
    await expect(deps.store.put(key as never, new TextEncoder().encode('{"synthetic":false}'), { ifNoneMatch: true, contentType: "application/json" })).rejects.toMatchObject({ code: "already_exists" });
    // The refused put changed nothing: the current object is still the first version, with the first bytes.
    const current = await deps.store.getObject(key as never);
    expect(current.meta.version.version).toBe(first.version.version);
    expect(new Uint8Array(await new Response(current.body).arrayBuffer())).toEqual(bytes);
    const authorize = async () => {};
    for (let i = 0; i < 10; i++) if ((await deps.eraser!.eraseStep(prefix, { scope: "proof", authorize })).status === "traversed") break;
    expect(await deps.eraser!.verifyErased(prefix, { authorize })).toMatchObject({ status: "erased" });
  });
});
