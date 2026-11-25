/**
 * The pipeline pool's checkout timeout (forward port of the backport lineage's 6d88b638): a stalled pooler fails
 * a checkout after HOOPDB_CONNECT_TIMEOUT_MS instead of holding the invocation until the Lambda timeout. Through
 * the real batch path: runtimeFromSecret's pool and pointer lock, the exported handler, a silent TCP server.
 * Synthetic URLs and ids only; no real database. The two batch tests wait the production 10 s on purpose.
 */
import { expect, test } from "bun:test";
import { createServer, type Server, type Socket } from "node:net";
import { createPipelineLambdaHandler, HOOPDB_CONNECT_TIMEOUT_MS, lambdaEnv, runtimeFromSecret, supabasePool, type Runtime } from "../lambda.ts";
import { captureLogs } from "./search-index-skip-flags-harness.ts";

const ENV = {
  AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "bucket-synthetic", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
  SESSION_PIPELINE_SECRET_ARN: "synthetic", SESSION_PIPELINE_QUEUE_URL: "https://sqs.invalid/q", HOOPDB_POOL_MAX: "1",
};
const KEY = "96bulls/org_synthetic/v1/ws=ws_synthetic/m=machine_a/h=claude/d=2026/11/06/s=session_synthetic/manifest.json";
const BODY = JSON.stringify({ reconcile: { key: KEY } });

/** A server that accepts TCP and never answers, like a stalled pooler. */
async function silentServer(): Promise<{ port: number; close(): void }> {
  const held: Socket[] = [];
  const server: Server = createServer((s) => { held.push(s); });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  return { port: (server.address() as { port: number }).port, close: () => { for (const s of held) s.destroy(); server.close(); } };
}

/** The runtime init would build (no secret lookup), over a hoopdb_url on the silent server. */
function runtime(port: number): Runtime {
  return runtimeFromSecret(lambdaEnv(ENV), JSON.stringify({ hoopdb_url: `postgresql://u:p@127.0.0.1:${port}/db` }), "synthetic-generation");
}

test("the pool is built with a 10 s checkout timeout and the given max", async () => {
  expect(HOOPDB_CONNECT_TIMEOUT_MS).toBe(10_000);
  const db = supabasePool("postgresql://u:p@127.0.0.1:1/db", 3);
  try {
    const o = (db.pool as unknown as { options: Record<string, unknown> }).options;
    expect(o.connectionTimeoutMillis).toBe(10_000);
    expect(o.max).toBe(3);
  } finally {
    await db.end();
  }
});

test("silent server: the batch's pointer checkout fails after about 10 s and the message is a batch item failure", async () => {
  const server = await silentServer();
  const rt = runtime(server.port);
  try {
    const handler = createPipelineLambdaHandler({ loadRuntime: async () => rt });
    const started = Date.now();
    const { result, lines } = await captureLogs(() => handler({ Records: [{ messageId: "m-silent", body: BODY }] } as never) as Promise<unknown>);
    const elapsed = Date.now() - started;
    expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: "m-silent" }] });
    expect(elapsed).toBeGreaterThanOrEqual(HOOPDB_CONNECT_TIMEOUT_MS - 500);
    expect(elapsed).toBeLessThan(HOOPDB_CONNECT_TIMEOUT_MS + 5_000);
    expect(lines.filter((l) => l.message === "pointer index failed")).toHaveLength(1);
  } finally {
    await rt.deps.pointer.db.end().catch(() => {});
    server.close();
  }
}, 30_000);

test("occupied pool: with its only slot held by another checkout, the batch's checkout fails at its own 10 s timer", async () => {
  const server = await silentServer();
  const rt = runtime(server.port);
  try {
    // HOOPDB_POOL_MAX=1: this checkout takes the pool's only slot (its connection never completes).
    const occupier = rt.deps.pointer.db.pool.connect().then(() => "connected", () => "failed");
    await new Promise((r) => setTimeout(r, 3_000));
    const handler = createPipelineLambdaHandler({ loadRuntime: async () => rt });
    const started = Date.now();
    const { result } = await captureLogs(() => handler({ Records: [{ messageId: "m-occupied", body: BODY }] } as never) as Promise<unknown>);
    const elapsed = Date.now() - started;
    expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: "m-occupied" }] });
    // Its own timer, counted from its own enqueue (the occupier's failure 3 s in does not end it early).
    expect(elapsed).toBeGreaterThanOrEqual(HOOPDB_CONNECT_TIMEOUT_MS - 500);
    expect(elapsed).toBeLessThan(HOOPDB_CONNECT_TIMEOUT_MS + 5_000);
    expect(await occupier).toBe("failed");
  } finally {
    await rt.deps.pointer.db.end().catch(() => {});
    server.close();
  }
}, 40_000);
