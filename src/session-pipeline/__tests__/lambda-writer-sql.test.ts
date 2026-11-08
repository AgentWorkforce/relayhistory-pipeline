import { afterEach, expect, test } from "bun:test";
import pg from "pg";
import { createSearchIndexRegistry } from "@rhc/adapters/defaults.ts";
import type { SearchEventDocument } from "@rhc/search-index/contracts.ts";
import { TENANT_SETTINGS_SQL } from "@rhc/search-index/postgres.ts";
import { lambdaEnv, runtimeFromSecret } from "../lambda.ts";
import { indexSearchScope } from "../search-index.ts";

type Query = { client: number; text: string; params?: unknown[] };
/** Capacity and checkout ownership are real constraints here; SQL responses
 * are inert fixtures. No database, socket, AWS client or provider is used. */
class BoundedPool {
  static instances: BoundedPool[] = [];
  readonly queries: Query[] = [];
  readonly config: pg.PoolConfig;
  active = 0;
  poolQueries = 0;
  peakWaiting = 0;
  failErase = false;
  synchronizeBegins = false;
  private nextClient = 0;
  private begun = 0;
  private start!: () => void;
  private barrier = new Promise<void>(resolve => { this.start = resolve; });
  private waiting: { resolve(client: ReturnType<BoundedPool["client"]>): void; reject(error: Error): void; timer: ReturnType<typeof setTimeout> }[] = [];

  constructor(config: pg.PoolConfig) { this.config = config; BoundedPool.instances.push(this); }
  on() { return this; }
  private client() {
    this.active++;
    const id = ++this.nextClient;
    let released = false;
    return {
      query: async (text: string, params?: unknown[]) => {
        this.queries.push({ client: id, text, params });
        if (text === "BEGIN" && this.synchronizeBegins) {
          if (++this.begun === this.config.max) this.start();
          await this.barrier;
        }
        await Promise.resolve(); // Interleave concurrent passes across awaits.
        if (this.failErase && text.startsWith('DELETE FROM "search_index".')) throw Object.assign(new Error("synthetic index failure"), { code: "XX000" });
        const rows = text.includes("pg_try_advisory_xact_lock") ? [{ ok: true }]
          : text.startsWith("WITH input AS") || text.startsWith("WITH gone AS") ? [{ n: 1 }]
          : text.includes("to_regclass") ? [{ present: true }] : [];
        return { rows };
      },
      release: () => {
        expect(released).toBe(false); released = true; this.active--;
        const waiting = this.waiting.shift();
        if (waiting) { clearTimeout(waiting.timer); waiting.resolve(this.client()); }
      },
    };
  }
  async connect(): Promise<ReturnType<BoundedPool["client"]>> {
    if (this.active < this.config.max!) return this.client();
    return new Promise((resolve, reject) => {
      const waiting = { resolve, reject, timer: undefined as unknown as ReturnType<typeof setTimeout> };
      waiting.timer = setTimeout(() => {
        this.waiting = this.waiting.filter(value => value !== waiting);
        reject(new Error("bounded pool capacity exhausted"));
      }, 50);
      this.waiting.push(waiting); this.peakWaiting = Math.max(this.peakWaiting, this.waiting.length);
    });
  }
  async query(text: string, params?: unknown[]) {
    this.poolQueries++;
    const client = await this.connect();
    try { return await client.query(text, params); } finally { client.release(); }
  }
  async end() { expect(this.active).toBe(0); expect(this.waiting).toHaveLength(0); }
}

const RealPool = pg.Pool;
afterEach(() => { pg.Pool = RealPool; BoundedPool.instances.length = 0; });
const scope = (i: number) => ({ orgId: `org-${i}`, workspaceId: `ws-${i}`, harness: "codex", sessionId: `session-${i}` });
function runtime(capacity: number) {
  pg.Pool = BoundedPool as unknown as typeof pg.Pool;
  const complete: string[] = [];
  const composition = {
    registry: createSearchIndexRegistry(),
    state: () => ({
      fromMs: async () => 0,
      async record(tx: { query(text: string, params?: unknown[]): Promise<unknown> }, actual: ReturnType<typeof scope>, value: { complete: boolean }) {
        await tx.query("SELECT state_record", [actual.orgId]);
        if (value.complete) complete.push(actual.orgId);
      },
      partials: async () => ({ sessions: [], overflow: false }),
    }),
  };
  const env = lambdaEnv({
    AWS_REGION: "us-west-2", PIPELINE_RAW_S3_BUCKET: "synthetic", PIPELINE_KEY_PREFIX: "96bulls",
    PIPELINE_ORG_FROM_PREFIX: "true", SESSION_PIPELINE_SECRET_ARN: "synthetic", SESSION_PIPELINE_QUEUE_URL: "synthetic",
    SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_BACKEND: "postgres", HOOPDB_POOL_MAX: String(capacity),
  }, composition);
  const rt = runtimeFromSecret(env, JSON.stringify({ hoopdb_url: "postgresql://synthetic.invalid/fixture" }), "synthetic-generation", composition);
  return { rt, deps: rt.deps.search!, pool: BoundedPool.instances.at(-1)!, complete };
}

for (const capacity of [1, 2]) {
  test(`portable writer completes erased scopes without nested pool acquisition at capacity ${capacity}`, async () => {
    const { rt, deps, pool, complete } = runtime(capacity);
    pool.synchronizeBegins = capacity === 2;
    try {
      const settled = await Promise.allSettled(Array.from({ length: capacity }, (_, i) => indexSearchScope(scope(i), deps)));
      expect(settled.map(result => result.status)).toEqual(Array.from({ length: capacity }, () => "fulfilled"));
      const results = settled.flatMap(result => result.status === "fulfilled" ? [result.value] : []);
      expect(results.every(result => result.complete && result.reasons[0] === "erased")).toBe(true);
      expect(complete.sort()).toEqual(Array.from({ length: capacity }, (_, i) => scope(i).orgId));
      expect(pool.poolQueries).toBe(0); expect(pool.peakWaiting).toBe(0);
      for (const query of pool.queries.filter(query => query.text.startsWith('DELETE FROM "search_index".'))) {
        const metadata = pool.queries.find(other => other.client === query.client && other.text.includes("SELECT p.manifest_key"))!;
        expect(query.params![0]).toBe(metadata.params![0]); expect(query.params![1]).toBe(metadata.params![1]);
        // Forced RLS: the statement right before it on the same connection binds
        // this pass's tenant, so concurrent passes never share or inherit one.
        const own = pool.queries.filter(other => other.client === query.client);
        expect(own[own.indexOf(query) - 1]).toEqual({ client: query.client, text: TENANT_SETTINGS_SQL, params: [metadata.params![0], metadata.params![1]] });
      }
      expect(pool.queries.filter(query => query.text === "COMMIT")).toHaveLength(capacity);
      expect(pool.queries.filter(query => query.text === "ROLLBACK")).toHaveLength(0);
      expect(pool.config.max).toBe(capacity);
      expect(pool.config.ssl).toMatchObject({ rejectUnauthorized: true });
      expect(BoundedPool.instances).toHaveLength(1);
    } finally { await rt.deps.pointer.db.end(); }
  });
}

test("portable index failure rolls back and releases its client without recording completeness", async () => {
  const { rt, deps, pool, complete } = runtime(1); pool.failErase = true;
  try {
    await expect(indexSearchScope(scope(0), deps)).rejects.toMatchObject({ code: "unavailable", detail: "sqlstate_XX" });
    expect(complete).toEqual([]); expect(pool.poolQueries).toBe(0);
    expect(pool.queries.filter(query => query.text === "ROLLBACK")).toHaveLength(1);
    expect(pool.queries.filter(query => query.text === "COMMIT")).toHaveLength(0);
    pool.failErase = false;
    expect((await indexSearchScope(scope(0), deps)).complete).toBe(true);
    expect(complete).toEqual([scope(0).orgId]);
  } finally { await rt.deps.pointer.db.end(); }
});

const document: SearchEventDocument = {
  orgId: "org", workspaceId: "ws", tsMs: 1, sessionId: "s", source: "codex", machineId: "m", kind: "history", eventId: "e",
  type: "history", userId: "u", actorName: "", actorRole: "", subagentId: "", projectId: "", taskTitle: "", taskRef: "{}", taskRefId: "",
  tags: [], filesTouched: [], toolName: "", toolStatus: "", deliveryRecordId: "", content: "content", fingerprint: "fingerprint", version: 1,
};
test("real public adapter scan/upsert/delete queries use the same transaction client; outside calls keep pool behavior", async () => {
  const { rt, deps, pool } = runtime(1); const index = deps.adapter!.index;
  try {
    await deps.transact(async tx => {
      await tx.query("SELECT transaction_marker");
      await index.scanFingerprints({ orgId: "org", workspaceId: "ws", source: "codex", sessionIds: ["s"] }, { fromMs: 0, limit: 1 });
      expect(await index.upsert([document])).toEqual({ written: 1 });
      expect(await index.deleteEvents({ orgId: "org", workspaceId: "ws" }, [document])).toEqual({ deleted: 1 });
    });
    expect(pool.poolQueries).toBe(0);
    expect(new Set(pool.queries.map(query => query.client)).size).toBe(1);
    expect(await index.health()).toEqual({ ok: true, code: "ok" });
    expect(pool.poolQueries).toBe(1); // Explicit nontransaction adapter use remains supported.
  } finally { await rt.deps.pointer.db.end(); }
});

test("nested work across two runtimes retains each runtime's transaction binding", async () => {
  const a = runtime(1), b = runtime(1);
  try {
    await a.deps.transact(async () => {
      await b.deps.transact(async () => {
        expect(await a.deps.adapter!.index.health()).toEqual({ ok: true, code: "ok" });
        expect(await b.deps.adapter!.index.health()).toEqual({ ok: true, code: "ok" });
      });
      expect(await a.deps.adapter!.index.health()).toEqual({ ok: true, code: "ok" });
    });
    expect(a.pool.poolQueries).toBe(0); expect(b.pool.poolQueries).toBe(0);
    expect(a.pool.queries.filter(query => query.text.includes("to_regclass"))).toHaveLength(2);
    expect(b.pool.queries.filter(query => query.text.includes("to_regclass"))).toHaveLength(1);
  } finally { await a.rt.deps.pointer.db.end(); await b.rt.deps.pointer.db.end(); }
});
