import { expect, test } from "bun:test";
import pg from "pg";
import { readAppFromSecret } from "../lambda.ts";
import { poolTenantRunner } from "../liveness.ts";
import { partialSessionsInWorkspace, readEventPage, type Liveness } from "../page.ts";
import { createPostgresSearchComposition } from "../../shared/search-composition.ts";
import type { SearchServiceBinding } from "../../session-pipeline/search-adapter.ts";
import type { SearchIndexAdapter } from "@rhc/search-index/contracts.ts";

/** Capture the real reader assembly without connecting or loading credentials. */
async function readerOptions(url: string): Promise<pg.PoolConfig> {
  const RealPool = pg.Pool;
  let options: pg.PoolConfig | undefined;
  pg.Pool = class extends RealPool {
    constructor(config: pg.PoolConfig) { super(config); options = config; }
  } as typeof pg.Pool;
  try {
    const reader = await readAppFromSecret(JSON.stringify({ supabase_url: url }), {
      READ_API_COMPLETE_SINCE: "epoch", SEARCH_INDEX_BACKEND: "postgres",
    }, createPostgresSearchComposition({ backendId: "synthetic-postgres", fromMs: 0 }));
    await reader.close();
    if (!options) throw new Error("reader did not construct its pool");
    return options;
  } finally { pg.Pool = RealPool; }
}

function hostileOptions(url: string) {
  const value = new URL(url);
  value.searchParams.set("statement_timeout", "0");
  value.searchParams.set("query_timeout", "1");
  value.searchParams.set("options", "-c statement_timeout=0");
  value.searchParams.set("sslmode", "disable");
  return value.toString();
}

test("the actual reader pool sends a fixed SQL bound despite URL and ambient overrides", async () => {
  const previous = process.env.PGOPTIONS;
  process.env.PGOPTIONS = "-c statement_timeout=0";
  try {
    const options = await readerOptions(hostileOptions("postgres://synthetic@127.0.0.1:1/postgres"));
    const client = new pg.Client(options) as pg.Client & { getStartupConf(): Record<string, string> };
    expect(client.getStartupConf()).toMatchObject({ statement_timeout: "2000", options: "-c statement_timeout=2000" });
    expect(options).toMatchObject({ max: 2, connectionTimeoutMillis: 2000, idleTimeoutMillis: 60_000 });
    expect(options.ssl).toMatchObject({ rejectUnauthorized: true });
    expect(typeof (options.ssl as { ca: string }).ca).toBe("string");
    const parsed = new URL(options.connectionString!);
    for (const key of ["statement_timeout", "query_timeout", "options", "sslmode"]) expect(parsed.searchParams.has(key)).toBe(false);
  } finally {
    if (previous === undefined) delete process.env.PGOPTIONS;
    else process.env.PGOPTIONS = previous;
  }
});

/** Opt-in, SELECT-only proof on a newly owned local cluster. This deliberately
 * substitutes only TLS for a plaintext disposable server; it is not TLS, RDS,
 * migration, application-schema or live-service proof. No default database URL. */
const url = process.env.READ_API_TIMEOUT_TEST_PG_URL;
test.skipIf(!url)("server cancels direct and tenant SQL after the HTTP deadline and the reader pool recovers", async () => {
  const target = new URL(url!);
  if (process.env.READ_API_TIMEOUT_TEST_DISPOSABLE !== "1" || target.hostname !== "127.0.0.1" ||
      target.username !== "read_deadline_proof" || target.password || target.pathname !== "/postgres" ||
      !target.port || Number(target.port) < 1024 || target.search) throw new Error("owned disposable local PostgreSQL required");
  const previous = process.env.PGOPTIONS;
  process.env.PGOPTIONS = "-c statement_timeout=0";
  let ownedPool: pg.Pool | undefined;
  try {
    const probeUrl = new URL(hostileOptions(url!));
    // The startup test covers client query_timeout. Here a client-side timer
    // must not mask whether the server actually cancels these SQL statements.
    probeUrl.searchParams.delete("query_timeout");
    const options = await readerOptions(probeUrl.toString());
    const pool = ownedPool = new pg.Pool({ ...options, ssl: false }); // Test-only plaintext local cluster.
    const clients: pg.PoolClient[] = [];
    const timeouts: string[] = [];
    try {
      clients.push(await pool.connect()); clients.push(await pool.connect());
      for (const client of clients) {
        timeouts.push((await client.query("SHOW statement_timeout")).rows[0].statement_timeout);
        expect((await client.query("SELECT current_user AS name")).rows[0].name).toBe("read_deadline_proof");
      }
      // Hold two owned clients, independent of the statement-cancellation
      // clock, so acquisition timeout cannot race with either SQL timeout.
      const queuedAt = performance.now();
      const queued = await pool.query("SELECT 1").then(() => null, error => error as Error);
      expect(queued?.message).toMatch(/timeout/);
      expect(performance.now() - queuedAt).toBeLessThan(4000);
      expect(pool.waitingCount).toBe(0);
    } finally { clients.forEach(client => client.release()); }
    const run = poolTenantRunner(pool);
    const flights: Promise<unknown>[] = [];
    let directStarted!: () => void, tenantStarted!: () => void;
    const directReady = new Promise<void>(resolve => { directStarted = resolve; });
    const tenantReady = new Promise<void>(resolve => { tenantStarted = resolve; });
    const remember = (flight: Promise<unknown>) => {
      flights.push(flight.then(() => ({ code: null }), error => ({ code: (error as { code?: string }).code })));
      return flight;
    };
    const adapter: SearchServiceBinding = {
      index: { async query() {
        const flight = remember(pool.query("SELECT pg_sleep(8)")); directStarted();
        await flight; return { hits: [] };
      } } as unknown as SearchIndexAdapter,
      state: {
        async fromMs() { return 0; }, async record() {},
        async partials(tenant) {
          await remember(run(tenant.orgId, tenant.workspaceId, q => {
            const flight = q.query("SELECT pg_sleep(8)"); tenantStarted(); return flight;
          }));
          return { sessions: [], overflow: false };
        },
      },
    };
    const liveness: Liveness = { async erased() { return new Set(); }, async livePointerSessions() { return new Set(); }, async sessionStart() { return { pointers: 0, live: 0, createdAtMs: null }; } };
    const deps = { adapter, liveness, completeSinceMs: 0, timeoutMs: 25 };
    const started = performance.now();
    const responses = Promise.allSettled([
      readEventPage(deps, { orgId: "synthetic-org", workspaceId: "synthetic-ws", order: "asc", limit: 1 }),
      partialSessionsInWorkspace(deps, "synthetic-org", "synthetic-ws"),
    ]);
    await Promise.all([directReady, tenantReady]);
    expect(pool.totalCount).toBe(2); expect(pool.idleCount).toBe(0);
    const answers = await responses;
    expect(answers.every(r => r.status === "rejected" && String(r.reason).includes("timeout"))).toBe(true);
    expect(performance.now() - started).toBeLessThan(2000);
    const results = await Promise.all(flights);
    expect(results).toEqual([{ code: "57014" }, { code: "57014" }]);
    expect(timeouts).toEqual(["2s", "2s"]);
    expect(performance.now() - started).toBeLessThan(6500);
    expect(pool.waitingCount).toBe(0);
    expect((await pool.query("SELECT 1 AS recovered")).rows).toEqual([{ recovered: 1 }]);
    const settings = await run("synthetic-org", "synthetic-ws", q => q.query(
      "SELECT current_setting('bulls96_read.org_id') AS org, current_setting('bulls96_read.workspace_id') AS workspace, current_setting('transaction_read_only') AS read_only"));
    expect(settings.rows).toEqual([{ org: "synthetic-org", workspace: "synthetic-ws", read_only: "on" }]);
    const cleared = await pool.query("SELECT nullif(current_setting('bulls96_read.org_id',true),'') AS org, nullif(current_setting('bulls96_read.workspace_id',true),'') AS workspace");
    expect(cleared.rows).toEqual([{ org: null, workspace: null }]);
    expect((await pool.query("SHOW statement_timeout")).rows[0].statement_timeout).toBe("2s");
  } finally {
    try { await ownedPool?.end(); }
    finally {
      if (previous === undefined) delete process.env.PGOPTIONS;
      else process.env.PGOPTIONS = previous;
    }
  }
}, 20_000);
