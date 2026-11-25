import { expect, test } from "bun:test";
import pg from "pg";
import { join } from "node:path";
import { postgresPublicationRetryStore } from "../publication-retry-postgres.ts";
import { initialRetryCheckpoint, type ReceiptRetry } from "../publication-retry.ts";

// This lane creates a new disposable database, following publication.postgres.test.ts.
const url = process.env.SESSION_INGEST_TEST_PG_URL;

test.skipIf(!url)("two PostgreSQL connections contend on creation and advancement; a CAS loser cannot mutate holds", async () => {
  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  const name = "pub_retry_" + crypto.randomUUID().replaceAll("-", "");
  const databaseUrl = new URL(url!);
  databaseUrl.pathname = "/" + name;
  const first = new pg.Client({ connectionString: databaseUrl.href });
  const second = new pg.Client({ connectionString: databaseUrl.href });
  let firstConnected = false;
  let created = false;
  let pending: Promise<boolean> | undefined;
  try {
    await admin.query("CREATE DATABASE " + name);
    created = true;
    await first.connect();
    firstConnected = true;
    await second.connect();
    await first.query(await Bun.file(join(import.meta.dir,"fixtures/publication-retry-store.sql")).text());
    const backend = (await second.query<{ pid: number }>("SELECT pg_backend_pid() AS pid")).rows[0]!.pid;
    const scope = "a".repeat(64);
    const wrap = (c: pg.Client) => postgresPublicationRetryStore({ async query<T>(sql: string, params?: unknown[]) {
      return { rows: (await c.query(sql, params)).rows as T[] };
    } }, scope);
    const a = wrap(first), b = wrap(second);
    const token = "00000000-0000-4000-8000-000000000001";
    const hold: ReceiptRetry = { token, state:"audit", attempts:1, due:0 };
    for (const version of [0,1]) {
      await first.query("BEGIN");
      const next = { ...initialRetryCheckpoint(), version:version+1 };
      expect(await a.commit(scope,version,next,new Map([[token,hold]]))).toBe(true);
      pending = Promise.resolve(b.commit(scope,version,next,new Map([[token,null]])));
      const deadline = Date.now() + 5000;
      let blockedByFirst = false;
      do {
        const { rows } = await first.query<{ blocked:boolean }>(
          "SELECT pg_backend_pid() = ANY(pg_blocking_pids($1)) AS blocked", [backend]);
        blockedByFirst = rows[0]!.blocked;
        if (!blockedByFirst) await Bun.sleep(10);
      } while (!blockedByFirst && Date.now() < deadline);
      expect(blockedByFirst).toBe(true);
      await first.query("COMMIT");
      expect(await pending).toBe(false);
      pending = undefined;
      expect(await b.lookup(scope,token)).toEqual(hold);
      expect(await b.load(scope)).toEqual(next);
    }
  } finally {
    // Release the lock before awaiting any blocked second writer.
    if (firstConnected) await first.query("ROLLBACK").catch(() => {});
    await pending?.catch(() => {});
    await Promise.allSettled([first.end(),second.end()]);
    try { if (created) await admin.query("DROP DATABASE "+name); } finally { await admin.end(); }
  }
},30000);
