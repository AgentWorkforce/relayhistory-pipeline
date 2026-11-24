/**
 * HOOPDB_STATEMENT_TIMEOUT_MS, HOOPDB_LOCK_TIMEOUT_MS, HOOPDB_ACQUIRE_TIMEOUT_MS: the env grammar,
 * the main pool's options, the startup parameters node-pg derives from them, pg-pool's acquire
 * timeout (on a fake client), PGOPTIONS still refused, and that the SET LOCAL values of
 * transaction(), the scope lock and the publication transaction still win over a connection
 * default (PGlite; a session-level SET stands in for the startup parameter there, since PGlite has
 * no wire startup).
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import { PGlite } from "@electric-sql/pglite";
import pg from "pg";
import { MS_KNOB_MAX, MS_KNOB_MIN, dbTimeoutsFromEnv, lambdaEnv, supabasePool, supabasePoolOptions, transaction } from "../lambda.ts";
import type { HoopDb } from "../../shared/db.ts";
import type { postgresTlsOptions } from "../../shared/postgres-tls.ts";

const base = {
  AWS_REGION: "us-east-1",
  PIPELINE_RAW_S3_BUCKET: "b",
  PIPELINE_KEY_PREFIX: "/96bulls/",
  PIPELINE_ORG_FROM_PREFIX: "true",
  SESSION_PIPELINE_SECRET_ARN: "arn",
  SESSION_PIPELINE_QUEUE_URL: "https://sqs",
};
const URL_ = "postgres://u:p@db.invalid:5432/postgres";
const TLS = { ca: "test-ca", rejectUnauthorized: true } as ReturnType<typeof postgresTlsOptions>;
const VARS = ["HOOPDB_STATEMENT_TIMEOUT_MS", "HOOPDB_LOCK_TIMEOUT_MS", "HOOPDB_ACQUIRE_TIMEOUT_MS"] as const;
const json = (v: unknown) => JSON.parse(JSON.stringify(v));

describe("HOOPDB_*_TIMEOUT_MS env", () => {
  test("unset or empty: the env is exactly as without them", () => {
    const plain = lambdaEnv(base);
    expect(Object.keys(plain)).not.toContain("dbTimeouts");
    const empty = lambdaEnv({ ...base, HOOPDB_STATEMENT_TIMEOUT_MS: "", HOOPDB_LOCK_TIMEOUT_MS: "", HOOPDB_ACQUIRE_TIMEOUT_MS: "" });
    expect(Object.keys(empty)).toEqual(Object.keys(plain));
    expect(json({ ...empty, adapterEnv: undefined })).toEqual(json({ ...plain, adapterEnv: undefined }));
    expect(dbTimeoutsFromEnv({})).toBeUndefined();
  });

  test("each is independent; set ones appear, unset ones do not; the key is last", () => {
    expect([MS_KNOB_MIN, MS_KNOB_MAX]).toEqual([1_000, 299_000]);
    expect(dbTimeoutsFromEnv({ HOOPDB_STATEMENT_TIMEOUT_MS: "20000" })).toEqual({ statementTimeoutMs: 20_000 });
    expect(dbTimeoutsFromEnv({ HOOPDB_LOCK_TIMEOUT_MS: "1000" })).toEqual({ lockTimeoutMs: 1_000 });
    expect(dbTimeoutsFromEnv({ HOOPDB_ACQUIRE_TIMEOUT_MS: "299000" })).toEqual({ acquireTimeoutMs: 299_000 });
    const plain = lambdaEnv(base);
    const on = lambdaEnv({ ...base, HOOPDB_STATEMENT_TIMEOUT_MS: "20000", HOOPDB_LOCK_TIMEOUT_MS: "5000", HOOPDB_ACQUIRE_TIMEOUT_MS: "10000" });
    expect(on.dbTimeouts).toEqual({ statementTimeoutMs: 20_000, lockTimeoutMs: 5_000, acquireTimeoutMs: 10_000 });
    expect(Object.keys(on)).toEqual([...Object.keys(plain), "dbTimeouts"]);
  });

  test("refuses negative, NaN, fractional, too small, too large and padded values, naming the variable but not the value", () => {
    for (const name of VARS)
      for (const v of ["-1", "0", "999", "NaN", "abc", "1.5", "1e4", "299001", "300000", " 2000", "+2000", "02000"]) {
        let message = "";
        try {
          lambdaEnv({ ...base, [name]: v });
        } catch (e) {
          message = (e as Error).message;
        }
        expect([name, v, message]).toEqual([name, v, `session pipeline: ${name} must be an integer from 1000 to 299000`]);
      }
  });
});

describe("supabasePoolOptions", () => {
  test("no timeouts: exactly the base options, tls passed through as given", () => {
    const off = supabasePoolOptions(URL_, 4, TLS);
    expect(Object.keys(off).sort()).toEqual(["application_name", "connectionString", "idleTimeoutMillis", "max", "ssl"]);
    expect(off.ssl).toBe(TLS);
    expect(supabasePoolOptions(URL_, 4, TLS, {})).toEqual(off);
  });

  test("each timeout adds exactly its own option; none sets `options`", () => {
    const off = supabasePoolOptions(URL_, 4, TLS);
    expect(supabasePoolOptions(URL_, 4, TLS, { statementTimeoutMs: 20_000 })).toEqual({ ...off, statement_timeout: 20_000 });
    expect(supabasePoolOptions(URL_, 4, TLS, { lockTimeoutMs: 5_000 })).toEqual({ ...off, lock_timeout: 5_000 });
    expect(supabasePoolOptions(URL_, 4, TLS, { acquireTimeoutMs: 10_000 })).toEqual({ ...off, connectionTimeoutMillis: 10_000 });
    const all = supabasePoolOptions(URL_, 4, TLS, { statementTimeoutMs: 20_000, lockTimeoutMs: 5_000, acquireTimeoutMs: 10_000 });
    expect(all.options).toBeUndefined();
  });

  test("node-pg sends statement_timeout and lock_timeout as startup parameters only when set (getStartupConf, no connection)", () => {
    const conf = (o: pg.PoolConfig) => (new pg.Client(o) as unknown as { getStartupConf(): Record<string, string> }).getStartupConf();
    const off = conf(supabasePoolOptions(URL_, 4, TLS));
    expect(Object.keys(off)).not.toContain("statement_timeout");
    expect(Object.keys(off)).not.toContain("lock_timeout");
    expect(Object.keys(off)).not.toContain("options");
    const on = conf(supabasePoolOptions(URL_, 4, TLS, { statementTimeoutMs: 20_000, lockTimeoutMs: 5_000 }));
    expect(on).toEqual({ ...off, statement_timeout: "20000", lock_timeout: "5000" });
  });

  test("supabasePool builds its pool from these options without connecting, and still refuses PGOPTIONS", async () => {
    const saved = process.env.PGOPTIONS;
    try {
      delete process.env.PGOPTIONS;
      const db = supabasePool(URL_, 4, TLS, { acquireTimeoutMs: 10_000 });
      const options = (db.pool as unknown as { options: pg.PoolConfig }).options;
      expect(options.connectionTimeoutMillis).toBe(10_000);
      expect(options.statement_timeout).toBeUndefined();
      expect(options.options).toBeUndefined();
      await db.end();
      const plain = supabasePool(URL_, 4, TLS);
      expect((plain.pool as unknown as { options: pg.PoolConfig }).options.connectionTimeoutMillis).toBeUndefined();
      await plain.end();
      process.env.PGOPTIONS = "-c statement_timeout=1";
      expect(() => supabasePool(URL_, 4, TLS, { statementTimeoutMs: 20_000 })).toThrow("PGOPTIONS");
    } finally {
      if (saved === undefined) delete process.env.PGOPTIONS;
      else process.env.PGOPTIONS = saved;
    }
  });
});

/** Stands in for pg.Client inside a real pg-pool: connects at once, never touches a network. */
class FakeClient extends EventEmitter {
  connect(cb: (err?: Error) => void) {
    setImmediate(() => cb());
  }
  end(cb?: () => void) {
    this.emit("end");
    cb?.();
    return Promise.resolve();
  }
}

describe("acquire timeout (pg-pool connectionTimeoutMillis, fake client)", () => {
  const pool = (acquireTimeoutMs?: number) =>
    new pg.Pool({ ...supabasePoolOptions(URL_, 1, TLS, acquireTimeoutMs === undefined ? {} : { acquireTimeoutMs }), Client: FakeClient as unknown as typeof pg.Client });

  test("set: a checkout waiting on a full pool fails after it", async () => {
    const p = pool(1_000);
    const first = await p.connect();
    const started = Date.now();
    const error = await p.connect().catch((e: unknown) => e as Error);
    expect((error as Error).message).toBe("timeout exceeded when trying to connect");
    expect(Date.now() - started).toBeGreaterThanOrEqual(900);
    first.release();
    await p.end();
  });

  test("unset: the same checkout keeps waiting and gets the client once released", async () => {
    const p = pool();
    const first = await p.connect();
    let settled = false;
    const second = p.connect().finally(() => {
      settled = true;
    });
    await new Promise((r) => setTimeout(r, 1_200));
    expect(settled).toBe(false);
    first.release();
    (await second).release();
    await p.end();
  });
});

describe("SET LOCAL values against connection defaults (PGlite)", () => {
  let lite: PGlite;
  let db: HoopDb;
  beforeAll(async () => {
    lite = new PGlite();
    // The session-level defaults a startup parameter would give the connection.
    await lite.query("SET statement_timeout = '7s'");
    await lite.query("SET lock_timeout = '3s'");
    const client = Object.assign(new EventEmitter(), {
      query: (text: string, params?: unknown[]) => (params === undefined && text.includes(";") ? lite.exec(text).then((r) => r.at(-1)!) : lite.query(text, params)),
      release: () => {},
    });
    db = { pool: { connect: async () => client } } as unknown as HoopDb;
  });
  afterAll(async () => {
    await lite.close();
  });

  const show = async (tx: { query<R>(t: string): Promise<{ rows: R[] }> }) => {
    const s = (await tx.query<{ statement_timeout: string }>("SHOW statement_timeout")).rows[0]!.statement_timeout;
    const l = (await tx.query<{ lock_timeout: string }>("SHOW lock_timeout")).rows[0]!.lock_timeout;
    return [s, l];
  };

  test("transaction(): its own 60 s statement_timeout; lock_timeout is the connection default; both back after", async () => {
    expect(await transaction(db, (tx) => show(tx))).toEqual(["1min", "3s"]);
    expect(await show(lite)).toEqual(["7s", "3s"]);
  });

  test("the scope lock (lambda.ts lock): 60 s statement, its own 30 s lock_timeout", async () => {
    const inLock = await transaction(db, async (tx) => {
      await tx.query("SET LOCAL lock_timeout = '30s'");
      return show(tx);
    });
    expect(inLock).toEqual(["1min", "30s"]);
  });

  test("the publication transaction (lambda.ts publication.transact): its own 5 s / 1 s", async () => {
    const inPub = await transaction(db, async (tx) => {
      await tx.query("SET LOCAL statement_timeout='5s';SET LOCAL lock_timeout='1s'");
      return show(tx);
    });
    expect(inPub).toEqual(["5s", "1s"]);
    expect(await show(lite)).toEqual(["7s", "3s"]);
  });
});
