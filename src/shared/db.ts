/** HoopDb access: a pg Pool that satisfies the vendored SqlExecutor, plus readiness gates. */
import pg from "pg";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { log } from "./log.ts";
import { required } from "./env.ts";

export interface HoopDb extends SqlExecutor {
  pool: pg.Pool;
  end(): Promise<void>;
}

export function openHoopDb(url = required("HOOPDB_URL")): HoopDb {
  const pool = new pg.Pool({ connectionString: url, max: 8, application_name: process.env.OTEL_SERVICE_NAME ?? "96bulls" });
  pool.on("error", (err) => log.error("hoopdb pool error", { error_code: (err as { code?: string }).code ?? "pool_error" }));
  return {
    pool,
    async query<R = Record<string, unknown>>(text: string, params?: unknown[]) {
      const r = await pool.query(text, params as unknown[]);
      return { rows: r.rows as R[] };
    },
    end: () => pool.end(),
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Blocks until HoopDb answers. Services start before the database in a plain container stack. */
export async function waitForHoopDb(db: HoopDb, timeoutMs = 120_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let attempt = 0;
  for (;;) {
    try {
      await db.query("SELECT 1");
      return;
    } catch (err) {
      attempt += 1;
      if (Date.now() > deadline) throw err;
      if (attempt % 5 === 1) log.info("waiting for hoopdb", { attempt });
      await sleep(2_000);
    }
  }
}

/**
 * Blocks until the schema this build needs is present: the vendored sessions
 * migrations and every 96bulls migration recorded in `sessions.__96bulls_migrations`.
 */
export async function waitForSchema(db: HoopDb, expected: string[], timeoutMs = 300_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let attempt = 0;
  for (;;) {
    try {
      const { rows } = await db.query<{ name: string }>(
        "SELECT name FROM sessions.__96bulls_migrations",
      );
      const have = new Set(rows.map((r) => r.name));
      // The Supabase role shim is an optional prerequisite for plain Postgres, not part of the
      // schema: Supabase (and CNPG) already have the roles, so it is never required here.
      if (expected.filter((n) => !n.startsWith("shim:")).every((n) => have.has(n))) return;
    } catch {
      // table not there yet
    }
    attempt += 1;
    if (Date.now() > deadline) throw new Error("96bulls: hoopdb schema did not become ready");
    if (attempt % 5 === 1) log.info("waiting for hoopdb schema", { attempt });
    await sleep(2_000);
  }
}
