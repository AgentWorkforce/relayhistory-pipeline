/**
 * The readiness marker: HoopDb is ready for an image only when phase 2 (the Worker
 * role's exact-grant validator) passed for that image's migration-set generation.
 * Runs the real migrator (runMigrations) on in-process PGlite.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import type { AddressInfo } from "node:net";
import {
  liveRoleGate,
  ROLE_VALIDATION_TABLE,
  ROLE_VERIFY_SQL_PATH,
  schemaVerdict,
  waitForSchema,
  type HoopDb,
  type Transact,
} from "../../shared/db.ts";
import {
  gateOpen,
  serveHealth,
  type WorkerHealth,
} from "../../shared/worker.ts";
import {
  expectedSchema,
  generation,
  plan,
  runMigrations,
  type MigrationClient,
} from "../migrate.ts";

setDefaultTimeout(60_000);
const root = join(import.meta.dir, "../../..");
const WORKER = "session_directory_search_worker";
const VERIFY = readFileSync(
  join(root, "supabase/bootstrap/session_directory_search_worker_verify.sql"),
  "utf8",
);

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

/** The migrator's client over PGlite: parameterless text may hold several statements. */
function client(db: PGlite): MigrationClient {
  return {
    async query<R>(text: string, params?: unknown[]) {
      if (params?.length)
        return (await db.query<R>(text, params)) as { rows: R[] };
      const results = await db.exec(text);
      return { rows: (results.at(-1)?.rows ?? []) as R[] };
    },
  };
}

async function fresh(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  return db;
}

/** One transaction on the PGlite connection, as inTransaction does on a pooled one. */
const transact =
  (db: PGlite): Transact =>
  (work) =>
    db.transaction((tx) => work(tx as never)) as never;

/** A HoopDb over PGlite (pool.connect yields the same single connection). */
function hoopLike(db: PGlite): HoopDb {
  const c = client(db);
  return {
    ...c,
    pool: { connect: async () => ({ ...c, release() {} }) } as never,
    end: async () => {},
  } as HoopDb;
}

const verdict = async (db: PGlite) =>
  schemaVerdict(client(db), await expectedSchema(), transact(db));
const marker = async (db: PGlite) =>
  (
    await db.query<{ generation: string; status: string }>(
      `SELECT generation, status FROM ${ROLE_VALIDATION_TABLE}`,
    )
  ).rows;
const failure = async (run: Promise<unknown>) =>
  (await run.then(
    () => null,
    (caught: unknown) => caught,
  )) as { code?: string; message?: string } | null;

describe("Worker role validation marker", () => {
  test("a clean run marks PASS for this generation and readiness goes true; a no-op rerun keeps it", async () => {
    const db = await fresh();
    expect(await verdict(db)).toMatchObject({
      ok: false,
      reason: "schema_pending",
    }); // no ledger yet
    const first = await runMigrations(client(db));
    expect(first.applied).toBeGreaterThan(0);
    expect(await marker(db)).toEqual([
      { generation: await generation(), status: "pass" },
    ]);
    expect(await verdict(db)).toEqual({ ok: true });
    const again = await runMigrations(client(db));
    expect(again.applied).toBe(0);
    expect(await verdict(db)).toEqual({ ok: true });
  });

  test("after phase 2 fails, readiness stays false across retries until the drift is fixed and phase 2 passes", async () => {
    const db = await fresh();
    await runMigrations(client(db));
    await db.exec(`GRANT SELECT ON sessions.session_directory TO ${WORKER}`);
    for (let attempt = 0; attempt < 2; attempt++) {
      const error = await failure(runMigrations(client(db)));
      expect(error?.code).toBe("55000");
      expect(error?.message).toContain(
        "has privileges on table sessions.session_directory",
      );
      expect(await verdict(db)).toEqual({
        ok: false,
        reason: "validation_pending",
      });
      expect((await marker(db))[0]?.status).toBe("pending");
    }
    await db.exec(`REVOKE SELECT ON sessions.session_directory FROM ${WORKER}`);
    await runMigrations(client(db));
    expect(await verdict(db)).toEqual({ ok: true });
  });

  test("an earlier PASS can't mask a later failing validator: the next migrator invalidates it first", async () => {
    const db = await fresh();
    await runMigrations(client(db)); // process A: PASS
    expect(await verdict(db)).toEqual({ ok: true });
    await db.exec(
      `GRANT SELECT (org_id) ON sessions.session_directory TO ${WORKER}`,
    );
    // Process B, observed by a reader racing it mid-run (just before phase 2).
    let seenMidRun: unknown;
    const error = await failure(
      runMigrations(client(db), {
        beforeVerify: async () => {
          seenMidRun = await verdict(db);
        },
      }),
    );
    expect(error?.code).toBe("55000");
    expect(seenMidRun).toEqual({ ok: false, reason: "validation_pending" });
    expect(await verdict(db)).toEqual({
      ok: false,
      reason: "validation_pending",
    });
  });

  test("a stale PASS from another generation is not ready, for either side", async () => {
    const db = await fresh();
    await runMigrations(client(db));
    await db.exec(
      `UPDATE ${ROLE_VALIDATION_TABLE} SET generation = '${"0".repeat(64)}'`,
    );
    expect(await verdict(db)).toEqual({
      ok: false,
      reason: "validation_pending",
    });
    // And a reader built from a different migration set never accepts this PASS.
    await runMigrations(client(db));
    const other = { ...(await expectedSchema()), generation: "f".repeat(64) };
    expect(await schemaVerdict(client(db), other, transact(db))).toEqual({
      ok: false,
      reason: "validation_pending",
    });
    expect(await verdict(db)).toEqual({ ok: true });
  });

  test("a failure between the final migration commit and phase 2 leaves the ledger complete and readiness false", async () => {
    const db = await fresh();
    const error = await failure(
      runMigrations(client(db), {
        beforeVerify: async () => {
          throw Object.assign(new Error("injected crash"), { code: "XXINJ" });
        },
      }),
    );
    expect(error?.code).toBe("XXINJ");
    expect((await marker(db))[0]?.status).toBe("pending");
    expect(await verdict(db)).toEqual({
      ok: false,
      reason: "validation_pending",
    });
  });

  test("a migration failure mid-run leaves readiness false, even after an earlier PASS", async () => {
    const db = await fresh();
    await runMigrations(client(db));
    const steps = await plan();
    const broken = [
      ...steps.slice(0, -1),
      {
        name: "96bulls:9999_broken.sql",
        path: join(root, "supabase/tests/does-not-exist.sql"),
      },
      ...steps.slice(-1),
    ];
    expect(
      await failure(runMigrations(client(db), { steps: broken })),
    ).not.toBeNull();
    expect(await verdict(db)).toEqual({
      ok: false,
      reason: "validation_pending",
    });
  });

  test("with the in-process verifier off the migrator never marks itself ready; only an external PASS for this generation does", async () => {
    const db = await fresh();
    const steps = (await plan()).filter((s) => !s.name.startsWith("verify:"));
    await runMigrations(client(db), { steps });
    expect((await marker(db))[0]).toEqual({
      generation: await generation(),
      status: "pending",
    });
    expect(await verdict(db)).toEqual({
      ok: false,
      reason: "validation_pending",
    });
    // The external verifier's transaction (the mark_pass.sql contract, minus psql).
    await db.exec(`BEGIN; SELECT pg_advisory_xact_lock(961996, 1);
      ${VERIFY}
      UPDATE ${ROLE_VALIDATION_TABLE} SET status = 'pass', updated_at = now()
       WHERE name = '${WORKER}' AND generation = '${await generation()}' AND status = 'pending';
      COMMIT;`);
    expect(await verdict(db)).toEqual({ ok: true });
  });

  test("waitForSchema refuses to report ready while the marker is pending", async () => {
    const db = await fresh();
    const steps = (await plan()).filter((s) => !s.name.startsWith("verify:"));
    await runMigrations(client(db), { steps });
    const error = await failure(
      waitForSchema(hoopLike(db), await expectedSchema(), 0),
    );
    expect(error?.message).toContain("validation_pending");
  });
});

describe("the live gate: the marker is never the sole authority", () => {
  const DRIFTED = {
    ok: false,
    reason: "role_validation_failed",
    error_code: "55000",
  };

  test("post-PASS drift with no migrator: schemaVerdict, waitForSchema and worker /readyz all fail, and recover when fixed", async () => {
    const db = await fresh();
    await runMigrations(client(db));
    expect(await verdict(db)).toEqual({ ok: true });
    expect((await marker(db))[0]?.status).toBe("pass"); // the marker still says PASS
    await db.exec(`GRANT SELECT ON sessions.session_directory TO ${WORKER}`);
    expect(await verdict(db)).toEqual(DRIFTED);
    const waited = await failure(
      waitForSchema(hoopLike(db), await expectedSchema(), 0),
    );
    expect(waited?.message).toContain("role_validation_failed");
    // Worker /readyz, the real health server.
    const expected = await expectedSchema();
    const health: WorkerHealth = {
      ready: true,
      lastTickAt: Date.now(),
      consecutiveDbFailures: 0,
      schemaReady: async () =>
        (await schemaVerdict(client(db), expected, transact(db))).ok,
    };
    const server = serveHealth(health, 0);
    await new Promise((r) => server.once("listening", r));
    const port = (server.address() as AddressInfo).port;
    try {
      const probe = async () =>
        (await fetch(`http://127.0.0.1:${port}/readyz`)).status;
      expect(await probe()).toBe(503);
      await db.exec(
        `REVOKE SELECT ON sessions.session_directory FROM ${WORKER}`,
      );
      expect(await probe()).toBe(200);
      expect(await verdict(db)).toEqual({ ok: true });
    } finally {
      server.close();
    }
  });

  test("the worker stops claiming when drift appears mid-run, and resumes once it is fixed", async () => {
    const db = await fresh();
    await runMigrations(client(db));
    const expected = await expectedSchema();
    const health = {
      schemaReady: async () =>
        (await schemaVerdict(client(db), expected, transact(db))).ok,
    };
    let claims = 0;
    const tick = async () => {
      if (await gateOpen(health)) claims += 1; // runWorker claims only past this gate
    };
    await tick();
    await tick();
    expect(claims).toBe(2);
    await db.exec(
      `GRANT EXECUTE ON FUNCTION sessions.session_directory_worker_scope(text) TO ${WORKER}`,
    );
    await tick();
    await tick();
    expect(claims).toBe(2); // paused: nothing new claimed
    await db.exec(
      `REVOKE EXECUTE ON FUNCTION sessions.session_directory_worker_scope(text) FROM ${WORKER}`,
    );
    await tick();
    expect(claims).toBe(3);
    // An unset or throwing gate is closed.
    expect(await gateOpen({})).toBe(false);
    expect(
      await gateOpen({
        schemaReady: async () => {
          throw new Error("db down");
        },
      }),
    ).toBe(false);
  });

  test("an unreadable catalog fails closed (42501), never open", async () => {
    const db = await fresh();
    await runMigrations(client(db));
    await db.exec(`CREATE ROLE g6_runtime NOLOGIN;
      GRANT USAGE ON SCHEMA sessions TO g6_runtime;
      GRANT SELECT ON sessions.__96bulls_migrations, ${ROLE_VALIDATION_TABLE} TO g6_runtime;`);
    await db.exec("SET ROLE g6_runtime");
    expect(await verdict(db)).toEqual({ ok: true }); // the runtime role reads every catalog it needs
    await db.exec(
      "RESET ROLE; REVOKE SELECT ON pg_catalog.pg_db_role_setting FROM PUBLIC; SET ROLE g6_runtime",
    );
    try {
      expect(await verdict(db)).toEqual({
        ok: false,
        reason: "role_validation_failed",
        error_code: "42501",
      });
    } finally {
      await db.exec(
        "RESET ROLE; GRANT SELECT ON pg_catalog.pg_db_role_setting TO PUBLIC",
      );
    }
  });

  test("the gate is one READ ONLY transaction with its own statement_timeout, running the shared phase 2 file", async () => {
    const seen: string[] = [];
    const recorder: Transact = async (work) =>
      work({
        async query(text: string) {
          seen.push(text);
          return { rows: [] } as never;
        },
      } as never);
    expect(await liveRoleGate(recorder)).toEqual({ ok: true });
    expect(seen[0]).toBe("SET TRANSACTION READ ONLY");
    expect(seen[1]).toMatch(/^SET LOCAL statement_timeout = \d+$/);
    expect(seen[2]).toBe(VERIFY);
    expect(seen).toHaveLength(3);
    // One source: the migrator's verify step and mark_pass.sql use the same file.
    expect(
      (await plan()).find((st) => st.name.startsWith("verify:"))?.path,
    ).toBe(ROLE_VERIFY_SQL_PATH);
    expect(MARK_PASS).toContain(
      "\\ir session_directory_search_worker_verify.sql",
    );
  });

  // mark_pass.sql as psql runs it: \ir inlines the verifier, :'generation' is the quoted value.
  const MARK_PASS = readFileSync(
    join(
      root,
      "supabase/bootstrap/session_directory_search_worker_mark_pass.sql",
    ),
    "utf8",
  );
  const markPass = (gen: string) =>
    MARK_PASS.split("\n")
      .filter((l) => !l.startsWith("\\set "))
      .map((l) =>
        l === "\\ir session_directory_search_worker_verify.sql"
          ? VERIFY
          : l.replace(":'generation'", `'${gen.replace(/'/g, "''")}'`),
      )
      .join("\n");
  const runMarkPass = async (db: PGlite, gen: string) => {
    const error = await failure(db.exec(markPass(gen)));
    if (error) await db.exec("ROLLBACK").catch(() => undefined);
    return error;
  };

  test("mark_pass.sql, run as written: wrong generation, drift and already-pass are refused; only this generation's pending row passes", async () => {
    const db = await fresh();
    const steps = (await plan()).filter((st) => !st.name.startsWith("verify:"));
    await runMigrations(client(db), { steps });
    const gen = await generation();
    expect((await runMarkPass(db, "f".repeat(64)))?.message).toContain(
      "no pending role validation for generation",
    );
    expect((await runMarkPass(db, "not-a-digest"))?.message).toContain(
      "generation must be the 64-hex migration-set digest",
    );
    await db.exec(
      `GRANT SELECT (org_id) ON sessions.session_directory TO ${WORKER}`,
    );
    const drift = await runMarkPass(db, gen);
    expect(drift?.code).toBe("55000");
    expect(drift?.message).toContain(
      "has privileges on column org_id of table sessions.session_directory",
    );
    expect((await marker(db))[0]?.status).toBe("pending");
    await db.exec(
      `REVOKE SELECT (org_id) ON sessions.session_directory FROM ${WORKER}`,
    );
    expect(await runMarkPass(db, gen)).toBeNull();
    expect(await marker(db)).toEqual([{ generation: gen, status: "pass" }]);
    expect(await verdict(db)).toEqual({ ok: true });
    expect((await runMarkPass(db, gen))?.message).toContain(
      "no pending role validation for generation",
    );
  });
});
