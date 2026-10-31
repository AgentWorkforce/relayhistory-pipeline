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
import {
  ROLE_VALIDATION_TABLE,
  schemaVerdict,
  waitForSchema,
  type HoopDb,
} from "../../shared/db.ts";
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

const verdict = async (db: PGlite) =>
  schemaVerdict(client(db), await expectedSchema());
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
    expect(await schemaVerdict(client(db), other)).toEqual({
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
      waitForSchema(client(db) as unknown as HoopDb, await expectedSchema(), 0),
    );
    expect(error?.message).toContain("validation_pending");
  });
});
