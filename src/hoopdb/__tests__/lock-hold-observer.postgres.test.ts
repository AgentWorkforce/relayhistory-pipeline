/** Real pg_stat_activity/pg_locks proof on an owned disposable PostgreSQL server. */
import { expect, test } from "bun:test";
import pg from "pg";
import {
  collectObserverSample,
  DEFAULT_TABLES,
  observerRelationOids,
  TURNS_MIGRATION,
  TURNS_MIGRATION_SHA256,
} from "../../../scripts/pg-proof/lock-hold-observer.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
  const parsed = new URL(url);
  if (
    !["postgres:", "postgresql:"].includes(parsed.protocol) ||
    !["127.0.0.1", "localhost", "[::1]"].includes(parsed.hostname) ||
    parsed.username !== "postgres" ||
    parsed.password ||
    parsed.pathname !== "/postgres" ||
    parsed.search ||
    parsed.hash
  )
    throw new Error("owned_loopback_fixture_required");
}

const databaseUrl = (database: string) => {
  const parsed = new URL(url!);
  parsed.pathname = `/${database}`;
  return parsed.href;
};

test.skipIf(!url)(
  "observer SQL isolates the database, finds target locks and binds the exact ledger",
  async () => {
    const suffix = crypto.randomUUID().replaceAll("-", "");
    const targetName = `turns_lock_observer_${suffix}`;
    const foreignName = `turns_lock_foreign_${suffix}`;
    const admin = new pg.Client({ connectionString: url });
    const clients: pg.Client[] = [];
    const connect = async (
      database: string,
      application_name: string,
    ): Promise<pg.Client> => {
      const client = new pg.Client({
        connectionString: databaseUrl(database),
        application_name,
        query_timeout: 5_000,
      });
      client.on("error", () => {});
      await client.connect();
      clients.push(client);
      return client;
    };
    await admin.connect();
    try {
      await admin.query(`CREATE DATABASE ${targetName}`);
      const setup = await connect(targetName, "turns-lock-setup");
      await setup.query(`
        CREATE SCHEMA sessions;
        CREATE TABLE sessions.upload_deletions(id bigint);
        CREATE TABLE sessions.operator_user_erasures(id bigint);
        CREATE TABLE sessions.session_directory(id bigint);
        CREATE TABLE sessions.__96bulls_migrations(
          name text PRIMARY KEY,
          sha256 text NOT NULL
        )`);
      await setup.end();
      clients.splice(clients.indexOf(setup), 1);
      await admin.query(`CREATE DATABASE ${foreignName} TEMPLATE ${targetName}`);

      const observer = await connect(targetName, "turns-062020-lock-observer");
      const migrator = await connect(
        targetName,
        "hoopdb-migrate-062020-pgtest",
      );
      const writer = await connect(targetName, "turns-lock-writer");
      const foreign = await connect(
        foreignName,
        "hoopdb-migrate-062020-foreign",
      );
      const migratorPid = (
        await migrator.query<{ pid: number }>("SELECT pg_backend_pid() pid")
      ).rows[0]!.pid;
      const writerPid = (
        await writer.query<{ pid: number }>("SELECT pg_backend_pid() pid")
      ).rows[0]!.pid;
      const foreignPid = (
        await foreign.query<{ pid: number }>("SELECT pg_backend_pid() pid")
      ).rows[0]!.pid;
      const resolved = await observerRelationOids(observer, [...DEFAULT_TABLES]);
      expect(resolved.missing).toEqual([]);
      const foreignOids = await observerRelationOids(foreign, [
        "sessions.upload_deletions",
      ]);
      const targetUploadOid = await observerRelationOids(observer, [
        "sessions.upload_deletions",
      ]);
      expect(foreignOids.oids[0]).toBe(targetUploadOid.oids[0]);

      await migrator.query(
        "BEGIN;LOCK TABLE sessions.upload_deletions IN ACCESS EXCLUSIVE MODE",
      );
      await foreign.query(
        "BEGIN;LOCK TABLE sessions.upload_deletions IN ACCESS EXCLUSIVE MODE",
      );
      const held = await collectObserverSample(observer, resolved.oids);
      expect(held.migrationPids).toContain(migratorPid);
      expect(held.migrationPids).not.toContain(foreignPid);
      expect(held.locks).toContainEqual({
        pid: migratorPid,
        relation: "sessions.upload_deletions",
        mode: "AccessExclusiveLock",
        granted: true,
        blockingPids: [],
      });
      expect(held.locks.some((lock) => lock.pid === foreignPid)).toBe(false);

      const pendingWriter = writer
        .query("INSERT INTO sessions.upload_deletions VALUES (1)")
        .then(() => null, (error) => error);
      let waiting = await collectObserverSample(observer, resolved.oids);
      for (let attempt = 0; attempt < 100; attempt += 1) {
        if (waiting.locks.some((lock) => lock.pid === writerPid)) break;
        await Bun.sleep(10);
        waiting = await collectObserverSample(observer, resolved.oids);
      }
      expect(waiting.locks).toContainEqual({
        pid: writerPid,
        relation: "sessions.upload_deletions",
        mode: "RowExclusiveLock",
        granted: false,
        blockingPids: [migratorPid],
      });

      await migrator.query(
        "INSERT INTO sessions.__96bulls_migrations(name,sha256) VALUES($1,$2)",
        [TURNS_MIGRATION, TURNS_MIGRATION_SHA256],
      );
      await migrator.query("COMMIT");
      expect(await pendingWriter).toBeNull();
      const complete = await collectObserverSample(observer, resolved.oids);
      expect(complete.ledgerSha256).toBe(TURNS_MIGRATION_SHA256);
      expect(complete.migrationPids).not.toContain(migratorPid);
      await foreign.query("ROLLBACK");
    } finally {
      for (const client of clients.splice(0).reverse())
        await client.end().catch(() => {});
      await admin
        .query(`DROP DATABASE IF EXISTS ${foreignName} WITH (FORCE)`)
        .catch(() => {});
      await admin
        .query(`DROP DATABASE IF EXISTS ${targetName} WITH (FORCE)`)
        .catch(() => {});
      await admin.end();
    }
  },
  60_000,
);
