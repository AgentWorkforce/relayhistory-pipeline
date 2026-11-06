// Real-PG fixture transport for frozen-copy-resume.postgres.test.ts. It redirects the copy's two pg clients to an
// OWNED, disposable loopback PostgreSQL 17 (two databases: source C.UTF-8, target ICU en-US) and injects at most one
// fault. Never a real database: both URLs must be postgres@127.0.0.1. No CLI (execFileSync refused), no network fetch.
import pg from "pg";
import { createRequire, syncBuiltinESMExports } from "node:module";
const cp = createRequire(import.meta.url)("node:child_process");
cp.execFileSync = () => { throw new Error("cli_refused_in_pg_fixture"); };
syncBuiltinESMExports();
globalThis.fetch = (async () => { throw new Error("network_refused_in_pg_fixture"); }) as unknown as typeof fetch;
const SRC = process.env.FROZEN_COPY_PG_SOURCE_URL ?? "", TGT = process.env.FROZEN_COPY_PG_TARGET_URL ?? "";
for (const u of [SRC, TGT]) if (!/^postgresql:\/\/postgres@127\.0\.0\.1:[0-9]{2,5}\/[a-z0-9_]{1,40}$/.test(u)) throw new Error("fixture_url_refused");
const [kind, nRaw] = (process.env.FROZEN_COPY_PG_FAULT ?? "").split(":");
const N = Number(nRaw || 0);
let inserts = 0;
const Base = pg.Client;
class FixtureClient extends Base {
  isTarget: boolean;
  constructor(cfg: any) {
    const isTarget = !!cfg?.host;
    super({ connectionString: isTarget ? TGT : SRC, application_name: isTarget ? "fixture-target" : "fixture-source" });
    this.isTarget = isTarget;
    this.on("error", () => {}); // a terminated backend must surface on the query, not as an unhandled emitter error
  }
  async query(sql: any, args?: any): Promise<any> {
    const isInsert = this.isTarget && typeof sql === "string" && sql.includes("INSERT INTO sessions.ingest_frozen_source_records");
    if (!isInsert) return (super.query as any)(sql, args);
    inserts++;
    if (kind === "terminate_mid" && inserts === N) {
      // Real server-side kill mid-statement: a fixture trigger sleeps inside the INSERT; an admin session terminates it.
      const admin = new Base({ connectionString: TGT });
      await admin.connect();
      await admin.query("UPDATE fixture_fault SET sleep_ms = 5000");
      const killer = (async () => {
        let done = false;
        for (let i = 0; i < 200 && !done; i++) {
          const r = await admin.query("SELECT pg_terminate_backend(pid) AS t FROM pg_stat_activity WHERE application_name = 'fixture-target' AND state = 'active' AND query LIKE 'INSERT INTO sessions.ingest_frozen_source_records%' AND wait_event = 'PgSleep'");
          done = r.rows.some((x: any) => x.t === true);
          if (!done) await new Promise((res) => setTimeout(res, 25));
        }
        await admin.query("UPDATE fixture_fault SET sleep_ms = 0");
        await admin.end();
        if (!done) console.log(JSON.stringify({ fixture: "terminate_not_observed" }));
      })();
      try { return await (super.query as any)(sql, args); } finally { await killer; }
    }
    const r = await (super.query as any)(sql, args);
    // The INSERT has COMMITTED (autocommit) when we get here: model a crash or a lost COMMIT ack before the checkpoint.
    if (kind === "kill_after_commit" && inserts === N) process.kill(process.pid, "SIGKILL");
    if (kind === "lost_ack" && inserts === N) throw Object.assign(new Error("fixture_lost_commit_ack"), { code: "08006" });
    return r;
  }
}
(pg as any).Client = FixtureClient;
