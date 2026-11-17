/**
 * The summary fallback's pointer read AS bulls96_read_api (every Supabase migration plus
 * the role bootstrap, tenant-bound): it reads only the bound tenant's live rows, by
 * logical session id, and needs no new grant (the read role has none on
 * sessions.session_summaries, which it never touches).
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { manifestKey } from "../../../packages/object-store/src/index.ts";
import { poolTenantRunner } from "../liveness.ts";
import { SUMMARY_POINTERS_SQL, type SummaryPointerRow } from "../summary-fallback.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const psql = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";

let supa: PGlite;
beforeAll(async () => {
  supa = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await supa.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await supa.exec(read(f));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) await supa.exec(read(`supabase/migrations/${file}`));
  await supa.exec(psql("supabase/bootstrap/session_read_api_role.sql"));
  await supa.exec(psql("supabase/bootstrap/session_read_api_role_verify.sql"));
}, 120_000);
afterAll(async () => {
  await supa?.close();
});
const asRole = () => ({
  async connect() {
    await supa.exec("SET ROLE bulls96_read_api");
    return {
      query: (text: string, params?: unknown[]) => supa.query(text, params as unknown[]) as Promise<{ rows: unknown[] }>,
      release: () => void supa.exec("RESET ROLE"),
    };
  },
});
async function row(over: { org?: string; ws?: string; session?: string; machine?: string; deleted?: boolean; canonical?: string; manifest?: boolean } = {}) {
  const org = over.org ?? "org_1", ws = over.ws ?? "ws_1", session = over.session ?? "sess_1", machine = over.machine ?? "m1";
  const key = String(manifestKey({ prefix: `96bulls/${org}`, workspaceId: ws, machineId: machine, harness: "codex", sessionId: session, startDate: "2026-11-01" }));
  await supa.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, canonical_session_id, machine_id, user_id, created_at, last_activity_at,
        storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision, deleted_at)
     VALUES ($1, $2, 'codex', $3, $4, $5, 'user_1', now(), now(), 's3', 'bucket', $6, decode($7, 'hex'), 'ndjson', $8, 4,
             CASE WHEN $9 THEN now() END)`,
    [org, ws, session, over.canonical ?? null, machine, over.manifest === false ? null : key, "c".repeat(64), TRACE, over.deleted ?? false],
  );
}
const pointers = (org: string, ws: string, session = "sess_1") =>
  poolTenantRunner(asRole())(org, ws, async (q) => (await q.query<SummaryPointerRow>(SUMMARY_POINTERS_SQL, [org, ws, "codex", session])).rows);

describe("summary fallback pointers as the read role", () => {
  test("only the bound tenant's live rows, by logical id; tombstoned rows never", async () => {
    await supa.exec("TRUNCATE sessions.session_index CASCADE");
    await row();
    await row({ org: "org_2", machine: "foreign" });
    await row({ ws: "ws_2", machine: "other-ws" });
    await row({ session: "sess_gone", deleted: true });
    await row({ session: "phys_2", canonical: "sess_alias" });
    const live = await pointers("org_1", "ws_1");
    expect(live).toHaveLength(1);
    expect(live[0]).toMatchObject({ org_id: "org_1", workspace_id: "ws_1", harness: "codex", session_id: "sess_1", logical_session_id: "sess_1", machine_id: "m1", bucket: "bucket", content_sha256: "c".repeat(64) });
    expect(String(live[0]!.revision)).toBe("4");
    expect(await pointers("org_1", "ws_1", "sess_gone")).toEqual([]);
    expect(await pointers("org_1", "ws_1", "sess_alias")).toMatchObject([{ session_id: "phys_2", logical_session_id: "sess_alias" }]);
    // A physical id behind a canonical alias is not its own logical session.
    expect(await pointers("org_1", "ws_1", "phys_2")).toEqual([]);
    // The tenant binding: asking for another org's session under org_1's binding sees nothing.
    expect(await poolTenantRunner(asRole())("org_1", "ws_1", async (q) => (await q.query(SUMMARY_POINTERS_SQL, ["org_2", "ws_1", "codex", "sess_1"])).rows)).toEqual([]);
  }, 60_000);

  test("a second machine's row is visible (LIMIT 2), so the fallback refuses rather than picking one", async () => {
    await supa.exec("TRUNCATE sessions.session_index CASCADE");
    await row();
    await row({ machine: "m2" });
    await row({ machine: "m3" });
    expect(await pointers("org_1", "ws_1")).toHaveLength(2);
  }, 60_000);

  test("the read role has no privilege on sessions.session_summaries (the fallback never needs one)", async () => {
    // The pointer table comes from the HoopDb chain (src/hoopdb/migrations 0002, 0004), as Bob Costas creates it.
    if ((await supa.query<{ t: string | null }>("SELECT to_regclass('sessions.session_summaries')::text AS t")).rows[0]!.t === null) {
      await supa.exec(read("src/hoopdb/migrations/0002_session_ext_and_summaries.sql"));
      await supa.exec(read("src/hoopdb/migrations/0004_summary_pointers.sql"));
    }
    const r = await supa.query<{ ok: boolean }>("SELECT has_table_privilege('bulls96_read_api', 'sessions.session_summaries', 'SELECT') AS ok");
    expect(r.rows[0]!.ok).toBe(false);
  });
});
