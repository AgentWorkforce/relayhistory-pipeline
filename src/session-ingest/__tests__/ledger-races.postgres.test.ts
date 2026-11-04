/**
 * Actual PostgreSQL + filesystem-object race controls. Opt in with an owned,
 * synthetic SESSION_INGEST_TEST_PG_URL; never a deployed database. Each run creates
 * its own database. No locks are held across object IO by the implementation.
 */
import { afterAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { type ObjectStore, objectKey, readAll } from "../../shared/vendor.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliverBatch } from "../deliver.ts";
import { fanout, type FanoutMessage } from "../staging.ts";
import { batch, record, TENANT } from "./support.ts";
import type { Ledger } from "../ledger.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
const migration =
  "supabase/migrations/20261104030000_ingest_fence_ownership.sql";
const root = join(import.meta.dir, "../../..");
const sql = (path: string) => readFileSync(join(root, path), "utf8");
const cleanup: (() => Promise<void>)[] = [];
const base = mkdtempSync(join(tmpdir(), "ingest-pg-races-"));
// Disposal only: five physical DROP DATABASE calls can exceed 5s under load.
afterAll(async () => {
  for (const f of cleanup.reverse()) await f();
  rmSync(base, { recursive: true, force: true });
}, 30000);

function barrier() {
  let reach!: () => void, release!: () => void;
  return {
    reached: new Promise<void>((r) => {
      reach = r;
    }),
    resumed: new Promise<void>((r) => {
      release = r;
    }),
    reach: () => reach(),
    release: () => release(),
  };
}
async function database(repair = true) {
  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  const name = "ingest_race_" + crypto.randomUUID().replaceAll("-", "");
  await admin.query("CREATE DATABASE " + name);
  const target = new URL(url!);
  target.pathname = "/" + name;
  const pool = new pg.Pool({ connectionString: target.href });
  const checked = new Set<pg.PoolClient>();
  cleanup.push(async () => {
    for (const c of checked) {
      await c.query("ROLLBACK");
      c.release();
    }
    await pool.end();
    await admin.query("DROP DATABASE " + name);
    await admin.end();
  });
  await pool.query("CREATE SCHEMA sessions");
  await pool.query(sql("supabase/tests/supabase_shim.sql"));
  await pool.query(sql("supabase/bootstrap/session_ingest_roles.sql"));
  await pool.query(
    sql("supabase/migrations/20261102010000_session_ingest_ledger.sql"),
  );
  if (repair) await apply(pool);
  const client = async () => {
    const c = await pool.connect();
    checked.add(c);
    const release = c.release.bind(c);
    c.release = (destroy?: boolean) => {
      checked.delete(c);
      release(destroy);
    };
    await c.query("SET ROLE bulls96_session_ingest");
    const ledger = sqlLedger(async (t, p) => {
      const out = await c.query(t, p);
      return { rows: out.rows };
    });
    return { c, ledger, pid: c.processID! };
  };
  return { pool, client };
}
async function apply(pool: pg.Pool) {
  let text = sql(migration);
  // Discriminating controls run the same fixture against one removed fence.
  if (process.env.INGEST_RACE_MUTATION === "no_serialization")
    text = text.replaceAll(
      /  PERFORM sessions\.ingest_lock_fences\([^\n]+\);\n/g,
      "",
    );
  if (process.env.INGEST_RACE_MUTATION === "no_pending_ownership")
    text = text.replace(
      /    UNION\n    SELECT o\.origin_id,o\.source,o\.session_id FROM sessions\.ingest_session_ownership AS o\n     WHERE o\.org_id=p_org AND o\.workspace_id=p_ws AND o\.user_id=p_user/,
      "",
    );
  if (process.env.INGEST_RACE_MUTATION === "no_commit_filter")
    text = text.replace(
      /   WHERE i\.session_id IS NULL OR NOT EXISTS \(SELECT 1 FROM sessions\.upload_deletions AS d\n     WHERE d\.org_id=k\.org_id AND d\.workspace_id=k\.workspace_id AND d\.user_id=k\.user_id\n       AND d\.origin_id=k\.origin_id AND d\.source=i\.source AND d\.session_id=i\.session_id\)/,
      "",
    );
  await pool.query(text);
}
function storage() {
  const dir = mkdtempSync(join(base, "objects-"));
  const store = createFsStore({ root: dir }) as unknown as ObjectStore;
  const target = { store, keyRoot: "96bulls", sleep: async () => {} };
  return { store, target };
}
async function keys(store: ObjectStore) {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey("96bulls"), cursor);
    out.push(...page.items.map((i) => String(i.key)));
    cursor = page.cursor;
  } while (cursor);
  return out.sort();
}
async function count(pool: pg.Pool, table: string) {
  return (await pool.query("SELECT count(*)::int n FROM sessions." + table))
    .rows[0].n;
}
async function waitForLock(pool: pg.Pool, pid: number, settled: () => boolean) {
  for (let i = 0; i < 100; i++) {
    if (settled()) return false;
    const r = await pool.query(
      "SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1",
      [pid],
    );
    if (r.rows[0]?.wait_event_type === "Lock") return true;
    await new Promise((r) => setTimeout(r, 5));
  }
  return false;
}
const selection = {
  tenant: TENANT,
  originId: "origin-1",
  sessions: [["claude", "sess-1"]] as [string, string][],
  enabled: false,
};
describe.skipIf(!url)("actual PG upload fence interleavings", () => {
  test("disable open during direct commit: revision stays absent and re-enable actually restores object bytes", async () => {
    const d = await database();
    const a = await d.client(),
      b = await d.client();
    const s = storage(),
      gate = barrier();
    const wrapped: Ledger = {
      ...b.ledger,
      async commit(p) {
        gate.reach();
        await gate.resumed;
        return b.ledger.commit(p);
      },
    };
    let settled = false;
    const upload = deliverBatch(
      { ledger: wrapped, target: s.target },
      TENANT,
      await batch([record({ record_id: "late-direct" })]),
    ).finally(() => {
      settled = true;
    });
    await gate.reached;
    expect((await keys(s.store)).some((k) => k.endsWith("manifest.json"))).toBe(
      true,
    );
    await a.c.query("BEGIN");
    await a.ledger.setSessionUploads(selection);
    gate.release();
    const blocked = await waitForLock(d.pool, b.pid, () => settled);
    await a.c.query("COMMIT");
    const first = await upload;
    expect(blocked).toBe(true);
    expect(first.stats.rearmed).toBe(1);
    expect(await count(d.pool, "ingest_record_revisions")).toBe(0);
    // Represent the completed erasure, including its old manifest and pointer.
    for (const key of await keys(s.store)) await s.store.delete(objectKey(key));
    await a.ledger.setSessionUploads({ ...selection, enabled: true });
    const fresh = await deliverBatch(
      { ledger: b.ledger, target: s.target },
      TENANT,
      await batch([record({ record_id: "late-direct" })], {
        batch_id: "fresh-after-erasure",
      }),
    );
    const restored = await keys(s.store);
    expect(fresh.outcome).toBe("accepted");
    expect(fresh.stats.manifests_written).toBe(1);
    const chunks = restored.filter((k) => k.endsWith(".ndjson.zst"));
    expect(chunks).toHaveLength(1);
    const bytes = await readAll(
      (await s.store.getObject(objectKey(chunks[0]!))).body,
    );
    expect(new TextDecoder().decode(zstdDecompressSync(bytes))).toContain(
      "late-direct",
    );
    a.c.release();
    b.c.release();
  }, 15000);
  for (const inflight of [false, true])
    test(
      "reset reaches acknowledged staged sessions " +
        (inflight
          ? "after fanout fence / before object write"
          : "before fanout"),
      async () => {
        const d = await database();
        const a = await d.client(),
          b = await d.client();
        const s = storage(),
          gate = barrier();
        const messages: FanoutMessage[] = [];
        const records = [
          record({ record_id: "stage-one" }),
          record({ record_id: "stage-two", session_id: "sess-2" }),
        ];
        const receipt = await deliverBatch(
          {
            ledger: b.ledger,
            target: s.target,
            inlineMaxSessions: 0,
            fanout: async (m) => {
              messages.push(...m);
            },
          },
          TENANT,
          await batch(records),
        );
        expect(receipt.outcome).toBe("accepted_staged");
        expect(messages).toHaveLength(1);
        expect(await count(d.pool, "ingest_record_revisions")).toBe(0);
        expect(
          (await keys(s.store)).some((k) => k.endsWith(".staged.ndjson.zst")),
        ).toBe(true);
        let copy: ReturnType<typeof fanout> | undefined;
        if (inflight) {
          const wrapped = {
            ...b.ledger,
            async fanoutFence(...p: Parameters<Ledger["fanoutFence"]>) {
              const out = await b.ledger.fanoutFence(...p);
              gate.reach();
              await gate.resumed;
              return out;
            },
          };
          copy = fanout(
            s.target,
            messages[0]!,
            (v) => new Uint8Array(zstdDecompressSync(v)),
            wrapped,
          );
          await gate.reached;
        }
        // Reset completes while object IO is paused; no long SQL lock crosses the pause.
        const deleted = await a.ledger.resetUploadedSessions(
          TENANT,
          "reset-stage",
        );
        if (inflight) gate.release();
        const result = await (copy ??
          fanout(
            s.target,
            messages[0]!,
            (v) => new Uint8Array(zstdDecompressSync(v)),
            b.ledger,
          ));
        expect(deleted).toBe(2);
        expect(
          await a.ledger.resetUploadedSessions(TENANT, "reset-stage"),
        ).toBe(2);
        expect(await count(d.pool, "upload_deletions")).toBe(2);
        expect(await count(d.pool, "ingest_record_revisions")).toBe(0);
        expect(result.skipped).toBe(inflight ? 0 : 2);
        expect(result.revisions.rearmed).toBe(inflight ? 2 : 0);
        expect(
          (await keys(s.store)).filter((k) => k.endsWith("manifest.json")),
        ).toHaveLength(inflight ? 2 : 0);
        expect(await count(d.pool, "upload_erasure_rearm")).toBe(
          inflight ? 2 : 0,
        );
        a.c.release();
        b.c.release();
      },
      15000,
    );
  test("reset held open serializes fanout commit; unrelated user and workspace remain independent", async () => {
    const d = await database();
    const a = await d.client(),
      b = await d.client(),
      other = await d.client(),
      workspace = await d.client();
    const r = {
      source: "claude",
      sessionId: "sess-1",
      recordId: "r",
      revisionId: "r:1",
      revision: 1,
      digest: "a".repeat(64),
    };
    await b.ledger.claim({
      tenant: TENANT,
      originId: "origin-1",
      batchId: "b",
      batchDigest: "b".repeat(64),
      receiptId: "rhr_" + "c".repeat(64),
      sessions: selection.sessions,
      records: [r],
    });
    await a.c.query("BEGIN");
    await a.ledger.resetUploadedSessions(TENANT, "held-reset");
    let settled = false;
    const commit = b.ledger
      .commitRevisions(TENANT, "origin-1", "srv_" + "a".repeat(32), [r])
      .finally(() => {
        settled = true;
      });
    const blocked = await waitForLock(d.pool, b.pid, () => settled);
    const parallel = await Promise.all([
      other.ledger.resetUploadedSessions(
        { ...TENANT, userId: "different-user" },
        "other-user",
      ),
      // Different connection because concurrent SQL on one connection queues.
      workspace.ledger.resetUploadedSessions(
        { ...TENANT, workspaceId: "different-ws" },
        "other-ws",
      ),
    ]);
    await a.c.query("COMMIT");
    const result = await commit;
    expect(blocked).toBe(true);
    expect(parallel).toEqual([0, 0]);
    expect(result).toEqual({ committed: 0, rearmed: 1 });
    expect(await count(d.pool, "ingest_record_revisions")).toBe(0);
    a.c.release();
    b.c.release();
    other.c.release();
    workspace.c.release();
  }, 15000);
  test("append upgrade preserves existing revisions and makes them reset-visible; helper/table access stays closed", async () => {
    const d = await database(false),
      a = await d.client();
    const r = {
      source: "claude",
      sessionId: "old-session",
      recordId: "old-r",
      revisionId: "old-r:1",
      revision: 1,
      digest: "a".repeat(64),
    };
    const claim = {
      tenant: TENANT,
      originId: "origin-1",
      batchId: "old-b",
      batchDigest: "b".repeat(64),
      receiptId: "rhr_" + "c".repeat(64),
      sessions: [["claude", "old-session"]] as [string, string][],
      records: [r],
    };
    await a.ledger.claim(claim);
    const { receiptId, ...commit } = claim;
    await a.ledger.commit({ ...commit, machineId: "srv_" + "a".repeat(32) });
    // Reproduce an already-corrupted old ledger, rather than only a clean install.
    const bad = {
      ...claim,
      batchId: "old-late",
      sessions: [["claude", "already-erased"]] as [string, string][],
      records: [{ ...r, recordId: "erased-r", sessionId: "already-erased" }],
    };
    await a.ledger.claim(bad);
    await a.ledger.setSessionUploads({
      tenant: TENANT,
      originId: "origin-1",
      sessions: bad.sessions,
      enabled: false,
    });
    const { receiptId: badReceipt, ...badCommit } = bad;
    await a.ledger.commit({ ...badCommit, machineId: "srv_" + "a".repeat(32) });
    expect(await count(d.pool, "ingest_record_revisions")).toBe(2);
    await apply(d.pool);
    expect(await count(d.pool, "ingest_record_revisions")).toBe(1);
    expect(await count(d.pool, "ingest_session_ownership")).toBe(1);
    await expect(
      a.c.query("SELECT sessions.ingest_lock_fences($1,$2,$3)", [
        TENANT.orgId,
        TENANT.workspaceId,
        TENANT.userId,
      ]),
    ).rejects.toMatchObject({ code: "42501" });
    await expect(
      a.c.query("SELECT * FROM sessions.ingest_session_ownership"),
    ).rejects.toMatchObject({ code: "42501" });
    expect(await a.ledger.resetUploadedSessions(TENANT, "upgrade-reset")).toBe(
      1,
    );
    expect(await count(d.pool, "ingest_record_revisions")).toBe(0);
    a.c.release();
  }, 15000);
});
