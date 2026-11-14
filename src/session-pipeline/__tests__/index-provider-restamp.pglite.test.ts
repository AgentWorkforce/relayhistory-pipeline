/**
 * index-provider-restamp.ts on PGlite with the real migrations (every session_index trigger installed):
 * the gate refuses anything but a GCS Locker before touching a store or database; plan writes nothing;
 * apply re-stamps exactly the 's3' rows in the Locker's bucket whose manifest the Locker holds; reruns
 * are idempotent; a row the consumer changed meanwhile is left as the consumer wrote it.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import {
  exitCodeForError,
  exitCodeForResult,
  restampArgs,
  restampDatabaseUrl,
  restampIndexProvider,
  RestampRefused,
  runRestampMain,
  type RestampDb,
} from "../index-provider-restamp.ts";
setDefaultTimeout(180_000);

const root = join(import.meta.dir, "../../..");
const read = (p: string) => readFileSync(join(root, p), "utf8");
const BUCKET = "selfhost-sessions";
const GCS_ENV = { LOCKER_PROVIDER: "gcs", LOCKER_BUCKET: BUCKET, LOCKER_ENDPOINT: "http://127.0.0.1:9", LOCKER_GCS_TOKEN_URI: "http://127.0.0.1:9/token" };
const manifest = (s: string) => `96bulls/org-a/v1/ws=ws-a/m=srv_a/h=claude/d=2026/11/01/s=${s}/manifest.json`;

let pg: PGlite;
const db: RestampDb = { query: async <R>(text: string, params?: unknown[]) => ({ rows: (await pg.query(text, params as never)).rows as R[] }) };

beforeAll(async () => {
  pg = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await pg.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await pg.exec(read(f));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await pg.exec(read("supabase/migrations/" + f));
  for (const f of readdirSync(join(root, "src/hoopdb/migrations")).filter((f) => f.endsWith(".sql")).sort()) await pg.exec(read("src/hoopdb/migrations/" + f));
});
afterAll(async () => { await pg?.close(); });

const insert = (session: string, provider: string, bucket: string, key: string = manifest(session)) =>
  pg.query(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,trace_id,revision)
    VALUES('org-a','ws-a','claude',$1,'srv_a','user-a',now(),now(),$2,$3,$4,'00000000-0000-0000-0000-000000000000',1)`, [session, provider, bucket, key]);
const providers = async () => Object.fromEntries((await pg.query<{ session_id: string; storage_provider: string }>(
  "SELECT session_id, storage_provider FROM sessions.session_index ORDER BY session_id")).rows.map((r) => [r.session_id, r.storage_provider]));
const snapshot = async () => (await pg.query("SELECT * FROM sessions.session_index ORDER BY session_id")).rows;

// In the GCS Locker: s-live-1, s-live-2, s-live-3 (and s-gcs). Not: s-gone (tombstone bytes erased).
const inLocker = new Set(["s-live-1", "s-live-2", "s-live-3", "s-gcs", "s-other", "s-fenced"].map(manifest));
const exists = async (key: string) => inLocker.has(key);

beforeEach(async () => {
  await pg.exec("DELETE FROM sessions.session_index");
  await insert("s-live-1", "s3", BUCKET);
  await insert("s-live-2", "s3", BUCKET);
  await insert("s-live-3", "s3", BUCKET);
  await insert("s-gone", "s3", BUCKET);
  await insert("s-other", "s3", "some-real-s3-bucket");
  await insert("s-gcs", "gcs", BUCKET);
});

describe("restampIndexProvider (real migrations)", () => {
  test("plan writes nothing and counts what apply would do", async () => {
    const before = await snapshot();
    const r = await restampIndexProvider(db, { bucket: BUCKET, apply: false, pageSize: 200, exists });
    expect(r).toEqual({ mode: "plan", scanned: 4, proven: 3, unproven: 1, restamped: 0, raced: 0, guard_skipped: 0, refused: 0, refused_codes: {}, other_bucket_s3: 1, complete: true, needs_attention: false });
    expect(await snapshot()).toEqual(before);
  });

  test("apply re-stamps only proven rows in the Locker's bucket, through every trigger; a rerun is a no-op", async () => {
    const before = await snapshot();
    const r = await restampIndexProvider(db, { bucket: BUCKET, apply: true, pageSize: 2, exists });
    expect(r).toMatchObject({ mode: "apply", scanned: 4, proven: 3, unproven: 1, restamped: 3, raced: 0, refused: 0, other_bucket_s3: 1, complete: true });
    expect(exitCodeForResult(r)).toBe(0);
    expect(await providers()).toEqual({ "s-gcs": "gcs", "s-gone": "s3", "s-live-1": "gcs", "s-live-2": "gcs", "s-live-3": "gcs", "s-other": "s3" });
    // Only storage_provider (and the row's updated_at) changed.
    const after = await snapshot();
    for (const [i, row] of after.entries()) {
      const was = before[i]!;
      expect({ ...row, storage_provider: was.storage_provider, updated_at: was.updated_at }).toEqual(was);
    }
    const again = await restampIndexProvider(db, { bucket: BUCKET, apply: true, pageSize: 2, exists });
    expect(again).toMatchObject({ scanned: 1, proven: 0, unproven: 1, restamped: 0, complete: true });
    expect(await snapshot()).toEqual(after);
  });

  test("--max-pages stops early and reports incomplete; resuming finishes", async () => {
    const r = await restampIndexProvider(db, { bucket: BUCKET, apply: true, pageSize: 1, maxPages: 2, exists });
    // Key order: s-gone (unproven) first, then s-live-1.
    expect(r).toMatchObject({ scanned: 2, unproven: 1, restamped: 1, complete: false });
    expect(exitCodeForResult(r)).toBe(3);
    const rest = await restampIndexProvider(db, { bucket: BUCKET, apply: true, pageSize: 1, exists });
    expect(rest).toMatchObject({ scanned: 3, unproven: 1, restamped: 2, complete: true });
  });

  test("a row the consumer re-upserted after selection is raced, not overwritten", async () => {
    const racing = async (key: string) => {
      if (key === manifest("s-live-2")) await pg.query("UPDATE sessions.session_index SET revision = 2 WHERE session_id = 's-live-2'");
      return exists(key);
    };
    const r = await restampIndexProvider(db, { bucket: BUCKET, apply: true, pageSize: 200, exists: racing });
    expect(r).toMatchObject({ restamped: 2, raced: 1, guard_skipped: 0, needs_attention: false });
    expect(exitCodeForResult(r)).toBe(0);
    expect((await providers())["s-live-2"]).toBe("s3");
  });

  test("an erase-fenced row the guard silently keeps is guard_skipped (not raced), exit 5, and stays so on rerun", async () => {
    // ingest_erase_fence_index_guard returns NULL for a live row of a fenced session: the UPDATE affects 0 rows.
    // Fences cannot be deleted, so this test fences a session no other test uses (row first: the guard drops a fenced INSERT).
    await insert("s-fenced", "s3", BUCKET);
    await pg.query("INSERT INTO sessions.session_erasure_fences(org_id,workspace_id,source,session_id) VALUES('org-a','ws-a','claude','s-fenced')");
    const r = await restampIndexProvider(db, { bucket: BUCKET, apply: true, pageSize: 200, exists });
    expect(r).toMatchObject({ restamped: 3, raced: 0, guard_skipped: 1, refused: 0, complete: true, needs_attention: true });
    expect(exitCodeForResult(r)).toBe(5);
    expect((await providers())["s-fenced"]).toBe("s3");
    const again = await restampIndexProvider(db, { bucket: BUCKET, apply: true, pageSize: 200, exists });
    expect(again).toMatchObject({ restamped: 0, raced: 0, guard_skipped: 1, needs_attention: true });
    // An incomplete run says rerun first (3), even with guard-skipped rows.
    expect(exitCodeForResult({ ...again, complete: false })).toBe(3);
  });

  test("a Locker that cannot answer fails the page before any write", async () => {
    const before = await snapshot();
    const failing = async () => { throw new Error("gcs token unavailable"); };
    await expect(restampIndexProvider(db, { bucket: BUCKET, apply: true, pageSize: 200, exists: failing })).rejects.toThrow("gcs token unavailable");
    expect(await snapshot()).toEqual(before);
  });

  test("a trigger refusal is counted by SQLSTATE and the run goes on", async () => {
    await pg.exec(`CREATE FUNCTION pg_temp.refuse_one() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN
      IF NEW.session_id = 's-live-1' THEN RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'fenced'; END IF; RETURN NEW; END $$;
      CREATE TRIGGER zz_test_refuse BEFORE UPDATE ON sessions.session_index FOR EACH ROW EXECUTE FUNCTION pg_temp.refuse_one();`);
    try {
      const r = await restampIndexProvider(db, { bucket: BUCKET, apply: true, pageSize: 200, exists });
      expect(r).toMatchObject({ restamped: 2, refused: 1, refused_codes: { "55000": 1 } });
      expect(exitCodeForResult(r)).toBe(3);
    } finally {
      await pg.exec("DROP TRIGGER zz_test_refuse ON sessions.session_index");
    }
  });
});

describe("runRestampMain gates (nothing touched unless the Locker is GCS)", () => {
  const untouched = () => {
    const touched: string[] = [];
    const guardDb: RestampDb = { query: async (text) => { touched.push(text); throw new Error("db touched"); } };
    const guardExists = async () => { touched.push("head"); throw new Error("store touched"); };
    return { touched, overrides: { db: guardDb, exists: guardExists } };
  };
  const cases: [string, NodeJS.ProcessEnv, string][] = [
    ["unset provider (S3 install)", { LOCKER_BUCKET: BUCKET }, "locker_provider_not_gcs"],
    ["s3 provider", { LOCKER_PROVIDER: "s3", LOCKER_BUCKET: BUCKET }, "locker_provider_not_gcs"],
    ["unsupported provider (no Azure Locker)", { LOCKER_PROVIDER: "azure", LOCKER_BUCKET: BUCKET }, "locker_provider_invalid"],
    ["gcs with HMAC keys", { ...GCS_ENV, LOCKER_ACCESS_KEY_ID: "k", LOCKER_SECRET_ACCESS_KEY: "s" }, "locker_selection_invalid"],
    ["gcs without a bucket", { LOCKER_PROVIDER: "gcs" }, "locker_bucket_missing"],
  ];
  for (const [name, env, code] of cases)
    test(`${name}: refused ${code}, exit 2`, async () => {
      const { touched, overrides } = untouched();
      const e = await runRestampMain(env, ["--apply"], overrides).catch((x) => x);
      expect(e).toBeInstanceOf(RestampRefused);
      expect(e.code).toBe(code);
      expect(exitCodeForError(e)).toBe(2);
      expect(touched).toEqual([]);
    });

  test("a GCS Locker runs (plan by default) against the injected ports", async () => {
    const r = await runRestampMain(GCS_ENV, [], { db, exists, log: () => {} });
    expect(r).toMatchObject({ mode: "plan", proven: 3, restamped: 0 });
  });

  test("arguments and the database URL source are strict", () => {
    expect(restampArgs([])).toEqual({ apply: false, pageSize: 200 });
    expect(restampArgs(["--apply", "--page", "50", "--max-pages", "3"])).toEqual({ apply: true, pageSize: 50, maxPages: 3 });
    for (const bad of [["--plan", "--apply"], ["--page", "0"], ["--page", "1001"], ["--page"], ["--max-pages", "0"], ["--force"]])
      expect(() => restampArgs(bad)).toThrow(RestampRefused);
    expect(() => restampDatabaseUrl({})).toThrow("database_url_missing");
    expect(() => restampDatabaseUrl({ INDEX_PROVIDER_RESTAMP_DATABASE_URL: "x", INDEX_PROVIDER_RESTAMP_DATABASE_URL_FILE: "/x" })).toThrow("secret_source_ambiguous");
    expect(() => restampDatabaseUrl({ INDEX_PROVIDER_RESTAMP_DATABASE_URL_FILE: "relative" })).toThrow("file_invalid");
  });
});
