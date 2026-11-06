// Real-PG crash/resume proof for `frozen-source-copy.ts --copy` shard + per-page resume (owned disposable PG17 only).
//   FROZEN_COPY_TEST_PG_URL=postgresql://postgres@127.0.0.1:<port>/postgres bun test src/migration/__tests__/frozen-copy-resume.postgres.test.ts
// Skipped without the variable. Refuses any URL that is not postgres@127.0.0.1 (never a real database).
// Source DB: libc C.UTF-8 (Neon's collation); target DB: ICU en-US (Supabase's). The target tables are created from the
// real migration DDL. The REAL script runs as a subprocess through fixtures/frozen-copy-pg-preload.ts, which redirects
// its pg clients to these databases and injects one fault per run.
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { createHash, randomBytes } from "node:crypto";
import { chmodSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const ADMIN = process.env.FROZEN_COPY_TEST_PG_URL ?? "";
const enabled = /^postgresql:\/\/postgres@127\.0\.0\.1:[0-9]{2,5}\/postgres$/.test(ADMIN);
const maybe = enabled ? describe : describe.skip;
const ROOT = fileURLToPath(new URL("../../../", import.meta.url));
const SCRIPT = join(ROOT, "scripts/frozen-source-copy.ts");
const PRELOAD = fileURLToPath(new URL("./fixtures/frozen-copy-pg-preload.ts", import.meta.url));
const MIGRATION = readFileSync(join(ROOT, "supabase/migrations/20261105010000_frozen_source_custody.sql"), "utf8");
const COLS = ["org_id", "workspace_id", "origin_id", "record_id", "revision_id", "revision", "digest", "kind", "source", "session_id", "operation", "user_id", "projection_accepted_seq", "catalog_accepted_seq"];
const rowText = (r: Record<string, unknown>) => JSON.stringify(COLS.map((k) => (r[k] === null || r[k] === undefined ? null : String(r[k]))));
const ORGS = ["org-big", "org-small"];               // C order: ordinal 0 = org-big, 1 = org-small
const ORG_SHA = createHash("sha256").update(ORGS.join("\n")).digest("hex");
// Keys whose C (codepoint) order differs from ICU en-US order.
const WS = ["B", "a", "_w", "Zz", "été", "b"], OR = ["o1", "O2"], RID = ["r0", "r1", "r2", "R3"];
const suffix = randomBytes(4).toString("hex");
const SRC_DB = "frozen_src_" + suffix, TGT_DB = "frozen_tgt_" + suffix;
const url = (db: string) => ADMIN.replace(/\/postgres$/, "/" + db);
const NEON_RECORD_SELECT = `SELECT r.org_id,r.workspace_id,r.origin_id,r.record_id,r.revision_id,r.revision,r.digest,r.kind,r.source,r.session_id,r.operation,r.user_id,
  (SELECT x.accepted_seq FROM sessions.delivery_projection_acceptance x WHERE x.org_id=r.org_id AND x.workspace_id=r.workspace_id AND x.origin_id=r.origin_id AND x.record_id=r.record_id LIMIT 1) AS projection_accepted_seq,
  (SELECT x.accepted_seq FROM sessions.delivery_catalog_acceptance x WHERE x.org_id=r.org_id AND x.workspace_id=r.workspace_id AND x.origin_id=r.origin_id AND x.record_id=r.record_id LIMIT 1) AS catalog_accepted_seq
 FROM sessions.delivery_records r`;

async function sql(db: string, text: string, args: unknown[] = []) {
  const c = new pg.Client({ connectionString: url(db) });
  await c.connect();
  try { return (await c.query(text, args)).rows; } finally { await c.end(); }
}

async function setup() {
  await sql("postgres", `CREATE DATABASE ${SRC_DB} TEMPLATE template0 ENCODING 'UTF8' LOCALE_PROVIDER libc LOCALE 'C.UTF-8'`);
  await sql("postgres", `CREATE DATABASE ${TGT_DB} TEMPLATE template0 ENCODING 'UTF8' LOCALE_PROVIDER icu ICU_LOCALE 'en-US' LOCALE 'C.UTF-8'`);
  await sql(SRC_DB, `CREATE SCHEMA sessions;
    CREATE TABLE sessions.delivery_records(org_id text,workspace_id text,origin_id text,record_id text,revision_id text,revision bigint,digest text,kind text,source text,session_id text,operation text,user_id text,PRIMARY KEY(org_id,workspace_id,origin_id,record_id));
    CREATE TABLE sessions.delivery_projection_acceptance(org_id text,workspace_id text,origin_id text,record_id text,accepted_seq bigint);
    CREATE TABLE sessions.delivery_catalog_acceptance(org_id text,workspace_id text,origin_id text,record_id text,accepted_seq bigint);
    CREATE TABLE sessions.delivery_receipts(org_id text,workspace_id text,origin_id text,batch_id text,digest text,receipt jsonb,PRIMARY KEY(org_id,workspace_id,origin_id,batch_id));
    CREATE TABLE sessions.delivery_origins(org_id text,origin_id text);
    CREATE TABLE sessions.deleted_upload_sessions(org_id text,workspace_id text,user_id text,origin_id text,source text,session_id text);
    CREATE TABLE sessions.session_resets(org_id text,workspace_id text,user_id text,request_id text,deleted bigint);
    CREATE TABLE sessions.delivery_cutover_control_heads(org_id text,workspace_id text,user_id text,epoch text,version bigint,digest text,kind text,payload jsonb,pending boolean,before_privacy jsonb,privacy_version bigint);
    CREATE FUNCTION sessions.delivery_cutover_status() RETURNS jsonb LANGUAGE sql AS $f$ SELECT jsonb_build_object('phase','active','epoch',repeat('e',64),'privacyBaselineSha256',repeat('a',64)) $f$;`);
  let seq = 0;
  for (const org of ORGS) {
    await sql(SRC_DB, "INSERT INTO sessions.delivery_origins VALUES($1,'o1'),($1,'O2')", [org]);
    const ws = org === "org-big" ? WS : WS.slice(0, 2);
    for (const w of ws) for (const o of OR) for (const r of RID) {
      seq++;
      await sql(SRC_DB, "INSERT INTO sessions.delivery_records VALUES($1,$2,$3,$4,$5,$6::bigint,$7,'kind','src','sess','op','user')", [org, w, o, r, "rev-" + seq, seq, "dg" + seq]);
      if (seq % 7) await sql(SRC_DB, "INSERT INTO sessions.delivery_projection_acceptance VALUES($1,$2,$3,$4,$5)", [org, w, o, r, 1000 + seq]);
      if (seq % 5) await sql(SRC_DB, "INSERT INTO sessions.delivery_catalog_acceptance VALUES($1,$2,$3,$4,$5)", [org, w, o, r, 9000 + seq]);
    }
    for (let b = 0; b < (org === "org-big" ? 12 : 2); b++)
      await sql(SRC_DB, "INSERT INTO sessions.delivery_receipts VALUES($1,$2,'o1',$3,$4,'{\"n\":1}')", [org, WS[b % WS.length], "b" + b, createHash("sha256").update(org + b).digest("hex")]);
  }
  await sql(SRC_DB, "INSERT INTO sessions.deleted_upload_sessions VALUES('org-big','B','user','o1','src','sess')");
  await sql(SRC_DB, "INSERT INTO sessions.session_resets VALUES('org-big','B','user','req',1)");
  await sql(SRC_DB, `INSERT INTO sessions.delivery_cutover_control_heads VALUES('org-big','B','user',repeat('e',64),3,repeat('d',64),'selection','{}',false,null,1)`);
  const ddl = [...MIGRATION.matchAll(/CREATE TABLE sessions\.(ingest_frozen_source_\w+|ingest_source_control_heads)\([\s\S]*?\n\);/g)].map((m) => m[0]);
  expect(ddl.length).toBe(7);
  await sql(TGT_DB, "CREATE SCHEMA sessions;\n" + ddl.join("\n") + `
    CREATE TABLE fixture_fault(sleep_ms int NOT NULL); INSERT INTO fixture_fault VALUES(0);
    CREATE FUNCTION fixture_sleep() RETURNS trigger LANGUAGE plpgsql AS $f$ BEGIN PERFORM pg_sleep((SELECT sleep_ms FROM fixture_fault)/1000.0); RETURN NULL; END $f$;
    CREATE TRIGGER fixture_sleep BEFORE INSERT ON sessions.ingest_frozen_source_records FOR EACH STATEMENT EXECUTE FUNCTION fixture_sleep();`);
}

async function resetTarget() {
  await sql(TGT_DB, "TRUNCATE sessions.ingest_frozen_source_records, sessions.ingest_frozen_source_receipts, sessions.ingest_frozen_source_deletions, sessions.ingest_frozen_source_resets, sessions.ingest_source_control_heads; UPDATE fixture_fault SET sleep_ms = 0");
}

function newResumeDir() {
  const d = mkdtempSync(join(tmpdir(), "frozen-resume-"));
  chmodSync(d, 0o700);
  return d;
}

function copy(dir: string, ordinals: string, seed: "leader" | "skip", fault = "") {
  const line = JSON.stringify({ neonOwnerUrl: "postgresql://neondb_owner:fixture-not-a-secret@ep-floral-pond-apkvat38.invalid/neondb", supabaseLoginPassword: "fixture-login-password-0000", notAfterUnix: 1999999999, planSha256: "f".repeat(64) }) + "\n";
  const p = Bun.spawnSync([process.execPath, "--no-env-file", "--preload", PRELOAD, SCRIPT, "--copy", "--concurrency", "1", "--page", "5", "--max-page-ms", "60000",
    "--orgs", ordinals, "--org-set-sha256", ORG_SHA, "--org-count", "2", "--target-login", "stdin", "--resume-dir", dir, "--seed-control", seed], {
    cwd: ROOT, stdin: Buffer.from(line), stdout: "pipe", stderr: "pipe",
    env: { PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: process.env.HOME ?? "/tmp", FROZEN_COPY_PG_SOURCE_URL: url(SRC_DB), FROZEN_COPY_PG_TARGET_URL: url(TGT_DB), FROZEN_COPY_PG_FAULT: fault },
  });
  const out = p.stdout.toString() + p.stderr.toString();
  if (!fault && p.exitCode !== 0 && process.env.FROZEN_COPY_TEST_VERBOSE === "1") console.log(out.slice(-4000));
  return { code: p.exitCode, signal: p.signalCode, out, logs: out.split("\n").flatMap((l) => { try { return [JSON.parse(l)]; } catch { return []; } }) };
}

async function sourceRows(org?: string) {
  return (await sql(SRC_DB, NEON_RECORD_SELECT + (org ? " WHERE r.org_id=$1" : ""), org ? [org] : [])).map(rowText).sort();
}
async function targetRows(org?: string) {
  return (await sql(TGT_DB, `SELECT ${COLS.join(",")} FROM sessions.ingest_frozen_source_records` + (org ? " WHERE org_id=$1" : ""), org ? [org] : [])).map(rowText).sort();
}
const receiptsText = async (db: string, table: string) => (await sql(db, `SELECT org_id,workspace_id,origin_id,batch_id,digest,receipt::text AS receipt FROM sessions.${table}`)).map((r) => JSON.stringify(Object.values(r))).sort();
const checkpoint = (dir: string) => { const f = readdirSync(dir).filter((x) => x.endsWith(".json")); return f.map((x) => ({ file: join(dir, x), cp: JSON.parse(readFileSync(join(dir, x), "utf8")) })); };
const recordCp = (dir: string, org: string) => checkpoint(dir).find((x) => x.cp.stream === "record" && x.cp.org === org)!;
const metricsFinal = (logs: any[]) => logs.filter((l) => l.m === "copy_metrics_final").at(-1) ?? {};

maybe("real PG17: shard + per-page resume (owned disposable fixture)", () => {
  beforeAll(setup, 120000);
  afterAll(async () => {
    await sql("postgres", `DROP DATABASE IF EXISTS ${SRC_DB} WITH (FORCE)`).catch(() => {});
    await sql("postgres", `DROP DATABASE IF EXISTS ${TGT_DB} WITH (FORCE)`).catch(() => {});
  });

  test("fixture really exercises C vs ICU order differences", async () => {
    const s = (await sql(SRC_DB, "SELECT DISTINCT workspace_id FROM sessions.delivery_records ORDER BY workspace_id")).map((r) => r.workspace_id);
    const t = (await sql(TGT_DB, "SELECT w FROM unnest($1::text[]) w ORDER BY w", [s])).map((r) => r.w);
    expect(s).not.toEqual(t);
    expect((await sourceRows("org-big")).length).toBe(WS.length * OR.length * RID.length);
  });

  test("pg_terminate_backend mid-page: uncommitted page rescanned from the last checkpoint; exact copy", async () => {
    await resetTarget();
    const dir = newResumeDir();
    const a = copy(dir, "0,1", "leader", "terminate_mid:4");
    expect(a.code).not.toBe(0);
    expect(a.out).not.toContain("terminate_not_observed");
    expect((await targetRows("org-big")).length).toBe(15);               // pages 1-3 committed, page 4 killed
    expect(recordCp(dir, "org-big").cp.pages).toBe(3);
    const b = copy(dir, "0,1", "leader");
    expect(b.code).toBe(0);
    expect(b.logs.some((l) => l.m === "resume" && l.stream === "record" && l.from === "checkpoint")).toBe(true);
    expect(await targetRows()).toEqual(await sourceRows());              // every value, accepted_seq included
    expect(await receiptsText(TGT_DB, "ingest_frozen_source_receipts")).toEqual(await receiptsText(SRC_DB, "delivery_receipts"));
    rmSync(dir, { recursive: true });
  }, 120000);

  test("SIGKILL after COMMIT before the checkpoint: the committed page is rescanned as conflicts, never skipped", async () => {
    await resetTarget();
    const dir = newResumeDir();
    const a = copy(dir, "0,1", "leader", "kill_after_commit:4");
    expect(a.signal).toBe("SIGKILL");
    expect((await targetRows("org-big")).length).toBe(20);               // 4 pages committed
    expect(recordCp(dir, "org-big").cp.pages).toBe(3);                  // checkpoint lags (written only after the ack)
    const b = copy(dir, "0,1", "leader");
    expect(b.code).toBe(0);
    expect(metricsFinal(b.logs).recordRowsConflicted).toBe(5);           // exactly page 4 rescanned
    expect(await targetRows()).toEqual(await sourceRows());
    rmSync(dir, { recursive: true });
  }, 120000);

  test("unknown COMMIT outcome (lost ack): no checkpoint; rescan makes either outcome safe", async () => {
    await resetTarget();
    const dir = newResumeDir();
    const a = copy(dir, "0,1", "leader", "lost_ack:4");
    expect(a.code).not.toBe(0);
    expect(recordCp(dir, "org-big").cp.pages).toBe(3);
    const b = copy(dir, "0,1", "leader");
    expect(b.code).toBe(0);
    expect(metricsFinal(b.logs).recordRowsConflicted).toBe(5);
    expect(await targetRows()).toEqual(await sourceRows());
    rmSync(dir, { recursive: true });
  }, 120000);

  test("forged complete=true on an intermediate checkpoint skips nothing (live Neon successor query)", async () => {
    await resetTarget();
    const dir = newResumeDir();
    expect(copy(dir, "0,1", "leader", "terminate_mid:4").code).not.toBe(0);
    const { file, cp } = recordCp(dir, "org-big");
    writeFileSync(file, JSON.stringify({ ...cp, complete: true }) + "\n");
    const b = copy(dir, "0,1", "leader");
    expect(b.code).toBe(0);
    expect(await targetRows()).toEqual(await sourceRows());
    rmSync(dir, { recursive: true });
  }, 120000);

  test("forged tail past committed data (valid digest, complete=true) -> tail_absent_on_target -> rescan from start", async () => {
    await resetTarget();
    const dir = newResumeDir();
    expect(copy(dir, "0,1", "leader", "terminate_mid:4").code).not.toBe(0);
    const { file, cp } = recordCp(dir, "org-big");
    const last = (await sql(SRC_DB, NEON_RECORD_SELECT + " WHERE r.org_id='org-big' ORDER BY r.workspace_id DESC,r.origin_id DESC,r.record_id DESC LIMIT 1"))[0]!;
    writeFileSync(file, JSON.stringify({ ...cp, tail: [last.workspace_id, last.origin_id, last.record_id], complete: true,
      tailRowSha256: createHash("sha256").update(rowText(last)).digest("hex") }) + "\n");
    const b = copy(dir, "0,1", "leader");
    expect(b.code).toBe(0);
    expect(b.logs.some((l) => l.m === "resume" && l.stream === "record" && l.from === "start" && l.reason === "tail_absent_on_target")).toBe(true);
    expect(await targetRows()).toEqual(await sourceRows());
    rmSync(dir, { recursive: true });
  }, 120000);

  test("follower --seed-control skip never touches privacy/control rows; leader seeds them atomically", async () => {
    await resetTarget();
    const dir = newResumeDir();
    const f = copy(dir, "1", "skip");
    expect(f.code).toBe(0);
    expect(f.logs.some((l) => l.m === "privacy skipped")).toBe(true);
    for (const t of ["ingest_frozen_source_deletions", "ingest_frozen_source_resets", "ingest_source_control_heads"])
      expect((await sql(TGT_DB, `SELECT count(*)::int AS n FROM sessions.${t}`))[0]!.n).toBe(0);
    expect(await targetRows("org-small")).toEqual(await sourceRows("org-small"));
    expect(await targetRows("org-big")).toEqual([]);
    const l = copy(dir, "0", "leader");
    expect(l.code).toBe(0);
    expect((await sql(TGT_DB, "SELECT count(*)::int AS n FROM sessions.ingest_source_control_heads"))[0]!.n).toBe(1);
    // A conflicting head already present on the target refuses (atomic insert-and-validate).
    await sql(TGT_DB, "UPDATE sessions.ingest_source_control_heads SET version = 99");
    const bad = copy(dir, "0", "leader");
    expect(bad.code).not.toBe(0);
    expect(bad.out).toContain("control head mirror disagrees with Neon");
    rmSync(dir, { recursive: true });
  }, 120000);
});
