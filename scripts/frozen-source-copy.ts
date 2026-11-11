/**
 * P1 frozen-source copy: Neon (frozen by relayhistory 0078 phase=active) -> Supabase
 * sessions.ingest_frozen_source_* (owner-only), then verify per org and seal.
 *
 * Credentials: ONE JSON document on stdin (a pipe; never a terminal, file or environment variable):
 *   {"neonOwnerUrl": "...", "supabaseOwnerUrl": "..."}   (--plan needs only neonOwnerUrl)
 * supabaseOwnerUrl is the existing direct schema-owner login (`postgres.<project>` through the pinned session
 * pooler); src/migration/frozen-source-copy-seal.ts validates it against the fixed schema-release pins, connects
 * with the pinned CA, and requires current_user = session_user = the owner. Nothing is minted, stored or logged.
 * Producer (operator shell; docs/p1-frozen-source-custody.md): `op read` the two URLs | jq -> this script.
 *
 * Usage:
 *   bun scripts/frozen-source-copy.ts --plan                   read-only: per-org Neon row counts, no writes
 *   bun scripts/frozen-source-copy.ts --copy [--org X] [--concurrency 2] [--page 5000] [--max-page-ms 4000]
 *   bun scripts/frozen-source-copy.ts --seal                   every org x dataset checkpoint complete under this
 *                                                              binding, verify every org and the privacy state, then
 *                                                              seal inside the fence (sealFence); refusal = no seal row
 *   --copy / --seal also take [--budget-ms N] (wall clock, default 6h) and [--receipt PATH] (new file, 0600):
 *   the job ends with ONE sanitized receipt (frozen-source-copy-job-v2: status COPIED | SEALED | REFUSED | ABORTED |
 *   UNKNOWN_COMMIT, progress / budget / abort / unknownCommit, next action). No credential, org id or raw key in it.
 *   bun scripts/frozen-source-copy.ts --repair-head --org O --workspace W --user U [--execute]
 *                                                              re-seed ONE stale mirror control head from Neon (dry run by default;
 *                                                              src/hoopdb/frozen-control-head-repair.ts has the refusal rules)
 *
 * Resumable: the ONLY cursor is the owner-only progress table in the target (src/migration/frozen-source-copy-progress.ts);
 * each page and its checkpoint commit in one target transaction; a resume re-proves the frozen source, the owner and
 * the exact binding (generation, source identity, epoch, privacy baseline, operator SHA). Absent table = refusal.
 * Throttled: at most --concurrency orgs read at once; a page slower than --max-page-ms backs off.
 * SIGINT/SIGTERM: stop before the next read/write, roll back what is open, receipt ABORTED; checkpoints are kept.
 * Never writes Neon. Never prints credentials.
 */
import { readFileSync, writeFileSync } from "node:fs";
import pg from "pg";
import { SUPA_PAGE, SUPA_RECEIPTS } from "../src/migration/frozen-source-copy-queries.ts";
import { CredentialRefusal, readStdinCredentials, refuseCredentialEnv } from "../src/migration/stdin-credentials.ts";
import {
  FrozenCopyRefusal, FrozenCopyUnknownCommit, type FrozenCopyMode, type OrgVerification, type OwnerConnection, RECORD_COLS as COLS, SUPA_EXTRA_ORGS,
  addDigest, emptyDigest, extraOrgRefusal, frozenCopyCredentialKeys, frozenCopyCredentials, frozenCopyOwnerClientConfig,
  neonFrozenStatus, orgParityRefusal, preRead, receiptText, requireFrozenCopyOwner, rowText, sealFence,
  DEFAULT_BUDGET_MS, type SealResult, guardedWrite, jobBudget,
} from "../src/migration/frozen-source-copy-seal.ts";
import {
  type CopyBinding, type Dataset, type Reconciliation, abortController, copyBinding, copyDataset, jobReceipt, operatorSha256, progressTracker,
  reconcileInDoubt, requireCopyComplete, requireProgressTable, requireResumeBinding, serialQueue, takeCopyOwnership,
} from "../src/migration/frozen-source-copy-progress.ts";
import { repairControlHead } from "../src/hoopdb/frozen-control-head-repair.ts";

const args = process.argv.slice(2), flag = (k: string) => args.includes(k), val = (k: string, d: string) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1]! : d; };
const mode = (flag("--plan") ? "plan" : flag("--copy") ? "copy" : flag("--seal") ? "seal" : flag("--repair-head") ? "repair-head" : "") as FrozenCopyMode | "";
if (!mode) { console.error("one of --plan | --copy | --seal | --repair-head"); process.exit(64); }
const PAGE = Number(val("--page", "5000")), CONC = Number(val("--concurrency", "2")), MAX_PAGE_MS = Number(val("--max-page-ms", "4000")), ONLY = val("--org", "");
const RECEIPT_PAGE = Math.min(PAGE, 1000);
if (![PAGE, CONC, MAX_PAGE_MS].every((v) => Number.isSafeInteger(v) && v > 0)) { console.error("--page, --concurrency and --max-page-ms must be positive integers"); process.exit(64); }
// A subset must never become the global completeness seal.
if (flag("--seal") && flag("--org")) {
  console.error("--seal requires the complete source; --org is not allowed");
  process.exit(64);
}
const BUDGET_MS = Number(val("--budget-ms", String(DEFAULT_BUDGET_MS))), RECEIPT = val("--receipt", "");
if (!Number.isSafeInteger(BUDGET_MS) || BUDGET_MS < 0 || RECEIPT.startsWith("--")) { console.error("--budget-ms must be a non-negative integer; --receipt needs a path"); process.exit(64); }
const log = (m: string, f: Record<string, unknown> = {}) => console.log(JSON.stringify({ at: new Date().toISOString(), m, ...f }));

/** Operator identity for the binding: the bytes of the operator's own source (driver + the modules it executes). */
const OPERATOR_SHA = operatorSha256(
  ["../src/migration/frozen-source-copy-seal.ts", "../src/migration/frozen-source-copy-progress.ts", "../src/migration/frozen-source-copy-queries.ts", "./frozen-source-copy.ts"]
    .map((f) => ({ name: f, bytes: readFileSync(new URL(f, import.meta.url)) })),
);

let neonOwnerUrl = "", owner: OwnerConnection | null = null;
try {
  refuseCredentialEnv();
  const keys = frozenCopyCredentialKeys(mode);
  ({ neonOwnerUrl, owner } = frozenCopyCredentials(await readStdinCredentials(keys.required, keys.allowed), mode));
} catch (e) {
  if (!(e instanceof CredentialRefusal || e instanceof FrozenCopyRefusal)) throw e;
  console.error(`credentials refused: ${e.message}`);
  process.exit(64);
}
if (!/^postgresql:\/\/neondb_owner:.+ep-floral-pond-apkvat38/.test(neonOwnerUrl)) { console.error("neonOwnerUrl (production owner) required on stdin"); process.exit(64); }

const neon = () => { const c = new pg.Client({ connectionString: neonOwnerUrl, ssl: { rejectUnauthorized: true }, application_name: "p1-frozen-copy-readonly", statement_timeout: 30000 }); return c; };
/**
 * The ONE Supabase owner connection of this run (copy keeps it for the whole job). The validated owner credential is
 * taken once and dropped from module state; the session must then be the pinned owner (no role switching anywhere).
 */
const supa = async () => {
  if (!owner) throw new FrozenCopyRefusal("frozen_copy_owner_url_refused");
  const c = new pg.Client(frozenCopyOwnerClientConfig(owner));
  owner = null;
  c.on("error", () => {});
  try { await c.connect(); await requireFrozenCopyOwner(c); return c; }
  catch (error) { await c.end().catch(() => {}); throw error; }
};

const NEON_PAGE = `SELECT r.org_id,r.workspace_id,r.origin_id,r.record_id,r.revision_id,r.revision,r.digest,r.kind,r.source,r.session_id,r.operation,r.user_id,
  a.accepted_seq AS projection_accepted_seq,k.accepted_seq AS catalog_accepted_seq
 FROM sessions.delivery_records r
 LEFT JOIN LATERAL(SELECT x.accepted_seq FROM sessions.delivery_projection_acceptance x WHERE x.org_id=r.org_id AND x.workspace_id=r.workspace_id AND x.origin_id=r.origin_id AND x.record_id=r.record_id LIMIT 1) a ON true
 LEFT JOIN LATERAL(SELECT x.accepted_seq FROM sessions.delivery_catalog_acceptance x WHERE x.org_id=r.org_id AND x.workspace_id=r.workspace_id AND x.origin_id=r.origin_id AND x.record_id=r.record_id LIMIT 1) k ON true
 WHERE r.org_id=$1 AND (r.workspace_id,r.origin_id,r.record_id) > ($2,$3,$4)
 ORDER BY r.workspace_id,r.origin_id,r.record_id LIMIT $5`;

const NEON_RECEIPTS = `SELECT org_id,workspace_id,origin_id,batch_id,digest,receipt::text AS receipt FROM sessions.delivery_receipts
 WHERE org_id=$1 AND (workspace_id,origin_id,batch_id) > ($2,$3,$4) ORDER BY workspace_id,origin_id,batch_id LIMIT $5`;

async function scanReceipts(c: pg.Client, sql: string, org: string, onPage: (rows: Record<string, unknown>[]) => Promise<void>) {
  let last = ["", "", ""];
  for (;;) {
    const t = Date.now();
    const rows = (await c.query(sql, [org, ...last, RECEIPT_PAGE])).rows;
    if (!rows.length) return;
    await onPage(rows);
    const tail = rows[rows.length - 1]!; last = [tail.workspace_id, tail.origin_id, tail.batch_id];
    const ms = Date.now() - t; if (ms > MAX_PAGE_MS) await new Promise((r) => setTimeout(r, Math.min(ms, 15000)));
    if (rows.length < RECEIPT_PAGE) return;
  }
}
async function orgs(n: pg.Client): Promise<string[]> {
  // delivery_origins (661 rows) names every origin a record can belong to; never scan the 66M-row table for this.
  return (await n.query("SELECT org_id FROM (SELECT DISTINCT org_id FROM sessions.delivery_origins) q ORDER BY org_id COLLATE \"C\"")).rows.map((r) => r.org_id as string);
}

async function scan(c: pg.Client, sql: string, org: string, onPage: (rows: Record<string, unknown>[]) => Promise<void>) {
  let last = ["", "", ""];
  for (;;) {
    const t = Date.now();
    const rows = (await c.query(sql, [org, ...last, PAGE])).rows;
    if (!rows.length) return;
    await onPage(rows);
    const tail = rows[rows.length - 1]!; last = [tail.workspace_id, tail.origin_id, tail.record_id];
    const ms = Date.now() - t; if (ms > MAX_PAGE_MS) await new Promise((r) => setTimeout(r, Math.min(ms, 15000)));
    if (rows.length < PAGE) return;
  }
}

/** Keyset read of one source page; a page slower than --max-page-ms backs off before returning. */
const throttledRead = (n: pg.Client, sql: string, org: string) => async (cursor: string[], limit: number) => {
  const t = Date.now();
  const rows = (await n.query(sql, [org, ...cursor, limit])).rows as Record<string, unknown>[];
  const ms = Date.now() - t; if (ms > MAX_PAGE_MS) await new Promise((r) => setTimeout(r, Math.min(ms, 15000)));
  return rows;
};
const INSERT_RECORDS = `INSERT INTO sessions.ingest_frozen_source_records(${COLS.join(",")})
  SELECT * FROM unnest($1::text[],$2::text[],$3::text[],$4::text[],$5::text[],$6::bigint[],$7::text[],$8::text[],$9::text[],$10::text[],$11::text[],$12::text[],$13::bigint[],$14::bigint[])
  ON CONFLICT DO NOTHING`;
const INSERT_RECEIPTS = `INSERT INTO sessions.ingest_frozen_source_receipts(org_id,workspace_id,origin_id,batch_id,digest,receipt)
  SELECT a,b,c,d,e,f::jsonb FROM unnest($1::text[],$2::text[],$3::text[],$4::text[],$5::text[],$6::text[]) AS u(a,b,c,d,e,f) ON CONFLICT DO NOTHING`;
const RECEIPT_COLS = ["org_id", "workspace_id", "origin_id", "batch_id", "digest", "receipt"];

/** One org: records then receipts, each from its own checkpoint; every page commits with its checkpoint (copyDataset). */
async function copyOrg(org: string, s: pg.Client, binding: CopyBinding, check: () => void, serialize: ReturnType<typeof serialQueue>) {
  check();
  const n = neon();
  openSource++;
  try {
    await n.connect();
    await n.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    const base = { target: s, binding, org, check, serialize, progress };
    await copyDataset({
      ...base, dataset: "records" as Dataset, pageSize: PAGE, readPage: throttledRead(n, NEON_PAGE, org),
      keyOf: (r) => [String(r.workspace_id), String(r.origin_id), String(r.record_id)],
      insert: (rows) => s.query(INSERT_RECORDS, COLS.map((k) => rows.map((r) => r[k] === null || r[k] === undefined ? null : String(r[k])))),
    });
    await copyDataset({
      ...base, dataset: "receipts" as Dataset, pageSize: RECEIPT_PAGE, readPage: throttledRead(n, NEON_RECEIPTS, org),
      keyOf: (r) => [String(r.workspace_id), String(r.origin_id), String(r.batch_id)],
      insert: (rows) => s.query(INSERT_RECEIPTS, RECEIPT_COLS.map((k) => rows.map((r) => String(r[k])))),
    });
    await n.query("ROLLBACK");
    progress.orgDone();
  } finally { await n.end().catch(() => {}); openSource--; }
}

async function repairHead() {
  const key = { org: val("--org", ""), workspace: val("--workspace", ""), user: val("--user", "") };
  if (!key.org || !key.user || !args.includes("--workspace") || [key.org, key.workspace, key.user].some((v) => v.startsWith("--"))) { console.error("--repair-head needs --org, --workspace (may be empty string) and --user"); process.exit(64); }
  const n = neon();
  try {
    await n.connect();
    const s = await supa();
    try {
      const r = await repairControlHead(n, s, key, { dryRun: !flag("--execute") });
      log("control head repair", { ...r });
    } finally { await s.end().catch(() => {}); }
  } finally { await n.end().catch(() => {}); }
}

/** Job envelope state (copy/seal): the receipt is built from it whatever way main() ends. */
const budget = jobBudget(BUDGET_MS);
const abort = abortController();
const progress = progressTracker(0);
const closed = { completedAt: null as number | null, sourceClosed: false, targetClosed: false, writesStopped: false };
const reconciliation = { result: "NOT_REQUIRED" as Reconciliation, readbackAt: null as number | null };
let openSource = 0, sealResult: SealResult | null = null;
// Only the jobs that check abort take over SIGINT/SIGTERM; --plan and --repair-head keep the default (terminate).
if (mode === "copy" || mode === "seal")
  for (const sig of ["SIGINT", "SIGTERM"] as const) process.on(sig, () => { abort.request(sig); log("abort requested", { signal: sig }); });
const check = () => { abort.check(); budget.check(); };

/** Bounded readback of an in-doubt COMMIT on the same owner session; no answer in 5s stays PENDING (the next run's resume reads it). */
async function readback(s: pg.Client, e: FrozenCopyUnknownCommit) {
  reconciliation.result = await Promise.race([
    reconcileInDoubt(s, e.context),
    new Promise<Reconciliation>((r) => setTimeout(() => r("PENDING"), 5000).unref()),
  ]);
  reconciliation.readbackAt = Date.now();
}

/** The one owner session of a copy/seal job: owner identity, progress table, exclusive ownership, binding re-proof. */
async function openJob(status: Awaited<ReturnType<typeof neonFrozenStatus>>) {
  const s = await supa();
  try {
    await requireProgressTable(s);
    await takeCopyOwnership(s);
    const binding = await copyBinding(s, status, OPERATOR_SHA);
    progress.bind(binding);
    const resumed = await requireResumeBinding(s, binding);
    log("job bound", { mode, checkpoints: resumed });
    return { s, binding };
  } catch (error) { await s.end().catch(() => {}); throw error; }
}

async function main() {
  if (mode === "repair-head") return repairHead();
  const n = neon();
  openSource++;
  try {
    await n.connect();
    await n.query("BEGIN READ ONLY");
    const status = await neonFrozenStatus(n);
    const all = ONLY ? [ONLY] : await orgs(n);
    progress.state.totalOrgs = all.length;
    if (mode === "plan") {
      const est = (await n.query("SELECT reltuples::bigint AS n FROM pg_class WHERE oid='sessions.delivery_records'::regclass")).rows[0].n;
      log("plan", { phase: status.phase, epoch: status.epoch, orgs: all.length, estimatedRecords: String(est) });
      return;
    }
    await n.query("ROLLBACK");
    check();
    const { s, binding } = await openJob(status);
    try {
      if (mode === "copy") await copyJob(n, s, binding, all);
      else await sealJob(n, s, binding, all);
    } catch (error) {
      if (error instanceof FrozenCopyUnknownCommit) await readback(s, error);
      throw error;
    } finally {
      closed.writesStopped = true;
      await s.end().catch(() => {});
      closed.targetClosed = true;
    }
  } finally {
    await n.end().catch(() => {});
    openSource--;
    closed.sourceClosed = openSource === 0;
    closed.completedAt = Date.now();
  }
}

async function copyJob(n: pg.Client, s: pg.Client, binding: CopyBinding, all: string[]) {
  // Privacy state is tiny; copy it whole, idempotently (autocommit ON CONFLICT DO NOTHING; parity is proven at seal).
  const w = { phase: "copy" as const, dataset: null, checkpointSeq: null };
  const del = (await n.query("SELECT org_id,workspace_id,user_id,origin_id,source,session_id FROM sessions.deleted_upload_sessions")).rows;
  for (const d of del) { check(); await guardedWrite(w, () => s.query("INSERT INTO sessions.ingest_frozen_source_deletions VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING", [d.org_id, d.workspace_id, d.user_id, d.origin_id, d.source, d.session_id])); }
  const rs = (await n.query("SELECT org_id,workspace_id,user_id,request_id,deleted FROM sessions.session_resets")).rows;
  for (const r of rs) { check(); await guardedWrite(w, () => s.query("INSERT INTO sessions.ingest_frozen_source_resets VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING", [r.org_id, r.workspace_id, r.user_id, r.request_id, r.deleted])); }
  // Neon control heads written before relayhistory began mirroring them: seed the mirror so it is in lockstep.
  const heads = (await n.query("SELECT org_id,workspace_id,user_id,epoch,version::text AS version,digest,kind,payload,pending,before_privacy,privacy_version::text AS privacy_version FROM sessions.delivery_cutover_control_heads")).rows;
  for (const h of heads) {
    check();
    const got = (await s.query("SELECT version::text AS version,digest,pending FROM sessions.ingest_source_control_heads WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3", [h.org_id, h.workspace_id, h.user_id])).rows[0];
    if (got && (got.version !== h.version || got.digest !== h.digest)) throw new FrozenCopyRefusal("frozen_copy_control_head_disagrees");
    if (!got) await guardedWrite(w, () => s.query("INSERT INTO sessions.ingest_source_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending,before_privacy,privacy_version) VALUES($1,$2,$3,$4,$5::bigint,$6,$7,$8,$9,$10,$11::bigint)",
      [h.org_id, h.workspace_id, h.user_id, h.epoch, h.version, h.digest, h.kind, JSON.stringify(h.payload), h.pending, h.before_privacy === null ? null : JSON.stringify(h.before_privacy), h.privacy_version]));
  }
  log("privacy copied", { deletions: del.length, resets: rs.length, controlHeads: heads.length });
  // One owner connection for the whole job; serialQueue keeps each page transaction contiguous on it while at most
  // CONC source readers run. The first failure stops every worker at its next check (before its next read/write).
  const queue = [...all]; let failure: unknown;
  const stop = () => { if (failure !== undefined) throw failure; check(); };
  const serialize = serialQueue();
  await Promise.all(Array.from({ length: CONC }, async () => {
    try {
      for (let org = queue.shift(); org; org = queue.shift()) {
        await copyOrg(org, s, binding, stop, serialize);
        log("progress", { done: progress.state.completedOrgs, of: all.length });
      }
    } catch (error) { if (failure === undefined) failure = error ?? new Error("copy_worker_failed"); }
  }));
  if (failure !== undefined) throw failure;
}

// --seal: every org x dataset checkpoint complete under THIS binding; pre-read the privacy state (it must already
// agree), verify every Neon org, then write the org seals and the completeness seal inside the fence (sealFence).
async function sealJob(n: pg.Client, s: pg.Client, binding: CopyBinding, all: string[]) {
  await requireCopyComplete(s, binding, all);
  const pre = await preRead(n, s);
  const results: OrgVerification[] = [];
  for (const org of all) {
    check();
    const a = emptyDigest(), b = emptyDigest(), ra = emptyDigest(), rb = emptyDigest();
    await n.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    try {
      await scan(n, NEON_PAGE, org, async (rows) => { for (const r of rows) addDigest(a, rowText(r)); });
      await scanReceipts(n, NEON_RECEIPTS, org, async (rows) => { for (const r of rows) addDigest(ra, receiptText(r)); });
    } finally { await n.query("ROLLBACK"); }
    await scan(s, SUPA_PAGE, org, async (rows) => { for (const r of rows) addDigest(b, rowText(r)); });
    await scanReceipts(s, SUPA_RECEIPTS, org, async (rows) => { for (const r of rows) addDigest(rb, receiptText(r)); });
    const bad = orgParityRefusal(org, { records: a, receipts: ra }, { records: b, receipts: rb });
    if (bad) throw bad;
    results.push({ org, records: a, receipts: ra });
    progress.orgDone();
  }
  log("orgs verified", { orgs: results.length });
  const extra = extraOrgRefusal((await s.query(SUPA_EXTRA_ORGS, [all])).rows[0].n);
  if (extra) throw extra;
  sealResult = await sealFence(n, s, { orgs: results, pre }, { budget: check });
  log("sealed", { ...sealResult });
}

/** The one job receipt (copy/seal): stdout always, plus a NEW 0600 file when --receipt is given. Sanitized (no org id). */
function writeReceipt(error: unknown) {
  if (mode !== "copy" && mode !== "seal") return;
  const r = jobReceipt(mode, error, {
    startedAt: budget.startedAt,
    finishedAt: Date.now(),
    budget: { maxWallMs: BUDGET_MS, deadline: budget.deadline, pageSizes: { records: PAGE, receipts: RECEIPT_PAGE }, concurrency: CONC, maxPageMs: MAX_PAGE_MS, statementTimeoutMs: 60000, lockTimeoutMs: 5000 },
    progress: progress.state,
    abort: { ...abort.state, ...closed },
    reconciliation,
  }, sealResult);
  log("receipt", { ...r });
  // Only COPIED / SEALED exit 0 (a seal "pass" without a committed manifest is REFUSED).
  if (r.status !== "COPIED" && r.status !== "SEALED") process.exitCode = 1;
  if (RECEIPT) {
    try { writeFileSync(RECEIPT, JSON.stringify(r) + "\n", { flag: "wx", mode: 0o600 }); }
    catch { log("receipt file not written", { path: RECEIPT }); process.exitCode = 1; }
  }
}
main().then(() => writeReceipt(undefined), (e) => {
  const code = e instanceof FrozenCopyRefusal || e instanceof FrozenCopyUnknownCommit ? e.code : undefined;
  // Counts and codes only: org ids and raw keys stay in the owner-only progress table.
  const { org: _org, ...detail } = (e instanceof FrozenCopyRefusal ? e.detail : {}) as Record<string, string>;
  log("STOPPED", code ? { refusal: code, ...detail } : { why: String(e?.message ?? e).slice(0, 300) });
  writeReceipt(e);
  process.exitCode = 1;
});
