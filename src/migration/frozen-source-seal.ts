/**
 * Frozen-source seal engine: bounded parallel verification, a read-only seal-leg timer and
 * per-org plan counts for scripts/frozen-source-copy.ts.
 *
 * The serial `--seal` in the script is left untouched. This module re-implements the same
 * verification (same queries, same canonical row text, same order-independent digest, same
 * global privacy/control/policy checks, same seal rows and manifest) and adds:
 *  - a bounded worker pool (each worker owns one read-only Neon connection; the Supabase owner
 *    connection is shared and pg-queued, exactly like the copy job);
 *  - a source-stability fence. Under relayhistory 0078 phase=active every legacy write is refused
 *    unless the transaction holds a control permit, and the only permit writers
 *    (set_session_uploads_cutover_v1 / reset_uploaded_sessions_cutover_v1) write data only when
 *    begin_delivery_cutover_control_v1 inserted or advanced (version+1) a control head in the
 *    same transaction. Control-head versions only grow and heads are never deleted, so an equal
 *    {cutover status, every control head} read before and after a window proves no legacy source
 *    row committed in that window. The seal reads that fence before any verification, after each
 *    org, and again inside the seal transaction before COMMIT, and refuses on any difference.
 *    Trust boundary: as for the serial seal, the Neon owner (who can disable triggers) is trusted.
 *
 * Nothing here samples, skips, or reuses a digest computed outside the fenced window.
 * Output from this module never contains tenant identifiers: orgs are reported by ordinal in the
 * C-ordered org list that the script derives from sessions.delivery_origins.
 */
import { createHash } from "node:crypto";
import { SUPA_PAGE, SUPA_RECEIPTS, SUPA_CONTROL_HEADS } from "./frozen-source-copy-queries.ts";

export interface Queryable { query(sql: string, params?: unknown[]): Promise<{ rows: any[] }>; }
export interface SourceConnection extends Queryable { end(): Promise<void>; }
export type Digest = { n: bigint; sum: bigint };

// ---- Byte-identical to scripts/frozen-source-copy.ts (asserted by frozen-source-seal.test.ts). ----
export const COLS = ["org_id", "workspace_id", "origin_id", "record_id", "revision_id", "revision", "digest", "kind", "source", "session_id", "operation", "user_id", "projection_accepted_seq", "catalog_accepted_seq"] as const;
export const rowText = (r: Record<string, unknown>) => JSON.stringify(COLS.map((k) => r[k] === null || r[k] === undefined ? null : String(r[k])));
export const receiptText = (r: Record<string, unknown>) => JSON.stringify([r.org_id, r.workspace_id, r.origin_id, r.batch_id, r.digest, r.receipt]);
const MOD = 1n << 64n;
export const addDigest = (acc: { n: bigint; sum: bigint }, text: string) => { acc.n += 1n; acc.sum = (acc.sum + BigInt("0x" + createHash("sha256").update(text).digest("hex").slice(0, 16))) % MOD; };

export const NEON_PAGE = `SELECT r.org_id,r.workspace_id,r.origin_id,r.record_id,r.revision_id,r.revision,r.digest,r.kind,r.source,r.session_id,r.operation,r.user_id,
  a.accepted_seq AS projection_accepted_seq,k.accepted_seq AS catalog_accepted_seq
 FROM sessions.delivery_records r
 LEFT JOIN LATERAL(SELECT x.accepted_seq FROM sessions.delivery_projection_acceptance x WHERE x.org_id=r.org_id AND x.workspace_id=r.workspace_id AND x.origin_id=r.origin_id AND x.record_id=r.record_id LIMIT 1) a ON true
 LEFT JOIN LATERAL(SELECT x.accepted_seq FROM sessions.delivery_catalog_acceptance x WHERE x.org_id=r.org_id AND x.workspace_id=r.workspace_id AND x.origin_id=r.origin_id AND x.record_id=r.record_id LIMIT 1) k ON true
 WHERE r.org_id=$1 AND (r.workspace_id,r.origin_id,r.record_id) > ($2,$3,$4)
 ORDER BY r.workspace_id,r.origin_id,r.record_id LIMIT $5`;

export const NEON_RECEIPTS = `SELECT org_id,workspace_id,origin_id,batch_id,digest,receipt::text AS receipt FROM sessions.delivery_receipts
 WHERE org_id=$1 AND (workspace_id,origin_id,batch_id) > ($2,$3,$4) ORDER BY workspace_id,origin_id,batch_id LIMIT $5`;
export const NEON_DELETIONS = "SELECT org_id,workspace_id,user_id,origin_id,source,session_id FROM sessions.deleted_upload_sessions";
export const SUPA_DELETIONS = "SELECT org_id,workspace_id,user_id,origin_id,source,session_id FROM sessions.ingest_frozen_source_deletions";
export const NEON_HEADS = "SELECT org_id,workspace_id,user_id,version::text v,digest,pending FROM sessions.delivery_cutover_control_heads ORDER BY 1,2,3";
// ---- end of byte-identical section ----

// Stricter than the serial seal: it also verifies the copied resets (the frozen witness reads them).
export const NEON_RESETS = "SELECT org_id,workspace_id,user_id,request_id,deleted::text AS deleted FROM sessions.session_resets";
export const SUPA_RESETS = "SELECT org_id,workspace_id,user_id,request_id,deleted::text AS deleted FROM sessions.ingest_frozen_source_resets";
// Source-stability fence: one REPEATABLE READ READ ONLY snapshot of the cutover status and every control head.
export const NEON_FENCE_STATUS = "SELECT sessions.delivery_cutover_status() AS s";
export const NEON_FENCE_HEADS = "SELECT org_id,workspace_id,user_id,epoch,version::text AS v,digest,pending FROM sessions.delivery_cutover_control_heads";
// The fence's premise: the 0078 statement and row guards exist, are enabled and run the 0078 guard
// function on every table the seal reads. Any change (disable, drop, replaced body) moves the fence.
export const GUARDED_TABLES = ["delivery_catalog_acceptance", "delivery_origins", "delivery_projection_acceptance", "delivery_receipts", "delivery_records", "deleted_upload_sessions", "session_resets"] as const;
export const NEON_FENCE_GUARDS = `SELECT c.relname AS t,t.tgname AS g,t.tgenabled::text AS e,p.proname AS f,md5(p.prosrc) AS h
 FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid
 WHERE n.nspname='sessions' AND t.tgname IN ('delivery_cutover_statement','delivery_cutover_row') AND NOT t.tgisinternal`;
export const NEON_ORGS = "SELECT org_id FROM (SELECT DISTINCT org_id FROM sessions.delivery_origins) q ORDER BY org_id COLLATE \"C\"";
// Per-org count chunk for --plan: one row (chunk size + last key), never the chunk itself.
export const NEON_COUNT_RECORDS = `SELECT count(*) OVER () AS n,workspace_id,origin_id,record_id FROM (
 SELECT workspace_id,origin_id,record_id FROM sessions.delivery_records WHERE org_id=$1 AND (workspace_id,origin_id,record_id) > ($2,$3,$4)
 ORDER BY workspace_id,origin_id,record_id LIMIT $5) q ORDER BY workspace_id DESC,origin_id DESC,record_id DESC LIMIT 1`;
export const NEON_COUNT_RECEIPTS = `SELECT count(*) OVER () AS n,workspace_id,origin_id,batch_id FROM (
 SELECT workspace_id,origin_id,batch_id FROM sessions.delivery_receipts WHERE org_id=$1 AND (workspace_id,origin_id,batch_id) > ($2,$3,$4)
 ORDER BY workspace_id,origin_id,batch_id LIMIT $5) q ORDER BY workspace_id DESC,origin_id DESC,batch_id DESC LIMIT 1`;

export const READ_ONLY_BEGIN = "BEGIN READ ONLY";
export const FENCE_BEGIN = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY";

export interface PageStat { rows: number; ms: number; backoffMs: number }
export interface ScanOptions {
  page: number; maxPageMs: number;
  /** Stop between pages (timer bounds only; the seal never passes it). */
  stop?: () => boolean;
  observe?: (s: PageStat) => void;
  sleep?: (ms: number) => Promise<void>;
}
const defaultSleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** The script's scan()/scanReceipts(), parameterized. Returns true when the keyset was exhausted. */
export async function scanKeyset(c: Queryable, sql: string, org: string, kind: "records" | "receipts", o: ScanOptions,
  onPage: (rows: Record<string, unknown>[]) => void | Promise<void>): Promise<boolean> {
  const size = kind === "records" ? o.page : Math.min(o.page, 1000), tailKey = kind === "records" ? "record_id" : "batch_id";
  let last: unknown[] = ["", "", ""];
  for (;;) {
    if (o.stop?.()) return false;
    const t = Date.now();
    const rows = (await c.query(sql, [org, ...last, size])).rows;
    if (!rows.length) { o.observe?.({ rows: 0, ms: Date.now() - t, backoffMs: 0 }); return true; }
    await onPage(rows);
    const tail = rows[rows.length - 1]!; last = [tail.workspace_id, tail.origin_id, tail[tailKey]];
    const ms = Date.now() - t; let backoffMs = 0;
    if (ms > o.maxPageMs) { backoffMs = Math.min(ms, 15000); await (o.sleep ?? defaultSleep)(backoffMs); }
    o.observe?.({ rows: rows.length, ms, backoffMs });
    if (rows.length < size) return true;
  }
}

export class SealRefusal extends Error {
  constructor(readonly code: string, detail = "") { super(detail ? `${code}: ${detail}` : code); this.name = "SealRefusal"; }
}

const canon = (v: unknown): string => {
  if (v === null || typeof v !== "object") return JSON.stringify(v);
  if (Array.isArray(v)) return "[" + v.map(canon).join(",") + "]";
  return "{" + Object.keys(v as object).sort().map((k) => JSON.stringify(k) + ":" + canon((v as any)[k])).join(",") + "}";
};

export interface Fence { status: string; heads: Map<string, string>; all: string; guards: string; guardsOk: boolean }
/** One snapshot of the cutover status and every control head, grouped per org. */
export async function readFence(c: Queryable): Promise<Fence> {
  await c.query(FENCE_BEGIN);
  try {
    const s = (await c.query(NEON_FENCE_STATUS)).rows[0]?.s;
    const rows = (await c.query(NEON_FENCE_HEADS)).rows;
    const guardRows = (await c.query(NEON_FENCE_GUARDS)).rows;
    await c.query("ROLLBACK");
    const guardList = guardRows.map((g) => [g.t, g.g, g.e, g.f, g.h].join(":")).sort();
    const guardsOk = GUARDED_TABLES.every((t) => ["delivery_cutover_statement", "delivery_cutover_row"].every((g) =>
      guardRows.some((r) => r.t === t && r.g === g && (r.e === "O" || r.e === "A") && r.f === "guard_delivery_cutover_v1")));
    const per = new Map<string, string[]>();
    for (const r of rows) {
      const org = String(r.org_id);
      const list = per.get(org) ?? []; per.set(org, list);
      list.push(JSON.stringify([r.workspace_id, r.user_id, r.epoch, String(r.v), r.digest, r.pending === true]));
    }
    const heads = new Map<string, string>();
    for (const [org, list] of per) heads.set(org, "[" + list.sort().join(",") + "]");
    const all = [...heads.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([o, h]) => JSON.stringify(o) + ":" + h).join(";");
    return { status: canon(s ?? null), heads, all, guards: guardList.join(","), guardsOk };
  } catch (error) {
    await c.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

export interface OrgVerification { records: Digest; receipts: Digest }
/** Exactly the serial per-org body: Neon leg (READ ONLY), Supabase leg, compare; then receipts. */
export async function verifyOrg(src: Queryable, tgt: Queryable, org: string, ordinal: number, o: ScanOptions,
  between?: () => void | Promise<void>): Promise<OrgVerification> {
  // An interrupted scan is never a verification, even if the partial digests happen to agree.
  const whole = async (p: Promise<boolean>) => { if (!(await p)) throw new SealRefusal("verification_interrupted", `org #${ordinal}`); };
  const a: Digest = { n: 0n, sum: 0n }, b: Digest = { n: 0n, sum: 0n };
  await src.query(READ_ONLY_BEGIN);
  await whole(scanKeyset(src, NEON_PAGE, org, "records", o, (rows) => { for (const r of rows) addDigest(a, rowText(r)); }));
  await src.query("ROLLBACK");
  await between?.();
  await whole(scanKeyset(tgt, SUPA_PAGE, org, "records", o, (rows) => { for (const r of rows) addDigest(b, rowText(r)); }));
  if (a.n !== b.n || a.sum !== b.sum) throw new SealRefusal("org_record_mismatch", `org #${ordinal} neon rows ${a.n} supabase rows ${b.n}`);
  const ra: Digest = { n: 0n, sum: 0n }, rb: Digest = { n: 0n, sum: 0n };
  await src.query(READ_ONLY_BEGIN);
  await whole(scanKeyset(src, NEON_RECEIPTS, org, "receipts", o, (rows) => { for (const r of rows) addDigest(ra, receiptText(r)); }));
  await src.query("ROLLBACK");
  await whole(scanKeyset(tgt, SUPA_RECEIPTS, org, "receipts", o, (rows) => { for (const r of rows) addDigest(rb, receiptText(r)); }));
  if (ra.n !== rb.n || ra.sum !== rb.sum) throw new SealRefusal("org_receipt_mismatch", `org #${ordinal} neon receipts ${ra.n} supabase receipts ${rb.n}`);
  return { records: a, receipts: ra };
}

export type SealEvent =
  | { kind: "fence-opened"; orgs: number; heads: number }
  | { kind: "org-neon-leg-done"; ordinal: number }
  | { kind: "org-verified"; ordinal: number; done: number; of: number; rows: string }
  | { kind: "before-commit" };

export interface SealDeps {
  /** Neon owner connection, autocommit (no open transaction). Used for orgs, privacy, heads and the fence. */
  main: Queryable;
  /** Supabase owner connection. Shared by every worker (pg queues statements), as in the copy job. */
  target: Queryable;
  /** Opens one more read-only Neon connection; one per worker. */
  openSource: () => Promise<SourceConnection>;
  /** The C-ordered org list from sessions.delivery_origins, read in the same transaction as `status`. */
  orgs: string[];
  /** sessions.delivery_cutover_status() as read at start (phase must be active). */
  status: Record<string, any>;
  workers: number; page: number; maxPageMs: number;
  onEvent?: (e: SealEvent) => void | Promise<void>;
  sleep?: (ms: number) => Promise<void>;
}
export interface SealResult { orgs: number; recordRows: string; manifestSha: string; privacyRows: string }

export const MAX_WORKERS = 8;
export function parseWorkers(raw: string): number {
  if (!/^[1-9][0-9]?$/.test(raw)) throw new SealRefusal("invalid_workers", "--workers must be an integer 1-" + MAX_WORKERS);
  const n = Number(raw);
  if (n > MAX_WORKERS) throw new SealRefusal("invalid_workers", "--workers must be an integer 1-" + MAX_WORKERS);
  return n;
}

/** Bounded parallel verification with the serial seal's attestation, plus the source-stability fence. */
export async function sealParallel(d: SealDeps): Promise<SealResult> {
  const workers = d.workers, all = d.orgs;
  if (!Number.isInteger(workers) || workers < 1 || workers > MAX_WORKERS) throw new SealRefusal("invalid_workers");
  if (new Set(all).size !== all.length) throw new SealRefusal("duplicate_orgs");
  if (d.status?.phase !== "active") throw new SealRefusal("source_not_frozen");
  const opts: ScanOptions = { page: d.page, maxPageMs: d.maxPageMs, sleep: d.sleep };
  const statusStr = canon(d.status);

  // T0: fence before any verification. The org list must still be the one `status` was read with.
  const f0 = await readFence(d.main);
  if (!f0.guardsOk) throw new SealRefusal("source_not_frozen", "relayhistory 0078 guards are not all present and enabled");
  if (f0.status !== statusStr) throw new SealRefusal("source_changed", "cutover status changed before verification");
  const orgsNow = (await d.main.query(NEON_ORGS)).rows.map((r) => String(r.org_id));
  if (JSON.stringify(orgsNow) !== JSON.stringify(all)) throw new SealRefusal("source_changed", "org set changed before verification");
  await d.onEvent?.({ kind: "fence-opened", orgs: all.length, heads: f0.heads.size });

  const results: (OrgVerification | undefined)[] = new Array(all.length).fill(undefined);
  let next = 0, done = 0, failure: unknown;
  const check = () => { if (failure !== undefined) throw failure; };
  await Promise.all(Array.from({ length: Math.min(workers, Math.max(1, all.length)) }, async () => {
    let src: SourceConnection | undefined;
    try {
      check();
      src = await d.openSource();
      for (let i = next++; i < all.length; i = next++) {
        check();
        const org = all[i]!;
        // The org's whole verification window lies after T0; an equal fence after it proves its
        // source did not change since T0 (and so not during the window).
        const fenced = async () => {
          const f = await readFence(src!);
          if (f.status !== statusStr) throw new SealRefusal("source_changed", "cutover status changed during verification");
          if (f.guards !== f0.guards) throw new SealRefusal("source_changed", "freeze guards changed during verification");
          if ((f.heads.get(org) ?? "") !== (f0.heads.get(org) ?? "")) throw new SealRefusal("source_changed", `org #${i} control head changed during verification`);
        };
        let r: OrgVerification;
        try {
          r = await verifyOrg(src, d.target, org, i, { ...opts, stop: () => failure !== undefined },
            async () => { check(); await d.onEvent?.({ kind: "org-neon-leg-done", ordinal: i }); check(); });
        } catch (error) {
          // A mismatch caused by a concurrent source change is reported as that change.
          if (error instanceof SealRefusal && /^org_(record|receipt)_mismatch$/.test(error.code)) await fenced();
          throw error;
        }
        check();
        await fenced();
        results[i] = r; done++;
        await d.onEvent?.({ kind: "org-verified", ordinal: i, done, of: all.length, rows: String(r.records.n) });
      }
    } catch (error) {
      if (failure === undefined) failure = error ?? new Error("seal_worker_failed");
    } finally {
      if (src) await src.end().catch(() => {});
    }
  }));
  if (failure !== undefined) throw failure;
  // Completeness: every org exactly once, in the serial order.
  if (done !== all.length || results.some((r) => r === undefined)) throw new SealRefusal("incomplete_verification");

  // The serial seal's global checks, unchanged.
  const extra = (await d.target.query("SELECT count(DISTINCT org_id)::int AS n FROM sessions.ingest_frozen_source_records WHERE NOT (org_id = ANY($1))", [all])).rows[0].n;
  if (extra) throw new SealRefusal("extra_target_orgs", `${extra} orgs in Supabase copy are not in Neon`);
  const priv: Digest = { n: 0n, sum: 0n };
  for (const r of (await d.main.query(NEON_DELETIONS)).rows) addDigest(priv, JSON.stringify(Object.values(r)));
  const privS: Digest = { n: 0n, sum: 0n };
  for (const r of (await d.target.query(SUPA_DELETIONS)).rows) addDigest(privS, JSON.stringify(Object.values(r)));
  if (priv.n !== privS.n || priv.sum !== privS.sum) throw new SealRefusal("privacy_copy_mismatch");
  const rs: Digest = { n: 0n, sum: 0n }, rsS: Digest = { n: 0n, sum: 0n };
  for (const r of (await d.main.query(NEON_RESETS)).rows) addDigest(rs, JSON.stringify(Object.values(r)));
  for (const r of (await d.target.query(SUPA_RESETS)).rows) addDigest(rsS, JSON.stringify(Object.values(r)));
  if (rs.n !== rsS.n || rs.sum !== rsS.sum) throw new SealRefusal("reset_copy_mismatch");
  const nh = (await d.main.query(NEON_HEADS)).rows, sh = (await d.target.query(SUPA_CONTROL_HEADS)).rows;
  const key = (r: any) => [r.org_id, r.workspace_id, r.user_id, r.v, r.digest, r.pending].join("|");
  if (JSON.stringify(nh.map(key).sort()) !== JSON.stringify(sh.map(key).sort())) throw new SealRefusal("control_head_mirror_differs");
  const pol = (await d.target.query("SELECT source_identity_sha256,control_epoch,privacy_baseline_sha256 FROM sessions.ingest_lazy_policy WHERE singleton")).rows[0];
  if (!pol || pol.control_epoch !== d.status.epoch || pol.privacy_baseline_sha256 !== d.status.privacyBaselineSha256) throw new SealRefusal("policy_mismatch");

  const manifest = all.map((org, i) => `${org}\t${results[i]!.records.n}\t${results[i]!.records.sum}`).join("\n") + `\nprivacy\t${priv.n}\t${priv.sum}\n`;
  const manifestSha = createHash("sha256").update(manifest).digest("hex");
  const recordRows = results.reduce((a, r) => a + r!.records.n, 0n);
  await d.target.query("BEGIN");
  try {
    for (let i = 0; i < all.length; i++) await d.target.query("INSERT INTO sessions.ingest_frozen_source_org_seals(org_id,record_rows,record_sum,privacy_rows,privacy_sum) VALUES($1,$2,$3,0,0) ON CONFLICT (org_id) DO UPDATE SET record_rows=EXCLUDED.record_rows,record_sum=EXCLUDED.record_sum", [all[i], String(results[i]!.records.n), String(results[i]!.records.sum)]);
    await d.target.query(`INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot)
        VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (singleton) DO NOTHING`,
      [pol.source_identity_sha256, pol.control_epoch, pol.privacy_baseline_sha256, all.length, String(recordRows), manifestSha, JSON.stringify({ cutover: d.status, sealedFrom: "neon-frozen", privacyRows: String(priv.n) })]);
    await d.onEvent?.({ kind: "before-commit" });
    // T1: the whole source (status, every head, the org set) must equal T0 at the last moment.
    const f1 = await readFence(d.main);
    const orgsEnd = (await d.main.query(NEON_ORGS)).rows.map((r) => String(r.org_id));
    if (f1.status !== statusStr || f1.all !== f0.all || f1.guards !== f0.guards || JSON.stringify(orgsEnd) !== JSON.stringify(all)) throw new SealRefusal("source_changed", "source changed before the seal committed");
    await d.target.query("COMMIT");
  } catch (error) {
    await d.target.query("ROLLBACK").catch(() => {});
    throw error;
  }
  return { orgs: all.length, recordRows: String(recordRows), manifestSha, privacyRows: String(priv.n) };
}

// ------------------------------------------------------------------ plan counts

export interface PlanCounts { perOrg: { ordinal: number; records: number; receipts: number }[]; summary: Record<string, unknown> }
/** Exact per-org counts by keyset chunks (no statement scans more than `chunk` index entries). */
export async function planCounts(c: Queryable, orgs: string[], chunk = 50000, onOrg?: (o: { ordinal: number; records: number; receipts: number }) => void): Promise<PlanCounts> {
  const count = async (sql: string, org: string, tailKey: string) => {
    let total = 0, last: unknown[] = ["", "", ""];
    for (;;) {
      const r = (await c.query(sql, [org, ...last, chunk])).rows[0];
      if (!r) return total;
      const n = Number(r.n); total += n; last = [r.workspace_id, r.origin_id, r[tailKey]];
      if (n < chunk) return total;
    }
  };
  const perOrg: PlanCounts["perOrg"] = [];
  for (let i = 0; i < orgs.length; i++) {
    const o = { ordinal: i, records: await count(NEON_COUNT_RECORDS, orgs[i]!, "record_id"), receipts: await count(NEON_COUNT_RECEIPTS, orgs[i]!, "batch_id") };
    perOrg.push(o); onOrg?.(o);
  }
  const recs = perOrg.map((o) => o.records).sort((a, b) => b - a), total = recs.reduce((a, b) => a + b, 0);
  const share = (k: number) => total ? Number((recs.slice(0, k).reduce((a, b) => a + b, 0) / total).toFixed(4)) : 0;
  return { perOrg, summary: { orgs: orgs.length, records: total, receipts: perOrg.reduce((a, o) => a + o.receipts, 0), maxOrgRecords: recs[0] ?? 0, top1Share: share(1), top5Share: share(5), emptyOrgs: perOrg.filter((o) => !o.records && !o.receipts).length } };
}

// ------------------------------------------------------------------ seal-leg timer

export type Leg = "neonRecords" | "supabaseRecords" | "neonReceipts" | "supabaseReceipts";
const LEGS: Leg[] = ["neonRecords", "supabaseRecords", "neonReceipts", "supabaseReceipts"];
export interface TimerDeps {
  target: Queryable;           // Supabase owner connection; the timer makes it read-only first
  openSource: () => Promise<SourceConnection>;
  orgs: string[];              // already-bounded subset, in order
  workers: number; page: number; maxPageMs: number;
  rowCap?: number;             // per leg, across workers
  maxSeconds: number;          // wall-clock bound, checked between pages
  now?: () => number; sleep?: (ms: number) => Promise<void>;
}
export interface LegReport { rows: number; pages: number; busyMs: number; queryMs: number; backoffMs: number; rowsPerBusySecond: number | null; rowsPerWallSecond: number | null; pageMsP50: number | null; pageMsP95: number | null }
export interface TimerReport { sealFencePremise: "ok" | "missing" | "unchecked"; legs: Record<Leg, LegReport>; orgsAttempted: number; orgsFullyScanned: number; orgsDigestEqual: number; orgsDigestUnequal: number; stoppedBy: "complete" | "row-cap" | "max-seconds"; wallMs: number; workers: number }

const pct = (xs: number[], p: number) => { if (!xs.length) return null; const s = [...xs].sort((a, b) => a - b); return s[Math.min(s.length - 1, Math.ceil(p * s.length) - 1)]!; };

/** Ensures every statement on the connection runs in a READ ONLY transaction. */
export async function makeReadOnly(c: Queryable) {
  await c.query("SET default_transaction_read_only = on");
  const ro = (await c.query("SELECT current_setting('transaction_read_only') AS ro")).rows[0]?.ro;
  if (ro !== "on") throw new SealRefusal("read_only_unavailable");
}

/** Runs the seal's exact legs on a bounded subset and reports aggregate throughput. Never writes. */
export async function timeLegs(d: TimerDeps): Promise<TimerReport> {
  if (!Number.isInteger(d.workers) || d.workers < 1 || d.workers > MAX_WORKERS) throw new SealRefusal("invalid_workers");
  if (!(d.maxSeconds > 0 && d.maxSeconds <= 3600)) throw new SealRefusal("invalid_max_seconds");
  if (d.rowCap !== undefined && !(Number.isInteger(d.rowCap) && d.rowCap > 0)) throw new SealRefusal("invalid_row_cap");
  const now = d.now ?? Date.now, start = now(), deadline = start + d.maxSeconds * 1000;
  await makeReadOnly(d.target);
  const stats = Object.fromEntries(LEGS.map((l) => [l, { rows: 0, pages: 0, busyMs: 0, queryMs: 0, backoffMs: 0, pageMs: [] as number[] }])) as Record<Leg, { rows: number; pages: number; busyMs: number; queryMs: number; backoffMs: number; pageMs: number[] }>;
  let stoppedBy: TimerReport["stoppedBy"] = "complete", failure: unknown, next = 0, attempted = 0, full = 0, equal = 0, unequal = 0;
  let premise: TimerReport["sealFencePremise"] = "unchecked";
  const capped = (l: Leg) => d.rowCap !== undefined && stats[l].rows >= d.rowCap;
  const timeUp = () => { if (now() >= deadline) { stoppedBy = "max-seconds"; return true; } return false; };
  const leg = async (c: Queryable, l: Leg, sql: string, org: string, kind: "records" | "receipts", acc: Digest, inTx: boolean) => {
    const t = now();
    if (inTx) await c.query(READ_ONLY_BEGIN);
    let complete = false;
    try {
      complete = await scanKeyset(c, sql, org, kind, {
        page: d.page, maxPageMs: d.maxPageMs, sleep: d.sleep,
        stop: () => {
          if (failure !== undefined || timeUp()) return true;
          if (capped(l)) { if (stoppedBy === "complete") stoppedBy = "row-cap"; return true; }
          return false;
        },
        observe: (s) => { const x = stats[l]; x.rows += s.rows; if (s.rows) x.pages++; x.queryMs += s.ms; x.backoffMs += s.backoffMs; if (s.rows) x.pageMs.push(s.ms); },
      }, (rows) => { for (const r of rows) addDigest(acc, kind === "records" ? rowText(r) : receiptText(r)); });
    } finally {
      if (inTx) await c.query("ROLLBACK");
      stats[l].busyMs += now() - t;
    }
    return complete;
  };
  await Promise.all(Array.from({ length: Math.min(d.workers, Math.max(1, d.orgs.length)) }, async () => {
    let src: SourceConnection | undefined;
    try {
      src = await d.openSource();
      await makeReadOnly(src);
      // Would --seal --workers accept this source's freeze guards? (Read-only catalog check.)
      if (premise === "unchecked") { premise = "missing"; premise = (await readFence(src)).guardsOk ? "ok" : "missing"; }
      for (let i = next++; i < d.orgs.length; i = next++) {
        if (failure !== undefined || timeUp() || LEGS.every(capped)) break;
        const org = d.orgs[i]!; attempted++;
        const a: Digest = { n: 0n, sum: 0n }, b: Digest = { n: 0n, sum: 0n }, ra: Digest = { n: 0n, sum: 0n }, rb: Digest = { n: 0n, sum: 0n };
        const c1 = await leg(src, "neonRecords", NEON_PAGE, org, "records", a, true);
        const c2 = await leg(d.target, "supabaseRecords", SUPA_PAGE, org, "records", b, false);
        const c3 = await leg(src, "neonReceipts", NEON_RECEIPTS, org, "receipts", ra, true);
        const c4 = await leg(d.target, "supabaseReceipts", SUPA_RECEIPTS, org, "receipts", rb, false);
        // Partial scans run in different collation orders; only whole orgs are comparable.
        if (c1 && c2 && c3 && c4) { full++; if (a.n === b.n && a.sum === b.sum && ra.n === rb.n && ra.sum === rb.sum) equal++; else unequal++; }
      }
    } catch (error) {
      if (failure === undefined) failure = error ?? new Error("timer_worker_failed");
    } finally {
      if (src) await src.end().catch(() => {});
    }
  }));
  if (failure !== undefined) throw failure;
  const wallMs = now() - start;
  const legs = Object.fromEntries(LEGS.map((l) => { const x = stats[l]; return [l, { rows: x.rows, pages: x.pages, busyMs: x.busyMs, queryMs: x.queryMs, backoffMs: x.backoffMs,
    rowsPerBusySecond: x.busyMs > 0 ? Math.round((x.rows * 1000) / x.busyMs) : null, rowsPerWallSecond: wallMs > 0 ? Math.round((x.rows * 1000) / wallMs) : null, pageMsP50: pct(x.pageMs, 0.5), pageMsP95: pct(x.pageMs, 0.95) }]; })) as Record<Leg, LegReport>;
  return { sealFencePremise: premise, legs, orgsAttempted: attempted, orgsFullyScanned: full, orgsDigestEqual: equal, orgsDigestUnequal: unequal, stoppedBy, wallMs, workers: d.workers };
}

/** "3,0,17" -> org list by ordinal in the C-ordered list; refuses duplicates or out-of-range ordinals. */
export function selectOrdinals(orgs: string[], raw: string): string[] {
  if (!/^[0-9]+(,[0-9]+)*$/.test(raw)) throw new SealRefusal("invalid_ordinals");
  const ids = raw.split(",").map(Number);
  if (new Set(ids).size !== ids.length || ids.some((i) => i >= orgs.length)) throw new SealRefusal("invalid_ordinals");
  return ids.map((i) => orgs[i]!);
}

/** Error text safe for aggregate-only output: refusal codes and PG SQLSTATE classes only. */
export function safeError(e: unknown): string {
  if (e instanceof SealRefusal) return e.code;
  const code = (e as any)?.code;
  return typeof code === "string" && /^[0-9A-Z]{5}$/.test(code) ? `pg_${code}` : "error";
}
