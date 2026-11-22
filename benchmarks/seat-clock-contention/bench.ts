/**
 * Receipt seat clock contention benchmark, on a THROWAWAY loopback PostgreSQL that the operator owns. Synthetic data only.
 * The script creates its own databases (one migrated template, then one copy per run) and drops all of them at the end.
 * It refuses any URL that is not a loopback IP literal, or that carries a password.
 *
 * What runs: the repository's migrator (local profile, as the *.postgres.test.ts files) and the CURRENT functions and lock
 * order, unchanged. Load goes through the real upload path (deliverBatch -> sqlLedger: claim, prepare, commit + staged ACK),
 * every ledger call its own transaction as bulls96_session_ingest with SET LOCAL lock_timeout 2s / statement_timeout 5s
 * (the role's settings in supabase/bootstrap/session_ingest_role.sql; SET ROLE does not apply role settings, so they are
 * set explicitly), READ COMMITTED.
 *
 * Workloads (same total records per run):
 *   W1 distinct origins: C workers, each its own user and origin (orgs rotate over 4), all competing on the clock.
 *   W2 hot origin: half the batches belong to ONE user/origin served by C/2 concurrent workers; the other half to C/2
 *      tail workers with one distinct user/origin each.
 * A failed delivery is re-queued (the client re-uploads the same batch after a 100-300 ms pause), as a real client would.
 *
 * Control: every controlEveryMs an owner connection runs sessions.ingest_lazy_clear(reset) for a dedicated control user,
 * with the same 2 s / 5 s limits. That is the step of the lazy ordered control (ingest_apply_ordered_control ->
 * ingest_lazy_clear) that takes the clock row FOR UPDATE, which conflicts with every other clock lock mode.
 *
 * Measurement: a dedicated superuser connection samples back to back (period reported) pgrowlocks() on the clock row and
 * the not-granted rows of pg_locks with pg_blocking_pids. Every ledger transaction is timed on the client and tagged with
 * SET LOCAL application_name (stage), so the samples map to stages and transactions:
 *   HOLD (per transaction holding the clock in an exclusive mode) = client-observed end of COMMIT - first sample showing it
 *        as the clock locker. Resolution: one sample period at the start; the end is exact.
 *   WAIT (per transaction, per lock class) = samples showing it waiting on that class x sample period.
 * Output: results/<runId>/summary.json (+ txns.jsonl.gz, waits.jsonl.gz raw receipts) and a line per cell on stdout.
 *
 * Usage: SESSION_INGEST_TEST_PG_URL=postgresql://postgres@127.0.0.1:<port>/postgres \
 *   bun benchmarks/seat-clock-contention/bench.ts '<plan json>'
 *   plan = { runId?, totalRecords?, maxSeconds?, controlEveryMs?, originClaimFence?, historyLive?,
 *            cells: [{ workload: "W1"|"W2", n: number, c: number }] }
 */
import pg from "pg";
import { isIP } from "node:net";
import { mkdirSync, writeFileSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { join } from "node:path";
import { TENANT } from "../../src/session-ingest/__tests__/support.ts";
import { runMigrations, type MigrationProfile } from "../../src/hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../src/session-pipeline/derived-schema.ts";
import type { LoadInit, Stream, Txn } from "./load-worker.ts";
import { sqlLedger } from "../../src/session-ingest/ledger-sql.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (!url) throw Error("SESSION_INGEST_TEST_PG_URL (an owned loopback fixture) is required");
const loopback = (h: string) => { const x = h.replace(/^\[|\]$/g, ""); return isIP(x) === 4 ? x.split(".")[0] === "127" : isIP(x) === 6 && x === "::1"; };
{ const u = new URL(url); if (!loopback(u.hostname) || u.password) throw Error("loopback throwaway fixture only"); }

type Workload = "W1" | "W2";
type Cell = { workload: Workload; n: number; c: number };
type Plan = { runId?: string; threads?: number; controls?: "c1" | "c2" | "none"; totalRecords?: number; maxSeconds?: number; controlEveryMs?: number; originClaimFence?: boolean; historyLive?: boolean; cells: Cell[] };
const plan = JSON.parse(process.argv[2] ?? "{}") as Plan;
const RUN_ID = plan.runId ?? new Date().toISOString().replace(/[:.]/g, "-");
const TOTAL = plan.totalRecords ?? 64000, MAX_S = plan.maxSeconds ?? 300, CONTROL_MS = plan.controlEveryMs ?? 1000;
const FENCE = plan.originClaimFence ?? true;
const OUT = join(import.meta.dir, "results", RUN_ID);
const epoch = "3".repeat(64), identity = "c".repeat(64);
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const now = () => performance.timeOrigin + performance.now();
const pct = (xs: number[], p: number) => { if (!xs.length) return null; const s = [...xs].sort((a, b) => a - b); return Math.round(s[Math.min(s.length - 1, Math.floor(p * s.length))]! * 100) / 100; };
const dist = (xs: number[]) => ({ n: xs.length, p50: pct(xs, 0.5), p95: pct(xs, 0.95), p99: pct(xs, 0.99), max: pct(xs, 1) });
const EXCL = new Set(["For No Key Update", "No Key Update", "For Update", "Update"]);

mkdirSync(OUT, { recursive: true });
const admin = new pg.Client({ connectionString: url }); await admin.connect();
const serverVersion = (await admin.query("SELECT version() v")).rows[0].v as string;
const settings = (await admin.query("SELECT name,setting,unit FROM pg_settings WHERE name=ANY($1) ORDER BY name",
  [["max_connections", "shared_buffers", "deadlock_timeout", "default_transaction_isolation", "fsync", "synchronous_commit", "wal_level", "max_wal_size", "checkpoint_timeout", "lock_timeout", "statement_timeout", "track_functions"]])).rows;
const tpl = "scb_tpl_" + crypto.randomUUID().replaceAll("-", "").slice(0, 16);
const dbUrl = (db: string) => { const u = new URL(url); u.pathname = "/" + db; return u.href; };
const created: string[] = [];
let generation = "";

async function buildTemplate() {
  await admin.query("CREATE DATABASE " + tpl); created.push(tpl);
  const c = new pg.Client({ connectionString: dbUrl(tpl) }); await c.connect();
  try {
    const t0 = now();
    await c.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin;CREATE EXTENSION pgrowlocks");
    await runMigrations(c as never, { profile });
    generation = (await derivedSchemaAssets()).expected.generation;
    await c.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch,
      privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 12 * 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await c.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
    if (plan.historyLive !== undefined) await c.query("UPDATE sessions.ingest_history_switch SET live=$1 WHERE singleton", [plan.historyLive]);
    const live = (await c.query("SELECT live FROM sessions.ingest_history_switch WHERE singleton")).rows[0]?.live;
    return { migrateMs: Math.round(now() - t0), historyLive: live, lazyPolicyRow: (await c.query("SELECT count(*)::int n FROM sessions.ingest_lazy_policy")).rows[0].n };
  } finally { await c.end(); }
}

type Tenant = typeof TENANT;
type Sample = { ts: number; holders: { x: string; m: string[]; p: number[] }[]; waits: any[]; tuples: { pid: number; rel: number }[]; apps: Record<number, string> };
const THREADS = plan.threads ?? 8;
const onMsg = <T>(w: Worker, kind: string) => new Promise<T>((res, rej) => {
  const f = (ev: MessageEvent) => { if ((ev.data as any).kind === kind) { w.removeEventListener("message", f); res(ev.data as T); } };
  w.addEventListener("message", f); w.addEventListener("error", (e) => rej(e), { once: true });
});

async function runCell(cell: Cell, index: number) {
  const db = `scb_run_${index}_` + crypto.randomUUID().replaceAll("-", "").slice(0, 8);
  await admin.query(`CREATE DATABASE ${db} TEMPLATE ${tpl}`); created.push(db);
  const pool = new pg.Pool({ connectionString: dbUrl(db), max: 2 });
  const threads: Worker[] = [];
  try {
    const rels = new Map<number, string>((await pool.query("SELECT c.oid::int oid,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sessions'")).rows.map((r) => [r.oid, r.relname]));
    const clockOid = [...rels].find(([, n]) => n === "ingest_receipt_seat_clock")![0];
    // Streams. W1: one user/origin per worker (orgs rotate over 4). W2: one hot user/origin with C/2 workers and half the
    // batches, plus C/2 tail streams of one worker each sharing the other half.
    const orgs = ["org-a", "org-b", "org-c", "org-d"];
    const tenantOf = (k: number): Tenant => ({ orgId: orgs[k % orgs.length]!, workspaceId: TENANT.workspaceId, userId: `scb-u${k}` });
    const batches = Math.ceil(TOTAL / cell.n);
    const T = Math.min(THREADS, cell.c);
    const per: Stream[][] = Array.from({ length: T }, () => []);
    let wk = 0;
    const control: Tenant = { orgId: "org-a", workspaceId: TENANT.workspaceId, userId: "scb-control" };
    const control2: Tenant = { orgId: "org-a", workspaceId: TENANT.workspaceId, userId: "scb-control2" };
    per[0]!.push({ t: control, origin: "scb-control-o", workers: 0, batches: 0, firstWorker: -1 });
    const logical: { origin: string; workers: number; share: number }[] = [];
    if (cell.workload === "W1") {
      const share = Math.floor(batches / cell.c), extra = batches - share * cell.c;
      for (let w = 0; w < cell.c; w++) per[w % T]!.push({ t: tenantOf(w), origin: `scb-o${w}`, workers: 1, batches: share + (w < extra ? 1 : 0), firstWorker: wk++ });
      logical.push({ origin: "scb-o<k> (one per worker)", workers: cell.c, share: 1 });
    } else {
      const hot = Math.max(1, Math.floor(cell.c / 2)), tail = Math.max(1, cell.c - hot);
      const hotBatches = Math.round(batches / 2), tailBatches = batches - hotBatches;
      const ht = Math.min(T, hot);
      for (let i = 0; i < ht; i++) {
        const w = Math.floor(hot / ht) + (i < hot % ht ? 1 : 0), b = Math.floor(hotBatches / ht) + (i < hotBatches % ht ? 1 : 0);
        per[i]!.push({ t: tenantOf(0), origin: "scb-hot", workers: w, batches: b, firstWorker: wk }); wk += w;
      }
      const share = Math.floor(tailBatches / tail), extra = tailBatches - share * tail;
      for (let k = 0; k < tail; k++) per[(k + ht) % T]!.push({ t: tenantOf(k + 1), origin: `scb-o${k + 1}`, workers: 1, batches: share + (k < extra ? 1 : 0), firstWorker: wk++ });
      logical.push({ origin: "scb-hot", workers: hot, share: hotBatches / batches }, { origin: "scb-o<k> (tail)", workers: tail, share: tailBatches / batches });
    }
    // Threads: warm (first-seen adoption of every stream and the control user, sequential) and build backlogs.
    const tg = now();
    for (let i = 0; i < T; i++) {
      const w = new Worker(new URL("./load-worker.ts", import.meta.url).href); threads.push(w);
      w.postMessage({ kind: "init", init: { dbUrl: dbUrl(db), generation, originClaimFence: FENCE, n: cell.n, streams: per[i]!, prefix: `scb-${index}-t${i}`, maxSeconds: MAX_S, epoch, identity } satisfies LoadInit });
      await onMsg(w, "ready");
    }
    const genMs = Math.round(now() - tg);
    const sampler = new Worker(new URL("./sampler-worker.ts", import.meta.url).href);
    const samplerDone = onMsg<{ samples: Sample[] }>(sampler, "done");
    sampler.postMessage({ kind: "start", dbUrl: dbUrl(db) });
    await onMsg(sampler, "started");
    await Bun.sleep(50);

    // Controls (main thread; light), each on its own connection at a fixed rate, one at a time:
    //   C1 "control":  sessions.ingest_lazy_clear(reset) as the owner -- the clock FOR UPDATE step alone;
    //   C2 "control2": sessions.ingest_reset_uploaded_sessions as bulls96_session_ingest (the Worker's front door), which runs
    //                  the full sessions.ingest_apply_ordered_control (lazy_begin, fence, receipt, apply_reset, lazy_clear, lazy_ack).
    // Missed slots (a control overrunning its slot) are counted. Both use the 2 s / 5 s limits.
    type Ctl = { stage: string; t0: number; t1: number; ok: boolean; code?: string; pid: number };
    const controls: Ctl[] = [];
    const missed: Record<string, number> = { control: 0, control2: 0 };
    let running = true;
    const c1Payload = JSON.stringify({ tenant: control, requestId: "scb-control-reset" });
    const ctlClients: pg.Client[] = [];
    let c2Applied = 0, c2Code = "";
    const controlLoop = async (stage: "control" | "control2", offset: number) => {
      const cc = new pg.Client({ connectionString: dbUrl(db) }); await cc.connect(); ctlClients.push(cc);
      const pid = (await cc.query("SELECT pg_backend_pid() p")).rows[0].p as number;
      const c2Ledger = sqlLedger(async (sql, params) => {
        try {
          await cc.query("BEGIN;SET LOCAL lock_timeout='2s';SET LOCAL statement_timeout='5s';SET LOCAL application_name='scb:control2';SET LOCAL ROLE bulls96_session_ingest");
          const r = await cc.query(sql, params as never); await cc.query("COMMIT"); return r as never;
        } catch (e: any) { c2Code = String(e?.code ?? "client"); await cc.query("ROLLBACK").catch(() => {}); throw e; }
      }, { acceptanceMode: "buffered", expectedGeneration: generation, originClaimFence: FENCE });
      let next = now() + CONTROL_MS + offset;
      while (running) {
        await Bun.sleep(Math.max(0, next - now())); if (!running) break;
        let t0 = now();
        while (next <= t0) { next += CONTROL_MS; if (next <= t0) missed[stage]!++; }
        try {
          if (stage === "control") {
            await cc.query("BEGIN;SET LOCAL lock_timeout='2s';SET LOCAL statement_timeout='5s';SET LOCAL application_name='scb:control'");
            await cc.query("SELECT sessions.ingest_lazy_clear($1::jsonb,'reset')", [c1Payload]);
            await cc.query("COMMIT");
          } else {
            // The acceptance control: ledger.applyOrderedControl (the Worker's entrypoint) -> ingest_set_session_uploads ->
            // ingest_apply_ordered_control, a selection disable of a fresh session of the control user, with a
            // relayhistory-cloud control witness; one transaction as the runtime role with the 2 s / 5 s limits.
            const v = String(c2Applied + 1);
            const payload = { origin_id: "scb-control2-o", sessions: [{ source: "claude", session_id: `scb-ctl-${index}-${v}` }], enabled: false };
            const digest = (await pool.query("SELECT encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',$1::text,$2::text,$3::text,$4::text,'selection',$5::jsonb)::text,'UTF8')),'hex') AS d",
              [epoch, control2.orgId, control2.workspaceId, control2.userId, JSON.stringify(payload)])).rows[0].d as string;
            const ctl = { epoch, version: v, digest, kind: "selection" as const };
            const witness = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant: control2,
              originId: payload.origin_id, batchId: null, batchDigest: null, sourceControlVersion: v, pending: true, pendingControl: ctl, privacyVersion: String(Number(v) - 1),
              originalRecordCount: 0, candidateCount: 0, records: [], privacy: v === "1" ? { complete: true, deletions: [], resets: [] } : null };
            const sourceDigest = (await pool.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).rows[0].d as string;
            t0 = now();
            c2Code = "";
            const r: any = await c2Ledger.applyOrderedControl!({ tenant: control2, originId: payload.origin_id, sessions: payload.sessions.map((x) => [x.source, x.session_id] as [string, string]),
              enabled: false, control: ctl, custody: { witness, sourceDigest } } as never);
            if (r?.control?.outcome !== "applied") throw Object.assign(Error("unexpected control outcome"), { code: "XXOUT:" + String(r?.control?.outcome) });
            c2Applied++;
            controls.push({ stage, t0, t1: now(), ok: true, pid });
            continue;
          }
          controls.push({ stage, t0, t1: now(), ok: true, pid });
        } catch (e: any) {
          await cc.query("ROLLBACK").catch(() => {});
          controls.push({ stage, t0, t1: now(), ok: false, code: (stage === "control2" && c2Code) || String(e?.code ?? e?.cause_code ?? "client"), pid });
        }
      }
    };
    // One control kind per run (matched runs, never both in one cell, so control traffic is the same in every cell).
    const controlLoops = (plan.controls ?? "c1") === "c1" ? [controlLoop("control", 0)] : plan.controls === "c2" ? [controlLoop("control2", 0)] : [];
    const cpu0 = process.cpuUsage();
    const results = threads.map((w) => onMsg<any>(w, "done"));
    for (const w of threads) w.postMessage({ kind: "go" });
    const done = await Promise.all(results);
    const cpu = process.cpuUsage(cpu0);
    running = false; await Promise.all(controlLoops); for (const cc of ctlClients) await cc.end();
    sampler.postMessage({ kind: "stop" });
    const { samples } = await samplerDone; sampler.terminate();
    const txns: Txn[] = done.flatMap((d) => d.txns);
    const start = Math.min(...done.map((d) => d.start)), lastDone = Math.max(...done.map((d) => d.lastDone));
    const durable = done.reduce((a, d) => a + d.durable, 0);
    const outcomes: Record<string, number> = {};
    for (const d of done) for (const [k, v] of Object.entries(d.outcomes as Record<string, number>)) outcomes[k] = (outcomes[k] ?? 0) + v;
    const nBatches = done.reduce((a, d) => a + d.batches, 0), tries = done.reduce((a, d) => a + d.tries, 0), undrained = done.reduce((a, d) => a + d.undrained, 0);
    const lags: number[] = done.flatMap((d) => d.lags);
    const loadEnd = Math.max(...done.map((d) => d.end as number));

    // Analysis.
    const periods = samples.slice(1).map((s, i) => s.ts - samples[i]!.ts);
    const period = pct(periods, 0.5) ?? 1;
    const all: (Txn & { kind: string })[] = [...txns.map((t) => ({ ...t, kind: "ledger" })),
      ...controls.map((c) => ({ stage: c.stage, worker: -1, pid: c.pid, t0: c.t0, t1: c.t1, ok: c.ok, code: c.code, n: 0, batch: "", kind: "control" }))];
    const byPid = new Map<number, (typeof all)[number][]>();
    for (const t of all) { const a = byPid.get(t.pid) ?? []; a.push(t); byPid.set(t.pid, a); }
    const txnAt = (pid: number, ts: number) => byPid.get(pid)?.find((t) => t.t0 - 1 <= ts && ts <= t.t1 + 1);
    const holdFirst = new Map<object, number>(), holdLast = new Map<object, number>(), waitCount = new Map<object, Record<string, number>>(), clockWaitFirst = new Map<object, number>();
    const graph: Record<string, number> = {};
    const keyNames = new Map<string, string>();
    for (const s of per.flat()) {
      const r = (await pool.query(`SELECT ((a>>32)&4294967295)::text||':'||(a&4294967295)::text na,((b>>32)&4294967295)::text||':'||(b&4294967295)::text oc FROM (SELECT
        hashtextextended(jsonb_build_array('96bulls:lazy-native-origin-v1',$1::text,$2::text,$3::text)::text,0) a,
        hashtextextended(jsonb_build_array('96bulls:ingest-origin-claim-v1',$1::text,$2::text,$4::text,$3::text)::text,0) b) k`, [s.t.orgId, s.t.workspaceId, s.origin, s.t.userId])).rows[0];
      keyNames.set(r.na, "advisory:native-origin"); keyNames.set(r.oc, "advisory:origin-claim");
    }
    const advisoryKind = (c: number, o: number, s: number) => (c === 961996 && o === 50000 && s === 2) ? "advisory:erase-gate(961996,50000)" : `advisory:other(${s === 1 ? "int8" : "int4,int4"})`;
    const stageName = (s: Sample, pid: number) => (s.apps[pid] ?? "").replace(/^scb:/, "") || "untagged";
    for (const s of samples) {
      for (const h of s.holders) h.p.forEach((pid, i) => {
        if (!EXCL.has(h.m[i] ?? h.m[0]!)) return;
        const t = txnAt(pid, s.ts); if (!t) return;
        if (!holdFirst.has(t)) holdFirst.set(t, s.ts); holdLast.set(t, s.ts);
      });
      const clockTuple = new Set(s.tuples.filter((x) => x.rel === clockOid).map((x) => x.pid));
      for (const wt of s.waits) {
        let cls: string;
        if (wt.t === "tuple") cls = wt.rel === clockOid ? "clock" : `row:${rels.get(wt.rel) ?? wt.rel}`;
        else if (wt.t === "transactionid") cls = clockTuple.has(wt.pid) ? "clock" : (() => { const r = s.tuples.find((x) => x.pid === wt.pid); return r ? `row:${rels.get(r.rel) ?? r.rel}` : "xid(row or unique-insert)"; })();
        else if (wt.t === "advisory") cls = (wt.s === 1 ? keyNames.get(`${wt.c}:${wt.o}`) : undefined) ?? advisoryKind(wt.c, wt.o, wt.s);
        else cls = `${wt.t}:${rels.get(wt.rel) ?? wt.rel ?? ""}`;
        const t = txnAt(wt.pid, s.ts);
        if (t) { const m = waitCount.get(t) ?? {}; m[cls] = (m[cls] ?? 0) + 1; waitCount.set(t, m); if (cls === "clock" && !clockWaitFirst.has(t)) clockWaitFirst.set(t, s.ts); }
        const blockers = [...new Set((wt.b as number[]).map((p) => stageName(s, p)))].sort().join("+") || "none";
        const key = `${stageName(s, wt.pid)} -> ${cls} [${wt.mode}] <- ${blockers}`; graph[key] = (graph[key] ?? 0) + 1;
      }
    }
    const holds: Record<string, number[]> = {}, clockWaits: Record<string, number[]> = {}, waitsByClass: Record<string, number[]> = {};
    let unobservedHolds = 0;
    for (const t of all) {
      const f = holdFirst.get(t);
      if (t.stage === "commit" || t.stage.startsWith("control")) {
        // End: the client's COMMIT completion, or one period after the last holder sample if the client was slower to see it.
        if (f !== undefined) (holds[t.stage] ??= []).push(Math.min(t.t1, holdLast.get(t)! + period) - f); else if (t.ok) unobservedHolds++;
        const cw = clockWaitFirst.get(t);
        (clockWaits[t.stage] ??= []).push(cw === undefined ? 0 : (f ?? t.t1) - cw);
      }
      const m = waitCount.get(t); if (m) for (const [k, v] of Object.entries(m)) (waitsByClass[`${t.stage}: ${k}`] ??= []).push(v * period);
    }
    const failures: Record<string, number> = {};
    for (const t of all) if (!t.ok) { const k = `${t.stage}:${t.code}`; failures[k] = (failures[k] ?? 0) + 1; }
    const commits = txns.filter((t) => t.stage === "commit" && t.ok);
    const seconds = (lastDone - start) / 1000;
    const result = {
      cell, originClaimFence: FENCE, threads: T, streams: logical,
      backlog: { batches: nBatches, records: nBatches * cell.n, generatedMs: genMs },
      drained: undrained === 0, undrainedBatches: undrained, drainSeconds: Math.round(seconds * 100) / 100,
      durableRecords: durable, durableRecordsPerSec: Math.round(durable / seconds), commitsPerSec: Math.round(commits.length / seconds * 100) / 100,
      deliveries: outcomes, retriesPerBatch: Math.round((tries - nBatches) / nBatches * 100) / 100,
      stageFailures: failures,
      stageLatencyMs: Object.fromEntries(["claim", "prepare", "commit"].map((s) => [s, dist(txns.filter((t) => t.stage === s && t.ok).map((t) => t.t1 - t.t0))])),
      clockHoldMs: Object.fromEntries(Object.entries(holds).map(([k, v]) => [k, dist(v)])), unobservedHolds,
      sampledCoverage: { samplePeriodMsP50: period, commitHoldsObserved: (holds.commit ?? []).length, commitsOk: commits.length, note: "hold start and every wait are sampled at the period shown; hold end is the COMMIT completion" },
      clockWaitMs: Object.fromEntries(Object.entries(clockWaits).map(([k, v]) => [k, dist(v)])),
      lockWaitMsByStageClass: Object.fromEntries(Object.entries(waitsByClass).sort().map(([k, v]) => [k, dist(v)])),
      controls: Object.fromEntries(["control", "control2"].map((st) => { const cs = controls.filter((c) => c.stage === st); return [st, {
        what: st === "control" ? "C1 ingest_lazy_clear(reset), owner" : "C2 ledger.applyOrderedControl -> ingest_set_session_uploads -> ingest_apply_ordered_control (selection disable), runtime role",
        everyMs: CONTROL_MS, schedule: "fixed-rate, one at a time", missedSlots: missed[st], attempted: cs.length, completed: cs.filter((c) => c.ok).length,
        completedDuringLoad: cs.filter((c) => c.ok && c.t1 <= loadEnd).length, completedAfterLoadEnd: cs.filter((c) => c.ok && c.t1 > loadEnd).length,
        failed: cs.filter((c) => !c.ok).reduce((a, c) => ({ ...a, [c.code!]: (a[c.code!] ?? 0) + 1 }), {} as Record<string, number>),
        latencyMs: dist(cs.filter((c) => c.ok).map((c) => c.t1 - c.t0)) }]; })),
      waitGraphSamples: Object.fromEntries(Object.entries(graph).sort((a, b) => b[1] - a[1]).slice(0, 25)),
      sampler: { samples: samples.length, periodMs: dist(periods) },
      client: { processCpuS: Math.round((cpu.user + cpu.system) / 1e4) / 100, eventLoopLagMs: dist(lags) },
      lastSeat: (await pool.query("SELECT last_seat::text s FROM sessions.ingest_receipt_seat_clock")).rows[0].s,
      seats: (await pool.query("SELECT count(*)::int n FROM sessions.ingest_receipt_seats")).rows[0].n,
    };
    const tag = `${index}-${cell.workload}-n${cell.n}-c${cell.c}`;
    writeFileSync(join(OUT, `txns-${tag}.jsonl.gz`), gzipSync(all.map((t) => JSON.stringify({ ...t, holdFirst: holdFirst.get(t) ?? null, holdLast: holdLast.get(t) ?? null, waits: waitCount.get(t) ?? null })).join("\n")));
    writeFileSync(join(OUT, `graph-${tag}.json`), JSON.stringify(graph, null, 1));
    return result;
  } finally {
    for (const w of threads) w.terminate();
    await pool.end();
    await admin.query(`DROP DATABASE IF EXISTS ${db} WITH(FORCE)`); created.splice(created.indexOf(db), 1);
  }
}

const rows: unknown[] = [];
let meta: Record<string, unknown> = {};
try {
  const t = await buildTemplate();
  meta = { runId: RUN_ID, serverVersion, settings, template: t, plan: { ...plan, totalRecords: TOTAL, maxSeconds: MAX_S, controlEveryMs: CONTROL_MS, originClaimFence: FENCE },
    client: { bun: Bun.version, cpus: navigator.hardwareConcurrency }, rtt: undefined as unknown };
  { const c = new pg.Client({ connectionString: url }); await c.connect(); const r: number[] = []; for (let i = 0; i < 200; i++) { const a = now(); await c.query("SELECT 1"); r.push(now() - a); } await c.end(); meta.rtt = dist(r); }
  console.log(JSON.stringify({ meta }));
  for (const [i, cell] of plan.cells.entries()) {
    const r = await runCell(cell, i); rows.push(r); console.log(JSON.stringify(r));
    writeFileSync(join(OUT, "summary.json"), JSON.stringify({ meta, rows }, null, 1));
  }
} finally {
  for (const db of [...created].reverse()) await admin.query(`DROP DATABASE IF EXISTS ${db} WITH(FORCE)`).catch(() => {});
  await admin.end();
}
