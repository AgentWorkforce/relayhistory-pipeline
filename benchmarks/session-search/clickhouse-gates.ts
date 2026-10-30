#!/usr/bin/env bun
/**
 * Gates for the proposed ClickHouse transcript store (relayhistory-cloud #233). Vanilla
 * ClickHouse on synthetic data: not ObsessionDB, and timings are relative.
 *
 *   BENCH_CH_URL   local HTTP endpoint (default http://127.0.0.1:8123)
 *   BENCH_SCALE    corpus scale for gates 1, 5, 6, 7 (default test)
 *   BENCH_CH_SHELL how to run a command beside the server's data, e.g.
 *                  "docker exec <id> sh -c" (default: run locally)
 *   BENCH_CH_COMPRESSOR  clickhouse-compressor command (default clickhouse compressor)
 *   BENCH_OUT      report path
 *
 * Every result carries the statements and settings that produced it. Exits 1 if gate 3
 * or 4 fails: those can end the design.
 */
import { spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { generateCorpus, SCALES, TERMS, type CorpusSession } from "./corpus.ts";
import { assertLocalTarget } from "./guard.ts";
import { summarize } from "./stats.ts";

const url = process.env.BENCH_CH_URL ?? "http://127.0.0.1:8123";
assertLocalTarget(url);
const scale = process.env.BENCH_SCALE ?? "test";
const shell = process.env.BENCH_CH_SHELL?.split(" ");
const compressor = process.env.BENCH_CH_COMPRESSOR ?? "clickhouse compressor";
const out = process.env.BENCH_OUT ?? join(import.meta.dir, `results/clickhouse-${scale}.json`);

type Auth = { user: string; password: string };
const admin: Auth = { user: process.env.BENCH_CH_USER ?? "default", password: process.env.BENCH_CH_PASSWORD ?? "" };

async function ch(sql: string, settings: Record<string, string | number> = {}, auth = admin, body?: string): Promise<string> {
  const u = new URL(url);
  for (const [k, v] of Object.entries(settings)) u.searchParams.set(k, String(v));
  u.searchParams.set("query", body === undefined ? "" : sql);
  if (body === undefined) u.searchParams.delete("query");
  const res = await fetch(u, {
    method: "POST", body: body ?? sql,
    headers: { "X-ClickHouse-User": auth.user, "X-ClickHouse-Key": auth.password },
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${res.status} ${text.slice(0, 400)}`);
  return text.trim();
}
const num = async (sql: string, settings = {}, auth = admin) => Number(await ch(sql, settings, auth));
const attempt = async (run: () => Promise<unknown>) => {
  try { return { value: await run(), error: null as string | null }; }
  catch (e) { return { value: null, error: (e as Error).message.slice(0, 200) }; }
};

function beside(command: string): { ok: boolean; stdout: string } {
  const [bin, ...args] = shell ? [...shell, command] : ["sh", "-c", command];
  const r = spawnSync(bin!, args, { encoding: "utf8", maxBuffer: 1 << 28 });
  return { ok: r.status === 0, stdout: r.stdout ?? "" };
}

/** The read paths the store could use. Each must give the same answer. */
const READS = {
  directRead: { query_plan_direct_read_from_text_index: 1, query_plan_optimize_count_from_text_index: 0, use_skip_indexes: 1 },
  standardRead: { query_plan_direct_read_from_text_index: 0, query_plan_optimize_count_from_text_index: 0, use_skip_indexes: 1 },
  noIndex: { query_plan_direct_read_from_text_index: 0, query_plan_optimize_count_from_text_index: 0, use_skip_indexes: 0 },
  countFromIndex: { query_plan_direct_read_from_text_index: 1, query_plan_optimize_count_from_text_index: 1, use_skip_indexes: 1 },
};
const countAllReads = async (table: string, term: string, auth = admin) => Object.fromEntries(await Promise.all(
  Object.entries(READS).map(async ([name, s]) =>
    [name, await num(`SELECT count() FROM ${table} WHERE hasAllTokens(content, '${term}')`, s, auth)] as const)));

const seedTable = (t: string) => `
CREATE TABLE ${t} (org_id String, event_id String, ts DateTime, content String,
  INDEX t content TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(content)))
ENGINE = MergeTree ORDER BY (org_id, ts, event_id)
SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 1, max_cleanup_delay_period = 1, cleanup_delay_period_random_add = 0`;
const seedRows = (t: string, secret: string) => `
INSERT INTO ${t} SELECT if(number % 2 = 0, 'org-a', 'org-b'), toString(number),
  toDateTime('2026-10-01 00:00:00') + number,
  concat('alpha beta ', if(number = 7, '${secret} ', ''), if(number % 2 = 0, 'orgaterm ', 'orgbterm '), 'gamma')
FROM numbers(50000)`;

async function gate3() {
  const table = "gate3", secret = "secretzebra";
  const statements = [seedTable(table), seedRows(table, secret), `OPTIMIZE TABLE ${table} FINAL`,
    `DELETE FROM ${table} WHERE event_id = '7'`, `ALTER TABLE ${table} APPLY DELETED MASK`, `OPTIMIZE TABLE ${table} FINAL`];
  await ch(`DROP TABLE IF EXISTS ${table} SYNC`);
  await ch(statements[0]!); await ch(statements[1]!); await ch(statements[2]!);
  const tableDir = (await ch(`SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = '${table}'`));
  const scanFiles = () => {
    // Index files are compressed; decompress each, and read the raw bytes too.
    const script = `cd '${tableDir}' && find . -type f | while read f; do
      if grep -qa '${secret}' "$f"; then echo "raw $f"; fi
      case "$f" in *skp_idx*) if ${compressor} -d < "$f" 2>/dev/null | grep -qa '${secret}'; then echo "decompressed $f"; fi;; esac
    done`;
    const r = beside(script);
    return r.ok ? r.stdout.split("\n").filter(Boolean) : null;
  };
  const phases: Record<string, unknown> = {};
  const snap = async (phase: string) => {
    phases[phase] = {
      rareTerm: await countAllReads(table, secret),
      commonTerm: await countAllReads(table, "alpha"),
      activeParts: await ch(`SELECT groupArray(name) FROM system.parts WHERE table = '${table}' AND active`),
      filesHoldingTerm: scanFiles(),
    };
  };
  await snap("beforeDelete");
  await ch(statements[3]!, { lightweight_deletes_sync: 2 });
  await snap("afterLightweightDelete");
  await ch(statements[4]!, { mutations_sync: 2 });
  await snap("afterApplyDeletedMask");
  await ch(statements[5]!);
  await snap("afterOptimizeFinal");
  // Replaced parts keep the term until cleanup removes them, so purge is a duration.
  const start = performance.now();
  let holding = scanFiles();
  while (holding?.length && performance.now() - start < 120_000) {
    await Bun.sleep(1_000);
    holding = scanFiles();
  }
  const purgeSeconds = Math.round((performance.now() - start) / 100) / 10;

  type P = { rareTerm: Record<string, number>; commonTerm: Record<string, number>; filesHoldingTerm: string[] | null };
  const p = phases as Record<string, P>;
  const control = Object.values(p.beforeDelete!.rareTerm).every((n) => n === 1);
  const after = ["afterLightweightDelete", "afterApplyDeletedMask", "afterOptimizeFinal"];
  const reads = control && after.every((ph) => Object.values(p[ph]!.rareTerm).every((n) => n === 0)
    && Object.values(p[ph]!.commonTerm).every((n) => n === 49_999));
  const scanControl = (p.beforeDelete!.filesHoldingTerm ?? []).some((f) => f.includes("skp_idx"));
  const active = String((phases.afterOptimizeFinal as { activeParts: string }).activeParts).replace(/[\[\]']/g, "");
  const inActivePart = (p.afterOptimizeFinal!.filesHoldingTerm ?? []).filter((f) => f.includes(`/${active}/`));
  return {
    reads: reads ? "PASS" : "FAIL",
    purgedIndexFiles: !scanControl ? "NOT MEASURED: the scan found no term before delete" : holding === null ? "NOT MEASURED: scan failed"
      : holding.length === 0 && inActivePart.length === 0 ? "PASS" : "FAIL",
    purgeSecondsAfterOptimize: purgeSeconds,
    purgeNote: "With old_parts_lifetime = 1 and cleanup delay 1 s. The defaults are longer; replicas, backups and object-store versions are not covered.",
    filesStillHolding: holding, inActivePart, statements, readSettings: READS, phases,
  };
}

async function gate4() {
  const table = "gate4";
  const tenant: Auth = { user: "bench_tenant_a", password: "local-only" };
  const statements = [
    `CREATE USER ${tenant.user} IDENTIFIED WITH plaintext_password BY '***'`,
    `CREATE ROW POLICY gate4_a ON ${table} USING org_id = 'org-a' TO ${tenant.user}`,
    `GRANT SELECT ON ${table} TO ${tenant.user}`,
  ];
  await ch(`DROP TABLE IF EXISTS ${table} SYNC`);
  await ch(`DROP USER IF EXISTS ${tenant.user}`);
  await ch(`DROP ROW POLICY IF EXISTS gate4_a ON ${table}`);
  await ch(seedTable(table)); await ch(seedRows(table, "secretzebra")); await ch(`OPTIMIZE TABLE ${table} FINAL`);
  await ch(statements[0]!.replace("***", tenant.password)); await ch(statements[1]!); await ch(statements[2]!);

  const own = await countAllReads(table, "orgaterm", tenant);
  const other = await countAllReads(table, "orgbterm", tenant);
  const shared = await countAllReads(table, "alpha", tenant);
  const bypass = {
    trivialCount: await attempt(() => num(`SELECT count() FROM ${table}`, {}, tenant)),
    // A side door: the index's dictionary, read without passing through the table.
    textIndexFunction: await attempt(() => ch(
      `SELECT count() FROM mergeTreeTextIndex(currentDatabase(), ${table}, t) WHERE token = 'orgbterm'`, {}, tenant)),
  };
  const ok = Object.values(own).every((n) => n === 25_000) && Object.values(other).every((n) => n === 0)
    && Object.values(shared).every((n) => n === 25_000) && bypass.trivialCount.value === 25_000;
  return {
    reads: ok ? "PASS" : "FAIL",
    sideDoor: bypass.textIndexFunction.error ? "REFUSED" : Number(bypass.textIndexFunction.value) === 0 ? "EMPTY" : "FAIL: tenant read another tenant's token",
    statements, readSettings: READS, tenantA: { own, other, shared }, bypass,
  };
}

const TOKENIZERS = { splitByNonAlpha: "splitByNonAlpha", ngrams3: "ngrams(3)", sparseGrams: "sparseGrams" } as const;
/** Gate 2's terms, planted beside the corpus text so corpus hashes stay stable. */
const G2_TERMS = [
  { id: "identifier", term: "handle_webhook_v2" }, { id: "camelCase", term: "parseSessionToken" },
  { id: "path", term: "src/lib/session-search.ts" }, { id: "substring", term: "webho" },
  { id: "word", term: TERMS.selective }, { id: "non-english", term: TERMS.nonAscii }, { id: "cjk", term: TERMS.cjk },
];
const PLANTS = ["handle_webhook_v2", "parseSessionToken", "src/lib/session-search.ts", TERMS.nonAscii, TERMS.cjk];

const eventRow = (s: CorpusSession, n: number) => {
  const e = s.events[n]!;
  const plant = n === 0 && parseInt(s.sessionId.slice(2), 36) % 20 === 0 ? PLANTS[parseInt(s.sessionId.slice(2), 36) % 100 / 20 | 0]! : "";
  return { org_id: s.orgId, workspace_id: s.workspaceId, harness: s.harness, session_id: s.sessionId, event_id: e.eventId,
    ts: Math.floor(e.ts.getTime() / 1000), revision: 1, content: plant ? `${e.content} ${plant} ${TERMS.selective}` : e.content };
};

async function loadEvents(table: string, index: string) {
  await ch(`DROP TABLE IF EXISTS ${table} SYNC`);
  await ch(`CREATE TABLE ${table} (org_id LowCardinality(String), workspace_id LowCardinality(String), harness LowCardinality(String),
    session_id String, event_id String, ts DateTime, revision UInt64, content String
    ${index ? `, INDEX t content TYPE text(tokenizer = ${index}, preprocessor = lower(content))` : ""})
    ENGINE = ReplacingMergeTree(revision) PARTITION BY toYYYYMM(ts) ORDER BY (org_id, workspace_id, ts, session_id, event_id)`);
  let rows: string[] = [], raw = 0, total = 0;
  const flush = async () => {
    if (rows.length) await ch(`INSERT INTO ${table} FORMAT JSONEachRow`, {}, admin, rows.join("\n"));
    rows = [];
  };
  for (const s of generateCorpus(SCALES[scale]!)) {
    if (s.deleted) continue;
    for (let n = 0; n < s.events.length; n++) {
      const r = eventRow(s, n);
      raw += Buffer.byteLength(r.content); total++;
      rows.push(JSON.stringify(r));
      if (rows.length >= 50_000) await flush();
    }
  }
  await flush();
  await ch(`OPTIMIZE TABLE ${table} FINAL`);
  return { rows: total, rawContentBytes: raw };
}

async function gate2(raw: number) {
  const results: Record<string, unknown> = {};
  await loadEvents("events_truth", "");
  for (const [name, spec] of Object.entries(TOKENIZERS)) {
    const table = `events_${name}`;
    await loadEvents(table, spec);
    const size = await ch(`SELECT sum(data_compressed_bytes) FROM system.data_skipping_indices WHERE table = '${table}' FORMAT TSV`);
    const terms: Record<string, unknown> = {};
    for (const { id, term } of G2_TERMS) {
      const truth = await num(`SELECT count() FROM events_truth WHERE positionCaseInsensitiveUTF8(content, '${term}') > 0`);
      const got = await attempt(() => num(`SELECT count() FROM ${table} WHERE hasAllTokens(content, '${term.toLowerCase()}')`));
      const missed = await attempt(() => num(`SELECT count() FROM events_truth WHERE positionCaseInsensitiveUTF8(content, '${term}') > 0
        AND event_id NOT IN (SELECT event_id FROM ${table} WHERE hasAllTokens(content, '${term.toLowerCase()}'))`));
      const extra = got.value === null || missed.value === null ? null : Number(got.value) - (truth - Number(missed.value));
      terms[id] = { term, truth, returned: got.value, error: got.error,
        recall: truth && missed.value !== null ? (truth - Number(missed.value)) / truth : null, falsePositives: extra };
    }
    results[name] = { tokenizer: spec, indexBytes: Number(size), indexBytesPerContentByte: Number(size) / raw, terms };
  }
  return { query: "hasAllTokens(content, lower(term)); truth is positionCaseInsensitiveUTF8 on an unindexed table", results };
}

const WHALE_ORG = "org-0", WHALE_WS = "ws-0-0", SMALL_ORG = "org-1", SMALL_WS = "ws-1-1";
const pageSql = (table: string, org: string, ws: string, term: string) => `SELECT session_id, harness, max(ts) AS latest FROM ${table}
 WHERE org_id = '${org}' AND workspace_id = '${ws}' AND hasAllTokens(content, '${term}')
 GROUP BY session_id, harness ORDER BY latest DESC, session_id DESC LIMIT 50`;

async function timed(sql: string, n: number, before?: () => Promise<unknown>) {
  const samples: number[] = [];
  for (let i = 0; i < n; i++) {
    if (before) await before();
    const t = performance.now();
    await ch(sql);
    samples.push(performance.now() - t);
  }
  return summarize(samples);
}

async function dropCaches() {
  const tried: Record<string, string> = {};
  for (const c of ["MARK", "UNCOMPRESSED", "PAGE", "QUERY", "TEXT INDEX TOKENS", "TEXT INDEX HEADER", "TEXT INDEX POSTINGS"]) {
    tried[c] = (await attempt(() => ch(`SYSTEM DROP ${c} CACHE`))).error ?? "ok";
  }
  return tried;
}

async function gate1(table: string) {
  const terms = { rare: "fossilmarker", common: "refactor", multiToken: "cache index" };
  const cacheDrops = await dropCaches();
  const results: Record<string, unknown> = {};
  for (const [id, term] of Object.entries(terms)) {
    const sql = pageSql(table, WHALE_ORG, WHALE_WS, term);
    results[id] = { cold: await timed(sql, 10, dropCaches), warm: await timed(sql, 50), sql };
  }
  return { note: "Indicative. Cold drops ClickHouse caches, not the OS page cache.", cacheDrops, results };
}

async function gate5() {
  const table = "gate5";
  await ch(`DROP TABLE IF EXISTS ${table} SYNC`);
  await ch(`CREATE TABLE ${table} (org_id String, event_id String, ts DateTime, revision UInt64, content String,
    INDEX t content TYPE text(tokenizer = ngrams(3), preprocessor = lower(content)))
    ENGINE = ReplacingMergeTree(revision) ORDER BY (org_id, event_id) SETTINGS non_replicated_deduplication_window = 1000`);
  const batch = (b: number) => Array.from({ length: 500 }, (_, i) => JSON.stringify({ org_id: "org-a", event_id: `${b}-${i}`,
    ts: 1_790_000_000, revision: 1, content: `refactor the worker queue ${b} ${i}`.repeat(10) })).join("\n");
  const settings = { async_insert: 1, wait_for_async_insert: 1, async_insert_busy_timeout_max_ms: 200 };
  const start = performance.now();
  await Promise.all(Array.from({ length: 8 }, async (_, w) => {
    for (let b = 0; b < 25; b++) await ch(`INSERT INTO ${table} FORMAT JSONEachRow`, settings, admin, batch(w * 1000 + b));
  }));
  const seconds = (performance.now() - start) / 1000;
  const rows = await num(`SELECT count() FROM ${table}`);
  const parts = await num(`SELECT count() FROM system.parts WHERE table = '${table}' AND active`);
  // A retried insert: same body and token, twice.
  const token = { insert_deduplication_token: "bench-retry-1" };
  for (let i = 0; i < 2; i++) await ch(`INSERT INTO ${table} FORMAT JSONEachRow`, token, admin, batch(999_999));
  const afterRetry = await num(`SELECT count() FROM ${table}`);
  const merged = await num(`SELECT count() FROM ${table} FINAL`);
  return { settings, rowsPerSecond: Math.round(rows / seconds), rows, activeParts: parts,
    retry: { insertedRows: afterRetry - rows, expected: 500, finalRows: merged, verdict: afterRetry - rows === 500 ? "PASS" : "FAIL" } };
}

async function gate7(table: string) {
  const victim = pageSql(table, SMALL_ORG, SMALL_WS, "refactor");
  const noisy = pageSql(table, WHALE_ORG, WHALE_WS, "refactor");
  const quiet = await timed(victim, 50);
  let stop = false;
  const load = Array.from({ length: 6 }, async () => { while (!stop) await ch(noisy); });
  const underLoad = await timed(victim, 50);
  stop = true;
  await Promise.all(load);
  return { note: "6 connections loop the whale's common term; no quotas or workload scheduling set", quiet, underLoad, victim, noisy };
}

async function bm25() {
  const fns = await ch("SELECT groupArray(name) FROM system.functions WHERE name ILIKE '%bm25%' OR name ILIKE '%relevance%'");
  return { functionsMatching: fns, verdict: fns === "[]" ? "No BM25 function in system.functions" : "See functions" };
}

const version = await ch("SELECT version()");
const report: Record<string, unknown> = { version, scale, warning: "Vanilla ClickHouse, synthetic data, relative numbers. Not ObsessionDB and not an e2e proof." };
report.bm25 = await bm25();
report.gate3 = await gate3();
report.gate4 = await gate4();
const { rawContentBytes } = await loadEvents("events_probe", "");
report.gate2 = await gate2(rawContentBytes);
report.gate1 = { splitByNonAlpha: await gate1("events_splitByNonAlpha"), ngrams3: await gate1("events_ngrams3") };
report.gate5 = await gate5();
report.gate6 = Object.fromEntries(await Promise.all(Object.keys(TOKENIZERS).map(async (n) => {
  const total = await num(`SELECT sum(bytes_on_disk) FROM system.parts WHERE table = 'events_${n}' AND active`);
  return [n, { bytesOnDisk: total, bytesPerContentByte: total / rawContentBytes }] as const;
})));
report.gate7 = await gate7("events_ngrams3");

mkdirSync(dirname(out), { recursive: true });
writeFileSync(out, JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, (k, v) => (k === "statements" || k === "readSettings" ? undefined : v), 1));
const g3 = report.gate3 as { reads: string; purgedIndexFiles: string };
const g4 = report.gate4 as { reads: string; sideDoor: string };
if (g3.reads === "FAIL" || g3.purgedIndexFiles === "FAIL" || g4.reads === "FAIL" || g4.sideDoor.startsWith("FAIL")) process.exit(1);
process.exit(0);
