#!/usr/bin/env bun
/**
 * Why does every class read the whole tenant? One client, one change at a time: row
 * policy, snippet in the first phase, top-k filtering, substring recheck. A variant that
 * errors is recorded as an error, never dropped. Same corpus limits as proof.ts.
 */
import { readFileSync, writeFileSync } from "node:fs";
import { cpus, totalmem } from "node:os";
import { SCALES } from "../corpus.ts";
import { percentile } from "../stats.ts";
import { applyShape } from "./shape.ts";
import { ClickHouseHttp, installSchema, SessionSearchStore } from "./store.ts";
import { NEEDLES, Oracle, transcriptBatches } from "./transcripts.ts";

const url = process.env.BENCH_CH_URL ?? "http://127.0.0.1:8123";
const pw = "synthetic-proof-only";
const base = SCALES[process.env.BENCH_SCALE ?? "M"]!;
const shape = process.env.BENCH_SHAPE_FILE ? JSON.parse(readFileSync(process.env.BENCH_SHAPE_FILE, "utf8")) : null;
const params = shape ? applyShape(base, shape) : base;
const admin = new ClickHouseHttp(url, process.env.BENCH_CH_USER ?? "default", process.env.BENCH_CH_PASSWORD ?? "");
const reader = new ClickHouseHttp(url, "search_reader", pw);
const writer = new ClickHouseHttp(url, "search_writer", pw);
const store = new SessionSearchStore(reader, writer);

const FILTER = "org_id = 'org-0' AND workspace_id IN ['ws-0-0','ws-0-1','ws-0-2']";
const TOKENS = "hasAllTokens(text, {q:String})";
const RECHECK = "positionCaseInsensitiveUTF8(text, {q:String}) > 0";
const SNIPPET = "substringUTF8(text, greatest(positionCaseInsensitiveUTF8(text, {q:String}) - 80, 1), 320) AS snippet";
const page = (columns: string, where: string) => `SELECT ${columns} FROM session_search_records WHERE ${FILTER} AND ${where} ORDER BY ts DESC, event_id DESC LIMIT 11`;
const ids = "event_id, ts";
const noTopK = { use_top_k_dynamic_filtering: 0 };
const direct = { query_plan_direct_read_from_text_index: 1 };
const VARIANTS: Record<string, { client: ClickHouseHttp; sql: string; settings?: Record<string, number> }> = {
  policy_snippet_current: { client: reader, sql: page(`${ids}, ${SNIPPET}`, `${TOKENS} AND ${RECHECK}`) },
  nopolicy_snippet: { client: admin, sql: page(`${ids}, ${SNIPPET}`, `${TOKENS} AND ${RECHECK}`) },
  policy_nosnippet: { client: reader, sql: page(ids, `${TOKENS} AND ${RECHECK}`) },
  nopolicy_nosnippet: { client: admin, sql: page(ids, `${TOKENS} AND ${RECHECK}`) },
  policy_tokens_only: { client: reader, sql: page(ids, TOKENS) },
  nopolicy_tokens_only: { client: admin, sql: page(ids, TOKENS) },
  policy_snippet_notopk: { client: reader, sql: page(`${ids}, ${SNIPPET}`, `${TOKENS} AND ${RECHECK}`), settings: noTopK },
  policy_tokens_direct: { client: reader, sql: page(ids, TOKENS), settings: direct },
  policy_prewhere_tokens: { client: reader, sql: `SELECT ${ids}, ${SNIPPET} FROM session_search_records PREWHERE ${FILTER} AND ${TOKENS} WHERE ${RECHECK} ORDER BY ts DESC, event_id DESC LIMIT 11` },
};

await installSchema(admin, pw);
const oracle = new Oracle();
let loaded = 0;
for (const batch of transcriptBatches(params, oracle)) { await store.insert(batch); loaded += batch.length; }
await admin.run("OPTIMIZE TABLE session_search_records PARTITION ID 'none'").catch(() => {});
const parts = (await admin.run("SELECT count() FROM system.parts WHERE table = 'session_search_records' AND active")).trim();

const rows: unknown[] = [];
for (const id of ["pr", "phrase", "common"]) {
  const n = NEEDLES.find((x) => x.id === id)!;
  for (const [name, v] of Object.entries(VARIANTS)) {
    const tag = `variant:${id}:${name}`;
    const lat: number[] = [];
    let error: string | null = null;
    for (let i = 0; i < 20 && !error; i++) {
      const s = performance.now();
      try { await v.client.run(v.sql, { q: n.query }, { SQL_relay_org: "org-0", log_comment: tag, ...v.settings }); lat.push(performance.now() - s); }
      catch (e) { error = String(e).slice(0, 200); }
    }
    await admin.run("SYSTEM FLUSH LOGS");
    const [c] = await admin.rows<Record<string, number>>(`SELECT avg(read_rows) AS read_rows, avg(read_bytes) AS read_bytes, avg(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1000 AS cpu_ms FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = {tag:String}`, { tag });
    lat.sort((a, b) => a - b);
    const plan = await v.client.run(`EXPLAIN indexes = 1 ${v.sql}`, { q: n.query }, { SQL_relay_org: "org-0", ...v.settings }).catch((e) => `error: ${String(e).slice(0, 200)}`);
    const row = { class: id, truth: oracle.counts.get(`org-0|${id}`) ?? 0, variant: name, p50: Math.round(percentile(lat, 50) ?? 0), p95: Math.round(percentile(lat, 95) ?? 0), readRows: Math.round(c?.read_rows ?? 0), readMB: Math.round((c?.read_bytes ?? 0) / 1e6), cpuMs: Math.round(c?.cpu_ms ?? 0), error, plan };
    rows.push(row);
    console.log(JSON.stringify({ ...row, plan: undefined }));
  }
}
writeFileSync(process.env.BENCH_OUT ?? "variants-report.json", JSON.stringify({ shapeFidelity: "PARTIAL: sanitized aggregate anchors only; cannot close T0.", loaded, parts, runner: { cores: cpus().length, memoryBytes: totalmem() }, adminBypass: "nopolicy_* variants show engine behaviour only; they are not a product path", rows }, null, 2));
process.exit(0);
