#!/usr/bin/env bun
/**
 * Gate 3 on a two-replica ReplicatedMergeTree at default cleanup: how long a deleted
 * term stays on each replica's disk, in any part, active or replaced. Local only.
 *
 *   BENCH_CH_URLS          two local HTTP endpoints, comma separated
 *   BENCH_CH_COMPRESSOR    clickhouse-compressor command
 *   BENCH_CH_PURGE_LIMIT_S how long to wait (default 1500)
 *   BENCH_OUT              report path
 *
 * Runs on the host holding both data directories. Every file is read raw, and .bin and
 * skip index files are also decompressed. A replica counts only if the same scan found
 * every term on it before the delete. Exits 1 if a term survives the wait.
 */
import { spawnSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import { assertLocalTarget } from "./guard.ts";

const urls = (process.env.BENCH_CH_URLS ?? "").split(",").filter(Boolean);
if (urls.length !== 2) throw new Error("BENCH_CH_URLS needs two endpoints");
urls.forEach((u) => assertLocalTarget(u));
const compressor = process.env.BENCH_CH_COMPRESSOR ?? "clickhouse compressor";
const limitMs = Number(process.env.BENCH_CH_PURGE_LIMIT_S ?? 1500) * 1000;
const out = process.env.BENCH_OUT ?? "clickhouse-replicated-purge.json";
const table = "gate3r", PARTS = 5;

async function ch(url: string, sql: string, settings: Record<string, string | number> = {}) {
  const u = new URL(url);
  for (const [k, v] of Object.entries(settings)) u.searchParams.set(k, String(v));
  const res = await fetch(u, { method: "POST", body: sql });
  const text = await res.text();
  if (!res.ok) throw new Error(`${res.status} ${text.slice(0, 400)}`);
  return text.trim();
}

const secret = (p: number, n: number) => `secretrep${"abcde"[p]}${"xy"[n]}`;
const terms = Array.from({ length: PARTS }, (_, p) => [secret(p, 0), secret(p, 1)]).flat();

function scan(dir: string) {
  const script = `cd '${dir}' && find . -type f | while read f; do
    t=$(grep -aoE 'secretrep[a-e][xy]' "$f" | sort -u | tr '\\n' ' ')
    [ -n "$t" ] && echo "raw|$f|$t"
    case "$f" in *.bin|*skp_idx*) t=$(${compressor} -d < "$f" 2>/dev/null | grep -aoE 'secretrep[a-e][xy]' | sort -u | tr '\\n' ' ')
      [ -n "$t" ] && echo "decompressed|$f|$t";; esac
  done`;
  const r = spawnSync("sh", ["-c", script], { encoding: "utf8", maxBuffer: 1 << 26 });
  if (r.status !== 0) throw new Error(`scan failed: ${r.stderr}`);
  return r.stdout.split("\n").filter(Boolean).map((l) => {
    const [how, file, found] = l.split("|");
    return { how: how!, part: file!.split("/")[1]!, file: file!, terms: found!.trim().split(" ") };
  });
}
type Hit = ReturnType<typeof scan>[number];
const termsIn = (hits: Hit[]) => new Set(hits.flatMap((h) => h.terms));

const settings = await Promise.all(urls.map(async (u) => ch(u,
  `SELECT groupArray((name, value)) FROM system.merge_tree_settings WHERE name IN ('old_parts_lifetime', 'cleanup_delay_period', 'max_cleanup_delay_period', 'cleanup_delay_period_random_add')`)));
const version = await ch(urls[0]!, "SELECT version()");
for (const u of urls) await ch(u, `DROP TABLE IF EXISTS ${table} SYNC`);
for (const [i, u] of urls.entries()) {
  await ch(u, `CREATE TABLE ${table} (org_id String, session_id String, event_id String, content String,
    INDEX t content TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(content)))
    ENGINE = ReplicatedMergeTree('/bench/${table}', 'r${i}') ORDER BY (org_id, session_id, event_id)`);
}
const dirs = await Promise.all(urls.map((u) => ch(u, `SELECT data_paths[1] FROM system.tables WHERE name = '${table}' AND database = currentDatabase()`)));
const deleted: string[] = [];
for (let p = 0; p < PARTS; p++) {
  for (let n = 0; n < 2; n++) deleted.push(`'p${p}-doomed${n}'`);
  await ch(urls[0]!, `INSERT INTO ${table} SELECT 'org-a', if(number < 2, concat('p${p}-doomed', toString(number)), concat('p${p}-keep', toString(number % 50))),
    toString(number), concat('alpha beta ', multiIf(number = 0, '${secret(p, 0)}', number = 1, '${secret(p, 1)}', 'plain'), ' gamma') FROM numbers(5000)`);
}
await ch(urls[1]!, `SYSTEM SYNC REPLICA ${table}`);

const control = dirs.map((d) => { const found = termsIn(scan(d)); return terms.every((t) => found.has(t)); });
if (!control.every(Boolean)) {
  writeFileSync(out, JSON.stringify({ version, verdict: "NOT MEASURED: positive control failed", control }, null, 2));
  console.log("NOT MEASURED: positive control failed", control);
  process.exit(1);
}
const statements = [`DELETE FROM ${table} WHERE session_id IN (<deleted sessions>)`, `ALTER TABLE ${table} APPLY DELETED MASK`];
await ch(urls[0]!, `DELETE FROM ${table} WHERE session_id IN (${deleted.join(",")})`, { lightweight_deletes_sync: 2, alter_sync: 2, mutations_sync: 2 });
await ch(urls[0]!, statements[1]!, { alter_sync: 2, mutations_sync: 2 });
const start = performance.now();
const active = async (u: string) => JSON.parse(await ch(u, `SELECT toJSONString(groupArray(name)) FROM system.parts WHERE table = '${table}' AND active`)) as string[];
const reads = await Promise.all(urls.map(async (u) => Number(await ch(u,
  `SELECT count() FROM ${table} WHERE hasAnyTokens(content, ['${terms.join("','")}'])`))));
const atMask = await Promise.all(dirs.map(async (d, i) => {
  const parts = await active(urls[i]!);
  const hits = scan(d);
  return { activeParts: parts, activePartsHolding: hits.filter((h) => parts.includes(h.part)), replacedPartsHolding: [...new Set(hits.map((h) => h.part))].filter((p) => !parts.includes(p)) };
}));

const purged: (number | null)[] = [null, null];
const timeline: { seconds: number; replica0Files: number; replica1Files: number }[] = [];
while (purged.includes(null) && performance.now() - start < limitMs) {
  const counts = dirs.map((d, i) => {
    const n = scan(d).length;
    if (n === 0 && purged[i] === null) purged[i] = Math.round((performance.now() - start) / 1000);
    return n;
  });
  timeline.push({ seconds: Math.round((performance.now() - start) / 1000), replica0Files: counts[0]!, replica1Files: counts[1]! });
  if (purged.includes(null)) await Bun.sleep(10_000);
}
const remaining = dirs.map((d) => scan(d));
const report = {
  version, warning: "Two local replicas with one embedded Keeper, synthetic data. Not ObsessionDB, and timings depend on cleanup jitter.",
  head: process.env.BENCH_HEAD_SHA ?? "unrecorded", settings, statements, parts: PARTS, deletedSessions: deleted.length,
  positiveControl: control, rowsMatchingAfterMask: reads, atMask, purgeSecondsAfterMask: purged, limitSeconds: limitMs / 1000,
  remaining, timeline,
  notCovered: "Backups, Keeper logs, object-store versions, OS page cache, and server logs",
};
writeFileSync(out, JSON.stringify(report, null, 2));
const ok = reads.every((n) => n === 0) && atMask.every((a) => a.activePartsHolding.length === 0) && purged.every((p) => p !== null);
console.log(JSON.stringify({ ...report, timeline: timeline.length, atMask: atMask.map((a) => ({ active: a.activeParts, activeHolding: a.activePartsHolding.length, replaced: a.replacedPartsHolding })) }, null, 1));
process.exit(ok ? 0 : 1);
