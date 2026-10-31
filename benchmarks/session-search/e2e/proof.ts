#!/usr/bin/env bun
/**
 * End-to-end transcript search proof on vanilla ClickHouse with synthetic data. Gates are
 * G1-G11 of docs/session-search-proof.md. Exits 1 if a logic gate fails. Timings are
 * relative: a local engine is not the production service, so G7 and G8 never report PASS.
 *
 *   BENCH_CH_URL      local HTTP endpoint (default http://127.0.0.1:8123)
 *   BENCH_CH_USER, BENCH_CH_PASSWORD  admin login
 *   BENCH_SCALE       test, S, M, L (default test)
 *   BENCH_LOAD_S      seconds per concurrency level (default 10)
 *   BENCH_SHAPE_FILE  aggregate-only shape file; sizes the corpus. G6 is at best PARTIAL
 *   BENCH_OUT         report path
 */
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { corpusHash, SCALES, tenantCanary } from "../corpus.ts";
import { percentile } from "../stats.ts";
import { PLANTED_CONTENT_MARKER, PLANTED_SECRETS } from "../../../src/concourse/seed.ts";
import { federatedSearch, LEGACY_DDL } from "./federation.ts";
import { indexManifest, type IndexDeps } from "./index-stage.ts";
import { startPipeline, type Pointer } from "./pipeline.ts";
import { applyShape, checkShapeFile } from "./shape.ts";
import { ClickHouseHttp, installSchema, SessionSearchStore, type Scope, type SearchHit } from "./store.ts";
import { NEEDLES, Oracle, oracleKey, QUERY_SET_VERSION, transcriptBatches } from "./transcripts.ts";

const url = process.env.BENCH_CH_URL ?? "http://127.0.0.1:8123";
const scaleName = process.env.BENCH_SCALE ?? "test";
const base = SCALES[scaleName];
if (!base) throw new Error(`unknown scale ${scaleName}`);
const shapeFile = process.env.BENCH_SHAPE_FILE ? JSON.parse(readFileSync(process.env.BENCH_SHAPE_FILE, "utf8")) : null;
const params = shapeFile ? applyShape(base, shapeFile) : base;
const loadSeconds = Number(process.env.BENCH_LOAD_S ?? 10);
const out = process.env.BENCH_OUT ?? join(import.meta.dir, `../results/e2e-${scaleName}.json`);
const pw = "synthetic-proof-only";

const admin = new ClickHouseHttp(url, process.env.BENCH_CH_USER ?? "default", process.env.BENCH_CH_PASSWORD ?? "");
const reader = new ClickHouseHttp(url, "search_reader", pw);
const writer = new ClickHouseHttp(url, "search_writer", pw);
const search = new SessionSearchStore(reader, writer);

type Status = "PASS" | "FAIL" | "PARTIAL" | "RELATIVE" | "NOT_RUN" | "INHERITED_FAIL";
const SHAPE_FIDELITY = "PARTIAL: sanitized aggregate anchors only, not a fresh production-derived shape artifact. Not production-scale parity; cannot close T0.";
console.log(`SHAPE FIDELITY: ${SHAPE_FIDELITY}`);
const report: Record<string, { status: Status; checks?: Record<string, boolean>; detail?: unknown }> = {};
function gate(id: string, checks: Record<string, boolean>, detail?: unknown) {
  const status: Status = Object.values(checks).every(Boolean) ? "PASS" : "FAIL";
  report[id] = { status, checks, detail };
  console.log(`${status.padEnd(9)} ${id}`);
  for (const [k, v] of Object.entries(checks)) if (!v) console.log(`          failed: ${k}`);
}
const scopeOf = (orgId: string, workspaceIds: string[]): Scope => ({ orgId, workspaceIds });
const orgScope = (org: number) => scopeOf(`org-${org}`, Array.from({ length: params.workspacesPerOrg }, (_, w) => `ws-${org}-${w}`));
const countRows = async (where: string) => Number((await admin.run(`SELECT count() FROM session_search_records WHERE ${where}`)).trim());

async function g1() {
  const deps: IndexDeps = { store: null as never, search, optedOutWorkspaces: new Set(["ws-optout"]), watermarks: new Map() };
  const outcomes: string[] = [];
  const pipeline = await startPipeline("org-e2e-a", async (p: Pointer) => { outcomes.push(await indexManifest(p.orgId, p.manifest, deps)); last = p; });
  let last: Pointer | undefined;
  deps.store = pipeline.store as never;
  const secret = Object.values(PLANTED_SECRETS)[0]!;
  const line = (text: string, n: number) => JSON.stringify({ type: "user", timestamp: `2026-10-30T10:00:0${n}.000Z`, message: { content: text } });
  const a = scopeOf("org-e2e-a", ["ws-e2e-a"]);
  const started = performance.now();
  const first = await pipeline.courier("org-e2e-a", "ws-e2e-a", [{ id: "s-e2e-1", lines: [
    line(`${PLANTED_CONTENT_MARKER} e2emarkeralpha fix packages/foo/bar.ts`, 1), line(`key ${secret} e2esecretline`, 2),
  ] }]);
  let hit = await search.search(a, { query: "e2emarkeralpha" });
  const firstHitMs = performance.now() - started;
  const secretProbe = secret.split("\n")[0]!.slice(0, 24);
  const stagedSecret = (await readFile(join(first.dir, "s-e2e-1.jsonl"), "utf8")).includes(secretProbe);
  const secretHits = (await search.search(a, { query: secretProbe })).hits.length;
  const scrubbedLineFound = (await search.search(a, { query: "e2esecretline" })).hits.length === 1;
  // An appended turn: only new bytes, and it becomes searchable.
  const second = await pipeline.courier("org-e2e-a", "ws-e2e-a", [{ id: "s-e2e-1", lines: [
    line(`${PLANTED_CONTENT_MARKER} e2emarkeralpha fix packages/foo/bar.ts`, 1), line(`key ${secret} e2esecretline`, 2), line("e2emarkerbeta", 3),
  ] }], first.dir);
  const appended = (await search.search(a, { query: "e2emarkerbeta" })).hits.length === 1;
  const rowsBefore = await countRows("org_id = 'org-e2e-a'");
  // A retried delivery with the watermark lost: the dedup token keeps one copy.
  deps.watermarks.clear();
  const retry = await indexManifest(last!.orgId, last!.manifest, deps);
  const rowsAfterRetry = await countRows("org_id = 'org-e2e-a'");
  const stale = await indexManifest(last!.orgId, last!.manifest, deps);
  const quarantined = await indexManifest("org-e2e-a", { ...last!.manifest, revision: 99, raw: { ...last!.manifest.raw, scrub: undefined } } as never, deps);
  await pipeline.courier("org-e2e-a", "ws-optout", [{ id: "s-e2e-2", lines: [line("optoutmarker", 1)] }]);
  const optOutRows = await countRows("workspace_id = 'ws-optout'");
  await pipeline.stop();
  gate("G1_courier_to_search", {
    realCourierUploaded: first.result.uploaded === 1 && second.result.uploaded === 1,
    jobsConsumed: first.landed.acked === 1 && first.indexed.acked === 1,
    firstHit: hit.hits.length === 1,
    plantedSecretWasInSource: stagedSecret,
    plantedSecretNotSearchable: secretHits === 0,
    scrubbedRecordStillIndexed: scrubbedLineFound,
    appendSearchable: appended,
    retryDeduplicated: retry === "indexed" && rowsAfterRetry === rowsBefore,
    staleSkipped: stale === "skipped_stale",
    quarantineSkipped: quarantined === "skipped_quarantined",
    optOutSkipped: optOutRows === 0,
  }, { firstHitMs: Math.round(firstHitMs), rowsBefore, rowsAfterRetry, outcomes, note: "the search-index step is proof code called from session-indexed; no such queue stage exists" });
}

function recallAt(k: number, got: string[], want: string[]) {
  const top = want.slice(0, k);
  return top.length ? top.filter((id) => got.slice(0, k).includes(id)).length / top.length : 1;
}

async function g2(oracle: Oracle) {
  const rows: unknown[] = [];
  const checks: Record<string, boolean> = {};
  for (const org of [0, 1]) for (const n of NEEDLES) {
    const scope = orgScope(org);
    const want = oracle.newest(oracleKey(scope.orgId, n.id)).map((h) => h.eventId);
    const { hits } = await search.search(scope, { query: n.query, limit: 10 });
    const got = hits.map((h) => h.event_id);
    const r5 = recallAt(5, got, want), r10 = recallAt(10, got, want);
    const falseMatches = hits.filter((h) => !h.snippet.toLowerCase().includes(n.query.toLowerCase())).length;
    const count = Number((await reader.run(
      `SELECT count() FROM session_search_records WHERE org_id = {org:String} AND positionCaseInsensitiveUTF8(text, {q:String}) > 0`,
      { org: scope.orgId, q: n.query }, { SQL_relay_org: scope.orgId })).trim());
    const truth = oracle.counts.get(oracleKey(scope.orgId, n.id)) ?? 0;
    rows.push({ org: scope.orgId, id: n.id, cls: n.cls, truth, count, r5, r10, falseMatches });
    checks[`${scope.orgId}/${n.id}`] = r5 === 1 && r10 === 1 && falseMatches === 0 && count === truth;
  }
  gate("G2_lexical_recall", checks, { queries: QUERY_SET_VERSION, rows });
}

async function g3(oracle: Oracle) {
  const checks: Record<string, boolean> = {};
  const scope = orgScope(0);
  for (const n of NEEDLES.filter((x) => x.id !== "common")) {
    const { hits, nextCursor } = await search.search(scope, { query: n.query, limit: 5 });
    if (!hits.length) { checks[`${n.id}/hasHits`] = (oracle.counts.get(oracleKey(scope.orgId, n.id)) ?? 0) === 0; continue; }
    const hit = hits[0]!;
    const byQuery = await search.context(scope, { sessionId: hit.session_id, source: hit.harness, query: n.query, maxEvents: 10 });
    const byFocus = await search.context(scope, { sessionId: hit.session_id, source: hit.harness, focusTs: hit.ts, maxEvents: 10 });
    const sorted = byQuery.every((e, i) => i === 0 || byQuery[i - 1]!.ts <= e.ts);
    checks[`${n.id}/windowHasMatch`] = byQuery.some((e) => e.is_match === 1);
    checks[`${n.id}/focusFindsEvent`] = byFocus.some((e) => e.event_id === hit.event_id);
    checks[`${n.id}/chronological`] = sorted;
    checks[`${n.id}/snippetBounded`] = hits.every((h) => [...h.snippet].length <= 320);
    checks[`${n.id}/citation`] = hits.every((h) => h.citation.includes(encodeURIComponent(h.session_id)));
    if (nextCursor) {
      const next = await search.search(scope, { query: n.query, limit: 5, cursor: nextCursor });
      checks[`${n.id}/cursorNoRepeat`] = !next.hits.some((h) => hits.some((x) => x.event_id === h.event_id));
    }
  }
  gate("G3_context_recall", checks);
}

async function g4() {
  const t0 = "2026-10-15T00:00:00.000Z";
  const legacy = new PGlite();
  await legacy.exec(LEGACY_DDL);
  const scope = scopeOf("org-fed", ["ws-fed"]);
  const add = async (id: string, session: string, ts: string, content: string) => legacy.query(
    "INSERT INTO legacy_events VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", ["org-fed", "ws-fed", "claude", session, "m1", id, ts, content]);
  const rec = (id: string, session: string, ts: string, text: string, recordSeq: number) => ({
    orgId: "org-fed", workspaceId: "ws-fed", harness: "claude", sessionId: session, machineId: "m1", eventId: id,
    revision: 1, chunkSeq: 0, recordSeq, ts, kind: "message", scrubVersion: 1, text,
  });
  await add("pre-1", "s-pre", "2026-10-01T00:00:00.000Z", "fedterm before t0");
  await add("str-1", "s-straddle", "2026-10-14T23:59:00.000Z", "fedterm straddling old");
  await add("otherorg", "s-x", "2026-10-02T00:00:00.000Z", "fedterm");
  await legacy.query("UPDATE legacy_events SET org_id = 'org-other' WHERE event_id = 'otherorg'");
  await add("long-1", "s-pre", "2026-10-02T00:00:00.000Z", `${"x".repeat(5000)} fedlongterm`);
  const pools = { legacy, store: search, t0 };
  const ids = (r: { hits: SearchHit[] }) => r.hits.map((h) => h.event_id).sort().join(",");
  const beforeT0 = await federatedSearch({ ...pools, legacyEnabled: true }, scope, { query: "fedterm" });
  await search.insert([
    rec("str-1", "s-straddle", "2026-10-14T23:59:00.000Z", "fedterm straddling old", 0), // backfilled copy
    rec("str-2", "s-straddle", "2026-10-15T00:01:00.000Z", "fedterm straddling new", 1),
    rec("post-1", "s-post", "2026-10-16T00:00:00.000Z", "fedterm after t0", 0),
  ]);
  const afterT0 = await federatedSearch({ ...pools, legacyEnabled: true }, scope, { query: "fedterm" });
  const pages: string[] = [];
  let cursor: string | undefined;
  do {
    const r = await federatedSearch({ ...pools, legacyEnabled: true }, scope, { query: "fedterm", limit: 2, cursor });
    pages.push(...r.hits.map((h) => h.event_id));
    cursor = r.nextCursor ?? undefined;
  } while (cursor);
  const brokenStore = new SessionSearchStore(new ClickHouseHttp("http://127.0.0.1:1", "x", "x"), writer);
  const storeDown = await federatedSearch({ legacy, store: brokenStore, t0, legacyEnabled: true }, scope, { query: "fedterm" });
  const longPreT0 = await federatedSearch({ ...pools, legacyEnabled: true }, scope, { query: "fedlongterm" });
  await legacy.close();
  const legacyDown = await federatedSearch({ ...pools, legacyEnabled: true }, scope, { query: "fedterm" });
  gate("G4_federation", {
    beforeT0LegacyOnly: ids(beforeT0) === "pre-1,str-1",
    afterT0EachOnce: ids(afterT0) === "post-1,pre-1,str-1,str-2",
    tenantScoped: !afterT0.hits.some((h) => h.event_id === "otherorg"),
    paginationEachOnce: [...pages].sort().join(",") === "post-1,pre-1,str-1,str-2",
    storeDownNamed: storeDown.degradedPools.join() === "transcript_store" && ids(storeDown) === "pre-1,str-1",
    legacyDownNamed: legacyDown.degradedPools.join() === "legacy_events" && ids(legacyDown) === "post-1,str-1,str-2",
  }, { knownGap: { query: "fedlongterm", preT0HitsBeyond4KB: longPreT0.hits.length, note: "pre-T0 text past 4 KB stays unsearchable until a backfill" } });
}

async function g5() {
  const checks: Record<string, boolean> = {};
  let attempts = 0, leaks = 0;
  for (let org = 0; org < params.orgs; org++) for (let ws = 0; ws < params.workspacesPerOrg; ws++) {
    const canary = tenantCanary(`org-${org}`, `ws-${org}-${ws}`);
    for (let other = 0; other < params.orgs; other++) {
      if (other === org) continue;
      attempts++;
      if ((await search.search(orgScope(other), { query: canary, limit: 1 })).hits.length) leaks++;
    }
    for (let w = 0; w < params.workspacesPerOrg; w++) {
      if (w === ws) continue;
      attempts++;
      if ((await search.search(scopeOf(`org-${org}`, [`ws-${org}-${w}`]), { query: canary, limit: 1 })).hits.length) leaks++;
    }
  }
  checks.zeroCanaryLeaks = leaks === 0;
  const own = await search.search(orgScope(1), { query: tenantCanary("org-1", "ws-1-0"), limit: 1 });
  checks.positiveControl = own.hits.length === 1;
  const refused = async (run: () => Promise<unknown>) => run().then(() => false, () => true);
  checks.emptyScopeRefused = await refused(() => search.search(scopeOf("org-1", []), { query: "deploy" }));
  checks.emptyOrgRefused = await refused(() => search.search(scopeOf("", ["ws-1-0"]), { query: "deploy" }));
  for (const [name, q] of [["quote", "' OR 1=1 --"], ["union", "x') UNION ALL SELECT * FROM system.users --"], ["brace", "{org:String}"], ["settings", "x' SETTINGS SQL_relay_org='org-1"]] as const)
    checks[`injection/${name}`] = (await search.search(orgScope(2), { query: q })).hits.length === 0;
  const hit = own.hits[0]!;
  checks.crossTenantContextEmpty = (await search.context(orgScope(2), { sessionId: hit.session_id, query: tenantCanary("org-1", "ws-1-0") })).length === 0;
  const mismatchedPolicy = await reader.run(`SELECT count() FROM session_search_records WHERE org_id = 'org-1'`, {}, { SQL_relay_org: "org-2" });
  checks.rowPolicyBackstop = Number(mismatchedPolicy.trim()) === 0;
  checks.missingSettingFailsClosed = await refused(() => reader.run("SELECT count() FROM session_search_records"));
  checks.readerCannotWrite = await refused(() => reader.run("INSERT INTO session_search_records (org_id) VALUES ('x')", {}, { SQL_relay_org: "org-1" }));
  checks.readerCannotDrop = await refused(() => reader.run("DROP TABLE session_search_records", {}, { SQL_relay_org: "org-1" }));
  const crafted = Buffer.from(JSON.stringify(["2999-01-01T00:00:00.000Z", "z"])).toString("base64url");
  const viaCursor = await search.search(orgScope(2), { query: tenantCanary("org-1", "ws-1-0"), cursor: crafted });
  checks.craftedCursorStaysScoped = viaCursor.hits.length === 0;
  gate("G5_tenant_isolation", checks, { attempts, leaks, boundary: "#233: the row policy is a backstop; any caller who can set SQL_relay_org reads that tenant, so the chokepoint is the boundary" });
}

async function g6(loaded: number) {
  if (!shapeFile) {
    report.G6_shape = { status: "NOT_RUN", detail: "no shape file given; corpus shape is assumed" };
    console.log("NOT_RUN   G6_shape");
    return;
  }
  const issues = checkShapeFile(shapeFile);
  const whale = Number((await admin.run("SELECT count() FROM session_search_records WHERE org_id = 'org-0'")).trim());
  const within = (got: number, want: number) => Math.abs(got - want) / want <= 0.05;
  const ok = !issues.length && within(loaded, shapeFile.events) && within(whale, shapeFile.whaleOrgEvents);
  report.G6_shape = {
    status: ok ? "PARTIAL" : "FAIL",
    detail: { issues, target: shapeFile, loaded, whale, unverified: "sanitized totals only; event sizes, session sizes, share over 4 KB and growth are assumed" },
  };
  console.log(`${report.G6_shape.status.padEnd(9)} G6_shape loaded=${loaded} whale=${whale}`);
}

const pct = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  const p = (q: number) => Math.round((percentile(s, q) ?? 0) * 10) / 10;
  return { n: s.length, p50: p(50), p95: p(95), p99: p(99) };
};

async function g7g8() {
  const mix = NEEDLES.flatMap((n) => [0, 1].map((org) => ({ q: n.query, scope: orgScope(org) })));
  await admin.run("SYSTEM DROP MARK CACHE").catch(() => {});
  await admin.run("SYSTEM DROP UNCOMPRESSED CACHE").catch(() => {});
  await admin.run("SYSTEM DROP QUERY CACHE").catch(() => {});
  const cold: number[] = [];
  for (const m of mix) { const s = performance.now(); await search.search(m.scope, { query: m.q }); cold.push(performance.now() - s); }
  const warm: number[] = [];
  for (const m of mix) { const s = performance.now(); await search.search(m.scope, { query: m.q }); warm.push(performance.now() - s); }
  const levels: unknown[] = [];
  const freshness: number[] = [];
  const scope = scopeOf("org-fresh", ["ws-fresh"]);
  for (const clients of [1, 8, 32]) {
    let stop = false, errors = 0;
    const lat: number[] = [];
    const worker = async (id: number) => {
      for (let i = id; !stop; i++) {
        const m = mix[i % mix.length]!;
        const s = performance.now();
        try { await search.search(m.scope, { query: m.q }); lat.push(performance.now() - s); } catch { errors++; }
      }
    };
    const ingest = async () => {
      for (let i = 0; !stop; i++) {
        const term = `freshterm${clients}x${i}`;
        const started = performance.now();
        await search.insert([{
          orgId: "org-fresh", workspaceId: "ws-fresh", harness: "claude", sessionId: `s-f-${clients}-${i}`, machineId: "m1", eventId: term,
          revision: 1, chunkSeq: 0, recordSeq: 0, ts: new Date().toISOString(), kind: "message", scrubVersion: 1, text: term,
        }]);
        while (!(await search.search(scope, { query: term, limit: 1 })).hits.length) await Bun.sleep(5);
        freshness.push(performance.now() - started);
        await Bun.sleep(200);
      }
    };
    const started = performance.now();
    const running = [...Array.from({ length: clients }, (_, i) => worker(i)), ingest()];
    await Bun.sleep(loadSeconds * 1000);
    stop = true;
    await Promise.all(running);
    const seconds = (performance.now() - started) / 1000;
    levels.push({ clients, qps: Math.round(lat.length / seconds), errors, errorRate: errors / Math.max(1, errors + lat.length), ...pct(lat) });
  }
  report.G7_latency_load = { status: "RELATIVE", detail: { engine: "vanilla ClickHouse, one local node", coldMs: pct(cold), warmMs: pct(warm), levels } };
  report.G8_freshness = { status: "RELATIVE", detail: { insertToFirstHitMs: pct(freshness), note: "store only; pipeline queue latency is in G1 firstHitMs" } };
  console.log("RELATIVE  G7_latency_load", JSON.stringify(report.G7_latency_load.detail));
  console.log("RELATIVE  G8_freshness", JSON.stringify(report.G8_freshness.detail));
}

async function g10() {
  const scope = orgScope(1);
  const { hits } = await search.search(scope, { query: "tailbeacon", limit: 1 });
  const hit = hits[0];
  if (!hit) { gate("G10_delete_logical", { hasTarget: false }); return; }
  await search.deleteSession(scope.orgId, hit.workspace_id, hit.session_id);
  const after = await search.context(scope, { sessionId: hit.session_id, query: "tailbeacon" });
  const direct = await search.search(scope, { query: "tailbeacon", limit: 20 });
  gate("G10_delete_logical", { contextGone: after.length === 0, searchGone: !direct.hits.some((h) => h.session_id === hit.session_id) },
    { physicalPurge: "gate 3 of #70; target-engine purge not measured" });
}

/** Each control breaks one protection on purpose. A control that stays green means the gate has no teeth. */
async function g0Controls() {
  const checks: Record<string, boolean> = {};
  const mark = "controlneedle";
  const row = (orgId: string, workspaceId: string, id: string, text: string) => ({
    orgId, workspaceId, harness: "claude", sessionId: `s-${id}`, machineId: "m1", eventId: id, revision: 1, chunkSeq: 0, recordSeq: 0,
    ts: "2026-10-30T10:00:00.000Z", kind: "message", scrubVersion: 1, text,
  });
  await search.insert([row("org-ctl-a", "ws-ctl-a", "ctl-1", mark), row("org-ctl-b", "ws-ctl-b", "ctl-2", "ndl cont rolne edle cne")]);
  // Tenant: without the org predicate the admin sees both orgs' canary.
  const unscoped = Number((await admin.run(`SELECT count() FROM session_search_records WHERE hasAllTokens(text, '${mark}')`)).trim());
  const scoped = (await search.search(scopeOf("org-ctl-b", ["ws-ctl-b"]), { query: mark })).hits.length;
  checks.unscopedSeesData = unscoped >= 1 && scoped === 0;
  // Recheck: scattered grams match the index alone, so the substring check is doing work.
  await search.insert([row("org-ctl-a", "ws-ctl-a", "ctl-3", "abcd bcde cdef abcdzz"), row("org-ctl-a", "ws-ctl-a", "ctl-4", "abc xx bcd yy cde zz def")]);
  const grams = Number((await admin.run(`SELECT count() FROM session_search_records WHERE org_id = 'org-ctl-a' AND hasAllTokens(text, 'abcdef')`)).trim());
  const rechecked = (await search.search(scopeOf("org-ctl-a", ["ws-ctl-a"]), { query: "abcdef" })).hits.length;
  checks.recheckRemovesScatteredGrams = grams >= 1 && rechecked === 0;
  // Dedup: the same batch without a token is two copies.
  const dup = [row("org-ctl-a", "ws-ctl-a", "ctl-5", "dupcontrol")];
  await search.insert(dup, "ctl-token"); await search.insert(dup, "ctl-token");
  const tokened = await countRows("event_id = 'ctl-5'");
  await search.insert(dup); await search.insert(dup);
  checks.dedupTokenMatters = tokened === 1 && (await countRows("event_id = 'ctl-5'")) > 1;
  // Oracle: a recall check over a wrong answer must score below 1.
  checks.recallCheckCanFail = recallAt(5, ["a", "b"], ["a", "c"]) < 1;
  gate("G0_negative_controls", checks);
}

await installSchema(admin, pw);
await g0Controls();
await g1();
const oracle = new Oracle();
let loaded = 0;
const loadStart = performance.now();
for (const batch of transcriptBatches(params, oracle)) { await search.insert(batch); loaded += batch.length; }
console.log(`loaded ${loaded} events in ${Math.round((performance.now() - loadStart) / 1000)}s`);
// The first search after a bulk load, without the 5 s limit, so a miss has a number.
const parts = Number((await admin.run("SELECT count() FROM system.parts WHERE table = 'session_search_records' AND active")).trim());
const probeStart = performance.now();
await admin.run(`SELECT count() FROM session_search_records WHERE org_id = 'org-0' AND hasAllTokens(text, 'flaky retry loop') AND positionCaseInsensitiveUTF8(text, 'flaky retry loop') > 0`, {}, { max_execution_time: 120 });
const firstQueryAfterLoad = { activeParts: parts, ms: Math.round(performance.now() - probeStart), over5sLimit: performance.now() - probeStart > 5000 };
console.log("first query after load", JSON.stringify(firstQueryAfterLoad));
await g2(oracle);
await g3(oracle);
await g4();
await g5();
await g6(loaded);
await g7g8();
await g10();
report.G9_noisy_neighbour = { status: "INHERITED_FAIL", detail: "#233 gate 7 fails against 2x; not rerun here" };
report.G11_cost_teardown = { status: "NOT_RUN", detail: "plan in docs/session-search-proof.md; no account exists" };
const summary = { shapeFidelity: SHAPE_FIDELITY, firstQueryAfterLoad, scale: scaleName, corpus: corpusHash(params), querySet: QUERY_SET_VERSION, engine: (await admin.run("SELECT version()")).trim(), loaded, gates: report };
mkdirSync(dirname(out), { recursive: true });
writeFileSync(out, JSON.stringify(summary, null, 2));
console.log(`report: ${out}`);
const failed = Object.entries(report).filter(([, g]) => g.status === "FAIL").map(([k]) => k);
if (failed.length) { console.error(`failed: ${failed.join(", ")}`); process.exit(1); }
process.exit(0);
