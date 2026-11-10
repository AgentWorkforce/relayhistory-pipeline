import { describe, expect, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import type { ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import { EVENTS_TABLE } from "../vendor/search-index/schema.ts";
import { createPipelineLambdaHandler, lambdaEnv, runtimeFromSecret } from "../lambda.ts";
import { SUPABASE_PROD_CA_2021 } from "../supabase-ca.ts";
import { BUFFERED_SEARCH_FEED_RUNTIME_ROLE } from "../buffered-feed-role.ts";
import { BUFFERED_SEARCH_GATE_CONTRACT } from "../../session-read-api/buffered-search-gate.ts";
import {
  FINGERPRINT_ORDINALS_SQL,
  FINGERPRINT_STAGE_SQL,
  bufferedFingerprint,
  parseBufferedFingerprint,
  type StageState,
  type StageStatus,
} from "../buffered-search-feed.ts";
import {
  BUFFERED_ERASURE_CURSOR,
  bufferedErasureConfig,
  runBufferedErasureDelete,
  type BufferedErasureDeps,
} from "../buffered-erasure-delete.ts";

const ORG = "org-a", WS = "ws-a", OTHER_WS = "ws-b", USER = "user-1";
const sha = (v: string) => createHash("sha256").update(v).digest("hex");
const GATE = { BUFFERED_SEARCH_FEED_GATE_DECLARED: BUFFERED_SEARCH_GATE_CONTRACT };
const CONFIG = { pageSize: 25, stagesPerSecond: 20, budgetMs: 40_000 };

interface Row {
  org_id: string; workspace_id: string; session_id: string; machine_id: string; source: string; kind: string; event_id: string;
  ts: string; origin: string; origin_claim_token: string; fingerprint: string; version: number;
}

/** A canonical-looking s3 fingerprint body (the feeder keeps the canonical body after `bf`). */
const body = (seed: string) => sha(seed).slice(0, 14);
let seq = 0;
function bufferedRow(claim: string, stage: string, ordinals: number[], over: Partial<Row> = {}): Row {
  seq += 1;
  return {
    org_id: ORG, workspace_id: WS, session_id: `s-${seq % 3}`, machine_id: "m-1", source: "claude", kind: "session_event", event_id: `e-${seq}`,
    ts: `2026-11-07 11:${String(seq % 60).padStart(2, "0")}:00.${String(seq % 1000).padStart(3, "0")}`,
    origin: "buffered", origin_claim_token: claim, fingerprint: bufferedFingerprint(`s3${body(String(seq))}`, stage, ordinals), version: 1_762_515_000_000 + seq, ...over,
  };
}
/** The canonical row for the same event identity as `r` (different provenance, older version). */
const canonicalOf = (r: Row, version = r.version - 1): Row => ({ ...r, origin: "canonical", origin_claim_token: "", fingerprint: `s3${body(r.event_id)}`, version });

/** JS twins of the SQL extractors, so the fake filters as ClickHouse would. */
const sqlRegex = (sql: string) => new RegExp(/extract\(fingerprint, '([^']+)'\)/.exec(sql)![1]!);
const STAGE_RE = sqlRegex(FINGERPRINT_STAGE_SQL), ORDS_RE = sqlRegex(FINGERPRINT_ORDINALS_SQL);
const stageOf = (fp: string) => STAGE_RE.exec(fp)?.[1] ?? "";
const ordsOf = (fp: string) => (ORDS_RE.exec(fp)?.[1] ?? "").split(",").filter((x) => x !== "");

/** In-memory transcript_events + search_ingest_state speaking exactly this lane's statements. */
function fakeClickHouse(seed: Row[], opts: { ignoreDeletes?: boolean; mutate?: (r: Record<string, unknown>) => void } = {}) {
  const rows = [...seed];
  const state: { name: string; value: string; updated_at: string }[] = [];
  const log = { deletes: 0, deleteKeys: 0, selects: 0, pageSelects: 0, eventInserts: 0, stateWrites: 0 };
  const tsMs = (ts: string) => Date.parse(`${ts.replace(" ", "T")}Z`);
  const ch: ClickHouseClient = {
    database: "test",
    async command(query) {
      // The only statement this lane may send: deleteBufferedRows' bound 11-tuple DELETE.
      expect(query.trimStart().startsWith(`DELETE FROM ${EVENTS_TABLE}`)).toBe(true);
      expect(query).toContain("origin_claim_token, fingerprint, version)");
      expect(query).toContain("AND origin = 'buffered'");
      expect(query).not.toContain("startsWith(");
      log.deletes += 1;
      const token = /'((?:[^'\\]|\\.)*)'|(\d+)/g;
      const tuples = [...query.slice(query.indexOf("IN [")).matchAll(/\(((?:'(?:[^'\\]|\\.)*'|\d+)(?:,(?:'(?:[^'\\]|\\.)*'|\d+))*)\)/g)]
        .map((m) => [...m[1]!.matchAll(token)].map((t) => t[1] ?? t[2]!));
      for (const t of tuples) {
        expect(t.length).toBe(11);
        log.deleteKeys += 1;
        if (opts.ignoreDeletes) continue;
        const [org, session, ws, machine, source, kind, eventId, ms, claim, fingerprint, version] = t;
        for (let i = rows.length - 1; i >= 0; i--) {
          const r = rows[i]!;
          if (r.org_id === org && r.session_id === session && r.workspace_id === ws && r.machine_id === machine && r.source === source && r.kind === kind &&
              sha(r.event_id) === eventId && tsMs(r.ts) === Number(ms) && r.origin === "buffered" && r.origin_claim_token === claim && r.fingerprint === fingerprint && r.version === Number(version))
            rows.splice(i, 1);
        }
      }
    },
    async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
      log.selects += 1;
      const p = options?.params ?? {};
      if (query.includes("system.columns"))
        // assertOriginColumns also checks default_kind (a plain DEFAULT stores the inserted origin).
        return [{ name: "origin", type: "LowCardinality(String)", default_kind: "DEFAULT", default_expression: "'canonical'" }, { name: "origin_claim_token", type: "String", default_kind: "DEFAULT", default_expression: "''" }] as T[];
      if (query.includes("search_ingest_state")) {
        const held = state.filter((s) => s.name === p.name).sort((a, b) => a.updated_at.localeCompare(b.updated_at)).at(-1);
        return (held ? [{ value: held.value, n: 1 }] : []) as T[];
      }
      expect(query).toContain("origin = 'buffered'");
      if (query.includes("GROUP BY claim, stage, org")) {
        log.pageSelects += 1;
        const seen = new Map<string, { claim: string; stage: string; org: string }>();
        for (const r of rows.filter((r) => r.origin === "buffered")) {
          const pair = { claim: r.origin_claim_token, stage: stageOf(r.fingerprint), org: r.org_id };
          seen.set(JSON.stringify([pair.claim, pair.stage, pair.org]), pair);
        }
        const key = (x: { claim: string; stage: string; org: string }) => [x.claim, x.stage, x.org];
        const cmp = (a: string[], b: string[]) => { for (let i = 0; i < 3; i++) if (a[i] !== b[i]) return a[i]! < b[i]! ? -1 : 1; return 0; };
        const after = "after_claim" in p ? [String(p.after_claim), String(p.after_stage), String(p.after_org)] : null;
        return [...seen.values()].filter((x) => !after || cmp(key(x), after) > 0).sort((a, b) => cmp(key(a), key(b))).slice(0, Number(p.limit)) as T[];
      }
      expect(query).toContain("NOT hasAny(");
      const keep = p.keep as string[];
      return rows
        .filter((r) => r.org_id === p.org && r.workspace_id === p.workspace && r.origin === "buffered" && r.origin_claim_token === p.claim &&
          stageOf(r.fingerprint) === p.stage && !ordsOf(r.fingerprint).some((o) => keep.includes(o)))
        .slice(0, Number(p.limit))
        .map((r) => { const out: Record<string, unknown> = { ...r, version: String(r.version) }; opts.mutate?.(out); return out; }) as T[];
    },
    async insert(table, batch) {
      if (table === EVENTS_TABLE) { log.eventInserts += 1; throw new Error("the erasure lane never inserts events"); }
      expect(table).toBe("search_ingest_state");
      log.stateWrites += 1;
      state.push(...(batch as typeof state));
    },
  };
  return { ch, rows, state, log };
}

type Answer = Partial<StageState> & { status: StageStatus };
/** The 043870 `stage` RPC as the lane sees it, keyed by stage token. */
function stageSource(answers: Record<string, Answer | Error>, claims: Record<string, string>) {
  const calls: string[] = [];
  const source: BufferedErasureDeps["source"] = {
    async stage(ref) {
      if (!("stageToken" in ref)) throw new Error("the lane looks stages up by token only");
      calls.push(ref.stageToken);
      const a = answers[ref.stageToken];
      if (a instanceof Error) throw a;
      if (!a) return { found: false, status: "gone", reason: "stage_absent", retained: [] };
      return { found: true, reason: null, retained: [], claimToken: claims[ref.stageToken], tenant: { orgId: ORG, workspaceId: WS, userId: USER }, originId: "o", batchId: "b", ...a };
    },
  };
  return { source, calls };
}

function clock(start = Date.parse("2026-11-08T12:00:00.000Z")) {
  let t = start;
  return { now: () => t, sleep: async (ms: number) => { t += ms; }, advance: (ms: number) => { t += ms; } };
}

/** One claim per stage, `n` rows over the given ordinal sets. */
function claimRows(ordinalSets: number[][], over: Partial<Row> = {}) {
  const claim = randomUUID(), stage = randomUUID();
  return { claim, stage, rows: ordinalSets.map((o) => bufferedRow(claim, stage, o, over)) };
}

describe("buffered later-erasure delete lane", () => {
  test("live, pending and deferred keep every row; stage_absent keeps too", async () => {
    const live = claimRows([[0], [1]]), pending = claimRows([[0]]), deferred = claimRows([[2]]), absent = claimRows([[0]]);
    const ch = fakeClickHouse([...live.rows, ...pending.rows, ...deferred.rows, ...absent.rows]);
    const src = stageSource(
      { [live.stage]: { status: "live", retained: [0, 1] }, [pending.stage]: { status: "pending", reason: "stage_preparing" }, [deferred.stage]: { status: "deferred", reason: "gc_in_flight" } },
      { [live.stage]: live.claim, [pending.stage]: pending.claim, [deferred.stage]: deferred.claim });
    const c = clock();
    const s = await runBufferedErasureDelete({ source: src.source, ch: ch.ch, ...c }, CONFIG);
    expect(s).toMatchObject({ pairs: 4, checked: 4, kept: 4, erased: 0, deleted: 0, failed: 0, caughtUp: true });
    expect(s.reasons).toMatchObject({ live: 1, stage_preparing: 1, gc_in_flight: 1, stage_absent: 1 });
    expect(ch.rows.length).toBe(5);
    expect(ch.log.deletes).toBe(0);
    // Idempotent: a second pass changes nothing.
    expect(await runBufferedErasureDelete({ source: src.source, ch: ch.ch, ...c }, CONFIG)).toMatchObject({ deleted: 0, kept: 4 });
  });

  test("gone and refused delete exactly that claim's rows; other claims and canonical rows stay", async () => {
    const gone = claimRows([[0], [1], [2]]), refused = claimRows([[0], [3]]), live = claimRows([[0], [1]]);
    // The same event identity carried by another claim and by canonical: never matched.
    const sameEventOtherClaim = { ...gone.rows[0]!, origin_claim_token: live.claim, fingerprint: bufferedFingerprint(`s3${body("x")}`, live.stage, [0]) };
    const canonical = [canonicalOf(gone.rows[0]!), canonicalOf(refused.rows[1]!, refused.rows[1]!.version + 5)];
    const ch = fakeClickHouse([...gone.rows, ...refused.rows, ...live.rows, sameEventOtherClaim, ...canonical]);
    const src = stageSource(
      { [gone.stage]: { status: "gone", reason: "batch_not_buffered" }, [refused.stage]: { status: "refused", reason: "retention_refused" }, [live.stage]: { status: "live", retained: [0, 1] } },
      { [gone.stage]: gone.claim, [refused.stage]: refused.claim, [live.stage]: live.claim });
    const s = await runBufferedErasureDelete({ source: src.source, ch: ch.ch, ...clock() }, CONFIG);
    expect(s).toMatchObject({ pairs: 3, checked: 3, erased: 2, kept: 1, deleted: 5, failed: 0, caughtUp: true });
    expect(s.reasons).toMatchObject({ batch_not_buffered: 1, retention_refused: 1, live: 1 });
    expect(ch.rows.filter((r) => r.origin_claim_token === gone.claim || r.origin_claim_token === refused.claim)).toEqual([]);
    expect(ch.rows).toContainEqual(sameEventOtherClaim);
    for (const r of [...live.rows, ...canonical]) expect(ch.rows).toContainEqual(r);
    expect(ch.rows.length).toBe(live.rows.length + 1 + canonical.length);
    // Idempotent: nothing left to delete.
    const again = await runBufferedErasureDelete({ source: src.source, ch: ch.ch, ...clock() }, CONFIG);
    expect(again).toMatchObject({ pairs: 1, deleted: 0 });
  });

  test("a retired or redacted stage keeps its retained ordinals' rows (any overlap keeps the row)", async () => {
    const parent = claimRows([[0], [1], [2], [1, 4], [5, 6], []]);
    const ch = fakeClickHouse(parent.rows);
    const src = stageSource({ [parent.stage]: { status: "gone", reason: "stage_redacted", retained: [1, 2] } }, { [parent.stage]: parent.claim });
    const s = await runBufferedErasureDelete({ source: src.source, ch: ch.ch, ...clock() }, CONFIG);
    expect(s).toMatchObject({ erased: 1, deleted: 3 });
    expect(ch.rows.map((r) => parseBufferedFingerprint(r.fingerprint)!.ordinals)).toEqual([[1], [2], [1, 4]]);
  });

  test("unbound rows refuse the whole pair; legacy, malformed and mismatched pairs are skipped untouched", async () => {
    const forged = claimRows([[0], [1]]);
    const ch = fakeClickHouse(forged.rows, { mutate: (r) => { if (r.event_id === forged.rows[1]!.event_id) r.version = "0"; } });
    const src = stageSource({ [forged.stage]: { status: "gone", reason: "stage_retired" } }, { [forged.stage]: forged.claim });
    const s = await runBufferedErasureDelete({ source: src.source, ch: ch.ch, ...clock() }, CONFIG);
    expect(s).toMatchObject({ failed: 1, deleted: 0 });
    expect(s.reasons).toMatchObject({ row_unbound: 1 });
    expect(ch.rows.length).toBe(2);
    expect(ch.log.deletes).toBe(0);

    for (const bad of [(r: Record<string, unknown>) => (r.origin = "canonical"), (r: Record<string, unknown>) => (r.fingerprint = `s3${body("c")}`), (r: Record<string, unknown>) => (r.workspace_id = OTHER_WS), (r: Record<string, unknown>) => (r.ts = "2026-11-07T11:00:00Z")]) {
      const c2 = claimRows([[0]]);
      const ch2 = fakeClickHouse(c2.rows, { mutate: bad });
      const s2 = await runBufferedErasureDelete({ source: stageSource({ [c2.stage]: { status: "refused" } }, { [c2.stage]: c2.claim }).source, ch: ch2.ch, ...clock() }, CONFIG);
      expect(s2).toMatchObject({ failed: 1, deleted: 0, reasons: { row_unbound: 1 } });
      expect(ch2.log.deletes).toBe(0);
    }

    // Legacy bare fingerprint, a malformed stage token, a stage naming another claim or another org.
    const legacy = { ...bufferedRow(randomUUID(), randomUUID(), [0]), fingerprint: `bf${body("legacy")}` };
    const claimMismatch = claimRows([[0]]), orgMismatch = claimRows([[0]]);
    const malformed = { ...bufferedRow("not-a-uuid", randomUUID(), [0]) };
    const ch3 = fakeClickHouse([legacy, malformed, ...claimMismatch.rows, ...orgMismatch.rows]);
    const src3 = stageSource(
      { [claimMismatch.stage]: { status: "gone" }, [orgMismatch.stage]: { status: "gone", tenant: { orgId: "org-z", workspaceId: WS, userId: USER } } },
      { [claimMismatch.stage]: randomUUID(), [orgMismatch.stage]: orgMismatch.claim });
    const s3 = await runBufferedErasureDelete({ source: src3.source, ch: ch3.ch, ...clock() }, CONFIG);
    expect(s3).toMatchObject({ pairs: 4, skipped: 4, deleted: 0, caughtUp: true });
    expect(s3.reasons).toMatchObject({ legacy_unmapped: 1, fingerprint_stage_malformed: 1, stage_claim_mismatch: 1, stage_tenant_mismatch: 1 });
    expect(src3.calls.length).toBe(2);
    expect(ch3.rows.length).toBe(4);
  });

  test("rows in a workspace other than the stage tenant's are never matched", async () => {
    const c = claimRows([[0]]);
    const elsewhere = bufferedRow(c.claim, c.stage, [1], { workspace_id: OTHER_WS });
    const ch = fakeClickHouse([...c.rows, elsewhere]);
    const s = await runBufferedErasureDelete({ source: stageSource({ [c.stage]: { status: "gone" } }, { [c.stage]: c.claim }).source, ch: ch.ch, ...clock() }, CONFIG);
    expect(s).toMatchObject({ deleted: 1 });
    expect(ch.rows).toEqual([elsewhere]);
  });

  test("an error on one pair is counted and the tick continues", async () => {
    const broken = claimRows([[0]]), fine = claimRows([[0]]);
    const ch = fakeClickHouse([...broken.rows, ...fine.rows]);
    const src = stageSource({ [broken.stage]: Object.assign(new Error("x"), {}), [fine.stage]: { status: "gone" } }, { [fine.stage]: fine.claim });
    const s = await runBufferedErasureDelete({ source: src.source, ch: ch.ch, ...clock() }, CONFIG);
    expect(s).toMatchObject({ pairs: 2, failed: 1, erased: 1, deleted: 1, caughtUp: true, reasons: { erase_failed: 1 } });
    expect(ch.rows).toEqual(broken.rows);
  });

  test("a delete that does not apply refuses the pair instead of spinning", async () => {
    const c = claimRows([[0], [1], [2]]);
    const ch = fakeClickHouse(c.rows, { ignoreDeletes: true });
    const s = await runBufferedErasureDelete({ source: stageSource({ [c.stage]: { status: "gone" } }, { [c.stage]: c.claim }).source, ch: ch.ch, ...clock(), rowsPerSelect: 2 }, CONFIG);
    expect(s).toMatchObject({ failed: 1, reasons: { delete_not_applied: 1 } });
    expect(ch.log.deletes).toBe(1);
  });

  test("page and budget bounds: continuation through the persisted cursor, then a fresh pass", async () => {
    const claims = Array.from({ length: 7 }, () => claimRows([[0], [1]]));
    const ch = fakeClickHouse(claims.flatMap((c) => c.rows));
    const src = stageSource(Object.fromEntries(claims.map((c) => [c.stage, { status: "gone" as const }])), Object.fromEntries(claims.map((c) => [c.stage, c.claim])));
    const c = clock();
    // Rate 1/s, budget 5 s: at most five stage checks fit in a run; pages of 2.
    const tight = { pageSize: 2, stagesPerSecond: 1, budgetMs: 5_000 };
    const first = await runBufferedErasureDelete({ source: src.source, ch: ch.ch, ...c }, tight);
    expect(first.caughtUp).toBe(false);
    expect(first.checked).toBeLessThanOrEqual(5);
    expect(first.checked).toBeGreaterThan(0);
    expect(first.cursor).not.toBeNull();
    expect(JSON.parse(ch.state.at(-1)!.value)).toEqual(first.cursor);
    expect(ch.state.every((s) => s.name === BUFFERED_ERASURE_CURSOR)).toBe(true);
    const left = ch.rows.length;
    expect(left).toBe((7 - first.erased) * 2);
    // The next run resumes after the cursor (no pair re-checked) and finishes the pass.
    const before = src.calls.length;
    const second = await runBufferedErasureDelete({ source: src.source, ch: ch.ch, ...c }, tight);
    expect(new Set(src.calls.slice(before)).size).toBe(src.calls.length - before);
    expect(src.calls.slice(before).some((t) => src.calls.slice(0, before).includes(t))).toBe(false);
    expect(first.erased + second.erased).toBe(7);
    expect(second.caughtUp).toBe(true);
    expect(ch.state.at(-1)!.value).toBe("");
    expect(ch.rows).toEqual([]);

    // Rows per select bound a pair's reads; the pair is finished across selects.
    const big = claimRows(Array.from({ length: 9 }, (_, i) => [i]));
    const ch2 = fakeClickHouse(big.rows);
    const s2 = await runBufferedErasureDelete({ source: stageSource({ [big.stage]: { status: "gone" } }, { [big.stage]: big.claim }).source, ch: ch2.ch, ...clock(), rowsPerSelect: 4 }, CONFIG);
    expect(s2).toMatchObject({ deleted: 9, erased: 1, caughtUp: true });
    expect(ch2.log.deletes).toBe(3);

    // Budget running out mid-pair: the cursor stays before the pair, which is finished next run.
    const c3 = clock();
    const ch3 = fakeClickHouse(claimRows(Array.from({ length: 9 }, (_, i) => [i])).rows);
    const pair = ch3.rows[0]!;
    const stage3 = parseBufferedFingerprint(pair.fingerprint)!.stageToken;
    const slowDeps: BufferedErasureDeps = { source: stageSource({ [stage3]: { status: "gone" } }, { [stage3]: pair.origin_claim_token }).source, ch: ch3.ch, ...c3, rowsPerSelect: 4 };
    const original = ch3.ch.command.bind(ch3.ch);
    ch3.ch.command = async (q, o) => { c3.advance(3_000); return original(q, o); };
    const mid = await runBufferedErasureDelete(slowDeps, { pageSize: 25, stagesPerSecond: 20, budgetMs: 5_000 });
    expect(mid).toMatchObject({ caughtUp: false, cursor: null, reasons: { budget_mid_pair: 1 } });
    expect(ch3.rows.length).toBe(1);
    const done = await runBufferedErasureDelete(slowDeps, { pageSize: 25, stagesPerSecond: 20, budgetMs: 40_000 });
    expect(done).toMatchObject({ caughtUp: true, deleted: 1 });
    expect(ch3.rows).toEqual([]);
  });

  test("an invalid stored cursor restarts the pass and is counted", async () => {
    const c = claimRows([[0]]);
    const ch = fakeClickHouse(c.rows);
    ch.state.push({ name: BUFFERED_ERASURE_CURSOR, value: "{not json", updated_at: "2026-11-08 00:00:00.000" });
    const s = await runBufferedErasureDelete({ source: stageSource({ [c.stage]: { status: "live" } }, { [c.stage]: c.claim }).source, ch: ch.ch, ...clock() }, CONFIG);
    expect(s).toMatchObject({ pairs: 1, caughtUp: true, reasons: { cursor_invalid: 1 } });
    expect(ch.state.at(-1)!.value).toBe("");
  });

  test("fingerprint binding: SQL extractors agree with the parser; unbindable inputs refuse", () => {
    const stage = randomUUID();
    for (const ords of [[], [0], [3, 1, 3], [999]]) {
      const fp = bufferedFingerprint(`s3${body("a")}`, stage, ords);
      expect(stageOf(fp)).toBe(stage);
      expect(ordsOf(fp).map(Number)).toEqual(parseBufferedFingerprint(fp)!.ordinals);
    }
    expect(parseBufferedFingerprint(bufferedFingerprint(`s3${body("a")}`, stage, [3, 1, 3]))).toEqual({ stageToken: stage, ordinals: [1, 3] });
    for (const fp of [`bf${body("a")}`, `s3${body("a")}:${stage}:0`, `bf${body("a")}:${stage}:01`, `bf${body("a")}:nope:0`]) {
      expect(parseBufferedFingerprint(fp)).toBeNull();
    }
    expect(stageOf(`bf${body("a")}`)).toBe("");
    expect(() => bufferedFingerprint(`s3${body("a")}`, "nope", [0])).toThrow(expect.objectContaining({ code: "fingerprint_unbindable" }));
    expect(() => bufferedFingerprint(`md5x`, stage, [0])).toThrow(expect.objectContaining({ code: "fingerprint_unbindable" }));
  });

  test("config: default off; only the literal 'on' arms it; gate and bounds refuse", () => {
    expect(bufferedErasureConfig({})).toEqual({ enabled: false, pageSize: 25, stagesPerSecond: 2, budgetMs: 40_000 });
    expect(bufferedErasureConfig({ INGEST_BUFFERED_ERASURE_DELETE: "on", ...GATE }).enabled).toBe(true);
    expect(bufferedErasureConfig({ ...GATE }).enabled).toBe(false);
    for (const v of ["1", "true", "ON", "yes", " on"]) expect(() => bufferedErasureConfig({ INGEST_BUFFERED_ERASURE_DELETE: v, ...GATE })).toThrow(expect.objectContaining({ code: "config_flag_invalid" }));
    expect(() => bufferedErasureConfig({ INGEST_BUFFERED_ERASURE_DELETE: "on" })).toThrow(expect.objectContaining({ code: "gate_not_declared" }));
    expect(() => bufferedErasureConfig({ INGEST_BUFFERED_ERASURE_DELETE: "on", BUFFERED_SEARCH_FEED_GATE_DECLARED: "buffered-search-gate/v0" })).toThrow(expect.objectContaining({ code: "gate_contract_mismatch" }));
    expect(bufferedErasureConfig({ INGEST_BUFFERED_ERASURE_PAGE: "200", INGEST_BUFFERED_ERASURE_RATE: "20", INGEST_BUFFERED_ERASURE_BUDGET_MS: "240000" })).toMatchObject({ pageSize: 200, stagesPerSecond: 20, budgetMs: 240_000 });
    for (const [k, v, code] of [["PAGE", "0", "config_page_invalid"], ["PAGE", "201", "config_page_invalid"], ["PAGE", "1e2", "config_page_invalid"], ["RATE", "0", "config_rate_invalid"], ["RATE", "21", "config_rate_invalid"],
      ["RATE", "-1", "config_rate_invalid"], ["BUDGET_MS", "4999", "config_budget_ms_invalid"], ["BUDGET_MS", "240001", "config_budget_ms_invalid"], ["BUDGET_MS", "40000.5", "config_budget_ms_invalid"]] as const)
      expect(() => bufferedErasureConfig({ [`INGEST_BUFFERED_ERASURE_${k}`]: v })).toThrow(expect.objectContaining({ code }));
  });

  test("lambda env and runtime: default off; a bad setting disarms only this lane; arms with the feeder login", () => {
    const base = { AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "bucket", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true", SESSION_PIPELINE_SECRET_ARN: "arn", SESSION_PIPELINE_QUEUE_URL: "https://sqs", SEARCH_INDEX_ENABLED: "1" };
    expect(lambdaEnv(base).bufferedErasureDelete).toMatchObject({ enabled: false });
    expect(lambdaEnv({ ...base, ...GATE, INGEST_BUFFERED_ERASURE_DELETE: "on" }).bufferedErasureDelete).toMatchObject({ enabled: true });
    expect(lambdaEnv({ ...base, INGEST_BUFFERED_ERASURE_DELETE: "on" }).bufferedErasureDelete).toBeUndefined();
    expect(lambdaEnv({ ...base, ...GATE, INGEST_BUFFERED_ERASURE_DELETE: "on", INGEST_BUFFERED_ERASURE_PAGE: "999" }).bufferedErasureDelete).toBeUndefined();
    const clickhouse = { url: "https://ch.synthetic.invalid", user: "u", password: "p", database: "d" };
    const login = { buffered_search_feed_database_url: `postgresql://${BUFFERED_SEARCH_FEED_RUNTIME_ROLE}:pw-test-only@db.synthetic.invalid:5432/history`, buffered_search_feed_database_ca_pem: SUPABASE_PROD_CA_2021 };
    const rt = (env: Record<string, string>, extra: Record<string, unknown>) => runtimeFromSecret(lambdaEnv(env), JSON.stringify({ hoopdb_url: "postgresql://synthetic.invalid/fixture", clickhouse, ...extra }), "fixture-generation");
    const on = { ...base, ...GATE, INGEST_BUFFERED_ERASURE_DELETE: "on" };
    expect(rt(on, login).bufferedErasure).toBeDefined();
    // The erasure lane alone does not arm the feeder.
    expect(rt(on, login).deps.bufferedFeed).toBeUndefined();
    expect(rt(on, {}).bufferedErasure).toBeUndefined();
    expect(rt(base, login).bufferedErasure).toBeUndefined();
  });

  test("lambda: default off before any runtime load; gate refused before any runtime load", async () => {
    const keys = ["INGEST_BUFFERED_ERASURE_DELETE", "BUFFERED_SEARCH_FEED_GATE_DECLARED"] as const;
    const saved = keys.map((k) => process.env[k]);
    let loaded = 0;
    const handler = createPipelineLambdaHandler({ loadRuntime: async () => { loaded++; throw new Error("synthetic runtime unavailable"); } });
    try {
      // Unset and off: nothing touched, no dependency resolved, whatever is declared.
      delete process.env.INGEST_BUFFERED_ERASURE_DELETE;
      process.env.BUFFERED_SEARCH_FEED_GATE_DECLARED = BUFFERED_SEARCH_GATE_CONTRACT;
      expect(await handler({ bufferedErasureDelete: true })).toEqual({ ok: true });
      process.env.INGEST_BUFFERED_ERASURE_DELETE = "off";
      expect(await handler({ bufferedErasureDelete: true })).toEqual({ ok: true });
      process.env.INGEST_BUFFERED_ERASURE_DELETE = "1";
      expect(await handler({ bufferedErasureDelete: true })).toEqual({ ok: true });
      expect(loaded).toBe(0);
      // On, gate undeclared or stale: refused, still nothing loaded.
      process.env.INGEST_BUFFERED_ERASURE_DELETE = "on";
      delete process.env.BUFFERED_SEARCH_FEED_GATE_DECLARED;
      expect(await handler({ bufferedErasureDelete: true })).toEqual({ ok: false });
      process.env.BUFFERED_SEARCH_FEED_GATE_DECLARED = "buffered-search-gate/v0";
      expect(await handler({ bufferedErasureDelete: true })).toEqual({ ok: false });
      expect(loaded).toBe(0);
      // Declared: the run proceeds to load the runtime (which this test makes fail).
      process.env.BUFFERED_SEARCH_FEED_GATE_DECLARED = BUFFERED_SEARCH_GATE_CONTRACT;
      expect(await handler({ bufferedErasureDelete: true })).toEqual({ ok: false });
      expect(loaded).toBe(1);
    } finally {
      keys.forEach((k, i) => { if (saved[i] === undefined) delete process.env[k]; else process.env[k] = saved[i]; });
    }
  });
});
