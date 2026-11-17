/**
 * The scheduled brief-document publication run (SESSION_BRIEF_DOCUMENTS_PUBLISH=1): bounded rounds
 * of publishBriefDocumentsOnce, and the Lambda branch that is off before any runtime load.
 */
import { afterEach, expect, test } from "bun:test";
import { publishBriefDocumentsTick } from "../brief-documents.ts";
import { createPipelineLambdaHandler } from "../lambda.ts";
import type { SqlExecutor } from "../../../packages/pipeline/src/lib/pipeline/queue.ts";
import type { ClickHouseClient } from "../vendor/search-index/clickhouse.ts";

/** `drained`: the backfill RPC's terminal answer (every scope initialized, no scope named). */
function fixture(plan: Array<{ backfilled: number; claims: number; drained?: boolean }>) {
  let round = 0;
  const inserts: object[][] = [];
  const db: SqlExecutor = {
    async query<T>(sql: string, params?: unknown[]) {
      const step = plan[Math.min(round, plan.length - 1)]!;
      if (sql.includes("backfill_"))
        return { rows: [{ r: step.drained ? { complete: true, processed: 0 } : { orgId: "org", workspaceId: "ws", complete: false, processed: step.backfilled } }] as T[] };
      if (sql.includes("claim_")) {
        round += 1;
        return { rows: [{ r: Array.from({ length: step.claims }, (_, i) => ({
          document: { version: 1, orgId: "org", workspaceId: "ws", source: "codex", sessionId: `r${round}s${i}`, deleted: false, visible: true,
            title: "t", summary: "s", projectId: "", lastActivityAt: "2026-11-01T10:00:00.000000Z" },
          epoch: String(round * 100 + i + 1), hash: "a".repeat(64), leaseNonce: params![0],
        })) }] as T[] };
      }
      if (sql.includes("ack_")) return { rows: [{ n: JSON.parse(params![1] as string).length }] as T[] };
      if (sql.includes("retry_")) return { rows: [] };
      throw new Error("unexpected SQL");
    },
  };
  const ch: ClickHouseClient = {
    database: "fixture",
    async insert(_t, rows) { inserts.push([...rows]); },
    async command() {},
    async select() { return []; },
  };
  return { db, ch, inserts };
}

test("runs rounds until every scope is initialized and nothing is left to claim", async () => {
  // Round 3 is an empty scope (nothing processed, nothing claimed): not the end.
  const f = fixture([{ backfilled: 2, claims: 2 }, { backfilled: 0, claims: 1 }, { backfilled: 0, claims: 0 }, { backfilled: 0, claims: 1, drained: true }, { backfilled: 0, claims: 0, drained: true }]);
  const s = await publishBriefDocumentsTick(f);
  expect(s).toEqual({ rounds: 5, claimed: 4, published: 4, backfilled: 2, caughtUp: true });
  expect(f.inserts.map((rows) => rows.length)).toEqual([2, 1, 1]);
});

test("stops at the round cap and at the time budget without claiming caught up", async () => {
  const busy = () => fixture([{ backfilled: 1, claims: 1 }]);
  expect(await publishBriefDocumentsTick(busy(), { maxRounds: 2 })).toMatchObject({ rounds: 2, caughtUp: false });
  let t = 0;
  expect(await publishBriefDocumentsTick(busy(), { budgetMs: 10, now: () => (t += 6) })).toMatchObject({ rounds: 1, caughtUp: false });
});

test("a failing round throws; earlier rounds stay published", async () => {
  const f = fixture([{ backfilled: 0, claims: 1 }]);
  let calls = 0;
  const ch: ClickHouseClient = { ...f.ch, async insert(t, rows) { if (++calls === 2) throw new Error("ch down"); await f.ch.insert(t, rows); } };
  await expect(publishBriefDocumentsTick({ db: f.db, ch })).rejects.toMatchObject({ reason: "publication_failed" });
  expect(f.inserts).toHaveLength(1);
});

const saved = process.env.SESSION_BRIEF_DOCUMENTS_PUBLISH;
afterEach(() => {
  if (saved === undefined) delete process.env.SESSION_BRIEF_DOCUMENTS_PUBLISH;
  else process.env.SESSION_BRIEF_DOCUMENTS_PUBLISH = saved;
});

test("Lambda branch: off unless exactly \"1\", before any runtime load", async () => {
  let loaded = 0;
  const handler = createPipelineLambdaHandler({ loadRuntime: async () => { loaded++; throw new Error("synthetic runtime unavailable"); } });
  for (const v of [undefined, "", "0", "true", "on", " 1"]) {
    if (v === undefined) delete process.env.SESSION_BRIEF_DOCUMENTS_PUBLISH;
    else process.env.SESSION_BRIEF_DOCUMENTS_PUBLISH = v;
    expect(await handler({ briefDocuments: true } as never)).toEqual({ ok: true });
  }
  expect(loaded).toBe(0);
  process.env.SESSION_BRIEF_DOCUMENTS_PUBLISH = "1";
  expect(await handler({ briefDocuments: true } as never)).toEqual({ ok: false });
  expect(loaded).toBe(1);
});

test("Lambda branch: without a ClickHouse search binding it publishes nothing and reports not ok", async () => {
  process.env.SESSION_BRIEF_DOCUMENTS_PUBLISH = "1";
  const f = fixture([{ backfilled: 1, claims: 1 }]);
  let queried = 0;
  const db: SqlExecutor = { async query(sql, params) { queried++; return f.db.query(sql, params); } };
  const handler = createPipelineLambdaHandler({ loadRuntime: async () => ({ deps: { pointer: { db } } }) as never });
  expect(await handler({ briefDocuments: true } as never)).toEqual({ ok: false });
  expect(queried).toBe(0);
  const withCh = createPipelineLambdaHandler({ loadRuntime: async () => ({ deps: { pointer: { db }, search: { ch: f.ch } } }) as never });
  process.env.SESSION_BRIEF_DOCUMENTS_PUBLISH = "1";
  expect(await withCh({ briefDocuments: true } as never)).toEqual({ ok: true });
  expect(f.inserts.length).toBeGreaterThan(0);
});
