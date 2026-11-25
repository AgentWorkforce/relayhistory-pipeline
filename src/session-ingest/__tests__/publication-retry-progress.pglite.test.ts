import { expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { join } from "node:path";
import { postgresPublicationRetryStore } from "../publication-retry-postgres.ts";
import { RetryablePublicationError, runPublicationRetryTurn } from "../publication-retry.ts";
import type { ReceiptPosition } from "../publication-reconciliation.ts";

const pos = (id: number): ReceiptPosition => ({
  createdAt: "2026-11-25T00:00:00.000001Z",
  stageToken: `00000000-0000-4000-8000-${String(id).padStart(12, "0")}`,
});
const schema = await Bun.file(join(import.meta.dir, "fixtures/publication-retry-store.sql")).text();

function origin(scope: string, inventory: number[]) {
  const committed = new Set<number>();
  const held = new Set<number>();
  const poison = new Set<number>();
  const visits: number[] = [];
  return { committed, held, poison, visits, scope,
    highest: async () => inventory.length ? pos(Math.max(...inventory)) : null,
    page: async (after: ReceiptPosition | null, through: ReceiptPosition, limit: number) => {
      return inventory.map(pos).filter(p => (!after || p.stageToken > after.stageToken) && p.stageToken <= through.stageToken)
        .sort((a,b) => a.stageToken < b.stageToken ? -1 : 1).slice(0,limit);
    },
    reconcile: async (token: string) => {
      const id = Number(token.slice(-12));
      visits.push(id);
      if (poison.has(id)) throw new RetryablePublicationError();
      if (held.has(id)) return "held" as const;
      if (committed.has(id)) return "already_published" as const;
      committed.add(id);
      return "published" as const;
    },
  };
}

test("one-item ticks under sustained arrivals revisit older eligible receipts but do not serialize origin effects", async () => {
  const db = new PGlite();
  try {
    await db.exec(schema);
    const inventory = [2,4,6];
    const hot = origin("a".repeat(64), inventory);
    const store = postgresPublicationRetryStore(db, hot.scope);
    hot.held.add(4);
    hot.poison.add(2);
    const tick = async (n: number) => {
      expect(await runPublicationRetryTurn(hot.scope,hot,store,1,n*60001)).toBe("committed");
    };
    await tick(0);
    inventory.push(1); // A committed lower key arrived after checkpointing key 2.
    for (let n=1;n<=28;n++) {
      inventory.push(6+n);
      if (n===10) hot.held.delete(4);
      await tick(n);
    }
    expect(hot.committed.has(1)).toBe(true);
    expect(hot.committed.has(4)).toBe(true);
    expect(await store.lookup(hot.scope,pos(2).stageToken)).toMatchObject({state:"exhausted",attempts:6});
    // The tail can publish later positions before older ones in the same origin.
    const commits = [...hot.committed];
    expect(commits).toContain(7);
    expect(commits.indexOf(7)).toBeLessThan(commits.indexOf(1));
    expect(commits.indexOf(7)).toBeLessThan(commits.indexOf(4));
    expect(hot.committed.size).toBeLessThan(inventory.length-1);
  } finally { await db.close(); }
},60000);

test("quiet and hot origins have independent progress only when the caller schedules both", async () => {
  const db = new PGlite();
  try {
    await db.exec(schema);
    const inventory = [1,2,3,4];
    const hot = origin("a".repeat(64), inventory), quiet = origin("b".repeat(64),[1]);
    const hotStore = postgresPublicationRetryStore(db,hot.scope), quietStore = postgresPublicationRetryStore(db,quiet.scope);
    for (let n=0;n<12;n++) {
      inventory.push(5+n);
      await runPublicationRetryTurn(hot.scope,hot,hotStore,1,n);
    }
    expect(quiet.committed.size).toBe(0);
    expect((await quietStore.load(quiet.scope)).version).toBe(0);
    await runPublicationRetryTurn(quiet.scope,quiet,quietStore,1,12);
    expect([...quiet.committed]).toEqual([1]);
    expect((await hotStore.load(hot.scope)).version).toBe(12);
  } finally { await db.close(); }
},60000);

test("three-origin synthetic rotation isolates persistent failures while arrivals continue every global tick", async () => {
  const db = new PGlite();
  try {
    await db.exec(schema);
    const hotRows = [1, 2], quietRows: number[] = [];
    const hot = origin("a".repeat(64), hotRows);
    const failing = origin("b".repeat(64), [1, 2]);
    const quiet = origin("c".repeat(64), quietRows);
    hot.poison.add(1);
    failing.poison.add(1);

    const sources = [hot, failing, quiet];
    const stores = sources.map(source => postgresPublicationRetryStore(db, source.scope));
    const turns = sources.map(() => [] as number[]);
    let quietPublishedAt: number | undefined;
    let hotPublishedAtMidpoint = 0;
    for (let tick = 0; tick < 90; tick++) {
      hotRows.push(3 + tick);
      if (tick === 3) quietRows.push(1); // Same token as both failing receipts, different scope.
      // The test supplies a rotation. runPublicationRetryTurn schedules work only within the selected scope.
      const i = tick % sources.length;
      const source = sources[i]!;
      turns[i]!.push(tick);
      const outcome = await runPublicationRetryTurn(source.scope, source, stores[i]!, 1, tick * 60_001);
      expect(outcome).toBe("committed");
      if (quiet.committed.has(1)) quietPublishedAt ??= tick;
      if (tick === 44) hotPublishedAtMidpoint = hot.committed.size;
    }

    for (const originTurns of turns) {
      expect(originTurns).toHaveLength(30);
      expect(originTurns.slice(1).every((t, i) => t - originTurns[i]! === 3)).toBe(true);
    }
    // Quiet's turn at 5 is a retry turn. Its next tail scan at 8 sees the new receipt.
    expect(quietPublishedAt).toBe(8);
    expect(quiet.committed.size).toBe(1);
    expect(hot.committed.size).toBeGreaterThan(hotPublishedAtMidpoint);
    expect(hot.committed.size).toBeLessThan(hotRows.length - 1);
    expect([...failing.committed]).toEqual([2]);
    for (const [i, source] of [hot, failing].entries()) {
      expect(source.visits.filter(id => id === 1)).toHaveLength(6);
      expect(await stores[i]!.lookup(source.scope,pos(1).stageToken)).toMatchObject({state:"exhausted",attempts:6});
    }
    expect(await stores[2]!.lookup(quiet.scope,pos(1).stageToken)).toBeNull();
  } finally { await db.close(); }
},60000);
