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
