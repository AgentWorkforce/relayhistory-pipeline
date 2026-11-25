import { expect, test } from "bun:test";
import { initialReconciliationState, reconcilePublicationPage, type ReceiptPosition, type ReconciliationSource } from "../publication-reconciliation.ts";

const pos = (n: number): ReceiptPosition => ({
  createdAt: "2026-11-01T00:00:00.000001Z",
  stageToken: `00000000-0000-0000-0000-${String(n).padStart(12, "0")}`,
});
function fixture(ids: number[]) {
  const seen: number[] = [], published = new Set<number>(), held = new Set<number>();
  const source: ReconciliationSource = {
    highest: async () => ids.length ? pos(Math.max(...ids)) : null,
    page: async (after, through, limit) => ids.map(pos).sort((a,b) => a.stageToken.localeCompare(b.stageToken))
      .filter(p => (!after || p.stageToken > after.stageToken) && p.stageToken <= through.stageToken).slice(0, limit),
    reconcile: async token => {
      const n = Number(token.slice(-12)); seen.push(n);
      if (!ids.includes(n)) return "missing";
      if (held.has(n)) return "held";
      if (published.has(n)) return "already_published";
      published.add(n);
      return "published";
    },
  };
  return { source, seen, published, held };
}

test("alternating bounded work includes tail while catchup revisits late lower keys and held receipts", async () => {
  const rows = [2,4,6], f = fixture(rows); f.held.add(4);
  let state = initialReconciliationState();
  const step = async () => { const result = await reconcilePublicationPage(state,f.source,1); state = result.next; return result.counts; };
  await step(); rows.push(1,7,8);
  await step(); expect(f.seen).toEqual([2,7]);
  expect((await step()).held).toBe(1);
  expect(f.published.has(4)).toBe(false);
  await step(); expect(f.seen).toEqual([2,7,4,8]);
  f.held.clear();
  for (let i=0;i<20;i++) await step();
  expect([...f.published].sort()).toEqual([1,2,4,6,7,8]);
});
test("a partial page retries idempotently from its unmodified input checkpoint", async () => {
  const f=fixture([1,2]), state=initialReconciliationState();
  await expect(reconcilePublicationPage(state,{...f.source,reconcile: async token => {
    if(token===pos(2).stageToken) throw Error("retry"); return f.source.reconcile(token);
  }},2)).rejects.toThrow("retry");
  expect(state).toEqual(initialReconciliationState());
  const r=await reconcilePublicationPage(state,f.source,2);
  expect(r.counts).toEqual({already_published:1,published:1,held:0,missing:0});
});
test("invalid pages refuse before reconciliation and missing receipts confer no authority", async () => {
  const f=fixture([1,2]), state=initialReconciliationState();
  for(const page of [[pos(2),pos(1)],[pos(1),pos(1)],[pos(3)],[pos(1),pos(2),pos(3)]])
    await expect(reconcilePublicationPage(state,{...f.source,page:async()=>page},2)).rejects.toThrow();
  expect(f.seen).toEqual([]);
  expect((await reconcilePublicationPage(state,{...f.source,reconcile:async()=>"missing"},1)).counts.missing).toBe(1);
});
test("rejects corrupted range even on a tail turn", async () => {
  const f = fixture([1,2]);
  const state = initialReconciliationState();
  state.lane = "tail";
  for (const through of [null, pos(1)]) {
    state.catchup = { after: pos(2), through };
    await expect(reconcilePublicationPage(state, f.source, 1)).rejects.toThrow("invalid receipt checkpoint");
  }
  expect(f.seen).toEqual([]);
});
