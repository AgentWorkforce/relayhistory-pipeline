/**
 * SESSION_POINTER_RESOLVE_MOVE_PENDING unset must leave the pointer stage byte-identical to 6d88b638 (the code
 * deployed when the resolution was written): every object-store call, every SQL statement with its parameters,
 * whether each ran inside the scope lock, the batch results, the stage's log records and the pointer row, over the
 * date-move sequences the resolution acts on (a provable move, the same event retried, a stale key's event while
 * the row names the newer key, a multi-hop chain, the erasure, the delete). Compared to a golden captured at
 * 6d88b638, before the resolution existed.
 *
 * Regenerate (only on purpose, from the base commit):
 *   UPDATE_POINTER_MOVE_PENDING_OFF_GOLDEN=1 bun test ./src/session-pipeline/__tests__/pointer-move-pending-off-golden.test.ts
 *
 * Synthetic, secret-free data only.
 */
import { afterEach, expect, test } from "bun:test";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { objectKey } from "../../shared/vendor.ts";
import { at, captureLogs, loc, pointerRig } from "./pointer-move-harness.ts";

const GOLDEN = join(import.meta.dir, "fixtures", "pointer-move-pending-off.golden.json");
const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});

type Entry = Record<string, unknown>;
/** Trace and span ids, fs etags and wall-clock times differ per run: replace them, keep everything else. */
const normalize = (v: unknown): unknown =>
  JSON.parse(JSON.stringify(v), (_k, value) =>
    typeof value === "string"
      ? value
          .replace(/\b00-[0-9a-f]{32}-[0-9a-f]{16}-0[0-9a-f]\b/g, "<traceparent>")
          .replace(/\b[0-9a-f]{5,}-[0-9a-f]{8,}-[0-9a-f]+\b/g, "<etag>")
          .replace(/\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/g, (m) => (m.startsWith("019a1f3c") || m.startsWith("0193a1f3") ? m : "<uuid>"))
          .replace(/\b[0-9a-f]{32}\b/g, "<trace>")
          .replace(/\b\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}(:\d{2})?)?/g, (m) => (m.startsWith("2026-11-0") || m.startsWith("2026-10-") ? m : "<now>"))
      : value,
  );

async function record(): Promise<Record<string, Entry>> {
  const r = await pointerRig();
  cleanup.push(r.close);
  const out: Record<string, Entry> = {};
  const step = async (name: string, act: () => Promise<unknown>) => {
    r.calls.length = 0;
    const { result, lines } = await captureLogs(act);
    out[name] = normalize({ result, calls: [...r.calls], log: lines, rows: await r.row() }) as Entry;
  };
  // The old copy: an exporter session at revision 3 under 2026-11-01.
  const old = at("2026-11-01");
  let keyOld = await r.writeExport(old, { revision: 1, watermarks: { o: 1 }, updatedAt: "2026-11-01T10:00:00.000Z" });
  await step("old key revision 1", () => r.run("a1", r.body(keyOld, "ObjectCreated:Put", "01")));
  keyOld = await r.writeExport(old, { revision: 3, watermarks: { o: 3 }, chunks: [["r1", "r2"]], updatedAt: "2026-11-01T11:00:00.000Z" });
  await step("old key revision 3", () => r.run("a3", r.body(keyOld, "ObjectCreated:Put", "02")));
  // The exporter re-keys under an earlier date: revision 1 there, newer, covering every origin.
  const keyNew = await r.writeExport(at("2026-10-31"), { revision: 1, watermarks: { o: 4 }, chunks: [["r0", "r1", "r2"]], updatedAt: "2026-11-01T12:00:00.000Z" });
  await step("provable move, both keys live", () => r.run("n1", r.body(keyNew, "ObjectCreated:Put", "03")));
  await step("the same event retried", () => r.run("n1", r.body(keyNew, "ObjectCreated:Put", "03")));
  // A not provable move: an open copy at a third date.
  const keyOpen = await r.writeExport(at("2026-10-30"), { revision: 1, state: "open", watermarks: { o: 5 }, updatedAt: "2026-11-01T13:00:00.000Z" });
  await step("multi-hop, third key open", () => r.run("o1", r.body(keyOpen, "ObjectCreated:Put", "04")));
  await r.store.delete(objectKey(keyOpen));
  await step("third key's delete", () => r.run("o2", r.body(keyOpen, "ObjectRemoved:DeleteMarkerCreated", "05")));
  // The old prefix erased: the move completes.
  await r.store.delete(objectKey(keyOld));
  await step("old key erased, new key retried", () => r.run("n2", r.body(keyNew, "ObjectCreated:Put", "06")));
  await step("old key's delete", () => r.run("d1", r.body(keyOld, "ObjectRemoved:DeleteMarkerCreated", "07")));
  // The newer key moves on to revision 3.
  await r.writeExport(at("2026-10-31"), { revision: 3, watermarks: { o: 6 }, chunks: [["r0", "r1", "r2"], ["r3"]], updatedAt: "2026-11-01T14:00:00.000Z" });
  await step("newer key revision 3", () => r.run("n3", r.body(keyNew, "ObjectCreated:Put", "08")));
  // The reverse: the row names the newer key and an older copy reappears at the old key (an event for the stale key).
  keyOld = await r.writeExport(old, { revision: 2, watermarks: { o: 3 }, chunks: [["r1", "r2"]], updatedAt: "2026-11-01T11:00:00.000Z" });
  await step("stale key's put while the row names the newer key", () => r.run("s1", r.body(keyOld, "ObjectCreated:Put", "09")));
  // Today's cross-key revision arithmetic: a stale copy with a higher revision re-keys the row back, silently.
  keyOld = await r.writeExport(old, { revision: 5, watermarks: { o: 3 }, chunks: [["r1", "r2"]], updatedAt: "2026-11-01T11:00:00.000Z" });
  await step("stale key with a higher revision", () => r.run("s2", r.body(keyOld, "ObjectCreated:Put", "10")));
  await r.store.delete(objectKey(keyNew));
  await r.store.delete(objectKey(keyOld));
  await step("delete (no sibling left)", () => r.run("d2", r.body(keyNew, "ObjectRemoved:DeleteMarkerCreated", "11")));
  void loc;
  return out;
}

test("move resolution unset: object-store calls, SQL, lock placement, results, logs and rows equal 6d88b638's golden", async () => {
  const got = await record();
  if (process.env.UPDATE_POINTER_MOVE_PENDING_OFF_GOLDEN === "1") writeFileSync(GOLDEN, JSON.stringify(got, null, 1) + "\n");
  const golden = JSON.parse(readFileSync(GOLDEN, "utf8")) as Record<string, Entry>;
  expect(Object.keys(got)).toEqual(Object.keys(golden));
  for (const k of Object.keys(golden)) expect(got[k]).toEqual(golden[k]!);
  // The golden pins today's failure: the provable move and its retry both fail with pointer_move_pending.
  const failed = (k: string) => (golden[k]!.result as { batchItemFailures: unknown[] }).batchItemFailures.length;
  expect(failed("provable move, both keys live")).toBe(1);
  expect(failed("the same event retried")).toBe(1);
  expect(failed("stale key's put while the row names the newer key")).toBe(1);
  expect(failed("old key erased, new key retried")).toBe(0);
  const codes = (k: string) => (golden[k]!.log as Entry[]).map((l) => l.error_code).filter(Boolean);
  expect(codes("provable move, both keys live")).toEqual(["pointer_move_pending"]);
}, 300_000);
