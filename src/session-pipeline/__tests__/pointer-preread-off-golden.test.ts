/**
 * SESSION_POINTER_PREREAD_ENABLED unset must leave the pointer stage byte-identical to the base commit: every
 * object-store call, every SQL statement (with its parameters, normalized for trace ids), whether each ran inside
 * the scope lock, the batch results and the stage's log records, for a put, the same put again, a newer revision,
 * a moved session, a move in progress and a delete, compared to a golden captured at the base commit before the
 * pre-read existed (pointer-lock-harness.ts).
 *
 * Regenerate (only on purpose, from the base commit):
 *   UPDATE_POINTER_PREREAD_OFF_GOLDEN=1 bun test ./src/session-pipeline/__tests__/pointer-preread-off-golden.test.ts
 *
 * Synthetic, secret-free data only.
 */
import { afterEach, expect, test } from "bun:test";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { objectKey } from "../../shared/vendor.ts";
import { loc, pointerRig } from "./pointer-lock-harness.ts";

const GOLDEN = join(import.meta.dir, "fixtures", "pointer-preread-off.golden.json");
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
          // The fs store's etags are inode, mtime and size: stable only within one run.
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
    const lines: Entry[] = [];
    const stdout = process.stdout.write, stderr = process.stderr.write;
    const tap = (w: typeof process.stdout.write, stream: NodeJS.WriteStream) =>
      ((chunk: unknown, ...rest: unknown[]) => {
        for (const line of String(chunk).split("\n"))
          if (line.startsWith("{"))
            try {
              const { ts: _ts, duration_ms: _d, ...entry } = JSON.parse(line) as Entry;
              if (entry.stage === "session-indexed" || entry.stage === "object-landed") lines.push(entry);
            } catch {}
        return (w as (...a: unknown[]) => boolean).call(stream, chunk, ...rest);
      }) as typeof process.stdout.write;
    process.stdout.write = tap(stdout, process.stdout);
    process.stderr.write = tap(stderr, process.stderr);
    let result: unknown;
    try {
      result = await act();
    } finally {
      process.stdout.write = stdout;
      process.stderr.write = stderr;
    }
    out[name] = normalize({ result, calls: [...r.calls], log: lines, rows: await r.row() }) as Entry;
  };
  let key = await r.writeManifest(1);
  await step("put revision 1", () => r.run("a", r.body(key, "ObjectCreated:Put", "01")));
  await step("the same put again", () => r.run("a2", r.body(key, "ObjectCreated:Put", "01")));
  key = await r.writeManifest(2);
  await step("put revision 2", () => r.run("b", r.body(key, "ObjectCreated:Put", "02")));
  // The exporter moved the start date: a new key under an earlier date, the old key erased.
  const moved = { ...loc, startDate: "2026-10-31" };
  const keyB = await r.writeManifest(1, moved);
  await step("move in progress (both keys live)", () => r.run("m1", r.body(keyB, "ObjectCreated:Put", "03")));
  await r.store.delete(objectKey(key));
  await step("move (old key erased)", () => r.run("m2", r.body(keyB, "ObjectCreated:Put", "04")));
  await r.store.delete(objectKey(keyB));
  await step("delete (no sibling left)", () => r.run("d", r.body(keyB, "ObjectRemoved:DeleteMarkerCreated", "05")));
  return out;
}

test("pointer pre-read unset: object-store calls, SQL, lock placement, results and logs equal the base commit's golden", async () => {
  const got = await record();
  if (process.env.UPDATE_POINTER_PREREAD_OFF_GOLDEN === "1") writeFileSync(GOLDEN, JSON.stringify(got, null, 1) + "\n");
  const golden = JSON.parse(readFileSync(GOLDEN, "utf8")) as Record<string, Entry>;
  expect(Object.keys(got)).toEqual(Object.keys(golden));
  for (const k of Object.keys(golden)) expect(got[k]).toEqual(golden[k]!);
  // The golden shows today's problem: object-store calls inside the lock (the reads, and the probe on delete).
  const inLock = (k: string) => ((golden[k]!.calls as Array<{ at: string; kind: string }>).filter((c) => c.at === "lock" && c.kind.startsWith("s3.")).length);
  expect(inLock("put revision 1")).toBeGreaterThan(0);
  expect(inLock("delete (no sibling left)")).toBeGreaterThan(90);
}, 300_000);
