/**
 * SESSION_POINTER_PREREAD_ENABLED=true (pointer.ts indexPreRead): no object-store call while the scope lock is
 * held, a short lock (SQL only), the same outcomes and rows as the flag-off pass, and the races the lock exists
 * for: a manifest replaced between the pre-read and the lock, a delete against a write in both orders, a move,
 * and a scope that keeps changing. Synthetic, secret-free data only (pointer-lock-harness.ts).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { objectKey } from "../../shared/vendor.ts";
import { lambdaEnv, pointerPreReadFromEnv, runtimeFromSecret } from "../lambda.ts";
import { loc, pointerRig, type Call } from "./pointer-lock-harness.ts";

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});
const ON = { preRead: true };
const inLock = (calls: Call[], kind: (k: string) => boolean) => calls.filter((c) => c.at === "lock" && kind(c.kind)).length;
const s3 = (k: string) => k.startsWith("s3.");
const sql = (k: string) => k === "sql";

/** One scenario run with the flag off and on, on two fresh rigs: same results and rows, no object-store call in the lock. */
async function both(act: (r: Awaited<ReturnType<typeof pointerRig>>, extra: Record<string, unknown>) => Promise<unknown>) {
  const out: Array<{ result: unknown; rows: unknown; calls: Call[] }> = [];
  for (const extra of [{}, ON]) {
    const r = await pointerRig();
    cleanup.push(r.close);
    const result = await act(r, extra);
    const rows = (await r.row()).map((x) => ({ revision: x.revision, deleted: x.deleted_at !== null, manifest_key: x.manifest_key }));
    out.push({ result, rows, calls: [...r.calls] });
  }
  return { off: out[0]!, on: out[1]! };
}

describe("pre-read on: same outcomes, nothing read under the lock, a short lock", () => {
  test("put, the same put again, a newer revision", async () => {
    let lastCalls: Call[] = [];
    const { off, on } = await both(async (r, extra) => {
      const key = await r.writeManifest(1);
      const results = [await r.run("a", r.body(key, "ObjectCreated:Put", "01"), extra)];
      results.push(await r.run("a2", r.body(key, "ObjectCreated:Put", "01"), extra));
      await r.writeManifest(2);
      r.calls.length = 0;
      results.push(await r.run("b", r.body(key, "ObjectCreated:Put", "02"), extra));
      lastCalls = [...r.calls];
      return results;
    });
    expect(on.result).toEqual(off.result);
    expect(on.rows).toEqual(off.rows);
    expect(inLock(off.calls, s3)).toBeGreaterThan(0);
    expect(inLock(on.calls, s3)).toBe(0);
    // The lock holds the same SQL as before plus the stamp re-read; the stamp and the reads moved out.
    expect(inLock(lastCalls, sql)).toBe(inLock(off.calls.slice(-lastCalls.length), sql) + 1);
  }, 300_000);

  test("a move in progress (retried), a move, a delete with the 90-HEAD probe outside the lock", async () => {
    const { off, on } = await both(async (r, extra) => {
      const key = await r.writeManifest(2);
      const results: unknown[] = [await r.run("a", r.body(key, "ObjectCreated:Put", "01"), extra)];
      const keyB = await r.writeManifest(1, { ...loc, startDate: "2026-10-31" });
      results.push(await r.run("m1", r.body(keyB, "ObjectCreated:Put", "03"), extra));
      await r.store.delete(objectKey(key));
      results.push(await r.run("m2", r.body(keyB, "ObjectCreated:Put", "04"), extra));
      await r.store.delete(objectKey(keyB));
      r.calls.length = 0;
      results.push(await r.run("d", r.body(keyB, "ObjectRemoved:DeleteMarkerCreated", "05"), extra));
      return results;
    });
    expect(on.result).toEqual(off.result);
    expect(on.rows).toEqual(off.rows);
    expect((on.result as Array<{ batchItemFailures: unknown[] }>)[1]!.batchItemFailures.length).toBe(1); // pointer_move_pending
    expect(on.rows).toEqual([expect.objectContaining({ deleted: true })]);
    expect(inLock(off.calls, s3)).toBeGreaterThan(90);
    expect(inLock(on.calls, s3)).toBe(0);
  }, 300_000);
});

describe("pre-read on: races", () => {
  test("a newer revision lands between the pre-read and the lock: the stamp moved, the reads are made again", async () => {
    const r = await pointerRig();
    cleanup.push(r.close);
    const key = await r.writeManifest(1);
    await r.run("a", r.body(key, "ObjectCreated:Put", "01"), ON);
    await r.writeManifest(2);
    r.hooks.beforeLock = async () => {
      // Another Lambda indexes revision 3 completely while ours holds a revision-2 read.
      await r.writeManifest(3);
      expect((await r.run("c", r.body(key, "ObjectCreated:Put", "03"), ON)).batchItemFailures).toEqual([]);
    };
    r.calls.length = 0;
    expect((await r.run("b", r.body(key, "ObjectCreated:Put", "02"), ON)).batchItemFailures).toEqual([]);
    expect((await r.row()).map((x) => x.revision)).toEqual(["3"]);
    expect(inLock(r.calls, s3)).toBe(0);
    // Ours read the manifest twice: the first pre-read, then again after the stamp check refused it.
    expect(r.calls.filter((c) => c.kind === "s3.read" && c.detail === key && c.at === "free").length).toBeGreaterThanOrEqual(3);
  }, 300_000);

  test("a put that read before an erase cannot bring the pointer back (the lock's reason), with the delete in between", async () => {
    const r = await pointerRig();
    cleanup.push(r.close);
    let key = await r.writeManifest(1);
    await r.run("a", r.body(key, "ObjectCreated:Put", "01"), ON);
    key = await r.writeManifest(2);
    r.hooks.beforeLock = async () => {
      // The put read revision 2; the erasure deletes the manifest and its delete event is handled completely.
      await r.store.delete(objectKey(key));
      expect((await r.run("d", r.body(key, "ObjectRemoved:DeleteMarkerCreated", "03"), ON)).batchItemFailures).toEqual([]);
    };
    expect((await r.run("p2", r.body(key, "ObjectCreated:Put", "02"), ON)).batchItemFailures).toEqual([]);
    const rows = await r.row();
    expect(rows.length).toBe(1);
    expect(rows[0]!.deleted_at).not.toBeNull();
    const directory = (await r.pg.query<{ deleted_at: string | null }>("SELECT deleted_at FROM sessions.session_directory WHERE session_id = 'sess_1'")).rows;
    expect(directory.every((d) => d.deleted_at !== null)).toBe(true);
  }, 300_000);

  test("a delete that read 'gone' before a newer put was indexed does not tombstone it", async () => {
    const r = await pointerRig();
    cleanup.push(r.close);
    const key = await r.writeManifest(1);
    await r.run("a", r.body(key, "ObjectCreated:Put", "01"), ON);
    await r.store.delete(objectKey(key));
    r.hooks.beforeLock = async () => {
      // The delete read "gone" (and probed); a re-upload at a newer revision is indexed before it locks.
      await r.writeManifest(2);
      expect((await r.run("p", r.body(key, "ObjectCreated:Put", "02"), ON)).batchItemFailures).toEqual([]);
    };
    expect((await r.run("d", r.body(key, "ObjectRemoved:DeleteMarkerCreated", "03"), ON)).batchItemFailures).toEqual([]);
    expect((await r.row()).map((x) => [x.revision, x.deleted_at])).toEqual([["2", null]]);
  }, 300_000);

  test("a scope that changes during both attempts is retried by the queue (pointer_scope_changed), nothing written", async () => {
    const r = await pointerRig();
    cleanup.push(r.close);
    const key = await r.writeManifest(1);
    await r.run("a", r.body(key, "ObjectCreated:Put", "01"), ON);
    await r.writeManifest(2);
    const before = await r.row();
    let n = 0;
    const touch = async () => {
      n += 1;
      await r.pg.exec("UPDATE sessions.session_index SET indexed_at = indexed_at + interval '1 second'");
      if (n < 2) r.hooks.beforeLock = touch;
    };
    r.hooks.beforeLock = touch;
    const lines: string[] = [];
    const stderr = process.stderr.write, stdout = process.stdout.write;
    const tap = (w: typeof process.stdout.write, s: NodeJS.WriteStream) => ((c: unknown, ...rest: unknown[]) => { lines.push(String(c)); return (w as (...a: unknown[]) => boolean).call(s, c, ...rest); }) as typeof process.stdout.write;
    process.stderr.write = tap(stderr, process.stderr);
    process.stdout.write = tap(stdout, process.stdout);
    let out;
    try {
      out = await r.run("b", r.body(key, "ObjectCreated:Put", "02"), ON);
    } finally {
      process.stderr.write = stderr;
      process.stdout.write = stdout;
    }
    expect(n).toBe(2);
    expect(out.batchItemFailures).toEqual([{ itemIdentifier: "b" }]);
    expect(lines.join("")).toContain('"error_code":"pointer_scope_changed"');
    expect((await r.row()).map((x) => x.revision)).toEqual(before.map((x) => x.revision));
  }, 300_000);
});

describe("SESSION_POINTER_PREREAD_ENABLED in the Lambda", () => {
  const base = {
    AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
    SESSION_PIPELINE_SECRET_ARN: "synthetic", SESSION_PIPELINE_QUEUE_URL: "https://sqs.invalid/q",
  };
  const secret = JSON.stringify({ hoopdb_url: "postgresql://synthetic.invalid/fixture", clickhouse: { url: "http://127.0.0.1:1", user: "u", password: "p", database: "d" } });
  test("unset: no field, no pointer flag; true: deps.pointer.preRead; strict grammar", async () => {
    for (const [value, expected] of [[undefined, false], ["", false], ["false", false], ["true", true]] as const) {
      const env = lambdaEnv({ ...base, ...(value === undefined ? {} : { SESSION_POINTER_PREREAD_ENABLED: value }) });
      expect(env.pointerPreReadEnabled === true).toBe(expected);
      const rt = runtimeFromSecret(env, secret, "synthetic-generation");
      try {
        expect("preRead" in rt.deps.pointer).toBe(expected);
      } finally {
        await (rt.deps.pointer.db as unknown as { end(): Promise<void> }).end();
      }
    }
    for (const bad of ["1", "TRUE", " true", "on"]) expect(() => pointerPreReadFromEnv({ SESSION_POINTER_PREREAD_ENABLED: bad })).toThrow("SESSION_POINTER_PREREAD_ENABLED must be true or false");
  });
});
