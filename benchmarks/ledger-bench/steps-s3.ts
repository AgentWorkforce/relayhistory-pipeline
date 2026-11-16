/**
 * Steps 1, 2 and 6 of the plan: raw request latency, conditional-write semantics, and socket/pool headroom.
 * Correctness checks return counts; `violations` above zero is a failed cell, never a pass.
 */
import { sha256Hex } from "../../packages/object-store/src/crypto.ts";
import { isObjectStoreError } from "../../packages/object-store/src/errors.ts";
import type { ObjectStore, ObjectVersion } from "../../packages/object-store/src/types.ts";
import type { TargetId } from "./config.ts";
import { cell, jsonDoc, key, KIB, need, randomBytes, readBody, readOptional, type Ctx } from "./context.ts";
import { errorClass, mapLimit } from "./load.ts";
import { Tally } from "./stats.ts";

type BucketId = Exclude<TargetId, "R6">;

/** Puts cycle over this many keys per cell, so a fast cell cannot leave millions of objects to tear down. */
const PUT_RING = 2048;
const READ_POOL = 64;
const LIST_KEYS = 1000;

async function seed(store: ObjectStore, prefix: string, count: number, size: number): Promise<void> {
  await mapLimit(Array.from({ length: count }, (_, i) => i), 32, (i) => store.put(key(prefix, i), randomBytes(size)));
}

/** Step 1: unconditioned GET, PUT, HEAD and LIST at every concurrency and size, plus the SSE-S3 comparison. */
export async function step1(ctx: Ctx, target: BucketId): Promise<void> {
  const store = need(ctx, target);
  const root = `${ctx.root}/s1/${target.toLowerCase()}`;
  const sizes = [1, 64];
  for (const kib of sizes) await seed(store, `${root}/pool-${kib}k`, READ_POOL, kib * KIB);
  await seed(store, `${root}/list`, LIST_KEYS, 64);

  for (const concurrency of ctx.profile.concurrencies) {
    for (const kib of sizes) {
      const body = randomBytes(kib * KIB);
      await cell(ctx, { step: 1, cell: "put", target, params: { concurrency, kib, sse: "kms" } }, { concurrency, op: async (w, i) => void (await store.put(key(root, `put-${kib}k-c${concurrency}`, (w * 7919 + i) % PUT_RING), body)) });
      await cell(ctx, { step: 1, cell: "get", target, params: { concurrency, kib, sse: "kms" } }, { concurrency, op: async (w, i) => void (await readBody(store, key(`${root}/pool-${kib}k`, (w + i) % READ_POOL))) });
    }
    await cell(ctx, { step: 1, cell: "head", target, params: { concurrency } }, { concurrency, op: async (w, i) => void (await store.head(key(`${root}/pool-1k`, (w + i) % READ_POOL))) });
    await cell(ctx, { step: 1, cell: "list", target, params: { concurrency, keys: LIST_KEYS } }, {
      concurrency,
      op: async () => {
        const page = await store.list(key(root, "list"));
        return page.items.length === LIST_KEYS ? "ok" : `listed_${page.items.length}`;
      },
    });
  }

  // 256 KiB only at concurrency 8, as the plan says.
  await seed(store, `${root}/pool-256k`, 16, 256 * KIB);
  const big = randomBytes(256 * KIB);
  await cell(ctx, { step: 1, cell: "put", target, params: { concurrency: 8, kib: 256, sse: "kms" } }, { concurrency: 8, op: async (w, i) => void (await store.put(key(root, "put-256k-c8", (w * 7919 + i) % 256), big)) });
  await cell(ctx, { step: 1, cell: "get", target, params: { concurrency: 8, kib: 256, sse: "kms" } }, { concurrency: 8, op: async (w, i) => void (await readBody(store, key(`${root}/pool-256k`, (w + i) % 16))) });

  // The KMS share: the same requests against SSE-S3 objects in the same bucket (R1 and R2 only).
  const plain = target === "R1" || target === "R2" ? ctx.sseS3[target] : undefined;
  if (plain) {
    for (const kib of sizes) {
      await seed(plain, `${root}/sse-s3-pool-${kib}k`, READ_POOL, kib * KIB);
      const body = randomBytes(kib * KIB);
      await cell(ctx, { step: 1, cell: "put", target, params: { concurrency: 8, kib, sse: "s3" } }, { concurrency: 8, op: async (w, i) => void (await plain.put(key(root, `sse-s3-put-${kib}k`, (w * 7919 + i) % PUT_RING), body)) });
      await cell(ctx, { step: 1, cell: "get", target, params: { concurrency: 8, kib, sse: "s3" } }, { concurrency: 8, op: async (w, i) => void (await readBody(plain, key(`${root}/sse-s3-pool-${kib}k`, (w + i) % READ_POOL))) });
    }
  }
}

/** What a racing round did: how many writers won, how the losers were refused, and whether the stored bytes are the winner's. */
interface RaceTotals {
  rounds: number;
  /** Rounds with exactly one winner and the winner's bytes stored. */
  clean: number;
  /** Safety: rounds with more than one winner, or stored bytes that are not the single winner's. */
  violations: number;
  /** Liveness, not safety: rounds where every writer was refused or failed and nothing new was stored. */
  no_winner: number;
  winners: Record<string, number>;
  refusals: Record<string, number>;
}

async function settle<T>(work: Promise<T>): Promise<{ ok: true; value: T } | { ok: false; error: unknown }> {
  try {
    return { ok: true, value: await work };
  } catch (error) {
    return { ok: false, error };
  }
}

/** One round's verdict from who won and what is stored afterwards. */
function judge(won: number, storedIsWinners: boolean, storedIsNew: boolean): "clean" | "no_winner" | "violation" {
  if (won === 1) return storedIsWinners ? "clean" : "violation";
  if (won === 0) return storedIsNew ? "violation" : "no_winner";
  return "violation";
}

/** `writers` race one create-if-absent on a fresh key per round. Exactly one may win. */
export async function raceCreate(store: ObjectStore, prefix: string, writers: number, rounds: number): Promise<RaceTotals> {
  const winners = new Tally();
  const refusals = new Tally();
  const verdicts = { clean: 0, no_winner: 0, violation: 0 };
  for (let round = 0; round < rounds; round++) {
    const k = key(prefix, round);
    const bodies = Array.from({ length: writers }, (_, w) => jsonDoc({ round, writer: w, nonce: crypto.randomUUID() }, KIB));
    const results = await Promise.all(bodies.map((b) => settle(store.put(k, b, { ifNoneMatch: true }))));
    const won = results.flatMap((r, w) => (r.ok && r.value.created ? [w] : []));
    for (const r of results) if (!r.ok) refusals.add(errorClass(r.error));
    winners.add(String(won.length));
    const after = await readOptional(store, k);
    const stored = after === undefined ? undefined : await sha256Hex(after.body);
    verdicts[judge(won.length, won.length === 1 && stored === (await sha256Hex(bodies[won[0]!]!)), stored !== undefined)]++;
  }
  return { rounds, clean: verdicts.clean, violations: verdicts.violation, no_winner: verdicts.no_winner, winners: winners.toJSON(), refusals: refusals.toJSON() };
}

/** `writers` race one `If-Match` CAS against the same version per round. Exactly one may win. */
export async function raceCas(store: ObjectStore, prefix: string, writers: number, rounds: number): Promise<RaceTotals> {
  const winners = new Tally();
  const refusals = new Tally();
  const verdicts = { clean: 0, no_winner: 0, violation: 0 };
  const k = key(prefix, "cas");
  const first = jsonDoc({ round: -1 }, KIB);
  let version: ObjectVersion = (await store.put(k, first)).version;
  let before = await sha256Hex(first);
  for (let round = 0; round < rounds; round++) {
    const bodies = Array.from({ length: writers }, (_, w) => jsonDoc({ round, writer: w, nonce: crypto.randomUUID() }, KIB));
    const results = await Promise.all(bodies.map((b) => settle(store.put(k, b, { ifMatch: version }))));
    const won = results.flatMap((r, w) => (r.ok ? [w] : []));
    for (const r of results) if (!r.ok) refusals.add(errorClass(r.error));
    winners.add(String(won.length));
    const now = (await readOptional(store, k))!;
    const stored = await sha256Hex(now.body);
    verdicts[judge(won.length, won.length === 1 && stored === (await sha256Hex(bodies[won[0]!]!)), stored !== before)]++;
    version = now.version;
    before = stored;
  }
  return { rounds, clean: verdicts.clean, violations: verdicts.violation, no_winner: verdicts.no_winner, winners: winners.toJSON(), refusals: refusals.toJSON() };
}

export type IfMatchSupport = "supported" | "ignored" | "rejected";

/**
 * Whether the backend honours `If-Match` at all (the open question for directory buckets). A fresh CAS must
 * succeed and a stale one must be refused with a precondition failure. A stale CAS that succeeds is
 * `ignored`; a fresh CAS the backend refuses outright (not implemented, bad request) is `rejected`.
 */
export async function probeIfMatch(store: ObjectStore, prefix: string): Promise<{ support: IfMatchSupport; fresh: string; stale: string }> {
  const k = key(prefix, "probe");
  const first = await store.put(k, jsonDoc({ v: "a" }, KIB));
  const fresh = await settle(store.put(k, jsonDoc({ v: "b" }, KIB), { ifMatch: first.version }));
  const stale = await settle(store.put(k, jsonDoc({ v: "c" }, KIB), { ifMatch: first.version }));
  const freshClass = fresh.ok ? "ok" : errorClass(fresh.error);
  const staleClass = stale.ok ? "ok" : errorClass(stale.error);
  const stored = new TextDecoder().decode(await readBody(store, k));
  let support: IfMatchSupport;
  if (!fresh.ok) support = "rejected";
  else if (stale.ok || !stored.includes('"v":"b"')) support = "ignored";
  else support = isObjectStoreError(stale.error, "precondition_failed") ? "supported" : "rejected";
  return { support, fresh: freshClass, stale: staleClass };
}

/**
 * A delete racing a conditional write on one object. Any outcome pair is legal; what must hold is that the
 * final object is either absent or exactly the conditional write's bytes, never the old version.
 */
export async function raceDelete(store: ObjectStore, prefix: string, rounds: number): Promise<{ rounds: number; violations: number; outcomes: Record<string, number> }> {
  const outcomes = new Tally();
  let violations = 0;
  for (let round = 0; round < rounds; round++) {
    const k = key(prefix, round);
    const before = await store.put(k, jsonDoc({ round, v: "old" }, KIB));
    const next = jsonDoc({ round, v: "new", nonce: crypto.randomUUID() }, KIB);
    const [put, del] = await Promise.all([settle(store.put(k, next, { ifMatch: before.version })), settle(store.delete(k))]);
    const after = await readOptional(store, k);
    const final = after === undefined ? "absent" : (await sha256Hex(after.body)) === (await sha256Hex(next)) ? "new" : "old";
    outcomes.add(`put_${put.ok ? "ok" : errorClass(put.error)} delete_${del.ok ? "ok" : errorClass(del.error)} final_${final}`);
    // The old bytes surviving both a delete and a CAS, or a refused CAS leaving new bytes, is a broken precondition.
    if (final === "old" && del.ok) violations++;
    if (final === "new" && !put.ok && isObjectStoreError(put.error, "precondition_failed")) violations++;
  }
  return { rounds, violations, outcomes: outcomes.toJSON() };
}

/** The body the ambiguous-commit child writes at index `i`: derivable by the parent for the readback. */
export function ambiguousBody(runTag: string, i: number): Uint8Array {
  const out = new Uint8Array(256 * KIB);
  const tag = new TextEncoder().encode(`${runTag}:${i}:`);
  for (let o = 0; o < out.length; o++) out[o] = tag[o % tag.length]! ^ (o & 0xff);
  return out;
}

/** The child: create-if-absent puts, one after another, announcing each before it starts. Killed by the parent. */
export async function ambiguousChild(store: ObjectStore, prefix: string, runTag: string, write: (line: string) => void): Promise<never> {
  for (let i = 0; ; i++) {
    write(`start ${i}`);
    await store.put(key(prefix, i), ambiguousBody(runTag, i), { ifNoneMatch: true });
    write(`done ${i}`);
  }
}

export interface AmbiguousTotals {
  kills: number;
  /** Rounds whose child never started a write (it crashed first): they prove nothing. */
  empty_rounds: number;
  /** Writes that were in flight when the client died. */
  in_flight: number;
  present_whole: number;
  absent: number;
  /** Present with other bytes or another size: a torn write. Must be 0. */
  torn: number;
  /** Writes the child saw acknowledged that are not readable afterwards. Must be 0. */
  acknowledged_lost: number;
}

/**
 * Ambiguous commit: SIGKILL the client mid-PUT, then read back. `spawnChild` starts the child for one round
 * and returns its announcements and a kill switch; main.ts supplies the real process, the test a fake.
 */
export async function ambiguousCommit(
  store: ObjectStore,
  prefix: string,
  kills: number,
  spawnChild: (roundPrefix: string, runTag: string) => Promise<{ started: number[]; done: number[] }>,
): Promise<AmbiguousTotals> {
  const totals: AmbiguousTotals = { kills, empty_rounds: 0, in_flight: 0, present_whole: 0, absent: 0, torn: 0, acknowledged_lost: 0 };
  for (let round = 0; round < kills; round++) {
    const roundPrefix = `${prefix}/k${round}`;
    const runTag = crypto.randomUUID();
    const { started, done } = await spawnChild(roundPrefix, runTag);
    const acknowledged = new Set(done);
    if (started.length === 0) totals.empty_rounds++;
    for (const i of started) {
      const got = await readOptional(store, key(roundPrefix, i));
      const whole = got !== undefined && (await sha256Hex(got.body)) === (await sha256Hex(ambiguousBody(runTag, i)));
      if (acknowledged.has(i)) {
        if (!whole) totals.acknowledged_lost++;
        continue;
      }
      totals.in_flight++;
      if (got === undefined) totals.absent++;
      else if (whole) totals.present_whole++;
      else totals.torn++;
    }
  }
  return totals;
}

export type SpawnAmbiguousChild = (target: BucketId, roundPrefix: string, runTag: string) => Promise<{ started: number[]; done: number[] }>;

/** Step 2: conditional semantics and latency on one target. */
export async function step2(ctx: Ctx, target: BucketId, spawnChild?: SpawnAmbiguousChild): Promise<void> {
  const store = need(ctx, target);
  const root = `${ctx.root}/s2/${target.toLowerCase()}`;
  const rounds = ctx.profile.raceRounds;
  const emit = (name: string, params: Record<string, string | number | boolean>, result: unknown) => {
    ctx.emit({ step: 2, cell: name, target, params, result });
    ctx.log(`step 2 ${name} ${target} ${JSON.stringify(params)}: ${JSON.stringify(result)}`);
  };

  const body = jsonDoc({ kind: "entry" }, KIB);
  await cell(ctx, { step: 2, cell: "create_if_absent", target, params: { writers: 1, kib: 1 } }, { concurrency: 1, maxOps: 4000, op: async (_w, i) => ((await store.put(key(root, "create", i), body, { ifNoneMatch: true })).created ? "created" : "existed") });
  for (const writers of [2, 8]) emit("race_create", { writers }, await raceCreate(store, `${root}/race-create-w${writers}`, writers, rounds));

  const probe = await probeIfMatch(store, `${root}/if-match`);
  emit("if_match_support", {}, probe);
  if (probe.support === "supported") {
    let version = (await store.put(key(root, "cas-single"), body)).version;
    await cell(ctx, { step: 2, cell: "cas", target, params: { writers: 1, kib: 1 } }, {
      concurrency: 1,
      maxOps: 4000,
      op: async (_w, i) => {
        version = (await store.put(key(root, "cas-single"), jsonDoc({ i }, KIB), { ifMatch: version })).version;
      },
    });
    emit("race_cas", { writers: 8 }, await raceCas(store, `${root}/race-cas`, 8, rounds));
    emit("race_delete", {}, await raceDelete(store, `${root}/race-delete`, rounds));
  }
  if (spawnChild) emit("ambiguous_commit", { kib: 256 }, await ambiguousCommit(store, `${root}/ambiguous`, Math.max(3, Math.floor(rounds / 10)), (p, tag) => spawnChild(target, p, tag)));
}

/** Step 6: ramp in-flight requests in this one process and record where transients and latency start to rise. */
export async function step6(ctx: Ctx, target: BucketId): Promise<void> {
  const store = need(ctx, target);
  const root = `${ctx.root}/s6/${target.toLowerCase()}`;
  await seed(store, `${root}/pool`, READ_POOL, KIB);
  const body = randomBytes(KIB);
  for (const inFlight of ctx.profile.poolRamp) {
    await cell(ctx, { step: 6, cell: "pool_ramp", target, params: { in_flight: inFlight, kib: 1 } }, {
      concurrency: inFlight,
      op: async (w, i) => {
        if ((w + i) % 2 === 0) await readBody(store, key(`${root}/pool`, (w + i) % READ_POOL));
        else await store.put(key(root, "put", (w * 7919 + i) % PUT_RING), body);
      },
    });
  }
}
