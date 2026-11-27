/**
 * Two independent real PostgreSQL connections (`a`, `b`) plus an observer, for proving that one transaction blocks
 * another: the waiter is seen in pg_stat_activity with wait_event_type 'Lock', holding an ungranted pg_locks row, and
 * with the holder in pg_blocking_pids. Timing alone is never the evidence.
 *
 *   const pair = await openPair({ url: db.ownerUrl, observerUrl: db.fixtureUrl });
 *   const r = await expectBlocks(pair, { holder: pair.a, waiter: pair.b, hold: install, wait: admit });
 *   r.receipt.locks[0].locktype === "advisory"; r.waited.ok === false && r.waited.error.code === "RH042";
 *   const both = await expectBlocksBothWays(pair, install, admit);   // a holds/b waits, then b holds/a waits
 *
 * The observer must be able to read other sessions' wait state: the fixture superuser (or pg_read_all_stats).
 * Each step runs in its own transaction (`begin`, default BEGIN); the holder ends it after the block is observed,
 * then the waiter's outcome (value or PostgreSQL error, never thrown) is returned for the caller to assert.
 */
import pg from "pg";

export interface PgError { code?: string; message: string; detail?: string; hint?: string }
export type Outcome<T> = { ok: true; value: T; ms: number } | { ok: false; error: PgError; ms: number };

export interface Conn {
  readonly name: string;
  readonly client: pg.Client;
  /** the backend pid (pg_backend_pid()), as pg_locks and pg_stat_activity name it */
  readonly pid: number;
  query<R extends pg.QueryResultRow = any>(text: string, params?: unknown[]): Promise<pg.QueryResult<R>>;
}

export interface Pair {
  a: Conn;
  b: Conn;
  observer: pg.Client;
  close(): Promise<void>;
}

export interface OpenPairOptions {
  /** what `a` and `b` log in as (the code under test's role, e.g. ScratchDatabase.ownerUrl) */
  url: string;
  /** what the observer logs in as (default `url`); it must see other sessions' wait events */
  observerUrl?: string;
  names?: [string, string];
  /** per-connection statement_timeout (default 30 s), so a wrong expectation fails instead of hanging */
  statementTimeoutMs?: number;
  /** per-connection lock_timeout (default 0: wait, so blocking is observable) */
  lockTimeoutMs?: number;
  /** runs once per connection after connect (SET ROLE, search_path, ...) */
  setup?: (c: Conn) => Promise<void>;
}

async function connect(url: string, name: string, opts: OpenPairOptions): Promise<Conn> {
  const client = new pg.Client({
    connectionString: url,
    application_name: `hold-harness:${name}`,
    statement_timeout: opts.statementTimeoutMs ?? 30_000,
    lock_timeout: opts.lockTimeoutMs ?? 0,
  });
  client.on("error", () => {}); // a backend terminated by cleanup must not crash the runner
  await client.connect();
  const pid = Number((await client.query("SELECT pg_backend_pid() AS pid")).rows[0].pid);
  const conn: Conn = { name, client, pid, query: (text, params) => client.query(text, params as unknown[]) };
  await opts.setup?.(conn);
  return conn;
}

export async function openPair(opts: OpenPairOptions): Promise<Pair> {
  const [na, nb] = opts.names ?? ["a", "b"];
  const opened: pg.Client[] = [];
  try {
    const observer = new pg.Client({ connectionString: opts.observerUrl ?? opts.url, application_name: "hold-harness:observer", statement_timeout: 10_000 });
    observer.on("error", () => {});
    await observer.connect(); opened.push(observer);
    const a = await connect(opts.url, na, opts); opened.push(a.client);
    const b = await connect(opts.url, nb, opts); opened.push(b.client);
    if (a.pid === b.pid) throw Error("pair_not_independent");
    return {
      a, b, observer,
      async close() {
        for (const c of [a, b]) await c.query("ROLLBACK").catch(() => {});
        await Promise.allSettled([a.client.end(), b.client.end(), observer.end()]);
      },
    };
  } catch (e) {
    await Promise.allSettled(opened.map((c) => c.end()));
    throw e;
  }
}

const pgError = (e: unknown): PgError => {
  const x = e as { code?: string; message?: string; detail?: string; hint?: string };
  return { code: x?.code, message: String(x?.message ?? e), detail: x?.detail, hint: x?.hint };
};

/** Run `fn` to an Outcome: a PostgreSQL error is a value, never a rejection. */
export async function settle<T>(fn: () => Promise<T>): Promise<Outcome<T>> {
  const t0 = performance.now();
  try { return { ok: true, value: await fn(), ms: performance.now() - t0 }; }
  catch (e) { return { ok: false, error: pgError(e), ms: performance.now() - t0 }; }
}

/** A statement (or several) started on a connection and not awaited yet. */
export interface Pending<T> {
  readonly promise: Promise<Outcome<T>>;
  settled(): boolean;
}

export function start<T>(c: Conn, op: (c: Conn) => Promise<T>): Pending<T> {
  let done = false;
  const promise = settle(() => op(c)).finally(() => { done = true; });
  return { promise, settled: () => done };
}

export interface LockRow {
  locktype: string; mode: string; granted: boolean;
  database: number | null; relation: number | null; page: number | null; tuple: number | null;
  virtualxid: string | null; transactionid: string | null;
  classid: number | null; objid: number | null; objsubid: number | null;
}

export interface WaitState {
  pid: number;
  state: string | null;
  waitEventType: string | null;
  waitEvent: string | null;
  query: string | null;
  blockingPids: number[];
  /** the pid's ungranted pg_locks rows */
  waiting: LockRow[];
}

export interface BlockReceipt extends WaitState {
  waiter: string;
  holder: string;
  holderPid: number;
  /** the holder's granted locks on the object(s) the waiter waits for (empty for a queue-position wait) */
  holderLocks: LockRow[];
}

// oids are unsigned 32-bit (an advisory key's halves exceed int4): read them as float8 numbers.
const oid = (c: string) => `${c}::bigint::float8 AS ${c}`;
const LOCK_COLS = ["locktype", "mode", "granted", oid("database"), oid("relation"), "page", "tuple", "virtualxid::text AS virtualxid",
  "transactionid::text AS transactionid", oid("classid"), oid("objid"), "objsubid"].join(",");

/** One snapshot of a backend's wait state, read on the observer. */
export async function observeWait(observer: pg.Client, pid: number): Promise<WaitState> {
  const a = (await observer.query(
    `SELECT state,wait_event_type,wait_event,query,pg_blocking_pids(pid) AS blocking FROM pg_stat_activity WHERE pid=$1`, [pid])).rows[0];
  const waiting = (await observer.query(`SELECT ${LOCK_COLS} FROM pg_locks WHERE pid=$1 AND NOT granted`, [pid])).rows as LockRow[];
  return {
    pid, state: a?.state ?? null, waitEventType: a?.wait_event_type ?? null, waitEvent: a?.wait_event ?? null,
    query: a?.query ?? null, blockingPids: (a?.blocking ?? []).map(Number), waiting,
  };
}

const sameObject = (x: LockRow, y: LockRow) =>
  x.locktype === y.locktype && x.database === y.database && x.relation === y.relation && x.page === y.page && x.tuple === y.tuple &&
  x.virtualxid === y.virtualxid && x.transactionid === y.transactionid && x.classid === y.classid && x.objid === y.objid && x.objsubid === y.objsubid;

const isBlockedBy = (w: WaitState, holderPid: number) =>
  w.waitEventType === "Lock" && w.waiting.length > 0 && w.blockingPids.includes(holderPid);

export interface WaitOptions { timeoutMs?: number; pollMs?: number }

/**
 * Poll until `waiter` is in a Lock wait with an ungranted lock and `holder` in its pg_blocking_pids. Throws
 * `waiter_not_blocked` (with the last state) on timeout, or `waiter_settled_unblocked` if `pending` finished first.
 */
export async function waitUntilBlocked(observer: pg.Client, waiter: Conn, holder: Conn, pending?: Pending<unknown>, opts: WaitOptions = {}): Promise<BlockReceipt> {
  const deadline = performance.now() + (opts.timeoutMs ?? 5_000);
  let last: WaitState | undefined;
  for (;;) {
    last = await observeWait(observer, waiter.pid);
    if (isBlockedBy(last, holder.pid)) {
      const granted = (await observer.query(`SELECT ${LOCK_COLS} FROM pg_locks WHERE pid=$1 AND granted`, [holder.pid])).rows as LockRow[];
      return { ...last, waiter: waiter.name, holder: holder.name, holderPid: holder.pid, holderLocks: granted.filter((g) => last!.waiting.some((w) => sameObject(g, w))) };
    }
    if (pending?.settled()) {
      const outcome = await pending.promise;
      throw Error(`waiter_settled_unblocked: ${waiter.name} finished without waiting on ${holder.name}: ${JSON.stringify(outcome)}`);
    }
    if (performance.now() > deadline) throw Error(`waiter_not_blocked: ${waiter.name} by ${holder.name}: ${JSON.stringify(last)}`);
    await Bun.sleep(opts.pollMs ?? 10);
  }
}

export type Op<T> = (c: Conn) => Promise<T>;
/** One side's work, inside its own transaction opened with `begin` (default BEGIN). */
export interface Step<T> { op: Op<T>; begin?: string; label?: string }
const asStep = <T>(s: Op<T> | Step<T>): Step<T> => (typeof s === "function" ? { op: s } : s);

export interface BlockCase<H, W> {
  holder: Conn;
  waiter: Conn;
  hold: Op<H> | Step<H>;
  wait: Op<W> | Step<W>;
  /** how the holder ends its transaction once the block is observed (default commit) */
  holderEnd?: "commit" | "rollback";
  /** how the waiter ends a transaction that succeeded (default commit; a failed one always rolls back) */
  waiterEnd?: "commit" | "rollback";
  /** called while the waiter is observed blocked, before the holder ends (extra assertions, a third party) */
  whileBlocked?: (receipt: BlockReceipt) => Promise<void>;
  timeoutMs?: number;
}

export interface BlockResult<H, W> {
  receipt: BlockReceipt;
  held: H;
  /** the waiter's op after the holder ended: its value, or the PostgreSQL error it raised */
  waited: Outcome<W>;
  /** the holder's COMMIT/ROLLBACK, as an outcome (a deferred constraint can fail the commit) */
  holderEnded: Outcome<unknown>;
}

async function cancelAndReset(observer: pg.Client, ...cs: Conn[]) {
  for (const c of cs) await observer.query("SELECT pg_cancel_backend($1)", [c.pid]).catch(() => {});
  for (const c of cs) await c.query("ROLLBACK").catch(() => {});
}

/** `hold` in an open transaction on holder blocks `wait` on waiter until the holder ends; observed, not timed. */
export async function expectBlocks<H, W>(pair: Pair, k: BlockCase<H, W>): Promise<BlockResult<H, W>> {
  const hold = asStep(k.hold), wait = asStep(k.wait);
  if (k.holder === k.waiter) throw Error("holder_is_waiter");
  try {
    await k.holder.query(hold.begin ?? "BEGIN");
    const held = await hold.op(k.holder);
    await k.waiter.query(wait.begin ?? "BEGIN");
    const pending = start(k.waiter, wait.op);
    const receipt = await waitUntilBlocked(pair.observer, k.waiter, k.holder, pending, { timeoutMs: k.timeoutMs });
    await k.whileBlocked?.(receipt);
    if (pending.settled()) throw Error(`waiter_settled_while_held: ${JSON.stringify(await pending.promise)}`);
    const holderEnded = await settle(() => k.holder.query(k.holderEnd === "rollback" ? "ROLLBACK" : "COMMIT"));
    const waited = await pending.promise;
    await k.waiter.query(waited.ok && k.waiterEnd !== "rollback" ? "COMMIT" : "ROLLBACK");
    return { receipt, held, waited, holderEnded };
  } catch (e) {
    await cancelAndReset(pair.observer, k.waiter, k.holder);
    throw e;
  }
}

export interface NoBlockResult<H, W> { held: H; waited: Outcome<W>; observed: WaitState[] }

/** `wait` completes while `hold`'s transaction is still open, and is never seen blocked by the holder. */
export async function expectNotBlocked<H, W>(pair: Pair, k: Omit<BlockCase<H, W>, "whileBlocked">): Promise<NoBlockResult<H, W>> {
  const hold = asStep(k.hold), wait = asStep(k.wait);
  const deadline = performance.now() + (k.timeoutMs ?? 5_000);
  try {
    await k.holder.query(hold.begin ?? "BEGIN");
    const held = await hold.op(k.holder);
    await k.waiter.query(wait.begin ?? "BEGIN");
    const pending = start(k.waiter, wait.op);
    const observed: WaitState[] = [];
    while (!pending.settled()) {
      const w = await observeWait(pair.observer, k.waiter.pid);
      observed.push(w);
      if (isBlockedBy(w, k.holder.pid)) throw Error(`waiter_blocked: ${k.waiter.name} by ${k.holder.name}: ${JSON.stringify(w)}`);
      if (performance.now() > deadline) throw Error(`waiter_did_not_finish: ${JSON.stringify(w)}`);
      await Bun.sleep(5);
    }
    const waited = await pending.promise;
    await k.waiter.query(waited.ok && k.waiterEnd !== "rollback" ? "COMMIT" : "ROLLBACK");
    await k.holder.query(k.holderEnd === "rollback" ? "ROLLBACK" : "COMMIT");
    return { held, waited, observed };
  } catch (e) {
    await cancelAndReset(pair.observer, k.waiter, k.holder);
    throw e;
  }
}

export interface BothWays<X, Y> {
  /** a holds x, b waits on y */
  aHolds: BlockResult<X, Y>;
  /** b holds y, a waits on x */
  bHolds: BlockResult<Y, X>;
}

/** Both orderings: x on a blocks y on b, then y on b blocks x on a. `between` resets state between them. */
export async function expectBlocksBothWays<X, Y>(pair: Pair, x: Op<X> | Step<X>, y: Op<Y> | Step<Y>,
  opts: Pick<BlockCase<X, Y>, "holderEnd" | "waiterEnd" | "timeoutMs"> & { between?: () => Promise<void> } = {}): Promise<BothWays<X, Y>> {
  const aHolds = await expectBlocks(pair, { ...opts, holder: pair.a, waiter: pair.b, hold: x, wait: y });
  await opts.between?.();
  const bHolds = await expectBlocks(pair, { ...opts, holder: pair.b, waiter: pair.a, hold: y, wait: x });
  return { aHolds, bHolds };
}
