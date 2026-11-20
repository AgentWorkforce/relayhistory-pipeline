// Throwaway: does lock_timeout bound EACH stage of a queued row-lock wait (tuple lock, then holder xid) rather than the total?
import pg from "pg";
const base = (() => { const u = new URL(process.env.SESSION_INGEST_TEST_PG_URL ?? ""); if (!["127.0.0.1", "localhost"].includes(u.hostname) || u.password) throw Error("loopback throwaway fixture only"); u.pathname = "/"; return u.href; })();
const admin = new pg.Client(base + "postgres"); await admin.connect();
await admin.query("DROP DATABASE IF EXISTS rcf_lt"); await admin.query("CREATE DATABASE rcf_lt");
const mk = async () => { const c = new pg.Client(base + "rcf_lt"); await c.connect(); return c; };
const s = await mk(); await s.query("CREATE TABLE clock(singleton boolean PRIMARY KEY, last bigint); INSERT INTO clock VALUES(true,0)");
const LOCK = "SELECT 1 FROM clock WHERE singleton FOR NO KEY UPDATE";
const timed = async (c, sql) => { const t = performance.now(); try { await c.query(sql); return { ok: true, ms: Math.round(performance.now() - t) }; } catch (e) { return { ok: false, code: e.code, ms: Math.round(performance.now() - t) }; } };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// Case 1: one holder for 3 s; one waiter with lock_timeout 2 s -> expect 55P03 at ~2 s.
{ const X = await mk(), A = await mk();
  await X.query("BEGIN"); await X.query(LOCK);
  await A.query("BEGIN;SET LOCAL lock_timeout='2s'"); const a = timed(A, LOCK);
  await sleep(3000); await X.query("COMMIT");
  console.log(JSON.stringify({ case: "single stage: holder 3s, waiter lock_timeout 2s", A: await a }));
  await A.query("ROLLBACK").catch(() => {}); await X.end(); await A.end(); }
// Case 2: chain. X holds 1.5 s. A (lock_timeout 2s) queues first, gets the row at ~1.5 s, holds 1.5 s more.
// B (lock_timeout 2s) queues behind A: total wait ~3 s, but each stage < 2 s.
{ const X = await mk(), A = await mk(), B = await mk();
  await X.query("BEGIN"); await X.query(LOCK);
  await A.query("BEGIN;SET LOCAL lock_timeout='2s'"); const a = timed(A, LOCK); await sleep(100);
  await B.query("BEGIN;SET LOCAL lock_timeout='2s'"); const b = timed(B, LOCK);
  await sleep(1500); await X.query("COMMIT");
  const ra = await a; await sleep(1500); await A.query("COMMIT");
  const rb = await b; await B.query("COMMIT").catch(() => {});
  console.log(JSON.stringify({ case: "chain: X 1.5s -> A holds 1.5s -> B", A: ra, B: rb }));
  await X.end(); await A.end(); await B.end(); }
// Case 3: chain with three waiters, each holder 1.2 s: C waits ~3.6 s in 3 stages.
{ const X = await mk(), W = [await mk(), await mk(), await mk()];
  await X.query("BEGIN"); await X.query(LOCK);
  const ps = []; for (const w of W) { await w.query("BEGIN;SET LOCAL lock_timeout='2s'"); ps.push(timed(w, LOCK)); await sleep(50); }
  await sleep(1200); await X.query("COMMIT");
  const out = [];
  for (let i = 0; i < W.length; i++) { out.push(await ps[i]); await sleep(1200); await W[i].query("COMMIT").catch(() => {}); }
  console.log(JSON.stringify({ case: "chain of 3 waiters, each holder 1.2s", waits: out }));
  await X.end(); for (const w of W) await w.end(); }
await s.end(); await admin.query("DROP DATABASE rcf_lt WITH (FORCE)"); await admin.end();
