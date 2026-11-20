// Throwaway: does a FOR KEY SHARE holder upgrade to FOR NO KEY UPDATE while a FOR UPDATE waiter is queued? (and with a 2nd KEY SHARE holder)
import pg from "pg";
const base = (() => { const u = new URL(process.env.SESSION_INGEST_TEST_PG_URL ?? ""); if (!["127.0.0.1", "localhost"].includes(u.hostname) || u.password) throw Error("loopback throwaway fixture only"); u.pathname = "/"; return u.href; })();
const admin = new pg.Client(base + "postgres"); await admin.connect();
await admin.query("DROP DATABASE IF EXISTS rcf_upgrade"); await admin.query("CREATE DATABASE rcf_upgrade");
const mk = async () => { const c = new pg.Client(base + "rcf_upgrade"); await c.connect(); return c; };
const s = await mk(); await s.query("CREATE TABLE clock(singleton boolean PRIMARY KEY, last bigint); INSERT INTO clock VALUES(true,0)");
const settle = (p, ms) => Promise.race([p.then(() => "done", (e) => "error " + e.code), new Promise((r) => setTimeout(() => r("waiting"), ms))]);
for (const variant of ["A KS, W FOR UPDATE queued, A upgrades", "A KS + B KS, W FOR UPDATE queued, A upgrades, then B upgrades"]) {
  const A = await mk(), B = await mk(), W = await mk();
  for (const c of [A, B, W]) await c.query("SET lock_timeout='4s';SET deadlock_timeout='200ms'");
  await A.query("BEGIN"); await A.query("SELECT 1 FROM clock WHERE singleton FOR KEY SHARE");
  const two = variant.includes("B KS");
  if (two) { await B.query("BEGIN"); await B.query("SELECT 1 FROM clock WHERE singleton FOR KEY SHARE"); }
  await W.query("BEGIN"); const w = W.query("SELECT 1 FROM clock WHERE singleton FOR UPDATE");
  await new Promise((r) => setTimeout(r, 300));
  const a = A.query("SELECT 1 FROM clock WHERE singleton FOR NO KEY UPDATE");
  const ra = await settle(a, 1500);
  let rb = "-";
  if (two) { const b = B.query("SELECT 1 FROM clock WHERE singleton FOR NO KEY UPDATE"); rb = await settle(b, 1500); if (ra === "done") { await A.query("COMMIT"); rb = await settle(b, 1500); } }
  const rw0 = await settle(w, 100);
  console.log(JSON.stringify({ variant, A_upgrade: ra, B_upgrade_after_A_commit: rb, W_while_held: rw0 }));
  for (const c of [A, B, W]) { await c.query("ROLLBACK").catch(() => {}); await c.end(); }
}
await s.end(); await admin.query("DROP DATABASE rcf_upgrade WITH (FORCE)"); await admin.end();
