/**
 * Lock sampler thread of bench.ts (a Bun Worker, its own event loop so client load cannot delay it). Back to back, on one
 * superuser connection to the throwaway database: pgrowlocks() of the clock row, the not-granted pg_locks rows with their
 * blockers, the granted tuple locks, and the application_name (stage tag) of every active backend.
 */
import pg from "pg";

declare const self: Worker;
export const SAMPLE_SQL = `SELECT extract(epoch from clock_timestamp())*1000 AS ts,
  (SELECT coalesce(json_agg(json_build_object('x',r.locker::text,'m',r.modes,'p',r.pids)),'[]') FROM pgrowlocks('sessions.ingest_receipt_seat_clock') r) AS holders,
  (SELECT coalesce(json_agg(json_build_object('pid',l.pid,'t',l.locktype,'rel',l.relation::oid::int,'c',l.classid::oid::int8,'o',l.objid::oid::int8,'s',l.objsubid,'mode',l.mode,'b',pg_blocking_pids(l.pid))),'[]')
     FROM pg_locks l WHERE NOT l.granted AND l.pid<>pg_backend_pid()) AS waits,
  (SELECT coalesce(json_agg(json_build_object('pid',l.pid,'rel',l.relation::oid::int)),'[]') FROM pg_locks l WHERE l.granted AND l.locktype='tuple') AS tuples,
  (SELECT coalesce(json_object_agg(a.pid,a.application_name),'{}') FROM pg_stat_activity a WHERE a.datname=current_database() AND a.state<>'idle' AND a.application_name LIKE 'scb:%') AS apps`;

let running = true;
self.onmessage = async (ev: MessageEvent) => {
  const m = ev.data as { kind: "start"; dbUrl: string } | { kind: "stop" };
  if (m.kind === "stop") { running = false; return; }
  const c = new pg.Client({ connectionString: m.dbUrl }); await c.connect();
  const pid = (await c.query("SELECT pg_backend_pid() p")).rows[0].p as number;
  self.postMessage({ kind: "started", pid });
  const samples: unknown[] = [];
  while (running) {
    const r = (await c.query(SAMPLE_SQL)).rows[0];
    samples.push({ ts: Number(r.ts), holders: r.holders, waits: r.waits, tuples: r.tuples, apps: r.apps });
    await new Promise((res) => setImmediate(res)); // lets the stop message in
  }
  await c.end();
  self.postMessage({ kind: "done", samples });
};
