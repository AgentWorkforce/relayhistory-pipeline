import pg from "pg";
const c = new pg.Client({ connectionString: process.env.URL, application_name: "96bulls-impact-check", statement_timeout: 60000 });
await c.connect();
await c.query("SET default_transaction_read_only = on");
const q = async (sql) => (await c.query(sql)).rows;
const win = "received_at BETWEEN '2026-10-30 20:20:00+00' AND '2026-10-30 20:50:00+00'";
console.log(JSON.stringify({
  deliveryPerMinute: await q(`SELECT to_char(date_trunc('minute', received_at), 'HH24:MI') AS m, count(*)::int AS n FROM sessions.delivery_records WHERE ${win} GROUP BY 1 ORDER BY 1`),
  eventsPerMinute: await q(`SELECT to_char(date_trunc('minute', ingested_at), 'HH24:MI') AS m, count(*)::int AS n FROM sessions.convergence_events WHERE ingested_at BETWEEN '2026-10-30 20:20:00+00' AND '2026-10-30 20:50:00+00' GROUP BY 1 ORDER BY 1`),
  receiptsPerMinute: await q(`SELECT to_char(date_trunc('minute', received_at), 'HH24:MI') AS m, count(*)::int AS n FROM sessions.delivery_receipts WHERE ${win} GROUP BY 1 ORDER BY 1`).catch(e => ({ error: e.message })),
  outbox: await q(`SELECT count(*) FILTER (WHERE sent_at IS NULL)::int AS unsent, min(created_at) FILTER (WHERE sent_at IS NULL)::text AS oldest_unsent FROM sessions.session_event_outbox`).catch(e => ({ error: e.message })),
  revisionFeed: await q(`SELECT count(*) FILTER (WHERE acknowledged_at IS NULL)::int AS unacked, min(created_at) FILTER (WHERE acknowledged_at IS NULL)::text AS oldest_unacked FROM sessions.session_revision_events`).catch(e => ({ error: e.message })),
  idleInTx: await q(`SELECT pid, usename, application_name, extract(epoch FROM now() - xact_start)::int AS xact_s, extract(epoch FROM now() - state_change)::int AS idle_s, left(regexp_replace(query, '\\s+', ' ', 'g'), 80) AS last_query FROM pg_stat_activity WHERE datname = current_database() AND state = 'idle in transaction' ORDER BY xact_start`),
}, null, 1));
await c.end();
