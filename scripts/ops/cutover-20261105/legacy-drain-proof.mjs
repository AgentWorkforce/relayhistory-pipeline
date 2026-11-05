// legacy-drain-proof.mjs — READ ONLY evidence that relayhistory's out-of-transaction legacy writers
// have drained, for relayhistory-cloud docs/buffered-delivery-cutover.md ("Prerequisite that this
// source CANNOT establish" and "Historical S3 batch marker bridge").
//
// Usage (sf-frame):  node legacy-drain-proof.mjs [--hold-start <ISO>] [--expect-version <cf version id>] < creds.json
// stdin: one JSON line {ownerUrl, cfToken, aws:{accessKeyId,secretAccessKey,sessionToken}}  (RAM only)
// Without --hold-start it runs as a PRE-HOLD BASELINE: it reports, and every gate is FAIL by definition.
//
// It never writes: Neon in BEGIN READ ONLY, Cloudflare GETs, S3 ListObjectsV2 GETs. No credential is printed.
import { readFileSync } from "node:fs";
import { createHash, createHmac } from "node:crypto";

const PG = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104/node_modules/pg/lib/index.js";
const ACC = "f7232cb80f6fab86a95426302af243e4";
const WORKER = "relayhistory-api";
const REPLICA_Q = "b2e92a0b2b984803b71cc3e02f32fac1";
const REPLICA_DLQ = "91960f439c4d4509af3af968440091bc";
const BUCKET = "96bulls-aws-sessions-prod-transcripts-941433935742";
const REGION = "us-east-1";
const KEY_ROOT = "96bulls"; // WRITE_PATH_KEY_ROOT is unset on relayhistory-api, so the default applies
// Old s3_first request wall budget (S3_FIRST_REQUEST_BUDGET_MS=60s) + its 90 s Neon fallback + waitUntil slack.
const OLD_REQUEST_HORIZON_S = 180;
const REPLICA_RETENTION_S = 345600; // 4 days; older messages expire unapplied

const args = process.argv.slice(2);
const opt = (k) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : undefined; };
const holdStart = opt("--hold-start") ? new Date(opt("--hold-start")) : null;
const expectVersion = opt("--expect-version");
if (holdStart && !Number.isFinite(holdStart.getTime())) throw new Error("bad --hold-start");
const creds = JSON.parse(readFileSync(0, "utf8").split("\n")[0]);
const H = (v) => createHash("sha256").update(JSON.stringify(v)).digest("hex");
const results = [];
const gate = (id, pass, evidence, note) => results.push({ id, result: pass ? "PASS" : "FAIL", note, evidence, evidenceSha256: H(evidence) });
const info = (id, evidence, note) => results.push({ id, result: "INFO", note, evidence, evidenceSha256: H(evidence) });

// ---------- Cloudflare (GET only) ----------
const cf = async (path) => {
  const r = await fetch(`https://api.cloudflare.com/client/v4/accounts/${ACC}${path}`, { headers: { Authorization: "Bearer " + creds.cfToken }, signal: AbortSignal.timeout(30000) });
  const j = await r.json();
  if (!j.success) throw new Error("cloudflare " + path + " " + JSON.stringify(j.errors).slice(0, 200));
  return j.result;
};

// ---------- S3 ListObjectsV2 (SigV4, GET only) ----------
const hmac = (k, s) => createHmac("sha256", k).update(s).digest();
const enc = (s) => encodeURIComponent(s).replace(/[!'()*]/g, (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase());
async function s3List(params) {
  const host = `${BUCKET}.s3.${REGION}.amazonaws.com`;
  const q = Object.entries({ "list-type": "2", ...params }).sort(([a], [b]) => (a < b ? -1 : 1)).map(([k, v]) => `${enc(k)}=${enc(v)}`).join("&");
  const now = new Date(), amz = now.toISOString().replace(/[:-]|\.\d{3}/g, ""), day = amz.slice(0, 8);
  const ph = createHash("sha256").update("").digest("hex");
  const hdrs = { host, "x-amz-content-sha256": ph, "x-amz-date": amz, ...(creds.aws.sessionToken ? { "x-amz-security-token": creds.aws.sessionToken } : {}) };
  const names = Object.keys(hdrs).sort();
  const creq = ["GET", "/", q, names.map((n) => `${n}:${hdrs[n]}\n`).join(""), names.join(";"), ph].join("\n");
  const scope = `${day}/${REGION}/s3/aws4_request`;
  const sts = ["AWS4-HMAC-SHA256", amz, scope, createHash("sha256").update(creq).digest("hex")].join("\n");
  const key = hmac(hmac(hmac(hmac("AWS4" + creds.aws.secretAccessKey, day), REGION), "s3"), "aws4_request");
  const auth = `AWS4-HMAC-SHA256 Credential=${creds.aws.accessKeyId}/${scope}, SignedHeaders=${names.join(";")}, Signature=${createHmac("sha256", key).update(sts).digest("hex")}`;
  const r = await fetch(`https://${host}/?${q}`, { headers: { ...hdrs, Authorization: auth }, signal: AbortSignal.timeout(30000) });
  const x = await r.text();
  if (r.status !== 200) throw new Error("s3 list " + r.status + " " + x.slice(0, 200));
  const all = (tag) => [...x.matchAll(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, "g"))].map((m) => m[1]);
  return { contents: [...x.matchAll(/<Contents>([\s\S]*?)<\/Contents>/g)].map((m) => ({ key: (m[1].match(/<Key>(.*?)<\/Key>/) || [])[1], lastModified: (m[1].match(/<LastModified>(.*?)<\/LastModified>/) || [])[1] })),
    prefixes: all("Prefix").filter((p) => p !== params.prefix), next: (x.match(/<NextContinuationToken>(.*?)<\/NextContinuationToken>/) || [])[1] };
}

// ---------- Neon (READ ONLY) ----------
const pg = (await import(PG)).default;
const db = new pg.Client({ connectionString: creds.ownerUrl, ssl: { rejectUnauthorized: true }, application_name: "bulls-legacy-drain-proof", statement_timeout: 30000 });
creds.ownerUrl = "";
await db.connect();
await db.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
const at = new Date();
const mode = holdStart ? "HOLD" : "PRE_HOLD_BASELINE";
const sinceHold = holdStart ? (at - holdStart) / 1000 : null;

try {
  // W0 cutover phase (context)
  const st = (await db.query("SELECT sessions.delivery_cutover_status() AS s")).rows[0].s;
  info("W0_cutover_phase", st, "0078 control state");

  // W1 old Worker version retired: only the held version serves, deployed after/at hold start, horizon elapsed.
  const deps = await cf(`/workers/scripts/${WORKER}/deployments`);
  const cur = (deps.deployments || deps)[0];
  const versions = (cur?.versions || []).map((v) => ({ id: v.version_id, pct: v.percentage }));
  const single = versions.length === 1 && versions[0].pct === 100;
  const ev1 = { deploymentId: cur?.id, createdOn: cur?.created_on, versions, expectVersion: expectVersion ?? null, sinceHoldSeconds: sinceHold };
  gate("W1_old_worker_version_retired",
    !!holdStart && single && (!expectVersion || versions[0].id === expectVersion) && new Date(cur.created_on) <= at && sinceHold >= OLD_REQUEST_HORIZON_S,
    ev1, `single 100% version${expectVersion ? " = expected" : ""}, and >= ${OLD_REQUEST_HORIZON_S}s since hold start (old s3_first budget 60s + fallback 90s + waitUntil)`);

  // W2 Neon in-flight legacy writers: none running, none started before the hold.
  const w = (await db.query(`SELECT CASE WHEN query ILIKE '%apply_neon_replica%' THEN 'replica' WHEN query ILIKE '%accept_delivery_batch%' THEN 'accept'
      WHEN query ILIKE '%set_session_uploads%' THEN 'selection' WHEN query ILIKE '%reset_uploaded_sessions%' THEN 'reset' END AS k,
      count(*)::int AS n, min(xact_start) AS oldest FROM pg_stat_activity WHERE datname='neondb' AND pid<>pg_backend_pid() AND xact_start IS NOT NULL
      AND (query ILIKE '%apply_neon_replica%' OR query ILIKE '%accept_delivery_batch%' OR query ILIKE '%set_session_uploads%' OR query ILIKE '%reset_uploaded_sessions%') GROUP BY 1`)).rows;
  const total = w.reduce((a, r) => a + r.n, 0);
  gate("W2_neon_inflight_legacy_writers", !!holdStart && total === 0, { byKind: w, total }, "0 transactions in accept/replica/selection/reset");

  // W3 NEON_REPLICA producer stopped and consumer quiesced.
  const q = await cf(`/queues/${REPLICA_Q}`), qm = await cf(`/queues/${REPLICA_Q}/metrics`);
  const oldestAgeS = qm.oldest_message_timestamp_ms ? Math.round((at - qm.oldest_message_timestamp_ms) / 1000) : null;
  const replicaInflight = (w.find((r) => r.k === "replica") || { n: 0 }).n;
  const ev3 = { paused: q.settings?.delivery_paused ?? null, backlog: qm.backlog_count, backlogBytes: qm.backlog_bytes, oldestAgeSeconds: oldestAgeS,
    secondsToExpiry: oldestAgeS === null ? null : REPLICA_RETENTION_S - oldestAgeS, consumers: (q.consumers || []).map((c) => ({ script: c.script, maxConcurrency: c.settings?.max_concurrency })), replicaInflight };
  gate("W3_neon_replica_quiesced", !!holdStart && replicaInflight === 0 && (qm.backlog_count === 0 || q.settings?.delivery_paused === true),
    ev3, "backlog 0, or delivery paused with 0 in-flight apply_neon_replica_v1; a paused nonzero backlog is UN-REPLICATED to Neon (bridge-covered for receipts only) and expires at 4 days");

  // W4 DLQ (context: ACKed-but-never-applied batches; their receipts are only in committed S3 markers)
  const dm = await cf(`/queues/${REPLICA_DLQ}/metrics`);
  info("W4_replica_dlq", { backlog: dm.backlog_count, backlogBytes: dm.backlog_bytes }, "dead replica messages; receipts recoverable only through the S3 marker bridge");

  // W5 No new live-batch marker PUT after hold start, over every org active in the last 72 h (bounded scan).
  const orgs = (await db.query("SELECT DISTINCT org_id FROM sessions.delivery_receipts WHERE received_at > now() - interval '72 hours'")).rows.map((r) => r.org_id);
  let scanned = 0, newestAll = null, afterHold = 0; const perOrg = [];
  const lim = 8; let idx = 0;
  await Promise.all(Array.from({ length: lim }, async () => {
    while (idx < orgs.length) {
      const org = orgs[idx++]; let token, newest = null, n = 0, late = 0;
      do {
        const page = await s3List({ prefix: `${KEY_ROOT}/${org}/live-batches/v1/`, "max-keys": "1000", ...(token ? { "continuation-token": token } : {}) });
        for (const c of page.contents) { n++; if (!newest || c.lastModified > newest) newest = c.lastModified; if (holdStart && new Date(c.lastModified) >= holdStart) late++; }
        token = page.next;
      } while (token);
      scanned += n; afterHold += late; if (newest && (!newestAll || newest > newestAll)) newestAll = newest;
      perOrg.push({ org, markers: n, newest, afterHold: late });
    }
  }));
  perOrg.sort((a, b) => (a.org < b.org ? -1 : 1));
  gate("W5_no_marker_put_after_hold", !!holdStart && afterHold === 0,
    { orgsScanned: orgs.length, markersScanned: scanned, newestMarker: newestAll, markersAtOrAfterHold: afterHold, perOrgSha256: H(perOrg) },
    "live-batches/v1 markers with LastModified >= hold start across orgs with receipts in 72 h (LastModified is S3 server time)");

  // W6 ACK/receipt coverage gap that the bridge must carry: committed s3_first ACKs whose Neon receipt is absent.
  info("W6_bridge_gap_estimate", { replicaBacklog: qm.backlog_count, replicaDlq: dm.backlog_count },
    "each unapplied replica message is an s3_first-ACKed batch whose only receipt is its committed S3 marker");
} finally {
  await db.query("ROLLBACK").catch(() => {});
  await db.end();
}
const gates = results.filter((r) => r.result !== "INFO");
const out = { tool: "legacy-drain-proof v1", mode, at: at.toISOString(), holdStart: holdStart?.toISOString() ?? null,
  overall: holdStart && gates.every((g) => g.result === "PASS") ? "PASS" : "FAIL", results };
out.reportSha256 = H(out);
console.log(JSON.stringify(out, null, 1));
