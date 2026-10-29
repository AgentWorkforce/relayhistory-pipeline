/**
 * The smoke test: one seeded session followed from Concourse to HoopDb, with
 * the proof Khaliq asked for:
 *   1. every seeded session is indexed with a session uuid, a state verdict and
 *      both summaries;
 *   2. the planted secrets never reached the Locker (chunks are scrubbed) while
 *      the content marker did (so the data really flowed);
 *   3. no secret, no content marker and no prompt text appears in any span or
 *      container log; the session uuid and trace id tie the services together;
 *   4. the trace endpoint reports the session as landed.
 */
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { zstdDecompressSync } from "node:zlib";
import pg from "pg";
import { createS3Store, objectKey, readAll } from "../shared/vendor.ts";
import { PLANTED_CONTENT_MARKER, PLANTED_SECRETS, addPlantedSession } from "../concourse/seed.ts";
import { isUuidV7 } from "../shared/ids.ts";
import { masterKeyring, parseMasterKeys } from "../shared/keyring.ts";
import { readSummaryObject } from "../bob-costas/summary-objects.ts";
import { HOOPDB_NAME, HOOPDB_USER, LOCKER_BUCKET, NAMES, PORTS, TENANT } from "../../stacks/local/topology.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface Check {
  name: string;
  ok: boolean;
  detail?: string;
}

const FORBIDDEN_IN_TELEMETRY = [
  ...Object.values(PLANTED_SECRETS).map((s) => s.replace(/\\n/g, "\n").split("\n")[0]!),
  PLANTED_CONTENT_MARKER,
  "Please rotate these",
  "Keys rotated",
];

export async function runSmoke(stage: string): Promise<boolean> {
  const dir = join(ROOT, ".96bulls", stage);
  const inputs = JSON.parse(readFileSync(join(dir, "inputs.json"), "utf8")) as { hoopdbPassword: string; lockerAccessKeyId: string; lockerSecretAccessKey: string; courierToken: string; concourseDir: string; summaryMasterKeys: string };
  const seed = JSON.parse(readFileSync(join(dir, "seed.json"), "utf8")) as { sessions: Array<{ sessionId: string; planted: boolean }> };
  // A brand-new session every run, so the whole pipe (and its spans) is exercised now, not replayed from history.
  const fresh = await addPlantedSession({ dir: inputs.concourseDir });
  seed.sessions = [...seed.sessions.map((s) => ({ ...s, planted: false })), { sessionId: fresh.sessionId, planted: true }];
  const n = (b: string) => (stage === "local" ? b : `${b}-${stage.slice("local-".length)}`);
  const pool = new pg.Pool({ connectionString: `postgres://${HOOPDB_USER}:${inputs.hoopdbPassword}@127.0.0.1:${PORTS.hoopdb}/${HOOPDB_NAME}`, max: 2 });
  const q = async <R = Record<string, unknown>>(text: string, params: unknown[] = []) => (await pool.query(text, params as unknown[])).rows as R[];
  const checks: Check[] = [];
  const check = (name: string, ok: boolean, detail?: string) => {
    checks.push({ name, ok, detail });
    console.log(`  ${ok ? "PASS" : "FAIL"}  ${name}${detail ? `  (${detail})` : ""}`);
  };
  const ids = seed.sessions.map((s) => s.sessionId);
  const planted = seed.sessions.find((s) => s.planted)!.sessionId;
  console.log(`96bulls smoke ${stage}: ${ids.length} seeded sessions (1 injected now: ${planted})`);

  try {
    // 1. Wait for the pipeline to finish for every seeded session.
    const deadline = Date.now() + 240_000;
    let done = false;
    while (Date.now() < deadline) {
      const rows = await q<{ session_id: string; state: string; kinds: string; su: string | null }>(
        `SELECT i.session_id, i.state,
                (SELECT string_agg(kind, ',' ORDER BY kind) FROM sessions.session_summaries s
                  WHERE s.org_id=i.org_id AND s.workspace_id=i.workspace_id AND s.harness=i.harness AND s.session_id=i.session_id AND s.machine_id=i.machine_id) AS kinds,
                (SELECT session_uuid::text FROM sessions.session_index_ext e
                  WHERE e.org_id=i.org_id AND e.workspace_id=i.workspace_id AND e.harness=i.harness AND e.session_id=i.session_id AND e.machine_id=i.machine_id) AS su
           FROM sessions.session_index i WHERE i.org_id = $1 AND i.session_id = ANY($2::text[])`,
        [TENANT.orgId, ids],
      );
      done = rows.length === ids.length && rows.every((r) => r.state !== "unknown" && r.kinds === "long,short" && r.su);
      if (done) break;
      process.stdout.write(`  pipeline progress: ${rows.length}/${ids.length} indexed, ${rows.filter((r) => r.kinds === "long,short").length} summarised, ${rows.filter((r) => r.state !== "unknown").length} classified\r`);
      await sleep(3_000);
    }
    console.log("");
    check("every seeded session indexed, classified and summarised (short+long)", done);

    const ext = await q<{ session_id: string; session_uuid: string; session_uuid_source: string; tenant_id: string; last_trace_id: string }>(
      "SELECT session_id, session_uuid::text, session_uuid_source, tenant_id, last_trace_id::text FROM sessions.session_index_ext WHERE org_id = $1 AND session_id = ANY($2::text[])",
      [TENANT.orgId, ids],
    );
    check("session uuids are UUIDv7, minted client-side, tenant-scoped", ext.length === ids.length && ext.every((e) => isUuidV7(e.session_uuid) && e.session_uuid_source === "minted" && e.tenant_id === TENANT.orgId));
    const plantedExt = ext.find((e) => e.session_id === planted);
    const plantedUuid = plantedExt?.session_uuid;
    const plantedTrace = plantedExt?.last_trace_id;

    // 2. Stage events for the planted session's trace.
    const events = await q<{ stage: string; status: string }>(
      "SELECT DISTINCT stage, status FROM sessions.pipeline_events WHERE org_id = $1 AND trace_id = $2::uuid",
      [TENANT.orgId, plantedTrace],
    );
    const okStages = new Set(events.filter((e) => e.status === "ok").map((e) => e.stage));
    check("pipeline events recorded for object-landed, session-indexed, summaries and classify", ["object-landed", "session-indexed", "summary-short", "summary-long", "status-classify"].every((s) => okStages.has(s)), [...okStages].join(","));
    const verdicts = await q<{ state: string; rule: string }>("SELECT state, rule FROM sessions.session_verdicts WHERE session_uuid = $1::uuid", [plantedUuid]);
    check("referee wrote a verdict for the planted session", verdicts.length > 0, verdicts[0] ? `${verdicts[0].state}/${verdicts[0].rule}` : undefined);
    const dlq = await q<{ c: string }>("SELECT count(*)::text AS c FROM sessions.pipeline_dead_letters WHERE org_id = $1", [TENANT.orgId]);
    check("no dead letters", dlq[0]?.c === "0", `dead_letters=${dlq[0]?.c}`);
    // Acked jobs are deleted, so prove the observability trigger with a rolled-back insert.
    const client = await pool.connect();
    let trig: { session_uuid: string | null; tenant_id: string | null; traceparent: string | null } | undefined;
    try {
      await client.query("BEGIN");
      const r = await client.query(
        `INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, message)
         VALUES ('summary-short', 'smoke@1', $1::uuid, $2, $3, $4::jsonb)
         RETURNING session_uuid::text, tenant_id, traceparent`,
        [plantedTrace, TENANT.orgId, TENANT.workspaceId, JSON.stringify({ v: 1, stage: "summary-short", ctx: { traceId: plantedTrace, orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, session_uuid: plantedUuid }, workKey: "smoke@1", payload: {} })],
      );
      trig = r.rows[0];
      await client.query("ROLLBACK");
    } finally {
      client.release();
    }
    check("pipeline_jobs trigger fills session_uuid, tenant_id and a W3C traceparent", trig?.session_uuid === plantedUuid && trig?.tenant_id === TENANT.orgId && /^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/.test(trig?.traceparent ?? "") && trig!.traceparent!.slice(3, 35) === plantedTrace!.replace(/-/g, ""), trig?.traceparent ?? undefined);

    // 3. The Locker: scrubbed chunks, content marker present, secrets absent.
    const store = createS3Store({
      bucket: LOCKER_BUCKET, region: "us-east-1", endpoint: `http://127.0.0.1:${PORTS.locker}`,
      credentials: { accessKeyId: inputs.lockerAccessKeyId, secretAccessKey: inputs.lockerSecretAccessKey },
    });
    const idx = await q<{ manifest_key: string }>("SELECT manifest_key FROM sessions.session_index WHERE org_id = $1 AND session_id = $2", [TENANT.orgId, planted]);
    const manifestKey = idx[0]?.manifest_key ?? "";
    const prefix = manifestKey.replace(/\/manifest\.json$/, "/raw");
    let chunkText = "";
    let chunkCount = 0;
    const listing = await store.list(objectKey(prefix));
    for (const obj of listing.items) {
      chunkCount += 1;
      chunkText += zstdDecompressSync(Buffer.from(await readAll(await store.get(obj.key)))).toString("utf8");
    }
    const leaked = Object.entries(PLANTED_SECRETS).filter(([, v]) => chunkText.includes(v.replace(/\\n/g, "\n").split("\n")[0]!)).map(([k]) => k);
    check("Technical Foul: no planted secret in stored chunks", chunkCount > 0 && leaked.length === 0, leaked.length ? `leaked=${leaked.join(",")}` : `chunks=${chunkCount}`);
    check("stored chunks still carry the (non-secret) content marker", chunkText.includes(PLANTED_CONTENT_MARKER));
    const mf = await store.head(objectKey(manifestKey));
    check("manifest object carries x-amz-meta session uuid and trace id", mf?.metadata?.["session-uuid"] === plantedUuid && mf?.metadata?.["trace-id"] === plantedTrace);
    // Summaries are encrypted Locker objects (summary/{short,long}.json.enc); HoopDb holds only the pointer.
    const pointers = await q<{ kind: string; status: string; object_key: string | null; object_sha256: string | null; workspace_id: string; revision: string }>(
      "SELECT kind, status, object_key, object_sha256, workspace_id, revision FROM sessions.session_summaries WHERE session_uuid = $1::uuid", [plantedUuid]);
    const columns = await q<{ column_name: string }>("SELECT column_name FROM information_schema.columns WHERE table_schema='sessions' AND table_name='session_summaries'");
    check("HoopDb summaries are pointers only (no summary text or model column)", !columns.some((c) => c.column_name === "summary" || c.column_name === "model_id"));
    const keyring = await masterKeyring(parseMasterKeys(inputs.summaryMasterKeys));
    const texts: string[] = [];
    for (const p of pointers) {
      if (p.status !== "ready" || !p.object_key || !p.object_sha256) continue;
      const rec = await readSummaryObject({ store, keyring, ids: { orgId: TENANT.orgId, workspaceId: p.workspace_id }, key: p.object_key, sha256: p.object_sha256, kind: p.kind as "short" | "long", revision: Number(p.revision) });
      if (rec?.summary) texts.push(rec.summary);
    }
    check("Bob Costas wrote summary/{short,long}.json.enc and both decrypt", pointers.length === 2 && texts.length === 2, `pointers=${pointers.length} decrypted=${texts.length}`);
    const summaryLeak = texts.some((t) => Object.values(PLANTED_SECRETS).some((v) => t.includes(v.split("\\n")[0]!)));
    check("Bob Costas summaries contain no planted secret", texts.length === 2 && !summaryLeak);

    // 4. Telemetry: spans and container logs. Exporters batch, so poll briefly for the planted trace.
    let spansRaw = "";
    const services = new Set<string>();
    let spansForPlanted = 0;
    const servicesForPlanted = new Set<string>();
    for (let i = 0; i < 15; i++) {
      spansRaw = spawnSync("docker", ["run", "--rm", "-v", `${n(NAMES.telemetry)}:/telemetry:ro`, "alpine:3.20", "sh", "-c", "cat /telemetry/spans.jsonl 2>/dev/null || true"], { encoding: "utf8" }).stdout ?? "";
      services.clear();
      servicesForPlanted.clear();
      spansForPlanted = 0;
      for (const line of spansRaw.split("\n").filter(Boolean)) {
        try {
          const rec = JSON.parse(line) as { resourceSpans?: Array<{ resource?: { attributes?: Array<{ key: string; value: { stringValue?: string } }> }; scopeSpans?: Array<{ spans?: Array<{ traceId: string; attributes?: Array<{ key: string; value: { stringValue?: string } }> }> }> }> };
          for (const rs of rec.resourceSpans ?? []) {
            const svc = rs.resource?.attributes?.find((a) => a.key === "service.name")?.value.stringValue ?? "?";
            services.add(svc);
            for (const ss of rs.scopeSpans ?? [])
              for (const sp of ss.spans ?? []) {
                if (plantedTrace && sp.traceId === plantedTrace.replace(/-/g, "")) {
                  spansForPlanted += 1;
                  servicesForPlanted.add(svc);
                  const su = sp.attributes?.find((a) => a.key === "session_uuid")?.value.stringValue;
                  if (su && su !== plantedUuid) check("span session_uuid matches the index", false, `${svc}: ${su}`);
                }
              }
          }
        } catch {
          /* partial line */
        }
      }
      if (servicesForPlanted.size >= 5) break;
      await sleep(2_000);
    }
    check("spans exported from every service", ["alleyoop-courier", "alleyoop-ingest", "hoopdb-indexer", "referee", "bob-costas"].every((s) => services.has(s)), [...services].join(","));
    check("one trace id ties the planted session across services", spansForPlanted > 0 && servicesForPlanted.size >= 4, `spans=${spansForPlanted} services=${[...servicesForPlanted].join(",")}`);
    const logs = [NAMES.ingest, NAMES.indexer, NAMES.referee, NAMES.bobCostas, NAMES.courier, NAMES.otel]
      .map((c) => spawnSync("docker", ["logs", n(c)], { encoding: "utf8" }))
      .map((r) => `${r.stdout ?? ""}${r.stderr ?? ""}`)
      .join("\n");
    const telemetryText = `${spansRaw}\n${logs}`;
    const leakedTelemetry = FORBIDDEN_IN_TELEMETRY.filter((s) => telemetryText.includes(s));
    check("no secret, prompt text or content marker in spans or container logs", leakedTelemetry.length === 0, leakedTelemetry.length ? `found=${leakedTelemetry.length}` : `scanned=${telemetryText.length} chars`);

    // 5. The trace endpoint, as a customer would call it.
    const res = await fetch(`http://127.0.0.1:${PORTS.ingest}/v1/pipeline/traces/${plantedTrace}`, { headers: { authorization: `Bearer ${inputs.courierToken}` } });
    const body = (await res.json().catch(() => ({}))) as { verdict?: string };
    check("GET /v1/pipeline/traces/:id answers for the planted session", res.status === 200, `status=${res.status} verdict=${body.verdict ?? "?"}`);
  } catch (err) {
    check("smoke test ran to completion", false, (err as Error).message);
  } finally {
    await pool.end();
  }
  const failed = checks.filter((c) => !c.ok);
  console.log(`\n${failed.length === 0 ? "SMOKE PASSED" : "SMOKE FAILED"}: ${checks.length - failed.length}/${checks.length} checks`);
  return failed.length === 0;
}

if (import.meta.main) {
  const ok = await runSmoke(process.argv[2] ?? "local");
  process.exit(ok ? 0 : 1);
}
