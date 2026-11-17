#!/usr/bin/env bun
/**
 * The two-replica ingest-edge proof (scripts/proof-ingest-edge-multireplica.ts) on THIS machine, with no cloud and
 * no real credential: a fresh compose project (e2e/selfhost/compose.yaml + e2e/multireplica/compose.yaml) runs
 * Postgres, MinIO, the role bootstrap, the Control introspection stub and TWO ingest-edge processes (ingest-r1,
 * ingest-r2) on one bucket and key root with INGEST_LEDGER=s3, async admission, INGEST_S3_TENANT_LIMIT=on with
 * halved per-replica caps, INGEST_LEDGER_LOG_COMMIT off and the direct upload inbox on. Then the proof's own
 * runProof() drives the burst over BOTH processes round-robin, the mid-burst S3-ledger reset, quiescence and the
 * exactly-once assertions, and additionally requires each process's log to show admissions of its own.
 *
 *   bun scripts/proof-ingest-edge-multireplica-local.ts [--image <tag>] [--users 3] [--sessions-per-origin 4]
 *     [--revisions 4] [--retry-copies 4] [--concurrency 12] [--quiesce-timeout-s 600] [--receipt-out <file>] [--keep]
 *
 * Without --image it builds docker/services.Dockerfile as 96bulls/services:mrp-<project> and removes it after.
 * Synthetic everything: per-run sh_at_ tokens (the stub's table), a per-run witness key, one test org/workspace,
 * the fence-authority marker for that org written into the local bucket (as e2e/pipeline upload.s3_ledger does).
 * The direct path stands in for relay-control: the script reads the user's privacy-journal seq, signs the witness
 * with the run's key and does the create-only PUT into the upload inbox itself (no presigned URL).
 * Receipt (counts only) on stdout; exit 0 PASS, 1 FAIL, 2 the stack did not come up. The compose project, its
 * volumes, the per-run certificate directory and (unless --image) the image are removed on exit (--keep: left up).
 */
import { spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { writeFileSync } from "node:fs";
import { parseArgs } from "node:util";
import { FENCE_AUTHORITY } from "../e2e/pipeline/fixtures.ts";
import { buildImage, createStack } from "../e2e/selfhost/stack.ts";
import { witnessPrivacyJournal } from "../src/session-ingest/async-admission.ts";
import { orgFenceAuthorityKey } from "../src/session-ingest/ledger-s3.ts";
import {
  encodeUploadWitness, parseUploadWitnessKey, uploadKeyPrefix, uploadObjectKey, uploadObjectSha256Hex,
} from "../src/session-ingest/upload-inbox-contract.ts";
import { createS3Store, isObjectStoreError, objectKey, type ObjectStore } from "../src/shared/vendor.ts";
import { planBurst, preflightRefusals, runProof, tallyLog, type DirectPut, type ProofConfig } from "./proof-ingest-edge-multireplica.ts";

const KEY_ROOT = "96bulls";
const BUCKET = "transcripts";
export const LOCAL_ORG = "org_mrp_local";
export const LOCAL_WORKSPACE = "ws_mrp_local";
export const REPLICAS = ["ingest-r1", "ingest-r2"] as const;

const { values: v } = parseArgs({
  args: process.argv.slice(2), strict: true, allowPositionals: false,
  options: {
    image: { type: "string" }, users: { type: "string", default: "3" }, "sessions-per-origin": { type: "string", default: "4" },
    revisions: { type: "string", default: "4" }, "retry-copies": { type: "string", default: "4" }, concurrency: { type: "string", default: "12" },
    "quiesce-timeout-s": { type: "string", default: "600" }, "receipt-out": { type: "string" }, keep: { type: "boolean", default: false },
  },
});
const int = (s: string | undefined) => (s !== undefined && /^[0-9]{1,6}$/.test(s) ? Number(s) : NaN);
const log = (msg: string) => process.stderr.write(`[mrp-local] ${msg}\n`);

const project = `rh-mrp-${randomBytes(4).toString("hex")}`;
const image = v.image?.trim() || `96bulls/services:mrp-${project}`;
const users = Array.from({ length: int(v.users) || 0 }, (_, i) => ({ userId: `user_mrp_${i}`, token: `sh_at_${randomBytes(18).toString("hex")}` }));
const tokenTable = Object.fromEntries(users.map((u) => [u.token, { sub: u.userId, org: LOCAL_ORG, workspace: LOCAL_WORKSPACE, scopes: ["rth:read", "rth:sync"] }]));
const witnessKeyText = randomBytes(32).toString("base64url");
const witnessKey = parseUploadWitnessKey(witnessKeyText);

const stack = createStack({
  files: [new URL("../e2e/multireplica/compose.yaml", import.meta.url).pathname],
  project,
  extraEnv: { E2E_IMAGE: image, E2E_TOKENS: JSON.stringify(tokenTable), E2E_UPLOAD_WITNESS_KEY: witnessKeyText },
});
let downDone = false;
const down = () => {
  if (downDone) return;
  downDone = true;
  if (v.keep) return log(`--keep: compose project ${project} left up (image ${image}). Remove it with: docker rm -f $(docker ps -aq --filter label=com.docker.compose.project=${project}); docker volume rm ${project}_pgdata ${project}_miniodata; docker network rm ${project}_default; and its .96bulls/e2e-* certificate directory`);
  stack.compose(["down", "-v", "--remove-orphans"], { quiet: true, allowFail: true });
  stack.cleanup();
  if (!v.image) spawnSync("docker", ["rmi", "-f", image], { stdio: "ignore" });
};
process.on("SIGINT", () => { down(); process.exit(130); });
process.on("SIGTERM", () => { down(); process.exit(143); });

const hostUrl = (service: string, port: string) => {
  const out = stack.compose(["port", service, port], { quiet: true, allowFail: true }).stdout.trim();
  const value = out.slice(out.lastIndexOf(":") + 1);
  if (!/^[1-9]\d*$/u.test(value)) throw new Error(`no host port for ${service}:${port} (is it running?)`);
  return `http://127.0.0.1:${value}`;
};
const ready = async (url: string) => {
  for (let i = 0; i < 120; i++) {
    try { if ((await fetch(url, { signal: AbortSignal.timeout(3000) })).ok) return; } catch {}
    await new Promise((r) => setTimeout(r, 2000));
  }
  throw new Error(`not ready within 240s: ${new URL(url).host}${new URL(url).pathname}`);
};
const replicaLog = (service: string) => stack.compose(["logs", "--no-color", "--no-log-prefix", service], { quiet: true, allowFail: true }).stdout;

/** relay-control's credential mint and the desktop's create-only PUT, for one batch object. */
function localDirectPut(store: ObjectStore): DirectPut {
  return async (u, originId, body, bytes, copies) => {
    const tenant = { orgId: LOCAL_ORG, workspaceId: LOCAL_WORKSPACE, userId: users[u]!.userId };
    let seq: number;
    try { seq = await witnessPrivacyJournal(store, KEY_ROOT, tenant); } catch { return { code: "privacy_journal_unreadable" }; }
    const key = uploadObjectKey(uploadKeyPrefix({ keyRoot: KEY_ROOT, ...tenant }), { originId, batchId: body.batch.batch_id, objectSha256Hex: uploadObjectSha256Hex(bytes), witnessSeq: seq });
    const witness = encodeUploadWitness(witnessKey, { v: 1, org: LOCAL_ORG, ws: LOCAL_WORKSPACE, user: tenant.userId, seq, exp: Math.floor(Date.now() / 1000) + 3600 });
    const put = async () => {
      try { await store.put(objectKey(key), bytes, { ifNoneMatch: true, metadata: { "relay-witness": witness } }); return true; }
      catch (e) { return isObjectStoreError(e, "already_exists") || isObjectStoreError(e, "precondition_failed"); }
    };
    const ok = (await Promise.all(Array.from({ length: copies }, put))).some(Boolean);
    return ok ? { uploadKey: key } : { code: "put_failed" };
  };
}

async function main(): Promise<number> {
  const c: ProofConfig = {
    targetEnv: "sandbox", deployment: "local-test", iKnowThisIsDev: true, ingestUrl: "http://127.0.0.1", org: LOCAL_ORG, workspace: LOCAL_WORKSPACE,
    users: users.map((u, i) => ({ userId: u.userId, tokenEnv: `MRP_LOCAL_TOKEN_${i}` })), keyRoot: KEY_ROOT, bucket: BUCKET, region: "us-east-1",
    credentials: "keys", replicasConfirmed: true, sessionsPerOrigin: int(v["sessions-per-origin"]), revisions: int(v.revisions),
    retryCopies: int(v["retry-copies"]), concurrency: int(v.concurrency), quiesceTimeoutS: int(v["quiesce-timeout-s"]), allowRetryCodes: [], planOnly: false,
  };
  // The proof's own shape checks (ranges, users); the tokens only ever live in this process.
  const refusals = preflightRefusals(c, Object.fromEntries(users.map((u, i) => [`MRP_LOCAL_TOKEN_${i}`, u.token])));
  if (refusals.length) { process.stdout.write(JSON.stringify({ refused: refusals }) + "\n"); return 2; }

  if (!v.image) { log(`building ${image} ...`); buildImage(image); }
  log(`starting compose project ${project}: ${REPLICAS.join(", ")} (+ db, minio, migrate, roles, control-stub)`);
  stack.compose(["down", "-v", "--remove-orphans"], { quiet: true, allowFail: true });
  stack.compose(["up", "-d", "minio", ...REPLICAS], { quiet: true });
  const ingestUrls = REPLICAS.map((r) => hostUrl(r, "8081"));
  await Promise.all(ingestUrls.map((u) => ready(`${u}/readyz`)));
  const store = createS3Store({
    bucket: BUCKET, region: "us-east-1", endpoint: hostUrl("minio", "9000"),
    credentials: { accessKeyId: stack.env.E2E_MINIO_USER!, secretAccessKey: stack.env.E2E_MINIO_PASSWORD! },
  }) as unknown as ObjectStore;
  // The operator's per-org fence-authority marker: without it the S3 ledger admits nothing for the org.
  await store.put(objectKey(orgFenceAuthorityKey(KEY_ROOT, LOCAL_ORG)), new TextEncoder().encode(JSON.stringify({ ...FENCE_AUTHORITY, orgId: LOCAL_ORG, writtenAtMs: Date.now() })), { ifNoneMatch: true });
  for (const r of REPLICAS) {
    const probe = await (await fetch(`${ingestUrls[REPLICAS.indexOf(r)]}/healthz`)).json() as { admission?: string };
    if (probe.admission !== "async") throw new Error(`${r} is not in async admission (${String(probe.admission)})`);
  }
  log("both replicas ready (async admission); fence-authority marker written; burst starting");

  const runId = randomBytes(5).toString("hex");
  const chains = planBurst(runId, users.length, { sessionsPerOrigin: c.sessionsPerOrigin, revisions: c.revisions, retryCopies: c.retryCopies, direct: true });
  const { receipt } = await runProof(c, {
    runId, chains, store, tokens: users.map((u) => u.token), ingestUrls, replicas: { by: "local_compose", ready: REPLICAS.length },
    collectLogs: async () => Object.fromEntries(REPLICAS.map((r) => [r, tallyLog(replicaLog(r))])),
    directPut: localDirectPut(store), requireAdmissionsPerReplica: true,
  });
  const restarts = Object.fromEntries(REPLICAS.map((r) => {
    const id = stack.compose(["ps", "-q", r], { quiet: true, allowFail: true }).stdout.trim();
    const out = id ? spawnSync("docker", ["inspect", "-f", "{{.RestartCount}}", id], { encoding: "utf8" }).stdout.trim() : "";
    return [r, /^\d+$/.test(out) ? Number(out) : null];
  }));
  const full = {
    ...receipt, mode: "local",
    local: { replicas: REPLICAS.length, replica_restarts: restarts, env: { INGEST_LEDGER: "s3", SELFHOST_INGEST_ADMISSION: "async", INGEST_S3_TENANT_LIMIT: "on", INGEST_LEDGER_LOG_COMMIT: "off", HISTORY_UPLOAD_DIRECT: "on",
      SELFHOST_ADMISSION_CONCURRENCY: stack.env.E2E_MRP_CONCURRENCY ?? "8", INGEST_ASYNC_MAX_PER_ORG: stack.env.E2E_MRP_ASYNC_MAX_PER_ORG ?? "2", INGEST_MAX_PER_USER: stack.env.E2E_MRP_MAX_PER_USER ?? "2", SELFHOST_ADMISSION_SCAN_MS: stack.env.E2E_MRP_SCAN_MS ?? "2000" } },
  };
  if (Object.values(restarts).some((n) => n !== 0)) { (full as any).verdict = "FAIL"; (full as any).failures = { ...(full as any).failures, replica_restarted: 1 }; }
  const text = JSON.stringify(full, null, 2);
  process.stdout.write(text + "\n");
  if (v["receipt-out"]) writeFileSync(v["receipt-out"], text + "\n");
  if (full.verdict !== "PASS") for (const r of REPLICAS) process.stderr.write(`--- ${r} (last 40 lines)\n${replicaLog(r).split("\n").slice(-40).join("\n")}\n`);
  return full.verdict === "PASS" ? 0 : 1;
}

let code = 2;
try { code = await main(); }
catch (error) {
  log(`aborted: ${(error as Error).message}`);
  for (const svc of ["migrate", "roles", ...REPLICAS]) process.stderr.write(`--- ${svc}\n${replicaLog(svc).split("\n").slice(-40).join("\n")}\n`);
}
finally { down(); }
process.exit(code);
