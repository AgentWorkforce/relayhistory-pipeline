import { describe, expect, test } from "bun:test";
import {
  BLOCKED_REVISION, evaluate, parseDeployment, parseProofArgs, planBurst, planCounts, ownershipCheck, preflightRefusals, resetOwnershipCheck, tallyLog,
  type BatchObservation, type Observation, type ProofConfig,
} from "../../../scripts/proof-ingest-edge-multireplica.ts";
import { s3Ledger, s3LedgerKeys, s3LedgerObjects, sessionKeyHash as sessionKey, type FenceDoc, type OwnerDoc } from "../ledger-s3.ts";
import { scratchStore } from "./support.ts";

const ARGS = [
  "--i-know-this-is-dev", "--target-env", "dev", "--deployment", "agentrelay-dev",
  "--ingest-url", "https://ingest.dev.example.test", "--org", "org_proof", "--workspace", "ws_proof",
  "--user", "user-one:PROOF_TOKEN_1", "--user", "user:two:PROOF_TOKEN_2",
  "--bucket", "proof-canonical-dev-bucket", "--region", "us-east-1", "--replicas-confirmed", "--reset-user-is-synthetic",
];
const ENV = { PROOF_TOKEN_1: "sh_at_one", PROOF_TOKEN_2: "sh_at_two" };
const cfg = (extra: string[] = [], drop: string[] = []): ProofConfig => {
  const args = [...ARGS];
  for (const d of drop) {
    const i = args.indexOf(d);
    const isBool = d === "--i-know-this-is-dev" || d === "--replicas-confirmed" || d === "--reset-user-is-synthetic";
    args.splice(i, isBool ? 1 : 2);
  }
  return parseProofArgs([...args, ...extra]);
};
const with_ = (c: ProofConfig, patch: Partial<ProofConfig>): ProofConfig => ({ ...c, ...patch });

describe("parseProofArgs", () => {
  test("splits each --user at its last colon and keeps the defaults", () => {
    const c = cfg();
    expect(c.users).toEqual([{ userId: "user-one", tokenEnv: "PROOF_TOKEN_1" }, { userId: "user:two", tokenEnv: "PROOF_TOKEN_2" }]);
    expect(c.keyRoot).toBe("96bulls");
    expect([c.sessionsPerOrigin, c.revisions, c.retryCopies, c.concurrency, c.quiesceTimeoutS]).toEqual([4, 4, 4, 12, 600]);
  });
  test("an unknown flag throws", () => {
    expect(() => parseProofArgs([...ARGS, "--force"])).toThrow();
  });
});

describe("preflightRefusals", () => {
  test("a complete dev configuration has none", () => {
    expect(preflightRefusals(cfg(), ENV)).toEqual([]);
  });
  test("refuses without the explicit dev acknowledgement", () => {
    expect(preflightRefusals(cfg([], ["--i-know-this-is-dev"]), ENV)).toContain("missing_i_know_this_is_dev");
  });
  test("refuses a production target env, deployment, host or bucket", () => {
    const c = cfg();
    expect(preflightRefusals(with_(c, { targetEnv: "prod" }), ENV)).toContain("target_env_not_dev_or_stage");
    expect(preflightRefusals(with_(c, { deployment: "agentrelay-prod" }), ENV)).toContain("deployment_looks_like_production");
    expect(preflightRefusals(with_(c, { deployment: "agentrelay-dev-production" }), ENV)).toContain("deployment_looks_like_production");
    expect(preflightRefusals(with_(c, { deployment: "agentrelay" }), ENV)).toContain("deployment_not_marked_non_production");
    expect(preflightRefusals(with_(c, { ingestUrl: "https://ingest.prod.example.test" }), ENV)).toContain("ingest_url_looks_like_production");
    expect(preflightRefusals(with_(c, { ingestUrl: "http://ingest.dev.example.test" }), ENV)).toContain("ingest_url_invalid");
    expect(preflightRefusals(with_(c, { bucket: "sessions-prod" }), ENV)).toContain("bucket_looks_like_production");
    expect(preflightRefusals(with_(c, { controlUrl: "https://control.production.example.test" }), ENV)).toContain("control_url_looks_like_production");
    expect(preflightRefusals(with_(c, { kubeNamespace: "prod", kubeDeployment: "x-dev-ingest-edge" }), ENV)).toContain("kube_namespace_looks_like_production");
  });
  test("a non-production word inside a longer token is not production", () => {
    expect(preflightRefusals(with_(cfg(), { deployment: "agentrelay-dev", bucket: "productive-dev-bucket" }), ENV)).toEqual([]);
  });
  test("needs two distinct users whose tokens are relay-control access tokens", () => {
    const c = cfg();
    expect(preflightRefusals(with_(c, { users: [c.users[0]!] }), ENV)).toContain("need_at_least_two_users");
    expect(preflightRefusals(with_(c, { users: [c.users[0]!, c.users[0]!] }), ENV)).toContain("users_not_distinct");
    expect(preflightRefusals(c, { PROOF_TOKEN_1: "sh_at_one" })).toContain("user_token_missing");
    expect(preflightRefusals(c, { ...ENV, PROOF_TOKEN_2: "rth_at_two" })).toContain("user_token_not_relaycontrol_access_token");
    expect(preflightRefusals(with_(c, { users: [{ userId: "a", tokenEnv: "lower" }, c.users[1]!] }), ENV)).toContain("user_token_env_invalid");
  });
  test("needs the replicas confirmed by flag or by kubectl (both kube flags)", () => {
    const c = cfg([], ["--replicas-confirmed"]);
    expect(preflightRefusals(c, ENV)).toContain("replicas_unconfirmed");
    expect(preflightRefusals(with_(c, { kubeNamespace: "sessions-dev" }), ENV)).toContain("kube_namespace_and_deployment_both_required");
    expect(preflightRefusals(with_(c, { kubeNamespace: "sessions-dev", kubeDeployment: "sp-dev-ingest-edge" }), ENV)).toEqual([]);
  });
  test("every target name needs a non-production marker, not just the absence of a production one", () => {
    const c = cfg();
    expect(preflightRefusals(with_(c, { bucket: "proof-canonical-bucket" }), ENV)).toContain("bucket_not_marked_non_production");
    expect(preflightRefusals(with_(c, { ledgerBucket: "proof-ledger-bucket" }), ENV)).toContain("ledger_bucket_not_marked_non_production");
    expect(preflightRefusals(with_(c, { ledgerBucket: "proof-ledger-prod" }), ENV)).toContain("ledger_bucket_looks_like_production");
    expect(preflightRefusals(with_(c, { ingestUrl: "https://ingest.example.com" }), ENV)).toContain("ingest_url_not_marked_non_production");
    expect(preflightRefusals(with_(c, { controlUrl: "https://control.example.com" }), ENV)).toContain("control_url_not_marked_non_production");
    expect(preflightRefusals(with_(c, { kubeNamespace: "sessions", kubeDeployment: "sp-dev-ingest-edge" }), ENV)).toContain("kube_namespace_not_marked_non_production");
    expect(preflightRefusals(with_(c, { kubeNamespace: "sessions-dev", kubeDeployment: "sp-ingest-edge" }), ENV)).toContain("kube_deployment_not_marked_non_production");
    expect(preflightRefusals(with_(c, { kubeNamespace: "sessions-dev", kubeDeployment: "sp-dev-ingest-edge", kubeContext: "prd-cluster" }), ENV)).toContain("kube_context_looks_like_production");
    expect(preflightRefusals(with_(c, { kubeNamespace: "sessions-dev", kubeDeployment: "sp-dev-ingest-edge", kubeContext: "shared-cluster" }), ENV)).toContain("kube_context_not_marked_non_production");
  });
  test("the endpoint is checked: https to a non-production host, plain http to loopback only", () => {
    const c = cfg();
    expect(preflightRefusals(with_(c, { endpoint: "https://s3.dev.example.test" }), ENV)).toEqual([]);
    expect(preflightRefusals(with_(c, { endpoint: "https://s3.prod.example.test" }), ENV)).toContain("endpoint_looks_like_production");
    expect(preflightRefusals(with_(c, { endpoint: "https://s3.us-east-1.example.com" }), ENV)).toContain("endpoint_not_marked_non_production");
    expect(preflightRefusals(with_(c, { endpoint: "http://minio.dev.example.test:9000" }), ENV)).toContain("endpoint_plain_http_not_loopback");
    expect(preflightRefusals(with_(c, { endpoint: "ftp://x" }), ENV)).toContain("endpoint_invalid");
    // A loopback object store (the local mode) exempts the bucket names from the marker, never from the deny-list.
    expect(preflightRefusals(with_(c, { endpoint: "http://127.0.0.1:9000", bucket: "transcripts" }), ENV)).toEqual([]);
    expect(preflightRefusals(with_(c, { endpoint: "http://localhost:9000", bucket: "transcripts-prod" }), ENV)).toContain("bucket_looks_like_production");
  });
  test("the user-wide reset needs user 0 declared a dedicated synthetic user", () => {
    expect(preflightRefusals(cfg([], ["--reset-user-is-synthetic"]), ENV)).toContain("reset_user_not_declared_synthetic");
    expect(preflightRefusals(cfg(["--plan-only"], ["--reset-user-is-synthetic"]), {})).toContain("reset_user_not_declared_synthetic");
  });
  test("plan-only needs neither tokens nor replicas", () => {
    expect(preflightRefusals(cfg(["--plan-only"], ["--replicas-confirmed"]), {})).toEqual([]);
  });
  test("bounds the burst knobs", () => {
    expect(preflightRefusals(cfg(["--revisions", "1"]), ENV)).toContain("revisions_out_of_range");
    expect(preflightRefusals(cfg(["--concurrency", "abc"]), ENV)).toContain("concurrency_out_of_range");
    expect(preflightRefusals(cfg(["--allow-retry-code", "Bad Code"]), ENV)).toContain("allow_retry_code_invalid");
  });
  test("a split ledger bucket must differ from the canonical bucket", () => {
    expect(preflightRefusals(with_(cfg(), { ledgerBucket: "proof-canonical-dev-bucket" }), ENV)).toContain("ledger_bucket_invalid");
    expect(preflightRefusals(with_(cfg(), { ledgerRegion: "us-west-2" }), ENV)).toContain("ledger_region_without_ledger_bucket");
  });
});

describe("planBurst", () => {
  const chains = planBurst("abc", 3, { sessionsPerOrigin: 4, revisions: 4, retryCopies: 4, direct: true });
  test("is deterministic and shaped as documented", () => {
    expect(planBurst("abc", 3, { sessionsPerOrigin: 4, revisions: 4, retryCopies: 4, direct: true })).toEqual(chains);
    // user 0: 2 origins x 4 reset sessions + 2 after sessions; users 1, 2: 2 origins x 4 sessions.
    expect(chains.filter((c) => c.role === "reset")).toHaveLength(8);
    expect(chains.filter((c) => c.role === "after")).toHaveLength(2);
    expect(chains.filter((c) => c.role === "main")).toHaveLength(16);
    expect(planCounts(chains)).toEqual({ chains: 26, sessions: 26, unique_batches: 8 * 4 + 8 * 2 + 2 + 16 * 8, upload_requests: 8 * (2 * 4 + 2) + 8 * 4 + 2 * 2 + 16 * (4 * 4 + 4), origins: 6 });
  });
  test("each user uploads from two origins, origin b over the direct path when enabled", () => {
    for (const u of [0, 1, 2]) {
      const mine = chains.filter((c) => c.user === u);
      expect(new Set(mine.map((c) => c.originId)).size).toBe(2);
      expect(new Set(mine.map((c) => c.path))).toEqual(new Set(["http", "direct"]));
    }
    expect(new Set(planBurst("abc", 2, { sessionsPerOrigin: 1, revisions: 2, retryCopies: 1, direct: false }).map((c) => c.path))).toEqual(new Set(["http"]));
  });
  test("revisions rise by one per batch, every other one is a retry storm, and only reset sessions race and get blocked", () => {
    for (const c of chains) {
      expect(c.batches.map((b) => b.revision)).toEqual(c.batches.map((_, i) => i + 1));
      expect(c.batches.filter((b, i) => i % 2 === 0).every((b) => b.copies === 4 || c.role === "after")).toBe(true);
      expect(c.race !== undefined).toBe(c.role === "reset");
      if (c.blocked) expect(c.blocked.revision).toBe(BLOCKED_REVISION);
    }
    const labels = chains.flatMap((c) => [...c.batches, ...(c.race ? [c.race] : []), ...(c.blocked ? [c.blocked] : [])].map((b) => `${c.originId}/${b.label}`));
    expect(new Set(labels).size).toBe(labels.length);
    expect(new Set(chains.map((c) => c.sessionId)).size).toBe(chains.length);
    expect(chains.every((c) => /^[A-Za-z0-9_-][A-Za-z0-9._-]{0,127}$/.test(c.originId))).toBe(true);
  });
});

/** A clean run: one admitted batch per kind, every record and session where it should be, two pods working. */
function clean(): Observation {
  const b = (patch: Partial<BatchObservation> = {}): BatchObservation => ({ expect: "admitted", path: "http", terminal: "admitted", record: "committed", receiptMatches: true, ...patch });
  const tally = { lines: 10, admitted: 5, refused: 0, retry: 2, deferred: 1, deadLettered: 0, uploads: 5, promoted: 0, firstWrites: 2, replayFast: 0, codes: { ledger_batch_contended: 1, tenant_limit: 1 } };
  return {
    batches: [b(), b({ path: "direct" }), b({ expect: "either" }), b({ expect: "blocked" })],
    records: [
      { role: "main", min: 8, max: 8, absentOk: false, indexRevision: 8 },
      { role: "reset", min: 4, max: 5, absentOk: true, indexRevision: 5 },
      { role: "reset", min: 4, max: 4, absentOk: true, indexRevision: null },
    ],
    sessions: [{ role: "main", min: 8, max: 8, manifestRevision: 8, manifests: 1 }, { role: "reset", min: 4, max: 5, manifestRevision: 4, manifests: 1 }],
    storage: { inboxPending: 0, uploadInboxPending: 0, deadLetters: 0 },
    http: { statuses: { "200": 10, "429": 1 }, retryCodes: { delivery_busy: 1 }, unexpected: 0 },
    reset: { count: 8, replay: 8, resetSessions: 8, fenceResets: 1 },
    replicas: { by: "kubectl", ready: 2, env: { INGEST_LEDGER: "s3", INGEST_LEDGER_LOG_COMMIT: null } },
    logs: { pod_0: tally, pod_1: { ...tally, codes: {} } },
    quiesced: true,
  };
}

describe("evaluate", () => {
  test("a clean observation passes with counts only", () => {
    const v = evaluate(clean());
    expect(v.failures).toEqual({});
    expect(v.verdict).toBe("PASS");
    expect(v.counts.batches_by_path).toEqual({ http: 3, direct: 1 });
    expect(JSON.stringify(v)).not.toMatch(/user|token|org_|ws_/);
  });

  const failsWith = (mutate: (o: Observation) => void, code: string) => {
    const o = clean();
    mutate(o);
    const v = evaluate(o);
    expect(v.verdict).toBe("FAIL");
    expect(Object.keys(v.failures)).toContain(code);
  };

  test("a duplicate admission (manifest written once more than admitted batches) fails", () =>
    failsWith((o) => { o.sessions[0]!.manifestRevision = 9; }, "duplicate_admission_manifest_written_twice"));
  test("a reset-superseded write fails, in the manifest and in the index", () => {
    failsWith((o) => { o.sessions[1]!.manifestRevision = 6; }, "reset_superseded_write_landed");
    failsWith((o) => { o.records[1]!.indexRevision = BLOCKED_REVISION; }, "reset_superseded_revision_admitted");
  });
  test("an index that regressed, ran ahead, is missing or unreadable fails", () => {
    failsWith((o) => { o.records[0]!.indexRevision = 7; }, "revision_regressed_or_lost");
    failsWith((o) => { o.records[0]!.indexRevision = 9; }, "revision_ahead_of_any_admitted");
    failsWith((o) => { o.records[0]!.indexRevision = null; }, "revision_missing_from_index");
    failsWith((o) => { o.records[0]!.indexRevision = -1; }, "revision_index_unreadable");
  });
  test("a lost write, a missing or doubled manifest fails", () => {
    failsWith((o) => { o.sessions[0]!.manifestRevision = 7; }, "admitted_batch_not_written");
    failsWith((o) => { o.sessions[0]!.manifestRevision = null; }, "manifest_missing");
    failsWith((o) => { o.sessions[0]!.manifests = 2; }, "more_than_one_manifest_for_session");
  });
  test("batch records must be committed, carry the answered receipt, and copies must share one receipt", () => {
    failsWith((o) => { o.batches[0]!.record = "pending"; }, "batch_record_left_pending");
    failsWith((o) => { o.batches[0]!.record = "absent"; }, "admitted_without_batch_record");
    failsWith((o) => { o.batches[0]!.record = "invalid"; }, "batch_record_invalid");
    failsWith((o) => { o.batches[0]!.receiptMatches = false; }, "batch_record_receipt_mismatch");
    failsWith((o) => { o.batches[0]!.receiptsDiverged = true; }, "retry_copies_got_different_receipts");
  });
  test("refusals: only reset codes, only on racing or blocked batches", () => {
    failsWith((o) => { Object.assign(o.batches[0]!, { terminal: "refused", refusedCode: "delivery_privacy_superseded" }); }, "unexpected_refusal");
    failsWith((o) => { Object.assign(o.batches[2]!, { terminal: "refused", refusedCode: "admission_failed" }); }, "unexpected_refusal");
    const o = clean();
    Object.assign(o.batches[2]!, { terminal: "refused", refusedCode: "delivery_privacy_superseded", record: "absent" });
    expect(evaluate(o).verdict).toBe("PASS");
  });
  test("never terminal, unsent and not quiesced fail", () => {
    failsWith((o) => { o.batches[0]!.terminal = "pending"; }, "batch_never_terminal");
    failsWith((o) => { o.batches[0]!.terminal = "unsent"; }, "batch_not_sent");
    failsWith((o) => { o.quiesced = false; }, "not_quiesced_within_timeout");
  });
  test("leftover inbox, upload inbox or dead letters fail", () => {
    failsWith((o) => { o.storage.inboxPending = 1; }, "inbox_left_unprocessed");
    failsWith((o) => { o.storage.uploadInboxPending = 2; }, "upload_inbox_left_unpromoted");
    failsWith((o) => { o.storage.deadLetters = 1; }, "dead_lettered");
    failsWith((o) => { o.logs!.pod_0!.deadLettered = 1; }, "dead_lettered_in_logs");
  });
  test("the reset must run, replay its first count, cover every owned session and be in the fence", () => {
    failsWith((o) => { o.reset.count = null; }, "reset_not_run");
    failsWith((o) => { o.reset.replay = 7; }, "reset_replay_answered_differently");
    failsWith((o) => { o.reset.count = 3; o.reset.replay = 3; }, "reset_count_not_equal_run_sessions");
    // More than the run's sessions: the reset reached a session outside the run.
    failsWith((o) => { o.reset.count = 9; o.reset.replay = 9; }, "reset_count_not_equal_run_sessions");
    failsWith((o) => { o.reset.count = null; o.reset.replay = null; o.reset.refused = "foreign_sessions"; }, "reset_refused_foreign_sessions");
    failsWith((o) => { o.reset.fenceResets = 0; }, "reset_not_recorded_in_fence");
  });
  test("retry codes: contention codes pass, anything else fails unless allowed", () => {
    failsWith((o) => { o.http.retryCodes.ledger_fence_authority_missing = 1; }, "unexpected_retry_code");
    failsWith((o) => { o.logs!.pod_1!.codes.ledger_batch_record_invalid = 2; }, "unexpected_log_error_code");
    const o = clean();
    o.logs!.pod_1!.codes.ledger_store_error = 1;
    expect(evaluate(o, { allowRetryCodes: ["ledger_store_error"] }).verdict).toBe("PASS");
  });
  test("both replicas must be ready and have done work; the ledger must be s3 with log commit off", () => {
    failsWith((o) => { o.logs = { pod_0: o.logs!.pod_0! }; }, "fewer_than_two_replicas_did_work");
    failsWith((o) => { o.logs!.pod_1 = { ...o.logs!.pod_1!, admitted: 0, uploads: 0 }; }, "fewer_than_two_replicas_did_work");
    failsWith((o) => { o.replicas.ready = 1; }, "fewer_than_two_ready_replicas");
    failsWith((o) => { o.replicas.env!.INGEST_LEDGER = "sql"; }, "ingest_ledger_not_s3");
    failsWith((o) => { o.replicas.env!.INGEST_LEDGER_LOG_COMMIT = "on"; }, "ledger_log_commit_not_off");
  });
  test("local mode: each replica must have first-written batches, and the first writes across replicas equal the admitted batches", () => {
    const local = (patch: (o: Observation) => void) => { const o = clean(); o.replicas = { by: "local_compose", ready: 2 }; patch(o); return evaluate(o, { requireAdmissionsPerReplica: true }); };
    expect(local(() => {}).verdict).toBe("PASS");
    expect(local((o) => { o.logs!.pod_1 = { ...o.logs!.pod_1!, firstWrites: 0 }; o.logs!.pod_0 = { ...o.logs!.pod_0!, firstWrites: 4 }; }).failures).toEqual({ replica_admitted_nothing: 1 });
    expect(local((o) => { o.logs!.pod_1 = { ...o.logs!.pod_1!, firstWrites: 3 }; }).failures).toEqual({ more_first_writes_than_admitted_batches: 1 });
    expect(local((o) => { o.logs!.pod_1 = { ...o.logs!.pod_1!, firstWrites: 1 }; }).failures).toEqual({ admitted_batches_without_a_logged_commit: 1 });
    // A takeover: the other replica committed the batch as replay_fast after this one wrote it.
    expect(local((o) => { o.logs!.pod_1 = { ...o.logs!.pod_1!, firstWrites: 1, replayFast: 1 }; }).verdict).toBe("PASS");
    // A replica that only replayed (admitted, but never the first writer) did not prove a second writer.
    expect(local((o) => { o.logs!.pod_1 = { ...o.logs!.pod_1!, firstWrites: 0 }; o.logs!.pod_0 = { ...o.logs!.pod_0!, firstWrites: 4 }; }).verdict).toBe("FAIL");
    // The reset refusal is an expected log code too.
    expect(local((o) => { o.logs!.pod_0!.codes.delivery_privacy_superseded = 2; }).verdict).toBe("PASS");
  });
  test("with the operator flag instead of kubectl, no replica or log check runs, and the verdict is PASS_UNATTRIBUTED", () => {
    const o = clean();
    o.replicas = { by: "operator_flag", ready: null };
    delete o.logs;
    expect(evaluate(o).verdict).toBe("PASS_UNATTRIBUTED");
    o.storage.deadLetters = 1;
    expect(evaluate(o).verdict).toBe("FAIL");
  });
});

describe("kubectl parsing", () => {
  test("parseDeployment reads ready replicas, the selector and the watched env", () => {
    const d = parseDeployment(JSON.stringify({
      spec: { replicas: 2, selector: { matchLabels: { "app.kubernetes.io/component": "ingest-edge", "app.kubernetes.io/instance": "sp" } },
        template: { spec: { containers: [{ env: [{ name: "INGEST_LEDGER", value: "s3" }, { name: "INGEST_MAX_PER_USER", value: "2" }, { name: "LOCKER_SECRET_ACCESS_KEY", valueFrom: {} }, { name: "SELFHOST_INGEST_ADMISSION", valueFrom: {} }] }] } } },
      status: { readyReplicas: 2 },
    }));
    expect(d).toEqual({ ready: 2, desired: 2, selector: "app.kubernetes.io/component=ingest-edge,app.kubernetes.io/instance=sp", env: { INGEST_LEDGER: "s3", INGEST_MAX_PER_USER: "2", SELFHOST_INGEST_ADMISSION: null } });
    expect(parseDeployment("not json")).toBeNull();
    expect(parseDeployment(JSON.stringify({ spec: {} }))).toBeNull();
  });
  test("tallyLog counts admission outcomes and non-admitted codes, and skips other lines", () => {
    const line = (o: object) => `2026-11-16T00:00:00Z ${JSON.stringify(o)}`;
    const t = tallyLog([
      line({ route: "ingest.admission", outcome: "admitted", error_code: "accepted" }),
      line({ route: "ingest.admission", outcome: "admitted", error_code: "replay" }),
      line({ route: "ingest.admission", outcome: "admitted", error_code: "replay_fast" }),
      line({ route: "ingest.upload_admission", outcome: "ok", promoted: 3 }),
      line({ route: "ingest.admission", outcome: "retry", error_code: "ledger_batch_contended" }),
      line({ route: "ingest.admission", outcome: "deferred", error_code: "tenant_limit" }),
      line({ route: "ingest.admission", outcome: "dead_lettered", error_code: "admission_failed" }),
      line({ route: "delivery.batches", outcome: "accepted_queued" }),
      "plain text", "{ not json",
    ].join("\n"));
    expect(t).toEqual({ lines: 8, admitted: 3, refused: 0, retry: 1, deferred: 1, deadLettered: 1, uploads: 1, promoted: 3, firstWrites: 1, replayFast: 1, codes: { ledger_batch_contended: 1, tenant_limit: 1, admission_failed: 1 } });
  });
});

describe("resetOwnershipCheck (what the user-wide reset would tombstone)", () => {
  const owner = (originId: string, sessionId: string, epoch = 0): OwnerDoc => ({ v: 1, originId, source: "claude", sessionId, epoch, resets: 0 });
  // A minimal fence: only the fields fenceEpoch / isDeleted read.
  const fence = (patch: Partial<FenceDoc> = {}): FenceDoc => ({ v: 1, instance: "i", version: 1, resets: 0, resetsCompleted: 0, lastResetAtMs: 0, deleted: {}, epochs: {}, resetRequests: {}, ...patch } as unknown as FenceDoc);
  const run = new Set(["o-a\ns1", "o-b\ns2"]);
  test("exactly the run's sessions: no foreign, none missing", () => {
    expect(resetOwnershipCheck([owner("o-a", "s1"), owner("o-b", "s2")], fence(), run)).toEqual({ live: 2, foreign: 0, missing: 0 });
    expect(resetOwnershipCheck([owner("o-a", "s1"), owner("o-b", "s2")], null, run)).toEqual({ live: 2, foreign: 0, missing: 0 });
  });
  test("a live session outside the run is foreign (the reset is refused)", () => {
    expect(resetOwnershipCheck([owner("o-a", "s1"), owner("o-b", "s2"), owner("o-x", "other")], fence(), run).foreign).toBe(1);
    // Before the burst: the reset user must own nothing live.
    expect(resetOwnershipCheck([owner("o-x", "other")], null, new Set()).foreign).toBe(1);
  });
  test("an already deleted or re-epoched session is not live, so not foreign", () => {
    const f = fence({ deleted: { "o-x": [["claude", "other"]] } } as Partial<FenceDoc>);
    expect(resetOwnershipCheck([owner("o-a", "s1"), owner("o-b", "s2"), owner("o-x", "other")], f, run)).toEqual({ live: 2, foreign: 0, missing: 0 });
    expect(resetOwnershipCheck([owner("o-a", "s1"), owner("o-b", "s2"), owner("o-y", "old", 0)], fence({ epochs: { "o-y": { [sessionKey("claude", "old")]: 1 } } } as Partial<FenceDoc>), run).foreign).toBe(0);
  });
  test("a run session without a stub is missing", () => {
    expect(resetOwnershipCheck([owner("o-a", "s1")], fence(), run)).toEqual({ live: 1, foreign: 0, missing: 1 });
  });
});

describe("ownershipCheck over a store (read-only, before the reset)", () => {
  const ROOT = "96bulls";
  const tenant = { orgId: "org_proof", workspaceId: "ws_proof", userId: "user_zero" };
  test("refuses (foreign) when the reset user owns a session outside the run; counts the run's own; reads nothing it writes", async () => {
    const { store, done } = scratchStore();
    try {
      const io = s3LedgerObjects(store, ROOT);
      const keys = s3LedgerKeys(ROOT, tenant);
      const stub = (originId: string, sessionId: string): OwnerDoc => ({ v: 1, originId, source: "claude", sessionId, epoch: 0, resets: 0 });
      expect(await ownershipCheck(store, ROOT, tenant, new Set())).toEqual({ live: 0, foreign: 0, missing: 0 });
      await io.registerOwner(keys.owner("o-a", "claude", "s1"), stub("o-a", "s1"));
      expect(await ownershipCheck(store, ROOT, tenant, new Set(["o-a\ns1"]))).toEqual({ live: 1, foreign: 0, missing: 0 });
      await io.registerOwner(keys.owner("o-x", "claude", "foreign"), stub("o-x", "foreign"));
      expect((await ownershipCheck(store, ROOT, tenant, new Set(["o-a\ns1"])))!.foreign).toBe(1);
      expect((await ownershipCheck(store, ROOT, tenant, new Set()))!.foreign).toBe(2);
      // After a reset tombstoned them, nothing is live: a rerun with the same synthetic user is allowed.
      await s3Ledger(store, ROOT, { acceptanceMode: "canonical" }).resetUploadedSessions(tenant, "r1");
      expect(await ownershipCheck(store, ROOT, tenant, new Set())).toEqual({ live: 0, foreign: 0, missing: 0 });
    } finally { done(); }
  });
});
