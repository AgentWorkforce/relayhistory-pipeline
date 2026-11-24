import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { sessionIngestVars } from "../session-ingest.ts";
import {
  PRIVATE_INGEST_CUTOVER_KEYS,
  privateIngestCutoverFromEnv,
  privateIngestProfileForPhase,
  privateIngestReleasePhase,
  privateIngestTransitionReadback,
  privateIngestVersionReadback,
  type CloudflareWorkerVersion,
} from "../../../src/session-ingest/private-ingest-release.ts";
import type { CloudflareStageInputs } from "../alchemy.cloudflare.run.ts";

// Decision 0005 cutover controls on the guarded private-ingest deploy: s3FenceMirror and httpAdmissionStoppedOrgs ride
// on the live rollout phase, come only from the protected environment, and never select or move a phase.
const kmsKeyArn = "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000";
const ORG_A = "00000000-0000-4000-8000-00000000000a", ORG_B = "00000000-0000-4000-8000-00000000000b";
const code = (fn: () => unknown) => {
  try { fn(); return null; } catch (error) { return (error as { code?: string }).code ?? String(error); }
};
const at = (phase: Parameters<typeof privateIngestProfileForPhase>[0], extra: Record<string, unknown> = {}): CloudflareStageInputs => {
  const base = privateIngestProfileForPhase(phase, kmsKeyArn);
  return { ...base, sessionIngest: { ...base.sessionIngest, ...extra } } as CloudflareStageInputs;
};

function live(input: CloudflareStageInputs, extraBindings: Array<Record<string, unknown>> = []) {
  const vars = sessionIngestVars(input.sessionIngest);
  const bindings: any[] = Object.entries(vars).map(([name, text]) => ({ name, type: "plain_text", text }));
  bindings.push(
    { name: "HYPERDRIVE", type: "hyperdrive", id: input.sessionIngest.hyperdriveId },
    { name: "INGEST_FANOUT", type: "queue", queue_name: "96bulls-session-ingest-fanout" },
    { name: "INGEST_ADMISSION", type: "queue", queue_name: "96bulls-session-ingest-admission" },
    { name: "S3_ACCESS_KEY_ID", type: "secrets_store_secret", store_id: input.sessionIngest.secretsStoreId, secret_name: "96bulls-session-ingest-s3-access-key-id" },
    { name: "S3_SECRET_ACCESS_KEY", type: "secrets_store_secret", store_id: input.sessionIngest.secretsStoreId, secret_name: "96bulls-session-ingest-s3-secret-access-key" },
    { name: "INGEST_SIGNING_KEY", type: "secrets_store_secret", store_id: input.sessionIngest.secretsStoreId, secret_name: "96bulls-session-ingest-signing-key" },
    ...extraBindings,
  );
  const active = { deploymentId: "1".repeat(8) + "-1111-1111-1111-111111111111", versionId: "2".repeat(8) + "-2222-2222-2222-222222222222", createdOn: null };
  const version: CloudflareWorkerVersion = {
    id: active.versionId,
    resources: { bindings, script_runtime: { compatibility_date: "2026-10-01", compatibility_flags: ["nodejs_compat", "new_module_registry"] } },
  };
  return { active, version };
}

test("the tracked public config never carries a cutover control (the org list names production tenants)", () => {
  const tracked = JSON.parse(readFileSync("config/cloudflare/private-ingest-production.json", "utf8"));
  for (const key of PRIVATE_INGEST_CUTOVER_KEYS) expect(Object.hasOwn(tracked.sessionIngest, key)).toBe(false);
  // The CI adapter refuses a tracked cutover key rather than merging it.
  expect(readFileSync("scripts/private-ingest-ci.ts", "utf8")).toContain('privateIngestRefuse("private_ingest_tracked_cutover")');
});

test("protected-environment variables: unset or empty is off; only exact shapes are accepted", () => {
  expect(privateIngestCutoverFromEnv({})).toEqual({});
  expect(privateIngestCutoverFromEnv({ PRIVATE_INGEST_S3_FENCE_MIRROR: "", PRIVATE_INGEST_HTTP_ADMISSION_STOPPED_ORGS: "" })).toEqual({});
  expect(privateIngestCutoverFromEnv({ PRIVATE_INGEST_S3_FENCE_MIRROR: "on" })).toEqual({ s3FenceMirror: "on" });
  expect(privateIngestCutoverFromEnv({ PRIVATE_INGEST_HTTP_ADMISSION_STOPPED_ORGS: `${ORG_A},${ORG_B}` }))
    .toEqual({ httpAdmissionStoppedOrgs: [ORG_A, ORG_B] });
  for (const mirror of ["off", "ON", "true", "1", " on"])
    expect(code(() => privateIngestCutoverFromEnv({ PRIVATE_INGEST_S3_FENCE_MIRROR: mirror }))).toBe("private_ingest_profile_fence_mirror");
  for (const orgs of [`${ORG_A},`, `,${ORG_A}`, `${ORG_A},,${ORG_B}`, ` ${ORG_A}`, `${ORG_A}, ${ORG_B}`, `${ORG_A},${ORG_A}`,
    ORG_A.toUpperCase(), "org-a", ORG_A.slice(1), `${ORG_A};${ORG_B}`])
    expect(code(() => privateIngestCutoverFromEnv({ PRIVATE_INGEST_HTTP_ADMISSION_STOPPED_ORGS: orgs }))).toBe("private_ingest_profile_stopped_orgs");
});

test("the contract admits the cutover keys on every rollout phase without changing the phase; bad shapes refuse distinctly", () => {
  for (const phase of ["baseline-drain", "step-0-shadow", "step-1-custody", "step-2-frozen-receipt", "step-4-async-on"] as const) {
    expect(privateIngestReleasePhase(at(phase, { s3FenceMirror: "on" }))).toBe(phase);
    expect(privateIngestReleasePhase(at(phase, { httpAdmissionStoppedOrgs: [ORG_A] }))).toBe(phase);
    expect(privateIngestReleasePhase(at(phase, { s3FenceMirror: "on", httpAdmissionStoppedOrgs: [ORG_A, ORG_B] }))).toBe(phase);
  }
  for (const mirror of ["off", "", true, null, ["on"]])
    expect(code(() => privateIngestReleasePhase(at("baseline-drain", { s3FenceMirror: mirror })))).toBe("private_ingest_profile_fence_mirror");
  for (const orgs of [[], [ORG_A, ORG_A], ["org-a"], [ORG_A.toUpperCase()], ORG_A, [7], null, [ORG_A, ""]])
    expect(code(() => privateIngestReleasePhase(at("baseline-drain", { httpAdmissionStoppedOrgs: orgs })))).toBe("private_ingest_profile_stopped_orgs");
  // Still exactly these two: any other new key refuses.
  expect(code(() => privateIngestReleasePhase(at("baseline-drain", { httpAdmissionStopped: [ORG_A] })))).toBe("private_ingest_profile_extra");
  expect(code(() => privateIngestReleasePhase(at("baseline-drain", { uploadConsent: "enforce" })))).toBe("private_ingest_profile_extra");
});

test("transition: the controls are added, changed and removed at a held phase; phase moves stay governed", () => {
  const plain = at("baseline-drain");
  const on = at("baseline-drain", { s3FenceMirror: "on", httpAdmissionStoppedOrgs: [ORG_A] });
  const wider = at("baseline-drain", { s3FenceMirror: "on", httpAdmissionStoppedOrgs: [ORG_A, ORG_B] });
  const pass = { status: "PRIVATE_INGEST_LIVE_TRANSITION_PASS", from: "baseline-drain", to: "baseline-drain" };
  const fromPlain = live(plain), fromOn = live(on);
  expect(privateIngestTransitionReadback(on, fromPlain.active, fromPlain.version)).toMatchObject(pass);     // cutover
  expect(privateIngestTransitionReadback(wider, fromOn.active, fromOn.version)).toMatchObject(pass);       // widen
  expect(privateIngestTransitionReadback(plain, fromOn.active, fromOn.version)).toMatchObject(pass);      // rollback
  expect(privateIngestTransitionReadback(on, fromOn.active, fromOn.version)).toMatchObject(pass);         // redeploy
  // The adjacent phase move is still read through the live controls, and a skip is still refused.
  expect(privateIngestTransitionReadback(at("step-0-shadow", { s3FenceMirror: "on" }), fromOn.active, fromOn.version))
    .toMatchObject({ from: "baseline-drain", to: "step-0-shadow" });
  expect(code(() => privateIngestTransitionReadback(at("step-1-custody", { s3FenceMirror: "on" }), fromOn.active, fromOn.version)))
    .toBe("private_ingest_live_transition");
});

test("readback is exact for the controls; a malformed live control refuses instead of being guessed through", () => {
  const on = at("baseline-drain", { s3FenceMirror: "on", httpAdmissionStoppedOrgs: [ORG_A] });
  const fromOn = live(on), fromPlain = live(at("baseline-drain"));
  expect(privateIngestVersionReadback(on, fromOn.active, fromOn.version)).toMatchObject({ phase: "baseline-drain" });
  // Target says stopped, live is not (and the reverse): never a pass.
  expect(code(() => privateIngestVersionReadback(on, fromPlain.active, fromPlain.version))).not.toBeNull();
  expect(code(() => privateIngestVersionReadback(at("baseline-drain"), fromOn.active, fromOn.version))).toBe("private_ingest_live_unknown_flag");
  const other = at("baseline-drain", { s3FenceMirror: "on", httpAdmissionStoppedOrgs: [ORG_B] });
  expect(code(() => privateIngestVersionReadback(other, fromOn.active, fromOn.version))).toBe("private_ingest_live_binding_target");
  for (const bad of [
    [{ name: "INGEST_S3_FENCE_MIRROR", type: "plain_text", text: "off" }],
    [{ name: "INGEST_HTTP_ADMISSION_STOPPED_ORGS", type: "plain_text", text: `${ORG_A},` }],
    [{ name: "INGEST_HTTP_ADMISSION_STOPPED_ORGS", type: "secret_text" }],
    [{ name: "INGEST_S3_FENCE_MIRROR", type: "plain_text", text: "on" }, { name: "INGEST_S3_FENCE_MIRROR", type: "plain_text", text: "on" }],
  ]) {
    const fixture = live(at("baseline-drain"), bad);
    expect(code(() => privateIngestTransitionReadback(at("baseline-drain"), fixture.active, fixture.version))).toBe("private_ingest_live_cutover");
  }
});

test("the retained sanitized inputs artifact reduces the stop list to a count", async () => {
  const { redactStoppedOrgs } = await import("../../../scripts/private-ingest-operator.ts");
  const redacted = redactStoppedOrgs({ s3FenceMirror: "on", httpAdmissionStoppedOrgs: [ORG_A, ORG_B] });
  expect(JSON.stringify(redacted)).not.toContain(ORG_A);
  expect(redacted).toEqual({ s3FenceMirror: "on", httpAdmissionStoppedOrgs: "redacted: 2 org(s) from the protected environment" });
  expect(redactStoppedOrgs({ asyncAdmission: "drain" })).toEqual({ asyncAdmission: "drain" });
});

// 2026-11-16 customer incident: the claim lock controls are cutover controls, so the release guard (the only thing
// this test covers) lets a deploy add, change or remove them at the live phase. (As COMMON_PROFILE keys they could
// never match the live version they replace.) The operator is a separate gate: it has no unbind for these vars and
// refuses a plan that removes one with native_binding_target (scripts/private-ingest-operator.ts).
test("claim lock controls: env shapes, and add / change / remove at a held phase", () => {
  expect(privateIngestCutoverFromEnv({ PRIVATE_INGEST_CLAIM_TIMEOUT_BUSY: "on", PRIVATE_INGEST_CLAIM_LOCK_WAIT_MS: "250" }))
    .toEqual({ claimTimeoutBusy: "on", claimLockWaitMs: 250 });
  expect(privateIngestCutoverFromEnv({ PRIVATE_INGEST_CLAIM_TIMEOUT_BUSY: "", PRIVATE_INGEST_CLAIM_LOCK_WAIT_MS: "" })).toEqual({});
  for (const v of ["off", "ON", "true", " on"])
    expect(code(() => privateIngestCutoverFromEnv({ PRIVATE_INGEST_CLAIM_TIMEOUT_BUSY: v }))).toBe("private_ingest_profile_claim_timeout_busy");
  for (const v of ["1e3", "0x100", "+250", "-250", "250.5", " 250", "24", "2001", "5", "abc"])
    expect(code(() => privateIngestCutoverFromEnv({ PRIVATE_INGEST_CLAIM_LOCK_WAIT_MS: v }))).toBe("private_ingest_profile_claim_lock_wait");
  for (const phase of ["baseline-drain", "step-4-async-on"] as const)
    expect(privateIngestReleasePhase(at(phase, { claimTimeoutBusy: "on", claimLockWaitMs: 250 }))).toBe(phase);
  expect(code(() => privateIngestReleasePhase(at("baseline-drain", { claimTimeoutBusy: "off" })))).toBe("private_ingest_profile_claim_timeout_busy");
  expect(code(() => privateIngestReleasePhase(at("baseline-drain", { claimLockWaitMs: 2001 })))).toBe("private_ingest_profile_claim_lock_wait");

  const plain = at("baseline-drain", { httpAdmissionStoppedOrgs: [ORG_A] });
  const claim = at("baseline-drain", { httpAdmissionStoppedOrgs: [ORG_A], claimTimeoutBusy: "on", claimLockWaitMs: 250 });
  const slower = at("baseline-drain", { httpAdmissionStoppedOrgs: [ORG_A], claimTimeoutBusy: "on", claimLockWaitMs: 500 });
  const pass = { status: "PRIVATE_INGEST_LIVE_TRANSITION_PASS", from: "baseline-drain", to: "baseline-drain" };
  const fromPlain = live(plain), fromClaim = live(claim);
  expect(privateIngestTransitionReadback(claim, fromPlain.active, fromPlain.version)).toMatchObject(pass);   // add (today's deploy)
  expect(privateIngestTransitionReadback(slower, fromClaim.active, fromClaim.version)).toMatchObject(pass);  // change
  expect(privateIngestTransitionReadback(plain, fromClaim.active, fromClaim.version)).toMatchObject(pass);   // remove (rollback)
  expect(privateIngestVersionReadback(claim, fromClaim.active, fromClaim.version)).toMatchObject({ phase: "baseline-drain" });
  expect(code(() => privateIngestVersionReadback(slower, fromClaim.active, fromClaim.version))).not.toBeNull();
  for (const bad of [
    [{ name: "INGEST_CLAIM_TIMEOUT_BUSY", type: "plain_text", text: "off" }],
    [{ name: "INGEST_CLAIM_LOCK_WAIT_MS", type: "plain_text", text: "1e3" }],
    [{ name: "INGEST_CLAIM_LOCK_WAIT_MS", type: "plain_text", text: "9" }],
    [{ name: "INGEST_CLAIM_LOCK_WAIT_MS", type: "secret_text" }],
  ]) {
    const fixture = live(plain, bad);
    expect(code(() => privateIngestTransitionReadback(plain, fixture.active, fixture.version))).toBe("private_ingest_live_cutover");
  }
});

// Buffer-stage lock controls (deliver.ts BufferLockBusy / bufferLedgerStage): cutover controls exactly like
// claimTimeoutBusy. Absent renders nothing; "on" is accepted and rendered; anything else refuses.
test("buffer lock controls: absent renders nothing, on renders, anything else refuses, add / remove at a held phase", () => {
  const keys = { bufferLockBusy: "INGEST_BUFFER_LOCK_BUSY", bufferLockRetry: "INGEST_BUFFER_LOCK_RETRY" } as const;
  const envName = { bufferLockBusy: "PRIVATE_INGEST_BUFFER_LOCK_BUSY", bufferLockRetry: "PRIVATE_INGEST_BUFFER_LOCK_RETRY" } as const;
  const refusal = { bufferLockBusy: "private_ingest_profile_buffer_lock_busy", bufferLockRetry: "private_ingest_profile_buffer_lock_retry" } as const;
  for (const key of ["bufferLockBusy", "bufferLockRetry"] as const) expect(PRIVATE_INGEST_CUTOVER_KEYS).toContain(key);

  // Absent: no var rendered and the profile check passes, at every phase.
  expect(privateIngestCutoverFromEnv({ PRIVATE_INGEST_BUFFER_LOCK_BUSY: "", PRIVATE_INGEST_BUFFER_LOCK_RETRY: "" })).toEqual({});
  for (const phase of ["baseline-drain", "step-0-shadow", "step-1-custody", "step-2-frozen-receipt", "step-4-async-on"] as const) {
    const absent = at(phase);
    expect(privateIngestReleasePhase(absent)).toBe(phase);
    for (const name of Object.values(keys)) expect(Object.hasOwn(sessionIngestVars(absent.sessionIngest), name)).toBe(false);
  }

  for (const key of ["bufferLockBusy", "bufferLockRetry"] as const) {
    // "on": accepted and rendered.
    expect(privateIngestCutoverFromEnv({ [envName[key]]: "on" })).toEqual({ [key]: "on" });
    for (const phase of ["baseline-drain", "step-4-async-on"] as const) {
      const on = at(phase, { [key]: "on" });
      expect(privateIngestReleasePhase(on)).toBe(phase);
      expect(sessionIngestVars(on.sessionIngest)).toEqual({ ...sessionIngestVars(at(phase).sessionIngest), [keys[key]]: "on" });
    }
    // Any other value refuses, from the environment, the profile and the live version.
    for (const v of ["off", "ON", "true", "1", " on"])
      expect(code(() => privateIngestCutoverFromEnv({ [envName[key]]: v }))).toBe(refusal[key]);
    for (const v of ["off", "ON", true, 1, ""])
      expect(code(() => privateIngestReleasePhase(at("baseline-drain", { [key]: v })))).toBe(refusal[key]);
    const plain = at("baseline-drain"), on = at("baseline-drain", { [key]: "on" });
    const pass = { status: "PRIVATE_INGEST_LIVE_TRANSITION_PASS", from: "baseline-drain", to: "baseline-drain" };
    const fromPlain = live(plain), fromOn = live(on);
    expect(privateIngestTransitionReadback(on, fromPlain.active, fromPlain.version)).toMatchObject(pass);  // add
    expect(privateIngestTransitionReadback(plain, fromOn.active, fromOn.version)).toMatchObject(pass);     // remove (rollback)
    expect(privateIngestVersionReadback(on, fromOn.active, fromOn.version)).toMatchObject({ phase: "baseline-drain" });
    expect(code(() => privateIngestVersionReadback(plain, fromOn.active, fromOn.version))).not.toBeNull();
    for (const bad of [[{ name: keys[key], type: "plain_text", text: "off" }], [{ name: keys[key], type: "secret_text" }]]) {
      const fixture = live(plain, bad);
      expect(code(() => privateIngestTransitionReadback(plain, fixture.active, fixture.version))).toBe("private_ingest_live_cutover");
    }
  }
  // The tracked production profile with the variables unset renders neither var, and the workflow supplies them only
  // as protected-environment variables (like PRIVATE_INGEST_CLAIM_TIMEOUT_BUSY), never as a dispatch input.
  const tracked = JSON.parse(readFileSync("config/cloudflare/private-ingest-production.json", "utf8"));
  const production = { ...tracked, sessionIngest: { ...tracked.sessionIngest, kmsKeyArn, ...privateIngestCutoverFromEnv({}) } };
  // The tracked profile is at step (1) of the planned 062220 release (G+H overlap at the baseline-drain phase).
  expect(privateIngestReleasePhase(production)).toBe("baseline-drain-overlap");
  for (const name of Object.values(keys)) expect(Object.hasOwn(sessionIngestVars(production.sessionIngest), name)).toBe(false);
  const workflow = readFileSync(".github/workflows/deploy-private-ingest-worker.yml", "utf8");
  for (const name of Object.values(envName)) {
    expect(workflow).toContain(`${name}: \${{ vars.${name} }}`);
    expect(workflow.split("\npermissions:")[0]).not.toContain(name);
  }
  // Independent: both together, alongside the claim controls.
  const both = at("baseline-drain", { claimTimeoutBusy: "on", claimLockWaitMs: 250, bufferLockBusy: "on", bufferLockRetry: "on" });
  expect(privateIngestReleasePhase(both)).toBe("baseline-drain");
  expect(privateIngestCutoverFromEnv({ PRIVATE_INGEST_BUFFER_LOCK_BUSY: "on", PRIVATE_INGEST_BUFFER_LOCK_RETRY: "on" }))
    .toEqual({ bufferLockBusy: "on", bufferLockRetry: "on" });
  const fromBoth = live(both);
  expect(privateIngestVersionReadback(both, fromBoth.active, fromBoth.version)).toMatchObject({ phase: "baseline-drain" });
});

// Per-isolate in-flight lease (inflight-lease.ts): a cutover control like the buffer lock controls, two values.
test("inflight lease control: absent renders nothing, observe / on render, anything else refuses, add / remove at a held phase", () => {
  const name = "INGEST_INFLIGHT_LEASE", envName = "PRIVATE_INGEST_INFLIGHT_LEASE", refusal = "private_ingest_profile_inflight_lease";
  expect(PRIVATE_INGEST_CUTOVER_KEYS).toContain("inflightLease");
  expect(privateIngestCutoverFromEnv({ [envName]: "" })).toEqual({});
  expect(privateIngestCutoverFromEnv({})).toEqual({});
  for (const phase of ["baseline-drain", "step-0-shadow", "step-1-custody", "step-2-frozen-receipt", "step-4-async-on"] as const) {
    const absent = at(phase);
    expect(privateIngestReleasePhase(absent)).toBe(phase);
    expect(Object.hasOwn(sessionIngestVars(absent.sessionIngest), name)).toBe(false);
  }
  for (const mode of ["observe", "on"] as const) {
    expect(privateIngestCutoverFromEnv({ [envName]: mode })).toEqual({ inflightLease: mode });
    for (const phase of ["baseline-drain", "step-4-async-on"] as const) {
      const on = at(phase, { inflightLease: mode });
      expect(privateIngestReleasePhase(on)).toBe(phase);
      expect(sessionIngestVars(on.sessionIngest)).toEqual({ ...sessionIngestVars(at(phase).sessionIngest), [name]: mode });
    }
    const plain = at("baseline-drain"), on = at("baseline-drain", { inflightLease: mode });
    const pass = { status: "PRIVATE_INGEST_LIVE_TRANSITION_PASS", from: "baseline-drain", to: "baseline-drain" };
    const fromPlain = live(plain), fromOn = live(on);
    expect(privateIngestTransitionReadback(on, fromPlain.active, fromPlain.version)).toMatchObject(pass);  // add
    expect(privateIngestTransitionReadback(plain, fromOn.active, fromOn.version)).toMatchObject(pass);     // remove (rollback)
    expect(privateIngestVersionReadback(on, fromOn.active, fromOn.version)).toMatchObject({ phase: "baseline-drain" });
    expect(code(() => privateIngestVersionReadback(plain, fromOn.active, fromOn.version))).not.toBeNull();
  }
  const observe = at("baseline-drain", { inflightLease: "observe" }), fromOn = live(at("baseline-drain", { inflightLease: "on" }));
  expect(code(() => privateIngestVersionReadback(observe, fromOn.active, fromOn.version))).not.toBeNull();
  for (const v of ["off", "ON", "Observe", "true", "1", " on", "enforce"])
    expect(code(() => privateIngestCutoverFromEnv({ [envName]: v }))).toBe(refusal);
  for (const v of ["off", "ON", true, 1, "", "enforce"])
    expect(code(() => privateIngestReleasePhase(at("baseline-drain", { inflightLease: v })))).toBe(refusal);
  const plain = at("baseline-drain");
  for (const bad of [[{ name, type: "plain_text", text: "off" }], [{ name, type: "plain_text", text: "enforce" }], [{ name, type: "secret_text" }]]) {
    const fixture = live(plain, bad);
    expect(code(() => privateIngestTransitionReadback(plain, fixture.active, fixture.version))).toBe("private_ingest_live_cutover");
  }
  const tracked = JSON.parse(readFileSync("config/cloudflare/private-ingest-production.json", "utf8"));
  const production = { ...tracked, sessionIngest: { ...tracked.sessionIngest, kmsKeyArn, ...privateIngestCutoverFromEnv({}) } };
  // The tracked profile is at step (1) of the planned 062220 release (G+H overlap at the baseline-drain phase).
  expect(privateIngestReleasePhase(production)).toBe("baseline-drain-overlap");
  expect(Object.hasOwn(sessionIngestVars(production.sessionIngest), name)).toBe(false);
  const workflow = readFileSync(".github/workflows/deploy-private-ingest-worker.yml", "utf8");
  expect(workflow).toContain(`${envName}: \${{ vars.${envName} }}`);
  expect(workflow.split("\npermissions:")[0]).not.toContain(envName);
});

// Advertised records-per-batch limit (advertised-limits.ts): a cutover control; acceptance stays 500 either way.
test("advertised max records control: env shape, profile check, and add / change / remove at a held phase", () => {
  expect(PRIVATE_INGEST_CUTOVER_KEYS).toContain("advertisedMaxRecords");
  expect(privateIngestCutoverFromEnv({ PRIVATE_INGEST_ADVERTISED_MAX_RECORDS: "100" })).toEqual({ advertisedMaxRecords: 100 });
  expect(privateIngestCutoverFromEnv({ PRIVATE_INGEST_ADVERTISED_MAX_RECORDS: "" })).toEqual({});
  for (const v of ["0", "501", "0100", "+100", "-1", " 100", "100 ", "1e2", "100.0", "abc", "1000"])
    expect(code(() => privateIngestCutoverFromEnv({ PRIVATE_INGEST_ADVERTISED_MAX_RECORDS: v }))).toBe("private_ingest_profile_advertised_max_records");
  for (const phase of ["baseline-drain", "step-4-async-on"] as const)
    expect(privateIngestReleasePhase(at(phase, { advertisedMaxRecords: 100 }))).toBe(phase);
  for (const bad of [0, 501, 12.5, "100"])
    expect(code(() => privateIngestReleasePhase(at("baseline-drain", { advertisedMaxRecords: bad } as never)))).toBe("private_ingest_profile_advertised_max_records");
  const plain = at("baseline-drain", { claimTimeoutBusy: "on", claimLockWaitMs: 250 });
  const lower = at("baseline-drain", { claimTimeoutBusy: "on", claimLockWaitMs: 250, advertisedMaxRecords: 100 });
  const lowest = at("baseline-drain", { claimTimeoutBusy: "on", claimLockWaitMs: 250, advertisedMaxRecords: 50 });
  const pass = { status: "PRIVATE_INGEST_LIVE_TRANSITION_PASS", from: "baseline-drain", to: "baseline-drain" };
  const fromPlain = live(plain), fromLower = live(lower);
  expect(privateIngestTransitionReadback(lower, fromPlain.active, fromPlain.version)).toMatchObject(pass);  // add
  expect(privateIngestTransitionReadback(lowest, fromLower.active, fromLower.version)).toMatchObject(pass); // change
  expect(privateIngestTransitionReadback(plain, fromLower.active, fromLower.version)).toMatchObject(pass);  // remove (rollback)
  expect(privateIngestVersionReadback(lower, fromLower.active, fromLower.version)).toMatchObject({ phase: "baseline-drain" });
  expect(code(() => privateIngestVersionReadback(lowest, fromLower.active, fromLower.version))).not.toBeNull();
  for (const bad of [
    [{ name: "INGEST_ADVERTISED_MAX_RECORDS", type: "plain_text", text: "0" }],
    [{ name: "INGEST_ADVERTISED_MAX_RECORDS", type: "plain_text", text: "501" }],
    [{ name: "INGEST_ADVERTISED_MAX_RECORDS", type: "plain_text", text: "1e2" }],
    [{ name: "INGEST_ADVERTISED_MAX_RECORDS", type: "secret_text" }],
  ]) {
    const fixture = live(plain, bad);
    expect(code(() => privateIngestTransitionReadback(plain, fixture.active, fixture.version))).toBe("private_ingest_live_cutover");
  }
  const workflow = readFileSync(".github/workflows/deploy-private-ingest-worker.yml", "utf8");
  expect(workflow).toContain("PRIVATE_INGEST_ADVERTISED_MAX_RECORDS: ${{ vars.PRIVATE_INGEST_ADVERTISED_MAX_RECORDS }}");
  expect(workflow.split("\npermissions:")[0]).not.toContain("PRIVATE_INGEST_ADVERTISED_MAX_RECORDS");
});

// Per-origin claim fence (migration 20261105062010): a cutover control like the claim lock controls.
test("origin claim fence control: env shape, and add / remove at a held phase", () => {
  expect(privateIngestCutoverFromEnv({ PRIVATE_INGEST_ORIGIN_CLAIM_FENCE: "on" })).toEqual({ originClaimFence: "on" });
  expect(privateIngestCutoverFromEnv({ PRIVATE_INGEST_ORIGIN_CLAIM_FENCE: "" })).toEqual({});
  for (const v of ["off", "ON", "true", " on", "1"])
    expect(code(() => privateIngestCutoverFromEnv({ PRIVATE_INGEST_ORIGIN_CLAIM_FENCE: v }))).toBe("private_ingest_profile_origin_claim_fence");
  for (const phase of ["baseline-drain", "step-4-async-on"] as const)
    expect(privateIngestReleasePhase(at(phase, { originClaimFence: "on" }))).toBe(phase);
  expect(code(() => privateIngestReleasePhase(at("baseline-drain", { originClaimFence: "off" } as never)))).toBe("private_ingest_profile_origin_claim_fence");
  const plain = at("baseline-drain", { claimTimeoutBusy: "on", claimLockWaitMs: 250 });
  const fenced = at("baseline-drain", { claimTimeoutBusy: "on", claimLockWaitMs: 250, originClaimFence: "on" });
  const pass = { status: "PRIVATE_INGEST_LIVE_TRANSITION_PASS", from: "baseline-drain", to: "baseline-drain" };
  const fromPlain = live(plain), fromFenced = live(fenced);
  expect(privateIngestTransitionReadback(fenced, fromPlain.active, fromPlain.version)).toMatchObject(pass);  // add
  expect(privateIngestTransitionReadback(plain, fromFenced.active, fromFenced.version)).toMatchObject(pass); // remove (rollback)
  expect(privateIngestVersionReadback(fenced, fromFenced.active, fromFenced.version)).toMatchObject({ phase: "baseline-drain" });
  for (const bad of [[{ name: "INGEST_ORIGIN_CLAIM_FENCE", type: "plain_text", text: "off" }], [{ name: "INGEST_ORIGIN_CLAIM_FENCE", type: "secret_text" }]]) {
    const fixture = live(plain, bad);
    expect(code(() => privateIngestTransitionReadback(plain, fixture.active, fixture.version))).toBe("private_ingest_live_cutover");
  }
});

test("turn-fence registration control is strict and independently default-off", () => {
  expect(privateIngestCutoverFromEnv({ PRIVATE_INGEST_TURN_FENCE_REGISTER: "on" }))
    .toEqual({ turnFenceRegister: "on" });
  expect(privateIngestCutoverFromEnv({ PRIVATE_INGEST_TURN_FENCE_REGISTER: "" })).toEqual({});
  for (const value of ["true", "1", "off", "ON"])
    expect(code(() => privateIngestCutoverFromEnv({ PRIVATE_INGEST_TURN_FENCE_REGISTER: value })))
      .toBe("private_ingest_profile_turn_fence_register");
  expect(privateIngestReleasePhase(at("baseline-drain", { turnFenceRegister: "on" }))).toBe("baseline-drain");
  const plain = at("baseline-drain");
  const registered = at("baseline-drain", { turnFenceRegister: "on" });
  const pass = { status: "PRIVATE_INGEST_LIVE_TRANSITION_PASS", from: "baseline-drain", to: "baseline-drain" };
  const fromPlain = live(plain), fromRegistered = live(registered);
  expect(privateIngestTransitionReadback(registered, fromPlain.active, fromPlain.version)).toMatchObject(pass);
  expect(privateIngestTransitionReadback(plain, fromRegistered.active, fromRegistered.version)).toMatchObject(pass);
  expect(privateIngestVersionReadback(registered, fromRegistered.active, fromRegistered.version))
    .toMatchObject({ phase: "baseline-drain" });
  for (const bad of [[{ name: "INGEST_TURN_FENCE_REGISTER", type: "plain_text", text: "off" }],
    [{ name: "INGEST_TURN_FENCE_REGISTER", type: "secret_text" }]]) {
    const fixture = live(plain, bad);
    expect(code(() => privateIngestTransitionReadback(plain, fixture.active, fixture.version)))
      .toBe("private_ingest_live_cutover");
  }
});

// Buffered commit lock wait (ledger-sql.ts bufferCommitLockWaitSql; docs/design/buffer-commit-lock-wait.md): a numeric
// cutover control like claimLockWaitMs, 2001..4000. Unset or empty is off and renders nothing; add, change (the live
// replacement path) and remove pass the transition guard at the held phase, and the version readback is exact.
// Removal is an unbind the operator admits only through bufferCommitLockWaitDeletionAllowed (private-ingest-operator.test.ts).
test("buffer commit lock wait: env shapes, profile refusals, render, add / change / remove at a held phase, exact readback", () => {
  const name = "INGEST_BUFFER_COMMIT_LOCK_WAIT_MS", envName = "PRIVATE_INGEST_BUFFER_COMMIT_LOCK_WAIT_MS";
  const refusal = "private_ingest_profile_buffer_commit_lock_wait";
  expect(PRIVATE_INGEST_CUTOVER_KEYS).toContain("bufferCommitLockWaitMs");
  expect(privateIngestCutoverFromEnv({ [envName]: "" })).toEqual({});
  expect(privateIngestCutoverFromEnv({})).toEqual({});
  for (const [v, ms] of [["2001", 2001], ["3000", 3000], ["4000", 4000]] as const)
    expect(privateIngestCutoverFromEnv({ [envName]: v })).toEqual({ bufferCommitLockWaitMs: ms });
  for (const v of [" ", "2000", "4001", "5000", "0", "02500", " 3000", "3000 ", "+3000", "-3000", "3000ms", "3s", "3e3", "3000.0", "0xBB8", "3,000", "30000", "abc", "on"])
    expect(code(() => privateIngestCutoverFromEnv({ [envName]: v }))).toBe(refusal);
  for (const v of [2000, 4001, 0, 3000.5, "3000", true, null])
    expect(code(() => privateIngestReleasePhase(at("baseline-drain", { bufferCommitLockWaitMs: v })))).toBe(refusal);

  for (const phase of ["baseline-drain", "step-0-shadow", "step-1-custody", "step-2-frozen-receipt", "step-4-async-on"] as const) {
    expect(Object.hasOwn(sessionIngestVars(at(phase).sessionIngest), name)).toBe(false);
    const on = at(phase, { bufferCommitLockWaitMs: 3000 });
    expect(privateIngestReleasePhase(on)).toBe(phase);
    expect(sessionIngestVars(on.sessionIngest)).toEqual({ ...sessionIngestVars(at(phase).sessionIngest), [name]: "3000" });
  }

  const plain = at("baseline-drain"), on = at("baseline-drain", { bufferCommitLockWaitMs: 3000 }), longer = at("baseline-drain", { bufferCommitLockWaitMs: 3500 });
  const pass = { status: "PRIVATE_INGEST_LIVE_TRANSITION_PASS", from: "baseline-drain", to: "baseline-drain" };
  const fromPlain = live(plain), fromOn = live(on), fromLonger = live(longer);
  expect(privateIngestTransitionReadback(on, fromPlain.active, fromPlain.version)).toMatchObject(pass);    // add
  expect(privateIngestTransitionReadback(longer, fromOn.active, fromOn.version)).toMatchObject(pass);     // change (replacement)
  expect(privateIngestTransitionReadback(on, fromLonger.active, fromLonger.version)).toMatchObject(pass); // change back
  expect(privateIngestTransitionReadback(plain, fromOn.active, fromOn.version)).toMatchObject(pass);      // remove (rollback)
  // Exact readback: the live version must carry exactly the candidate's value (or none).
  expect(privateIngestVersionReadback(on, fromOn.active, fromOn.version)).toMatchObject({ phase: "baseline-drain" });
  expect(privateIngestVersionReadback(longer, fromLonger.active, fromLonger.version)).toMatchObject({ phase: "baseline-drain" });
  expect(privateIngestVersionReadback(plain, fromPlain.active, fromPlain.version)).toMatchObject({ phase: "baseline-drain" });
  expect(code(() => privateIngestVersionReadback(longer, fromOn.active, fromOn.version))).not.toBeNull();
  expect(code(() => privateIngestVersionReadback(plain, fromOn.active, fromOn.version))).not.toBeNull();
  expect(code(() => privateIngestVersionReadback(on, fromPlain.active, fromPlain.version))).not.toBeNull();
  // A malformed live value refuses the transition (never read as off or clamped).
  for (const bad of [
    [{ name, type: "plain_text", text: "6000" }], [{ name, type: "plain_text", text: "2000" }], [{ name, type: "plain_text", text: " 3000" }],
    [{ name, type: "plain_text", text: "3000ms" }], [{ name, type: "plain_text", text: "" }], [{ name, type: "secret_text" }],
    [{ name, type: "plain_text", text: "3000" }, { name, type: "plain_text", text: "3000" }],
  ]) {
    const fixture = live(plain, bad);
    expect(code(() => privateIngestTransitionReadback(plain, fixture.active, fixture.version))).toBe("private_ingest_live_cutover");
  }
  // Independent of the buffer lock and claim controls: all together pass.
  const all = at("baseline-drain", { bufferCommitLockWaitMs: 4000, bufferLockBusy: "on", bufferLockRetry: "on", claimTimeoutBusy: "on", claimLockWaitMs: 250 });
  expect(privateIngestReleasePhase(all)).toBe("baseline-drain");
  const fromAll = live(all);
  expect(privateIngestVersionReadback(all, fromAll.active, fromAll.version)).toMatchObject({ phase: "baseline-drain" });
  // Tracked production profile: no var with the variable unset; the workflow supplies it only from the protected environment.
  const tracked = JSON.parse(readFileSync("config/cloudflare/private-ingest-production.json", "utf8"));
  const production = { ...tracked, sessionIngest: { ...tracked.sessionIngest, kmsKeyArn, ...privateIngestCutoverFromEnv({}) } };
  expect(Object.hasOwn(sessionIngestVars(production.sessionIngest), name)).toBe(false);
  const workflow = readFileSync(".github/workflows/deploy-private-ingest-worker.yml", "utf8");
  expect(workflow).toContain(`${envName}: \${{ vars.${envName} }}`);
  expect(workflow.split("\npermissions:")[0]).not.toContain(envName);
});

// Unit C controls: the buffered commit's own statement budget (numeric, 5001..7000) and the buffer-stage statement
// timeout class ("on"). Cutover controls like the unit A lock wait: unset/empty off, exact shapes, add / change /
// remove at a held phase, exact readback, malformed live values refused; the combination rule is checked too.
test("buffer commit statement budget and buffer timeout busy: env shapes, refusals, render, transitions, readback", () => {
  const budgetName = "INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS", budgetEnv = "PRIVATE_INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS";
  const tbName = "INGEST_BUFFER_TIMEOUT_BUSY", tbEnv = "PRIVATE_INGEST_BUFFER_TIMEOUT_BUSY";
  for (const k of ["bufferCommitBudgetMs", "bufferTimeoutBusy"]) expect(PRIVATE_INGEST_CUTOVER_KEYS).toContain(k as never);
  expect(privateIngestCutoverFromEnv({ [budgetEnv]: "", [tbEnv]: "" })).toEqual({});
  for (const [v, ms] of [["5001", 5001], ["6000", 6000], ["7000", 7000]] as const) expect(privateIngestCutoverFromEnv({ [budgetEnv]: v })).toEqual({ bufferCommitBudgetMs: ms });
  expect(privateIngestCutoverFromEnv({ [tbEnv]: "on" })).toEqual({ bufferTimeoutBusy: "on" });
  for (const v of [" ", "5000", "7001", "06000", " 6000", "6000 ", "+6000", "6000ms", "6e3", "6000.0", "60000", "on"])
    expect(code(() => privateIngestCutoverFromEnv({ [budgetEnv]: v }))).toBe("private_ingest_profile_buffer_commit_budget");
  for (const v of ["off", "ON", "true", "1", " on"]) expect(code(() => privateIngestCutoverFromEnv({ [tbEnv]: v }))).toBe("private_ingest_profile_buffer_timeout_busy");
  for (const v of [5000, 7001, 6000.5, "6000", true, null]) expect(code(() => privateIngestReleasePhase(at("baseline-drain", { bufferCommitBudgetMs: v })))).toBe("private_ingest_profile_buffer_commit_budget");
  for (const v of ["off", "ON", true, 1, ""]) expect(code(() => privateIngestReleasePhase(at("baseline-drain", { bufferTimeoutBusy: v })))).toBe("private_ingest_profile_buffer_timeout_busy");
  // The lock wait must end 1 s before the budget: 4000 with 5001 passes (both ranges' extremes).
  expect(privateIngestReleasePhase(at("baseline-drain", { bufferCommitBudgetMs: 5001, bufferCommitLockWaitMs: 4000 }))).toBe("baseline-drain");
  for (const phase of ["baseline-drain", "step-4-async-on"] as const) {
    const plainVars = sessionIngestVars(at(phase).sessionIngest);
    for (const name of [budgetName, tbName]) expect(Object.hasOwn(plainVars, name)).toBe(false);
    expect(sessionIngestVars(at(phase, { bufferCommitBudgetMs: 6000 }).sessionIngest)).toEqual({ ...plainVars, [budgetName]: "6000" });
    expect(sessionIngestVars(at(phase, { bufferTimeoutBusy: "on" }).sessionIngest)).toEqual({ ...plainVars, [tbName]: "on" });
  }
  const plain = at("baseline-drain"), on = at("baseline-drain", { bufferCommitBudgetMs: 6000, bufferTimeoutBusy: "on" }), longer = at("baseline-drain", { bufferCommitBudgetMs: 6500, bufferTimeoutBusy: "on" });
  const pass = { status: "PRIVATE_INGEST_LIVE_TRANSITION_PASS", from: "baseline-drain", to: "baseline-drain" };
  const fromPlain = live(plain), fromOn = live(on), fromLonger = live(longer);
  expect(privateIngestTransitionReadback(on, fromPlain.active, fromPlain.version)).toMatchObject(pass);
  expect(privateIngestTransitionReadback(longer, fromOn.active, fromOn.version)).toMatchObject(pass);
  expect(privateIngestTransitionReadback(plain, fromOn.active, fromOn.version)).toMatchObject(pass);
  expect(privateIngestVersionReadback(on, fromOn.active, fromOn.version)).toMatchObject({ phase: "baseline-drain" });
  expect(code(() => privateIngestVersionReadback(longer, fromOn.active, fromOn.version))).not.toBeNull();
  expect(code(() => privateIngestVersionReadback(plain, fromOn.active, fromOn.version))).not.toBeNull();
  for (const bad of [[{ name: budgetName, type: "plain_text", text: "9000" }], [{ name: budgetName, type: "plain_text", text: "5000" }], [{ name: budgetName, type: "plain_text", text: "" }],
    [{ name: budgetName, type: "secret_text" }], [{ name: tbName, type: "plain_text", text: "off" }], [{ name: tbName, type: "secret_text" }]]) {
    const fixture = live(plain, bad);
    expect(code(() => privateIngestTransitionReadback(plain, fixture.active, fixture.version))).toBe("private_ingest_live_cutover");
  }
  const workflow = readFileSync(".github/workflows/deploy-private-ingest-worker.yml", "utf8");
  for (const n of [budgetEnv, tbEnv]) { expect(workflow).toContain(`${n}: \${{ vars.${n} }}`); expect(workflow.split("\npermissions:")[0]).not.toContain(n); }
  const tracked = JSON.parse(readFileSync("config/cloudflare/private-ingest-production.json", "utf8"));
  const production = { ...tracked, sessionIngest: { ...tracked.sessionIngest, kmsKeyArn, ...privateIngestCutoverFromEnv({}) } };
  for (const n of [budgetName, tbName]) expect(Object.hasOwn(sessionIngestVars(production.sessionIngest), n)).toBe(false);
});
