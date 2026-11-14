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
