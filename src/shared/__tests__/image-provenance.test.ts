import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BUILD_TYPE, IMAGE, SOURCE_TYPE, predicates, releaseParams, verifyRelease } from "../../../scripts/image-release-provenance.mjs";
const REPO = "AgentWorkforce/relayhistory-pipeline", LEGACY = "AgentWorkforce/sessions-pipeline";

function fixture(signer?: string) {
  const directory = mkdtempSync(join(tmpdir(), "image-provenance-"));
  const index = JSON.stringify({ schemaVersion: 2, mediaType: "application/vnd.oci.image.index.v1+json", manifests: [
    { platform: { os: "linux", architecture: "amd64" } },
    { platform: { os: "linux", architecture: "arm64" } },
    { platform: { os: "unknown", architecture: "unknown" } },
  ] });
  const env = { SOURCE_SHA: "a".repeat(40), WORKFLOW_SHA: "b".repeat(40), SOURCE_CI_RUN: "123",
    PUBLISH_RUN_ID: "456", PUBLISH_RUN_ATTEMPT: "2",
    IMAGE_DIGEST: `sha256:${createHash("sha256").update(index).digest("hex")}` };
  const values = predicates(env, signer);
  writeFileSync(join(directory, "services-index.json"), index);
  for (const [file, type, predicate] of [
    ["build-provenance.sigstore.json", BUILD_TYPE, values.build],
    ["reviewed-source.sigstore.json", SOURCE_TYPE, values.source],
  ] as const) {
    const payload = { _type: "https://in-toto.io/Statement/v1", predicateType: type,
      subject: [{ name: "services-index.json", digest: { sha256: env.IMAGE_DIGEST.slice(7) } }], predicate };
    // Synthetic envelopes: tests stub the cryptographic CLI boundary. They are
    // deliberately not real signed fixtures and cannot pass production cosign.
    writeFileSync(join(directory, file), JSON.stringify({ dsseEnvelope: {
      payloadType: "application/vnd.in-toto+json", payload: Buffer.from(JSON.stringify(payload)).toString("base64"),
    } }));
  }
  return { directory, env, cleanup: () => rmSync(directory, { recursive: true, force: true }) };
}
function change(directory: string, file: string, mutate: (value: any) => void) {
  const path = join(directory, file);
  const bundle = JSON.parse(readFileSync(path, "utf8"));
  const value = JSON.parse(Buffer.from(bundle.dsseEnvelope.payload, "base64").toString("utf8"));
  mutate(value);
  bundle.dsseEnvelope.payload = Buffer.from(JSON.stringify(value)).toString("base64");
  writeFileSync(path, JSON.stringify(bundle));
}

for (const [label, signer, name] of [["new-name", undefined, REPO], ["pre-rename", LEGACY, LEGACY]] as const)
test(`verifies both ${label} signatures with exact workflow identity before accepting their claims`, () => {
  const f = fixture(signer); const calls: string[][] = [];
  try {
    expect(predicates(f.env, signer).source.repository).toBe(`https://github.com/${name}`);
    const receipt = verifyRelease(f.env, f.directory, (command: string, args: string[]) => {
      expect(command).toBe("cosign"); calls.push(args);
      for (const [flag, value] of [
        ["--certificate-identity", `https://github.com/${name}/.github/workflows/publish-image.yml@refs/heads/main`],
        ["--certificate-oidc-issuer", "https://token.actions.githubusercontent.com"],
        ["--certificate-github-workflow-sha", f.env.WORKFLOW_SHA],
        ["--certificate-github-workflow-repository", name],
        ["--certificate-github-workflow-ref", "refs/heads/main"],
        ["--certificate-github-workflow-trigger", "workflow_dispatch"],
        ["--digest", f.env.IMAGE_DIGEST.slice(7)], ["--digestAlg", "sha256"],
      ]) expect(args[args.indexOf(flag) + 1]).toBe(value);
    }, signer);
    expect(calls).toHaveLength(2);
    expect(calls.map(args => args[args.indexOf("--type") + 1])).toEqual([BUILD_TYPE, SOURCE_TYPE]);
    expect(receipt.image).toBe(`${IMAGE}@${f.env.IMAGE_DIGEST}`);
  } finally { f.cleanup(); }
});

for (const signer of ["AgentWorkforce/sessions-pipeline-oss", "AgentWorkforce/sessions-pipeline-cloud", "fork/relayhistory-pipeline",
  "agentworkforce/relayhistory-pipeline", `${REPO} `, `${REPO}/`, "AgentWorkforce/relayhistory", "", null])
  test(`an unrelated signer repository ${JSON.stringify(signer)} is refused before any crypto command`, () => {
    const f = fixture(); let calls = 0;
    try {
      expect(() => verifyRelease(f.env, f.directory, () => { calls++; }, signer as any)).toThrow("unknown signer repository");
      expect(() => predicates(f.env, signer as any)).toThrow("unknown signer repository");
      expect(calls).toBe(0);
    } finally { f.cleanup(); }
  });

for (const [claimed, verifiedAs] of [[LEGACY, REPO], [REPO, LEGACY]] as const)
  test(`claims made under ${claimed} cannot be accepted while verifying as ${verifiedAs}`, () => {
    const f = fixture(claimed);
    try {
      expect(() => verifyRelease(f.env, f.directory, () => {}, verifiedAs)).toThrow("does not match");
    } finally { f.cleanup(); }
  });

for (const failingCall of [1, 2]) test(`crypto failure ${failingCall} stops acceptance and hides diagnostics`, () => {
  const f = fixture(); let calls = 0;
  try {
    expect(() => verifyRelease(f.env, f.directory, () => {
      if (++calls === failingCall) throw Error("private CLI diagnostic");
    })).toThrow("signature, transparency or workflow identity verification failed");
    expect(calls).toBe(failingCall);
  } finally { f.cleanup(); }
});

for (const field of ["repository", "source_sha", "workflow_sha", "source_ref", "ci_run_id", "image"]) {
  test(`a cryptographically accepted but wrong reviewed-source ${field} is refused`, () => {
    const f = fixture();
    try {
      change(f.directory, "reviewed-source.sigstore.json", v => { v.predicate[field] = "foreign"; });
      expect(() => verifyRelease(f.env, f.directory, () => {})).toThrow("reviewed source, workflow, image or CI");
    } finally { f.cleanup(); }
  });
}

for (const mutate of [
  (v: any) => { v.predicate.runDetails.builder.id = "other builder"; },
  (v: any) => { v.predicate.runDetails.metadata.invocationId = "other invocation"; },
  (v: any) => { v.predicate.buildDefinition.resolvedDependencies[0].digest.gitCommit = "c".repeat(40); },
  (v: any) => { v.predicate.buildDefinition.externalParameters.source_ref = "refs/heads/topic"; },
]) test("foreign builder, invocation or source dependency is refused", () => {
  const f = fixture();
  try {
    change(f.directory, "build-provenance.sigstore.json", mutate);
    expect(() => verifyRelease(f.env, f.directory, () => {})).toThrow("build provenance does not match");
  } finally { f.cleanup(); }
});

for (const mutate of [
  (v: any) => { v.subject[0].digest.sha256 = "c".repeat(64); },
  (v: any) => { v.predicateType = "unrelated"; },
  (v: any) => { v._type = "unsupported"; },
  (v: any) => { v.subject.push(v.subject[0]); },
]) test("an unrelated attestation subject/type cannot authorize the image", () => {
  const f = fixture();
  try {
    change(f.directory, "build-provenance.sigstore.json", mutate);
    expect(() => verifyRelease(f.env, f.directory, () => {})).toThrow("subject or type does not match");
  } finally { f.cleanup(); }
});

test("index tampering stops verification before either crypto command", () => {
  const f = fixture(); let calls = 0;
  try {
    writeFileSync(join(f.directory, "services-index.json"), "other index");
    expect(() => verifyRelease(f.env, f.directory, () => { calls++; })).toThrow("OCI index does not match");
    expect(calls).toBe(0);
  } finally { f.cleanup(); }
});

test("an attestation changing during crypto verification is refused", () => {
  const f = fixture();
  try {
    expect(() => verifyRelease(f.env, f.directory, () => {
      writeFileSync(join(f.directory, "build-provenance.sigstore.json"), "changed bundle");
    })).toThrow("changed during verification");
  } finally { f.cleanup(); }
});

for (const [field, value] of [
  ["SOURCE_SHA", "main"], ["WORKFLOW_SHA", "b".repeat(7)], ["SOURCE_CI_RUN", "0"],
  ["IMAGE_DIGEST", "latest"], ["PUBLISH_RUN_ID", "-1"], ["PUBLISH_RUN_ATTEMPT", "0"],
]) test(`unsafe ${field} cannot enter a verification command`, () => {
  const f = fixture();
  try { expect(() => releaseParams({ ...f.env, [field]: value })).toThrow("exact source"); }
  finally { f.cleanup(); }
});
