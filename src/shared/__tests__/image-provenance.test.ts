import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BUILD_TYPE, IMAGE, LEGACY_IMAGE, PUBLISHED_IMAGES, RELEASE_LAYOUTS, SOURCE_TYPE, predicates, releaseParams, verifyRelease } from "../../../scripts/image-release-provenance.mjs";
const NEW_IMAGE = "ghcr.io/agentworkforce/sessions-pipeline", OLD_IMAGE = "ghcr.io/agentworkforce/96bulls-services";
const REPO = "AgentWorkforce/relayhistory-pipeline", LEGACY = "AgentWorkforce/sessions-pipeline";

function fixture(signer?: string, layout?: any) {
  const directory = mkdtempSync(join(tmpdir(), "image-provenance-"));
  const index = JSON.stringify({ schemaVersion: 2, mediaType: "application/vnd.oci.image.index.v1+json", manifests: [
    { platform: { os: "linux", architecture: "amd64" } },
    { platform: { os: "linux", architecture: "arm64" } },
    { platform: { os: "unknown", architecture: "unknown" } },
  ] });
  const env = { SOURCE_SHA: "a".repeat(40), WORKFLOW_SHA: "b".repeat(40), SOURCE_CI_RUN: "123",
    PUBLISH_RUN_ID: "456", PUBLISH_RUN_ATTEMPT: "2",
    IMAGE_DIGEST: `sha256:${createHash("sha256").update(index).digest("hex")}` };
  const values = predicates(env, signer, layout);
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
    expect(receipt.images).toEqual([`${NEW_IMAGE}@${f.env.IMAGE_DIGEST}`, `${OLD_IMAGE}@${f.env.IMAGE_DIGEST}`]);
  } finally { f.cleanup(); }
});

test("new releases sign the primary sessions-pipeline image and the exact dual-publish list", () => {
  const f = fixture();
  try {
    expect([IMAGE, LEGACY_IMAGE]).toEqual([NEW_IMAGE, OLD_IMAGE]);
    expect(PUBLISHED_IMAGES).toEqual([NEW_IMAGE, OLD_IMAGE]);
    const { source, build } = predicates(f.env);
    expect(source.image).toBe(`${NEW_IMAGE}@${f.env.IMAGE_DIGEST}`);
    expect(source.images).toEqual([`${NEW_IMAGE}@${f.env.IMAGE_DIGEST}`, `${OLD_IMAGE}@${f.env.IMAGE_DIGEST}`]);
    expect(build.buildDefinition.externalParameters.image).toBe(NEW_IMAGE);
    expect(build.buildDefinition.externalParameters.images).toEqual([NEW_IMAGE, OLD_IMAGE]);
  } finally { f.cleanup(); }
});

test("a release signed before the dual publish verifies only under the legacy-only layout", () => {
  for (const signer of [undefined, LEGACY] as const) {
    const f = fixture(signer, RELEASE_LAYOUTS.legacyOnly);
    try {
      const { source, build } = predicates(f.env, signer, RELEASE_LAYOUTS.legacyOnly);
      expect(source.image).toBe(`${OLD_IMAGE}@${f.env.IMAGE_DIGEST}`);
      expect("images" in source).toBe(false);
      expect("images" in build.buildDefinition.externalParameters).toBe(false);
      const receipt = verifyRelease(f.env, f.directory, () => {}, signer, RELEASE_LAYOUTS.legacyOnly);
      expect(receipt.image).toBe(`${OLD_IMAGE}@${f.env.IMAGE_DIGEST}`);
      expect(receipt.images).toBeUndefined();
      expect(() => verifyRelease(f.env, f.directory, () => {}, signer)).toThrow("does not match");
    } finally { f.cleanup(); }
  }
});

test("dual-publish claims cannot be accepted under the legacy-only layout", () => {
  const f = fixture();
  try { expect(() => verifyRelease(f.env, f.directory, () => {}, undefined, RELEASE_LAYOUTS.legacyOnly)).toThrow("does not match"); }
  finally { f.cleanup(); }
});

for (const layout of [null, "current", "legacyOnly", { image: OLD_IMAGE, images: undefined },
  { image: "ghcr.io/attacker/sessions-pipeline", images: undefined }, { ...RELEASE_LAYOUTS.current }])
  test(`an unknown image layout ${JSON.stringify(layout)} is refused before any crypto command`, () => {
    const f = fixture(); let calls = 0;
    try {
      expect(() => verifyRelease(f.env, f.directory, () => { calls++; }, undefined, layout as any)).toThrow("unknown image layout");
      expect(() => predicates(f.env, undefined, layout as any)).toThrow("unknown image layout");
      expect(calls).toBe(0);
    } finally { f.cleanup(); }
  });

const listMutations: Array<[string, (list: string[]) => unknown]> = [
  ["reordered", l => [...l].reverse()], ["missing legacy", l => l.slice(0, 1)], ["missing primary", l => l.slice(1)],
  ["extra repository", l => [...l, l[0]!.replace("agentworkforce", "attacker")]], ["foreign entry", l => [l[0], "ghcr.io/attacker/x"]],
  ["string instead of list", l => l.join(",")], ["removed", () => undefined], ["empty", () => []],
];
for (const [label, mutate] of listMutations) {
  test(`a ${label} reviewed-source images claim is refused`, () => {
    const f = fixture();
    try {
      change(f.directory, "reviewed-source.sigstore.json", v => { v.predicate.images = mutate(v.predicate.images); });
      expect(() => verifyRelease(f.env, f.directory, () => {})).toThrow("reviewed source, workflow, image or CI");
    } finally { f.cleanup(); }
  });
  test(`a ${label} build provenance images claim is refused`, () => {
    const f = fixture();
    try {
      change(f.directory, "build-provenance.sigstore.json", v => {
        v.predicate.buildDefinition.externalParameters.images = mutate(v.predicate.buildDefinition.externalParameters.images);
      });
      expect(() => verifyRelease(f.env, f.directory, () => {})).toThrow("build provenance does not match");
    } finally { f.cleanup(); }
  });
}

test("a legacy-only release that gained an images claim is refused", () => {
  for (const [file, mutate, message] of [
    ["reviewed-source.sigstore.json", (v: any) => { v.predicate.images = [`${OLD_IMAGE}@x`]; }, "reviewed source"],
    ["build-provenance.sigstore.json", (v: any) => { v.predicate.buildDefinition.externalParameters.images = [OLD_IMAGE]; }, "build provenance"],
  ] as const) {
    const f = fixture(undefined, RELEASE_LAYOUTS.legacyOnly);
    try {
      change(f.directory, file, mutate);
      expect(() => verifyRelease(f.env, f.directory, () => {}, undefined, RELEASE_LAYOUTS.legacyOnly)).toThrow(message);
    } finally { f.cleanup(); }
  }
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

for (const field of ["repository", "source_sha", "workflow_sha", "source_ref", "ci_run_id", "image", "images"]) {
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
  (v: any) => { v.predicate.buildDefinition.externalParameters.image = "ghcr.io/agentworkforce/96bulls-services"; },
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
