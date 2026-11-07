import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readFileSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { REPOSITORY, validateRequest } from "./check-image-release.mjs";

export const IMAGE = "ghcr.io/agentworkforce/96bulls-services";
export const SOURCE_TYPE = "https://agentrelay.com/attestations/reviewed-image-source/v1";
export const BUILD_TYPE = "https://slsa.dev/provenance/v1";
const refuse = reason => { throw new Error(`Image provenance refused: ${reason}`); };
// Exact signer names accepted when verifying an ALREADY-PUBLISHED release (the
// Amp copy path). Releases signed before the 2026-11-07 rename carry the old
// name in their Fulcio certificate, predicates and receipt; both entries are
// the same repository (id REPOSITORY_ID, checked against the GitHub API by the
// caller). Exact string matches only, one name used consistently for every
// check of a release. New signatures and in-run verification use REPOSITORY.
export const LEGACY_SIGNER_REPOSITORY = "AgentWorkforce/sessions-pipeline";
export const PUBLICATION_SIGNER_REPOSITORIES = Object.freeze([REPOSITORY, LEGACY_SIGNER_REPOSITORY]);
const signerIdentity = repository => {
  if (typeof repository !== "string" || !PUBLICATION_SIGNER_REPOSITORIES.includes(repository))
    refuse("unknown signer repository");
  const url = `https://github.com/${repository}`;
  return { repository, url, workflow: `${url}/.github/workflows/publish-image.yml` };
};

export function releaseParams(env) {
  const values = {
    source: env.SOURCE_SHA, workflow: env.WORKFLOW_SHA, ci: env.SOURCE_CI_RUN,
    run: env.PUBLISH_RUN_ID ?? env.GITHUB_RUN_ID,
    attempt: env.PUBLISH_RUN_ATTEMPT ?? env.GITHUB_RUN_ATTEMPT,
    digest: env.IMAGE_DIGEST,
  };
  if (![values.source, values.workflow].every(v => typeof v === "string" && /^[a-f0-9]{40}$/.test(v)) ||
      ![values.ci, values.run, values.attempt].every(v => typeof v === "string" && /^[1-9][0-9]{0,19}$/.test(v)) ||
      typeof values.digest !== "string" || !/^sha256:[a-f0-9]{64}$/.test(values.digest))
    refuse("exact source, workflow, CI, publication and image identities required");
  return values;
}

export function predicates(env, repository = REPOSITORY) {
  const p = releaseParams(env);
  const { url: REPO_URL, workflow: WORKFLOW } = signerIdentity(repository);
  const source = {
    repository: REPO_URL, source_sha: p.source, workflow_sha: p.workflow,
    source_ref: "refs/heads/main", ci_run_id: p.ci, image: `${IMAGE}@${p.digest}`,
  };
  const build = {
    buildDefinition: {
      buildType: "https://agentrelay.com/builders/services-image/v1",
      externalParameters: { repository: REPO_URL, source_sha: p.source, source_ref: "refs/heads/main", image: IMAGE },
      resolvedDependencies: [{ uri: `git+${REPO_URL}`, digest: { gitCommit: p.source } }],
    },
    runDetails: {
      builder: { id: `${WORKFLOW}@${p.workflow}` },
      metadata: { invocationId: `${REPO_URL}/actions/runs/${p.run}/attempts/${p.attempt}` },
    },
  };
  return { source, build };
}

function artifactBytes(path) {
  const metadata = statSync(path);
  if (!metadata.isFile() || !metadata.size || metadata.size > 4 * 1024 * 1024) refuse("invalid attestation artifact size");
  const bytes = readFileSync(path);
  if (!bytes.length || bytes.length > 4 * 1024 * 1024) refuse("invalid attestation artifact size");
  return bytes;
}

function verifyIndex(bytes, p) {
  if (`sha256:${createHash("sha256").update(bytes).digest("hex")}` !== p.digest)
    refuse("OCI index does not match the expected published digest");
  try {
    const index = JSON.parse(bytes);
    const platforms = index.manifests.filter(m => m.platform?.os !== "unknown")
      .map(m => `${m.platform?.os}/${m.platform?.architecture}`).sort();
    if (index.schemaVersion !== 2 || index.mediaType !== "application/vnd.oci.image.index.v1+json" ||
        platforms.join(",") !== "linux/amd64,linux/arm64") throw Error();
  } catch { refuse("expected the complete amd64/arm64 OCI index"); }
}

function statement(bytes, type, p) {
  try {
    const bundle = JSON.parse(bytes);
    if (bundle.dsseEnvelope?.payloadType !== "application/vnd.in-toto+json") throw Error();
    const payload = JSON.parse(Buffer.from(bundle.dsseEnvelope.payload, "base64").toString("utf8"));
    if (!["https://in-toto.io/Statement/v1", "https://in-toto.io/Statement/v0.1"].includes(payload._type) ||
        payload.predicateType !== type || !Array.isArray(payload.subject) || payload.subject.length !== 1 ||
        payload.subject[0].digest?.sha256 !== p.digest.slice(7)) throw Error();
    return payload.predicate;
  } catch { refuse("verified attestation subject or type does not match"); }
}

export function verifyRelease(env, directory, run = execFileSync, repository = REPOSITORY) {
  const p = releaseParams(env);
  const { url: REPO_URL, workflow: WORKFLOW } = signerIdentity(repository);
  const index = artifactBytes(join(directory, "services-index.json"));
  verifyIndex(index, p);
  const expected = predicates(env, repository);
  for (const [file, type, kind] of [
    ["build-provenance.sigstore.json", BUILD_TYPE, "build"],
    ["reviewed-source.sigstore.json", SOURCE_TYPE, "source"],
  ]) {
    const bundlePath = join(directory, file);
    const bytes = artifactBytes(bundlePath);
    try {
      run("cosign", ["verify-blob-attestation", "--bundle", bundlePath,
        "--digest", p.digest.slice(7), "--digestAlg", "sha256", "--type", type,
        "--certificate-identity", `${WORKFLOW}@refs/heads/main`,
        "--certificate-oidc-issuer", "https://token.actions.githubusercontent.com",
        "--certificate-github-workflow-repository", repository,
        "--certificate-github-workflow-ref", "refs/heads/main",
        "--certificate-github-workflow-sha", p.workflow,
        "--certificate-github-workflow-trigger", "workflow_dispatch"],
      { stdio: ["ignore", "pipe", "pipe"], maxBuffer: 4 * 1024 * 1024 });
    } catch { refuse("signature, transparency or workflow identity verification failed"); }
    if (!artifactBytes(bundlePath).equals(bytes)) refuse("attestation changed during verification");
    const claims = statement(bytes, type, p);
    if (kind === "source") {
      if (!claims || Object.entries(expected.source).some(([key, value]) => claims[key] !== value))
        refuse("reviewed source, workflow, image or CI does not match");
    } else {
      const definition = claims?.buildDefinition;
      if (definition?.buildType !== expected.build.buildDefinition.buildType ||
          Object.entries(expected.build.buildDefinition.externalParameters).some(([key, value]) => definition?.externalParameters?.[key] !== value) ||
          !Array.isArray(definition?.resolvedDependencies) || definition.resolvedDependencies.length !== 1 ||
          definition.resolvedDependencies[0].uri !== `git+${REPO_URL}` ||
          definition.resolvedDependencies[0].digest?.gitCommit !== p.source ||
          claims?.runDetails?.builder?.id !== expected.build.runDetails.builder.id ||
          claims?.runDetails?.metadata?.invocationId !== expected.build.runDetails.metadata.invocationId)
        refuse("build provenance does not match the approved invocation");
    }
  }
  return { image: `${IMAGE}@${p.digest}`, source_sha: p.source, workflow_sha: p.workflow, ci_run_id: p.ci };
}

function main() {
  const directory = process.env.ARTIFACT_DIR;
  if (!directory) refuse("artifact directory required");
  if (process.argv[2] === "prepare") {
    validateRequest({ ...process.env, GITHUB_SHA: process.env.WORKFLOW_SHA });
    const p = releaseParams(process.env);
    const bytes = artifactBytes(join(directory, "services-index.json"));
    verifyIndex(bytes, p);
    const values = predicates(process.env);
    writeFileSync(join(directory, "build-predicate.json"), JSON.stringify(values.build) + "\n");
    writeFileSync(join(directory, "source-predicate.json"), JSON.stringify(values.source) + "\n");
  } else if (process.argv[2] === "verify") {
    const receipt = verifyRelease(process.env, directory);
    console.log(`Verified image provenance for ${receipt.image}`);
  } else refuse("expected prepare or verify");
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
