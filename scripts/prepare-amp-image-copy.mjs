import { readFileSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { REPOSITORY, REPOSITORY_ID, githubJSON, verifyAncestry, verifySourceCI } from "./check-image-release.mjs";
import { LEGACY_SIGNER_REPOSITORY, PUBLICATION_SIGNER_REPOSITORIES, RELEASE_LAYOUTS, verifyRelease } from "./image-release-provenance.mjs";
// The legacy name is accepted only for publications created before the rename
// (2026-11-07 ~05:56Z; no publish-image run exists between this bound and it),
// so a later repository reusing the freed old name cannot sign accepted releases.
export const LEGACY_SIGNER_CREATED_BEFORE = "2026-11-07T00:00:00Z";
const fail = reason => { throw new Error(`Amp image copy refused: ${reason}`); };
const sha = value => typeof value === "string" && /^[a-f0-9]{40}$/.test(value);
const id = value => typeof value === "string" && /^[1-9][0-9]{0,14}$/.test(value);
export function validateCopyRequest(e) {
  if (e.GITHUB_REPOSITORY !== REPOSITORY || e.GITHUB_EVENT_NAME !== "workflow_dispatch" ||
      e.GITHUB_REF !== "refs/heads/main" || e.GITHUB_ACTOR !== "khaliqgant" ||
      e.GITHUB_TRIGGERING_ACTOR !== "khaliqgant" || e.CONFIRM !== "copy reviewed Amp image" ||
      e.GITHUB_WORKFLOW_REF !== `${REPOSITORY}/.github/workflows/copy-amp-image.yml@refs/heads/main` ||
      !sha(e.GITHUB_SHA)) fail("manual owner/main workflow identity required");
  if (!sha(e.SOURCE_SHA) || !id(e.PUBLISH_RUN_ID) ||
      !/^sha256:[a-f0-9]{64}$/.test(e.IMAGE_DIGEST ?? "") ||
      !/^[a-f0-9]{32}$/.test(e.CLOUDFLARE_ACCOUNT_ID ?? "") || /^0+$/.test(e.CLOUDFLARE_ACCOUNT_ID))
    fail("exact approved source, signed image, publication and target account required");
}
// The receipt selects exactly one known image layout: a dual-published release
// (primary sessions-pipeline image plus the exact ordered list of every
// repository it was published to) or a pre-dual-publish release (96bulls-services
// only, no list). The signed claims are then verified under that same layout.
export function receiptLayout(receipt, digest) {
  const listed = layout => layout.images === undefined ? receipt.images === undefined :
    Array.isArray(receipt.images) && receipt.images.length === layout.images.length &&
    layout.images.every((name, i) => receipt.images[i] === `${name}@${digest}`);
  return Object.values(RELEASE_LAYOUTS).find(layout => receipt.image === `${layout.image}@${digest}` && listed(layout));
}
export function validatePublication(e, receipt, run) {
  validateCopyRequest(e);
  // A publication signed before the rename names the old repository; accept
  // only the exact allow-listed names and verify it under that one name.
  const signer = PUBLICATION_SIGNER_REPOSITORIES.find(name => receipt.repository === `https://github.com/${name}`);
  const layout = receiptLayout(receipt, e.IMAGE_DIGEST);
  if (!signer || !layout ||
      receipt.source_sha !== e.SOURCE_SHA ||
      receipt.source_ref !== "refs/heads/main" || receipt.attestation_backend !== "sigstore" ||
      !sha(receipt.workflow_sha) || !id(receipt.ci_run_id) || receipt.publish_run_id !== e.PUBLISH_RUN_ID ||
      !id(receipt.publish_run_attempt)) fail("publication receipt does not match approval");
  if (!Number.isSafeInteger(run.id) || String(run.id) !== e.PUBLISH_RUN_ID ||
      run.status !== "completed" || run.conclusion !== "success" || run.event !== "workflow_dispatch" ||
      run.head_branch !== "main" || run.path !== ".github/workflows/publish-image.yml" ||
      run.head_sha !== receipt.workflow_sha || String(run.run_attempt) !== receipt.publish_run_attempt ||
      run.repository?.full_name !== REPOSITORY || run.head_repository?.full_name !== REPOSITORY ||
      run.repository?.id !== REPOSITORY_ID || run.head_repository?.id !== REPOSITORY_ID ||
      run.actor?.login !== "khaliqgant" || run.triggering_actor?.login !== "khaliqgant")
    fail("successful canonical owner publication identity required");
  if (signer === LEGACY_SIGNER_REPOSITORY && !(typeof run.created_at === "string" &&
      Date.parse(run.created_at) < Date.parse(LEGACY_SIGNER_CREATED_BEFORE)))
    fail("legacy repository name is accepted only for publications before the rename");
  return { signer, layout };
}
async function main() {
  const e = process.env; validateCopyRequest(e);
  if (!e.ARTIFACT_DIR) fail("artifact directory required");
  const receiptPath = join(e.ARTIFACT_DIR, "release-receipt.json");
  if (statSync(receiptPath).size > 65536) fail("oversized publication receipt");
  const receipt = JSON.parse(readFileSync(receiptPath, "utf8"));
  const get = githubJSON(e.GH_TOKEN);
  const run = await get(`/repos/${REPOSITORY}/actions/runs/${e.PUBLISH_RUN_ID}`);
  const { signer, layout } = validatePublication(e, receipt, run);
  verifyAncestry(e.SOURCE_SHA); verifyAncestry(receipt.workflow_sha);
  const currentCI = await verifySourceCI(e.SOURCE_SHA, get);
  if (String(currentCI) !== receipt.ci_run_id) fail("source CI changed after signing");
  verifyRelease({ SOURCE_SHA: e.SOURCE_SHA, WORKFLOW_SHA: receipt.workflow_sha,
    SOURCE_CI_RUN: receipt.ci_run_id, PUBLISH_RUN_ID: e.PUBLISH_RUN_ID,
    PUBLISH_RUN_ATTEMPT: receipt.publish_run_attempt, IMAGE_DIGEST: e.IMAGE_DIGEST }, e.ARTIFACT_DIR, undefined, signer, layout);
  const index = JSON.parse(readFileSync(join(e.ARTIFACT_DIR, "services-index.json"), "utf8"));
  const native = index.manifests.filter(m => m.platform?.os === "linux" && m.platform?.architecture === "amd64");
  if (native.length !== 1 || !/^sha256:[a-f0-9]{64}$/.test(native[0].digest ?? ""))
    fail("one verified native amd64 manifest required");
  const plan = { source_sha: e.SOURCE_SHA, signed_index_digest: e.IMAGE_DIGEST,
    linux_amd64_digest: native[0].digest, account_id: e.CLOUDFLARE_ACCOUNT_ID,
    publication_run_id: e.PUBLISH_RUN_ID, publication_workflow_sha: receipt.workflow_sha,
    source_ci_run_id: receipt.ci_run_id, copy_workflow_sha: e.GITHUB_SHA };
  writeFileSync(join(e.ARTIFACT_DIR, "amp-copy-plan.json"), JSON.stringify(plan, null, 2) + "\n");
  console.log(`Verified reviewed native image ${native[0].digest} from publication ${e.PUBLISH_RUN_ID}`);
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(() => { console.error("Amp copy provenance validation failed"); process.exitCode = 1; });
}
