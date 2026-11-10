import { expect, test } from "bun:test";
import { receiptLayout, validateCopyRequest, validatePublication } from "../../../scripts/prepare-amp-image-copy.mjs";
import { RELEASE_LAYOUTS } from "../../../scripts/image-release-provenance.mjs";
const repo = "AgentWorkforce/relayhistory-pipeline";
const legacy = "AgentWorkforce/sessions-pipeline";
const env = { GITHUB_REPOSITORY: repo, GITHUB_EVENT_NAME: "workflow_dispatch", GITHUB_REF: "refs/heads/main",
  GITHUB_ACTOR: "khaliqgant", GITHUB_TRIGGERING_ACTOR: "khaliqgant", CONFIRM: "copy reviewed Amp image",
  GITHUB_WORKFLOW_REF: `${repo}/.github/workflows/copy-amp-image.yml@refs/heads/main`, GITHUB_SHA: "a".repeat(40),
  SOURCE_SHA: "b".repeat(40), PUBLISH_RUN_ID: "123", IMAGE_DIGEST: "sha256:" + "c".repeat(64), CLOUDFLARE_ACCOUNT_ID: "d".repeat(32) };
const receipt = { repository: `https://github.com/${repo}`, source_sha: env.SOURCE_SHA,
  image: `ghcr.io/agentworkforce/sessions-pipeline@${env.IMAGE_DIGEST}`,
  images: [`ghcr.io/agentworkforce/sessions-pipeline@${env.IMAGE_DIGEST}`, `ghcr.io/agentworkforce/96bulls-services@${env.IMAGE_DIGEST}`],
  source_ref: "refs/heads/main",
  attestation_backend: "sigstore", workflow_sha: "e".repeat(40), ci_run_id: "456", publish_run_id: "123", publish_run_attempt: "1" };
const run = { id: 123, status: "completed", conclusion: "success", event: "workflow_dispatch", head_branch: "main",
  path: ".github/workflows/publish-image.yml", head_sha: receipt.workflow_sha, run_attempt: 1,
  repository: { full_name: repo, id: 1437714333 }, head_repository: { full_name: repo, id: 1437714333 },
  actor: { login: "khaliqgant" }, triggering_actor: { login: "khaliqgant" }, created_at: "2026-11-08T00:00:00Z" };
const legacyImageReceipt = (() => { const { images: _, ...rest } = receipt;
  return { ...rest, image: `ghcr.io/agentworkforce/96bulls-services@${env.IMAGE_DIGEST}` }; })();
test("matches only the approved source/index and successful owner publication", () => {
  expect(validatePublication(env, receipt, run)).toEqual({ signer: repo, layout: RELEASE_LAYOUTS.current });
});
test("a release published before the dual publish (96bulls-services only) is verified under the legacy-only layout", () => {
  expect(validatePublication(env, legacyImageReceipt, run)).toEqual({ signer: repo, layout: RELEASE_LAYOUTS.legacyOnly });
  const old = { ...legacyImageReceipt, repository: `https://github.com/${legacy}` };
  expect(validatePublication(env, old, { ...run, created_at: "2026-11-06T07:34:08Z" })).toEqual({ signer: legacy, layout: RELEASE_LAYOUTS.legacyOnly });
});
const d = env.IMAGE_DIGEST, NEW = "ghcr.io/agentworkforce/sessions-pipeline", OLD = "ghcr.io/agentworkforce/96bulls-services";
for (const [label, image, images] of [
  ["primary without list", `${NEW}@${d}`, undefined],
  ["legacy primary with list", `${OLD}@${d}`, [`${NEW}@${d}`, `${OLD}@${d}`]],
  ["legacy with legacy-only list", `${OLD}@${d}`, [`${OLD}@${d}`]],
  ["reordered list", `${NEW}@${d}`, [`${OLD}@${d}`, `${NEW}@${d}`]],
  ["list missing legacy", `${NEW}@${d}`, [`${NEW}@${d}`]],
  ["list with extra repository", `${NEW}@${d}`, [`${NEW}@${d}`, `${OLD}@${d}`, `ghcr.io/attacker/x@${d}`]],
  ["list with other digest", `${NEW}@${d}`, [`${NEW}@${d}`, `${OLD}@sha256:${"f".repeat(64)}`]],
  ["list as string", `${NEW}@${d}`, `${NEW}@${d},${OLD}@${d}`],
  ["foreign primary", `ghcr.io/attacker/sessions-pipeline@${d}`, undefined],
  ["primary with other digest", `${NEW}@sha256:${"f".repeat(64)}`, [`${NEW}@${d}`, `${OLD}@${d}`]],
] as const) {
  test(`a receipt with ${label} selects no image layout and cannot authorize a copy`, () => {
    const { images: _, ...rest } = receipt;
    const changed = { ...rest, image, ...(images === undefined ? {} : { images }) };
    expect(receiptLayout(changed, d)).toBeUndefined();
    expect(() => validatePublication(env, changed, run)).toThrow("publication receipt");
  });
}
test("a publication signed before the rename is verified under its exact legacy name only", () => {
  const old = { ...receipt, repository: `https://github.com/${legacy}` };
  expect(validatePublication(env, old, { ...run, created_at: "2026-11-06T07:34:08Z" }).signer).toBe(legacy);
  for (const created_at of ["2026-11-07T00:00:00Z", "2026-11-08T00:00:00Z", "not a date", undefined])
    expect(() => validatePublication(env, old, { ...run, created_at })).toThrow("only for publications before the rename");
});
for (const repository of ["https://github.com/AgentWorkforce/sessions-pipeline-oss", "https://github.com/AgentWorkforce/sessions-pipeline-cloud",
  "https://github.com/fork/relayhistory-pipeline", "https://github.com/agentworkforce/relayhistory-pipeline",
  `https://github.com/${repo}/`, `https://github.com/${repo}.git`, `https://github.com/${legacy}-oss`, `http://github.com/${repo}`, repo, legacy]) {
  test(`an unrelated receipt repository ${repository} cannot authorize a copy`, () => {
    expect(() => validatePublication(env, { ...receipt, repository }, { ...run, created_at: "2026-11-06T00:00:00Z" })).toThrow("publication receipt");
  });
}
for (const name of [legacy, "AgentWorkforce/sessions-pipeline-oss", "fork/relayhistory-pipeline"]) {
  test(`copy authority is refused from repository ${name}`, () => {
    expect(() => validateCopyRequest({ ...env, GITHUB_REPOSITORY: name,
      GITHUB_WORKFLOW_REF: `${name}/.github/workflows/copy-amp-image.yml@refs/heads/main` })).toThrow();
  });
}
for (const field of ["GITHUB_REPOSITORY", "GITHUB_EVENT_NAME", "GITHUB_REF", "GITHUB_ACTOR", "GITHUB_TRIGGERING_ACTOR",
  "CONFIRM", "GITHUB_WORKFLOW_REF", "GITHUB_SHA", "SOURCE_SHA", "PUBLISH_RUN_ID", "IMAGE_DIGEST", "CLOUDFLARE_ACCOUNT_ID"]) {
  test(`foreign or malformed ${field} cannot acquire a copy authority`, () => {
    expect(() => validateCopyRequest({ ...env, [field]: "foreign" })).toThrow();
  });
}
for (const field of ["repository", "source_sha", "image", "images", "source_ref", "attestation_backend", "workflow_sha", "ci_run_id", "publish_run_id", "publish_run_attempt"]) {
  test(`a substituted receipt ${field} cannot authorize a copy`, () => {
    expect(() => validatePublication(env, { ...receipt, [field]: "foreign" }, run)).toThrow();
  });
}
for (const change of [ { id: 124 }, { status: "in_progress" }, { conclusion: "failure" }, { event: "pull_request" },
  { head_branch: "other" }, { path: ".github/workflows/other.yml" }, { head_sha: "f".repeat(40) }, { run_attempt: 2 },
  { repository: { full_name: "other" } }, { head_repository: { full_name: "fork" } },
  { repository: { full_name: legacy, id: 1437714333 } }, { head_repository: { full_name: legacy, id: 1437714333 } },
  { repository: { full_name: repo, id: 1437714334 } }, { head_repository: { full_name: repo, id: 1 } },
  { repository: { full_name: repo, id: "1437714333" } }, { repository: { full_name: repo } },
  { actor: { login: "other" } }, { triggering_actor: { login: "other" } } ]) {
  test("another publication identity/attempt cannot reuse a receipt", () => {
    expect(() => validatePublication(env, receipt, { ...run, ...change })).toThrow();
  });
}
