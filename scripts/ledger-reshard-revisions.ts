/**
 * One-time reshard of the S3 upload ledger's revision index from 64 to 4096 shards (src/session-ingest/revision-reshard.ts).
 *
 *   bun scripts/ledger-reshard-revisions.ts --bucket <name> [--region us-west-2] [--org <orgId> ...] [--key-root 96bulls]
 *   bun scripts/ledger-reshard-revisions.ts --execute --kms-key-arn <arn> --bucket <name> [--org <orgId> ...]
 *   bun scripts/ledger-reshard-revisions.ts --verify --bucket <name> [--org <orgId> ...]
 *
 * Dry run by default (lists and reads only). --execute merges each legacy entry into the new layout under the user's
 * current fence (idempotent; safe to rerun and to run while admissions run). Legacy shards are never deleted.
 * --verify (read-only) reads every legacy entry back from the new layout and exits 0 only with `verify_passed: true`
 * (missing, invalid and failed all zero). Only then may the operator add `"revisionLayout":"4096"` to that org's
 * fence-authority marker; admission and the drain refuse the org until the marker has it.
 * Credentials: `--credentials keys` (default) reads AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY [/ AWS_SESSION_TOKEN];
 * `--credentials podIdentity` uses EKS Pod Identity (AWS_CONTAINER_CREDENTIALS_FULL_URI, trusted endpoints only, refreshed
 * by the store), the same helper as the re-arm sweeper and every Locker client, so it runs as a one-off Job on a pod's
 * service account. Credentials are never logged. Prints one JSON summary per org: counts only, never keys or ids.
 */
import { initScrubEngine } from "../packages/pipeline/src/lib/scrub-engine/index.ts";
import { parseArgs } from "node:util";
import { createS3Store, type ObjectStore } from "../src/shared/vendor.ts";
import { reshardRevisions } from "../src/session-ingest/revision-reshard.ts";
import { podIdentityCredentials, trustedCredentialsUri } from "../src/shared/locker.ts";

type AwsCredentials = { accessKeyId: string; secretAccessKey: string; sessionToken?: string; expiresAt?: Date };
export type ReshardCredentialMode = "keys" | "podIdentity";

/**
 * The store's credentials for one run. `keys`: today's static AWS_* environment. `podIdentity`: a provider the store calls
 * (and so refreshes) on demand; the first fetch happens here so a missing or untrusted endpoint refuses before any listing.
 * Refusals are fixed codes; nothing credential-like is ever returned in an error.
 */
export async function reshardCredentials(mode: ReshardCredentialMode, env: NodeJS.ProcessEnv, fetcher: typeof fetch = fetch):
  Promise<AwsCredentials | (() => Promise<AwsCredentials>)> {
  if (mode === "podIdentity") {
    const uri = env.AWS_CONTAINER_CREDENTIALS_FULL_URI;
    if (!uri) throw new Error("credentials_missing");
    if (!trustedCredentialsUri(uri)) throw new Error("credentials_endpoint_untrusted");
    const provider = () => podIdentityCredentials(env, fetcher);
    const first = await provider().catch(() => { throw new Error("credentials_unavailable"); });
    if (!first.accessKeyId || !first.secretAccessKey) throw new Error("credentials_missing");
    return provider;
  }
  const accessKeyId = env.AWS_ACCESS_KEY_ID, secretAccessKey = env.AWS_SECRET_ACCESS_KEY;
  if (!accessKeyId || !secretAccessKey) throw new Error("credentials_missing");
  return { accessKeyId, secretAccessKey, ...(env.AWS_SESSION_TOKEN ? { sessionToken: env.AWS_SESSION_TOKEN } : {}) };
}

const ORG = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/;

if (import.meta.main) {
  let values;
  try {
    ({ values } = parseArgs({ options: {
      bucket: { type: "string" }, region: { type: "string", default: "us-east-1" }, org: { type: "string", multiple: true },
      "key-root": { type: "string", default: "96bulls" }, execute: { type: "boolean", default: false }, verify: { type: "boolean", default: false }, "kms-key-arn": { type: "string" },
      concurrency: { type: "string", default: "8" }, credentials: { type: "string", default: "keys" },
    } }));
  } catch {
    values = undefined;
  }
  const kms = values?.["kms-key-arn"];
  if (!values?.bucket || (values.execute && values.verify) || (values.execute && !kms?.startsWith("arn:aws:kms:")) || (values.org ?? []).some((o) => !ORG.test(o)) || !/^[a-z0-9_.-]{1,64}$/.test(values["key-root"]!)
      || (values.credentials !== "keys" && values.credentials !== "podIdentity")) {
    console.error("usage: --bucket <name> [--region r] [--org <orgId> ...] [--key-root 96bulls] [--concurrency 8] [--credentials keys|podIdentity] [--execute --kms-key-arn <arn> | --verify]; dry run unless --execute");
    process.exit(2);
  }
  let credentials: Awaited<ReturnType<typeof reshardCredentials>>;
  try {
    credentials = await reshardCredentials(values.credentials as ReshardCredentialMode, process.env);
  } catch (error) {
    const m = (error as Error)?.message;
    console.error(JSON.stringify({ refused: typeof m === "string" && /^[a-z0-9_]{1,64}$/.test(m) ? m : "credentials_unavailable" }));
    process.exit(2);
  }
  // Scrub engine (docs/scrub-engine.md, "Default flip"): the pinned wasm-bindgen engine, read from the repo checkout.
  await initScrubEngine();
  const keyRoot = values["key-root"]!;
  const store = createS3Store({
    bucket: values.bucket, region: values.region!,
    credentials,
    ...(values.execute ? { serverSideEncryption: "aws:kms" as const, sseKmsKeyId: kms! } : {}),
  }) as unknown as ObjectStore;
  const prefixes = values.org?.length ? values.org.map((o) => `${keyRoot}/.admission-inbox/${o}/`) : [`${keyRoot}/.admission-inbox/`];
  let failed = false;
  for (const [i, prefix] of prefixes.entries()) {
    try {
      const summary = await reshardRevisions(store, keyRoot, { prefix, execute: values.execute!, verify: values.verify!, concurrency: Number(values.concurrency) });
      const clean = summary.failed === 0 && summary.invalid === 0 && summary.missing === 0;
      const mode = values.execute ? "execute" : values.verify ? "verify" : "dry_run";
      process.stdout.write(JSON.stringify({ org_index: i, mode, ...summary, ...(values.verify ? { verify_passed: clean } : {}) }) + "\n");
      if (!clean) failed = true;
    } catch (error) {
      const m = (error as Error)?.message;
      process.stdout.write(JSON.stringify({ org_index: i, refused: typeof m === "string" && /^[a-z0-9_]{1,64}$/.test(m) ? m : "internal_error" }) + "\n");
      failed = true;
    }
  }
  process.exitCode = failed ? 1 : 0;
}
