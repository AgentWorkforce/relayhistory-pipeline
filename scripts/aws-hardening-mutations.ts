/**
 * Mutation check for the AWS proof-stage hardening (PR #75): revert one control
 * at a time and require that at least one test turns red. A control whose
 * reversion leaves every test green is untested, and this script fails.
 *
 *   bun scripts/aws-hardening-mutations.ts            # all mutations
 *   bun scripts/aws-hardening-mutations.ts --list     # names only
 *
 * Each mutation edits one file in place, runs the named test files, and
 * restores the file (also on error or Ctrl-C). Run it on a clean tree.
 */
import { spawnSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("..", import.meta.url));

interface Mutation {
  name: string;
  file: string;
  from: string;
  to: string;
  tests: string[];
  /** Substrings of test titles; at least one must fail with an assertion for the mutation to count as killed. */
  expect: string[];
}

const S = "./stacks/aws/__tests__/";
const M: Mutation[] = [
  { name: "public endpoint 0.0.0.0/0 by default", file: "stacks/aws/hardening.ts", from: ": { endpointPrivateAccess: true, endpointPublicAccess: false };", to: ': { endpointPrivateAccess: true, endpointPublicAccess: true, publicAccessCidrs: ["0.0.0.0/0"] };', tests: [`${S}hardening.test.ts`], expect: ["is private only by default"] },
  { name: "only the api log type", file: "stacks/aws/hardening.ts", from: '["api", "audit", "authenticator", "controllerManager", "scheduler"] as const', to: '["api"] as const', tests: [`${S}hardening.test.ts`], expect: ["enables all five log types"] },
  { name: "worker gets s3:DeleteObject", file: "stacks/aws/hardening.ts", from: 'Action: ["s3:PutObject"], Resource: [sessionObjects(i.lockerBucketArn, "/summary/*")]', to: 'Action: ["s3:PutObject", "s3:DeleteObject"], Resource: [sessionObjects(i.lockerBucketArn, "/summary/*")]', tests: [`${S}hardening.test.ts`], expect: ["grants s3:DeleteObject to no pod role"] },
  { name: "worker Put on every session object", file: "stacks/aws/hardening.ts", from: 'sessionObjects(i.lockerBucketArn, "/summary/*")', to: 'sessionObjects(i.lockerBucketArn, "")', tests: [`${S}hardening.test.ts`], expect: ["write only encrypted summaries"] },
  { name: "search worker reuses the worker policy", file: "stacks/aws/hardening.ts", from: 'policyName: "no-locker", policy: searchWorkerPolicy(i)', to: 'policyName: "no-locker", policy: workerPolicy(i)', tests: [`${S}hardening.test.ts`], expect: ["search worker an explicit role"] },
  { name: "services image by tag accepted", file: "stacks/aws/hardening.ts", from: '    assertDigestPinned("eks.imageDigest", i.imageDigest);\n    throw new Error', to: "    if (false) throw new Error", tests: [`${S}hardening.test.ts`], expect: ["bare digest with its 40-hex source commit"] },
  { name: "CNPG allowed on sessions-dev (empty database)", file: "stacks/aws/hardening.ts", from: 'if (b === "cnpg" && !CNPG_STAGE_RE.test(stage))', to: "if (false)", tests: [`${S}hardening.test.ts`], expect: ["allows CNPG only on proof and sandbox stages"] },
  { name: "noncurrent-version expiry on by default", file: "stacks/aws/hardening.ts", from: "    ...(noncurrentVersionDays !== undefined\n", to: "    ...(true\n", tests: [`${S}hardening.test.ts`], expect: ["only when a stage opts in"] },
  { name: "Relaycast as a LoadBalancer", file: "stacks/aws/proof-addons.ts", from: 'service: { type: "ClusterIP" },', to: 'service: { type: "LoadBalancer" },', tests: [`${S}proof-addons.test.ts`], expect: ["is ClusterIP only"] },
  { name: "Relaycast tag image accepted", file: "stacks/aws/proof-addons.ts", from: 'if (!DIGEST_RE.test(i.imageDigest ?? "")) assertDigestPinned("eks.relaycast.imageDigest", i.imageDigest);', to: "", tests: [`${S}proof-addons.test.ts`], expect: ["refuses tag-only images"] },
  { name: "CNPG on the default storage class", file: "stacks/aws/proof-addons.ts", from: 'storage: { size: "20Gi", storageClass: STORAGE_CLASS },', to: 'storage: { size: "20Gi" },', tests: [`${S}proof-addons.test.ts`], expect: ["shared by every in-cluster volume", "switches the apps to the CNPG Secret"] },
  { name: "unencrypted StorageClass", file: "stacks/aws/proof-addons.ts", from: 'parameters: { type: "gp3", encrypted: "true" },', to: 'parameters: { type: "gp3" },', tests: [`${S}proof-addons.test.ts`], expect: ["is gp3 with encryption on"] },
  { name: "HTTP :80 listener", file: "stacks/aws/proof-addons.ts", from: `'[{"HTTPS":443}]'`, to: `'[{"HTTP":80},{"HTTPS":443}]'`, tests: [`${S}proof-addons.test.ts`], expect: ["never opens :80"] },
  { name: "WAF without the rate limit", file: "stacks/aws/proof-addons.ts", from: '      Name: "rate-limit-per-ip",', to: '      Name: "rate-limit-off",', tests: [`${S}proof-addons.test.ts`], expect: ["a per-IP rate limit"] },
  { name: "default-deny removed", file: "stacks/aws/proof-addons.ts", from: '  np(namespace, "default-deny", { podSelector: {}, policyTypes: ["Ingress", "Egress"] }),\n', to: "", tests: [`${S}proof-addons.test.ts`], expect: ["default-denies ingress and egress"] },
  { name: "AlleyOop:8080 open to the whole VPC (WAF bypass)", file: "stacks/aws/proof-addons.ts", from: 'ingress: [{ from: alleyoopIngressSubnets(i.ingestScheme ?? "internet-facing").map((cidr) => ({ ipBlock: { cidr } })), ports: tcp(8080) }],', to: "ingress: [{ from: [{ ipBlock: { cidr: VPC_CIDR } }], ports: tcp(8080) }],", tests: [`${S}proof-addons.test.ts`], expect: ["only the ALB's public subnets"] },
  { name: "proof volumes deleted on every stage", file: "stacks/aws/eks.ts", from: 'storageClassManifest(i.purgeOnDestroy ? "Delete" : "Retain")', to: 'storageClassManifest("Delete")', tests: [`${S}proof-addons.test.ts`], expect: ["deletes volumes only when a proof stage asks"] },
  { name: "NetworkPolicy enforcement in the same apply as the policies", file: "stacks/aws/eks.ts", from: '    if (i.networkPolicyEnforcement)\n      yield* Kubernetes.Manifest("network-policy-controller"', to: '    if (true)\n      yield* Kubernetes.Manifest("network-policy-controller"', tests: [`${S}rollout.test.ts`], expect: ["enforces only in a later, separate apply"] },
  { name: "old shared pods role dropped in the split apply", file: "stacks/aws/eks.ts", from: '    if (i.legacyPodsRole === "keep") yield* AWS.IAM.Role("pods-role", legacyPodsRoleProps(i));', to: "", tests: [`${S}rollout.test.ts`], expect: ["keeps the old shared pods role"] },
  { name: "OTel collector by tag only", file: "stacks/aws/eks.ts", from: ", digest: PINNED_IMAGES.otelCollector.digest },", to: " },", tests: [`${S}hardening.test.ts`], expect: ["renders the collector by digest"] },
  { name: "deployer regains Locker access", file: "stacks/aws/alchemy.aws.run.ts", from: "inlinePolicies: { ecrPush: ecrPushPolicy(inputs.region, inputs.accountId) as never },", to: 'inlinePolicies: { ecrPush: ecrPushPolicy(inputs.region, inputs.accountId) as never, locker: { Statement: [{ Action: ["s3:GetObject"] }] } as never },', tests: [`${S}hardening.test.ts`], expect: ["push to the services ECR repository and nothing else"] },
  { name: "live STS account not bound to the inputs", file: "stacks/aws/alchemy.aws.run.ts", from: "    assertAwsDeploymentTarget(", to: "    void (", tests: [`${S}deploy-guard.test.ts`], expect: ["binds the live STS account and region"] },
  { name: "mode taken from env only (direct alchemy destroy passes as deploy)", file: "stacks/aws/deploy-guard.ts", from: '  if (fromArgv === "destroy" && declared !== "destroy")', to: "  if (false)", tests: [`${S}deploy-guard.test.ts`], expect: ["cannot pass as a deploy"] },
  { name: "destroy of sessions-dev allowed inside the stack", file: "stacks/aws/deploy-guard.ts", from: 'export const NON_DESTROYABLE = (stage: string) => stage === "aws-sessions-dev" || /prod/.test(stage);', to: "export const NON_DESTROYABLE = (_stage: string) => false;", tests: [`${S}deploy-guard.test.ts`, "./src/cli/__tests__/aws.test.ts"], expect: ["refuses destroy of sessions-dev", "down destroys only throwaway stages"] },
  { name: "placeholder clearance accepted", file: "stacks/aws/deploy-guard.ts", from: "  if (placeholders.length) throw new Error(`96bulls: aws-inputs.json still has placeholder values", to: "  if (false) throw new Error(`96bulls: aws-inputs.json still has placeholder values", tests: [`${S}deploy-guard.test.ts`], expect: ["refuses every mismatch in the record"] },
  { name: "impossible dates accepted", file: "stacks/aws/deploy-guard.ts", from: "  return t.getUTCFullYear() === y && t.getUTCMonth() === mo - 1 && t.getUTCDate() === d ? t : undefined;", to: "  return t;", tests: [`${S}deploy-guard.test.ts`], expect: ["accepts only real calendar dates"] },
  { name: "approval accepted without decision \"approved\"", file: "stacks/aws/deploy-guard.ts", from: '  if (a.decision !== "approved") refuse(', to: '  if (false) refuse(', tests: [`${S}deploy-guard.test.ts`], expect: ["decision is exactly"] },
  { name: "approval for another operation accepted", file: "stacks/aws/deploy-guard.ts", from: '  if (!Array.isArray(a.operations) || !a.operations.includes(mode)) refuse(`approval.operations does not include "${mode}"`);', to: "", tests: [`${S}deploy-guard.test.ts`], expect: ["refuses every mismatch in the record"] },
  { name: "image tag/revision mismatch accepted", file: "stacks/aws/preflight.ts", from: "  if (!(d!.imageTags ?? []).includes(want))", to: "  if (false)", tests: [`${S}preflight.test.ts`], expect: ["refuses a digest built from another commit"] },
  { name: "NetworkPolicy phase 2 without state evidence", file: "stacks/aws/preflight.ts", from: "      return fail(`NetworkPolicy ${policyLogicalId(p)} is not in Alchemy state: apply phase 1 first`);", to: "      continue;", tests: [`${S}preflight.test.ts`], expect: ["refuses on a fresh stage"] },
  { name: "unapproved services image accepted", file: "stacks/aws/inputs.ts", from: "    if (!hit)\n", to: "    if (false)\n", tests: [`${S}inputs.test.ts`], expect: ["approved versions.lock.json entry"] },
  { name: "bucket purge allowed on sessions-dev", file: "stacks/aws/inputs.ts", from: "  if (inputs.teardown && !CNPG_STAGE_RE.test(stage))", to: "  if (false)", tests: [`${S}inputs.test.ts`], expect: ["allows bucket purge on destroy only"] },
  { name: "captured plan not bound to inputs", file: "src/cli/plan-provenance.ts", from: '  cmp("inputs", p.inputs.sha256, o.inputsSha256);', to: "", tests: ["./src/cli/__tests__/plan-provenance.test.ts"], expect: ["any change to source, inputs, state"] },
  { name: "captured plan not bound to state", file: "src/cli/plan-provenance.ts", from: '  cmp("state digest", p.state.versionsDigest, o.state.versionsDigest);', to: "", tests: ["./src/cli/__tests__/plan-provenance.test.ts"], expect: ["any change to source, inputs, state"] },
  { name: "RelayAuth plain HTTP open to the whole namespace", file: "stacks/aws/relayauth.ts", from: "ingressFrom: relayauthPlainIngressFrom(i.clientNamespace),", to: 'ingressFrom: [{ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": i.clientNamespace } } }],', tests: [`${S}relayauth.test.ts`], expect: ["admits plain HTTP only from the TLS proxy"] },
  { name: "TLS proxy accepts TLS 1.2", file: "stacks/aws/relayauth-tls.ts", from: '"    ssl_protocols TLSv1.3;",', to: '"    ssl_protocols TLSv1.2 TLSv1.3;",', tests: [`${S}relayauth-tls.test.ts`], expect: ["serves TLS 1.3 only"] },
  { name: "stale 'production HoopDb = Aurora, never Supabase' doc line", file: "docs/aws-stage.md", from: "- **Proof-stage HoopDb:**", to: "- **Production HoopDb:** dedicated Aurora PostgreSQL 17 in the existing `sessions-prod` account, never Supabase or in-cluster CloudNativePG.\n- **Proof-stage HoopDb:**", tests: [`${S}data-plane-docs.test.ts`], expect: ["production HoopDb named as Aurora/RDS", "Supabase excluded from production"] },
  { name: "1Password recipe splits signing-key.pem into section + field", file: "docs/aws-stage.md", from: '"signing-key\\.pem[password]=$k"', to: '"signing-key.pem[password]=$k"', tests: [`${S}op-recipes.test.ts`], expect: ["escape every dot in an op field name"] },
  { name: "state digest ignores delete markers (transient lease write invisible)", file: "src/cli/plan-provenance.ts", from: "    ...(l.DeleteMarkers ?? []).map((d) => `D ${d.Key} ${d.VersionId} ${d.IsLatest}`),\n", to: "", tests: ["./src/cli/__tests__/plan-provenance.test.ts"], expect: ["transient write that was undone"] },
  { name: "state snapshot usable by deploy", file: "stacks/aws/state-snapshot.ts", from: '  if (!argv.includes("plan")) throw', to: "  if (false) throw", tests: [`${S}state-snapshot.test.ts`], expect: ["refuses to load for anything but"] },
  { name: "--detailed not forwarded", file: "src/cli/aws.ts", from: '...(flags.detailed ? ["--detailed"] : [])', to: "", tests: ["./src/cli/__tests__/aws.test.ts"], expect: ["is forwarded to alchemy plan"] },
  { name: "ingest requireAuth removed", file: "src/alleyoop/routes.ts", from: '  v1.use("*", async (c, next) => requireAuth(c as never, next));', to: "", tests: ["./src/alleyoop/__tests__/routes-auth.test.ts"], expect: ["refuses a request without credentials"] },
];

if (process.argv.includes("--list")) {
  for (const m of M) console.log(m.name);
  process.exit(0);
}

let restore: (() => void) | undefined;
for (const sig of ["SIGINT", "SIGTERM"] as const) process.on(sig, () => (restore?.(), process.exit(130)));

let failures = 0;
for (const m of M) {
  const path = `${ROOT}${m.file}`;
  const original = readFileSync(path, "utf8");
  const count = original.split(m.from).length - 1;
  if (count !== 1) {
    console.log(`STALE  ${m.name}: pattern found ${count} times in ${m.file}`);
    failures++;
    continue;
  }
  restore = () => writeFileSync(path, original);
  try {
    writeFileSync(path, original.replace(m.from, m.to));
    const r = spawnSync("bun", ["test", "--timeout", "60000", ...m.tests], { cwd: ROOT, encoding: "utf8" });
    const out = `${r.stdout}${r.stderr}`;
    const ran = Number(/Ran (\d+) tests?/.exec(out)?.[1] ?? 0);
    const failed = [...out.matchAll(/^\(fail\) (.+?)(?: \[[\d.]+m?s\])?$/gm)].map((x) => x[1]!);
    const assertion = /error: expect\(/.test(out);
    const hit = failed.filter((f) => m.expect.some((e) => f.includes(e)));
    // Killed only if tests ran, an assertion failed, and one of the named tests is among the failures.
    if (ran > 0 && assertion && hit.length) console.log(`KILLED ${m.name} | ${hit.join("; ")}`);
    else {
      console.log(`ALIVE  ${m.name}: ran=${ran} assertion=${assertion} failed=${JSON.stringify(failed)}`);
      failures++;
    }
  } finally {
    restore();
    restore = undefined;
  }
}
console.log(`${M.length - failures}/${M.length} mutations caught`);
process.exit(failures ? 1 : 0);
