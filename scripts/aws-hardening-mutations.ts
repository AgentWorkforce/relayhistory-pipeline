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
}

const S = "stacks/aws/__tests__/";
const M: Mutation[] = [
  { name: "public endpoint 0.0.0.0/0 by default", file: "stacks/aws/hardening.ts", from: ": { endpointPrivateAccess: true, endpointPublicAccess: false };", to: ': { endpointPrivateAccess: true, endpointPublicAccess: true, publicAccessCidrs: ["0.0.0.0/0"] };', tests: [`${S}hardening.test.ts`] },
  { name: "only the api log type", file: "stacks/aws/hardening.ts", from: '["api", "audit", "authenticator", "controllerManager", "scheduler"] as const', to: '["api"] as const', tests: [`${S}hardening.test.ts`] },
  { name: "worker gets s3:DeleteObject", file: "stacks/aws/hardening.ts", from: 'Action: ["s3:PutObject"], Resource: [sessionObjects(i.lockerBucketArn, "/summary/*")]', to: 'Action: ["s3:PutObject", "s3:DeleteObject"], Resource: [sessionObjects(i.lockerBucketArn, "/summary/*")]', tests: [`${S}hardening.test.ts`] },
  { name: "worker Put on every session object", file: "stacks/aws/hardening.ts", from: 'sessionObjects(i.lockerBucketArn, "/summary/*")', to: 'sessionObjects(i.lockerBucketArn, "")', tests: [`${S}hardening.test.ts`] },
  { name: "search worker reuses the worker policy", file: "stacks/aws/hardening.ts", from: 'policyName: "no-locker", policy: searchWorkerPolicy(i)', to: 'policyName: "no-locker", policy: workerPolicy(i)', tests: [`${S}hardening.test.ts`] },
  { name: "services image by tag accepted", file: "stacks/aws/hardening.ts", from: '    assertDigestPinned("eks.imageDigest", i.imageDigest);\n    throw new Error', to: "    if (false) throw new Error", tests: [`${S}hardening.test.ts`] },
  { name: "CNPG allowed on a production stage", file: "stacks/aws/hardening.ts", from: 'if (b === "cnpg" && /prod/.test(stage) && !CNPG_STAGE_RE.test(stage))', to: "if (false)", tests: [`${S}hardening.test.ts`, `${S}inputs.test.ts`] },
  { name: "CNPG allowed on sessions-dev (empty database)", file: "stacks/aws/hardening.ts", from: 'if (b === "cnpg" && !CNPG_STAGE_RE.test(stage))', to: "if (false)", tests: [`${S}hardening.test.ts`] },
  { name: "noncurrent-version expiry on by default", file: "stacks/aws/hardening.ts", from: "    ...(noncurrentVersionDays !== undefined\n", to: "    ...(true\n", tests: [`${S}hardening.test.ts`] },
  { name: "Relaycast as a LoadBalancer", file: "stacks/aws/proof-addons.ts", from: 'service: { type: "ClusterIP" },', to: 'service: { type: "LoadBalancer" },', tests: [`${S}proof-addons.test.ts`] },
  { name: "Relaycast tag image accepted", file: "stacks/aws/proof-addons.ts", from: 'if (!DIGEST_RE.test(i.imageDigest ?? "")) assertDigestPinned("eks.relaycast.imageDigest", i.imageDigest);', to: "", tests: [`${S}proof-addons.test.ts`] },
  { name: "CNPG on the default storage class", file: "stacks/aws/proof-addons.ts", from: 'storage: { size: "20Gi", storageClass: STORAGE_CLASS },', to: 'storage: { size: "20Gi" },', tests: [`${S}proof-addons.test.ts`] },
  { name: "unencrypted StorageClass", file: "stacks/aws/proof-addons.ts", from: 'parameters: { type: "gp3", encrypted: "true" },', to: 'parameters: { type: "gp3" },', tests: [`${S}proof-addons.test.ts`] },
  { name: "HTTP :80 listener", file: "stacks/aws/proof-addons.ts", from: `'[{"HTTPS":443}]'`, to: `'[{"HTTP":80},{"HTTPS":443}]'`, tests: [`${S}proof-addons.test.ts`] },
  { name: "WAF without the rate limit", file: "stacks/aws/proof-addons.ts", from: '      Name: "rate-limit-per-ip",', to: '      Name: "rate-limit-off",', tests: [`${S}proof-addons.test.ts`] },
  { name: "default-deny removed", file: "stacks/aws/proof-addons.ts", from: '  np(namespace, "default-deny", { podSelector: {}, policyTypes: ["Ingress", "Egress"] }),\n', to: "", tests: [`${S}proof-addons.test.ts`] },
  { name: "AlleyOop:8080 open to the whole VPC (WAF bypass)", file: "stacks/aws/proof-addons.ts", from: 'ingress: [{ from: alleyoopIngressSubnets(i.ingestScheme ?? "internet-facing").map((cidr) => ({ ipBlock: { cidr } })), ports: tcp(8080) }],', to: "ingress: [{ from: [{ ipBlock: { cidr: VPC_CIDR } }], ports: tcp(8080) }],", tests: [`${S}proof-addons.test.ts`] },
  { name: "NetworkPolicy enforcement in the same apply as the policies", file: "stacks/aws/eks.ts", from: "    if (i.networkPolicyEnforcement)\n      yield* Kubernetes.Manifest(\"network-policy-controller\"", to: "    if (true)\n      yield* Kubernetes.Manifest(\"network-policy-controller\"", tests: [`${S}rollout.test.ts`] },
  { name: "old shared pods role dropped in the split apply", file: "stacks/aws/eks.ts", from: '    if (i.legacyPodsRole === "keep") yield* AWS.IAM.Role("pods-role", legacyPodsRoleProps(i));', to: "", tests: [`${S}hardening.test.ts`, `${S}rollout.test.ts`] },
  { name: "OTel collector by tag only", file: "stacks/aws/eks.ts", from: ", digest: PINNED_IMAGES.otelCollector.digest },", to: " },", tests: [`${S}hardening.test.ts`] },
  { name: "deployer regains Locker access", file: "stacks/aws/alchemy.aws.run.ts", from: "inlinePolicies: { ecrPush: ecrPushPolicy(inputs.region, inputs.accountId) as never },", to: 'inlinePolicies: { ecrPush: ecrPushPolicy(inputs.region, inputs.accountId) as never, locker: { Statement: [{ Action: ["s3:GetObject"] }] } as never },', tests: [`${S}hardening.test.ts`] },
  { name: "live STS account not bound to the inputs", file: "stacks/aws/alchemy.aws.run.ts", from: "    assertAwsDeploymentTarget(", to: "    void (", tests: [`${S}deploy-guard.test.ts`] },
  { name: "placeholder clearance accepted", file: "stacks/aws/deploy-guard.ts", from: "  if (placeholders.length) throw new Error(`96bulls: aws-inputs.json still has placeholder values", to: "  if (false) throw new Error(`96bulls: aws-inputs.json still has placeholder values", tests: [`${S}deploy-guard.test.ts`] },
  { name: "undated clearance accepted for deploy", file: "stacks/aws/deploy-guard.ts", from: "  if (!/\\b20\\d\\d-\\d\\d-\\d\\d\\b/.test(c) || !/khaliq/i.test(c) || c.length < 24)", to: "  if (false)", tests: [`${S}deploy-guard.test.ts`] },
  { name: "RelayAuth plain HTTP open to the whole namespace", file: "stacks/aws/relayauth.ts", from: "ingressFrom: relayauthPlainIngressFrom(i.clientNamespace),", to: 'ingressFrom: [{ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": i.clientNamespace } } }],', tests: [`${S}relayauth.test.ts`] },
  { name: "TLS proxy accepts TLS 1.2", file: "stacks/aws/relayauth-tls.ts", from: '"    ssl_protocols TLSv1.3;",', to: '"    ssl_protocols TLSv1.2 TLSv1.3;",', tests: [`${S}relayauth-tls.test.ts`] },
  { name: "stale 'production HoopDb = Aurora, never Supabase' doc line", file: "docs/aws-stage.md", from: "- **Proof-stage HoopDb:**", to: "- **Production HoopDb:** dedicated Aurora PostgreSQL 17 in the existing `sessions-prod` account, never Supabase or in-cluster CloudNativePG.\n- **Proof-stage HoopDb:**", tests: [`${S}data-plane-docs.test.ts`] },
  { name: "destroy allowed on sessions-dev", file: "src/cli/aws.ts", from: 'export const NON_DESTROYABLE = (stage: string) => stage === "aws-sessions-dev" || /prod/.test(stage);', to: "export const NON_DESTROYABLE = (_stage: string) => false;", tests: ["src/cli/__tests__/aws.test.ts"] },
  { name: "ingest requireAuth removed", file: "src/alleyoop/routes.ts", from: '  v1.use("*", async (c, next) => requireAuth(c as never, next));', to: "", tests: ["src/alleyoop/__tests__/routes-auth.test.ts"] },
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
    const red = Number(/(\d+) fail/.exec(out)?.[1] ?? (r.status === 0 ? 0 : 1));
    if (red > 0) console.log(`RED    ${red} fail | ${m.name}`);
    else {
      console.log(`GREEN  ${m.name}: no test caught it`);
      failures++;
    }
  } finally {
    restore();
    restore = undefined;
  }
}
console.log(`${M.length - failures}/${M.length} mutations caught`);
process.exit(failures ? 1 : 0);
