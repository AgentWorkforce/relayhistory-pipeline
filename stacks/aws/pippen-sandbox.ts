/**
 * Pippen Tools sandbox mode (`inputs.pippenSandbox`, stage aws-proof-sandbox): what
 * the stage changes so that everything it creates fits the pippen agent-session
 * boundary and `pippen sandbox down` finds and deletes it (Khaliq, 2026-11-01,
 * "Widen pippen boundary"; pippen-tools iam/drafts/agent-session-boundary.policy.json
 * and pippen-sweeper-role.json). Pure: data in, props out. Every other stage is
 * untouched: none of this applies without `pippenSandbox`.
 *
 *   - IAM roles: path /pippen/workload/ (the only path the boundary lets the
 *     session create roles under, and the one the sweeper deletes), each with the
 *     boundary. That includes the EKS Auto Mode cluster and node roles, which
 *     Alchemy would otherwise create itself at path / with no boundary.
 *   - Tags: pippen:sandbox=true on every resource (the sweeper lists by it, and the
 *     boundary grants CloudTrail, WAFv2, ACM, ECR and Budgets only with it),
 *     including the budget, and the load balancer and EBS volumes that EKS Auto
 *     Mode creates from Kubernetes objects.
 *   - Budget name: prefixed pippen-sbx- (the boundary lets the session create
 *     only budget/pippen-sbx-*, and the sweeper deletes those by name: Budgets
 *     is scoped by name, so the agent can never touch the baseline budget).
 *   - No ECR repository policy (the boundary denies SetRepositoryPolicy:
 *     a resource policy could grant push past it), so ecr.pullAccounts is refused.
 *   - S3 bucket names: prefixed pippen-sbx- (S3 deletes cannot be tag-conditioned,
 *     so pippen-sweeper may delete only pippen-sbx-* buckets).
 */
export const PIPPEN_SANDBOX_TAG: Readonly<Record<string, string>> = Object.freeze({ "pippen:sandbox": "true" });
export const PIPPEN_WORKLOAD_ROLE_PATH = "/pippen/workload/";
export const PIPPEN_BUCKET_PREFIX = "pippen-sbx-";

/** `96bulls-<stage>-<base>-<account>`, with the pippen-sbx- prefix in a Pippen sandbox. */
export function stageBucketName(stage: string, base: string, accountId: string, pippenSandbox: boolean): string {
  const name = `${pippenSandbox ? PIPPEN_BUCKET_PREFIX : ""}96bulls-${stage}-${base}-${accountId}`;
  if (name.length > 63) throw new Error(`96bulls: bucket name ${name} is longer than S3's 63 characters. Nothing was created.`);
  return name;
}

/** `96bulls-<stage>-monthly`, with the pippen-sbx- prefix in a Pippen sandbox. */
export const stageBudgetName = (stage: string, pippenSandbox: boolean) => `${pippenSandbox ? PIPPEN_BUCKET_PREFIX : ""}96bulls-${stage}-monthly`;

/** The managed policies EKS Auto Mode needs (the same lists Alchemy's own Auto Mode roles attach). */
export const AUTO_MODE_CLUSTER_POLICIES = Object.freeze([
  "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy",
  "arn:aws:iam::aws:policy/AmazonEKSComputePolicy",
  "arn:aws:iam::aws:policy/AmazonEKSBlockStoragePolicy",
  "arn:aws:iam::aws:policy/AmazonEKSLoadBalancingPolicy",
  "arn:aws:iam::aws:policy/AmazonEKSNetworkingPolicy",
] as const);
export const AUTO_MODE_NODE_POLICIES = Object.freeze(["arn:aws:iam::aws:policy/AmazonEKSWorkerNodeMinimalPolicy", "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryPullOnly"] as const);

/**
 * The EKS Auto Mode cluster and node roles, declared by the stage (passed to the
 * cluster as roleArn and computeConfig.nodeRoleArn) so they sit under
 * /pippen/workload/ with the boundary. Both are capped by that boundary, which
 * grants what Auto Mode calls (pippen-tools AutoModeInstanceProfiles,
 * AutoModeNodesAndAmiLookup, ReadWorkloadExtras, ManageTaggedWorkloadExtras).
 */
export function autoModeRoleProps(i: { stage: string; permissionsBoundaryArn: string; tags: Record<string, string> }) {
  const role = (id: "cluster" | "node", service: string, actions: string[], managedPolicyArns: readonly string[], description: string) => ({
    roleName: `96bulls-${i.stage}-${id}`,
    path: PIPPEN_WORKLOAD_ROLE_PATH,
    description: `96bulls ${i.stage}: ${description}`,
    assumeRolePolicyDocument: {
      Version: "2012-10-17" as const,
      Statement: [{ Effect: "Allow" as const, Principal: { Service: service }, Action: actions }],
    },
    managedPolicyArns: [...managedPolicyArns],
    maxSessionDuration: "1 hour" as const,
    permissionsBoundary: i.permissionsBoundaryArn,
    tags: { ...i.tags, "96bulls:bounded": "true" },
  });
  return {
    cluster: role("cluster", "eks.amazonaws.com", ["sts:AssumeRole", "sts:TagSession"], AUTO_MODE_CLUSTER_POLICIES, "EKS Auto Mode cluster role"),
    node: role("node", "ec2.amazonaws.com", ["sts:AssumeRole"], AUTO_MODE_NODE_POLICIES, "EKS Auto Mode node role"),
  };
}

/** IngressClassParams `spec.tags`: what EKS Auto Mode puts on the load balancer, target groups and security groups it creates. */
export const ingressClassTags = (tags: Readonly<Record<string, string>>) => Object.entries(tags).map(([key, value]) => ({ key, value }));

/** StorageClass parameters that tag every EBS volume the CSI driver creates (`tagSpecification_<n>: key=value`). */
export const storageClassTagParameters = (tags: Readonly<Record<string, string>>) =>
  Object.fromEntries(Object.entries(tags).map(([key, value], n) => [`tagSpecification_${n + 1}`, `${key}=${value}`]));
